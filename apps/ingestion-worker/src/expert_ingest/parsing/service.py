"""Fenced parse preparation. Parse readiness never completes an ingestion job."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import UUID, NAMESPACE_URL, uuid5

from psycopg import Error as PostgresError, OperationalError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from expert_ingest.source import LocalSource
from expert_ingest.transport import JobRejected, LEASE_ERRORS
from expert_ingest.worker import Execution, PipelineFailure

from .artifact_store import ArtifactReference, ParseArtifactStore, finish_storage_call
from .dto import CanonicalDocument, Diagnostic, ParsedArtifact, QualityReport, RegionReview
from .quality import ArtifactValidator
from .runner import ParserFailure, ParserRunner


@dataclass(frozen=True)
class ParsedGeneration:
    parse_generation_id: UUID
    artifact_object_id: UUID
    artifact_sha256: str
    canonical_document: CanonicalDocument
    quality_report: QualityReport


def _validated_artifact(wire: bytes, source: LocalSource, parse_id: UUID,
                        recipe: tuple[str, str, str, bool], reviews: tuple[RegionReview, ...]) -> ParsedArtifact:
    try:
        artifact = ParsedArtifact.model_validate_json(wire)
        document = artifact.document
        if (document.version_id != source.reference.version_id
                or document.parse_generation_id != parse_id
                or document.source_sha256 != source.reference.sha256
                or document.manifest.parser_fingerprint != recipe[0]
                or document.manifest.normalizer_version != recipe[1]
                or document.manifest.structure_version != recipe[2]
                or document.manifest.runtime_profile is None
                or document.manifest.runtime_profile.enforced != recipe[3]):
            raise ParserFailure("GENERATION_INVALID")
        verified_quality = ArtifactValidator(trusted_region_reviews=reviews).validate(document)
        if verified_quality != artifact.quality_report:
            raise ParserFailure("GENERATION_INVALID")
        return artifact
    except (ValidationError, ValueError):
        raise ParserFailure("GENERATION_INVALID") from None


def _load_artifact(path: Path, source: LocalSource, parse_id: UUID,
                   recipe: tuple[str, str, str, bool], reviews: tuple[RegionReview, ...]) -> ParsedArtifact:
    return _validated_artifact(path.read_bytes(), source, parse_id, recipe, reviews)


def _node_batches(document: CanonicalDocument):
    batch: list[dict[str, Any]] = []
    size = 2
    # Parent-first, stable across replay, preserving ordinal within each parent.
    for node in sorted(document.nodes, key=lambda n: (n.level, str(n.parent_id), n.ordinal, str(n.id))):
        value = node.model_dump(mode="json")
        item_size = len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")) + 2
        if item_size > 7 * 1024**2:
            raise ParserFailure("GENERATION_INVALID")
        if batch and (len(batch) == 500 or size + item_size > 7 * 1024**2):
            yield batch
            batch, size = [], 2
        batch.append(value)
        size += item_size
    if batch:
        yield batch


class ParsingService:
    def __init__(self, runner: ParserRunner, artifacts: ParseArtifactStore, *,
                 parser_fingerprint: str, normalizer_version: str = "mapped-nfc-v1",
                 structure_version: str = "generic-tree-v1"):
        if any(not value.strip() or len(value) > 512 for value in (
            parser_fingerprint, normalizer_version, structure_version,
        )):
            raise ValueError("Invalid parsing recipe")
        self.runner, self.artifacts = runner, artifacts
        self.parser_recipe = parser_fingerprint
        self.parser_fingerprint = runner.fingerprint(parser_fingerprint,
            normalizer_version=normalizer_version, structure_version=structure_version)
        self.normalizer_version, self.structure_version = normalizer_version, structure_version
        self._recipe = (self.parser_fingerprint, normalizer_version, structure_version, runner.options.enforce_sandbox)

    async def _query(self, execution: Execution, query: str, parameters: tuple[Any, ...]) -> dict[str, Any]:
        async with execution.transaction() as connection:
            try:
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(query, parameters)
                    row = await cursor.fetchone()
                if row is None:
                    raise ParserFailure("GENERATION_INVALID")
                return row
            except PostgresError as error:
                code = error.diag.message_primary
                if code in LEASE_ERRORS or code in {"SOURCE_REVOKED", "VERSION_CONFLICT"}:
                    raise JobRejected(code) from None
                # Constraint/binding/payload failures are deterministic. Only
                # actual connection/resource errors are a retryable dependency.
                if (isinstance(error, OperationalError) and error.sqlstate is None
                        or error.sqlstate == "55P03"
                        or error.sqlstate and error.sqlstate[:2] in {"08", "53", "57", "40"}):
                    raise ParserFailure("DEPENDENCY_UNAVAILABLE") from None
                raise ParserFailure("GENERATION_INVALID") from None

    async def _call(self, execution: Execution, function: str, *args: Any) -> dict[str, Any]:
        # Function names are internal constants; all values are bound parameters.
        params = (execution.job_id, execution.owner, execution.epoch, *args)
        placeholders = ",".join("%s" for _ in params)
        return await self._query(execution, f"SELECT * FROM {function}({placeholders})", params)

    async def _existing_artifact(self, execution: Execution, parse_id: UUID) -> ArtifactReference:
        row = await self._query(execution,
            "SELECT * FROM app.parse_artifact_intents WHERE parse_generation_id=%s "
            "AND artifact_role='canonical' AND slot='document' AND state='attached'",
            (parse_id,))
        return ArtifactReference.from_intent(row)

    async def prepare(self, execution: Execution, source: LocalSource) -> ParsedGeneration:
        try:
            return await self._prepare(execution, source)
        except ParserFailure as error:
            if error.code in {"PDF_INVALID", "SIZE_LIMIT_EXCEEDED", "GENERATION_INVALID", "SOURCE_UNAVAILABLE"}:
                # Even a parser that cannot produce an artifact leaves a durable
                # safe failure on its own staging attempt. Dependency interruptions
                # keep staging recoverable; cancelled/stale owners cannot write.
                operation = self._operation(execution)
                record = await self._query(execution,
                    "SELECT (SELECT id FROM knowledge.parse_generations WHERE creating_job_id=%s "
                    "AND creating_job_epoch=%s AND operation_id=%s AND status='staging') AS id",
                    (execution.job_id, execution.epoch, operation))
                if record["id"] is not None:
                    report = QualityReport(status="failed", complete_page_count=0, critical_count=1,
                                           warning_count=0, diagnostics=(Diagnostic(code=error.code, severity="critical"),))
                    await self._call(execution, "knowledge.fail_parse", record["id"],
                                     Jsonb(report.model_dump(mode="json")))
            raise PipelineFailure(error.code, retryable=error.code in {
                "DEPENDENCY_UNAVAILABLE", "DEADLINE_EXCEEDED",
            }) from None

    def _operation(self, execution: Execution) -> UUID:
        return uuid5(NAMESPACE_URL, f"expert:parse:{execution.job_id}:{execution.epoch}:{self.parser_fingerprint}")

    async def _prepare(self, execution: Execution, source: LocalSource) -> ParsedGeneration:
        if execution.job_id != source.reference.job_id:
            raise ParserFailure("SOURCE_UNAVAILABLE")
        operation = self._operation(execution)
        parse = await self._call(execution, "knowledge.begin_parse", operation, self.parser_fingerprint,
                                 self.normalizer_version, self.structure_version)
        parse_id = parse["id"]
        if (parse["document_version_id"] != source.reference.version_id
                or parse["source_sha256"] != source.reference.sha256):
            raise ParserFailure("GENERATION_INVALID")
        if parse["status"] == "failed":
            raise ParserFailure("EXTRACTION_QUALITY_FAILED")
        if parse["status"] == "ready":
            reference = await self._existing_artifact(execution, parse_id)
            wire = await self.artifacts.read(reference)
            artifact = await finish_storage_call(_validated_artifact, wire, source, parse_id, self._recipe,
                                                self.runner.trusted_region_reviews)
            if artifact.quality_report.status != "passed":
                raise ParserFailure("GENERATION_INVALID")
            await execution.guard()
            return ParsedGeneration(parse_id, reference.object_id, reference.sha256,
                                    artifact.document, artifact.quality_report)
        metadata = await self._query(execution,
            "SELECT source_metadata->>'title' AS title FROM app.document_versions WHERE id=%s",
            (source.reference.version_id,))
        async with self.runner.run(source.path, version_id=source.reference.version_id,
                                   parse_generation_id=parse_id, source_sha256=source.reference.sha256,
                                   source_size_bytes=source.reference.size_bytes, title=metadata["title"],
                                   parser_recipe=self.parser_recipe, normalizer_version=self.normalizer_version,
                                   structure_version=self.structure_version) as output:
            artifact = await finish_storage_call(_load_artifact, output.path, source, parse_id, self._recipe,
                                                self.runner.trusted_region_reviews)
            # A parser can report failed quality while retaining a valid, useful
            # diagnostic artifact. Store that artifact before recording failure.
            intent = await self._call(execution, "app.reserve_parse_artifact", parse_id, "canonical", "document",
                                      output.sha256, output.size_bytes, "artifacts")
            if intent["state"] not in {"reserved", "attached"}:
                raise ParserFailure("GENERATION_INVALID")
            reference = ArtifactReference.from_intent(intent)
            object_version = await self.artifacts.put(reference, output.path)
            attached = await self._call(execution, "app.attach_parse_artifact", reference.intent_id,
                                        object_version, output.sha256, output.size_bytes)
            quality = Jsonb(artifact.quality_report.model_dump(mode="json"))
            if artifact.quality_report.status != "passed":
                await self._call(execution, "knowledge.fail_parse", parse_id, quality)
                raise ParserFailure("EXTRACTION_QUALITY_FAILED")
            for number, batch in enumerate(_node_batches(artifact.document)):
                # Content digest prevents accidental batch-identity reuse after a
                # changed projection even within an otherwise identical recipe.
                digest = hashlib.sha256(json.dumps(batch, sort_keys=True, ensure_ascii=False,
                                                  allow_nan=False).encode("utf-8")).hexdigest()
                batch_id = uuid5(parse_id, f"nodes:{number}:{digest}")
                await self._call(execution, "knowledge.write_parse_nodes", parse_id, batch_id, Jsonb(batch))
            await self._call(execution, "knowledge.finalize_parse", parse_id, len(artifact.document.nodes),
                             len(artifact.document.pages), quality)
            return ParsedGeneration(parse_id, attached["artifact_object_id"], output.sha256,
                                    artifact.document, artifact.quality_report)
