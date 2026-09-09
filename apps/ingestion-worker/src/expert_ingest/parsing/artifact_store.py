"""Create-only immutable parse artifacts; all object identities come from PG."""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any, Callable, TypeVar
from uuid import UUID

from .runner import ParserFailure


T = TypeVar("T")


async def finish_storage_call(function: Callable[..., T], *args: Any) -> T:
    """Join the bounded SDK operation before a caller releases its local file."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if task.done() and not task.cancelled():
            task.exception()
        raise


def _close_body_preserving_primary(body: Any, *, primary_failure: bool) -> None:
    try:
        body.close()
    except Exception:
        if not primary_failure:
            raise


@dataclass(frozen=True)
class ArtifactReference:
    intent_id: UUID
    object_id: UUID
    version_id: UUID
    parse_id: UUID
    role: str
    sha256: str
    size_bytes: int
    bucket: str
    key: str
    media_type: str
    object_version_id: str | None = None

    def __post_init__(self) -> None:
        suffix = ".json" if self.role == "canonical" else ".png"
        expected = f"parses/{self.version_id}/{self.parse_id}/{self.intent_id}/{self.sha256}{suffix}"
        if (self.role not in {"canonical", "source_crop"} or self.bucket != "artifacts"
                or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
                or not 0 < self.size_bytes <= 268_435_456 or self.key != expected
                or self.media_type != ("application/json" if self.role == "canonical" else "image/png")):
            raise ParserFailure("GENERATION_INVALID")

    @classmethod
    def from_intent(cls, value: dict[str, Any]) -> ArtifactReference:
        return cls(
            intent_id=value["id"], object_id=value["artifact_object_id"],
            version_id=value["document_version_id"], parse_id=value["parse_generation_id"],
            role=value["artifact_role"], sha256=value["sha256"], size_bytes=value["size_bytes"],
            bucket=value["bucket"], key=value["object_key"], media_type=value["media_type"],
            object_version_id=value["object_version_id"],
        )


class ParseArtifactStore:
    """The supplied SDK client must have finite connect/read timeouts/retries."""

    def __init__(self, storage: Any):
        self.storage = storage

    def _head(self, reference: ArtifactReference) -> str | None:
        request: dict[str, Any] = {"Bucket": reference.bucket, "Key": reference.key}
        if reference.object_version_id:
            request["VersionId"] = reference.object_version_id
        head = self.storage.head_object(**request)
        version = head.get("VersionId")
        if (head.get("ContentLength") != reference.size_bytes
                or head.get("ContentType") != reference.media_type
                or head.get("Metadata", {}).get("sha256") != reference.sha256
                or reference.object_version_id and version != reference.object_version_id
                or version is not None and not isinstance(version, str)):
            raise ParserFailure("SOURCE_UNAVAILABLE")
        return version

    def _put(self, reference: ArtifactReference, path: Path) -> str | None:
        if path.is_symlink() or path.is_junction() or not path.is_file():
            raise ParserFailure("SOURCE_UNAVAILABLE")
        with path.open("rb") as body:
            digest, size = hashlib.sha256(), 0
            while chunk := body.read(1024 * 1024):
                size += len(chunk)
                if size > reference.size_bytes:
                    raise ParserFailure("SOURCE_UNAVAILABLE")
                digest.update(chunk)
            if size != reference.size_bytes or digest.hexdigest() != reference.sha256:
                raise ParserFailure("SOURCE_UNAVAILABLE")
            body.seek(0)
            try:
                self.storage.put_object(
                    Bucket=reference.bucket, Key=reference.key, Body=body,
                    ContentLength=size, ContentType=reference.media_type,
                    Metadata={"sha256": reference.sha256}, IfNoneMatch="*",
                    ChecksumSHA256=base64.b64encode(digest.digest()).decode("ascii"),
                )
            except Exception:
                # A 412 or lost acknowledgement can still mean the exact immutable
                # bytes are present. HEAD is authoritative for this exclusive key.
                return self._head(reference)
        return self._head(reference)

    async def put(self, reference: ArtifactReference, path: Path) -> str | None:
        try:
            return await finish_storage_call(self._put, reference, path)
        except ParserFailure:
            raise
        except Exception:
            raise ParserFailure("DEPENDENCY_UNAVAILABLE") from None

    def _read(self, reference: ArtifactReference) -> bytes:
        self._head(reference)
        request: dict[str, Any] = {"Bucket": reference.bucket, "Key": reference.key}
        if reference.object_version_id:
            request["VersionId"] = reference.object_version_id
        response = self.storage.get_object(**request)
        body = response["Body"]
        primary_failure = False
        try:
            result = bytearray()
            while chunk := body.read(min(1024 * 1024, reference.size_bytes - len(result) + 1)):
                result.extend(chunk)
                if len(result) > reference.size_bytes:
                    raise ParserFailure("SOURCE_UNAVAILABLE")
            if len(result) != reference.size_bytes or hashlib.sha256(result).hexdigest() != reference.sha256:
                raise ParserFailure("SOURCE_UNAVAILABLE")
            return bytes(result)
        except BaseException:
            primary_failure = True
            raise
        finally:
            _close_body_preserving_primary(body, primary_failure=primary_failure)

    async def read(self, reference: ArtifactReference) -> bytes:
        try:
            return await finish_storage_call(self._read, reference)
        except ParserFailure:
            raise
        except Exception:
            raise ParserFailure("DEPENDENCY_UNAVAILABLE") from None
