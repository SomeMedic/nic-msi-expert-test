"""Private LLM values. Durable attempts and publication belong to the run owner."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal
from uuid import UUID

from pydantic import ConfigDict, Field

from expert_contracts.common import NonBlank, SHA256, ShortID, StrictDTO

if TYPE_CHECKING:
    from expert_contracts.model import DraftAnswer

MODEL: Final = "Qwen/Qwen3-14B-AWQ"
REVISION: Final = "31c69efc29464b6bb0aee1398b5a7b50a99340c3"
IMAGE: Final = "sha256:f2309d913a07da49ea20b2a694703f4cfcb5ad8e7437ec0f26145479ac01e002"
Role = Literal["router", "drafter", "critic", "repair"]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def ordered_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def sha256(value: bytes | str) -> str:
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


class LlmError(RuntimeError):
    """Safe code only: never transport provider text, prompt, draft or credentials."""

    def __init__(self, code: str, *, call_id: UUID | None = None, schema_attempt: int = 0,
                 failure_kind: str = "contract"):
        super().__init__(code)
        self.code = code
        self.call_id = call_id
        self.schema_attempt = schema_attempt
        self.failure_kind = failure_kind
        # Private recovery data is deliberately absent from exception args/repr.
        # Only a schema-valid invented-citation draft may be attached by gateway.
        self.private_result: RoleResult[DraftAnswer] | None = None


class Descriptor(StrictDTO):
    model_config = ConfigDict(frozen=True)
    descriptor_id: ShortID
    text: NonBlank = Field(max_length=10000)


class ServingProfile(StrictDTO):
    """Trusted deployment attestation, not an assertion made by /v1/models.

    The deployment owner verifies image, mounted artifacts and actual arguments.
    No private paths, credentials or raw server configuration belong here.
    """
    model_config = ConfigDict(frozen=True)
    schema_version: Literal["p07.vllm.v1"] = "p07.vllm.v1"
    model: Literal["Qwen/Qwen3-14B-AWQ"] = MODEL
    revision: Literal["31c69efc29464b6bb0aee1398b5a7b50a99340c3"] = REVISION
    tokenizer_revision: Literal["31c69efc29464b6bb0aee1398b5a7b50a99340c3"] = REVISION
    image_digest: Literal["sha256:f2309d913a07da49ea20b2a694703f4cfcb5ad8e7437ec0f26145479ac01e002"] = IMAGE
    vllm_version: Literal["0.12.0"] = "0.12.0"
    structured_backend: Literal["xgrammar"] = "xgrammar"
    disable_fallback: Literal[True] = True
    enable_thinking: Literal[False] = False
    max_model_len: Literal[8192] = 8192
    max_num_seqs: Literal[1] = 1
    model_fingerprint: SHA256
    tokenizer_fingerprint: SHA256
    chat_template_sha256: SHA256
    observed_config_sha256: SHA256

    @property
    def fingerprint(self) -> str:
        return sha256(canonical_json(self.model_dump(mode="json")))

    def validate_lock(self, lock_path: Path) -> None:
        try:
            row = json.loads(lock_path.read_bytes())["models"]["llm"]
            files = [{k: f[k] for k in ("path", "size_bytes", "sha256")} for f in row["files"]]
            files.sort(key=lambda f: f["path"])
            tokenizer = [f for f in files if f["path"] in {
                "tokenizer.json", "tokenizer_config.json", "merges.txt", "vocab.json", "added_tokens.json",
                "special_tokens_map.json", "chat_template.jinja"}]
            valid = (row["model_id"] == self.model and row["revision"] == self.revision
                     and row["tokenizer_revision"] == self.tokenizer_revision
                     and sha256(canonical_json(files)) == self.model_fingerprint
                     and sha256(canonical_json(tokenizer)) == self.tokenizer_fingerprint)
        except (ValueError, KeyError, TypeError, OSError):
            valid = False
        if not valid:
            raise LlmError("MODEL_UNAVAILABLE")


@dataclass(frozen=True)
class CallContext:
    call_id: UUID
    deadline: float  # asyncio event-loop monotonic time, not a wall-clock timestamp.
    schema_attempt: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.call_id, UUID) or not math.isfinite(self.deadline):
            raise ValueError("Invalid LLM call identity/deadline")
        if type(self.schema_attempt) is not int or self.schema_attempt not in (0, 1):
            raise ValueError("At most one explicit schema retry is permitted")


@dataclass(frozen=True)
class CallProvenance:
    call_id: UUID
    role: Role
    schema_attempt: int
    model: str
    revision: str
    profile_sha256: str
    prompt_version: str
    prompt_sha256: str  # Effective system message, including wrappers and schema.
    schema_sha256: str
    input_sha256: str
    messages_sha256: str
    request_sha256: str
    token_ids_sha256: str
    input_tokens: int
    output_tokens: int
    elapsed_ms: int
    evidence_manifest_sha256: str | None


@dataclass(frozen=True)
class RoleResult[T]:
    value: T = field(repr=False)
    provenance: CallProvenance


@dataclass(frozen=True)
class PreparedCall:
    role: Role
    # Serialized immutable messages prevent mutation between /tokenize and generation.
    messages_json: str = field(repr=False)
    schema_json: str = field(repr=False)
    input_sha256: str
    prompt_version: str
    prompt_sha256: str  # Effective system message; raw file hash lives in manifest.
    max_output_tokens: int
    timeout_seconds: int
    evidence_manifest_sha256: str | None
