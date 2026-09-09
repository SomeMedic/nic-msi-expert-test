"""Transport primitives; source text is validated without rewriting it."""
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Generic, TypeVar
from urllib.parse import urlsplit

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints


def nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("text must not be blank")
    return value


def api_path(value: str) -> str:
    parsed = urlsplit(value)
    if (not value.startswith("/api/v1/") or parsed.scheme or parsed.netloc
            or "\\" in value or any(ord(c) < 32 for c in value)):
        raise ValueError("expected a same-origin /api/v1/ path")
    return value


def unique(values, label="IDs") -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label}")


class StrictDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, validate_default=True)


NonBlank = Annotated[str, AfterValidator(nonblank)]
ShortID = Annotated[NonBlank, StringConstraints(min_length=1, max_length=64)]
SHA256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
UTCDateTime = Annotated[AwareDatetime, AfterValidator(lambda value: value.astimezone(timezone.utc))]
ApiPath = Annotated[str, AfterValidator(api_path)]
Count = Annotated[int, Field(ge=0, strict=True)]
PositiveInt = Annotated[int, Field(ge=1, strict=True)]


class LegalStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class PublicationStatus(StrEnum):
    STAGING = "staging"
    PUBLISHED = "published"
    SUPERSEDED = "superseded"
    DEACTIVATED = "deactivated"


class GenerationStatus(StrEnum):
    STAGING = "staging"
    READY = "ready"
    FAILED = "failed"


class RunStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    REFUSED = "refused"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunStage(StrEnum):
    SNAPSHOTTING = "snapshotting"
    ROUTING = "routing"
    RETRIEVING = "retrieving"
    RERANKING = "reranking"
    BUILDING_CONTEXT = "building_context"
    DRAFTING = "drafting"
    CHECKING_CITATIONS = "checking_citations"
    VALIDATING = "validating"
    REPAIRING = "repairing"
    REVALIDATING = "revalidating"
    RENDERING = "rendering"
    FINALIZING = "finalizing"


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class IngestionStage(StrEnum):
    QUEUED = "queued"
    VALIDATING = "validating"
    PARSING = "parsing"
    ASSESSING_EXTRACTION = "assessing_extraction"
    FALLBACK_PARSING = "fallback_parsing"
    NORMALIZING = "normalizing"
    BUILDING_STRUCTURE = "building_structure"
    CHUNKING = "chunking"
    EMBEDDING = "embedding"
    INDEXING = "indexing"
    READY_TO_PUBLISH = "ready_to_publish"
    PUBLISHING = "publishing"


class RunLinks(StrictDTO):
    self: ApiPath
    events: ApiPath


T = TypeVar("T")


class CursorPage(StrictDTO, Generic[T]):
    items: list[T] = Field(max_length=100)
    next_cursor: str | None = None


class ListQuery(StrictDTO):
    cursor: str | None = Field(default=None, max_length=1000)
    limit: int = Field(default=50, ge=1, le=100, strict=True)


def validate_times(created: datetime, started: datetime | None, finished: datetime | None) -> None:
    if started is not None and started < created:
        raise ValueError("started_at precedes created_at")
    if finished is not None and finished < (started or created):
        raise ValueError("finished_at precedes start/creation")
