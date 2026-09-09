"""Private authenticated backend projection. Excluded from PUBLIC_SCHEMA_MODELS."""
import hashlib
import json
import math
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from .common import NonBlank, SHA256, StrictDTO, unique

BlockID = Annotated[NonBlank, Field(max_length=200)]


class SourceRegistryIdentity(StrictDTO):
    model_config = ConfigDict(frozen=True)
    run_id: UUID
    snapshot_id: UUID
    parse_generation_id: UUID
    document_version_id: UUID
    source_sha256: SHA256
    artifact_object_id: UUID


class RegistryOwnerRequest(StrictDTO):
    model_config = ConfigDict(frozen=True)
    node_id: UUID
    owner_kind: Literal["node_body", "table_cell"]
    text_owner_id: BlockID

    @model_validator(mode="after")
    def body_identity(self):
        if self.owner_kind == "node_body" and self.text_owner_id != str(self.node_id):
            raise ValueError("body owner must identify its canonical node")
        return self


class RegistryOwnerHash(RegistryOwnerRequest):
    sha256: SHA256


def canonical_owner_hash(owner: RegistryOwnerRequest, text: str, char_map: list[dict[str, Any]]) -> str:
    """One wire recipe for PG JSONB and SHA-verified artifact source owners.

    JSONB may serialize 400.0 as 400. Normalize numeric representations without
    rounding nonintegral source geometry or transforming canonical/raw text.
    """
    def normalize(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if isinstance(value, (float, Decimal)):
            if not math.isfinite(value):
                raise ValueError("owner geometry must be finite")
            return int(value) if value == int(value) else float(value)
        return value

    payload = {**owner.model_dump(mode="json", include=set(RegistryOwnerRequest.model_fields)), "text": text, "char_map": char_map}
    encoded = json.dumps(normalize(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class SourceRegistryRequest(SourceRegistryIdentity):
    block_ids: tuple[BlockID, ...] = Field(min_length=1, max_length=2000)
    owners: tuple[RegistryOwnerRequest, ...] = Field(default=(), max_length=1024)

    @model_validator(mode="after")
    def unique_blocks(self):
        unique(self.block_ids, "requested raw block IDs")
        unique([(owner.node_id, owner.owner_kind, owner.text_owner_id) for owner in self.owners], "requested canonical owners")
        return self


class RegistryBlock(StrictDTO):
    model_config = ConfigDict(frozen=True)
    block_id: BlockID
    pdf_page: int = Field(ge=1, le=500, strict=True)
    text: str = Field(max_length=2_000_000, repr=False)


class RegistryPage(StrictDTO):
    model_config = ConfigDict(frozen=True)
    pdf_page: int = Field(ge=1, le=500, strict=True)
    width: float = Field(gt=0, le=100000)
    height: float = Field(gt=0, le=100000)


class SourceRegistryResponse(SourceRegistryIdentity):
    artifact_sha256: SHA256
    artifact_size_bytes: int = Field(ge=1, le=268_435_456, strict=True)
    page_count: int = Field(ge=1, le=500, strict=True)
    pages: tuple[RegistryPage, ...] = Field(min_length=1, max_length=500)
    blocks: tuple[RegistryBlock, ...] = Field(min_length=1, max_length=2000)
    owner_hashes: tuple[RegistryOwnerHash, ...] = Field(default=(), max_length=1024)

    @model_validator(mode="after")
    def registry_bounds(self):
        unique([block.block_id for block in self.blocks], "returned raw block IDs")
        unique([(owner.node_id, owner.owner_kind, owner.text_owner_id) for owner in self.owner_hashes], "returned canonical owners")
        if [page.pdf_page for page in self.pages] != list(range(1, self.page_count + 1)):
            raise ValueError("physical page sizes must be complete and ordered")
        if any(block.pdf_page > self.page_count for block in self.blocks):
            raise ValueError("raw block belongs to an unavailable page")
        if sum(len(block.text) for block in self.blocks) > 2_000_000:
            raise ValueError("raw registry projection exceeds total text limit")
        return self

    def validate_binding(self, request: SourceRegistryRequest) -> None:
        if any(getattr(self, field) != getattr(request, field) for field in SourceRegistryIdentity.model_fields):
            raise ValueError("raw registry identity differs from request")
        if {block.block_id for block in self.blocks} != set(request.block_ids):
            raise ValueError("raw registry must return exactly the requested block IDs")
        if {(o.node_id, o.owner_kind, o.text_owner_id) for o in self.owner_hashes} != {(o.node_id, o.owner_kind, o.text_owner_id) for o in request.owners}:
            raise ValueError("canonical hashes must match exactly the requested owners")
