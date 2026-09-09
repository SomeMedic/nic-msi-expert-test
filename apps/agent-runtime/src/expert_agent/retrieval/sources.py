"""Runtime obtains bounded raw spans through backend, never S3 credentials."""
import asyncio
import json
from uuid import UUID

from expert_clients.http import ServiceClient
from expert_contracts.retrieval_sources import RegistryOwnerRequest, SourceRegistryRequest, SourceRegistryResponse

from .ml import bounded
from .types import Node, RetrievalError, Snapshot, SourceBlock, SourceIdentity, SourceRegistry


def requested_blocks(nodes: tuple[Node, ...], parse_generation_id: UUID) -> tuple[str, ...]:
    """Request only source IDs referenced by the bounded expansion node selection."""
    found: set[str] = set()

    def visit(value) -> None:
        if isinstance(value, dict):
            if {"pdf_page", "block_id", "start_offset", "end_offset"} <= value.keys():
                found.add(value["block_id"])
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for node in nodes:
        if node.parse_generation_id == parse_generation_id:
            found.update(span.block_id for span in node.source_spans)
            visit(json.loads(node.metadata_json))
            if node.table_json:
                visit(json.loads(node.table_json))
    if not found or len(found) > 2000:
        raise RetrievalError("SOURCE_REGISTRY_LIMIT")
    return tuple(sorted(found))


def requested_owners(nodes: tuple[Node, ...], parse_generation_id: UUID) -> tuple[RegistryOwnerRequest, ...]:
    values = []
    for node in nodes:
        if node.parse_generation_id != parse_generation_id:
            continue
        if node.canonical_text:
            values.append(RegistryOwnerRequest(node_id=node.id, owner_kind="node_body", text_owner_id=str(node.id)))
        if node.table_json:
            values.extend(RegistryOwnerRequest(node_id=node.id, owner_kind="table_cell", text_owner_id=cell["id"])
                          for cell in json.loads(node.table_json)["cells"] if cell["text"])
    if not values or len(values) > 1024:
        raise RetrievalError("SOURCE_REGISTRY_LIMIT")
    return tuple(values)


class SourceRegistryLoader:
    def __init__(self, client: ServiceClient):
        self.client = client

    async def load(self, snapshot: Snapshot, identity: SourceIdentity, block_ids: tuple[str, ...],
                   request_id: UUID, *, cancel: asyncio.Event, deadline: float,
                   owners: tuple[RegistryOwnerRequest, ...] = ()) -> SourceRegistry:
        request = SourceRegistryRequest(run_id=snapshot.run_id, snapshot_id=snapshot.snapshot_id,
            parse_generation_id=identity.parse_generation_id, document_version_id=identity.document_version_id,
            source_sha256=identity.source_sha256, artifact_object_id=identity.artifact_object_id, block_ids=block_ids, owners=owners)
        response = await bounded(self.client.post("/internal/v1/retrieval/source-registry", request,
            SourceRegistryResponse, request_id=request_id, timeout_seconds=30), cancel, deadline)
        try:
            response.validate_binding(request)
        except ValueError:
            raise RetrievalError("SOURCE_REGISTRY_INVALID") from None
        if identity.artifact_sha256 is not None and identity.artifact_sha256 != response.artifact_sha256:
            raise RetrievalError("SOURCE_ARTIFACT_CHANGED")
        return SourceRegistry(response.parse_generation_id, response.document_version_id, response.source_sha256,
            response.artifact_object_id, response.artifact_sha256,
            tuple(SourceBlock(block.block_id, block.pdf_page, block.text) for block in response.blocks),
            tuple((page.width, page.height) for page in response.pages),
            tuple((owner.node_id, owner.owner_kind, owner.text_owner_id, owner.sha256) for owner in response.owner_hashes))
