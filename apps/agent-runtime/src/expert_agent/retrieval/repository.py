"""Runtime-role SELECT queries under one short repeatable-read snapshot per stage."""
from __future__ import annotations

from contextlib import asynccontextmanager
import json
from typing import Any
from uuid import UUID

from psycopg import Error as PostgresError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from expert_contracts.inference import QueryEmbeddingResponse
from expert_contracts.internal import SnapshotItem
from expert_contracts.ml_profile import EmbeddingRecipe
from expert_contracts.sources import SourceSpan
from expert_observability.tracing import safe_span

from .types import (
    CandidateBatch, ChannelHit, Chunk, DescriptorHit, Node, RetrievalConfig,
    RetrievalError, Snapshot, SourceIdentity, canonical_json,
)

_SNAPSHOT = """
SELECT s.id,s.catalog_epoch,s.selection_hash,r.question FROM agent.runs r
JOIN agent.kb_snapshots s ON s.id=r.snapshot_id
WHERE r.id=%s AND r.principal_id=%s
"""
_ITEMS = """
SELECT si.*,d.security_revoked_at,g.embedding_recipe
FROM agent.kb_snapshot_items si
JOIN app.logical_documents d ON d.id=si.logical_document_id
JOIN knowledge.index_generations g ON g.id=si.index_generation_id AND g.parse_generation_id=si.parse_generation_id
WHERE si.snapshot_id=%s ORDER BY si.logical_document_id
"""
_FIELDS = """c.id,c.index_generation_id,c.parse_generation_id,si.document_version_id,c.node_id,
c.source_text,c.header_text,c.embedding_text,c.source_spans,c.content_hash,c.projection_metadata"""
_ELIGIBLE = """
FROM knowledge.chunks c
JOIN agent.kb_snapshot_items si ON si.index_generation_id=c.index_generation_id AND si.parse_generation_id=c.parse_generation_id
JOIN app.logical_documents d ON d.id=si.logical_document_id
WHERE si.snapshot_id=%(snapshot)s AND d.security_revoked_at IS NULL
"""
_CURRENT_LEXICAL_VECTOR = """(c.search_vector ||
    setweight(to_tsvector('pg_catalog.russian', c.header_text || E'\n' || c.embedding_text),'A') ||
    setweight(to_tsvector('pg_catalog.simple', c.header_text || E'\n' || c.embedding_text),'B'))"""
_HISTORICAL_LEXICAL_VECTOR = """(search_vector ||
    setweight(to_tsvector('pg_catalog.russian', header_text || E'\n' || embedding_text),'A') ||
    setweight(to_tsvector('pg_catalog.simple', header_text || E'\n' || embedding_text),'B'))"""


def _chunk(row: dict[str, Any]) -> Chunk:
    return Chunk(id=row["id"], index_generation_id=row["index_generation_id"],
                 parse_generation_id=row["parse_generation_id"], document_version_id=row["document_version_id"],
                 node_id=row["node_id"], source_text=row["source_text"], header_text=row["header_text"],
                 embedding_text=row["embedding_text"],
                 source_spans=tuple(SourceSpan.model_validate(span) for span in row["source_spans"]),
                 content_hash=row["content_hash"], projection_json=canonical_json(row["projection_metadata"]))


def _item(row: dict[str, Any]) -> SnapshotItem:
    return SnapshotItem.model_validate({name: row[name] for name in SnapshotItem.model_fields})


class RetrievalRepository:
    def __init__(self, pool: AsyncConnectionPool, config: RetrievalConfig | None = None):
        self.pool, self.config = pool, config or RetrievalConfig()

    @asynccontextmanager
    async def _transaction(self):
        try:
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        await cursor.execute("SELECT set_config('statement_timeout',%s,true)",
                                             (str(self.config.statement_timeout_ms),))
                        yield cursor
        except PostgresError:
            # Never propagate query text, source values, internal hosts or driver messages.
            raise RetrievalError("RETRIEVAL_DATABASE_UNAVAILABLE") from None

    async def snapshot(self, run_id: UUID, principal_id: str) -> Snapshot:
        async with self._transaction() as cursor:
            await cursor.execute(_SNAPSHOT, (run_id, principal_id))
            row = await cursor.fetchone()
            if row is None:
                raise RetrievalError("SNAPSHOT_NOT_AVAILABLE")
            await cursor.execute(_ITEMS, (row["id"],))
            items = await cursor.fetchall()
            if any(item["security_revoked_at"] is not None for item in items):
                raise RetrievalError("SOURCE_REVOKED")
            return Snapshot(run_id, principal_id, row["id"], row["catalog_epoch"], row["selection_hash"],
                            tuple(_item(item) for item in items), row["question"])

    async def _guard(self, cursor, snapshot: Snapshot, recipe: EmbeddingRecipe | None = None) -> int:
        await cursor.execute(_SNAPSHOT, (snapshot.run_id, snapshot.principal_id))
        row = await cursor.fetchone()
        if row is None or (row["id"], row["catalog_epoch"], row["selection_hash"], row["question"]) != (
                snapshot.snapshot_id, snapshot.catalog_epoch, snapshot.selection_hash, snapshot.original_question):
            raise RetrievalError("SNAPSHOT_BINDING_INVALID")
        await cursor.execute(_ITEMS, (snapshot.snapshot_id,))
        rows = await cursor.fetchall()
        if tuple(_item(row) for row in rows) != snapshot.items:
            raise RetrievalError("SNAPSHOT_BINDING_INVALID")
        if any(row["security_revoked_at"] is not None for row in rows):
            raise RetrievalError("SOURCE_REVOKED")
        if recipe is not None:
            for row in rows:
                # A reconfigured runtime cannot query vectors from a different
                # embedding recipe just because both vectors have 1536 dimensions.
                if row["embedding_recipe"] != recipe.model_dump(mode="json"):
                    raise RetrievalError("INDEX_RECIPE_MISMATCH")
        await cursor.execute("SELECT epoch FROM app.knowledge_catalog WHERE workspace_id='00000000-0000-0000-0000-000000000001'")
        catalog = await cursor.fetchone()
        if catalog is None:
            raise RetrievalError("SNAPSHOT_NOT_AVAILABLE")
        return catalog["epoch"]

    async def check_access(self, snapshot: Snapshot) -> None:
        async with self._transaction() as cursor:
            await self._guard(cursor, snapshot)

    async def channels(self, snapshot: Snapshot, embedding: QueryEmbeddingResponse,
                       question: str, recipe: EmbeddingRecipe) -> CandidateBatch:
        if not question.strip() or len(question) > 4000:
            raise RetrievalError("QUERY_INVALID")
        if (embedding.model, embedding.revision) != (recipe.model, recipe.revision):
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        parameters = {"snapshot": snapshot.snapshot_id, "vector": canonical_json(embedding.vector),
                      "dense_limit": self.config.dense_top_k, "lexical_limit": self.config.lexical_top_k}
        async with self._transaction() as cursor:
            epoch = await self._guard(cursor, snapshot, recipe)
            current = epoch == snapshot.catalog_epoch
            # Fixed SQL templates only; query text and vector always parameters.
            if current:
                dense_sql = f"SELECT {_FIELDS},c.embedding <=> %(vector)s::public.vector AS score " + _ELIGIBLE + " AND c.searchable ORDER BY score,c.id LIMIT %(dense_limit)s"
            else:
                dense_sql = (f"WITH eligible AS MATERIALIZED (SELECT {_FIELDS},c.embedding " + _ELIGIBLE +
                    ") SELECT *,embedding <=> %(vector)s::public.vector AS score FROM eligible ORDER BY score,id LIMIT %(dense_limit)s")
            with safe_span("retrieval.dense", snapshot_id=snapshot.snapshot_id) as span:
                await cursor.execute(dense_sql, parameters)
                dense = tuple(ChannelHit(_chunk(row), row["score"]) for row in await cursor.fetchall())
                span.set_attributes(dense_count=len(dense))
            # Build a bounded OR over PostgreSQL-produced lexemes, quoted by PG.
            # No raw tsquery operators or strict AND of all natural-language words.
            await cursor.execute("""
                SELECT DISTINCT term FROM unnest(tsvector_to_array(to_tsvector('pg_catalog.russian',%s)) ||
                  tsvector_to_array(to_tsvector('pg_catalog.simple',%s))) term ORDER BY term LIMIT %s
                """, (question, question, self.config.max_lexical_terms + 1))
            terms = [row["term"] for row in await cursor.fetchall()]
            if len(terms) > self.config.max_lexical_terms:
                raise RetrievalError("QUERY_LEXICAL_LIMIT")
            lexical: tuple[ChannelHit, ...] = ()
            if terms:
                await cursor.execute("SELECT string_agg(quote_literal(term),' | ' ORDER BY term)::tsquery::text AS query FROM unnest(%s::text[]) term", (terms,))
                built = await cursor.fetchone()
                parameters["query"] = built["query"]
                if current:
                    lexical_sql = (f"SELECT {_FIELDS},ts_rank_cd({_CURRENT_LEXICAL_VECTOR},%(query)s::tsquery) AS score " +
                        _ELIGIBLE + f" AND c.searchable AND {_CURRENT_LEXICAL_VECTOR} @@ %(query)s::tsquery ORDER BY score DESC,c.id LIMIT %(lexical_limit)s")
                else:
                    lexical_sql = (f"WITH eligible AS MATERIALIZED (SELECT {_FIELDS},c.search_vector " + _ELIGIBLE +
                        f") SELECT *,ts_rank_cd({_HISTORICAL_LEXICAL_VECTOR},%(query)s::tsquery) AS score FROM eligible "
                        f"WHERE {_HISTORICAL_LEXICAL_VECTOR} @@ %(query)s::tsquery ORDER BY score DESC,id LIMIT %(lexical_limit)s")
                with safe_span("retrieval.lexical", snapshot_id=snapshot.snapshot_id) as span:
                    await cursor.execute(lexical_sql, parameters)
                    lexical = tuple(ChannelHit(_chunk(row), row["score"]) for row in await cursor.fetchall())
                    span.set_attributes(lexical_count=len(lexical))
            return CandidateBatch(dense, lexical, "current" if current else "historical", epoch)

    async def descriptors(self, snapshot: Snapshot, embedding: QueryEmbeddingResponse,
                          recipe: EmbeddingRecipe) -> tuple[DescriptorHit, ...]:
        if (embedding.model, embedding.revision) != (recipe.model, recipe.revision):
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        async with self._transaction() as cursor:
            await self._guard(cursor, snapshot, recipe)
            # Catalogs are bounded and small: exact search also serves pinned history.
            await cursor.execute("""WITH eligible AS MATERIALIZED (
                SELECT r.* FROM knowledge.node_routing_embeddings r
                JOIN agent.kb_snapshot_items si ON si.index_generation_id=r.index_generation_id AND si.parse_generation_id=r.parse_generation_id
                JOIN app.logical_documents d ON d.id=si.logical_document_id
                WHERE si.snapshot_id=%s AND d.security_revoked_at IS NULL)
                SELECT *,embedding <=> %s::public.vector AS distance FROM eligible
                ORDER BY distance,node_id,index_generation_id LIMIT %s""",
                (snapshot.snapshot_id, canonical_json(embedding.vector), self.config.descriptor_top_k))
            return tuple(DescriptorHit(f"D{rank:03}", row["node_id"], row["index_generation_id"],
                                       row["parse_generation_id"], row["descriptor_text"], row["distance"],
                                       tuple(SourceSpan.model_validate(s) for s in row["source_spans"]), row["content_hash"])
                         for rank, row in enumerate(await cursor.fetchall(), 1))

    async def expansion_inputs(self, snapshot: Snapshot, chunks: tuple[Chunk, ...]) -> tuple[tuple[Node, ...], tuple[SourceIdentity, ...]]:
        if not chunks:
            return (), ()
        if len(chunks) > self.config.max_context_candidates:
            raise RetrievalError("EXPANSION_LIMIT")
        async with self._transaction() as cursor:
            await self._guard(cursor, snapshot)
            await cursor.execute(f"SELECT {_FIELDS} " + _ELIGIBLE + " AND c.id=ANY(%(ids)s::uuid[])",
                                 {"snapshot": snapshot.snapshot_id, "ids": [c.id for c in chunks]})
            persisted = {_chunk(row).key: _chunk(row) for row in await cursor.fetchall()}
            if len(persisted) != len(chunks) or any(persisted.get(chunk.key) != chunk for chunk in chunks):
                raise RetrievalError("SOURCE_BINDING_INVALID")
            await cursor.execute("""WITH RECURSIVE ancestry AS (
                SELECT id,parent_id,parse_generation_id FROM knowledge.document_nodes WHERE id=ANY(%s::uuid[])
                UNION
                SELECT n.id,n.parent_id,n.parse_generation_id FROM knowledge.document_nodes n JOIN ancestry a
                ON n.id=a.parent_id AND n.parse_generation_id=a.parse_generation_id
                ), wanted AS (SELECT id FROM ancestry UNION
                SELECT n.id FROM knowledge.document_nodes n JOIN ancestry a ON n.parent_id=a.id
                AND n.parse_generation_id=a.parse_generation_id WHERE n.node_type IN ('table','footnote','editorial_note'))
                SELECT n.* FROM knowledge.document_nodes n JOIN wanted w ON w.id=n.id
                ORDER BY n.parse_generation_id,n.level,n.ordinal,n.id LIMIT %s""",
                ([chunk.node_id for chunk in chunks], self.config.max_expansion_nodes + 1))
            rows = await cursor.fetchall()
            if len(rows) > self.config.max_expansion_nodes:
                raise RetrievalError("EXPANSION_LIMIT")
            # Captions and inherited conditions may be owned by sibling nodes.
            # Resolve their exact source anchors inside the pinned parse, rather
            # than guessing that every header is an ancestor's owning body.
            def anchors(value: Any, parse_id: UUID) -> list[dict[str, Any]]:
                found: list[dict[str, Any]] = []
                if isinstance(value, dict):
                    for ref in value.get("context_refs", []):
                        found.extend(dict(parse_generation_id=str(parse_id), **span) for span in ref["source_spans"])
                    for key, item in value.items():
                        if key != "context_refs":
                            found.extend(anchors(item, parse_id))
                elif isinstance(value, list):
                    for item in value:
                        found.extend(anchors(item, parse_id))
                return found

            requested = []
            for chunk in chunks:
                requested.extend(anchors(json.loads(chunk.projection_json), chunk.parse_generation_id))
            seen_nodes = {row["id"] for row in rows}
            latest = rows
            for _ in range(self.config.max_ancestor_depth):
                for row in latest:
                    requested.extend(anchors(row["extra_metadata"], row["parse_generation_id"]))
                    if row["table_data"]:
                        requested.extend(anchors(row["table_data"], row["parse_generation_id"]))
                if not requested:
                    break
                if len(requested) > 4000:
                    raise RetrievalError("EXPANSION_LIMIT")
                await cursor.execute("""SELECT DISTINCT n.* FROM knowledge.document_nodes n
                    JOIN jsonb_to_recordset(%s::jsonb) AS ref(parse_generation_id uuid,pdf_page integer,
                         block_id text,start_offset integer,end_offset integer) ON n.parse_generation_id=ref.parse_generation_id
                    WHERE NOT(n.id=ANY(%s::uuid[])) AND EXISTS (
                        SELECT 1 FROM jsonb_array_elements(n.source_spans) span
                        WHERE span->>'block_id'=ref.block_id AND (span->>'pdf_page')::integer=ref.pdf_page
                        AND (span->>'start_offset')::integer<ref.end_offset AND (span->>'end_offset')::integer>ref.start_offset)
                    ORDER BY n.id LIMIT %s""", (Jsonb(requested), list(seen_nodes), self.config.max_expansion_nodes + 1))
                latest = await cursor.fetchall()
                requested = []
                if not latest:
                    break
                rows.extend(latest)
                seen_nodes.update(row["id"] for row in latest)
                if len(rows) > self.config.max_expansion_nodes:
                    raise RetrievalError("EXPANSION_LIMIT")
            else:
                raise RetrievalError("EXPANSION_LIMIT")
            nodes = tuple(Node(row["id"], row["parse_generation_id"], row["parent_id"], row["node_type"],
                               row["canonical_text"], tuple(row["structural_path"]),
                               tuple(SourceSpan.model_validate(s) for s in row["source_spans"]),
                               canonical_json(row["extra_metadata"]),
                               canonical_json(row["table_data"]) if row["table_data"] is not None else None)
                          for row in rows)
            await cursor.execute("""SELECT DISTINCT p.id,p.document_version_id,p.artifact_object_id,p.source_sha256,v.source_title
                FROM knowledge.parse_generations p JOIN app.document_versions v ON v.id=p.document_version_id
                JOIN agent.kb_snapshot_items si ON si.parse_generation_id=p.id AND si.document_version_id=p.document_version_id
                WHERE si.snapshot_id=%s AND p.id=ANY(%s::uuid[])""",
                (snapshot.snapshot_id, list({c.parse_generation_id for c in chunks})))
            identities = tuple(SourceIdentity(row["id"], row["document_version_id"], row["artifact_object_id"],
                                              row["source_sha256"], row["source_title"]) for row in await cursor.fetchall())
            if {n.parse_generation_id for n in nodes} - {i.parse_generation_id for i in identities}:
                raise RetrievalError("SOURCE_BINDING_INVALID")
            return nodes, identities
