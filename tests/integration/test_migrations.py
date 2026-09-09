"""Actual isolated PostgreSQL schema, publication, snapshot and ACL checks."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.types.json import Jsonb
import pytest

from scripts.migrate import run_migrations
from scripts import migrate


@pytest.fixture
def migrated_database(isolated_database):
    run_migrations(isolated_database.migrate_dsn.get_secret_value())
    return isolated_database


def seed_ready_generation(connection, *, logical_id=None, version_id=None, legal_status="active", structural_path=()):
    """Synthetic source fixture; no user corpus or object-store requests."""
    document = logical_id or uuid4()
    version = version_id or uuid4()
    source, artifact, parse, node, index = (uuid4() for _ in range(5))
    digest = "a" * 64
    if version_id is not None:
        digest = connection.execute("SELECT source_sha256 FROM app.document_versions WHERE id=%s", (version_id,)).fetchone()[0]
    if logical_id is None:
        connection.execute("INSERT INTO app.logical_documents(id,canonical_title) VALUES(%s,'Synthetic regulation')", (document,))
    if version_id is None:
        connection.execute(
            "INSERT INTO app.stored_objects(id,bucket,object_key,media_type,size_bytes,sha256,kind,state) "
            "VALUES(%s,'test',%s,'application/pdf',10,%s,'original','attached')", (source, str(source), digest))
        connection.execute(
            "INSERT INTO app.document_versions(id,logical_document_id,source_title,approved_at,legal_status,"
            "source_object_id,source_sha256,original_filename,content_size,metadata_hash,created_by_subject) "
            "VALUES(%s,%s,'Synthetic regulation','2026-01-01',%s,%s,%s,'synthetic.pdf',10,%s,'test')",
            (version, document, legal_status, source, digest, digest))
    connection.execute(
        "INSERT INTO app.stored_objects(id,bucket,object_key,media_type,size_bytes,sha256,kind,state) "
        "VALUES(%s,'test',%s,'application/json',10,%s,'parse_artifact','attached')", (artifact, str(artifact), digest))
    connection.execute(
        "INSERT INTO knowledge.parse_generations(id,document_version_id,source_sha256,parser_fingerprint,normalizer_version,structure_version,artifact_object_id,quality_report) "
        "VALUES(%s,%s,%s,'parser-v1','normalizer-v1','structure-v1',%s,%s)",
        (parse, version, digest, artifact, Jsonb({"status": "passed"})))
    spans = Jsonb([{"pdf_page": 1, "block_id": "b1", "start_offset": 0, "end_offset": 5}])
    connection.execute(
        "INSERT INTO knowledge.document_nodes(id,parse_generation_id,node_type,level,ordinal,canonical_text,structural_path,page_start,page_end,source_spans,content_hash) "
        "VALUES(%s,%s,'document',0,0,'Текст',%s,1,1,%s,%s)", (node, parse, Jsonb(list(structural_path)), spans, digest))
    connection.execute("UPDATE knowledge.parse_generations SET status='ready',node_count=1,completed_at=clock_timestamp() WHERE id=%s", (parse,))
    connection.execute(
        "INSERT INTO knowledge.index_generations(id,document_version_id,parse_generation_id,embedding_model_id,embedding_revision,embedding_dimension,tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version) "
        "VALUES(%s,%s,%s,'frida','pinned',1536,'pinned','cls','prefix-v1','l2','chunk-v1','lex-v1')", (index, version, parse))
    vector = "[1," + ",".join("0" for _ in range(1535)) + "]"
    connection.execute(
        "INSERT INTO knowledge.chunks(index_generation_id,parse_generation_id,node_id,chunk_index,source_text,embedding_text,header_text,token_count,source_spans,content_hash,embedding) "
        "VALUES(%s,%s,%s,0,'Текст','search_document: Текст','Заголовок',5,%s,%s,%s::public.vector)",
        (index, parse, node, spans, digest, vector))
    connection.execute(
        "INSERT INTO knowledge.node_routing_embeddings(index_generation_id,parse_generation_id,node_id,descriptor_text,embedding,token_count) VALUES(%s,%s,%s,'Текст',%s::public.vector,5)",
        (index, parse, node, vector))
    connection.execute("UPDATE knowledge.index_generations SET status='ready',chunk_count=1,routing_count=1,completed_at=clock_timestamp() WHERE id=%s", (index,))
    return {"document": document, "version": version, "parse": parse, "node": node, "index": index}


def publish(connection, generation, expected=None, operation=None):
    return connection.execute("SELECT app.publish_version(%s,%s,%s,%s)",
                              (generation["version"], generation["index"], expected, operation or uuid4())).fetchone()[0]


def test_real_upgrade_repeats_without_changes(isolated_database):
    first = run_migrations(isolated_database.migrate_dsn.get_secret_value())
    second = run_migrations(isolated_database.migrate_dsn.get_secret_value())
    assert first["before_revision"] is None
    assert first["after_revision"] == "p08_023_repeated_draft_guard"
    assert first["changed"] and not second["changed"]
    assert first["requested_revision"] == "head"
    assert first["target_revision"] == first["head_revision"] == first["after_revision"]
    with isolated_database.connect() as connection:
        assert connection.execute("SELECT max(v) FROM agent.checkpoint_migrations").fetchone() == (9,)
        assert connection.execute("SELECT count(*) FROM app.knowledge_catalog").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='knowledge' AND indexdef LIKE '%WHERE searchable%'").fetchone() == (3,)


@pytest.mark.parametrize("target", ["missing_revision", "p04_009", "head-1", "heads", "base", "", "p04_009_parse_persistence:head"])
def test_unknown_or_implicit_revision_selectors_never_connect(monkeypatch, target):
    def unexpected_connection(*args, **kwargs):
        pytest.fail("Invalid revision reached database engine creation")

    monkeypatch.setattr(migrate, "create_engine", unexpected_connection)
    with pytest.raises(ValueError, match="Unknown migration target revision"):
        run_migrations("invalid conninfo must never be parsed", target_revision=target)


def test_explicit_revision_cli_stops_at_009_then_api_resumes_to_010(isolated_database, tmp_path):
    db = isolated_database
    target = "p04_009_parse_persistence"
    next_revision = "p05_010_index_persistence"
    head = "p08_023_repeated_draft_guard"
    dsn_file = db.write_dsn_file("migrate", tmp_path / "migrate_dsn")
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, str(root / "scripts/migrate.py"), "--dsn-file", str(dsn_file)]
    try:
        rejected = subprocess.run([*command, "--revision", "unknown_revision"], cwd=root,
                                  capture_output=True, text=True, timeout=30, check=False)
        assert rejected.returncode == 1 and rejected.stdout == ""
        assert json.loads(rejected.stderr) == {
            "status": "failed", "error_class": "ValueError", "sqlstate": None,
            "credentials_disclosed": False,
        }
        with db.connect() as connection:
            assert connection.execute("SELECT to_regnamespace('app')").fetchone() == (None,)
        applied = subprocess.run([*command, "--revision", target], cwd=root,
                                 capture_output=True, text=True, timeout=30, check=False)
        assert applied.returncode == 0, applied.stderr
        assert json.loads(applied.stdout) == {
            "database": db.dbname, "before_revision": None, "after_revision": target,
            "changed": True, "requested_revision": target, "target_revision": target,
            "head_revision": head,
        }
    finally:
        dsn_file.unlink()
    with db.connect() as connection:
        assert connection.execute("SELECT version_num FROM app.alembic_version").fetchone() == (target,)
        assert connection.execute("SELECT to_regclass('knowledge.parse_node_batches') IS NOT NULL").fetchone() == (True,)
        assert connection.execute("SELECT to_regclass('knowledge.index_write_batches')").fetchone() == (None,)
    repeated = run_migrations(db.migrate_dsn.get_secret_value(), target_revision=target)
    assert repeated["before_revision"] == repeated["after_revision"] == repeated["target_revision"] == target
    assert repeated["head_revision"] == head and not repeated["changed"]
    advanced = run_migrations(db.migrate_dsn.get_secret_value(), target_revision=next_revision)
    assert advanced["before_revision"] == target and advanced["changed"]
    assert advanced["after_revision"] == advanced["target_revision"] == next_revision
    assert advanced["head_revision"] == head
    with db.connect() as connection:
        assert connection.execute("SELECT to_regclass('knowledge.index_write_batches') IS NOT NULL").fetchone() == (True,)
    with pytest.raises(ValueError, match="downgrade is unsupported"):
        run_migrations(db.migrate_dsn.get_secret_value(), target_revision=target)
    with db.connect() as connection:
        assert connection.execute("SELECT version_num FROM app.alembic_version").fetchone() == (next_revision,)
    assert run_migrations(db.migrate_dsn.get_secret_value())["after_revision"] == head
    assert not run_migrations(db.migrate_dsn.get_secret_value())["changed"]


def test_publication_is_atomic_idempotent_and_reindex_keeps_version(migrated_database):
    db = migrated_database
    with db.connect() as connection:
        first = seed_ready_generation(connection)
        reindex = seed_ready_generation(connection, logical_id=first["document"], version_id=first["version"])
    operation = uuid4()
    with db.connect("backend") as connection:
        initial = publish(connection, first, operation=operation)
        assert publish(connection, first, operation=operation) == initial
    with db.connect("backend", autocommit=True) as connection:
        with pytest.raises(psycopg.Error, match="VERSION_CONFLICT"):
            publish(connection, reindex)
    with db.connect("backend") as connection:
        replacement = publish(connection, reindex, initial)
    with db.connect() as connection:
        assert connection.execute("SELECT publication_status FROM app.document_versions WHERE id=%s", (first["version"],)).fetchone() == ("published",)
        assert connection.execute("SELECT epoch FROM app.knowledge_catalog").fetchone() == (2,)
        assert connection.execute("SELECT current_publication_id FROM app.logical_documents WHERE id=%s", (first["document"],)).fetchone() == (replacement,)
        assert connection.execute("SELECT searchable FROM knowledge.chunks WHERE index_generation_id=%s", (first["index"],)).fetchone() == (False,)
        assert connection.execute("SELECT searchable FROM knowledge.chunks WHERE index_generation_id=%s", (reindex["index"],)).fetchone() == (True,)


def test_snapshot_pins_generation_and_excludes_archived(migrated_database):
    db = migrated_database
    with db.connect() as connection:
        active = seed_ready_generation(connection)
        archived = seed_ready_generation(connection, legal_status="archived")
    with db.connect("backend") as connection:
        publication = publish(connection, active)
        publish(connection, archived)
        run_id, owner = uuid4(), uuid4()
        connection.execute("SELECT agent.create_run(%s,'p','question',%s,%s,clock_timestamp()+interval '5 minutes','config',false)", (run_id, str(run_id), "a" * 64))
    with db.connect("runtime") as connection:
        epoch = connection.execute("SELECT execution_epoch FROM agent.acquire_run(%s,%s,60)", (run_id, owner)).fetchone()[0]
    with db.connect("runtime") as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        snapshot = connection.execute("SELECT agent.capture_snapshot(%s,%s,%s)", (run_id, owner, epoch)).fetchone()[0]
    with db.connect() as connection:
        assert connection.execute("SELECT publication_id FROM agent.kb_snapshot_items WHERE snapshot_id=%s", (snapshot,)).fetchall() == [(publication,)]
        replacement = seed_ready_generation(connection, logical_id=active["document"], version_id=active["version"])
    with db.connect("backend") as connection:
        publish(connection, replacement, publication)
    with db.connect("runtime") as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        assert connection.execute("SELECT agent.capture_snapshot(%s,%s,%s)", (run_id, owner, epoch)).fetchone() == (snapshot,)
        assert connection.execute("SELECT index_generation_id FROM agent.kb_snapshot_items WHERE snapshot_id=%s", (snapshot,)).fetchall() == [(active["index"],)]


def test_source_null_offsets_and_unprepared_ready_insert_are_rejected(migrated_database):
    with migrated_database.connect(autocommit=True) as connection:
        malformed = [{"pdf_page": 1, "block_id": "b", "start_offset": None, "end_offset": None}]
        assert connection.execute("SELECT knowledge.valid_spans(%s)", (Jsonb(malformed),)).fetchone() == (False,)
        with pytest.raises(psycopg.errors.CheckViolation, match="GENERATION_MUST_START_STAGING"):
            connection.execute("INSERT INTO knowledge.parse_generations(id,document_version_id,source_sha256,parser_fingerprint,normalizer_version,structure_version,status) VALUES(%s,%s,%s,'p','n','s','ready')", (uuid4(), uuid4(), "a" * 64))


def test_private_routines_and_binding_table_are_not_runtime_authority(migrated_database):
    with migrated_database.connect("runtime", autocommit=True) as connection:
        for statement in ("SELECT agent._append_command_event(NULL,NULL,NULL)",
                          "SELECT agent._assert_run_write(NULL,NULL,NULL)",
                          "INSERT INTO agent.checkpoint_write_bindings DEFAULT VALUES",
                          "SELECT * FROM agent.checkpoint_write_bindings",
                          "DELETE FROM agent.checkpoint_blobs WHERE false"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(statement)


def test_vendor_autocommit_failure_resumes_without_granting_runtime(isolated_database, monkeypatch):
    setup = PostgresSaver.setup

    def interrupted_setup(saver):
        setup(saver)
        raise RuntimeError("SYNTHETIC_VENDOR_SETUP_INTERRUPTION")

    with monkeypatch.context() as patch:
        patch.setattr(PostgresSaver, "setup", interrupted_setup)
        with pytest.raises(RuntimeError, match="SYNTHETIC_VENDOR_SETUP_INTERRUPTION"):
            run_migrations(isolated_database.migrate_dsn.get_secret_value())
    with isolated_database.connect() as connection:
        assert connection.execute("SELECT version_num FROM app.alembic_version").fetchone() == ("p02_004_routines",)
        assert connection.execute("SELECT max(v) FROM agent.checkpoint_migrations").fetchone() == (9,)
        assert connection.execute("SELECT has_table_privilege('expert_runtime','agent.checkpoints','INSERT')").fetchone() == (False,)
    report = run_migrations(isolated_database.migrate_dsn.get_secret_value())
    assert report["before_revision"] == "p02_004_routines" and report["changed"]
    assert report["after_revision"] == "p08_023_repeated_draft_guard"


def _staging_copies(connection, ready):
    parse, index = uuid4(), uuid4()
    connection.execute("INSERT INTO knowledge.parse_generations(id,document_version_id,source_sha256,parser_fingerprint,normalizer_version,structure_version) SELECT %s,document_version_id,source_sha256,parser_fingerprint,normalizer_version,structure_version FROM knowledge.parse_generations WHERE id=%s", (parse, ready["parse"]))
    connection.execute("INSERT INTO knowledge.index_generations(id,document_version_id,parse_generation_id,embedding_model_id,embedding_revision,embedding_dimension,tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version) SELECT %s,document_version_id,parse_generation_id,embedding_model_id,embedding_revision,embedding_dimension,tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version FROM knowledge.index_generations WHERE id=%s", (index, ready["index"]))
    return parse, index


@pytest.mark.parametrize("span", [
    {"pdf_page": 1, "block_id": "b", "start_offset": None, "end_offset": None},
    {"pdf_page": 1, "block_id": "b", "start_offset": "0", "end_offset": "1"},
    {"pdf_page": 1, "block_id": "b", "start_offset": 0},
    {"pdf_page": 0, "block_id": "b", "start_offset": 0, "end_offset": 1},
    {"pdf_page": 1, "block_id": "b", "start_offset": 2, "end_offset": 1},
    {"pdf_page": 1, "block_id": "b", "start_offset": 0, "end_offset": 1, "bbox": [0, 0, 0, 1]},
])
def test_malformed_spans_fail_actual_node_and_chunk_constraints(migrated_database, span):
    with migrated_database.connect() as connection:
        ready = seed_ready_generation(connection)
        parse, index = _staging_copies(connection, ready)
    with migrated_database.connect(autocommit=True) as connection:
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute("INSERT INTO knowledge.document_nodes(id,parse_generation_id,node_type,level,ordinal,canonical_text,structural_path,page_start,page_end,source_spans,content_hash) VALUES(%s,%s,'document',0,0,'text','[]',1,1,%s,%s)", (uuid4(), parse, Jsonb([span]), "a" * 64))
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute("INSERT INTO knowledge.chunks(index_generation_id,parse_generation_id,node_id,chunk_index,source_text,embedding_text,header_text,token_count,source_spans,content_hash,embedding) SELECT %s,parse_generation_id,node_id,0,source_text,embedding_text,header_text,token_count,%s,content_hash,embedding FROM knowledge.chunks WHERE index_generation_id=%s", (index, Jsonb([span]), ready["index"]))


def test_ready_index_insert_and_cyclic_tree_edits_cannot_bypass_guards(migrated_database):
    with migrated_database.connect() as connection:
        ready = seed_ready_generation(connection)
        parse, _ = _staging_copies(connection, ready)
        root, child = uuid4(), uuid4()
        connection.execute("INSERT INTO knowledge.document_nodes(id,parse_generation_id,node_type,level,ordinal,canonical_text,structural_path,page_start,page_end,source_spans,content_hash) VALUES(%s,%s,'document',0,0,'root','[]',1,1,'[]',%s)", (root, parse, "a" * 64))
        connection.execute("INSERT INTO knowledge.document_nodes(id,parse_generation_id,parent_id,node_type,level,ordinal,canonical_text,structural_path,page_start,page_end,source_spans,content_hash) VALUES(%s,%s,%s,'clause',1,0,'child','[]',1,1,'[]',%s)", (child, parse, root, "a" * 64))
    with migrated_database.connect(autocommit=True) as connection:
        with pytest.raises(psycopg.errors.CheckViolation, match="GENERATION_MUST_START_STAGING"):
            connection.execute("INSERT INTO knowledge.index_generations(id,document_version_id,parse_generation_id,embedding_model_id,embedding_revision,embedding_dimension,tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version,status,chunk_count,routing_count,completed_at) SELECT %s,document_version_id,parse_generation_id,embedding_model_id,embedding_revision,embedding_dimension,tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version,'ready',1,1,clock_timestamp() FROM knowledge.index_generations WHERE id=%s", (uuid4(), ready["index"]))
        for statement, params in [
            ("UPDATE knowledge.document_nodes SET parent_id=%s,level=2 WHERE id=%s", (child, root)),
            ("UPDATE knowledge.document_nodes SET level=3 WHERE id=%s", (child,)),
            ("UPDATE knowledge.document_nodes SET parent_id=NULL,level=0 WHERE id=%s", (child,)),
        ]:
            with pytest.raises(psycopg.errors.CheckViolation, match="NODE_STRUCTURE_IMMUTABLE"):
                connection.execute(statement, params)


def test_publication_required_ids_reject_null_before_insert_and_on_replay(migrated_database):
    with migrated_database.connect() as connection:
        ready = seed_ready_generation(connection)
    operation = uuid4()
    with migrated_database.connect("backend", autocommit=True) as connection:
        for already_published in (False, True):
            if already_published:
                publish(connection, ready, operation=operation)
            for position in (0, 1, 3):
                arguments = [ready["version"], ready["index"], None, operation]
                arguments[position] = None
                with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_PUBLICATION_ARGUMENT"):
                    connection.execute("SELECT app.publish_version(%s,%s,%s,%s)", arguments)


def test_published_source_and_ready_generation_content_remain_immutable(migrated_database):
    with migrated_database.connect() as connection:
        ready = seed_ready_generation(connection)
    with migrated_database.connect("backend") as connection:
        publish(connection, ready)
    with migrated_database.connect(autocommit=True) as connection:
        for statement, parameters in [
            ("UPDATE app.document_versions SET source_title='changed' WHERE id=%s", (ready["version"],)),
            ("UPDATE app.stored_objects SET object_key='retargeted' WHERE id=(SELECT source_object_id FROM app.document_versions WHERE id=%s)", (ready["version"],)),
            ("UPDATE knowledge.document_nodes SET canonical_text='changed' WHERE id=%s", (ready["node"],)),
            ("UPDATE knowledge.chunks SET source_text='changed' WHERE index_generation_id=%s", (ready["index"],)),
            ("UPDATE knowledge.index_generations SET status='failed' WHERE id=%s", (ready["index"],)),
            ("DELETE FROM knowledge.parse_generations WHERE id=%s", (ready["parse"],)),
        ]:
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(statement, parameters)
