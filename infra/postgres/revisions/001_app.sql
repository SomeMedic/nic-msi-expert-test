CREATE SCHEMA knowledge AUTHORIZATION expert_migrate;
CREATE SCHEMA agent AUTHORIZATION expert_migrate;
CREATE SCHEMA eval AUTHORIZATION expert_migrate;
REVOKE ALL ON SCHEMA app, knowledge, agent, eval FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE expert_migrate REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

CREATE DOMAIN app.sha256 AS TEXT CHECK (VALUE ~ '^[0-9a-f]{64}$');
CREATE TABLE app.knowledge_catalog (
    workspace_id UUID PRIMARY KEY DEFAULT '00000000-0000-0000-0000-000000000001',
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (workspace_id='00000000-0000-0000-0000-000000000001')
);
INSERT INTO app.knowledge_catalog DEFAULT VALUES;
CREATE TABLE app.stored_objects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), bucket TEXT NOT NULL CHECK (btrim(bucket)<>''),
    object_key TEXT NOT NULL CHECK (btrim(object_key)<>''), object_version_id TEXT,
    media_type TEXT NOT NULL, size_bytes BIGINT NOT NULL CHECK (size_bytes>0), sha256 app.sha256 NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('original','parse_artifact','debug','export')),
    state TEXT NOT NULL DEFAULT 'uploaded' CHECK(state IN ('uploaded','attached','orphaned','purge_pending','deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), expires_at TIMESTAMPTZ,
    UNIQUE(bucket,object_key), UNIQUE(id,sha256,size_bytes)
);
CREATE TABLE app.logical_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES app.knowledge_catalog ON DELETE RESTRICT,
    external_key TEXT, document_type TEXT, document_number TEXT, canonical_title TEXT NOT NULL CHECK(btrim(canonical_title)<>''),
    authority TEXT, current_publication_id UUID, archived_at TIMESTAMPTZ, security_revoked_at TIMESTAMPTZ,
    row_version BIGINT NOT NULL DEFAULT 0 CHECK(row_version>=0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE app.document_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), logical_document_id UUID NOT NULL REFERENCES app.logical_documents ON DELETE RESTRICT,
    version_label TEXT, source_title TEXT NOT NULL CHECK(btrim(source_title)<>''), approved_at DATE NOT NULL,
    edition_at DATE, effective_from DATE, effective_to DATE,
    legal_status TEXT NOT NULL CHECK(legal_status IN ('active','archived')),
    publication_status TEXT NOT NULL DEFAULT 'staging' CHECK(publication_status IN ('staging','published','superseded','deactivated')),
    source_object_id UUID NOT NULL, source_sha256 app.sha256 NOT NULL, original_filename TEXT NOT NULL,
    content_size BIGINT NOT NULL CHECK(content_size>0), metadata_hash app.sha256 NOT NULL,
    supersedes_version_id UUID, created_by_subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), published_at TIMESTAMPTZ, deactivated_at TIMESTAMPTZ,
    UNIQUE(id,logical_document_id), UNIQUE(id,source_sha256),
    FOREIGN KEY(source_object_id,source_sha256,content_size) REFERENCES app.stored_objects(id,sha256,size_bytes) ON DELETE RESTRICT,
    FOREIGN KEY(supersedes_version_id,logical_document_id) REFERENCES app.document_versions(id,logical_document_id) ON DELETE RESTRICT,
    CHECK(effective_from IS NULL OR effective_to IS NULL OR effective_to>=effective_from),
    CHECK(supersedes_version_id IS DISTINCT FROM id)
);
CREATE INDEX versions_document_idx ON app.document_versions(logical_document_id);
CREATE TABLE app.ingestion_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), version_id UUID NOT NULL REFERENCES app.document_versions ON DELETE RESTRICT,
    desired_auto_publish BOOLEAN NOT NULL DEFAULT true, expected_publication_id UUID,
    idempotency_key TEXT NOT NULL, input_fingerprint app.sha256 NOT NULL, principal_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','retry_wait','completed','failed','cancelled')),
    stage TEXT CHECK(stage IS NULL OR stage IN ('queued','validating','parsing','assessing_extraction','fallback_parsing','normalizing','building_structure','chunking','embedding','indexing','ready_to_publish','publishing')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt>=0), max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts>0),
    lease_owner UUID, lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK(lease_epoch>=0), lease_until TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ, last_event_sequence BIGINT NOT NULL DEFAULT 0 CHECK(last_event_sequence>=0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), processed_units BIGINT NOT NULL DEFAULT 0 CHECK(processed_units>=0),
    total_units BIGINT CHECK(total_units>=0), error_code TEXT, safe_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
    UNIQUE(principal_id,idempotency_key), CHECK(attempt<=max_attempts),
    CHECK(total_units IS NULL OR processed_units<=total_units),
    CHECK((status IN ('completed','failed','cancelled'))=(finished_at IS NOT NULL)),
    CHECK(status<>'cancelled' OR cancel_requested_at IS NOT NULL)
);
CREATE TABLE app.ingestion_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1),
    job_id UUID NOT NULL REFERENCES app.ingestion_jobs ON DELETE RESTRICT, sequence BIGINT NOT NULL CHECK(sequence>0),
    event_type TEXT NOT NULL CHECK(event_type IN ('ingestion.created','stage.started','stage.progress','stage.completed','ingestion.ready_to_publish','ingestion.completed','ingestion.failed','ingestion.cancelled')),
    stage TEXT, attempt INTEGER NOT NULL CHECK(attempt>0), execution_epoch BIGINT NOT NULL DEFAULT 0 CHECK(execution_epoch>=0),
    public_payload JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(public_payload)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), UNIQUE(job_id,sequence)
);
CREATE TABLE app.outbox_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), aggregate_type TEXT NOT NULL, aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version>0), topic TEXT NOT NULL,
    payload JSONB NOT NULL CHECK(jsonb_typeof(payload)='object'), available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK(publish_attempts>=0), claim_owner UUID, claim_token UUID,
    claim_epoch BIGINT NOT NULL DEFAULT 0 CHECK(claim_epoch>=0), claim_until TIMESTAMPTZ,
    published_at TIMESTAMPTZ, last_error_code TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX outbox_available_idx ON app.outbox_events(available_at,created_at) WHERE published_at IS NULL;
CREATE INDEX ingestion_available_idx ON app.ingestion_jobs(status,available_at);
CREATE TABLE app.purge_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), document_id UUID NOT NULL REFERENCES app.logical_documents ON DELETE RESTRICT,
    plan_version BIGINT NOT NULL CHECK(plan_version>0), principal_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'planned' CHECK(status IN ('planned','purge_pending','completed','failed')),
    reference_report JSONB NOT NULL CHECK(jsonb_typeof(reference_report)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), expires_at TIMESTAMPTZ NOT NULL,
    CHECK(expires_at>created_at)
);
