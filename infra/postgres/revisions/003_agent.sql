CREATE TABLE agent.kb_snapshots (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), catalog_epoch BIGINT NOT NULL CHECK(catalog_epoch>=0),
 captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), selection_hash app.sha256 NOT NULL, principal_scope_hash app.sha256 NOT NULL
);
CREATE TABLE agent.kb_snapshot_items (
 snapshot_id UUID NOT NULL REFERENCES agent.kb_snapshots ON DELETE RESTRICT, logical_document_id UUID NOT NULL,
 document_version_id UUID NOT NULL, parse_generation_id UUID NOT NULL, index_generation_id UUID NOT NULL, publication_id UUID NOT NULL,
 legal_status_at_capture TEXT NOT NULL CHECK(legal_status_at_capture='active'),
 PRIMARY KEY(snapshot_id,logical_document_id), UNIQUE(snapshot_id,index_generation_id),
 FOREIGN KEY(publication_id,logical_document_id) REFERENCES app.publications(id,logical_document_id) ON DELETE RESTRICT,
 FOREIGN KEY(publication_id,document_version_id,index_generation_id) REFERENCES app.publications(id,document_version_id,index_generation_id) ON DELETE RESTRICT,
 FOREIGN KEY(index_generation_id,parse_generation_id) REFERENCES knowledge.index_generations(id,parse_generation_id) ON DELETE RESTRICT
);
CREATE TABLE agent.runs (
 id UUID PRIMARY KEY, idempotency_key TEXT NOT NULL CHECK(btrim(idempotency_key)<>''), request_hash app.sha256 NOT NULL,
 principal_id TEXT NOT NULL CHECK(btrim(principal_id)<>''), question TEXT NOT NULL CHECK(btrim(question)<>''),
 status TEXT NOT NULL DEFAULT 'created' CHECK(status IN ('created','running','cancelling','completed','refused','failed','cancelled')),
 current_stage TEXT, snapshot_id UUID REFERENCES agent.kb_snapshots ON DELETE RESTRICT,
 execution_owner UUID, execution_epoch BIGINT NOT NULL DEFAULT 0 CHECK(execution_epoch>=0),
 lease_until TIMESTAMPTZ, heartbeat_at TIMESTAMPTZ, deadline_at TIMESTAMPTZ NOT NULL,
 cancel_requested_at TIMESTAMPTZ, repair_attempts INTEGER NOT NULL DEFAULT 0 CHECK(repair_attempts BETWEEN 0 AND 1),
 last_event_sequence BIGINT NOT NULL DEFAULT 0 CHECK(last_event_sequence>=0), result_id UUID, refusal_code TEXT, error_code TEXT,
 trace_id TEXT, configuration_fingerprint TEXT NOT NULL, debug_capture BOOLEAN NOT NULL DEFAULT false,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
 UNIQUE(principal_id,idempotency_key), UNIQUE(id,snapshot_id),
 CHECK(deadline_at>created_at),
 CHECK((status IN ('completed','refused','failed','cancelled'))=(finished_at IS NOT NULL)),
 CHECK(status NOT IN ('cancelling','cancelled') OR cancel_requested_at IS NOT NULL),
 CHECK(status<>'running' OR (execution_owner IS NOT NULL AND execution_epoch>0 AND lease_until IS NOT NULL)),
 CHECK(status NOT IN ('completed','refused') OR result_id IS NOT NULL),
 CHECK(status IN ('completed','refused') OR result_id IS NULL)
);
CREATE INDEX runs_recovery_idx ON agent.runs(status,lease_until) WHERE status IN ('created','running','cancelling');
CREATE TABLE agent.run_events (
 event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version=1),
 run_id UUID NOT NULL REFERENCES agent.runs ON DELETE RESTRICT, sequence BIGINT NOT NULL CHECK(sequence>0),
 event_type TEXT NOT NULL CHECK(event_type IN ('run.created','run.started','run.resuming','run.cancel_requested','stage.started','stage.completed','stage.retry_scheduled','run.completed','run.refused','run.failed','run.cancelled')),
 stage TEXT, attempt INTEGER NOT NULL DEFAULT 1 CHECK(attempt>0), execution_epoch BIGINT NOT NULL CHECK(execution_epoch>=0),
 public_payload JSONB NOT NULL CHECK(jsonb_typeof(public_payload)='object'), created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(run_id,sequence)
);
CREATE TABLE agent.run_results (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID NOT NULL UNIQUE, snapshot_id UUID NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('completed','refused')), verified_answer TEXT, refusal_code TEXT,
 claims_public JSONB NOT NULL CHECK(jsonb_typeof(claims_public)='array'), citations_public JSONB NOT NULL CHECK(jsonb_typeof(citations_public)='array'),
 critic_public_result JSONB NOT NULL CHECK(jsonb_typeof(critic_public_result)='object'), completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(id,run_id), FOREIGN KEY(run_id,snapshot_id) REFERENCES agent.runs(id,snapshot_id) ON DELETE RESTRICT,
 CHECK((kind='completed' AND verified_answer IS NOT NULL AND refusal_code IS NULL) OR
       (kind='refused' AND verified_answer IS NULL AND refusal_code IS NOT NULL))
);
ALTER TABLE agent.runs ADD CONSTRAINT result_belongs_to_run FOREIGN KEY(result_id,id) REFERENCES agent.run_results(id,run_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE agent.run_step_artifacts (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID NOT NULL REFERENCES agent.runs ON DELETE RESTRICT,
 execution_epoch BIGINT NOT NULL CHECK(execution_epoch>0), logical_step_key TEXT NOT NULL, input_hash app.sha256 NOT NULL,
 output_reference UUID REFERENCES app.stored_objects ON DELETE RESTRICT, private_json JSONB,
 status TEXT NOT NULL CHECK(status IN ('completed','failed')), model_revision TEXT, prompt_revision TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), UNIQUE(run_id,execution_epoch,logical_step_key,input_hash),
 CHECK((output_reference IS NOT NULL)<>(private_json IS NOT NULL))
);
CREATE TABLE agent.evidence_packs (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID NOT NULL, snapshot_id UUID NOT NULL,
 manifest JSONB NOT NULL CHECK(jsonb_typeof(manifest)='object'), content_hash app.sha256 NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), FOREIGN KEY(run_id,snapshot_id) REFERENCES agent.runs(id,snapshot_id) ON DELETE RESTRICT
);
CREATE FUNCTION app.reject_mutation() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='APPEND_ONLY'; END $$;
CREATE TRIGGER snapshots_immutable BEFORE UPDATE OR DELETE ON agent.kb_snapshots FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER snapshot_items_immutable BEFORE UPDATE OR DELETE ON agent.kb_snapshot_items FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER events_immutable BEFORE UPDATE OR DELETE ON agent.run_events FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER ingestion_events_immutable BEFORE UPDATE OR DELETE ON app.ingestion_events FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER results_immutable BEFORE UPDATE OR DELETE ON agent.run_results FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER artifacts_immutable BEFORE UPDATE OR DELETE ON agent.run_step_artifacts FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER evidence_immutable BEFORE UPDATE OR DELETE ON agent.evidence_packs FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE FUNCTION agent.guard_run_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.status IN ('completed','refused','failed','cancelled') AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RUN_TERMINAL'; END IF;
 IF OLD.snapshot_id IS NOT NULL AND NEW.snapshot_id IS DISTINCT FROM OLD.snapshot_id THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SNAPSHOT_IMMUTABLE'; END IF;
 IF NEW.execution_epoch<OLD.execution_epoch OR (NEW.execution_owner IS DISTINCT FROM OLD.execution_owner AND NEW.execution_epoch<=OLD.execution_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 IF (NEW.id,NEW.principal_id,NEW.idempotency_key,NEW.request_hash,NEW.question,NEW.deadline_at,NEW.configuration_fingerprint,NEW.debug_capture)
  IS DISTINCT FROM (OLD.id,OLD.principal_id,OLD.idempotency_key,OLD.request_hash,OLD.question,OLD.deadline_at,OLD.configuration_fingerprint,OLD.debug_capture) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RUN_IDENTITY_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER run_identity_guard BEFORE UPDATE ON agent.runs FOR EACH ROW EXECUTE FUNCTION agent.guard_run_identity();
CREATE FUNCTION app.guard_outbox_payload() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['available_at','publish_attempts','claim_owner','claim_token','claim_epoch','claim_until','published_at','last_error_code'])<>
    (to_jsonb(OLD)-ARRAY['available_at','publish_attempts','claim_owner','claim_token','claim_epoch','claim_until','published_at','last_error_code']) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='OUTBOX_PAYLOAD_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER outbox_payload_guard BEFORE UPDATE ON app.outbox_events FOR EACH ROW EXECUTE FUNCTION app.guard_outbox_payload();
