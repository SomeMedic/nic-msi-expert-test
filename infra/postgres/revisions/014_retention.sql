-- P11: immutable per-run policy and final cited evidence survive private expiry.
-- No source object/generation, public result, snapshot, event or budget is purged.
CREATE TABLE agent.run_retention_policies (
 run_id UUID PRIMARY KEY REFERENCES agent.runs ON DELETE RESTRICT,
 operation_id UUID NOT NULL, policy_version TEXT NOT NULL CHECK(policy_version='p11.capture.v1'),
 capture_enabled BOOLEAN NOT NULL, capture_ttl_hours INTEGER NOT NULL CHECK(capture_ttl_hours BETWEEN 1 AND 168),
 capture_expires_at TIMESTAMPTZ,
 private_retention_hours INTEGER NOT NULL CHECK(private_retention_hours BETWEEN 1 AND 8760),
 private_payload_expires_at TIMESTAMPTZ, payloads_purged_at TIMESTAMPTZ,
 cleanup_counts JSONB, registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 CHECK(capture_enabled=(capture_expires_at IS NOT NULL)),
 CHECK(capture_expires_at IS NULL OR isfinite(capture_expires_at)),
 CHECK(private_payload_expires_at IS NULL OR isfinite(private_payload_expires_at)),
 CHECK(payloads_purged_at IS NULL OR (private_payload_expires_at IS NOT NULL AND payloads_purged_at>=private_payload_expires_at)),
 CHECK((payloads_purged_at IS NULL)=(cleanup_counts IS NULL))
);
CREATE INDEX retention_expiry ON agent.run_retention_policies(private_payload_expires_at,run_id)
 WHERE private_payload_expires_at IS NOT NULL AND payloads_purged_at IS NULL;
-- Historical consent never retroactively creates capture authority.
INSERT INTO agent.run_retention_policies(run_id,operation_id,policy_version,capture_enabled,capture_ttl_hours,
 private_retention_hours,private_payload_expires_at)
SELECT id,id,'p11.capture.v1',false,24,168,finished_at+interval '168 hours' FROM agent.runs;

CREATE TABLE agent.private_cleanup_bindings (
 transaction_id XID8 NOT NULL, backend_pid INTEGER NOT NULL, run_id UUID NOT NULL REFERENCES agent.runs,
 PRIMARY KEY(transaction_id,backend_pid,run_id)
);
CREATE FUNCTION agent._private_cleanup_authorized(p_run UUID) RETURNS BOOLEAN
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM agent.private_cleanup_bindings b JOIN agent.runs r ON r.id=b.run_id
  JOIN agent.run_retention_policies p ON p.run_id=r.id
  WHERE b.run_id=p_run AND b.transaction_id=pg_current_xact_id() AND b.backend_pid=pg_backend_pid()
   AND r.status IN ('completed','refused','failed','cancelled') AND r.finished_at IS NOT NULL
   AND p.private_payload_expires_at<=clock_timestamp() AND p.payloads_purged_at IS NULL)
$$;
CREATE FUNCTION agent._guard_retention_policy() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RETENTION_POLICY_IMMUTABLE'; END IF;
 IF (to_jsonb(NEW)-ARRAY['private_payload_expires_at','payloads_purged_at','cleanup_counts'])<>
    (to_jsonb(OLD)-ARRAY['private_payload_expires_at','payloads_purged_at','cleanup_counts']) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RETENTION_POLICY_IMMUTABLE'; END IF;
 IF OLD.private_payload_expires_at IS NOT NULL AND NEW.private_payload_expires_at IS DISTINCT FROM OLD.private_payload_expires_at THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RETENTION_POLICY_IMMUTABLE'; END IF;
 IF OLD.private_payload_expires_at IS NULL AND NEW.private_payload_expires_at IS NOT NULL THEN
  SELECT * INTO STRICT r FROM agent.runs WHERE id=OLD.run_id;
  IF r.finished_at IS NULL OR NEW.private_payload_expires_at<>r.finished_at+make_interval(hours=>OLD.private_retention_hours) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RETENTION_POLICY_IMMUTABLE'; END IF;
 END IF;
 IF (NEW.payloads_purged_at,NEW.cleanup_counts) IS DISTINCT FROM (OLD.payloads_purged_at,OLD.cleanup_counts) AND
   (OLD.payloads_purged_at IS NOT NULL OR NOT agent._private_cleanup_authorized(OLD.run_id)) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PRIVATE_CLEANUP_BINDING_REQUIRED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER retention_policy_guard BEFORE UPDATE OR DELETE ON agent.run_retention_policies
 FOR EACH ROW EXECUTE FUNCTION agent._guard_retention_policy();
CREATE FUNCTION agent.register_run_retention(p_run_id UUID,p_principal_id TEXT,p_operation_id UUID,
 p_capture_ttl_hours INTEGER,p_private_retention_hours INTEGER) RETURNS agent.run_retention_policies
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; p agent.run_retention_policies;
BEGIN
 IF p_operation_id IS NULL OR p_capture_ttl_hours IS NULL OR p_capture_ttl_hours NOT BETWEEN 1 AND 168 OR
  p_private_retention_hours IS NULL OR p_private_retention_hours NOT BETWEEN 1 AND 8760 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RETENTION_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id AND principal_id=p_principal_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 SELECT * INTO p FROM agent.run_retention_policies WHERE run_id=r.id;
 IF FOUND THEN RETURN p; END IF;
 -- Only creation may register opt-in; missing policy on an already started run is disabled.
 INSERT INTO agent.run_retention_policies(run_id,operation_id,policy_version,capture_enabled,capture_ttl_hours,
  capture_expires_at,private_retention_hours,private_payload_expires_at)
 VALUES(r.id,p_operation_id,'p11.capture.v1',r.debug_capture AND r.status='created',p_capture_ttl_hours,
  CASE WHEN r.debug_capture AND r.status='created' THEN r.created_at+make_interval(hours=>p_capture_ttl_hours) END,
  p_private_retention_hours,r.finished_at+make_interval(hours=>p_private_retention_hours)) RETURNING * INTO p;
 RETURN p;
END $$;
ALTER FUNCTION agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN,INTEGER) RENAME TO _create_run_before_retention;
REVOKE ALL ON FUNCTION agent._create_run_before_retention(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN,INTEGER)
 FROM PUBLIC,expert_backend,expert_runtime,expert_ingest,expert_outbox;
CREATE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN,
 p_max_recovery_attempts INTEGER,p_capture_ttl_hours INTEGER,p_private_retention_hours INTEGER) RETURNS agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 r:=agent._create_run_before_retention(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,
  p_configuration_fingerprint,p_debug_capture,p_max_recovery_attempts);
 PERFORM agent.register_run_retention(r.id,p_principal_id,r.id,p_capture_ttl_hours,p_private_retention_hours);
 RETURN r;
END $$;
CREATE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN,p_max_recovery_attempts INTEGER)
RETURNS agent.runs LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT agent.create_run(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,
  p_configuration_fingerprint,p_debug_capture,p_max_recovery_attempts,24,168)
$$;
CREATE OR REPLACE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN)
RETURNS agent.runs LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT agent.create_run(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,
  p_configuration_fingerprint,p_debug_capture,2,24,168)
$$;
CREATE FUNCTION agent._set_terminal_retention() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 UPDATE agent.run_retention_policies SET private_payload_expires_at=NEW.finished_at+make_interval(hours=>private_retention_hours)
  WHERE run_id=NEW.id AND private_payload_expires_at IS NULL;
 RETURN NEW;
END $$;
CREATE TRIGGER terminal_retention AFTER UPDATE OF finished_at ON agent.runs FOR EACH ROW
 WHEN(OLD.finished_at IS NULL AND NEW.finished_at IS NOT NULL) EXECUTE FUNCTION agent._set_terminal_retention();
CREATE FUNCTION agent.get_run_capture_policy(p_run_id UUID,p_owner UUID,p_epoch BIGINT)
RETURNS TABLE(enabled BOOLEAN,policy_version TEXT,expires_at TIMESTAMPTZ)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.run_retention_policies;
BEGIN
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
 SELECT * INTO p FROM agent.run_retention_policies WHERE run_id=p_run_id;
 enabled:=coalesce(p.capture_enabled AND p.capture_expires_at>clock_timestamp(),false);
 policy_version:='p11.capture.v1'; expires_at:=p.capture_expires_at; RETURN NEXT;
END $$;

CREATE TABLE agent.result_evidence (
 result_id UUID NOT NULL, run_id UUID NOT NULL, snapshot_id UUID NOT NULL,
 evidence_id TEXT NOT NULL CHECK(length(evidence_id) BETWEEN 1 AND 64),
 document_version_id UUID NOT NULL, parse_generation_id UUID NOT NULL, index_generation_id UUID NOT NULL,
 canonical_node_id UUID NOT NULL, source_sha256 app.sha256 NOT NULL,
 unit JSONB NOT NULL CHECK(jsonb_typeof(unit)='object'), binding JSONB NOT NULL CHECK(jsonb_typeof(binding)='object'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(result_id,evidence_id),
 FOREIGN KEY(result_id,run_id) REFERENCES agent.run_results(id,run_id) ON DELETE RESTRICT,
 FOREIGN KEY(run_id,snapshot_id) REFERENCES agent.runs(id,snapshot_id) ON DELETE RESTRICT,
 FOREIGN KEY(snapshot_id,index_generation_id) REFERENCES agent.kb_snapshot_items(snapshot_id,index_generation_id) ON DELETE RESTRICT,
 FOREIGN KEY(document_version_id,source_sha256) REFERENCES app.document_versions(id,source_sha256) ON DELETE RESTRICT,
 FOREIGN KEY(index_generation_id,parse_generation_id) REFERENCES knowledge.index_generations(id,parse_generation_id) ON DELETE RESTRICT,
 FOREIGN KEY(canonical_node_id,parse_generation_id) REFERENCES knowledge.document_nodes(id,parse_generation_id) ON DELETE RESTRICT
);
CREATE TRIGGER result_evidence_immutable BEFORE UPDATE OR DELETE ON agent.result_evidence FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE FUNCTION agent._materialize_result_evidence(p_result_id UUID) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE rr agent.run_results; ep agent.evidence_packs; c JSONB; u JSONB; b JSONB; v app.document_versions; item agent.kb_snapshot_items;
BEGIN
 SELECT * INTO STRICT rr FROM agent.run_results WHERE id=p_result_id;
 IF rr.kind='refused' THEN
  IF jsonb_array_length(rr.citations_public)<>0 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='FINAL_EVIDENCE_INVALID'; END IF;
  RETURN;
 END IF;
 SELECT * INTO ep FROM agent.evidence_packs WHERE id=rr.evidence_pack_id AND run_id=rr.run_id AND snapshot_id=rr.snapshot_id;
 IF NOT FOUND OR ep.pack IS NULL OR ep.manifest IS NULL OR jsonb_array_length(rr.citations_public)=0 THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='FINAL_EVIDENCE_MISSING'; END IF;
 FOR c IN SELECT DISTINCT value->'evidence_id' FROM jsonb_array_elements(rr.citations_public) LOOP
  SELECT value INTO STRICT u FROM jsonb_array_elements(ep.pack->'units') WHERE value->'evidence_id'=c;
  SELECT value INTO STRICT b FROM jsonb_array_elements(ep.manifest->'bindings') WHERE value->'evidence_id'=c;
  SELECT * INTO STRICT item FROM agent.kb_snapshot_items WHERE snapshot_id=rr.snapshot_id
   AND document_version_id=(u->>'document_version_id')::uuid AND index_generation_id=(u->>'index_generation_id')::uuid;
  SELECT * INTO STRICT v FROM app.document_versions WHERE id=item.document_version_id;
  IF b->>'parse_generation_id' IS DISTINCT FROM item.parse_generation_id::text OR
   u->>'content_hash' IS DISTINCT FROM encode(sha256(convert_to(u->>'excerpt','UTF8')),'hex') THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='FINAL_EVIDENCE_INVALID'; END IF;
  INSERT INTO agent.result_evidence(result_id,run_id,snapshot_id,evidence_id,document_version_id,parse_generation_id,
   index_generation_id,canonical_node_id,source_sha256,unit,binding)
  VALUES(rr.id,rr.run_id,rr.snapshot_id,u->>'evidence_id',item.document_version_id,item.parse_generation_id,
   item.index_generation_id,(u->>'canonical_node_id')::uuid,v.source_sha256,u,b);
 END LOOP;
END $$;
CREATE FUNCTION agent._capture_final_evidence() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN PERFORM agent._materialize_result_evidence(NEW.id); RETURN NEW; END $$;
CREATE TRIGGER result_evidence_capture AFTER INSERT ON agent.run_results FOR EACH ROW EXECUTE FUNCTION agent._capture_final_evidence();
DO $$ DECLARE rr RECORD; BEGIN
 FOR rr IN SELECT id FROM agent.run_results ORDER BY id LOOP PERFORM agent._materialize_result_evidence(rr.id); END LOOP;
END $$;
CREATE OR REPLACE FUNCTION agent.get_public_evidence(p_run_id UUID,p_principal_id TEXT,p_evidence_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE unit JSONB; version app.document_versions; revoked TIMESTAMPTZ;
BEGIN
 IF p_run_id IS NULL OR p_principal_id IS NULL OR btrim(p_principal_id)='' OR
  p_evidence_id IS NULL OR btrim(p_evidence_id)='' OR length(p_evidence_id)>64 THEN RETURN NULL; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT e.unit INTO unit FROM agent.runs r
 JOIN agent.run_results rr ON rr.id=r.result_id AND rr.run_id=r.id AND rr.snapshot_id=r.snapshot_id
 JOIN agent.result_evidence e ON e.result_id=rr.id AND e.run_id=r.id AND e.snapshot_id=r.snapshot_id
 WHERE r.id=p_run_id AND r.principal_id=p_principal_id AND r.status='completed' AND rr.kind='completed'
  AND e.evidence_id=p_evidence_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT * INTO STRICT version FROM app.document_versions WHERE id=(unit->>'document_version_id')::uuid;
 SELECT security_revoked_at INTO STRICT revoked FROM app.logical_documents WHERE id=version.logical_document_id;
 IF revoked IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 RETURN jsonb_build_object('evidence_id',p_evidence_id,'run_id',p_run_id,'document_version_id',version.id,
  'document_title',unit->'document_title','version_label',version.version_label,'structural_path',unit->'structural_path',
  'excerpt',unit->'excerpt','source_spans',unit->'source_spans','source_url','/api/v1/versions/'||version.id::text||'/source');
END $$;

ALTER TABLE agent.run_step_artifacts ADD COLUMN payload_purged_at TIMESTAMPTZ, ADD COLUMN payload_sha256 app.sha256,
 ADD COLUMN safe_metadata JSONB;
DO $$ DECLARE c TEXT; BEGIN
 SELECT conname INTO STRICT c FROM pg_constraint WHERE conrelid='agent.run_step_artifacts'::regclass AND contype='c'
  AND pg_get_constraintdef(oid) LIKE '%output_reference%private_json%';
 EXECUTE format('ALTER TABLE agent.run_step_artifacts DROP CONSTRAINT %I',c);
END $$;
ALTER TABLE agent.run_step_artifacts ADD CONSTRAINT artifact_payload_shape CHECK(
 (payload_purged_at IS NULL AND ((output_reference IS NOT NULL)<>(private_json IS NOT NULL))) OR
 (payload_purged_at IS NOT NULL AND output_reference IS NULL AND private_json IS NULL AND payload_sha256 IS NOT NULL AND safe_metadata IS NOT NULL));
ALTER TABLE agent.evidence_packs ALTER COLUMN manifest DROP NOT NULL,
 ADD COLUMN payload_purged_at TIMESTAMPTZ, ADD COLUMN pack_sha256 app.sha256, ADD COLUMN manifest_sha256 app.sha256;
ALTER TABLE agent.evidence_packs ADD CONSTRAINT evidence_payload_shape CHECK(
 (payload_purged_at IS NULL AND manifest IS NOT NULL) OR
 (payload_purged_at IS NOT NULL AND manifest IS NULL AND pack IS NULL AND canonical_manifest IS NULL AND manifest_sha256 IS NOT NULL));
CREATE FUNCTION agent._safe_model_metadata(a agent.run_step_artifacts) RETURNS JSONB
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('artifact_id',a.id,'call_id',coalesce(a.private_json#>'{provenance,call_id}',a.private_json->'call_id','null'::jsonb),
  'role',a.kind,'status',a.status,'execution_epoch',a.execution_epoch,
  'schema_attempt',coalesce(a.private_json#>'{provenance,schema_attempt}',a.private_json->'schema_attempt','null'::jsonb),
  'model_revision',a.model_revision,'prompt_version',a.prompt_revision,'profile_sha256',a.identity->'profile_sha256',
  'prompt_sha256',a.identity->'prompt_sha256','schema_sha256',a.identity->'schema_sha256',
  'input_tokens',a.private_json#>'{provenance,input_tokens}','output_tokens',a.private_json#>'{provenance,output_tokens}',
  'elapsed_ms',a.private_json#>'{provenance,elapsed_ms}','created_at',a.created_at)
$$;
CREATE FUNCTION agent._artifact_retention_guard() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.payload_purged_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PRIVATE_TOMBSTONE'; END IF;
  NEW.payload_sha256:=CASE WHEN NEW.private_json IS NOT NULL THEN encode(sha256(convert_to(NEW.private_json::text,'UTF8')),'hex') END;
  NEW.safe_metadata:=agent._safe_model_metadata(NEW); RETURN NEW;
 END IF;
 IF TG_OP='DELETE' OR OLD.payload_purged_at IS NOT NULL OR NOT agent._private_cleanup_authorized(OLD.run_id) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='APPEND_ONLY'; END IF;
 IF (to_jsonb(NEW)-ARRAY['private_json','payload_purged_at','payload_sha256','safe_metadata'])<>
    (to_jsonb(OLD)-ARRAY['private_json','payload_purged_at','payload_sha256','safe_metadata']) OR
  NEW.private_json IS NOT NULL OR NEW.payload_purged_at IS NULL OR OLD.output_reference IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PRIVATE_TOMBSTONE'; END IF;
 NEW.payload_sha256:=coalesce(OLD.payload_sha256,encode(sha256(convert_to(OLD.private_json::text,'UTF8')),'hex'));
 NEW.safe_metadata:=coalesce(OLD.safe_metadata,agent._safe_model_metadata(OLD)); RETURN NEW;
END $$;
DROP TRIGGER artifacts_immutable ON agent.run_step_artifacts;
CREATE TRIGGER artifacts_retention_guard BEFORE INSERT OR UPDATE OR DELETE ON agent.run_step_artifacts
 FOR EACH ROW EXECUTE FUNCTION agent._artifact_retention_guard();
CREATE FUNCTION agent._evidence_retention_guard() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.payload_purged_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PRIVATE_TOMBSTONE'; END IF;
  NEW.pack_sha256:=CASE WHEN NEW.pack IS NOT NULL THEN encode(sha256(convert_to(NEW.pack::text,'UTF8')),'hex') END;
  NEW.manifest_sha256:=encode(sha256(convert_to(NEW.manifest::text,'UTF8')),'hex'); RETURN NEW;
 END IF;
 IF TG_OP='DELETE' OR OLD.payload_purged_at IS NOT NULL OR NOT agent._private_cleanup_authorized(OLD.run_id) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='APPEND_ONLY'; END IF;
 IF (to_jsonb(NEW)-ARRAY['manifest','pack','canonical_manifest','payload_purged_at','pack_sha256','manifest_sha256'])<>
    (to_jsonb(OLD)-ARRAY['manifest','pack','canonical_manifest','payload_purged_at','pack_sha256','manifest_sha256']) OR
  NEW.manifest IS NOT NULL OR NEW.pack IS NOT NULL OR NEW.canonical_manifest IS NOT NULL OR NEW.payload_purged_at IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PRIVATE_TOMBSTONE'; END IF;
 NEW.pack_sha256:=coalesce(OLD.pack_sha256,CASE WHEN OLD.pack IS NOT NULL THEN encode(sha256(convert_to(OLD.pack::text,'UTF8')),'hex') END);
 NEW.manifest_sha256:=coalesce(OLD.manifest_sha256,encode(sha256(convert_to(OLD.manifest::text,'UTF8')),'hex')); RETURN NEW;
END $$;
DROP TRIGGER evidence_immutable ON agent.evidence_packs;
CREATE TRIGGER evidence_retention_guard BEFORE INSERT OR UPDATE OR DELETE ON agent.evidence_packs
 FOR EACH ROW EXECUTE FUNCTION agent._evidence_retention_guard();

CREATE FUNCTION agent.expire_private_run_payloads(p_limit INTEGER DEFAULT 20) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.run_retention_policies; r agent.runs; rr agent.run_results; ep agent.evidence_packs;
 n INTEGER; runs_n INTEGER:=0; artifacts_n BIGINT:=0; packs_n BIGINT:=0; checkpoints_n BIGINT:=0; blobs_n BIGINT:=0; writes_n BIGINT:=0;
 counts JSONB;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 20 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RETENTION_LIMIT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 FOR p IN SELECT policy.* FROM agent.run_retention_policies policy JOIN agent.runs run ON run.id=policy.run_id
  WHERE policy.payloads_purged_at IS NULL AND policy.private_payload_expires_at<=clock_timestamp()
   AND run.status IN ('completed','refused','failed','cancelled') AND run.finished_at IS NOT NULL
  ORDER BY policy.private_payload_expires_at,policy.run_id LIMIT p_limit FOR UPDATE OF run,policy SKIP LOCKED LOOP
  SELECT * INTO STRICT r FROM agent.runs WHERE id=p.run_id;
  IF EXISTS(SELECT 1 FROM agent.run_step_artifacts WHERE run_id=r.id AND output_reference IS NOT NULL) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PRIVATE_RETENTION_LEGACY_REFERENCE'; END IF;
  IF r.result_id IS NOT NULL THEN
   SELECT * INTO STRICT rr FROM agent.run_results WHERE id=r.result_id AND run_id=r.id;
   IF (SELECT count(*) FROM agent.result_evidence WHERE result_id=rr.id)<>(
     SELECT count(DISTINCT value->>'evidence_id') FROM jsonb_array_elements(rr.citations_public)) THEN
    RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='FINAL_EVIDENCE_MISSING'; END IF;
   IF rr.kind='completed' THEN
    SELECT * INTO ep FROM agent.evidence_packs WHERE id=rr.evidence_pack_id AND run_id=r.id;
    IF NOT FOUND OR ep.pack IS NULL OR EXISTS(SELECT 1 FROM agent.result_evidence e WHERE e.result_id=rr.id AND
      (NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ep.pack->'units') u WHERE u=e.unit) OR
       NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ep.manifest->'bindings') b WHERE b=e.binding))) THEN
     RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='FINAL_EVIDENCE_MISSING'; END IF;
   END IF;
  END IF;
  INSERT INTO agent.private_cleanup_bindings VALUES(pg_current_xact_id(),pg_backend_pid(),r.id);
  counts:='{}'::jsonb;
  UPDATE agent.run_step_artifacts SET private_json=NULL,payload_purged_at=clock_timestamp() WHERE run_id=r.id AND payload_purged_at IS NULL;
  GET DIAGNOSTICS n=ROW_COUNT; artifacts_n:=artifacts_n+n; counts:=counts||jsonb_build_object('artifacts_purged',n);
  UPDATE agent.evidence_packs SET manifest=NULL,pack=NULL,canonical_manifest=NULL,payload_purged_at=clock_timestamp()
   WHERE run_id=r.id AND payload_purged_at IS NULL;
  GET DIAGNOSTICS n=ROW_COUNT; packs_n:=packs_n+n; counts:=counts||jsonb_build_object('evidence_packs_purged',n);
  DELETE FROM agent.checkpoint_writes WHERE thread_id=r.id::text;
  GET DIAGNOSTICS n=ROW_COUNT; writes_n:=writes_n+n; counts:=counts||jsonb_build_object('checkpoint_writes_deleted',n);
  DELETE FROM agent.checkpoint_blobs WHERE thread_id=r.id::text;
  GET DIAGNOSTICS n=ROW_COUNT; blobs_n:=blobs_n+n; counts:=counts||jsonb_build_object('checkpoint_blobs_deleted',n);
  DELETE FROM agent.checkpoints WHERE thread_id=r.id::text;
  GET DIAGNOSTICS n=ROW_COUNT; checkpoints_n:=checkpoints_n+n; counts:=counts||jsonb_build_object('checkpoint_rows_deleted',n);
  UPDATE agent.run_retention_policies SET payloads_purged_at=clock_timestamp(),cleanup_counts=counts WHERE run_id=r.id;
  DELETE FROM agent.private_cleanup_bindings WHERE transaction_id=pg_current_xact_id() AND backend_pid=pg_backend_pid() AND run_id=r.id;
  runs_n:=runs_n+1;
 END LOOP;
 RETURN jsonb_build_object('runs_purged',runs_n,'artifacts_purged',artifacts_n,'evidence_packs_purged',packs_n,
  'checkpoint_rows_deleted',checkpoints_n,'checkpoint_blobs_deleted',blobs_n,'checkpoint_writes_deleted',writes_n);
END $$;
