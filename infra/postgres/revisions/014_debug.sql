-- P11 bounded opt-in object registry. Storage I/O never runs in this transaction.
CREATE TABLE agent.debug_parts (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), run_id UUID NOT NULL REFERENCES agent.run_retention_policies(run_id),
 execution_owner UUID NOT NULL, execution_epoch BIGINT NOT NULL CHECK(execution_epoch>0), call_id UUID NOT NULL,
 part TEXT NOT NULL CHECK(part IN ('request','response')), role TEXT NOT NULL CHECK(role IN ('router','drafter','critic','repair')),
 schema_attempt INTEGER NOT NULL CHECK(schema_attempt BETWEEN 0 AND 1), request_sha256 app.sha256 NOT NULL,
 payload_sha256 app.sha256 NOT NULL, metadata JSONB NOT NULL CHECK(jsonb_typeof(metadata)='object'),
 object_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(), bucket TEXT NOT NULL DEFAULT 'debug' CHECK(bucket='debug'),
 object_key TEXT NOT NULL UNIQUE, object_version_id TEXT CHECK(length(object_version_id) BETWEEN 1 AND 1000),
 sha256 app.sha256 NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 2097152),
 expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), attached_at TIMESTAMPTZ,
 state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','attached','cleanup_pending','cleaned')),
 claim_owner UUID, claim_token UUID, claim_until TIMESTAMPTZ, cleaned_at TIMESTAMPTZ, next_cleanup_check_at TIMESTAMPTZ,
 UNIQUE(run_id,call_id,execution_epoch,part), CHECK(expires_at>created_at AND isfinite(expires_at)),
 CHECK((state='cleanup_pending')=(claim_owner IS NOT NULL AND claim_token IS NOT NULL AND claim_until IS NOT NULL)),
 CHECK((state='cleaned')=(cleaned_at IS NOT NULL AND next_cleanup_check_at IS NOT NULL)),
 CHECK(state<>'attached' OR attached_at IS NOT NULL),
 CHECK(object_key='runs/'||run_id::text||'/debug/'||execution_epoch::text||'/'||call_id::text||'/'||part||'/'||object_id::text||'/'||sha256||'.json')
);
CREATE INDEX debug_parts_expiry ON agent.debug_parts(expires_at,id) WHERE state IN ('reserved','attached');
CREATE INDEX debug_parts_orphan ON agent.debug_parts(created_at,id) WHERE state='reserved';
CREATE INDEX debug_parts_lease ON agent.debug_parts(claim_until,id) WHERE state='cleanup_pending';
CREATE INDEX debug_parts_audit ON agent.debug_parts(next_cleanup_check_at,id) WHERE state='cleaned';
CREATE TABLE agent.debug_capture_failures (
 run_id UUID NOT NULL REFERENCES agent.run_retention_policies(run_id), call_id UUID NOT NULL,
 execution_epoch BIGINT NOT NULL CHECK(execution_epoch>0), part TEXT NOT NULL CHECK(part IN ('request','response')),
 reason TEXT NOT NULL CHECK(reason IN ('oversized','unavailable','credential_detected')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(run_id,call_id,execution_epoch,part)
);
CREATE TRIGGER debug_failures_immutable BEFORE UPDATE OR DELETE ON agent.debug_capture_failures
 FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE FUNCTION agent._guard_debug_part() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='DEBUG_IDENTITY_IMMUTABLE'; END IF;
 IF (to_jsonb(NEW)-ARRAY['object_version_id','attached_at','state','claim_owner','claim_token','claim_until','cleaned_at','next_cleanup_check_at'])<>
    (to_jsonb(OLD)-ARRAY['object_version_id','attached_at','state','claim_owner','claim_token','claim_until','cleaned_at','next_cleanup_check_at']) OR
  (OLD.object_version_id IS NOT NULL AND NEW.object_version_id IS DISTINCT FROM OLD.object_version_id) OR
  (OLD.attached_at IS NOT NULL AND NEW.attached_at IS DISTINCT FROM OLD.attached_at) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='DEBUG_IDENTITY_IMMUTABLE'; END IF;
 IF NOT ((OLD.state='reserved' AND NEW.state IN ('attached','cleanup_pending')) OR
  (OLD.state='attached' AND NEW.state='cleanup_pending') OR
  (OLD.state='cleanup_pending' AND NEW.state IN ('cleanup_pending','cleaned')) OR
  (OLD.state='cleaned' AND NEW.state='cleanup_pending')) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='DEBUG_STATE_CONFLICT'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER debug_part_guard BEFORE UPDATE OR DELETE ON agent.debug_parts FOR EACH ROW EXECUTE FUNCTION agent._guard_debug_part();
CREATE FUNCTION agent._debug_ref(p agent.debug_parts) RETURNS JSONB LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('part_id',p.id,'object_id',p.object_id,'bucket',p.bucket,'object_key',p.object_key,
  'object_version_id',p.object_version_id,'sha256',p.sha256,'size_bytes',p.size_bytes,'expires_at',p.expires_at,'state',p.state)
$$;
CREATE FUNCTION agent._debug_revoked(p_run_id UUID) RETURNS BOOLEAN LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM agent.runs r JOIN agent.kb_snapshot_items i ON i.snapshot_id=r.snapshot_id
  JOIN app.logical_documents d ON d.id=i.logical_document_id WHERE r.id=p_run_id AND d.security_revoked_at IS NOT NULL)
$$;
CREATE FUNCTION agent._assert_capture(p_run_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS agent.run_retention_policies
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.run_retention_policies;
BEGIN
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
 SELECT * INTO p FROM agent.run_retention_policies WHERE run_id=p_run_id;
 IF NOT FOUND OR NOT p.capture_enabled THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_DISABLED'; END IF;
 IF p.capture_expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_EXPIRED'; END IF;
 RETURN p;
END $$;
CREATE FUNCTION agent.reserve_debug_part(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_call_id UUID,p_part TEXT,p_role TEXT,
 p_schema_attempt INTEGER,p_request_sha256 TEXT,p_payload_sha256 TEXT,p_object_sha256 TEXT,p_object_size BIGINT,p_metadata JSONB)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE policy agent.run_retention_policies; r agent.runs; p agent.debug_parts; other agent.debug_parts; object_uuid UUID; k TEXT;
BEGIN
 policy:=agent._assert_capture(p_run_id,p_owner,p_epoch);
 SELECT * INTO STRICT r FROM agent.runs WHERE id=p_run_id;
 IF p_call_id IS NULL OR p_part IS NULL OR p_part NOT IN ('request','response') OR p_role IS NULL OR
  p_role NOT IN ('router','drafter','critic','repair') OR p_schema_attempt IS NULL OR p_schema_attempt NOT BETWEEN 0 AND 1 OR
  p_request_sha256 IS NULL OR p_request_sha256!~'^[0-9a-f]{64}$' OR p_payload_sha256 IS NULL OR p_payload_sha256!~'^[0-9a-f]{64}$' OR
  p_object_sha256 IS NULL OR p_object_sha256!~'^[0-9a-f]{64}$' OR p_object_size IS NULL OR p_object_size NOT BETWEEN 1 AND 2097152 OR
  (p_part='request' AND p_request_sha256<>p_payload_sha256) OR
  NOT agent._closed(p_metadata,ARRAY['policy_version','configuration_fingerprint','model_revision','profile_sha256','prompt_version',
   'prompt_sha256','schema_sha256','input_sha256','messages_sha256','payload_size_bytes']) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEBUG_ARGUMENT'; END IF;
 IF p_metadata->>'policy_version' IS DISTINCT FROM policy.policy_version OR
  p_metadata->>'configuration_fingerprint' IS DISTINCT FROM r.configuration_fingerprint OR
  NOT agent._text(p_metadata->'model_revision',200) OR NOT agent._text(p_metadata->'prompt_version',200) OR
  NOT knowledge._parse_integer(p_metadata->'payload_size_bytes',0,1048576) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEBUG_ARGUMENT'; END IF;
 FOREACH k IN ARRAY ARRAY['profile_sha256','prompt_sha256','schema_sha256','input_sha256','messages_sha256'] LOOP
  IF NOT agent._sha(p_metadata->k) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEBUG_ARGUMENT'; END IF;
 END LOOP;
 SELECT * INTO p FROM agent.debug_parts WHERE run_id=p_run_id AND call_id=p_call_id AND execution_epoch=p_epoch AND part=p_part;
 IF FOUND THEN
  IF (p.role,p.schema_attempt,p.request_sha256,p.payload_sha256,p.sha256,p.size_bytes,p.metadata) IS DISTINCT FROM
   (p_role,p_schema_attempt,p_request_sha256,p_payload_sha256,p_object_sha256,p_object_size,p_metadata) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  IF p.state NOT IN ('reserved','attached') THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_EXPIRED'; END IF;
  PERFORM agent._assert_capture(p_run_id,p_owner,p_epoch); RETURN agent._debug_ref(p);
 END IF;
 -- The opposite part must describe the same physical exchange, even on replay.
 SELECT * INTO other FROM agent.debug_parts WHERE run_id=p_run_id AND call_id=p_call_id AND execution_epoch=p_epoch;
 IF FOUND AND (other.role,other.schema_attempt,other.request_sha256,other.metadata-'payload_size_bytes') IS DISTINCT FROM
  (p_role,p_schema_attempt,p_request_sha256,p_metadata-'payload_size_bytes') THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
 IF (SELECT count(*)>=60 OR coalesce(sum(size_bytes),0)+p_object_size>67108864 FROM agent.debug_parts WHERE run_id=p_run_id) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CAPACITY_EXCEEDED'; END IF;
 object_uuid:=gen_random_uuid();
 INSERT INTO agent.debug_parts(run_id,execution_owner,execution_epoch,call_id,part,role,schema_attempt,request_sha256,payload_sha256,
  metadata,object_id,object_key,sha256,size_bytes,expires_at)
 VALUES(p_run_id,p_owner,p_epoch,p_call_id,p_part,p_role,p_schema_attempt,p_request_sha256,p_payload_sha256,p_metadata,object_uuid,
  'runs/'||p_run_id::text||'/debug/'||p_epoch::text||'/'||p_call_id::text||'/'||p_part||'/'||object_uuid::text||'/'||p_object_sha256||'.json',
  p_object_sha256,p_object_size,policy.capture_expires_at) RETURNING * INTO p;
 PERFORM agent._assert_capture(p_run_id,p_owner,p_epoch); RETURN agent._debug_ref(p);
END $$;
CREATE FUNCTION agent.attach_debug_part(p_part_id UUID,p_owner UUID,p_epoch BIGINT,p_object_sha256 TEXT,p_object_size BIGINT,p_object_version_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.debug_parts;
BEGIN
 SELECT * INTO p FROM agent.debug_parts WHERE id=p_part_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 PERFORM agent._assert_capture(p.run_id,p_owner,p_epoch);
 SELECT * INTO STRICT p FROM agent.debug_parts WHERE id=p_part_id FOR UPDATE;
 IF p.execution_epoch<>p_epoch OR p.execution_owner<>p_owner THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_EXECUTION'; END IF;
 IF p_object_sha256 IS DISTINCT FROM p.sha256 OR p_object_size IS DISTINCT FROM p.size_bytes OR
  (p_object_version_id IS NOT NULL AND (length(p_object_version_id) NOT BETWEEN 1 AND 1000 OR p_object_version_id~'[[:cntrl:]]')) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_OBJECT_MISMATCH'; END IF;
 IF p.state='attached' THEN
  IF p.object_version_id IS DISTINCT FROM p_object_version_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_OBJECT_MISMATCH'; END IF;
  PERFORM agent._assert_capture(p.run_id,p_owner,p_epoch); RETURN agent._debug_ref(p);
 END IF;
 IF p.state<>'reserved' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_EXPIRED'; END IF;
 INSERT INTO app.stored_objects(id,bucket,object_key,object_version_id,media_type,size_bytes,sha256,kind,state,expires_at)
 VALUES(p.object_id,p.bucket,p.object_key,p_object_version_id,'application/json',p.size_bytes,p.sha256,'debug','attached',p.expires_at);
 UPDATE agent.debug_parts SET object_version_id=p_object_version_id,attached_at=clock_timestamp(),state='attached'
  WHERE id=p.id RETURNING * INTO p;
 PERFORM agent._assert_capture(p.run_id,p_owner,p_epoch); RETURN agent._debug_ref(p);
END $$;
CREATE FUNCTION agent.mark_debug_part_unavailable(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_call_id UUID,p_part TEXT,p_reason TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM agent._assert_capture(p_run_id,p_owner,p_epoch);
 IF p_call_id IS NULL OR p_part IS NULL OR p_part NOT IN ('request','response') OR p_reason IS NULL OR
  p_reason NOT IN ('oversized','unavailable','credential_detected') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEBUG_ARGUMENT'; END IF;
 IF EXISTS(SELECT 1 FROM agent.debug_capture_failures WHERE run_id=p_run_id AND call_id=p_call_id AND execution_epoch=p_epoch AND part=p_part) THEN RETURN true; END IF;
 IF (SELECT count(*) FROM agent.debug_capture_failures WHERE run_id=p_run_id)>=60 THEN RETURN false; END IF;
 INSERT INTO agent.debug_capture_failures(run_id,call_id,execution_epoch,part,reason) VALUES(p_run_id,p_call_id,p_epoch,p_part,p_reason);
 PERFORM agent._assert_capture(p_run_id,p_owner,p_epoch); RETURN true;
END $$;
CREATE FUNCTION agent._debug_summary(p_run_id UUID) RETURNS JSONB LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.run_retention_policies; r agent.runs; part_n INTEGER; attached_n INTEGER; failed_n INTEGER; s TEXT;
BEGIN
 SELECT * INTO STRICT r FROM agent.runs WHERE id=p_run_id;
 SELECT * INTO STRICT p FROM agent.run_retention_policies WHERE run_id=p_run_id;
 SELECT count(*),count(*) FILTER(WHERE state='attached') INTO part_n,attached_n FROM agent.debug_parts WHERE run_id=p_run_id;
 SELECT count(*) INTO failed_n FROM agent.debug_capture_failures WHERE run_id=p_run_id;
 IF NOT p.capture_enabled THEN s:='disabled';
 ELSIF p.capture_expires_at<=clock_timestamp() THEN s:='expired';
 ELSIF agent._debug_revoked(p_run_id) THEN s:='unavailable';
 ELSIF attached_n=0 THEN s:=CASE WHEN failed_n>0 OR r.finished_at IS NOT NULL THEN 'unavailable' ELSE 'pending' END;
 ELSIF failed_n>0 OR attached_n<>part_n OR EXISTS(SELECT 1 FROM agent.debug_parts d WHERE d.run_id=p_run_id AND
   NOT EXISTS(SELECT 1 FROM agent.debug_parts pair WHERE pair.run_id=d.run_id AND pair.call_id=d.call_id AND
    pair.execution_epoch=d.execution_epoch AND pair.part<>d.part AND pair.state='attached')) THEN s:='partial';
 ELSE s:='available'; END IF;
 RETURN jsonb_build_object('enabled',p.capture_enabled,'policy_version',p.policy_version,'expires_at',p.capture_expires_at,
  'status',s,'part_count',part_n,'attached_count',attached_n,'unavailable_count',failed_n);
END $$;
CREATE FUNCTION agent.list_debug_parts(p_run_id UUID,p_principal_id TEXT) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE summary JSONB; items JSONB;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM agent.runs WHERE id=p_run_id AND principal_id=p_principal_id) THEN RETURN NULL; END IF;
 summary:=agent._debug_summary(p_run_id);
 SELECT coalesce(jsonb_agg(jsonb_build_object('part_id',id,'call_id',call_id,'execution_epoch',execution_epoch,
  'part',part,'role',role,'schema_attempt',schema_attempt,'created_at',created_at,'expires_at',expires_at,'state',state,
  'size_bytes',size_bytes,'payload_size_bytes',metadata->'payload_size_bytes','payload_sha256',payload_sha256,
  'download_url',CASE WHEN state='attached' AND summary->>'status' IN ('available','partial') THEN
    '/api/v1/runs/'||p_run_id::text||'/debug/captures/'||id::text END) ORDER BY created_at,id),'[]'::jsonb)
 INTO items FROM agent.debug_parts WHERE run_id=p_run_id;
 RETURN summary||jsonb_build_object('run_id',p_run_id,'items',items);
END $$;
CREATE FUNCTION agent.authorize_debug_part(p_run_id UUID,p_part_id UUID,p_principal_id TEXT) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.debug_parts; policy agent.run_retention_policies;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 IF NOT EXISTS(SELECT 1 FROM agent.runs WHERE id=p_run_id AND principal_id=p_principal_id) THEN RETURN NULL; END IF;
 SELECT * INTO p FROM agent.debug_parts WHERE run_id=p_run_id AND id=p_part_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF agent._debug_revoked(p_run_id) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 SELECT * INTO STRICT policy FROM agent.run_retention_policies WHERE run_id=p_run_id;
 IF NOT policy.capture_enabled THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_DISABLED'; END IF;
 IF policy.capture_expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEBUG_CAPTURE_EXPIRED'; END IF;
 IF p.state<>'attached' THEN RETURN NULL; END IF;
 RETURN agent._debug_ref(p);
END $$;
-- A debug object is removable only by this ledger; no source/recovery reference may point at it.
CREATE FUNCTION agent._debug_unreferenced(p agent.debug_parts) RETURNS BOOLEAN
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT NOT EXISTS(SELECT 1 FROM app.document_versions WHERE source_object_id=p.object_id)
  AND NOT EXISTS(SELECT 1 FROM knowledge.parse_generations WHERE artifact_object_id=p.object_id)
  AND NOT EXISTS(SELECT 1 FROM agent.run_step_artifacts WHERE output_reference=p.object_id)
  AND NOT EXISTS(SELECT 1 FROM app.stored_objects s WHERE (s.id=p.object_id OR (s.bucket=p.bucket AND s.object_key=p.object_key)) AND
   (s.id<>p.object_id OR s.bucket<>p.bucket OR s.object_key<>p.object_key OR s.object_version_id IS DISTINCT FROM p.object_version_id OR
    s.kind<>'debug' OR s.sha256<>p.sha256 OR s.size_bytes<>p.size_bytes))
$$;
CREATE FUNCTION agent.claim_debug_cleanup(p_owner UUID,p_limit INTEGER,p_lease_seconds INTEGER) RETURNS SETOF JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.debug_parts;
BEGIN
 IF p_owner IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 20 OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 30 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEBUG_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 FOR p IN SELECT * FROM agent.debug_parts WHERE
  (state IN ('reserved','attached') AND expires_at<=clock_timestamp()) OR
  (state='reserved' AND created_at<=clock_timestamp()-interval '1 hour') OR
  (state='cleanup_pending' AND claim_until<=clock_timestamp()) OR
  (state='cleaned' AND next_cleanup_check_at<=clock_timestamp())
  ORDER BY coalesce(next_cleanup_check_at,claim_until,expires_at),id LIMIT p_limit FOR UPDATE SKIP LOCKED LOOP
  IF NOT agent._debug_unreferenced(p) THEN CONTINUE; END IF;
  UPDATE agent.debug_parts SET state='cleanup_pending',claim_owner=p_owner,claim_token=gen_random_uuid(),
   claim_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),cleaned_at=NULL,next_cleanup_check_at=NULL
   WHERE id=p.id RETURNING * INTO p;
  UPDATE app.stored_objects SET state='purge_pending' WHERE id=p.object_id;
  RETURN NEXT agent._debug_ref(p)||jsonb_build_object('claim_token',p.claim_token,'claim_until',p.claim_until);
 END LOOP;
END $$;
CREATE FUNCTION agent.confirm_debug_deleted(p_part_id UUID,p_owner UUID,p_token UUID) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p agent.debug_parts;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO p FROM agent.debug_parts WHERE id=p_part_id FOR UPDATE;
 IF NOT FOUND OR p.state<>'cleanup_pending' OR p.claim_owner IS DISTINCT FROM p_owner OR p.claim_token IS DISTINCT FROM p_token OR
  p.claim_until<=clock_timestamp() OR NOT agent._debug_unreferenced(p) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_DEBUG_CLEANUP'; END IF;
 UPDATE app.stored_objects SET state='deleted' WHERE id=p.object_id;
 UPDATE agent.debug_parts SET state='cleaned',claim_owner=NULL,claim_token=NULL,claim_until=NULL,
  cleaned_at=clock_timestamp(),next_cleanup_check_at=clock_timestamp()+interval '1 hour' WHERE id=p.id;
 RETURN true;
END $$;
CREATE FUNCTION agent.get_run_debug_metadata(p_run_id UUID,p_principal_id TEXT) RETURNS JSONB
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; p agent.run_retention_policies; snapshot JSONB; calls JSONB;
BEGIN
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id AND principal_id=p_principal_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT * INTO STRICT p FROM agent.run_retention_policies WHERE run_id=p_run_id;
 SELECT jsonb_build_object('id',s.id,'captured_at',s.captured_at,'version_count',
  (SELECT count(*) FROM agent.kb_snapshot_items WHERE snapshot_id=s.id)) INTO snapshot FROM agent.kb_snapshots s WHERE s.id=r.snapshot_id;
 SELECT coalesce(jsonb_agg(coalesce(a.safe_metadata,agent._safe_model_metadata(a))||
  jsonb_build_object('payload_purged',a.payload_purged_at IS NOT NULL) ORDER BY a.created_at,a.id),'[]'::jsonb)
 INTO calls FROM agent.run_step_artifacts a WHERE a.run_id=p_run_id AND a.kind IN ('router','drafter','critic','repair');
 RETURN jsonb_build_object('run_id',r.id,'status',r.status,'duration_ms',
  floor(extract(epoch FROM coalesce(r.finished_at,clock_timestamp())-r.created_at)*1000)::bigint,'snapshot',snapshot,
  'configuration_fingerprint',r.configuration_fingerprint,'trace_id',r.trace_id,'model_calls',calls,
  'capture',agent._debug_summary(p_run_id),'private_payload_expires_at',p.private_payload_expires_at,'payloads_purged_at',p.payloads_purged_at);
END $$;

REVOKE ALL ON agent.run_retention_policies,agent.private_cleanup_bindings,agent.result_evidence,agent.debug_parts,agent.debug_capture_failures
 FROM PUBLIC,expert_runtime,expert_backend,expert_ingest,expert_outbox;
-- Explicitly close every new function, including renamed creation helper and trigger helpers.
DO $$ DECLARE f RECORD; BEGIN
 FOR f IN SELECT oid::regprocedure AS signature FROM pg_proc WHERE pronamespace='agent'::regnamespace AND proname=ANY(ARRAY[
  '_private_cleanup_authorized','_guard_retention_policy','register_run_retention','_create_run_before_retention','create_run',
  '_set_terminal_retention','get_run_capture_policy','_materialize_result_evidence','_capture_final_evidence','get_public_evidence',
  '_safe_model_metadata','_artifact_retention_guard','_evidence_retention_guard','expire_private_run_payloads','_guard_debug_part',
  '_debug_ref','_debug_revoked','_assert_capture','reserve_debug_part','attach_debug_part','mark_debug_part_unavailable',
  '_debug_summary','list_debug_parts','authorize_debug_part','_debug_unreferenced','claim_debug_cleanup','confirm_debug_deleted','get_run_debug_metadata']) LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,expert_runtime,expert_backend,expert_ingest,expert_outbox',f.signature);
 END LOOP;
END $$;
GRANT EXECUTE ON FUNCTION agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN),
 agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN,INTEGER),
 agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN,INTEGER,INTEGER,INTEGER),
 agent.get_public_evidence(UUID,TEXT,TEXT),agent.get_run_debug_metadata(UUID,TEXT),agent.list_debug_parts(UUID,TEXT),
 agent.authorize_debug_part(UUID,UUID,TEXT),agent.reserve_debug_part(UUID,UUID,BIGINT,UUID,TEXT,TEXT,INTEGER,TEXT,TEXT,TEXT,BIGINT,JSONB),
 agent.attach_debug_part(UUID,UUID,BIGINT,TEXT,BIGINT,TEXT),agent.mark_debug_part_unavailable(UUID,UUID,BIGINT,UUID,TEXT,TEXT),
 agent.claim_debug_cleanup(UUID,INTEGER,INTEGER),agent.confirm_debug_deleted(UUID,UUID,UUID),agent.expire_private_run_payloads(INTEGER)
 TO expert_backend;
GRANT EXECUTE ON FUNCTION agent.get_run_capture_policy(UUID,UUID,BIGINT) TO expert_runtime;
