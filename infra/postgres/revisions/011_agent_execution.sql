-- P08: durable execution. Network/model/source reads are outside these short
-- transactions. SQL verifies identity, shape and policy, not semantic entailment.
ALTER TABLE agent.runs
 ADD COLUMN max_recovery_attempts INTEGER NOT NULL DEFAULT 2 CHECK(max_recovery_attempts BETWEEN 0 AND 10),
 ADD COLUMN recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK(recovery_attempts BETWEEN 0 AND max_recovery_attempts),
 ADD COLUMN current_stage_attempt INTEGER NOT NULL DEFAULT 1 CHECK(current_stage_attempt BETWEEN 1 AND 32),
 ADD COLUMN terminal_operation_id UUID,
 ADD COLUMN terminal_request_hash app.sha256,
 ADD COLUMN terminal_error JSONB;
ALTER TABLE agent.run_step_artifacts
 ADD COLUMN snapshot_id UUID REFERENCES agent.kb_snapshots ON DELETE RESTRICT,
 ADD COLUMN identity JSONB,
 ADD COLUMN identity_hash app.sha256,
 ADD COLUMN schema_retry_role TEXT CHECK(schema_retry_role IN ('router','drafter','critic','repair')),
 ADD COLUMN kind TEXT CHECK(kind IN ('router','drafter','critic','repair','retrieval','precheck')),
 ADD CONSTRAINT artifact_run_identity UNIQUE(run_id,id);
DO $$ DECLARE c TEXT; BEGIN
 SELECT conname INTO STRICT c FROM pg_constraint WHERE conrelid='agent.run_step_artifacts'::regclass AND
  pg_get_constraintdef(oid)='UNIQUE (run_id, execution_epoch, logical_step_key, input_hash)';
 EXECUTE format('ALTER TABLE agent.run_step_artifacts DROP CONSTRAINT %I',c);
END $$;
CREATE UNIQUE INDEX artifact_reuse_identity ON agent.run_step_artifacts(run_id,logical_step_key,identity_hash);
CREATE UNIQUE INDEX artifact_one_schema_retry ON agent.run_step_artifacts(run_id,schema_retry_role);
ALTER TABLE agent.evidence_packs ADD COLUMN pack JSONB, ADD COLUMN canonical_manifest TEXT,
 ADD CONSTRAINT evidence_one_per_run UNIQUE(run_id), ADD CONSTRAINT evidence_run_identity UNIQUE(run_id,id);
ALTER TABLE agent.run_results ADD COLUMN public_result JSONB,
 ADD COLUMN draft_artifact_id UUID, ADD COLUMN critic_artifact_id UUID,
 ADD COLUMN decision_artifact_id UUID, ADD COLUMN evidence_pack_id UUID,
 ADD FOREIGN KEY(run_id,draft_artifact_id) REFERENCES agent.run_step_artifacts(run_id,id),
 ADD FOREIGN KEY(run_id,critic_artifact_id) REFERENCES agent.run_step_artifacts(run_id,id),
 ADD FOREIGN KEY(run_id,decision_artifact_id) REFERENCES agent.run_step_artifacts(run_id,id),
 ADD FOREIGN KEY(run_id,evidence_pack_id) REFERENCES agent.evidence_packs(run_id,id);
CREATE TABLE agent.run_operations (
 run_id UUID NOT NULL REFERENCES agent.runs, operation_id UUID NOT NULL, kind TEXT NOT NULL,
 request JSONB NOT NULL, event_id UUID NOT NULL REFERENCES agent.run_events,
 PRIMARY KEY(run_id,operation_id)
);
CREATE TABLE agent.run_attempt_budgets (
 run_id UUID NOT NULL REFERENCES agent.runs, kind TEXT NOT NULL CHECK(kind IN ('schema','repair')),
 role TEXT NOT NULL CHECK(role IN ('router','drafter','critic','repair')),
 operation_id UUID NOT NULL, artifact_id UUID NOT NULL, execution_epoch BIGINT NOT NULL CHECK(execution_epoch>0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(run_id,kind,role),
 UNIQUE(run_id,operation_id), FOREIGN KEY(run_id,artifact_id) REFERENCES agent.run_step_artifacts(run_id,id),
 CHECK(kind<>'repair' OR role='repair')
);
CREATE TRIGGER run_operations_immutable BEFORE UPDATE OR DELETE ON agent.run_operations FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE TRIGGER run_budgets_immutable BEFORE UPDATE OR DELETE ON agent.run_attempt_budgets FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE FUNCTION agent._execution_identity_guard() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.max_recovery_attempts<>OLD.max_recovery_attempts OR NEW.recovery_attempts<OLD.recovery_attempts OR NEW.repair_attempts<OLD.repair_attempts THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RUN_IDENTITY_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER execution_identity_guard BEFORE UPDATE ON agent.runs FOR EACH ROW EXECUTE FUNCTION agent._execution_identity_guard();

CREATE FUNCTION agent._closed(v JSONB,keys TEXT[]) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(v)='object' AND v ?& keys AND v-keys='{}'::jsonb,false)
$$;
CREATE FUNCTION agent._text(v JSONB,maximum INTEGER) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(v)='string' AND btrim(v#>>'{}')<>'' AND length(v#>>'{}')<=maximum,false)
$$;
CREATE FUNCTION agent._strings(v JSONB,maximum INTEGER,item_length INTEGER,distinct_items BOOLEAN DEFAULT true) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 RETURN jsonb_array_length(v)<=maximum AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v) x WHERE NOT agent._text(x,item_length)) AND
  (NOT distinct_items OR jsonb_array_length(v)=(SELECT count(DISTINCT x) FROM jsonb_array_elements(v) x));
END $$;
CREATE FUNCTION agent._sha(v JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(v)='string' AND v#>>'{}'~'^[0-9a-f]{64}$',false)
$$;
CREATE FUNCTION agent._uuid(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'string' THEN RETURN false; END IF;
 PERFORM (v#>>'{}')::uuid; RETURN true;
EXCEPTION WHEN invalid_text_representation THEN RETURN false;
END $$;
CREATE FUNCTION agent._error_code(v TEXT) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(v IN ('DEPENDENCY_UNAVAILABLE','DEADLINE_EXCEEDED','INTERNAL_ERROR','MODEL_UNAVAILABLE','MODEL_TIMEOUT','CAPACITY_EXCEEDED',
  'OUTPUT_SCHEMA_INVALID','TOKEN_LIMIT_EXCEEDED','SOURCE_REVOKED','SOURCE_UNAVAILABLE','GENERATION_INVALID'),false)
$$;
CREATE FUNCTION agent._retry_delay(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'number' THEN RETURN false; END IF;
 RETURN (v#>>'{}')::numeric BETWEEN 0 AND 300;
END $$;
CREATE FUNCTION agent._error_info(p_code TEXT,p_id UUID) RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('code',p_code,'message',CASE p_code
  WHEN 'DEADLINE_EXCEEDED' THEN 'Превышено время выполнения запроса.'
  WHEN 'SOURCE_REVOKED' THEN 'Доступ к источнику отозван.'
  WHEN 'OUTPUT_SCHEMA_INVALID' THEN 'Ответ модели не соответствует обязательной схеме.'
  ELSE 'Не удалось завершить обработку запроса.' END,'retryable',false,'request_id',p_id,'details','{}'::jsonb)
$$;
-- Used by recovery/revoke too. Event is appended before marking the immutable
-- terminal row, so its authoritative sequence remains in that row.
CREATE FUNCTION agent._end_without_result(p_run UUID,p_outcome TEXT,p_code TEXT) RETURNS agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; err JSONB;
BEGIN
 IF p_outcome NOT IN ('failed','cancelled') OR (p_outcome='failed' AND NOT agent._error_code(p_code)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TERMINAL'; END IF;
 err:=CASE WHEN p_outcome='failed' THEN agent._error_info(p_code,p_run) END;
 PERFORM agent._append_command_event(p_run,'run.'||p_outcome,CASE WHEN err IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('error',err) END);
 UPDATE agent.runs SET status=p_outcome,finished_at=clock_timestamp(),lease_until=NULL,
  cancel_requested_at=CASE WHEN p_outcome='cancelled' THEN coalesce(cancel_requested_at,clock_timestamp()) ELSE cancel_requested_at END,
  error_code=p_code,terminal_error=err WHERE id=p_run RETURNING * INTO r;
 RETURN r;
END $$;

CREATE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN,p_max_recovery_attempts INTEGER)
RETURNS agent.runs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; inserted UUID;
BEGIN
 IF p_run_id IS NULL OR p_principal_id IS NULL OR btrim(p_principal_id)='' OR p_question IS NULL OR length(p_question) NOT BETWEEN 1 AND 4000 OR
  btrim(p_question)='' OR p_idempotency_key IS NULL OR btrim(p_idempotency_key)='' OR p_request_hash IS NULL OR p_deadline_at IS NULL OR
  p_configuration_fingerprint IS NULL OR btrim(p_configuration_fingerprint)='' OR p_debug_capture IS NULL OR
  p_max_recovery_attempts IS NULL OR p_max_recovery_attempts NOT BETWEEN 0 AND 10 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RUN_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE principal_id=p_principal_id AND idempotency_key=p_idempotency_key FOR UPDATE;
 IF FOUND THEN
  IF r.request_hash<>p_request_hash OR r.question<>p_question OR r.debug_capture<>p_debug_capture THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN r;
 END IF;
 IF p_deadline_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEADLINE_EXCEEDED'; END IF;
 INSERT INTO agent.runs(id,principal_id,question,idempotency_key,request_hash,deadline_at,configuration_fingerprint,debug_capture,max_recovery_attempts)
 VALUES(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,p_configuration_fingerprint,p_debug_capture,p_max_recovery_attempts)
 ON CONFLICT(principal_id,idempotency_key) DO NOTHING RETURNING id INTO inserted;
 SELECT * INTO STRICT r FROM agent.runs WHERE principal_id=p_principal_id AND idempotency_key=p_idempotency_key FOR UPDATE;
 IF r.request_hash<>p_request_hash OR r.question<>p_question OR r.debug_capture<>p_debug_capture THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
 IF inserted IS NOT NULL THEN PERFORM agent._append_command_event(r.id,'run.created','{}'); END IF;
 SELECT * INTO STRICT r FROM agent.runs WHERE id=r.id; RETURN r;
END $$;
CREATE OR REPLACE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN)
RETURNS agent.runs LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT agent.create_run(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,p_configuration_fingerprint,p_debug_capture,2)
$$;
CREATE OR REPLACE FUNCTION agent.acquire_run(p_run_id UUID,p_owner UUID,p_lease_seconds INTEGER)
RETURNS TABLE(outcome TEXT,run_id UUID,execution_epoch BIGINT,lease_until TIMESTAMPTZ,status TEXT)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; ts TIMESTAMPTZ;
BEGIN
 IF p_owner IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_LEASE'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 ts:=clock_timestamp();
 IF r.status IN ('completed','refused','failed','cancelled') THEN outcome:='already_terminal';
 ELSIF r.cancel_requested_at IS NOT NULL THEN
  IF r.lease_until IS NULL OR r.lease_until<=ts THEN r:=agent._end_without_result(r.id,'cancelled',NULL); END IF;
  outcome:='cancel_requested';
 ELSIF r.deadline_at<=ts THEN r:=agent._end_without_result(r.id,'failed','DEADLINE_EXCEEDED'); outcome:='deadline_exceeded';
 ELSIF r.lease_until>ts THEN outcome:='already_running';
 ELSIF EXISTS(SELECT 1 FROM agent.kb_snapshot_items i JOIN app.logical_documents d ON d.id=i.logical_document_id
   WHERE i.snapshot_id=r.snapshot_id AND d.security_revoked_at IS NOT NULL) THEN
  r:=agent._end_without_result(r.id,'failed','SOURCE_REVOKED'); outcome:='already_terminal';
 ELSIF r.execution_epoch>0 AND r.recovery_attempts>=r.max_recovery_attempts THEN
  r:=agent._end_without_result(r.id,'failed','INTERNAL_ERROR'); outcome:='recovery_exhausted';
 ELSE
  UPDATE agent.runs SET status='running',execution_owner=p_owner,execution_epoch=r.execution_epoch+1,
   recovery_attempts=r.recovery_attempts+CASE WHEN r.execution_epoch>0 THEN 1 ELSE 0 END,
   lease_until=least(r.deadline_at,ts+make_interval(secs=>p_lease_seconds)),heartbeat_at=ts,started_at=coalesce(r.started_at,ts) WHERE id=p_run_id;
  PERFORM agent._append_command_event(p_run_id,CASE WHEN r.execution_epoch=0 THEN 'run.started' ELSE 'run.resuming' END,'{}');
  SELECT * INTO STRICT r FROM agent.runs WHERE id=p_run_id; outcome:='acquired';
 END IF;
 run_id:=r.id; execution_epoch:=r.execution_epoch; lease_until:=r.lease_until; status:=r.status; RETURN NEXT;
END $$;
CREATE FUNCTION agent.recover_runs(p_limit INTEGER) RETURNS SETOF agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RECOVERY_LIMIT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 -- A departed backend cannot use its old transaction binding. Never remove
 -- bindings belonging to a currently visible backend or its active transaction.
 DELETE FROM agent.checkpoint_write_bindings WHERE transaction_id IN (
  SELECT b.transaction_id FROM agent.checkpoint_write_bindings b WHERE NOT EXISTS(
   SELECT 1 FROM pg_stat_activity a WHERE a.pid=b.backend_pid) ORDER BY b.created_at LIMIT 100);
 FOR r IN SELECT * FROM agent.runs WHERE status IN ('created','running','cancelling') AND
  (lease_until IS NULL OR lease_until<=clock_timestamp() OR (cancel_requested_at IS NULL AND deadline_at<=clock_timestamp()))
  ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT p_limit LOOP
  IF r.cancel_requested_at IS NOT NULL THEN r:=agent._end_without_result(r.id,'cancelled',NULL);
  ELSIF r.deadline_at<=clock_timestamp() THEN r:=agent._end_without_result(r.id,'failed','DEADLINE_EXCEEDED');
  ELSIF EXISTS(SELECT 1 FROM agent.kb_snapshot_items i JOIN app.logical_documents d ON d.id=i.logical_document_id
   WHERE i.snapshot_id=r.snapshot_id AND d.security_revoked_at IS NOT NULL) THEN r:=agent._end_without_result(r.id,'failed','SOURCE_REVOKED');
  ELSIF r.execution_epoch>0 AND r.recovery_attempts>=r.max_recovery_attempts THEN r:=agent._end_without_result(r.id,'failed','INTERNAL_ERROR'); END IF;
  RETURN NEXT r;
 END LOOP;
END $$;

CREATE FUNCTION agent.record_stage_event(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,
 p_stage TEXT,p_event_type TEXT,p_attempt INTEGER,p_data JSONB) RETURNS agent.run_events
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; e agent.run_events; o agent.run_operations; request JSONB; payload JSONB; started TIMESTAMPTZ;
 seq BIGINT; eid UUID:=gen_random_uuid();
BEGIN
 IF p_operation_id IS NULL OR p_stage IS NULL OR p_stage NOT IN ('snapshotting','routing','retrieving','reranking','building_context',
  'drafting','checking_citations','validating','repairing','revalidating','rendering','finalizing') OR p_event_type IS NULL OR
  p_event_type NOT IN ('stage.started','stage.completed','stage.retry_scheduled') OR p_attempt IS NULL OR p_attempt NOT BETWEEN 1 AND 32 OR
  p_data IS NULL OR octet_length(p_data::text)>4096 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STAGE_EVENT'; END IF;
 payload:=p_data;
 IF p_event_type='stage.started' THEN
  IF NOT agent._closed(p_data,ARRAY['message_code']) OR (p_data->'message_code'<>'null'::jsonb AND
   p_data->>'message_code' IS DISTINCT FROM 'RUN_'||upper(p_stage)) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STAGE_EVENT'; END IF;
 ELSIF p_event_type='stage.completed' THEN
  IF NOT agent._closed(p_data,ARRAY['duration_ms','candidate_count','evidence_count','claim_count']) OR
   NOT knowledge._parse_integer(p_data->'duration_ms',0,86400000) OR
   (p_data->'candidate_count'<>'null'::jsonb AND NOT knowledge._parse_integer(p_data->'candidate_count',0,100000)) OR
   (p_data->'evidence_count'<>'null'::jsonb AND NOT knowledge._parse_integer(p_data->'evidence_count',0,72)) OR
   (p_data->'claim_count'<>'null'::jsonb AND NOT knowledge._parse_integer(p_data->'claim_count',0,12)) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STAGE_EVENT'; END IF;
  -- Caller duration is shape-checked, but is neither authority nor replay identity.
  payload:=p_data-'duration_ms';
 ELSE
  IF NOT agent._closed(p_data,ARRAY['error_code','retry_after_seconds']) OR
   NOT agent._error_code(p_data->>'error_code') OR NOT agent._retry_delay(p_data->'retry_after_seconds') THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STAGE_EVENT'; END IF;
 END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 request:=jsonb_build_object('stage',p_stage,'event_type',p_event_type,'attempt',p_attempt,'data',payload);
 SELECT * INTO o FROM agent.run_operations WHERE run_id=p_run_id AND operation_id=p_operation_id;
 IF FOUND THEN
  IF o.kind<>'stage' OR o.request<>request THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  SELECT * INTO STRICT e FROM agent.run_events WHERE event_id=o.event_id; RETURN e;
 END IF;
 IF (SELECT count(*) FROM agent.run_operations WHERE run_id=p_run_id)>=500 THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CAPACITY_EXCEEDED'; END IF;
 SELECT * INTO e FROM agent.run_events WHERE run_id=p_run_id AND stage=p_stage AND attempt=p_attempt AND event_type=p_event_type;
 IF FOUND THEN
  IF (CASE WHEN p_event_type='stage.completed' THEN e.public_payload-'duration_ms' ELSE e.public_payload END)<>payload THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  INSERT INTO agent.run_operations VALUES(p_run_id,p_operation_id,'stage',request,e.event_id); RETURN e;
 END IF;
 IF (SELECT count(*) FROM agent.run_events WHERE run_id=p_run_id AND stage IS NOT NULL)>=500 THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CAPACITY_EXCEEDED'; END IF;
 IF p_event_type<>'stage.started' THEN
  SELECT created_at INTO started FROM agent.run_events WHERE run_id=p_run_id AND stage=p_stage AND attempt=p_attempt AND event_type='stage.started';
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STAGE_NOT_STARTED'; END IF;
  IF p_event_type='stage.completed' THEN payload:=payload||jsonb_build_object('duration_ms',greatest(0,floor(extract(epoch FROM clock_timestamp()-started)*1000)::bigint)); END IF;
 END IF;
 UPDATE agent.runs SET current_stage=p_stage,current_stage_attempt=p_attempt,last_event_sequence=last_event_sequence+1
  WHERE id=p_run_id RETURNING last_event_sequence INTO seq;
 INSERT INTO agent.run_events(event_id,run_id,sequence,event_type,stage,attempt,execution_epoch,public_payload)
 VALUES(eid,p_run_id,seq,p_event_type,p_stage,p_attempt,p_epoch,payload) RETURNING * INTO e;
 INSERT INTO app.outbox_events(event_id,aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES(eid,'run',p_run_id,p_event_type,'run.events',jsonb_build_object('run_id',p_run_id,'sequence',seq,'event_id',eid,'schema_version',1));
 INSERT INTO agent.run_operations VALUES(p_run_id,p_operation_id,'stage',request,eid);
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch); RETURN e;
END $$;

CREATE FUNCTION agent.save_evidence_pack(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_pack JSONB,p_canonical_manifest TEXT)
RETURNS agent.evidence_packs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; ep agent.evidence_packs; m JSONB; u JSONB; b JSONB; n knowledge.document_nodes; v app.document_versions;
 item agent.kb_snapshot_items; g knowledge.parse_generations; digest TEXT; span JSONB;
BEGIN
 IF p_canonical_manifest IS NULL OR octet_length(p_canonical_manifest)>8388608 OR p_pack IS NULL OR octet_length(p_pack::text)>8388608 OR
  NOT agent._closed(p_pack,ARRAY['pack_id','run_id','snapshot_id','units','llm_token_count','manifest_hash']) OR
  NOT agent._uuid(p_pack->'pack_id') OR NOT agent._uuid(p_pack->'run_id') OR NOT agent._uuid(p_pack->'snapshot_id') OR
  NOT agent._sha(p_pack->'manifest_hash') OR NOT knowledge._parse_integer(p_pack->'llm_token_count',0,100000) OR
  jsonb_typeof(p_pack->'units') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_EVIDENCE_PACK'; END IF;
 m:=p_canonical_manifest::jsonb; digest:=encode(sha256(convert_to(p_canonical_manifest,'UTF8')),'hex');
 IF NOT agent._closed(m,ARRAY['run_id','snapshot_id','units','bindings']) OR m->'units'<>p_pack->'units' OR
  m->'run_id'<>p_pack->'run_id' OR m->'snapshot_id'<>p_pack->'snapshot_id' OR jsonb_typeof(m->'bindings') IS DISTINCT FROM 'array' OR
  digest<>p_pack->>'manifest_hash' OR (p_pack->>'run_id')::uuid<>p_run_id OR jsonb_array_length(p_pack->'units')>72 OR
  jsonb_array_length(m->'bindings')<>jsonb_array_length(p_pack->'units') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_EVIDENCE_PACK'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 IF r.snapshot_id IS NULL OR r.snapshot_id<>(p_pack->>'snapshot_id')::uuid THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SNAPSHOT_MISMATCH'; END IF;
 SELECT * INTO ep FROM agent.evidence_packs WHERE run_id=p_run_id;
 IF FOUND THEN
  IF ep.pack IS DISTINCT FROM p_pack OR ep.canonical_manifest IS DISTINCT FROM p_canonical_manifest THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN ep;
 END IF;
 IF (SELECT count(DISTINCT x->>'evidence_id') FROM jsonb_array_elements(p_pack->'units') x)<>jsonb_array_length(p_pack->'units') OR
  (SELECT count(DISTINCT x->>'evidence_id') FROM jsonb_array_elements(m->'bindings') x)<>jsonb_array_length(m->'bindings') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='DUPLICATE_EVIDENCE'; END IF;
 FOR u IN SELECT value FROM jsonb_array_elements(p_pack->'units') LOOP
  IF NOT agent._closed(u,ARRAY['evidence_id','document_version_id','index_generation_id','canonical_node_id','source_chunk_ids',
   'document_title','structural_path','excerpt','source_spans','content_hash']) OR NOT agent._text(u->'evidence_id',64) OR
   NOT agent._uuid(u->'document_version_id') OR NOT agent._uuid(u->'index_generation_id') OR NOT agent._uuid(u->'canonical_node_id') OR
   NOT agent._strings(u->'source_chunk_ids',100,36) OR NOT agent._text(u->'document_title',500) OR
   NOT agent._strings(u->'structural_path',32,8388608,false) OR NOT agent._text(u->'excerpt',30000) OR NOT agent._sha(u->'content_hash') OR
   NOT knowledge._parse_spans(u->'source_spans',500) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_EVIDENCE_UNIT'; END IF;
  IF jsonb_array_length(u->'source_chunk_ids')=0 OR jsonb_array_length(u->'source_spans')=0 OR
   u->>'content_hash'<>encode(sha256(convert_to(u->>'excerpt','UTF8')),'hex') OR
   (SELECT count(DISTINCT jsonb_build_array(x->'pdf_page',x->'block_id',x->'start_offset',x->'end_offset')) FROM jsonb_array_elements(u->'source_spans') x)
    <>jsonb_array_length(u->'source_spans') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_EVIDENCE_UNIT'; END IF;
  SELECT * INTO item FROM agent.kb_snapshot_items WHERE snapshot_id=r.snapshot_id AND document_version_id=(u->>'document_version_id')::uuid
   AND index_generation_id=(u->>'index_generation_id')::uuid;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_OUTSIDE_SNAPSHOT'; END IF;
  SELECT * INTO n FROM knowledge.document_nodes WHERE id=(u->>'canonical_node_id')::uuid AND parse_generation_id=item.parse_generation_id;
  IF NOT FOUND OR n.structural_path<>u->'structural_path' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_NODE_MISMATCH'; END IF;
  SELECT * INTO STRICT v FROM app.document_versions WHERE id=item.document_version_id;
  SELECT * INTO STRICT g FROM knowledge.parse_generations WHERE id=item.parse_generation_id;
  IF v.source_title<>u->>'document_title' OR EXISTS(SELECT 1 FROM jsonb_array_elements(u->'source_chunk_ids') x WHERE
    NOT agent._uuid(x) OR NOT EXISTS(SELECT 1 FROM knowledge.chunks c WHERE c.id=(x#>>'{}')::uuid AND c.index_generation_id=item.index_generation_id)) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_SOURCE_MISMATCH'; END IF;
  SELECT x INTO b FROM jsonb_array_elements(m->'bindings') x WHERE x->>'evidence_id'=u->>'evidence_id';
  IF NOT agent._closed(b,ARRAY['evidence_id','parse_generation_id','artifact_object_id','artifact_sha256','relations','owner_ranges','source_chunk_semantics']) OR
   b->>'parse_generation_id'<>item.parse_generation_id::text OR b->>'artifact_object_id'<>g.artifact_object_id::text OR
   NOT agent._sha(b->'artifact_sha256') OR NOT agent._strings(b->'relations',10,30) OR jsonb_typeof(b->'owner_ranges') IS DISTINCT FROM 'array' OR
   b->>'source_chunk_semantics' IS DISTINCT FROM 'origin_hit_lineage; ownership is canonical_node and exact mapped ranges' OR
   NOT EXISTS(SELECT 1 FROM app.stored_objects s WHERE s.id=g.artifact_object_id AND s.sha256=b->>'artifact_sha256' AND s.state='attached' AND s.kind='parse_artifact') THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_ARTIFACT_MISMATCH'; END IF;
  IF jsonb_array_length(b->'owner_ranges')>1000 OR jsonb_array_length(b->'relations')=0 OR EXISTS(
   SELECT 1 FROM jsonb_array_elements_text(b->'relations') x WHERE x NOT IN ('hit','scope','header','unit','note','caption','table','template')) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_EVIDENCE_BINDING'; END IF;
  -- Exact raw text/normalized-group checks use the SHA-verified artifact in
  -- Python. A JSONB FK cannot prove a glyph or an omitted source condition.
  FOR span IN SELECT value FROM jsonb_array_elements(u->'source_spans') LOOP
   IF (span->>'pdf_page')::int<n.page_start OR (span->>'pdf_page')::int>n.page_end THEN
    RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_SOURCE_MISMATCH'; END IF;
  END LOOP;
 END LOOP;
 INSERT INTO agent.evidence_packs(id,run_id,snapshot_id,manifest,content_hash,pack,canonical_manifest)
 VALUES((p_pack->>'pack_id')::uuid,p_run_id,r.snapshot_id,m,digest,p_pack,p_canonical_manifest) RETURNING * INTO ep;
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch); RETURN ep;
END $$;

CREATE FUNCTION agent._draft(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE c JSONB;
BEGIN
 IF NOT agent._closed(v,ARRAY['disposition','claims','limitation']) OR v->>'disposition' NOT IN ('answer','insufficient_evidence') OR
  jsonb_typeof(v->'disposition') IS DISTINCT FROM 'string' OR jsonb_typeof(v->'claims') IS DISTINCT FROM 'array' OR
  (v->'limitation'<>'null'::jsonb AND NOT agent._text(v->'limitation',500)) THEN RETURN false; END IF;
 IF jsonb_array_length(v->'claims')>12 OR (v->>'disposition'='answer')<>(jsonb_array_length(v->'claims')>0) OR
  (SELECT count(DISTINCT x->>'claim_id') FROM jsonb_array_elements(v->'claims') x)<>jsonb_array_length(v->'claims') THEN RETURN false; END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(v->'claims') LOOP
  IF NOT agent._closed(c,ARRAY['claim_id','text','evidence_ids']) OR NOT agent._text(c->'claim_id',64) OR NOT agent._text(c->'text',1800) OR
   NOT agent._strings(c->'evidence_ids',6,64) THEN RETURN false; END IF;
  IF jsonb_array_length(c->'evidence_ids')=0 THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION agent._draft_bound(d JSONB,p JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT NOT EXISTS(SELECT 1 FROM jsonb_array_elements(d->'claims') c CROSS JOIN LATERAL jsonb_array_elements_text(c->'evidence_ids') eid
  WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'units') u WHERE u->>'evidence_id'=eid))
$$;
CREATE FUNCTION agent._draft_publishable(d JSONB,p JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT d->>'disposition'='answer' AND jsonb_array_length(d->'claims')>0 AND agent._draft_bound(d,p)
$$;
CREATE FUNCTION agent._critic(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE c JSONB;
BEGIN
 IF NOT agent._closed(v,ARRAY['claim_verdicts','question_match','missing_answer_parts','global_issues']) OR
  jsonb_typeof(v->'question_match') IS DISTINCT FROM 'string' OR v->>'question_match' NOT IN ('yes','partial','no') OR
  jsonb_typeof(v->'claim_verdicts') IS DISTINCT FROM 'array' OR NOT agent._strings(v->'missing_answer_parts',12,1000,false) OR
  NOT agent._strings(v->'global_issues',12,1000,false) THEN RETURN false; END IF;
 IF jsonb_array_length(v->'claim_verdicts')>12 OR
  (SELECT count(DISTINCT x->>'claim_id') FROM jsonb_array_elements(v->'claim_verdicts') x)<>jsonb_array_length(v->'claim_verdicts') THEN RETURN false; END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(v->'claim_verdicts') LOOP
  IF NOT agent._closed(c,ARRAY['claim_id','verdict','reason_code','explanation','evidence_ids','issue_type']) OR
   NOT agent._text(c->'claim_id',64) OR NOT agent._text(c->'explanation',1000) OR NOT agent._strings(c->'evidence_ids',6,64) OR
   NOT agent._text(c->'verdict',30) OR c->>'verdict' NOT IN ('supported','partially_supported','unsupported','contradicted') OR
   NOT agent._text(c->'reason_code',30) OR c->>'reason_code' NOT IN ('SUPPORTED','MISSING_CONDITION','INCOMPLETE_SUPPORT','WRONG_VALUE','WRONG_SCOPE',
    'IRRELEVANT_EVIDENCE','CITATION_MISMATCH','UNSUPPORTED','CONTRADICTED','OTHER') OR
   NOT agent._text(c->'issue_type',30) OR c->>'issue_type' NOT IN ('none','missing_condition','incomplete_support','wrong_value','wrong_scope',
    'irrelevant_evidence','citation_mismatch','other') THEN RETURN false; END IF;
  IF (c->>'verdict'='supported')<>(c->>'issue_type'='none') OR (c->>'verdict'='supported')<>(c->>'reason_code'='SUPPORTED') OR
   (c->>'verdict' IN ('supported','partially_supported') AND jsonb_array_length(c->'evidence_ids')=0) THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION agent._critic_bound(c JSONB,d JSONB,p JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT agent._draft_publishable(d,p) AND
  (SELECT coalesce(jsonb_agg(x->'claim_id' ORDER BY x->>'claim_id'),'[]') FROM jsonb_array_elements(c->'claim_verdicts') x)=
  (SELECT coalesce(jsonb_agg(x->'claim_id' ORDER BY x->>'claim_id'),'[]') FROM jsonb_array_elements(d->'claims') x) AND
  NOT EXISTS(SELECT 1 FROM jsonb_array_elements(c->'claim_verdicts') v CROSS JOIN LATERAL jsonb_array_elements_text(v->'evidence_ids') eid
   WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'units') u WHERE u->>'evidence_id'=eid) OR
   (v->>'verdict' IN ('supported','partially_supported') AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(d->'claims') claim
     WHERE claim->>'claim_id'=v->>'claim_id' AND claim->'evidence_ids' ? eid)))
$$;
CREATE FUNCTION agent._policy(c JSONB) RETURNS TEXT LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(c->'claim_verdicts') v WHERE v->>'verdict' IN ('unsupported','contradicted')) OR
  c->>'question_match'='no' OR jsonb_array_length(c->'global_issues')>0 OR EXISTS(
   SELECT 1 FROM jsonb_array_elements(c->'claim_verdicts') v WHERE v->>'verdict'='partially_supported' AND
    (v->>'issue_type' NOT IN ('missing_condition','incomplete_support') OR v->>'reason_code' NOT IN ('MISSING_CONDITION','INCOMPLETE_SUPPORT'))) THEN RETURN 'refuse'; END IF;
 IF c->>'question_match'='partial' OR jsonb_array_length(c->'missing_answer_parts')>0 OR EXISTS(
  SELECT 1 FROM jsonb_array_elements(c->'claim_verdicts') v WHERE v->>'verdict'='partially_supported') THEN RETURN 'repair'; END IF;
 RETURN 'render';
END $$;

CREATE FUNCTION agent._check_step_identity(r agent.runs,v JSONB) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE x JSONB; a agent.run_step_artifacts;
BEGIN
 IF v IS NULL OR octet_length(v::text)>16384 OR NOT agent._closed(v,ARRAY['input_sha256','snapshot_id','configuration_fingerprint',
  'model_revision','profile_sha256','prompt_version','prompt_sha256','schema_sha256','evidence_pack_id','parent_artifact_ids']) OR
  NOT agent._sha(v->'input_sha256') OR NOT agent._uuid(v->'snapshot_id') OR NOT agent._text(v->'configuration_fingerprint',1000) OR
  NOT agent._strings(v->'parent_artifact_ids',8,36) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_IDENTITY'; END IF;
 IF r.snapshot_id IS NULL OR (v->>'snapshot_id')::uuid<>r.snapshot_id OR v->>'configuration_fingerprint'<>r.configuration_fingerprint THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STEP_IDENTITY_MISMATCH'; END IF;
 FOREACH x IN ARRAY ARRAY[v->'profile_sha256',v->'prompt_sha256',v->'schema_sha256'] LOOP
  IF x<>'null'::jsonb AND NOT agent._sha(x) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_IDENTITY'; END IF;
 END LOOP;
 IF (v->'model_revision'<>'null'::jsonb AND NOT agent._text(v->'model_revision',200)) OR
  (v->'prompt_version'<>'null'::jsonb AND NOT agent._text(v->'prompt_version',200)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_IDENTITY'; END IF;
 IF v->'evidence_pack_id'<>'null'::jsonb AND (NOT agent._uuid(v->'evidence_pack_id') OR NOT EXISTS(
  SELECT 1 FROM agent.evidence_packs WHERE run_id=r.id AND id=(v->>'evidence_pack_id')::uuid AND snapshot_id=r.snapshot_id)) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_PACK_MISMATCH'; END IF;
 FOR x IN SELECT value FROM jsonb_array_elements(v->'parent_artifact_ids') LOOP
  IF NOT agent._uuid(x) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_IDENTITY'; END IF;
  SELECT * INTO a FROM agent.run_step_artifacts WHERE run_id=r.id AND id=(x#>>'{}')::uuid;
  IF NOT FOUND OR a.snapshot_id IS DISTINCT FROM r.snapshot_id OR a.identity->>'configuration_fingerprint' IS DISTINCT FROM r.configuration_fingerprint THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_PARENT_MISMATCH'; END IF;
 END LOOP;
END $$;
CREATE FUNCTION agent.find_step_artifact(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_logical_step_key TEXT,p_identity JSONB)
RETURNS SETOF agent.run_step_artifacts LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch); PERFORM agent._check_step_identity(r,p_identity);
 RETURN QUERY SELECT * FROM agent.run_step_artifacts WHERE run_id=p_run_id AND logical_step_key=p_logical_step_key AND identity=p_identity;
END $$;

CREATE FUNCTION agent.save_step_artifact(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_artifact_id UUID,p_logical_step_key TEXT,
 p_identity JSONB,p_kind TEXT,p_status TEXT,p_private_json JSONB) RETURNS agent.run_step_artifacts
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; a agent.run_step_artifacts; parent agent.run_step_artifacts; proof agent.run_step_artifacts; pack agent.evidence_packs;
 value JSONB; provenance JSONB; x JSONB; digest TEXT; schema_try INTEGER; role_name TEXT;
BEGIN
 IF p_artifact_id IS NULL OR p_logical_step_key IS NULL OR p_logical_step_key!~'^[A-Za-z0-9_.:-]{1,200}$' OR
  p_kind IS NULL OR p_kind NOT IN ('router','drafter','critic','repair','retrieval','precheck') OR p_status IS NULL OR p_status NOT IN ('completed','failed') OR
  p_private_json IS NULL OR octet_length(p_private_json::text)>8388608 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_ARTIFACT'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch); PERFORM agent._check_step_identity(r,p_identity);
 digest:=encode(sha256(convert_to(p_identity::text,'UTF8')),'hex');
 SELECT * INTO a FROM agent.run_step_artifacts WHERE id=p_artifact_id OR (run_id=p_run_id AND logical_step_key=p_logical_step_key AND identity_hash=digest);
 IF FOUND THEN
  IF a.run_id<>p_run_id OR a.id<>p_artifact_id OR a.logical_step_key<>p_logical_step_key OR a.identity<>p_identity OR
   a.kind<>p_kind OR a.status<>p_status OR a.private_json<>p_private_json THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN a;
 END IF;
 IF (SELECT count(*) FROM agent.run_step_artifacts WHERE run_id=p_run_id)>=100 THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CAPACITY_EXCEEDED'; END IF;
 IF p_identity->'evidence_pack_id'<>'null'::jsonb THEN SELECT * INTO STRICT pack FROM agent.evidence_packs WHERE id=(p_identity->>'evidence_pack_id')::uuid; END IF;
 IF p_kind IN ('drafter','critic','repair','retrieval','precheck') AND pack.id IS NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_PACK_MISMATCH'; END IF;
 IF p_status='failed' THEN
  IF NOT agent._closed(p_private_json,ARRAY['error_code','call_id','schema_attempt','failure_kind']) OR
   NOT agent._text(p_private_json->'error_code',64) OR
   NOT agent._uuid(p_private_json->'call_id') OR NOT knowledge._parse_integer(p_private_json->'schema_attempt',0,1) OR
   NOT agent._text(p_private_json->'failure_kind',30) OR p_private_json->>'failure_kind' NOT IN ('contract','provider_refusal','truncated','citation_binding') OR
   NOT (agent._error_code(p_private_json->>'error_code') OR p_private_json->>'error_code'='VERIFICATION_FAILED') THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STEP_FAILURE'; END IF;
  schema_try:=(p_private_json->>'schema_attempt')::int;
 ELSE
  IF p_kind IN ('router','drafter','critic','repair') THEN
   IF NOT agent._closed(p_private_json,ARRAY['value','provenance']) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_MODEL_ARTIFACT'; END IF;
   value:=p_private_json->'value'; provenance:=p_private_json->'provenance';
   IF NOT agent._closed(provenance,ARRAY['call_id','role','schema_attempt','model','revision','profile_sha256','prompt_version','prompt_sha256',
    'schema_sha256','input_sha256','messages_sha256','request_sha256','token_ids_sha256','input_tokens','output_tokens','elapsed_ms','evidence_manifest_sha256']) OR
    NOT agent._uuid(provenance->'call_id') OR provenance->>'role' IS DISTINCT FROM p_kind OR NOT agent._text(provenance->'model',200) OR
    NOT knowledge._parse_integer(provenance->'schema_attempt',0,1) OR NOT knowledge._parse_integer(provenance->'input_tokens',1,8192) OR
    NOT knowledge._parse_integer(provenance->'output_tokens',1,8192) OR NOT knowledge._parse_integer(provenance->'elapsed_ms',0,86400000) OR
    NOT agent._text(p_identity->'model_revision',200) OR NOT agent._text(p_identity->'prompt_version',200) OR
    provenance->'revision' IS DISTINCT FROM p_identity->'model_revision' OR provenance->'profile_sha256' IS DISTINCT FROM p_identity->'profile_sha256' OR
    provenance->'prompt_version' IS DISTINCT FROM p_identity->'prompt_version' OR provenance->'prompt_sha256' IS DISTINCT FROM p_identity->'prompt_sha256' OR
    provenance->'schema_sha256' IS DISTINCT FROM p_identity->'schema_sha256' OR provenance->'input_sha256' IS DISTINCT FROM p_identity->'input_sha256' OR
    provenance->'evidence_manifest_sha256' IS DISTINCT FROM coalesce(to_jsonb(pack.content_hash),'null'::jsonb) THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_MODEL_PROVENANCE'; END IF;
   IF (provenance->>'input_tokens')::integer+(provenance->>'output_tokens')::integer>8192 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_MODEL_PROVENANCE'; END IF;
   FOREACH role_name IN ARRAY ARRAY['profile_sha256','prompt_sha256','schema_sha256','input_sha256','messages_sha256','request_sha256','token_ids_sha256'] LOOP
    IF NOT agent._sha(provenance->role_name) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_MODEL_PROVENANCE'; END IF;
   END LOOP;
   schema_try:=(provenance->>'schema_attempt')::int;
   IF p_kind='router' THEN
    IF NOT agent._closed(value,ARRAY['classification','matched_descriptor_ids','reason']) OR NOT agent._text(value->'classification',20) OR
     value->>'classification' NOT IN ('in_scope','out_of_scope','uncertain') OR NOT agent._strings(value->'matched_descriptor_ids',12,64) OR
     NOT agent._text(value->'reason',500) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_ROUTE_ARTIFACT'; END IF;
   ELSIF p_kind IN ('drafter','repair') THEN
    IF NOT agent._draft(value) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DRAFT_ARTIFACT'; END IF;
   ELSIF NOT agent._critic(value) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_CRITIC_ARTIFACT'; END IF;
  ELSIF p_kind='retrieval' THEN
   IF NOT agent._closed(p_private_json,ARRAY['evidence_pack_id','disposition']) OR
    p_private_json->>'evidence_pack_id' IS DISTINCT FROM pack.id::text OR p_private_json->>'disposition' IS DISTINCT FROM
     (CASE WHEN jsonb_array_length(pack.pack->'units')=0 THEN 'empty' ELSE 'ready' END) THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RETRIEVAL_ARTIFACT'; END IF;
  ELSE
   IF NOT agent._closed(p_private_json,ARRAY['draft_artifact_id']) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PRECHECK_ARTIFACT'; END IF;
  END IF;
 END IF;
 IF schema_try=1 AND p_kind IN ('router','drafter','critic','repair') THEN
  SELECT stored.* INTO proof FROM agent.run_step_artifacts stored JOIN agent.run_attempt_budgets b ON b.run_id=stored.run_id AND b.artifact_id=stored.id
   WHERE b.run_id=p_run_id AND b.kind='schema' AND b.role=p_kind;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SCHEMA_RETRY_NOT_RESERVED'; END IF;
  IF proof.identity IS DISTINCT FROM p_identity THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SCHEMA_RETRY_IDENTITY_MISMATCH'; END IF;
  IF EXISTS(SELECT 1 FROM agent.run_step_artifacts WHERE run_id=p_run_id AND schema_retry_role=p_kind) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SCHEMA_RETRY_EXHAUSTED'; END IF;
 END IF;
 IF p_kind IN ('critic','repair','precheck') THEN
  IF jsonb_array_length(p_identity->'parent_artifact_ids')<>1 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ARTIFACT_PARENT_MISMATCH'; END IF;
  SELECT * INTO STRICT parent FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=(p_identity->'parent_artifact_ids'->>0)::uuid;
  IF parent.status<>'completed' OR parent.identity->'evidence_pack_id'<>p_identity->'evidence_pack_id' OR
   (p_kind IN ('critic','precheck') AND parent.kind NOT IN ('drafter','repair')) OR (p_kind='repair' AND parent.kind<>'critic') THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_PARENT_MISMATCH'; END IF;
  IF p_kind='critic' AND p_status='completed' AND NOT agent._critic_bound(value,parent.private_json->'value',pack.pack) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='CRITIC_BINDING_INVALID'; END IF;
  IF p_kind='repair' AND NOT EXISTS(SELECT 1 FROM agent.run_attempt_budgets WHERE run_id=p_run_id AND kind='repair' AND artifact_id=parent.id) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_NOT_RESERVED'; END IF;
  IF p_kind='precheck' AND p_status='completed' AND (p_private_json->>'draft_artifact_id' IS DISTINCT FROM parent.id::text OR
   NOT agent._draft_publishable(parent.private_json->'value',pack.pack)) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='DRAFT_BINDING_INVALID'; END IF;
 END IF;
 INSERT INTO agent.run_step_artifacts(id,run_id,execution_epoch,logical_step_key,input_hash,private_json,status,model_revision,prompt_revision,
  snapshot_id,identity,identity_hash,kind,schema_retry_role)
 VALUES(p_artifact_id,p_run_id,p_epoch,p_logical_step_key,p_identity->>'input_sha256',p_private_json,p_status,p_identity->>'model_revision',
  p_identity->>'prompt_version',r.snapshot_id,p_identity,digest,p_kind,
  CASE WHEN schema_try=1 AND p_kind IN ('router','drafter','critic','repair') THEN p_kind END) RETURNING * INTO a;
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch); RETURN a;
END $$;

CREATE FUNCTION agent.consume_schema_retry(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_role TEXT,p_failed_artifact_id UUID)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE a agent.run_step_artifacts; b agent.run_attempt_budgets;
BEGIN
 IF p_operation_id IS NULL OR p_failed_artifact_id IS NULL OR p_role IS NULL OR p_role NOT IN ('router','drafter','critic','repair') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RETRY_ARGUMENT'; END IF;
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
 SELECT * INTO b FROM agent.run_attempt_budgets WHERE run_id=p_run_id AND (operation_id=p_operation_id OR (kind='schema' AND role=p_role));
 IF FOUND THEN
  IF b.operation_id<>p_operation_id OR b.kind<>'schema' OR b.role<>p_role OR b.artifact_id<>p_failed_artifact_id THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SCHEMA_RETRY_EXHAUSTED'; END IF;
  RETURN 1;
 END IF;
 SELECT * INTO a FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_failed_artifact_id;
 IF NOT FOUND OR a.kind<>p_role OR a.status<>'failed' OR a.private_json->>'error_code' IS DISTINCT FROM 'OUTPUT_SCHEMA_INVALID' OR
  a.private_json->>'schema_attempt' IS DISTINCT FROM '0' OR a.private_json->>'failure_kind' NOT IN ('contract','truncated') THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SCHEMA_RETRY_FORBIDDEN'; END IF;
 INSERT INTO agent.run_attempt_budgets(run_id,kind,role,operation_id,artifact_id,execution_epoch)
 VALUES(p_run_id,'schema',p_role,p_operation_id,p_failed_artifact_id,p_epoch); RETURN 1;
END $$;
CREATE FUNCTION agent.consume_repair(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_critic_artifact_id UUID)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE a agent.run_step_artifacts; b agent.run_attempt_budgets; r agent.runs;
BEGIN
 IF p_operation_id IS NULL OR p_critic_artifact_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REPAIR_ARGUMENT'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 SELECT * INTO b FROM agent.run_attempt_budgets WHERE run_id=p_run_id AND (operation_id=p_operation_id OR kind='repair');
 IF FOUND THEN
  IF b.operation_id<>p_operation_id OR b.kind<>'repair' OR b.artifact_id<>p_critic_artifact_id THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_EXHAUSTED'; END IF;
  RETURN 1;
 END IF;
 SELECT * INTO a FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_critic_artifact_id;
 IF NOT FOUND OR a.kind<>'critic' OR a.status<>'completed' OR agent._policy(a.private_json->'value')<>'repair' OR r.repair_attempts<>0 THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 IF NOT EXISTS(SELECT 1 FROM agent.run_step_artifacts d WHERE d.run_id=p_run_id AND
  d.id=(a.identity->'parent_artifact_ids'->>0)::uuid AND d.private_json#>>'{value,disposition}'='answer' AND
  jsonb_array_length(d.private_json#>'{value,claims}')>0) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 INSERT INTO agent.run_attempt_budgets(run_id,kind,role,operation_id,artifact_id,execution_epoch)
 VALUES(p_run_id,'repair','repair',p_operation_id,p_critic_artifact_id,p_epoch);
 UPDATE agent.runs SET repair_attempts=1 WHERE id=p_run_id; RETURN 1;
END $$;

CREATE FUNCTION agent._public_answer(r agent.runs,p_draft JSONB,p_pack JSONB,p_result_id UUID) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c JSONB; eid TEXT; unit JSONB; citation JSONB; citations JSONB:='[]'; claims JSONB:='[]'; ids JSONB;
 seen JSONB:='{}'; cid TEXT; paragraph TEXT; answer TEXT:=''; labels JSONB; pages JSONB; version_label TEXT; snap agent.kb_snapshots;
BEGIN
 SELECT * INTO STRICT snap FROM agent.kb_snapshots WHERE id=r.snapshot_id;
 FOR c IN SELECT value FROM jsonb_array_elements(p_draft->'claims') LOOP
  ids:='[]'; paragraph:=c->>'text';
  FOR eid IN SELECT value FROM jsonb_array_elements_text(c->'evidence_ids') LOOP
   cid:=seen->>eid;
   IF cid IS NULL THEN
    SELECT value INTO STRICT unit FROM jsonb_array_elements(p_pack->'units') WHERE value->>'evidence_id'=eid;
    cid:='C'||lpad((jsonb_array_length(citations)+1)::text,3,'0'); seen:=seen||jsonb_build_object(eid,cid);
    SELECT v.version_label INTO STRICT version_label FROM app.document_versions v WHERE id=(unit->>'document_version_id')::uuid;
    SELECT jsonb_agg(page ORDER BY page) INTO pages FROM (SELECT DISTINCT (s->>'pdf_page')::int AS page FROM jsonb_array_elements(unit->'source_spans') s) q;
    SELECT coalesce(jsonb_agg(label ORDER BY first_seen),'[]') INTO labels FROM
     (SELECT s->'printed_page_label' AS label,min(ord) AS first_seen FROM jsonb_array_elements(unit->'source_spans') WITH ORDINALITY t(s,ord)
      WHERE s->'printed_page_label'<>'null'::jsonb GROUP BY s->'printed_page_label') q;
    citation:=jsonb_build_object('citation_id',cid,'evidence_id',eid,'document_title',unit->'document_title','version_label',version_label,
     'structural_path',unit->'structural_path','pdf_pages',pages,'printed_page_labels',labels,
     'source_url','/api/v1/versions/'||(unit->>'document_version_id')||'/source');
    citations:=citations||jsonb_build_array(citation);
   END IF;
   ids:=ids||jsonb_build_array(cid); paragraph:=paragraph||' ['||cid||']';
  END LOOP;
  claims:=claims||jsonb_build_array(jsonb_build_object('claim_id',c->'claim_id','text',c->'text','citation_ids',ids));
  answer:=answer||CASE WHEN answer='' THEN '' ELSE E'\n\n' END||paragraph;
 END LOOP;
 answer:=answer||E'\n\nОтвет сформирован по загруженной базе на момент '||
  to_char(snap.captured_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')||'+00:00; автоматическая проверка не заменяет экспертную оценку.';
 RETURN jsonb_build_object('kind','completed','result_id',p_result_id,'text',answer,'claims',claims,'citations',citations,
  'validation',jsonb_build_object('status','confirmed','claim_count',jsonb_array_length(claims),'supported_count',jsonb_array_length(claims),
   'repair_used',r.repair_attempts=1));
END $$;

CREATE FUNCTION agent.finalize_run(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_outcome TEXT,p_result JSONB,
 p_draft_artifact_id UUID,p_critic_artifact_id UUID,p_decision_artifact_id UUID,p_error_code TEXT) RETURNS agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; d agent.run_step_artifacts; c agent.run_step_artifacts; decision agent.run_step_artifacts; parent agent.run_step_artifacts;
 ep agent.evidence_packs; snap agent.kb_snapshots; expected JSONB; digest TEXT; request JSONB; final_result_id UUID; refusal TEXT; message TEXT;
 final_outcome TEXT:=p_outcome; error_code TEXT:=p_error_code; ts TIMESTAMPTZ; count_versions BIGINT;
BEGIN
 IF p_run_id IS NULL OR p_owner IS NULL OR p_epoch IS NULL OR p_operation_id IS NULL OR p_outcome IS NULL OR
  p_outcome NOT IN ('completed','refused','failed','cancelled') OR (p_result IS NOT NULL AND octet_length(p_result::text)>262144) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_FINALIZATION'; END IF;
 request:=jsonb_build_object('outcome',p_outcome,'result',p_result,'draft',p_draft_artifact_id,'critic',p_critic_artifact_id,
  'decision',p_decision_artifact_id,'error_code',p_error_code);
 digest:=encode(sha256(convert_to(request::text,'UTF8')),'hex');
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 IF r.execution_owner IS DISTINCT FROM p_owner OR r.execution_epoch<>p_epoch THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_EXECUTION'; END IF;
 IF r.status IN ('completed','refused','failed','cancelled') THEN
  IF r.terminal_operation_id=p_operation_id AND r.terminal_request_hash=digest THEN RETURN r; END IF;
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='TERMINAL_CONFLICT';
 END IF;
 ts:=clock_timestamp();
 IF r.cancel_requested_at IS NOT NULL THEN final_outcome:='cancelled'; error_code:=NULL;
 ELSIF r.deadline_at<=ts THEN final_outcome:='failed'; error_code:='DEADLINE_EXCEEDED';
 ELSIF EXISTS(SELECT 1 FROM agent.kb_snapshot_items i JOIN app.logical_documents l ON l.id=i.logical_document_id
  WHERE i.snapshot_id=r.snapshot_id AND l.security_revoked_at IS NOT NULL) THEN final_outcome:='failed'; error_code:='SOURCE_REVOKED';
 ELSIF r.status<>'running' OR r.lease_until IS NULL OR r.lease_until<=ts THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='LEASE_EXPIRED';
 END IF;
 IF final_outcome IN ('completed','refused') THEN
  IF p_error_code IS NOT NULL OR p_result IS NULL OR p_result->>'kind' IS DISTINCT FROM final_outcome OR NOT agent._uuid(p_result->'result_id') OR
   NOT agent._closed(p_result->'snapshot',ARRAY['id','captured_at','version_count']) OR NOT agent._uuid(p_result#>'{snapshot,id}') OR
   NOT knowledge._parse_integer(p_result#>'{snapshot,version_count}',0,1000000) OR NOT agent._text(p_result#>'{snapshot,captured_at}',60) OR
   r.snapshot_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PUBLIC_RESULT'; END IF;
  SELECT * INTO STRICT snap FROM agent.kb_snapshots WHERE id=r.snapshot_id;
  SELECT count(*) INTO count_versions FROM agent.kb_snapshot_items WHERE snapshot_id=r.snapshot_id;
  IF p_result#>>'{snapshot,id}'<>r.snapshot_id::text OR (p_result#>>'{snapshot,captured_at}')::timestamptz<>snap.captured_at OR
   (p_result#>>'{snapshot,version_count}')::bigint<>count_versions THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SNAPSHOT_MISMATCH'; END IF;
  final_result_id:=(p_result->>'result_id')::uuid;
  SELECT * INTO ep FROM agent.evidence_packs WHERE run_id=p_run_id;
  IF p_draft_artifact_id IS NOT NULL THEN SELECT * INTO d FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_draft_artifact_id; END IF;
  IF p_critic_artifact_id IS NOT NULL THEN SELECT * INTO c FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_critic_artifact_id; END IF;
  IF p_decision_artifact_id IS NOT NULL THEN SELECT * INTO decision FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_decision_artifact_id; END IF;
  IF (p_draft_artifact_id IS NOT NULL AND d.id IS NULL) OR (p_critic_artifact_id IS NOT NULL AND c.id IS NULL) OR
   (p_decision_artifact_id IS NOT NULL AND decision.id IS NULL) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_PARENT_MISMATCH'; END IF;
  IF final_outcome='completed' THEN
   IF NOT agent._closed(p_result,ARRAY['kind','result_id','text','claims','citations','validation','snapshot']) OR
    ep.id IS NULL OR d.id IS NULL OR c.id IS NULL OR d.status<>'completed' OR c.status<>'completed' OR
    d.kind<>(CASE WHEN r.repair_attempts=1 THEN 'repair' ELSE 'drafter' END) OR c.kind<>'critic' OR
    c.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array(d.id) OR
    c.identity->'model_revision' IS DISTINCT FROM d.identity->'model_revision' OR c.identity->'profile_sha256' IS DISTINCT FROM d.identity->'profile_sha256' OR
    d.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR c.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR
    d.private_json#>>'{value,disposition}' IS DISTINCT FROM 'answer' OR jsonb_array_length(d.private_json#>'{value,claims}')=0 OR
    NOT agent._critic_bound(c.private_json->'value',d.private_json->'value',ep.pack) OR agent._policy(c.private_json->'value')<>'render' OR
    (decision.id IS NOT NULL AND decision.id<>c.id) OR count_versions=0 THEN
    RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_RESULT'; END IF;
   expected:=agent._public_answer(r,d.private_json->'value',ep.pack,final_result_id);
   IF p_result-'snapshot'<>expected THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PUBLIC_RESULT_MISMATCH'; END IF;
  ELSE
   IF NOT agent._closed(p_result,ARRAY['kind','result_id','code','text','snapshot']) OR decision.id IS NULL THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REFUSAL'; END IF;
   refusal:=p_result->>'code';
   IF refusal='OUT_OF_SCOPE' THEN
    IF decision.kind<>'router' OR decision.status<>'completed' OR decision.private_json#>>'{value,classification}' IS DISTINCT FROM 'out_of_scope' THEN
     RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_REFUSAL'; END IF;
    message:='Извините, запрос не относится к предметной области загруженных нормативных документов. Пожалуйста, уточните запрос';
   ELSIF refusal='NO_RELEVANT_CONTEXT' THEN
    IF ep.id IS NULL OR decision.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR decision.status<>'completed' OR NOT (
     (decision.kind='retrieval' AND decision.private_json->>'disposition'='empty' AND jsonb_array_length(ep.pack->'units')=0) OR
     (decision.kind='drafter' AND decision.private_json#>>'{value,disposition}'='insufficient_evidence')) THEN
     RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_REFUSAL'; END IF;
    message:='Извините, по вашему запросу не найдено информации в действующих нормативных документах. Пожалуйста, уточните запрос';
   ELSIF refusal='VERIFICATION_FAILED' THEN
    IF decision.kind='critic' AND decision.status='completed' THEN
     IF agent._policy(decision.private_json->'value')='render' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_REFUSAL'; END IF;
    ELSIF decision.kind='precheck' AND decision.status='failed' AND decision.private_json->>'error_code'='VERIFICATION_FAILED' THEN
     SELECT * INTO STRICT parent FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=(decision.identity->'parent_artifact_ids'->>0)::uuid;
     IF ep.id IS NULL OR (agent._draft_bound(parent.private_json->'value',ep.pack) AND NOT
      (parent.kind='repair' AND parent.private_json#>>'{value,disposition}'='insufficient_evidence' AND
       jsonb_array_length(parent.private_json#>'{value,claims}')=0)) THEN
      RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_REFUSAL'; END IF;
    ELSE RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UNVERIFIED_REFUSAL'; END IF;
    message:='Извините, система не смогла верифицировать ответ. Пожалуйста, уточните запрос';
   ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REFUSAL'; END IF;
   IF p_result->>'text' IS DISTINCT FROM message THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PUBLIC_RESULT_MISMATCH'; END IF;
  END IF;
  PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
  INSERT INTO agent.run_results(id,run_id,snapshot_id,kind,verified_answer,refusal_code,claims_public,citations_public,critic_public_result,
   public_result,draft_artifact_id,critic_artifact_id,decision_artifact_id,evidence_pack_id)
  VALUES(final_result_id,p_run_id,r.snapshot_id,final_outcome,CASE WHEN final_outcome='completed' THEN p_result->>'text' END,refusal,
   coalesce(p_result->'claims','[]'),coalesce(p_result->'citations','[]'),coalesce(p_result->'validation','{}'),
   p_result,p_draft_artifact_id,p_critic_artifact_id,p_decision_artifact_id,ep.id);
  PERFORM agent._append_command_event(p_run_id,'run.'||final_outcome,CASE WHEN final_outcome='completed' THEN
   jsonb_build_object('result_id',final_result_id,'claim_count',jsonb_array_length(p_result->'claims'),'citation_count',jsonb_array_length(p_result->'citations'))
   ELSE jsonb_build_object('result_id',final_result_id,'refusal_code',refusal) END);
  PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
  UPDATE agent.runs SET status=final_outcome,result_id=final_result_id,refusal_code=refusal,finished_at=clock_timestamp(),lease_until=NULL,
   terminal_operation_id=p_operation_id,terminal_request_hash=digest WHERE id=p_run_id RETURNING * INTO r;
 ELSE
  IF final_outcome='failed' AND NOT agent._error_code(error_code) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TERMINAL_ERROR'; END IF;
  IF final_outcome='cancelled' AND r.cancel_requested_at IS NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CANCEL_NOT_REQUESTED'; END IF;
  IF final_outcome=p_outcome AND (p_result IS NOT NULL OR p_draft_artifact_id IS NOT NULL OR p_critic_artifact_id IS NOT NULL OR
   p_decision_artifact_id IS NOT NULL OR (final_outcome='cancelled' AND p_error_code IS NOT NULL)) THEN
   RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TERMINAL_RESULT'; END IF;
  UPDATE agent.runs SET terminal_operation_id=p_operation_id,terminal_request_hash=digest WHERE id=p_run_id;
  r:=agent._end_without_result(p_run_id,final_outcome,error_code);
 END IF;
 RETURN r;
END $$;

CREATE TABLE app.document_security_revocations (
 document_id UUID PRIMARY KEY REFERENCES app.logical_documents, principal_id TEXT NOT NULL CHECK(btrim(principal_id)<>''),
 reason_code TEXT NOT NULL CHECK(reason_code IN ('SECURITY_REVOKED','ACCESS_WITHDRAWN')), revoked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER security_revocations_immutable BEFORE UPDATE OR DELETE ON app.document_security_revocations FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();
CREATE FUNCTION app.revoke_document(p_document_id UUID,p_principal_id TEXT,p_reason_code TEXT) RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE d app.logical_documents; r agent.runs; affected INTEGER:=0;
BEGIN
 IF p_document_id IS NULL OR p_principal_id IS NULL OR btrim(p_principal_id)='' OR p_reason_code IS NULL OR
  p_reason_code NOT IN ('SECURITY_REVOKED','ACCESS_WITHDRAWN') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REVOKE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO d FROM app.logical_documents WHERE id=p_document_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_NOT_FOUND'; END IF;
 IF d.security_revoked_at IS NOT NULL THEN RETURN 0; END IF;
 INSERT INTO app.document_security_revocations(document_id,principal_id,reason_code) VALUES(p_document_id,p_principal_id,p_reason_code);
 UPDATE app.logical_documents SET security_revoked_at=clock_timestamp(),row_version=row_version+1,updated_at=clock_timestamp() WHERE id=p_document_id;
 UPDATE knowledge.chunks SET searchable=false WHERE index_generation_id IN (
  SELECT g.id FROM knowledge.index_generations g JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=p_document_id);
 UPDATE knowledge.node_routing_embeddings SET searchable=false WHERE index_generation_id IN (
  SELECT g.id FROM knowledge.index_generations g JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=p_document_id);
 FOR r IN SELECT * FROM agent.runs WHERE status IN ('created','running','cancelling') AND snapshot_id IN (
  SELECT snapshot_id FROM agent.kb_snapshot_items WHERE logical_document_id=p_document_id) ORDER BY id FOR UPDATE LOOP
  IF r.cancel_requested_at IS NOT NULL THEN PERFORM agent._end_without_result(r.id,'cancelled',NULL);
  ELSE PERFORM agent._end_without_result(r.id,'failed','SOURCE_REVOKED'); END IF;
  affected:=affected+1;
 END LOOP;
 UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=clock_timestamp();
 INSERT INTO app.outbox_events(aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES('document',p_document_id,'document.security_revoked','knowledge.events',jsonb_build_object('document_id',p_document_id));
 RETURN affected;
END $$;

REVOKE ALL ON agent.run_operations,agent.run_attempt_budgets,app.document_security_revocations FROM PUBLIC,expert_runtime,expert_backend,expert_ingest,expert_outbox;
GRANT SELECT ON agent.run_attempt_budgets TO expert_runtime;
GRANT SELECT ON app.document_security_revocations TO expert_backend;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA agent FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
 agent.record_stage_event(UUID,UUID,BIGINT,UUID,TEXT,TEXT,INTEGER,JSONB),
 agent.save_evidence_pack(UUID,UUID,BIGINT,JSONB,TEXT),
 agent.save_step_artifact(UUID,UUID,BIGINT,UUID,TEXT,JSONB,TEXT,TEXT,JSONB),
 agent.find_step_artifact(UUID,UUID,BIGINT,TEXT,JSONB),
 agent.consume_schema_retry(UUID,UUID,BIGINT,UUID,TEXT,UUID),
 agent.consume_repair(UUID,UUID,BIGINT,UUID,UUID),
 agent.finalize_run(UUID,UUID,BIGINT,UUID,TEXT,JSONB,UUID,UUID,UUID,TEXT),agent.recover_runs(INTEGER) TO expert_runtime;
REVOKE ALL ON FUNCTION app.revoke_document(UUID,TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.revoke_document(UUID,TEXT,TEXT),
 agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN,INTEGER) TO expert_backend;
