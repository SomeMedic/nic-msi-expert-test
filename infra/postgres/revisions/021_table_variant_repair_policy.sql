-- Persist deterministic graph policy separately from raw Critic artifacts. Revision filename remains table_variant_repair_policy for wrapper continuity.
-- No new execution grants are introduced: policy artifacts are saved through
-- the existing fenced agent.save_step_artifact surface.

ALTER TABLE agent.run_step_artifacts DROP CONSTRAINT IF EXISTS run_step_artifacts_kind_check;
ALTER TABLE agent.run_step_artifacts ADD CONSTRAINT run_step_artifacts_kind_check
 CHECK(kind IN ('router','drafter','critic','repair','retrieval','precheck','policy'));

CREATE OR REPLACE FUNCTION agent._policy_decision(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF NOT agent._closed(v,ARRAY['draft_artifact_id','critic_artifact_id','action','outcome','reason','refusal_code',
  'raw_critic_policy','evidence_manifest_sha256','policy_version']) OR NOT agent._uuid(v->'draft_artifact_id') OR
  NOT agent._uuid(v->'critic_artifact_id') OR jsonb_typeof(v->'action') IS DISTINCT FROM 'string' OR v->>'action' NOT IN ('render','repair','refuse') OR
  jsonb_typeof(v->'outcome') IS DISTINCT FROM 'string' OR v->>'outcome' NOT IN ('confirmed','partially_confirmed','hallucinated') OR
  jsonb_typeof(v->'reason') IS DISTINCT FROM 'string' OR v->>'reason' NOT IN ('SUPPORTED','UNSUPPORTED_CLAIM','QUESTION_MISMATCH','GLOBAL_ISSUE',
   'UNSAFE_PARTIAL','INCOMPLETE_ANSWER','REPAIR_EXHAUSTED','INCOMPLETE_TABLE_VARIANT') OR
  jsonb_typeof(v->'raw_critic_policy') IS DISTINCT FROM 'string' OR v->>'raw_critic_policy' NOT IN ('render','repair','refuse') OR
  NOT agent._sha(v->'evidence_manifest_sha256') OR v->>'policy_version' IS DISTINCT FROM 'p08.deterministic_policy.v1' THEN
  RETURN false;
 END IF;
 IF v->'refusal_code'<>'null'::jsonb AND (jsonb_typeof(v->'refusal_code') IS DISTINCT FROM 'string' OR v->>'refusal_code' IS DISTINCT FROM 'VERIFICATION_FAILED') THEN RETURN false; END IF;
 IF v->>'action'='render' THEN
  RETURN v->>'outcome'='confirmed' AND v->>'reason'='SUPPORTED' AND v->'refusal_code'='null'::jsonb AND v->>'raw_critic_policy'='render';
 ELSIF v->>'action'='repair' THEN
  RETURN v->>'outcome'='partially_confirmed' AND v->'refusal_code'='null'::jsonb AND v->>'reason' IN ('INCOMPLETE_ANSWER','INCOMPLETE_TABLE_VARIANT') AND
   (v->>'raw_critic_policy'='repair' OR (v->>'raw_critic_policy'='render' AND v->>'reason'='INCOMPLETE_TABLE_VARIANT'));
 ELSE
  RETURN v->'refusal_code'='"VERIFICATION_FAILED"'::jsonb AND (
   v->>'raw_critic_policy'='refuse' OR (v->>'raw_critic_policy'='repair' AND v->>'reason' IN ('REPAIR_EXHAUSTED','INCOMPLETE_TABLE_VARIANT')) OR
   (v->>'raw_critic_policy'='render' AND v->>'reason'='INCOMPLETE_TABLE_VARIANT'));
 END IF;
END $$;
REVOKE ALL ON FUNCTION agent._policy_decision(JSONB) FROM PUBLIC,expert_runtime,expert_backend,expert_ingest,expert_outbox;

CREATE OR REPLACE FUNCTION agent.save_step_artifact(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_artifact_id UUID,p_logical_step_key TEXT,
 p_identity JSONB,p_kind TEXT,p_status TEXT,p_private_json JSONB) RETURNS agent.run_step_artifacts
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; a agent.run_step_artifacts; parent agent.run_step_artifacts; proof agent.run_step_artifacts; pack agent.evidence_packs;
 value JSONB; provenance JSONB; x JSONB; digest TEXT; schema_try INTEGER; role_name TEXT;
BEGIN
 IF p_artifact_id IS NULL OR p_logical_step_key IS NULL OR p_logical_step_key!~'^[A-Za-z0-9_.:-]{1,200}$' OR
  p_kind IS NULL OR p_kind NOT IN ('router','drafter','critic','repair','retrieval','precheck','policy') OR p_status IS NULL OR p_status NOT IN ('completed','failed') OR
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
 IF p_kind IN ('drafter','critic','repair','retrieval','precheck','policy') AND pack.id IS NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EVIDENCE_PACK_MISMATCH'; END IF;
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
  ELSIF p_kind='policy' THEN
   IF agent._policy_decision(p_private_json) IS NOT TRUE THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_POLICY_ARTIFACT'; END IF;
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
 ELSIF p_kind='policy' THEN
  IF jsonb_array_length(p_identity->'parent_artifact_ids')<>2 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ARTIFACT_PARENT_MISMATCH'; END IF;
  SELECT * INTO STRICT parent FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=(p_identity->'parent_artifact_ids'->>0)::uuid;
  SELECT * INTO STRICT proof FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=(p_identity->'parent_artifact_ids'->>1)::uuid;
  IF parent.status<>'completed' OR proof.status<>'completed' OR parent.kind NOT IN ('drafter','repair') OR proof.kind<>'critic' OR
   parent.identity->'evidence_pack_id'<>p_identity->'evidence_pack_id' OR proof.identity->'evidence_pack_id'<>p_identity->'evidence_pack_id' OR
   proof.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array(parent.id) OR
   p_private_json->>'draft_artifact_id' IS DISTINCT FROM parent.id::text OR p_private_json->>'critic_artifact_id' IS DISTINCT FROM proof.id::text OR
   p_private_json->>'raw_critic_policy' IS DISTINCT FROM agent._policy(proof.private_json->'value') OR
   p_private_json->>'evidence_manifest_sha256' IS DISTINCT FROM pack.content_hash THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='POLICY_DECISION_MISMATCH'; END IF;
  IF p_private_json->>'action'='refuse' AND p_private_json->>'reason' IN ('REPAIR_EXHAUSTED','INCOMPLETE_TABLE_VARIANT') AND r.repair_attempts<>1 THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='POLICY_DECISION_MISMATCH'; END IF;
 END IF;
 INSERT INTO agent.run_step_artifacts(id,run_id,execution_epoch,logical_step_key,input_hash,private_json,status,model_revision,prompt_revision,
  snapshot_id,identity,identity_hash,kind,schema_retry_role)
 VALUES(p_artifact_id,p_run_id,p_epoch,p_logical_step_key,p_identity->>'input_sha256',p_private_json,p_status,p_identity->>'model_revision',
  p_identity->>'prompt_version',r.snapshot_id,p_identity,digest,p_kind,
  CASE WHEN schema_try=1 AND p_kind IN ('router','drafter','critic','repair') THEN p_kind END) RETURNING * INTO a;
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch); RETURN a;
END $$;

CREATE OR REPLACE FUNCTION agent.consume_repair(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_critic_artifact_id UUID)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE decision agent.run_step_artifacts; critic agent.run_step_artifacts; b agent.run_attempt_budgets; r agent.runs; raw_critic_id UUID;
BEGIN
 IF p_operation_id IS NULL OR p_critic_artifact_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REPAIR_ARGUMENT'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 SELECT * INTO decision FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=p_critic_artifact_id;
 IF NOT FOUND OR decision.status<>'completed' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 IF decision.kind='critic' THEN
  raw_critic_id:=decision.id; critic:=decision;
  IF agent._policy(critic.private_json->'value')<>'repair' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 ELSIF decision.kind='policy' THEN
  IF agent._policy_decision(decision.private_json) IS NOT TRUE OR decision.private_json->>'action' IS DISTINCT FROM 'repair' THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
  raw_critic_id:=(decision.private_json->>'critic_artifact_id')::uuid;
  SELECT * INTO critic FROM agent.run_step_artifacts WHERE run_id=p_run_id AND id=raw_critic_id;
  IF NOT FOUND OR critic.kind<>'critic' OR critic.status<>'completed' OR
   decision.private_json->>'raw_critic_policy' IS DISTINCT FROM agent._policy(critic.private_json->'value') OR
   decision.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array((decision.private_json->>'draft_artifact_id')::uuid,raw_critic_id) OR
   critic.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array((decision.private_json->>'draft_artifact_id')::uuid) OR
   decision.identity->>'evidence_pack_id' IS DISTINCT FROM critic.identity->>'evidence_pack_id' THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 ELSE RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 SELECT * INTO b FROM agent.run_attempt_budgets WHERE run_id=p_run_id AND (operation_id=p_operation_id OR kind='repair');
 IF FOUND THEN
  IF b.operation_id<>p_operation_id OR b.kind<>'repair' OR b.artifact_id<>raw_critic_id THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_EXHAUSTED'; END IF;
  RETURN 1;
 END IF;
 IF r.repair_attempts<>0 THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 IF NOT EXISTS(SELECT 1 FROM agent.run_step_artifacts d WHERE d.run_id=p_run_id AND
  d.id=(critic.identity->'parent_artifact_ids'->>0)::uuid AND d.private_json#>>'{value,disposition}'='answer' AND
  jsonb_array_length(d.private_json#>'{value,claims}')>0) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='REPAIR_FORBIDDEN'; END IF;
 INSERT INTO agent.run_attempt_budgets(run_id,kind,role,operation_id,artifact_id,execution_epoch)
 VALUES(p_run_id,'repair','repair',p_operation_id,raw_critic_id,p_epoch);
 UPDATE agent.runs SET repair_attempts=1 WHERE id=p_run_id; RETURN 1;
END $$;

CREATE OR REPLACE FUNCTION agent.finalize_run(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_outcome TEXT,p_result JSONB,
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
    ELSIF decision.kind='policy' AND decision.status='completed' THEN
     IF ep.id IS NULL OR d.id IS NULL OR c.id IS NULL OR agent._policy_decision(decision.private_json) IS NOT TRUE OR
      decision.private_json->>'action' IS DISTINCT FROM 'refuse' OR decision.private_json->>'refusal_code' IS DISTINCT FROM 'VERIFICATION_FAILED' OR
      decision.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array(d.id,c.id) OR decision.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR
      decision.private_json->>'draft_artifact_id' IS DISTINCT FROM d.id::text OR decision.private_json->>'critic_artifact_id' IS DISTINCT FROM c.id::text OR
      c.identity->'parent_artifact_ids' IS DISTINCT FROM jsonb_build_array(d.id) OR c.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR
      d.identity->>'evidence_pack_id' IS DISTINCT FROM ep.id::text OR decision.private_json->>'raw_critic_policy' IS DISTINCT FROM agent._policy(c.private_json->'value') OR
      decision.private_json->>'evidence_manifest_sha256' IS DISTINCT FROM ep.content_hash OR
      (decision.private_json->>'reason' IN ('REPAIR_EXHAUSTED','INCOMPLETE_TABLE_VARIANT') AND r.repair_attempts<>1) THEN
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


