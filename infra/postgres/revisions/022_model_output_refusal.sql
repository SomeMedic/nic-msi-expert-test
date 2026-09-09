-- Honest verification refusal from immutable failed model evidence.
-- CREATE OR REPLACE preserves the finalizer OID and existing ACL; no grants change.
-- Historical revisions stay immutable. Cancellation, fencing and publication guards remain intact.

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
    ELSIF decision.kind IN ('router','drafter','critic','repair') AND decision.status='failed' THEN
     -- A technical model failure is proof that no verified answer was produced,
     -- not proof of empty retrieval or a substitute for a completed Critic.
     PERFORM agent._check_step_identity(r,decision.identity);
     IF (p_draft_artifact_id IS NULL AND p_critic_artifact_id IS NULL AND
      decision.snapshot_id=r.snapshot_id AND decision.execution_epoch<=p_epoch AND
      decision.private_json->>'error_code'='OUTPUT_SCHEMA_INVALID' AND
      decision.private_json->>'failure_kind' IN ('contract','truncated','provider_refusal') AND
      NOT EXISTS(SELECT 1 FROM agent.run_step_artifacts successful WHERE successful.run_id=p_run_id AND
       successful.kind=decision.kind AND successful.identity=decision.identity AND successful.status='completed') AND (
       -- Provider refusal is explicitly non-retryable; do not manufacture a retry.
       (decision.private_json->>'failure_kind'='provider_refusal' AND
        decision.private_json->>'schema_attempt'='0' AND decision.schema_retry_role IS NULL) OR
       -- A reserved budget alone is insufficient: the failed schema1 is persisted
       -- and must bind to the exact schema0 for which the role budget was consumed.
       (decision.private_json->>'schema_attempt'='1' AND decision.schema_retry_role=decision.kind AND
        EXISTS(SELECT 1 FROM agent.run_attempt_budgets budget
         JOIN agent.run_step_artifacts first_failure ON first_failure.run_id=budget.run_id AND first_failure.id=budget.artifact_id
         WHERE budget.run_id=p_run_id AND budget.kind='schema' AND budget.role=decision.kind AND
          first_failure.kind=decision.kind AND first_failure.status='failed' AND
          first_failure.private_json->>'error_code'='OUTPUT_SCHEMA_INVALID' AND
          first_failure.private_json->>'schema_attempt'='0' AND
          first_failure.private_json->>'failure_kind' IN ('contract','truncated') AND
          first_failure.identity=decision.identity AND
          first_failure.execution_epoch<=budget.execution_epoch AND budget.execution_epoch<=decision.execution_epoch)) OR
       -- Critic shares its schema budget across initial/repaired phases. Accept
       -- repaired schema0 only along the actual schema1 Critic -> Repair chain.
       (decision.kind='critic' AND decision.private_json->>'schema_attempt'='0' AND
        decision.schema_retry_role IS NULL AND r.repair_attempts=1 AND
        EXISTS(SELECT 1 FROM agent.run_attempt_budgets budget
         JOIN agent.run_step_artifacts first_failure ON first_failure.run_id=budget.run_id AND first_failure.id=budget.artifact_id
         JOIN agent.run_step_artifacts retried ON retried.run_id=budget.run_id AND retried.kind='critic' AND
          retried.schema_retry_role='critic' AND retried.status='completed' AND retried.identity=first_failure.identity
         JOIN agent.run_attempt_budgets repair_budget ON repair_budget.run_id=budget.run_id AND
          repair_budget.kind='repair' AND repair_budget.role='repair' AND repair_budget.artifact_id=retried.id
         JOIN agent.run_step_artifacts repaired ON repaired.run_id=budget.run_id AND repaired.kind='repair' AND
          repaired.status='completed' AND repaired.identity->'parent_artifact_ids'=jsonb_build_array(retried.id)
         WHERE budget.run_id=p_run_id AND budget.kind='schema' AND budget.role='critic' AND
          first_failure.kind='critic' AND first_failure.status='failed' AND
          first_failure.private_json->>'error_code'='OUTPUT_SCHEMA_INVALID' AND
          first_failure.private_json->>'schema_attempt'='0' AND
          first_failure.private_json->>'failure_kind' IN ('contract','truncated') AND
          retried.private_json#>>'{provenance,schema_attempt}'='1' AND
          decision.identity->'parent_artifact_ids'=jsonb_build_array(repaired.id) AND
          retried.identity->'snapshot_id'=decision.identity->'snapshot_id' AND
          retried.identity->'configuration_fingerprint'=decision.identity->'configuration_fingerprint' AND
          retried.identity->'evidence_pack_id'=decision.identity->'evidence_pack_id' AND
          retried.identity->'model_revision'=decision.identity->'model_revision' AND
          retried.identity->'profile_sha256'=decision.identity->'profile_sha256' AND
          first_failure.execution_epoch<=budget.execution_epoch AND budget.execution_epoch<=retried.execution_epoch AND
          retried.execution_epoch<=repair_budget.execution_epoch AND repair_budget.execution_epoch<=repaired.execution_epoch AND
          repaired.execution_epoch<=decision.execution_epoch))
      )) IS NOT TRUE THEN
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


