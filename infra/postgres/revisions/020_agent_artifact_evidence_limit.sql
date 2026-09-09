-- Align durable agent artifact shape checks with the canonical Python DTOs.
-- Historical 011 remains immutable; this forward revision only replaces the
-- private shape validators for model artifacts.

CREATE OR REPLACE FUNCTION agent._draft(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE c JSONB;
BEGIN
 IF NOT agent._closed(v,ARRAY['disposition','claims','limitation']) OR v->>'disposition' NOT IN ('answer','insufficient_evidence') OR
  jsonb_typeof(v->'disposition') IS DISTINCT FROM 'string' OR jsonb_typeof(v->'claims') IS DISTINCT FROM 'array' OR
  (v->'limitation'<>'null'::jsonb AND NOT agent._text(v->'limitation',500)) THEN RETURN false; END IF;
 IF jsonb_array_length(v->'claims')>12 OR (v->>'disposition'='answer')<>(jsonb_array_length(v->'claims')>0) OR
  (SELECT count(DISTINCT x->>'claim_id') FROM jsonb_array_elements(v->'claims') x)<>jsonb_array_length(v->'claims') THEN RETURN false; END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(v->'claims') LOOP
  IF NOT agent._closed(c,ARRAY['claim_id','text','evidence_ids']) OR NOT agent._text(c->'claim_id',64) OR NOT agent._text(c->'text',1800) OR
   NOT agent._strings(c->'evidence_ids',10,64) THEN RETURN false; END IF;
  IF jsonb_array_length(c->'evidence_ids')=0 THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;

CREATE OR REPLACE FUNCTION agent._critic(v JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
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
   NOT agent._text(c->'claim_id',64) OR NOT agent._text(c->'explanation',1000) OR NOT agent._strings(c->'evidence_ids',10,64) OR
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
