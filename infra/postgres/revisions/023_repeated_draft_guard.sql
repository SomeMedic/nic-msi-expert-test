-- Keep source membership checks and reject only exact repeated claim text with
-- the same evidence set. JSONB string equality preserves Unicode, whitespace,
-- case and punctuation; mutual array containment ignores evidence order.
-- CREATE OR REPLACE preserves the existing function identity and ACL.
CREATE OR REPLACE FUNCTION agent._draft_bound(d JSONB,p JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT NOT EXISTS(SELECT 1 FROM jsonb_array_elements(d->'claims') c CROSS JOIN LATERAL jsonb_array_elements_text(c->'evidence_ids') eid
  WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p->'units') u WHERE u->>'evidence_id'=eid))
 AND NOT EXISTS(
  SELECT 1 FROM jsonb_array_elements(d->'claims') WITH ORDINALITY a(claim,position)
  CROSS JOIN jsonb_array_elements(d->'claims') WITH ORDINALITY b(claim,position)
  WHERE a.position<b.position AND a.claim->'text'=b.claim->'text'
   AND (a.claim->'evidence_ids') @> (b.claim->'evidence_ids')
   AND (b.claim->'evidence_ids') @> (a.claim->'evidence_ids')
 )
$$;
