ALTER TABLE knowledge.parse_generations ADD COLUMN operation_id UUID,
 ADD COLUMN physical_page_count INTEGER CHECK(physical_page_count BETWEEN 1 AND 500),
 ADD UNIQUE(creating_job_id,creating_job_epoch,operation_id),
 ADD UNIQUE(id,document_version_id,creating_job_id,creating_job_epoch);

CREATE FUNCTION knowledge.guard_parse_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (NEW.id,NEW.document_version_id,NEW.source_sha256,NEW.creating_job_id,NEW.creating_job_epoch,
     NEW.operation_id,NEW.parser_fingerprint,NEW.normalizer_version,NEW.structure_version)
  IS DISTINCT FROM
    (OLD.id,OLD.document_version_id,OLD.source_sha256,OLD.creating_job_id,OLD.creating_job_epoch,
     OLD.operation_id,OLD.parser_fingerprint,OLD.normalizer_version,OLD.structure_version) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.status IN ('ready','failed') AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 IF OLD.artifact_object_id IS NOT NULL AND NEW.artifact_object_id IS DISTINCT FROM OLD.artifact_object_id THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='ARTIFACT_IDENTITY_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER parse_identity_guard BEFORE UPDATE ON knowledge.parse_generations
 FOR EACH ROW EXECUTE FUNCTION knowledge.guard_parse_identity();

CREATE FUNCTION knowledge._assert_parse_execution(p_job_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS app.ingestion_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE binding app.ingestion_write_bindings;
BEGIN
 SELECT * INTO binding FROM app.ingestion_write_bindings
 WHERE transaction_id=pg_current_xact_id() AND backend_pid=pg_backend_pid();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_BINDING_REQUIRED'; END IF;
 IF (binding.job_id,binding.lease_owner,binding.lease_epoch) IS DISTINCT FROM (p_job_id,p_owner,p_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 RETURN app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
END $$;
CREATE FUNCTION knowledge._assert_parse_owner(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID)
RETURNS knowledge.parse_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; g knowledge.parse_generations;
BEGIN
 j:=knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 SELECT * INTO g FROM knowledge.parse_generations WHERE id=p_parse_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_FOUND'; END IF;
 IF (g.creating_job_id,g.creating_job_epoch,g.document_version_id) IS DISTINCT FROM (j.id,p_epoch,j.version_id) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PARSE_OWNERSHIP_MISMATCH'; END IF;
 RETURN g;
END $$;

CREATE FUNCTION knowledge.begin_parse(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,
 p_parser_fingerprint TEXT,p_normalizer_version TEXT,p_structure_version TEXT)
RETURNS knowledge.parse_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; g knowledge.parse_generations; source_sha TEXT;
BEGIN
 IF p_operation_id IS NULL OR p_parser_fingerprint IS NULL OR length(btrim(p_parser_fingerprint)) NOT BETWEEN 1 AND 512 OR
  p_normalizer_version IS NULL OR length(btrim(p_normalizer_version)) NOT BETWEEN 1 AND 512 OR
  p_structure_version IS NULL OR length(btrim(p_structure_version)) NOT BETWEEN 1 AND 512 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PARSE_ARGUMENT'; END IF;
 j:=knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 SELECT * INTO g FROM knowledge.parse_generations WHERE creating_job_id=p_job_id AND creating_job_epoch=p_epoch AND operation_id=p_operation_id;
 IF FOUND THEN
  IF (g.parser_fingerprint,g.normalizer_version,g.structure_version) IS DISTINCT FROM (p_parser_fingerprint,p_normalizer_version,p_structure_version) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 SELECT source_sha256 INTO STRICT source_sha FROM app.document_versions WHERE id=j.version_id;
 INSERT INTO knowledge.parse_generations(document_version_id,source_sha256,parser_fingerprint,normalizer_version,structure_version,
  creating_job_id,creating_job_epoch,operation_id)
 VALUES(j.version_id,source_sha,p_parser_fingerprint,p_normalizer_version,p_structure_version,p_job_id,p_epoch,p_operation_id) RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;

CREATE TABLE app.parse_artifact_intents (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), artifact_object_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
 parse_generation_id UUID NOT NULL, document_version_id UUID NOT NULL, creating_job_id UUID NOT NULL, creating_job_epoch BIGINT NOT NULL,
 artifact_role TEXT NOT NULL CHECK(artifact_role IN ('canonical','source_crop')),
 slot TEXT NOT NULL CHECK(slot ~ '^[A-Za-z0-9_.:-]{1,200}$'),
 sha256 app.sha256 NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 268435456),
 bucket TEXT NOT NULL CHECK(bucket='artifacts'), object_key TEXT NOT NULL UNIQUE, object_version_id TEXT,
 media_type TEXT NOT NULL CHECK(media_type IN ('application/json','image/png')),
 state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','attached','cleanup_pending','cleaned')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), expires_at TIMESTAMPTZ NOT NULL, attached_at TIMESTAMPTZ,
 cleanup_owner UUID, cleanup_token UUID, cleanup_until TIMESTAMPTZ, cleaned_at TIMESTAMPTZ, next_cleanup_check_at TIMESTAMPTZ,
 UNIQUE(parse_generation_id,artifact_role,slot),
 FOREIGN KEY(parse_generation_id,document_version_id,creating_job_id,creating_job_epoch)
  REFERENCES knowledge.parse_generations(id,document_version_id,creating_job_id,creating_job_epoch) ON DELETE RESTRICT,
 CHECK((artifact_role='canonical' AND slot='document' AND media_type='application/json') OR (artifact_role='source_crop' AND media_type='image/png')),
 CHECK(object_key='parses/'||document_version_id::text||'/'||parse_generation_id::text||'/'||id::text||'/'||sha256||
  CASE WHEN artifact_role='canonical' THEN '.json' ELSE '.png' END),
 CHECK(isfinite(expires_at) AND expires_at>created_at),
 CHECK((state='attached')=(attached_at IS NOT NULL)), CHECK((state='cleaned')=(cleaned_at IS NOT NULL)),
 CHECK((state='cleaned')=(next_cleanup_check_at IS NOT NULL)), CHECK(next_cleanup_check_at IS NULL OR isfinite(next_cleanup_check_at)),
 CHECK(state<>'cleanup_pending' OR (cleanup_owner IS NOT NULL AND cleanup_token IS NOT NULL AND cleanup_until IS NOT NULL))
);
CREATE INDEX parse_artifact_intents_cleanup ON app.parse_artifact_intents(state,expires_at);
CREATE INDEX parse_artifact_intents_audit ON app.parse_artifact_intents(next_cleanup_check_at,id) WHERE state='cleaned';
CREATE FUNCTION app.guard_parse_artifact_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['state','object_version_id','attached_at','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])<>
    (to_jsonb(OLD)-ARRAY['state','object_version_id','attached_at','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at']) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='ARTIFACT_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.state='attached' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='ARTIFACT_TERMINAL'; END IF;
 IF OLD.state='cleaned' AND NEW IS DISTINCT FROM OLD AND (
  NEW.state<>'cleanup_pending' OR NEW.cleanup_token IS NOT DISTINCT FROM OLD.cleanup_token OR
  NEW.cleaned_at IS NOT NULL OR NEW.next_cleanup_check_at IS NOT NULL OR
  (to_jsonb(NEW)-ARRAY['state','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])<>
  (to_jsonb(OLD)-ARRAY['state','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='ARTIFACT_TERMINAL'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER parse_artifact_identity_guard BEFORE UPDATE ON app.parse_artifact_intents
 FOR EACH ROW EXECUTE FUNCTION app.guard_parse_artifact_identity();

CREATE FUNCTION app.reserve_parse_artifact(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID,
 p_artifact_role TEXT,p_slot TEXT,p_sha256 TEXT,p_size_bytes BIGINT,p_bucket TEXT,p_ttl_seconds INTEGER DEFAULT 3600)
RETURNS app.parse_artifact_intents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.parse_generations; u app.parse_artifact_intents; identity UUID:=gen_random_uuid();
BEGIN
 IF p_artifact_role IS NULL OR p_artifact_role NOT IN ('canonical','source_crop') OR p_slot IS NULL OR p_slot!~'^[A-Za-z0-9_.:-]{1,200}$' OR
  (p_artifact_role='canonical' AND p_slot<>'document') OR p_sha256 IS NULL OR p_sha256!~'^[0-9a-f]{64}$' OR
  p_size_bytes IS NULL OR p_size_bytes NOT BETWEEN 1 AND 268435456 OR p_bucket IS DISTINCT FROM 'artifacts' OR
  p_ttl_seconds IS NULL OR p_ttl_seconds NOT BETWEEN 60 AND 86400 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_ARTIFACT_ARGUMENT'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,p_parse_id);
 SELECT * INTO u FROM app.parse_artifact_intents WHERE parse_generation_id=p_parse_id AND artifact_role=p_artifact_role AND slot=p_slot;
 IF FOUND THEN
  IF (u.sha256,u.size_bytes,u.bucket) IS DISTINCT FROM (p_sha256,p_size_bytes,p_bucket) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  IF u.state IN ('cleanup_pending','cleaned') OR (u.state='reserved' AND u.expires_at<=clock_timestamp()) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_UPLOAD_EXPIRED'; END IF;
  RETURN u;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 INSERT INTO app.parse_artifact_intents(id,parse_generation_id,document_version_id,creating_job_id,creating_job_epoch,
  artifact_role,slot,sha256,size_bytes,bucket,object_key,media_type,expires_at)
 VALUES(identity,g.id,g.document_version_id,p_job_id,p_epoch,p_artifact_role,p_slot,p_sha256,p_size_bytes,p_bucket,
  'parses/'||g.document_version_id::text||'/'||g.id::text||'/'||identity::text||'/'||p_sha256||CASE WHEN p_artifact_role='canonical' THEN '.json' ELSE '.png' END,
  CASE WHEN p_artifact_role='canonical' THEN 'application/json' ELSE 'image/png' END,clock_timestamp()+make_interval(secs=>p_ttl_seconds)) RETURNING * INTO u;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN u;
END $$;
CREATE FUNCTION app.attach_parse_artifact(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_intent_id UUID,
 p_object_version_id TEXT,p_verified_sha256 TEXT,p_verified_size BIGINT)
RETURNS app.parse_artifact_intents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.parse_artifact_intents; g knowledge.parse_generations;
BEGIN
 SELECT * INTO u FROM app.parse_artifact_intents WHERE id=p_intent_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_NOT_FOUND'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,u.parse_generation_id);
 SELECT * INTO STRICT u FROM app.parse_artifact_intents WHERE id=p_intent_id FOR UPDATE;
 IF (u.sha256,u.size_bytes) IS DISTINCT FROM (p_verified_sha256,p_verified_size) OR
  (p_object_version_id IS NOT NULL AND (length(p_object_version_id) NOT BETWEEN 1 AND 1024 OR p_object_version_id ~ '[[:cntrl:]]')) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_VERIFICATION_FAILED'; END IF;
 IF u.state='attached' THEN
  IF u.object_version_id IS DISTINCT FROM p_object_version_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_VERIFICATION_FAILED'; END IF;
  RETURN u;
 END IF;
 IF u.state<>'reserved' OR u.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_UPLOAD_EXPIRED'; END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 IF u.artifact_role='canonical' AND g.artifact_object_id IS NOT NULL AND g.artifact_object_id<>u.artifact_object_id THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='ARTIFACT_IDENTITY_IMMUTABLE'; END IF;
 INSERT INTO app.stored_objects(id,bucket,object_key,object_version_id,media_type,size_bytes,sha256,kind,state)
 VALUES(u.artifact_object_id,u.bucket,u.object_key,p_object_version_id,u.media_type,u.size_bytes,u.sha256,'parse_artifact','attached');
 UPDATE app.parse_artifact_intents SET state='attached',attached_at=clock_timestamp(),object_version_id=p_object_version_id WHERE id=u.id RETURNING * INTO u;
 IF u.artifact_role='canonical' THEN UPDATE knowledge.parse_generations SET artifact_object_id=u.artifact_object_id WHERE id=g.id; END IF;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN u;
END $$;

CREATE FUNCTION app.claim_parse_artifact_cleanup(p_owner UUID,p_limit INTEGER,p_lease_seconds INTEGER DEFAULT 60)
RETURNS SETOF app.parse_artifact_intents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.parse_artifact_intents; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
 IF p_owner IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_CLEANUP_CLAIM'; END IF;
 FOR u IN SELECT * FROM app.parse_artifact_intents WHERE
  ((state IN ('reserved','cleanup_pending') AND expires_at<=observed_at-interval '1 hour' AND (cleanup_until IS NULL OR cleanup_until<=observed_at)) OR
   (state='cleaned' AND next_cleanup_check_at<=observed_at))
  AND NOT EXISTS(SELECT 1 FROM knowledge.parse_generations g WHERE g.artifact_object_id=app.parse_artifact_intents.artifact_object_id)
  AND NOT EXISTS(SELECT 1 FROM app.stored_objects s WHERE s.id=app.parse_artifact_intents.artifact_object_id OR (s.bucket=app.parse_artifact_intents.bucket AND s.object_key=app.parse_artifact_intents.object_key))
  ORDER BY CASE WHEN state='cleaned' THEN next_cleanup_check_at ELSE greatest(expires_at+interval '1 hour',cleanup_until) END,id
  FOR UPDATE SKIP LOCKED LIMIT p_limit LOOP
  IF EXISTS(SELECT 1 FROM knowledge.parse_generations WHERE artifact_object_id=u.artifact_object_id) OR
   EXISTS(SELECT 1 FROM app.stored_objects WHERE id=u.artifact_object_id OR (bucket=u.bucket AND object_key=u.object_key)) THEN CONTINUE; END IF;
  UPDATE app.parse_artifact_intents SET state='cleanup_pending',cleanup_owner=p_owner,cleanup_token=gen_random_uuid(),
   cleanup_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),cleaned_at=NULL,next_cleanup_check_at=NULL WHERE id=u.id RETURNING * INTO u;
  RETURN NEXT u;
 END LOOP;
END $$;
CREATE FUNCTION app.mark_parse_artifact_cleaned(p_intent_id UUID,p_owner UUID,p_token UUID) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.parse_artifact_intents; checked_at TIMESTAMPTZ;
BEGIN
 SELECT * INTO u FROM app.parse_artifact_intents WHERE id=p_intent_id FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_token IS NULL OR u.cleanup_owner IS DISTINCT FROM p_owner OR u.cleanup_token IS DISTINCT FROM p_token THEN RETURN false; END IF;
 IF u.state='cleaned' THEN RETURN true; END IF;
 IF u.state<>'cleanup_pending' OR u.cleanup_until IS NULL OR u.cleanup_until<=clock_timestamp() THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM knowledge.parse_generations WHERE artifact_object_id=u.artifact_object_id) OR
  EXISTS(SELECT 1 FROM app.stored_objects WHERE id=u.artifact_object_id OR (bucket=u.bucket AND object_key=u.object_key)) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_STILL_REFERENCED'; END IF;
 checked_at:=clock_timestamp();
 UPDATE app.parse_artifact_intents SET state='cleaned',cleaned_at=checked_at,next_cleanup_check_at=checked_at+interval '1 hour' WHERE id=u.id;
 RETURN true;
END $$;

-- Private JSON shape checks follow parsing/dto.py. Raw block text/geometry and
-- table glyph/coverage validation remain the worker's pinned-artifact duty.
CREATE FUNCTION knowledge._parse_closed(v JSONB,keys TEXT[]) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(v)='object' AND v ?& keys AND (v-keys)='{}'::jsonb,false)
$$;
CREATE FUNCTION knowledge._parse_integer(v JSONB,minimum BIGINT,maximum BIGINT) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'number' OR (v#>>'{}')!~'^[0-9]+$' THEN RETURN false; END IF;
 RETURN (v#>>'{}')::numeric BETWEEN minimum AND maximum;
END $$;
CREATE FUNCTION knowledge._parse_bbox(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(v)<>4 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v) n WHERE jsonb_typeof(n)<>'number') THEN RETURN false; END IF;
 RETURN (v->>0)::numeric>=0 AND (v->>1)::numeric>=0 AND (v->>2)::numeric>(v->>0)::numeric AND (v->>3)::numeric>(v->>1)::numeric;
END $$;
CREATE FUNCTION knowledge._parse_spans(v JSONB,maximum INTEGER DEFAULT 100000) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE s JSONB;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(v)>maximum OR NOT knowledge.valid_spans(v) THEN RETURN false; END IF;
 FOR s IN SELECT value FROM jsonb_array_elements(v) LOOP
  IF NOT knowledge._parse_closed(s,ARRAY['pdf_page','printed_page_label','block_id','start_offset','end_offset','bbox']) OR
   NOT knowledge._parse_integer(s->'pdf_page',1,500) OR length(s->>'block_id')>200 OR
   (s->>'start_offset')::bigint>=(s->>'end_offset')::bigint OR
   (s->'printed_page_label'<>'null'::jsonb AND (jsonb_typeof(s->'printed_page_label')<>'string' OR length(s->>'printed_page_label')>100)) OR
   (s->'bbox'<>'null'::jsonb AND NOT knowledge._parse_bbox(s->'bbox')) THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION knowledge._parse_map(body TEXT,mappings JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE m JSONB; finish BIGINT:=0; span JSONB;
BEGIN
 IF body IS NULL OR jsonb_typeof(mappings) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(mappings)>100000 OR length(body)>2000000 THEN RETURN false; END IF;
 FOR m IN SELECT value FROM jsonb_array_elements(mappings) LOOP
  IF NOT knowledge._parse_closed(m,ARRAY['canonical_start','canonical_end','source_spans','mapping','operation']) OR
   NOT knowledge._parse_integer(m->'canonical_start',0,10000000) OR NOT knowledge._parse_integer(m->'canonical_end',1,10000000) OR
   (m->>'canonical_start')::bigint<>finish OR (m->>'canonical_end')::bigint<=finish OR (m->>'canonical_end')::bigint>length(body) OR
   NOT knowledge._parse_spans(m->'source_spans',1000) OR jsonb_typeof(m->'mapping')<>'string' OR
   m->>'mapping' NOT IN ('exact','normalized_group') OR
   (m->'operation'<>'null'::jsonb AND m->>'operation' NOT IN ('nfc','whitespace','line_join','format_separator','dehyphenate','typography')) THEN RETURN false; END IF;
  IF m->>'mapping'='exact' THEN
   IF jsonb_array_length(m->'source_spans')<>1 OR m->'operation'<>'null'::jsonb THEN RETURN false; END IF;
   span:=m->'source_spans'->0;
   IF (span->>'end_offset')::bigint-(span->>'start_offset')::bigint<>(m->>'canonical_end')::bigint-finish THEN RETURN false; END IF;
  ELSIF jsonb_array_length(m->'source_spans')=0 THEN
   IF m->>'operation' IS DISTINCT FROM 'format_separator' OR
    substring(body FROM finish::integer+1 FOR (m->>'canonical_end')::integer-finish::integer)!~'^[[:space:]]+$' THEN RETURN false; END IF;
  END IF;
  finish:=(m->>'canonical_end')::bigint;
 END LOOP;
 RETURN finish=length(body);
END $$;
CREATE FUNCTION knowledge._parse_context(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE r JSONB;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(v)>1000 THEN RETURN false; END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(v) LOOP
  IF NOT knowledge._parse_closed(r,ARRAY['role','text','source_spans','required']) OR
   jsonb_typeof(r->'role')<>'string' OR r->>'role' NOT IN ('scope','header','unit','note','caption') OR
   jsonb_typeof(r->'text')<>'string' OR length(r->>'text') NOT BETWEEN 1 AND 2000000 OR
   jsonb_typeof(r->'required')<>'boolean' OR NOT knowledge._parse_spans(r->'source_spans',1000) THEN RETURN false; END IF;
  IF jsonb_array_length(r->'source_spans')=0 THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION knowledge._parse_table(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE c JSONB; r JSONB; s JSONB; row_count INTEGER; columns INTEGER; pages INTEGER[];
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['id','kind','cells','rows','column_count','source_block_ids','owned_source_spans','pdf_pages','page_segments','context_refs','continuation_evidence','row_groups']) OR
  jsonb_typeof(v->'id')<>'string' OR length(v->>'id') NOT BETWEEN 1 AND 200 OR
  jsonb_typeof(v->'kind') IS DISTINCT FROM 'string' OR v->>'kind' NOT IN ('data','blank_template') OR
  jsonb_typeof(v->'cells')<>'array' OR jsonb_typeof(v->'rows')<>'array' OR
  jsonb_typeof(v->'source_block_ids')<>'array' OR jsonb_typeof(v->'pdf_pages')<>'array' OR
  jsonb_typeof(v->'page_segments')<>'array' OR jsonb_typeof(v->'continuation_evidence')<>'array' OR jsonb_typeof(v->'row_groups')<>'array' OR
  NOT knowledge._parse_integer(v->'column_count',1,1000) OR NOT knowledge._parse_spans(v->'owned_source_spans') OR
  NOT knowledge._parse_context(v->'context_refs') THEN RETURN false; END IF;
 row_count:=jsonb_array_length(v->'rows'); columns:=(v->>'column_count')::integer;
 IF row_count NOT BETWEEN 1 AND 100000 OR jsonb_array_length(v->'cells') NOT BETWEEN 1 AND 100000 OR
  jsonb_array_length(v->'pdf_pages') NOT BETWEEN 1 AND 500 OR jsonb_array_length(v->'page_segments')>500 OR
  jsonb_array_length(v->'source_block_ids')>100000 OR jsonb_array_length(v->'continuation_evidence')>500 OR jsonb_array_length(v->'row_groups')>10000 OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(v->'source_block_ids') n WHERE jsonb_typeof(n)<>'string' OR length(n#>>'{}') NOT BETWEEN 1 AND 200) OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(v->'continuation_evidence') n WHERE jsonb_typeof(n)<>'string') OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(v->'pdf_pages') n WHERE NOT knowledge._parse_integer(n,1,500)) THEN RETURN false; END IF;
 SELECT array_agg((n#>>'{}')::integer ORDER BY ordinal) INTO pages FROM jsonb_array_elements(v->'pdf_pages') WITH ORDINALITY AS p(n,ordinal);
 IF cardinality(pages)<>(SELECT count(DISTINCT n) FROM unnest(pages) n) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v->'cells') n GROUP BY n->>'id' HAVING count(*)>1) OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(v->'rows') n GROUP BY n->>'row_index' HAVING count(*)>1) THEN RETURN false; END IF;
 FOR c IN SELECT value FROM jsonb_array_elements(v->'cells') LOOP
  IF NOT knowledge._parse_closed(c,ARRAY['id','row','column','row_span','column_span','role','text','char_map','bbox','pdf_page']) OR
   jsonb_typeof(c->'id')<>'string' OR length(c->>'id') NOT BETWEEN 1 AND 200 OR
   NOT knowledge._parse_integer(c->'row',0,10000000) OR NOT knowledge._parse_integer(c->'column',0,columns-1) OR
   NOT knowledge._parse_integer(c->'row_span',1,10000) OR NOT knowledge._parse_integer(c->'column_span',1,1000) OR
   (c->>'column')::integer+(c->>'column_span')::integer>columns OR
   c->>'role' NOT IN ('header','unit','data','note') OR jsonb_typeof(c->'role')<>'string' OR
   jsonb_typeof(c->'text')<>'string' OR NOT knowledge._parse_map(c->>'text',c->'char_map') OR
   (c->'bbox'<>'null'::jsonb AND NOT knowledge._parse_bbox(c->'bbox')) OR
   (c->'pdf_page'<>'null'::jsonb AND (NOT knowledge._parse_integer(c->'pdf_page',1,500) OR NOT (c->>'pdf_page')::integer=ANY(pages))) THEN RETURN false; END IF;
 END LOOP;
 -- Rows describe actual anchors; a cell spanning several rows need not have
 -- another anchor in every covered row. Geometry completeness belongs to the
 -- artifact validator, so use grid extent rather than the number of anchors.
 SELECT max((n->>'row')::integer+(n->>'row_span')::integer) INTO row_count FROM jsonb_array_elements(v->'cells') n;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v->'owned_source_spans') n WHERE NOT (n->>'pdf_page')::integer=ANY(pages)) THEN RETURN false; END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(v->'rows') LOOP
  IF NOT knowledge._parse_closed(r,ARRAY['row_index','cell_ids','context_refs']) OR
   NOT knowledge._parse_integer(r->'row_index',0,row_count-1) OR jsonb_typeof(r->'cell_ids')<>'array' OR
   NOT knowledge._parse_context(r->'context_refs') THEN RETURN false; END IF;
  IF jsonb_array_length(r->'cell_ids') NOT BETWEEN 1 AND 1000 OR
   EXISTS(SELECT 1 FROM jsonb_array_elements(r->'cell_ids') n WHERE jsonb_typeof(n)<>'string' OR NOT EXISTS(
    SELECT 1 FROM jsonb_array_elements(v->'cells') cell WHERE cell->>'id'=n#>>'{}' AND
     (r->>'row_index')::integer BETWEEN (cell->>'row')::integer AND (cell->>'row')::integer+(cell->>'row_span')::integer-1)) OR
   (SELECT count(*)<>count(DISTINCT n) FROM jsonb_array_elements(r->'cell_ids') n) THEN RETURN false; END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v->'cells') cell_entry WHERE NOT EXISTS(
  SELECT 1 FROM jsonb_array_elements(v->'rows') row_entry WHERE row_entry->'cell_ids' ? (cell_entry->>'id'))) THEN RETURN false; END IF;
 FOR s IN SELECT value FROM jsonb_array_elements(v->'page_segments') LOOP
  IF NOT knowledge._parse_closed(s,ARRAY['pdf_page','bbox','first_row','last_row']) OR
   NOT knowledge._parse_integer(s->'pdf_page',1,500) OR NOT (s->>'pdf_page')::integer=ANY(pages) OR
   NOT knowledge._parse_bbox(s->'bbox') OR NOT knowledge._parse_integer(s->'first_row',0,row_count-1) OR
   NOT knowledge._parse_integer(s->'last_row',0,row_count-1) OR (s->>'last_row')::integer<(s->>'first_row')::integer THEN RETURN false; END IF;
 END LOOP;
 FOR s IN SELECT value FROM jsonb_array_elements(v->'row_groups') LOOP
  IF NOT knowledge._parse_closed(s,ARRAY['first_row','last_row','context_refs']) OR
   NOT knowledge._parse_integer(s->'first_row',0,row_count-1) OR NOT knowledge._parse_integer(s->'last_row',0,row_count-1) OR
   (s->>'last_row')::integer<(s->>'first_row')::integer OR NOT knowledge._parse_context(s->'context_refs') THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;
CREATE FUNCTION knowledge._parse_quality(v JSONB,pages INTEGER DEFAULT NULL) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE d JSONB; critical INTEGER:=0; warning INTEGER:=0;
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['schema_version','status','complete_page_count','critical_count','warning_count','diagnostics']) OR
  v->>'schema_version' IS DISTINCT FROM 'p04.quality.v1' OR v->>'status' NOT IN ('passed','failed') OR jsonb_typeof(v->'status')<>'string' OR
  NOT knowledge._parse_integer(v->'complete_page_count',0,500) OR NOT knowledge._parse_integer(v->'critical_count',0,100000) OR
  NOT knowledge._parse_integer(v->'warning_count',0,100000) OR jsonb_typeof(v->'diagnostics')<>'array' THEN RETURN false; END IF;
 IF jsonb_array_length(v->'diagnostics')>100000 OR (pages IS NOT NULL AND (v->>'complete_page_count')::integer<>pages) THEN RETURN false; END IF;
 FOR d IN SELECT value FROM jsonb_array_elements(v->'diagnostics') LOOP
  IF NOT knowledge._parse_closed(d,ARRAY['code','severity','pdf_page','block_id','bbox']) OR
   jsonb_typeof(d->'code')<>'string' OR d->>'code'!~'^[A-Z][A-Z0-9_]{0,79}$' OR
   jsonb_typeof(d->'severity')<>'string' OR d->>'severity' NOT IN ('critical','warning') OR
   (d->'pdf_page'<>'null'::jsonb AND NOT knowledge._parse_integer(d->'pdf_page',1,coalesce(pages,500))) OR
   (d->'block_id'<>'null'::jsonb AND (jsonb_typeof(d->'block_id')<>'string' OR length(d->>'block_id') NOT BETWEEN 1 AND 200)) OR
   (d->'bbox'<>'null'::jsonb AND NOT knowledge._parse_bbox(d->'bbox')) THEN RETURN false; END IF;
  IF d->>'severity'='critical' THEN critical:=critical+1; ELSE warning:=warning+1; END IF;
 END LOOP;
 RETURN critical=(v->>'critical_count')::integer AND warning=(v->>'warning_count')::integer AND ((v->>'status'='passed')=(critical=0));
END $$;

CREATE FUNCTION knowledge._parse_node(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE ignored UUID;
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['id','parent_id','node_type','ordinal','number','title','own_body','char_map','context_refs','table','level','structural_path','page_start','page_end','source_spans','content_hash']) OR
  jsonb_typeof(v->'id')<>'string' OR (v->'parent_id'<>'null'::jsonb AND jsonb_typeof(v->'parent_id')<>'string') OR
  jsonb_typeof(v->'node_type')<>'string' OR v->>'node_type' NOT IN ('document','section','chapter','article','clause','subclause','paragraph','appendix','table','figure','footnote','editorial_note','unknown') OR
  NOT knowledge._parse_integer(v->'ordinal',0,10000000) OR NOT knowledge._parse_integer(v->'level',0,100) OR
  NOT knowledge._parse_integer(v->'page_start',1,500) OR NOT knowledge._parse_integer(v->'page_end',1,500) OR
  (v->>'page_end')::integer<(v->>'page_start')::integer OR
  (v->'number'<>'null'::jsonb AND (jsonb_typeof(v->'number')<>'string' OR length(v->>'number')>200)) OR
  (v->'title'<>'null'::jsonb AND (jsonb_typeof(v->'title')<>'string' OR length(v->>'title')>2000)) OR
  jsonb_typeof(v->'own_body')<>'string' OR NOT knowledge._parse_map(v->>'own_body',v->'char_map') OR
  NOT knowledge._parse_context(v->'context_refs') OR NOT knowledge._parse_spans(v->'source_spans') OR
  jsonb_typeof(v->'structural_path')<>'array' OR jsonb_typeof(v->'content_hash')<>'string' OR v->>'content_hash'!~'^[0-9a-f]{64}$' THEN RETURN false; END IF;
 ignored:=(v->>'id')::uuid; ignored:=(v->>'parent_id')::uuid;
 IF jsonb_array_length(v->'structural_path')>100 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v->'structural_path') p WHERE jsonb_typeof(p)<>'string') OR
  EXISTS(SELECT 1 FROM jsonb_path_query(v,'$.**.source_spans[*]') s WHERE (s->>'pdf_page')::integer NOT BETWEEN (v->>'page_start')::integer AND (v->>'page_end')::integer) THEN RETURN false; END IF;
 IF v->'table'='null'::jsonb THEN
  IF v->>'node_type'='table' OR v->>'content_hash'<>encode(sha256(convert_to(v->>'own_body','UTF8')),'hex') THEN RETURN false; END IF;
 ELSE
  -- Table hash uses the private DTO serializer; jsonb::text is a different
  -- serialization. Immutable full batch digests bind all submitted table data.
  IF v->>'node_type'<>'table' OR v->>'own_body'<>'' OR NOT knowledge._parse_table(v->'table') OR
   EXISTS(SELECT 1 FROM jsonb_array_elements(v->'table'->'pdf_pages') p WHERE (p#>>'{}')::integer NOT BETWEEN (v->>'page_start')::integer AND (v->>'page_end')::integer) THEN RETURN false; END IF;
 END IF;
 RETURN true;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE TABLE knowledge.parse_node_batches (
 parse_generation_id UUID NOT NULL REFERENCES knowledge.parse_generations ON DELETE RESTRICT,
 batch_id UUID NOT NULL, payload_hash app.sha256 NOT NULL, inserted_count INTEGER NOT NULL CHECK(inserted_count BETWEEN 1 AND 500),
 total_count INTEGER NOT NULL CHECK(total_count>=inserted_count), created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(parse_generation_id,batch_id)
);
CREATE FUNCTION knowledge.write_parse_nodes(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID,p_batch_id UUID,p_nodes JSONB)
RETURNS TABLE(inserted_count INTEGER,total_count INTEGER) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.parse_generations; batch knowledge.parse_node_batches; n JSONB; digest TEXT; count_before INTEGER;
BEGIN
 IF p_batch_id IS NULL OR jsonb_typeof(p_nodes) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_NODE_BATCH'; END IF;
 IF jsonb_array_length(p_nodes) NOT BETWEEN 1 AND 500 OR octet_length(p_nodes::text)>8388608 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='NODE_BATCH_LIMIT_EXCEEDED'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,p_parse_id);
 digest:=encode(sha256(convert_to(p_nodes::text,'UTF8')),'hex');
 SELECT * INTO batch FROM knowledge.parse_node_batches WHERE parse_generation_id=p_parse_id AND batch_id=p_batch_id;
 IF FOUND THEN
  IF batch.payload_hash<>digest THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  inserted_count:=batch.inserted_count; total_count:=batch.total_count; RETURN NEXT; RETURN;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 SELECT count(*) INTO count_before FROM knowledge.document_nodes WHERE parse_generation_id=p_parse_id;
 IF count_before+jsonb_array_length(p_nodes)>100000 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='NODE_BATCH_LIMIT_EXCEEDED'; END IF;
 FOR n IN SELECT value FROM jsonb_array_elements(p_nodes) LOOP
  IF NOT knowledge._parse_node(n) THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PARSE_NODE'; END IF;
  IF EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=p_parse_id AND parent_id IS NOT DISTINCT FROM (n->>'parent_id')::uuid AND ordinal=(n->>'ordinal')::integer) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='DUPLICATE_NODE_ORDINAL'; END IF;
  INSERT INTO knowledge.document_nodes(id,parse_generation_id,parent_id,node_type,level,ordinal,number,title,canonical_text,
   structural_path,page_start,page_end,source_spans,table_data,extra_metadata,content_hash)
  VALUES((n->>'id')::uuid,p_parse_id,(n->>'parent_id')::uuid,n->>'node_type',(n->>'level')::integer,(n->>'ordinal')::integer,
   n->>'number',n->>'title',n->>'own_body',n->'structural_path',(n->>'page_start')::integer,(n->>'page_end')::integer,
   n->'source_spans',nullif(n->'table','null'::jsonb),
   jsonb_build_object('schema_version','p04.node.v1','char_map',n->'char_map','context_refs',n->'context_refs'),n->>'content_hash');
 END LOOP;
 inserted_count:=jsonb_array_length(p_nodes); total_count:=count_before+inserted_count;
 INSERT INTO knowledge.parse_node_batches(parse_generation_id,batch_id,payload_hash,inserted_count,total_count)
 VALUES(p_parse_id,p_batch_id,digest,inserted_count,total_count);
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN NEXT;
END $$;

CREATE FUNCTION knowledge.finalize_parse(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID,
 p_expected_node_count INTEGER,p_physical_page_count INTEGER,p_quality_report JSONB)
RETURNS knowledge.parse_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.parse_generations; actual_count BIGINT;
BEGIN
 IF p_expected_node_count IS NULL OR p_expected_node_count NOT BETWEEN 1 AND 100000 OR
  p_physical_page_count IS NULL OR p_physical_page_count NOT BETWEEN 1 AND 500 OR p_quality_report IS NULL OR
  octet_length(p_quality_report::text)>8388608 OR NOT knowledge._parse_quality(p_quality_report,p_physical_page_count) OR
  p_quality_report->>'status' IS DISTINCT FROM 'passed' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PARSE_QUALITY'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,p_parse_id);
 IF g.status='ready' THEN
  IF (g.node_count,g.physical_page_count,g.quality_report) IS DISTINCT FROM (p_expected_node_count,p_physical_page_count,p_quality_report) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 SELECT count(*) INTO actual_count FROM knowledge.document_nodes WHERE parse_generation_id=g.id;
 IF actual_count<>p_expected_node_count OR NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=g.id AND parent_id IS NULL) OR
  EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=g.id AND page_end>p_physical_page_count) OR
  NOT EXISTS(SELECT 1 FROM app.parse_artifact_intents u JOIN app.stored_objects s ON s.id=u.artifact_object_id
   WHERE u.parse_generation_id=g.id AND u.artifact_role='canonical' AND u.state='attached' AND u.artifact_object_id=g.artifact_object_id AND
    s.state='attached' AND s.kind='parse_artifact' AND s.sha256=u.sha256 AND s.size_bytes=u.size_bytes) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PARSE_NOT_COMPLETE'; END IF;
 UPDATE knowledge.parse_generations SET status='ready',node_count=p_expected_node_count,physical_page_count=p_physical_page_count,
  quality_report=p_quality_report,completed_at=clock_timestamp() WHERE id=g.id RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;
CREATE FUNCTION knowledge.fail_parse(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID,p_quality_report JSONB)
RETURNS knowledge.parse_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.parse_generations;
BEGIN
 IF p_quality_report IS NULL OR octet_length(p_quality_report::text)>8388608 OR NOT knowledge._parse_quality(p_quality_report) OR
  p_quality_report->>'status' IS DISTINCT FROM 'failed' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PARSE_QUALITY'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,p_parse_id);
 IF g.status='failed' THEN
  IF g.quality_report IS DISTINCT FROM p_quality_report THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 UPDATE knowledge.parse_generations SET status='failed',node_count=(SELECT count(*) FROM knowledge.document_nodes WHERE parse_generation_id=g.id),
  quality_report=p_quality_report,completed_at=clock_timestamp() WHERE id=g.id RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;

REVOKE ALL ON knowledge.parse_node_batches FROM PUBLIC,expert_backend,expert_runtime,expert_outbox,expert_ingest;
GRANT SELECT ON knowledge.parse_node_batches TO expert_ingest;
REVOKE ALL ON app.parse_artifact_intents FROM PUBLIC,expert_backend,expert_runtime,expert_outbox,expert_ingest;
GRANT SELECT ON app.parse_artifact_intents TO expert_ingest;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA knowledge FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
GRANT EXECUTE ON FUNCTION knowledge.begin_parse(UUID,UUID,BIGINT,UUID,TEXT,TEXT,TEXT),
 knowledge.write_parse_nodes(UUID,UUID,BIGINT,UUID,UUID,JSONB),knowledge.finalize_parse(UUID,UUID,BIGINT,UUID,INTEGER,INTEGER,JSONB),
 knowledge.fail_parse(UUID,UUID,BIGINT,UUID,JSONB),
 app.reserve_parse_artifact(UUID,UUID,BIGINT,UUID,TEXT,TEXT,TEXT,BIGINT,TEXT,INTEGER),
 app.attach_parse_artifact(UUID,UUID,BIGINT,UUID,TEXT,TEXT,BIGINT),
 app.claim_parse_artifact_cleanup(UUID,INTEGER,INTEGER),app.mark_parse_artifact_cleaned(UUID,UUID,UUID) TO expert_ingest;
