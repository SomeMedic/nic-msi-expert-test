ALTER TABLE app.document_versions ADD COLUMN source_metadata JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(source_metadata)='object');
CREATE TABLE app.auth_sessions (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), token_hash app.sha256 NOT NULL UNIQUE,
 principal_id TEXT NOT NULL CHECK(btrim(principal_id)<>''), role TEXT NOT NULL CHECK(role IN ('viewer','operator','admin')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), expires_at TIMESTAMPTZ NOT NULL, revoked_at TIMESTAMPTZ,
 CHECK(expires_at>created_at)
);
CREATE INDEX auth_sessions_expiry ON app.auth_sessions(expires_at);
CREATE FUNCTION app.create_session(p_token_hash TEXT,p_principal_id TEXT,p_role TEXT,p_expires_at TIMESTAMPTZ) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE identity UUID;
BEGIN
 IF p_expires_at IS NULL OR NOT isfinite(p_expires_at) OR p_expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_SESSION_EXPIRY'; END IF;
 INSERT INTO app.auth_sessions(token_hash,principal_id,role,expires_at) VALUES(p_token_hash,p_principal_id,p_role,p_expires_at) RETURNING id INTO identity;
 RETURN identity;
END $$;
CREATE FUNCTION app.lookup_session(p_token_hash TEXT) RETURNS TABLE(session_id UUID,principal_id TEXT,role TEXT,expires_at TIMESTAMPTZ)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT id,principal_id,role,expires_at FROM app.auth_sessions WHERE token_hash=p_token_hash AND revoked_at IS NULL AND expires_at>clock_timestamp()
$$;
CREATE FUNCTION app.revoke_session(p_token_hash TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 UPDATE app.auth_sessions SET revoked_at=coalesce(revoked_at,clock_timestamp()) WHERE token_hash=p_token_hash;
 RETURN FOUND;
END $$;
CREATE FUNCTION app.valid_source_metadata(m JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE begin_date DATE; end_date DATE;
BEGIN
 IF jsonb_typeof(m) IS DISTINCT FROM 'object' OR NOT(m ?& ARRAY['schema_version','title','legal_status','approved_at']) THEN RETURN false; END IF;
 IF (m->>'schema_version') IS DISTINCT FROM '1' OR jsonb_typeof(m->'schema_version')<>'number' OR
  jsonb_typeof(m->'title')<>'string' OR length(btrim(m->>'title')) NOT BETWEEN 1 AND 500 OR
  (m->>'legal_status') NOT IN ('active','archived') OR jsonb_typeof(m->'legal_status')<>'string' OR
  jsonb_typeof(m->'approved_at')<>'string' THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_object_keys(m) k WHERE k NOT IN ('schema_version','title','legal_status','approved_at','document_type','document_number','authority','version_label','edition_at','effective_from','effective_to')) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(m) e WHERE e.key IN ('approved_at','edition_at','effective_from','effective_to') AND e.value<>'null'::jsonb AND
  (jsonb_typeof(e.value)<>'string' OR (e.value#>>'{}')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')) THEN RETURN false; END IF;
 PERFORM (m->>'approved_at')::date;
 IF m->>'approved_at' IS NULL THEN RETURN false; END IF;
 IF m->>'edition_at' IS NOT NULL THEN PERFORM (m->>'edition_at')::date; END IF;
 begin_date:=(m->>'effective_from')::date; end_date:=(m->>'effective_to')::date;
 IF begin_date IS NOT NULL AND end_date IS NOT NULL AND end_date<begin_date THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(m) e WHERE e.key IN ('document_type','document_number','authority','version_label') AND e.value<>'null'::jsonb AND (jsonb_typeof(e.value)<>'string' OR btrim(e.value#>>'{}')='')) THEN RETURN false; END IF;
 IF length(m->>'document_type')>100 OR length(m->>'document_number')>100 OR length(m->>'authority')>300 OR length(m->>'version_label')>200 THEN RETURN false; END IF;
 RETURN true;
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN RETURN false;
END $$;
CREATE TABLE app.upload_intents (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), principal_id TEXT NOT NULL CHECK(btrim(principal_id)<>''),
 idempotency_key TEXT NOT NULL CHECK(length(btrim(idempotency_key)) BETWEEN 1 AND 200), request_hash app.sha256 NOT NULL,
 document_id UUID NOT NULL, is_new_document BOOLEAN NOT NULL, version_id UUID NOT NULL UNIQUE, job_id UUID NOT NULL UNIQUE,
 source_object_id UUID NOT NULL UNIQUE, metadata JSONB NOT NULL CHECK(app.valid_source_metadata(metadata)),
 source_sha256 app.sha256 NOT NULL, size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 52428800),
 original_filename TEXT NOT NULL CHECK(length(btrim(original_filename)) BETWEEN 1 AND 500),
 expected_publication_id UUID REFERENCES app.publications ON DELETE RESTRICT, auto_publish BOOLEAN NOT NULL,
 pipeline_fingerprint TEXT NOT NULL CHECK(btrim(pipeline_fingerprint)<>''), bucket TEXT NOT NULL CHECK(btrim(bucket)<>''),
 object_key TEXT NOT NULL UNIQUE, object_version_id TEXT,
 state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','stored','attached','cleanup_pending','cleaned')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), expires_at TIMESTAMPTZ NOT NULL, stored_at TIMESTAMPTZ, attached_at TIMESTAMPTZ,
 cleanup_owner UUID, cleanup_token UUID, cleanup_until TIMESTAMPTZ, cleaned_at TIMESTAMPTZ, next_cleanup_check_at TIMESTAMPTZ,
 UNIQUE(principal_id,idempotency_key), CHECK(expires_at>created_at), CHECK(NOT is_new_document OR expected_publication_id IS NULL),
 CHECK(object_key='originals/'||document_id::text||'/'||version_id::text||'/'||source_sha256||'.pdf'),
 CHECK(state NOT IN ('stored','attached') OR stored_at IS NOT NULL), CHECK((state='attached')=(attached_at IS NOT NULL)),
 CHECK(state<>'cleanup_pending' OR (cleanup_owner IS NOT NULL AND cleanup_token IS NOT NULL AND cleanup_until IS NOT NULL)),
 CHECK((state='cleaned')=(cleaned_at IS NOT NULL)),
 CHECK((state='cleaned')=(next_cleanup_check_at IS NOT NULL)), CHECK(next_cleanup_check_at IS NULL OR isfinite(next_cleanup_check_at))
);
CREATE INDEX upload_intents_cleanup ON app.upload_intents(state,expires_at);
CREATE INDEX upload_intents_cleanup_audit ON app.upload_intents(next_cleanup_check_at,id) WHERE state='cleaned';
CREATE FUNCTION app.guard_upload_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['state','object_version_id','stored_at','attached_at','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])<>
    (to_jsonb(OLD)-ARRAY['state','object_version_id','stored_at','attached_at','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at']) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='UPLOAD_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.state='attached' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='UPLOAD_TERMINAL'; END IF;
 -- A cleaned reservation remains permanently closed to PUT/attach. Only a new
 -- cleanup claim may reopen its audit work; service roles have no direct DML.
 IF OLD.state='cleaned' AND NEW IS DISTINCT FROM OLD AND (
  NEW.state<>'cleanup_pending' OR NEW.cleanup_token IS NOT DISTINCT FROM OLD.cleanup_token OR
  NEW.cleaned_at IS NOT NULL OR NEW.next_cleanup_check_at IS NOT NULL OR
  (to_jsonb(NEW)-ARRAY['state','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])<>
  (to_jsonb(OLD)-ARRAY['state','cleanup_owner','cleanup_token','cleanup_until','cleaned_at','next_cleanup_check_at'])) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='UPLOAD_TERMINAL'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER upload_identity_guard BEFORE UPDATE ON app.upload_intents FOR EACH ROW EXECUTE FUNCTION app.guard_upload_identity();
CREATE INDEX ingestion_jobs_admission ON app.ingestion_jobs(id) WHERE status IN ('queued','running','retry_wait');
CREATE FUNCTION app._lock_ingestion_admission() RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 -- An old repeatable-read snapshot cannot recount a competing reservation
 -- after waiting on an advisory lock. Admission commands use short RC txs.
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ADMISSION_REQUIRES_READ_COMMITTED'; END IF;
 PERFORM pg_advisory_xact_lock(92608,3);
END $$;
CREATE FUNCTION app._assert_ingestion_capacity(p_max_queued INTEGER,p_existing_job_id UUID DEFAULT NULL) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE occupied BIGINT;
BEGIN
 IF p_max_queued IS NULL OR p_max_queued<1 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INGESTION_CAPACITY'; END IF;
 PERFORM app._lock_ingestion_admission();
 IF p_existing_job_id IS NOT NULL AND EXISTS(SELECT 1 FROM app.ingestion_jobs WHERE id=p_existing_job_id AND status IN ('queued','running','retry_wait')) THEN RETURN; END IF;
 -- One statement snapshot counts attachment's reservation→job transfer once.
 SELECT (SELECT count(*) FROM app.upload_intents WHERE state IN ('reserved','stored') AND expires_at>clock_timestamp())+
        (SELECT count(*) FROM app.ingestion_jobs WHERE status IN ('queued','running','retry_wait')) INTO occupied;
 IF occupied>=p_max_queued THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CAPACITY_EXCEEDED'; END IF;
END $$;
CREATE FUNCTION app.reserve_upload(p_principal_id TEXT,p_key TEXT,p_request_hash TEXT,p_document_id UUID,p_metadata JSONB,
 p_source_sha256 TEXT,p_size_bytes BIGINT,p_filename TEXT,p_expected_publication_id UUID,p_auto_publish BOOLEAN,
 p_pipeline_fingerprint TEXT,p_bucket TEXT,p_ttl_seconds INTEGER DEFAULT 3600,p_max_queued INTEGER DEFAULT 10) RETURNS app.upload_intents
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.upload_intents; doc UUID:=coalesce(p_document_id,gen_random_uuid()); ver UUID:=gen_random_uuid(); current_pub UUID;
BEGIN
 IF p_principal_id IS NULL OR p_key IS NULL OR p_request_hash IS NULL OR p_metadata IS NULL OR p_source_sha256 IS NULL OR
 p_size_bytes IS NULL OR p_filename IS NULL OR p_auto_publish IS NULL OR p_pipeline_fingerprint IS NULL OR p_bucket IS NULL OR
  p_ttl_seconds IS NULL OR p_ttl_seconds NOT BETWEEN 60 AND 86400 OR p_max_queued IS NULL OR p_max_queued<1 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_UPLOAD_ARGUMENT'; END IF;
 -- Serialize the absent-row idempotency race without holding a long object PUT.
 PERFORM pg_advisory_xact_lock(hashtextextended('upload:'||p_principal_id||':'||p_key,0));
 SELECT * INTO u FROM app.upload_intents WHERE principal_id=p_principal_id AND idempotency_key=p_key;
 IF FOUND THEN
  IF u.request_hash<>p_request_hash THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  IF u.state IN ('cleanup_pending','cleaned') OR (u.state IN ('reserved','stored') AND u.expires_at<=clock_timestamp()) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UPLOAD_EXPIRED'; END IF;
  RETURN u;
 END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 PERFORM app._assert_ingestion_capacity(p_max_queued);
 IF p_document_id IS NULL THEN
  IF p_expected_publication_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 ELSE
  SELECT current_publication_id INTO current_pub FROM app.logical_documents WHERE id=p_document_id AND security_revoked_at IS NULL FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_NOT_FOUND'; END IF;
  IF current_pub IS DISTINCT FROM p_expected_publication_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 END IF;
 INSERT INTO app.upload_intents(principal_id,idempotency_key,request_hash,document_id,is_new_document,version_id,job_id,source_object_id,
 metadata,source_sha256,size_bytes,original_filename,expected_publication_id,auto_publish,pipeline_fingerprint,bucket,object_key,expires_at)
 VALUES(p_principal_id,p_key,p_request_hash,doc,p_document_id IS NULL,ver,gen_random_uuid(),gen_random_uuid(),p_metadata,p_source_sha256,
 p_size_bytes,p_filename,p_expected_publication_id,p_auto_publish,p_pipeline_fingerprint,p_bucket,
 'originals/'||doc::text||'/'||ver::text||'/'||p_source_sha256||'.pdf',clock_timestamp()+make_interval(secs=>p_ttl_seconds)) RETURNING * INTO u;
 RETURN u;
END $$;
CREATE FUNCTION app.mark_upload_stored(p_intent_id UUID,p_principal_id TEXT,p_object_version_id TEXT,p_verified_sha256 TEXT,p_verified_size BIGINT)
RETURNS app.upload_intents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.upload_intents;
BEGIN
 SELECT * INTO u FROM app.upload_intents WHERE id=p_intent_id AND principal_id=p_principal_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UPLOAD_NOT_FOUND'; END IF;
 IF u.source_sha256 IS DISTINCT FROM p_verified_sha256 OR u.size_bytes IS DISTINCT FROM p_verified_size THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_VERIFICATION_FAILED'; END IF;
 IF u.state IN ('stored','attached') THEN
  IF u.object_version_id IS DISTINCT FROM p_object_version_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_VERIFICATION_FAILED'; END IF;
  RETURN u;
 END IF;
 IF u.state<>'reserved' OR u.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UPLOAD_EXPIRED'; END IF;
 UPDATE app.upload_intents SET state='stored',stored_at=clock_timestamp(),object_version_id=p_object_version_id WHERE id=p_intent_id RETURNING * INTO u;
 RETURN u;
END $$;
CREATE FUNCTION app._append_ingestion_event(p_job_id UUID,p_type TEXT,p_stage TEXT,p_payload JSONB) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e UUID:=gen_random_uuid(); j app.ingestion_jobs;
BEGIN
 UPDATE app.ingestion_jobs SET last_event_sequence=last_event_sequence+1 WHERE id=p_job_id RETURNING * INTO STRICT j;
 INSERT INTO app.ingestion_events(event_id,job_id,sequence,event_type,stage,attempt,execution_epoch,public_payload)
 VALUES(e,j.id,j.last_event_sequence,p_type,p_stage,greatest(1,j.attempt),j.lease_epoch,p_payload);
 INSERT INTO app.outbox_events(event_id,aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES(e,'ingestion_job',j.id,p_type,'ingestion.events.v1',jsonb_build_object('schema_version',1,'event_id',e,'job_id',j.id,'sequence',j.last_event_sequence));
 RETURN e;
END $$;
CREATE FUNCTION app._dispatch_ingestion(p_job_id UUID,p_available_at TIMESTAMPTZ) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e UUID:=gen_random_uuid();
BEGIN
 INSERT INTO app.outbox_events(event_id,aggregate_type,aggregate_id,event_type,topic,payload,available_at)
 VALUES(e,'ingestion_job',p_job_id,'ingestion.dispatch','ingestion.jobs.v1',jsonb_build_object('schema_version',1,'event_id',e,'job_id',p_job_id),p_available_at);
 RETURN e;
END $$;
CREATE FUNCTION app.attach_upload(p_intent_id UUID,p_principal_id TEXT) RETURNS app.upload_intents
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.upload_intents; d app.logical_documents; metadata_hash TEXT;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 -- Preserve a reserved slot until its queued job commits, including expiry
 -- during the transaction. Other admissions cannot observe a transfer gap.
 PERFORM app._lock_ingestion_admission();
 SELECT * INTO u FROM app.upload_intents WHERE id=p_intent_id AND principal_id=p_principal_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UPLOAD_NOT_FOUND'; END IF;
 SELECT * INTO d FROM app.logical_documents WHERE id=u.document_id FOR UPDATE;
 SELECT * INTO STRICT u FROM app.upload_intents WHERE id=p_intent_id AND principal_id=p_principal_id FOR UPDATE;
 IF u.state='attached' THEN RETURN u; END IF;
 IF u.state<>'stored' OR u.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='UPLOAD_NOT_ATTACHABLE'; END IF;
 IF u.is_new_document THEN
  IF d.id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
  INSERT INTO app.logical_documents(id,canonical_title,document_type,document_number,authority)
  VALUES(u.document_id,u.metadata->>'title',u.metadata->>'document_type',u.metadata->>'document_number',u.metadata->>'authority');
 ELSE
  IF d.id IS NULL OR d.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_NOT_FOUND'; END IF;
  IF d.current_publication_id IS DISTINCT FROM u.expected_publication_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 END IF;
 metadata_hash:=encode(sha256(convert_to(u.metadata::text,'UTF8')),'hex');
 INSERT INTO app.stored_objects(id,bucket,object_key,object_version_id,media_type,size_bytes,sha256,kind,state)
 VALUES(u.source_object_id,u.bucket,u.object_key,u.object_version_id,'application/pdf',u.size_bytes,u.source_sha256,'original','attached');
 INSERT INTO app.document_versions(id,logical_document_id,source_title,approved_at,edition_at,effective_from,effective_to,legal_status,
 version_label,source_object_id,source_sha256,original_filename,content_size,metadata_hash,created_by_subject,source_metadata)
 VALUES(u.version_id,u.document_id,u.metadata->>'title',(u.metadata->>'approved_at')::date,(u.metadata->>'edition_at')::date,
 (u.metadata->>'effective_from')::date,(u.metadata->>'effective_to')::date,u.metadata->>'legal_status',u.metadata->>'version_label',
 u.source_object_id,u.source_sha256,u.original_filename,u.size_bytes,metadata_hash,u.principal_id,u.metadata);
 INSERT INTO app.ingestion_jobs(id,version_id,desired_auto_publish,expected_publication_id,idempotency_key,input_fingerprint,principal_id,stage)
 VALUES(u.job_id,u.version_id,u.auto_publish,u.expected_publication_id,'upload:'||u.idempotency_key,u.request_hash,u.principal_id,'queued');
 PERFORM app._append_ingestion_event(u.job_id,'ingestion.created',NULL,'{}');
 PERFORM app._dispatch_ingestion(u.job_id,clock_timestamp());
 UPDATE app.upload_intents SET state='attached',attached_at=clock_timestamp() WHERE id=u.id RETURNING * INTO u;
 RETURN u;
END $$;
CREATE FUNCTION app.claim_upload_cleanup(p_owner UUID,p_limit INTEGER,p_lease_seconds INTEGER DEFAULT 60)
RETURNS SETOF app.upload_intents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.upload_intents; observed_at TIMESTAMPTZ:=clock_timestamp();
BEGIN
 IF p_owner IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_CLEANUP_CLAIM'; END IF;
 -- A timed-out native PUT can still finish after HEAD 404 and a successful
 -- cleanup mark. Keep exact-key audit tombstones; never rely on HTTP timeout
 -- or unsupported S3 DeleteObject IfMatch as a cross-system write fence.
 FOR u IN SELECT * FROM app.upload_intents WHERE
  ((state IN ('reserved','stored','cleanup_pending') AND expires_at<=observed_at-interval '1 hour'
    AND (cleanup_until IS NULL OR cleanup_until<=observed_at)) OR
   (state='cleaned' AND next_cleanup_check_at<=observed_at))
  AND NOT EXISTS(SELECT 1 FROM app.document_versions v WHERE v.source_object_id=app.upload_intents.source_object_id OR v.id=app.upload_intents.version_id)
  AND NOT EXISTS(SELECT 1 FROM app.stored_objects s WHERE s.id=app.upload_intents.source_object_id OR (s.bucket=app.upload_intents.bucket AND s.object_key=app.upload_intents.object_key))
  ORDER BY CASE WHEN state='cleaned' THEN next_cleanup_check_at ELSE greatest(expires_at+interval '1 hour',cleanup_until) END,id
  FOR UPDATE SKIP LOCKED LIMIT p_limit LOOP
  IF EXISTS(SELECT 1 FROM app.document_versions WHERE source_object_id=u.source_object_id OR id=u.version_id) OR
     EXISTS(SELECT 1 FROM app.stored_objects WHERE id=u.source_object_id OR (bucket=u.bucket AND object_key=u.object_key)) THEN CONTINUE; END IF;
  UPDATE app.upload_intents SET state='cleanup_pending',cleanup_owner=p_owner,cleanup_token=gen_random_uuid(),cleanup_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),
   cleaned_at=NULL,next_cleanup_check_at=NULL
  WHERE id=u.id RETURNING * INTO u;
  RETURN NEXT u;
 END LOOP;
END $$;
CREATE FUNCTION app.mark_upload_cleaned(p_intent_id UUID,p_owner UUID,p_token UUID) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE u app.upload_intents; checked_at TIMESTAMPTZ;
BEGIN
 SELECT * INTO u FROM app.upload_intents WHERE id=p_intent_id FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_token IS NULL OR u.cleanup_owner IS DISTINCT FROM p_owner OR u.cleanup_token IS DISTINCT FROM p_token THEN RETURN false; END IF;
 IF u.state='cleaned' THEN RETURN true; END IF;
 IF u.state<>'cleanup_pending' OR u.cleanup_until IS NULL OR u.cleanup_until<=clock_timestamp() THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM app.document_versions WHERE source_object_id=u.source_object_id OR id=u.version_id) OR
    EXISTS(SELECT 1 FROM app.stored_objects WHERE id=u.source_object_id OR (bucket=u.bucket AND object_key=u.object_key)) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='OBJECT_STILL_REFERENCED'; END IF;
 checked_at:=clock_timestamp();
 UPDATE app.upload_intents SET state='cleaned',cleaned_at=checked_at,next_cleanup_check_at=checked_at+interval '1 hour' WHERE id=u.id;
 RETURN true;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.create_session(TEXT,TEXT,TEXT,TIMESTAMPTZ),app.lookup_session(TEXT),app.revoke_session(TEXT),
 app.reserve_upload(TEXT,TEXT,TEXT,UUID,JSONB,TEXT,BIGINT,TEXT,UUID,BOOLEAN,TEXT,TEXT,INTEGER,INTEGER),
 app.mark_upload_stored(UUID,TEXT,TEXT,TEXT,BIGINT),app.attach_upload(UUID,TEXT),app.claim_upload_cleanup(UUID,INTEGER,INTEGER),app.mark_upload_cleaned(UUID,UUID,UUID) TO expert_backend;
GRANT SELECT ON app.upload_intents,app.stored_objects TO expert_backend;
