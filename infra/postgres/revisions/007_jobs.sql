ALTER TABLE app.ingestion_jobs ADD COLUMN pipeline_fingerprint TEXT NOT NULL DEFAULT 'legacy-p02',
 ADD COLUMN last_failure_retryable BOOLEAN NOT NULL DEFAULT false,
 ADD COLUMN last_dispatch_at TIMESTAMPTZ,
 ADD COLUMN last_dispatch_event_id UUID REFERENCES app.outbox_events ON DELETE RESTRICT,
 ADD COLUMN stage_started_at TIMESTAMPTZ,
 ADD COLUMN stage_completed_at TIMESTAMPTZ,
 ADD COLUMN progress_unit TEXT CHECK(progress_unit IN ('pages','chunks')),
 ADD COLUMN completed_index_generation_id UUID,
 ADD COLUMN completion_operation_id UUID,
 ADD UNIQUE(id,version_id),
 ADD FOREIGN KEY(completed_index_generation_id,version_id) REFERENCES knowledge.index_generations(id,document_version_id) ON DELETE RESTRICT;
ALTER TABLE knowledge.parse_generations ADD COLUMN creating_job_id UUID, ADD COLUMN creating_job_epoch BIGINT,
 ADD CHECK((creating_job_id IS NULL)=(creating_job_epoch IS NULL)), ADD CHECK(creating_job_epoch>0),
 ADD FOREIGN KEY(creating_job_id,document_version_id) REFERENCES app.ingestion_jobs(id,version_id) ON DELETE RESTRICT;
ALTER TABLE knowledge.index_generations ADD COLUMN creating_job_id UUID, ADD COLUMN creating_job_epoch BIGINT,
 ADD CHECK((creating_job_id IS NULL)=(creating_job_epoch IS NULL)), ADD CHECK(creating_job_epoch>0),
 ADD FOREIGN KEY(creating_job_id,document_version_id) REFERENCES app.ingestion_jobs(id,version_id) ON DELETE RESTRICT;
CREATE FUNCTION app.guard_ingestion_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE pipeline TEXT;
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.pipeline_fingerprint='legacy-p02' THEN
   SELECT pipeline_fingerprint INTO pipeline FROM app.upload_intents WHERE job_id=NEW.id;
   IF FOUND THEN NEW.pipeline_fingerprint:=pipeline; END IF;
  END IF;
  RETURN NEW;
 END IF;
 IF (NEW.id,NEW.version_id,NEW.principal_id,NEW.idempotency_key,NEW.input_fingerprint,NEW.pipeline_fingerprint,NEW.desired_auto_publish,NEW.expected_publication_id)
  IS DISTINCT FROM (OLD.id,OLD.version_id,OLD.principal_id,OLD.idempotency_key,OLD.input_fingerprint,OLD.pipeline_fingerprint,OLD.desired_auto_publish,OLD.expected_publication_id) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INGESTION_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.status IN ('completed','cancelled') AND (to_jsonb(NEW)-'last_event_sequence')<>(to_jsonb(OLD)-'last_event_sequence') THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INGESTION_TERMINAL'; END IF;
 IF NEW.lease_epoch<OLD.lease_epoch OR (NEW.lease_owner IS DISTINCT FROM OLD.lease_owner AND NEW.lease_owner IS NOT NULL AND NEW.lease_epoch<=OLD.lease_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 IF OLD.cancel_requested_at IS NOT NULL AND NEW.cancel_requested_at IS DISTINCT FROM OLD.cancel_requested_at THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='CANCEL_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER ingestion_identity_guard BEFORE INSERT OR UPDATE ON app.ingestion_jobs FOR EACH ROW EXECUTE FUNCTION app.guard_ingestion_identity();
CREATE OR REPLACE FUNCTION app._dispatch_ingestion(p_job_id UUID,p_available_at TIMESTAMPTZ) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e UUID:=gen_random_uuid();
BEGIN
 INSERT INTO app.outbox_events(event_id,aggregate_type,aggregate_id,event_type,topic,payload,available_at)
 VALUES(e,'ingestion_job',p_job_id,'ingestion.dispatch','ingestion.jobs.v1',jsonb_build_object('schema_version',1,'event_id',e,'job_id',p_job_id),p_available_at);
 UPDATE app.ingestion_jobs SET last_dispatch_at=clock_timestamp(),last_dispatch_event_id=e WHERE id=p_job_id;
 RETURN e;
END $$;
CREATE FUNCTION app._ingestion_error_payload(p_code TEXT,p_message TEXT,p_retryable BOOLEAN) RETURNS JSONB
LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('error',jsonb_build_object('code',p_code,'message',p_message,'retryable',p_retryable,'request_id',gen_random_uuid(),'details','{}'::jsonb))
$$;
CREATE FUNCTION app._assert_ingestion_write(p_job_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS app.ingestion_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_FOUND'; END IF;
 IF p_owner IS NULL OR p_epoch IS NULL OR j.lease_owner IS DISTINCT FROM p_owner OR j.lease_epoch<>p_epoch THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_INGESTION'; END IF;
 IF j.cancel_requested_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CANCEL_REQUESTED'; END IF;
 IF j.status<>'running' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_RUNNING'; END IF;
 IF j.lease_until IS NULL OR j.lease_until<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='LEASE_EXPIRED'; END IF;
 IF EXISTS(SELECT 1 FROM app.document_versions v JOIN app.logical_documents d ON d.id=v.logical_document_id WHERE v.id=j.version_id AND d.security_revoked_at IS NOT NULL) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 RETURN j;
END $$;
CREATE TABLE app.ingestion_write_bindings (
 transaction_id XID8 PRIMARY KEY, backend_pid INTEGER NOT NULL, job_id UUID NOT NULL REFERENCES app.ingestion_jobs ON DELETE RESTRICT,
 lease_owner UUID NOT NULL, lease_epoch BIGINT NOT NULL CHECK(lease_epoch>0), created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ingestion_write_bindings_pid ON app.ingestion_write_bindings(backend_pid);
CREATE FUNCTION app.guard_ingestion_write(p_job_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; binding app.ingestion_write_bindings;
BEGIN
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 SELECT * INTO binding FROM app.ingestion_write_bindings WHERE transaction_id=pg_current_xact_id();
 IF FOUND THEN
  IF (binding.job_id,binding.lease_owner,binding.lease_epoch,binding.backend_pid) IS DISTINCT FROM (p_job_id,p_owner,p_epoch,pg_backend_pid()) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 ELSE
  DELETE FROM app.ingestion_write_bindings WHERE backend_pid=pg_backend_pid();
  INSERT INTO app.ingestion_write_bindings(transaction_id,backend_pid,job_id,lease_owner,lease_epoch) VALUES(pg_current_xact_id(),pg_backend_pid(),p_job_id,p_owner,p_epoch);
 END IF;
 RETURN j.lease_epoch;
END $$;
CREATE FUNCTION app.acquire_ingestion(p_job_id UUID,p_owner UUID,p_lease_seconds INTEGER)
RETURNS TABLE(outcome TEXT,job_id UUID,lease_epoch BIGINT,status TEXT,attempt INTEGER,lease_until TIMESTAMPTZ)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; ts TIMESTAMPTZ;
BEGIN
 IF p_owner IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_LEASE'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_FOUND'; END IF;
 ts:=clock_timestamp();
 IF j.status IN ('completed','failed','cancelled') THEN outcome:='already_terminal';
 ELSIF j.cancel_requested_at IS NOT NULL THEN outcome:='cancel_requested';
 ELSIF j.lease_until>ts THEN outcome:='already_running';
 ELSIF j.available_at>ts THEN outcome:='not_available';
 ELSIF j.attempt>=j.max_attempts THEN
  UPDATE app.ingestion_jobs SET status='failed',finished_at=ts,error_code='DEPENDENCY_UNAVAILABLE',safe_error_message='Исчерпан лимит попыток обработки',last_failure_retryable=false WHERE id=p_job_id;
  PERFORM app._append_ingestion_event(p_job_id,'ingestion.failed',j.stage,app._ingestion_error_payload('DEPENDENCY_UNAVAILABLE','Исчерпан лимит попыток обработки',false));
  SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
  outcome:='already_terminal';
 ELSE
  UPDATE app.ingestion_jobs SET status='running',lease_owner=p_owner,lease_epoch=j.lease_epoch+1,attempt=j.attempt+1,
   lease_until=ts+make_interval(secs=>p_lease_seconds),heartbeat_at=ts,started_at=coalesce(j.started_at,ts),finished_at=NULL,
   stage='queued',stage_started_at=ts,stage_completed_at=NULL,processed_units=0,total_units=NULL,error_code=NULL,safe_error_message=NULL WHERE id=p_job_id;
  PERFORM app._append_ingestion_event(p_job_id,'stage.started','queued','{}');
  SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
  outcome:='acquired';
 END IF;
 job_id:=j.id; lease_epoch:=j.lease_epoch; status:=j.status; attempt:=j.attempt; lease_until:=j.lease_until; RETURN NEXT;
END $$;
CREATE FUNCTION app.heartbeat_ingestion(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_lease_seconds INTEGER) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; expires TIMESTAMPTZ;
BEGIN
 IF p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_LEASE'; END IF;
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 expires:=clock_timestamp()+make_interval(secs=>p_lease_seconds);
 UPDATE app.ingestion_jobs SET lease_until=expires,heartbeat_at=clock_timestamp() WHERE id=j.id;
 RETURN expires;
END $$;
CREATE FUNCTION app.advance_ingestion(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_stage TEXT,p_event_type TEXT,p_processed BIGINT,p_total BIGINT)
RETURNS app.ingestion_jobs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; payload JSONB; unit TEXT;
BEGIN
 IF p_stage IS NULL OR p_stage NOT IN ('queued','validating','parsing','assessing_extraction','fallback_parsing','normalizing','building_structure','chunking','embedding','indexing','ready_to_publish','publishing') OR
  p_event_type IS NULL OR p_event_type NOT IN ('stage.started','stage.progress','stage.completed') OR
  p_processed IS NULL OR p_processed<0 OR (p_total IS NOT NULL AND p_total<p_processed) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_STAGE_EVENT'; END IF;
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 unit:=CASE WHEN p_stage IN ('chunking','embedding','indexing','ready_to_publish','publishing') THEN 'chunks' ELSE 'pages' END;
 IF p_event_type='stage.started' THEN
  IF j.stage=p_stage AND j.stage_started_at IS NOT NULL AND j.stage_completed_at IS NULL THEN RETURN j; END IF;
  UPDATE app.ingestion_jobs SET stage=p_stage,stage_started_at=clock_timestamp(),stage_completed_at=NULL,processed_units=p_processed,total_units=p_total,progress_unit=unit WHERE id=j.id;
  payload:='{}';
 ELSE
  IF j.stage IS DISTINCT FROM p_stage OR j.stage_started_at IS NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STAGE_NOT_STARTED'; END IF;
  IF j.stage_completed_at IS NOT NULL THEN
   IF p_event_type='stage.completed' THEN RETURN j; END IF;
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STAGE_ALREADY_COMPLETED';
  END IF;
  IF p_processed<j.processed_units THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PROGRESS_REGRESSION'; END IF;
  IF p_event_type='stage.progress' AND p_processed=j.processed_units AND p_total IS NOT DISTINCT FROM j.total_units THEN RETURN j; END IF;
  UPDATE app.ingestion_jobs SET processed_units=p_processed,total_units=p_total,progress_unit=unit,
   stage_completed_at=CASE WHEN p_event_type='stage.completed' THEN clock_timestamp() ELSE NULL END WHERE id=j.id;
  payload:=CASE WHEN p_event_type='stage.progress' THEN jsonb_build_object('processed_units',p_processed,'total_units',p_total,'unit',unit)
   ELSE jsonb_build_object('duration_ms',greatest(0,floor(extract(epoch FROM clock_timestamp()-j.stage_started_at)*1000)::bigint)) END;
 END IF;
 PERFORM app._append_ingestion_event(j.id,p_event_type,p_stage,payload);
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
 RETURN j;
END $$;
CREATE FUNCTION app.fail_ingestion(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_error_code TEXT,p_safe_message TEXT,p_retryable BOOLEAN,p_retry_delay_seconds INTEGER)
RETURNS app.ingestion_jobs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; retry BOOLEAN;
BEGIN
 IF p_error_code IS NULL OR p_error_code NOT IN ('PDF_INVALID','EXTRACTION_QUALITY_FAILED','GENERATION_INVALID','DEPENDENCY_UNAVAILABLE','MODEL_UNAVAILABLE','MODEL_TIMEOUT','TOKEN_LIMIT_EXCEEDED','VERSION_CONFLICT','SOURCE_REVOKED','SOURCE_UNAVAILABLE','INTERNAL_ERROR','DEADLINE_EXCEEDED','SIZE_LIMIT_EXCEEDED') OR
  p_safe_message IS NULL OR length(btrim(p_safe_message)) NOT BETWEEN 1 AND 1000 OR p_retryable IS NULL OR
  p_retry_delay_seconds IS NULL OR p_retry_delay_seconds NOT BETWEEN 0 AND 3600 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_FAILURE'; END IF;
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 retry:=p_retryable AND j.attempt<j.max_attempts AND p_error_code NOT IN ('PDF_INVALID','EXTRACTION_QUALITY_FAILED','GENERATION_INVALID','TOKEN_LIMIT_EXCEEDED','VERSION_CONFLICT','SOURCE_REVOKED','SIZE_LIMIT_EXCEEDED');
 UPDATE app.ingestion_jobs SET status=CASE WHEN retry THEN 'retry_wait' ELSE 'failed' END,
  finished_at=CASE WHEN retry THEN NULL ELSE clock_timestamp() END,available_at=clock_timestamp()+make_interval(secs=>p_retry_delay_seconds),
  error_code=p_error_code,safe_error_message=p_safe_message,last_failure_retryable=retry,lease_until=NULL WHERE id=j.id;
 IF retry THEN
  -- The public ingestion event vocabulary has no retry event. Preserve the
  -- durable job transition and dispatch; final failure alone emits failed.
  PERFORM app._dispatch_ingestion(j.id,clock_timestamp()+make_interval(secs=>p_retry_delay_seconds));
 ELSE
  PERFORM app._append_ingestion_event(j.id,'ingestion.failed',j.stage,app._ingestion_error_payload(p_error_code,p_safe_message,false));
 END IF;
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
 RETURN j;
END $$;
CREATE FUNCTION app.cancel_ingestion(p_job_id UUID,p_principal_id TEXT) RETURNS app.ingestion_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs;
BEGIN
 IF p_principal_id IS NULL OR btrim(p_principal_id)='' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PRINCIPAL'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_FOUND'; END IF;
 IF j.status IN ('completed','failed','cancelled') THEN RETURN j; END IF;
 UPDATE app.ingestion_jobs SET status='cancelled',cancel_requested_at=clock_timestamp(),finished_at=clock_timestamp(),lease_until=NULL WHERE id=j.id;
 PERFORM app._append_ingestion_event(j.id,'ingestion.cancelled',j.stage,'{}');
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
 RETURN j;
END $$;
CREATE FUNCTION app.request_ingestion_retry(p_job_id UUID,p_principal_id TEXT,p_max_queued INTEGER DEFAULT 10) RETURNS app.ingestion_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs;
BEGIN
 IF p_principal_id IS NULL OR btrim(p_principal_id)='' THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PRINCIPAL'; END IF;
 IF p_max_queued IS NULL OR p_max_queued<1 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INGESTION_CAPACITY'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 PERFORM app._lock_ingestion_admission();
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_FOUND'; END IF;
 IF j.cancel_requested_at IS NOT NULL OR j.status NOT IN ('failed','retry_wait') OR NOT j.last_failure_retryable OR j.attempt>=j.max_attempts THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='TERMINAL_CONFLICT'; END IF;
 PERFORM app._assert_ingestion_capacity(p_max_queued,p_job_id);
 UPDATE app.ingestion_jobs SET status='retry_wait',available_at=clock_timestamp(),finished_at=NULL WHERE id=j.id;
 PERFORM app._dispatch_ingestion(j.id,clock_timestamp());
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
 RETURN j;
END $$;
CREATE FUNCTION app.complete_ingestion(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_generation_id UUID,p_operation_id UUID)
RETURNS app.ingestion_jobs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; pub UUID;
BEGIN
 IF p_index_generation_id IS NULL OR p_operation_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_COMPLETION'; END IF;
 -- Take the exclusive barrier first: never upgrade a shared barrier while
 -- holding a job lock and competing with another publishing worker.
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id;
 IF j.status='completed' THEN
  IF j.lease_owner IS DISTINCT FROM p_owner OR j.lease_epoch IS DISTINCT FROM p_epoch OR j.completed_index_generation_id IS DISTINCT FROM p_index_generation_id OR j.completion_operation_id IS DISTINCT FROM p_operation_id THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_INGESTION'; END IF;
  RETURN j;
 END IF;
 -- Publication takes document then job. Lock the document before asserting job.
 PERFORM 1 FROM app.logical_documents d JOIN app.document_versions v ON v.logical_document_id=d.id WHERE v.id=j.version_id FOR UPDATE OF d;
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 IF NOT EXISTS(SELECT 1 FROM knowledge.index_generations g JOIN knowledge.parse_generations p ON p.id=g.parse_generation_id
  WHERE g.id=p_index_generation_id AND g.document_version_id=j.version_id AND g.status='ready' AND p.status='ready'
   AND (g.creating_job_id IS NULL OR g.creating_job_id=j.id)) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_READY'; END IF;
 PERFORM app._append_ingestion_event(j.id,'ingestion.ready_to_publish','ready_to_publish',jsonb_build_object('version_id',j.version_id,'index_generation_id',p_index_generation_id));
 IF j.desired_auto_publish THEN pub:=app.publish_version(j.version_id,p_index_generation_id,j.expected_publication_id,p_operation_id,j.id,p_owner,p_epoch); END IF;
 UPDATE app.ingestion_jobs SET status='completed',stage=CASE WHEN desired_auto_publish THEN 'publishing' ELSE 'ready_to_publish' END,
  finished_at=clock_timestamp(),completed_index_generation_id=p_index_generation_id,completion_operation_id=p_operation_id WHERE id=j.id;
 PERFORM app._append_ingestion_event(j.id,'ingestion.completed',NULL,jsonb_build_object('version_id',j.version_id,'index_generation_id',p_index_generation_id,'publication_id',pub));
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=p_job_id;
 RETURN j;
END $$;
CREATE FUNCTION app.request_reindex(p_version_id UUID,p_principal_id TEXT,p_key TEXT,p_request_hash TEXT,p_pipeline_fingerprint TEXT,
 p_expected_publication_id UUID,p_auto_publish BOOLEAN,p_max_queued INTEGER DEFAULT 10) RETURNS app.ingestion_jobs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; document UUID; current_pub UUID;
BEGIN
 IF p_version_id IS NULL OR p_principal_id IS NULL OR p_key IS NULL OR p_request_hash IS NULL OR p_pipeline_fingerprint IS NULL OR p_auto_publish IS NULL OR p_max_queued IS NULL OR p_max_queued<1 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_REINDEX_ARGUMENT'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('reindex:'||p_principal_id||':'||p_key,0));
 SELECT * INTO j FROM app.ingestion_jobs WHERE principal_id=p_principal_id AND idempotency_key='reindex:'||p_key;
 IF FOUND THEN
  IF j.input_fingerprint<>p_request_hash OR j.version_id<>p_version_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN j;
 END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 PERFORM app._assert_ingestion_capacity(p_max_queued);
 SELECT logical_document_id INTO document FROM app.document_versions WHERE id=p_version_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_NOT_FOUND'; END IF;
 SELECT current_publication_id INTO current_pub FROM app.logical_documents WHERE id=document AND security_revoked_at IS NULL FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 IF current_pub IS DISTINCT FROM p_expected_publication_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 INSERT INTO app.ingestion_jobs(version_id,desired_auto_publish,expected_publication_id,idempotency_key,input_fingerprint,principal_id,pipeline_fingerprint,stage)
 VALUES(p_version_id,p_auto_publish,p_expected_publication_id,'reindex:'||p_key,p_request_hash,p_principal_id,p_pipeline_fingerprint,'queued') RETURNING * INTO j;
 PERFORM app._append_ingestion_event(j.id,'ingestion.created',NULL,'{}');
 PERFORM app._dispatch_ingestion(j.id,clock_timestamp());
 SELECT * INTO STRICT j FROM app.ingestion_jobs WHERE id=j.id;
 RETURN j;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.acquire_ingestion(UUID,UUID,INTEGER),app.heartbeat_ingestion(UUID,UUID,BIGINT,INTEGER),app.guard_ingestion_write(UUID,UUID,BIGINT),
 app.advance_ingestion(UUID,UUID,BIGINT,TEXT,TEXT,BIGINT,BIGINT),app.fail_ingestion(UUID,UUID,BIGINT,TEXT,TEXT,BOOLEAN,INTEGER),app.complete_ingestion(UUID,UUID,BIGINT,UUID,UUID) TO expert_ingest;
GRANT EXECUTE ON FUNCTION app.cancel_ingestion(UUID,TEXT),app.request_ingestion_retry(UUID,TEXT,INTEGER),app.request_reindex(UUID,TEXT,TEXT,TEXT,TEXT,UUID,BOOLEAN,INTEGER) TO expert_backend;
