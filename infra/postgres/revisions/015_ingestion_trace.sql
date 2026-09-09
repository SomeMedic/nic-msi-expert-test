-- Trace correlation is optional metadata, never lease or publication authority.
-- Old jobs stay NULL until a current worker binds a trace; no history is fabricated.
ALTER TABLE app.ingestion_jobs ADD COLUMN trace_id TEXT
 CHECK(trace_id IS NULL OR (length(trace_id)=32 AND trace_id~'^[0-9a-f]{32}$' AND trace_id<>repeat('0',32)));
CREATE FUNCTION app._guard_ingestion_trace() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF OLD.trace_id IS NOT NULL AND NEW.trace_id IS DISTINCT FROM OLD.trace_id THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='TRACE_ID_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER ingestion_trace_guard BEFORE UPDATE OF trace_id ON app.ingestion_jobs
 FOR EACH ROW EXECUTE FUNCTION app._guard_ingestion_trace();

-- Backend calls this in the SAME short transaction as attach_upload/request_reindex.
-- Original principal and catalog barrier precede inspection of any correlation.
CREATE FUNCTION app.bind_queued_ingestion_trace(p_job_id UUID,p_principal_id TEXT,p_trace_id TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs;
BEGIN
 IF p_trace_id IS NULL OR length(p_trace_id)<>32 OR p_trace_id!~'^[0-9a-f]{32}$' OR p_trace_id=repeat('0',32) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TRACE_ID'; END IF;
 IF p_principal_id IS NULL OR btrim(p_principal_id)='' OR length(p_principal_id)>200 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PRINCIPAL'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO j FROM app.ingestion_jobs WHERE id=p_job_id AND principal_id=p_principal_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_FOUND'; END IF;
 IF EXISTS(SELECT 1 FROM app.document_versions v JOIN app.logical_documents d ON d.id=v.logical_document_id
  WHERE v.id=j.version_id AND d.security_revoked_at IS NOT NULL) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 -- A browser retry can have a different HTTP trace. It observes the original
 -- durable correlation; it cannot replace it or reopen a terminal job.
 IF j.trace_id IS NOT NULL THEN RETURN j.trace_id; END IF;
 IF j.cancel_requested_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CANCEL_REQUESTED'; END IF;
 IF j.status<>'queued' OR j.lease_epoch<>0 OR j.lease_owner IS NOT NULL OR j.lease_until IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INGESTION_NOT_QUEUED'; END IF;
 UPDATE app.ingestion_jobs SET trace_id=p_trace_id WHERE id=j.id RETURNING * INTO j;
 RETURN j.trace_id;
END $$;

CREATE FUNCTION app.bind_ingestion_trace(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_trace_id TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs;
BEGIN
 IF p_trace_id IS NULL OR length(p_trace_id)<>32 OR p_trace_id!~'^[0-9a-f]{32}$' OR p_trace_id=repeat('0',32) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TRACE_ID'; END IF;
 j:=app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 IF j.trace_id IS NOT NULL AND j.trace_id<>p_trace_id THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='TRACE_ID_CONFLICT'; END IF;
 IF j.trace_id IS NULL THEN
  UPDATE app.ingestion_jobs SET trace_id=p_trace_id WHERE id=j.id RETURNING * INTO j;
 END IF;
 PERFORM app._assert_ingestion_write(p_job_id,p_owner,p_epoch);
 RETURN j.trace_id;
END $$;

-- A publisher learns only the trace associated with its current committed
-- claim. No new job/private-table SELECT grant and no Redis payload changes.
CREATE FUNCTION app.get_claimed_outbox_trace(p_event_id UUID,p_owner UUID,p_token UUID)
RETURNS TEXT LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT j.trace_id FROM app.outbox_events e JOIN app.ingestion_jobs j ON j.id=e.aggregate_id
 WHERE e.event_id=p_event_id AND e.aggregate_type='ingestion_job' AND e.schema_version=1
  AND e.claim_owner=p_owner AND e.claim_token=p_token AND e.published_at IS NULL AND e.claim_until>clock_timestamp()
$$;
REVOKE ALL ON FUNCTION app._guard_ingestion_trace(),app.bind_queued_ingestion_trace(UUID,TEXT,TEXT),
 app.bind_ingestion_trace(UUID,UUID,BIGINT,TEXT),app.get_claimed_outbox_trace(UUID,UUID,UUID)
 FROM PUBLIC,expert_backend,expert_runtime,expert_ingest,expert_outbox;
GRANT EXECUTE ON FUNCTION app.bind_queued_ingestion_trace(UUID,TEXT,TEXT) TO expert_backend;
GRANT EXECUTE ON FUNCTION app.bind_ingestion_trace(UUID,UUID,BIGINT,TEXT) TO expert_ingest;
GRANT EXECUTE ON FUNCTION app.get_claimed_outbox_trace(UUID,UUID,UUID) TO expert_outbox;
