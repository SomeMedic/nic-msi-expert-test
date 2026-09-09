-- This singleton row is the catalog/security barrier. All run writes lock it
-- before a run; publication locks it before a document. P08 revoke/finalize
-- must retain this order. Routines never call network services.
CREATE FUNCTION agent._append_command_event(p_run UUID,p_type TEXT,p_payload JSONB) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e UUID:=gen_random_uuid(); seq BIGINT; ep BIGINT;
BEGIN
 UPDATE agent.runs SET last_event_sequence=last_event_sequence+1 WHERE id=p_run RETURNING last_event_sequence,execution_epoch INTO STRICT seq,ep;
 INSERT INTO agent.run_events(event_id,run_id,sequence,event_type,execution_epoch,public_payload) VALUES(e,p_run,seq,p_type,ep,p_payload);
 INSERT INTO app.outbox_events(event_id,aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES(e,'run',p_run,p_type,'run.events',jsonb_build_object('run_id',p_run,'sequence',seq,'event_id',e,'schema_version',1));
 RETURN e;
END $$;
CREATE FUNCTION agent.create_run(p_run_id UUID,p_principal_id TEXT,p_question TEXT,p_idempotency_key TEXT,
 p_request_hash TEXT,p_deadline_at TIMESTAMPTZ,p_configuration_fingerprint TEXT,p_debug_capture BOOLEAN)
RETURNS agent.runs LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; inserted UUID;
BEGIN
 IF p_run_id IS NULL OR p_principal_id IS NULL OR p_question IS NULL OR p_idempotency_key IS NULL OR
  p_request_hash IS NULL OR p_deadline_at IS NULL OR p_configuration_fingerprint IS NULL OR p_debug_capture IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RUN_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE principal_id=p_principal_id AND idempotency_key=p_idempotency_key FOR UPDATE;
 IF FOUND THEN
  IF r.request_hash<>p_request_hash THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN r;
 END IF;
 IF p_deadline_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEADLINE_EXCEEDED'; END IF;
 INSERT INTO agent.runs(id,principal_id,question,idempotency_key,request_hash,deadline_at,configuration_fingerprint,debug_capture)
 VALUES(p_run_id,p_principal_id,p_question,p_idempotency_key,p_request_hash,p_deadline_at,p_configuration_fingerprint,p_debug_capture)
 ON CONFLICT(principal_id,idempotency_key) DO NOTHING RETURNING id INTO inserted;
 SELECT * INTO STRICT r FROM agent.runs WHERE principal_id=p_principal_id AND idempotency_key=p_idempotency_key FOR UPDATE;
 IF r.request_hash<>p_request_hash THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
 IF inserted IS NOT NULL THEN
  PERFORM agent._append_command_event(r.id,'run.created','{}');
  SELECT * INTO STRICT r FROM agent.runs WHERE id=r.id;
 END IF;
 RETURN r;
END $$;
CREATE FUNCTION agent.request_cancel(p_run_id UUID,p_principal_id TEXT) RETURNS agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id AND principal_id=p_principal_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 IF r.status IN ('completed','refused','failed','cancelled') OR r.cancel_requested_at IS NOT NULL THEN RETURN r; END IF;
 UPDATE agent.runs SET status='cancelling',cancel_requested_at=clock_timestamp() WHERE id=p_run_id;
 PERFORM agent._append_command_event(p_run_id,'run.cancel_requested','{}');
 SELECT * INTO STRICT r FROM agent.runs WHERE id=p_run_id;
 RETURN r;
END $$;
CREATE FUNCTION agent.acquire_run(p_run_id UUID,p_owner UUID,p_lease_seconds INTEGER)
RETURNS TABLE(outcome TEXT,run_id UUID,execution_epoch BIGINT,lease_until TIMESTAMPTZ,status TEXT)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; ts TIMESTAMPTZ; event_type TEXT;
BEGIN
 IF p_owner IS NULL OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_LEASE'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 ts:=clock_timestamp();
 IF r.status IN ('completed','refused','failed','cancelled') THEN outcome:='already_terminal';
 ELSIF r.cancel_requested_at IS NOT NULL THEN outcome:='cancel_requested';
 ELSIF r.deadline_at<=ts THEN outcome:='deadline_exceeded';
 ELSIF r.lease_until>ts THEN outcome:='already_running';
 ELSE
  event_type:=CASE WHEN r.execution_epoch=0 THEN 'run.started' ELSE 'run.resuming' END;
  UPDATE agent.runs SET status='running',execution_owner=p_owner,execution_epoch=r.execution_epoch+1,
   lease_until=least(r.deadline_at,ts+make_interval(secs=>p_lease_seconds)),heartbeat_at=ts,started_at=coalesce(r.started_at,ts) WHERE id=p_run_id;
  PERFORM agent._append_command_event(p_run_id,event_type,'{}');
  SELECT * INTO STRICT r FROM agent.runs WHERE id=p_run_id;
  outcome:='acquired';
 END IF;
 run_id:=r.id; execution_epoch:=r.execution_epoch; lease_until:=r.lease_until; status:=r.status;
 RETURN NEXT;
END $$;
CREATE FUNCTION agent._assert_run_write(p_run_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS agent.runs
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; ts TIMESTAMPTZ;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO r FROM agent.runs WHERE id=p_run_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_FOUND'; END IF;
 ts:=clock_timestamp();
 IF p_owner IS NULL OR p_epoch IS NULL OR r.execution_owner IS DISTINCT FROM p_owner OR r.execution_epoch<>p_epoch THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_EXECUTION'; END IF;
 IF r.cancel_requested_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CANCEL_REQUESTED'; END IF;
 IF r.status<>'running' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='RUN_NOT_RUNNING'; END IF;
 IF r.deadline_at<=ts THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DEADLINE_EXCEEDED'; END IF;
 IF r.lease_until IS NULL OR r.lease_until<=ts THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='LEASE_EXPIRED'; END IF;
 IF EXISTS(SELECT 1 FROM agent.kb_snapshot_items i JOIN app.logical_documents d ON d.id=i.logical_document_id
  WHERE i.snapshot_id=r.snapshot_id AND d.security_revoked_at IS NOT NULL) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 RETURN r;
END $$;
CREATE TABLE agent.checkpoint_write_bindings (
 transaction_id XID8 PRIMARY KEY, backend_pid INTEGER NOT NULL, run_id UUID NOT NULL REFERENCES agent.runs ON DELETE RESTRICT,
 execution_owner UUID NOT NULL, execution_epoch BIGINT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX checkpoint_write_bindings_pid ON agent.checkpoint_write_bindings(backend_pid);
CREATE FUNCTION agent.guard_run_write(p_run_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; binding agent.checkpoint_write_bindings;
BEGIN
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 -- Runtime cannot insert/modify this protected transaction binding. GUCs,
 -- checkpoint metadata and graph state cannot create or replace authority.
 SELECT * INTO binding FROM agent.checkpoint_write_bindings WHERE transaction_id=pg_current_xact_id();
 IF FOUND THEN
  IF (binding.run_id,binding.execution_owner,binding.execution_epoch,binding.backend_pid)
   IS DISTINCT FROM (p_run_id,p_owner,p_epoch,pg_backend_pid()) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 ELSE
  DELETE FROM agent.checkpoint_write_bindings WHERE backend_pid=pg_backend_pid();
  INSERT INTO agent.checkpoint_write_bindings(transaction_id,backend_pid,run_id,execution_owner,execution_epoch)
  VALUES(pg_current_xact_id(),pg_backend_pid(),p_run_id,p_owner,p_epoch);
 END IF;
 RETURN r.execution_epoch;
END $$;
CREATE FUNCTION agent.heartbeat_run(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_lease_seconds INTEGER)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; expires TIMESTAMPTZ;
BEGIN
 IF p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_LEASE'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 expires:=least(r.deadline_at,clock_timestamp()+make_interval(secs=>p_lease_seconds));
 UPDATE agent.runs SET lease_until=expires,heartbeat_at=clock_timestamp() WHERE id=p_run_id;
 RETURN expires;
END $$;
CREATE FUNCTION agent.capture_snapshot(p_run_id UUID,p_owner UUID,p_epoch BIGINT) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs; snap UUID; catalog_epoch BIGINT; selection TEXT;
BEGIN
 IF current_setting('transaction_isolation')<>'repeatable read' THEN RAISE EXCEPTION USING ERRCODE='25001',MESSAGE='SNAPSHOT_REQUIRES_REPEATABLE_READ'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 IF r.snapshot_id IS NOT NULL THEN RETURN r.snapshot_id; END IF;
 SELECT epoch INTO STRICT catalog_epoch FROM app.knowledge_catalog;
 snap:=gen_random_uuid();
 SELECT coalesce(string_agg(d.id::text||':'||p.id::text||':'||g.id::text||':'||g.parse_generation_id::text,',' ORDER BY d.id),'') INTO selection
 FROM app.logical_documents d JOIN app.publications p ON p.id=d.current_publication_id
 JOIN app.document_versions v ON v.id=p.document_version_id JOIN knowledge.index_generations g ON g.id=p.index_generation_id
 JOIN knowledge.parse_generations pg ON pg.id=g.parse_generation_id
 WHERE d.archived_at IS NULL AND d.security_revoked_at IS NULL AND v.legal_status='active' AND v.publication_status='published' AND p.retired_at IS NULL AND g.status='ready' AND pg.status='ready';
 INSERT INTO agent.kb_snapshots(id,catalog_epoch,selection_hash,principal_scope_hash)
 VALUES(snap,catalog_epoch,encode(sha256(convert_to(selection,'UTF8')),'hex'),encode(sha256(convert_to(r.principal_id,'UTF8')),'hex'));
 INSERT INTO agent.kb_snapshot_items(snapshot_id,logical_document_id,document_version_id,parse_generation_id,index_generation_id,publication_id,legal_status_at_capture)
 SELECT snap,d.id,v.id,g.parse_generation_id,g.id,p.id,v.legal_status
 FROM app.logical_documents d JOIN app.publications p ON p.id=d.current_publication_id
 JOIN app.document_versions v ON v.id=p.document_version_id JOIN knowledge.index_generations g ON g.id=p.index_generation_id
 JOIN knowledge.parse_generations pg ON pg.id=g.parse_generation_id
 WHERE d.archived_at IS NULL AND d.security_revoked_at IS NULL AND v.legal_status='active' AND v.publication_status='published' AND p.retired_at IS NULL AND g.status='ready' AND pg.status='ready';
 UPDATE agent.runs SET snapshot_id=snap WHERE id=p_run_id;
 RETURN snap;
END $$;
CREATE FUNCTION app.publish_version(p_version_id UUID,p_index_generation_id UUID,p_expected_current_publication_id UUID,p_operation_id UUID,
 p_job_id UUID DEFAULT NULL,p_job_owner UUID DEFAULT NULL,p_job_epoch BIGINT DEFAULT NULL)
RETURNS UUID LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v app.document_versions; d app.logical_documents; g knowledge.index_generations; pg knowledge.parse_generations;
 previous app.publications; existing app.publications; pub UUID:=gen_random_uuid(); job app.ingestion_jobs; ts TIMESTAMPTZ;
BEGIN
 IF p_version_id IS NULL OR p_index_generation_id IS NULL OR p_operation_id IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PUBLICATION_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO existing FROM app.publications WHERE operation_id=p_operation_id;
 IF FOUND THEN
  IF existing.document_version_id<>p_version_id OR existing.index_generation_id<>p_index_generation_id OR existing.expected_previous_publication_id IS DISTINCT FROM p_expected_current_publication_id THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN existing.id;
 END IF;
 SELECT * INTO v FROM app.document_versions WHERE id=p_version_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_NOT_FOUND'; END IF;
 SELECT * INTO STRICT d FROM app.logical_documents WHERE id=v.logical_document_id FOR UPDATE;
 SELECT * INTO STRICT v FROM app.document_versions WHERE id=p_version_id FOR UPDATE;
 IF d.current_publication_id IS DISTINCT FROM p_expected_current_publication_id THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 IF d.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 IF session_user='expert_ingest' OR p_job_id IS NOT NULL THEN
  SELECT * INTO job FROM app.ingestion_jobs WHERE id=p_job_id FOR UPDATE;
  IF NOT FOUND OR job.version_id<>p_version_id OR job.lease_owner IS DISTINCT FROM p_job_owner OR p_job_owner IS NULL OR
    job.lease_epoch IS DISTINCT FROM p_job_epoch OR job.lease_until IS NULL OR job.lease_until<=clock_timestamp() OR job.status<>'running' THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='STALE_INGESTION'; END IF;
  IF job.cancel_requested_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CANCEL_REQUESTED'; END IF;
  IF job.expected_publication_id IS DISTINCT FROM p_expected_current_publication_id OR NOT job.desired_auto_publish THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 END IF;
 SELECT * INTO g FROM knowledge.index_generations WHERE id=p_index_generation_id AND document_version_id=p_version_id FOR UPDATE;
 IF NOT FOUND OR g.status<>'ready' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_READY'; END IF;
 SELECT * INTO pg FROM knowledge.parse_generations WHERE id=g.parse_generation_id AND document_version_id=p_version_id FOR UPDATE;
 IF NOT FOUND OR pg.status<>'ready' OR pg.source_sha256<>v.source_sha256 OR pg.quality_report->>'status' IS DISTINCT FROM 'passed' OR
  NOT EXISTS(SELECT 1 FROM app.stored_objects WHERE id=v.source_object_id AND kind='original' AND state='attached') OR
  NOT EXISTS(SELECT 1 FROM app.stored_objects WHERE id=pg.artifact_object_id AND kind='parse_artifact' AND state='attached') OR
  g.chunk_count<>(SELECT count(*) FROM knowledge.chunks WHERE index_generation_id=g.id) OR
  g.routing_count<>(SELECT count(*) FROM knowledge.node_routing_embeddings WHERE index_generation_id=g.id) OR
  pg.node_count<>(SELECT count(*) FROM knowledge.document_nodes WHERE parse_generation_id=pg.id) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_READY'; END IF;
 ts:=clock_timestamp();
 IF d.current_publication_id IS NOT NULL THEN
  SELECT * INTO STRICT previous FROM app.publications WHERE id=d.current_publication_id;
  UPDATE app.publications SET retired_at=ts WHERE id=previous.id;
  UPDATE knowledge.chunks SET searchable=false WHERE index_generation_id=previous.index_generation_id;
  UPDATE knowledge.node_routing_embeddings SET searchable=false WHERE index_generation_id=previous.index_generation_id;
  IF previous.document_version_id<>p_version_id THEN UPDATE app.document_versions SET publication_status='superseded' WHERE id=previous.document_version_id; END IF;
 END IF;
 INSERT INTO app.publications(id,logical_document_id,document_version_id,index_generation_id,operation_id,expected_previous_publication_id,actor,reason)
 VALUES(pub,d.id,v.id,g.id,p_operation_id,p_expected_current_publication_id,session_user,'publish');
 UPDATE app.document_versions SET publication_status='published',published_at=coalesce(published_at,ts) WHERE id=v.id;
 UPDATE knowledge.chunks SET searchable=(v.legal_status='active' AND d.archived_at IS NULL) WHERE index_generation_id=g.id;
 UPDATE knowledge.node_routing_embeddings SET searchable=(v.legal_status='active' AND d.archived_at IS NULL) WHERE index_generation_id=g.id;
 UPDATE app.logical_documents SET current_publication_id=pub,row_version=row_version+1,updated_at=ts WHERE id=d.id;
 UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=ts;
 INSERT INTO app.outbox_events(aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES('document',d.id,'document.published','knowledge.events',jsonb_build_object('publication_id',pub,'version_id',v.id,'index_generation_id',g.id));
 RETURN pub;
END $$;
