CREATE FUNCTION agent.guard_checkpoint_write() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE binding agent.checkpoint_write_bindings;
BEGIN
 SELECT * INTO binding FROM agent.checkpoint_write_bindings
 WHERE transaction_id=pg_current_xact_id() AND backend_pid=pg_backend_pid();
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CHECKPOINT_BINDING_REQUIRED'; END IF;
 IF NEW.thread_id IS DISTINCT FROM binding.run_id::text THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='CHECKPOINT_RUN_MISMATCH'; END IF;
 PERFORM agent._assert_run_write(binding.run_id,binding.execution_owner,binding.execution_epoch);
 RETURN NEW;
END $$;
CREATE TRIGGER checkpoints_fenced BEFORE INSERT OR UPDATE ON agent.checkpoints FOR EACH ROW EXECUTE FUNCTION agent.guard_checkpoint_write();
CREATE TRIGGER checkpoint_blobs_fenced BEFORE INSERT OR UPDATE ON agent.checkpoint_blobs FOR EACH ROW EXECUTE FUNCTION agent.guard_checkpoint_write();
CREATE TRIGGER checkpoint_writes_fenced BEFORE INSERT OR UPDATE ON agent.checkpoint_writes FOR EACH ROW EXECUTE FUNCTION agent.guard_checkpoint_write();
-- No DDL ownership, no implicit function execution, no blanket execution table
-- writes. P03 and P08 add separately reviewed command routines and grants.
REVOKE ALL ON ALL TABLES IN SCHEMA app,knowledge,agent,eval FROM PUBLIC,expert_backend,expert_runtime,expert_ingest,expert_outbox;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app,knowledge,agent,eval FROM PUBLIC,expert_backend,expert_runtime,expert_ingest,expert_outbox;
GRANT USAGE ON SCHEMA app,knowledge,agent TO expert_backend,expert_runtime,expert_ingest;
GRANT USAGE ON SCHEMA app TO expert_outbox;
GRANT SELECT ON app.logical_documents,app.document_versions,app.publications,app.knowledge_catalog,app.ingestion_jobs,app.ingestion_events TO expert_backend;
GRANT SELECT ON agent.runs,agent.run_events,agent.run_results,agent.kb_snapshots,agent.kb_snapshot_items TO expert_backend;
GRANT SELECT ON knowledge.parse_generations,knowledge.index_generations,knowledge.document_nodes,knowledge.chunks,knowledge.node_routing_embeddings TO expert_backend,expert_runtime;
GRANT SELECT ON app.logical_documents,app.document_versions,app.publications,app.knowledge_catalog TO expert_runtime;
GRANT SELECT ON agent.runs,agent.kb_snapshots,agent.kb_snapshot_items,agent.run_step_artifacts,agent.evidence_packs TO expert_runtime;
GRANT SELECT ON app.stored_objects,app.document_versions,app.logical_documents,app.ingestion_jobs,app.publications TO expert_ingest;
-- Ingestion persistence is fenced via P03 routines; P02 deliberately grants
-- no direct child-table writes capable of bypassing the future job lease.
GRANT SELECT ON ALL TABLES IN SCHEMA knowledge TO expert_ingest;
GRANT SELECT ON app.outbox_events TO expert_outbox;
GRANT SELECT,INSERT ON agent.checkpoints,agent.checkpoint_blobs,agent.checkpoint_writes TO expert_runtime;
GRANT UPDATE(checkpoint,metadata) ON agent.checkpoints TO expert_runtime;
GRANT UPDATE(channel,type,blob) ON agent.checkpoint_writes TO expert_runtime;
GRANT EXECUTE ON FUNCTION agent.create_run(UUID,TEXT,TEXT,TEXT,TEXT,TIMESTAMPTZ,TEXT,BOOLEAN),agent.request_cancel(UUID,TEXT) TO expert_backend;
GRANT EXECUTE ON FUNCTION agent.acquire_run(UUID,UUID,INTEGER),agent.guard_run_write(UUID,UUID,BIGINT),agent.heartbeat_run(UUID,UUID,BIGINT,INTEGER),agent.capture_snapshot(UUID,UUID,BIGINT) TO expert_runtime;
GRANT EXECUTE ON FUNCTION app.publish_version(UUID,UUID,UUID,UUID,UUID,UUID,BIGINT) TO expert_backend,expert_ingest;
