-- P11: trace correlation is optional observability, never an execution authority.
-- The existing runtime role may already read runs.trace_id. No new read grant.
CREATE FUNCTION agent.bind_run_trace(p_run_id UUID,p_owner UUID,p_epoch BIGINT,p_trace_id TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r agent.runs;
BEGIN
 IF p_trace_id IS NULL OR p_trace_id !~ '^[0-9a-f]{32}$' OR p_trace_id=repeat('0',32) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_TRACE_ID'; END IF;
 r:=agent._assert_run_write(p_run_id,p_owner,p_epoch);
 IF r.trace_id IS NOT NULL AND r.trace_id<>p_trace_id THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='TRACE_ID_CONFLICT'; END IF;
 IF r.trace_id IS NULL THEN
  UPDATE agent.runs SET trace_id=p_trace_id WHERE id=p_run_id RETURNING * INTO r;
 END IF;
 PERFORM agent._assert_run_write(p_run_id,p_owner,p_epoch);
 RETURN r.trace_id;
END $$;
REVOKE ALL ON FUNCTION agent.bind_run_trace(UUID,UUID,BIGINT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agent.bind_run_trace(UUID,UUID,BIGINT,TEXT) TO expert_runtime;
