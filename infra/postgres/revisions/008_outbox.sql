CREATE FUNCTION app.claim_outbox(p_owner UUID,p_limit INTEGER,p_lease_seconds INTEGER DEFAULT 60) RETURNS SETOF app.outbox_events
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e app.outbox_events;
BEGIN
 IF p_owner IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 300 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_OUTBOX_CLAIM'; END IF;
 FOR e IN SELECT * FROM app.outbox_events WHERE published_at IS NULL AND available_at<=clock_timestamp()
  AND (claim_until IS NULL OR claim_until<=clock_timestamp()) AND publish_attempts<8
  AND coalesce(last_error_code,'') NOT IN ('OUTBOX_INVALID_TOPIC','OUTBOX_INVALID_PAYLOAD','OUTBOX_ATTEMPTS_EXHAUSTED')
  ORDER BY available_at,created_at,event_id FOR UPDATE SKIP LOCKED LIMIT p_limit LOOP
  UPDATE app.outbox_events SET claim_owner=p_owner,claim_token=gen_random_uuid(),claim_epoch=claim_epoch+1,
   claim_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),publish_attempts=publish_attempts+1
  WHERE event_id=e.event_id RETURNING * INTO e;
  RETURN NEXT e;
 END LOOP;
END $$;
CREATE FUNCTION app.mark_outbox_published(p_event_id UUID,p_owner UUID,p_token UUID) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e app.outbox_events;
BEGIN
 SELECT * INTO e FROM app.outbox_events WHERE event_id=p_event_id FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_token IS NULL OR e.claim_owner IS DISTINCT FROM p_owner OR e.claim_token IS DISTINCT FROM p_token THEN RETURN false; END IF;
 IF e.published_at IS NOT NULL THEN RETURN true; END IF;
 IF e.claim_until IS NULL OR e.claim_until<=clock_timestamp() THEN RETURN false; END IF;
 UPDATE app.outbox_events SET published_at=clock_timestamp(),last_error_code=NULL WHERE event_id=p_event_id;
 RETURN true;
END $$;
CREATE FUNCTION app.reschedule_outbox(p_event_id UUID,p_owner UUID,p_token UUID,p_error_code TEXT,p_delay_seconds INTEGER) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE e app.outbox_events;
BEGIN
 IF p_error_code IS NULL OR p_error_code!~'^[A-Z][A-Z0-9_]{0,99}$' OR p_delay_seconds IS NULL OR p_delay_seconds NOT BETWEEN 0 AND 3600 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_OUTBOX_RETRY'; END IF;
 SELECT * INTO e FROM app.outbox_events WHERE event_id=p_event_id FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_token IS NULL OR e.claim_owner IS DISTINCT FROM p_owner OR e.claim_token IS DISTINCT FROM p_token OR
  e.published_at IS NOT NULL OR e.claim_until IS NULL OR e.claim_until<=clock_timestamp() THEN RETURN false; END IF;
 UPDATE app.outbox_events SET available_at=clock_timestamp()+make_interval(secs=>p_delay_seconds),claim_until=NULL,
  last_error_code=CASE WHEN publish_attempts>=8 THEN 'OUTBOX_ATTEMPTS_EXHAUSTED' ELSE p_error_code END WHERE event_id=p_event_id;
 RETURN true;
END $$;
CREATE FUNCTION app.reconcile_ingestion(p_limit INTEGER DEFAULT 100,p_min_interval_seconds INTEGER DEFAULT 60) RETURNS INTEGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; dispatched INTEGER:=0;
BEGIN
 IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_min_interval_seconds IS NULL OR p_min_interval_seconds NOT BETWEEN 1 AND 3600 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_RECONCILE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 FOR j IN SELECT * FROM app.ingestion_jobs WHERE status IN ('queued','retry_wait','running') AND cancel_requested_at IS NULL
  AND available_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
  AND (last_dispatch_at IS NULL OR last_dispatch_at<=clock_timestamp()-make_interval(secs=>p_min_interval_seconds))
  AND NOT EXISTS(SELECT 1 FROM app.outbox_events e WHERE e.event_id=app.ingestion_jobs.last_dispatch_event_id AND e.published_at IS NULL)
  ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT p_limit LOOP
  -- published_at cannot prove a command survived Redis loss. An unpublished
  -- latest command remains the publisher's retry/attention responsibility;
  -- minting a replacement would bypass its bounded attempt budget.
  PERFORM app._dispatch_ingestion(j.id,clock_timestamp());
  dispatched:=dispatched+1;
 END LOOP;
 RETURN dispatched;
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.claim_outbox(UUID,INTEGER,INTEGER),app.mark_outbox_published(UUID,UUID,UUID),app.reschedule_outbox(UUID,UUID,UUID,TEXT,INTEGER) TO expert_outbox;
GRANT EXECUTE ON FUNCTION app.reconcile_ingestion(INTEGER,INTEGER) TO expert_outbox,expert_ingest;
