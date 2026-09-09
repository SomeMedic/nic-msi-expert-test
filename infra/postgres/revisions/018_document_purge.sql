-- Admin plan/confirmation and backend exact-object deletion saga. Original IDs,
-- hashes and lifecycle metadata remain as audit tombstones; no referenced answer
-- source, snapshot, result or checkpoint is removed by this migration.
ALTER TABLE app.logical_documents ADD COLUMN purge_plan_id UUID REFERENCES app.purge_plans ON DELETE RESTRICT,
 ADD COLUMN purge_pending_at TIMESTAMPTZ, ADD COLUMN purged_at TIMESTAMPTZ,
 ADD CHECK((purge_plan_id IS NULL)=(purge_pending_at IS NULL)),
 ADD CHECK(purged_at IS NULL OR purge_pending_at IS NOT NULL);
ALTER TABLE app.purge_plans ADD COLUMN retention_seconds INTEGER NOT NULL DEFAULT 3600 CHECK(retention_seconds BETWEEN 3600 AND 31536000),
 ADD COLUMN document_fingerprint app.sha256, ADD COLUMN accepted_at TIMESTAMPTZ, ADD COLUMN completed_at TIMESTAMPTZ,
 ADD COLUMN error_code TEXT CHECK(error_code IN ('PURGE_STORAGE_UNAVAILABLE','PURGE_OBJECT_MISMATCH','PURGE_DELETE_UNVERIFIED','PURGE_ATTEMPTS_EXHAUSTED')),
 ADD COLUMN cleanup_counts JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(cleanup_counts)='object'),
 ADD UNIQUE(document_id,plan_version), ADD UNIQUE(id,document_id),
 ADD CHECK(document_fingerprint IS NULL OR (status='planned')=(accepted_at IS NULL)),
 ADD CHECK(document_fingerprint IS NULL OR (status='completed')=(completed_at IS NOT NULL));
CREATE TABLE app.purge_objects (
 plan_id UUID NOT NULL, document_id UUID NOT NULL, object_id UUID NOT NULL,
 document_version_id UUID NOT NULL, parse_generation_id UUID, intent_id UUID,
 kind TEXT NOT NULL CHECK(kind IN ('original','parse_artifact')), bucket TEXT NOT NULL,
 object_key TEXT NOT NULL, object_version_id TEXT, sha256 app.sha256 NOT NULL,
 size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 268435456), media_type TEXT NOT NULL,
 attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt BETWEEN 0 AND 8),
 claim_owner UUID, claim_token UUID, claim_until TIMESTAMPTZ, available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 deleted_at TIMESTAMPTZ, next_check_at TIMESTAMPTZ,
 last_error_code TEXT CHECK(last_error_code IN ('PURGE_STORAGE_UNAVAILABLE','PURGE_OBJECT_MISMATCH','PURGE_DELETE_UNVERIFIED','PURGE_ATTEMPTS_EXHAUSTED')),
 PRIMARY KEY(plan_id,object_id), FOREIGN KEY(plan_id,document_id) REFERENCES app.purge_plans(id,document_id) ON DELETE RESTRICT,
 CHECK((kind='original' AND bucket='originals' AND media_type='application/pdf' AND parse_generation_id IS NULL AND
  object_key='originals/'||document_id::text||'/'||document_version_id::text||'/'||sha256||'.pdf') OR
  (kind='parse_artifact' AND bucket='artifacts' AND parse_generation_id IS NOT NULL AND intent_id IS NOT NULL AND
   media_type IN ('application/json','image/png') AND object_key='parses/'||document_version_id::text||'/'||parse_generation_id::text||'/'||
   intent_id::text||'/'||sha256||CASE WHEN media_type='application/json' THEN '.json' ELSE '.png' END)),
 CHECK((claim_owner IS NULL)=(claim_token IS NULL)), CHECK((claim_owner IS NULL)=(claim_until IS NULL)),
 CHECK((deleted_at IS NULL)=(next_check_at IS NULL))
);
CREATE INDEX purge_objects_due ON app.purge_objects(available_at,plan_id,object_id) WHERE deleted_at IS NULL;
CREATE INDEX purge_objects_audit_due ON app.purge_objects(next_check_at,plan_id,object_id) WHERE deleted_at IS NOT NULL;
CREATE TABLE app.purge_write_bindings (
 transaction_id XID8 PRIMARY KEY, backend_pid INTEGER NOT NULL, plan_id UUID NOT NULL, document_id UUID NOT NULL,
 FOREIGN KEY(plan_id,document_id) REFERENCES app.purge_plans(id,document_id) ON DELETE RESTRICT
);
CREATE FUNCTION app._purge_bound(p_document UUID) RETURNS BOOLEAN
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT EXISTS(SELECT 1 FROM app.purge_write_bindings b WHERE b.transaction_id=pg_current_xact_id_if_assigned()
  AND b.backend_pid=pg_backend_pid() AND b.document_id=p_document)
$$;
CREATE FUNCTION app._purge_identity_guard() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PURGE_AUDIT_IMMUTABLE'; END IF;
 IF TG_TABLE_NAME='purge_plans' THEN
  IF (to_jsonb(NEW)-ARRAY['status','accepted_at','completed_at','error_code','cleanup_counts'])<>
     (to_jsonb(OLD)-ARRAY['status','accepted_at','completed_at','error_code','cleanup_counts']) OR
    (OLD.status='completed' AND NEW IS DISTINCT FROM OLD) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PURGE_AUDIT_IMMUTABLE'; END IF;
 ELSE
  IF (to_jsonb(NEW)-ARRAY['attempt','claim_owner','claim_token','claim_until','available_at','deleted_at','next_check_at','last_error_code'])<>
     (to_jsonb(OLD)-ARRAY['attempt','claim_owner','claim_token','claim_until','available_at','deleted_at','next_check_at','last_error_code']) OR
     (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS DISTINCT FROM OLD.deleted_at) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PURGE_AUDIT_IMMUTABLE'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER purge_plan_identity BEFORE UPDATE OR DELETE ON app.purge_plans FOR EACH ROW EXECUTE FUNCTION app._purge_identity_guard();
CREATE TRIGGER purge_object_identity BEFORE UPDATE OR DELETE ON app.purge_objects FOR EACH ROW EXECUTE FUNCTION app._purge_identity_guard();

CREATE FUNCTION app._purge_manifest(p_document UUID) RETURNS JSONB
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 WITH objects AS (
  SELECT s.id object_id,v.id document_version_id,NULL::uuid parse_generation_id,u.id intent_id,
   s.kind,s.bucket,s.object_key,s.object_version_id,s.sha256,s.size_bytes,s.media_type,0 priority
  FROM app.document_versions v JOIN app.stored_objects s ON s.id=v.source_object_id
   LEFT JOIN app.upload_intents u ON u.source_object_id=s.id WHERE v.logical_document_id=p_document
  UNION ALL
  SELECT u.source_object_id,u.version_id,NULL::uuid,u.id,'original',u.bucket,u.object_key,u.object_version_id,
   u.source_sha256,u.size_bytes,'application/pdf',1 FROM app.upload_intents u WHERE u.document_id=p_document
  UNION ALL
  SELECT a.artifact_object_id,a.document_version_id,a.parse_generation_id,a.id,'parse_artifact',a.bucket,a.object_key,
   a.object_version_id,a.sha256,a.size_bytes,a.media_type,0 FROM app.parse_artifact_intents a
   JOIN app.document_versions v ON v.id=a.document_version_id WHERE v.logical_document_id=p_document
  UNION ALL
  SELECT s.id,p.document_version_id,p.id,NULL::uuid,s.kind,s.bucket,s.object_key,s.object_version_id,s.sha256,s.size_bytes,s.media_type,1
   FROM knowledge.parse_generations p JOIN app.document_versions v ON v.id=p.document_version_id
   JOIN app.stored_objects s ON s.id=p.artifact_object_id WHERE v.logical_document_id=p_document
 ), selected AS (SELECT DISTINCT ON(object_id) * FROM objects ORDER BY object_id,priority)
 SELECT coalesce(jsonb_agg(to_jsonb(selected)-'priority' ORDER BY object_id),'[]'::jsonb) FROM selected
$$;
CREATE FUNCTION app._purge_manifest_valid(p_document UUID,p_manifest JSONB) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p_manifest) o WHERE NOT coalesce(
  ((o->>'kind'='original' AND o->>'bucket'='originals' AND o->>'media_type'='application/pdf' AND
   o->'parse_generation_id'='null'::jsonb AND o->>'object_key'='originals/'||p_document::text||'/'||(o->>'document_version_id')||'/'||(o->>'sha256')||'.pdf') OR
   (o->>'kind'='parse_artifact' AND o->>'bucket'='artifacts' AND o->>'media_type' IN ('application/json','image/png') AND
   o->>'parse_generation_id' IS NOT NULL AND o->>'intent_id' IS NOT NULL AND
   o->>'object_key'='parses/'||(o->>'document_version_id')||'/'||(o->>'parse_generation_id')||'/'||(o->>'intent_id')||'/'||
   (o->>'sha256')||CASE WHEN o->>'media_type'='application/json' THEN '.json' ELSE '.png' END)),false))
$$;
CREATE FUNCTION app._purge_report(p_document UUID,p_retention INTEGER) RETURNS JSONB
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE d app.logical_documents; refs JSONB; objects JSONB; blockers JSONB:='[]'; latest TIMESTAMPTZ; eligible TIMESTAMPTZ;
BEGIN
 SELECT * INTO STRICT d FROM app.logical_documents WHERE id=p_document;
 objects:=app._purge_manifest(p_document);
 WITH snapshots AS (SELECT snapshot_id FROM agent.kb_snapshot_items WHERE logical_document_id=p_document),
 runs AS (SELECT r.id,r.status FROM agent.runs r WHERE r.snapshot_id IN (SELECT snapshot_id FROM snapshots))
 SELECT jsonb_build_object(
  'snapshots',(SELECT count(*) FROM snapshots),
  'active_runs',(SELECT count(*) FROM runs WHERE status IN ('created','running','cancelling')),
  'results',(SELECT count(*) FROM agent.run_results WHERE run_id IN (SELECT id FROM runs)),
  'checkpoints',(SELECT count(*) FROM agent.checkpoints WHERE thread_id IN (SELECT id::text FROM runs))+
   (SELECT count(*) FROM agent.checkpoint_blobs WHERE thread_id IN (SELECT id::text FROM runs))+
   (SELECT count(*) FROM agent.checkpoint_writes WHERE thread_id IN (SELECT id::text FROM runs)),
  'objects',jsonb_array_length(objects),
  'active_ingestion_jobs',(SELECT count(*) FROM app.ingestion_jobs j JOIN app.document_versions v ON v.id=j.version_id
   WHERE v.logical_document_id=p_document AND j.status IN ('queued','running','retry_wait')),
  'pending_reservations',(SELECT count(*) FROM app.upload_intents WHERE document_id=p_document AND
   ((state IN ('reserved','stored') AND expires_at>clock_timestamp()) OR cleanup_until>clock_timestamp()))+
   (SELECT count(*) FROM app.parse_artifact_intents a JOIN app.document_versions v ON v.id=a.document_version_id
    WHERE v.logical_document_id=p_document AND ((a.state='reserved' AND a.expires_at>clock_timestamp()) OR a.cleanup_until>clock_timestamp()))) INTO refs;
 SELECT max(t) INTO latest FROM (
  SELECT greatest(d.created_at,d.updated_at) t
  UNION ALL SELECT greatest(created_at,published_at,deactivated_at) FROM app.document_versions WHERE logical_document_id=p_document
  UNION ALL SELECT greatest(j.created_at,j.finished_at,j.heartbeat_at) FROM app.ingestion_jobs j
   JOIN app.document_versions v ON v.id=j.version_id WHERE v.logical_document_id=p_document
  UNION ALL SELECT greatest(p.created_at,p.completed_at) FROM knowledge.parse_generations p
   JOIN app.document_versions v ON v.id=p.document_version_id WHERE v.logical_document_id=p_document
  UNION ALL SELECT greatest(g.created_at,g.completed_at) FROM knowledge.index_generations g
   JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=p_document
  UNION ALL SELECT greatest(created_at,stored_at,attached_at,CASE WHEN state<>'attached' THEN expires_at END)
   FROM app.upload_intents WHERE document_id=p_document
  UNION ALL SELECT greatest(a.created_at,a.attached_at,CASE WHEN a.state<>'attached' THEN a.expires_at END)
   FROM app.parse_artifact_intents a JOIN app.document_versions v ON v.id=a.document_version_id WHERE v.logical_document_id=p_document
 ) activity;
 eligible:=latest+make_interval(secs=>p_retention);
 IF d.archived_at IS NULL AND d.security_revoked_at IS NULL THEN blockers:=blockers||'"DOCUMENT_NOT_ARCHIVED"'::jsonb; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each_text(refs) r WHERE key<>'objects' AND value::bigint>0) THEN blockers:=blockers||'"PROTECTED_REFERENCES"'::jsonb; END IF;
 IF eligible>clock_timestamp() THEN blockers:=blockers||'"RETENTION_ACTIVE"'::jsonb; END IF;
  IF NOT app._purge_manifest_valid(p_document,objects) OR EXISTS(
  SELECT 1 FROM app.document_versions v WHERE v.logical_document_id<>p_document AND v.source_object_id IN
   (SELECT (o->>'object_id')::uuid FROM jsonb_array_elements(objects) o)) OR EXISTS(
   SELECT 1 FROM knowledge.parse_generations g JOIN app.document_versions v ON v.id=g.document_version_id
   WHERE v.logical_document_id<>p_document AND g.artifact_object_id IN
    (SELECT (o->>'object_id')::uuid FROM jsonb_array_elements(objects) o)) OR EXISTS(
   SELECT 1 FROM agent.run_step_artifacts WHERE output_reference IN
    (SELECT (o->>'object_id')::uuid FROM jsonb_array_elements(objects) o)) THEN blockers:=blockers||'"OBJECT_SCOPE_INVALID"'::jsonb; END IF;
 RETURN jsonb_build_object('allowed',jsonb_array_length(blockers)=0,'blockers',blockers,'references',refs,
  'retention_policy','p14.purge.v1','eligible_after',eligible);
END $$;
CREATE FUNCTION app._purge_fingerprint(p_document UUID,p_report JSONB) RETURNS TEXT
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT encode(sha256(convert_to(jsonb_build_object('document',to_jsonb(d)-ARRAY['purge_plan_id','purge_pending_at','purged_at'],
  'report',p_report,'objects',app._purge_manifest(p_document),
  'versions',(SELECT coalesce(jsonb_agg(to_jsonb(v) ORDER BY id),'[]') FROM app.document_versions v WHERE logical_document_id=p_document),
  'parses',(SELECT coalesce(jsonb_agg(jsonb_build_array(p.id,p.status,p.node_count,p.artifact_object_id) ORDER BY p.id),'[]')
   FROM knowledge.parse_generations p JOIN app.document_versions v ON v.id=p.document_version_id WHERE v.logical_document_id=p_document),
  'indexes',(SELECT coalesce(jsonb_agg(jsonb_build_array(g.id,g.status,g.chunk_count,g.routing_count) ORDER BY g.id),'[]')
   FROM knowledge.index_generations g JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=p_document))::text,'UTF8')),'hex')
 FROM app.logical_documents d WHERE d.id=p_document
$$;

CREATE FUNCTION app._purge_bind(p_plan UUID,p_document UUID) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM app.purge_write_bindings WHERE transaction_id=pg_current_xact_id() AND
  (backend_pid,plan_id,document_id) IS DISTINCT FROM (pg_backend_pid(),p_plan,p_document)) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='EXECUTION_BINDING_IMMUTABLE'; END IF;
 INSERT INTO app.purge_write_bindings VALUES(pg_current_xact_id(),pg_backend_pid(),p_plan,p_document) ON CONFLICT DO NOTHING;
END $$;
CREATE FUNCTION app._purge_status(p app.purge_plans) RETURNS JSONB
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('plan_id',p.id,'document_id',p.document_id,'plan_version',p.plan_version,'status',p.status,
  'created_at',p.created_at,'expires_at',p.expires_at,'accepted_at',p.accepted_at,'completed_at',p.completed_at,
  'total_object_count',count(*),'deleted_object_count',count(*) FILTER(WHERE deleted_at IS NOT NULL),'error_code',p.error_code)
 FROM app.purge_objects WHERE plan_id=p.id
$$;
CREATE FUNCTION app.plan_document_purge(p_document UUID,p_principal TEXT,p_retention_seconds INTEGER DEFAULT 3600,p_plan_ttl_seconds INTEGER DEFAULT 900)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE d app.logical_documents; p app.purge_plans; report JSONB; objects JSONB; version BIGINT;
BEGIN
 IF p_document IS NULL OR p_principal IS NULL OR p_principal ~ '^[[:space:]]*$' OR length(p_principal)>200 OR
  p_retention_seconds IS NULL OR p_retention_seconds NOT BETWEEN 3600 AND 31536000 OR
  p_plan_ttl_seconds IS NULL OR p_plan_ttl_seconds NOT BETWEEN 60 AND 3600 OR current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PURGE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO d FROM app.logical_documents WHERE id=p_document FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_NOT_FOUND'; END IF;
 IF d.purged_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_PURGED'; END IF;
 IF d.purge_plan_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_ALREADY_PENDING'; END IF;
 report:=app._purge_report(p_document,p_retention_seconds); objects:=app._purge_manifest(p_document);
 SELECT coalesce(max(plan_version),0)+1 INTO version FROM app.purge_plans WHERE document_id=p_document;
 INSERT INTO app.purge_plans(document_id,plan_version,principal_id,reference_report,expires_at,retention_seconds,document_fingerprint)
 VALUES(p_document,version,p_principal,report,clock_timestamp()+make_interval(secs=>p_plan_ttl_seconds),p_retention_seconds,
  app._purge_fingerprint(p_document,report)) RETURNING * INTO p;
 IF app._purge_manifest_valid(p_document,objects) THEN
  INSERT INTO app.purge_objects(plan_id,document_id,object_id,document_version_id,parse_generation_id,intent_id,
   kind,bucket,object_key,object_version_id,sha256,size_bytes,media_type)
  SELECT p.id,p_document,(o->>'object_id')::uuid,(o->>'document_version_id')::uuid,(o->>'parse_generation_id')::uuid,(o->>'intent_id')::uuid,
   o->>'kind',o->>'bucket',o->>'object_key',o->>'object_version_id',o->>'sha256',(o->>'size_bytes')::bigint,o->>'media_type'
  FROM jsonb_array_elements(objects) o;
 END IF;
 RETURN jsonb_build_object('plan_id',p.id,'document_id',p.document_id,'plan_version',p.plan_version,
  'created_at',p.created_at,'expires_at',p.expires_at)||report;
END $$;
CREATE FUNCTION app.get_document_purge(p_document UUID,p_plan UUID,p_principal TEXT) RETURNS JSONB
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT app._purge_status(p) FROM app.purge_plans p WHERE id=p_plan AND document_id=p_document AND principal_id=p_principal AND document_fingerprint IS NOT NULL
$$;
CREATE FUNCTION app.document_purge_status(p_document UUID) RETURNS TEXT
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT p.status FROM app.logical_documents d JOIN app.purge_plans p ON p.id=d.purge_plan_id AND p.document_id=d.id
 WHERE d.id=p_document AND p.status IN ('purge_pending','failed','completed')
$$;
CREATE FUNCTION app._purge_still_unreferenced(p app.purge_plans) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE report JSONB;
BEGIN
 report:=app._purge_report(p.document_id,p.retention_seconds);
 -- The acceptance transition updates document.updated_at itself. Quiet-time
 -- eligibility was fixed/rechecked there; later guards recheck every live ref.
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_each_text(report->'references') r WHERE key<>'objects' AND value::bigint>0)
  AND NOT (report->'blockers' ? 'OBJECT_SCOPE_INVALID') AND EXISTS(
   SELECT 1 FROM app.logical_documents WHERE id=p.document_id AND purge_plan_id=p.id AND
    (archived_at IS NOT NULL OR security_revoked_at IS NOT NULL));
END $$;
CREATE FUNCTION app._purge_scope_guard() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE document UUID; d app.logical_documents;
BEGIN
 IF TG_TABLE_NAME='logical_documents' THEN
  IF (NEW.purge_plan_id,NEW.purge_pending_at,NEW.purged_at) IS DISTINCT FROM (OLD.purge_plan_id,OLD.purge_pending_at,OLD.purged_at)
    AND NOT app._purge_bound(NEW.id) THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PURGE_AUDIT_IMMUTABLE'; END IF;
  IF NEW IS NOT DISTINCT FROM OLD OR app._purge_bound(NEW.id) THEN RETURN NEW; END IF;
  document:=NEW.id;
 ELSIF TG_TABLE_NAME='upload_intents' THEN
  document:=NEW.document_id;
  IF TG_OP='UPDATE' AND NEW.state IN ('cleanup_pending','cleaned') THEN RETURN NEW; END IF;
 ELSIF TG_TABLE_NAME IN ('kb_snapshot_items','publications') THEN document:=NEW.logical_document_id;
 ELSIF TG_TABLE_NAME='document_versions' THEN document:=NEW.logical_document_id;
 ELSIF TG_TABLE_NAME='ingestion_jobs' THEN
  SELECT logical_document_id INTO document FROM app.document_versions WHERE id=NEW.version_id;
 ELSIF TG_TABLE_NAME='parse_artifact_intents' THEN
  IF TG_OP='UPDATE' AND NEW.state IN ('cleanup_pending','cleaned') THEN RETURN NEW; END IF;
  SELECT logical_document_id INTO document FROM app.document_versions WHERE id=NEW.document_version_id;
 ELSIF TG_TABLE_NAME IN ('parse_generations','index_generations') THEN
  SELECT logical_document_id INTO document FROM app.document_versions WHERE id=NEW.document_version_id;
 ELSE
  SELECT v.logical_document_id INTO document FROM knowledge.parse_generations p JOIN app.document_versions v ON v.id=p.document_version_id
   WHERE p.id=NEW.parse_generation_id;
 END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT * INTO d FROM app.logical_documents WHERE id=document;
 IF d.purged_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_PURGED'; END IF;
 IF d.purge_plan_id IS NOT NULL AND NOT app._purge_bound(document) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_PURGE_PENDING'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER a_purge_scope BEFORE UPDATE ON app.logical_documents FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON app.document_versions FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON app.upload_intents FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON app.ingestion_jobs FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON app.publications FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON app.parse_artifact_intents FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON knowledge.parse_generations FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON knowledge.index_generations FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON knowledge.document_nodes FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON knowledge.chunks FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT OR UPDATE ON knowledge.node_routing_embeddings FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();
CREATE TRIGGER a_purge_scope BEFORE INSERT ON agent.kb_snapshot_items FOR EACH ROW EXECUTE FUNCTION app._purge_scope_guard();

ALTER FUNCTION app.publish_version(UUID,UUID,UUID,UUID,UUID,UUID,BIGINT) RENAME TO _publish_before_purge;
REVOKE ALL ON FUNCTION app._publish_before_purge(UUID,UUID,UUID,UUID,UUID,UUID,BIGINT) FROM PUBLIC,expert_backend,expert_ingest;
CREATE FUNCTION app.publish_version(p_version UUID,p_index UUID,p_expected UUID,p_operation UUID,
 p_job UUID DEFAULT NULL,p_owner UUID DEFAULT NULL,p_epoch BIGINT DEFAULT NULL) RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE d app.logical_documents;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT doc.* INTO d FROM app.logical_documents doc JOIN app.document_versions v ON v.logical_document_id=doc.id
  WHERE v.id=p_version FOR UPDATE OF doc;
 IF d.purged_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_PURGED'; END IF;
 IF d.purge_plan_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_PURGE_PENDING'; END IF;
 RETURN app._publish_before_purge(p_version,p_index,p_expected,p_operation,p_job,p_owner,p_epoch);
END $$;
GRANT EXECUTE ON FUNCTION app.publish_version(UUID,UUID,UUID,UUID,UUID,UUID,BIGINT) TO expert_backend,expert_ingest;

-- INSERT/UPDATE retain the original guard unchanged. DELETE preserves its staging
-- behavior and admits ready content only under the protected finalizer binding.
CREATE FUNCTION knowledge._purge_content_delete() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE document UUID; state TEXT;
BEGIN
 SELECT v.logical_document_id INTO document FROM knowledge.parse_generations p JOIN app.document_versions v ON v.id=p.document_version_id
  WHERE p.id=OLD.parse_generation_id;
 IF TG_TABLE_NAME='document_nodes' THEN
  SELECT status INTO state FROM knowledge.parse_generations WHERE id=OLD.parse_generation_id FOR UPDATE;
 ELSE
  SELECT status INTO state FROM knowledge.index_generations WHERE id=OLD.index_generation_id FOR UPDATE;
 END IF;
 IF state<>'staging' AND NOT app._purge_bound(document) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 RETURN OLD;
END $$;
DROP TRIGGER nodes_content_guard ON knowledge.document_nodes;
DROP TRIGGER chunks_content_guard ON knowledge.chunks;
DROP TRIGGER routing_content_guard ON knowledge.node_routing_embeddings;
CREATE TRIGGER nodes_content_guard BEFORE INSERT OR UPDATE ON knowledge.document_nodes FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();
CREATE TRIGGER chunks_content_guard BEFORE INSERT OR UPDATE ON knowledge.chunks FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();
CREATE TRIGGER routing_content_guard BEFORE INSERT OR UPDATE ON knowledge.node_routing_embeddings FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();
CREATE TRIGGER nodes_purge_delete BEFORE DELETE ON knowledge.document_nodes FOR EACH ROW EXECUTE FUNCTION knowledge._purge_content_delete();
CREATE TRIGGER chunks_purge_delete BEFORE DELETE ON knowledge.chunks FOR EACH ROW EXECUTE FUNCTION knowledge._purge_content_delete();
CREATE TRIGGER routing_purge_delete BEFORE DELETE ON knowledge.node_routing_embeddings FOR EACH ROW EXECUTE FUNCTION knowledge._purge_content_delete();

CREATE FUNCTION app.finalize_document_purge(p_plan UUID) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p app.purge_plans; document UUID; node_level INTEGER; counts JSONB:='{}'; n BIGINT; nodes_n BIGINT:=0;
BEGIN
 IF p_plan IS NULL THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PURGE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT document_id INTO document FROM app.purge_plans WHERE id=p_plan;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_PLAN_NOT_FOUND'; END IF;
 PERFORM 1 FROM app.logical_documents WHERE id=document FOR UPDATE;
 SELECT * INTO STRICT p FROM app.purge_plans WHERE id=p_plan FOR UPDATE;
 IF p.status='completed' THEN RETURN app._purge_status(p); END IF;
 IF p.status<>'purge_pending' OR EXISTS(SELECT 1 FROM app.purge_objects WHERE plan_id=p.id AND deleted_at IS NULL) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_BLOCKED'; END IF;
 IF NOT app._purge_still_unreferenced(p) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_BLOCKED'; END IF;
 PERFORM app._purge_bind(p.id,document);
 DELETE FROM knowledge.chunks WHERE parse_generation_id IN (SELECT g.id FROM knowledge.parse_generations g
  JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=document);
 GET DIAGNOSTICS n=ROW_COUNT; counts:=counts||jsonb_build_object('chunks_deleted',n);
 DELETE FROM knowledge.node_routing_embeddings WHERE parse_generation_id IN (SELECT g.id FROM knowledge.parse_generations g
  JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=document);
 GET DIAGNOSTICS n=ROW_COUNT; counts:=counts||jsonb_build_object('descriptors_deleted',n);
 -- Parent links are RESTRICT. Delete one depth at a time, leaves first.
 FOR node_level IN SELECT DISTINCT level FROM knowledge.document_nodes WHERE parse_generation_id IN
  (SELECT g.id FROM knowledge.parse_generations g JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=document)
  ORDER BY level DESC LOOP
  DELETE FROM knowledge.document_nodes WHERE level=node_level AND parse_generation_id IN
   (SELECT g.id FROM knowledge.parse_generations g JOIN app.document_versions v ON v.id=g.document_version_id WHERE v.logical_document_id=document);
  GET DIAGNOSTICS n=ROW_COUNT; nodes_n:=nodes_n+n;
 END LOOP;
 counts:=counts||jsonb_build_object('nodes_deleted',nodes_n);
 UPDATE app.stored_objects SET state='deleted' WHERE id IN (SELECT object_id FROM app.purge_objects WHERE plan_id=p.id);
 UPDATE app.logical_documents SET purged_at=clock_timestamp(),updated_at=clock_timestamp(),row_version=row_version+1 WHERE id=document;
 UPDATE app.purge_plans SET status='completed',completed_at=clock_timestamp(),error_code=NULL,cleanup_counts=counts WHERE id=p.id RETURNING * INTO p;
 UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=clock_timestamp();
 DELETE FROM app.purge_write_bindings WHERE transaction_id=pg_current_xact_id() AND backend_pid=pg_backend_pid();
 RETURN app._purge_status(p);
END $$;
CREATE FUNCTION app.accept_document_purge(p_document UUID,p_plan UUID,p_plan_version BIGINT,p_principal TEXT) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE p app.purge_plans; d app.logical_documents; report JSONB;
BEGIN
 IF p_document IS NULL OR p_plan IS NULL OR p_plan_version IS NULL OR p_plan_version<1 OR
  p_principal IS NULL OR p_principal ~ '^[[:space:]]*$' OR length(p_principal)>200 OR
  current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PURGE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO d FROM app.logical_documents WHERE id=p_document FOR UPDATE;
 SELECT * INTO p FROM app.purge_plans WHERE id=p_plan AND document_id=p_document AND principal_id=p_principal AND document_fingerprint IS NOT NULL FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_PLAN_NOT_FOUND'; END IF;
 IF p.plan_version<>p_plan_version THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_PLAN_CHANGED'; END IF;
 IF p.status='planned' THEN
  IF d.purge_plan_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_ALREADY_PENDING'; END IF;
  IF p.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_PLAN_EXPIRED'; END IF;
  report:=app._purge_report(p_document,p.retention_seconds);
  IF p.reference_report->'allowed' IS DISTINCT FROM 'true'::jsonb OR report->'allowed' IS DISTINCT FROM 'true'::jsonb THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_BLOCKED'; END IF;
  IF p.document_fingerprint IS DISTINCT FROM app._purge_fingerprint(p_document,report) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_PLAN_CHANGED'; END IF;
  PERFORM app._purge_bind(p.id,p_document);
  UPDATE app.logical_documents SET purge_plan_id=p.id,purge_pending_at=clock_timestamp(),updated_at=clock_timestamp(),row_version=row_version+1 WHERE id=p_document;
  UPDATE app.stored_objects SET state='purge_pending' WHERE id IN (SELECT object_id FROM app.purge_objects WHERE plan_id=p.id);
  UPDATE app.purge_plans SET status='purge_pending',accepted_at=clock_timestamp() WHERE id=p.id RETURNING * INTO p;
  UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=clock_timestamp();
  DELETE FROM app.purge_write_bindings WHERE transaction_id=pg_current_xact_id() AND backend_pid=pg_backend_pid();
 ELSIF p.status='failed' THEN
  IF NOT app._purge_still_unreferenced(p) THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='PURGE_BLOCKED'; END IF;
  UPDATE app.purge_objects SET attempt=0,available_at=clock_timestamp(),claim_owner=NULL,claim_token=NULL,claim_until=NULL,last_error_code=NULL
   WHERE plan_id=p.id AND deleted_at IS NULL;
  UPDATE app.purge_plans SET status='purge_pending',error_code=NULL WHERE id=p.id RETURNING * INTO p;
 END IF;
 IF p.status='purge_pending' AND NOT EXISTS(SELECT 1 FROM app.purge_objects WHERE plan_id=p.id AND deleted_at IS NULL) THEN
  PERFORM app.finalize_document_purge(p.id); SELECT * INTO STRICT p FROM app.purge_plans WHERE id=p.id;
 END IF;
 RETURN jsonb_build_object('plan_id',p.id,'document_id',p.document_id,'status',p.status);
END $$;

CREATE FUNCTION app._purge_object_claim(o app.purge_objects,p_audit BOOLEAN) RETURNS JSONB
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT (to_jsonb(o)-ARRAY['claim_owner','available_at','deleted_at','next_check_at','last_error_code'])||jsonb_build_object('audit',p_audit)
$$;
CREATE FUNCTION app.claim_purge_objects(p_owner UUID,p_limit INTEGER DEFAULT 20,p_lease_seconds INTEGER DEFAULT 30)
RETURNS SETOF JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE o app.purge_objects; p app.purge_plans; audit BOOLEAN;
BEGIN
 IF p_owner IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 20 OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 30 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PURGE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 -- A worker crash on its eighth attempt is still bounded, even if no explicit
 -- reschedule call arrives. Its expired lease must not strand a pending plan.
 UPDATE app.purge_plans plan SET status='failed',error_code='PURGE_ATTEMPTS_EXHAUSTED'
  WHERE plan.status='purge_pending' AND EXISTS(SELECT 1 FROM app.purge_objects object WHERE object.plan_id=plan.id AND
   object.deleted_at IS NULL AND object.attempt>=8 AND object.claim_until<=clock_timestamp());
 FOR o IN SELECT object.* FROM app.purge_objects object JOIN app.purge_plans plan ON plan.id=object.plan_id
  WHERE object.available_at<=clock_timestamp() AND (object.claim_until IS NULL OR object.claim_until<=clock_timestamp()) AND
   ((plan.status='purge_pending' AND object.deleted_at IS NULL AND object.attempt<8) OR
    (plan.status='completed' AND object.next_check_at<=clock_timestamp()))
  ORDER BY object.available_at,object.plan_id,object.object_id LIMIT p_limit FOR UPDATE OF object SKIP LOCKED LOOP
  SELECT * INTO STRICT p FROM app.purge_plans WHERE id=o.plan_id;
  IF NOT app._purge_still_unreferenced(p) THEN CONTINUE; END IF;
  audit:=p.status='completed';
  UPDATE app.purge_objects SET claim_owner=p_owner,claim_token=gen_random_uuid(),claim_until=clock_timestamp()+make_interval(secs=>p_lease_seconds),
   attempt=CASE WHEN audit AND attempt>=8 THEN 1 ELSE attempt+1 END
   WHERE plan_id=o.plan_id AND object_id=o.object_id RETURNING * INTO o;
  RETURN NEXT app._purge_object_claim(o,audit);
 END LOOP;
END $$;
CREATE FUNCTION app.authorize_purge_delete(p_plan UUID,p_object UUID,p_owner UUID,p_token UUID) RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE o app.purge_objects; p app.purge_plans;
BEGIN
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO p FROM app.purge_plans WHERE id=p_plan;
 IF NOT FOUND OR p.status NOT IN ('purge_pending','completed') THEN RETURN NULL; END IF;
 SELECT * INTO o FROM app.purge_objects WHERE plan_id=p_plan AND object_id=p_object FOR UPDATE;
 IF NOT FOUND OR p_owner IS NULL OR p_token IS NULL OR (o.claim_owner,o.claim_token) IS DISTINCT FROM (p_owner,p_token) OR
  o.claim_until IS NULL OR o.claim_until<=clock_timestamp() OR NOT app._purge_still_unreferenced(p) THEN RETURN NULL; END IF;
 RETURN app._purge_object_claim(o,p.status='completed');
END $$;
CREATE FUNCTION app.mark_purge_object_deleted(p_plan UUID,p_object UUID,p_owner UUID,p_token UUID) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE authority JSONB; p app.purge_plans;
BEGIN
 authority:=app.authorize_purge_delete(p_plan,p_object,p_owner,p_token);
 IF authority IS NULL THEN RETURN false; END IF;
 UPDATE app.purge_objects SET deleted_at=coalesce(deleted_at,clock_timestamp()),next_check_at=clock_timestamp()+interval '1 hour',
  available_at=clock_timestamp()+interval '1 hour',attempt=0,claim_owner=NULL,claim_token=NULL,claim_until=NULL,last_error_code=NULL
  WHERE plan_id=p_plan AND object_id=p_object;
 SELECT * INTO STRICT p FROM app.purge_plans WHERE id=p_plan;
 IF p.status='purge_pending' AND NOT EXISTS(SELECT 1 FROM app.purge_objects WHERE plan_id=p_plan AND deleted_at IS NULL) THEN
  PERFORM app.finalize_document_purge(p_plan);
 END IF;
 RETURN true;
END $$;
CREATE FUNCTION app.reschedule_purge_object(p_plan UUID,p_object UUID,p_owner UUID,p_token UUID,p_code TEXT,p_delay_seconds INTEGER) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE authority JSONB; exhausted BOOLEAN; audit BOOLEAN;
BEGIN
 IF p_code IS NULL OR p_code NOT IN ('PURGE_STORAGE_UNAVAILABLE','PURGE_OBJECT_MISMATCH','PURGE_DELETE_UNVERIFIED','PURGE_ATTEMPTS_EXHAUSTED') OR
  p_delay_seconds IS NULL OR p_delay_seconds NOT BETWEEN 1 AND 3600 THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_PURGE_ARGUMENT'; END IF;
 authority:=app.authorize_purge_delete(p_plan,p_object,p_owner,p_token);
 IF authority IS NULL THEN RETURN false; END IF;
 exhausted:=(authority->>'attempt')::integer>=8; audit:=(authority->>'audit')::boolean;
 UPDATE app.purge_objects SET claim_owner=NULL,claim_token=NULL,claim_until=NULL,
  available_at=clock_timestamp()+make_interval(secs=>CASE WHEN audit AND exhausted THEN 3600 ELSE p_delay_seconds END),
  next_check_at=CASE WHEN audit AND exhausted THEN clock_timestamp()+interval '1 hour' ELSE next_check_at END,
  last_error_code=CASE WHEN exhausted THEN 'PURGE_ATTEMPTS_EXHAUSTED' ELSE p_code END
  WHERE plan_id=p_plan AND object_id=p_object;
 IF NOT audit THEN
  UPDATE app.purge_plans SET status=CASE WHEN exhausted THEN 'failed' ELSE status END,
   error_code=CASE WHEN exhausted THEN 'PURGE_ATTEMPTS_EXHAUSTED' ELSE p_code END WHERE id=p_plan;
 END IF;
 RETURN true;
END $$;

REVOKE ALL ON app.purge_plans,app.purge_objects,app.purge_write_bindings FROM PUBLIC,expert_backend,expert_runtime,expert_ingest,expert_outbox;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA app FROM PUBLIC;
REVOKE ALL ON FUNCTION knowledge._purge_content_delete() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app.plan_document_purge(UUID,TEXT,INTEGER,INTEGER),app.accept_document_purge(UUID,UUID,BIGINT,TEXT),
 app.get_document_purge(UUID,UUID,TEXT),app.claim_purge_objects(UUID,INTEGER,INTEGER),app.authorize_purge_delete(UUID,UUID,UUID,UUID),
 app.mark_purge_object_deleted(UUID,UUID,UUID,UUID),app.reschedule_purge_object(UUID,UUID,UUID,UUID,TEXT,INTEGER),
 app.finalize_document_purge(UUID) TO expert_backend;
GRANT EXECUTE ON FUNCTION app.document_purge_status(UUID) TO expert_backend;
