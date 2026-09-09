-- P09: selected committed evidence and ordinary version deactivation.
-- The caller releases this short PostgreSQL transaction before any PDF/S3 work.
CREATE FUNCTION agent.get_public_evidence(p_run_id UUID,p_principal_id TEXT,p_evidence_id TEXT)
RETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE unit JSONB; version app.document_versions; revoked TIMESTAMPTZ;
BEGIN
 IF p_run_id IS NULL OR p_principal_id IS NULL OR btrim(p_principal_id)='' OR
  p_evidence_id IS NULL OR btrim(p_evidence_id)='' OR length(p_evidence_id)>64 THEN RETURN NULL; END IF;
 -- A revoke committed before this barrier cannot be hidden by a stale current
 -- publication/cache lookup. The immutable run/result/snapshot remain pinned.
 PERFORM 1 FROM app.knowledge_catalog FOR SHARE;
 SELECT u.value INTO unit
 FROM agent.runs r
 JOIN agent.run_results rr ON rr.id=r.result_id AND rr.run_id=r.id AND rr.snapshot_id=r.snapshot_id
 JOIN agent.evidence_packs ep ON ep.id=rr.evidence_pack_id AND ep.run_id=r.id AND ep.snapshot_id=r.snapshot_id
 CROSS JOIN LATERAL jsonb_array_elements(ep.pack->'units') u(value)
 JOIN agent.kb_snapshot_items si ON si.snapshot_id=r.snapshot_id
  AND si.document_version_id=(u.value->>'document_version_id')::uuid
  AND si.index_generation_id=(u.value->>'index_generation_id')::uuid
 WHERE r.id=p_run_id AND r.principal_id=p_principal_id AND r.status='completed' AND rr.kind='completed'
  AND u.value->>'evidence_id'=p_evidence_id
  AND EXISTS(SELECT 1 FROM jsonb_array_elements(rr.citations_public) c WHERE c->>'evidence_id'=p_evidence_id);
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT * INTO version FROM app.document_versions WHERE id=(unit->>'document_version_id')::uuid;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT security_revoked_at INTO STRICT revoked FROM app.logical_documents WHERE id=version.logical_document_id;
 IF revoked IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 RETURN jsonb_build_object('evidence_id',p_evidence_id,'run_id',p_run_id,
  'document_version_id',version.id,'document_title',unit->'document_title','version_label',version.version_label,
  'structural_path',unit->'structural_path','excerpt',unit->'excerpt','source_spans',unit->'source_spans',
  'source_url','/api/v1/versions/'||version.id::text||'/source');
END $$;

CREATE TABLE app.version_deactivations (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 version_id UUID NOT NULL REFERENCES app.document_versions ON DELETE RESTRICT,
 principal_id TEXT NOT NULL CHECK(btrim(principal_id)<>'' AND length(principal_id)<=200),
 reason TEXT NOT NULL CHECK(btrim(reason)<>'' AND length(reason)<=1000),
 retired_publication_id UUID REFERENCES app.publications ON DELETE RESTRICT,
 deactivated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX version_deactivations_version ON app.version_deactivations(version_id,deactivated_at);
CREATE TRIGGER version_deactivations_immutable BEFORE UPDATE OR DELETE ON app.version_deactivations
 FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();

CREATE FUNCTION app.deactivate_version(p_version_id UUID,p_principal_id TEXT,p_reason TEXT)
RETURNS app.document_versions LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v app.document_versions; d app.logical_documents; retired UUID; ts TIMESTAMPTZ;
BEGIN
 IF p_version_id IS NULL OR p_principal_id IS NULL OR btrim(p_principal_id)='' OR length(p_principal_id)>200 OR
  p_reason IS NULL OR btrim(p_reason)='' OR length(p_reason)>1000 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_DEACTIVATION_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO v FROM app.document_versions WHERE id=p_version_id;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_NOT_FOUND'; END IF;
 SELECT * INTO STRICT d FROM app.logical_documents WHERE id=v.logical_document_id FOR UPDATE;
 SELECT * INTO STRICT v FROM app.document_versions WHERE id=p_version_id FOR UPDATE;
 IF d.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 IF v.publication_status='deactivated' THEN RETURN v; END IF;
 ts:=clock_timestamp();
 IF d.current_publication_id IS NOT NULL THEN
  SELECT id INTO retired FROM app.publications WHERE id=d.current_publication_id AND document_version_id=p_version_id;
 END IF;
 IF retired IS NOT NULL THEN UPDATE app.publications SET retired_at=ts WHERE id=retired; END IF;
 UPDATE knowledge.chunks SET searchable=false WHERE index_generation_id IN
  (SELECT id FROM knowledge.index_generations WHERE document_version_id=p_version_id) AND searchable;
 UPDATE knowledge.node_routing_embeddings SET searchable=false WHERE index_generation_id IN
  (SELECT id FROM knowledge.index_generations WHERE document_version_id=p_version_id) AND searchable;
 UPDATE app.document_versions SET publication_status='deactivated',deactivated_at=ts WHERE id=p_version_id RETURNING * INTO v;
 UPDATE app.logical_documents SET current_publication_id=CASE WHEN retired IS NOT NULL THEN NULL ELSE current_publication_id END,
  row_version=row_version+1,updated_at=ts WHERE id=d.id;
 UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=ts;
 INSERT INTO app.version_deactivations(version_id,principal_id,reason,retired_publication_id,deactivated_at)
 VALUES(v.id,p_principal_id,p_reason,retired,ts);
 INSERT INTO app.outbox_events(aggregate_type,aggregate_id,event_type,topic,payload)
 VALUES('document',d.id,'document.deactivated','knowledge.events',jsonb_build_object('document_id',d.id,'version_id',v.id));
 RETURN v;
END $$;

-- A background job accepted while a version was staging must not resurrect it
-- after the operator deactivates it. The existing publication transaction rolls
-- back even when the old current publication was already retired earlier in it.
CREATE FUNCTION app.guard_deactivated_auto_publication() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF session_user='expert_ingest' AND EXISTS(SELECT 1 FROM app.document_versions
  WHERE id=NEW.document_version_id AND publication_status='deactivated') THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER deactivated_auto_publication_guard BEFORE INSERT ON app.publications
 FOR EACH ROW EXECUTE FUNCTION app.guard_deactivated_auto_publication();

-- A new explicit publication reactivates operational exposure. Earlier periods
-- of deactivation stay in the immutable audit; legal/source metadata stays fixed.
CREATE FUNCTION app.clear_reactivated_version_marker() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.publication_status='published' AND OLD.publication_status='deactivated' THEN NEW.deactivated_at:=NULL; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER reactivated_version_marker BEFORE UPDATE ON app.document_versions
 FOR EACH ROW EXECUTE FUNCTION app.clear_reactivated_version_marker();

REVOKE ALL ON FUNCTION agent.get_public_evidence(UUID,TEXT,TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION app.deactivate_version(UUID,TEXT,TEXT),app.guard_deactivated_auto_publication(),app.clear_reactivated_version_marker() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agent.get_public_evidence(UUID,TEXT,TEXT) TO expert_backend;
GRANT EXECUTE ON FUNCTION app.deactivate_version(UUID,TEXT,TEXT) TO expert_backend;
GRANT SELECT ON app.version_deactivations TO expert_backend;
