-- Ordinary archive changes eligibility for future snapshots only. It preserves
-- the current publication pointer and every immutable historical source binding.
CREATE TABLE app.document_archivals (
 operation_id UUID PRIMARY KEY,
 document_id UUID NOT NULL REFERENCES app.logical_documents ON DELETE RESTRICT,
 principal_id TEXT NOT NULL CHECK(length(principal_id)<=200 AND principal_id !~ '^[[:space:]]*$'),
 expected_current_publication_id UUID,
 reason TEXT NOT NULL CHECK(length(reason)<=2000 AND reason !~ '^[[:space:]]*$'),
 archived_at TIMESTAMPTZ NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(expected_current_publication_id,document_id)
  REFERENCES app.publications(id,logical_document_id) ON DELETE RESTRICT
);
CREATE INDEX document_archivals_document ON app.document_archivals(document_id,recorded_at);
CREATE TRIGGER document_archivals_immutable BEFORE UPDATE OR DELETE ON app.document_archivals
 FOR EACH ROW EXECUTE FUNCTION app.reject_mutation();

CREATE FUNCTION app.archive_document(p_document_id UUID,p_principal_id TEXT,
 p_expected_current_publication_id UUID,p_operation_id UUID,p_reason TEXT)
RETURNS app.logical_documents LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE d app.logical_documents; existing app.document_archivals; ts TIMESTAMPTZ;
BEGIN
 IF p_document_id IS NULL OR p_operation_id IS NULL OR p_principal_id IS NULL OR
  length(p_principal_id)>200 OR p_principal_id ~ '^[[:space:]]*$' OR
  p_reason IS NULL OR length(p_reason)>2000 OR p_reason ~ '^[[:space:]]*$' THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_ARCHIVE_ARGUMENT'; END IF;
 PERFORM 1 FROM app.knowledge_catalog FOR UPDATE;
 SELECT * INTO d FROM app.logical_documents WHERE id=p_document_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='DOCUMENT_NOT_FOUND'; END IF;
 IF d.security_revoked_at IS NOT NULL THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='SOURCE_REVOKED'; END IF;
 SELECT * INTO existing FROM app.document_archivals WHERE operation_id=p_operation_id;
 IF FOUND THEN
  IF (existing.document_id,existing.principal_id,existing.expected_current_publication_id,existing.reason)
   IS DISTINCT FROM (p_document_id,p_principal_id,p_expected_current_publication_id,p_reason) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN d;
 END IF;
 IF d.current_publication_id IS DISTINCT FROM p_expected_current_publication_id THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='VERSION_CONFLICT'; END IF;
 IF d.archived_at IS NULL THEN
  ts:=clock_timestamp();
  UPDATE app.logical_documents SET archived_at=ts,row_version=row_version+1,updated_at=ts
   WHERE id=d.id RETURNING * INTO d;
  UPDATE knowledge.chunks SET searchable=false WHERE searchable AND index_generation_id IN
   (SELECT g.id FROM knowledge.index_generations g JOIN app.document_versions v
    ON v.id=g.document_version_id WHERE v.logical_document_id=d.id);
  UPDATE knowledge.node_routing_embeddings SET searchable=false WHERE searchable AND index_generation_id IN
   (SELECT g.id FROM knowledge.index_generations g JOIN app.document_versions v
    ON v.id=g.document_version_id WHERE v.logical_document_id=d.id);
  UPDATE app.knowledge_catalog SET epoch=epoch+1,updated_at=ts;
  INSERT INTO app.outbox_events(aggregate_type,aggregate_id,event_type,topic,payload)
   VALUES('document',d.id,'document.archived','knowledge.events',jsonb_build_object('document_id',d.id));
 END IF;
 INSERT INTO app.document_archivals(operation_id,document_id,principal_id,
  expected_current_publication_id,reason,archived_at)
 VALUES(p_operation_id,d.id,p_principal_id,p_expected_current_publication_id,p_reason,d.archived_at);
 RETURN d;
END $$;

REVOKE ALL ON app.document_archivals FROM PUBLIC;
REVOKE ALL ON FUNCTION app.archive_document(UUID,TEXT,UUID,UUID,TEXT) FROM PUBLIC;
GRANT SELECT ON app.document_archivals TO expert_backend;
GRANT EXECUTE ON FUNCTION app.archive_document(UUID,TEXT,UUID,UUID,TEXT) TO expert_backend;
