CREATE FUNCTION knowledge.valid_spans(spans JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE s JSONB; b JSONB;
BEGIN
 IF jsonb_typeof(spans) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 FOR s IN SELECT value FROM jsonb_array_elements(spans) LOOP
  IF jsonb_typeof(s)<>'object' OR NOT(s ?& ARRAY['pdf_page','block_id','start_offset','end_offset']) THEN RETURN false; END IF;
  IF jsonb_typeof(s->'pdf_page')<>'number' OR (s->>'pdf_page')!~'^[0-9]+$' OR (s->>'pdf_page')::integer<1 OR
     jsonb_typeof(s->'block_id')<>'string' OR btrim(s->>'block_id')='' OR
     jsonb_typeof(s->'start_offset')<>'number' OR jsonb_typeof(s->'end_offset')<>'number' OR
     (s->>'start_offset')!~'^[0-9]+$' OR (s->>'end_offset')!~'^[0-9]+$' OR
     (s->>'end_offset')::bigint<(s->>'start_offset')::bigint THEN RETURN false; END IF;
  IF s ? 'bbox' AND s->'bbox'<>'null'::jsonb THEN
   b:=s->'bbox';
   IF jsonb_typeof(b)<>'array' OR jsonb_array_length(b)<>4 THEN RETURN false; END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements(b) v WHERE jsonb_typeof(v)<>'number') THEN RETURN false; END IF;
   IF (b->>0)::numeric<0 OR (b->>1)::numeric<0 OR (b->>0)::numeric>=(b->>2)::numeric OR (b->>1)::numeric>=(b->>3)::numeric THEN RETURN false; END IF;
  END IF;
 END LOOP;
 RETURN true;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;
CREATE TABLE knowledge.parse_generations (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), document_version_id UUID NOT NULL, source_sha256 app.sha256 NOT NULL,
 parser_fingerprint TEXT NOT NULL, normalizer_version TEXT NOT NULL, structure_version TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'staging' CHECK(status IN ('staging','ready','failed')),
 artifact_object_id UUID REFERENCES app.stored_objects ON DELETE RESTRICT,
 quality_report JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(quality_report)='object'),
 node_count INTEGER NOT NULL DEFAULT 0 CHECK(node_count>=0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), completed_at TIMESTAMPTZ,
 UNIQUE(id,document_version_id),
 FOREIGN KEY(document_version_id,source_sha256) REFERENCES app.document_versions(id,source_sha256) ON DELETE RESTRICT,
 CHECK(status<>'ready' OR (artifact_object_id IS NOT NULL AND completed_at IS NOT NULL AND node_count>0))
);
CREATE TABLE knowledge.document_nodes (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), parse_generation_id UUID NOT NULL REFERENCES knowledge.parse_generations ON DELETE RESTRICT,
 parent_id UUID, node_type TEXT NOT NULL CHECK(node_type IN ('document','section','chapter','article','clause','subclause','paragraph','appendix','table','figure','footnote','editorial_note','unknown')),
 level INTEGER NOT NULL CHECK(level>=0), ordinal INTEGER NOT NULL CHECK(ordinal>=0), number TEXT, title TEXT,
 canonical_text TEXT NOT NULL, structural_path JSONB NOT NULL CHECK(jsonb_typeof(structural_path)='array'),
 page_start INTEGER NOT NULL CHECK(page_start>0), page_end INTEGER NOT NULL CHECK(page_end>=page_start),
 source_spans JSONB NOT NULL CHECK(knowledge.valid_spans(source_spans)), table_data JSONB,
 extra_metadata JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(extra_metadata)='object'), content_hash app.sha256 NOT NULL,
 UNIQUE(id,parse_generation_id),
 FOREIGN KEY(parent_id,parse_generation_id) REFERENCES knowledge.document_nodes(id,parse_generation_id) ON DELETE RESTRICT,
 CHECK((parent_id IS NULL)=(level=0)), CHECK(parent_id IS DISTINCT FROM id), CHECK(parent_id IS NOT NULL OR node_type='document')
);
CREATE UNIQUE INDEX document_nodes_one_root ON knowledge.document_nodes(parse_generation_id) WHERE parent_id IS NULL;
CREATE INDEX document_nodes_parent_idx ON knowledge.document_nodes(parse_generation_id,parent_id,ordinal);
CREATE TABLE knowledge.index_generations (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), document_version_id UUID NOT NULL, parse_generation_id UUID NOT NULL,
 embedding_model_id TEXT NOT NULL, embedding_revision TEXT NOT NULL, embedding_dimension INTEGER NOT NULL CHECK(embedding_dimension=1536),
 tokenizer_revision TEXT NOT NULL, pooling_fingerprint TEXT NOT NULL, prefix_fingerprint TEXT NOT NULL,
 normalization TEXT NOT NULL CHECK(normalization='l2'), chunking_fingerprint TEXT NOT NULL, lexical_config_version TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'staging' CHECK(status IN ('staging','ready','failed')),
 chunk_count INTEGER NOT NULL DEFAULT 0 CHECK(chunk_count>=0), routing_count INTEGER NOT NULL DEFAULT 0 CHECK(routing_count>=0),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), completed_at TIMESTAMPTZ,
 UNIQUE(id,document_version_id), UNIQUE(id,parse_generation_id),
 FOREIGN KEY(parse_generation_id,document_version_id) REFERENCES knowledge.parse_generations(id,document_version_id) ON DELETE RESTRICT,
 CHECK(status<>'ready' OR (completed_at IS NOT NULL AND chunk_count>0 AND routing_count>0))
);
CREATE TABLE knowledge.chunks (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), index_generation_id UUID NOT NULL, parse_generation_id UUID NOT NULL, node_id UUID NOT NULL,
 chunk_index INTEGER NOT NULL CHECK(chunk_index>=0), source_text TEXT NOT NULL CHECK(length(source_text)>0),
 embedding_text TEXT NOT NULL, header_text TEXT NOT NULL, token_count INTEGER NOT NULL CHECK(token_count BETWEEN 1 AND 512),
 source_spans JSONB NOT NULL CHECK(knowledge.valid_spans(source_spans) AND jsonb_array_length(source_spans)>0),
 content_hash app.sha256 NOT NULL, embedding public.vector(1536) NOT NULL CHECK(abs(public.vector_norm(embedding)-1)<0.001),
 search_vector TSVECTOR GENERATED ALWAYS AS (
  setweight(to_tsvector('pg_catalog.simple',header_text),'A') ||
  setweight(to_tsvector('pg_catalog.russian',source_text),'B') ||
  setweight(to_tsvector('pg_catalog.simple',source_text),'C')) STORED,
 searchable BOOLEAN NOT NULL DEFAULT false, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(index_generation_id,node_id,chunk_index),
 FOREIGN KEY(index_generation_id,parse_generation_id) REFERENCES knowledge.index_generations(id,parse_generation_id) ON DELETE RESTRICT,
 FOREIGN KEY(node_id,parse_generation_id) REFERENCES knowledge.document_nodes(id,parse_generation_id) ON DELETE RESTRICT
);
CREATE INDEX chunks_dense_idx ON knowledge.chunks USING hnsw(embedding public.vector_cosine_ops) WHERE searchable;
CREATE INDEX chunks_lexical_idx ON knowledge.chunks USING gin(search_vector) WHERE searchable;
CREATE INDEX chunks_node_idx ON knowledge.chunks(node_id,index_generation_id);
CREATE TABLE knowledge.node_routing_embeddings (
 index_generation_id UUID NOT NULL, parse_generation_id UUID NOT NULL, node_id UUID NOT NULL,
 descriptor_text TEXT NOT NULL, embedding public.vector(1536) NOT NULL CHECK(abs(public.vector_norm(embedding)-1)<0.001),
 token_count INTEGER NOT NULL CHECK(token_count BETWEEN 1 AND 512), searchable BOOLEAN NOT NULL DEFAULT false,
 PRIMARY KEY(index_generation_id,node_id),
 FOREIGN KEY(index_generation_id,parse_generation_id) REFERENCES knowledge.index_generations(id,parse_generation_id) ON DELETE RESTRICT,
 FOREIGN KEY(node_id,parse_generation_id) REFERENCES knowledge.document_nodes(id,parse_generation_id) ON DELETE RESTRICT
);
CREATE INDEX routing_dense_idx ON knowledge.node_routing_embeddings USING hnsw(embedding public.vector_cosine_ops) WHERE searchable;
CREATE INDEX routing_node_idx ON knowledge.node_routing_embeddings(node_id);
CREATE TABLE app.publications (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), logical_document_id UUID NOT NULL,
 document_version_id UUID NOT NULL, index_generation_id UUID NOT NULL,
 published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), retired_at TIMESTAMPTZ,
 operation_id UUID NOT NULL UNIQUE, expected_previous_publication_id UUID REFERENCES app.publications ON DELETE RESTRICT,
 actor TEXT NOT NULL, reason TEXT NOT NULL,
 UNIQUE(id,logical_document_id), UNIQUE(id,document_version_id,index_generation_id),
 FOREIGN KEY(document_version_id,logical_document_id) REFERENCES app.document_versions(id,logical_document_id) ON DELETE RESTRICT,
 FOREIGN KEY(index_generation_id,document_version_id) REFERENCES knowledge.index_generations(id,document_version_id) ON DELETE RESTRICT,
 CHECK(retired_at IS NULL OR retired_at>=published_at)
);
ALTER TABLE app.logical_documents ADD CONSTRAINT current_publication_same_document
 FOREIGN KEY(current_publication_id,id) REFERENCES app.publications(id,logical_document_id) ON DELETE RESTRICT;
ALTER TABLE app.ingestion_jobs ADD FOREIGN KEY(expected_publication_id) REFERENCES app.publications ON DELETE RESTRICT;

CREATE FUNCTION knowledge.guard_generation_content() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE generation UUID; state TEXT;
BEGIN
 IF TG_TABLE_NAME='document_nodes' THEN
  generation:=CASE WHEN TG_OP='DELETE' THEN OLD.parse_generation_id ELSE NEW.parse_generation_id END;
  SELECT status INTO state FROM knowledge.parse_generations WHERE id=generation FOR UPDATE;
 ELSE
  generation:=CASE WHEN TG_OP='DELETE' THEN OLD.index_generation_id ELSE NEW.index_generation_id END;
  SELECT status INTO state FROM knowledge.index_generations WHERE id=generation FOR UPDATE;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)->>'id') IS DISTINCT FROM (to_jsonb(OLD)->>'id') OR
     (to_jsonb(NEW)->>'node_id') IS DISTINCT FROM (to_jsonb(OLD)->>'node_id') THEN
   RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='GENERATION_IDENTITY_IMMUTABLE';
  END IF;
  IF (to_jsonb(NEW)->>'parse_generation_id') IS DISTINCT FROM (to_jsonb(OLD)->>'parse_generation_id') OR
     (to_jsonb(NEW)->>'index_generation_id') IS DISTINCT FROM (to_jsonb(OLD)->>'index_generation_id') THEN
   RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='GENERATION_IDENTITY_IMMUTABLE';
  END IF;
 END IF;
 IF state<>'staging' THEN
  IF TG_TABLE_NAME<>'document_nodes' AND TG_OP='UPDATE' AND
     (to_jsonb(NEW)-ARRAY['searchable','search_vector'])=(to_jsonb(OLD)-ARRAY['searchable','search_vector']) AND current_user='expert_migrate' THEN RETURN NEW; END IF;
  RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='GENERATION_IMMUTABLE';
 END IF;
 IF TG_TABLE_NAME='document_nodes' AND TG_OP<>'DELETE' THEN
  IF TG_OP='UPDATE' THEN
   IF (NEW.parent_id,NEW.level) IS DISTINCT FROM (OLD.parent_id,OLD.level) THEN
    RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='NODE_STRUCTURE_IMMUTABLE';
   END IF;
  END IF;
  IF NEW.parent_id IS NOT NULL THEN
   IF NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE id=NEW.parent_id AND parse_generation_id=NEW.parse_generation_id AND level=NEW.level-1) THEN
    RAISE EXCEPTION USING ERRCODE='23514', MESSAGE='INVALID_NODE_PARENT_LEVEL';
   END IF;
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER nodes_content_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge.document_nodes FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();
CREATE TRIGGER chunks_content_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge.chunks FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();
CREATE TRIGGER routing_content_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge.node_routing_embeddings FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_content();

CREATE FUNCTION knowledge.guard_generation_state() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE actual_count BIGINT; actual_routing BIGINT;
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_MUST_START_STAGING'; END IF;
  RETURN NEW;
 END IF;
 IF OLD.status='ready' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF NEW.id<>OLD.id OR NEW.document_version_id<>OLD.document_version_id THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IDENTITY_IMMUTABLE'; END IF;
 IF NEW.status='ready' THEN
  IF TG_TABLE_NAME='parse_generations' THEN
   SELECT count(*) INTO actual_count FROM knowledge.document_nodes WHERE parse_generation_id=NEW.id;
   IF actual_count<>NEW.node_count OR NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=NEW.id AND parent_id IS NULL) OR
      NOT EXISTS(SELECT 1 FROM app.stored_objects WHERE id=NEW.artifact_object_id AND kind='parse_artifact' AND state='attached') OR
      NEW.quality_report->>'status' IS DISTINCT FROM 'passed' THEN
    RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PARSE_NOT_COMPLETE';
   END IF;
  ELSE
   SELECT count(*) INTO actual_count FROM knowledge.chunks WHERE index_generation_id=NEW.id;
   SELECT count(*) INTO actual_routing FROM knowledge.node_routing_embeddings WHERE index_generation_id=NEW.id;
   IF actual_count<>NEW.chunk_count OR actual_routing<>NEW.routing_count OR NOT EXISTS(SELECT 1 FROM knowledge.parse_generations WHERE id=NEW.parse_generation_id AND status='ready') THEN
    RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INDEX_NOT_COMPLETE';
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER parse_state_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge.parse_generations FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_state();
CREATE TRIGGER index_state_guard BEFORE INSERT OR UPDATE OR DELETE ON knowledge.index_generations FOR EACH ROW EXECUTE FUNCTION knowledge.guard_generation_state();
CREATE FUNCTION app.guard_version_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP<>'DELETE' AND NOT EXISTS(SELECT 1 FROM app.stored_objects WHERE id=NEW.source_object_id AND kind='original') THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_SOURCE_OBJECT_KIND';
 END IF;
 IF TG_OP='INSERT' THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND (NEW.id,NEW.logical_document_id) IS DISTINCT FROM (OLD.id,OLD.logical_document_id) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='VERSION_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.published_at IS NOT NULL AND (TG_OP='DELETE' OR
   (to_jsonb(NEW)-ARRAY['publication_status','deactivated_at'])<>(to_jsonb(OLD)-ARRAY['publication_status','deactivated_at'])) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PUBLISHED_VERSION_IMMUTABLE';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER version_identity_guard BEFORE INSERT OR UPDATE OR DELETE ON app.document_versions FOR EACH ROW EXECUTE FUNCTION app.guard_version_identity();
CREATE FUNCTION app.guard_object_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['state','expires_at'])<>(to_jsonb(OLD)-ARRAY['state','expires_at']) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='OBJECT_IDENTITY_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER object_identity_guard BEFORE UPDATE ON app.stored_objects FOR EACH ROW EXECUTE FUNCTION app.guard_object_identity();
