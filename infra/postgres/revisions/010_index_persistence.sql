ALTER TABLE knowledge.index_generations
 ADD COLUMN operation_id UUID,
 ADD COLUMN projection_manifest JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(projection_manifest)='object'),
 ADD COLUMN embedding_recipe JSONB NOT NULL DEFAULT '{}' CHECK(jsonb_typeof(embedding_recipe)='object'),
 ADD COLUMN expected_chunk_count INTEGER CHECK(expected_chunk_count BETWEEN 1 AND 100000),
 ADD COLUMN expected_routing_count INTEGER CHECK(expected_routing_count BETWEEN 1 AND 100000),
 ADD COLUMN failure_code TEXT,
 ADD UNIQUE(creating_job_id,creating_job_epoch,operation_id);
ALTER TABLE knowledge.chunks ADD COLUMN projection_metadata JSONB NOT NULL DEFAULT '{}'
 CHECK(jsonb_typeof(projection_metadata)='object');
ALTER TABLE knowledge.node_routing_embeddings
 ADD COLUMN source_spans JSONB NOT NULL DEFAULT '[]' CHECK(knowledge.valid_spans(source_spans)),
 ADD COLUMN content_hash app.sha256;

CREATE FUNCTION knowledge.guard_index_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (NEW.id,NEW.document_version_id,NEW.parse_generation_id,NEW.creating_job_id,NEW.creating_job_epoch,NEW.operation_id,
     NEW.embedding_model_id,NEW.embedding_revision,NEW.embedding_dimension,NEW.tokenizer_revision,NEW.pooling_fingerprint,
     NEW.prefix_fingerprint,NEW.normalization,NEW.chunking_fingerprint,NEW.lexical_config_version,
     NEW.projection_manifest,NEW.embedding_recipe,NEW.expected_chunk_count,NEW.expected_routing_count)
  IS DISTINCT FROM
    (OLD.id,OLD.document_version_id,OLD.parse_generation_id,OLD.creating_job_id,OLD.creating_job_epoch,OLD.operation_id,
     OLD.embedding_model_id,OLD.embedding_revision,OLD.embedding_dimension,OLD.tokenizer_revision,OLD.pooling_fingerprint,
     OLD.prefix_fingerprint,OLD.normalization,OLD.chunking_fingerprint,OLD.lexical_config_version,
     OLD.projection_manifest,OLD.embedding_recipe,OLD.expected_chunk_count,OLD.expected_routing_count) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IDENTITY_IMMUTABLE'; END IF;
 IF OLD.status IN ('ready','failed') AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER index_identity_guard BEFORE UPDATE ON knowledge.index_generations
 FOR EACH ROW EXECUTE FUNCTION knowledge.guard_index_identity();

CREATE TABLE knowledge.index_write_batches (
 index_generation_id UUID NOT NULL REFERENCES knowledge.index_generations ON DELETE RESTRICT,
 batch_kind TEXT NOT NULL CHECK(batch_kind IN ('chunks','descriptors')), batch_id UUID NOT NULL,
 payload_hash app.sha256 NOT NULL, request_hash app.sha256 NOT NULL,
 inserted_count INTEGER NOT NULL CHECK(inserted_count BETWEEN 1 AND 16),
 total_count INTEGER NOT NULL CHECK(total_count BETWEEN 1 AND 100000),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(index_generation_id,batch_kind,batch_id)
);
CREATE FUNCTION knowledge.guard_index_batch_identity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INDEX_BATCH_IMMUTABLE'; END $$;
CREATE TRIGGER index_batch_identity_guard BEFORE UPDATE OR DELETE ON knowledge.index_write_batches
 FOR EACH ROW EXECUTE FUNCTION knowledge.guard_index_batch_identity();

CREATE FUNCTION knowledge._index_uuid(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'string' THEN RETURN false; END IF;
 RETURN (v#>>'{}')=((v#>>'{}')::uuid)::text;
EXCEPTION WHEN invalid_text_representation THEN RETURN false;
END $$;
CREATE FUNCTION knowledge._index_sha(v JSONB) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT coalesce(jsonb_typeof(v)='string' AND (v#>>'{}') ~ '^[0-9a-f]{64}$',false)
$$;
CREATE FUNCTION knowledge._index_recipe(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['schema_version','model','revision','dimension','tokenizer_revision','pooling',
  'normalization','document_prefix','query_prefix','max_input_tokens','silent_truncation','tokenizer_fingerprint',
  'model_fingerprint','runtime_fingerprint','device','dtype']) THEN RETURN false; END IF;
 RETURN v->>'schema_version' IS NOT DISTINCT FROM 'p05.frida.v1' AND v->>'model' IS NOT DISTINCT FROM 'ai-forever/FRIDA' AND
  v->>'revision' IS NOT DISTINCT FROM '850455b605544a944739b25f81ddf812b6e3d0d5' AND
  v->>'tokenizer_revision' IS NOT DISTINCT FROM v->>'revision' AND knowledge._parse_integer(v->'dimension',1536,1536) AND
  v->>'pooling' IS NOT DISTINCT FROM 'cls_first_token' AND v->>'normalization' IS NOT DISTINCT FROM 'l2' AND
  v->>'document_prefix' IS NOT DISTINCT FROM 'search_document: ' AND v->>'query_prefix' IS NOT DISTINCT FROM 'search_query: ' AND
  knowledge._parse_integer(v->'max_input_tokens',512,512) AND v->'silent_truncation' IS NOT DISTINCT FROM 'false'::jsonb AND
  knowledge._index_sha(v->'tokenizer_fingerprint') AND knowledge._index_sha(v->'model_fingerprint') AND knowledge._index_sha(v->'runtime_fingerprint') AND
  v->>'device' IS NOT DISTINCT FROM 'cpu' AND v->>'dtype' IS NOT DISTINCT FROM 'float32';
END $$;
CREATE FUNCTION knowledge._index_parser_runtime(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE key TEXT;
BEGIN
 IF v='null'::jsonb THEN RETURN true; END IF;
 IF NOT knowledge._parse_closed(v,ARRAY['enforced','platform','no_new_privs','seccomp_network_denied','process_creation_denied',
  'landlock_abi','memory_bytes','cpu_seconds','file_bytes','open_files','processes','cpu_count','memory_max_bytes','swap_max_bytes','requested_profile_sha256']) OR
  jsonb_typeof(v->'platform') IS DISTINCT FROM 'string' OR length(v->>'platform') NOT BETWEEN 1 AND 32 THEN RETURN false; END IF;
 FOREACH key IN ARRAY ARRAY['enforced','no_new_privs','seccomp_network_denied','process_creation_denied'] LOOP
  IF jsonb_typeof(v->key) IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
 END LOOP;
 RETURN knowledge._parse_integer(v->'landlock_abi',0,100) AND knowledge._parse_integer(v->'memory_bytes',1,17179869184) AND
  knowledge._parse_integer(v->'cpu_seconds',1,7200) AND knowledge._parse_integer(v->'file_bytes',1,268435456) AND
  knowledge._parse_integer(v->'open_files',1,1024) AND knowledge._parse_integer(v->'processes',1,256) AND
  knowledge._parse_integer(v->'cpu_count',1,2) AND
  (v->'memory_max_bytes'='null'::jsonb OR knowledge._parse_integer(v->'memory_max_bytes',1,9223372036854775807)) AND
  (v->'swap_max_bytes'='null'::jsonb OR knowledge._parse_integer(v->'swap_max_bytes',0,9223372036854775807)) AND
  (v->'requested_profile_sha256'='null'::jsonb OR knowledge._index_sha(v->'requested_profile_sha256'));
END $$;
CREATE FUNCTION knowledge._index_parser_manifest(v JSONB,pages INTEGER) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE n JSONB; limits JSONB; key TEXT;
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['schema_version','parser_version','pymupdf_version','normalizer_version','structure_version',
  'coordinate_system','limits','table_adapter_version','docling_version','fallback_pages','asset_fingerprints','symbol_adapter_version','parser_fingerprint','runtime_profile']) OR
  v->>'schema_version' IS DISTINCT FROM 'p04.parse.v1' OR v->>'coordinate_system' IS DISTINCT FROM 'unrotated_cropbox_top_left_pt' OR
  octet_length(v::text)>65536 OR (v->'parser_fingerprint'<>'null'::jsonb AND NOT knowledge._index_sha(v->'parser_fingerprint')) OR
  NOT knowledge._index_parser_runtime(v->'runtime_profile') OR
  jsonb_typeof(v->'fallback_pages') IS DISTINCT FROM 'array' OR jsonb_typeof(v->'asset_fingerprints') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 FOREACH key IN ARRAY ARRAY['parser_version','pymupdf_version','normalizer_version','structure_version'] LOOP
  IF jsonb_typeof(v->key) IS DISTINCT FROM 'string' OR length(v->>key) NOT BETWEEN 1 AND 200 THEN RETURN false; END IF;
 END LOOP;
 FOREACH key IN ARRAY ARRAY['table_adapter_version','docling_version','symbol_adapter_version'] LOOP
  IF v->key<>'null'::jsonb AND (jsonb_typeof(v->key)<>'string' OR length(v->>key)>100) THEN RETURN false; END IF;
 END LOOP;
 limits:=v->'limits';
 IF NOT knowledge._parse_closed(limits,ARRAY['max_bytes','max_pages','max_characters','max_blocks','max_artifact_bytes','wall_seconds','render_dpi','symbol_render_dpi','max_page_pixels']) OR
  NOT knowledge._parse_integer(limits->'max_bytes',1,52428800) OR NOT knowledge._parse_integer(limits->'max_pages',pages,500) OR
  NOT knowledge._parse_integer(limits->'max_characters',1,10000000) OR NOT knowledge._parse_integer(limits->'max_blocks',1,100000) OR
  NOT knowledge._parse_integer(limits->'max_artifact_bytes',1,268435456) OR NOT knowledge._parse_integer(limits->'wall_seconds',1,480) OR
  NOT knowledge._parse_integer(limits->'render_dpi',72,300) OR NOT knowledge._parse_integer(limits->'symbol_render_dpi',72,600) OR
  NOT knowledge._parse_integer(limits->'max_page_pixels',1,40000000) THEN RETURN false; END IF;
 IF jsonb_array_length(v->'fallback_pages')>500 OR jsonb_array_length(v->'asset_fingerprints')>100 OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(v->'fallback_pages') e WHERE NOT knowledge._parse_integer(e,1,pages)) OR
  (SELECT count(*)<>count(DISTINCT e) FROM jsonb_array_elements(v->'fallback_pages') e) THEN RETURN false; END IF;
 FOR n IN SELECT value FROM jsonb_array_elements(v->'asset_fingerprints') LOOP
  IF NOT knowledge._parse_closed(n,ARRAY['name','sha256']) OR jsonb_typeof(n->'name') IS DISTINCT FROM 'string' OR
   n->>'name'!~'^[a-zA-Z0-9_.-]{1,200}$' OR NOT knowledge._index_sha(n->'sha256') THEN RETURN false; END IF;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v->'asset_fingerprints') e GROUP BY e->>'name' HAVING count(*)>1);
END $$;
CREATE FUNCTION knowledge._index_manifest(v JSONB,recipe JSONB,g knowledge.parse_generations,expected_chunks INTEGER) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE c JSONB; n JSONB;
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['schema_version','source_sha256','version_id','parse_generation_id','parser','config',
  'tokenizer_model_id','tokenizer_revision','tokenizer_fingerprint','document_prefix','rendering','splitting','descriptors','lexical',
  'shortened_header_nodes','table_types','recipe_hash']) OR v->>'schema_version' IS DISTINCT FROM 'p04.chunks.v2' OR
  v->>'source_sha256' IS DISTINCT FROM g.source_sha256 OR v->>'version_id' IS DISTINCT FROM g.document_version_id::text OR
  v->>'parse_generation_id' IS DISTINCT FROM g.id::text OR NOT knowledge._index_sha(v->'recipe_hash') OR
  NOT knowledge._index_parser_manifest(v->'parser',g.physical_page_count) OR
  (v->'parser'->'parser_fingerprint'<>'null'::jsonb AND v->'parser'->>'parser_fingerprint' IS DISTINCT FROM g.parser_fingerprint) OR
  v->'parser'->>'normalizer_version' IS DISTINCT FROM g.normalizer_version OR v->'parser'->>'structure_version' IS DISTINCT FROM g.structure_version OR
  v->>'tokenizer_model_id' IS DISTINCT FROM recipe->>'model' OR v->>'tokenizer_revision' IS DISTINCT FROM recipe->>'tokenizer_revision' OR
  v->>'tokenizer_fingerprint' IS DISTINCT FROM recipe->>'tokenizer_fingerprint' OR v->>'document_prefix' IS DISTINCT FROM recipe->>'document_prefix' OR
  v->>'rendering' IS DISTINCT FROM 'header-newline-context-newline-body-v1' OR v->>'splitting' IS DISTINCT FROM 'paragraph-sentence-map-unicode-zero-overlap-v1' OR
  v->>'descriptors' IS DISTINCT FROM 'mapped-source-intro-v1' OR v->>'lexical' IS DISTINCT FROM 'canonical-header-source-v1' OR
  jsonb_typeof(v->'shortened_header_nodes') IS DISTINCT FROM 'array' OR jsonb_typeof(v->'table_types') IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 c:=v->'config';
 IF NOT knowledge._parse_closed(c,ARRAY['max_input_tokens','descriptor_max_tokens','max_chunks','max_tokenizer_calls','overlap']) OR
  NOT knowledge._parse_integer(c->'max_input_tokens',8,512) OR NOT knowledge._parse_integer(c->'descriptor_max_tokens',8,512) OR
  NOT knowledge._parse_integer(c->'max_chunks',expected_chunks,100000) OR NOT knowledge._parse_integer(c->'max_tokenizer_calls',1,1000000) OR
  NOT knowledge._parse_integer(c->'overlap',0,0) THEN RETURN false; END IF;
 IF jsonb_array_length(v->'shortened_header_nodes')>g.node_count OR jsonb_array_length(v->'table_types')>10000 OR
  (SELECT count(*)<>count(DISTINCT e) FROM jsonb_array_elements(v->'shortened_header_nodes') e) THEN RETURN false; END IF;
 FOR n IN SELECT value FROM jsonb_array_elements(v->'shortened_header_nodes') LOOP
  IF NOT knowledge._index_uuid(n) THEN RETURN false; END IF;
  IF NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE id=(n#>>'{}')::uuid AND parse_generation_id=g.id) THEN RETURN false; END IF;
 END LOOP;
 FOR n IN SELECT value FROM jsonb_array_elements(v->'table_types') LOOP
  IF NOT knowledge._parse_closed(n,ARRAY['node_id','table_id','kind']) OR NOT knowledge._index_uuid(n->'node_id') OR
   jsonb_typeof(n->'table_id') IS DISTINCT FROM 'string' OR jsonb_typeof(n->'kind') IS DISTINCT FROM 'string' OR
   n->>'kind' NOT IN ('data','blank_template') THEN RETURN false; END IF;
  IF NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE id=(n->>'node_id')::uuid AND parse_generation_id=g.id AND
   table_data->>'id'=n->>'table_id' AND table_data->>'kind'=n->>'kind') THEN RETURN false; END IF;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v->'table_types') e GROUP BY e->>'node_id' HAVING count(*)>1) AND
  jsonb_array_length(v->'table_types')=(SELECT count(*) FROM knowledge.document_nodes WHERE parse_generation_id=g.id AND table_data IS NOT NULL);
END $$;

CREATE FUNCTION knowledge._assert_index_owner(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID)
RETURNS knowledge.index_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; g knowledge.index_generations;
BEGIN
 j:=knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 SELECT * INTO g FROM knowledge.index_generations WHERE id=p_index_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_FOUND'; END IF;
 IF (g.creating_job_id,g.creating_job_epoch,g.document_version_id) IS DISTINCT FROM (j.id,p_epoch,j.version_id) THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='INDEX_OWNERSHIP_MISMATCH'; END IF;
 RETURN g;
END $$;
CREATE FUNCTION knowledge.begin_index(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_operation_id UUID,p_parse_id UUID,
 p_projection_manifest JSONB,p_embedding_recipe JSONB,p_expected_chunks INTEGER,p_expected_descriptors INTEGER)
RETURNS knowledge.index_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE j app.ingestion_jobs; parsed knowledge.parse_generations; g knowledge.index_generations;
BEGIN
 IF p_operation_id IS NULL OR p_parse_id IS NULL OR p_expected_chunks IS NULL OR p_expected_chunks NOT BETWEEN 1 AND 100000 OR
  p_expected_descriptors IS NULL OR p_expected_descriptors NOT BETWEEN 1 AND 100000 OR p_projection_manifest IS NULL OR
  p_embedding_recipe IS NULL OR octet_length(p_projection_manifest::text)>8388608 OR octet_length(p_embedding_recipe::text)>65536 OR
  NOT knowledge._index_recipe(p_embedding_recipe) THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INDEX_ARGUMENT'; END IF;
 j:=knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 SELECT * INTO parsed FROM knowledge.parse_generations WHERE id=p_parse_id AND document_version_id=j.version_id;
 IF NOT FOUND OR parsed.status<>'ready' OR parsed.physical_page_count IS NULL OR
  NOT EXISTS(SELECT 1 FROM app.document_versions WHERE id=j.version_id AND source_sha256=parsed.source_sha256) OR
  NOT EXISTS(SELECT 1 FROM app.stored_objects WHERE id=parsed.artifact_object_id AND kind='parse_artifact' AND state='attached') THEN
  RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_NOT_READY'; END IF;
 IF p_expected_descriptors>parsed.node_count OR NOT knowledge._index_manifest(p_projection_manifest,p_embedding_recipe,parsed,p_expected_chunks) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_INDEX_MANIFEST'; END IF;
 SELECT * INTO g FROM knowledge.index_generations WHERE creating_job_id=p_job_id AND creating_job_epoch=p_epoch AND operation_id=p_operation_id;
 IF FOUND THEN
  IF (g.parse_generation_id,g.projection_manifest,g.embedding_recipe,g.expected_chunk_count,g.expected_routing_count) IS DISTINCT FROM
     (p_parse_id,p_projection_manifest,p_embedding_recipe,p_expected_chunks,p_expected_descriptors) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 INSERT INTO knowledge.index_generations(document_version_id,parse_generation_id,creating_job_id,creating_job_epoch,operation_id,
  projection_manifest,embedding_recipe,expected_chunk_count,expected_routing_count,embedding_model_id,embedding_revision,embedding_dimension,
  tokenizer_revision,pooling_fingerprint,prefix_fingerprint,normalization,chunking_fingerprint,lexical_config_version)
 VALUES(j.version_id,p_parse_id,p_job_id,p_epoch,p_operation_id,p_projection_manifest,p_embedding_recipe,p_expected_chunks,p_expected_descriptors,
  p_embedding_recipe->>'model',p_embedding_recipe->>'revision',1536,p_embedding_recipe->>'tokenizer_revision',
  encode(sha256(convert_to((jsonb_build_object('pooling',p_embedding_recipe->'pooling','model_fingerprint',p_embedding_recipe->'model_fingerprint'))::text,'UTF8')),'hex'),
  encode(sha256(convert_to((jsonb_build_object('document',p_embedding_recipe->'document_prefix','query',p_embedding_recipe->'query_prefix'))::text,'UTF8')),'hex'),
  'l2',p_projection_manifest->>'recipe_hash',p_projection_manifest->>'lexical') RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;

CREATE FUNCTION knowledge._index_spans(p_parse_id UUID,v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
BEGIN
 IF NOT knowledge._parse_spans(v) OR jsonb_array_length(v)=0 THEN RETURN false; END IF;
 -- The immutable node registry provides membership/bounds. Raw glyph equality,
 -- normalization and exact crop geometry remain the pinned-artifact validator's job.
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v) s WHERE NOT EXISTS(
  SELECT 1 FROM knowledge.document_nodes n CROSS JOIN LATERAL jsonb_array_elements(n.source_spans) owned
  WHERE n.parse_generation_id=p_parse_id AND owned->>'pdf_page'=s->>'pdf_page' AND owned->>'block_id'=s->>'block_id' AND
   (owned->>'start_offset')::bigint<=(s->>'start_offset')::bigint AND (owned->>'end_offset')::bigint>=(s->>'end_offset')::bigint));
END $$;
CREATE FUNCTION knowledge._index_ranges(n knowledge.document_nodes,v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE r JSONB; body TEXT; mappings JSONB;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RETURN false; END IF;
 IF jsonb_array_length(v) NOT BETWEEN 1 AND 100000 THEN RETURN false; END IF;
 FOR r IN SELECT value FROM jsonb_array_elements(v) LOOP
  IF NOT knowledge._parse_closed(r,ARRAY['owner_kind','text_owner_id','start','end']) OR
   jsonb_typeof(r->'owner_kind') IS DISTINCT FROM 'string' OR r->>'owner_kind' NOT IN ('node_body','table_cell') OR
   jsonb_typeof(r->'text_owner_id') IS DISTINCT FROM 'string' OR NOT knowledge._parse_integer(r->'start',0,2000000) OR
   NOT knowledge._parse_integer(r->'end',1,2000000) OR (r->>'start')::integer>=(r->>'end')::integer THEN RETURN false; END IF;
  IF r->>'owner_kind'='node_body' THEN
   IF r->>'text_owner_id'<>n.id::text THEN RETURN false; END IF;
   body:=n.canonical_text; mappings:=n.extra_metadata->'char_map';
  ELSE
   SELECT cell->>'text',cell->'char_map' INTO body,mappings FROM jsonb_array_elements(n.table_data->'cells') cell WHERE cell->>'id'=r->>'text_owner_id';
   IF NOT FOUND THEN RETURN false; END IF;
  END IF;
  IF (r->>'end')::integer>length(body) OR EXISTS(SELECT 1 FROM jsonb_array_elements(mappings) m
   WHERE m->>'mapping'='normalized_group' AND (
    (r->>'start')::integer>(m->>'canonical_start')::integer AND (r->>'start')::integer<(m->>'canonical_end')::integer OR
    (r->>'end')::integer>(m->>'canonical_start')::integer AND (r->>'end')::integer<(m->>'canonical_end')::integer)) THEN RETURN false; END IF;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM (
  SELECT (range_entry->>'start')::integer AS start_at,lag((range_entry->>'end')::integer) OVER (
   PARTITION BY range_entry->>'owner_kind',range_entry->>'text_owner_id' ORDER BY (range_entry->>'start')::integer,(range_entry->>'end')::integer) AS previous_end
  FROM jsonb_array_elements(v) range_entry) intervals WHERE start_at<previous_end);
END $$;
CREATE FUNCTION knowledge._index_template(n knowledge.document_nodes,d JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE slot JSONB; cell JSONB;
BEGIN
 IF jsonb_typeof(d->'table_rows') IS DISTINCT FROM 'array' OR jsonb_typeof(d->'template_grid') IS DISTINCT FROM 'array' OR
  jsonb_typeof(d->'requires_expansion') IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
 IF jsonb_array_length(d->'table_rows')>100000 OR jsonb_array_length(d->'template_grid')>100000 OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(d->'table_rows') e WHERE NOT knowledge._parse_integer(e,0,10000000)) OR
  (SELECT count(*)<>count(DISTINCT e) FROM jsonb_array_elements(d->'table_rows') e) THEN RETURN false; END IF;
 IF n.table_data IS NULL THEN
  RETURN d->'table_kind'='null'::jsonb AND d->'table_rows'='[]'::jsonb AND d->'template_grid'='[]'::jsonb;
 END IF;
 IF d->>'table_kind' IS DISTINCT FROM n.table_data->>'kind' OR jsonb_array_length(d->'table_rows')=0 OR
  EXISTS(SELECT 1 FROM jsonb_array_elements(d->'table_rows') e WHERE NOT EXISTS(
   SELECT 1 FROM jsonb_array_elements(n.table_data->'rows') r WHERE r->'row_index'=e)) THEN RETURN false; END IF;
 IF d->>'table_kind'='data' THEN RETURN d->'template_grid'='[]'::jsonb; END IF;
 IF d->'requires_expansion' IS DISTINCT FROM 'true'::jsonb OR
  jsonb_array_length(d->'template_grid')<>jsonb_array_length(n.table_data->'cells') OR
  jsonb_array_length(d->'table_rows')<>jsonb_array_length(n.table_data->'rows') THEN RETURN false; END IF;
 FOR slot IN SELECT value FROM jsonb_array_elements(d->'template_grid') LOOP
  IF NOT knowledge._parse_closed(slot,ARRAY['cell_id','row','column','row_span','column_span','role','empty','pdf_page','bbox']) THEN RETURN false; END IF;
  SELECT e INTO cell FROM jsonb_array_elements(n.table_data->'cells') e WHERE e->>'id'=slot->>'cell_id';
  IF NOT FOUND OR (slot-'cell_id'-'empty') IS DISTINCT FROM (cell-ARRAY['id','text','char_map']) OR
   slot->'empty' IS DISTINCT FROM to_jsonb(btrim(cell->>'text')='') THEN RETURN false; END IF;
 END LOOP;
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(d->'template_grid') e GROUP BY e->>'cell_id' HAVING count(*)>1);
END $$;
CREATE FUNCTION knowledge._index_chunk(n knowledge.document_nodes,d JSONB,max_tokens INTEGER) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE context JSONB;
BEGIN
 IF NOT knowledge._parse_closed(d,ARRAY['node_id','chunk_index','canonical_ranges','source_text','header_text','embedding_text','source_spans',
  'context_refs','input_tokens','content_hash','table_rows','table_kind','template_grid','requires_expansion']) OR
  d->>'node_id' IS DISTINCT FROM n.id::text OR NOT knowledge._parse_integer(d->'chunk_index',0,99999) OR
  jsonb_typeof(d->'source_text') IS DISTINCT FROM 'string' OR length(d->>'source_text') NOT BETWEEN 1 AND 2000000 OR
  jsonb_typeof(d->'header_text') IS DISTINCT FROM 'string' OR length(d->>'header_text')>30000 OR
  jsonb_typeof(d->'embedding_text') IS DISTINCT FROM 'string' OR length(d->>'embedding_text') NOT BETWEEN 1 AND 30000 OR
  NOT knowledge._parse_integer(d->'input_tokens',1,max_tokens) OR NOT knowledge._index_sha(d->'content_hash') OR
  NOT knowledge._index_ranges(n,d->'canonical_ranges') OR NOT knowledge._index_spans(n.parse_generation_id,d->'source_spans') OR
  NOT knowledge._parse_context(d->'context_refs') OR NOT knowledge._index_template(n,d) THEN RETURN false; END IF;
 FOR context IN SELECT value FROM jsonb_array_elements(d->'context_refs') LOOP
  IF NOT knowledge._index_spans(n.parse_generation_id,context->'source_spans') THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;

CREATE FUNCTION knowledge._write_index_batch(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID,p_batch_id UUID,
 p_items JSONB,p_response_metadata JSONB,p_kind TEXT) RETURNS TABLE(inserted_count INTEGER,total_count INTEGER)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.index_generations; prior knowledge.index_write_batches; n knowledge.document_nodes;
 item JSONB; draft JSONB; payload app.sha256; request_digest app.sha256; input_items JSONB; vector_value public.vector;
 count_new INTEGER; count_all INTEGER; token_sum INTEGER:=0; max_tokens INTEGER;
BEGIN
 IF p_index_id IS NULL OR p_batch_id IS NULL OR p_kind IS NULL OR p_kind NOT IN ('chunks','descriptors') OR
  jsonb_typeof(p_items) IS DISTINCT FROM 'array' OR p_response_metadata IS NULL THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INDEX_BATCH'; END IF;
 IF jsonb_array_length(p_items) NOT BETWEEN 1 AND 16 OR octet_length(p_items::text)>8388608 OR
  NOT knowledge._parse_closed(p_response_metadata,ARRAY['model','revision','dimension','normalized']) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INDEX_BATCH'; END IF;
 g:=knowledge._assert_index_owner(p_job_id,p_owner,p_epoch,p_index_id);
 IF p_response_metadata->>'model' IS DISTINCT FROM g.embedding_model_id OR p_response_metadata->>'revision' IS DISTINCT FROM g.embedding_revision OR
  NOT knowledge._parse_integer(p_response_metadata->'dimension',1536,1536) OR p_response_metadata->'normalized' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='EMBEDDING_RECIPE_MISMATCH'; END IF;
 payload:=encode(sha256(convert_to(jsonb_build_object('items',p_items,'metadata',p_response_metadata)::text,'UTF8')),'hex');
 SELECT jsonb_agg(e-'vector' ORDER BY position) INTO input_items FROM jsonb_array_elements(p_items) WITH ORDINALITY AS items(e,position);
 request_digest:=encode(sha256(convert_to(jsonb_build_object('items',input_items,'metadata',p_response_metadata)::text,'UTF8')),'hex');
 SELECT * INTO prior FROM knowledge.index_write_batches WHERE index_generation_id=g.id AND batch_kind=p_kind AND batch_id=p_batch_id;
 IF FOUND THEN
  IF prior.payload_hash<>payload THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN QUERY SELECT prior.inserted_count,prior.total_count; RETURN;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 max_tokens:=(g.projection_manifest->'config'->>'max_input_tokens')::integer;
 -- The current projector's no-intro fallback reuses a complete admitted chunk;
 -- descriptor_max_tokens limits ordinary intros, while this path still obeys 512.
 FOR item IN SELECT value FROM jsonb_array_elements(p_items) LOOP
  IF NOT knowledge._parse_closed(item,ARRAY['id','draft','vector','input_tokens']) OR NOT knowledge._index_uuid(item->'id') OR
   NOT knowledge._parse_integer(item->'input_tokens',1,max_tokens) OR jsonb_typeof(item->'vector') IS DISTINCT FROM 'array' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_INDEX_ITEM'; END IF;
  IF jsonb_array_length(item->'vector')<>1536 OR EXISTS(SELECT 1 FROM jsonb_array_elements(item->'vector') e WHERE jsonb_typeof(e)<>'number') THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_EMBEDDING_VECTOR'; END IF;
  BEGIN
   vector_value:=(item->'vector')::text::public.vector(1536);
   IF abs(public.vector_norm(vector_value)-1)>=0.001 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_EMBEDDING_VECTOR'; END IF;
  EXCEPTION WHEN data_exception THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_EMBEDDING_VECTOR'; END;
  draft:=item->'draft';
  IF NOT knowledge._index_uuid(draft->'node_id') OR draft->'input_tokens' IS DISTINCT FROM item->'input_tokens' THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='EMBEDDING_INPUT_MISMATCH'; END IF;
  SELECT * INTO n FROM knowledge.document_nodes WHERE id=(draft->>'node_id')::uuid AND parse_generation_id=g.parse_generation_id;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='23503',MESSAGE='INDEX_NODE_PROVENANCE_MISMATCH'; END IF;
  IF p_kind='chunks' THEN
   IF NOT knowledge._index_chunk(n,draft,max_tokens) THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_CHUNK_PROJECTION'; END IF;
   INSERT INTO knowledge.chunks(id,index_generation_id,parse_generation_id,node_id,chunk_index,source_text,embedding_text,header_text,
    token_count,source_spans,content_hash,embedding,projection_metadata)
   VALUES((item->>'id')::uuid,g.id,g.parse_generation_id,n.id,(draft->>'chunk_index')::integer,draft->>'source_text',draft->>'embedding_text',
    draft->>'header_text',(draft->>'input_tokens')::integer,draft->'source_spans',draft->>'content_hash',vector_value,
    (draft-ARRAY['node_id','chunk_index','source_text','embedding_text','header_text','input_tokens','source_spans','content_hash'])||jsonb_build_object('schema_version','p04.chunks.v2'));
  ELSE
   IF NOT knowledge._parse_closed(draft,ARRAY['node_id','text','source_spans','input_tokens']) OR item->'id' IS DISTINCT FROM draft->'node_id' OR
    jsonb_typeof(draft->'text') IS DISTINCT FROM 'string' OR length(draft->>'text') NOT BETWEEN 1 AND 30000 OR
    NOT knowledge._index_spans(g.parse_generation_id,draft->'source_spans') THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_ROUTING_PROJECTION'; END IF;
   INSERT INTO knowledge.node_routing_embeddings(index_generation_id,parse_generation_id,node_id,descriptor_text,embedding,token_count,source_spans,content_hash)
   VALUES(g.id,g.parse_generation_id,n.id,draft->>'text',vector_value,(draft->>'input_tokens')::integer,draft->'source_spans',
    encode(sha256(convert_to(jsonb_build_object('draft',draft,'recipe',g.projection_manifest,'embedding_recipe',g.embedding_recipe)::text,'UTF8')),'hex'));
  END IF;
  token_sum:=token_sum+(item->>'input_tokens')::integer;
 END LOOP;
 IF token_sum>8192 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='EMBEDDING_BATCH_TOKEN_LIMIT'; END IF;
 count_new:=jsonb_array_length(p_items);
 IF p_kind='chunks' THEN
  SELECT count(*) INTO count_all FROM knowledge.chunks WHERE index_generation_id=g.id;
  IF count_all>g.expected_chunk_count THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INDEX_COUNT_EXCEEDED'; END IF;
 ELSE
  SELECT count(*) INTO count_all FROM knowledge.node_routing_embeddings WHERE index_generation_id=g.id;
  IF count_all>g.expected_routing_count THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INDEX_COUNT_EXCEEDED'; END IF;
 END IF;
 INSERT INTO knowledge.index_write_batches(index_generation_id,batch_kind,batch_id,payload_hash,request_hash,inserted_count,total_count)
 VALUES(g.id,p_kind,p_batch_id,payload,request_digest,count_new,count_all);
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN QUERY SELECT count_new,count_all;
END $$;
CREATE FUNCTION knowledge.write_index_chunks(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID,p_batch_id UUID,p_items JSONB,p_response_metadata JSONB)
RETURNS TABLE(inserted_count INTEGER,total_count INTEGER) LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT * FROM knowledge._write_index_batch(p_job_id,p_owner,p_epoch,p_index_id,p_batch_id,p_items,p_response_metadata,'chunks')
$$;
CREATE FUNCTION knowledge.write_index_descriptors(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID,p_batch_id UUID,p_items JSONB,p_response_metadata JSONB)
RETURNS TABLE(inserted_count INTEGER,total_count INTEGER) LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT * FROM knowledge._write_index_batch(p_job_id,p_owner,p_epoch,p_index_id,p_batch_id,p_items,p_response_metadata,'descriptors')
$$;
CREATE FUNCTION knowledge.finalize_index(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID)
RETURNS knowledge.index_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.index_generations; actual_chunks INTEGER; actual_descriptors INTEGER;
BEGIN
 g:=knowledge._assert_index_owner(p_job_id,p_owner,p_epoch,p_index_id);
 IF g.status='ready' THEN RETURN g; END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 SELECT count(*) INTO actual_chunks FROM knowledge.chunks WHERE index_generation_id=g.id;
 SELECT count(*) INTO actual_descriptors FROM knowledge.node_routing_embeddings WHERE index_generation_id=g.id;
 IF actual_chunks<>g.expected_chunk_count OR actual_descriptors<>g.expected_routing_count OR
  EXISTS(SELECT 1 FROM knowledge.chunks WHERE index_generation_id=g.id GROUP BY node_id HAVING min(chunk_index)<>0 OR max(chunk_index)<>count(*)-1) OR
  EXISTS(SELECT 1 FROM knowledge.chunks WHERE index_generation_id=g.id AND searchable) OR
  EXISTS(SELECT 1 FROM knowledge.node_routing_embeddings WHERE index_generation_id=g.id AND searchable) OR
  NOT EXISTS(SELECT 1 FROM knowledge.parse_generations p JOIN app.stored_objects o ON o.id=p.artifact_object_id
   WHERE p.id=g.parse_generation_id AND p.document_version_id=g.document_version_id AND p.status='ready' AND p.quality_report->>'status'='passed' AND
    o.kind='parse_artifact' AND o.state='attached') THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INDEX_NOT_COMPLETE'; END IF;
 UPDATE knowledge.index_generations SET status='ready',chunk_count=actual_chunks,routing_count=actual_descriptors,completed_at=clock_timestamp()
 WHERE id=g.id RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;
CREATE FUNCTION knowledge.fail_index(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_index_id UUID,p_error_code TEXT)
RETURNS knowledge.index_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.index_generations;
BEGIN
 IF p_error_code IS NULL OR p_error_code NOT IN ('GENERATION_INVALID','TOKEN_LIMIT_EXCEEDED','OUTPUT_SCHEMA_INVALID','SOURCE_UNAVAILABLE',
  'SIZE_LIMIT_EXCEEDED','DEPENDENCY_UNAVAILABLE','DEADLINE_EXCEEDED') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='INVALID_INDEX_FAILURE'; END IF;
 g:=knowledge._assert_index_owner(p_job_id,p_owner,p_epoch,p_index_id);
 IF g.status='failed' THEN
  IF g.failure_code<>p_error_code THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 UPDATE knowledge.index_generations SET status='failed',failure_code=p_error_code,completed_at=clock_timestamp(),
  chunk_count=(SELECT count(*) FROM knowledge.chunks WHERE index_generation_id=g.id),
  routing_count=(SELECT count(*) FROM knowledge.node_routing_embeddings WHERE index_generation_id=g.id) WHERE id=g.id RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA knowledge FROM PUBLIC;
REVOKE ALL ON knowledge.index_write_batches FROM PUBLIC,expert_backend,expert_runtime,expert_outbox,expert_ingest;
GRANT SELECT ON knowledge.index_write_batches TO expert_ingest;
GRANT EXECUTE ON FUNCTION knowledge.begin_index(UUID,UUID,BIGINT,UUID,UUID,JSONB,JSONB,INTEGER,INTEGER),
 knowledge.write_index_chunks(UUID,UUID,BIGINT,UUID,UUID,JSONB,JSONB),knowledge.write_index_descriptors(UUID,UUID,BIGINT,UUID,UUID,JSONB,JSONB),
 knowledge.finalize_index(UUID,UUID,BIGINT,UUID),knowledge.fail_index(UUID,UUID,BIGINT,UUID,TEXT) TO expert_ingest;
