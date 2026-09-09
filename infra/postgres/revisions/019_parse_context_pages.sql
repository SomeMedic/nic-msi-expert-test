-- P04 forward repair: a caption/context may precede its table on another page.
-- Keep node/body/cell ownership local; only the four closed canonical context
-- paths use document scope. Raw block/text binding is verified by the existing
-- ArtifactValidator before artifact attachment, not inferred by this SQL.
-- Finalization binds all retained contextual page numbers to this parse's
-- authoritative physical count. No direct writer privilege or fence is changed.

CREATE OR REPLACE FUNCTION knowledge._parse_node(v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE ignored UUID;
BEGIN
 IF NOT knowledge._parse_closed(v,ARRAY['id','parent_id','node_type','ordinal','number','title','own_body','char_map','context_refs','table','level','structural_path','page_start','page_end','source_spans','content_hash']) OR
  jsonb_typeof(v->'id')<>'string' OR (v->'parent_id'<>'null'::jsonb AND jsonb_typeof(v->'parent_id')<>'string') OR
  jsonb_typeof(v->'node_type')<>'string' OR v->>'node_type' NOT IN ('document','section','chapter','article','clause','subclause','paragraph','appendix','table','figure','footnote','editorial_note','unknown') OR
  NOT knowledge._parse_integer(v->'ordinal',0,10000000) OR NOT knowledge._parse_integer(v->'level',0,100) OR
  NOT knowledge._parse_integer(v->'page_start',1,500) OR NOT knowledge._parse_integer(v->'page_end',1,500) OR
  (v->>'page_end')::integer<(v->>'page_start')::integer OR
  (v->'number'<>'null'::jsonb AND (jsonb_typeof(v->'number')<>'string' OR length(v->>'number')>200)) OR
  (v->'title'<>'null'::jsonb AND (jsonb_typeof(v->'title')<>'string' OR length(v->>'title')>2000)) OR
  jsonb_typeof(v->'own_body')<>'string' OR NOT knowledge._parse_map(v->>'own_body',v->'char_map') OR
  NOT knowledge._parse_context(v->'context_refs') OR NOT knowledge._parse_spans(v->'source_spans') OR
  jsonb_typeof(v->'structural_path')<>'array' OR jsonb_typeof(v->'content_hash')<>'string' OR v->>'content_hash'!~'^[0-9a-f]{64}$' THEN RETURN false; END IF;
 ignored:=(v->>'id')::uuid; ignored:=(v->>'parent_id')::uuid;
 IF jsonb_array_length(v->'structural_path')>100 OR EXISTS(SELECT 1 FROM jsonb_array_elements(v->'structural_path') p WHERE jsonb_typeof(p)<>'string') OR
  EXISTS(SELECT 1 FROM (
   SELECT s FROM jsonb_path_query(v,'$.source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(v,'$.char_map[*].source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(v,'$.table.owned_source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(v,'$.table.cells[*].char_map[*].source_spans[*]') s
  ) owned WHERE (s->>'pdf_page')::integer NOT BETWEEN (v->>'page_start')::integer AND (v->>'page_end')::integer) THEN RETURN false; END IF;
 IF v->'table'='null'::jsonb THEN
  IF v->>'node_type'='table' OR v->>'content_hash'<>encode(sha256(convert_to(v->>'own_body','UTF8')),'hex') THEN RETURN false; END IF;
 ELSE
  -- Table hash uses the private DTO serializer; jsonb::text is a different
  -- serialization. Immutable full batch digests bind all submitted table data.
  IF v->>'node_type'<>'table' OR v->>'own_body'<>'' OR NOT knowledge._parse_table(v->'table') OR
   EXISTS(SELECT 1 FROM jsonb_array_elements(v->'table'->'pdf_pages') p WHERE (p#>>'{}')::integer NOT BETWEEN (v->>'page_start')::integer AND (v->>'page_end')::integer) THEN RETURN false; END IF;
 END IF;
 RETURN true;
EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN RETURN false;
END $$;

CREATE OR REPLACE FUNCTION knowledge.finalize_parse(p_job_id UUID,p_owner UUID,p_epoch BIGINT,p_parse_id UUID,
 p_expected_node_count INTEGER,p_physical_page_count INTEGER,p_quality_report JSONB)
RETURNS knowledge.parse_generations LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE g knowledge.parse_generations; actual_count BIGINT;
BEGIN
 IF p_expected_node_count IS NULL OR p_expected_node_count NOT BETWEEN 1 AND 100000 OR
  p_physical_page_count IS NULL OR p_physical_page_count NOT BETWEEN 1 AND 500 OR p_quality_report IS NULL OR
  octet_length(p_quality_report::text)>8388608 OR NOT knowledge._parse_quality(p_quality_report,p_physical_page_count) OR
  p_quality_report->>'status' IS DISTINCT FROM 'passed' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='INVALID_PARSE_QUALITY'; END IF;
 g:=knowledge._assert_parse_owner(p_job_id,p_owner,p_epoch,p_parse_id);
 IF g.status='ready' THEN
  IF (g.node_count,g.physical_page_count,g.quality_report) IS DISTINCT FROM (p_expected_node_count,p_physical_page_count,p_quality_report) THEN
   RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='IDEMPOTENCY_CONFLICT'; END IF;
  RETURN g;
 END IF;
 IF g.status<>'staging' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='GENERATION_IMMUTABLE'; END IF;
 SELECT count(*) INTO actual_count FROM knowledge.document_nodes WHERE parse_generation_id=g.id;
 IF actual_count<>p_expected_node_count OR NOT EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=g.id AND parent_id IS NULL) OR
  EXISTS(SELECT 1 FROM knowledge.document_nodes WHERE parse_generation_id=g.id AND page_end>p_physical_page_count) OR
  EXISTS(SELECT 1 FROM knowledge.document_nodes n CROSS JOIN LATERAL (
   SELECT s FROM jsonb_path_query(n.extra_metadata,'$.context_refs[*].source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(n.table_data,'$.context_refs[*].source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(n.table_data,'$.rows[*].context_refs[*].source_spans[*]') s
   UNION ALL SELECT s FROM jsonb_path_query(n.table_data,'$.row_groups[*].context_refs[*].source_spans[*]') s
  ) contextual WHERE n.parse_generation_id=g.id AND (s->>'pdf_page')::integer NOT BETWEEN 1 AND p_physical_page_count) OR
  NOT EXISTS(SELECT 1 FROM app.parse_artifact_intents u JOIN app.stored_objects s ON s.id=u.artifact_object_id
   WHERE u.parse_generation_id=g.id AND u.artifact_role='canonical' AND u.state='attached' AND u.artifact_object_id=g.artifact_object_id AND
    s.state='attached' AND s.kind='parse_artifact' AND s.sha256=u.sha256 AND s.size_bytes=u.size_bytes) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PARSE_NOT_COMPLETE'; END IF;
 UPDATE knowledge.parse_generations SET status='ready',node_count=p_expected_node_count,physical_page_count=p_physical_page_count,
  quality_report=p_quality_report,completed_at=clock_timestamp() WHERE id=g.id RETURNING * INTO g;
 PERFORM knowledge._assert_parse_execution(p_job_id,p_owner,p_epoch);
 RETURN g;
END $$;
