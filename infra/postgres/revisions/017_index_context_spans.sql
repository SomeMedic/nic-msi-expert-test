-- Canonical context can retain raw trailing whitespace omitted from node body maps.
-- Admit only bounded intervals recorded in this same immutable parse registry.
CREATE OR REPLACE FUNCTION knowledge._index_spans(p_parse_id UUID,v JSONB) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
BEGIN
 IF NOT knowledge._parse_spans(v) OR jsonb_array_length(v)=0 THEN RETURN false; END IF;
 -- Most calls already match node-owned spans. Preserve the original fast path
 -- before expanding any table JSON, including unrelated tables in this parse.
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v) s WHERE NOT EXISTS(
  SELECT 1 FROM knowledge.document_nodes n CROSS JOIN LATERAL jsonb_array_elements(n.source_spans) owned
  WHERE n.parse_generation_id=p_parse_id AND owned->>'pdf_page'=s->>'pdf_page' AND owned->>'block_id'=s->>'block_id' AND
   (owned->>'start_offset')::bigint<=(s->>'start_offset')::bigint AND (owned->>'end_offset')::bigint>=(s->>'end_offset')::bigint)) THEN
  RETURN true;
 END IF;
 -- These four context locations are the closed P04 CanonicalNode/Table contract.
 -- Do not search arbitrary JSON, widen offsets, or cross parse generations.
 -- Raw text/geometry equality remains the pinned-artifact validator's job.
 RETURN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v) s WHERE NOT EXISTS(
  SELECT 1 FROM knowledge.document_nodes n CROSS JOIN LATERAL jsonb_array_elements(n.source_spans) owned
  WHERE n.parse_generation_id=p_parse_id AND owned->>'pdf_page'=s->>'pdf_page' AND owned->>'block_id'=s->>'block_id' AND
   (owned->>'start_offset')::bigint<=(s->>'start_offset')::bigint AND (owned->>'end_offset')::bigint>=(s->>'end_offset')::bigint)
  AND NOT EXISTS(
  SELECT 1 FROM knowledge.document_nodes n
  CROSS JOIN LATERAL (
   SELECT span AS owned FROM (
    SELECT value AS ref FROM jsonb_array_elements(coalesce(n.extra_metadata->'context_refs','[]'::jsonb))
    UNION ALL
    SELECT value FROM jsonb_array_elements(coalesce(n.table_data->'context_refs','[]'::jsonb))
    UNION ALL
    SELECT ref FROM jsonb_array_elements(n.table_data->'rows') row_entry
     CROSS JOIN LATERAL jsonb_array_elements(row_entry->'context_refs') ref
    UNION ALL
    SELECT ref FROM jsonb_array_elements(n.table_data->'row_groups') group_entry
     CROSS JOIN LATERAL jsonb_array_elements(group_entry->'context_refs') ref
   ) contexts CROSS JOIN LATERAL jsonb_array_elements(contexts.ref->'source_spans') span
  ) registry
  WHERE n.parse_generation_id=p_parse_id AND owned->>'pdf_page'=s->>'pdf_page' AND owned->>'block_id'=s->>'block_id' AND
   (owned->>'start_offset')::bigint<=(s->>'start_offset')::bigint AND (owned->>'end_offset')::bigint>=(s->>'end_offset')::bigint));
END $$;

-- Replacement retains the helper's existing owner/ACL. No service gets direct EXECUTE.
