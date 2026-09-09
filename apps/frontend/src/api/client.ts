import { canonicalTreeNodeNode_typeValues, debugCaptureItemPartValues, debugCaptureItemRoleValues, debugCaptureItemStateValues, debugCaptureListStatusValues, debugCaptureSummaryStatusValues, debugStepStatusValues, errorCodeValues, purgeFailureCodeValues, purgeStatusStatusValues, qualityDiagnosticSeverityValues, qualitySummaryStatusValues, runStageValues, structuredTableCellRoleValues, structuredTableContextRoleValues, structuredTablePageKindValues } from "./generated";
import type { components, operations } from "./generated";
import { isIngestionStage, isJobStatus } from "../workspace";
import { isSystemState } from "../system-status";

type ErrorInfo = components["schemas"]["ErrorInfo"];
type ErrorCode = components["schemas"]["ErrorCode"];
type ComponentStatus = components["schemas"]["ComponentStatus"];
type SystemStatus = operations["get_system_status"]["responses"][200]["content"]["application/json"];
type SessionInfo = operations["get_auth_session"]["responses"][200]["content"]["application/json"];
type SessionRequest = components["schemas"]["SessionRequest"];
type UploadAccepted = operations["upload_document"]["responses"][202]["content"]["application/json"];
type VersionUploadOptions = components["schemas"]["VersionUploadOptions"];
type IngestionJob = operations["get_ingestion_job"]["responses"][200]["content"]["application/json"];
type JobCommandAccepted = operations["cancel_ingestion_job"]["responses"][202]["content"]["application/json"];
type ReindexRequest = components["schemas"]["ReindexRequest"];
type IngestionCapabilities = components["schemas"]["IngestionCapabilities"];
type DebugCapturePolicy = components["schemas"]["DebugCapturePolicy"];
type DebugCaptureList = components["schemas"]["DebugCaptureList"];
type DebugCaptureItem = components["schemas"]["DebugCaptureItem"];
type DebugCaptureSummary = components["schemas"]["DebugCaptureSummary"];
type PublishRequest = components["schemas"]["PublishRequest"];
type DeactivateRequest = components["schemas"]["DeactivateRequest"];
type PublicationInfo = components["schemas"]["PublicationInfo"];
type CreateRunRequest = components["schemas"]["CreateRunRequest"];
type RunAccepted = components["schemas"]["RunAccepted"];
type RunCancelAccepted = components["schemas"]["RunCancelAccepted"];
type RunList = components["schemas"]["RunList"];
type RunSummary = components["schemas"]["RunSummary"];
type PublicRun = components["schemas"]["PublicRun"];
type PublicEvidence = components["schemas"]["PublicEvidence"];
type RunDebug = components["schemas"]["RunDebug"];
type DebugStep = components["schemas"]["DebugStep"];
type RunStage = components["schemas"]["RunStage"];
type RunResult = components["schemas"]["FinalAnswer"] | components["schemas"]["RefusalResult"];
type DocumentList = components["schemas"]["DocumentList"];
type DocumentSummary = components["schemas"]["DocumentSummary"];
type DocumentDetail = components["schemas"]["DocumentDetail"];
type VersionSummary = components["schemas"]["VersionSummary"];
type VersionDetail = components["schemas"]["VersionDetail"];
type ArchiveDocumentRequest = components["schemas"]["ArchiveDocumentRequest"];
type LibraryProfile = components["schemas"]["LibraryProfile"];
type PurgeAccepted = components["schemas"]["PurgeAccepted"];
type PurgePlan = components["schemas"]["PurgePlan"];
type PurgeReferenceCounts = components["schemas"]["PurgeReferenceCounts"];
type PurgeRequest = components["schemas"]["PurgeRequest"];
type PurgeStatus = components["schemas"]["PurgeStatus"];
type CanonicalTreeNode = components["schemas"]["CanonicalTreeNode"];
type CanonicalTreePage = components["schemas"]["CanonicalTreePage"];
type ParseQualityPage = components["schemas"]["ParseQualityPage"];
type QualityDiagnostic = components["schemas"]["QualityDiagnostic"];
type StructuredTableCell = components["schemas"]["StructuredTableCell"];
type StructuredTableContext = components["schemas"]["StructuredTableContext"];
type StructuredTablePage = components["schemas"]["StructuredTablePage"];
type StructuredTablePageSegment = components["schemas"]["StructuredTablePageSegment"];
type StructuredTableRow = components["schemas"]["StructuredTableRow"];

export type SystemStatusResponse = {
  data: SystemStatus;
  requestId: string;
};

export type ApiResponse<T> = {
  data: T;
  requestId: string;
};

const DEBUG_CAPTURE_DOWNLOAD_MAX_BYTES = 2 * 1024 * 1024;

export class ApiFailure extends Error {
  readonly kind: "http" | "network" | "invalid_response";
  readonly httpStatus: number | null;
  readonly requestId: string;
  readonly serverError: ErrorInfo | null;

  constructor(
    kind: ApiFailure["kind"],
    message: string,
    requestId: string,
    httpStatus: number | null = null,
    serverError: ErrorInfo | null = null,
  ) {
    super(message);
    this.name = "ApiFailure";
    this.kind = kind;
    this.httpStatus = httpStatus;
    this.requestId = requestId;
    this.serverError = serverError;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" && /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(value);
}

function isErrorCode(value: unknown): value is ErrorCode {
  return typeof value === "string" && errorCodeValues.some((code) => code === value);
}

function isScalar(value: unknown): value is string | number | boolean | null {
  return value === null || typeof value === "string" || typeof value === "boolean"
    || (typeof value === "number" && Number.isFinite(value));
}

function parsePublicApiPath(value: unknown, allowedPrefixes: readonly string[]): string | null {
  if (typeof value !== "string" || !value.startsWith("/api/v1/")
    || value.startsWith("//") || value.includes("\\")
    || Array.from(value).some((char) => {
      const code = char.charCodeAt(0);
      return code < 32 || code === 127;
    })) return null;
  const pathEnd = value.search(/[?#]/);
  const path = pathEnd === -1 ? value : value.slice(0, pathEnd);
  const segments = path.split("/");
  if (segments.some((segment) => segment === "." || segment === "..")) return null;
  if (!allowedPrefixes.some((prefix) => path.startsWith(prefix))) return null;
  try {
    const parsed = new URL(value, "http://localhost");
    if (parsed.origin !== "http://localhost" || parsed.pathname !== path) return null;
  } catch {
    return null;
  }
  return value;
}

function parsePublicVersionSourcePath(value: unknown, expectedVersionId: string | null = null): string | null {
  const sourceUrl = parsePublicApiPath(value, ["/api/v1/versions/"]);
  if (!sourceUrl) return null;
  const pathEnd = sourceUrl.search(/[?#]/);
  const path = pathEnd === -1 ? sourceUrl : sourceUrl.slice(0, pathEnd);
  const match = /^\/api\/v1\/versions\/([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12})\/source$/i.exec(path);
  if (!match) return null;
  const versionId = match[1];
  if (!versionId) return null;
  if (expectedVersionId !== null && versionId.toLowerCase() !== expectedVersionId.toLowerCase()) return null;
  return sourceUrl;
}

function parsePublicSourcePath(value: unknown): string | null {
  return parsePublicApiPath(value, ["/api/v1/runs/"]) ?? parsePublicVersionSourcePath(value);
}

function parseError(payload: unknown): ErrorInfo | null {
  if (!isRecord(payload) || !isRecord(payload.error)) return null;
  const error = payload.error;
  const details = error.details ?? {};
  if (!isErrorCode(error.code) || typeof error.message !== "string"
    || !error.message.trim() || error.message.length > 1000
    || typeof error.retryable !== "boolean" || !isUuid(error.request_id)
    || !isRecord(details) || Object.keys(details).length > 8
    || !Object.values(details).every(isScalar)) return null;

  return {
    code: error.code,
    message: error.message,
    retryable: error.retryable,
    request_id: error.request_id,
    details: Object.fromEntries(Object.entries(details).filter((entry): entry is [string, string | number | boolean | null] => isScalar(entry[1]))),
  };
}

function parseComponent(value: unknown): ComponentStatus | null {
  if (!isRecord(value) || typeof value.name !== "string" || !value.name.trim()
    || value.name.length > 100 || !isSystemState(value.status)) return null;
  const errorCode = value.error_code ?? null;
  if (errorCode !== null && !isErrorCode(errorCode)) return null;
  return { name: value.name, status: value.status, error_code: errorCode };
}

function parseSystemStatus(value: unknown): SystemStatus | null {
  if (!isRecord(value) || !isSystemState(value.status)
    || typeof value.checked_at !== "string" || !Number.isFinite(Date.parse(value.checked_at))
    || !/(?:Z|\+00:00)$/.test(value.checked_at)
    || !Array.isArray(value.components) || value.components.length > 30) return null;

  const components: ComponentStatus[] = [];
  for (const raw of value.components) {
    const component = parseComponent(raw);
    if (!component || components.some((item) => item.name === component.name)) return null;
    if (value.status === "ready" && component.status !== "ready") return null;
    components.push(component);
  }
  return { status: value.status, checked_at: value.checked_at, components };
}

function parseSessionInfo(value: unknown): SessionInfo | null {
  if (!isRecord(value) || typeof value.principal_id !== "string" || !value.principal_id.trim()
    || !["viewer", "operator", "admin"].includes(String(value.role))) return null;
  const expiresAt = value.expires_at ?? null;
  if (expiresAt !== null && (typeof expiresAt !== "string" || !Number.isFinite(Date.parse(expiresAt)))) return null;
  return { principal_id: value.principal_id, role: value.role as SessionInfo["role"], expires_at: expiresAt };
}

function parseUploadAccepted(value: unknown): UploadAccepted | null {
  if (!isRecord(value) || !isUuid(value.document_id) || !isUuid(value.version_id) || !isUuid(value.job_id)
    || (value.status ?? "queued") !== "queued" || !isRecord(value.links)) return null;
  const links = value.links;
  if (typeof links.document !== "string" || typeof links.version !== "string"
    || typeof links.job !== "string" || !links.job.startsWith("/api/v1/ingestion-jobs/")
    || typeof links.events !== "string") return null;
  return {
    document_id: value.document_id,
    version_id: value.version_id,
    job_id: value.job_id,
    links: { document: links.document, version: links.version, job: links.job, events: links.events },
    status: "queued",
  };
}

function parseJobCommandAccepted(value: unknown): JobCommandAccepted | null {
  if (!isRecord(value) || !isUuid(value.job_id) || !isJobStatus(value.status)
    || typeof value.last_sequence !== "number" || value.last_sequence < 1 || !Number.isInteger(value.last_sequence)) return null;
  return { job_id: value.job_id, status: value.status, last_sequence: value.last_sequence };
}

function parseRunAccepted(value: unknown): RunAccepted | null {
  if (!isRecord(value) || !isUuid(value.run_id) || (value.status ?? "created") !== "created"
    || typeof value.last_sequence !== "number" || !Number.isInteger(value.last_sequence) || value.last_sequence < 1
    || !isRecord(value.links) || typeof value.links.events !== "string" || typeof value.links.self !== "string") return null;
  return { run_id: value.run_id, status: "created", last_sequence: value.last_sequence, links: { self: value.links.self, events: value.links.events } };
}

function parseRunSummary(value: unknown): RunSummary | null {
  if (!isRecord(value) || !isUuid(value.run_id) || typeof value.question_excerpt !== "string"
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || !["created", "running", "cancelling", "completed", "refused", "failed", "cancelled"].includes(String(value.status))) return null;
  const duration = value.duration_ms ?? null;
  if (duration !== null && (typeof duration !== "number" || !Number.isInteger(duration) || duration < 0)) return null;
  return {
    run_id: value.run_id,
    question_excerpt: value.question_excerpt,
    status: value.status as RunSummary["status"],
    created_at: value.created_at,
    duration_ms: duration,
  };
}

function parseRunList(value: unknown): RunList | null {
  if (!isRecord(value) || !Array.isArray(value.items) || value.items.length > 100) return null;
  const items: RunSummary[] = [];
  for (const raw of value.items) {
    const item = parseRunSummary(raw);
    if (!item) return null;
    items.push(item);
  }
  const nextCursor = value.next_cursor ?? null;
  if (nextCursor !== null && typeof nextCursor !== "string") return null;
  return { items, next_cursor: nextCursor };
}

function parseSnapshot(value: unknown): PublicRun["snapshot"] {
  if (value === null || value === undefined) return null;
  if (!isRecord(value) || !isUuid(value.id) || typeof value.captured_at !== "string"
    || !Number.isFinite(Date.parse(value.captured_at))
    || typeof value.version_count !== "number" || !Number.isInteger(value.version_count) || value.version_count < 0) return undefined;
  return { id: value.id, captured_at: value.captured_at, version_count: value.version_count };
}


function parseValidationSummary(value: unknown): components["schemas"]["ValidationSummary"] | null {
  if (!isRecord(value) || value.status !== "confirmed" || typeof value.repair_used !== "boolean"
    || typeof value.claim_count !== "number" || !Number.isInteger(value.claim_count) || value.claim_count < 0
    || typeof value.supported_count !== "number" || !Number.isInteger(value.supported_count) || value.supported_count < 0
    || value.supported_count > value.claim_count) return null;
  return { claim_count: value.claim_count, supported_count: value.supported_count, repair_used: value.repair_used, status: "confirmed" };
}

function parseCitation(value: unknown): components["schemas"]["CitationDTO"] | null {
  if (!isRecord(value) || typeof value.citation_id !== "string" || !value.citation_id.trim()
    || typeof value.evidence_id !== "string" || !value.evidence_id.trim()
    || typeof value.document_title !== "string" || typeof value.source_url !== "string") return null;
  const sourceUrl = parsePublicSourcePath(value.source_url);
  const pdfPages = parsePositiveIntegerArray(value.pdf_pages, 100);
  const printedPageLabels = parseStringArray(value.printed_page_labels, 100);
  const structuralPath = parseStringArray(value.structural_path, 32);
  if (!sourceUrl || !pdfPages || !printedPageLabels || !structuralPath) return null;
  const versionLabel = value.version_label ?? null;
  if (versionLabel !== null && typeof versionLabel !== "string") return null;
  return {
    citation_id: value.citation_id,
    evidence_id: value.evidence_id,
    document_title: value.document_title,
    pdf_pages: pdfPages,
    printed_page_labels: printedPageLabels,
    source_url: sourceUrl,
    structural_path: structuralPath,
    version_label: versionLabel,
  };
}

function parsePublicClaim(value: unknown): components["schemas"]["PublicClaim"] | null {
  if (!isRecord(value) || typeof value.claim_id !== "string" || !value.claim_id.trim()
    || typeof value.text !== "string" || !value.text.trim()) return null;
  const citationIds = parseStringArray(value.citation_ids, 100);
  if (!citationIds || citationIds.some((id) => !id.trim())) return null;
  return { claim_id: value.claim_id, text: value.text, citation_ids: citationIds };
}

function parseFinalAnswer(value: unknown): components["schemas"]["FinalAnswer"] | null {
  if (!isRecord(value) || value.kind !== "completed" || !isUuid(value.result_id) || typeof value.text !== "string" || !value.text.trim()
    || !Array.isArray(value.claims) || value.claims.length > 100 || !Array.isArray(value.citations) || value.citations.length > 500) return null;
  const snapshot = parseSnapshot(value.snapshot);
  const validation = parseValidationSummary(value.validation);
  if (!snapshot || !validation) return null;
  const claims = value.claims.map(parsePublicClaim);
  const citations = value.citations.map(parseCitation);
  if (claims.some((claim) => claim === null) || citations.some((citation) => citation === null)) return null;
  const parsedClaims = claims as components["schemas"]["PublicClaim"][];
  const parsedCitations = citations as components["schemas"]["CitationDTO"][];
  const citationIds = new Set(parsedCitations.map((citation) => citation.citation_id));
  if (validation.claim_count !== parsedClaims.length
    || parsedClaims.some((claim) => claim.citation_ids.length < 1 || claim.citation_ids.some((id) => !citationIds.has(id)))) return null;
  return { kind: "completed", result_id: value.result_id, text: value.text, snapshot, validation, claims: parsedClaims, citations: parsedCitations };
}

function parseRefusalResult(value: unknown): components["schemas"]["RefusalResult"] | null {
  if (!isRecord(value) || value.kind !== "refused" || !isUuid(value.result_id) || typeof value.text !== "string" || !value.text.trim()
    || !["NO_RELEVANT_CONTEXT", "VERIFICATION_FAILED", "OUT_OF_SCOPE"].includes(String(value.code))) return null;
  const snapshot = parseSnapshot(value.snapshot);
  if (!snapshot) return null;
  return { kind: "refused", result_id: value.result_id, text: value.text, code: value.code as components["schemas"]["RefusalCode"], snapshot };
}

function parseRunResult(value: unknown): RunResult | null {
  return parseFinalAnswer(value) ?? parseRefusalResult(value);
}

function parsePublicRun(value: unknown): PublicRun | null {
  if (!isRecord(value) || !isUuid(value.run_id) || !["created", "running", "cancelling", "completed", "refused", "failed", "cancelled"].includes(String(value.status))
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || typeof value.last_sequence !== "number" || !Number.isInteger(value.last_sequence) || value.last_sequence < 1) return null;
  const snapshot = parseSnapshot(value.snapshot);
  if (snapshot === undefined) return null;
  const currentStage: RunStage | null = value.current_stage === undefined ? null : value.current_stage as RunStage | null;
  if (currentStage !== null && !["snapshotting", "routing", "retrieving", "reranking", "building_context", "drafting", "checking_citations", "validating", "repairing", "revalidating", "rendering", "finalizing"].includes(String(currentStage))) return null;
  const startedAt = parseNullableDateTime(value.started_at);
  const finishedAt = parseNullableDateTime(value.finished_at);
  if (startedAt === undefined || finishedAt === undefined) return null;
  const error = value.error === undefined || value.error === null ? null : parseError({ error: value.error });
  if (value.error !== null && value.error !== undefined && error === null) return null;
  const result = value.result === null || value.result === undefined ? null : parseRunResult(value.result);
  if ((value.result !== null && value.result !== undefined && result === null)
    || (value.status === "completed" && result?.kind !== "completed")
    || (value.status === "refused" && result?.kind !== "refused")
    || (["created", "running", "cancelling"].includes(String(value.status)) && result !== null)) return null;
  const parsed: PublicRun = {
    run_id: value.run_id,
    status: value.status as PublicRun["status"],
    created_at: value.created_at,
    last_sequence: value.last_sequence,
    cancel_requested: Boolean(value.cancel_requested ?? false),
    current_stage: currentStage,
    error,
    finished_at: finishedAt ?? null,
    result,
    snapshot,
    stage_attempt: typeof value.stage_attempt === "number" && Number.isInteger(value.stage_attempt) && value.stage_attempt >= 1 ? value.stage_attempt : 1,
    started_at: startedAt ?? null,
  };
  return parsed;
}

function parseRunCancelAccepted(value: unknown): RunCancelAccepted | null {
  if (!isRecord(value) || !isUuid(value.run_id) || (value.status ?? "cancelling") !== "cancelling"
    || typeof value.last_sequence !== "number" || !Number.isInteger(value.last_sequence) || value.last_sequence < 1) return null;
  return { run_id: value.run_id, status: "cancelling", last_sequence: value.last_sequence, cancel_requested: true };
}

function parseStringArray(value: unknown, maxItems: number): string[] | null {
  if (!Array.isArray(value) || value.length > maxItems) return null;
  const items: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") return null;
    items.push(item);
  }
  return items;
}

function parsePositiveIntegerArray(value: unknown, maxItems: number): number[] | null {
  if (!Array.isArray(value) || value.length < 1 || value.length > maxItems) return null;
  const items: number[] = [];
  for (const item of value) {
    if (typeof item !== "number" || !Number.isInteger(item) || item < 1) return null;
    items.push(item);
  }
  return items;
}

function parseBbox(value: unknown, required: true): [number, number, number, number] | null;
function parseBbox(value: unknown, required?: false): [number, number, number, number] | null | undefined;
function parseBbox(value: unknown, required = false): [number, number, number, number] | null | undefined {
  if (value === null || value === undefined) return required ? null : null;
  if (!Array.isArray(value) || value.length !== 4 || !value.every((item) => typeof item === "number" && Number.isFinite(item))) return undefined;
  return [value[0], value[1], value[2], value[3]];
}

function parseCanonicalTreeNode(value: unknown): CanonicalTreeNode | null {
  if (!isRecord(value) || !isUuid(value.node_id) || !canonicalTreeNodeNode_typeValues.includes(value.node_type as CanonicalTreeNode["node_type"])
    || typeof value.has_children !== "boolean" || typeof value.ordinal !== "number" || !Number.isInteger(value.ordinal) || value.ordinal < 0
    || typeof value.page_start !== "number" || !Number.isInteger(value.page_start) || value.page_start < 1
    || typeof value.page_end !== "number" || !Number.isInteger(value.page_end) || value.page_end < value.page_start) return null;
  const parentId = value.parent_id ?? null;
  const number = value.number ?? null;
  const title = value.title ?? null;
  if ((parentId !== null && !isUuid(parentId)) || (number !== null && typeof number !== "string") || (title !== null && typeof title !== "string")) return null;
  return {
    has_children: value.has_children,
    node_id: value.node_id,
    node_type: value.node_type as CanonicalTreeNode["node_type"],
    number,
    ordinal: value.ordinal,
    page_end: value.page_end,
    page_start: value.page_start,
    parent_id: parentId,
    title,
  };
}

function parseCanonicalTreePage(value: unknown): CanonicalTreePage | null {
  if (!isRecord(value) || !isUuid(value.version_id) || !isUuid(value.parse_generation_id)
    || !Array.isArray(value.items) || value.items.length > 100) return null;
  const parentId = value.parent_id ?? null;
  const nextCursor = value.next_cursor ?? null;
  if ((parentId !== null && !isUuid(parentId)) || (nextCursor !== null && typeof nextCursor !== "string")) return null;
  const items = value.items.map(parseCanonicalTreeNode);
  if (items.some((item) => item === null)) return null;
  return { version_id: value.version_id, parse_generation_id: value.parse_generation_id, parent_id: parentId, next_cursor: nextCursor, items: items as CanonicalTreeNode[] };
}

function parseQualityDiagnostic(value: unknown): QualityDiagnostic | null {
  if (!isRecord(value) || typeof value.code !== "string" || !value.code.trim() || value.code.length > 100
    || !qualityDiagnosticSeverityValues.includes(value.severity as QualityDiagnostic["severity"])
    || typeof value.ordinal !== "number" || !Number.isInteger(value.ordinal) || value.ordinal < 0) return null;
  const pdfPage = value.pdf_page ?? null;
  const blockId = value.block_id ?? null;
  const bbox = parseBbox(value.bbox);
  if ((pdfPage !== null && (typeof pdfPage !== "number" || !Number.isInteger(pdfPage) || pdfPage < 1))
    || (blockId !== null && typeof blockId !== "string") || bbox === undefined) return null;
  return { bbox: bbox ?? null, block_id: blockId, code: value.code, ordinal: value.ordinal, pdf_page: pdfPage, severity: value.severity as QualityDiagnostic["severity"] };
}

function parseQualityPage(value: unknown): ParseQualityPage | null {
  if (!isRecord(value) || !isUuid(value.version_id) || !isUuid(value.parse_generation_id)
    || typeof value.source_url !== "string" || !isRecord(value.summary)
    || !Array.isArray(value.items) || value.items.length > 100) return null;
  const sourceUrl = parsePublicApiPath(value.source_url, ["/api/v1/versions/"]);
  const nextCursor = value.next_cursor ?? null;
  const reasonCodes = value.summary.reason_codes ?? [];
  if (!sourceUrl || (nextCursor !== null && typeof nextCursor !== "string")
    || !qualitySummaryStatusValues.includes(value.summary.status as ParseQualityPage["summary"]["status"])
    || typeof value.summary.warning_count !== "number" || !Number.isInteger(value.summary.warning_count) || value.summary.warning_count < 0
    || !Array.isArray(reasonCodes) || reasonCodes.length > 100 || !reasonCodes.every((item) => typeof item === "string")) return null;
  const items = value.items.map(parseQualityDiagnostic);
  if (items.some((item) => item === null)) return null;
  return {
    version_id: value.version_id,
    parse_generation_id: value.parse_generation_id,
    source_url: sourceUrl,
    next_cursor: nextCursor,
    summary: { status: value.summary.status as ParseQualityPage["summary"]["status"], warning_count: value.summary.warning_count, reason_codes: reasonCodes },
    items: items as QualityDiagnostic[],
  };
}

function parseTableContext(value: unknown): StructuredTableContext | null {
  if (!isRecord(value) || typeof value.text !== "string" || typeof value.required !== "boolean"
    || !structuredTableContextRoleValues.includes(value.role as StructuredTableContext["role"])
    || !Array.isArray(value.source_spans) || value.source_spans.length > 100) return null;
  const sourceSpans = value.source_spans.map(parseSourceSpan);
  if (sourceSpans.some((span) => span === null)) return null;
  return { text: value.text, required: value.required, role: value.role as StructuredTableContext["role"], source_spans: sourceSpans as StructuredTableContext["source_spans"] };
}

function parseTableCell(value: unknown): StructuredTableCell | null {
  if (!isRecord(value) || typeof value.id !== "string" || !value.id.trim() || typeof value.text !== "string"
    || !structuredTableCellRoleValues.includes(value.role as StructuredTableCell["role"])
    || typeof value.row !== "number" || !Number.isInteger(value.row) || value.row < 0
    || typeof value.column !== "number" || !Number.isInteger(value.column) || value.column < 0
    || typeof value.row_span !== "number" || !Number.isInteger(value.row_span) || value.row_span < 1
    || typeof value.column_span !== "number" || !Number.isInteger(value.column_span) || value.column_span < 1
    || !Array.isArray(value.source_spans) || value.source_spans.length > 100) return null;
  const pdfPage = value.pdf_page ?? null;
  const bbox = parseBbox(value.bbox);
  const sourceSpans = value.source_spans.map(parseSourceSpan);
  if ((pdfPage !== null && (typeof pdfPage !== "number" || !Number.isInteger(pdfPage) || pdfPage < 1))
    || bbox === undefined || sourceSpans.some((span) => span === null)) return null;
  return {
    id: value.id,
    text: value.text,
    role: value.role as StructuredTableCell["role"],
    row: value.row,
    column: value.column,
    row_span: value.row_span,
    column_span: value.column_span,
    pdf_page: pdfPage,
    bbox: bbox ?? null,
    source_spans: sourceSpans as StructuredTableCell["source_spans"],
  };
}

function parseTableRow(value: unknown): StructuredTableRow | null {
  if (!isRecord(value) || typeof value.row_index !== "number" || !Number.isInteger(value.row_index) || value.row_index < 0
    || !Array.isArray(value.context_refs) || value.context_refs.length > 100) return null;
  const cellIds = parseStringArray(value.cell_ids, 200);
  if (!cellIds || cellIds.some((item) => !item.trim())) return null;
  const contextRefs = value.context_refs.map(parseTableContext);
  if (contextRefs.some((context) => context === null)) return null;
  return { row_index: value.row_index, cell_ids: cellIds, context_refs: contextRefs as StructuredTableContext[] };
}

function parseTablePageSegment(value: unknown): StructuredTablePageSegment | null {
  if (!isRecord(value) || typeof value.pdf_page !== "number" || !Number.isInteger(value.pdf_page) || value.pdf_page < 1
    || typeof value.first_row !== "number" || !Number.isInteger(value.first_row) || value.first_row < 0
    || typeof value.last_row !== "number" || !Number.isInteger(value.last_row) || value.last_row < value.first_row) return null;
  const bbox = parseBbox(value.bbox, true);
  if (!bbox) return null;
  return { pdf_page: value.pdf_page, first_row: value.first_row, last_row: value.last_row, bbox };
}

function parseStructuredTablePage(value: unknown): StructuredTablePage | null {
  if (!isRecord(value) || !isUuid(value.version_id) || !isUuid(value.parse_generation_id) || !isUuid(value.node_id)
    || typeof value.table_id !== "string" || !value.table_id.trim() || typeof value.source_url !== "string"
    || !structuredTablePageKindValues.includes(value.kind as StructuredTablePage["kind"])
    || typeof value.row_start !== "number" || !Number.isInteger(value.row_start) || value.row_start < 0
    || typeof value.row_end !== "number" || !Number.isInteger(value.row_end) || value.row_end < value.row_start
    || typeof value.total_rows !== "number" || !Number.isInteger(value.total_rows) || value.total_rows < 0
    || typeof value.column_count !== "number" || !Number.isInteger(value.column_count) || value.column_count < 1
    || !Array.isArray(value.rows) || value.rows.length > 50 || !Array.isArray(value.cells) || value.cells.length > 2500
    || !Array.isArray(value.context_refs) || value.context_refs.length > 500 || !Array.isArray(value.page_segments) || value.page_segments.length > 500) return null;
  const sourceUrl = parsePublicApiPath(value.source_url, ["/api/v1/versions/"]);
  const nextCursor = value.next_cursor ?? null;
  const pdfPages = parsePositiveIntegerArray(value.pdf_pages, 100);
  if (nextCursor !== null && typeof nextCursor !== "string") return null;
  if (!sourceUrl || !pdfPages) return null;
  const rows = value.rows.map(parseTableRow);
  const cells = value.cells.map(parseTableCell);
  const contexts = value.context_refs.map(parseTableContext);
  const segments = value.page_segments.map(parseTablePageSegment);
  if (rows.some((row) => row === null) || cells.some((cell) => cell === null) || contexts.some((context) => context === null) || segments.some((segment) => segment === null)) return null;
  const cellIds = new Set((cells as StructuredTableCell[]).map((cell) => cell.id));
  if ((rows as StructuredTableRow[]).some((row) => row.cell_ids.some((cellId) => !cellIds.has(cellId)))) return null;
  return {
    version_id: value.version_id,
    parse_generation_id: value.parse_generation_id,
    node_id: value.node_id,
    table_id: value.table_id,
    kind: value.kind as StructuredTablePage["kind"],
    source_url: sourceUrl,
    row_start: value.row_start,
    row_end: value.row_end,
    total_rows: value.total_rows,
    column_count: value.column_count,
    pdf_pages: pdfPages,
    next_cursor: nextCursor,
    rows: rows as StructuredTableRow[],
    cells: cells as StructuredTableCell[],
    context_refs: contexts as StructuredTableContext[],
    page_segments: segments as StructuredTablePageSegment[],
  };
}

function parsePublication(value: unknown): components["schemas"]["PublicationInfo"] | null {
  if (value === null || value === undefined) return null;
  if (!isRecord(value) || !isUuid(value.publication_id) || !isUuid(value.document_version_id)
    || !isUuid(value.index_generation_id) || typeof value.published_at !== "string"
    || !Number.isFinite(Date.parse(value.published_at))) return null;
  const retiredAt = value.retired_at ?? null;
  if (retiredAt !== null && (typeof retiredAt !== "string" || !Number.isFinite(Date.parse(retiredAt)))) return null;
  return {
    publication_id: value.publication_id,
    document_version_id: value.document_version_id,
    index_generation_id: value.index_generation_id,
    published_at: value.published_at,
    retired_at: retiredAt,
  };
}

function parseDocumentSummary(value: unknown): DocumentSummary | null {
  if (!isRecord(value) || !isUuid(value.document_id) || typeof value.canonical_title !== "string"
    || typeof value.version_count !== "number" || !Number.isInteger(value.version_count) || value.version_count < 0
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))) return null;
  const archivedAt = parseNullableDateTime(value.archived_at);
  if (archivedAt === undefined) return null;
  const currentPublication = parsePublication(value.current_publication);
  if (value.current_publication !== null && value.current_publication !== undefined && currentPublication === null) return null;
  return {
    document_id: value.document_id,
    canonical_title: value.canonical_title,
    version_count: value.version_count,
    created_at: value.created_at,
    archived_at: archivedAt ?? null,
    authority: typeof value.authority === "string" || value.authority === null ? value.authority : null,
    current_publication: currentPublication,
    document_number: typeof value.document_number === "string" || value.document_number === null ? value.document_number : null,
    document_type: typeof value.document_type === "string" || value.document_type === null ? value.document_type : null,
    security_revoked: Boolean(value.security_revoked ?? false),
  };
}

function parseDocumentList(value: unknown): DocumentList | null {
  if (!isRecord(value) || !Array.isArray(value.items) || value.items.length > 100) return null;
  const items: DocumentSummary[] = [];
  for (const raw of value.items) {
    const item = parseDocumentSummary(raw);
    if (!item) return null;
    items.push(item);
  }
  const nextCursor = value.next_cursor ?? null;
  if (nextCursor !== null && typeof nextCursor !== "string") return null;
  return { items, next_cursor: nextCursor };
}

function parseLibraryProfile(value: unknown): LibraryProfile | null {
  if (!isRecord(value)
    || typeof value.logical_document_count !== "number" || !Number.isInteger(value.logical_document_count) || value.logical_document_count < 0
    || typeof value.version_count !== "number" || !Number.isInteger(value.version_count) || value.version_count < 0
    || typeof value.eligible_document_count !== "number" || !Number.isInteger(value.eligible_document_count) || value.eligible_document_count < 0
    || value.eligible_document_count > value.logical_document_count) return null;
  const lastPublicationAt = parseNullableDateTime(value.last_publication_at);
  if (lastPublicationAt === undefined) return null;
  return {
    logical_document_count: value.logical_document_count,
    version_count: value.version_count,
    eligible_document_count: value.eligible_document_count,
    last_publication_at: lastPublicationAt ?? null,
  };
}

function parsePurgeReferenceCounts(value: unknown): PurgeReferenceCounts | null {
  if (!isRecord(value)) return null;
  const names = ["active_ingestion_jobs", "active_runs", "checkpoints", "objects", "pending_reservations", "results", "snapshots"] as const;
  if (!names.every((name) => typeof value[name] === "number" && Number.isInteger(value[name]) && value[name] >= 0)) return null;
  return {
    active_ingestion_jobs: value.active_ingestion_jobs as number,
    active_runs: value.active_runs as number,
    checkpoints: value.checkpoints as number,
    objects: value.objects as number,
    pending_reservations: value.pending_reservations as number,
    results: value.results as number,
    snapshots: value.snapshots as number,
  };
}

function parsePurgePlan(value: unknown): PurgePlan | null {
  if (!isRecord(value) || typeof value.allowed !== "boolean" || !isUuid(value.document_id)
    || !isUuid(value.plan_id) || typeof value.plan_version !== "number" || !Number.isInteger(value.plan_version) || value.plan_version < 1
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || typeof value.expires_at !== "string" || !Number.isFinite(Date.parse(value.expires_at))
    || Date.parse(value.expires_at) <= Date.parse(value.created_at)
    || !Array.isArray(value.blockers)
    || !value.blockers.every((blocker) => typeof blocker === "string" && blocker.length > 0 && blocker.length <= 1000)) return null;
  const references = parsePurgeReferenceCounts(value.references);
  if (!references) return null;
  const eligibleAfter = parseNullableDateTime(value.eligible_after);
  if (eligibleAfter === undefined) return null;
  if (value.retention_policy !== "p14.purge.v1") return null;
  if (value.allowed && (value.blockers.length > 0 || references.active_ingestion_jobs > 0 || references.active_runs > 0
    || references.checkpoints > 0 || references.pending_reservations > 0 || references.results > 0 || references.snapshots > 0)) return null;
  const blockers = value.blockers.filter((blocker): blocker is string => typeof blocker === "string");
  return {
    allowed: value.allowed,
    blockers,
    created_at: value.created_at,
    document_id: value.document_id,
    eligible_after: eligibleAfter ?? null,
    expires_at: value.expires_at,
    plan_id: value.plan_id,
    plan_version: value.plan_version,
    references,
    retention_policy: value.retention_policy,
  };
}

function parsePurgeAccepted(value: unknown): PurgeAccepted | null {
  if (!isRecord(value) || !isUuid(value.document_id) || !isUuid(value.plan_id) || !(value.status === "purge_pending" || value.status === "completed")) return null;
  return { document_id: value.document_id, plan_id: value.plan_id, status: value.status };
}

function parsePurgeStatus(value: unknown): PurgeStatus | null {
  if (!isRecord(value) || !isUuid(value.document_id) || !isUuid(value.plan_id)
    || typeof value.plan_version !== "number" || !Number.isInteger(value.plan_version) || value.plan_version < 1
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || typeof value.expires_at !== "string" || !Number.isFinite(Date.parse(value.expires_at))
    || !purgeStatusStatusValues.some((status) => status === value.status)
    || typeof value.deleted_object_count !== "number" || !Number.isInteger(value.deleted_object_count) || value.deleted_object_count < 0
    || typeof value.total_object_count !== "number" || !Number.isInteger(value.total_object_count) || value.total_object_count < value.deleted_object_count) return null;
  const acceptedAt = parseNullableDateTime(value.accepted_at);
  const completedAt = parseNullableDateTime(value.completed_at);
  if (acceptedAt === undefined || completedAt === undefined) return null;
  const errorCode = value.error_code ?? null;
  if (errorCode !== null && !purgeFailureCodeValues.some((code) => code === errorCode)) return null;
  const status = purgeStatusStatusValues.find((candidate) => candidate === value.status);
  if (!status) return null;
  const typedErrorCode = purgeFailureCodeValues.find((candidate) => candidate === errorCode) ?? null;
  return {
    accepted_at: acceptedAt ?? null,
    completed_at: completedAt ?? null,
    created_at: value.created_at,
    deleted_object_count: value.deleted_object_count,
    document_id: value.document_id,
    error_code: typedErrorCode,
    expires_at: value.expires_at,
    plan_id: value.plan_id,
    plan_version: value.plan_version,
    status,
    total_object_count: value.total_object_count,
  };
}

function parseMetadata(value: unknown): components["schemas"]["DocumentMetadata"] | null {
  if (!isRecord(value) || typeof value.title !== "string" || !(value.legal_status === "active" || value.legal_status === "archived")
    || typeof value.approved_at !== "string") return null;
  return value as components["schemas"]["DocumentMetadata"];
}

function parseVersionSummary(value: unknown): VersionSummary | null {
  if (!isRecord(value) || !isUuid(value.version_id) || !isUuid(value.document_id)
    || !["staging", "published", "superseded", "deactivated"].includes(String(value.publication_status))
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))) return null;
  const metadata = parseMetadata(value.metadata);
  const deactivatedAt = parseNullableDateTime(value.deactivated_at);
  const publishedAt = parseNullableDateTime(value.published_at);
  if (!metadata || deactivatedAt === undefined || publishedAt === undefined) return null;
  return {
    version_id: value.version_id,
    document_id: value.document_id,
    metadata,
    publication_status: value.publication_status as VersionSummary["publication_status"],
    created_at: value.created_at,
    deactivated_at: deactivatedAt ?? null,
    published_at: publishedAt ?? null,
  };
}

function parseDocumentDetail(value: unknown): DocumentDetail | null {
  const summary = parseDocumentSummary(value);
  if (!summary || !isRecord(value) || !Array.isArray(value.versions) || value.versions.length > 100) return null;
  const versions: VersionSummary[] = [];
  for (const raw of value.versions) {
    const version = parseVersionSummary(raw);
    if (!version) return null;
    versions.push(version);
  }
  const nextVersionsCursor = value.next_versions_cursor ?? null;
  if (nextVersionsCursor !== null && typeof nextVersionsCursor !== "string") return null;
  return { ...summary, versions, next_versions_cursor: nextVersionsCursor };
}

function parseSourceDescriptor(value: unknown): components["schemas"]["SourceDescriptor"] | null {
  if (!isRecord(value) || !isUuid(value.version_id) || typeof value.source_url !== "string"
    || typeof value.original_filename !== "string" || typeof value.size_bytes !== "number"
    || !Number.isInteger(value.size_bytes) || value.size_bytes < 1 || typeof value.sha256 !== "string"
    || !/^[0-9a-f]{64}$/.test(value.sha256)) return null;
  const sourceUrl = parsePublicApiPath(value.source_url, ["/api/v1/versions/"]);
  const pageCount = value.page_count ?? null;
  if (!sourceUrl || (pageCount !== null && (typeof pageCount !== "number" || !Number.isInteger(pageCount) || pageCount < 1))) return null;
  return {
    version_id: value.version_id,
    source_url: sourceUrl,
    original_filename: value.original_filename,
    size_bytes: value.size_bytes,
    sha256: value.sha256,
    media_type: "application/pdf",
    page_count: pageCount,
  };
}

function parseVersionDetail(value: unknown): VersionDetail | null {
  const version = parseVersionSummary(value);
  if (!version || !isRecord(value)) return null;
  const source = parseSourceDescriptor(value.source);
  if (!source) return null;
  const currentPublication = parsePublication(value.current_publication);
  if (value.current_publication !== null && value.current_publication !== undefined && currentPublication === null) return null;
  const generations: NonNullable<VersionDetail["generations"]> = Array.isArray(value.generations) && value.generations.length <= 100
    ? value.generations as NonNullable<VersionDetail["generations"]>
    : [];
  return {
    ...version,
    source,
    current_publication: currentPublication,
    generations,
  };
}


function parseSourceSpan(value: unknown): components["schemas"]["SourceSpan"] | null {
  if (!isRecord(value) || typeof value.block_id !== "string" || !value.block_id.trim()
    || typeof value.pdf_page !== "number" || !Number.isInteger(value.pdf_page) || value.pdf_page < 1
    || typeof value.start_offset !== "number" || !Number.isInteger(value.start_offset) || value.start_offset < 0
    || typeof value.end_offset !== "number" || !Number.isInteger(value.end_offset) || value.end_offset < value.start_offset) return null;
  const printedPageLabel = value.printed_page_label ?? null;
  if (printedPageLabel !== null && typeof printedPageLabel !== "string") return null;
  const bbox = value.bbox ?? null;
  if (bbox !== null && (!Array.isArray(bbox) || bbox.length !== 4 || !bbox.every((item) => typeof item === "number" && Number.isFinite(item)))) return null;
  const parsedBbox: [number, number, number, number] | null = bbox === null ? null : [bbox[0], bbox[1], bbox[2], bbox[3]];
  return {
    block_id: value.block_id,
    pdf_page: value.pdf_page,
    start_offset: value.start_offset,
    end_offset: value.end_offset,
    bbox: parsedBbox,
    printed_page_label: printedPageLabel,
  };
}

function parsePublicEvidence(value: unknown): PublicEvidence | null {
  if (!isRecord(value) || typeof value.evidence_id !== "string" || !isUuid(value.run_id) || !isUuid(value.document_version_id)
    || typeof value.document_title !== "string" || typeof value.excerpt !== "string" || typeof value.source_url !== "string") return null;
  const sourceUrl = parsePublicApiPath(value.source_url, ["/api/v1/runs/"])
    ?? parsePublicVersionSourcePath(value.source_url, value.document_version_id);
  const structuralPath = parseStringArray(value.structural_path, 32);
  if (!sourceUrl || !structuralPath || !Array.isArray(value.source_spans) || value.source_spans.length < 1 || value.source_spans.length > 500) return null;
  const sourceSpans = value.source_spans.map(parseSourceSpan);
  if (sourceSpans.some((span) => span === null)) return null;
  return {
    evidence_id: value.evidence_id,
    run_id: value.run_id,
    document_version_id: value.document_version_id,
    document_title: value.document_title,
    structural_path: structuralPath,
    excerpt: value.excerpt,
    source_spans: sourceSpans as PublicEvidence["source_spans"],
    source_url: sourceUrl,
    version_label: typeof value.version_label === "string" || value.version_label === null ? value.version_label : null,
  };
}


function parseDebugCaptureSummary(value: unknown): DebugCaptureSummary | null {
  if (value === null || value === undefined) return null;
  if (!isRecord(value) || value.policy_version !== "p11.capture.v1"
    || typeof value.enabled !== "boolean" || !debugCaptureSummaryStatusValues.includes(value.status as DebugCaptureSummary["status"])
    || typeof value.part_count !== "number" || !Number.isInteger(value.part_count) || value.part_count < 0
    || typeof value.attached_count !== "number" || !Number.isInteger(value.attached_count) || value.attached_count < 0
    || typeof value.unavailable_count !== "number" || !Number.isInteger(value.unavailable_count) || value.unavailable_count < 0) return null;
  const expiresAt = parseNullableDateTime(value.expires_at);
  if (expiresAt === undefined) return null;
  return { enabled: value.enabled, policy_version: "p11.capture.v1", status: value.status as DebugCaptureSummary["status"], expires_at: expiresAt, part_count: value.part_count, attached_count: value.attached_count, unavailable_count: value.unavailable_count };
}

function parseDebugCaptureItem(value: unknown, expectedRunId: string): DebugCaptureItem | null {
  if (!isRecord(value) || !isUuid(value.part_id) || !isUuid(value.call_id)
    || !debugCaptureItemPartValues.includes(value.part as DebugCaptureItem["part"])
    || !debugCaptureItemRoleValues.includes(value.role as DebugCaptureItem["role"])
    || !debugCaptureItemStateValues.includes(value.state as DebugCaptureItem["state"])
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || typeof value.expires_at !== "string" || !Number.isFinite(Date.parse(value.expires_at))
    || typeof value.execution_epoch !== "number" || !Number.isInteger(value.execution_epoch) || value.execution_epoch < 0
    || typeof value.schema_attempt !== "number" || !Number.isInteger(value.schema_attempt) || value.schema_attempt < 0
    || typeof value.size_bytes !== "number" || !Number.isInteger(value.size_bytes) || value.size_bytes < 0
    || typeof value.payload_size_bytes !== "number" || !Number.isInteger(value.payload_size_bytes) || value.payload_size_bytes < 0
    || typeof value.payload_sha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.payload_sha256)) return null;
  const downloadUrl = value.download_url ?? null;
  if (downloadUrl !== null && (typeof downloadUrl !== "string" || downloadUrl !== `/api/v1/runs/${expectedRunId}/debug/captures/${value.part_id}`)) return null;
  return {
    part_id: value.part_id,
    call_id: value.call_id,
    part: value.part as DebugCaptureItem["part"],
    role: value.role as DebugCaptureItem["role"],
    state: value.state as DebugCaptureItem["state"],
    created_at: value.created_at,
    expires_at: value.expires_at,
    execution_epoch: value.execution_epoch,
    schema_attempt: value.schema_attempt,
    size_bytes: value.size_bytes,
    payload_size_bytes: value.payload_size_bytes,
    payload_sha256: value.payload_sha256,
    download_url: downloadUrl,
  };
}

function parseDebugCaptureList(value: unknown, expectedRunId: string): DebugCaptureList | null {
  if (!isRecord(value) || !isUuid(value.run_id) || value.run_id !== expectedRunId || value.policy_version !== "p11.capture.v1"
    || typeof value.enabled !== "boolean" || !debugCaptureListStatusValues.includes(value.status as DebugCaptureList["status"])
    || typeof value.part_count !== "number" || !Number.isInteger(value.part_count) || value.part_count < 0
    || typeof value.attached_count !== "number" || !Number.isInteger(value.attached_count) || value.attached_count < 0
    || typeof value.unavailable_count !== "number" || !Number.isInteger(value.unavailable_count) || value.unavailable_count < 0
    || !Array.isArray(value.items) || value.items.length > 60) return null;
  const expiresAt = parseNullableDateTime(value.expires_at);
  if (expiresAt === undefined) return null;
  const items = value.items.map((item) => parseDebugCaptureItem(item, expectedRunId));
  if (items.some((item) => item === null)) return null;
  return { run_id: value.run_id, enabled: value.enabled, policy_version: "p11.capture.v1", status: value.status as DebugCaptureList["status"], expires_at: expiresAt, part_count: value.part_count, attached_count: value.attached_count, unavailable_count: value.unavailable_count, items: items as DebugCaptureItem[] };
}

function parseOptionalNonNegativeInteger(value: unknown): number | null | undefined {
  if (value === undefined || value === null) return null;
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : undefined;
}

function parseDebugStep(value: unknown): DebugStep | null {
  if (!isRecord(value) || typeof value.attempt !== "number" || !Number.isInteger(value.attempt) || value.attempt < 1
    || !runStageValues.includes(value.stage as RunStage)
    || !debugStepStatusValues.includes(value.status as DebugStep["status"])
    || typeof value.occurred_at !== "string" || !Number.isFinite(Date.parse(value.occurred_at))) return null;
  const candidateCount = parseOptionalNonNegativeInteger(value.candidate_count);
  const durationMs = parseOptionalNonNegativeInteger(value.duration_ms);
  const inputTokens = parseOptionalNonNegativeInteger(value.input_tokens);
  const outputTokens = parseOptionalNonNegativeInteger(value.output_tokens);
  if (candidateCount === undefined || durationMs === undefined || inputTokens === undefined || outputTokens === undefined) return null;
  return {
    attempt: value.attempt,
    candidate_count: candidateCount,
    duration_ms: durationMs,
    input_tokens: inputTokens,
    occurred_at: value.occurred_at,
    output_tokens: outputTokens,
    stage: value.stage as DebugStep["stage"],
    status: value.status as DebugStep["status"],
  };
}

function parseRunDebug(value: unknown): RunDebug | null {
  if (!isRecord(value) || !isUuid(value.run_id) || !Array.isArray(value.steps) || value.steps.length > 500) return null;
  const traceId = value.trace_id ?? null;
  const traceUrl = value.trace_url ?? null;
  const capture = value.capture === undefined || value.capture === null ? null : parseDebugCaptureSummary(value.capture);
  const steps = value.steps.map(parseDebugStep);
  if (traceId !== null && (typeof traceId !== "string" || !/^[0-9a-f]{32}$/.test(traceId))) return null;
  if (traceUrl !== null && typeof traceUrl !== "string") return null;
  if (value.capture !== undefined && value.capture !== null && capture === null) return null;
  if (steps.some((step) => step === null)) return null;
  return { ...value, run_id: value.run_id, steps: steps as DebugStep[], trace_id: traceId, trace_url: traceUrl, capture };
}

function parseJobProgress(value: unknown): IngestionJob["progress"] | null | undefined {
  if (value === null || value === undefined) return value;
  if (!isRecord(value) || typeof value.processed_units !== "number" || value.processed_units < 0
    || !Number.isInteger(value.processed_units) || !(value.unit === "pages" || value.unit === "chunks")) return undefined;
  const total = value.total_units;
  if (total !== null && (typeof total !== "number" || total < 0 || !Number.isInteger(total))) return undefined;
  return { processed_units: value.processed_units, total_units: total, unit: value.unit };
}

function parseNullableDateTime(value: unknown): string | null | undefined {
  if (value === null || value === undefined) return value;
  return typeof value === "string" && Number.isFinite(Date.parse(value)) ? value : undefined;
}


function parseIngestionCapabilities(value: unknown): IngestionCapabilities | null {
  if (!isRecord(value) || typeof value.pipeline_config_alias !== "string" || !/^[A-Za-z0-9:._-]{1,200}$/.test(value.pipeline_config_alias)) return null;
  return { pipeline_config_alias: value.pipeline_config_alias };
}

function parseDebugCapturePolicy(value: unknown): DebugCapturePolicy | null {
  if (!isRecord(value) || typeof value.debug_capture_allowed !== "boolean"
    || typeof value.debug_capture_ttl_hours !== "number" || !Number.isInteger(value.debug_capture_ttl_hours) || value.debug_capture_ttl_hours < 0) return null;
  return { debug_capture_allowed: value.debug_capture_allowed, debug_capture_ttl_hours: value.debug_capture_ttl_hours };
}

function parseIngestionJob(value: unknown): IngestionJob | null {
  if (!isRecord(value) || !isUuid(value.job_id) || !isUuid(value.version_id) || !isJobStatus(value.status)
    || typeof value.attempt !== "number" || !Number.isInteger(value.attempt) || value.attempt < 0
    || typeof value.max_attempts !== "number" || !Number.isInteger(value.max_attempts) || value.max_attempts < 1
    || typeof value.created_at !== "string" || !Number.isFinite(Date.parse(value.created_at))
    || typeof value.last_sequence !== "number" || !Number.isInteger(value.last_sequence) || value.last_sequence < 1) return null;
  const progress = parseJobProgress(value.progress);
  const stage = value.stage ?? null;
  const startedAt = parseNullableDateTime(value.started_at);
  const availableAt = parseNullableDateTime(value.available_at);
  const finishedAt = parseNullableDateTime(value.finished_at);
  if (progress === undefined || (stage !== null && !isIngestionStage(stage))
    || startedAt === undefined || availableAt === undefined || finishedAt === undefined) return null;
  const error = value.error === undefined ? null : parseError({ error: value.error });
  if (value.error !== null && value.error !== undefined && error === null) return null;
  return {
    job_id: value.job_id,
    version_id: value.version_id,
    status: value.status,
    attempt: value.attempt,
    max_attempts: value.max_attempts,
    created_at: value.created_at,
    last_sequence: value.last_sequence,
    cancel_requested: Boolean(value.cancel_requested ?? false),
    progress: progress ?? null,
    stage,
    started_at: startedAt ?? null,
    available_at: availableAt ?? null,
    finished_at: finishedAt ?? null,
    error,
  };
}

function requestIdFrom(response: Response, fallback: string): string {
  const headerRequestId = response.headers.get("X-Request-ID");
  return isUuid(headerRequestId) ? headerRequestId : fallback;
}

async function readJson(response: Response, requestId: string): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    throw new ApiFailure(
      response.ok ? "invalid_response" : "http",
      response.ok ? "API вернул некорректный ответ." : "API временно недоступен. Повторите операцию.",
      requestId,
      response.status,
    );
  }
}

async function requestJson<T>(
  path: string,
  init: RequestInit,
  validate: (value: unknown) => T | null,
  signal?: AbortSignal,
): Promise<ApiResponse<T>> {
  const localRequestId = crypto.randomUUID();
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  headers.set("X-Request-ID", localRequestId);
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      headers,
      credentials: "same-origin",
      redirect: "error",
      cache: "no-store",
      ...(signal ? { signal } : {}),
    });
  } catch (error) {
    if (signal?.aborted) throw error;
    throw new ApiFailure("network", "Не удалось связаться с API. Проверьте, запущены ли сервисы.", localRequestId);
  }

  const requestId = requestIdFrom(response, localRequestId);
  const payload = await readJson(response, requestId);
  if (!response.ok) {
    const error = parseError(payload);
    throw new ApiFailure(
      "http",
      error?.message ?? "Не удалось выполнить операцию API.",
      error?.request_id ?? requestId,
      response.status,
      error,
    );
  }
  const data = validate(payload);
  if (!data) throw new ApiFailure("invalid_response", "Ответ API не соответствует опубликованному контракту.", requestId, response.status);
  return { data, requestId };
}

async function requestBlob(
  path: string,
  signal?: AbortSignal,
): Promise<ApiResponse<Blob>> {
  if (!/^\/api\/v1\/runs\/[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}\/debug\/captures\/[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(path)) {
    throw new ApiFailure("invalid_response", "Ссылка на debug capture не соответствует опубликованному контракту.", crypto.randomUUID());
  }
  const localRequestId = crypto.randomUUID();
  let response: Response;
  try {
    response = await fetch(path, {
      method: "GET",
      headers: { Accept: "application/json", "X-Request-ID": localRequestId },
      credentials: "same-origin",
      redirect: "error",
      cache: "no-store",
      ...(signal ? { signal } : {}),
    });
  } catch (error) {
    if (signal?.aborted) throw error;
    throw new ApiFailure("network", "Не удалось связаться с API. Проверьте, запущены ли сервисы.", localRequestId);
  }

  const requestId = requestIdFrom(response, localRequestId);
  if (!response.ok) {
    let payload: unknown = null;
    try { payload = await response.json(); } catch { /* keep safe generic error */ }
    const error = parseError(payload);
    throw new ApiFailure("http", error?.message ?? "Debug capture недоступен или срок доступа истёк.", error?.request_id ?? requestId, response.status, error);
  }
  const contentLength = response.headers.get("Content-Length");
  if (contentLength && Number(contentLength) > DEBUG_CAPTURE_DOWNLOAD_MAX_BYTES) {
    throw new ApiFailure("invalid_response", "Debug capture превышает безопасный лимит браузера.", requestId, response.status);
  }
  const blob = await response.blob();
  if (blob.size > DEBUG_CAPTURE_DOWNLOAD_MAX_BYTES) {
    throw new ApiFailure("invalid_response", "Debug capture превышает безопасный лимит браузера.", requestId, response.status);
  }
  return { data: blob, requestId };
}

function jsonBody(value: unknown): BodyInit {
  return JSON.stringify(value);
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value);
}

export async function getSystemStatus(signal?: AbortSignal): Promise<SystemStatusResponse> {
  return requestJson("/api/v1/system/status", { method: "GET" }, parseSystemStatus, signal);
}

export async function getAuthSession(signal?: AbortSignal): Promise<ApiResponse<SessionInfo>> {
  return requestJson("/api/v1/auth/session", { method: "GET" }, parseSessionInfo, signal);
}

export async function createAuthSession(accessKey: string, signal?: AbortSignal): Promise<ApiResponse<SessionInfo>> {
  const body: SessionRequest = { access_key: accessKey };
  return requestJson("/api/v1/auth/session", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: jsonBody(body),
  }, parseSessionInfo, signal);
}

export async function deleteAuthSession(signal?: AbortSignal): Promise<ApiResponse<null>> {
  const localRequestId = crypto.randomUUID();
  let response: Response;
  try {
    response = await fetch("/api/v1/auth/session", {
      method: "DELETE",
      headers: { "X-Request-ID": localRequestId },
      credentials: "same-origin",
      redirect: "error",
      cache: "no-store",
      ...(signal ? { signal } : {}),
    });
  } catch (error) {
    if (signal?.aborted) throw error;
    throw new ApiFailure("network", "Не удалось связаться с API. Проверьте, запущены ли сервисы.", localRequestId);
  }
  const requestId = requestIdFrom(response, localRequestId);
  if (response.status === 204) return { data: null, requestId };
  let payload: unknown = null;
  try { payload = await response.json(); } catch { /* keep safe generic error */ }
  const error = parseError(payload);
  throw new ApiFailure("http", error?.message ?? "Не удалось завершить сессию.", error?.request_id ?? requestId, response.status, error);
}

export async function uploadDocument(
  file: File,
  options: VersionUploadOptions,
  idempotencyKey: string,
  signal?: AbortSignal,
): Promise<ApiResponse<UploadAccepted>> {
  const form = new FormData();
  form.set("file", file);
  form.set("options", JSON.stringify(options));
  return requestJson("/api/v1/documents", {
    method: "POST",
    headers: { "Idempotency-Key": idempotencyKey },
    body: form,
  }, parseUploadAccepted, signal);
}

export async function uploadDocumentVersion(
  documentId: string,
  file: File,
  options: VersionUploadOptions,
  idempotencyKey: string,
  signal?: AbortSignal,
): Promise<ApiResponse<UploadAccepted>> {
  const form = new FormData();
  form.set("file", file);
  form.set("options", JSON.stringify(options));
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}/versions`, {
    method: "POST",
    headers: { "Idempotency-Key": idempotencyKey },
    body: form,
  }, parseUploadAccepted, signal);
}

export async function getIngestionJob(jobId: string, signal?: AbortSignal): Promise<ApiResponse<IngestionJob>> {
  return requestJson(`/api/v1/ingestion-jobs/${encodePathSegment(jobId)}`, { method: "GET" }, parseIngestionJob, signal);
}

export async function cancelIngestionJob(jobId: string, signal?: AbortSignal): Promise<ApiResponse<JobCommandAccepted>> {
  return requestJson(`/api/v1/ingestion-jobs/${encodePathSegment(jobId)}/cancel`, { method: "POST" }, parseJobCommandAccepted, signal);
}

export async function retryIngestionJob(jobId: string, signal?: AbortSignal): Promise<ApiResponse<JobCommandAccepted>> {
  return requestJson(`/api/v1/ingestion-jobs/${encodePathSegment(jobId)}/retry`, { method: "POST" }, parseJobCommandAccepted, signal);
}


export async function getIngestionCapabilities(signal?: AbortSignal): Promise<ApiResponse<IngestionCapabilities>> {
  return requestJson("/api/v1/system/ingestion-capabilities", { method: "GET" }, parseIngestionCapabilities, signal);
}

export async function getDebugCapturePolicy(signal?: AbortSignal): Promise<ApiResponse<DebugCapturePolicy>> {
  return requestJson("/api/v1/system/debug-capture-policy", { method: "GET" }, parseDebugCapturePolicy, signal);
}

export async function reindexVersion(
  versionId: string,
  request: ReindexRequest,
  idempotencyKey: string,
  signal?: AbortSignal,
): Promise<ApiResponse<JobCommandAccepted>> {
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/reindex`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": idempotencyKey },
    body: jsonBody(request),
  }, parseJobCommandAccepted, signal);
}

export async function publishVersion(
  versionId: string,
  request: PublishRequest,
  signal?: AbortSignal,
): Promise<ApiResponse<PublicationInfo>> {
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: jsonBody(request),
  }, parsePublication, signal);
}

export async function deactivateVersion(
  versionId: string,
  request: DeactivateRequest,
  signal?: AbortSignal,
): Promise<ApiResponse<VersionSummary>> {
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/deactivate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: jsonBody(request),
  }, parseVersionSummary, signal);
}

export async function createRun(
  question: string,
  debugCapture: boolean,
  idempotencyKey: string,
  signal?: AbortSignal,
): Promise<ApiResponse<RunAccepted>> {
  const body: CreateRunRequest = { question, debug_capture: debugCapture };
  return requestJson("/api/v1/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": idempotencyKey },
    body: jsonBody(body),
  }, parseRunAccepted, signal);
}

export async function listRuns(params: { cursor?: string | null; limit?: number } = {}, signal?: AbortSignal): Promise<ApiResponse<RunList>> {
  const query = queryString({ cursor: params.cursor ?? null, limit: params.limit ?? 50 });
  return requestJson(`/api/v1/runs${query}`, { method: "GET" }, parseRunList, signal);
}

export async function getRun(runId: string, signal?: AbortSignal): Promise<ApiResponse<PublicRun>> {
  return requestJson(`/api/v1/runs/${encodePathSegment(runId)}`, { method: "GET" }, parsePublicRun, signal);
}

export async function cancelRun(runId: string, signal?: AbortSignal): Promise<ApiResponse<RunCancelAccepted | PublicRun>> {
  return requestJson(`/api/v1/runs/${encodePathSegment(runId)}/cancel`, { method: "POST" }, (payload) => (
    parseRunCancelAccepted(payload) ?? parsePublicRun(payload)
  ), signal);
}

export async function getRunDebug(runId: string, signal?: AbortSignal): Promise<ApiResponse<RunDebug>> {
  return requestJson(`/api/v1/runs/${encodePathSegment(runId)}/debug`, { method: "GET" }, parseRunDebug, signal);
}


export async function listDebugCaptures(runId: string, signal?: AbortSignal): Promise<ApiResponse<DebugCaptureList>> {
  return requestJson(`/api/v1/runs/${encodePathSegment(runId)}/debug/captures`, { method: "GET" }, (payload) => parseDebugCaptureList(payload, runId), signal);
}

export async function downloadDebugCapture(downloadUrl: string, signal?: AbortSignal): Promise<ApiResponse<Blob>> {
  return requestBlob(downloadUrl, signal);
}

export async function getSourceEvidence(runId: string, evidenceId: string, signal?: AbortSignal): Promise<ApiResponse<PublicEvidence>> {
  return requestJson(`/api/v1/runs/${encodePathSegment(runId)}/sources/${encodePathSegment(evidenceId)}`, { method: "GET" }, parsePublicEvidence, signal);
}

export async function getLibraryProfile(signal?: AbortSignal): Promise<ApiResponse<LibraryProfile>> {
  return requestJson("/api/v1/library/profile", { method: "GET" }, parseLibraryProfile, signal);
}

export async function listDocuments(params: { q?: string; status?: "all" | "active" | "archived" | "staging" | "failed"; cursor?: string | null; limit?: number } = {}, signal?: AbortSignal): Promise<ApiResponse<DocumentList>> {
  const query = queryString({ limit: params.limit ?? 50, status: params.status ?? "all", q: params.q?.trim() || null, cursor: params.cursor ?? null });
  return requestJson(`/api/v1/documents${query}`, { method: "GET" }, parseDocumentList, signal);
}

export async function archiveDocument(
  documentId: string,
  request: ArchiveDocumentRequest,
  signal?: AbortSignal,
): Promise<ApiResponse<DocumentSummary>> {
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}/archive`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: jsonBody(request),
  }, parseDocumentSummary, signal);
}

export async function createPurgePlan(documentId: string, signal?: AbortSignal): Promise<ApiResponse<PurgePlan>> {
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}/purge-plan`, { method: "POST" }, parsePurgePlan, signal);
}

export async function submitPurgePlan(
  documentId: string,
  request: PurgeRequest,
  signal?: AbortSignal,
): Promise<ApiResponse<PurgeAccepted>> {
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}/purge`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: jsonBody(request),
  }, parsePurgeAccepted, signal);
}

export async function getPurgeStatus(documentId: string, planId: string, signal?: AbortSignal): Promise<ApiResponse<PurgeStatus>> {
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}/purge-plans/${encodePathSegment(planId)}`, { method: "GET" }, parsePurgeStatus, signal);
}

export async function getDocument(documentId: string, params: { versionsCursor?: string | null; versionsLimit?: number } = {}, signal?: AbortSignal): Promise<ApiResponse<DocumentDetail>> {
  const query = queryString({ versions_cursor: params.versionsCursor ?? null, versions_limit: params.versionsLimit ?? 50 });
  return requestJson(`/api/v1/documents/${encodePathSegment(documentId)}${query}`, { method: "GET" }, parseDocumentDetail, signal);
}

export async function getVersion(versionId: string, signal?: AbortSignal): Promise<ApiResponse<VersionDetail>> {
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}`, { method: "GET" }, parseVersionDetail, signal);
}

function queryString(params: Record<string, string | number | null | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== "") search.set(key, String(value));
  }
  const rendered = search.toString();
  return rendered ? `?${rendered}` : "";
}

export async function getCanonicalTree(
  versionId: string,
  parseGenerationId: string,
  params: { parentId?: string | null; cursor?: string | null; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ApiResponse<CanonicalTreePage>> {
  const query = queryString({ parse_generation_id: parseGenerationId, parent_id: params.parentId ?? null, cursor: params.cursor ?? null, limit: params.limit ?? 50 });
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/tree${query}`, { method: "GET" }, parseCanonicalTreePage, signal);
}

export async function getParseQuality(
  versionId: string,
  parseGenerationId: string,
  params: { cursor?: string | null; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ApiResponse<ParseQualityPage>> {
  const query = queryString({ parse_generation_id: parseGenerationId, cursor: params.cursor ?? null, limit: params.limit ?? 50 });
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/quality${query}`, { method: "GET" }, parseQualityPage, signal);
}

export async function getStructuredTable(
  versionId: string,
  nodeId: string,
  parseGenerationId: string,
  params: { cursor?: string | null; limit?: number } = {},
  signal?: AbortSignal,
): Promise<ApiResponse<StructuredTablePage>> {
  const query = queryString({ parse_generation_id: parseGenerationId, cursor: params.cursor ?? null, limit: params.limit ?? 25 });
  return requestJson(`/api/v1/versions/${encodePathSegment(versionId)}/tables/${encodePathSegment(nodeId)}${query}`, { method: "GET" }, parseStructuredTablePage, signal);
}
