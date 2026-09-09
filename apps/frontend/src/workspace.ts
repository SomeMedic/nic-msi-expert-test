import { ingestionStageValues, jobStatusValues, runStageValues, runStatusValues } from "./api/generated";
import type { components, paths } from "./api/generated";

export type SessionInfo = components["schemas"]["SessionInfo"];
export type SystemStatus = components["schemas"]["SystemStatus"];
export type RunStage = components["schemas"]["RunStage"];
export type RunStatus = components["schemas"]["RunStatus"];
export type IngestionJob = components["schemas"]["IngestionJob"];
export type IngestionStage = components["schemas"]["IngestionStage"];
export type JobStatus = components["schemas"]["JobStatus"];
export type FinalAnswer = components["schemas"]["FinalAnswer"];
export type RefusalResult = components["schemas"]["RefusalResult"];
export type CitationDTO = components["schemas"]["CitationDTO"];
export type PublicRun = components["schemas"]["PublicRun"];
export type PublicEvidence = components["schemas"]["PublicEvidence"];
export type RunDebug = components["schemas"]["RunDebug"];
export type DocumentMetadata = components["schemas"]["DocumentMetadata"];
export type UploadAccepted = components["schemas"]["UploadAccepted"];
export type VersionDetail = components["schemas"]["VersionDetail"];
export type DocumentDetail = components["schemas"]["DocumentDetail"];
export type DocumentSummary = components["schemas"]["DocumentSummary"];
export type VersionSummary = components["schemas"]["VersionSummary"];
export type PurgeAccepted = components["schemas"]["PurgeAccepted"];
export type PurgePlan = components["schemas"]["PurgePlan"];
export type PurgeStatus = components["schemas"]["PurgeStatus"];

export type RunActionIdentity = {
  principalId: string | null;
  runId: string;
  requestSeq: number;
};

export type CurrentRunActionIdentity = {
  principalId: string | null;
  displayedRunId: string | null;
  requestSeq: number;
};

export type QuestionComposerPhase = "idle" | "loading" | "loaded" | "error";

export function questionComposerVisible(input: { expanded: boolean; runPhase: QuestionComposerPhase }): boolean {
  return input.expanded || input.runPhase === "idle" || input.runPhase === "error";
}

type ExistingPath = keyof paths;

export const supportedRoutes = {
  session: "/api/v1/auth/session",
  uploadDocument: "/api/v1/documents",
  uploadVersionTemplate: "/api/v1/documents/{document_id}/versions",
  ingestionJobTemplate: "/api/v1/ingestion-jobs/{job_id}",
  ingestionCancelTemplate: "/api/v1/ingestion-jobs/{job_id}/cancel",
  ingestionRetryTemplate: "/api/v1/ingestion-jobs/{job_id}/retry",
  systemStatus: "/api/v1/system/status",
  reindexTemplate: "/api/v1/versions/{version_id}/reindex",
  publishTemplate: "/api/v1/versions/{version_id}/publish",
  deactivateTemplate: "/api/v1/versions/{version_id}/deactivate",
  archiveDocumentTemplate: "/api/v1/documents/{document_id}/archive",
  purgePlanTemplate: "/api/v1/documents/{document_id}/purge-plan",
  purgeSubmitTemplate: "/api/v1/documents/{document_id}/purge",
  purgeStatusTemplate: "/api/v1/documents/{document_id}/purge-plans/{plan_id}",
  libraryProfile: "/api/v1/library/profile",
  qualityTemplate: "/api/v1/versions/{version_id}/quality",
  tableTemplate: "/api/v1/versions/{version_id}/tables/{node_id}",
  treeTemplate: "/api/v1/versions/{version_id}/tree",
} satisfies Record<string, ExistingPath>;

export const statusLabels: Record<SystemStatus["status"], string> = {
  ready: "готово",
  degraded: "частично доступно",
  unavailable: "недоступно",
};

export const runStatusLabels: Record<RunStatus, string> = {
  created: "создан",
  running: "выполняется",
  cancelling: "останавливается",
  completed: "завершён",
  refused: "отказ",
  failed: "ошибка",
  cancelled: "отменён",
};

export const runStageLabels: Record<RunStage, string> = {
  snapshotting: "снимок базы",
  routing: "маршрутизация",
  retrieving: "поиск источников",
  reranking: "ранжирование",
  building_context: "контекст",
  drafting: "формирование ответа",
  checking_citations: "проверка цитат",
  validating: "валидация",
  repairing: "одно исправление",
  revalidating: "повторная проверка",
  rendering: "сборка ответа",
  finalizing: "финализация",
};

export const jobStatusLabels: Record<JobStatus, string> = {
  queued: "в очереди",
  running: "выполняется",
  retry_wait: "ожидает повтор",
  completed: "завершено",
  failed: "ошибка",
  cancelled: "отменено",
};

export const ingestionStageLabels: Record<IngestionStage, string> = {
  queued: "очередь",
  validating: "проверка файла",
  parsing: "чтение PDF",
  assessing_extraction: "оценка извлечения",
  fallback_parsing: "резервный парсинг",
  normalizing: "нормализация",
  building_structure: "структура",
  chunking: "фрагменты",
  embedding: "эмбеддинги",
  indexing: "индексация",
  ready_to_publish: "готово к публикации",
  publishing: "публикация",
};

export function isRunStatus(value: unknown): value is RunStatus {
  return typeof value === "string" && runStatusValues.some((status) => status === value);
}

export function isRunStage(value: unknown): value is RunStage {
  return typeof value === "string" && runStageValues.some((stage) => stage === value);
}

export function isJobStatus(value: unknown): value is JobStatus {
  return typeof value === "string" && jobStatusValues.some((status) => status === value);
}

export function isIngestionStage(value: unknown): value is IngestionStage {
  return typeof value === "string" && ingestionStageValues.some((stage) => stage === value);
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return value;
  return new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeStyle: "short" }).format(new Date(timestamp));
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  const timestamp = Date.parse(`${value}T00:00:00Z`);
  if (!Number.isFinite(timestamp)) return value;
  return new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeZone: "UTC" }).format(new Date(timestamp));
}

export function formatBytes(size: number | null | undefined): string {
  if (!Number.isFinite(size ?? NaN) || !size) return "—";
  if (size < 1024) return `${size} байт`;
  const units = ["КиБ", "МиБ", "ГиБ"];
  let value = size / 1024;
  let unit = units[0] ?? "КиБ";
  for (let index = 1; value >= 1024 && index < units.length; index += 1) {
    value /= 1024;
    unit = units[index] ?? unit;
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
}

export function formatDurationSeconds(durationMs: number | null | undefined): string {
  if (!Number.isFinite(durationMs ?? NaN) || durationMs === null || durationMs === undefined) return "—";
  return `${Math.max(0, durationMs / 1000).toFixed(1)} с`;
}


export const maxPdfUploadBytes = 50 * 1024 * 1024;

export type UploadFileValidation = {
  ok: boolean;
  message: string;
};

export function validatePdfUploadFile(file: Pick<File, "name" | "size" | "type"> | null): UploadFileValidation {
  if (!file) return { ok: false, message: "Файл не выбран" };
  if (file.size <= 0) return { ok: false, message: "Файл пустой" };
  if (file.size > maxPdfUploadBytes) return { ok: false, message: `PDF больше ${formatBytes(maxPdfUploadBytes)}` };
  const hasPdfType = file.type === "application/pdf";
  const hasPdfName = file.name.toLowerCase().endsWith(".pdf");
  if (!hasPdfType && !hasPdfName) return { ok: false, message: "Клиентская проверка: нужен PDF; сервер всё равно перепроверит файл." };
  return { ok: true, message: `Готово к передаче: ${formatBytes(file.size)}` };
}

export function firstParseGenerationId(version: Pick<VersionDetail, "generations">): string {
  const generations = version.generations ?? [];
  return generations.find((item) => item.kind === "parse" && item.status === "ready")?.generation_id
    ?? generations.find((item) => item.kind === "parse")?.generation_id
    ?? "";
}

export function firstReadyIndexGenerationId(version: Pick<VersionDetail, "generations">): string {
  return (version.generations ?? []).find((item) => item.kind === "index" && item.status === "ready")?.generation_id ?? "";
}

export function currentPublicationId(source: Pick<DocumentSummary | DocumentDetail | VersionDetail, "current_publication"> | null | undefined): string | null {
  return source?.current_publication?.publication_id ?? null;
}

export function uploadExpectedPublicationId(document: Pick<DocumentSummary | DocumentDetail, "current_publication"> | null | undefined): string | null {
  return currentPublicationId(document);
}


export type DocumentLifecycleBadgeTexts = {
  document: string;
  publication: string;
  security: string;
};

export function documentLifecycleBadgeTexts(source: Pick<DocumentSummary | DocumentDetail, "archived_at" | "current_publication" | "security_revoked">): DocumentLifecycleBadgeTexts {
  return {
    document: source.archived_at ? "документ: архивирован" : "документ: в библиотеке",
    publication: source.current_publication ? "публикация: опубликован" : "публикация: не опубликован",
    security: source.security_revoked ? "доступ: отозван" : "доступ: разрешён",
  };
}

export function versionLegalStatusText(version: Pick<VersionSummary | VersionDetail, "metadata">): string {
  return version.metadata.legal_status === "archived" ? "правовой статус версии: архивная" : "правовой статус версии: действующая";
}

export function summarizeVersionChoice(version: VersionSummary | VersionDetail): string {
  const label = version.metadata.version_label ?? version.metadata.approved_at ?? version.version_id;
  return `${label} · ${version.publication_status}`;
}



export type CursorPageState = {
  cursors: Array<string | null>;
  index: number;
};

export function currentCursorPage(state: CursorPageState): string | null {
  return state.cursors[state.index] ?? null;
}

export function previousCursorPage(state: CursorPageState): CursorPageState {
  return { cursors: state.cursors, index: Math.max(0, state.index - 1) };
}

export function nextCursorPage(state: CursorPageState, nextCursor: string): CursorPageState {
  return {
    cursors: state.cursors.slice(0, state.index + 1).concat(nextCursor),
    index: state.index + 1,
  };
}

export type VersionPageIdentity = {
  versionId: string;
  parseGenerationId: string;
};

export type TreePageIdentity = VersionPageIdentity & {
  parentId: string | null;
};

export type TablePageIdentity = VersionPageIdentity & {
  nodeId: string;
};

export type DocumentListRequest = {
  query: string;
  cursor: string | null;
  cursors: Array<string | null>;
  cursorIndex: number;
};

export type DocumentDetailRequest = {
  documentId: string;
  versionsCursor: string | null;
  versionsCursors: Array<string | null>;
  versionsCursorIndex: number;
  resetVersion: boolean;
};

export type VersionDetailIdentity = {
  versionId: string;
  documentId: string | null;
};

export function resolveRequestedTreeParentId(requestedParentId: string | null | undefined, currentParentId: string | null): string | null {
  return requestedParentId === undefined ? currentParentId : requestedParentId;
}

export function resolveDocumentListRequest(
  request: Partial<DocumentListRequest>,
  fallbackQuery: string,
): DocumentListRequest {
  const cursor = request.cursor ?? null;
  const cursors = request.cursors ?? [cursor];
  return {
    query: (request.query ?? fallbackQuery).trim(),
    cursor,
    cursors,
    cursorIndex: request.cursorIndex ?? cursors.length - 1,
  };
}

export function documentDetailMatches(detail: Pick<components["schemas"]["DocumentDetail"], "document_id">, documentId: string): boolean {
  return detail.document_id === documentId;
}

export function versionDetailMatches(version: Pick<components["schemas"]["VersionDetail"], "version_id" | "document_id">, identity: VersionDetailIdentity): boolean {
  return version.version_id === identity.versionId && (identity.documentId === null || version.document_id === identity.documentId);
}

export type PurgePlanIdentity = {
  documentId: string;
  planId?: string;
  planVersion?: number;
};

export function purgePlanMatches(plan: Pick<PurgePlan, "document_id" | "plan_id" | "plan_version">, identity: PurgePlanIdentity): boolean {
  return plan.document_id === identity.documentId
    && (identity.planId === undefined || plan.plan_id === identity.planId)
    && (identity.planVersion === undefined || plan.plan_version === identity.planVersion);
}

export function purgeAcceptedMatches(accepted: Pick<PurgeAccepted, "document_id" | "plan_id">, identity: Pick<PurgePlanIdentity, "documentId" | "planId">): boolean {
  return accepted.document_id === identity.documentId && accepted.plan_id === identity.planId;
}

export function purgeStatusMatches(status: Pick<PurgeStatus, "document_id" | "plan_id" | "plan_version">, identity: PurgePlanIdentity): boolean {
  return purgePlanMatches(status, identity);
}

export function selectedDocumentReadyForVersionUpload(
  selectedDocument: Pick<DocumentSummary | DocumentDetail, "document_id" | "current_publication"> | null,
  targetDocumentId: string,
): boolean {
  return Boolean(targetDocumentId) && selectedDocument?.document_id === targetDocumentId;
}

export function selectedVersionStillTargetsAction(currentVersionId: string, actionVersionId: string): boolean {
  return Boolean(actionVersionId) && currentVersionId === actionVersionId;
}

export function evidenceIdForClaimCitation(citations: Array<Pick<components["schemas"]["CitationDTO"], "citation_id" | "evidence_id">>, claimCitationId: string): string | null {
  return citations.find((citation) => citation.citation_id === claimCitationId)?.evidence_id ?? null;
}

export function publicEvidenceMatches(evidence: Pick<components["schemas"]["PublicEvidence"], "run_id" | "evidence_id">, identity: { runId: string; evidenceId: string }): boolean {
  return evidence.run_id === identity.runId && evidence.evidence_id === identity.evidenceId;
}

export function sourcePdfPages(sourceSpans: Array<Pick<components["schemas"]["SourceSpan"], "pdf_page">>): number[] {
  return [...new Set(sourceSpans.map((span) => span.pdf_page))].sort((left, right) => left - right);
}

export function formatSourcePdfPages(pdfPages: readonly number[]): string {
  if (pdfPages.length === 0) return "Страница PDF: не указана";
  return `${pdfPages.length === 1 ? "Страница PDF" : "Страницы PDF"}: ${pdfPages.join(", ")}`;
}

export function sourceUrlForFirstPdfPage(sourceUrl: string, pdfPages: readonly number[]): string {
  const [base] = sourceUrl.split("#", 1);
  const firstPage = pdfPages[0];
  return firstPage ? `${base}#page=${firstPage}` : sourceUrl;
}

export function publicRunMatches(run: Pick<PublicRun, "run_id">, runId: string): boolean {
  return run.run_id === runId;
}

export function runEventCursorAfterReload(currentLastSequence: number, responseLastSequence: number, resetEvents: boolean): number {
  return resetEvents ? responseLastSequence : Math.max(currentLastSequence, responseLastSequence);
}

export function runActionStillTargetsDisplayedRun(action: RunActionIdentity, current: CurrentRunActionIdentity): boolean {
  return action.principalId === current.principalId
    && action.runId === current.displayedRunId
    && action.requestSeq === current.requestSeq;
}

export function canonicalTreePageMatches(page: Pick<components["schemas"]["CanonicalTreePage"], "version_id" | "parse_generation_id" | "parent_id">, identity: TreePageIdentity): boolean {
  return page.version_id === identity.versionId && page.parse_generation_id === identity.parseGenerationId && page.parent_id === identity.parentId;
}

export function qualityPageMatches(page: Pick<components["schemas"]["ParseQualityPage"], "version_id" | "parse_generation_id">, identity: VersionPageIdentity): boolean {
  return page.version_id === identity.versionId && page.parse_generation_id === identity.parseGenerationId;
}

export function structuredTablePageMatches(page: Pick<components["schemas"]["StructuredTablePage"], "version_id" | "parse_generation_id" | "node_id">, identity: TablePageIdentity): boolean {
  return page.version_id === identity.versionId && page.parse_generation_id === identity.parseGenerationId && page.node_id === identity.nodeId;
}

export function progressText(job: IngestionJob): string {
  if (!job.progress) return "Прогресс уточняется сервером";
  const total = job.progress.total_units;
  const unit = job.progress.unit === "pages" ? "страниц" : "фрагментов";
  return total === null ? `${job.progress.processed_units} ${unit}` : `${job.progress.processed_units} из ${total} ${unit}`;
}

export function isFinalAnswer(result: FinalAnswer | RefusalResult | null | undefined): result is FinalAnswer {
  return result?.kind === "completed";
}

export function isRefusal(result: FinalAnswer | RefusalResult | null | undefined): result is RefusalResult {
  return result?.kind === "refused";
}

export function answerHasDraftLeak(text: string): boolean {
  return /DraftAnswer|черновик ответа|raw prompt|PRIVATE_INTERNAL_TRACE/i.test(text);
}



