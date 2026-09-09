import { Component, type Dispatch, type FormEvent, type ReactNode, type SetStateAction, useEffect, useMemo, useRef, useState } from "react";
import {
  ApiFailure,
  archiveDocument,
  cancelIngestionJob,
  cancelRun,
  createPurgePlan,
  createAuthSession,
  createRun,
  deactivateVersion,
  deleteAuthSession,
  downloadDebugCapture,
  getCanonicalTree,
  getDebugCapturePolicy,
  getIngestionCapabilities,
  getLibraryProfile,
  getParseQuality,
  getPurgeStatus,
  getStructuredTable,
  getAuthSession,
  getIngestionJob,
  getDocument,
  getVersion,
  getRun,
  getRunDebug,
  getSourceEvidence,
  listDebugCaptures,
  getSystemStatus,
  listDocuments,
  listRuns,
  publishVersion,
  retryIngestionJob,
  reindexVersion,
  submitPurgePlan,
  uploadDocument,
  uploadDocumentVersion,
} from "./api/client";
import type { ApiResponse, SystemStatusResponse } from "./api/client";
import { isTerminalJobEvent, jobEventStatusText, reduceJobEvent, subscribeJobEvents } from "./api/job-events";
import type { JobEventState } from "./api/job-events";
import { eventStatusText, isTerminalRunEvent, liveRunProgress, reduceRunEvent, subscribeRunEvents } from "./api/run-events";
import type { RunEventState } from "./api/run-events";
import type { components } from "./api/generated";
import { Button } from "@/components/ui/button";
import { ThemeToggle } from "./components/ThemeToggle";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  currentPublicationId,
  canonicalTreePageMatches,
  currentCursorPage,
  documentDetailMatches,
  firstParseGenerationId,
  firstReadyIndexGenerationId,
  formatBytes,
  formatDateTime,
  formatDurationSeconds,
  ingestionStageLabels,
  isFinalAnswer,
  isRefusal,
  jobStatusLabels,
  previousCursorPage,
  progressText,
  purgeAcceptedMatches,
  purgePlanMatches,
  purgeStatusMatches,
  publicEvidenceMatches,
  publicRunMatches,
  qualityPageMatches,
  resolveDocumentListRequest,
  resolveRequestedTreeParentId,
  runActionStillTargetsDisplayedRun,
  runEventCursorAfterReload,
  runStageLabels,
  runStatusLabels,
  formatSourcePdfPages,
  sourcePdfPages,
  sourceUrlForFirstPdfPage,
  statusLabels,
  structuredTablePageMatches,
  nextCursorPage,
  selectedDocumentReadyForVersionUpload,
  selectedVersionStillTargetsAction,
  versionDetailMatches,
  versionLegalStatusText,
  summarizeVersionChoice,
  uploadExpectedPublicationId,
  validatePdfUploadFile,
} from "./workspace";
import type { DocumentDetailRequest, DocumentListRequest, VersionDetailIdentity } from "./workspace";

type SessionInfo = components["schemas"]["SessionInfo"];
type VersionUploadOptions = components["schemas"]["VersionUploadOptions"];
type UploadAccepted = components["schemas"]["UploadAccepted"];
type IngestionJob = components["schemas"]["IngestionJob"];
type PublicRun = components["schemas"]["PublicRun"];
type RunList = components["schemas"]["RunList"];
type PublicEvidence = components["schemas"]["PublicEvidence"];
type RunDebug = components["schemas"]["RunDebug"];
type DebugCapturePolicy = components["schemas"]["DebugCapturePolicy"];
type DebugCaptureList = components["schemas"]["DebugCaptureList"];
type DebugCaptureItem = components["schemas"]["DebugCaptureItem"];
type IngestionCapabilities = components["schemas"]["IngestionCapabilities"];
type LibraryProfile = components["schemas"]["LibraryProfile"];
type PublicationInfo = components["schemas"]["PublicationInfo"];
type VersionSummary = components["schemas"]["VersionSummary"];
type DocumentSummary = components["schemas"]["DocumentSummary"];
type DocumentList = components["schemas"]["DocumentList"];
type DocumentDetail = components["schemas"]["DocumentDetail"];
type LegalStatus = components["schemas"]["LegalStatus"];
type VersionDetail = components["schemas"]["VersionDetail"];
type CanonicalTreeNode = components["schemas"]["CanonicalTreeNode"];
type CanonicalTreePage = components["schemas"]["CanonicalTreePage"];
type ParseQualityPage = components["schemas"]["ParseQualityPage"];
type StructuredTableCell = components["schemas"]["StructuredTableCell"];
type StructuredTablePage = components["schemas"]["StructuredTablePage"];
type PurgeAccepted = components["schemas"]["PurgeAccepted"];
type PurgePlan = components["schemas"]["PurgePlan"];
type PurgeStatus = components["schemas"]["PurgeStatus"];
type CitationDTO = components["schemas"]["CitationDTO"];
type FinalAnswer = components["schemas"]["FinalAnswer"];

const terminalRunStatuses = new Set<PublicRun["status"]>(["completed", "failed", "refused", "cancelled"]);
const terminalJobStatuses = new Set<IngestionJob["status"]>(["completed", "failed", "cancelled"]);

type Loadable<T> =
  | { phase: "idle" }
  | { phase: "loading" }
  | { phase: "loaded"; response: ApiResponse<T> }
  | { phase: "error"; failure: ApiFailure | null };

type CaptureDownloadState =
  | { phase: "idle" }
  | { phase: "loading" }
  | { phase: "loaded"; filename: string }
  | { phase: "error"; failure: ApiFailure | null };

type View = "workspace" | "documents" | "ingestion" | "history" | "debug" | "system";

type WorkspaceSelection = {
  document: DocumentDetail | DocumentSummary | null;
  version: VersionDetail | null;
  jobId: string;
};

type HistoricalRunSelection = {
  runId: string;
  questionExcerpt: string;
};

type LoadedRunQuestion = {
  runId: string;
  label: string;
  text: string;
};

const viewLabels: Record<View, string> = {
  workspace: "Вопрос",
  documents: "Документы",
  ingestion: "Обработка",
  history: "История",
  debug: "Диагностика",
  system: "Настройки",
};

const stageOrder = Object.keys(runStageLabels) as Array<keyof typeof runStageLabels>;
const ingestionOrder = Object.keys(ingestionStageLabels) as Array<keyof typeof ingestionStageLabels>;

function asApiFailure(error: unknown): ApiFailure | null {
  return error instanceof ApiFailure ? error : null;
}

function newIdempotencyKey(prefix: string): string {
  return `${prefix}-${crypto.randomUUID()}`;
}

function ErrorNotice({ failure, fallback, headline }: { failure: ApiFailure | null; fallback: string; headline?: string }) {
  return (
    <div className="notice notice-error" role="alert" data-component="TechnicalErrorPanel">
      <strong>{headline ?? failure?.message ?? fallback}</strong>
      {failure && (
        <details>
          <summary>Дополнительно</summary>
          {failure.serverError?.code && <p>Код: {failure.serverError.code}</p>}
          <p>Сообщение: {failure.message}</p>
          <p>ID запроса: {failure.requestId}</p>
        </details>
      )}
    </div>
  );
}

class ErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { failed: false };
  }

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true };
  }

  render() {
    if (this.state.failed) {
      return (
        <main className="fatal" data-component="ErrorBoundary">
          <h1>Интерфейс не отрисовался</h1>
          <p>Серверное задание не изменено. Обновите страницу, чтобы повторить только отображение.</p>
          <button type="button" onClick={() => window.location.reload()}>Обновить страницу</button>
        </main>
      );
    }
    return this.props.children;
  }
}

export function App() {
  return (
    <ErrorBoundary>
      <ExpertWorkspace />
    </ErrorBoundary>
  );
}

function ExpertWorkspace() {
  const [view, setView] = useState<View>("workspace");
  const [system, setSystem] = useState<Loadable<SystemStatusResponse["data"]>>({ phase: "idle" });
  const [session, setSession] = useState<Loadable<SessionInfo>>({ phase: "idle" });
  const [toast, setToast] = useState<string | null>(null);
  const [selection, setSelection] = useState<WorkspaceSelection>({ document: null, version: null, jobId: "" });
  const [historicalRunSelection, setHistoricalRunSelection] = useState<HistoricalRunSelection | null>(null);
  const workspaceTrigger = useRef<HTMLElement | null>(null);

  function openWorkspace(nextView: View) {
    if (document.activeElement instanceof HTMLElement) workspaceTrigger.current = document.activeElement;
    setView(nextView);
  }


  async function refreshSystem(signal?: AbortSignal) {
    setSystem({ phase: "loading" });
    try {
      setSystem({ phase: "loaded", response: await getSystemStatus(signal) });
    } catch (error) {
      if (!signal?.aborted) setSystem({ phase: "error", failure: asApiFailure(error) });
    }
  }

  async function refreshSession(signal?: AbortSignal) {
    setSession({ phase: "loading" });
    try {
      setSession({ phase: "loaded", response: await getAuthSession(signal) });
    } catch (error) {
      if (!signal?.aborted) setSession({ phase: "error", failure: asApiFailure(error) });
    }
  }

  useEffect(() => {
    const controller = new AbortController();
    void refreshSystem(controller.signal);
    void refreshSession(controller.signal);
    return () => controller.abort();
  }, []);

  const sessionData = session.phase === "loaded" ? session.response.data : null;

  useEffect(() => {
    setHistoricalRunSelection(null);
  }, [sessionData?.principal_id]);

  function openHistoricalRun(selection: HistoricalRunSelection) {
    setHistoricalRunSelection(selection);
    setView("workspace");
  }

  return (
    <div className="app-shell" data-component="AppShell">
      <a className="skip-link" href="#workspace-main">Перейти к рабочей области</a>
      <header className="chat-topbar">
        <h1>Цифровой эксперт<span>по вашим документам</span></h1>
        <div className="header-actions">
          <Button type="button" variant="ghost" onClick={() => openWorkspace("documents")}>Документы</Button>
          <Button type="button" variant="ghost" className="header-history" aria-label="История" onClick={() => openWorkspace("history")}>История</Button>
          <ThemeToggle />
          <Button type="button" variant="ghost" aria-label={sessionData ? "Дополнительно" : "Войти"} onClick={() => openWorkspace("system")}>{sessionData ? "···" : "Войти"}</Button>
        </div>
      </header>
      <main className="content chat-main" id="workspace-main" tabIndex={-1}>
        {toast && <div className="toast chat-toast" data-component="ToastCoordinator" role="status"><span>{toast}</span><button type="button" aria-label="Закрыть уведомление" onClick={() => setToast(null)}>×</button></div>}
        {(system.phase === "error" || (system.phase === "loaded" && system.response.data.status !== "ready")) && <ConnectionBanner system={system} />}
        <QuestionWorkspace session={sessionData} historicalRunSelection={historicalRunSelection} onHistoricalRunConsumed={() => setHistoricalRunSelection(null)} onToast={setToast} onOpenDocuments={() => openWorkspace("documents")} onLogin={() => openWorkspace("system")} />
      </main>
      <Dialog open={view !== "workspace"} onOpenChange={(open) => { if (!open) setView("workspace"); }}>
        <DialogContent className={`workspace-modal ${view === "system" ? "settings-modal" : ""}`} aria-describedby={undefined} onCloseAutoFocus={(event) => {
          if (workspaceTrigger.current?.isConnected) { event.preventDefault(); workspaceTrigger.current.focus(); }
        }}>
          <DialogHeader className="modal-heading"><DialogTitle>{viewLabels[view]}</DialogTitle><DialogClose asChild><Button type="button" variant="ghost" aria-label="Закрыть окно">×</Button></DialogClose></DialogHeader>
          {view === "documents" && <DocumentsWorkspace session={sessionData} selection={selection} onSelection={setSelection} onOpenProcessing={() => setView("ingestion")} onToast={setToast} />}
          {view === "ingestion" && <IngestionWorkspace session={sessionData} selection={selection} onSelection={setSelection} onToast={setToast} />}
          {view === "history" && <HistoryWorkspace session={sessionData} onOpenRun={openHistoricalRun} onToast={setToast} />}
          {view === "debug" && <DebugWorkspace session={sessionData} />}
          {view === "system" && <>
            <SessionPanel state={session} onRefresh={() => void refreshSession()} onToast={setToast} />
            <div className="settings-actions"><Button type="button" variant="secondary" onClick={() => setView("ingestion")}>Обработка документов</Button><Button type="button" variant="secondary" onClick={() => setView("debug")}>Диагностика</Button></div>
            <details><summary>Дополнительно: состояние системы</summary><SystemStatusIndicator state={system} onRefresh={() => void refreshSystem()} /><SystemWorkspace state={system} onRefresh={() => void refreshSystem()} /></details>
          </>}
        </DialogContent>
      </Dialog>
    </div>
  );
}

function SystemStatusIndicator({ state, onRefresh }: { state: Loadable<SystemStatusResponse["data"]>; onRefresh: () => void }) {
  const stale = state.phase === "error";
  return (
    <section className="sidebar-card" data-component="SystemStatusIndicator" aria-live="polite">
      <div className="inline-heading">
        <h3>Сервисы</h3>
        <button type="button" className="small-button" onClick={onRefresh} disabled={state.phase === "loading"}>Обновить</button>
      </div>
      {state.phase === "loading" && <p className="muted">Проверяем API…</p>}
      {state.phase === "error" && <p className="status-text unavailable">Недоступно или устарело</p>}
      {state.phase === "loaded" && <p className={`status-text ${state.response.data.status}`}>{statusLabels[state.response.data.status]}</p>}
      {stale && <p className="muted">Сбой трассировки не блокирует вопрос, но API сейчас не подтвердил готовность.</p>}
    </section>
  );
}

function SessionPanel({ state, onRefresh, onToast }: { state: Loadable<SessionInfo>; onRefresh: () => void; onToast: (message: string) => void }) {
  const [accessKey, setAccessKey] = useState("");
  const busy = state.phase === "loading";

  async function submit(event: FormEvent) {
    event.preventDefault();
    try {
      await createAuthSession(accessKey);
      setAccessKey("");
      onToast("Сессия создана");
      onRefresh();
    } catch (error) {
      onToast(asApiFailure(error)?.message ?? "Не удалось войти");
    }
  }

  async function logout() {
    try {
      await deleteAuthSession();
      onToast("Сессия завершена");
      onRefresh();
    } catch (error) {
      onToast(asApiFailure(error)?.message ?? "Не удалось выйти");
    }
  }

  if (state.phase === "loaded") {
    return (
      <section className="session-card" aria-label="Сессия">
        <details className="session-details">
          <summary>Моя сессия</summary>
          <p>{state.response.data.role} · {state.response.data.principal_id}</p>
          <p>До {formatDateTime(state.response.data.expires_at)}</p>
        </details>
        <button type="button" className="small-button" onClick={() => void logout()}>Выйти</button>
      </section>
    );
  }

  return (
    <form className="session-card" onSubmit={(event) => void submit(event)} aria-label="Вход">
      <label>
        <span>Ключ доступа</span>
        <input type="password" minLength={32} maxLength={256} value={accessKey} onChange={(event) => setAccessKey(event.currentTarget.value)} placeholder="минимум 32 символа" />
      </label>
      <button type="submit" className="small-button" disabled={busy || accessKey.length < 32}>Войти</button>
      {state.phase === "error" && <span className="form-error">{state.failure?.message ?? "Сессия не подтверждена"}</span>}
    </form>
  );
}

function ConnectionBanner({ system }: { system: Loadable<SystemStatusResponse["data"]> }) {
  const text = system.phase === "loaded" && system.response.data.status === "ready"
    ? "Соединение активно. История событий может восстановиться отдельно от результата."
    : "Если соединение прервётся, итог запуска не меняется; перечитайте запуск после восстановления.";
  return <div className="connection" data-component="ConnectionBanner">{text}</div>;
}

function QuestionWorkspace({
  session,
  historicalRunSelection,
  onHistoricalRunConsumed,
  onOpenDocuments,
  onLogin,
  onToast,
}: {
  session: SessionInfo | null;
  historicalRunSelection: HistoricalRunSelection | null;
  onHistoricalRunConsumed: () => void;
  onOpenDocuments: () => void;
  onLogin: () => void;
  onToast: (message: string) => void;
}) {
  const [question, setQuestion] = useState("");
  const [pastAnswers, setPastAnswers] = useState<Array<{ run: PublicRun; question: LoadedRunQuestion | null }>>([]);
  const [sourceRunId, setSourceRunId] = useState("");
  const [questionOptionsOpen, setQuestionOptionsOpen] = useState(false);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const [loadedRunQuestion, setLoadedRunQuestion] = useState<LoadedRunQuestion | null>(null);
  const [debugCapture, setDebugCapture] = useState(false);
  const [run, setRun] = useState<Loadable<PublicRun>>({ phase: "idle" });
  const [lookupRunId, setLookupRunId] = useState("");
  const [source, setSource] = useState<Loadable<PublicEvidence>>({ phase: "idle" });
  const [evidenceId, setEvidenceId] = useState("");
  const [runEvents, setRunEvents] = useState<RunEventState>({ connection: "idle", lastSequence: 0, events: [] });
  const [debugPolicy, setDebugPolicy] = useState<Loadable<DebugCapturePolicy>>({ phase: "idle" });
  const runRequestSeq = useRef(0);
  const runAbort = useRef<AbortController | null>(null);
  const displayedRunIdRef = useRef<string | null>(null);
  const principalIdRef = useRef<string | null>(session?.principal_id ?? null);
  const sourceRequestSeq = useRef(0);
  const sourceAbort = useRef<AbortController | null>(null);
  const sourceTrigger = useRef<HTMLElement | null>(null);
  const displayedRunId = run.phase === "loaded" ? run.response.data.run_id : null;
  const canRequestDebugPolicy = session?.role === "operator" || session?.role === "admin";
  const canDebug = canRequestDebugPolicy && debugPolicy.phase === "loaded" && debugPolicy.response.data.debug_capture_allowed;

  useEffect(() => {
    displayedRunIdRef.current = displayedRunId;
    principalIdRef.current = session?.principal_id ?? null;
  });

  function resetSourceState() {
    sourceAbort.current?.abort();
    sourceRequestSeq.current += 1;
    setSource({ phase: "idle" });
    setEvidenceId("");
    setSourceRunId("");
  }

  function beginRunRequest() {
    resetSourceState();
    runAbort.current?.abort();
    const controller = new AbortController();
    runAbort.current = controller;
    const requestSeq = runRequestSeq.current + 1;
    runRequestSeq.current = requestSeq;
    setRun({ phase: "loading" });
    return { controller, requestSeq };
  }

  function isCurrentRunRequest(requestSeq: number, controller: AbortController) {
    return runRequestSeq.current === requestSeq && !controller.signal.aborted;
  }

  useEffect(() => {
    resetSourceState();
  }, [session?.principal_id, displayedRunId]);

  useEffect(() => {
    runAbort.current?.abort();
    sourceAbort.current?.abort();
    runRequestSeq.current += 1;
    sourceRequestSeq.current += 1;
    setRun({ phase: "idle" });
    setLookupRunId("");
    setLoadedRunQuestion(null);
    setPastAnswers([]);
    setQuestion("");
    setRunEvents({ connection: "idle", lastSequence: 0, events: [] });
    setSource({ phase: "idle" });
    setEvidenceId("");
    return () => {
      runAbort.current?.abort();
      sourceAbort.current?.abort();
      runRequestSeq.current += 1;
      sourceRequestSeq.current += 1;
    };
  }, [session?.principal_id]);

  useEffect(() => {
    if (!canRequestDebugPolicy) {
      setDebugPolicy({ phase: "idle" });
      setDebugCapture(false);
      return undefined;
    }
    const controller = new AbortController();
    setDebugPolicy({ phase: "loading" });
    void getDebugCapturePolicy(controller.signal)
      .then((response) => {
        if (!controller.signal.aborted) {
          setDebugPolicy({ phase: "loaded", response });
          if (!response.data.debug_capture_allowed) setDebugCapture(false);
        }
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) {
          setDebugPolicy({ phase: "error", failure: asApiFailure(error) });
          setDebugCapture(false);
        }
      });
    return () => controller.abort();
  }, [canRequestDebugPolicy]);

  async function submitQuestion(event: FormEvent) {
    event.preventDefault();
    const submittedQuestion = question.trim();
    if (!submittedQuestion || !session || run.phase === "loading" || (run.phase === "loaded" && !terminalRunStatuses.has(run.response.data.status))) return;
    if (run.phase === "loaded") {
      const completedRun = run.response.data;
      setPastAnswers((items) => [...items.filter((item) => item.run.run_id !== completedRun.run_id), { run: completedRun, question: loadedRunQuestion?.runId === completedRun.run_id ? loadedRunQuestion : null }].slice(-20));
    }
    const { controller, requestSeq } = beginRunRequest();
    try {
      const accepted = await createRun(submittedQuestion, debugCapture && canDebug, newIdempotencyKey("run"), controller.signal);
      if (!isCurrentRunRequest(requestSeq, controller)) return;
      setLookupRunId(accepted.data.run_id);
      setRunEvents({ connection: "idle", lastSequence: accepted.data.last_sequence, events: [] });
      const current = await getRun(accepted.data.run_id, controller.signal);
      if (!isCurrentRunRequest(requestSeq, controller)) return;
      if (!publicRunMatches(current.data, accepted.data.run_id)) {
        setRun({ phase: "error", failure: new ApiFailure("invalid_response", "Сервер вернул другой запуск. Задайте вопрос ещё раз.", "local", 200, null) });
        return;
      }
      setRunEvents({ connection: "idle", lastSequence: current.data.last_sequence, events: [] });
      setRun({ phase: "loaded", response: current });
      setLoadedRunQuestion({ runId: current.data.run_id, label: "Вопрос", text: submittedQuestion });
      setQuestion("");
    } catch (error) {
      if (!controller.signal.aborted && runRequestSeq.current === requestSeq) {
        setLoadedRunQuestion(null);
        setRun({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function reloadRun(targetRunId: string, options: { resetEvents: boolean; updateLookup: boolean; questionLabel?: LoadedRunQuestion | null }) {
    const requestedRunId = targetRunId.trim();
    if (!requestedRunId) return;
    const { controller, requestSeq } = beginRunRequest();
    try {
      const response = await getRun(requestedRunId, controller.signal);
      if (!isCurrentRunRequest(requestSeq, controller)) return;
      if (!publicRunMatches(response.data, requestedRunId)) {
        setLoadedRunQuestion(null);
        setRun({ phase: "error", failure: new ApiFailure("invalid_response", "Сервер вернул другой запуск. Откройте запуск ещё раз.", "local", 200, null) });
        return;
      }
      if (options.updateLookup) setLookupRunId(response.data.run_id);
      setRunEvents((current) => options.resetEvents
        ? { connection: "idle", lastSequence: runEventCursorAfterReload(current.lastSequence, response.data.last_sequence, true), events: [] }
        : { ...current, lastSequence: runEventCursorAfterReload(current.lastSequence, response.data.last_sequence, false) });
      setRun({ phase: "loaded", response });
      if (options.questionLabel !== undefined) {
        setLoadedRunQuestion(options.questionLabel && options.questionLabel.runId === response.data.run_id ? options.questionLabel : null);
      }
    } catch (error) {
      if (!controller.signal.aborted && runRequestSeq.current === requestSeq) {
        setLoadedRunQuestion(null);
        setRun({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function stopRun() {
    const activeRunId = displayedRunId;
    if (!activeRunId) return;
    const action = { principalId: session?.principal_id ?? null, runId: activeRunId, requestSeq: runRequestSeq.current };
    try {
      await cancelRun(activeRunId);
      if (!runActionStillTargetsDisplayedRun(action, {
        principalId: principalIdRef.current,
        displayedRunId: displayedRunIdRef.current,
        requestSeq: runRequestSeq.current,
      })) return;
      onToast("Остановка принята сервером");
      await reloadRun(activeRunId, { resetEvents: false, updateLookup: false });
    } catch (error) {
      onToast(asApiFailure(error)?.message ?? "Не удалось остановить запуск");
    }
  }

  async function openSource(nextEvidenceId: string, targetRunId = displayedRunId) {
    const activeRunId = targetRunId;
    if (!activeRunId || !nextEvidenceId.trim()) return;
    if (document.activeElement instanceof HTMLElement) sourceTrigger.current = document.activeElement;
    const request = { runId: activeRunId, evidenceId: nextEvidenceId.trim() };
    const requestSeq = sourceRequestSeq.current + 1;
    sourceRequestSeq.current = requestSeq;
    sourceAbort.current?.abort();
    const controller = new AbortController();
    sourceAbort.current = controller;
    setEvidenceId(request.evidenceId);
    setSourceRunId(request.runId);
    setSource({ phase: "loading" });
    try {
      const response = await getSourceEvidence(request.runId, request.evidenceId, controller.signal);
      if (sourceRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      if (!publicEvidenceMatches(response.data, request)) {
        setSource({ phase: "error", failure: new ApiFailure("invalid_response", "Сервер вернул источник для другого запуска. Обновите ответ и откройте источник ещё раз.", "local", 200, null) });
        return;
      }
      setSource({ phase: "loaded", response });
    } catch (error) {
      if (!controller.signal.aborted && sourceRequestSeq.current === requestSeq) {
        setSource({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  useEffect(() => {
    if (!historicalRunSelection || !session) return;
    const questionLabel = {
      runId: historicalRunSelection.runId,
      label: "Вопрос из истории",
      text: historicalRunSelection.questionExcerpt,
    };
    onHistoricalRunConsumed();
    void reloadRun(historicalRunSelection.runId, { resetEvents: true, updateLookup: true, questionLabel });
  }, [historicalRunSelection, session?.principal_id]);

  useEffect(() => {
    const activeRunId = displayedRunId;
    if (!activeRunId || run.phase !== "loaded" || terminalRunStatuses.has(run.response.data.status)) {
      return undefined;
    }
    let closed = false;
    const subscription = subscribeRunEvents(activeRunId, runEvents.lastSequence, (update) => {
      if (closed) return;
      if (update.kind === "connection") {
        setRunEvents((current) => ({ ...current, connection: update.connection }));
        return;
      }
      setRunEvents((current) => reduceRunEvent(current, update.event));
      if (isTerminalRunEvent(update.event)) {
        void reloadRun(activeRunId, { resetEvents: false, updateLookup: false });
      }
    });
    return () => {
      closed = true;
      subscription.close();
    };
  }, [displayedRunId, run.phase, run.phase === "loaded" ? run.response.data.status : null]);

  const runBusy = run.phase === "loading" || (run.phase === "loaded" && !terminalRunStatuses.has(run.response.data.status));

  return (
    <section className="chat-workspace" aria-label="Чат по документам">
      <div className="chat-transcript">
        {pastAnswers.filter((item) => item.run.run_id !== displayedRunId).map((item) => (
          <div className="chat-exchange" key={item.run.run_id}>
            {item.question && <RunQuestionExcerpt question={item.question} />}
            <RunView run={item.run} events={{ connection: "idle", lastSequence: item.run.last_sequence, events: [] }} onCancel={() => undefined} onOpenSource={(id) => void openSource(id, item.run.run_id)} />
          </div>
        ))}
        {run.phase === "error" && <ErrorNotice failure={run.failure} fallback="Не удалось отправить вопрос. Попробуйте ещё раз." />}
        {run.phase === "loading" && <p className="chat-loading" role="status">Готовим ответ…</p>}
        {run.phase === "loaded" && loadedRunQuestion?.runId === run.response.data.run_id && <RunQuestionExcerpt question={loadedRunQuestion} />}
        {run.phase === "loaded" && <RunView run={run.response.data} events={runEvents} onCancel={() => void stopRun()} onOpenSource={(id) => void openSource(id)} />}
        {run.phase === "idle" && (
          <div className="chat-welcome">
            <div className="welcome-mark" aria-hidden="true">✳</div>
            <h2>Разберёмся в документах</h2>
            <p>Задайте вопрос — найдём ответ и покажем,<br className="desktop-break" /> на какие фрагменты он опирается.</p>
            <div className="welcome-actions">
              <Button type="button" variant="secondary" onClick={onOpenDocuments}>Загрузить PDF</Button>
              {!session && <Button type="button" onClick={onLogin}>Войти</Button>}
            </div>
          </div>
        )}
      </div>
      <div className="chat-composer-wrap">
        <form id="question-composer-form" className="chat-composer" data-component="QueryComposer" onSubmit={(event) => void submitQuestion(event)}>
          <textarea ref={composerRef} value={question} maxLength={4000} rows={2}
            onChange={(event) => setQuestion(event.currentTarget.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
                event.preventDefault();
                event.currentTarget.form?.requestSubmit();
              }
            }}
            placeholder={session ? "Что вы хотите узнать из документов?" : "Войдите, чтобы задать вопрос"}
            aria-label="Текст вопроса" disabled={!session} />
          <div className="chat-composer-actions">
            <div className="composer-tools">
              <button type="button" className="icon-button" aria-label="Документы и загрузка PDF" title="Документы и загрузка PDF" onClick={onOpenDocuments}>＋</button>
              <button type="button" className="quiet-button" onClick={() => setQuestionOptionsOpen(true)}>Параметры</button>
            </div>
            <div className="composer-send">
              {question.length > 3500 && <span className="muted">{question.length}/4000</span>}
              <button type="submit" disabled={!session || !question.trim() || runBusy} aria-label="Отправить вопрос">Отправить <span aria-hidden="true">↑</span></button>
            </div>
          </div>
        </form>
        <p className="composer-hint">Каждый вопрос рассматривается отдельно. Проверяйте важные выводы по источникам.</p>
      </div>
      <Dialog open={questionOptionsOpen} onOpenChange={setQuestionOptionsOpen}>
        <DialogContent aria-describedby={undefined}>
          <div className="modal-heading"><DialogTitle>Параметры вопроса</DialogTitle><DialogClose asChild><button type="button" className="icon-button" aria-label="Закрыть параметры">×</button></DialogClose></div>
          <label className="check" data-component="DebugCaptureToggle">
            <input type="checkbox" checked={debugCapture} disabled={!canDebug} onChange={(event) => setDebugCapture(event.currentTarget.checked)} />
            <span>Сохранить расширенную диагностику</span>
          </label>
          {debugPolicy.phase === "loaded" && debugPolicy.response.data.debug_capture_allowed && <p className="muted">Диагностика следующего вопроса хранится до {debugPolicy.response.data.debug_capture_ttl_hours} ч.</p>}
          {!canDebug && <p className="muted">Расширенная диагностика доступна оператору, если разрешена на сервере.</p>}
          <div className="lookup-row">
            <label><span>ID запуска</span><input value={lookupRunId} onChange={(event) => setLookupRunId(event.currentTarget.value)} placeholder="UUID запуска" /></label>
            <button type="button" disabled={!session || !lookupRunId.trim() || runBusy} onClick={() => {
              setQuestionOptionsOpen(false);
              void reloadRun(lookupRunId, { resetEvents: true, updateLookup: true, questionLabel: null });
            }}>Открыть запуск</button>
          </div>
        </DialogContent>
      </Dialog>
      <Dialog open={source.phase !== "idle"} onOpenChange={(open) => { if (!open) resetSourceState(); }}>
        <DialogContent className="source-modal" aria-describedby={undefined} onCloseAutoFocus={(event) => {
          if (sourceTrigger.current?.isConnected) { event.preventDefault(); sourceTrigger.current.focus(); }
        }}>
          <div className="modal-heading"><DialogTitle>Источник</DialogTitle><DialogClose asChild><button type="button" className="icon-button" aria-label="Закрыть источник">×</button></DialogClose></div>
          <SourceDrawer state={source} runId={sourceRunId} evidenceId={evidenceId} onEvidenceId={setEvidenceId} onOpen={() => void openSource(evidenceId, sourceRunId)} />
        </DialogContent>
      </Dialog>
    </section>
  );
}

function RunQuestionExcerpt({ question }: { question: LoadedRunQuestion }) {
  return (
    <article className="chat-user-message" aria-label={question.label}>
      <p>{question.text}</p>
    </article>
  );
}

function RunView({ run, events, onCancel, onOpenSource }: { run: PublicRun; events: RunEventState; onCancel: () => void; onOpenSource: (evidenceId: string) => void }) {
  const answer = isFinalAnswer(run.result) ? run.result : null;
  const refusal = isRefusal(run.result) ? run.result : null;
  const progress = liveRunProgress(run, events);
  return (
    <article className="run-card chat-assistant-message">
      {!terminalRunStatuses.has(run.status) && <div className="run-header">
        <div data-component="RunProgressPanel">
          <h3>{runStatusLabels[run.status]}</h3>
          {!(["completed", "failed", "refused", "cancelled"].includes(run.status)) && <p>{progress.current_stage ? runStageLabels[progress.current_stage] : "Готовим вопрос"}</p>}
        </div>
        {!terminalRunStatuses.has(run.status) && (
          <button type="button" data-component="CancelRunButton" onClick={onCancel}>
            {run.status === "cancelling" || run.cancel_requested ? "Останавливается" : "Остановить"}
          </button>
        )}
      </div>}
      {answer && <VerifiedAnswer answer={answer} onOpenSource={onOpenSource} />}
      {refusal && <RefusalPanel refusal={refusal} />}
      {run.error && <ErrorNotice failure={new ApiFailure("http", run.error.message, run.error.request_id, null, run.error)} fallback="Запуск завершился ошибкой" headline="Не удалось подготовить ответ. Подробности сохранены ниже." />}
      <DetailsDialog title="Подробности ответа" trigger="Подробнее">
        <p className="muted">ID: {run.run_id} · попытка {progress.stage_attempt}</p>
        <SnapshotBadge run={run} />
        <RunEventPanel events={events} />
        <StageTimeline current={progress.current_stage} status={run.status} />
        {answer && <ValidationSummary answer={answer} />}
        {refusal && <p className="muted">Код: {refusal.code}</p>}
      </DetailsDialog>
    </article>
  );
}

function DetailsDialog({ title, trigger, children }: { title: string; trigger: string; children: ReactNode }) {
  return <Dialog>
    <DialogTrigger asChild><button type="button" className="quiet-button">{trigger}</button></DialogTrigger>
    <DialogContent aria-describedby={undefined}>
      <div className="modal-heading"><DialogTitle>{title}</DialogTitle><DialogClose asChild><button type="button" className="icon-button" aria-label={`Закрыть: ${title}`}>×</button></DialogClose></div>
      {children}
    </DialogContent>
  </Dialog>;
}

function SnapshotBadge({ run }: { run: PublicRun }) {
  return (
    <div className="snapshot" data-component="SnapshotBadge">
      Снимок базы: {run.snapshot ? `${formatDateTime(run.snapshot.captured_at)}, версий: ${run.snapshot.version_count}` : "ещё не зафиксирован"}
    </div>
  );
}

function RunEventPanel({ events }: { events: RunEventState }) {
  return (
    <section className={`run-events ${events.connection}`} aria-live="polite">
      <div className="inline-heading">
        <strong>{eventStatusText(events)}</strong>
        <span>событие {events.lastSequence || "—"}</span>
      </div>
      {events.events.length > 0 && (
        <ol>
          {events.events.slice(-5).map((event) => (
            <li key={event.event_id}>
              <span>{event.type}</span>
              <em>{event.stage ? runStageLabels[event.stage] : "запуск"}</em>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

function StageTimeline({ current, status }: { current: PublicRun["current_stage"]; status: PublicRun["status"] }) {
  const currentIndex = current ? stageOrder.indexOf(current) : -1;
  return (
    <ol className="timeline" data-component="StageTimeline" aria-label="Стадии ответа">
      {stageOrder.map((stage, index) => (
        <li key={stage} className={index <= currentIndex ? "done" : "pending"}>
          <span>{runStageLabels[stage]}</span>
          {stage === current && <em>{runStatusLabels[status]}</em>}
        </li>
      ))}
    </ol>
  );
}

type CitationFragmentView = {
  citationId: string;
  evidenceId: string;
  buttonLabel: string;
  accessibleLabel: string;
  pageLabel: string;
  structuralPath: string;
};

type CitationSourceGroup = {
  groupId: string;
  documentTitle: string;
  versionLabel: string | null | undefined;
  sourceUrl: string;
  pagesLabel: string;
  fragments: CitationFragmentView[];
};

function citationGroupKey(citation: CitationDTO): string {
  return JSON.stringify([citation.source_url, citation.document_title, citation.version_label ?? null]);
}

function citationPagesLabel(citation: Pick<CitationDTO, "pdf_pages" | "printed_page_labels">): string {
  const printed = citation.printed_page_labels.filter((label) => label.trim());
  if (printed.length > 0) return `стр. ${printed.join(", ")}`;
  return `стр. ${citation.pdf_pages.join(", ")}`;
}

// eslint-disable-next-line react-refresh/only-export-components -- pure citation presenter helper covered by frontend tests.
export function citationButtonLabels(citations: CitationDTO[]): Map<string, CitationFragmentView> {
  const pageCounts = new Map<string, number>();
  for (const citation of citations) {
    const pageKey = `${citationGroupKey(citation)}:${citationPagesLabel(citation)}`;
    pageCounts.set(pageKey, (pageCounts.get(pageKey) ?? 0) + 1);
  }

  const perGroupPage = new Map<string, number>();
  const result = new Map<string, CitationFragmentView>();
  citations.forEach((citation) => {
    const pageLabel = citationPagesLabel(citation);
    const pageKey = `${citationGroupKey(citation)}:${pageLabel}`;
    const duplicateIndex = (perGroupPage.get(pageKey) ?? 0) + 1;
    perGroupPage.set(pageKey, duplicateIndex);
    const hasMultipleFragmentsOnPage = (pageCounts.get(pageKey) ?? 0) > 1;
    const buttonLabel = hasMultipleFragmentsOnPage ? `Фрагмент ${duplicateIndex} · ${pageLabel}` : `Источник · ${pageLabel}`;
    const structuralPath = citation.structural_path.join(" → ");
    result.set(citation.citation_id, {
      citationId: citation.citation_id,
      evidenceId: citation.evidence_id,
      buttonLabel,
      pageLabel,
      structuralPath,
      accessibleLabel: hasMultipleFragmentsOnPage
        ? `Открыть фрагмент ${duplicateIndex}: ${citation.document_title}, ${pageLabel}`
        : `Открыть источник: ${citation.document_title}, ${pageLabel}`,
    });
  });
  return result;
}

// eslint-disable-next-line react-refresh/only-export-components -- pure citation presenter helper covered by frontend tests.
export function citationSourceGroups(citations: CitationDTO[]): CitationSourceGroup[] {
  const labels = citationButtonLabels(citations);
  const groups = new Map<string, CitationSourceGroup>();
  for (const citation of citations) {
    const groupId = citationGroupKey(citation);
    const fragment = labels.get(citation.citation_id);
    if (!fragment) continue;
    const existing = groups.get(groupId);
    if (existing) {
      existing.fragments.push(fragment);
      existing.pagesLabel = [...new Set(existing.fragments.map((item) => item.pageLabel))].join(", ");
      continue;
    }
    groups.set(groupId, {
      groupId,
      documentTitle: citation.document_title,
      versionLabel: citation.version_label,
      sourceUrl: citation.source_url,
      pagesLabel: fragment.pageLabel,
      fragments: [fragment],
    });
  }
  return [...groups.values()];
}

function VerifiedAnswer({ answer, onOpenSource }: { answer: FinalAnswer; onOpenSource: (evidenceId: string) => void }) {
  const citationLabels = citationButtonLabels(answer.citations);
  return (
    <section data-component="VerifiedAnswer" className="answer-card">
      <p className="answer-author">Цифровой эксперт</p>
      {answer.claims.map((claim) => (
        <section key={claim.claim_id} className="claim" data-component="ClaimParagraph">
          <p>{claim.text}</p>
          <div className="citation-row">
            {claim.citation_ids.map((claimCitationId) => {
              const fragment = citationLabels.get(claimCitationId);
              return <CitationBadge key={claimCitationId} claimCitationId={claimCitationId} fragment={fragment} onOpenEvidence={fragment ? () => onOpenSource(fragment.evidenceId) : undefined} />;
            })}
          </div>
        </section>
      ))}
      <DetailsDialog title="Источники ответа" trigger={`Все источники · ${answer.citations.length}`}>
        <SourcesList citations={answer.citations} onOpenSource={onOpenSource} />
      </DetailsDialog>
    </section>
  );
}

function CitationBadge({ claimCitationId, fragment, onOpenEvidence }: { claimCitationId: string; fragment: CitationFragmentView | undefined; onOpenEvidence: (() => void) | undefined }) {
  const label = fragment?.accessibleLabel ?? `цитата ${claimCitationId} без связанного источника`;
  return <button type="button" className="citation" data-component="CitationBadge" aria-label={label} onClick={onOpenEvidence} disabled={!fragment || !onOpenEvidence}>{fragment?.buttonLabel ?? "Источник"}</button>;
}

function SourcesList({ citations, onOpenSource }: { citations: CitationDTO[]; onOpenSource: (evidenceId: string) => void }) {
  const groups = citationSourceGroups(citations);
  return (
    <section data-component="SourcesList">
      <h4>Источники</h4>
      <ul className="source-list">
        {groups.map((group) => (
          <li key={group.groupId} className="source-group">
            <div>
              <strong>{group.documentTitle}</strong>
              {group.versionLabel && <span className="muted"> · {group.versionLabel}</span>}
            </div>
            <span className="muted">Использованы фрагменты: {group.pagesLabel}</span>
            <div className="source-fragments">
              {group.fragments.map((fragment) => (
                <button key={fragment.evidenceId} type="button" className="link-button" onClick={() => onOpenSource(fragment.evidenceId)}>
                  {fragment.buttonLabel}
                </button>
              ))}
            </div>
            <details className="technical-details">
              <summary>Дополнительно: фрагменты источника</summary>
              <ul>
                {group.fragments.map((fragment) => (
                  <li key={fragment.citationId}>
                    <span>{fragment.buttonLabel}</span>
                    <span>{fragment.structuralPath || "структурный путь не указан"}</span>
                  </li>
                ))}
              </ul>
              <p className="muted">URL источника: {group.sourceUrl}</p>
            </details>
          </li>
        ))}
      </ul>
    </section>
  );
}

function ValidationSummary({ answer }: { answer: components["schemas"]["FinalAnswer"] }) {
  return (
    <div className="validation" data-component="ValidationSummary">
      Подтверждено {answer.validation.supported_count} из {answer.validation.claim_count}; исправление: {answer.validation.repair_used ? "использовалось" : "нет"}. Автоматическая проверка подтверждает только доступные источники.
    </div>
  );
}

function RefusalPanel({ refusal }: { refusal: components["schemas"]["RefusalResult"] }) {
  return (
    <div className="notice" data-component="RefusalPanel">
      <strong>Недостаточно оснований для ответа</strong>
      <p>{refusal.text}</p>
      <p>Уточните вопрос или загрузите релевантный документ.</p>
    </div>
  );
}

function SourceDrawer({ state, runId, evidenceId, onEvidenceId, onOpen }: { state: Loadable<PublicEvidence>; runId: string; evidenceId: string; onEvidenceId: (value: string) => void; onOpen: () => void }) {
  return (
    <section className="source-content" data-component="SourceDrawer">
      {state.phase === "loading" && <p className="muted">Запрашиваем источник…</p>}
      {state.phase === "error" && <ErrorNotice failure={state.failure} fallback="Источник недоступен или отозван" />}
      {state.phase === "loaded" && <SourceEvidence evidence={state.response.data} />}
      <details className="technical-details">
        <summary>Открыть фрагмент по ID</summary>
        <label><span>ID источника</span><input value={evidenceId} onChange={(event) => onEvidenceId(event.currentTarget.value)} /></label>
        <button type="button" onClick={onOpen} disabled={!runId.trim() || !evidenceId.trim()}>Открыть источник</button>
      </details>
    </section>
  );
}

function SourceEvidence({ evidence }: { evidence: PublicEvidence }) {
  const pdfPages = sourcePdfPages(evidence.source_spans);
  const pdfSourceUrl = sourceUrlForFirstPdfPage(evidence.source_url, pdfPages);
  return (
    <article className="source-evidence">
      <h4>{evidence.document_title}</h4>
      <blockquote>{evidence.excerpt}</blockquote>
      <div data-component="PdfSourceViewer" className="pdf-viewer source-actions">
        <a className="button-link" href={pdfSourceUrl} target="_blank" rel="noopener noreferrer">Открыть PDF-источник</a>
        <span className="source-pages">{formatSourcePdfPages(pdfPages)}</span>
      </div>
      <details className="technical-details">
        <summary>Дополнительно об источнике</summary>
        <div data-component="CanonicalTreeView" className="tree-view">Структурный путь: {evidence.structural_path.join(" / ") || "не указан"}</div>
        <p className="muted">Фрагментов источника: {evidence.source_spans.length}</p>
      </details>
    </article>
  );
}

function DocumentsWorkspace({
  session,
  selection,
  onSelection,
  onOpenProcessing,
  onToast,
}: {
  session: SessionInfo | null;
  selection: WorkspaceSelection;
  onSelection: Dispatch<SetStateAction<WorkspaceSelection>>;
  onOpenProcessing: () => void;
  onToast: (message: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [detailOpen, setDetailOpen] = useState(false);
  const [archiveOpen, setArchiveOpen] = useState(false);
  const [purgeOpen, setPurgeOpen] = useState(false);
  const [documents, setDocuments] = useState<Loadable<DocumentList>>({ phase: "idle" });
  const [documentCursors, setDocumentCursors] = useState<Array<string | null>>([null]);
  const [documentCursorIndex, setDocumentCursorIndex] = useState(0);
  const [detail, setDetail] = useState<Loadable<DocumentDetail>>({ phase: "idle" });
  const [versionCursors, setVersionCursors] = useState<Array<string | null>>([null]);
  const [versionCursorIndex, setVersionCursorIndex] = useState(0);
  const [version, setVersion] = useState<Loadable<VersionDetail>>({ phase: "idle" });
  const [profile, setProfile] = useState<Loadable<LibraryProfile>>({ phase: "idle" });
  const [archiveReason, setArchiveReason] = useState("");
  const [archiveOperationId, setArchiveOperationId] = useState(() => crypto.randomUUID());
  const [archive, setArchive] = useState<Loadable<DocumentSummary>>({ phase: "idle" });
  const [purgePlan, setPurgePlan] = useState<Loadable<PurgePlan>>({ phase: "idle" });
  const [purgeAccepted, setPurgeAccepted] = useState<Loadable<PurgeAccepted>>({ phase: "idle" });
  const [purgeStatus, setPurgeStatus] = useState<Loadable<PurgeStatus>>({ phase: "idle" });
  const [purgeConfirmation, setPurgeConfirmation] = useState("");
  const [clockMs, setClockMs] = useState(0);
  const [appliedDocumentQuery, setAppliedDocumentQuery] = useState("");
  const [lastDocumentRequest, setLastDocumentRequest] = useState<DocumentListRequest | null>(null);
  const [lastDetailRequest, setLastDetailRequest] = useState<DocumentDetailRequest | null>(null);
  const [lastVersionRequest, setLastVersionRequest] = useState<VersionDetailIdentity | null>(null);
  const documentRequestSeq = useRef(0);
  const detailRequestSeq = useRef(0);
  const versionRequestSeq = useRef(0);
  const documentAbort = useRef<AbortController | null>(null);
  const detailAbort = useRef<AbortController | null>(null);
  const versionAbort = useRef<AbortController | null>(null);
  const selectionDocumentIdRef = useRef<string | null>(selection.document?.document_id ?? null);
  const lastDocumentActionRef = useRef<HTMLButtonElement | null>(null);
  const canArchive = session?.role === "operator" || session?.role === "admin";
  const canPurge = session?.role === "admin";

  useEffect(() => {
    selectionDocumentIdRef.current = selection.document?.document_id ?? null;
  }, [selection.document?.document_id]);

  useEffect(() => {
    if (!session) {
      documentAbort.current?.abort();
      detailAbort.current?.abort();
      versionAbort.current?.abort();
      documentRequestSeq.current += 1;
      detailRequestSeq.current += 1;
      versionRequestSeq.current += 1;
      setDocuments({ phase: "idle" });
      setDocumentCursors([null]);
      setDocumentCursorIndex(0);
      setAppliedDocumentQuery("");
      setLastDocumentRequest(null);
      setLastDetailRequest(null);
      setLastVersionRequest(null);
      setDetail({ phase: "idle" });
      setVersionCursors([null]);
      setVersionCursorIndex(0);
      setVersion({ phase: "idle" });
      setProfile({ phase: "idle" });
      return;
    }
    void loadProfile();
    void loadDocuments({ query, cursor: null, cursors: [null], cursorIndex: 0 });
  }, [session?.principal_id, session?.role]);

  useEffect(() => {
    setClockMs(Date.now());
    const timer = window.setInterval(() => setClockMs(Date.now()), 30000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (purgeAccepted.phase !== "loaded" || !selection.document) return undefined;
    const documentId = purgeAccepted.response.data.document_id;
    const planId = purgeAccepted.response.data.plan_id;
    if (!purgeAcceptedMatches(purgeAccepted.response.data, { documentId: selection.document.document_id, planId })) {
      setPurgeStatus({ phase: "idle" });
      return undefined;
    }
    let stopped = false;
    let terminal = false;
    const controller = new AbortController();

    async function poll() {
      setPurgeStatus((current) => current.phase === "idle" ? { phase: "loading" } : current);
      try {
        const response = await getPurgeStatus(documentId, planId, controller.signal);
        if (stopped) return;
        if (!purgeStatusMatches(response.data, { documentId, planId })) {
          setPurgeStatus({ phase: "error", failure: identityFailure("Сервер вернул статус удаления другого документа. Обновите план удаления.") });
          terminal = true;
          return;
        }
        setPurgeStatus({ phase: "loaded", response });
        terminal = response.data.status === "completed" || response.data.status === "failed";
        if (response.data.status === "completed") {
          await loadDocuments();
          await loadProfile();
        }
      } catch (error) {
        if (!stopped) setPurgeStatus({ phase: "error", failure: asApiFailure(error) });
      }
    }

    void poll();
    const timer = window.setInterval(() => {
      if (terminal) return;
      void poll();
    }, 2500);
    return () => {
      stopped = true;
      controller.abort();
      window.clearInterval(timer);
    };
  }, [purgeAccepted, selection.document?.document_id]);

  async function loadProfile() {
    setProfile({ phase: "loading" });
    try {
      setProfile({ phase: "loaded", response: await getLibraryProfile() });
    } catch (error) {
      setProfile({ phase: "error", failure: asApiFailure(error) });
    }
  }

  function identityFailure(message: string) {
    return new ApiFailure("invalid_response", message, "local", 200, null);
  }

  async function loadDocuments(request: Partial<DocumentListRequest> = {}) {
    const exactRequest = resolveDocumentListRequest(request, appliedDocumentQuery);
    const requestSeq = documentRequestSeq.current + 1;
    documentRequestSeq.current = requestSeq;
    documentAbort.current?.abort();
    const controller = new AbortController();
    documentAbort.current = controller;
    setLastDocumentRequest(exactRequest);
    setDocuments({ phase: "loading" });
    try {
      const response = await listDocuments({ q: exactRequest.query, cursor: exactRequest.cursor }, controller.signal);
      if (documentRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      setDocuments({ phase: "loaded", response });
      setDocumentCursors(exactRequest.cursors);
      setDocumentCursorIndex(exactRequest.cursorIndex);
      setAppliedDocumentQuery(exactRequest.query);
    } catch (error) {
      if (!controller.signal.aborted && documentRequestSeq.current === requestSeq) {
        setDocuments({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function openDocument(documentId: string, options: { versionsCursor?: string | null; versionsCursors?: Array<string | null>; versionsCursorIndex?: number; resetVersion?: boolean } = {}) {
    const request: DocumentDetailRequest = {
      documentId,
      versionsCursor: options.versionsCursor ?? null,
      versionsCursors: options.versionsCursors ?? [options.versionsCursor ?? null],
      versionsCursorIndex: options.versionsCursorIndex ?? (options.versionsCursors ?? [options.versionsCursor ?? null]).length - 1,
      resetVersion: options.resetVersion ?? true,
    };
    const requestSeq = detailRequestSeq.current + 1;
    detailRequestSeq.current = requestSeq;
    detailAbort.current?.abort();
    const controller = new AbortController();
    detailAbort.current = controller;
    setLastDetailRequest(request);
    setDetailOpen(true);
    setDetail({ phase: "loading" });
    if (request.resetVersion) {
      versionAbort.current?.abort();
      versionRequestSeq.current += 1;
      setLastVersionRequest(null);
      setVersion({ phase: "idle" });
      setPurgePlan({ phase: "idle" });
      setPurgeAccepted({ phase: "idle" });
      setPurgeStatus({ phase: "idle" });
      setPurgeConfirmation("");
    }
    try {
      const response = await getDocument(request.documentId, { versionsCursor: request.versionsCursor }, controller.signal);
      if (detailRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      if (!documentDetailMatches(response.data, request.documentId)) {
        setDetail({ phase: "error", failure: identityFailure("Сервер вернул другой документ. Обновите библиотеку и откройте документ ещё раз.") });
        return;
      }
      setDetail({ phase: "loaded", response });
      setVersionCursors(request.versionsCursors);
      setVersionCursorIndex(request.versionsCursorIndex);
      selectionDocumentIdRef.current = response.data.document_id;
      onSelection((current) => ({ ...current, document: response.data, version: request.resetVersion ? null : current.version }));
    } catch (error) {
      if (!controller.signal.aborted && detailRequestSeq.current === requestSeq) {
        setDetail({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function openVersion(versionId: string, documentId: string | null = selection.document?.document_id ?? null) {
    const request: VersionDetailIdentity = { versionId, documentId };
    const requestSeq = versionRequestSeq.current + 1;
    versionRequestSeq.current = requestSeq;
    versionAbort.current?.abort();
    const controller = new AbortController();
    versionAbort.current = controller;
    setLastVersionRequest(request);
    setVersion({ phase: "loading" });
    try {
      const response = await getVersion(request.versionId, controller.signal);
      if (versionRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      if (!versionDetailMatches(response.data, request) || (request.documentId !== null && selectionDocumentIdRef.current !== request.documentId)) {
        setVersion({ phase: "error", failure: identityFailure("Сервер вернул версию другого документа. Откройте документ заново.") });
        return;
      }
      setVersion({ phase: "loaded", response });
      onSelection((current) => ({ ...current, version: response.data }));
    } catch (error) {
      if (!controller.signal.aborted && versionRequestSeq.current === requestSeq) {
        setVersion({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function acceptedUpload(upload: UploadAccepted) {
    onSelection((current) => ({ ...current, jobId: upload.job_id }));
    await openDocument(upload.document_id);
    await openVersion(upload.version_id, upload.document_id);
    await loadProfile();
  }

  async function archiveSelectedDocument() {
    const selectedDocument = selection.document;
    if (!selectedDocument || !archiveReason.trim()) return;
    setArchive({ phase: "loading" });
    try {
      const response = await archiveDocument(selectedDocument.document_id, {
        expected_current_publication_id: currentPublicationId(selectedDocument),
        operation_id: archiveOperationId,
        reason: archiveReason.trim(),
      });
      setArchive({ phase: "loaded", response });
      setArchiveReason("");
      setArchiveOperationId(crypto.randomUUID());
      setDocuments((current) => current.phase === "loaded"
        ? { ...current, response: { ...current.response, data: { ...current.response.data, items: current.response.data.items.map((item) => (item.document_id === response.data.document_id ? response.data : item)) } } }
        : current);
      setDetail((current) => current.phase === "loaded" && current.response.data.document_id === response.data.document_id
        ? { ...current, response: { ...current.response, data: { ...current.response.data, ...response.data } } }
        : current);
      onSelection((current) => current.document?.document_id === response.data.document_id ? { ...current, document: { ...current.document, ...response.data } } : current);
      await loadProfile();
      onToast("Документ архивирован; исторические ссылки и источники сохранены");
    } catch (error) {
      const failure = asApiFailure(error);
      setArchive({ phase: "error", failure });
      if (failure?.httpStatus === 409) {
        await openDocument(selectedDocument.document_id);
        onToast("Состояние документа изменилось на сервере; данные обновлены, проверьте архивирование ещё раз");
      }
    }
  }

  async function loadPurgePlan() {
    const selectedDocument = selection.document;
    if (!selectedDocument) return;
    const documentId = selectedDocument.document_id;
    setPurgeAccepted({ phase: "idle" });
    setPurgeStatus({ phase: "idle" });
    setPurgeConfirmation("");
    setPurgePlan({ phase: "loading" });
    try {
      const response = await createPurgePlan(documentId);
      if (selectionDocumentIdRef.current !== documentId) return;
      if (!purgePlanMatches(response.data, { documentId })) {
        setPurgePlan({ phase: "error", failure: identityFailure("Сервер вернул план удаления другого документа. Обновите библиотеку и повторите.") });
        return;
      }
      setPurgePlan({ phase: "loaded", response });
    } catch (error) {
      if (selectionDocumentIdRef.current === documentId) {
        setPurgePlan({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function submitSelectedPurge() {
    const selectedDocument = selection.document;
    if (!selectedDocument || purgePlan.phase !== "loaded") return;
    const documentId = selectedDocument.document_id;
    const plan = purgePlan.response.data;
    if (!purgePlanMatches(plan, { documentId })) {
      setPurgeAccepted({ phase: "error", failure: identityFailure("План удаления относится к другому документу. Получите новый план.") });
      return;
    }
    setPurgeAccepted({ phase: "loading" });
    try {
      const response = await submitPurgePlan(documentId, {
        plan_id: plan.plan_id,
        plan_version: plan.plan_version,
      });
      if (selectionDocumentIdRef.current !== documentId) return;
      if (!purgeAcceptedMatches(response.data, { documentId, planId: plan.plan_id })) {
        setPurgeAccepted({ phase: "error", failure: identityFailure("Сервер принял удаление для другого документа. Обновите план удаления.") });
        return;
      }
      setPurgeAccepted({ phase: "loaded", response });
      setPurgeStatus({ phase: "loading" });
      setPurgeConfirmation("");
      onToast("Удаление принято в обработку");
    } catch (error) {
      const failure = asApiFailure(error);
      if (selectionDocumentIdRef.current === documentId) {
        setPurgeAccepted({ phase: "error", failure });
      }
      if (failure?.httpStatus === 409 || failure?.httpStatus === 410) {
        await loadPurgePlan();
        onToast("План удаления устарел или конфликтует с текущим состоянием; план обновлён");
      }
    }
  }

  function selectDocumentForAction(document: DocumentSummary) {
    detailAbort.current?.abort();
    versionAbort.current?.abort();
    detailRequestSeq.current += 1;
    versionRequestSeq.current += 1;
    selectionDocumentIdRef.current = document.document_id;
    setDetailOpen(false);
    setLastDetailRequest(null);
    setLastVersionRequest(null);
    setDetail({ phase: "idle" });
    setVersion({ phase: "idle" });
    onSelection((current) => ({ ...current, document, version: null }));
  }

  function focusLastDocumentAction(event: Event) {
    if (!lastDocumentActionRef.current?.isConnected) return;
    event.preventDefault();
    lastDocumentActionRef.current.focus();
  }

  function openArchiveDialog(document: DocumentSummary, trigger: HTMLButtonElement) {
    lastDocumentActionRef.current = trigger;
    selectDocumentForAction(document);
    setArchive({ phase: "idle" });
    setArchiveReason("");
    setArchiveOperationId(crypto.randomUUID());
    setArchiveOpen(true);
  }

  function openPurgeDialog(document: DocumentSummary, trigger: HTMLButtonElement) {
    lastDocumentActionRef.current = trigger;
    selectDocumentForAction(document);
    setPurgePlan({ phase: "idle" });
    setPurgeAccepted({ phase: "idle" });
    setPurgeStatus({ phase: "idle" });
    setPurgeConfirmation("");
    setPurgeOpen(true);
  }

  return (
    <section className="documents-workspace">
      <div className="panel" data-component="DocumentLibraryTable">
        <p className="muted">Документы, по которым эксперт ищет ответы. Откройте документ, чтобы посмотреть его версии.</p>
        <div className="lookup-row">
          <label><span>Поиск</span><input value={query} onChange={(event) => setQuery(event.currentTarget.value)} placeholder="название, номер, орган" /></label>
          <button type="button" onClick={() => void loadDocuments({ query, cursor: null, cursors: [null], cursorIndex: 0 })}>Обновить список</button>
        </div>
        {documents.phase === "loading" && <p className="muted">Читаем библиотеку…</p>}
        {documents.phase === "error" && (
          <>
            <ErrorNotice failure={documents.failure} fallback="Библиотека недоступна" />
            {lastDocumentRequest && <button type="button" onClick={() => void loadDocuments(lastDocumentRequest)}>Повторить список</button>}
          </>
        )}
        {documents.phase === "loaded" && (
          <>
            <DocumentTable
              list={documents.response.data}
              canArchive={canArchive}
              canPurge={canPurge}
              onOpen={(documentId) => void openDocument(documentId)}
              onArchive={openArchiveDialog}
              onPurge={openPurgeDialog}
            />
            <CursorPager
              label="Страницы библиотеки"
              currentIndex={documentCursorIndex}
              hasNext={Boolean(documents.response.data.next_cursor)}
              onPrevious={() => {
                const previous = previousCursorPage({ cursors: documentCursors, index: documentCursorIndex });
                void loadDocuments({ query: appliedDocumentQuery, cursor: currentCursorPage(previous), cursors: previous.cursors, cursorIndex: previous.index });
              }}
              onNext={() => {
                const nextCursor = documents.response.data.next_cursor;
                if (!nextCursor) return;
                const next = nextCursorPage({ cursors: documentCursors, index: documentCursorIndex }, nextCursor);
                void loadDocuments({ query: appliedDocumentQuery, cursor: currentCursorPage(next), cursors: next.cursors, cursorIndex: next.index });
              }}
            />
          </>
        )}
        {documents.phase === "idle" && <p className="muted">Войдите, чтобы открыть список документов.</p>}
        <details className="technical-details"><summary>Состояние библиотеки</summary><KnowledgeBaseSummary profile={profile} onRefresh={() => void loadProfile()} /></details>
        <ArchiveDocumentDialog
          open={archiveOpen}
          onOpenChange={setArchiveOpen}
          onCloseAutoFocus={focusLastDocumentAction}
          disabled={!canArchive || !selection.document || archive.phase === "loading" || Boolean(selection.document?.archived_at)}
          document={selection.document}
          reason={archiveReason}
          state={archive}
          onReason={setArchiveReason}
          onConfirm={() => void archiveSelectedDocument()}
        />
        <SourcePurgeDialog
          open={purgeOpen}
          onOpenChange={setPurgeOpen}
          onCloseAutoFocus={focusLastDocumentAction}
          disabled={!canPurge || !selection.document}
          document={selection.document}
          plan={purgePlan}
          accepted={purgeAccepted}
          status={purgeStatus}
          confirmation={purgeConfirmation}
          clockMs={clockMs}
          onConfirmation={setPurgeConfirmation}
          onPlan={() => void loadPurgePlan()}
          onPurge={() => void submitSelectedPurge()}
        />
      </div>
      <div className="document-tools">
        <button type="button" className="quiet-button" onClick={onOpenProcessing}>Обработка документов</button>
        <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
          <DialogContent className="workspace-modal" aria-describedby={undefined}>
            <div className="modal-heading"><DialogTitle>О документе</DialogTitle><DialogClose asChild><button type="button" className="icon-button" aria-label="Закрыть документ">×</button></DialogClose></div>
        <DocumentDetailPanel
          detail={detail}
          version={version}
          versionCursorIndex={versionCursorIndex}
          onRetryDetail={() => {
            if (lastDetailRequest) void openDocument(lastDetailRequest.documentId, lastDetailRequest);
          }}
          onOpenVersion={(versionId) => {
            const documentId = detail.phase === "loaded" ? detail.response.data.document_id : selection.document?.document_id ?? null;
            void openVersion(versionId, documentId);
          }}
          onRetryVersion={() => {
            if (lastVersionRequest) void openVersion(lastVersionRequest.versionId, lastVersionRequest.documentId);
          }}
          onPreviousVersions={() => {
            if (detail.phase !== "loaded") return;
            const previous = previousCursorPage({ cursors: versionCursors, index: versionCursorIndex });
            void openDocument(detail.response.data.document_id, { versionsCursor: currentCursorPage(previous), versionsCursors: previous.cursors, versionsCursorIndex: previous.index, resetVersion: false });
          }}
          onNextVersions={() => {
            if (detail.phase !== "loaded" || !detail.response.data.next_versions_cursor) return;
            const next = nextCursorPage({ cursors: versionCursors, index: versionCursorIndex }, detail.response.data.next_versions_cursor);
            void openDocument(detail.response.data.document_id, { versionsCursor: currentCursorPage(next), versionsCursors: next.cursors, versionsCursorIndex: next.index, resetVersion: false });
          }}
        />
          </DialogContent>
        </Dialog>
        <Dialog>
          <DialogTrigger asChild><Button type="button">Загрузить PDF</Button></DialogTrigger>
          <DialogContent className="upload-modal" aria-describedby={undefined}>
            <div className="modal-heading"><DialogTitle>Загрузить документ</DialogTitle><DialogClose asChild><button type="button" className="icon-button" aria-label="Закрыть загрузку">×</button></DialogClose></div>
        <UploadDocumentDialog
          session={session}
          documents={documents.phase === "loaded" ? documents.response.data : null}
          selectedDocument={selection.document}
          onSelectDocument={(documentId) => {
            if (documentId) void openDocument(documentId);
          }}
          onAcceptedUpload={(upload) => void acceptedUpload(upload)}
          onOpenProcessing={onOpenProcessing}
          onToast={onToast}
        />
          </DialogContent>
        </Dialog>
      </div>
    </section>
  );
}

function DocumentTable({
  list,
  canArchive,
  canPurge,
  onOpen,
  onArchive,
  onPurge,
}: {
  list: DocumentList;
  canArchive: boolean;
  canPurge: boolean;
  onOpen: (documentId: string) => void;
  onArchive: (document: DocumentSummary, trigger: HTMLButtonElement) => void;
  onPurge: (document: DocumentSummary, trigger: HTMLButtonElement) => void;
}) {
  if (!list.items.length) return <p className="muted">Документы не найдены.</p>;
  return (
    <ul className="document-list" aria-label="Документы в библиотеке">
      {list.items.map((item) => (
        <li key={item.document_id}>
          <div className="document-list-row">
            <div className="document-row-main">
              <button type="button" className="document-title-button" onClick={() => onOpen(item.document_id)}>{item.canonical_title}<span aria-hidden="true">↗</span></button>
              <div className="document-list-meta">
                {item.document_number && <span>№ {item.document_number}</span>}
                <span>{item.security_revoked ? "Доступ отозван" : item.archived_at ? "В архиве" : item.current_publication ? "Опубликован" : "Не опубликован"}</span>
              </div>
            </div>
            <div className="document-row-actions" aria-label={`Действия: ${item.canonical_title}`}>
              <Button type="button" variant="secondary" size="sm" disabled={!canArchive || Boolean(item.archived_at)} onClick={(event) => onArchive(item, event.currentTarget)}>
                {item.archived_at ? "В архиве" : "Архив"}
              </Button>
              <Button type="button" variant="secondary" size="sm" disabled={!canPurge} onClick={(event) => onPurge(item, event.currentTarget)}>
                Удалить
              </Button>
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}

function DocumentDetailPanel({
  detail,
  version,
  versionCursorIndex,
  onRetryDetail,
  onOpenVersion,
  onRetryVersion,
  onPreviousVersions,
  onNextVersions,
}: {
  detail: Loadable<DocumentDetail>;
  version: Loadable<VersionDetail>;
  versionCursorIndex: number;
  onRetryDetail: () => void;
  onOpenVersion: (versionId: string) => void;
  onRetryVersion: () => void;
  onPreviousVersions: () => void;
  onNextVersions: () => void;
}) {
  return (
    <section className="panel" data-component="VersionTimeline">
      <h3>Версии документа</h3>
      {detail.phase === "idle" && <p className="muted">Выберите документ из библиотеки.</p>}
      {detail.phase === "loading" && <p className="muted">Читаем версии…</p>}
      {detail.phase === "error" && <><ErrorNotice failure={detail.failure} fallback="Версии недоступны" /><button type="button" onClick={onRetryDetail}>Повторить версии</button></>}
      {detail.phase === "loaded" && (
        <>
          <p><strong>{detail.response.data.canonical_title}</strong></p>
          <ol className="version-list">
            {detail.response.data.versions.map((item) => (
              <li key={item.version_id}>
                <button type="button" className="link-button" onClick={() => onOpenVersion(item.version_id)}>{item.metadata.version_label ?? `Редакция от ${item.metadata.approved_at}`}</button>
                <span>{versionLegalStatusText(item)}; утверждён {formatDateTime(`${item.metadata.approved_at}T00:00:00Z`)}</span>
              </li>
            ))}
          </ol>
          <CursorPager
            label="Страницы версий"
            currentIndex={versionCursorIndex}
            hasNext={Boolean(detail.response.data.next_versions_cursor)}
            onPrevious={onPreviousVersions}
            onNext={onNextVersions}
          />
        </>
      )}
      {version.phase === "loading" && <p className="muted">Читаем сведения об источнике…</p>}
      {version.phase === "error" && <><ErrorNotice failure={version.failure} fallback="Версия недоступна" /><button type="button" onClick={onRetryVersion}>Повторить версию</button></>}
      {version.phase === "loaded" && <><VersionSourceCard version={version.response.data} /><details className="technical-details"><summary>Дополнительно: структура и качество</summary><VersionStructurePanel version={version.response.data} /></details></>}
    </section>
  );
}

function VersionSourceCard({ version }: { version: VersionDetail }) {
  return (
    <article className="source-evidence">
      <h4>{version.metadata.title}</h4>
      <p>{versionLegalStatusText(version)}; {version.source.original_filename}; {formatBytes(version.source.size_bytes)}</p>
      <a className="button-link" href={version.source.source_url}>Открыть оригинал PDF</a>
      <div className="badges">
        <span>{version.source.page_count ?? "?"} стр.</span>
        <span>{version.generations?.length ?? 0} результатов обработки</span>
      </div>
      <details><summary>Дополнительно об источнике</summary><p>ID версии: {version.version_id}</p><p>Публикация: {version.publication_status}</p><p>SHA-256: {version.source.sha256}</p></details>
    </article>
  );
}

type TreeLoadRequest = {
  parentId: string | null;
  cursor: string | null;
  trail: Array<{ nodeId: string; label: string }>;
  cursors: Array<string | null>;
  cursorIndex: number;
};

type QualityLoadRequest = {
  cursor: string | null;
  cursors: Array<string | null>;
  cursorIndex: number;
};

type TableLoadRequest = {
  nodeId: string;
  label: string;
  cursor: string | null;
  cursors: Array<string | null>;
  cursorIndex: number;
};

function VersionStructurePanel({ version }: { version: VersionDetail }) {
  const [parseGenerationId, setParseGenerationId] = useState(firstParseGenerationId(version));
  const [treeParentInput, setTreeParentInput] = useState("");
  const [treeParentId, setTreeParentId] = useState<string | null>(null);
  const [treeTrail, setTreeTrail] = useState<Array<{ nodeId: string; label: string }>>([]);
  const [treeCursors, setTreeCursors] = useState<Array<string | null>>([null]);
  const [treeCursorIndex, setTreeCursorIndex] = useState(0);
  const [qualityCursors, setQualityCursors] = useState<Array<string | null>>([null]);
  const [qualityCursorIndex, setQualityCursorIndex] = useState(0);
  const [tableNodeId, setTableNodeId] = useState("");
  const [tableNodeLabel, setTableNodeLabel] = useState("");
  const [tableCursors, setTableCursors] = useState<Array<string | null>>([null]);
  const [tableCursorIndex, setTableCursorIndex] = useState(0);
  const [tree, setTree] = useState<Loadable<CanonicalTreePage>>({ phase: "idle" });
  const [quality, setQuality] = useState<Loadable<ParseQualityPage>>({ phase: "idle" });
  const [table, setTable] = useState<Loadable<StructuredTablePage>>({ phase: "idle" });
  const [lastTreeRequest, setLastTreeRequest] = useState<TreeLoadRequest | null>(null);
  const [lastQualityRequest, setLastQualityRequest] = useState<QualityLoadRequest | null>(null);
  const [lastTableRequest, setLastTableRequest] = useState<TableLoadRequest | null>(null);
  const treeRequestSeq = useRef(0);
  const qualityRequestSeq = useRef(0);
  const tableRequestSeq = useRef(0);
  const canLoad = Boolean(parseGenerationId.trim());
  const parseGenerations = (version.generations ?? []).filter((item) => item.kind === "parse");

  function resetStructureState(nextParseGenerationId = firstParseGenerationId(version)) {
    treeRequestSeq.current += 1;
    qualityRequestSeq.current += 1;
    tableRequestSeq.current += 1;
    setParseGenerationId(nextParseGenerationId);
    setTreeParentInput("");
    setTreeParentId(null);
    setTreeTrail([]);
    setTreeCursors([null]);
    setTreeCursorIndex(0);
    setQualityCursors([null]);
    setQualityCursorIndex(0);
    setTableNodeId("");
    setTableNodeLabel("");
    setTableCursors([null]);
    setTableCursorIndex(0);
    setLastTreeRequest(null);
    setLastQualityRequest(null);
    setLastTableRequest(null);
    setTree({ phase: "idle" });
    setQuality({ phase: "idle" });
    setTable({ phase: "idle" });
  }

  useEffect(() => {
    resetStructureState(firstParseGenerationId(version));
  }, [version.version_id, version.generations]);

  function identityFailure(message: string) {
    return new ApiFailure("invalid_response", message, "local", 200, null);
  }

  function nodeLabel(node: CanonicalTreeNode): string {
    return `${node.number ? `${node.number} ` : ""}${node.title ?? node.node_type}`;
  }

  async function loadTree(options: {
    parentId?: string | null;
    cursor?: string | null;
    trail?: Array<{ nodeId: string; label: string }>;
    cursors?: Array<string | null>;
    cursorIndex?: number;
  } = {}) {
    const parseId = parseGenerationId.trim();
    if (!parseId) return;
    const requestedParentId = resolveRequestedTreeParentId(options.parentId, treeParentId);
    const requestedCursor = options.cursor ?? null;
    const request: TreeLoadRequest = {
      parentId: requestedParentId,
      cursor: requestedCursor,
      trail: options.trail ?? treeTrail,
      cursors: options.cursors ?? [requestedCursor],
      cursorIndex: options.cursorIndex ?? (options.cursors ?? [requestedCursor]).length - 1,
    };
    const requestSeq = treeRequestSeq.current + 1;
    treeRequestSeq.current = requestSeq;
    setLastTreeRequest(request);
    setTree({ phase: "loading" });
    try {
      const response = await getCanonicalTree(version.version_id, parseId, { parentId: request.parentId, cursor: request.cursor });
      if (treeRequestSeq.current !== requestSeq) return;
      if (!canonicalTreePageMatches(response.data, { versionId: version.version_id, parseGenerationId: parseId, parentId: request.parentId })) {
        setTree({ phase: "error", failure: identityFailure("Сервер вернул структуру для другой версии. Обновите выбранную версию.") });
        return;
      }
      setTree({ phase: "loaded", response });
      setTreeParentId(request.parentId);
      setTreeParentInput(request.parentId ?? "");
      setTreeTrail(request.trail);
      setTreeCursors(request.cursors);
      setTreeCursorIndex(request.cursorIndex);
    } catch (error) {
      if (treeRequestSeq.current === requestSeq) setTree({ phase: "error", failure: asApiFailure(error) });
    }
  }

  async function loadQuality(cursor: string | null = null, cursors: Array<string | null> = [cursor], cursorIndex = cursors.length - 1) {
    const parseId = parseGenerationId.trim();
    if (!parseId) return;
    const request: QualityLoadRequest = { cursor, cursors, cursorIndex };
    const requestSeq = qualityRequestSeq.current + 1;
    qualityRequestSeq.current = requestSeq;
    setLastQualityRequest(request);
    setQuality({ phase: "loading" });
    try {
      const response = await getParseQuality(version.version_id, parseId, { cursor: request.cursor });
      if (qualityRequestSeq.current !== requestSeq) return;
      if (!qualityPageMatches(response.data, { versionId: version.version_id, parseGenerationId: parseId })) {
        setQuality({ phase: "error", failure: identityFailure("Сервер вернул предупреждения для другой версии. Обновите выбранную версию.") });
        return;
      }
      setQuality({ phase: "loaded", response });
      setQualityCursors(request.cursors);
      setQualityCursorIndex(request.cursorIndex);
    } catch (error) {
      if (qualityRequestSeq.current === requestSeq) setQuality({ phase: "error", failure: asApiFailure(error) });
    }
  }

  async function loadTable(options: {
    nodeId?: string;
    label?: string;
    cursor?: string | null;
    cursors?: Array<string | null>;
    cursorIndex?: number;
  } = {}) {
    const parseId = parseGenerationId.trim();
    const nodeId = options.nodeId ?? tableNodeId.trim();
    if (!parseId || !nodeId) return;
    const requestedCursor = options.cursor ?? null;
    const request: TableLoadRequest = {
      nodeId,
      label: options.label ?? tableNodeLabel,
      cursor: requestedCursor,
      cursors: options.cursors ?? [requestedCursor],
      cursorIndex: options.cursorIndex ?? (options.cursors ?? [requestedCursor]).length - 1,
    };
    const requestSeq = tableRequestSeq.current + 1;
    tableRequestSeq.current = requestSeq;
    setLastTableRequest(request);
    setTable({ phase: "loading" });
    try {
      const response = await getStructuredTable(version.version_id, request.nodeId, parseId, { cursor: request.cursor });
      if (tableRequestSeq.current !== requestSeq) return;
      if (!structuredTablePageMatches(response.data, { versionId: version.version_id, parseGenerationId: parseId, nodeId: request.nodeId })) {
        setTable({ phase: "error", failure: identityFailure("Сервер вернул таблицу для другой версии или узла. Откройте таблицу заново.") });
        return;
      }
      setTable({ phase: "loaded", response });
      setTableNodeId(request.nodeId);
      setTableNodeLabel(request.label);
      setTableCursors(request.cursors);
      setTableCursorIndex(request.cursorIndex);
    } catch (error) {
      if (tableRequestSeq.current === requestSeq) setTable({ phase: "error", failure: asApiFailure(error) });
    }
  }

  const currentTreeCursor = currentCursorPage({ cursors: treeCursors, index: treeCursorIndex });
  const currentQualityCursor = currentCursorPage({ cursors: qualityCursors, index: qualityCursorIndex });
  const currentTableCursor = currentCursorPage({ cursors: tableCursors, index: tableCursorIndex });
  const treeNextCursor = tree.phase === "loaded" ? tree.response.data.next_cursor : null;
  const qualityNextCursor = quality.phase === "loaded" ? quality.response.data.next_cursor : null;
  const tableNextCursor = table.phase === "loaded" ? table.response.data.next_cursor : null;

  return (
    <section className="version-structure" data-component="CanonicalTreeView">
      <div className="inline-heading">
        <h4>Структура и качество</h4>
        <span>Выбранная версия: {version.metadata.version_label ?? `от ${version.metadata.approved_at}`}</span>
      </div>
      <label>
        <span>Результат обработки</span>
        <Select value={parseGenerationId} onValueChange={(value) => resetStructureState(value)} disabled={!parseGenerations.length}>
          <SelectTrigger aria-label="Поколение парсинга">
            <SelectValue placeholder="Нет результата парсинга" />
          </SelectTrigger>
          <SelectContent>
            {parseGenerations.map((item) => <SelectItem key={item.generation_id} value={item.generation_id}>{item.status === "ready" ? "готово" : item.status} · {formatDateTime(item.created_at)}</SelectItem>)}
          </SelectContent>
        </Select>
      </label>
      <div className="actions compact-actions" aria-label="Навигация по структуре">
        <button type="button" disabled={!canLoad || tree.phase === "loading"} onClick={() => void loadTree({ parentId: null, cursor: null, trail: [], cursors: [null], cursorIndex: 0 })}>Открыть корень</button>
        <button type="button" disabled={!canLoad || tree.phase === "loading" || treeTrail.length === 0} onClick={() => {
          const nextTrail = treeTrail.slice(0, -1);
          const nextParent = nextTrail.at(-1)?.nodeId ?? null;
          void loadTree({ parentId: nextParent, cursor: null, trail: nextTrail, cursors: [null], cursorIndex: 0 });
        }}>Назад к родителю</button>
        <button type="button" disabled={!canLoad || quality.phase === "loading"} onClick={() => void loadQuality(null, [null], 0)}>Проверить качество</button>
      </div>
      {treeTrail.length > 0 && <p className="muted">Путь: {treeTrail.map((item) => item.label).join(" → ")}</p>}
      <details>
        <summary>Открыть узел по ID</summary>
        <div className="lookup-row">
          <label><span>Parent ID</span><input value={treeParentInput} onChange={(event) => setTreeParentInput(event.currentTarget.value)} placeholder="пусто = корень" /></label>
          <button type="button" disabled={!canLoad || tree.phase === "loading"} onClick={() => void loadTree({ parentId: treeParentInput.trim() || null, cursor: null, trail: [], cursors: [null], cursorIndex: 0 })}>Открыть узел</button>
        </div>
      </details>
      {tree.phase === "loading" && <p className="muted">Читаем структуру…</p>}
      {tree.phase === "error" && <><ErrorNotice failure={tree.failure} fallback="Структура недоступна" /><button type="button" onClick={() => void loadTree(lastTreeRequest ?? { parentId: treeParentId, cursor: currentTreeCursor, trail: treeTrail, cursors: treeCursors, cursorIndex: treeCursorIndex })}>Повторить структуру</button></>}
      {tree.phase === "loaded" && (
        <>
          <CanonicalTreeList page={tree.response.data} onOpenNode={(node) => void loadTree({ parentId: node.node_id, cursor: null, trail: [...treeTrail, { nodeId: node.node_id, label: nodeLabel(node) }], cursors: [null], cursorIndex: 0 })} onOpenTable={(node) => void loadTable({ nodeId: node.node_id, label: nodeLabel(node), cursor: null, cursors: [null], cursorIndex: 0 })} />
          <CursorPager
            label="Страницы структуры"
            currentIndex={treeCursorIndex}
            hasNext={Boolean(treeNextCursor)}
            loading={false}
            onPrevious={() => {
              const previous = previousCursorPage({ cursors: treeCursors, index: treeCursorIndex });
              void loadTree({ parentId: treeParentId, cursor: currentCursorPage(previous), trail: treeTrail, cursors: previous.cursors, cursorIndex: previous.index });
            }}
            onNext={() => {
              if (!treeNextCursor) return;
              const next = nextCursorPage({ cursors: treeCursors, index: treeCursorIndex }, treeNextCursor);
              void loadTree({ parentId: treeParentId, cursor: currentCursorPage(next), trail: treeTrail, cursors: next.cursors, cursorIndex: next.index });
            }}
          />
        </>
      )}
      {quality.phase === "loading" && <p className="muted">Читаем предупреждения качества…</p>}
      {quality.phase === "error" && <><ErrorNotice failure={quality.failure} fallback="Качество недоступно" /><button type="button" onClick={() => {
        const request = lastQualityRequest ?? { cursor: currentQualityCursor, cursors: qualityCursors, cursorIndex: qualityCursorIndex };
        void loadQuality(request.cursor, request.cursors, request.cursorIndex);
      }}>Повторить качество</button></>}
      {quality.phase === "loaded" && (
        <QualityWarnings
          page={quality.response.data}
          currentPage={qualityCursorIndex + 1}
          onPrevious={() => {
            const previous = previousCursorPage({ cursors: qualityCursors, index: qualityCursorIndex });
            void loadQuality(currentCursorPage(previous), previous.cursors, previous.index);
          }}
          onNext={() => {
            if (!qualityNextCursor) return;
            const next = nextCursorPage({ cursors: qualityCursors, index: qualityCursorIndex }, qualityNextCursor);
            void loadQuality(currentCursorPage(next), next.cursors, next.index);
          }}
        />
      )}
      <details>
        <summary>Открыть таблицу по ID</summary>
        <div className="lookup-row">
          <label><span>Table node ID</span><input value={tableNodeId} onChange={(event) => setTableNodeId(event.currentTarget.value)} placeholder="UUID table node" /></label>
          <button type="button" disabled={!canLoad || !tableNodeId.trim() || table.phase === "loading"} onClick={() => void loadTable({ nodeId: tableNodeId.trim(), label: "", cursor: null, cursors: [null], cursorIndex: 0 })}>Открыть таблицу</button>
        </div>
      </details>
      {table.phase === "loading" && <p className="muted">Читаем строки таблицы…</p>}
      {table.phase === "error" && <><ErrorNotice failure={table.failure} fallback="Таблица недоступна" /><button type="button" onClick={() => void loadTable(lastTableRequest ?? { nodeId: tableNodeId.trim(), label: tableNodeLabel, cursor: currentTableCursor, cursors: tableCursors, cursorIndex: tableCursorIndex })}>Повторить таблицу</button></>}
      {table.phase === "loaded" && <StructuredTablePreview page={table.response.data} label={tableNodeLabel} currentPage={tableCursorIndex + 1} onPrevious={() => {
        const previous = previousCursorPage({ cursors: tableCursors, index: tableCursorIndex });
        void loadTable({ nodeId: tableNodeId.trim(), label: tableNodeLabel, cursor: currentCursorPage(previous), cursors: previous.cursors, cursorIndex: previous.index });
      }} onNext={() => {
        if (!tableNextCursor) return;
        const next = nextCursorPage({ cursors: tableCursors, index: tableCursorIndex }, tableNextCursor);
        void loadTable({ nodeId: tableNodeId.trim(), label: tableNodeLabel, cursor: currentCursorPage(next), cursors: next.cursors, cursorIndex: next.index });
      }} />}
      {!canLoad && <p className="muted">У выбранной версии нет результата парсинга. Структура и предупреждения появятся после обработки PDF.</p>}
    </section>
  );
}

function CursorPager({ label, currentIndex, hasNext, loading, onPrevious, onNext }: { label: string; currentIndex: number; hasNext: boolean; loading?: boolean; onPrevious: () => void; onNext: () => void }) {
  return (
    <div className="actions compact-actions" aria-label={label}>
      <span className="muted">Страница {currentIndex + 1}</span>
      <button type="button" disabled={loading || currentIndex === 0} onClick={onPrevious}>Предыдущая</button>
      <button type="button" disabled={loading || !hasNext} onClick={onNext}>Следующая</button>
    </div>
  );
}

function CanonicalTreeList({ page, onOpenNode, onOpenTable }: { page: CanonicalTreePage; onOpenNode: (node: CanonicalTreeNode) => void; onOpenTable: (node: CanonicalTreeNode) => void }) {
  if (!page.items.length) return <p className="muted">Дочерних узлов нет.</p>;
  return (
    <ol className="tree-list">
      {page.items.map((node) => (
        <li key={node.node_id}>
          <div>
            <strong>{node.number ? `${node.number} ` : ""}{node.title ?? node.node_type}</strong>
            <span>стр. {node.page_start}{node.page_end !== node.page_start ? `–${node.page_end}` : ""}; {node.node_type}</span>
          </div>
          <div className="actions compact-actions">
            <button type="button" className="link-button" disabled={!node.has_children} onClick={() => onOpenNode(node)}>Открыть раздел</button>
            {node.node_type === "table" && <button type="button" className="link-button" onClick={() => onOpenTable(node)}>Открыть таблицу</button>}
          </div>
        </li>
      ))}
    </ol>
  );
}

function QualityWarnings({ page, currentPage, onPrevious, onNext }: { page: ParseQualityPage; currentPage: number; onPrevious: () => void; onNext: () => void }) {
  return (
    <section data-component="ExtractionWarnings" className={`quality quality-${page.summary.status}`}>
      <div className="inline-heading"><strong>Качество: {page.summary.status}</strong><span>{page.summary.warning_count} предупреждений</span></div>
      {page.summary.reason_codes?.length ? <p className="muted">Причины: {page.summary.reason_codes.join(", ")}</p> : null}
      {page.items.length ? (
        <ul>
          {page.items.map((item) => <li key={`${item.ordinal}-${item.code}`}>{item.severity}: {item.code}{item.pdf_page ? ` · стр. ${item.pdf_page}` : ""}{item.block_id ? ` · block ${item.block_id}` : ""}</li>)}
        </ul>
      ) : <p className="muted">В текущем окне предупреждений нет.</p>}
      <CursorPager label="Страницы предупреждений" currentIndex={currentPage - 1} hasNext={Boolean(page.next_cursor)} onPrevious={onPrevious} onNext={onNext} />
      <a className="button-link" href={page.source_url}>Открыть источник</a>
    </section>
  );
}

function StructuredTablePreview({ page, label, currentPage, onPrevious, onNext }: { page: StructuredTablePage; label: string; currentPage: number; onPrevious: () => void; onNext: () => void }) {
  const cells = new Map(page.cells.map((cell) => [cell.id, cell]));
  return (
    <section className="table-preview">
      <div className="inline-heading"><strong>{label || (page.kind === "blank_template" ? "Шаблон таблицы" : "Таблица")}</strong><span>строки {page.row_start}–{page.row_end} из {page.total_rows}</span></div>
      {page.context_refs.length ? <p className="muted">Контекст: {page.context_refs.map((context) => context.text).join(" · ")}</p> : null}
      <table>
        <tbody>
          {page.rows.map((row) => (
            <tr key={row.row_index}>
              {row.cell_ids.map((cellId) => <TableCell key={cellId} cell={cells.get(cellId)} />)}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">Страницы PDF: {page.pdf_pages.join(", ") || "—"}</p>
      <CursorPager label="Страницы таблицы" currentIndex={currentPage - 1} hasNext={Boolean(page.next_cursor)} onPrevious={onPrevious} onNext={onNext} />
    </section>
  );
}

function TableCell({ cell }: { cell: StructuredTableCell | undefined }) {
  if (!cell) return <td className="missing-cell">нет ячейки</td>;
  return <td className={`cell-${cell.role}`} colSpan={cell.column_span} rowSpan={cell.row_span}>{cell.text || "—"}</td>;
}

function KnowledgeBaseSummary({ profile, onRefresh }: { profile: Loadable<LibraryProfile>; onRefresh: () => void }) {
  return (
    <div className="summary-card" data-component="KnowledgeBaseSummary">
      <div className="inline-heading"><strong>Сводка базы</strong><button type="button" className="small-button" onClick={onRefresh}>Обновить</button></div>
      {profile.phase === "loading" && <p className="muted">Читаем профиль библиотеки…</p>}
      {profile.phase === "error" && <ErrorNotice failure={profile.failure} fallback="Профиль библиотеки недоступен" />}
      {profile.phase === "loaded" && (
        <dl className="metric-grid">
          <div><dt>Документы</dt><dd>{profile.response.data.logical_document_count}</dd></div>
          <div><dt>Версии</dt><dd>{profile.response.data.version_count}</dd></div>
          <div><dt>Готовы к ответам</dt><dd>{profile.response.data.eligible_document_count}</dd></div>
          <div><dt>Последняя публикация</dt><dd>{profile.response.data.last_publication_at ? formatDateTime(profile.response.data.last_publication_at) : "нет"}</dd></div>
        </dl>
      )}
      {profile.phase === "idle" && <p className="muted">Сводка появится после входа.</p>}
    </div>
  );
}

function UploadDocumentDialog({
  session,
  documents,
  selectedDocument,
  onSelectDocument,
  onAcceptedUpload,
  onOpenProcessing,
  onToast,
}: {
  session: SessionInfo | null;
  documents: DocumentList | null;
  selectedDocument: DocumentDetail | components["schemas"]["DocumentSummary"] | null;
  onSelectDocument: (documentId: string) => void;
  onAcceptedUpload: (upload: UploadAccepted) => void;
  onOpenProcessing: () => void;
  onToast: (message: string) => void;
}) {
  const [mode, setMode] = useState<"new" | "version">("new");
  const [file, setFile] = useState<File | null>(null);
  const [metadata, setMetadata] = useState({
    title: "",
    legalStatus: "" as LegalStatus | "",
    approvedAt: "",
    versionLabel: "",
    authority: "",
    documentNumber: "",
    documentType: "",
    editionAt: "",
    effectiveFrom: "",
    effectiveTo: "",
  });
  const [autoPublish, setAutoPublish] = useState(true);
  const [upload, setUpload] = useState<Loadable<UploadAccepted>>({ phase: "idle" });
  const [targetDocumentId, setTargetDocumentId] = useState(selectedDocument?.document_id ?? "");
  const selectedDocumentIdRef = useRef(selectedDocument?.document_id ?? "");
  const canUpload = session?.role === "operator" || session?.role === "admin";
  const fileValidation = validatePdfUploadFile(file);
  const selectedDocumentId = selectedDocument?.document_id ?? "";
  const selectedDocumentReady = mode !== "version" || selectedDocumentReadyForVersionUpload(selectedDocument, targetDocumentId);
  const expectedPublication = mode === "version" && selectedDocumentReady ? uploadExpectedPublicationId(selectedDocument) : null;
  const effectiveRangeValid = !metadata.effectiveFrom || !metadata.effectiveTo || metadata.effectiveTo > metadata.effectiveFrom;

  useEffect(() => {
    selectedDocumentIdRef.current = selectedDocumentId;
  }, [selectedDocumentId]);

  useEffect(() => {
    if (mode === "new") return;
    if (!targetDocumentId && selectedDocumentId) {
      setTargetDocumentId(selectedDocumentId);
    }
  }, [mode, selectedDocumentId, targetDocumentId]);

  const options: VersionUploadOptions | null = useMemo(() => {
    if (!metadata.title.trim() || !metadata.legalStatus || !metadata.approvedAt || !effectiveRangeValid) return null;
    return {
      auto_publish: autoPublish,
      expected_current_publication_id: expectedPublication,
      metadata: {
        title: metadata.title.trim(),
        legal_status: metadata.legalStatus,
        approved_at: metadata.approvedAt,
        authority: metadata.authority.trim() || null,
        document_number: metadata.documentNumber.trim() || null,
        document_type: metadata.documentType.trim() || null,
        edition_at: metadata.editionAt || null,
        effective_from: metadata.effectiveFrom || null,
        effective_to: metadata.effectiveTo || null,
        schema_version: 1,
        version_label: metadata.versionLabel.trim() || null,
      },
    };
  }, [autoPublish, effectiveRangeValid, expectedPublication, metadata]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    const uploadDocumentId = targetDocumentId;
    const canSubmitVersion = mode !== "version" || selectedDocumentReadyForVersionUpload(selectedDocument, uploadDocumentId);
    if (!file || !options || !canUpload || !fileValidation.ok || !canSubmitVersion) return;
    setUpload({ phase: "loading" });
    try {
      const response = mode === "new"
        ? await uploadDocument(file, options, newIdempotencyKey("upload"))
        : await uploadDocumentVersion(uploadDocumentId, file, options, newIdempotencyKey("upload-version"));
      if (mode === "version" && selectedDocumentIdRef.current !== uploadDocumentId) return;
      setUpload({ phase: "loaded", response });
      onAcceptedUpload(response.data);
      onToast("Документ принят в обработку");
    } catch (error) {
      setUpload({ phase: "error", failure: asApiFailure(error) });
    }
  }

  return (
    <form className="panel upload" data-component="UploadDocumentDialog" onSubmit={(event) => void submit(event)}>
      <h3>Добавить документ</h3>
      <div className="segmented" data-component="LogicalDocumentPicker">
        <button type="button" className={mode === "new" ? "selected" : ""} onClick={() => setMode("new")}>Новый документ</button>
        <button type="button" className={mode === "version" ? "selected" : ""} onClick={() => setMode("version")}>Новая версия</button>
      </div>
      {mode === "version" && (
        <label>
          <span>Документ для новой версии</span>
          <Select
            value={targetDocumentId}
            onValueChange={(documentId) => {
              setTargetDocumentId(documentId);
              onSelectDocument(documentId);
            }}
            disabled={!documents?.items.length || upload.phase === "loading"}
          >
            <SelectTrigger aria-label="Документ для новой версии">
              <SelectValue placeholder="Выберите документ из загруженной библиотеки" />
            </SelectTrigger>
            <SelectContent>
              {documents?.items.map((item) => <SelectItem key={item.document_id} value={item.document_id}>{item.canonical_title}</SelectItem>)}
            </SelectContent>
          </Select>
        </label>
      )}
      {mode === "version" && selectedDocument && (
        <details><summary>Дополнительно о версии</summary><p className="muted">Текущая публикация: {expectedPublication ?? "публикации нет"}.</p></details>
      )}
      {mode === "version" && targetDocumentId && !selectedDocumentReady && <p className="muted">Загружаем выбранный документ перед отправкой версии…</p>}
      <label data-component="FileDropZone" className={`dropzone ${file ? "selected" : ""}`}>
        <span>{file ? `${file.name} · ${formatBytes(file.size)}` : "Выберите PDF до 50 МиБ"}</span>
        <input type="file" accept="application/pdf" onChange={(event) => setFile(event.currentTarget.files?.[0] ?? null)} />
      </label>
      {file && !fileValidation.ok && <p className="form-error">{fileValidation.message}</p>}
      <fieldset data-component="DocumentMetadataForm">
        <legend>Сведения о документе</legend>
        <label><span>Название *</span><input value={metadata.title} required maxLength={500} onChange={(event) => setMetadata({ ...metadata, title: event.currentTarget.value })} /></label>
        <label>
          <span>Правовой статус *</span>
          <Select value={metadata.legalStatus} onValueChange={(value) => setMetadata({ ...metadata, legalStatus: value as LegalStatus })}>
            <SelectTrigger aria-label="Правовой статус">
              <SelectValue placeholder="Выберите статус" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="active">Действует</SelectItem>
              <SelectItem value="archived">Архив</SelectItem>
            </SelectContent>
          </Select>
        </label>
        <label><span>Дата утверждения *</span><input type="date" value={metadata.approvedAt} required onChange={(event) => setMetadata({ ...metadata, approvedAt: event.currentTarget.value })} /></label>
        <details className="metadata-details"><summary>Дополнительные сведения</summary><div className="metadata-fields">
        <label><span>Метка версии</span><input value={metadata.versionLabel} maxLength={200} onChange={(event) => setMetadata({ ...metadata, versionLabel: event.currentTarget.value })} /></label>
        <label><span>Орган</span><input value={metadata.authority} maxLength={300} onChange={(event) => setMetadata({ ...metadata, authority: event.currentTarget.value })} /></label>
        <label><span>Номер</span><input value={metadata.documentNumber} maxLength={100} onChange={(event) => setMetadata({ ...metadata, documentNumber: event.currentTarget.value })} /></label>
        <label><span>Тип документа</span><input value={metadata.documentType} maxLength={100} onChange={(event) => setMetadata({ ...metadata, documentType: event.currentTarget.value })} /></label>
        <label><span>Дата редакции</span><input type="date" value={metadata.editionAt} onChange={(event) => setMetadata({ ...metadata, editionAt: event.currentTarget.value })} /></label>
        <label><span>Действует с</span><input type="date" value={metadata.effectiveFrom} onChange={(event) => setMetadata({ ...metadata, effectiveFrom: event.currentTarget.value })} /></label>
        <label><span>Действует до</span><input type="date" value={metadata.effectiveTo} onChange={(event) => setMetadata({ ...metadata, effectiveTo: event.currentTarget.value })} /></label>
        </div></details>
      </fieldset>
      {!effectiveRangeValid && <p className="form-error">Дата окончания действия должна быть позже даты начала.</p>}
      <label className="check"><input type="checkbox" checked={autoPublish} onChange={(event) => setAutoPublish(event.currentTarget.checked)} />Автопубликация после успешной проверки</label>
      <UploadTransferProgress upload={upload} fileValidation={fileValidation} />
      <button type="submit" disabled={!canUpload || !file || !fileValidation.ok || !options || upload.phase === "loading" || (mode === "version" && (!targetDocumentId || !selectedDocumentReady))}>Отправить в обработку</button>
      {!canUpload && <p className="muted">Загрузка требует роль operator/admin.</p>}
      {upload.phase === "error" && <ErrorNotice failure={upload.failure} fallback="Загрузка не принята" />}
      {upload.phase === "loaded" && <AcceptedUpload upload={upload.response.data} onOpenProcessing={onOpenProcessing} />}
    </form>
  );
}

function UploadTransferProgress({ upload, fileValidation }: { upload: Loadable<UploadAccepted>; fileValidation: ReturnType<typeof validatePdfUploadFile> }) {
  return <p data-component="UploadTransferProgress" className="muted">{upload.phase === "loaded" ? "Файл передан и ожидает обработки" : fileValidation.message}</p>;
}

function AcceptedUpload({ upload, onOpenProcessing }: { upload: UploadAccepted; onOpenProcessing: () => void }) {
  return (
    <div className="notice">
      <strong>Документ принят в обработку</strong>
      <button type="button" onClick={onOpenProcessing}>Открыть обработку</button>
      <details><summary>Дополнительно: номера загрузки</summary><p>Обработка: {upload.job_id}</p><p>Документ: {upload.document_id}; версия: {upload.version_id}</p></details>
    </div>
  );
}

function IngestionWorkspace({
  session,
  selection,
  onSelection,
  onToast,
}: {
  session: SessionInfo | null;
  selection: WorkspaceSelection;
  onSelection: Dispatch<SetStateAction<WorkspaceSelection>>;
  onToast: (message: string) => void;
}) {
  const [diagnosticJobId, setDiagnosticJobId] = useState("");
  const [deactivateReason, setDeactivateReason] = useState("");
  const [job, setJob] = useState<Loadable<IngestionJob>>({ phase: "idle" });
  const [jobEvents, setJobEvents] = useState<JobEventState>({ connection: "idle", lastSequence: 0, events: [] });
  const [versionCommand, setVersionCommand] = useState<Loadable<PublicationInfo | VersionSummary | IngestionJob>>({ phase: "idle" });
  const [capabilities, setCapabilities] = useState<Loadable<IngestionCapabilities>>({ phase: "idle" });
  const jobRequestSeq = useRef(0);
  const jobAbort = useRef<AbortController | null>(null);
  const selectedVersionIdRef = useRef<string>("");
  const selectedVersion = selection.version;
  const selectedVersionId = selectedVersion?.version_id ?? "";
  const activeJobId = selection.jobId || diagnosticJobId.trim();
  const readyIndexGenerationId = selectedVersion ? firstReadyIndexGenerationId(selectedVersion) : "";
  const expectedPublication = currentPublicationId(selectedVersion) ?? currentPublicationId(selection.document);
  const reindexAlias = capabilities.phase === "loaded" ? capabilities.response.data.pipeline_config_alias : "";
  const processingTitle = selectedVersion?.metadata.title ?? "Обработка документа";

  useEffect(() => {
    selectedVersionIdRef.current = selectedVersionId;
  }, [selectedVersionId]);

  useEffect(() => () => {
    jobAbort.current?.abort();
    jobRequestSeq.current += 1;
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setCapabilities({ phase: "loading" });
    void getIngestionCapabilities(controller.signal)
      .then((response) => { if (!controller.signal.aborted) setCapabilities({ phase: "loaded", response }); })
      .catch((error: unknown) => { if (!controller.signal.aborted) setCapabilities({ phase: "error", failure: asApiFailure(error) }); });
    return () => controller.abort();
  }, []);

  async function refreshSelectedVersion(versionId = selectedVersionId) {
    if (!versionId) return null;
    const response = await getVersion(versionId);
    onSelection((current) => current.version?.version_id === response.data.version_id ? { ...current, version: response.data } : current);
    return response.data;
  }

  async function refreshJob(jobId = activeJobId) {
    if (!jobId) return null;
    const requestSeq = jobRequestSeq.current + 1;
    jobRequestSeq.current = requestSeq;
    jobAbort.current?.abort();
    const controller = new AbortController();
    jobAbort.current = controller;
    setJob({ phase: "loading" });
    try {
      const response = await getIngestionJob(jobId, controller.signal);
      if (jobRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      if (response.data.job_id !== jobId) {
        setJob({ phase: "error", failure: new ApiFailure("invalid_response", "Сервер вернул другую обработку. Обновите состояние ещё раз.", "local", 200, null) });
        return;
      }
      setJobEvents({ connection: "idle", lastSequence: response.data.last_sequence, events: [] });
      setJob({ phase: "loaded", response });
      return response.data;
    } catch (error) {
      if (!controller.signal.aborted && jobRequestSeq.current === requestSeq) {
        setJob({ phase: "error", failure: asApiFailure(error) });
      }
    }
    return null;
  }

  async function refreshSelectedVersionForJob(currentJob: IngestionJob) {
    if (selectedVersionIdRef.current !== currentJob.version_id) return;
    const response = await getVersion(currentJob.version_id);
    onSelection((current) => current.version?.version_id === response.data.version_id ? { ...current, version: response.data } : current);
  }

  async function refreshJobAndSelectedVersion(jobId = activeJobId) {
    const refreshedJob = await refreshJob(jobId);
    if (refreshedJob && terminalJobStatuses.has(refreshedJob.status)) {
      await refreshSelectedVersionForJob(refreshedJob);
    }
    return refreshedJob;
  }

  useEffect(() => {
    if (!selection.jobId) return;
    void refreshJobAndSelectedVersion(selection.jobId);
  }, [selection.jobId]);

  useEffect(() => {
    const currentJob = job.phase === "loaded" ? job.response.data : null;
    if (!session || !currentJob || currentJob.job_id !== activeJobId || terminalJobStatuses.has(currentJob.status)) {
      return undefined;
    }
    let closed = false;
    const subscribedJobId = currentJob.job_id;
    const subscription = subscribeJobEvents(subscribedJobId, jobEvents.lastSequence, (update) => {
      if (closed) return;
      if (update.kind === "connection") {
        setJobEvents((current) => ({ ...current, connection: update.connection }));
        if (update.connection === "history_expired") {
          void refreshJobAndSelectedVersion(subscribedJobId);
        }
        return;
      }
      setJobEvents((current) => reduceJobEvent(current, update.event));
      if (isTerminalJobEvent(update.event)) {
        void refreshJobAndSelectedVersion(subscribedJobId);
      }
    });
    return () => {
      closed = true;
      subscription.close();
    };
  }, [session?.principal_id, activeJobId, job.phase, job.phase === "loaded" ? job.response.data.job_id : null, job.phase === "loaded" ? job.response.data.status : null]);

  async function command(kind: "cancel" | "retry") {
    if (!activeJobId) return;
    try {
      await (kind === "cancel" ? cancelIngestionJob(activeJobId) : retryIngestionJob(activeJobId));
      onToast(kind === "cancel" ? "Отмена обработки принята" : "Повторная обработка поставлена в очередь");
      await refreshJobAndSelectedVersion();
    } catch (error) {
      onToast(asApiFailure(error)?.message ?? "Команда не принята");
    }
  }

  async function requestReindex() {
    if (!selectedVersionId || !reindexAlias) return;
    const actionVersionId = selectedVersionId;
    setVersionCommand({ phase: "loading" });
    try {
      const refreshed = await refreshSelectedVersion(actionVersionId);
      if (!selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) return;
      const response = await reindexVersion(actionVersionId, {
        pipeline_config_alias: reindexAlias,
        expected_current_publication_id: currentPublicationId(refreshed) ?? currentPublicationId(selection.document),
        auto_publish: true,
      }, newIdempotencyKey("reindex"));
      if (!selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) return;
      onSelection((current) => ({ ...current, jobId: response.data.job_id }));
      setVersionCommand({ phase: "idle" });
      onToast("Повторная обработка поставлена в очередь");
    } catch (error) {
      if (selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) {
        setVersionCommand({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function requestPublish() {
    if (!selectedVersionId || !readyIndexGenerationId) return;
    const actionVersionId = selectedVersionId;
    setVersionCommand({ phase: "loading" });
    try {
      const refreshed = await refreshSelectedVersion(actionVersionId);
      if (!selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) return;
      const indexGenerationId = refreshed ? firstReadyIndexGenerationId(refreshed) : readyIndexGenerationId;
      const refreshedExpected = currentPublicationId(refreshed) ?? currentPublicationId(selection.document);
      if (!indexGenerationId) {
        setVersionCommand({ phase: "idle" });
        onToast("Нет готового индекса для публикации");
        return;
      }
      const response = await publishVersion(actionVersionId, {
        index_generation_id: indexGenerationId,
        expected_current_publication_id: refreshedExpected,
        operation_id: crypto.randomUUID(),
      });
      if (!selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) return;
      setVersionCommand({ phase: "loaded", response });
      onToast("Публикация версии принята");
    } catch (error) {
      if (selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) {
        setVersionCommand({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  async function requestDeactivate() {
    if (!selectedVersionId || !deactivateReason.trim()) return;
    const actionVersionId = selectedVersionId;
    setVersionCommand({ phase: "loading" });
    try {
      const response = await deactivateVersion(actionVersionId, { reason: deactivateReason.trim() });
      if (!selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) return;
      setVersionCommand({ phase: "loaded", response });
      onSelection((current) => ({ ...current, version: current.version ? { ...current.version, ...response.data } : current.version }));
      onToast("Версия деактивирована сервером");
    } catch (error) {
      if (selectedVersionStillTargetsAction(selectedVersionIdRef.current, actionVersionId)) {
        setVersionCommand({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }

  return (
    <section className="grid two-columns">
      <div className="panel" data-component="IngestionTimeline">
        <h3>{processingTitle}</h3>
        {job.phase === "loading" && <p className="muted">Обновляем состояние обработки…</p>}
        {job.phase === "error" && <ErrorNotice failure={job.failure} fallback="Состояние обработки недоступно" />}
        {job.phase === "loaded" && <JobDetails job={job.response.data} events={jobEvents} onCancel={() => void command("cancel")} onRetry={() => void command("retry")} />}
        {job.phase === "idle" && <p className="muted">После загрузки или повторной обработки здесь появится текущий статус.</p>}
        <details>
          <summary>Дополнительно: найти обработку</summary>
          <p className="muted">Активная обработка: {activeJobId || "пока нет"}.</p>
          {!selection.jobId && (
            <label>
              <span>Диагностический ID обработки</span>
              <input value={diagnosticJobId} onChange={(event) => setDiagnosticJobId(event.currentTarget.value)} placeholder="опционально, если открываете старую обработку" />
            </label>
          )}
          <button type="button" onClick={() => void refreshJobAndSelectedVersion()} disabled={!activeJobId || job.phase === "loading"}>Проверить</button>
        </details>
      </div>
      <div className="panel" data-component="PublishVersionDialog">
        <details>
          <summary>Дополнительно: управление версией</summary>
          <h3>Версия: повторная индексация и публикация</h3>
          {selectedVersion ? (
            <div className="summary-card">
              <strong>{selectedVersion.metadata.title}</strong>
              <p>{summarizeVersionChoice(selectedVersion)}</p>
              <p className="muted">Готовый индекс: {readyIndexGenerationId || "нет"} · текущая публикация: {expectedPublication ?? "нет"}</p>
            </div>
          ) : <p className="muted">Выберите документ и версию во вкладке «Документы». Ручной Version ID не нужен в обычном сценарии.</p>}
          <div className="actions">
            <Button type="button" onClick={() => void requestReindex()} disabled={!selectedVersionId || !reindexAlias || versionCommand.phase === "loading"}>Переиндексировать</Button>
            <button type="button" onClick={() => void requestPublish()} disabled={!selectedVersionId || !readyIndexGenerationId || versionCommand.phase === "loading"}>Опубликовать готовый индекс</button>
          </div>
          <DeactivateVersionDialog
            disabled={!selectedVersionId || versionCommand.phase === "loading"}
            reason={deactivateReason}
            onReason={setDeactivateReason}
            onConfirm={() => void requestDeactivate()}
          />
          {versionCommand.phase === "loading" && <p className="muted">Отправляем команду версии…</p>}
          {versionCommand.phase === "error" && <ErrorNotice failure={versionCommand.failure} fallback="Команда версии не принята" />}
          {versionCommand.phase === "loaded" && <VersionCommandResult value={versionCommand.response.data} />}
          <p className="muted">Перед публикацией версия перечитывается, чтобы команда применялась к текущему состоянию документа.</p>
          {capabilities.phase === "loaded" && <p className="muted">Повторная индексация использует серверный профиль обработки: {reindexAlias}.</p>}
          {capabilities.phase === "loading" && <p className="muted">Загружаем доступный профиль повторной индексации…</p>}
          {capabilities.phase === "error" && <p className="muted">Повторная индексация временно недоступна: сервер не вернул профиль обработки.</p>}
        </details>
      </div>
    </section>
  );
}

function DeactivateVersionDialog({
  disabled,
  reason,
  onReason,
  onConfirm,
}: {
  disabled: boolean;
  reason: string;
  onReason: (value: string) => void;
  onConfirm: () => void;
}) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button type="button" variant="link" data-component="DeactivateVersionDialog" disabled={disabled}>Деактивировать версию</Button>
      </DialogTrigger>
      <DialogContent aria-describedby="deactivate-version-description">
        <DialogHeader>
          <DialogTitle>Деактивировать выбранную версию?</DialogTitle>
          <DialogDescription id="deactivate-version-description">
            Команда снимает выбранную версию с публикации, не удаляя исходный PDF и историю запусков.
          </DialogDescription>
        </DialogHeader>
        <label><span>Причина деактивации версии</span><input value={reason} maxLength={500} onChange={(event) => onReason(event.currentTarget.value)} placeholder="например: superseded by newer version" /></label>
        <div className="actions">
          <DialogClose asChild><Button type="button" variant="secondary">Отмена</Button></DialogClose>
          <DialogClose asChild><Button type="button" disabled={!reason.trim()} onClick={onConfirm}>Подтвердить</Button></DialogClose>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function VersionCommandResult({ value }: { value: PublicationInfo | VersionSummary | IngestionJob }) {
  if ("publication_id" in value) {
    return <div className="notice"><strong>Версия опубликована</strong><p>{formatDateTime(value.published_at)}</p><details><summary>Дополнительно</summary><p>ID публикации: {value.publication_id}</p></details></div>;
  }
  if ("job_id" in value) {
    return <div className="notice"><strong>Повторная обработка принята</strong><p>{jobStatusLabels[value.status]}</p><details><summary>Дополнительно: номер обработки</summary><p>ID обработки: {value.job_id}</p></details></div>;
  }
  return <div className="notice"><strong>Состояние версии обновлено</strong><details><summary>Дополнительно</summary><p>{value.publication_status}; версия: {value.version_id}</p></details></div>;
}

function JobDetails({ job, events, onCancel, onRetry }: { job: IngestionJob; events: JobEventState; onCancel: () => void; onRetry: () => void }) {
  const currentIndex = job.stage ? ingestionOrder.indexOf(job.stage) : -1;
  const canCancel = !terminalJobStatuses.has(job.status);
  const canRetry = job.status === "failed";
  return (
    <article>
      <div className="run-header">
        <div>
          <h3>{jobStatusLabels[job.status]}</h3>
          <p>{job.stage ? ingestionStageLabels[job.stage] : "этап уточняется"} · {progressText(job)}</p>
        </div>
        {(canCancel || canRetry) && (
          <div className="actions">
            {canCancel && <button type="button" onClick={onCancel}>Отменить</button>}
            {canRetry && <button type="button" onClick={onRetry}>Повторить</button>}
          </div>
        )}
      </div>
      <div className="notice">Качество документа доступно во вкладке «Документы» в панели выбранной версии.</div>
      <details data-component="VersionTimeline">
        <summary>Дополнительно: ход обработки</summary>
        <p className="muted">{jobEventStatusText(events)}</p>
        <p className="muted">Обработка: {job.job_id}; версия: {job.version_id}; попытка {job.attempt}/{job.max_attempts}; создано {formatDateTime(job.created_at)}</p>
        <ol className="timeline">{ingestionOrder.map((stage, index) => <li key={stage} className={index <= currentIndex ? "done" : "pending"}>{ingestionStageLabels[stage]}</li>)}</ol>
      </details>
    </article>
  );
}

function HistoryWorkspace({ session, onOpenRun, onToast }: { session: SessionInfo | null; onOpenRun: (selection: HistoricalRunSelection) => void; onToast: (message: string) => void }) {
  const [history, setHistory] = useState<Loadable<RunList>>({ phase: "idle" });
  const [historyCursors, setHistoryCursors] = useState<Array<string | null>>([null]);
  const [historyCursorIndex, setHistoryCursorIndex] = useState(0);
  const [lastHistoryRequest, setLastHistoryRequest] = useState<{ cursor: string | null; cursors: Array<string | null>; cursorIndex: number } | null>(null);
  const historyRequestSeq = useRef(0);
  const historyAbort = useRef<AbortController | null>(null);
  useEffect(() => () => {
    historyAbort.current?.abort();
    historyRequestSeq.current += 1;
  }, []);

  useEffect(() => {
    historyAbort.current?.abort();
    historyRequestSeq.current += 1;
    setHistoryCursors([null]);
    setHistoryCursorIndex(0);
    setLastHistoryRequest(null);
    if (!session) {
      setHistory({ phase: "idle" });
      return;
    }
    void load(null, [null], 0, { toast: false });
  }, [session?.principal_id]);

  async function load(cursor: string | null = null, cursors: Array<string | null> = [cursor], cursorIndex = cursors.length - 1, options: { toast: boolean } = { toast: true }) {
    const request = { cursor, cursors, cursorIndex };
    const requestSeq = historyRequestSeq.current + 1;
    historyRequestSeq.current = requestSeq;
    historyAbort.current?.abort();
    const controller = new AbortController();
    historyAbort.current = controller;
    setLastHistoryRequest(request);
    setHistory({ phase: "loading" });
    try {
      const response = await listRuns({ cursor }, controller.signal);
      if (historyRequestSeq.current !== requestSeq || controller.signal.aborted) return;
      setHistory({ phase: "loaded", response });
      setHistoryCursors(cursors);
      setHistoryCursorIndex(cursorIndex);
      if (options.toast) onToast("История обновлена");
    } catch (error) {
      if (!controller.signal.aborted && historyRequestSeq.current === requestSeq) {
        setHistory({ phase: "error", failure: asApiFailure(error) });
      }
    }
  }
  return (
    <section className="panel" data-component="RunHistoryTable">
      <div className="inline-heading"><h3>История запусков</h3><button type="button" onClick={() => { onToast("Запрошена история запусков"); void load(null, [null], 0); }} disabled={!session || history.phase === "loading"}>Обновить</button></div>
      <p className="muted">История показывает последние вопросы и итоговый статус.</p>
      {history.phase === "loading" && <p className="muted">Читаем историю…</p>}
      {history.phase === "error" && <><ErrorNotice failure={history.failure} fallback="История недоступна" />{lastHistoryRequest && <button type="button" onClick={() => void load(lastHistoryRequest.cursor, lastHistoryRequest.cursors, lastHistoryRequest.cursorIndex)}>Повторить историю</button>}</>}
      {history.phase === "loaded" && (history.response.data.items.length ? (
        <>
          <table><thead><tr><th>Вопрос</th><th>Статус</th><th>Создан</th><th>Длительность</th></tr></thead><tbody>{history.response.data.items.map((item) => (
            <tr key={item.run_id}>
              <td><Button type="button" variant="link" onClick={() => onOpenRun({ runId: item.run_id, questionExcerpt: item.question_excerpt })}>{item.question_excerpt}</Button></td>
              <td>{runStatusLabels[item.status]}</td>
              <td>{formatDateTime(item.created_at)}</td>
              <td>{formatDurationSeconds(item.duration_ms)}</td>
            </tr>
          ))}</tbody></table>
          <CursorPager
            label="Страницы истории"
            currentIndex={historyCursorIndex}
            hasNext={Boolean(history.response.data.next_cursor)}
            onPrevious={() => {
              const previous = previousCursorPage({ cursors: historyCursors, index: historyCursorIndex });
              void load(currentCursorPage(previous), previous.cursors, previous.index);
            }}
            onNext={() => {
              const nextCursor = history.response.data.next_cursor;
              if (!nextCursor) return;
              const next = nextCursorPage({ cursors: historyCursors, index: historyCursorIndex }, nextCursor);
              void load(currentCursorPage(next), next.cursors, next.index);
            }}
          />
        </>
      ) : <p className="muted">История пуста.</p>)}
      {history.phase === "idle" && <p className="muted">Войдите, чтобы увидеть последние запуски.</p>}
    </section>
  );
}

function DebugWorkspace({ session }: { session: SessionInfo | null }) {
  const [runId, setRunId] = useState("");
  const [debug, setDebug] = useState<Loadable<RunDebug>>({ phase: "idle" });
  const [captures, setCaptures] = useState<Loadable<DebugCaptureList>>({ phase: "idle" });
  const canDebug = session?.role === "operator" || session?.role === "admin";
  async function load() {
    if (!runId.trim()) return;
    setDebug({ phase: "loading" });
    try {
      const [debugResponse, captureResponse] = await Promise.all([
        getRunDebug(runId.trim()),
        listDebugCaptures(runId.trim()),
      ]);
      setDebug({ phase: "loaded", response: debugResponse });
      setCaptures({ phase: "loaded", response: captureResponse });
    } catch (error) {
      setDebug({ phase: "error", failure: asApiFailure(error) });
      setCaptures({ phase: "error", failure: asApiFailure(error) });
    }
  }
  return (
    <section className="panel" data-component="RunDebugView">
      <h3>Безопасная диагностика</h3>
      <p className="muted">Откройте запуск, чтобы увидеть безопасные шаги, тайминги и разрешённые диагностические файлы.</p>
      <label><span>ID запуска</span><input value={runId} onChange={(event) => setRunId(event.currentTarget.value)} /></label>
      <button type="button" disabled={!canDebug || !runId.trim()} onClick={() => void load()}>Открыть диагностику</button>
      {!canDebug && <p className="muted">Нужна роль operator/admin. Закрытые материалы выполнения и системный вывод не отображаются.</p>}
      {debug.phase === "loading" && <p className="muted">Читаем шаги…</p>}
      {debug.phase === "error" && <ErrorNotice failure={debug.failure} fallback="Диагностика недоступна" />}
      {debug.phase === "loaded" && <DebugSteps debug={debug.response.data} captures={captures.phase === "loaded" ? captures.response.data : null} />}
    </section>
  );
}

function DebugSteps({ debug, captures }: { debug: RunDebug; captures: DebugCaptureList | null }) {
  const [downloads, setDownloads] = useState<Record<string, CaptureDownloadState>>({});

  useEffect(() => {
    setDownloads({});
  }, [debug.run_id, captures?.status, captures?.part_count]);

  async function downloadCapture(item: DebugCaptureItem) {
    if (!item.download_url) return;
    setDownloads((current) => ({ ...current, [item.part_id]: { phase: "loading" } }));
    try {
      const response = await downloadDebugCapture(item.download_url);
      const filename = `debug-${debug.run_id}-${item.role}-${item.part}-${item.part_id}.json`;
      const objectUrl = URL.createObjectURL(response.data);
      try {
        const anchor = document.createElement("a");
        anchor.href = objectUrl;
        anchor.download = filename;
        anchor.rel = "noreferrer";
        document.body.append(anchor);
        anchor.click();
        anchor.remove();
      } finally {
        URL.revokeObjectURL(objectUrl);
      }
      setDownloads((current) => ({ ...current, [item.part_id]: { phase: "loaded", filename } }));
    } catch (error) {
      setDownloads((current) => ({ ...current, [item.part_id]: { phase: "error", failure: asApiFailure(error) } }));
    }
  }

  return (
    <div>
      <div data-component="TraceLink" className="trace">Трассировка: {debug.trace_url ? <a href={debug.trace_url} target="_blank" rel="noreferrer">открыть трассировку</a> : (debug.trace_id ?? "не настроена")}</div>
      {debug.capture && <p className="muted">Расширенная диагностика: {debug.capture.status}; файлов {debug.capture.attached_count}/{debug.capture.part_count}; срок {formatDateTime(debug.capture.expires_at)}.</p>}
      {captures && <ul className="source-list" data-component="DebugCaptureList">{captures.items.map((item) => <DebugCaptureRow key={item.part_id} item={item} state={downloads[item.part_id] ?? { phase: "idle" }} onDownload={() => void downloadCapture(item)} />)}</ul>}
      <table><thead><tr><th>Стадия</th><th>Попытка</th><th>Статус</th><th>Кандидаты</th><th>Токены</th></tr></thead><tbody>{debug.steps.map((step) => <tr key={`${step.stage}-${step.attempt}-${step.occurred_at}`}><td>{runStageLabels[step.stage]}</td><td>{step.attempt}</td><td>{step.status}</td><td>{step.candidate_count ?? "—"}</td><td>{step.input_tokens ?? "—"}/{step.output_tokens ?? "—"}</td></tr>)}</tbody></table>
    </div>
  );
}

function ArchiveDocumentDialog({
  open,
  onOpenChange,
  onCloseAutoFocus,
  disabled,
  document,
  reason,
  state,
  onReason,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCloseAutoFocus: (event: Event) => void;
  disabled: boolean;
  document: DocumentSummary | DocumentDetail | null;
  reason: string;
  state: Loadable<DocumentSummary>;
  onReason: (value: string) => void;
  onConfirm: () => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent aria-describedby="archive-document-description" onCloseAutoFocus={onCloseAutoFocus}>
        <DialogHeader>
          <DialogTitle>Архивировать документ?</DialogTitle>
          <DialogDescription id="archive-document-description">
            Команда архивирует логический документ по текущей публикации. Исторические запуски, цитаты и PDF остаются доступны по серверным правилам.
          </DialogDescription>
        </DialogHeader>
        {document && (
          <div className="summary-card">
            <strong>{document.canonical_title}</strong>
            <p className="muted">Текущая публикация: {currentPublicationId(document) ?? "нет"}</p>
          </div>
        )}
        <label><span>Причина архивации</span><input value={reason} maxLength={2000} onChange={(event) => onReason(event.currentTarget.value)} placeholder="например: документ утратил силу" /></label>
        {state.phase === "error" && <ErrorNotice failure={state.failure} fallback="Архивация не принята" />}
        {state.phase === "loaded" && <p className="muted">Архивировано: {formatDateTime(state.response.data.archived_at ?? new Date().toISOString())}</p>}
        <div className="actions">
          <DialogClose asChild><Button type="button" variant="secondary">Отмена</Button></DialogClose>
          <DialogClose asChild><Button type="button" disabled={disabled || !document || !reason.trim() || state.phase === "loading"} onClick={onConfirm}>Архивировать</Button></DialogClose>
        </div>
      </DialogContent>
    </Dialog>
  );
}

const PURGE_CONFIRMATION_TEXT = "УДАЛИТЬ НАВСЕГДА";

function isPurgePlanExpired(plan: Pick<PurgePlan, "expires_at">, clockMs: number): boolean {
  return clockMs > 0 && Date.parse(plan.expires_at) <= clockMs;
}

function SourcePurgeDialog({
  open,
  onOpenChange,
  onCloseAutoFocus,
  disabled,
  document,
  plan,
  accepted,
  status,
  confirmation,
  clockMs,
  onConfirmation,
  onPlan,
  onPurge,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCloseAutoFocus: (event: Event) => void;
  disabled: boolean;
  document: DocumentSummary | DocumentDetail | null;
  plan: Loadable<PurgePlan>;
  accepted: Loadable<PurgeAccepted>;
  status: Loadable<PurgeStatus>;
  confirmation: string;
  clockMs: number;
  onConfirmation: (value: string) => void;
  onPlan: () => void;
  onPurge: () => void;
}) {
  const currentPlan = plan.phase === "loaded" ? plan.response.data : null;
  const expired = currentPlan ? isPurgePlanExpired(currentPlan, clockMs) : false;
  const confirmed = confirmation.trim() === PURGE_CONFIRMATION_TEXT;
  const canRequestPlan = !disabled && Boolean(document) && plan.phase !== "loading";
  const canSubmit = !disabled && Boolean(currentPlan?.allowed) && !expired && confirmed && accepted.phase !== "loading";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent aria-describedby="source-purge-description" onCloseAutoFocus={onCloseAutoFocus}>
        <DialogHeader>
          <DialogTitle>Необратимое удаление источников</DialogTitle>
          <DialogDescription id="source-purge-description">
            Только для администратора. Сначала сервер показывает план, ссылки, ограничения и срок действия. Документ исчезает из источников только после завершения серверной операции.
          </DialogDescription>
        </DialogHeader>
        {document ? (
          <div className="summary-card">
            <strong>{document.canonical_title}</strong>
            <details><summary>Дополнительно о документе</summary><p className="muted">ID: {document.document_id}</p></details>
            <p className="muted">Архивация сохраняет ссылки. Физическое удаление доступно только после проверки серверного плана.</p>
          </div>
        ) : <p className="muted">Выберите документ в библиотеке.</p>}
        <Button type="button" variant="secondary" disabled={!canRequestPlan} onClick={onPlan}>
          {plan.phase === "loading" ? "Готовим план удаления…" : currentPlan ? "Обновить план удаления" : "Построить план удаления"}
        </Button>
        {plan.phase === "error" && <ErrorNotice failure={plan.failure} fallback="План удаления недоступен" />}
        {currentPlan && <PurgePlanView plan={currentPlan} expired={expired} />}
        {currentPlan?.allowed && !expired && (
          <label>
            <span>Для необратимого удаления введите: {PURGE_CONFIRMATION_TEXT}</span>
            <input value={confirmation} onChange={(event) => onConfirmation(event.currentTarget.value)} autoComplete="off" />
          </label>
        )}
        {expired && <p className="gate">План удаления истёк. Обновите его перед отправкой команды.</p>}
        {accepted.phase === "error" && <ErrorNotice failure={accepted.failure} fallback="Удаление не принято" />}
        {accepted.phase === "loaded" && (
          <p className="notice">
            Удаление принято: план {accepted.response.data.plan_id}, статус {accepted.response.data.status}. Статус будет обновляться до завершения операции.
          </p>
        )}
        <PurgeStatusView status={status} />
        <div className="actions">
          <DialogClose asChild><Button type="button" variant="secondary">Закрыть</Button></DialogClose>
          <Button type="button" disabled={!canSubmit} onClick={onPurge}>
            {accepted.phase === "loading" ? "Отправляем удаление…" : "Подтвердить физическое удаление"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function PurgePlanView({ plan, expired }: { plan: PurgePlan; expired: boolean }) {
  const referenceRows = [
    ["Активные обработки", plan.references.active_ingestion_jobs],
    ["Активные запуски", plan.references.active_runs],
    ["Снимки базы", plan.references.snapshots],
    ["Ответы", plan.references.results],
    ["Точки восстановления", plan.references.checkpoints],
    ["Ожидающие резервы", plan.references.pending_reservations],
    ["Объекты хранения", plan.references.objects],
  ];
  return (
    <div className={plan.allowed && !expired ? "summary-card" : "gate"} data-component="SourcePurgePlan">
      <div className="inline-heading">
        <strong>{plan.allowed ? "Сервер разрешил удаление" : "Удаление заблокировано"}</strong>
        <span>план v{plan.plan_version}</span>
      </div>
      <p className="muted">ID плана: {plan.plan_id}</p>
      <p className="muted">Создан: {formatDateTime(plan.created_at)} · действует до {formatDateTime(plan.expires_at)} · правило хранения {plan.retention_policy}</p>
      {plan.eligible_after && <p className="muted">Retention разрешает не раньше {formatDateTime(plan.eligible_after)}.</p>}
      <table>
        <thead><tr><th>Проверка ссылок</th><th>Количество</th></tr></thead>
        <tbody>{referenceRows.map(([label, value]) => <tr key={label}><td>{label}</td><td>{value}</td></tr>)}</tbody>
      </table>
      {plan.blockers.length ? (
        <ul className="source-list">{plan.blockers.map((blocker) => <li key={blocker}>{blocker}</li>)}</ul>
      ) : <p className="muted">Ограничений нет.</p>}
      {plan.allowed ? <p className="limit">Команда необратима: будут удалены исходные файлы и связанные артефакты из серверного плана.</p> : <p className="muted">Физическое удаление недоступно, пока сервер видит активные ссылки или ограничения.</p>}
    </div>
  );
}

function PurgeStatusView({ status }: { status: Loadable<PurgeStatus> }) {
  if (status.phase === "idle") return <p className="muted">После подтверждения статус будет обновляться до завершения.</p>;
  if (status.phase === "loading") return <p className="muted">Проверяем статус удаления…</p>;
  if (status.phase === "error") return <ErrorNotice failure={status.failure} fallback="Статус удаления недоступен" />;
  const data = status.response.data;
  return (
    <div className="summary-card" data-component="SourcePurgeStatus">
      <div className="inline-heading">
        <strong>Статус удаления: {data.status}</strong>
        <span>{data.deleted_object_count}/{data.total_object_count} объектов</span>
      </div>
      <p className="muted">ID плана: {data.plan_id}; версия {data.plan_version}</p>
      <p className="muted">Принято: {formatDateTime(data.accepted_at)} · завершено: {formatDateTime(data.completed_at)}</p>
      {data.error_code && <p className="limit">Ошибка удаления: {data.error_code}</p>}
    </div>
  );
}

function DebugCaptureRow({ item, state, onDownload }: { item: DebugCaptureItem; state: CaptureDownloadState; onDownload: () => void }) {
  return (
    <li>
      <span>{item.role}/{item.part} · {formatBytes(item.size_bytes)} · {item.state} · expires {formatDateTime(item.expires_at)}</span>
      {item.download_url
        ? <button type="button" className="link-button" disabled={state.phase === "loading"} onClick={onDownload}>{state.phase === "loading" ? "Скачиваем…" : "Скачать файл диагностики"}</button>
        : <span className="muted">Скачивание недоступно: файл очищен, срок истёк или диагностика не была прикреплена.</span>}
      {state.phase === "loaded" && <span className="muted">Файл диагностики подготовлен: {state.filename}</span>}
      {state.phase === "error" && <ErrorNotice failure={state.failure} fallback="Файл диагностики недоступен: доступ отозван, срок истёк или файл очищен." />}
    </li>
  );
}

function SystemWorkspace({ state, onRefresh }: { state: Loadable<SystemStatusResponse["data"]>; onRefresh: () => void }) {
  return (
    <section className="panel">
      <div className="inline-heading"><h3>Состояние системы</h3><button type="button" onClick={onRefresh}>Обновить</button></div>
      {state.phase === "loading" && <p className="muted">Проверяем компоненты…</p>}
      {state.phase === "error" && <ErrorNotice failure={state.failure} fallback="Состояние недоступно" />}
      {state.phase === "loaded" && <><p className={`status-text ${state.response.data.status}`}>API: {statusLabels[state.response.data.status]}</p><ul className="components">{state.response.data.components.map((component) => <li key={component.name}><span>{component.name}</span><span>{statusLabels[component.status]}</span></li>)}</ul><p className="muted">Проверено {formatDateTime(state.response.data.checked_at)} · ID {state.response.requestId}</p></>}
    </section>
  );
}
