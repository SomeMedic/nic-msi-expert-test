import type { components } from "./generated";

type PublicRun = components["schemas"]["PublicRun"];

export type RunEvent =
  | components["schemas"]["RunCreatedEvent"]
  | components["schemas"]["RunStartedEvent"]
  | components["schemas"]["RunResumingEvent"]
  | components["schemas"]["StageStartedEvent"]
  | components["schemas"]["StageCompletedEvent"]
  | components["schemas"]["StageRetryScheduledEvent"]
  | components["schemas"]["RunCancelRequestedEvent"]
  | components["schemas"]["RunCompletedEvent"]
  | components["schemas"]["RunRefusedEvent"]
  | components["schemas"]["RunFailedEvent"]
  | components["schemas"]["RunCancelledEvent"];

export type RunEventConnection = "idle" | "connected" | "reconnecting" | "closed" | "offline" | "history_expired" | "unsupported";

export type RunEventState = {
  connection: RunEventConnection;
  lastSequence: number;
  events: RunEvent[];
};

export type RunEventUpdate =
  | { kind: "connection"; connection: RunEventConnection }
  | { kind: "event"; event: RunEvent };

export type RunEventSubscription = {
  close: () => void;
};

const eventTypeValues = [
  "run.created",
  "run.started",
  "run.resuming",
  "stage.started",
  "stage.completed",
  "stage.retry_scheduled",
  "run.cancel_requested",
  "run.completed",
  "run.refused",
  "run.failed",
  "run.cancelled",
] as const;

const eventTypes = new Set<string>(eventTypeValues);

const terminalTypes = new Set(["run.completed", "run.refused", "run.failed", "run.cancelled"]);
const terminalStatuses = new Set<PublicRun["status"]>(["completed", "refused", "failed", "cancelled"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" && /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(value);
}

function isRunStage(value: unknown): value is components["schemas"]["RunStage"] {
  return typeof value === "string" && [
    "snapshotting",
    "routing",
    "retrieving",
    "reranking",
    "building_context",
    "drafting",
    "checking_citations",
    "validating",
    "repairing",
    "revalidating",
    "rendering",
    "finalizing",
  ].includes(value);
}

export function parseRunEvent(payload: string, expectedRunId: string): RunEvent | null {
  let value: unknown;
  try {
    value = JSON.parse(payload);
  } catch {
    return null;
  }
  if (!isRecord(value) || value.schema_version !== 1 || !isUuid(value.event_id) || value.run_id !== expectedRunId
    || typeof value.sequence !== "number" || !Number.isInteger(value.sequence) || value.sequence < 1
    || typeof value.execution_epoch !== "number" || !Number.isInteger(value.execution_epoch) || value.execution_epoch < 0
    || typeof value.occurred_at !== "string" || !Number.isFinite(Date.parse(value.occurred_at))
    || typeof value.type !== "string" || !eventTypes.has(value.type) || !isRecord(value.data)) return null;
  const stage = value.stage ?? null;
  if (stage !== null && !isRunStage(stage)) return null;
  return value as RunEvent;
}

export function reduceRunEvent(state: RunEventState, event: RunEvent): RunEventState {
  if (event.sequence <= state.lastSequence || state.events.some((item) => item.sequence === event.sequence)) {
    return state;
  }
  return {
    connection: state.connection,
    lastSequence: event.sequence,
    events: [...state.events, event].sort((left, right) => left.sequence - right.sequence),
  };
}

export function isTerminalRunEvent(event: RunEvent): boolean {
  return terminalTypes.has(event.type);
}

export function liveRunProgress(
  run: Pick<PublicRun, "run_id" | "status" | "last_sequence" | "current_stage" | "stage_attempt">,
  state: RunEventState,
): Pick<PublicRun, "current_stage" | "stage_attempt"> {
  const restProgress = { current_stage: run.current_stage ?? null, stage_attempt: run.stage_attempt };
  if (terminalStatuses.has(run.status)) return restProgress;

  let latestStageEvent: RunEvent | null = null;
  for (const event of state.events) {
    if (event.run_id !== run.run_id || !event.stage || event.sequence <= run.last_sequence) continue;
    if (!event.type.startsWith("stage.")) continue;
    if (!latestStageEvent || event.sequence > latestStageEvent.sequence) {
      latestStageEvent = event;
    }
  }

  return latestStageEvent?.stage
    ? { current_stage: latestStageEvent.stage, stage_attempt: latestStageEvent.attempt }
    : restProgress;
}

export function eventStatusText(state: RunEventState): string {
  if (state.connection === "unsupported") return "SSE недоступен в этом браузере; используйте обновление run.";
  if (state.connection === "history_expired") return "История событий истекла; перечитайте run целиком.";
  if (state.connection === "offline") return "События недоступны; итог run не меняется.";
  if (state.connection === "closed") return "Run завершён; поток событий закрыт.";
  if (state.connection === "reconnecting") return "Восстанавливаем поток событий…";
  if (state.connection === "connected") return state.events.length ? `Получено событий: ${state.events.length}` : "Поток событий подключён.";
  return "Поток событий ещё не открыт.";
}

export function subscribeRunEvents(runId: string, lastSequence: number, onUpdate: (update: RunEventUpdate) => void): RunEventSubscription {
  if (typeof EventSource === "undefined") {
    onUpdate({ kind: "connection", connection: "unsupported" });
    return { close: () => undefined };
  }
  const query = lastSequence > 0 ? `?after=${encodeURIComponent(String(lastSequence))}` : "";
  const source = new EventSource(`/api/v1/runs/${encodeURIComponent(runId)}/events${query}`, { withCredentials: true });
  let seenTerminal = false;
  let reportedClosed = false;
  const reportClosed = () => {
    if (reportedClosed) return;
    reportedClosed = true;
    onUpdate({ kind: "connection", connection: "closed" });
  };
  source.onopen = () => onUpdate({ kind: "connection", connection: "connected" });
  source.onerror = () => {
    if (seenTerminal) {
      reportClosed();
      return;
    }
    onUpdate({ kind: "connection", connection: "reconnecting" });
  };
  const handleEventMessage = (message: MessageEvent<string>) => {
    const event = parseRunEvent(message.data, runId);
    if (!event) return;
    if (isTerminalRunEvent(event)) seenTerminal = true;
    onUpdate({ kind: "event", event });
    if (seenTerminal) {
      reportClosed();
      source.close();
    }
  };
  source.onmessage = handleEventMessage;
  for (const eventType of eventTypeValues) {
    source.addEventListener(eventType, (message) => handleEventMessage(message as MessageEvent<string>));
  }
  source.addEventListener("history_expired", () => onUpdate({ kind: "connection", connection: "history_expired" }));
  return { close: () => source.close() };
}
