import { ingestionStageValues } from "./generated";
import type { components } from "./generated";

export type JobEvent =
  | components["schemas"]["IngestionCreatedEvent"]
  | components["schemas"]["IngestionStageStartedEvent"]
  | components["schemas"]["IngestionProgressEvent"]
  | components["schemas"]["IngestionStageCompletedEvent"]
  | components["schemas"]["IngestionReadyEvent"]
  | components["schemas"]["IngestionCompletedEvent"]
  | components["schemas"]["IngestionFailedEvent"]
  | components["schemas"]["IngestionCancelledEvent"];

export type JobEventConnection = "idle" | "connected" | "reconnecting" | "offline" | "history_expired" | "unsupported";

export type JobEventState = {
  connection: JobEventConnection;
  lastSequence: number;
  events: JobEvent[];
};

export type JobEventUpdate =
  | { kind: "connection"; connection: JobEventConnection }
  | { kind: "event"; event: JobEvent };

export type JobEventSubscription = {
  close: () => void;
};

const eventTypeValues = [
  "ingestion.created",
  "stage.started",
  "stage.progress",
  "stage.completed",
  "ingestion.ready_to_publish",
  "ingestion.completed",
  "ingestion.failed",
  "ingestion.cancelled",
] as const;

const eventTypes = new Set<string>(eventTypeValues);
const terminalTypes = new Set(["ingestion.completed", "ingestion.failed", "ingestion.cancelled"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUuid(value: unknown): value is string {
  return typeof value === "string" && /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(value);
}

function isIngestionStage(value: unknown): value is components["schemas"]["IngestionStage"] {
  return typeof value === "string" && ingestionStageValues.some((stage) => stage === value);
}

export function parseJobEvent(payload: string, expectedJobId: string): JobEvent | null {
  let value: unknown;
  try {
    value = JSON.parse(payload);
  } catch {
    return null;
  }
  if (!isRecord(value) || value.schema_version !== 1 || !isUuid(value.event_id) || value.job_id !== expectedJobId
    || typeof value.sequence !== "number" || !Number.isInteger(value.sequence) || value.sequence < 1
    || typeof value.execution_epoch !== "number" || !Number.isInteger(value.execution_epoch) || value.execution_epoch < 0
    || typeof value.occurred_at !== "string" || !Number.isFinite(Date.parse(value.occurred_at))
    || typeof value.type !== "string" || !eventTypes.has(value.type) || !isRecord(value.data)) return null;
  const stage = value.stage ?? null;
  if (stage !== null && !isIngestionStage(stage)) return null;
  return value as JobEvent;
}

export function reduceJobEvent(state: JobEventState, event: JobEvent): JobEventState {
  if (event.sequence <= state.lastSequence || state.events.some((item) => item.sequence === event.sequence)) {
    return state;
  }
  return {
    connection: state.connection,
    lastSequence: event.sequence,
    events: [...state.events, event].sort((left, right) => left.sequence - right.sequence),
  };
}

export function isTerminalJobEvent(event: JobEvent): boolean {
  return terminalTypes.has(event.type);
}

export function jobEventStatusText(state: JobEventState): string {
  if (state.connection === "unsupported") return "Автообновление недоступно в этом браузере; обновите задание вручную.";
  if (state.connection === "history_expired") return "История событий устарела; задание перечитано целиком.";
  if (state.connection === "offline") return "События задания временно недоступны.";
  if (state.connection === "reconnecting") return "Восстанавливаем обновление задания…";
  if (state.connection === "connected") return state.events.length ? `Обновлений задания: ${state.events.length}` : "Автообновление задания подключено.";
  return "Автообновление задания ещё не открыто.";
}

export function subscribeJobEvents(jobId: string, lastSequence: number, onUpdate: (update: JobEventUpdate) => void): JobEventSubscription {
  if (typeof EventSource === "undefined") {
    onUpdate({ kind: "connection", connection: "unsupported" });
    return { close: () => undefined };
  }
  const query = lastSequence > 0 ? `?after=${encodeURIComponent(String(lastSequence))}` : "";
  const source = new EventSource(`/api/v1/ingestion-jobs/${encodeURIComponent(jobId)}/events${query}`, { withCredentials: true });
  source.onopen = () => onUpdate({ kind: "connection", connection: "connected" });
  source.onerror = () => onUpdate({ kind: "connection", connection: "reconnecting" });
  const handleEventMessage = (message: MessageEvent<string>) => {
    const event = parseJobEvent(message.data, jobId);
    if (event) onUpdate({ kind: "event", event });
  };
  source.onmessage = handleEventMessage;
  for (const eventType of eventTypeValues) {
    source.addEventListener(eventType, (message) => handleEventMessage(message as MessageEvent<string>));
  }
  source.addEventListener("history_expired", () => onUpdate({ kind: "connection", connection: "history_expired" }));
  return { close: () => source.close() };
}
