import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ApiFailure,
  archiveDocument,
  cancelIngestionJob,
  createAuthSession,
  createPurgePlan,
  createRun,
  deactivateVersion,
  downloadDebugCapture,
  getCanonicalTree,
  getDebugCapturePolicy,
  getIngestionCapabilities,
  getLibraryProfile,
  getDocument,
  getIngestionJob,
  getRun,
  getRunDebug,
  getParseQuality,
  getPurgeStatus,
  getSourceEvidence,
  getStructuredTable,
  getSystemStatus,
  getVersion,
  listDebugCaptures,
  listDocuments,
  listRuns,
  publishVersion,
  reindexVersion,
  submitPurgePlan,
  uploadDocument,
} from "../src/api/client";
import type { components } from "../src/api/generated";
import { isTerminalJobEvent, parseJobEvent, reduceJobEvent, subscribeJobEvents } from "../src/api/job-events";
import type { JobEventState } from "../src/api/job-events";
import { eventStatusText, liveRunProgress, parseRunEvent, reduceRunEvent, subscribeRunEvents } from "../src/api/run-events";
import type { RunEventState } from "../src/api/run-events";
import { answerHasDraftLeak, currentPublicationId, canonicalTreePageMatches, currentCursorPage, documentDetailMatches, documentLifecycleBadgeTexts, evidenceIdForClaimCitation, firstParseGenerationId, firstReadyIndexGenerationId, formatDurationSeconds, formatSourcePdfPages, maxPdfUploadBytes, nextCursorPage, previousCursorPage, publicEvidenceMatches, publicRunMatches, purgeAcceptedMatches, purgePlanMatches, purgeStatusMatches, qualityPageMatches, questionComposerVisible, resolveDocumentListRequest, resolveRequestedTreeParentId, runActionStillTargetsDisplayedRun, runEventCursorAfterReload, selectedDocumentReadyForVersionUpload, selectedVersionStillTargetsAction, sourcePdfPages, sourceUrlForFirstPdfPage, structuredTablePageMatches, uploadExpectedPublicationId, validatePdfUploadFile, versionDetailMatches, versionLegalStatusText } from "../src/workspace";

const requestId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const unavailable: components["schemas"]["SystemStatus"] = {
  status: "degraded",
  checked_at: "2026-09-08T00:00:00Z",
  components: [
    { name: "backend", status: "ready", error_code: null },
    { name: "agent-runtime", status: "unavailable", error_code: "DEPENDENCY_UNAVAILABLE" },
  ],
};
const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => vi.unstubAllGlobals());


function bodyText(body: BodyInit | null | undefined): string {
  return typeof body === "string" ? body : "";
}

function jsonResponse(body: unknown, status = 200, correlation = requestId) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", "X-Request-ID": correlation },
  });
}

describe("same-origin system status client", () => {
  it("preserves actual degraded readiness and sends only browser-safe correlation", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(unavailable));
    const response = await getSystemStatus();

    expect(response).toEqual({ data: unavailable, requestId });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [path, options] = fetchMock.mock.calls[0]!;
    expect(path).toBe("/api/v1/system/status");
    expect(options).toMatchObject({ method: "GET", credentials: "same-origin", redirect: "error", cache: "no-store" });
    const headers = new Headers(options?.headers);
    expect(headers.get("X-Request-ID")).toMatch(/^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/i);
    expect(headers.has("Authorization")).toBe(false);
  });

  it("returns the typed safe backend error and its request ID", async () => {
    const body = {
      error: { code: "DEPENDENCY_UNAVAILABLE", message: "Сервис временно недоступен", retryable: true, request_id: requestId, details: {} },
    } satisfies components["schemas"]["ErrorEnvelope"];
    fetchMock.mockResolvedValueOnce(jsonResponse(body, 503));

    await expect(getSystemStatus()).rejects.toMatchObject({
      name: "ApiFailure", kind: "http", httpStatus: 503, requestId,
      message: body.error.message, serverError: body.error,
    });
  });

  it("does not expose a reverse-proxy HTML error body", async () => {
    fetchMock.mockResolvedValueOnce(new Response("<html>PRIVATE_INTERNAL_TRACE</html>", { status: 502 }));
    const error = await getSystemStatus().catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(ApiFailure);
    expect(error).toMatchObject({ kind: "http", httpStatus: 502, serverError: null });
    expect(String(error)).not.toContain("PRIVATE_INTERNAL_TRACE");
  });

  it("rejects an unknown backend error code without rendering its body", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({
      error: { code: "UNRECOGNIZED_PRIVATE_CODE", message: "PRIVATE_INTERNAL_TRACE", retryable: false, request_id: requestId, details: {} },
    }, 500));
    const error = await getSystemStatus().catch((failure: unknown) => failure);

    expect(error).toMatchObject({ kind: "http", serverError: null, requestId });
    expect(String(error)).not.toContain("PRIVATE_INTERNAL_TRACE");
  });

  it("rejects nested diagnostic details instead of retaining raw payloads", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({
      error: { code: "INTERNAL_ERROR", message: "PRIVATE_INTERNAL_TRACE", retryable: false, request_id: requestId, details: { exception: { stack: "PRIVATE" } } },
    }, 500));
    const error = await getSystemStatus().catch((failure: unknown) => failure);

    expect(error).toMatchObject({ kind: "http", serverError: null });
    expect(JSON.stringify(error)).not.toContain("PRIVATE");
  });

  it("maps transport failure to a safe message without raw exception text", async () => {
    fetchMock.mockRejectedValueOnce(new TypeError("PRIVATE_INTERNAL_TRACE"));
    const error = await getSystemStatus().catch((failure: unknown) => failure);

    expect(error).toMatchObject({ kind: "network", httpStatus: null, serverError: null });
    expect(String(error)).not.toContain("PRIVATE_INTERNAL_TRACE");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("propagates unmount cancellation instead of turning it into a user error", async () => {
    const controller = new AbortController();
    const aborted = new DOMException("Aborted", "AbortError");
    controller.abort();
    fetchMock.mockRejectedValueOnce(aborted);

    await expect(getSystemStatus(controller.signal)).rejects.toBe(aborted);
  });

  it.each([
    { ...unavailable, status: "ready" },
    { ...unavailable, checked_at: "2026-09-08" },
    { ...unavailable, components: [unavailable.components[0], unavailable.components[0]] },
    { status: "ready", checked_at: unavailable.checked_at, components: null },
  ])("rejects inconsistent or malformed successful readiness data", async (payload) => {
    fetchMock.mockResolvedValueOnce(jsonResponse(payload));
    await expect(getSystemStatus()).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200, serverError: null });
  });

  it("keeps the outgoing correlation ID if the response header is invalid", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(unavailable, 200, "INVALID_RESPONSE_ID"));
    const response = await getSystemStatus();
    const sent = new Headers(fetchMock.mock.calls[0]![1]?.headers).get("X-Request-ID");

    expect(response.requestId).toBe(sent);
    expect(response.requestId).not.toBe("INVALID_RESPONSE_ID");
  });

  it("reads server-approved ingestion capabilities and debug capture policy", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ pipeline_config_alias: "canonical:default" } satisfies components["schemas"]["IngestionCapabilities"]));
    fetchMock.mockResolvedValueOnce(jsonResponse({ debug_capture_allowed: true, debug_capture_ttl_hours: 24 } satisfies components["schemas"]["DebugCapturePolicy"]));

    await expect(getIngestionCapabilities()).resolves.toMatchObject({ data: { pipeline_config_alias: "canonical:default" } });
    await expect(getDebugCapturePolicy()).resolves.toMatchObject({ data: { debug_capture_allowed: true, debug_capture_ttl_hours: 24 } });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/system/ingestion-capabilities",
      "/api/v1/system/debug-capture-policy",
    ]);
    expect(fetchMock.mock.calls.every((call) => new Headers(call[1]?.headers).has("Authorization") === false)).toBe(true);
  });

  it("reads safe debug capture metadata without leaking service auth or accepting foreign downloads", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const captureList: components["schemas"]["DebugCaptureList"] = {
      run_id: runId,
      enabled: true,
      policy_version: "p11.capture.v1",
      status: "available",
      expires_at: "2026-09-09T00:00:00Z",
      part_count: 1,
      attached_count: 1,
      unavailable_count: 0,
      items: [{
        part_id: "55555555-5555-4555-8555-555555555555",
        call_id: "66666666-6666-4666-8666-666666666666",
        created_at: "2026-09-08T00:00:01Z",
        expires_at: "2026-09-09T00:00:00Z",
        execution_epoch: 0,
        schema_attempt: 1,
        size_bytes: 128,
        payload_size_bytes: 96,
        payload_sha256: "b".repeat(64),
        part: "request",
        role: "router",
        state: "attached",
        download_url: `/api/v1/runs/${runId}/debug/captures/55555555-5555-4555-8555-555555555555`,
      }],
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(captureList));
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ...captureList,
      items: [{ ...captureList.items[0], download_url: "https://private.example/debug.json" }],
    }));
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ...captureList,
      run_id: "77777777-7777-4777-8777-777777777777",
    }));

    await expect(listDebugCaptures(runId)).resolves.toMatchObject({ data: captureList });
    await expect(listDebugCaptures(runId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(listDebugCaptures(runId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      `/api/v1/runs/${runId}/debug/captures`,
      `/api/v1/runs/${runId}/debug/captures`,
      `/api/v1/runs/${runId}/debug/captures`,
    ]);
    expect(fetchMock.mock.calls.every((call) => new Headers(call[1]?.headers).has("Authorization") === false)).toBe(true);
  });

  it("downloads debug capture through same-origin fetch and maps expired or forbidden access to safe errors", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const partId = "55555555-5555-4555-8555-555555555555";
    const downloadUrl = `/api/v1/runs/${runId}/debug/captures/${partId}`;
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ schema_version: "p11.capture-part.v1" }), {
      status: 200,
      headers: { "Content-Type": "application/json", "Content-Length": "36", "X-Request-ID": requestId },
    }));
    fetchMock.mockResolvedValueOnce(jsonResponse({
      error: { code: "SOURCE_REVOKED", message: "Capture больше недоступен", retryable: false, request_id: requestId, details: {} },
    } satisfies components["schemas"]["ErrorEnvelope"], 410));
    fetchMock.mockResolvedValueOnce(jsonResponse({
      error: { code: "FORBIDDEN", message: "Недостаточно прав для capture", retryable: false, request_id: requestId, details: {} },
    } satisfies components["schemas"]["ErrorEnvelope"], 403));

    const response = await downloadDebugCapture(downloadUrl);
    await expect(response.data.text()).resolves.toContain("p11.capture-part.v1");
    await expect(downloadDebugCapture(downloadUrl)).rejects.toMatchObject({ kind: "http", httpStatus: 410, message: "Capture больше недоступен" });
    await expect(downloadDebugCapture(downloadUrl)).rejects.toMatchObject({ kind: "http", httpStatus: 403, message: "Недостаточно прав для capture" });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([downloadUrl, downloadUrl, downloadUrl]);
    expect(fetchMock.mock.calls.every((call) => {
      const headers = new Headers(call[1]?.headers);
      return headers.get("Accept") === "application/json" && !headers.has("Authorization");
    })).toBe(true);
  });
});
describe("P10 real API transport contracts", () => {
  it("creates auth session without sending a service Authorization header", async () => {
    const session: components["schemas"]["SessionInfo"] = { principal_id: "operator-1", role: "operator", expires_at: "2026-09-08T01:00:00Z" };
    fetchMock.mockResolvedValueOnce(jsonResponse(session));

    const response = await createAuthSession("x".repeat(32));

    expect(response.data).toEqual(session);
    const [path, options] = fetchMock.mock.calls[0]!;
    expect(path).toBe("/api/v1/auth/session");
    expect(options?.method).toBe("POST");
    const headers = new Headers(options?.headers);
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(headers.has("Authorization")).toBe(false);
    expect(bodyText(options?.body)).toContain("access_key");
  });

  it("uploads a PDF as multipart with idempotency and exact metadata options", async () => {
    const accepted: components["schemas"]["UploadAccepted"] = {
      document_id: "11111111-1111-4111-8111-111111111111",
      version_id: "22222222-2222-4222-8222-222222222222",
      job_id: "33333333-3333-4333-8333-333333333333",
      links: {
        document: "/api/v1/documents/11111111-1111-4111-8111-111111111111",
        version: "/api/v1/documents/11111111-1111-4111-8111-111111111111/versions/22222222-2222-4222-8222-222222222222",
        job: "/api/v1/ingestion-jobs/33333333-3333-4333-8333-333333333333",
        events: "/api/v1/ingestion-jobs/33333333-3333-4333-8333-333333333333/events",
      },
      status: "queued",
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(accepted, 202));

    const file = new File(["%PDF"], "rules.pdf", { type: "application/pdf" });
    const response = await uploadDocument(file, {
      auto_publish: true,
      expected_current_publication_id: null,
      metadata: {
        title: "Правила",
        legal_status: "active",
        approved_at: "2026-09-08",
        authority: null,
        document_number: null,
        document_type: null,
        edition_at: null,
        effective_from: null,
        effective_to: null,
        schema_version: 1,
        version_label: null,
      },
    }, "idem-upload");

    expect(response.data).toEqual(accepted);
    const [path, options] = fetchMock.mock.calls[0]!;
    expect(path).toBe("/api/v1/documents");
    expect(options?.body).toBeInstanceOf(FormData);
    const headers = new Headers(options?.headers);
    expect(headers.get("Idempotency-Key")).toBe("idem-upload");
    expect(headers.has("Authorization")).toBe(false);
  });

  it("uses canonical ingestion job routes for read, cancel and reindex", async () => {
    const job: components["schemas"]["IngestionJob"] = {
      job_id: "33333333-3333-4333-8333-333333333333",
      version_id: "22222222-2222-4222-8222-222222222222",
      status: "running",
      attempt: 1,
      max_attempts: 3,
      created_at: "2026-09-08T00:00:00Z",
      last_sequence: 9,
      cancel_requested: false,
      progress: { processed_units: 2, total_units: 10, unit: "pages" },
      stage: "parsing",
      started_at: "2026-09-08T00:01:00Z",
      available_at: null,
      finished_at: null,
      error: null,
    };
    const accepted: components["schemas"]["JobCommandAccepted"] = { job_id: job.job_id, status: "queued", last_sequence: 10 };
    fetchMock.mockResolvedValueOnce(jsonResponse(job));
    fetchMock.mockResolvedValueOnce(jsonResponse(accepted, 202));
    fetchMock.mockResolvedValueOnce(jsonResponse(accepted, 202));

    await expect(getIngestionJob(job.job_id)).resolves.toMatchObject({ data: job });
    await expect(cancelIngestionJob(job.job_id)).resolves.toMatchObject({ data: accepted });
    await expect(reindexVersion(job.version_id, {
      pipeline_config_alias: "canonical:default",
      expected_current_publication_id: null,
      auto_publish: true,
    }, "idem-reindex")).resolves.toMatchObject({ data: accepted });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      `/api/v1/ingestion-jobs/${job.job_id}`,
      `/api/v1/ingestion-jobs/${job.job_id}/cancel`,
      `/api/v1/versions/${job.version_id}/reindex`,
    ]);
  });

  it("uses version publish and deactivate command routes with generated DTO shapes", async () => {
    const versionId = "22222222-2222-4222-8222-222222222222";
    const publication: components["schemas"]["PublicationInfo"] = {
      publication_id: "99999999-9999-4999-8999-999999999999",
      document_version_id: versionId,
      index_generation_id: "88888888-8888-4888-8888-888888888888",
      published_at: "2026-09-08T00:03:00Z",
      retired_at: null,
    };
    const metadata: components["schemas"]["DocumentMetadata"] = {
      title: "Правила",
      legal_status: "active",
      approved_at: "2026-09-08",
      authority: null,
      document_number: null,
      document_type: null,
      edition_at: null,
      effective_from: null,
      effective_to: null,
      schema_version: 1,
      version_label: null,
    };
    const deactivated: components["schemas"]["VersionSummary"] = {
      version_id: versionId,
      document_id: "11111111-1111-4111-8111-111111111111",
      metadata,
      publication_status: "deactivated",
      created_at: "2026-09-08T00:01:00Z",
      deactivated_at: "2026-09-08T00:04:00Z",
      published_at: null,
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(publication));
    fetchMock.mockResolvedValueOnce(jsonResponse(deactivated));

    await expect(publishVersion(versionId, {
      index_generation_id: publication.index_generation_id,
      expected_current_publication_id: null,
      operation_id: "77777777-7777-4777-8777-777777777777",
    })).resolves.toMatchObject({ data: publication });
    await expect(deactivateVersion(versionId, { reason: "superseded by newer version" })).resolves.toMatchObject({ data: deactivated });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      `/api/v1/versions/${versionId}/publish`,
      `/api/v1/versions/${versionId}/deactivate`,
    ]);
    const publishOptions = fetchMock.mock.calls[0]![1]!;
    const deactivateOptions = fetchMock.mock.calls[1]![1]!;
    expect(publishOptions.method).toBe("POST");
    expect(deactivateOptions.method).toBe("POST");
    expect(new Headers(publishOptions.headers).get("Authorization")).toBeNull();
    expect(bodyText(publishOptions.body)).toContain("index_generation_id");
    expect(bodyText(deactivateOptions.body)).toContain("superseded by newer version");
  });

  it("validates completed run answers before rendering citations", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const versionId = "22222222-2222-4222-8222-222222222222";
    const snapshot: components["schemas"]["SnapshotInfo"] = { id: "99999999-9999-4999-8999-999999999999", captured_at: "2026-09-08T00:00:02Z", version_count: 1 };
    const answer: components["schemas"]["FinalAnswer"] = {
      kind: "completed",
      result_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      text: "Проверенный ответ.",
      snapshot,
      validation: { status: "confirmed", claim_count: 1, supported_count: 1, repair_used: false },
      claims: [{ claim_id: "claim-1", text: "Требование подтверждено.", citation_ids: ["c1"] }],
      citations: [{ citation_id: "c1", evidence_id: "ev-1", document_title: "Правила", pdf_pages: [1], printed_page_labels: ["1"], source_url: `/api/v1/versions/${versionId}/source`, structural_path: ["раздел 1"], version_label: null }],
    };
    const run: components["schemas"]["PublicRun"] = {
      run_id: runId,
      status: "completed",
      created_at: "2026-09-08T00:00:00Z",
      last_sequence: 8,
      cancel_requested: false,
      current_stage: "finalizing",
      error: null,
      finished_at: "2026-09-08T00:00:08Z",
      result: answer,
      snapshot,
      stage_attempt: 1,
      started_at: "2026-09-08T00:00:01Z",
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(run));
    fetchMock.mockResolvedValueOnce(jsonResponse({ ...run, result: { ...answer, claims: [{ ...answer.claims[0], citation_ids: ["missing"] }] } }));

    await expect(getRun(runId)).resolves.toMatchObject({ data: run });
    await expect(getRun(runId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
  });

  it("rejects unsafe source URLs before rendering public source links", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const versionId = "22222222-2222-4222-8222-222222222222";
    const parseGenerationId = "33333333-3333-4333-8333-333333333333";
    const nodeId = "55555555-5555-4555-8555-555555555555";
    const snapshot: components["schemas"]["SnapshotInfo"] = { id: "99999999-9999-4999-8999-999999999999", captured_at: "2026-09-08T00:00:02Z", version_count: 1 };
    const answer: components["schemas"]["FinalAnswer"] = {
      kind: "completed",
      result_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      text: "Проверенный ответ.",
      snapshot,
      validation: { status: "confirmed", claim_count: 1, supported_count: 1, repair_used: false },
      claims: [{ claim_id: "claim-1", text: "Требование подтверждено.", citation_ids: ["c1"] }],
      citations: [{ citation_id: "c1", evidence_id: "ev-1", document_title: "Правила", pdf_pages: [1], printed_page_labels: ["1"], source_url: "javascript:alert(1)", structural_path: ["раздел 1"], version_label: null }],
    };
    const evidence: components["schemas"]["PublicEvidence"] = {
      evidence_id: "ev-1",
      run_id: runId,
      document_version_id: versionId,
      document_title: "Правила",
      structural_path: ["раздел 1"],
      excerpt: "точный источник",
      source_spans: [{ pdf_page: 1, block_id: "b1", start_offset: 0, end_offset: 7, bbox: null, printed_page_label: null }],
      source_url: "https://private.example/source.pdf",
      version_label: null,
    };
    const mismatchedEvidence: components["schemas"]["PublicEvidence"] = {
      ...evidence,
      source_url: "/api/v1/versions/99999999-9999-4999-8999-999999999999/source",
    };
    const version: components["schemas"]["VersionDetail"] = {
      version_id: versionId,
      document_id: "11111111-1111-4111-8111-111111111111",
      metadata: {
        title: "Правила",
        legal_status: "active",
        approved_at: "2026-09-08",
        authority: null,
        document_number: null,
        document_type: null,
        edition_at: null,
        effective_from: null,
        effective_to: null,
        schema_version: 1,
        version_label: null,
      },
      publication_status: "staging",
      created_at: "2026-09-08T00:01:00Z",
      deactivated_at: null,
      published_at: null,
      current_publication: null,
      generations: [],
      source: {
        version_id: versionId,
        source_url: "//private.example/source.pdf",
        original_filename: "rules.pdf",
        size_bytes: 4096,
        sha256: "a".repeat(64),
        media_type: "application/pdf",
        page_count: 12,
      },
    };
    const quality: components["schemas"]["ParseQualityPage"] = {
      version_id: versionId,
      parse_generation_id: parseGenerationId,
      source_url: "/api/v1/versions/../source",
      next_cursor: null,
      summary: { status: "passed", warning_count: 0, reason_codes: [] },
      items: [],
    };
    const table: components["schemas"]["StructuredTablePage"] = {
      version_id: versionId,
      parse_generation_id: parseGenerationId,
      node_id: nodeId,
      table_id: "table-1",
      kind: "data",
      source_url: "/api/v1/versions/22222222-2222-4222-8222-222222222222/source\\evil",
      row_start: 0,
      row_end: 0,
      total_rows: 0,
      column_count: 1,
      pdf_pages: [1],
      next_cursor: null,
      rows: [],
      cells: [],
      context_refs: [],
      page_segments: [],
    };
    fetchMock.mockResolvedValueOnce(jsonResponse({ run_id: runId, status: "completed", created_at: "2026-09-08T00:00:00Z", last_sequence: 8, cancel_requested: false, current_stage: "finalizing", error: null, finished_at: "2026-09-08T00:00:08Z", result: answer, snapshot, stage_attempt: 1, started_at: "2026-09-08T00:00:01Z" } satisfies components["schemas"]["PublicRun"]));
    fetchMock.mockResolvedValueOnce(jsonResponse(evidence));
    fetchMock.mockResolvedValueOnce(jsonResponse(mismatchedEvidence));
    fetchMock.mockResolvedValueOnce(jsonResponse(version));
    fetchMock.mockResolvedValueOnce(jsonResponse(quality));
    fetchMock.mockResolvedValueOnce(jsonResponse(table));

    await expect(getRun(runId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(getSourceEvidence(runId, "ev-1")).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(getSourceEvidence(runId, "ev-1")).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(getVersion(versionId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(getParseQuality(versionId, parseGenerationId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    await expect(getStructuredTable(versionId, nodeId, parseGenerationId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
  });

  it("reads run history with cursor pagination", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const history: components["schemas"]["RunList"] = {
      items: [{ run_id: runId, question_excerpt: "Норма", status: "completed", created_at: "2026-09-08T00:00:00Z", duration_ms: 1200 }],
      next_cursor: "run-cursor-2",
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(history));

    const result = await listRuns({ cursor: "run-cursor-1", limit: 50 });

    expect(result.data).toEqual(history);
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual(["/api/v1/runs?cursor=run-cursor-1&limit=50"]);
  });

  it("rejects malformed debug steps before operator diagnostics render", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    fetchMock.mockResolvedValueOnce(jsonResponse({
      run_id: runId,
      steps: [{
        attempt: 1,
        candidate_count: -1,
        duration_ms: 10,
        input_tokens: 20,
        output_tokens: 30,
        occurred_at: "2026-09-08T00:00:03Z",
        stage: "retrieving",
        status: "completed",
      }],
      trace_id: null,
      trace_url: null,
      capture: null,
    } satisfies components["schemas"]["RunDebug"]));

    await expect(getRunDebug(runId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
  });

  it("accepts debug steps with omitted optional metrics as null", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    fetchMock.mockResolvedValueOnce(jsonResponse({
      run_id: runId,
      steps: [{
        attempt: 1,
        occurred_at: "2026-09-08T00:00:03Z",
        stage: "retrieving",
        status: "completed",
      }],
      trace_id: null,
      trace_url: null,
      capture: null,
    }));

    await expect(getRunDebug(runId)).resolves.toMatchObject({
      data: {
        steps: [{
          attempt: 1,
          candidate_count: null,
          duration_ms: null,
          input_tokens: null,
          output_tokens: null,
          occurred_at: "2026-09-08T00:00:03Z",
          stage: "retrieving",
          status: "completed",
        }],
      },
    });
  });

  it("uses the coordinated P09 public run and source routes without synthetic answers", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    const versionId = "22222222-2222-4222-8222-222222222222";
    const accepted: components["schemas"]["RunAccepted"] = { run_id: runId, status: "created", last_sequence: 1, links: { self: `/api/v1/runs/${runId}`, events: `/api/v1/runs/${runId}/events` } };
    const run: components["schemas"]["PublicRun"] = { run_id: runId, status: "running", created_at: "2026-09-08T00:00:00Z", last_sequence: 2, cancel_requested: false, current_stage: "retrieving", error: null, finished_at: null, result: null, snapshot: null, stage_attempt: 1, started_at: "2026-09-08T00:00:01Z" };
    const evidence: components["schemas"]["PublicEvidence"] = {
      evidence_id: "ev-1",
      run_id: runId,
      document_version_id: versionId,
      document_title: "Правила",
      structural_path: ["раздел 1"],
      excerpt: "точный источник",
      source_spans: [{ pdf_page: 1, block_id: "b1", start_offset: 0, end_offset: 7, bbox: null, printed_page_label: null }],
      source_url: `/api/v1/versions/${versionId}/source`,
      version_label: null,
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(accepted, 202));
    fetchMock.mockResolvedValueOnce(jsonResponse(run));
    fetchMock.mockResolvedValueOnce(jsonResponse(evidence));

    await expect(createRun("Норма <script>alert(1)</script>", false, "idem-run")).resolves.toMatchObject({ data: accepted });
    await expect(getRun(runId)).resolves.toMatchObject({ data: run });
    await expect(getSourceEvidence(runId, "ev-1")).resolves.toMatchObject({ data: evidence });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/runs",
      `/api/v1/runs/${runId}`,
      `/api/v1/runs/${runId}/sources/ev-1`,
    ]);
    expect(answerHasDraftLeak(evidence.excerpt)).toBe(false);
  });

  it("rejects malformed source spans before source drawer rendering", async () => {
    const runId = "44444444-4444-4444-8444-444444444444";
    fetchMock.mockResolvedValueOnce(jsonResponse({
      evidence_id: "ev-1",
      run_id: runId,
      document_version_id: "22222222-2222-4222-8222-222222222222",
      document_title: "Правила",
      structural_path: ["раздел 1"],
      excerpt: "точный источник",
      source_spans: [{ pdf_page: 0, block_id: "b1", start_offset: 7, end_offset: 3, bbox: [0, 1, 2, 3], printed_page_label: null }],
      source_url: `/api/v1/runs/${runId}/sources/ev-1/pdf`,
      version_label: null,
    }));

    await expect(getSourceEvidence(runId, "ev-1")).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
  });

  it("reads backend document library/detail/version routes using generated component schemas", async () => {
    const documentId = "11111111-1111-4111-8111-111111111111";
    const versionId = "22222222-2222-4222-8222-222222222222";
    const metadata: components["schemas"]["DocumentMetadata"] = {
      title: "Правила",
      legal_status: "active",
      approved_at: "2026-09-08",
      authority: "Орган",
      document_number: "1",
      document_type: null,
      edition_at: null,
      effective_from: null,
      effective_to: null,
      schema_version: 1,
      version_label: "ред. 1",
    };
    const summary: components["schemas"]["DocumentSummary"] = {
      document_id: documentId,
      canonical_title: "Правила",
      version_count: 1,
      created_at: "2026-09-08T00:00:00Z",
      archived_at: null,
      authority: "Орган",
      current_publication: null,
      document_number: "1",
      document_type: null,
      security_revoked: false,
    };
    const versionSummary: components["schemas"]["VersionSummary"] = {
      version_id: versionId,
      document_id: documentId,
      metadata,
      publication_status: "staging",
      created_at: "2026-09-08T00:01:00Z",
      deactivated_at: null,
      published_at: null,
    };
    const detail: components["schemas"]["DocumentDetail"] = { ...summary, versions: [versionSummary], next_versions_cursor: "version-cursor-2" };
    const version: components["schemas"]["VersionDetail"] = {
      ...versionSummary,
      current_publication: null,
      generations: [],
      source: {
        version_id: versionId,
        source_url: `/api/v1/versions/${versionId}/source`,
        original_filename: "rules.pdf",
        size_bytes: 4096,
        sha256: "a".repeat(64),
        media_type: "application/pdf",
        page_count: 12,
      },
    };
    fetchMock.mockResolvedValueOnce(jsonResponse({ items: [summary], next_cursor: "doc-cursor-2" } satisfies components["schemas"]["DocumentList"]));
    fetchMock.mockResolvedValueOnce(jsonResponse(detail));
    fetchMock.mockResolvedValueOnce(jsonResponse(version));

    await expect(listDocuments({ q: "Правила", cursor: "doc-cursor-1", limit: 50 })).resolves.toMatchObject({ data: { items: [summary], next_cursor: "doc-cursor-2" } });
    await expect(getDocument(documentId, { versionsCursor: "version-cursor-1", versionsLimit: 50 })).resolves.toMatchObject({ data: detail });
    await expect(getVersion(versionId)).resolves.toMatchObject({ data: version });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/documents?limit=50&status=all&q=%D0%9F%D1%80%D0%B0%D0%B2%D0%B8%D0%BB%D0%B0&cursor=doc-cursor-1",
      `/api/v1/documents/${documentId}?versions_cursor=version-cursor-1&versions_limit=50`,
      `/api/v1/versions/${versionId}`,
    ]);
  });

  it("reads library profile and archives selected logical document with CAS body", async () => {
    const documentId = "11111111-1111-4111-8111-111111111111";
    const publicationId = "22222222-2222-4222-8222-222222222222";
    const operationId = "33333333-3333-4333-8333-333333333333";
    const archived: components["schemas"]["DocumentSummary"] = {
      document_id: documentId,
      canonical_title: "Правила",
      version_count: 2,
      created_at: "2026-09-08T00:00:00Z",
      archived_at: "2026-09-08T01:00:00Z",
      authority: "Орган",
      current_publication: {
        publication_id: publicationId,
        document_version_id: "44444444-4444-4444-8444-444444444444",
        index_generation_id: "55555555-5555-4555-8555-555555555555",
        published_at: "2026-09-08T00:30:00Z",
      },
      document_number: "1",
      document_type: "Приказ",
      security_revoked: false,
    };
    fetchMock.mockResolvedValueOnce(jsonResponse({
      logical_document_count: 7,
      version_count: 11,
      eligible_document_count: 5,
      last_publication_at: "2026-09-08T00:30:00Z",
    } satisfies components["schemas"]["LibraryProfile"]));
    fetchMock.mockResolvedValueOnce(jsonResponse(archived));

    await expect(getLibraryProfile()).resolves.toMatchObject({ data: { logical_document_count: 7, version_count: 11, eligible_document_count: 5 } });
    await expect(archiveDocument(documentId, {
      expected_current_publication_id: publicationId,
      operation_id: operationId,
      reason: "утратил силу",
    })).resolves.toMatchObject({ data: archived });
    fetchMock.mockResolvedValueOnce(jsonResponse({
      logical_document_count: 1,
      version_count: 11,
      eligible_document_count: 2,
      last_publication_at: null,
    } satisfies components["schemas"]["LibraryProfile"]));
    await expect(getLibraryProfile()).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      "/api/v1/library/profile",
      `/api/v1/documents/${documentId}/archive`,
      "/api/v1/library/profile",
    ]);
    expect(JSON.parse(bodyText(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      expected_current_publication_id: publicationId,
      operation_id: operationId,
      reason: "утратил силу",
    });
  });

  it("builds and submits admin source purge only through the displayed plan identity", async () => {
    const documentId = "11111111-1111-4111-8111-111111111111";
    const planId = "22222222-2222-4222-8222-222222222222";
    const plan: components["schemas"]["PurgePlan"] = {
      allowed: true,
      blockers: [],
      created_at: "2026-09-08T00:00:00Z",
      document_id: documentId,
      eligible_after: null,
      expires_at: "2026-09-08T00:10:00Z",
      plan_id: planId,
      plan_version: 3,
      references: { active_ingestion_jobs: 0, active_runs: 0, checkpoints: 0, objects: 4, pending_reservations: 0, results: 0, snapshots: 0 },
      retention_policy: "p14.purge.v1",
    };
    const accepted: components["schemas"]["PurgeAccepted"] = {
      document_id: documentId,
      plan_id: planId,
      status: "purge_pending",
    };
    const status: components["schemas"]["PurgeStatus"] = {
      accepted_at: "2026-09-08T00:01:00Z",
      completed_at: null,
      created_at: "2026-09-08T00:00:00Z",
      deleted_object_count: 1,
      document_id: documentId,
      error_code: null,
      expires_at: "2026-09-08T00:10:00Z",
      plan_id: planId,
      plan_version: 3,
      status: "purge_pending",
      total_object_count: 4,
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(plan));
    fetchMock.mockResolvedValueOnce(jsonResponse(accepted, 202));
    fetchMock.mockResolvedValueOnce(jsonResponse(status));

    await expect(createPurgePlan(documentId)).resolves.toMatchObject({ data: plan });
    await expect(submitPurgePlan(documentId, { plan_id: planId, plan_version: 3 })).resolves.toMatchObject({ data: accepted });
    await expect(getPurgeStatus(documentId, planId)).resolves.toMatchObject({ data: status });

    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      `/api/v1/documents/${documentId}/purge-plan`,
      `/api/v1/documents/${documentId}/purge`,
      `/api/v1/documents/${documentId}/purge-plans/${planId}`,
    ]);
    expect(JSON.parse(bodyText(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({ plan_id: planId, plan_version: 3 });
    expect(new Headers(fetchMock.mock.calls[1]?.[1]?.headers).has("Authorization")).toBe(false);

    fetchMock.mockResolvedValueOnce(jsonResponse({
      ...plan,
      references: { ...plan.references, snapshots: 1 },
    } satisfies components["schemas"]["PurgePlan"]));
    await expect(createPurgePlan(documentId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
  });

  it("rejects stale purge identities before the destructive UI can reuse them", () => {
    const documentId = "11111111-1111-4111-8111-111111111111";
    const otherDocumentId = "33333333-3333-4333-8333-333333333333";
    const planId = "22222222-2222-4222-8222-222222222222";
    const plan: Pick<components["schemas"]["PurgePlan"], "document_id" | "plan_id" | "plan_version"> = {
      document_id: documentId,
      plan_id: planId,
      plan_version: 3,
    };
    const accepted: Pick<components["schemas"]["PurgeAccepted"], "document_id" | "plan_id"> = {
      document_id: documentId,
      plan_id: planId,
    };
    const status: Pick<components["schemas"]["PurgeStatus"], "document_id" | "plan_id" | "plan_version"> = {
      document_id: documentId,
      plan_id: planId,
      plan_version: 3,
    };

    expect(purgePlanMatches(plan, { documentId })).toBe(true);
    expect(purgePlanMatches(plan, { documentId: otherDocumentId })).toBe(false);
    expect(purgePlanMatches(plan, { documentId, planId, planVersion: 4 })).toBe(false);
    expect(purgeAcceptedMatches(accepted, { documentId, planId })).toBe(true);
    expect(purgeAcceptedMatches(accepted, { documentId: otherDocumentId, planId })).toBe(false);
    expect(purgeStatusMatches(status, { documentId, planId, planVersion: 3 })).toBe(true);
    expect(purgeStatusMatches(status, { documentId, planId, planVersion: 2 })).toBe(false);
  });


  it("reads canonical tree, quality and structured table pages from frozen version routes", async () => {
    const versionId = "22222222-2222-4222-8222-222222222222";
    const parseGenerationId = "33333333-3333-4333-8333-333333333333";
    const nodeId = "44444444-4444-4444-8444-444444444444";
    const sourceSpan: components["schemas"]["SourceSpan"] = { pdf_page: 3, block_id: "b-1", start_offset: 0, end_offset: 6, bbox: [1, 2, 3, 4], printed_page_label: "3" };
    const tree: components["schemas"]["CanonicalTreePage"] = {
      version_id: versionId,
      parse_generation_id: parseGenerationId,
      parent_id: null,
      next_cursor: "tree-cursor-2",
      items: [{ node_id: nodeId, parent_id: null, node_type: "table", number: "1", title: "Таблица", ordinal: 0, page_start: 3, page_end: 4, has_children: false }],
    };
    const quality: components["schemas"]["ParseQualityPage"] = {
      version_id: versionId,
      parse_generation_id: parseGenerationId,
      source_url: `/api/v1/versions/${versionId}/source`,
      next_cursor: "quality-cursor-2",
      summary: { status: "warning", warning_count: 1, reason_codes: ["LOW_CONFIDENCE_TABLE"] },
      items: [{ ordinal: 0, severity: "warning", code: "LOW_CONFIDENCE_TABLE", pdf_page: 3, block_id: "b-1", bbox: [1, 2, 3, 4] }],
    };
    const table: components["schemas"]["StructuredTablePage"] = {
      version_id: versionId,
      parse_generation_id: parseGenerationId,
      node_id: nodeId,
      table_id: "table-1",
      kind: "data",
      source_url: `/api/v1/versions/${versionId}/source`,
      row_start: 0,
      row_end: 0,
      total_rows: 1,
      column_count: 1,
      pdf_pages: [3],
      next_cursor: "table-cursor-2",
      page_segments: [{ pdf_page: 3, first_row: 0, last_row: 0, bbox: [1, 2, 3, 4] }],
      context_refs: [{ role: "caption", text: "Нормы", required: true, source_spans: [sourceSpan] }],
      rows: [{ row_index: 0, cell_ids: ["cell-1"], context_refs: [] }],
      cells: [{ id: "cell-1", row: 0, column: 0, row_span: 1, column_span: 1, role: "data", text: "±3", pdf_page: 3, bbox: null, source_spans: [sourceSpan] }],
    };
    fetchMock.mockResolvedValueOnce(jsonResponse(tree));
    fetchMock.mockResolvedValueOnce(jsonResponse(quality));
    fetchMock.mockResolvedValueOnce(jsonResponse(table));
    fetchMock.mockResolvedValueOnce(jsonResponse({ ...table, rows: [{ ...table.rows[0], cell_ids: ["missing-cell"] }] }));

    await expect(getCanonicalTree(versionId, parseGenerationId, { parentId: null, cursor: "tree-cursor-1", limit: 50 })).resolves.toMatchObject({ data: tree });
    await expect(getParseQuality(versionId, parseGenerationId, { cursor: "quality-cursor-1", limit: 50 })).resolves.toMatchObject({ data: quality });
    await expect(getStructuredTable(versionId, nodeId, parseGenerationId, { cursor: "table-cursor-1", limit: 25 })).resolves.toMatchObject({ data: table });
    await expect(getStructuredTable(versionId, nodeId, parseGenerationId)).rejects.toMatchObject({ kind: "invalid_response", httpStatus: 200 });
    expect(fetchMock.mock.calls.map((call) => call[0])).toEqual([
      `/api/v1/versions/${versionId}/tree?parse_generation_id=${parseGenerationId}&cursor=tree-cursor-1&limit=50`,
      `/api/v1/versions/${versionId}/quality?parse_generation_id=${parseGenerationId}&cursor=quality-cursor-1&limit=50`,
      `/api/v1/versions/${versionId}/tables/${nodeId}?parse_generation_id=${parseGenerationId}&cursor=table-cursor-1&limit=25`,
      `/api/v1/versions/${versionId}/tables/${nodeId}?parse_generation_id=${parseGenerationId}&limit=25`,
    ]);
    expect(fetchMock.mock.calls.every((call) => new Headers(call[1]?.headers).has("Authorization") === false)).toBe(true);
  });
});

describe("P10 public run event stream contracts", () => {
  const runId = "44444444-4444-4444-8444-444444444444";
  const otherRunId = "66666666-6666-4666-8666-666666666666";
  const event = {
    schema_version: 1,
    event_id: "55555555-5555-4555-8555-555555555555",
    run_id: runId,
    sequence: 3,
    execution_epoch: 0,
    attempt: 1,
    occurred_at: "2026-09-08T00:00:03Z",
    type: "stage.started",
    stage: "retrieving",
    data: { message_code: "RUN_RETRIEVING" },
  } satisfies components["schemas"]["StageStartedEvent"];
  const run = (changes: Partial<components["schemas"]["PublicRun"]> = {}) => ({
    run_id: runId,
    status: "running",
    created_at: "2026-09-08T00:00:00Z",
    last_sequence: 2,
    cancel_requested: false,
    current_stage: "retrieving",
    error: null,
    finished_at: null,
    result: null,
    snapshot: null,
    stage_attempt: 1,
    started_at: "2026-09-08T00:00:01Z",
    ...changes,
  }) satisfies components["schemas"]["PublicRun"];

  it("accepts only well-formed same-run events", () => {
    expect(parseRunEvent(JSON.stringify(event), runId)).toEqual(event);
    expect(parseRunEvent("{", runId)).toBeNull();
    expect(parseRunEvent(JSON.stringify({ ...event, run_id: "66666666-6666-4666-8666-666666666666" }), runId)).toBeNull();
    expect(parseRunEvent(JSON.stringify({ ...event, type: "draft.delta" }), runId)).toBeNull();
    expect(parseRunEvent(JSON.stringify({ ...event, stage: "PRIVATE_STAGE" }), runId)).toBeNull();
  });

  it("deduplicates replayed or stale events by sequence", () => {
    const initial: RunEventState = { connection: "connected", lastSequence: 2, events: [] };
    const next = reduceRunEvent(initial, event);

    expect(next.lastSequence).toBe(3);
    expect(next.events).toEqual([event]);
    expect(reduceRunEvent(next, event)).toBe(next);
    expect(reduceRunEvent(next, { ...event, event_id: "77777777-7777-4777-8777-777777777777", sequence: 2 })).toBe(next);
  });

  it("derives live run progress from newer same-run stage events", () => {
    const newer = { ...event, event_id: "77777777-7777-4777-8777-777777777777", sequence: 5, stage: "validating", attempt: 2,
      data: { message_code: "RUN_VALIDATING" } } satisfies components["schemas"]["StageStartedEvent"];
    const state: RunEventState = { connection: "connected", lastSequence: 5, events: [newer] };

    expect(liveRunProgress(run(), state)).toEqual({ current_stage: "validating", stage_attempt: 2 });
  });

  it("keeps REST progress when no newer stage messages exist", () => {
    expect(liveRunProgress(run(), { connection: "connected", lastSequence: 2, events: [] }))
      .toEqual({ current_stage: "retrieving", stage_attempt: 1 });
    expect(liveRunProgress(run({ last_sequence: 5, current_stage: "validating", stage_attempt: 2 }), {
      connection: "connected", lastSequence: 5, events: [event],
    })).toEqual({ current_stage: "validating", stage_attempt: 2 });
  });

  it("ignores cross-run events and lets terminal REST snapshots own display state", () => {
    const crossRun = { ...event, run_id: otherRunId, event_id: "77777777-7777-4777-8777-777777777777", sequence: 5, stage: "validating", attempt: 2,
      data: { message_code: "RUN_VALIDATING" } } satisfies components["schemas"]["StageStartedEvent"];
    const terminalRun = run({ status: "completed", current_stage: "rendering", stage_attempt: 3, last_sequence: 6 });
    const sameRunAfterTerminal = { ...event, event_id: "88888888-8888-4888-8888-888888888888", sequence: 7, stage: "finalizing", attempt: 4,
      data: { message_code: "RUN_FINALIZING" } } satisfies components["schemas"]["StageStartedEvent"];

    expect(liveRunProgress(run(), { connection: "connected", lastSequence: 5, events: [crossRun] }))
      .toEqual({ current_stage: "retrieving", stage_attempt: 1 });
    expect(liveRunProgress(terminalRun, { connection: "connected", lastSequence: 7, events: [sameRunAfterTerminal] }))
      .toEqual({ current_stage: "rendering", stage_attempt: 3 });
  });

  it("opens native EventSource with same-origin cookies and after cursor", () => {
    const updates: string[] = [];
    class FakeEventSource {
      static instances: FakeEventSource[] = [];
      url: string;
      withCredentials: boolean;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onmessage: ((message: MessageEvent<string>) => void) | null = null;
      private listeners = new Map<string, Array<(payload?: string) => void>>();
      closed = false;

      constructor(url: string | URL, configuration?: EventSourceInit) {
        this.url = String(url);
        this.withCredentials = configuration?.withCredentials ?? false;
        FakeEventSource.instances.push(this);
      }

      addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
        const callbacks = this.listeners.get(type) ?? [];
        callbacks.push((payload?: string) => {
          const event = { type, data: payload ?? "" } as MessageEvent<string>;
          if (typeof listener === "function") listener(event);
          else listener.handleEvent(event);
        });
        this.listeners.set(type, callbacks);
      }

      dispatch(type: string, payload?: string) {
        for (const listener of this.listeners.get(type) ?? []) listener(payload);
      }

      close() {
        this.closed = true;
      }
    }
    vi.stubGlobal("EventSource", FakeEventSource);

    const subscription = subscribeRunEvents(runId, 2, (update) => {
      if (update.kind === "connection") updates.push(update.connection);
      else updates.push(update.event.type);
    });
    const instance = FakeEventSource.instances[0]!;
    instance.onopen?.();
    instance.dispatch("stage.started", JSON.stringify(event));
    instance.onerror?.();
    instance.onmessage?.({ data: JSON.stringify({ ...event, event_id: "88888888-8888-4888-8888-888888888888", sequence: 4 }) } as MessageEvent<string>);
    instance.dispatch("history_expired");
    subscription.close();

    expect(instance.url).toBe(`/api/v1/runs/${runId}/events?after=2`);
    expect(instance.withCredentials).toBe(true);
    expect(updates).toEqual(["connected", "stage.started", "reconnecting", "stage.started", "history_expired"]);
    expect(instance.closed).toBe(true);
  });

  it("marks terminal run events as closed instead of reconnecting after EventSource close", () => {
    const updates: string[] = [];
    class FakeEventSource {
      static instances: FakeEventSource[] = [];
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onmessage: ((message: MessageEvent<string>) => void) | null = null;
      private listeners = new Map<string, Array<(payload?: string) => void>>();
      closed = false;

      constructor(...args: [string | URL, EventSourceInit?]) {
        void args;
        FakeEventSource.instances.push(this);
      }

      addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
        const callbacks = this.listeners.get(type) ?? [];
        callbacks.push((payload?: string) => {
          const event = { type, data: payload ?? "" } as MessageEvent<string>;
          if (typeof listener === "function") listener(event);
          else listener.handleEvent(event);
        });
        this.listeners.set(type, callbacks);
      }

      dispatch(type: string, payload?: string) {
        for (const listener of this.listeners.get(type) ?? []) listener(payload);
      }

      close() {
        this.closed = true;
      }
    }
    vi.stubGlobal("EventSource", FakeEventSource);

    subscribeRunEvents(runId, 2, (update) => {
      if (update.kind === "connection") updates.push(update.connection);
      else updates.push(update.event.type);
    });
    const terminal = {
      ...event,
      event_id: "99999999-9999-4999-8999-999999999999",
      sequence: 20,
      stage: "finalizing",
      type: "run.completed",
      data: {
        citation_count: 6,
        claim_count: 2,
        result_id: "44444444-4444-4444-8444-444444444444",
      },
    } satisfies components["schemas"]["RunCompletedEvent"];
    const instance = FakeEventSource.instances[0]!;

    instance.onopen?.();
    instance.dispatch("run.completed", JSON.stringify(terminal));
    instance.onerror?.();

    expect(updates).toEqual(["connected", "run.completed", "closed"]);
    expect(instance.closed).toBe(true);
  });

  it("reports browser stream support boundaries in user-facing copy", () => {
    expect(eventStatusText({ connection: "unsupported", lastSequence: 0, events: [] })).toContain("SSE недоступен");
    expect(eventStatusText({ connection: "history_expired", lastSequence: 0, events: [] })).toContain("перечитайте run");
    expect(eventStatusText({ connection: "closed", lastSequence: 20, events: [] })).toContain("закрыт");
  });
});

describe("P14 question composer state contracts", () => {
  it("collapses only accepted or loaded runs until the operator opens it again", () => {
    expect(questionComposerVisible({ expanded: true, runPhase: "idle" })).toBe(true);
    expect(questionComposerVisible({ expanded: false, runPhase: "idle" })).toBe(true);
    expect(questionComposerVisible({ expanded: false, runPhase: "error" })).toBe(true);
    expect(questionComposerVisible({ expanded: false, runPhase: "loading" })).toBe(false);
    expect(questionComposerVisible({ expanded: false, runPhase: "loaded" })).toBe(false);
    expect(questionComposerVisible({ expanded: true, runPhase: "loaded" })).toBe(true);
  });

  it("binds version upload and version commands to the selected identity", () => {
    const document = { document_id: "11111111-1111-4111-8111-111111111111", current_publication: null };
    const targetDocumentId = "22222222-2222-4222-8222-222222222222";

    expect(selectedDocumentReadyForVersionUpload(document, document.document_id)).toBe(true);
    expect(selectedDocumentReadyForVersionUpload(document, targetDocumentId)).toBe(false);
    expect(selectedDocumentReadyForVersionUpload(null, targetDocumentId)).toBe(false);
    expect(selectedVersionStillTargetsAction("33333333-3333-4333-8333-333333333333", "33333333-3333-4333-8333-333333333333")).toBe(true);
    expect(selectedVersionStillTargetsAction("33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444")).toBe(false);
  });
});

describe("P14 ingestion job event stream contracts", () => {
  const jobId = "33333333-3333-4333-8333-333333333333";
  const event = {
    schema_version: 1,
    event_id: "55555555-5555-4555-8555-555555555555",
    job_id: jobId,
    sequence: 3,
    execution_epoch: 0,
    attempt: 1,
    occurred_at: "2026-09-08T00:00:03Z",
    type: "stage.progress",
    stage: "parsing",
    data: { processed_units: 1, total_units: 2, unit: "pages" },
  } satisfies components["schemas"]["IngestionProgressEvent"];

  it("accepts only well-formed same-job ingestion events", () => {
    expect(parseJobEvent(JSON.stringify(event), jobId)).toEqual(event);
    expect(parseJobEvent("{", jobId)).toBeNull();
    expect(parseJobEvent(JSON.stringify({ ...event, job_id: "66666666-6666-4666-8666-666666666666" }), jobId)).toBeNull();
    expect(parseJobEvent(JSON.stringify({ ...event, type: "PRIVATE_STAGE" }), jobId)).toBeNull();
    expect(parseJobEvent(JSON.stringify({ ...event, stage: "PRIVATE_STAGE" }), jobId)).toBeNull();
  });

  it("deduplicates job events and identifies terminal ingestion events", () => {
    const initial: JobEventState = { connection: "connected", lastSequence: 2, events: [] };
    const next = reduceJobEvent(initial, event);
    const terminal = {
      ...event,
      event_id: "77777777-7777-4777-8777-777777777777",
      sequence: 4,
      type: "ingestion.completed",
      stage: null,
      data: {
        version_id: "22222222-2222-4222-8222-222222222222",
        index_generation_id: "88888888-8888-4888-8888-888888888888",
        publication_id: null,
      },
    } satisfies components["schemas"]["IngestionCompletedEvent"];

    expect(next.lastSequence).toBe(3);
    expect(next.events).toEqual([event]);
    expect(reduceJobEvent(next, event)).toBe(next);
    expect(isTerminalJobEvent(event)).toBe(false);
    expect(isTerminalJobEvent(terminal)).toBe(true);
  });

  it("opens native job EventSource with same-origin cookies and after cursor", () => {
    class FakeEventSource {
      static instances: FakeEventSource[] = [];
      url: string;
      withCredentials: boolean;
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;
      onmessage: ((message: MessageEvent<string>) => void) | null = null;
      private listeners = new Map<string, Array<(payload?: string) => void>>();
      closed = false;

      constructor(url: string | URL, configuration?: EventSourceInit) {
        this.url = String(url);
        this.withCredentials = configuration?.withCredentials ?? false;
        FakeEventSource.instances.push(this);
      }

      addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
        const callbacks = this.listeners.get(type) ?? [];
        callbacks.push((payload?: string) => {
          const emitted = { type, data: payload ?? "" } as MessageEvent<string>;
          if (typeof listener === "function") listener(emitted);
          else listener.handleEvent(emitted);
        });
        this.listeners.set(type, callbacks);
      }

      dispatch(type: string, payload?: string) {
        for (const listener of this.listeners.get(type) ?? []) listener(payload);
      }

      close() {
        this.closed = true;
      }
    }
    const updates: string[] = [];
    vi.stubGlobal("EventSource", FakeEventSource);

    const subscription = subscribeJobEvents(jobId, 2, (update) => {
      if (update.kind === "connection") updates.push(update.connection);
      else updates.push(update.event.type);
    });
    const instance = FakeEventSource.instances[0]!;
    instance.onopen?.();
    instance.dispatch("stage.progress", JSON.stringify(event));
    instance.dispatch("history_expired");
    subscription.close();

    expect(instance.url).toBe(`/api/v1/ingestion-jobs/${jobId}/events?after=2`);
    expect(instance.withCredentials).toBe(true);
    expect(updates).toEqual(["connected", "stage.progress", "history_expired"]);
    expect(instance.closed).toBe(true);
  });
});

describe("Workspace behavior", () => {
  it("honors explicit null parent navigation when returning to the tree root", () => {
    const currentParentId = "44444444-4444-4444-8444-444444444444";

    expect(resolveRequestedTreeParentId(undefined, currentParentId)).toBe(currentParentId);
    expect(resolveRequestedTreeParentId(null, currentParentId)).toBeNull();
    expect(resolveRequestedTreeParentId("55555555-5555-4555-8555-555555555555", currentParentId)).toBe("55555555-5555-4555-8555-555555555555");
  });

  it("keeps cursor page history bounded when navigating forward after going back", () => {
    const first = { cursors: [null], index: 0 };
    const second = nextCursorPage(first, "cursor-2");
    const third = nextCursorPage(second, "cursor-3");
    const back = previousCursorPage(third);
    const branch = nextCursorPage(back, "cursor-2b");

    expect(currentCursorPage(first)).toBeNull();
    expect(currentCursorPage(second)).toBe("cursor-2");
    expect(currentCursorPage(back)).toBe("cursor-2");
    expect(branch).toEqual({ cursors: [null, "cursor-2", "cursor-2b"], index: 2 });
    expect(currentCursorPage(branch)).toBe("cursor-2b");
  });

  it("binds document cursor pages to the applied query instead of the draft input", () => {
    const first = resolveDocumentListRequest({ query: "  rules  ", cursor: null, cursors: [null], cursorIndex: 0 }, "");
    const next = resolveDocumentListRequest({ cursor: "cursor-for-rules", cursors: [null, "cursor-for-rules"], cursorIndex: 1 }, first.query);
    const retry = resolveDocumentListRequest(next, "law typed after first page");

    expect(first).toEqual({ query: "rules", cursor: null, cursors: [null], cursorIndex: 0 });
    expect(next).toEqual({ query: "rules", cursor: "cursor-for-rules", cursors: [null, "cursor-for-rules"], cursorIndex: 1 });
    expect(retry.query).toBe("rules");
  });

  it("rejects stale structure, quality and table page identities before UI state replacement", () => {
    const versionId = "22222222-2222-4222-8222-222222222222";
    const parseGenerationId = "33333333-3333-4333-8333-333333333333";
    const parentId = "44444444-4444-4444-8444-444444444444";
    const nodeId = "55555555-5555-4555-8555-555555555555";

    expect(canonicalTreePageMatches({ version_id: versionId, parse_generation_id: parseGenerationId, parent_id: parentId }, { versionId, parseGenerationId, parentId })).toBe(true);
    expect(canonicalTreePageMatches({ version_id: versionId, parse_generation_id: parseGenerationId, parent_id: null }, { versionId, parseGenerationId, parentId })).toBe(false);
    expect(qualityPageMatches({ version_id: versionId, parse_generation_id: parseGenerationId }, { versionId, parseGenerationId })).toBe(true);
    expect(qualityPageMatches({ version_id: versionId, parse_generation_id: parentId }, { versionId, parseGenerationId })).toBe(false);
    expect(structuredTablePageMatches({ version_id: versionId, parse_generation_id: parseGenerationId, node_id: nodeId }, { versionId, parseGenerationId, nodeId })).toBe(true);
    expect(structuredTablePageMatches({ version_id: versionId, parse_generation_id: parseGenerationId, node_id: parentId }, { versionId, parseGenerationId, nodeId })).toBe(false);
  });

  it("rejects document and version responses that do not match the pending selection", () => {
    const documentId = "11111111-1111-4111-8111-111111111111";
    const otherDocumentId = "99999999-9999-4999-8999-999999999999";
    const versionId = "22222222-2222-4222-8222-222222222222";
    const otherVersionId = "88888888-8888-4888-8888-888888888888";

    expect(documentDetailMatches({ document_id: documentId }, documentId)).toBe(true);
    expect(documentDetailMatches({ document_id: otherDocumentId }, documentId)).toBe(false);
    expect(versionDetailMatches({ version_id: versionId, document_id: documentId }, { versionId, documentId })).toBe(true);
    expect(versionDetailMatches({ version_id: otherVersionId, document_id: documentId }, { versionId, documentId })).toBe(false);
    expect(versionDetailMatches({ version_id: versionId, document_id: otherDocumentId }, { versionId, documentId })).toBe(false);
  });

  it("opens source evidence by evidence_id instead of claim citation_id", () => {
    const evidenceId = "77777777-7777-4777-8777-777777777777";
    const runId = "66666666-6666-4666-8666-666666666666";

    expect(evidenceIdForClaimCitation([{ citation_id: "C001", evidence_id: evidenceId }], "C001")).toBe(evidenceId);
    expect(evidenceIdForClaimCitation([{ citation_id: "C001", evidence_id: evidenceId }], "C404")).toBeNull();
    expect(evidenceIdForClaimCitation([{ citation_id: "C001", evidence_id: evidenceId }], "C001")).not.toBe("C001");
    expect(publicEvidenceMatches({ run_id: runId, evidence_id: evidenceId }, { runId, evidenceId })).toBe(true);
    expect(publicEvidenceMatches({ run_id: runId, evidence_id: "88888888-8888-4888-8888-888888888888" }, { runId, evidenceId })).toBe(false);
  });

  it("shows source PDF pages once and opens the first cited page without changing the source route", () => {
    const pages = sourcePdfPages([
      { pdf_page: 3 },
      { pdf_page: 3 },
      { pdf_page: 4 },
      { pdf_page: 3 },
    ]);

    expect(pages).toEqual([3, 4]);
    expect(formatSourcePdfPages(pages)).toBe("Страницы PDF: 3, 4");
    expect(formatSourcePdfPages([3])).toBe("Страница PDF: 3");
    expect(sourceUrlForFirstPdfPage("/api/v1/runs/run-1/sources/ev-1/pdf", pages)).toBe("/api/v1/runs/run-1/sources/ev-1/pdf#page=3");
    expect(sourceUrlForFirstPdfPage("/api/v1/runs/run-1/sources/ev-1/pdf?download=0#page=9", pages)).toBe("/api/v1/runs/run-1/sources/ev-1/pdf?download=0#page=3");
  });

  it("binds run reloads and event cursors to the selected run", () => {
    const currentRunId = "66666666-6666-4666-8666-666666666666";
    const otherRunId = "77777777-7777-4777-8777-777777777777";
    const action = { principalId: "operator-1", runId: currentRunId, requestSeq: 3 };

    expect(publicRunMatches({ run_id: currentRunId }, currentRunId)).toBe(true);
    expect(publicRunMatches({ run_id: otherRunId }, currentRunId)).toBe(false);
    expect(runEventCursorAfterReload(9, 4, false)).toBe(9);
    expect(runEventCursorAfterReload(9, 4, true)).toBe(4);
    expect(runActionStillTargetsDisplayedRun(action, { principalId: "operator-1", displayedRunId: currentRunId, requestSeq: 3 })).toBe(true);
    expect(runActionStillTargetsDisplayedRun(action, { principalId: "operator-1", displayedRunId: otherRunId, requestSeq: 3 })).toBe(false);
    expect(runActionStillTargetsDisplayedRun(action, { principalId: "operator-2", displayedRunId: currentRunId, requestSeq: 3 })).toBe(false);
    expect(runActionStillTargetsDisplayedRun(action, { principalId: "operator-1", displayedRunId: currentRunId, requestSeq: 4 })).toBe(false);
  });

  it("formats history durations for operators and keeps run ids as row actions", () => {
    const item: components["schemas"]["RunList"]["items"][number] = {
      run_id: "66666666-6666-4666-8666-666666666666",
      question_excerpt: "Какие нормы действуют?",
      status: "completed",
      created_at: "2026-09-08T00:00:00Z",
      duration_ms: 1234,
    };

    expect(formatDurationSeconds(item.duration_ms)).toBe("1.2 с");
    expect(formatDurationSeconds(null)).toBe("—");
    expect(item.run_id).toMatch(/^[\da-f]{8}-(?:[\da-f]{4}-){3}[\da-f]{12}$/);
  });

  it("keeps logical document lifecycle separate from selected version legal status", () => {
    const publication: components["schemas"]["PublicationInfo"] = {
      publication_id: "11111111-1111-4111-8111-111111111111",
      document_version_id: "22222222-2222-4222-8222-222222222222",
      index_generation_id: "33333333-3333-4333-8333-333333333333",
      published_at: "2026-09-08T00:00:00Z",
    };
    const archivedLegalVersion: Pick<components["schemas"]["VersionSummary"], "metadata"> = {
      metadata: {
        title: "Технический fixture",
        legal_status: "archived",
        approved_at: "2026-09-08",
        authority: null,
        document_number: null,
        document_type: null,
        edition_at: null,
        effective_from: null,
        effective_to: null,
        schema_version: 1,
        version_label: "ред. архивная",
      },
    };
    const activeLogicalDocument: Pick<components["schemas"]["DocumentSummary"], "archived_at" | "current_publication" | "security_revoked"> = {
      archived_at: null,
      current_publication: publication,
      security_revoked: false,
    };
    const archivedLogicalDocument: Pick<components["schemas"]["DocumentSummary"], "archived_at" | "current_publication" | "security_revoked"> = {
      archived_at: "2026-09-08T01:00:00Z",
      current_publication: null,
      security_revoked: true,
    };

    expect(documentLifecycleBadgeTexts(activeLogicalDocument)).toEqual({
      document: "документ: в библиотеке",
      publication: "публикация: опубликован",
      security: "доступ: разрешён",
    });
    expect(documentLifecycleBadgeTexts(archivedLogicalDocument)).toEqual({
      document: "документ: архивирован",
      publication: "публикация: не опубликован",
      security: "доступ: отозван",
    });
    expect(Object.values(documentLifecycleBadgeTexts(activeLogicalDocument)).join(" ")).not.toContain("legal");
    expect(versionLegalStatusText(archivedLegalVersion)).toBe("правовой статус версии: архивная");
  });

  it("derives generation and publication defaults from selected server DTOs", () => {
    const publication: components["schemas"]["PublicationInfo"] = {
      publication_id: "11111111-1111-4111-8111-111111111111",
      document_version_id: "33333333-3333-4333-8333-333333333333",
      index_generation_id: "44444444-4444-4444-8444-444444444444",
      published_at: "2026-09-08T00:00:00Z",
    };
    const version: Pick<components["schemas"]["VersionDetail"], "generations" | "current_publication"> = {
      current_publication: publication,
      generations: [
        { generation_id: "55555555-5555-4555-8555-555555555555", kind: "parse", status: "failed", created_at: "2026-09-08T00:00:00Z", completed_at: null, quality: null, node_count: null, chunk_count: null, routing_count: null },
        { generation_id: "66666666-6666-4666-8666-666666666666", kind: "parse", status: "ready", created_at: "2026-09-08T00:01:00Z", completed_at: "2026-09-08T00:02:00Z", quality: null, node_count: null, chunk_count: null, routing_count: null },
        { generation_id: "77777777-7777-4777-8777-777777777777", kind: "index", status: "ready", created_at: "2026-09-08T00:03:00Z", completed_at: "2026-09-08T00:04:00Z", quality: null, node_count: null, chunk_count: null, routing_count: null },
      ],
    };

    expect(firstParseGenerationId(version)).toBe("66666666-6666-4666-8666-666666666666");
    expect(firstReadyIndexGenerationId(version)).toBe("77777777-7777-4777-8777-777777777777");
    expect(currentPublicationId(version)).toBe(publication.publication_id);
    expect(uploadExpectedPublicationId(version)).toBe(publication.publication_id);
  });

  it("rejects public text that looks like private draft/debug leakage", () => {
    expect(answerHasDraftLeak("Проверенный ответ с цитатой")).toBe(false);
    expect(answerHasDraftLeak("DraftAnswer PRIVATE_INTERNAL_TRACE")).toBe(true);
  });

  it("validates PDF upload type and client-side size before transport", () => {
    expect(validatePdfUploadFile({ name: "rules.pdf", size: 1024, type: "" })).toMatchObject({ ok: true });
    expect(validatePdfUploadFile({ name: "rules.bin", size: 1024, type: "application/octet-stream" })).toMatchObject({ ok: false });
    expect(validatePdfUploadFile({ name: "empty.pdf", size: 0, type: "application/pdf" })).toMatchObject({ ok: false, message: "Файл пустой" });
    expect(validatePdfUploadFile({ name: "large.pdf", size: maxPdfUploadBytes + 1, type: "application/pdf" })).toMatchObject({ ok: false });
  });
});
