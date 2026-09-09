"""One bounded local vLLM call per admitted role; no implicit inference retries."""
from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
import json
from pathlib import Path
import time
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, SecretStr, ValidationError

from expert_contracts.internal import EvidencePack
from expert_contracts.model import CriticInternalResult, DraftAnswer, RouteDecision
from expert_contracts.debug_capture import DebugRequestPart, DebugResponsePart

from ..debug_capture import (
    CAPTURE_TIMEOUT_SECONDS, MAX_CAPTURE_BYTES, CaptureContext, DebugCaptureSink, submit_bounded,
)

from .prompts import (
    OUTPUT_LIMITS, PromptCatalog, validate_critic, validate_repair,
)
from .types import (
    MODEL, CallContext, CallProvenance, Descriptor, LlmError, PreparedCall, Role,
    RoleResult, ServingProfile, canonical_json, ordered_json, sha256,
)

SCHEMA_RETRY_OUTPUT_LIMIT_MULTIPLIER = 2


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object field")
        result[key] = value
    return result


def _invalid_constant(value: str):
    raise ValueError("Non-finite JSON constant")


def _json(raw: bytes | str):
    return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)


def _draft_binding(value: DraftAnswer, pack: EvidencePack) -> None:
    try:
        value.validate_binding({unit.evidence_id for unit in pack.units})
    except ValueError:
        # Well-formed invented citations are verification failures, not decoder
        # failures that could consume a schema retry and conceal hallucination.
        raise LlmError("VERIFICATION_FAILED", failure_kind="citation_binding") from None


class LlmGateway:
    """Private gateway. Caller owns durable call IDs, attempts and cancellation.

    Stable request IDs are correlation identities, NOT server-side deduplication.
    A lost response may have consumed inference. No answer is invented or cached.
    """

    def __init__(self, base_url: str, token: SecretStr, *, profile: ServingProfile,
                 models_lock: Path, prompts_directory: Path,
                 transport: httpx.AsyncBaseTransport | None = None,
                 capture_sink: DebugCaptureSink | None = None):
        parsed = urlsplit(base_url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.hostname not in {"vllm", "llm", "localhost", "127.0.0.1", "::1"}
                or any((parsed.username, parsed.password, parsed.query, parsed.fragment))
                or parsed.path.rstrip("/") not in {"", "/v1"} or not token.get_secret_value()):
            raise ValueError("LLM URL must identify a configured local serving origin")
        # No caller input can choose a URL, model, tool or fallback provider.
        origin = f"{parsed.scheme}://{parsed.netloc}/"
        self.profile = profile
        profile.validate_lock(models_lock)
        self.prompts = PromptCatalog(prompts_directory)
        self._client = httpx.AsyncClient(base_url=origin, trust_env=False, follow_redirects=False,
            headers={"Authorization": f"Bearer {token.get_secret_value()}"},
            timeout=httpx.Timeout(120, connect=3, pool=1),
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1), transport=transport)
        self._ready = False
        self._busy = False
        self._closed = False
        self._capture_sink = capture_sink
        self._capture_secret = token.get_secret_value().encode("utf-8")

    async def open(self, *, deadline: float) -> None:
        self._ready = False
        if self._closed:
            raise LlmError("MODEL_UNAVAILABLE")
        try:
            async with asyncio.timeout_at(deadline):
                version = await self._http("GET", "/version", None, "profile-version")
                models = await self._http("GET", "/v1/models", None, "profile-models")
        except (TimeoutError, httpx.TimeoutException):
            raise LlmError("MODEL_TIMEOUT") from None
        try:
            if (version["version"] != self.profile.vllm_version
                    or len(models["data"]) != 1 or models["data"][0]["id"] != MODEL
                    or models["data"][0]["max_model_len"] != self.profile.max_model_len):
                raise ValueError("Serving identity mismatch")
        except (KeyError, TypeError, ValueError):
            raise LlmError("MODEL_UNAVAILABLE") from None
        self._ready = True

    async def aclose(self) -> None:
        self._ready = False
        self._closed = True
        await self._client.aclose()

    def input_token_limit(self, role: Role) -> int:
        return self.profile.max_model_len - OUTPUT_LIMITS[role]

    def prepare_draft(self, question: str, pack: EvidencePack, bindings_json: str) -> PreparedCall:
        return self.prompts.answer("drafter", question, pack, bindings_json)

    async def count_prepared(self, prepared: PreparedCall, *, context: CallContext) -> int:
        """Exact message count; may exceed the role limit so packing can reject it."""
        self._admit(context)
        try:
            async with asyncio.timeout_at(min(context.deadline, asyncio.get_running_loop().time() + 10)):
                count, _ = await self._tokenize(prepared, context)
                return count
        except (httpx.TimeoutException, TimeoutError):
            raise self._timeout(context) from None
        except LlmError as error:
            error.call_id, error.schema_attempt = context.call_id, context.schema_attempt
            raise
        finally:
            self._busy = False

    async def router(self, question: str, descriptors: tuple[Descriptor, ...], *,
                     context: CallContext, capture: CaptureContext | None = None) -> RoleResult[RouteDecision]:
        prepared = self.prompts.router(question, descriptors)
        return await self._generate(prepared, RouteDecision, context,
            lambda value: value.validate_binding({d.descriptor_id for d in descriptors}), capture)

    async def draft(self, question: str, pack: EvidencePack, bindings_json: str, *,
                    context: CallContext, capture: CaptureContext | None = None) -> RoleResult[DraftAnswer]:
        def bind(value: DraftAnswer) -> None:
            _draft_binding(value, pack)

        return await self._generate(self.prepare_draft(question, pack, bindings_json), DraftAnswer, context,
            bind, capture)

    async def critic(self, question: str, pack: EvidencePack, bindings_json: str, draft: DraftAnswer, *,
                     context: CallContext, capture: CaptureContext | None = None) -> RoleResult[CriticInternalResult]:
        # Capture input before yielding: model-facing contracts contain mutable lists.
        frozen_draft = DraftAnswer.model_validate_json(draft.model_dump_json())
        prepared = self.prompts.answer("critic", question, pack, bindings_json, frozen_draft)
        return await self._generate(prepared, CriticInternalResult, context,
                                    lambda value: validate_critic(value, frozen_draft, pack), capture)

    async def repair(self, question: str, pack: EvidencePack, bindings_json: str, draft: DraftAnswer,
                     verdicts: CriticInternalResult, *, context: CallContext,
                     capture: CaptureContext | None = None) -> RoleResult[DraftAnswer]:
        validate_repair(draft, verdicts, pack)
        prepared = self.prompts.answer("repair", question, pack, bindings_json, draft, verdicts)
        def bind(value: DraftAnswer) -> None:
            _draft_binding(value, pack)

        return await self._generate(prepared, DraftAnswer, context,
            bind, capture)

    def _admit(self, context: CallContext) -> None:
        if not self._ready or self._closed:
            raise LlmError("MODEL_UNAVAILABLE", call_id=context.call_id, schema_attempt=context.schema_attempt)
        if context.deadline <= asyncio.get_running_loop().time():
            raise self._timeout(context)
        if self._busy:
            raise LlmError("CAPACITY_EXCEEDED", call_id=context.call_id, schema_attempt=context.schema_attempt)
        self._busy = True

    def _timeout(self, context: CallContext) -> LlmError:
        code = "DEADLINE_EXCEEDED" if asyncio.get_running_loop().time() >= context.deadline else "MODEL_TIMEOUT"
        return LlmError(code, call_id=context.call_id, schema_attempt=context.schema_attempt)

    @staticmethod
    def _template(prepared: PreparedCall) -> dict:
        return {"model": MODEL, "messages": json.loads(prepared.messages_json),
                "chat_template_kwargs": {"enable_thinking": False},
                "add_generation_prompt": True, "add_special_tokens": False,
                "continue_final_message": False}

    async def _tokenize(self, prepared: PreparedCall, context: CallContext) -> tuple[int, str]:
        response = await self._http("POST", "/tokenize", canonical_json(self._template(prepared)),
                                    f"{context.call_id}:{context.schema_attempt}:tokenize")
        try:
            count, tokens = response["count"], response["tokens"]
            if (type(count) is not int or count <= 0 or not isinstance(tokens, list) or len(tokens) != count
                    or any(type(token) is not int or token < 0 for token in tokens)
                    or response["max_model_len"] != self.profile.max_model_len):
                raise ValueError("Invalid tokenizer result")
            return count, sha256(canonical_json(tokens))
        except (KeyError, TypeError, ValueError):
            raise LlmError("MODEL_UNAVAILABLE") from None

    async def _generate[T: BaseModel](self, prepared: PreparedCall, schema: type[T], context: CallContext,
                                      binding: Callable[[T], None], capture: CaptureContext | None) -> RoleResult[T]:
        self._admit(context)
        started = time.monotonic()
        try:
            deadline = min(context.deadline, asyncio.get_running_loop().time() + prepared.timeout_seconds)
            async with asyncio.timeout_at(deadline):
                count, tokens_hash = await self._tokenize(prepared, context)
                if count + prepared.max_output_tokens > self.profile.max_model_len:
                    raise LlmError("TOKEN_LIMIT_EXCEEDED")
                output_limit = self._output_limit(prepared, context, count)
                payload = self._template(prepared) | {
                    "temperature": 0, "seed": 0, "n": 1, "stream": False,
                    "max_tokens": output_limit,
                    "request_id": f"{context.call_id}:{context.schema_attempt}",
                    "response_format": {"type": "json_schema", "json_schema": {
                        "name": schema.__name__, "schema": json.loads(prepared.schema_json), "strict": True}},
                }
                request_json = ordered_json(payload)
                on_response = None
                if (capture is not None and self._capture_sink is not None and capture.policy.active
                        and capture.call_id == context.call_id and capture.schema_attempt == context.schema_attempt):
                    request_hash = sha256(request_json)
                    await self._capture(prepared, capture, request_hash, request_json.encode("utf-8"), deadline)

                    async def on_response(raw: bytes, status: int) -> None:
                        await self._capture(prepared, capture, request_hash, raw, deadline, status)

                response = await self._http("POST", "/v1/chat/completions", request_json,
                    f"{context.call_id}:{context.schema_attempt}", on_response=on_response, capture_deadline=deadline)
                value, output_tokens = self._decode(response, schema, count, output_limit)
                provenance = CallProvenance(context.call_id, prepared.role, context.schema_attempt,
                    MODEL, self.profile.revision, self.profile.fingerprint, prepared.prompt_version,
                    prepared.prompt_sha256, sha256(prepared.schema_json), prepared.input_sha256,
                    sha256(prepared.messages_json), sha256(request_json), tokens_hash, count, output_tokens,
                    round((time.monotonic() - started) * 1000), prepared.evidence_manifest_sha256)
                try:
                    binding(value)
                except LlmError as error:
                    if (prepared.role in {"drafter", "repair"} and isinstance(value, DraftAnswer)
                            and error.code == "VERIFICATION_FAILED" and error.failure_kind == "citation_binding"):
                        # Persist this completed private model response before the
                        # failed precheck. It must never become a public answer.
                        error.private_result = RoleResult(value, provenance)
                    raise
                except ValueError:
                    raise LlmError("OUTPUT_SCHEMA_INVALID") from None
                return RoleResult(value, provenance)
        except (httpx.TimeoutException, TimeoutError):
            raise self._timeout(context) from None
        except LlmError as error:
            error.call_id, error.schema_attempt = context.call_id, context.schema_attempt
            raise
        finally:
            # Cancelling the task closes the HTTP response; vLLM owns engine abort.
            # No inference fallback or synthetic completed result is produced.
            self._busy = False

    def _output_limit(self, prepared: PreparedCall, context: CallContext, input_tokens: int) -> int:
        # The prepared prompt identity keeps the initial reserved allowance; only
        # the explicit schema retry may spend extra room left in the model window.
        if context.schema_attempt == 0:
            return prepared.max_output_tokens
        remaining = self.profile.max_model_len - input_tokens
        return min(prepared.max_output_tokens * SCHEMA_RETRY_OUTPUT_LIMIT_MULTIPLIER, remaining)

    async def _capture(self, prepared: PreparedCall, context: CaptureContext, request_hash: str,
                       raw: bytes, deadline: float, status: int | None = None) -> None:
        if (self._capture_sink is None or len(raw) > MAX_CAPTURE_BYTES or self._capture_secret in raw
                or not context.policy.active
                or deadline - asyncio.get_running_loop().time() < CAPTURE_TIMEOUT_SECONDS):
            return
        try:
            payload_sha256 = sha256(raw)
            payload_base64 = base64.b64encode(raw).decode("ascii")
            schema_sha256 = sha256(prepared.schema_json)
            messages_sha256 = sha256(prepared.messages_json)
            part: DebugRequestPart | DebugResponsePart
            if status is None:
                part = DebugRequestPart(part="request", role=prepared.role, request_sha256=request_hash,
                    payload_sha256=payload_sha256, payload_encoding="base64", payload_base64=payload_base64,
                    model_revision=self.profile.revision, profile_sha256=self.profile.fingerprint,
                    prompt_version=prepared.prompt_version, prompt_sha256=prepared.prompt_sha256,
                    schema_sha256=schema_sha256, input_sha256=prepared.input_sha256,
                    messages_sha256=messages_sha256)
            else:
                part = DebugResponsePart(part="response", role=prepared.role, request_sha256=request_hash,
                    payload_sha256=payload_sha256, payload_encoding="base64", payload_base64=payload_base64,
                    model_revision=self.profile.revision, profile_sha256=self.profile.fingerprint,
                    prompt_version=prepared.prompt_version, prompt_sha256=prepared.prompt_sha256,
                    schema_sha256=schema_sha256, input_sha256=prepared.input_sha256,
                    messages_sha256=messages_sha256, http_status=status)
            await submit_bounded(self._capture_sink, context, part, deadline=deadline)
        except Exception:
            # Optional capture may fail validation/storage; it never replaces a
            # model result or the precise model failure classification.
            return

    @staticmethod
    def _decode[T: BaseModel](response: dict, schema: type[T], input_tokens: int, output_limit: int) -> tuple[T, int]:
        try:
            if response["model"] != MODEL or len(response["choices"]) != 1:
                raise ValueError("Invalid response identity")
            choice, usage = response["choices"][0], response["usage"]
            message = choice["message"]
            if message.get("refusal") or choice["finish_reason"] == "content_filter":
                raise LlmError("OUTPUT_SCHEMA_INVALID", failure_kind="provider_refusal")
            if choice["finish_reason"] == "length":
                raise LlmError("OUTPUT_SCHEMA_INVALID", failure_kind="truncated")
            if (type(choice["index"]) is not int or choice["index"] != 0
                    or choice["finish_reason"] != "stop" or message["role"] != "assistant"
                    or message.get("refusal") or message.get("tool_calls")
                    or message.get("reasoning") or message.get("reasoning_content")
                    or not isinstance(message["content"], str)
                    or type(usage["prompt_tokens"]) is not int or usage["prompt_tokens"] != input_tokens
                    or type(usage["completion_tokens"]) is not int
                    or not 0 < usage["completion_tokens"] <= output_limit
                    or type(usage["total_tokens"]) is not int
                    or usage["total_tokens"] != input_tokens + usage["completion_tokens"]):
                raise ValueError("Invalid stop, reasoning or usage")
            _json(message["content"])  # Reject duplicate fields / NaN before Pydantic.
            value = schema.model_validate_json(message["content"], strict=True)
            return value, usage["completion_tokens"]
        except (KeyError, TypeError, AttributeError, RecursionError, ValueError, ValidationError):
            # Provider refusal/truncation is technical failure, never a corpus refusal.
            raise LlmError("OUTPUT_SCHEMA_INVALID") from None

    async def _http(self, method: str, path: str, body: str | None, request_id: str, *,
                    on_response: Callable[[bytes, int], Awaitable[None]] | None = None,
                    capture_deadline: float | None = None) -> dict:
        try:
            async with self._client.stream(method, path.lstrip("/"),
                    content=body.encode("utf-8") if body is not None else None,
                    headers={"Content-Type": "application/json", "X-Request-ID": request_id}) as response:
                status = response.status_code
                failed_capture_deadline = asyncio.get_running_loop().time() + CAPTURE_TIMEOUT_SECONDS
                if status != 200 and (on_response is None or capture_deadline is None
                        or capture_deadline - asyncio.get_running_loop().time() < CAPTURE_TIMEOUT_SECONDS):
                    raise LlmError("MODEL_UNAVAILABLE")
                raw = bytearray()
                try:
                    # A known failed response keeps its existing failure code,
                    # even if this optional bounded body collection fails.
                    async with asyncio.timeout_at(failed_capture_deadline) if status != 200 else nullcontext():
                        async for chunk in response.aiter_bytes():
                            if len(raw) + len(chunk) > MAX_CAPTURE_BYTES:
                                raise LlmError("MODEL_UNAVAILABLE")
                            raw.extend(chunk)
                except (TimeoutError, httpx.HTTPError):
                    if status != 200:
                        raise LlmError("MODEL_UNAVAILABLE") from None
                    raise
            if status != 200:
                if on_response is not None:
                    try:
                        async with asyncio.timeout_at(failed_capture_deadline):
                            await on_response(bytes(raw), status)
                    except TimeoutError:
                        pass
                raise LlmError("MODEL_UNAVAILABLE")
            if on_response is not None:
                await on_response(bytes(raw), status)
            value = _json(bytes(raw))
            if not isinstance(value, dict):
                raise ValueError("Expected object")
            return value
        except httpx.TimeoutException:
            raise
        except (httpx.HTTPError, RecursionError, ValueError):
            raise LlmError("MODEL_UNAVAILABLE") from None
