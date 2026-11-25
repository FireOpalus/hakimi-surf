"""Gemini Developer API and Vertex AI gateway transport, routing, and billing settlement."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from importlib.metadata import version
from typing import Any

import httpx
from google import genai
from google.genai._interactions import APITimeoutError
from google.oauth2 import service_account

from hakimi_plugin_sdk.bili_common import extract_bvid as extract_bvid

from .config import Settings
from .context_budget import estimate_tokens
from .gateway import FilterManager
from .gemini_generate import _GeminiGenerateMixin
from .gemini_search_screen import _GeminiSearchScreenMixin
from .gemini_wire import (
    BILI_SELECTION_RESPONSE_SCHEMA,
    COMPRESSION_INSTRUCTION,
    DIARY_INSTRUCTION,
    DIGEST_LONGNICK_RESPONSE_SCHEMA,
    LIFE_PLAN_INSTRUCTION,
    LIFE_PLAN_RESPONSE_SCHEMA,
    OWNER_COMPANION_INSTRUCTION,
    OWNER_DIGEST_INSTRUCTION,
    SCREEN_WATCH_INSTRUCTION,
    VOICE_COMPANION_INSTRUCTION,
    GenerationTimeout,
    WireAgendaChange,
    WireBiliPick,
    WireBiliSelection,
    WireDigestLongnick,
    WireLedgerPost,
    WireLifeIntent,
    WireLifePlan,
    _extract_usage_fields,
    _FreeTierFallback,
    _supports_provider_kwarg,
    _validate_audios,
    probability,
    questions,
)
from .media import ModelImage
from .prefix_runtime import MAX_CHAIN_TURNS, PrefixRuntime, StaticPrefixes
from .provider_diagnostics import MAX_ATTEMPTS, error_facts
from .provider_routing import ProviderRoutingManager
from .security import Rejected
from .store import DigestReviewCredit, Store
from .streaming import StreamProgress
from .thinking_policy import FLEX_MODELS, MODEL_LANES
from .usage_accounting import VALID_THINKING_LEVELS, TokenUsage
from .video_files import VideoFiles, run_guard
from .work_usage import model_work

__all__ = [
    "BILI_SELECTION_RESPONSE_SCHEMA",
    "COMPRESSION_INSTRUCTION",
    "DIARY_INSTRUCTION",
    "DIGEST_LONGNICK_RESPONSE_SCHEMA",
    "GenerationTimeout",
    "GeminiGateway",
    "LIFE_PLAN_INSTRUCTION",
    "LIFE_PLAN_RESPONSE_SCHEMA",
    "MAX_CHAIN_TURNS",
    "ModelImage",
    "OWNER_COMPANION_INSTRUCTION",
    "OWNER_DIGEST_INSTRUCTION",
    "SCREEN_WATCH_INSTRUCTION",
    "VOICE_COMPANION_INSTRUCTION",
    "WireAgendaChange",
    "WireBiliPick",
    "WireBiliSelection",
    "WireDigestLongnick",
    "WireLedgerPost",
    "WireLifeIntent",
    "WireLifePlan",
    "_FreeTierFallback",
    "_extract_usage_fields",
    "_supports_provider_kwarg",
    "_validate_audios",
    "probability",
    "questions",
]


class GeminiGateway(_GeminiSearchScreenMixin, _GeminiGenerateMixin):
    def __init__(
        self,
        settings: Settings,
        store: Store,
        decision_client: Any = None,
        chat_client: Any = None,
        *,
        files_client: Any = None,
        free_decision_client: Any = None,
        vertex_chat_client: Any = None,
    ):
        if tuple(int(p) for p in version("google-genai").split(".")[:3]) < (3, 2, 0):
            raise RuntimeError("google-genai >= 3.2.0 is required")
        self.s, self.store = settings, store
        self._owned = decision_client is None and chat_client is None
        self._transports: list[httpx.AsyncClient] = []
        self.decision_client = decision_client or self._client("v1beta", settings.decision_timeout)
        # Explicit v1 guards against SDK default-version changes.
        self.chat_client = chat_client or self._client("v1", settings.generation_timeout)
        self.slots = asyncio.Semaphore(settings.workers)
        self.memory_slots = asyncio.Semaphore(1)
        self.carbon_slots = asyncio.Semaphore(1)
        self.prefixes = StaticPrefixes()
        self.prefix_runtime = PrefixRuntime()
        self.blocked_until = 0.0
        self.vertex_blocked_until = 0.0
        self.files_client = files_client
        self._owned_files = files_client is None
        self.flex_client = chat_client
        self._owned_flex = False
        self.video_files = VideoFiles(store, self._files)
        self.filters = getattr(store.runtime, "filters", None) or FilterManager(store)
        routing = getattr(store.runtime, "provider_routing", None)
        if routing is None:
            routing = ProviderRoutingManager(store)
            store.runtime.provider_routing = routing
        self.routing: ProviderRoutingManager = routing
        self._injected_free_decision_client = free_decision_client
        self._injected_vertex_chat_client = vertex_chat_client
        self._owned_free_decision_client: Any = None
        self._owned_vertex_chat_client: Any = None
        self._retired_provider_clients: list[Any] = []
        self._routing_generation = -1
        self.free_decision_client: Any = free_decision_client
        self.vertex_chat_client: Any = vertex_chat_client
        self.reload_provider_clients()

    def _max_chain_turns(self) -> int:
        return MAX_CHAIN_TURNS

    def _files(self):
        if self.files_client is None:
            # Large uploads use a separate transport deadline.
            self.files_client = self._client("v1beta", 120)
        return self.files_client.aio.files

    def _flex_chat_client(self):
        if self.flex_client is None:
            self.flex_client = self._client("v1", self.s.memory_flex_timeout)
            self._owned_flex = True
        return self.flex_client

    def _client(self, api_version: str, timeout: float, *, api_key: str | None = None):
        transport = httpx.AsyncClient(
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=self.s.workers + 2),
        )
        self._transports.append(transport)
        return genai.Client(
            enterprise=False,
            vertexai=False,
            api_key=api_key if api_key is not None else self.s.gemini_api_key.get_secret_value(),
            http_options={
                "api_version": api_version,
                "timeout": int(timeout * 1000),
                "base_url": "https://generativelanguage.googleapis.com",
                "retry_options": {"attempts": 1},
                "httpx_async_client": transport,
                "client_args": {"trust_env": False},
                "async_client_args": {"trust_env": False},
            },
        )

    def _build_free_decision_client(self) -> Any:
        key = self.routing.gemini_free_api_key()
        if not key:
            return None
        return self._client("v1beta", self.s.decision_timeout, api_key=key)

    def _build_vertex_chat_client(self) -> Any:
        if not self.routing.has_vertex_credentials():
            return None
        cfg = self.routing.config
        timeout = self.s.generation_timeout
        transport = httpx.AsyncClient(
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=self.s.workers + 2),
        )
        self._transports.append(transport)
        http_options = {
            "timeout": int(timeout * 1000),
            "retry_options": {"attempts": 1},
            "httpx_async_client": transport,
            "client_args": {"trust_env": False},
            "async_client_args": {"trust_env": False},
        }
        if cfg.vertex_auth_mode == "service_account":
            sa_info, _ = self.routing.service_account_info()
            project = self.routing.effective_vertex_project()
            if sa_info is None or not project:
                return None
            creds = service_account.Credentials.from_service_account_info(
                sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            return genai.Client(
                enterprise=True,
                vertexai=True,
                credentials=creds,
                project=project,
                location=cfg.vertex_location or "global",
                http_options=http_options,
            )
        vkey = self.routing.vertex_api_key()
        if not vkey:
            return None
        project = cfg.vertex_project.strip() or None
        location = (cfg.vertex_location or "global").strip() if project else None
        return genai.Client(
            enterprise=True,
            vertexai=True,
            api_key=vkey,
            project=project,
            location=location,
            http_options=http_options,
        )

    def reload_provider_clients(self) -> None:
        """Rebuild runtime-owned Free Tier and Vertex clients after config/credential changes."""
        for old in (self._owned_free_decision_client, self._owned_vertex_chat_client):
            if old is not None:
                self._retired_provider_clients.append(old)
        self._owned_free_decision_client = (
            self._build_free_decision_client()
            if self._injected_free_decision_client is None
            else None
        )
        self._owned_vertex_chat_client = (
            self._build_vertex_chat_client() if self._injected_vertex_chat_client is None else None
        )
        self.free_decision_client = (
            self._injected_free_decision_client or self._owned_free_decision_client
        )
        self.vertex_chat_client = (
            self._injected_vertex_chat_client or self._owned_vertex_chat_client
        )
        self.vertex_blocked_until = 0.0
        self._routing_generation = self.routing.generation

    async def _ensure_routing_ready(self) -> None:
        await self.routing.initialize()
        if self._routing_generation != self.routing.generation:
            self.reload_provider_clients()

    def _should_use_vertex(self) -> bool:
        if self.routing.hydrogen_provider_for() == "vertex":
            return True
        return bool(
            self.routing.config.vertex_enabled and self._injected_vertex_chat_client is not None
        )

    async def close(self):
        if self._owned:
            for client in (self.decision_client, self.chat_client):
                await client.aio.aclose()
                client.close()
        if self._owned_files and self.files_client is not None:
            await self.files_client.aio.aclose()
            self.files_client.close()
        if self._owned_flex and self.flex_client is not None:
            await self.flex_client.aio.aclose()
            self.flex_client.close()
        for client in (
            *self._retired_provider_clients,
            self._owned_free_decision_client,
            self._owned_vertex_chat_client,
        ):
            if client is not None:
                await client.aio.aclose()
                client.close()
        for transport in self._transports:
            await transport.aclose()

    def _price(self, lane: str, *, at=None) -> tuple[Decimal, Decimal]:
        if lane == "decision":
            return self.s.decision_input_usd_per_m, Decimal(0)
        if lane in FLEX_MODELS:
            base = self._price(FLEX_MODELS[lane], at=at)
            return base[0] / 2, base[1] / 2
        if lane == "neon":
            return self.s.neon_input_usd_per_m, self.s.neon_output_usd_per_m
        if lane == "carbon":
            regular = (at or datetime.now(UTC)).date().isoformat() >= "2026-12-01"
            prefix = "carbon_regular_" if regular else "carbon_"
            return (
                getattr(self.s, prefix + "input_usd_per_m"),
                getattr(self.s, prefix + "output_usd_per_m"),
            )
        return self.s.chat_input_usd_per_m, self.s.chat_output_usd_per_m

    def _actual(self, result: Any, price: tuple[Decimal, Decimal]) -> Decimal | None:
        fields = _extract_usage_fields(result)
        if fields is None:
            return None
        incoming, outgoing, thoughts, _, total = fields
        if any(
            isinstance(x, bool) or not isinstance(x, int) or x < 0 for x in (incoming, outgoing)
        ):
            return None
        if price[1] and thoughts is None and total is None:
            return None
        thoughts = 0 if thoughts is None else thoughts
        total = 0 if total is None else total
        if any(isinstance(x, bool) or not isinstance(x, int) or x < 0 for x in (thoughts, total)):
            return None
        outgoing = max(outgoing + thoughts, total - incoming)
        return (incoming * price[0] + outgoing * price[1]) / 1_000_000

    @staticmethod
    def _usage(
        result: Any, model_lane: str, thinking_level: str | None = None
    ) -> TokenUsage | None:
        """Only provider-reported observations; missing optional fields remain unknown."""
        fields = _extract_usage_fields(result)
        if fields is None:
            return None
        incoming, outgoing, raw_thought, raw_cached, _ = fields

        def optional_count(value):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 100_000_000
            ):
                return None
            return value

        cached = optional_count(raw_cached)
        if cached is not None and (
            isinstance(incoming, bool) or not isinstance(incoming, int) or cached > incoming
        ):
            cached = None
        level = thinking_level if thinking_level in VALID_THINKING_LEVELS else None
        try:
            return TokenUsage(
                input_tokens=incoming,
                output_tokens=outgoing,
                cached_tokens=cached,
                thought_tokens=optional_count(raw_thought),
                model_lane=model_lane if model_lane in MODEL_LANES else "interaction",
                thinking_level=level,
            )
        except ValueError:
            return None

    def _cached_input_rate(self, model_lane: str, *, priced_at=None) -> Decimal | None:
        if model_lane == "decision":
            return None
        if model_lane in FLEX_MODELS:
            return self._cached_input_rate(FLEX_MODELS[model_lane], priced_at=priced_at) / 2
        if model_lane == "neon":
            return self.s.neon_cached_input_usd_per_m
        if model_lane == "carbon":
            regular = (priced_at or datetime.now(UTC)).date().isoformat() >= "2026-12-01"
            prefix = "carbon_regular_" if regular else "carbon_"
            cached_rate = getattr(self.s, prefix + "cached_input_usd_per_m")
        else:
            cached_rate = self.s.chat_cached_input_usd_per_m
        return cached_rate

    def _discounted(self, actual, usage, rates, *, priced_at=None):
        if actual is None:
            return None
        if usage is None or usage.model_lane == "decision" or not usage.cached_tokens:
            return actual
        cached_rate = self._cached_input_rate(usage.model_lane, priced_at=priced_at)
        if cached_rate is None or not Decimal(0) <= cached_rate <= rates[0]:
            return actual
        return actual - usage.cached_tokens * (rates[0] - cached_rate) / 1_000_000

    @staticmethod
    def _http_status(exc: BaseException) -> int | None:
        for candidate in (
            getattr(exc, "status_code", None),
            getattr(exc, "code", None),
            getattr(getattr(exc, "response", None), "status_code", None),
        ):
            if isinstance(candidate, int) and not isinstance(candidate, bool):
                return candidate
        return None

    @classmethod
    def _non_billable_http_error(cls, exc: BaseException, stream_started: bool) -> bool:
        if stream_started:
            return False
        status = cls._http_status(exc)
        return status in {400, 401, 403, 404, 409, 413, 422, 429}

    def _reserve_kwargs(
        self, model_lane: str | None, thinking_level: str | None, provider: str | None
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model_lane": model_lane,
            "thinking_level": thinking_level,
        }
        if provider is not None and _supports_provider_kwarg(self.store.reserve, type(self.store)):
            kwargs["provider"] = provider
        return kwargs

    def _settle_kwargs(
        self,
        *,
        usage: TokenUsage | None = None,
        discounted_actual: Decimal | None = None,
        fallback_actual: Decimal | None = None,
        model_lane: str | None = None,
        thinking_level: str | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "discounted_actual": discounted_actual,
            "fallback_actual": fallback_actual,
            "model_lane": model_lane,
            "thinking_level": thinking_level,
        }
        if usage is not None:
            kwargs["usage"] = usage
        if provider is not None and _supports_provider_kwarg(self.store.settle, type(self.store)):
            kwargs["provider"] = provider
        return kwargs

    async def _call(
        self,
        subject: str,
        budget_lane: str,
        model_lane: str,
        input_payload: Any,
        output_cap: int,
        deadline_seconds: float,
        operation: Callable[[], Awaitable[Any]],
        *,
        image_tokens: int = 0,
        work_kind: str | None = None,
        digest_review: DigestReviewCredit | None = None,
        before_call=None,
        context_limit=None,
        stream_retryable=False,
        additional_reserve=Decimal(0),
        additional_actual=None,
        fallback_media_tokens: int | None = None,
        fallback_additional: Decimal | None = None,
        cached_prefix_tokens: int = 0,
        stream_progress: StreamProgress | None = None,
        call_state: dict[str, bool] | None = None,
        thinking_level: str | None = None,
        price_override: tuple[Decimal, Decimal] | None = None,
        provider: str = "gemini_paid",
        audit_stage: str = "generation",
        attempts: list[dict] | None = None,
        free_fallback_eligible: bool = False,
        vertex_fallback_eligible: bool = False,
    ) -> Any:
        attempts = attempts if attempts is not None else []

        def observe(facts, elapsed):
            entry = {
                "stage": audit_stage,
                "provider": provider,
                "model_lane": model_lane,
                "timeout_ms": round(deadline_seconds * 1000),
                "elapsed_ms": round(elapsed * 1000),
                **facts,
            }
            if stream_progress is not None:
                entry["stream_phase"] = (
                    "partial_output"
                    if stream_progress.output_text
                    else "started"
                    if stream_progress.started
                    else "waiting"
                )
            attempts.append(entry)
            del attempts[:-MAX_ATTEMPTS]

        def rejected(code, detail=""):
            exc = Rejected(code, detail)
            exc.provider_diagnostics = list(attempts)
            return exc

        remaining = self.blocked_until - time.monotonic()
        if provider == "gemini_paid" and remaining > 0:
            observe({"cause": "circuit_open", "cooldown_ms": round(remaining * 1000)}, 0)
            raise rejected("gemini_circuit_open")
        # Count text bytes and separately reserve bounded image tokens, never base64 as text.
        serialized = json.dumps(input_payload, ensure_ascii=False)
        byte_count = len(serialized.encode("utf-8"))
        if byte_count > (context_limit or self.s.max_context_bytes + 12000):
            raise Rejected("context_too_large")
        model_slot = (
            self.carbon_slots
            if FLEX_MODELS.get(model_lane, model_lane) == "carbon"
            else nullcontext()
        )
        async with model_slot, self.memory_slots if model_lane in FLEX_MODELS else self.slots:
            await run_guard(before_call)
            priced_at = datetime.now(UTC)
            rates = (
                price_override
                if price_override is not None
                else self._price(model_lane, at=priced_at)
            )
            reserve_usd = (
                (byte_count + 2048 + image_tokens) * rates[0] + output_cap * rates[1]
            ) / 1_000_000 + additional_reserve

            def fallback_cost(
                elapsed: float,
                *,
                result: Any = None,
                usage: TokenUsage | None = None,
                token_actual: Decimal | None = None,
            ) -> Decimal:
                extra = additional_reserve if fallback_additional is None else fallback_additional
                if result is not None and token_actual is not None:
                    disc = self._discounted(token_actual, usage, rates, priced_at=priced_at)
                    est = (disc if disc is not None else token_actual) + extra
                elif result is not None and usage is not None:
                    est_out = usage.output_tokens + min(output_cap, 256)
                    gross = (usage.input_tokens * rates[0] + est_out * rates[1]) / 1_000_000
                    disc = self._discounted(gross, usage, rates, priced_at=priced_at)
                    est = (disc if disc is not None else gross) + extra
                else:
                    media = image_tokens if fallback_media_tokens is None else fallback_media_tokens
                    in_tokens = estimate_tokens(serialized) + 256 + media
                    cached_rate = self._cached_input_rate(model_lane, priced_at=priced_at)
                    cached_in = (
                        min(in_tokens, max(0, cached_prefix_tokens))
                        if (cached_rate is not None and Decimal(0) <= cached_rate <= rates[0])
                        else 0
                    )
                    in_cost = (in_tokens - cached_in) * rates[0] + cached_in * (
                        cached_rate or Decimal(0)
                    )
                    if output_cap <= 0:
                        out_tokens = 0
                    elif stream_progress is not None:
                        streamed = estimate_tokens(stream_progress.output_text)
                        out_tokens = min(
                            output_cap,
                            streamed + min(512, max(64, math.ceil(elapsed * 90))),
                        )
                    else:
                        out_tokens = min(output_cap, min(768, max(128, math.ceil(elapsed * 90))))
                    est = (in_cost + out_tokens * rates[1]) / 1_000_000 + extra
                floor = Decimal("0.000001") if reserve_usd > 0 else Decimal(0)
                return min(reserve_usd, max(floor, est))

            with model_work(work_kind or budget_lane + "." + model_lane):
                reservation = await self.store.reserve(
                    subject,
                    budget_lane,
                    reserve_usd,
                    **self._reserve_kwargs(model_lane, thinking_level, provider),
                    **({"digest_review": digest_review} if digest_review is not None else {}),
                )
            try:
                await run_guard(before_call)
            except BaseException:
                await asyncio.shield(
                    self.store.settle(
                        reservation,
                        Decimal(0),
                        **self._settle_kwargs(
                            discounted_actual=Decimal(0),
                            model_lane=model_lane,
                            thinking_level=thinking_level,
                            provider=provider,
                        ),
                    )
                )
                raise
            started_at = time.monotonic()
            deadline = asyncio.timeout(deadline_seconds)
            try:
                async with deadline:
                    result = await operation()
            except asyncio.CancelledError:
                not_dispatched = (
                    stream_progress is not None and not stream_progress.dispatched
                ) or (call_state is not None and not call_state.get("dispatched", True))
                if not_dispatched:
                    await asyncio.shield(
                        self.store.settle(
                            reservation,
                            Decimal(0),
                            **self._settle_kwargs(
                                discounted_actual=Decimal(0),
                                model_lane=model_lane,
                                thinking_level=thinking_level,
                                provider=provider,
                            ),
                        )
                    )
                else:
                    elapsed = max(0.0, time.monotonic() - started_at)
                    await asyncio.shield(
                        self.store.settle(
                            reservation,
                            None,
                            **self._settle_kwargs(
                                fallback_actual=fallback_cost(elapsed),
                                model_lane=model_lane,
                                thinking_level=thinking_level,
                                provider=provider,
                            ),
                        )
                    )
                raise
            except (TimeoutError, httpx.TimeoutException, APITimeoutError) as exc:
                elapsed = max(0.0, time.monotonic() - started_at)
                observe(
                    error_facts(
                        exc,
                        expired=deadline.expired(),
                        http_status=None,
                        stream_code=stream_progress.error_code if stream_progress else None,
                    ),
                    elapsed,
                )
                await asyncio.shield(
                    self.store.settle(
                        reservation,
                        None,
                        **self._settle_kwargs(
                            fallback_actual=fallback_cost(elapsed),
                            model_lane=model_lane,
                            thinking_level=thinking_level,
                            provider=provider,
                        ),
                    )
                )
                if provider == "gemini_paid":
                    await self.store.runtime.provider_result(
                        "gemini_memory"
                        if model_lane in FLEX_MODELS
                        else "gemini_chat"
                        if model_lane != "decision"
                        else "gemini_decision",
                        True,
                    )
                    if stream_retryable:
                        raise GenerationTimeout() from None
                    if model_lane not in FLEX_MODELS and budget_lane != "search":
                        self.blocked_until = time.monotonic() + 60
                elif provider == "gemini_free":
                    if free_fallback_eligible:
                        raise _FreeTierFallback() from None
                elif provider == "vertex_paid":
                    if stream_retryable and (
                        not vertex_fallback_eligible
                        or (stream_progress is not None and stream_progress.started)
                    ):
                        raise GenerationTimeout() from None
                raise rejected("gemini_unavailable") from None
            except Rejected as exc:
                elapsed = max(0.0, time.monotonic() - started_at)
                if str(exc) == "stream_provider_error":
                    observe(
                        error_facts(
                            exc,
                            expired=False,
                            http_status=None,
                            stream_code=stream_progress.error_code
                            if stream_progress
                            else "unknown",
                        ),
                        elapsed,
                    )
                    exc.provider_diagnostics = list(attempts)
                await asyncio.shield(
                    self.store.settle(
                        reservation,
                        None,
                        **self._settle_kwargs(
                            fallback_actual=fallback_cost(elapsed),
                            model_lane=model_lane,
                            thinking_level=thinking_level,
                            provider=provider,
                        ),
                    )
                )
                raise
            except Exception as exc:
                stream_started = bool(stream_progress and stream_progress.started)
                status = self._http_status(exc)
                observe(
                    error_facts(exc, expired=False, http_status=status, stream_code=None),
                    max(0.0, time.monotonic() - started_at),
                )
                if stream_progress is not None and status is not None and 400 <= status <= 599:
                    stream_progress.error_code = str(status)
                non_billable = self._non_billable_http_error(exc, stream_started) or (
                    model_lane in FLEX_MODELS and not stream_started and status == 503
                )
                if non_billable:
                    await asyncio.shield(
                        self.store.settle(
                            reservation,
                            Decimal(0),
                            **self._settle_kwargs(
                                discounted_actual=Decimal(0),
                                model_lane=model_lane,
                                thinking_level=thinking_level,
                                provider=provider,
                            ),
                        )
                    )
                else:
                    elapsed = max(0.0, time.monotonic() - started_at)
                    await asyncio.shield(
                        self.store.settle(
                            reservation,
                            None,
                            **self._settle_kwargs(
                                fallback_actual=fallback_cost(elapsed),
                                model_lane=model_lane,
                                thinking_level=thinking_level,
                                provider=provider,
                            ),
                        )
                    )
                if provider == "gemini_paid":
                    await self.store.runtime.provider_result(
                        "gemini_memory"
                        if model_lane in FLEX_MODELS
                        else "gemini_decision"
                        if model_lane == "decision"
                        else "gemini_chat",
                        True,
                    )
                    if model_lane not in FLEX_MODELS and budget_lane != "search":
                        self.blocked_until = time.monotonic() + 60
                elif provider == "gemini_free":
                    if (
                        free_fallback_eligible
                        and not stream_started
                        and (status == 429 or (status is not None and 500 <= status < 600))
                    ):
                        raise _FreeTierFallback() from None
                if non_billable and model_lane in FLEX_MODELS and status == 503:
                    raise rejected("gemini_unavailable", "flex_capacity_busy") from None
                if non_billable and status == 429:
                    if provider == "vertex_paid" and vertex_fallback_eligible:
                        self.vertex_blocked_until = max(
                            self.vertex_blocked_until, time.monotonic() + 60
                        )
                    raise rejected("gemini_unavailable", "rate_limited") from None
                raise rejected("gemini_unavailable") from None
            elapsed = max(0.0, time.monotonic() - started_at)
            if provider == "gemini_paid":
                await self.store.runtime.provider_result(
                    "gemini_memory"
                    if model_lane in FLEX_MODELS
                    else "gemini_decision"
                    if model_lane == "decision"
                    else "gemini_chat",
                    False,
                )
            token_actual = self._actual(result, rates)
            usage = self._usage(result, model_lane, thinking_level=thinking_level)
            actual = token_actual
            if additional_actual is not None:
                extra = additional_actual(result)
                actual = (
                    token_actual + extra if token_actual is not None and extra is not None else None
                )
            discounted = self._discounted(actual, usage, rates, priced_at=priced_at)
            fallback = (
                fallback_cost(elapsed, result=result, usage=usage, token_actual=token_actual)
                if actual is None
                else None
            )
            try:
                await self.store.settle(
                    reservation,
                    actual,
                    **self._settle_kwargs(
                        usage=usage,
                        discounted_actual=discounted,
                        fallback_actual=fallback,
                        model_lane=model_lane,
                        thinking_level=thinking_level,
                        provider=provider,
                    ),
                )
            except Rejected as exc:
                if digest_review is None or str(exc) != "budget_reservation_exceeded":
                    raise
            await run_guard(before_call)
            return result
