"""Vertex AI-first semantic embedding client and binary vector codec.

Vectors are L2-normalized and serialized into compact little-endian IEEE 754 float16 blobs
prefixed with ``b"E1" + <H dim>`` so callers can distinguish semantic vectors from legacy
192-dim float32 hashed vectors without schema breakage. All remote calls reserve and settle
against the Store financial ledger and never raise unhandled exceptions into callers.
"""

from __future__ import annotations

import asyncio
import math
import numbers
import operator
import re
import struct
import time
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import httpx
from google import genai
from google.genai import types
from google.oauth2 import service_account

from .config import Settings
from .context_budget import estimate_tokens
from .gemini_wire import _supports_provider_kwarg
from .provider_routing import ProviderRoutingManager
from .security import Rejected, clean_text, configured_secrets
from .store import Store
from .usage_accounting import TokenUsage
from .work_usage import model_work

try:
    import numpy as _np  # type: ignore[import-not-found]
except ImportError:
    _np = None

SEMANTIC_MAGIC = b"E1"
DEFAULT_EMBEDDING_MODEL = "gemini-embedding-001"
DEFAULT_EMBEDDING_DIM = 768
DEFAULT_VERTEX_EMBEDDING_LOCATION = "us-central1"
LEGACY_HASH_DIM = 192
LEGACY_HASH_BYTES = LEGACY_HASH_DIM * 4

EmbeddingTaskType = Literal["RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "SEMANTIC_SIMILARITY"]
VALID_TASK_TYPES = frozenset({"RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "SEMANTIC_SIMILARITY"})

_NON_BILLABLE_HTTP_STATUSES = frozenset({400, 401, 403, 404, 409, 413, 422, 429})
_COOLDOWN_HTTP_STATUSES = frozenset({401, 403, 404, 429})
_DEFAULT_COOLDOWN_SECONDS = 60.0
_MAX_SEMANTIC_DIM = 0xFFFF
_SAFE_PURPOSE_RE = re.compile(r"^[a-z0-9_.-]{1,64}$")
_VERTEX_EMBEDDING_REGION_FALLBACKS = {
    "global": DEFAULT_VERTEX_EMBEDDING_LOCATION,
    "us": DEFAULT_VERTEX_EMBEDDING_LOCATION,
    "eu": "europe-west4",
}

__all__ = [
    "DEFAULT_EMBEDDING_DIM",
    "DEFAULT_EMBEDDING_MODEL",
    "DEFAULT_VERTEX_EMBEDDING_LOCATION",
    "EmbeddingClient",
    "EmbeddingTaskType",
    "LEGACY_HASH_BYTES",
    "LEGACY_HASH_DIM",
    "SEMANTIC_MAGIC",
    "cosine_similarity_blobs",
    "is_semantic_blob",
    "pack_semantic_vector",
    "unpack_semantic_vector",
]


def pack_semantic_vector(values: Sequence[float]) -> bytes:
    """L2-normalize a non-zero vector and pack as ``b'E1' + <H dim + <{dim}e``."""
    if isinstance(values, (str, bytes, bytearray, memoryview)) or not isinstance(values, Sequence):
        raise ValueError("embedding vector must be a numeric sequence")
    dim = len(values)
    if dim <= 0:
        raise ValueError("embedding vector must be non-empty")
    if dim > _MAX_SEMANTIC_DIM:
        raise ValueError("embedding dimension exceeds uint16 limit")

    vec: list[float] = []
    max_abs = 0.0
    for item in values:
        if isinstance(item, bool) or not isinstance(item, numbers.Real):
            raise ValueError("embedding vector must contain finite real numbers")
        num = float(item)
        if not math.isfinite(num):
            raise ValueError("embedding vector contains NaN or Inf")
        abs_num = abs(num)
        if abs_num > max_abs:
            max_abs = abs_num
        vec.append(num)

    if max_abs == 0.0:
        normed = [0.0] * dim
    else:
        scaled = [x / max_abs for x in vec]
        norm = math.sqrt(math.fsum(x * x for x in scaled))
        if not math.isfinite(norm) or norm == 0.0:
            normed = [0.0] * dim
        else:
            normed = [x / norm for x in scaled]

    return SEMANTIC_MAGIC + struct.pack("<H", dim) + struct.pack(f"<{dim}e", *normed)


def is_semantic_blob(blob: bytes) -> bool:
    """Return True iff ``blob`` has a valid ``E1`` semantic vector header and byte length."""
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        return False
    mv = memoryview(blob)
    if len(mv) < 6 or mv[:2].tobytes() != SEMANTIC_MAGIC:
        return False
    (dim,) = struct.unpack_from("<H", mv, 2)
    return dim > 0 and len(mv) == 4 + dim * 2


def unpack_semantic_vector(blob: bytes) -> tuple[str, tuple[float, ...]] | None:
    """Decode an ``E1`` semantic vector or legacy 192-dim float32 hashed vector."""
    if not isinstance(blob, (bytes, bytearray, memoryview)):
        return None
    mv = memoryview(blob)
    if len(mv) >= 6 and mv[:2].tobytes() == SEMANTIC_MAGIC:
        (dim,) = struct.unpack_from("<H", mv, 2)
        if dim > 0 and len(mv) == 4 + dim * 2:
            values = struct.unpack_from(f"<{dim}e", mv, 4)
            if all(math.isfinite(v) for v in values):
                return ("semantic", values)
            return None
    if len(mv) == LEGACY_HASH_BYTES:
        values = struct.unpack_from(f"<{LEGACY_HASH_DIM}f", mv, 0)
        if all(math.isfinite(v) for v in values):
            return ("legacy_hash", values)
        return None
    return None


def cosine_similarity_blobs(a: bytes, b: bytes) -> float | None:
    """Compute dot-product cosine similarity for two compatible packed vector blobs."""
    if not isinstance(a, (bytes, bytearray, memoryview)) or not isinstance(
        b, (bytes, bytearray, memoryview)
    ):
        return None

    if _np is not None:
        a_sem = is_semantic_blob(a)
        b_sem = is_semantic_blob(b)
        if a_sem and b_sem:
            (dim_a,) = struct.unpack_from("<H", a, 2)
            (dim_b,) = struct.unpack_from("<H", b, 2)
            if dim_a != dim_b:
                return None
            va = _np.frombuffer(a, dtype="<f2", offset=4).astype(_np.float32)
            vb = _np.frombuffer(b, dtype="<f2", offset=4).astype(_np.float32)
            if not (bool(_np.all(_np.isfinite(va))) and bool(_np.all(_np.isfinite(vb)))):
                return None
            dot = float(_np.dot(va, vb))
            return max(-1.0, min(1.0, dot)) if math.isfinite(dot) else None
        if not a_sem and not b_sem and len(a) == LEGACY_HASH_BYTES and len(b) == LEGACY_HASH_BYTES:
            va = _np.frombuffer(a, dtype="<f4")
            vb = _np.frombuffer(b, dtype="<f4")
            if not (bool(_np.all(_np.isfinite(va))) and bool(_np.all(_np.isfinite(vb)))):
                return None
            dot = float(_np.dot(va, vb))
            return max(-1.0, min(1.0, dot)) if math.isfinite(dot) else None
        return None

    unpacked_a = unpack_semantic_vector(a)
    unpacked_b = unpack_semantic_vector(b)
    if unpacked_a is None or unpacked_b is None:
        return None
    kind_a, vec_a = unpacked_a
    kind_b, vec_b = unpacked_b
    if kind_a != kind_b or len(vec_a) != len(vec_b):
        return None
    dot = sum(map(operator.mul, vec_a, vec_b))
    if not math.isfinite(dot):
        return None
    return max(-1.0, min(1.0, float(dot)))


@dataclass
class _RetiredEmbedClient:
    client: Any
    transport: httpx.AsyncClient | None
    client_closed: bool = False
    transport_closed: bool = False
    scheduled: bool = False


class EmbeddingClient:
    """Async Vertex AI-first semantic embedding client with Developer API fallback and ledgering."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        *,
        routing: ProviderRoutingManager | None = None,
        vertex_client: Any = None,
        developer_client: Any = None,
        timeout: float = 20.0,
        max_batch_size: int = 32,
        max_text_chars: int = 8000,
        max_batch_bytes: int = 24_000,
    ) -> None:
        self.s = settings
        self.store = store
        if routing is None:
            runtime = getattr(store, "runtime", None)
            routing = getattr(runtime, "provider_routing", None)
            if routing is None and runtime is not None:
                routing = ProviderRoutingManager(store)
                runtime.provider_routing = routing
        self.routing: ProviderRoutingManager | None = routing
        self.timeout = max(1.0, float(timeout))
        self.max_batch_size = max(1, int(max_batch_size))
        self.max_text_chars = max(64, int(max_text_chars))
        self.max_batch_bytes = max(1024, int(max_batch_bytes))

        self._injected_vertex_client = vertex_client
        self._injected_developer_client = developer_client
        self._owned_vertex_client: Any = None
        self._owned_vertex_transport: httpx.AsyncClient | None = None
        self._owned_developer_client: Any = None
        self._owned_developer_transport: httpx.AsyncClient | None = None
        self._vertex_built = False
        self._developer_built = False
        self._retired_clients: list[_RetiredEmbedClient] = []
        self._retire_tasks: set[asyncio.Task[None]] = set()
        self._retire_delay_seconds = self.timeout + 5.0
        self._routing_generation = -1
        self._vertex_blocked_until = 0.0
        self._developer_blocked_until = 0.0
        self._cooldown_seconds = _DEFAULT_COOLDOWN_SECONDS
        self._closed = False
        workers = int(getattr(settings, "workers", 2) or 2)
        self._slots = asyncio.Semaphore(max(1, min(4, workers)))

    def effective_vertex_embedding_location(self) -> str:
        """Resolve a concrete regional Vertex endpoint for embedding models."""
        if self.routing is not None:
            return self.routing.effective_vertex_embedding_location()
        explicit = (getattr(self.s, "vertex_embedding_location", "") or "").strip()
        fields_set = getattr(self.s, "model_fields_set", set())
        if explicit and "vertex_embedding_location" in fields_set:
            return explicit
        loc = (getattr(self.s, "vertex_location", "") or "").strip().lower()
        if loc in _VERTEX_EMBEDDING_REGION_FALLBACKS:
            return _VERTEX_EMBEDDING_REGION_FALLBACKS[loc]
        if loc and "-" in loc:
            return loc
        return explicit or DEFAULT_VERTEX_EMBEDDING_LOCATION

    def _should_use_vertex(self) -> bool:
        if self.routing is not None:
            if not self.routing.config.vertex_enabled:
                return False
            if self.routing.has_vertex_credentials():
                return True
            return self._injected_vertex_client is not None
        return bool(
            getattr(self.s, "vertex_enabled", False) and self._injected_vertex_client is not None
        )

    def _vertex_fallback_allowed(self) -> bool:
        if self.routing is not None:
            return bool(self.routing.config.vertex_fallback_to_gemini)
        return True

    def _make_transport(self) -> httpx.AsyncClient:
        workers = int(getattr(self.s, "workers", 2) or 2)
        return httpx.AsyncClient(
            timeout=self.timeout,
            trust_env=False,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=max(4, workers + 2)),
        )

    def _build_vertex_client(self) -> tuple[Any, httpx.AsyncClient] | None:
        if self.routing is None or not self.routing.has_vertex_credentials():
            return None
        cfg = self.routing.config
        embed_location = self.effective_vertex_embedding_location()
        if cfg.vertex_auth_mode == "service_account":
            sa_info, _ = self.routing.service_account_info()
            project = self.routing.effective_vertex_project()
            if sa_info is None or not project:
                return None
            creds = service_account.Credentials.from_service_account_info(
                sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            transport = self._make_transport()
            http_options = {
                "timeout": int(self.timeout * 1000),
                "retry_options": {"attempts": 1},
                "httpx_async_client": transport,
                "client_args": {"trust_env": False},
                "async_client_args": {"trust_env": False},
            }
            client = genai.Client(
                enterprise=True,
                vertexai=True,
                credentials=creds,
                project=project,
                location=embed_location,
                http_options=http_options,
            )
            return client, transport

        vkey = self.routing.vertex_api_key()
        if not vkey:
            return None
        project = cfg.vertex_project.strip() or None
        location = embed_location if project else None
        transport = self._make_transport()
        http_options = {
            "timeout": int(self.timeout * 1000),
            "retry_options": {"attempts": 1},
            "httpx_async_client": transport,
            "client_args": {"trust_env": False},
            "async_client_args": {"trust_env": False},
        }
        client = genai.Client(
            enterprise=True,
            vertexai=True,
            api_key=vkey,
            project=project,
            location=location,
            http_options=http_options,
        )
        return client, transport

    def _build_developer_client(self) -> tuple[Any, httpx.AsyncClient] | None:
        raw_key = self.s.gemini_api_key.get_secret_value().strip()
        if not raw_key:
            return None
        transport = self._make_transport()
        client = genai.Client(
            enterprise=False,
            vertexai=False,
            api_key=raw_key,
            http_options={
                "api_version": "v1beta",
                "timeout": int(self.timeout * 1000),
                "base_url": "https://generativelanguage.googleapis.com",
                "retry_options": {"attempts": 1},
                "httpx_async_client": transport,
                "client_args": {"trust_env": False},
                "async_client_args": {"trust_env": False},
            },
        )
        return client, transport

    def _get_vertex_client(self) -> Any:
        if self._injected_vertex_client is not None:
            return self._injected_vertex_client
        if not self._vertex_built:
            self._vertex_built = True
            built = self._build_vertex_client()
            if built is not None:
                self._owned_vertex_client, self._owned_vertex_transport = built
        return self._owned_vertex_client

    def _get_developer_client(self) -> Any:
        if self._injected_developer_client is not None:
            return self._injected_developer_client
        if not self._developer_built:
            self._developer_built = True
            built = self._build_developer_client()
            if built is not None:
                self._owned_developer_client, self._owned_developer_transport = built
        return self._owned_developer_client

    def reload_provider_clients(self) -> None:
        """Retire runtime-owned Vertex embedding client after routing/credential updates."""
        if self._owned_vertex_client is not None or self._owned_vertex_transport is not None:
            self._retired_clients.append(
                _RetiredEmbedClient(
                    client=self._owned_vertex_client,
                    transport=self._owned_vertex_transport,
                )
            )
            self._owned_vertex_client = None
            self._owned_vertex_transport = None
        self._vertex_built = False
        self._vertex_blocked_until = 0.0
        self._routing_generation = self.routing.generation if self.routing is not None else 0
        self._schedule_retired_cleanup()

    def _schedule_retired_cleanup(self) -> None:
        if self._closed:
            return
        unscheduled = [entry for entry in self._retired_clients if not entry.scheduled]
        if not unscheduled:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        for entry in unscheduled:
            entry.scheduled = True
        task = loop.create_task(self._retire_entries(unscheduled))
        self._retire_tasks.add(task)
        task.add_done_callback(self._retire_tasks.discard)

    @staticmethod
    async def _close_entry(entry: _RetiredEmbedClient) -> None:
        try:
            if entry.client is not None and not entry.client_closed:
                try:
                    aio_client = getattr(entry.client, "aio", None)
                    aclose = getattr(aio_client, "aclose", None)
                    if callable(aclose):
                        await aclose()
                finally:
                    close_sync = getattr(entry.client, "close", None)
                    if callable(close_sync):
                        close_sync()
                entry.client_closed = True
        finally:
            if entry.transport is not None and not entry.transport_closed:
                if not getattr(entry.transport, "is_closed", False):
                    await entry.transport.aclose()
                entry.transport_closed = True

    async def _retire_entries(self, entries: list[_RetiredEmbedClient]) -> None:
        delay = max(0.0, float(self._retire_delay_seconds))
        if delay > 0:
            await asyncio.sleep(delay)
        for entry in entries:
            if entry not in self._retired_clients:
                continue
            try:
                await self._close_entry(entry)
            except Exception:
                pass
            if entry in self._retired_clients:
                self._retired_clients.remove(entry)

    async def _ensure_routing_ready(self) -> None:
        if self.routing is None:
            return
        await self.routing.initialize()
        if self._routing_generation != self.routing.generation:
            self.reload_provider_clients()

    async def close(self) -> None:
        """Close all runtime-owned clients and httpx transports cleanly."""
        self._closed = True
        pending = list(self._retire_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._retire_tasks.clear()

        active: list[_RetiredEmbedClient] = []
        if self._owned_vertex_client is not None or self._owned_vertex_transport is not None:
            active.append(
                _RetiredEmbedClient(
                    client=self._owned_vertex_client,
                    transport=self._owned_vertex_transport,
                )
            )
            self._owned_vertex_client = None
            self._owned_vertex_transport = None
        if self._owned_developer_client is not None or self._owned_developer_transport is not None:
            active.append(
                _RetiredEmbedClient(
                    client=self._owned_developer_client,
                    transport=self._owned_developer_transport,
                )
            )
            self._owned_developer_client = None
            self._owned_developer_transport = None

        retired = list(self._retired_clients)
        self._retired_clients.clear()
        for entry in (*retired, *active):
            try:
                await self._close_entry(entry)
            except Exception:
                pass

    def _sanitize_text(self, raw: Any) -> str:
        text = clean_text(str(raw or ""))
        for secret in configured_secrets(self.s):
            if secret and len(secret) >= 6 and secret in text:
                text = text.replace(secret, "[REDACTED]")
        return text[: self.max_text_chars].strip()

    @staticmethod
    def _safe_purpose(purpose: str) -> str:
        raw = str(purpose or "").strip().lower()
        return raw if _SAFE_PURPOSE_RE.fullmatch(raw) else "embed"

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

    @staticmethod
    def _extract_observed_tokens(resp: Any) -> int | None:
        um = getattr(resp, "usage_metadata", None)
        if um is not None:
            for attr in ("prompt_token_count", "total_token_count"):
                val = getattr(um, attr, None)
                if isinstance(val, int) and not isinstance(val, bool) and 0 <= val <= 100_000_000:
                    return val
        embeddings = getattr(resp, "embeddings", None)
        if isinstance(embeddings, Sequence) and embeddings:
            total = 0.0
            all_known = True
            for emb in embeddings:
                stats = getattr(emb, "statistics", None)
                tc = getattr(stats, "token_count", None) if stats is not None else None
                if (
                    isinstance(tc, (int, float))
                    and not isinstance(tc, bool)
                    and math.isfinite(tc)
                    and tc >= 0
                ):
                    total += float(tc)
                else:
                    all_known = False
                    break
            if all_known:
                count = int(math.ceil(total))
                if 0 <= count <= 100_000_000:
                    return count
        return None

    @staticmethod
    def _extract_vectors(resp: Any, expected_count: int) -> list[bytes | None] | None:
        embeddings = getattr(resp, "embeddings", None)
        if not isinstance(embeddings, Sequence) or len(embeddings) != expected_count:
            return None
        packed: list[bytes | None] = []
        for emb in embeddings:
            values = getattr(emb, "values", None)
            if values is None:
                packed.append(None)
                continue
            try:
                packed.append(pack_semantic_vector(values))
            except ValueError:
                packed.append(None)
        return packed

    def _reserve_kwargs(self, provider: str) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model_lane": "embedding",
            "thinking_level": "none",
        }
        if _supports_provider_kwarg(self.store.reserve, type(self.store)):
            kwargs["provider"] = provider
        return kwargs

    def _settle_kwargs(
        self,
        *,
        usage: TokenUsage | None = None,
        discounted_actual: Decimal | None = None,
        fallback_actual: Decimal | None = None,
        provider: str,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "discounted_actual": discounted_actual,
            "fallback_actual": fallback_actual,
            "model_lane": "embedding",
            "thinking_level": "none",
        }
        if usage is not None:
            kwargs["usage"] = usage
        if _supports_provider_kwarg(self.store.settle, type(self.store)):
            kwargs["provider"] = provider
        return kwargs

    async def _safe_settle(
        self,
        reservation: str,
        actual: Decimal | None,
        *,
        usage: TokenUsage | None = None,
        discounted_actual: Decimal | None = None,
        fallback_actual: Decimal | None = None,
        provider: str,
    ) -> None:
        try:
            await self.store.settle(
                reservation,
                actual,
                **self._settle_kwargs(
                    usage=usage,
                    discounted_actual=discounted_actual,
                    fallback_actual=fallback_actual,
                    provider=provider,
                ),
            )
        except Exception:
            pass

    def _note_provider_failure(
        self, provider: Literal["vertex_paid", "gemini_paid"], status: int | None
    ) -> None:
        if status is not None and 400 <= status < 500 and status not in _COOLDOWN_HTTP_STATUSES:
            return
        until = time.monotonic() + max(0.0, float(self._cooldown_seconds))
        if provider == "vertex_paid":
            self._vertex_blocked_until = max(self._vertex_blocked_until, until)
        else:
            self._developer_blocked_until = max(self._developer_blocked_until, until)

    def _note_provider_success(self, provider: Literal["vertex_paid", "gemini_paid"]) -> None:
        if provider == "vertex_paid":
            self._vertex_blocked_until = 0.0
        else:
            self._developer_blocked_until = 0.0

    async def _call_provider_batch(
        self,
        *,
        client: Any,
        provider: Literal["vertex_paid", "gemini_paid"],
        texts: list[str],
        task_type: EmbeddingTaskType,
        budget_lane: str,
        subject: str,
        purpose: str,
    ) -> tuple[list[bytes | None] | None, bool]:
        """Execute one batch against a single provider with atomic reserve/settle.

        Returns ``(vectors_or_None, budget_blocked)``.
        """
        rate = Decimal(str(getattr(self.s, "embedding_input_usd_per_m", Decimal("0.15"))))
        if not rate.is_finite() or rate < 0:
            rate = Decimal("0.15")

        # Conservative upper bound: 2 tokens/UTF-8 byte + 32 overhead per item avoids overrun.
        reserve_tokens = sum(max(32, len(t.encode("utf-8")) * 2 + 32) for t in texts)
        reserve_usd = (Decimal(reserve_tokens) * rate) / Decimal(1_000_000)

        est_tokens = sum(max(1, estimate_tokens(t)) for t in texts)
        est_usd = (Decimal(est_tokens) * rate) / Decimal(1_000_000)
        floor = Decimal("0.000001") if reserve_usd > 0 else Decimal(0)
        fallback_usd = min(reserve_usd, max(floor, est_usd))

        work_kind = f"embedding.{self._safe_purpose(purpose)}"
        try:
            with model_work(work_kind, independent=True):
                reservation = await self.store.reserve(
                    subject,
                    budget_lane,
                    reserve_usd,
                    **self._reserve_kwargs(provider),
                )
        except Rejected:
            return None, True
        except Exception:
            return None, True

        model_name = (
            getattr(self.s, "embedding_model", DEFAULT_EMBEDDING_MODEL) or DEFAULT_EMBEDDING_MODEL
        )
        dim = int(getattr(self.s, "embedding_dim", DEFAULT_EMBEDDING_DIM) or DEFAULT_EMBEDDING_DIM)
        config = types.EmbedContentConfig(
            task_type=task_type,
            output_dimensionality=dim,
        )

        try:
            async with self._slots:
                async with asyncio.timeout(self.timeout):
                    resp = await client.aio.models.embed_content(
                        model=model_name,
                        contents=texts,
                        config=config,
                    )
        except asyncio.CancelledError:
            await asyncio.shield(
                self._safe_settle(
                    reservation,
                    None,
                    fallback_actual=fallback_usd,
                    provider=provider,
                )
            )
            raise
        except (TimeoutError, httpx.TimeoutException):
            await asyncio.shield(
                self._safe_settle(
                    reservation,
                    None,
                    fallback_actual=fallback_usd,
                    provider=provider,
                )
            )
            self._note_provider_failure(provider, None)
            return None, False
        except Exception as exc:
            status = self._http_status(exc)
            if status in _NON_BILLABLE_HTTP_STATUSES:
                await asyncio.shield(
                    self._safe_settle(
                        reservation,
                        Decimal(0),
                        discounted_actual=Decimal(0),
                        provider=provider,
                    )
                )
            else:
                await asyncio.shield(
                    self._safe_settle(
                        reservation,
                        None,
                        fallback_actual=fallback_usd,
                        provider=provider,
                    )
                )
            self._note_provider_failure(provider, status)
            return None, False

        observed_tokens = self._extract_observed_tokens(resp)
        if observed_tokens is not None:
            usage = TokenUsage(
                input_tokens=observed_tokens,
                output_tokens=0,
                model_lane="embedding",
                thinking_level="none",
            )
            actual = (Decimal(observed_tokens) * rate) / Decimal(1_000_000)
            await self._safe_settle(
                reservation,
                actual,
                usage=usage,
                discounted_actual=actual,
                provider=provider,
            )
        else:
            await self._safe_settle(
                reservation,
                None,
                fallback_actual=fallback_usd,
                provider=provider,
            )

        packed = self._extract_vectors(resp, len(texts))
        if packed is None:
            return None, False
        self._note_provider_success(provider)
        return packed, False

    def _chunk_items(self, items: list[tuple[int, str]]) -> list[list[tuple[int, str]]]:
        chunks: list[list[tuple[int, str]]] = []
        current: list[tuple[int, str]] = []
        current_bytes = 0
        for idx, text in items:
            item_bytes = len(text.encode("utf-8"))
            if current and (
                len(current) >= self.max_batch_size
                or current_bytes + item_bytes > self.max_batch_bytes
            ):
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append((idx, text))
            current_bytes += item_bytes
        if current:
            chunks.append(current)
        return chunks

    async def embed_texts(
        self,
        texts: Sequence[str],
        *,
        task_type: EmbeddingTaskType = "RETRIEVAL_DOCUMENT",
        budget_lane: str = "memory",
        subject: str = "embeddings",
        purpose: str = "memory_embed",
    ) -> list[bytes | None]:
        """Embed a sequence of texts into packed ``E1`` semantic vector blobs.

        Priority: Vertex AI first -> Gemini Developer API fallback -> ``None`` per item.
        Never raises unhandled exceptions into callers (except ``asyncio.CancelledError``).
        """
        if not texts:
            return []
        results: list[bytes | None] = [None] * len(texts)
        if self._closed or not getattr(self.s, "embedding_enabled", True):
            return results

        resolved_task: EmbeddingTaskType = (
            task_type if task_type in VALID_TASK_TYPES else "RETRIEVAL_DOCUMENT"
        )
        active: list[tuple[int, str]] = []
        for idx, raw in enumerate(texts):
            cleaned = self._sanitize_text(raw)
            if cleaned:
                active.append((idx, cleaned))
        if not active:
            return results

        try:
            await self._ensure_routing_ready()
        except asyncio.CancelledError:
            raise
        except Exception:
            return results

        use_vertex = self._should_use_vertex()
        fallback_allowed = self._vertex_fallback_allowed()

        for batch in self._chunk_items(active):
            batch_texts = [text for _, text in batch]
            vectors: list[bytes | None] | None = None
            budget_blocked = False
            now = time.monotonic()

            if use_vertex and now >= self._vertex_blocked_until:
                vertex_client = self._get_vertex_client()
                if vertex_client is not None:
                    vectors, budget_blocked = await self._call_provider_batch(
                        client=vertex_client,
                        provider="vertex_paid",
                        texts=batch_texts,
                        task_type=resolved_task,
                        budget_lane=budget_lane,
                        subject=subject,
                        purpose=purpose,
                    )

            if budget_blocked:
                break

            if vectors is None and (not use_vertex or fallback_allowed):
                if time.monotonic() >= self._developer_blocked_until:
                    dev_client = self._get_developer_client()
                    if dev_client is not None:
                        vectors, budget_blocked = await self._call_provider_batch(
                            client=dev_client,
                            provider="gemini_paid",
                            texts=batch_texts,
                            task_type=resolved_task,
                            budget_lane=budget_lane,
                            subject=subject,
                            purpose=purpose,
                        )
                        if budget_blocked:
                            break

            if vectors is not None:
                for (orig_idx, _), vec in zip(batch, vectors, strict=True):
                    results[orig_idx] = vec

        return results

    async def embed_one(
        self,
        text: str,
        *,
        task_type: EmbeddingTaskType = "RETRIEVAL_DOCUMENT",
        budget_lane: str = "memory",
        subject: str = "embeddings",
        purpose: str = "memory_embed",
    ) -> bytes | None:
        """Embed a single text into a packed ``E1`` semantic vector blob, or ``None``."""
        if not text:
            return None
        items = await self.embed_texts(
            [text],
            task_type=task_type,
            budget_lane=budget_lane,
            subject=subject,
            purpose=purpose,
        )
        return items[0] if items else None
