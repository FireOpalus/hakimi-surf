"""Tests for semantic vector codec, Vertex AI-first EmbeddingClient, config, and billing."""

from __future__ import annotations

import asyncio
import json
import math
import struct
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from google import genai
from google.genai import types
from pydantic import ValidationError

import hakimi.embeddings as embeddings_mod
from hakimi.budget_policy import BudgetConfig
from hakimi.config import Settings
from hakimi.dual_memory import similarity as legacy_similarity
from hakimi.dual_memory import vector as legacy_vector
from hakimi.embeddings import (
    DEFAULT_EMBEDDING_DIM,
    DEFAULT_EMBEDDING_MODEL,
    LEGACY_HASH_BYTES,
    LEGACY_HASH_DIM,
    SEMANTIC_MAGIC,
    EmbeddingClient,
    cosine_similarity_blobs,
    is_semantic_blob,
    pack_semantic_vector,
    unpack_semantic_vector,
)
from hakimi.gemini import GeminiGateway
from hakimi.provider_routing import (
    ProviderCredentialsUpdate,
    ProviderRoutingConfig,
    ProviderRoutingManager,
)
from hakimi.usage_accounting import TokenUsage
from tests.conftest import cached_rsa_private_key_pem
from tests.test_sdk_contract import decision_response, interaction_response, stream_response


def _make_sa_json(
    *,
    project_id: str = "hakimi-embed-proj",
    client_email: str = "hakimi-embed@hakimi-embed-proj.iam.gserviceaccount.com",
) -> str:
    return json.dumps(
        {
            "type": "service_account",
            "project_id": project_id,
            "private_key_id": "embed-key-1",
            "private_key": cached_rsa_private_key_pem(),
            "client_email": client_email,
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )


def _mock_vertex_genai_client(handler, *, location: str | None = "us-central1"):
    transport = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    client = genai.Client(
        enterprise=True,
        vertexai=True,
        api_key="vertex-embed-test-key",
        project="test-proj" if location else None,
        location=location,
        http_options={
            "httpx_async_client": transport,
            "retry_options": {"attempts": 1},
            "client_args": {"trust_env": False},
            "async_client_args": {"trust_env": False},
        },
    )
    return client, transport


def _mock_dev_genai_client(handler):
    transport = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    client = genai.Client(
        enterprise=False,
        vertexai=False,
        api_key="dev-embed-test-key",
        http_options={
            "api_version": "v1beta",
            "base_url": "https://generativelanguage.googleapis.com",
            "httpx_async_client": transport,
            "retry_options": {"attempts": 1},
            "client_args": {"trust_env": False},
            "async_client_args": {"trust_env": False},
        },
    )
    return client, transport


async def _close_mock(client, transport):
    await client.aio.aclose()
    client.close()
    await transport.aclose()


class _FakeEmbedModels:
    def __init__(self, fn):
        self._fn = fn
        self.calls: list[dict] = []

    async def embed_content(self, *, model, contents, config=None):
        self.calls.append({"model": model, "contents": list(contents), "config": config})
        return await self._fn(model=model, contents=contents, config=config)


class _FakeGenAIClient:
    def __init__(self, fn):
        self.models_impl = _FakeEmbedModels(fn)
        self.closed = False
        self.aio = SimpleNamespace(models=self.models_impl, aclose=self._aclose)

    async def _aclose(self):
        self.closed = True

    def close(self):
        self.closed = True


def _vertex_predict_response(vectors: list[list[float]], token_counts: list[int] | None = None):
    predictions = []
    for idx, vec in enumerate(vectors):
        emb: dict = {"values": vec}
        if token_counts is not None:
            emb["statistics"] = {"token_count": float(token_counts[idx]), "truncated": False}
        predictions.append({"embeddings": emb})
    return {"predictions": predictions}


def _mldev_batch_response(vectors: list[list[float]]):
    return {"embeddings": [{"values": vec} for vec in vectors]}


# ---------------------------------------------------------------------------
# 1. Pure Vector Codec Tests
# ---------------------------------------------------------------------------


def test_vector_codec_constants_and_pack_unpack_roundtrip():
    assert SEMANTIC_MAGIC == b"E1"
    assert DEFAULT_EMBEDDING_MODEL == "gemini-embedding-001"
    assert DEFAULT_EMBEDDING_DIM == 768

    blob = pack_semantic_vector([3.0, 4.0])
    assert blob[:2] == b"E1"
    assert struct.unpack_from("<H", blob, 2) == (2,)
    assert len(blob) == 4 + 2 * 2
    assert is_semantic_blob(blob) is True

    decoded = unpack_semantic_vector(blob)
    assert decoded is not None
    kind, values = decoded
    assert kind == "semantic"
    assert len(values) == 2
    assert values[0] == pytest.approx(0.6, abs=1e-3)
    assert values[1] == pytest.approx(0.8, abs=1e-3)

    # 768-dim realistic vector normalizes to unit length even when unnormalized inputs are passed.
    raw_768 = [float((i % 13) - 6) * 2.5 for i in range(DEFAULT_EMBEDDING_DIM)]
    blob_768 = pack_semantic_vector(raw_768)
    assert len(blob_768) == 4 + DEFAULT_EMBEDDING_DIM * 2
    assert is_semantic_blob(blob_768) is True
    kind_768, vals_768 = unpack_semantic_vector(blob_768)
    assert kind_768 == "semantic" and len(vals_768) == DEFAULT_EMBEDDING_DIM
    norm_768 = math.sqrt(sum(v * v for v in vals_768))
    assert norm_768 == pytest.approx(1.0, abs=1e-2)


def test_pack_semantic_vector_zero_huge_and_rejections():
    # Zero vector packs cleanly without ZeroDivisionError.
    zero_blob = pack_semantic_vector([0.0, 0.0, 0.0, 0.0])
    assert is_semantic_blob(zero_blob) is True
    assert unpack_semantic_vector(zero_blob) == ("semantic", (0.0, 0.0, 0.0, 0.0))
    unit_blob = pack_semantic_vector([1.0, 0.0, 0.0, 0.0])
    assert cosine_similarity_blobs(zero_blob, unit_blob) == pytest.approx(0.0)

    # Very large magnitudes normalize without overflow.
    huge_blob = pack_semantic_vector([1e200, 1e200])
    _, huge_vals = unpack_semantic_vector(huge_blob)
    assert huge_vals[0] == pytest.approx(math.sqrt(0.5), abs=1e-3)
    assert huge_vals[1] == pytest.approx(math.sqrt(0.5), abs=1e-3)

    # Integer inputs accepted cleanly.
    int_blob = pack_semantic_vector([3, 4])
    assert cosine_similarity_blobs(int_blob, pack_semantic_vector([3.0, 4.0])) == pytest.approx(
        1.0, abs=1e-3
    )

    # Invalid inputs rejected with ValueError.
    for bad in (
        [],
        [float("nan"), 1.0],
        [float("inf"), 1.0],
        [float("-inf"), 0.0],
        [True, False],
        ["1.0"],
        "not-a-vector",
        b"bytes",
    ):
        with pytest.raises(ValueError):
            pack_semantic_vector(bad)  # type: ignore[arg-type]


def test_unpack_legacy_hash_and_disambiguation_and_cosine():
    leg_a = legacy_vector("今天去吃火锅")
    leg_b = legacy_vector("今天去吃火锅很开心")
    assert len(leg_a) == LEGACY_HASH_BYTES
    assert is_semantic_blob(leg_a) is False

    unpacked = unpack_semantic_vector(leg_a)
    assert unpacked is not None
    assert unpacked[0] == "legacy_hash"
    assert len(unpacked[1]) == LEGACY_HASH_DIM

    # Cosine similarity on legacy blobs matches legacy_similarity().
    assert cosine_similarity_blobs(leg_a, leg_b) == pytest.approx(
        legacy_similarity(leg_a, leg_b), abs=1e-5
    )

    # Semantic dim=382 has exact byte length 4 + 382*2 == 768 == LEGACY_HASH_BYTES,
    # and must still decode unambiguously as semantic, not legacy_hash.
    sem_382 = pack_semantic_vector([1.0] + [0.0] * 381)
    assert len(sem_382) == LEGACY_HASH_BYTES
    assert is_semantic_blob(sem_382) is True
    assert unpack_semantic_vector(sem_382)[0] == "semantic"
    assert cosine_similarity_blobs(sem_382, leg_a) is None

    # Semantic cosine properties: identical, orthogonal, opposite, mismatched dim/kind.
    v1 = pack_semantic_vector([1.0, 0.0, 0.0, 0.0])
    v2 = pack_semantic_vector([0.0, 1.0, 0.0, 0.0])
    v3 = pack_semantic_vector([-1.0, 0.0, 0.0, 0.0])
    v_dim2 = pack_semantic_vector([1.0, 0.0])
    assert cosine_similarity_blobs(v1, v1) == pytest.approx(1.0, abs=1e-3)
    assert cosine_similarity_blobs(v1, v2) == pytest.approx(0.0, abs=1e-3)
    assert cosine_similarity_blobs(v1, v3) == pytest.approx(-1.0, abs=1e-3)
    assert cosine_similarity_blobs(v1, v_dim2) is None
    assert cosine_similarity_blobs(v1, leg_a) is None

    # Corrupt / truncated / NaN blobs return None cleanly.
    nan_e1 = b"E1" + struct.pack("<H", 2) + struct.pack("<2e", float("nan"), 1.0)
    nan_legacy = struct.pack("<192f", *([float("nan")] + [0.0] * 191))
    assert unpack_semantic_vector(b"") is None
    assert unpack_semantic_vector(b"E1\x00\x00") is None
    assert unpack_semantic_vector(b"E1\x04\x00\x00\x00") is None
    assert unpack_semantic_vector(nan_e1) is None
    assert unpack_semantic_vector(nan_legacy) is None
    assert is_semantic_blob(b"E1\x00\x00") is False
    assert is_semantic_blob(b"E1\x04\x00\x00\x00") is False
    assert is_semantic_blob(b"bad") is False
    assert cosine_similarity_blobs(nan_e1, v_dim2) is None
    assert cosine_similarity_blobs(b"bad", v1) is None


def test_cosine_similarity_optional_numpy_fast_path(monkeypatch):
    """Verify the optional numpy fast path produces identical results when numpy is present."""

    class _FakeArray:
        def __init__(self, vals):
            self.vals = [float(v) for v in vals]

        def astype(self, _dtype):
            return self

    class _FakeNumpy:
        float32 = "float32"

        @staticmethod
        def frombuffer(buf, dtype, offset=0):
            fmt = "<{}e" if dtype == "<f2" else "<{}f"
            item_size = 2 if dtype == "<f2" else 4
            count = (len(buf) - offset) // item_size
            return _FakeArray(struct.unpack_from(fmt.format(count), buf, offset))

        @staticmethod
        def isfinite(arr):
            return [math.isfinite(x) for x in arr.vals]

        @staticmethod
        def all(flags):
            return all(flags)

        @staticmethod
        def dot(a, b):
            return sum(x * y for x, y in zip(a.vals, b.vals, strict=True))

    monkeypatch.setattr(embeddings_mod, "_np", _FakeNumpy)
    a = pack_semantic_vector([3.0, 4.0])
    b = pack_semantic_vector([4.0, 3.0])
    leg = legacy_vector("测试向量")
    assert cosine_similarity_blobs(a, b) == pytest.approx(0.96, abs=1e-2)
    assert cosine_similarity_blobs(leg, leg) == pytest.approx(1.0, abs=1e-5)
    assert cosine_similarity_blobs(a, leg) is None
    assert cosine_similarity_blobs(a, pack_semantic_vector([1.0, 0.0, 0.0])) is None


# ---------------------------------------------------------------------------
# 2. Settings, TokenUsage, and Vertex Region Resolution Tests
# ---------------------------------------------------------------------------


def test_embedding_settings_aliases_and_validation(tmp_path, monkeypatch):
    monkeypatch.setenv("EMBEDDING_ENABLED", "false")
    monkeypatch.setenv("EMBEDDING_MODEL", "gemini-embedding-001")
    monkeypatch.setenv("EMBEDDING_DIM", "512")
    monkeypatch.setenv("VERTEX_EMBEDDING_LOCATION", "europe-west4")
    monkeypatch.setenv("EMBEDDING_INPUT_USD_PER_M", "0.20")

    s = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        gemini_api_key="paid-env-secret-abcdef",
        encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    )
    assert s.embedding_enabled is False
    assert s.embedding_model == "gemini-embedding-001"
    assert s.embedding_dim == 512
    assert s.vertex_embedding_location == "europe-west4"
    assert s.embedding_input_usd_per_m == Decimal("0.20")

    # TokenUsage accepts model_lane="embedding".
    tu = TokenUsage(input_tokens=42, output_tokens=0, model_lane="embedding", thinking_level="none")
    assert tu.model_lane == "embedding" and tu.input_tokens == 42

    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            data_dir=tmp_path / "data2",
            gemini_api_key="paid-env-secret-abcdef",
            encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            embedding_dim=0,
        )
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            data_dir=tmp_path / "data3",
            gemini_api_key="paid-env-secret-abcdef",
            encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            embedding_input_usd_per_m=Decimal("-0.01"),
        )


async def test_vertex_embedding_location_resolution_and_client_building(settings, store):
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    embedder = EmbeddingClient(settings, store, routing=routing)
    try:
        # Default vertex_location='global' -> resolves to 'us-central1' for embeddings.
        assert routing.config.vertex_location == "global"
        assert routing.effective_vertex_embedding_location() == "us-central1"
        assert embedder.effective_vertex_embedding_location() == "us-central1"

        # Non-global routing location is inherited unless vertex_embedding_location is pinned.
        await routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="service_account",
                vertex_location="europe-west4",
            )
        )
        assert routing.effective_vertex_embedding_location() == "europe-west4"

        # Multi-regional endpoints ('us', 'eu') 404 on gemini-embedding-001 -> map to regions.
        await routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="service_account",
                vertex_location="us",
            )
        )
        assert routing.effective_vertex_embedding_location() == "us-central1"
        assert embedder.effective_vertex_embedding_location() == "us-central1"

        await routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="service_account",
                vertex_location="eu",
            )
        )
        assert routing.effective_vertex_embedding_location() == "europe-west4"
        assert embedder.effective_vertex_embedding_location() == "europe-west4"

        # Switching routing back to global falls back to us-central1.
        await routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="service_account",
                vertex_location="global",
            )
        )
        assert routing.effective_vertex_embedding_location() == "us-central1"

        # Service account client builds against us-central1-aiplatform.googleapis.com.
        await routing.update_credentials(
            ProviderCredentialsUpdate(gcp_service_account_json=_make_sa_json())
        )
        embedder.reload_provider_clients()
        v_client = embedder._get_vertex_client()
        assert v_client is not None
        assert v_client._api_client.vertexai is True
        assert v_client._api_client.location == "us-central1"
        assert (
            "us-central1-aiplatform.googleapis.com" in v_client._api_client._http_options.base_url
        )

        # Explicit vertex_embedding_location overrides even a regional vertex_location.
        settings.vertex_embedding_location = "asia-northeast1"
        await routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="api_key",
                vertex_project="my-proj",
                vertex_location="europe-west4",
            )
        )
        await routing.update_credentials(
            ProviderCredentialsUpdate(vertex_api_key="vertex-key-test-123456")
        )
        assert routing.effective_vertex_embedding_location() == "asia-northeast1"
        embedder.reload_provider_clients()
        v_key_client = embedder._get_vertex_client()
        assert v_key_client is not None
        assert v_key_client._api_client.location == "asia-northeast1"
        assert (
            "asia-northeast1-aiplatform.googleapis.com"
            in v_key_client._api_client._http_options.base_url
        )

        # Developer fallback client targets MLDev v1beta with vertexai=False.
        dev_client = embedder._get_developer_client()
        assert dev_client is not None
        assert dev_client._api_client.vertexai is False
        assert dev_client._api_client._http_options.api_version == "v1beta"
        assert "generativelanguage.googleapis.com" in dev_client._api_client._http_options.base_url
    finally:
        await embedder.close()


# ---------------------------------------------------------------------------
# 3. Wire Contract & Billing Settlement Tests (Vertex First -> MLDev Fallback)
# ---------------------------------------------------------------------------


async def test_vertex_first_wire_contract_and_billing_settlement(settings, store):
    settings.embedding_dim = 4
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    await routing.set_config(ProviderRoutingConfig(vertex_enabled=True))

    vertex_requests: list[tuple[str, dict]] = []
    dev_requests: list[str] = []

    def vertex_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        vertex_requests.append((str(request.url), body))
        return httpx.Response(
            200,
            json=_vertex_predict_response(
                [[3.0, 4.0, 0.0, 0.0], [0.0, 0.0, 5.0, 12.0]],
                token_counts=[10, 15],
            ),
        )

    def dev_handler(request: httpx.Request) -> httpx.Response:
        dev_requests.append(str(request.url))
        return httpx.Response(200, json=_mldev_batch_response([[1.0, 0.0, 0.0, 0.0]]))

    v_client, v_transport = _mock_vertex_genai_client(vertex_handler)
    d_client, d_transport = _mock_dev_genai_client(dev_handler)
    embedder = EmbeddingClient(
        settings,
        store,
        routing=routing,
        vertex_client=v_client,
        developer_client=d_client,
    )
    try:
        blobs = await embedder.embed_texts(
            ["哈基米喜欢小鱼干", "今天在MC挖了钻石"],
            task_type="RETRIEVAL_DOCUMENT",
            budget_lane="memory",
            subject="embeddings",
            purpose="memory_embed",
        )
        assert len(blobs) == 2
        assert all(b is not None and is_semantic_blob(b) for b in blobs)
        assert len(vertex_requests) == 1
        assert len(dev_requests) == 0

        url, payload = vertex_requests[0]
        assert "us-central1-aiplatform.googleapis.com" in url
        assert "publishers/google/models/gemini-embedding-001:predict" in url
        assert payload["parameters"]["outputDimensionality"] == 4
        assert payload["instances"][0]["task_type"] == "RETRIEVAL_DOCUMENT"
        assert payload["instances"][0]["content"] == "哈基米喜欢小鱼干"

        # Verify L2 normalization applied to output vectors.
        _, v0 = unpack_semantic_vector(blobs[0])
        assert v0[0] == pytest.approx(0.6, abs=1e-3)
        assert v0[1] == pytest.approx(0.8, abs=1e-3)

        # Verify ledger billing, provider attribution, work kind, and TokenUsage.
        ledger_row = await store.one("SELECT lane, subject, status, amount FROM ledger")
        assert ledger_row["lane"] == "memory"
        assert ledger_row["subject"] == "embeddings"
        assert ledger_row["status"] == "settled"
        # 25 tokens * $0.15 / 1M = $0.00000375 -> ceil to 4 microdollars
        assert ledger_row["amount"] == 4

        prov_row = await store.one("SELECT provider FROM ledger_provider")
        assert prov_row["provider"] == "vertex_paid"

        work_row = await store.one("SELECT kind, root_kind FROM ledger_work")
        assert work_row["kind"] == "embedding.memory_embed"
        assert work_row["root_kind"] == "embedding.memory_embed"

        usage_row = await store.one(
            "SELECT input_tokens, output_tokens, model_lane FROM ledger_usage"
        )
        assert (usage_row["input_tokens"], usage_row["output_tokens"], usage_row["model_lane"]) == (
            25,
            0,
            "embedding",
        )

        # Interaction cache ratio is unaffected by embedding calls.
        usage_summary = await store.usage()
        assert usage_summary["token_usage"]["today"]["input_tokens"] == 25
        assert usage_summary["token_usage"]["today"]["interaction_calls"] == 0
    finally:
        await embedder.close()
        await _close_mock(v_client, v_transport)
        await _close_mock(d_client, d_transport)


async def test_vertex_404_falls_back_to_developer_api_with_zero_vertex_charge(settings, store):
    settings.embedding_dim = 4
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    await routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=True)
    )

    vertex_calls = 0
    dev_requests: list[tuple[str, dict]] = []

    def vertex_handler(request: httpx.Request) -> httpx.Response:
        nonlocal vertex_calls
        vertex_calls += 1
        return httpx.Response(404, json={"error": {"code": 404, "message": "model not found"}})

    def dev_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        dev_requests.append((str(request.url), body))
        return httpx.Response(200, json=_mldev_batch_response([[0.0, 3.0, 4.0, 0.0]]))

    v_client, v_transport = _mock_vertex_genai_client(vertex_handler)
    d_client, d_transport = _mock_dev_genai_client(dev_handler)
    embedder = EmbeddingClient(
        settings,
        store,
        routing=routing,
        vertex_client=v_client,
        developer_client=d_client,
    )
    try:
        blob = await embedder.embed_one(
            "查询猫猫喜欢吃什么",
            task_type="RETRIEVAL_QUERY",
            budget_lane="embedding",
            subject="embeddings",
            purpose="share_resonance",
        )
        assert blob is not None and is_semantic_blob(blob)
        assert vertex_calls == 1
        assert len(dev_requests) == 1
        dev_url, dev_body = dev_requests[0]
        assert (
            "generativelanguage.googleapis.com/v1beta/models/"
            "gemini-embedding-001:batchEmbedContents" in dev_url
        )
        assert dev_body["requests"][0]["taskType"] == "RETRIEVAL_QUERY"
        assert dev_body["requests"][0]["outputDimensionality"] == 4

        # Vertex 404 settled at $0 (non-billable); Developer call settled via bounded fallback.
        rows = await store.all(
            "SELECT l.lane, l.status, l.amount, p.provider "
            "FROM ledger l JOIN ledger_provider p ON p.id=l.id ORDER BY l.rowid"
        )
        assert len(rows) == 2
        assert (rows[0]["provider"], rows[0]["status"], rows[0]["amount"]) == (
            "vertex_paid",
            "settled",
            0,
        )
        assert rows[1]["provider"] == "gemini_paid"
        assert rows[1]["status"] == "unknown"  # MLDev omits token_count; never invented
        assert rows[1]["amount"] >= 1

        # Usage category "embedding" shows up in store.usage_overview().
        overview = await store.usage_overview()
        cats = {c["category"]: c for c in overview["categories"]}
        assert "embedding" in cats
        assert cats["embedding"]["calls"] == 2

        # Second call during Vertex cooldown skips Vertex and goes straight to Developer API.
        blob2 = await embedder.embed_one("再来一次", task_type="SEMANTIC_SIMILARITY")
        assert blob2 is not None
        assert vertex_calls == 1
        assert len(dev_requests) == 2
    finally:
        await embedder.close()
        await _close_mock(v_client, v_transport)
        await _close_mock(d_client, d_transport)


async def test_vertex_fallback_disabled_and_all_fail_returns_none(settings, store):
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    await routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=False)
    )

    async def fail_vertex(**_kwargs):
        raise RuntimeError("vertex boom")

    dev_called = False

    async def ok_dev(**_kwargs):
        nonlocal dev_called
        dev_called = True
        return types.EmbedContentResponse(
            embeddings=[types.ContentEmbedding(values=[1.0, 0.0, 0.0, 0.0])]
        )

    v_client = _FakeGenAIClient(fail_vertex)
    d_client = _FakeGenAIClient(ok_dev)
    embedder = EmbeddingClient(
        settings, store, routing=routing, vertex_client=v_client, developer_client=d_client
    )
    try:
        res = await embedder.embed_texts(["hello"])
        assert res == [None]
        assert dev_called is False
    finally:
        await embedder.close()


async def test_disabled_empty_whitespace_secret_scrub_and_batch_chunking(settings, store):
    settings.embedding_dim = 4
    secret_val = settings.gemini_api_key.get_secret_value()

    received_batches: list[list[str]] = []

    async def fake_dev(*, model, contents, config):
        batch = list(contents)
        received_batches.append(batch)
        return types.EmbedContentResponse(
            embeddings=[
                types.ContentEmbedding(
                    values=[float(i + 1), 1.0, 0.0, 0.0],
                    statistics=types.ContentEmbeddingStatistics(token_count=4.0),
                )
                for i in range(len(batch))
            ]
        )

    d_client = _FakeGenAIClient(fake_dev)
    embedder = EmbeddingClient(
        settings,
        store,
        developer_client=d_client,
        max_batch_size=2,
    )
    try:
        # Empty input -> empty output, 0 calls.
        assert await embedder.embed_texts([]) == []
        assert await embedder.embed_one("") is None
        assert await embedder.embed_one("   \n\t ") is None
        assert received_batches == []

        # Mixed empty/non-empty preserves positional alignment, chunks by max_batch_size=2,
        # and redacts configured secrets before sending to remote API.
        out = await embedder.embed_texts(
            [
                f"my key is {secret_val} please hide",
                "   ",
                "second item",
                "",
                "third item",
            ]
        )
        assert len(out) == 5
        assert out[0] is not None and is_semantic_blob(out[0])
        assert out[1] is None
        assert out[2] is not None and is_semantic_blob(out[2])
        assert out[3] is None
        assert out[4] is not None and is_semantic_blob(out[4])

        assert len(received_batches) == 2
        assert len(received_batches[0]) == 2
        assert len(received_batches[1]) == 1
        assert secret_val not in received_batches[0][0]
        assert "[REDACTED]" in received_batches[0][0]

        # Disabled returns [None, ...] with 0 additional calls.
        settings.embedding_enabled = False
        assert await embedder.embed_texts(["a", "b"]) == [None, None]
        assert len(received_batches) == 2
    finally:
        await embedder.close()


async def test_budget_hard_stop_blocks_without_calling_api_or_raising(settings, store):
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    await routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=True)
    )
    await store.set_budget_config(BudgetConfig(memory_daily_usd=Decimal("0")))

    calls = 0

    async def should_not_run(**_kwargs):
        nonlocal calls
        calls += 1
        return types.EmbedContentResponse(
            embeddings=[types.ContentEmbedding(values=[1.0, 0.0, 0.0, 0.0])]
        )

    v_client = _FakeGenAIClient(should_not_run)
    d_client = _FakeGenAIClient(should_not_run)
    embedder = EmbeddingClient(
        settings, store, routing=routing, vertex_client=v_client, developer_client=d_client
    )
    try:
        # Memory lane exhausted -> returns None, zero calls on BOTH Vertex and Developer API.
        assert await embedder.embed_one("测试预算熔断", budget_lane="memory") is None
        assert calls == 0

        # Proactive lane still has budget -> succeeds.
        assert await embedder.embed_one("主动分享共鸣", budget_lane="proactive") is not None
        assert calls == 1
    finally:
        await embedder.close()


async def test_cancellation_settles_reservation_and_close_cleans_owned_transports(settings, store):
    started = asyncio.Event()
    gate = asyncio.Event()

    async def slow_dev(**_kwargs):
        started.set()
        await gate.wait()
        return types.EmbedContentResponse(
            embeddings=[types.ContentEmbedding(values=[1.0, 0.0, 0.0, 0.0])]
        )

    d_client = _FakeGenAIClient(slow_dev)
    embedder = EmbeddingClient(settings, store, developer_client=d_client)
    task = asyncio.create_task(embedder.embed_one("in-flight cancel test"))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Ledger row must be settled (never left stuck in 'reserved').
    row = await store.one("SELECT status FROM ledger ORDER BY rowid DESC LIMIT 1")
    assert row is not None and row["status"] == "unknown"

    # Injected mock client is NOT closed by embedder.close().
    await embedder.close()
    assert d_client.closed is False

    # Owned clients/transports (active + retired) ARE closed cleanly on close().
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    await routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_auth_mode="service_account")
    )
    await routing.update_credentials(
        ProviderCredentialsUpdate(gcp_service_account_json=_make_sa_json())
    )
    owned = EmbeddingClient(settings, store, routing=routing)
    assert owned._get_vertex_client() is not None
    first_vertex_transport = owned._owned_vertex_transport
    assert first_vertex_transport is not None and not first_vertex_transport.is_closed

    owned.reload_provider_clients()
    assert owned._get_vertex_client() is not None
    second_vertex_transport = owned._owned_vertex_transport
    assert owned._get_developer_client() is not None
    dev_transport = owned._owned_developer_transport

    await owned.close()
    assert first_vertex_transport.is_closed is True
    assert second_vertex_transport is not None and second_vertex_transport.is_closed is True
    assert dev_transport is not None and dev_transport.is_closed is True
    # Calls after close return None safely.
    assert await owned.embed_one("after close") is None


async def test_gemini_gateway_exposes_and_closes_embedding_client(settings, store):
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("decisions"):
            return stream_response(request, httpx.Response(200, json=decision_response()))
        return stream_response(request, httpx.Response(200, json=interaction_response()))

    t1 = httpx.AsyncClient(transport=httpx.MockTransport(handle), trust_env=False)
    t2 = httpx.AsyncClient(transport=httpx.MockTransport(handle), trust_env=False)
    c1 = genai.Client(
        enterprise=False,
        vertexai=False,
        api_key="test-only",
        http_options={"api_version": "v1beta", "httpx_async_client": t1},
    )
    c2 = genai.Client(
        enterprise=False,
        vertexai=False,
        api_key="test-only",
        http_options={"api_version": "v1", "httpx_async_client": t2},
    )
    gateway = GeminiGateway(settings, store, c1, c2)
    try:
        assert isinstance(gateway.embeddings, EmbeddingClient)
        assert gateway.embeddings.routing is gateway.routing
        # Lazy: no embedding transports created until actually used.
        assert gateway.embeddings._owned_developer_transport is None
        assert gateway.embeddings._owned_vertex_transport is None
        # Force building developer transport and confirm gateway.close() closes it.
        assert gateway.embeddings._get_developer_client() is not None
        dev_tr = gateway.embeddings._owned_developer_transport
        assert dev_tr is not None and not dev_tr.is_closed
    finally:
        await gateway.close()
        await _close_mock(c1, t1)
        await _close_mock(c2, t2)
    assert dev_tr.is_closed is True
