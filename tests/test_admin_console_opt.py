"""Regression tests for Admin Console UI & Backend Optimization (Batches 1-3)."""

from __future__ import annotations

import asyncio
import base64
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from admin.build import (
    _extract_js_id_refs,
    _IdCollector,
    audit_html,
    build_html,
    read_version,
)
from admin.csp import content_security_policy
from hakimi import (
    __version__,
    admin_common,
    admin_dashboard_routes,
    admin_policy_routes,
    admin_routes,
    admin_session_routes,
    life_admin,
    life_companion_admin,
    life_modules_admin,
    life_world_admin,
)
from hakimi.app import create_app
from hakimi.usage_accounting import TokenUsage
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_voice_gateway_admin import GW_TOKEN, _gw_cfg
from voice_gateway.app import create_gateway_app

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_DIR = REPO_ROOT / "admin"
ADMIN_HTML = ADMIN_DIR / "admin_page.html"
NGINX_CONF = ADMIN_DIR / "nginx.conf"


def test_admin_build_check_and_version_sync():
    assert read_version() == __version__
    proc = subprocess.run(
        [sys.executable, str(ADMIN_DIR / "build.py"), "--check"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    built = build_html()
    assert ADMIN_HTML.read_text(encoding="utf-8") == built
    assert f"v{__version__}" in built
    assert "{{VERSION}}" not in built


def test_admin_html_static_security_and_dom_integrity():
    html = ADMIN_HTML.read_text(encoding="utf-8")
    assert audit_html(html) == []

    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(styles) == 1
    assert len(scripts) == 1
    script = scripts[0]

    # Single authoritative DOM ID table: zero duplicates, zero missing JS refs, no life-batteries.
    collector = _IdCollector()
    collector.feed(html)
    assert len(collector.ids) == len(set(collector.ids))
    id_set = set(collector.ids)
    assert "life-batteries" not in html
    missing = sorted({ref for ref in _extract_js_id_refs(script) if ref not in id_set})
    assert missing == []

    # Strict CSP and zero unsafe DOM/dialog primitives.
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy
    assert "innerHTML" not in html
    assert "insertAdjacentHTML" not in html
    assert "outerHTML" not in html
    assert "document.write" not in html
    assert not re.search(r"\sstyle\s*=", html)
    assert not re.search(r"\son[a-z]+\s*=", html, re.I)
    assert not re.search(r"(?<![A-Za-z0-9_$.])(?:confirm|prompt|alert)\s*\(", script)

    # Key Batches 1-3 UI upgrades present in assembled document.
    for elem_id in ("notice-text", "notice-close", "modal-dialog", "vc-gw-tts-preview"):
        assert elem_id in id_set
    for marker in ("provider-fold", "card-edit-fold", "song-audio-slot", "page-hidden"):
        assert marker in html
    assert "global/overview" in script
    assert "audio_b64" in script


def test_nginx_conf_enables_gzip_safely_and_preserves_security():
    conf = NGINX_CONF.read_text(encoding="utf-8")
    assert re.search(r"^\s*gzip\s+on;", conf, re.M)
    assert re.search(r"^\s*gzip_vary\s+on;", conf, re.M)
    gzip_types_match = re.search(r"^\s*gzip_types\s+([^;]+);", conf, re.M)
    assert gzip_types_match is not None
    gzip_types = gzip_types_match.group(1).split()
    assert "text/html" not in gzip_types
    assert "application/json" in gzip_types

    for required in (
        "server_tokens off;",
        "client_max_body_size 20m;",
        "proxy_buffering off;",
        "auth_request /_admin_access;",
        'if ($args != "") { return 400; }',
        'add_header Cache-Control "no-store" always;',
        "add_header X-Content-Type-Options nosniff always;",
        "add_header X-Frame-Options DENY always;",
        "add_header Referrer-Policy no-referrer always;",
        "include /etc/nginx/admin-csp.conf;",
    ):
        assert required in conf


async def test_global_overview_is_lightweight_authenticated_and_rejects_query_params(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    app = create_app(settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        # Seed a usage record so full ledger carries heavier detail than lightweight overview.
        reservation = await svc.store.reserve("opt-test", "chat", Decimal(".01"))
        await svc.store.settle(
            reservation,
            Decimal(".0025"),
            usage=TokenUsage(1200, 400, cached_tokens=300, thought_tokens=50),
            discounted_actual=Decimal(".0018"),
        )
        await svc.store.runtime.flush()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            r_unauth = await client.get("/admin/api/global/overview", headers={"Authorization": ""})
            assert r_unauth.status_code == 401

            r_bad_token = await client.get(
                "/admin/api/global/overview", headers={"Authorization": "Bearer wrong"}
            )
            assert r_bad_token.status_code == 401

            r_query = await client.get("/admin/api/global/overview?refresh=1")
            assert r_query.status_code == 400

            r_overview = await client.get("/admin/api/global/overview")
            r_full = await client.get("/admin/api/global/dashboard")
            assert r_overview.status_code == 200
            assert r_full.status_code == 200

            overview_data = r_overview.json()
            full_data = r_full.json()
            assert set(overview_data.keys()) == set(full_data.keys())

            usage = overview_data["usage"]
            for key in (
                "day",
                "day_local",
                "month_local",
                "day_utc",
                "timezone",
                "budget_timezone",
                "lanes",
                "jobs",
            ):
                assert key in usage
            for heavy_key in (
                "token_usage",
                "cost_estimates",
                "history",
                "cache_health",
                "recent_work",
            ):
                assert heavy_key not in usage
                assert heavy_key in full_data["usage"]

            assert len(r_overview.content) < len(r_full.content)

            # Verify safe invalid_field hint surfaced by app.py without changing detail.
            r_bad_budget = await client.put("/admin/api/budget", json={"daily_usd": -5})
            assert r_bad_budget.status_code == 400
            bad_json = r_bad_budget.json()
            assert bad_json["detail"] == "invalid_admin_input"
            assert bad_json.get("invalid_field") == "daily_usd"
            assert r_bad_budget.headers.get("x-invalid-field") == "daily_usd"


async def test_voice_gateway_test_tts_audio_b64_and_lazy_voices_sync(settings, monkeypatch):
    brz_key = "brz_opt_secret_777777"
    fake_mp3 = b"ID3\x03\x00\x00OPT-TEST-MP3-PAYLOAD"
    gw_calls: list[tuple[str, str]] = []

    def upstream_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/voices":
            return httpx.Response(
                200,
                json={
                    "voices": [
                        {"voice_id": "voc_1", "name": "声线一", "voice_type": "personal"},
                    ]
                },
            )
        return httpx.Response(200, content=fake_mp3)

    monkeypatch.setattr(
        "voice_client.providers._make_default_http_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(upstream_handler)),
    )

    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "voice_reply_enabled": True,
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )

    gw_app1 = create_gateway_app(_gw_cfg())
    bot_app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with gw_app1.router.lifespan_context(gw_app1), bot_app.router.lifespan_context(bot_app):
        svc = bot_app.state.service
        gw_holder = {"app": gw_app1}

        async def _dispatch(request: httpx.Request) -> httpx.Response:
            gw_calls.append((request.method, request.url.path))
            if request.url.path == "/v1/tts":
                return httpx.Response(
                    200,
                    content=fake_mp3,
                    headers={
                        "content-type": "audio/mpeg",
                        "x-audio-format": "mp3",
                        "x-audio-duration-ms": "987",
                    },
                )
            transport = httpx.ASGITransport(app=gw_holder["app"])
            async with httpx.AsyncClient(
                transport=transport, base_url="http://hakimi-voice-gateway:12145"
            ) as gw_client:
                return await gw_client.request(
                    request.method,
                    request.url.raw_path.decode("ascii"),
                    headers=dict(request.headers),
                    content=request.content,
                )

        bridge_http = httpx.AsyncClient(transport=httpx.MockTransport(_dispatch))
        svc.voice_gateway_admin.http = bridge_http
        svc.voice_reply.http = bridge_http

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=bot_app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as client:
                r_put = await client.put(
                    "/admin/api/life/voice-companion/gateway",
                    json={
                        "stt_provider": "siliconflow",
                        "tts_provider": "breezeblue",
                        "breezeblue_api_key": brz_key,
                        "breezeblue_voice_id": "voc_1",
                    },
                )
                assert r_put.status_code == 200, r_put.text

                # 1. Gateway is already in sync: listing voices must NOT POST /v1/admin/config.
                gw_calls.clear()
                r_voices = await client.get("/admin/api/life/voice-companion/gateway/voices")
                assert r_voices.status_code == 200
                assert [v["voice_id"] for v in r_voices.json()["voices"]] == ["voc_1"]
                assert ("POST", "/v1/admin/config") not in gw_calls
                assert gw_calls == [("GET", "/v1/admin/voices")]

                # 2. After gateway restart (in-memory override lost): lazy resync + retry succeeds.
                gw_app2 = create_gateway_app(_gw_cfg())
                async with gw_app2.router.lifespan_context(gw_app2):
                    gw_holder["app"] = gw_app2
                    gw_calls.clear()
                    r_voices2 = await client.get("/admin/api/life/voice-companion/gateway/voices")
                    assert r_voices2.status_code == 200
                    assert [v["voice_id"] for v in r_voices2.json()["voices"]] == ["voc_1"]
                    assert gw_calls.count(("POST", "/v1/admin/config")) == 1
                    assert gw_calls[0] == ("GET", "/v1/admin/voices")

                # 3. POST /gateway/test returns audio_b64 + mime_type alongside existing fields.
                r_test = await client.post("/admin/api/life/voice-companion/gateway/test")
                assert r_test.status_code == 200
                body = r_test.json()
                assert body["ok"] is True
                assert body["bytes"] == len(fake_mp3)
                assert body["duration_ms"] == 987
                assert isinstance(body["latency_ms"], (int, float))
                assert body["fmt"] == "mp3"
                assert body["mime_type"] == "audio/mpeg"
                assert base64.b64decode(body["audio_b64"]) == fake_mp3
        finally:
            await bridge_http.aclose()


def test_split_routers_preserve_backward_compatible_exports_and_routes():
    for symbol in (
        "ADMIN_PAGE_URL",
        "router",
        "service",
        "body",
        "partial_body",
        "domain_lock",
        "read_binary_upload",
        "dashboard",
        "global_dashboard",
        "global_overview",
        "budget_save",
        "sessions",
    ):
        assert hasattr(admin_routes, symbol), f"admin_routes missing {symbol}"

    for symbol in (
        "router",
        "LifeConfigMirror",
        "WorldStateAdminUpdate",
        "life_status",
        "weather_set_location",
        "life_voice_companion_gateway_test",
    ):
        assert hasattr(life_admin, symbol), f"life_admin missing {symbol}"

    admin_app = FastAPI()
    admin_app.include_router(admin_routes.router)
    admin_paths = set(admin_app.openapi()["paths"].keys())
    for sub in (admin_dashboard_routes, admin_policy_routes, admin_session_routes):
        sub_app = FastAPI()
        sub_app.include_router(sub.router)
        sub_paths = set(sub_app.openapi()["paths"].keys())
        assert sub_paths and sub_paths.issubset(admin_paths)
    assert "/admin/api/global/overview" in admin_paths
    assert "/admin/api/global/dashboard" in admin_paths
    assert "/admin/api/dashboard" in admin_paths

    life_app = FastAPI()
    life_app.include_router(life_admin.router)
    life_paths = set(life_app.openapi()["paths"].keys())
    for sub in (life_world_admin, life_modules_admin, life_companion_admin):
        sub_app = FastAPI()
        sub_app.include_router(sub.router)
        sub_paths = set(sub_app.openapi()["paths"].keys())
        assert sub_paths and sub_paths.issubset(life_paths)
    assert "/admin/api/life/voice-companion/gateway/test" in life_paths


async def test_admin_common_domain_lock_partial_body_and_binary_upload(settings):
    # 1. Per-domain locks isolate distinct domains while reusing the same lock per domain.
    fake_req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
    lock_a1 = admin_common.domain_lock(fake_req, "budget")
    lock_a2 = admin_common.domain_lock(fake_req, "budget")
    lock_b = admin_common.domain_lock(fake_req, "weather")
    assert isinstance(lock_a1, asyncio.Lock)
    assert lock_a1 is lock_a2
    assert lock_a1 is not lock_b

    async with lock_a1:
        assert lock_a1.locked()
        assert not lock_b.locked()
        async with lock_b:
            assert lock_b.locked()

    # 2. partial_body merges patches, rejects empty payloads, and exposes safe invalid_field hints.
    class SampleCfg(BaseModel):
        model_config = ConfigDict(extra="forbid")
        daily_usd: float = Field(ge=0.0, le=100.0)
        label: str = Field(min_length=1, max_length=20)

    mini = FastAPI()
    mini.state.settings = settings

    @mini.put("/patch")
    async def _patch(request: Request):
        parsed = await admin_common.partial_body(
            request, {"daily_usd": 5.0, "label": "ok"}, SampleCfg
        )
        return parsed.model_dump()

    @mini.post("/upload")
    async def _upload(request: Request):
        data = await admin_common.read_binary_upload(
            request,
            allowed_mimes={"audio/mpeg"},
            max_bytes=16,
            timeout_seconds=1.0,
            compressed_error="upload_compressed",
            mime_error="upload_bad_mime",
            size_error="upload_too_large",
            timeout_error="upload_timeout",
        )
        return {"size": len(data)}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mini), base_url="http://127.0.0.1"
    ) as client:
        r_ok = await client.put("/patch", json={"daily_usd": 12.5})
        assert r_ok.status_code == 200
        assert r_ok.json() == {"daily_usd": 12.5, "label": "ok"}

        r_empty = await client.put("/patch", json={})
        assert r_empty.status_code == 400
        assert r_empty.json()["detail"] == "invalid_admin_input"

        r_bad_val = await client.put("/patch", json={"daily_usd": -999.0})
        assert r_bad_val.status_code == 400
        assert r_bad_val.json()["detail"] == "invalid_admin_input"
        assert r_bad_val.headers.get("x-invalid-field") == "daily_usd"
        assert "-999" not in r_bad_val.text

        r_extra = await client.put("/patch", json={"secret_leaked_key": "nope"})
        assert r_extra.status_code == 400
        assert r_extra.headers.get("x-invalid-field") == "unknown_field"
        assert "secret_leaked_key" not in r_extra.text

        # 3. read_binary_upload validates compression, MIME, size, and accepts valid bytes.
        r_comp = await client.post(
            "/upload",
            content=b"1234",
            headers={"content-type": "audio/mpeg", "content-encoding": "gzip"},
        )
        assert r_comp.status_code == 415 and r_comp.json()["detail"] == "upload_compressed"

        r_mime = await client.post(
            "/upload", content=b"1234", headers={"content-type": "image/png"}
        )
        assert r_mime.status_code == 415 and r_mime.json()["detail"] == "upload_bad_mime"

        r_big = await client.post(
            "/upload", content=b"x" * 17, headers={"content-type": "audio/mpeg"}
        )
        assert r_big.status_code == 413 and r_big.json()["detail"] == "upload_too_large"

        r_good = await client.post(
            "/upload", content=b"0123456789", headers={"content-type": "audio/mpeg"}
        )
        assert r_good.status_code == 200 and r_good.json() == {"size": 10}

    # Verify timeout path directly via minimal async stream mock.
    class _SlowRequest:
        headers = {"content-type": "audio/mpeg", "content-length": "4"}

        async def stream(self):
            await asyncio.sleep(0.2)
            yield b"late"

    with pytest.raises(HTTPException) as exc_info:
        await admin_common.read_binary_upload(
            _SlowRequest(),  # type: ignore[arg-type]
            allowed_mimes={"audio/mpeg"},
            max_bytes=16,
            timeout_seconds=0.02,
            compressed_error="upload_compressed",
            mime_error="upload_bad_mime",
            size_error="upload_too_large",
            timeout_error="upload_timeout",
        )
    assert exc_info.value.status_code == 408
    assert exc_info.value.detail == "upload_timeout"
