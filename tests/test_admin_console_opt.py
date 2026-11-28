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
from tests.js_runtime import find_js_engine, run_js_harness
from tests.test_admin import TOKEN
from tests.test_voice_gateway_admin import GW_TOKEN, _gw_cfg
from voice_gateway.app import create_gateway_app

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_DIR = REPO_ROOT / "admin"
NGINX_CONF = ADMIN_DIR / "nginx.conf"


def test_admin_build_check_and_version_sync(tmp_path):
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
    assert f"v{__version__}" in built
    assert "{{VERSION}}" not in built

    override = build_html(version_override="9.9.9")
    assert "v9.9.9" in override
    assert audit_html(override, version_override="9.9.9") == []

    out_path = tmp_path / "admin_page.html"
    proc_write = subprocess.run(
        [
            sys.executable,
            str(ADMIN_DIR / "build.py"),
            "--version",
            __version__,
            "--output",
            str(out_path),
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_write.returncode == 0, proc_write.stderr or proc_write.stdout
    assert out_path.read_text(encoding="utf-8") == built
    proc_verify = subprocess.run(
        [
            sys.executable,
            str(ADMIN_DIR / "build.py"),
            "--check",
            "--verify-file",
            "--output",
            str(out_path),
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_verify.returncode == 0, proc_verify.stderr or proc_verify.stdout


def test_admin_html_static_security_and_dom_integrity():
    html = build_html()
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
    assert re.search(r"^\s*gzip_static\s+on;", conf, re.M)
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

    assert "colab/drive/inbox" not in conf
    for exact_loc, expected_size in (
        ("location = /admin/api/songs/inbox", "client_max_body_size 100m;"),
        ("location = /admin/api/songs/upload", "client_max_body_size 100m;"),
        ("location = /admin/api/songs/ref-voices", "client_max_body_size 25m;"),
    ):
        m = re.search(
            re.escape(exact_loc) + r"\s*\{([^}]*)\}",
            conf,
            re.S,
        )
        assert m is not None, f"missing {exact_loc} block"
        block = m.group(1)
        assert expected_size in block
        assert "proxy_request_buffering off;" in block
        assert conf.index(exact_loc) < conf.index("location /admin/api/")


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


def test_unified_cloud_resource_view_dom_contract_and_csp():
    """Verify Unified Cloud Resource View DOM IDs, #6 Colab schema hooks, SVG chart, and CSP."""
    html = build_html()
    assert audit_html(html) == []

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)
    assert len(collector.ids) == len(id_set)

    required_ids = (
        "cloud-resources-card",
        "cloud-sync-bq",
        "cloud-sync-mon",
        "cloud-refresh",
        "cloud-gcp-pills",
        "cloud-gcp-credit-label",
        "cloud-gcp-credit-bar",
        "cloud-gcp-kpis",
        "cloud-gcp-sync-meta",
        "cloud-trend-14d",
        "cloud-trend-30d",
        "cloud-colab-pills",
        "cloud-colab-kpis",
        "cloud-warm-sessions",
        "cloud-trend-readout",
        "cloud-cu-chart",
        "cloud-cu-chart-note",
        "cloud-gpu-breakdown",
        "cloud-accel-stats",
        "cloud-accel-note",
        "colab-analytics-card",
        "colab-trend-14d",
        "colab-trend-30d",
        "colab-warm-sessions",
        "colab-trend-readout",
        "colab-cu-chart",
        "colab-cu-chart-note",
        "colab-gpu-breakdown",
        "colab-accel-stats",
        "colab-accel-note",
        "overview-go-colab",
    )
    for elem_id in required_ids:
        assert elem_id in id_set, f"Missing unified cloud DOM id: {elem_id}"

    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(scripts) == 1
    script = scripts[0]

    # Verify #6 Colab contract fields & GCP reconciliation fields are consumed without query params.
    for token in (
        "cu_trend",
        "gpu_breakdown",
        "warm_sessions",
        "acceleration_stats",
        "last_14d_cu_used",
        "last_30d_cu_used",
        "remaining_ttl_seconds",
        "drive_direct_jobs",
        "drive_direct_fallbacks",
        "warm_reuse_hits",
        "warm_cold_starts",
        "cache_weight_hits",
        "cache_weight_misses",
        "cache_venv_hits",
        "cache_venv_misses",
        "reconciliation",
        "cost_delta_usd",
        "cost_delta_after_unsettled_usd",
        "createElementNS",
        "http://www.w3.org/2000/svg",
    ):
        assert token in script, f"Expected cloud resource contract token {token!r} in JS"

    # Zero query parameters in API routes, zero external chart CDNs, strict CSP.
    assert not re.search(r"api\(\s*['\"][^'\"\n]*\?", script)
    assert "chart.js" not in html.lower()
    assert "echarts" not in html.lower()
    assert "d3.min.js" not in html.lower()
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy


def test_unified_cloud_resource_view_js_runtime_and_degradation():
    """Execute assembled JS renderers in a mock DOM (jsc/node) to verify rendering & fallback."""
    import json

    find_js_engine("No local JS engine (node/jsc) available for DOM renderer execution test")

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    ledger_js = (ADMIN_DIR / "src" / "js" / "02-overview-ledger.js").read_text(encoding="utf-8")
    shell_js = (ADMIN_DIR / "src" / "js" / "08-modules-shell.js").read_text(encoding="utf-8")
    colab_start = shell_js.index("const colabErrorLabels=")
    colab_end = shell_js.index("async function colabCompute(")
    colab_view_js = shell_js[colab_start:colab_end]

    # Collect every static HTML ID so the mock document returns a valid MockElement for all of them.
    html = build_html()
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))

    harness = f"""
    const ALL_IDS = {all_ids_json};
    class MockNode {{
      constructor(tag, ns) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.namespaceURI = ns || null;
        this.children = [];
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.value = '';
        this.className = '';
        this.open = false;
        this.classList = {{
          add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }}
        }};
      }}
      get textContent() {{
        if (this.children.length) {{
          return this._text + this.children.map(c => c.textContent).join('');
        }}
        return this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            this.children.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this._text = ''; this.append(...nodes);
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) {{
          fn({{ target: this, preventDefault(){{}} }});
        }}
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest() {{ return null; }}
      focus() {{}}
      select() {{}}
      remove() {{}}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createElementNS(ns, tag) {{ return new MockNode(tag, ns); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function previewWakeWindow(){{}}
    function lifeFmt(v){{ return String(v||''); }}
    function showNotebookAvailability(){{}}
    function stampDashboard(){{}}
    function providerBadge(p){{ return String(p||''); }}

    {core_js}
    {ledger_js}
    {colab_view_js}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    // 1. Full contract rendering (GCP reconciliation + 30d Colab trend + warm + GPU + accel)
    const trend30 = [];
    for (let i = 1; i <= 30; i++) {{
      trend30.push({{
        date: '2026-11-' + String(i).padStart(2, '0'),
        jobs: i % 3 === 0 ? 2 : (i % 2),
        succeeded: i % 2,
        failed: i % 3 === 0 ? 1 : 0,
        cancelled: 0,
        cu_used: Number((i * 0.12).toFixed(3))
      }});
    }}
    const xss = '<img src=x onerror=alert(1)>';
    const fullColab = {{
      configured: true,
      source: 'sqlite',
      config: {{
        enabled: true, job_timeout_seconds: 1800, auto_terminate_orphan_minutes: 20
      }},
      supported_job_types: ['rvc_cover', 'rvc_train'],
      job_types: [
        {{
          job_type: 'rvc_cover',
          owner_module: 'song_studio',
          default_gpu: 'L4',
          fallback_gpus: ['T4'],
          allowed_gpus: ['L4', 'T4', 'A100', 'H100'],
          timeout_seconds: 1800
        }},
        {{
          job_type: 'rvc_train',
          owner_module: 'song_studio-' + xss,
          default_gpu: 'H100',
          fallback_gpus: ['A100'],
          allowed_gpus: ['H100', 'A100'],
          timeout_seconds: 3600
        }}
      ],
      worker: {{ reachable: true, compute_units: 42.5, active_sessions: 1, running_job_id: null }},
      today: {{ jobs_count: 2, cu_used: 0.85 }},
      jobs: [],
      summary: {{
        total_jobs: 18, succeeded_jobs: 16, failed_jobs: 2, cancelled_jobs: 0,
        running_jobs: 0, queued_jobs: 0, success_rate: 16/18,
        total_cu_used: 12.345, last_14d_cu_used: 6.789, last_30d_cu_used: 12.345,
        last_14d_jobs: 10, last_30d_jobs: 18
      }},
      cu_trend: trend30,
      gpu_breakdown: [
        {{ gpu: 'L4', jobs: 15, succeeded: 14, failed: 1, cu_used: 10.2, cu_share: 0.826 }},
        {{ gpu: 'T4', jobs: 3, succeeded: 2, failed: 1, cu_used: 2.145, cu_share: 0.174 }}
      ],
      warm_sessions: [
        {{
          session_name: 'sess-' + xss,
          gpu: 'L4',
          idle_ttl_seconds: 360,
          remaining_ttl_seconds: 215,
          created_at: 1760000000,
          last_used_at: 1760000100,
          expires_at: 1760000460,
          reuse_count: 3,
          setup_ready: true
        }}
      ],
      acceleration_stats: {{
        warm_pool_enabled: true, warm_allowed_gpus: ['L4', 'T4'],
        warm_idle_ttl_seconds: 360, warm_max_ttl_seconds: 900, warm_active_count: 1,
        drive_direct_jobs: 12, drive_direct_fallbacks: 2,
        warm_reuse_hits: 9, warm_cold_starts: 5,
        cache_weight_hits: 11, cache_weight_misses: 3,
        cache_venv_hits: 8, cache_venv_misses: 6, measured_jobs: 14
      }}
    }};
    cachedColabOverview = fullColab;
    renderUnifiedCloudColab(fullColab, '');
    renderColabAnalyticsOnPage(fullColab, 'colab');
    assert(
      $('cloud-colab-pills').textContent.includes('调度开启 · 按模块自选 GPU'),
      'cloud-colab-pills per-module GPU pill missing'
    );
    assert(
      $('cloud-colab-kpis').textContent.includes('2 次')
        && !$('cloud-colab-kpis').textContent.includes('日限'),
      'cloud-colab-kpis should show uncapped today job count'
    );

    const chartNodes14 = walk($('cloud-cu-chart'));
    const hits14 = chartNodes14.filter(n => n.className === 'cu-hit');
    assert(hits14.length === 14, '14d chart expected 14 hit targets, got ' + hits14.length);
    assert(chartNodes14.some(n => n.tagName === 'POLYLINE'), '14d chart missing polyline');
    for (const n of chartNodes14) {{
      for (const [k, v] of Object.entries(n.attrs)) {{
        assert(!String(v).includes('NaN'), 'SVG attr NaN: ' + k + '=' + v);
      }}
    }}
    // Hover a day column updates readout text
    hits14[0].dispatch('mouseenter');
    assert(
      $('cloud-trend-readout').textContent.includes('2026-11-17'),
      'hover readout date mismatch'
    );
    hits14[0].dispatch('mouseleave');
    assert(
      $('cloud-trend-readout').textContent.includes('近 14 天合计'),
      'leave readout reset mismatch'
    );

    // Switch to 30d window
    $('cloud-trend-30d').dispatch('click');
    assert(cloudTrendDays === 30, 'trend toggle did not switch to 30');
    assert($('cloud-trend-30d').getAttribute('aria-pressed') === 'true', '30d aria-pressed');
    assert($('colab-trend-30d').getAttribute('aria-pressed') === 'true', 'colab 30d sync');
    const hits30 = walk($('cloud-cu-chart')).filter(n => n.className === 'cu-hit');
    assert(hits30.length === 30, '30d chart expected 30 hit targets, got ' + hits30.length);

    // Warm session rendered with XSS stored safely as plain textContent (never HTML tag)
    const warmNodes = walk($('cloud-warm-sessions'));
    assert(!warmNodes.some(n => n.tagName === 'IMG'), 'XSS must never create IMG element');
    assert(
      $('cloud-warm-sessions').textContent.includes('热机 L4'),
      'warm session GPU pill missing'
    );
    assert(
      $('cloud-warm-sessions').textContent.includes('3 分 35 秒'),
      'warm TTL formatting missing'
    );

    // GPU breakdown & acceleration stats rendered
    assert($('cloud-gpu-breakdown').textContent.includes('L4'), 'GPU L4 missing');
    assert($('cloud-gpu-breakdown').textContent.includes('82.6%'), 'GPU share missing');
    assert($('cloud-accel-stats').textContent.includes('Drive 直传'), 'Drive direct missing');
    assert($('cloud-accel-stats').textContent.includes('85.7%'), 'Drive direct rate missing');

    // 2. GCP reconciliation rendering (both backend-supplied & legacy fallback)
    const gcpSynced = {{
      config: {{
        bigquery_enabled: true, enforce_credit_hard_stop: true, initial_credits_usd: 300
      }},
      bigquery: {{
        synced: true, initial_credits_usd: 300, credits_used_usd: 45.5,
        settled_remaining_usd: 254.5, unsettled_local_usd: 0.5, realtime_remaining_usd: 254.0,
        gross_cost_usd: 45.5, net_cost_usd: 0,
        latest_usage_end_ts: 1760000000, last_sync_at: 1760000600
      }},
      monitoring: {{ status: 'in_sync', last_sync_at: 1760000600, delta_tokens: 0 }},
      reconciliation: {{
        status: 'in_sync', billing_actual_usd: 45.5, billing_net_usd: 0,
        billing_plus_unsettled_usd: 46.0, local_reference_usd: 45.48,
        local_reference_basis: 'estimated', cost_delta_usd: 0.02,
        cost_delta_after_unsettled_usd: 0.52, cost_delta_ratio: 0.00044,
        credit_utilization_ratio: 45.5/300, monitoring_status: 'in_sync'
      }}
    }};
    renderUnifiedCloudGcp(gcpSynced, {{ mode: 'gcp', exhausted: false }}, null);
    assert($('cloud-gcp-pills').textContent.includes('账单对账已对齐'), 'in_sync pill missing');
    assert($('cloud-gcp-kpis').textContent.includes('+$0.0200'), 'signed delta missing');

    // Legacy GCP without reconciliation key -> falls back cleanly using dashboard usage
    const gcpLegacy = {{
      config: {{ bigquery_enabled: true, initial_credits_usd: 100 }},
      bigquery: {{
        synced: true, initial_credits_usd: 100, credits_used_usd: 10,
        settled_remaining_usd: 90, unsettled_local_usd: 5, realtime_remaining_usd: 85,
        gross_cost_usd: 10, net_cost_usd: 0
      }},
      monitoring: {{ status: 'not_synced' }}
    }};
    renderUnifiedCloudGcp(gcpLegacy, null, {{
      cost_estimates: {{
        lifetime: {{
          charged_microdollars: 15000000, estimated_microdollars: 15000000, complete: true
        }}
      }}
    }});
    assert(
      $('cloud-gcp-pills').textContent.includes('导出延迟正常'),
      'legacy export_lag fallback missing'
    );

    // 3. Legacy Colab payload (none of the #6 fields) & unavailable Colab (null)
    const legacyColab = {{
      configured: true,
      config: {{ enabled: true, preferred_gpu: 'L4', daily_job_limit: 5, daily_cu_budget: 10 }},
      worker: {{ reachable: true, compute_units: 12.0 }},
      today: {{ jobs_count: 1, cu_used: 0.4, daily_job_limit: 5, daily_cu_budget: 10 }},
      jobs: [{{ id: 'j1', status: 'succeeded', cu_used: 0.4 }}]
    }};
    renderUnifiedCloudColab(legacyColab, '');
    const legacyChartTxt = $('cloud-cu-chart').textContent;
    assert(
      legacyChartTxt.includes('暂未返回') || legacyChartTxt.includes('未返回'),
      'legacy trend empty state missing'
    );
    assert($('cloud-gpu-breakdown').textContent.includes('未返回'), 'legacy GPU fallback');
    assert($('cloud-accel-stats').textContent.includes('未上报'), 'legacy accel fallback');

    // Unavailable Colab (503) with empty & non-empty reason + zero-data 30d trend
    renderUnifiedCloudColab(null, '');
    assert($('cloud-colab-pills').textContent.includes('暂不可用'), 'unavail pill missing');
    assert(
      !$('cloud-colab-pills').textContent.includes('null'),
      'empty unavailReason must not render literal null text'
    );
    renderUnifiedCloudColab(null, 'Colab Worker 尚未配置');
    assert($('cloud-colab-pills').textContent.includes('Colab Worker 尚未配置'), 'reason pill');

    // renderColabOverview without warm sessions: no literal "null", uncapped KPIs, policy table
    renderColabOverview({{
      configured: true,
      source: 'default',
      config: {{ enabled: true, job_timeout_seconds: 1800, auto_terminate_orphan_minutes: 20 }},
      supported_job_types: ['rvc_cover', 'rvc_train'],
      job_types: fullColab.job_types,
      worker: {{ reachable: true, compute_units: 10.0, active_sessions: 0, warm_sessions: [] }},
      today: {{ jobs_count: 7, cu_used: 3.45 }},
      jobs: [
        {{
          id: 'job_dual_partial',
          job_type: 'rvc_cover',
          status: 'succeeded',
          preferred_gpu: 'L4',
          assigned_gpu: 'L4',
          cu_used: 0.42,
          created_at: 1763800000,
          finished_at: 1763800400,
          payload: {{ title: '枝垂桜', engine: 'dual', pitch: 0 }},
          result: {{
            engine_requested: 'dual',
            engine_used: 'seed_vc',
            candidates: [
              {{
                key: 'yingmusic_svc',
                engine: 'yingmusic_svc',
                status: 'failed',
                error_code: 'svc_oom'
              }},
              {{ key: 'seed_vc', engine: 'seed_vc', status: 'succeeded' }}
            ]
          }}
        }}
      ],
      warm_sessions: []
    }});
    assert(
      !$('colab-pills').textContent.includes('null'),
      'colab-pills without warm sessions must not render literal null text'
    );
    assert(
      $('colab-pills').textContent.includes('调度开启 · 按模块自选 GPU'),
      'colab-pills per-module GPU pill missing'
    );
    assert(
      $('colab-kpi-strip').textContent.includes('7 次')
        && $('colab-kpi-strip').textContent.includes('不设每日次数上限 · 排队上限 20')
        && $('colab-kpi-strip').textContent.includes('3.45 CU')
        && $('colab-kpi-strip').textContent.includes('不设每日 CU 上限 · 仅实时统计'),
      'colab-kpi-strip uncapped KPI cards mismatch'
    );
    assert(
      $('colab-state').textContent.includes('已注册作业：2 类')
        && $('colab-state').textContent.includes('1800 秒')
        && $('colab-state').textContent.includes('20 分钟'),
      'colab-state summary mismatch'
    );
    const policyTxt = $('colab-job-policies-table').textContent;
    assert(
      policyTxt.includes('翻唱学歌 (SVC/RVC)（rvc_cover）')
        && policyTxt.includes('L4 / T4 / A100 / H100')
        && policyTxt.includes('RVC 音色训练（rvc_train）')
        && policyTxt.includes('H100 / A100')
        && policyTxt.includes('3600 秒'),
      'colab-job-policies-table content mismatch: ' + policyTxt
    );
    const jobsTxt = $('colab-jobs-table').textContent;
    assert(
      jobsTxt.includes('枝垂桜')
        && jobsTxt.includes('双引擎亲挑')
        && jobsTxt.includes('1/2 候选成功 → Seed-VC')
        && jobsTxt.includes('YingMusic-SVC:svc_oom'),
      'colab-jobs-table dual candidate display mismatch: ' + jobsTxt
    );
    assert(
      !walk($('colab-job-policies-table')).some(n => n.tagName === 'IMG'),
      'XSS in job policy owner_module must never create IMG element'
    );
    assert($('colab-enabled').checked === true, 'colab-enabled not synced');
    assert($('colab-job-timeout').value === 1800, 'colab-job-timeout not synced');
    assert($('colab-orphan-minutes').value === 20, 'colab-orphan-minutes not synced');
    assert(
      colabErrorLabels.colab_gpu_not_allowed === '该作业类型不支持所选 GPU 型号',
      'colab_gpu_not_allowed label missing'
    );

    // renderGcpBilling & renderUnifiedBalance without optional pills must not render literal null
    renderGcpBilling(gcpLegacy);
    assert(!$('gcp-mon-pills').textContent.includes('null'), 'gcp-mon-pills literal null');
    renderUnifiedBalance({{ mode: 'none' }}, null, null);
    assert(
      !$('unified-balance-pills').textContent.includes('null'),
      'unified-balance-pills literal null'
    );

    const zeroTrend = trend30.map(d => ({{
      date: d.date, jobs: 0, succeeded: 0, failed: 0, cancelled: 0, cu_used: 0
    }}));
    renderCuTrendChart(
      'cloud-cu-chart', 'cloud-trend-readout', 'cloud-cu-chart-note', zeroTrend, 14
    );
    for (const n of walk($('cloud-cu-chart'))) {{
      for (const [k, v] of Object.entries(n.attrs)) {{
        assert(!String(v).includes('NaN'), 'Zero-trend SVG attr NaN: ' + k + '=' + v);
      }}
    }}

    // 4. Logout clears all cloud view state
    logout();
    assert(cloudTrendDays === 14, 'logout should reset trend days to 14');
    assert($('cloud-cu-chart').children.length === 0, 'logout should clear cloud-cu-chart');
    assert($('cloud-sync-bq').disabled === true, 'logout should disable cloud-sync-bq');
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"JS runtime test failed:\n{proc.stderr}\n{proc.stdout}"


def test_colab_and_songs_ui_physical_decoupling():
    """Verify #colab-compute and #songs are physically decoupled in HTML, JS, and runtime."""
    import json

    html = build_html()
    assert audit_html(html) == []

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)

    # Removed Colab song/Drive coupling IDs, legacy endpoints, and removed daily limit/GPU fields
    for removed_token in (
        "colab/drive/inbox",
        "colab-drive-details",
        "colab-drive-folder-id",
        "colab-inbox-table",
        "colab-refresh-inbox",
        "colab-preferred-gpu",
        "colab-fallback-gpus",
        "colab-daily-job-limit",
        "colab-daily-cu-budget",
        "colab-jobs-progress",
        "colab-jobs-progress-label",
        "colab-cu-progress",
        "colab-cu-progress-label",
    ):
        assert removed_token not in html, f"Residual deprecated token found: {removed_token!r}"

    # Pure Colab GPU & standalone Song Studio Drive DOM IDs
    for required_id in (
        "colab-job-types",
        "colab-job-policies-table",
        "colab-enabled",
        "colab-job-timeout",
        "colab-orphan-minutes",
        "song-storage-source",
        "song-drive-details",
        "song-storage-form",
        "song-drive-folder-id",
        "song-storage-save",
        "song-storage-reset",
        "song-refresh-inbox",
        "song-inbox-table",
        "song-inbox-file",
        "song-inbox-upload",
        "song-learn-heading",
        "song-learn-engine",
        "song-learn-pitch-hint",
        "song-learn-svc-fields",
        "song-learn-refresh-voices",
        "song-learn-ref-voice",
        "song-ref-voice-file",
        "song-ref-voice-upload",
        "song-ref-voice-hint",
        "song-learn-svc-adv",
        "song-learn-svc-steps",
        "song-learn-rvc-fields",
        "song-learn-refresh-rvc-models",
        "song-learn-model",
        "song-learn-rvc-adv",
        "song-learn-rvc-defaults-hint",
        "song-learn-rvc-volume-envelope",
        "song-learn-rvc-index-rate",
        "song-learn-rvc-protect",
        "song-learn-rvc-backing-gain",
        "song-learn-rvc-skip-dereverb",
        "song-learn-rvc-skip-backing",
        "song-learn-inbox",
        "song-learn-gpu",
    ):
        assert required_id in id_set, f"Missing expected decoupled DOM id: {required_id}"

    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(scripts) == 1
    script = scripts[0]
    assert "/admin/api/songs/inbox" in script
    assert "/admin/api/songs/ref-voices" in script
    assert "api('songs/inbox')" in script
    assert "api('songs/storage'" in script
    assert "api('songs/voices')" in script
    assert "/takes/'+" in script and "+'/audio'" in script
    assert "+'/activate'" in script
    assert "supported_job_types" in script
    assert "colab_gpu_not_allowed" in script
    assert "learn_gpu_policy" in script

    find_js_engine("No local JS engine (node/jsc) available for decoupled UI runtime test")

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")
    song_start = media_js.index("let songGeneration=0")
    song_end = media_js.index("async function songs()")
    song_helpers_js = media_js[song_start:song_end]
    all_ids_json = json.dumps(sorted(id_set))

    harness = f"""
    const ALL_IDS = {all_ids_json};
    const cfgSourceLabels = {{
      admin: 'SQLite 热配置', sqlite: 'SQLite 热配置', env: '.env 默认值', none: '未配置'
    }};
    class MockNode {{
      constructor(tag) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.children = [];
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.value = '';
        this.className = '';
        this.tabIndex = 0;
        this.options = [];
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get textContent() {{
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            this.children.push(n);
            if (n.tagName === 'OPTION') this.options.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this.options = []; this._text = ''; this.append(...nodes);
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) fn({{ target: this, preventDefault(){{}} }});
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest() {{ return null; }}
      focus() {{ this._focused = true; }}
      scrollIntoView() {{ this._scrolled = true; }}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};

    {core_js}
    {song_helpers_js}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    // 1. Song storage bar displays Drive & Colab independently (Drive ready while Colab offline)
    renderSongStorageBar({{
      drive_configured: true,
      colab_configured: false,
      drive_backed_count: 3,
      cache_bytes: 1048576,
      cache_max_bytes: 31457280,
      cached_count: 2
    }});
    assert($('song-storage-pills').textContent.includes('Drive 云盘已配置'), 'Drive ready pill');
    assert(
      $('song-storage-pills').textContent.includes('Colab GPU 算力未配置'),
      'Colab unconfigured pill'
    );
    assert(
      $('song-learn-colab-hint').textContent.includes('仍可正常播放歌单'),
      'independent hint'
    );

    // 2. Drive storage details + folder ID input populate safely (including XSS text escaping)
    const xss = '<img src=x onerror=alert(1)>';
    renderSongStorageDetails({{
      configured: true,
      source: 'sqlite',
      drive_songs_folder_id: 'folder_root_123',
      folders: {{
        root: 'folder_root_123',
        models: 'folder_models_456',
        songs: 'folder_songs_789',
        inbox: 'folder_inbox_' + xss
      }},
      error: ''
    }});
    assert($('song-drive-folder-id').value === 'folder_root_123', 'folder id populated');
    assert(
      $('song-drive-details').textContent.includes('folder_models_456'),
      'models folder shown'
    );
    assert(!walk($('song-drive-details')).some(n => n.tagName === 'IMG'), 'no XSS IMG in details');
    assert($('song-storage-save').disabled === false, 'save enabled');
    assert($('song-storage-reset').disabled === false, 'reset enabled');

    // 3. Inbox table renders safely and 「填入学歌」 prefills & scrolls within #songs
    const inboxItems = [
      {{
        id: 'file_1', name: '晴天 - 周杰伦.mp3', size: 4194304,
        modified_time: '2026-11-22T09:00:00Z'
      }},
      {{ id: 'file_xss', name: xss + '.wav', size: 1024, modified_time: '' }}
    ];
    renderSongInboxSelect(inboxItems, '');
    renderSongInboxTable(inboxItems, '');
    assert(
      !walk($('song-inbox-table')).some(n => n.tagName === 'IMG'),
      'no XSS IMG in inbox table'
    );
    const prefillBtns = walk($('song-inbox-table')).filter(
      n => n.tagName === 'BUTTON' && n.textContent === '填入学歌'
    );
    assert(prefillBtns.length === 2, 'expected 2 prefill buttons');
    prefillBtns[0].dispatch('click');
    assert($('song-learn-title').value.includes('晴天'), 'prefill title failed');
    assert($('song-learn-inbox').value === '晴天 - 周杰伦.mp3', 'prefill select failed');
    assert($('song-learn-auto-search').checked === false, 'prefill unchecks auto search');
    assert($('song-learn-heading')._focused === true, 'prefill focuses learn heading');
    assert($('song-learn-heading')._scrolled === true, 'prefill scrolls learn heading');

    // 4. Song learn GPU policy select rendering, result GPU badge, error map & reset
    assert(
      songErrorLabels.colab_gpu_not_allowed
        === '翻唱作业（SVC/RVC）暂不支持该 GPU 型号（已排除不兼容卡型）',
      'song colab_gpu_not_allowed label missing'
    );
    for (const code of [
      'invalid_song_engine',
      'invalid_song_take',
      'invalid_ref_voice',
      'song_take_not_found',
      'ref_voice_not_found',
      'song_take_unavailable',
      'song_ref_voice_too_large',
      'svc_all_engines_failed'
    ]) {{
      assert(Boolean(songErrorLabels[code]), 'missing songErrorLabels.' + code);
    }}
    renderSongLearnGpuPolicy(null);
    const defOpts = $('song-learn-gpu').options;
    assert(defOpts.length === 5, 'default GPU policy should render 5 options');
    assert(
      defOpts[0].value === '' && defOpts[0].textContent === '默认 L4（自动回退 T4）',
      'default opt'
    );
    assert(
      defOpts.slice(1).map(o => o.value).join(',') === 'L4,T4,A100,H100',
      'default allowed GPUs mismatch'
    );
    $('song-learn-gpu').value = 'A100';
    renderSongLearnGpuPolicy({{
      default_gpu: 'L4',
      fallback_gpus: ['T4'],
      allowed_gpus: ['L4', 'T4', 'A100', 'H100']
    }});
    assert($('song-learn-gpu').value === 'A100', 'valid selected GPU should be preserved');
    renderSongLearnGpuPolicy({{
      default_gpu: 'L4',
      fallback_gpus: [],
      allowed_gpus: ['L4', 'T4', xss]
    }});
    assert($('song-learn-gpu').value === '', 'disallowed previous GPU should reset to default');
    assert(
      $('song-learn-gpu').options[0].textContent === '默认 L4（不自动回退）',
      'empty fallback label mismatch'
    );
    assert(
      !walk($('song-learn-gpu')).some(n => n.tagName === 'IMG'),
      'XSS in allowed_gpus must never create IMG element'
    );
    showSongLearnResult(
      {{ id: 'job_abc', status: 'queued', preferred_gpu: 'A100', engine: 'dual' }},
      '晴天',
      '晴天 - 周杰伦.mp3',
      0
    );
    assert(
      $('song-learn-result').textContent.includes('首选 GPU：A100')
        && $('song-learn-result').textContent.includes('双引擎亲挑'),
      'song-learn-result must display preferred_gpu and engine'
    );
    $('song-learn-gpu').value = 'T4';
    $('song-learn-engine').value = 'rvc';
    syncSongLearnEngineFields();
    assert($('song-learn-svc-fields').hidden === true, 'rvc mode must hide SVC fields');
    assert($('song-learn-rvc-fields').hidden === false, 'rvc mode must show RVC fields');
    clearSongLearnForm();
    assert($('song-learn-gpu').value === '', 'clearSongLearnForm must reset song-learn-gpu');
    assert($('song-learn-engine').value === 'dual', 'clearSongLearnForm must reset engine to dual');
    assert($('song-learn-svc-fields').hidden === false, 'dual mode must show SVC fields');
    assert($('song-learn-rvc-fields').hidden === true, 'dual mode must hide RVC fields');
    assert(
      $('song-learn-rvc-volume-envelope').value === '0.25'
        && $('song-learn-rvc-skip-dereverb').checked === false
        && $('song-learn-rvc-index-rate').value === '0.15'
        && $('song-learn-rvc-protect').value === '0.42'
        && $('song-learn-rvc-backing-gain').value === '-14'
        && $('song-learn-rvc-skip-backing').checked === true,
      'rolled-back RVC default values mismatch'
    );

    // 5. Ref voices & RVC models rendering (with XSS guard)
    renderSongVoices({{
      default_ref_voice: 'hakimi_ref_22s.wav',
      ref_voices: [
        {{ id: 'ref_1', name: 'hakimi_ref_22s.wav', size: 1196032, default: true }},
        {{ id: 'ref_xss', name: xss + '.wav', size: 2048, default: false }}
      ],
      rvc_models: [
        {{ name: 'hakimi', has_index: true }},
        {{ name: 'alt_' + xss, has_index: false }}
      ]
    }});
    assert(
      $('song-learn-ref-voice').options.length === 3
        && $('song-learn-ref-voice').options[0].textContent.includes('hakimi_ref_22s.wav'),
      'ref voice select mismatch'
    );
    assert(
      $('song-learn-model').options.length === 2
        && $('song-learn-model').options[1].textContent.includes('缺 .index'),
      'rvc model select mismatch'
    );
    assert(
      !walk($('song-learn-ref-voice')).some(n => n.tagName === 'IMG')
        && !walk($('song-learn-model')).some(n => n.tagName === 'IMG'),
      'XSS in voices/models must never create IMG element'
    );

    // 6. Dual-engine song card rendering: engine badge, review_note, takes list, legacy fallback
    renderSongList([
      {{
        id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        title: '枝垂桜',
        artist: 'Anna',
        tags: '日语 治愈',
        duration_ms: 50000,
        byte_size: 801271,
        drive_backed: true,
        drive_full_backed: true,
        cached_local: true,
        source_origin: 'colab',
        active_engine: 'yingmusic_svc',
        active_take: 'yingmusic_svc',
        audition_status: 'picked',
        audition_selected_engine: 'yingmusic_svc',
        review_note: '这版气息张力很足，听感更真唱喵~ ' + xss,
        takes: [
          {{
            key: 'yingmusic_svc',
            engine: 'yingmusic_svc',
            status: 'succeeded',
            active: true,
            playable: true,
            duration_ms: 50000,
            byte_size: 801271,
            drive_full_backed: true,
            acoustic_metrics: {{
              chorus_mean_db: -11.5,
              chorus_dynamic_range_db: 6.4,
              clipped_peak_flag: false
            }}
          }},
          {{
            key: 'seed_vc',
            engine: 'seed_vc',
            status: 'succeeded',
            active: false,
            playable: true,
            duration_ms: 50000,
            byte_size: 790000,
            drive_full_backed: true,
            acoustic_metrics: {{
              chorus_mean_db: -12.0,
              chorus_dynamic_range_db: 5.1,
              clipped_peak_flag: false
            }}
          }}
        ]
      }},
      {{
        id: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        title: '旧歌兼容',
        artist: '',
        tags: '',
        duration_ms: 42000,
        byte_size: 650000,
        drive_backed: false,
        source_origin: 'local'
      }}
    ], songGeneration);
    const listText = $('song-list').textContent;
    assert(
      listText.includes('YingMusic-SVC')
        && listText.includes('猫猫亲挑')
        && listText.includes('这版气息张力很足')
        && listText.includes('保留候选版本（2）')
        && listText.includes('Seed-VC')
        && listText.includes('高潮 -11.5 dB')
        && listText.includes('动态差 6.4 dB')
        && listText.includes('设为当前')
        && listText.includes('旧歌兼容'),
      'dual-engine song card render mismatch: ' + listText
    );
    assert(
      !walk($('song-list')).some(n => n.tagName === 'IMG'),
      'XSS in review_note or song metadata must never create IMG element'
    );
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Song decoupled JS test failed:\n{proc.stderr}\n{proc.stdout}"


def test_colab_config_and_song_learn_form_submit_payloads():
    """Verify #colab-config-form and #song-learn-form submit payloads via actual JS listeners."""
    import json

    find_js_engine("No local JS engine (node/jsc) available for form submit payload test")

    html = build_html()
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    shell_js = (ADMIN_DIR / "src" / "js" / "08-modules-shell.js").read_text(encoding="utf-8")
    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")

    colab_cfg_start = shell_js.index("$('colab-config-form').addEventListener('submit'")
    colab_cfg_end = shell_js.index("$('colab-reset').addEventListener('click'")
    colab_cfg_submit_js = shell_js[colab_cfg_start:colab_cfg_end]

    song_start = media_js.index("let songGeneration=0")
    song_end = media_js.index("async function songs()")
    song_helpers_js = media_js[song_start:song_end]
    song_submit_start = media_js.index("$('song-learn-form').addEventListener('submit'")
    song_submit_end = media_js.index("$('song-file').addEventListener('change'")
    song_submit_js = media_js[song_submit_start:song_submit_end]

    harness = f"""
    const ALL_IDS = {all_ids_json};
    const cfgSourceLabels = {{}};
    class MockNode {{
      constructor(tag) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.children = [];
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.value = '';
        this.className = '';
        this.options = [];
        this.selectedOptions = [];
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get textContent() {{
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            this.children.push(n);
            if (n.tagName === 'OPTION') this.options.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this.options = []; this._text = ''; this.append(...nodes);
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) fn({{ target: this, preventDefault(){{}} }});
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest() {{ return null; }}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};

    {core_js}

    const calls = [];
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};
    api = async (path, method='GET', data) => {{
      calls.push({{ path, method, data }});
      if (path === 'colab' && method === 'PUT') return {{ config: data }};
      if (path === 'songs/learn' && method === 'POST') {{
        return {{ id: 'job_99', status: 'queued', preferred_gpu: data.gpu || 'L4' }};
      }}
      return {{}};
    }};
    function renderColabOverview(){{}}
    function throwColabError(err){{ throw err; }}
    async function songs(){{}}

    {song_helpers_js}
    {colab_cfg_submit_js}
    {song_submit_js}

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. #colab-config-form submits only enabled, job_timeout_seconds,
      //    auto_terminate_orphan_minutes.
      $('colab-enabled').checked = true;
      $('colab-job-timeout').value = '2400';
      $('colab-orphan-minutes').value = '30';
      $('colab-config-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 1, 'expected 1 colab PUT call');
      const colabKeys = Object.keys(calls[0].data).sort().join(',');
      assert(
        colabKeys === 'auto_terminate_orphan_minutes,enabled,job_timeout_seconds',
        'unexpected colab PUT payload keys: ' + colabKeys
      );
      assert(calls[0].data.job_timeout_seconds === 2400, 'timeout mismatch');
      assert(calls[0].data.auto_terminate_orphan_minutes === 30, 'orphan mins mismatch');

      // 2. #song-learn-form without explicit GPU omits gpu field and defaults to engine=dual
      $('song-learn-title').value = '晴天';
      $('song-learn-pitch').value = '0';
      $('song-learn-engine').value = '';
      $('song-learn-inbox').value = '';
      $('song-learn-gpu').value = '';
      $('song-learn-auto-search').checked = true;
      $('song-learn-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 2, 'expected first song learn POST');
      assert(!('gpu' in calls[1].data), 'default GPU selection must omit gpu field');
      assert(calls[1].data.engine === 'dual', 'default engine must be dual');
      assert(!('volume_envelope' in calls[1].data), 'SVC mode must omit RVC params');
      assert(
        $('song-learn-result').textContent.includes('首选 GPU：L4')
          && $('song-learn-result').textContent.includes('双引擎亲挑'),
        'default submit result must show preferred_gpu L4 and engine'
      );

      // 3. #song-learn-form with explicit GPU + seed_vc + ref_voice + svc_steps
      $('song-learn-title').value = '夜曲';
      $('song-learn-pitch').value = '2';
      $('song-learn-engine').value = 'seed_vc';
      $('song-learn-ref-voice').value = 'hakimi_ref_22s.wav';
      $('song-learn-ref-voice').selectedOptions = [{{ dataset: {{ fileId: 'ref_id_1' }} }}];
      $('song-learn-svc-steps').value = '40';
      $('song-learn-inbox').value = '';
      $('song-learn-gpu').value = 'A100';
      $('song-learn-auto-search').checked = true;
      $('song-learn-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 3, 'expected second song learn POST');
      assert(calls[2].data.gpu === 'A100', 'explicit GPU selection must send gpu=A100');
      assert(calls[2].data.engine === 'seed_vc', 'engine must be seed_vc');
      assert(calls[2].data.ref_voice === 'hakimi_ref_22s.wav', 'ref_voice mismatch');
      assert(calls[2].data.ref_audio_file_id === 'ref_id_1', 'ref_audio_file_id mismatch');
      assert(calls[2].data.svc_steps === 40, 'svc_steps mismatch');
      assert(!('model_name' in calls[2].data), 'seed_vc must omit RVC model_name');
      assert(
        $('song-learn-result').textContent.includes('首选 GPU：A100'),
        'override submit result must show preferred_gpu A100'
      );
      assert($('song-learn-gpu').value === '', 'song-learn-gpu must reset after submit');
      assert($('song-learn-engine').value === 'dual', 'engine must reset to dual after submit');

      // 4. #song-learn-form in rvc mode sends rolled-back RVC defaults and omits SVC fields
      $('song-learn-title').value = '老歌测试';
      $('song-learn-pitch').value = '-2';
      $('song-learn-engine').value = 'rvc';
      $('song-learn-model').value = 'hakimi';
      $('song-learn-rvc-volume-envelope').value = '0.25';
      $('song-learn-rvc-index-rate').value = '0.15';
      $('song-learn-rvc-protect').value = '0.42';
      $('song-learn-rvc-backing-gain').value = '-14';
      $('song-learn-rvc-skip-dereverb').checked = false;
      $('song-learn-rvc-skip-backing').checked = true;
      $('song-learn-inbox').value = '';
      $('song-learn-gpu').value = '';
      $('song-learn-auto-search').checked = true;
      $('song-learn-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 4, 'expected third song learn POST');
      assert(calls[3].data.engine === 'rvc', 'engine must be rvc');
      assert(calls[3].data.model_name === 'hakimi', 'model_name mismatch');
      assert(calls[3].data.volume_envelope === 0.25, 'volume_envelope mismatch');
      assert(calls[3].data.skip_dereverb === false, 'skip_dereverb mismatch');
      assert(calls[3].data.skip_backing === true, 'skip_backing mismatch');
      assert(calls[3].data.backing_gain_db === -14, 'backing_gain_db mismatch');
      assert(!('ref_voice' in calls[3].data) && !('svc_steps' in calls[3].data), 'rvc omits SVC');

      // 5. Dual-engine song take preview & activate buttons use path-only URLs (zero query params)
      token = 'test-token';
      const fetchPaths = [];
      const origCreateObjectURL = globalThis.URL?.createObjectURL;
      globalThis.URL = {{
        createObjectURL() {{ return 'blob:take-seed-vc'; }},
        revokeObjectURL() {{}}
      }};
      songRequest = async (path) => {{
        fetchPaths.push('/admin/api/songs' + path);
        return {{ blob: async () => ({{}}) }};
      }};
      function walkAll(node, acc=[]) {{
        acc.push(node);
        for (const c of (node.children || [])) walkAll(c, acc);
        return acc;
      }}
      renderSongList([
        {{
          id: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          title: '枝垂桜',
          duration_ms: 50000,
          byte_size: 800000,
          drive_backed: true,
          active_engine: 'yingmusic_svc',
          active_take: 'yingmusic_svc',
          takes: [
            {{
              key: 'yingmusic_svc',
              engine: 'yingmusic_svc',
              status: 'succeeded',
              active: true,
              playable: true
            }},
            {{
              key: 'seed_vc',
              engine: 'seed_vc',
              status: 'succeeded',
              active: false,
              playable: true
            }}
          ]
        }}
      ], songGeneration);
      const btns = walkAll($('song-list')).filter(n => n.tagName === 'BUTTON');
      const previewAltBtn = btns.find(b => b.textContent === '试听此版');
      const activateAltBtn = btns.find(b => b.textContent === '设为当前');
      assert(Boolean(previewAltBtn) && Boolean(activateAltBtn), 'take action buttons missing');
      previewAltBtn.dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      const expectedTakeUrl =
        '/admin/api/songs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/takes/seed_vc/audio';
      assert(
        fetchPaths.length === 1
          && fetchPaths[0] === expectedTakeUrl
          && !fetchPaths[0].includes('?'),
        'take preview URL mismatch: ' + JSON.stringify(fetchPaths)
      );
      assert(
        $('song-list').textContent.includes('正在试听备用版：Seed-VC'),
        'playing hint should show non-active take preview'
      );
      activateAltBtn.dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        calls.length === 5
          && calls[4].method === 'POST'
          && calls[4].path === 'songs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/takes/seed_vc/activate'
          && !calls[4].path.includes('?'),
        'take activate API mismatch: ' + JSON.stringify(calls[4])
      );
      if (origCreateObjectURL) globalThis.URL.createObjectURL = origCreateObjectURL;
      if (typeof print === 'function') print('FORM_AND_TAKES_OK');
      else console.log('FORM_AND_TAKES_OK');
    }})().catch(err => {{
      if (typeof process !== 'undefined' && process.exit) {{
        console.error(err);
        process.exit(1);
      }}
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Form submit JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "FORM_AND_TAKES_OK" in proc.stdout, (
        f"Async JS test did not reach completion sentinel:\n{proc.stderr}\n{proc.stdout}"
    )


def test_overview_ledger_non_blocking_colab_and_panel_fold():
    """Verify non-blocking Colab fetch, one-shot refresh retry, and .panel-fold structure."""
    import json

    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)
    assert len(collector.ids) == len(id_set)

    for required_id in (
        "overview-colab-sync-note",
        "overview-cloud-details",
        "cloud-colab-sync-note",
        "cloud-fold-breakdown",
        "ledger-fold-breakdown",
        "ledger-fold-work-cache",
        "ledger-fold-bq-config",
        "ledger-fold-gcp-sa",
        "ledger-fold-mon-detail",
        "community-fold-tiers",
        "community-fold-history",
        "colab-sync-note",
        "colab-fold-breakdown",
        "colab-fold-policies",
    ):
        assert required_id in id_set, f"Missing panel-fold / sync DOM id: {required_id}"

    # Verify ledger heavy tables/forms sit inside .panel-fold while core summary stays open.
    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    ledger_html = panels["ledger"]
    assert "panel-fold" in ledger_html
    assert 'id="token-ledger"' in ledger_html.split('id="ledger-fold-breakdown"')[0]
    breakdown_block = ledger_html.split('id="ledger-fold-breakdown"')[1].split(
        'id="ledger-fold-work-cache"'
    )[0]
    for elem_id in ("provider-ledger", "model-ledger", "thinking-ledger"):
        assert f'id="{elem_id}"' in breakdown_block

    work_cache_block = ledger_html.split('id="ledger-fold-work-cache"')[1].split(
        'data-section-group="billing"'
    )[0]
    for elem_id in ("work-ledger", "cost-ledger", "effective-cache", "cache-alert"):
        assert f'id="{elem_id}"' in work_cache_block

    bq_fold_block = ledger_html.split('id="ledger-fold-bq-config"')[1].split(
        'id="gcp-sync-monitoring"'
    )[0]
    for elem_id in ("gcp-credits-table", "gcp-bq-sql", "gcp-bq-form", "gcp-sa-form"):
        assert f'id="{elem_id}"' in bq_fold_block

    mon_fold_block = ledger_html.split('id="ledger-fold-mon-detail"')[1].split(
        'data-section-group="budget"'
    )[0]
    for elem_id in ("gcp-mon-compare-table", "gcp-mon-models-table", "gcp-mon-form"):
        assert f'id="{elem_id}"' in mon_fold_block

    # Zero query parameters in API calls.
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(scripts) == 1
    script = scripts[0]
    assert not re.search(r"api\(\s*['\"][^'\"\n]*\?", script)
    assert "api('colab/refresh','POST')" in script

    find_js_engine("No local JS engine (node/jsc) available for non-blocking Colab runtime test")

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    ledger_js = (ADMIN_DIR / "src" / "js" / "02-overview-ledger.js").read_text(encoding="utf-8")
    all_ids_json = json.dumps(sorted(id_set))

    harness = f"""
    const ALL_IDS = {all_ids_json};
    class MockNode {{
      constructor(tag, ns) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.namespaceURI = ns || null;
        this.children = [];
        this.parentElement = null;
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.value = '';
        this.className = '';
        this.open = false;
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get textContent() {{
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            n.parentElement = this;
            this.children.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this._text = ''; this.append(...nodes);
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) fn({{ target: this, preventDefault(){{}} }});
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest() {{ return null; }}
      focus() {{}}
      select() {{}}
      remove() {{}}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    // Wire gcp-bq-form inside a closed <details> ancestor to verify dirty auto-expand.
    const bqFold = byId.get('ledger-fold-bq-config');
    bqFold.tagName = 'DETAILS';
    bqFold.open = false;
    byId.get('gcp-bq-form').parentElement = bqFold;

    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createElementNS(ns, tag) {{ return new MockNode(tag, ns); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    const scheduledTimers = [];
    let nextTimerId = 1;
    function setTimeout(fn, ms) {{
      const id = nextTimerId++;
      scheduledTimers.push({{ id, fn, ms, cleared: false }});
      return id;
    }}
    function clearTimeout(id) {{
      const item = scheduledTimers.find(t => t.id === id);
      if (item) item.cleared = true;
    }}
    function clearInterval() {{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function previewWakeWindow(){{}}
    function lifeFmt(v){{ return String(v||''); }}
    function showNotebookAvailability(){{}}
    function stampDashboard(){{}}
    function providerBadge(p){{ return String(p||''); }}

    {core_js}
    token = 'test-token';
    confirmDialog = async () => true;
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};

    const apiCalls = [];
    let colabResolvers = [];
    const sampleOverview = {{
      primary_model: 'hydrogen',
      uptime_seconds: 3720,
      minute_requests: 2,
      minute_limit: 30,
      daily_limit_usd: 5,
      metrics: [],
      pipeline: {{ gate: 0, generating: 0, image_cache_bytes: 0 }},
      usage: {{
        timezone: 'Asia/Shanghai',
        day_local: '2026-11-26',
        lanes: [{{ lane: 'chat', amount: 120000, calls: 4 }}],
        categories: [{{ category: 'chat', amount: 120000, calls: 4 }}],
        token_usage: {{
          today: {{ calls: 4, known_usage_calls: 4, input_tokens: 1000, cached_tokens: 500,
                    output_tokens: 200, thought_tokens: 50, cache_known_calls: 4,
                    interaction_calls: 4, cache_hit_ratio: 0.5, by_provider: [], by_model: [],
                    by_thinking: [], effective_cache_calls: 4, effective_cache_hit_ratio: 0.9 }},
          month: {{ calls: 4, known_usage_calls: 4, input_tokens: 1000, cached_tokens: 500,
                    output_tokens: 200, thought_tokens: 50, cache_known_calls: 4,
                    interaction_calls: 4, cache_hit_ratio: 0.5, by_provider: [], by_model: [],
                    by_thinking: [], effective_cache_calls: 4, effective_cache_hit_ratio: 0.9 }},
          lifetime: {{ calls: 4, known_usage_calls: 4, input_tokens: 1000, cached_tokens: 500,
                       output_tokens: 200, thought_tokens: 50, cache_known_calls: 4,
                       interaction_calls: 4, cache_hit_ratio: 0.5, by_provider: [], by_model: [],
                       by_thinking: [], effective_cache_calls: 4, effective_cache_hit_ratio: 0.9 }}
        }},
        cost_estimates: {{}},
        cache_health: {{ prefix_busts_today: 0 }},
        recent_work: [],
        history: {{ since_utc: '2026-11-01' }}
      }},
      budget_status: {{
        daily: 120000, daily_limit: 5000000, monthly: 120000, monthly_limit: 50000000,
        total: 120000, sub_budgets: {{}}
      }},
      rest_hours: null,
      notebook: {{ enabled: false }}
    }};

    api = (path, method='GET', data) => {{
      apiCalls.push({{ path, method, data }});
      if (path === 'colab' || path === 'colab/refresh') {{
        return new Promise(resolve => colabResolvers.push({{ path, method, resolve }}));
      }}
      if (path === 'global/overview' || path === 'global/dashboard') {{
        return Promise.resolve(sampleOverview);
      }}
      if (path === 'budget') {{
        return Promise.resolve({{
          source: 'env',
          config: {{
            daily_usd: 5, monthly_usd: 50, user_daily_usd: 1, patrol_daily_usd: 1,
            memory_daily_usd: 1, proactive_daily_usd: 1, admin_daily_usd: 1
          }}
        }});
      }}
      if (path === 'global/providers') {{
        return Promise.resolve({{ config: {{ vertex_enabled: true }}, credentials: {{}} }});
      }}
      if (path === 'gcp-billing') {{
        return Promise.resolve({{
          source: 'env',
          config: {{
            bigquery_enabled: false, enforce_credit_hard_stop: false, initial_credits_usd: 0,
            bigquery_sync_interval_seconds: 1800, monitoring_enabled: false,
            monitoring_sync_interval_seconds: 300, credit_grants: []
          }},
          credentials: {{}},
          bigquery: {{ synced: false }},
          monitoring: {{ status: 'not_synced' }}
        }});
      }}
      if (path === 'account-balance') {{
        return Promise.resolve({{ mode: 'none' }});
      }}
      return Promise.resolve({{}});
    }};

    {ledger_js}

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}
    async function flushMicrotasks() {{
      for (let i = 0; i < 6; i++) await Promise.resolve();
    }}

    (async () => {{
      // 1. dashboard() renders immediately WITHOUT waiting for api('colab')
      await dashboard();
      assert(colabResolvers.length === 1 && colabResolvers[0].path === 'colab',
        'colab should be in-flight');
      assert($('stat-calls').textContent === '4',
        'overview must render immediately before colab resolves');

      // Resolve colab with worker_refreshing=true -> shows hint & schedules ONE 3000ms timer
      colabResolvers.shift().resolve({{
        configured: true,
        worker_cached: true,
        worker_refreshing: true,
        config: {{ enabled: true }},
        worker: {{ reachable: true, compute_units: null, refreshing: true }},
        today: {{ jobs_count: 1, cu_used: 0.2 }}
      }});
      await flushMicrotasks();
      assert($('overview-colab-sync-note').hidden === false,
        'overview sync note must be visible when refreshing');
      assert($('overview-colab-sync-note').textContent.includes('正在后台同步最新 CU 余额'),
        'overview sync note text mismatch');
      const retryTimers1 = scheduledTimers.filter(t => t.ms === 3000 && !t.cleared);
      assert(retryTimers1.length === 1, 'must schedule exactly one 3000ms follow-up timer');

      // Fire the one-shot follow-up; even if still refreshing, must NOT schedule a loop timer
      retryTimers1[0].fn();
      assert(colabResolvers.length === 1 && colabResolvers[0].path === 'colab',
        'follow-up should call api(colab)');
      colabResolvers.shift().resolve({{
        configured: true,
        worker_cached: true,
        worker_refreshing: true,
        config: {{ enabled: true }},
        worker: {{ reachable: true, compute_units: 18.5, refreshing: true }},
        today: {{ jobs_count: 1, cu_used: 0.2 }}
      }});
      await flushMicrotasks();
      assert(
        scheduledTimers.filter(t => t.ms === 3000).length === 1,
        'follow-up must NEVER loop-schedule another 3000ms timer'
      );

      // 2. ledger() renders immediately WITHOUT waiting for api('colab')
      cachedColabOverview = null;
      await ledger();
      assert(colabResolvers.length === 1 && colabResolvers[0].path === 'colab',
        'ledger colab should be in-flight');
      assert($('token-ledger').textContent.includes('1000'),
        'ledger must render main tables before colab resolves');
      assert(!$('budget-periods').textContent.includes('累计总上限'),
        'budget-periods must not render removed cumulative total cap row');
      assert($('cloud-colab-pills').textContent.includes('正在同步'),
        'cloud colab card should show non-blocking syncing pill');

      // Resolve ledger colab with fresh cached balance
      colabResolvers.shift().resolve({{
        configured: true,
        worker_cached: true,
        worker_cache_age_seconds: 12,
        worker_refreshing: false,
        config: {{ enabled: true }},
        worker: {{ reachable: true, compute_units: 35.5 }},
        today: {{ jobs_count: 2, cu_used: 0.5 }},
        warm_sessions: []
      }});
      await flushMicrotasks();
      assert($('cloud-colab-kpis').textContent.includes('35.50 CU'),
        'ledger colab card must update locally after async resolve');
      assert($('cloud-colab-sync-note').textContent.includes('12 秒前'),
        'cache age hint mismatch');

      // 3. Manual cloud-refresh triggers POST colab/refresh (zero query params)
      $('cloud-refresh').dispatch('click');
      await flushMicrotasks();
      assert(
        colabResolvers.length === 1
          && colabResolvers[0].path === 'colab/refresh'
          && colabResolvers[0].method === 'POST',
        'cloud-refresh must POST colab/refresh'
      );
      colabResolvers.shift().resolve({{
        configured: true,
        worker_cached: false,
        worker_cache_age_seconds: 0,
        worker_refreshing: false,
        config: {{ enabled: true }},
        worker: {{ reachable: true, compute_units: 34.25 }},
        today: {{ jobs_count: 2, cu_used: 0.5 }},
        warm_sessions: []
      }});
      await Promise.all(pendingTasks.splice(0));
      assert($('cloud-colab-kpis').textContent.includes('34.25 CU'),
        'manual refresh must render updated live balance');

      // 4. Dirty form inside collapsed <details> automatically opens enclosing <details>
      assert(bqFold.open === false, 'bqFold should start collapsed');
      markDirty('gcp-bq-form');
      assert(bqFold.open === true, 'marking dirty form must auto-open enclosing <details>');

      if (typeof print === 'function') print('NON_BLOCKING_COLAB_FOLD_OK');
      else console.log('NON_BLOCKING_COLAB_FOLD_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Non-blocking Colab JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "NON_BLOCKING_COLAB_FOLD_OK" in proc.stdout, (
        f"Async JS test did not reach completion sentinel:\n{proc.stderr}\n{proc.stdout}"
    )


def test_song_finder_ui_rendering_and_fetch_prefill_runtime():
    """Verify renderSongFinderResults() builds valid DOM nodes and fetchOne() prefills cleanly."""
    import json

    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")
    for stale_fn in ("refreshSongInbox", "guessTitleFromFilename", "setSongLearnInboxValue"):
        assert stale_fn not in media_js, f"Undefined helper {stale_fn!r} still in 07-media-bili.js"

    find_js_engine("No local JS engine (node/jsc) available for Song Finder JS runtime test")

    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))
    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    song_start = media_js.index("let songGeneration=0")
    song_end = media_js.index("async function songs()")
    song_helpers_js = media_js[song_start:song_end]

    harness = f"""
    const ALL_IDS = {all_ids_json};
    const cfgSourceLabels = {{}};
    class MockNode {{
      constructor(tag) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.children = [];
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.value = '';
        this.className = '';
        this.options = [];
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get textContent() {{
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            this.children.push(n);
            if (n.tagName === 'OPTION') this.options.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this.options = []; this._text = ''; this.append(...nodes);
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) fn({{ target: this, preventDefault(){{}} }});
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest() {{ return null; }}
      focus() {{ this._focused = true; }}
      scrollIntoView() {{ this._scrolled = true; }}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};

    {core_js}
    token = 'test-token';
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};
    const apiCalls = [];
    let inboxItems = [];
    api = async (path, method='GET', data) => {{
      apiCalls.push({{ path, method, data }});
      if (path === 'songs/fetch-to-inbox' && method === 'POST') {{
        const saved = {{
          id: 'drive_file_88',
          file_id: 'drive_file_88',
          name: (data.title || 'fallback_stem') + ' - ' + (data.artist || 'Artist') + '.mp3',
          size: 3145728,
          song_finder: {{ online: true, node_region: 'CN', provider_order: ['bilibili_public'] }}
        }};
        inboxItems = [{{ id: saved.file_id, name: saved.name, size: saved.size }}];
        return saved;
      }}
      if (path === 'songs/inbox' && method === 'GET') {{
        return {{ items: inboxItems }};
      }}
      return {{}};
    }};

    {song_helpers_js}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. el() array child support stays XSS-safe
      const xss = '<img src=x onerror=alert(1)>';
      const arrNode = el('div', [el('span', 'safe', 'pill'), xss, null, false], 'wrap');
      assert(arrNode.className === 'wrap', 'el array cls mismatch');
      assert(arrNode.children.length === 2, 'el array should skip null/false');
      assert(!walk(arrNode).some(n => n.tagName === 'IMG'), 'el array must escape strings');
      assert(arrNode.textContent.includes(xss), 'el array text node missing');

      // 2. renderSongFinderResults produces valid pills/buttons with zero [object ...]
      renderSongFinderResults({{
        provider_used: 'bilibili_public',
        song_finder: {{
          online: true,
          node_region: 'CN',
          provider_order: ['bilibili_public', 'youtube'],
          providers: {{ bilibili_public: {{}}, youtube: {{}} }}
        }},
        candidates: [
          {{
            candidate_id: 'bili:BV1xx',
            provider: 'bilibili_public',
            title: '晴天 Official ' + xss,
            uploader: '周杰伦',
            duration_sec: 269,
            score: 9.6
          }},
          {{
            candidate_id: 'yt:abc',
            provider: 'youtube',
            title: '夜曲 MV',
            uploader: 'Jay',
            duration_sec: 226,
            score: 8.8
          }}
        ]
      }}, '晴天', '周杰伦');

      const box = $('song-finder-results');
      assert(!box.textContent.includes('[object '), 'must not stringify DOM nodes');
      assert(!walk(box).some(n => n.tagName === 'IMG'), 'XSS must never create IMG');
      const allNodes = walk(box);
      const buttons = allNodes.filter(n => n.tagName === 'BUTTON');
      const pills = allNodes.filter(
        n => n.tagName === 'SPAN' && n.className && n.className.includes('pill-')
      );
      assert(buttons.length === 4, 'expected 4 action buttons across 2 candidates');
      assert(pills.length === 6, 'expected 6 pills across 2 candidates, got ' + pills.length);
      assert(box.textContent.includes('B 站公开源'), 'provider pill missing');
      assert(box.textContent.includes('时长 4:29'), 'duration pill missing');
      assert(box.textContent.includes('评分 9.6'), 'score pill missing');

      // 3. Click 「下载到收件箱」 (fillAfter=false)
      const dlBtns = buttons.filter(b => b.textContent === '下载到收件箱');
      const fillBtns = buttons.filter(b => b.textContent === '下载并填入学歌');
      assert(dlBtns.length === 2 && fillBtns.length === 2, 'button labels mismatch');
      dlBtns[0].dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(apiCalls[0].path === 'songs/fetch-to-inbox', 'first call must fetch-to-inbox');
      assert(apiCalls[0].data.candidate_id === 'bili:BV1xx', 'candidate_id mismatch');
      assert(apiCalls[1].path === 'songs/inbox', 'must refresh inbox via loadSongInbox');
      assert($('notice-text').textContent.includes('Drive inbox/'), 'download-only notice');

      // 4. Click 「下载并填入学歌」 (fillAfter=true) prefills title/artist/inbox/auto-search
      $('song-learn-auto-search').checked = true;
      $('song-learn-heading')._scrolled = false;
      fillBtns[0].dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert($('song-learn-title').value === '晴天', 'prefill title mismatch');
      assert($('song-learn-artist').value === '周杰伦', 'prefill artist mismatch');
      assert(
        $('song-learn-inbox').value === '晴天 - 周杰伦.mp3',
        'prefill inbox select mismatch: ' + $('song-learn-inbox').value
      );
      const chosenOpt = $('song-learn-inbox').options.find(o => o.value === '晴天 - 周杰伦.mp3');
      assert(chosenOpt && chosenOpt.dataset.fileId === 'drive_file_88', 'inbox fileId missing');
      assert($('song-learn-auto-search').checked === false, 'auto-search must uncheck');
      assert($('song-learn-heading')._scrolled === true, 'must scroll to learn heading');

      // 5. Fallback title stem when queryTitle & item.title are empty
      apiCalls.length = 0;
      renderSongFinderResults({{
        candidates: [{{
          candidate_id: 'bili:BV2yy', provider: 'bilibili_public',
          title: '', uploader: '', duration_sec: 180, score: 7.5
        }}]
      }}, '', '');
      const fallbackFill = walk($('song-finder-results')).find(
        n => n.tagName === 'BUTTON' && n.textContent === '下载并填入学歌'
      );
      fallbackFill.dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        $('song-learn-title').value === 'fallback_stem - Artist',
        'stem fallback title mismatch: ' + $('song-learn-title').value
      );

      if (typeof print === 'function') print('SONG_FINDER_UI_OK');
      else console.log('SONG_FINDER_UI_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Song Finder JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "SONG_FINDER_UI_OK" in proc.stdout, (
        f"Song Finder async JS test did not complete:\n{proc.stderr}\n{proc.stdout}"
    )


def test_views_polish_markup_and_css_contracts():
    """Verify #songs grouping, folds, #bili-policy button order, multi-platform copy & CSS."""
    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy and "style-src 'sha256-" in policy

    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))

    # 1. #songs 4 top-level section groups, Drive config fold, and .song-finder-list container
    songs_html = panels["songs"]
    groups = re.findall(r'<div class="card" data-section-group="([^"]+)">', songs_html)
    assert groups == ["song_list", "song_inbox", "song_learn", "song_upload"]
    assert 'id="song-fold-drive-config" class="panel-fold"' in songs_html
    fold_body = songs_html.split('id="song-fold-drive-config"')[1].split("</details>")[0]
    for elem_id in ("song-storage-source", "song-drive-details", "song-storage-form"):
        assert f'id="{elem_id}"' in fold_body
    assert '<div id="song-finder-results" class="song-finder-list"></div>' in songs_html
    assert "QQ / Telegram 私聊发送音频" in songs_html
    assert "兼容 QQ / Telegram 语音条与各平台" in songs_html
    assert "Telegram 自动匹配贴纸 / 动图 / 图片通道发送" in panels["memes"]

    # 2. #bili-policy button order: primary save -> secondary run -> secondary reset (far right)
    bili_html = panels["bili-account"]
    bili_policy_block = bili_html.split('id="bili-policy"')[1].split("</form>")[0]
    btn_row = bili_policy_block.rsplit('<div class="row">', 1)[1].split("</div>")[0]
    assert btn_row.index("保存并立即生效") < btn_row.index('id="bili-run"')
    assert btn_row.index('id="bili-run"') < btn_row.index('id="bili-reset"')
    assert 'id="bili-run" class="secondary"' in btn_row
    assert btn_row.count('class="primary"') == 1

    # 3. #memory legacy compression parameters folded + neutral account isolation copy
    memory_html = panels["memory"]
    assert 'id="memory-fold-legacy" class="panel-fold"' in memory_html
    legacy_block = memory_html.split('id="memory-fold-legacy"')[1].split("</details>")[0]
    for elem_id in (
        "group-messages",
        "group-chars",
        "group-keep",
        "group-min_chars",
        "group-max_messages",
        "private-messages",
        "private-chars",
        "private-keep",
    ):
        assert f'id="{elem_id}"' in legacy_block
    assert "画像按平台账号隔离" in memory_html
    assert "画像按数字账号隔离" not in memory_html

    # 4. #life / #voice-companion / #screen-watch / #community multi-platform copy
    assert 'id="social-schedule-card"' in panels["life"]
    assert "社交语音条（QQ / Telegram）与实时连麦" in panels["voice-companion"]
    assert 'id="sw-qq-fallback"' in panels["screen-watch"]
    assert "私聊至 Owner 主通道" in panels["screen-watch"]
    assert "用户 ID（QQ / Telegram / OpenID）" in panels["community"]

    # 5. CSS rules for song finder list, TG inline controls, and MC control bar
    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    assert len(styles) == 1
    css = styles[0]
    for selector in (
        ".song-finder-list",
        ".song-finder-item",
        ".tg-item-controls",
        ".mc-control-bar",
    ):
        assert selector in css, f"Missing CSS selector: {selector}"


def test_views_polish_js_runtime_behaviors():
    """Verify proactive TG fallback, logout cleanup, plugin-bus fold, badges, & song/bili UX."""
    import json

    find_js_engine("No local JS engine (node/jsc) available for views-polish JS runtime test")

    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    life_js = (ADMIN_DIR / "src" / "js" / "05-life-companion.js").read_text(encoding="utf-8")
    comm_js = (ADMIN_DIR / "src" / "js" / "06-identity-community.js").read_text(encoding="utf-8")
    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")

    harness = f"""
    const ALL_IDS = {all_ids_json};
    class MockNode {{
      constructor(tag) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.children = [];
        this.parentElement = null;
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.open = false;
        this.value = '';
        this.className = '';
        this.tabIndex = 0;
        this.options = [];
        this.selectedOptions = [];
        this.files = [];
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get textContent() {{
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{ this._text = String(v ?? ''); this.children = []; }}
      setAttribute(k, v) {{
        if (k === 'class') this.className = String(v);
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            n.parentElement = this;
            this.children.push(n);
            if (n.tagName === 'OPTION') this.options.push(n);
          }} else {{
            const t = new MockNode('#text'); t._text = String(n); this.children.push(t);
          }}
        }}
      }}
      replaceChildren(...nodes) {{
        this.children = []; this.options = []; this._text = ''; this.append(...nodes);
      }}
      add(opt) {{ this.append(opt); }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt, extra={{}}) {{
        const ev = {{ target: this, preventDefault(){{}}, ...extra }};
        for (const fn of (this.listeners[evt] || [])) fn(ev);
      }}
      showModal() {{ this.open = true; }}
      close() {{ this.open = false; }}
      focus() {{ this._focused = true; }}
      select() {{}}
      scrollIntoView() {{ this._scrolled = true; }}
      reset() {{}}
      querySelector(sel) {{
        if (sel === '.dirty-badge[data-dirty-indicator]') {{
          return this.children.find(
            c => c.className === 'dirty-badge' && c.dataset.dirtyIndicator === '1'
          ) || null;
        }}
        return null;
      }}
      querySelectorAll(sel) {{
        if (sel === '.row') return this.children.filter(c => c.className === 'row');
        return [];
      }}
      closest(sel) {{
        if (sel === 'section.tab') return {{ id: this._tabId || 'bili-account' }};
        return null;
      }}
    }}
    class Option extends MockNode {{
      constructor(label, value) {{
        super('option');
        this._text = String(label ?? '');
        this.value = value !== undefined ? String(value) : this._text;
      }}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{ const n = new MockNode('#text'); n._text = String(t); return n; }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    const window = {{}};
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function renderRestHours(){{}}
    function regionName(r){{ return r === 'CN' ? '境内' : '境外'; }}
    const revealedTargets = [];
    function revealSectionFor(target) {{ revealedTargets.push(target?.id || null); }}

    {core_js}
    token = 'polish-token';
    confirmDialog = async () => true;
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};

    const apiCalls = [];
    let modulesResp = [{{ id: 'telegram', owner_delivery: true }}];
    let tgDeliveryResp = {{
      platform: 'telegram',
      connected: true,
      configured: true,
      delivery_enabled: true,
      day: '2026-11-27',
      timezone: 'Asia/Shanghai',
      next_digest_at: 1795780800,
      proactive_daily_limit_usd: 1.5,
      budget: {{ total: 250000, hard_stop: false }},
      queue: [{{ kind: 'digest', status: 'queued', n: 1 }}],
      alerts: []
    }};
    let inboxItems = [];
    api = async (path, method='GET', data) => {{
      apiCalls.push({{ path, method, data }});
      if (path === 'proactive' && method === 'GET') return {{ assigned: false }};
      if (path === 'life/modules' && method === 'GET') return modulesResp;
      if (path === 'life/modules/telegram/owner-delivery' && method === 'GET') {{
        return tgDeliveryResp;
      }}
      if (path.startsWith('life/modules/telegram/owner-delivery/') && method === 'POST') {{
        return {{ status: 'sent', delivery: 'sent' }};
      }}
      if (path === 'songs/fetch-to-inbox' && method === 'POST') {{
        const saved = {{
          id: 'df_1', file_id: 'df_1', name: '晴天 - 周杰伦.mp3', size: 2048000
        }};
        inboxItems = [{{ id: 'df_1', name: saved.name, size: saved.size }}];
        return saved;
      }}
      if (path === 'songs/inbox' && method === 'GET') return {{ items: inboxItems }};
      if (path === 'bili-account' && method === 'GET') {{
        return {{
          config: {{
            enabled: false, allow_like: false, allow_favorite: false,
            favorite_folder_id: '', daily_action_limit: 1
          }},
          watch_history_counts: {{
            done: 0, processing: 0, retryable: 0, exhausted: 0, uncertain: 0
          }},
          credentials: {{ sessdata: false }},
          recent_runs: [],
          source: 'env',
          credential_source: 'env',
          timezone: 'Asia/Shanghai',
          actions_today: 0
        }};
      }}
      if (path === 'bili-account/video-watch' && method === 'GET') {{
        return {{
          config: {{ enabled: false, max_seconds: 180, daily_limit: 3 }},
          source: 'env', attempts_today: 0, remaining_today: 3, recent_attempts: []
        }};
      }}
      return {{ ok: true }};
    }};

    {life_js}
    {comm_js}
    {media_js}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. proactive() falls back to telegram owner-delivery when proactive returns assigned=false
      await proactive();
      assert(activeProactiveBase === 'life/modules/telegram/owner-delivery', 'base mismatch');
      assert($('dm-test').disabled === false && $('dm-now').disabled === false, 'buttons enabled');
      assert($('proactive-stats').textContent.includes('Telegram (telegram)'), 'TG label missing');
      assert(!$('proactive-stats').textContent.includes('NapCat'), 'NapCat leaked');
      assert($('proactive-state').textContent.includes('允许投递'), 'delivery state missing');
      assert(
        $('proactive-budget').textContent.includes('本地累计 $0.2500')
        && !$('proactive-budget').textContent.includes('未设累计上限'),
        'proactive budget must omit removed cumulative total cap'
      );
      assert($('proactive-queue').textContent.includes('每日汇报'), 'queue row missing');

      apiCalls.length = 0;
      $('dm-test').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        apiCalls.some(
          c => c.method === 'POST' && c.path === 'life/modules/telegram/owner-delivery/test'
        ),
        'dm-test must POST to telegram owner-delivery/test: ' + JSON.stringify(apiCalls)
      );

      apiCalls.length = 0;
      $('dm-now').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        apiCalls.some(
          c => c.method === 'POST' && c.path === 'life/modules/telegram/owner-delivery/digest'
        ),
        'dm-now must POST to telegram owner-delivery/digest: ' + JSON.stringify(apiCalls)
      );

      // Graceful unassigned fallback when life/modules returns non-array
      modulesResp = {{}};
      await proactive();
      assert(activeProactiveBase === 'proactive', 'unassigned must reset base');
      assert($('dm-test').disabled === true, 'dm-test must disable when unassigned');
      assert(
        $('proactive-state').textContent.includes('Owner 投递已分配给其他社交模块'),
        'fallback message missing'
      );

      // 2. renderScreenWatch neutral channel label + onLogout cleans MC fields & proactive base
      renderScreenWatch({{
        timezone: 'Asia/Shanghai',
        config: {{ enabled: true }},
        buffered_frames: 0,
        frames_received: 0,
        routed_voice_count: 0,
        standalone_commentary_count: 0,
        recent_commentaries: [{{ at: 1795780800, channel: 'qq_dm', game_title: 'MC', text: 'hi' }}]
      }});
      assert(
        $('sw-commentary-table').textContent.includes('Owner 私聊 + 气泡')
          && !$('sw-commentary-table').textContent.includes('Owner QQ'),
        'screen-watch qq_dm label must be platform-neutral'
      );

      renderMinecraft({{
        active_plan: '去挖铁矿',
        waypoints: [{{ name: 'home', pos: [1, 64, 2] }}],
        action_log: [{{ ts: 1795780800, trigger: 'plan', action: 'mine', outcome: 'ok' }}]
      }});
      activeProactiveBase = 'life/modules/telegram/owner-delivery';
      for (const fn of logoutHooks) fn();
      assert(activeProactiveBase === 'proactive', 'logout must reset activeProactiveBase');
      assert($('mc-waypoints-list').children.length === 0, 'logout must clear mc-waypoints-list');
      assert($('mc-action-log').children.length === 0, 'logout must clear mc-action-log');
      assert(
        $('mc-plan-summary').textContent === '暂无连贯计划或路标记录。',
        'logout must reset mc-plan-summary'
      );
      token = 'polish-token';

      // 3. #plugin-bus card edit form folded + unified Telegram platform badge across tables
      renderPluginBus({{
        ws_enabled: true,
        ws_path: '/ws/plugin',
        allowed_scopes_catalog: ['actions', 'chat'],
        plugins: [{{
          plugin_id: 'telegram', display_name: 'Telegram', online: true,
          enabled: true, node_region: 'KR', scopes: ['chat'], actions_count: 1
        }}]
      }});
      const pbFolds = walk($('pb-plugins-list')).filter(
        n => n.tagName === 'DETAILS' && n.className === 'card-edit-fold'
      );
      assert(pbFolds.length === 1 && pbFolds[0].open === false, 'plugin card fold missing');
      assert(
        pbFolds[0].textContent.includes('编辑插件配置 / Scope')
          && walk(pbFolds[0]).some(n => n.tagName === 'FORM'),
        'plugin fold must wrap inline edit form'
      );

      renderIdentityCenter({{
        persons: [{{
          person_id: 'p_tg1', display_name: 'TG 友', is_owner: false, muted: false,
          accounts: [{{ platform: 'telegram', user_id: '777', nickname: 'tgu', source: 'auto' }}]
        }}]
      }});
      assert(
        walk($('identity-persons-list')).some(
          n => n.className.includes('pill-tg') && n.textContent === 'Telegram'
        ),
        'identity table must use unified Telegram pill-tg badge'
      );

      renderAffection({{
        source: 'env',
        config: {{ enabled: true }},
        users: [{{
          user_id: '777', platform: 'telegram', nickname: 'tgu',
          score: 65, level: 4, tier: 'familiar'
        }}]
      }});
      assert(
        walk($('aff-users-table')).some(
          n => n.className.includes('pill-tg') && n.textContent === 'Telegram'
        ),
        'aff-users-table must use unified Telegram pill-tg badge'
      );

      renderAffectionHistory([{{
        user_id: '777', platform: 'telegram', prev_score: 60, new_score: 65, delta: 5, kind: 'chat'
      }}]);
      assert(
        walk($('aff-history-table')).some(
          n => n.className.includes('pill-tg') && n.textContent === 'Telegram'
        ),
        'aff-history-table must use unified Telegram pill-tg badge'
      );

      // 4. Song Finder item class + revealSectionFor on both prefill paths
      renderSongInboxTable([{{ id: 'df_1', name: '晴天.mp3', size: 1024 }}], '');
      revealedTargets.length = 0;
      const inboxFillBtn = walk($('song-inbox-table')).find(
        n => n.tagName === 'BUTTON' && n.textContent === '填入学歌'
      );
      inboxFillBtn.dispatch('click');
      assert(
        revealedTargets.includes('song-learn-heading'),
        'inbox prefill must call revealSectionFor(song-learn-heading)'
      );

      renderSongFinderResults({{
        provider_used: 'bilibili_public',
        candidates: [{{
          candidate_id: 'bili:BV1', provider: 'bilibili_public',
          title: '晴天', uploader: '周杰伦', duration_sec: 260, score: 9.5
        }}]
      }}, '晴天', '周杰伦');
      const finderItems = $('song-finder-results').children.filter(
        n => n.className === 'song-finder-item'
      );
      assert(finderItems.length === 1, 'finder row must use class song-finder-item');
      revealedTargets.length = 0;
      const finderFillBtn = walk(finderItems[0]).find(
        n => n.tagName === 'BUTTON' && n.textContent === '下载并填入学歌'
      );
      finderFillBtn.dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        revealedTargets.includes('song-learn-heading'),
        'finder download+fill must call revealSectionFor(song-learn-heading)'
      );

      // 5. #bili-credentials dirty tracking marks dirty and clears on save & clear
      const biliCredForm = $('bili-credentials');
      assert(dirtyRegistry.has(biliCredForm), 'bili-credentials must be tracked by trackDirty');
      biliCredForm.dispatch('input', {{ target: $('bili-sessdata') }});
      assert(
        dirtyRegistry.get(biliCredForm).dirty === true,
        'input must mark bili-credentials dirty'
      );
      biliCredForm.dispatch('submit', {{ submitter: null }});
      await Promise.all(pendingTasks.splice(0));
      assert(
        dirtyRegistry.get(biliCredForm).dirty === false,
        'save must clear bili-credentials dirty'
      );

      biliCredForm.dispatch('input', {{ target: $('bili-jct') }});
      assert(dirtyRegistry.get(biliCredForm).dirty === true, 're-edit must mark dirty');
      $('bili-clear').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        dirtyRegistry.get(biliCredForm).dirty === false,
        'clear must clear bili-credentials dirty'
      );

      if (typeof print === 'function') print('VIEWS_POLISH_JS_OK');
      else console.log('VIEWS_POLISH_JS_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Views polish JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "VIEWS_POLISH_JS_OK" in proc.stdout, (
        f"Views polish async JS test did not complete:\n{proc.stderr}\n{proc.stdout}"
    )
