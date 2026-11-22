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

    assert "colab/drive/inbox" not in conf
    for exact_loc in (
        "location = /admin/api/songs/inbox",
        "location = /admin/api/songs/upload",
    ):
        m = re.search(
            re.escape(exact_loc) + r"\s*\{([^}]*)\}",
            conf,
            re.S,
        )
        assert m is not None, f"missing {exact_loc} block"
        block = m.group(1)
        assert "client_max_body_size 100m;" in block
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
    html = ADMIN_HTML.read_text(encoding="utf-8")
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
    import shutil

    jsc_path = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")
    runner: list[str] | None = None
    if shutil.which("node"):
        runner = ["node", "-e"]
    elif jsc_path.is_file():
        runner = [str(jsc_path), "-e"]
    if runner is None:
        pytest.skip("No local JS engine (node/jsc) available for DOM renderer execution test")

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    ledger_js = (ADMIN_DIR / "src" / "js" / "02-overview-ledger.js").read_text(encoding="utf-8")
    shell_js = (ADMIN_DIR / "src" / "js" / "08-modules-shell.js").read_text(encoding="utf-8")
    colab_start = shell_js.index("const colabErrorLabels=")
    colab_end = shell_js.index("async function colabCompute(")
    colab_view_js = shell_js[colab_start:colab_end]

    # Collect every static HTML ID so the mock document returns a valid MockElement for all of them.
    html = ADMIN_HTML.read_text(encoding="utf-8")
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
        enabled: true, preferred_gpu: 'L4', fallback_gpus: ['T4'],
        daily_job_limit: 5, daily_cu_budget: 10
      }},
      worker: {{ reachable: true, compute_units: 42.5, active_sessions: 1, running_job_id: null }},
      today: {{ jobs_count: 2, cu_used: 0.85, daily_job_limit: 5, daily_cu_budget: 10 }},
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

    // renderColabOverview without warm sessions must never render literal "null" in colab-pills
    renderColabOverview({{
      configured: true,
      source: 'default',
      config: {{ enabled: true, preferred_gpu: 'L4', fallback_gpus: ['T4'],
        daily_job_limit: 5, daily_cu_budget: 10 }},
      worker: {{ reachable: true, compute_units: 10.0, active_sessions: 0, warm_sessions: [] }},
      today: {{ jobs_count: 0, cu_used: 0, daily_job_limit: 5, daily_cu_budget: 10 }},
      jobs: [],
      warm_sessions: []
    }});
    assert(
      !$('colab-pills').textContent.includes('null'),
      'colab-pills without warm sessions must not render literal null text'
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

    proc = subprocess.run(
        [*runner, harness],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"JS runtime test failed:\n{proc.stderr}\n{proc.stdout}"


def test_colab_and_songs_ui_physical_decoupling():
    """Verify #colab-compute and #songs are physically decoupled in HTML, JS, and runtime."""
    import json
    import shutil

    html = ADMIN_HTML.read_text(encoding="utf-8")
    assert audit_html(html) == []

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)

    # Removed Colab song/Drive coupling DOM IDs and legacy endpoints
    for removed_token in (
        "colab/drive/inbox",
        "colab-drive-details",
        "colab-drive-folder-id",
        "colab-inbox-table",
        "colab-refresh-inbox",
    ):
        assert removed_token not in html, f"Residual coupled token found: {removed_token!r}"

    # Pure Colab GPU & standalone Song Studio Drive DOM IDs
    for required_id in (
        "colab-job-types",
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
        "song-learn-inbox",
    ):
        assert required_id in id_set, f"Missing expected decoupled DOM id: {required_id}"

    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(scripts) == 1
    script = scripts[0]
    assert "/admin/api/songs/inbox" in script
    assert "api('songs/inbox')" in script
    assert "api('songs/storage'" in script
    assert "supported_job_types" in script

    jsc_path = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")
    runner: list[str] | None = None
    if shutil.which("node"):
        runner = ["node", "-e"]
    elif jsc_path.is_file():
        runner = [str(jsc_path), "-e"]
    if runner is None:
        pytest.skip("No local JS engine (node/jsc) available for decoupled UI runtime test")

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
    """

    proc = subprocess.run(
        [*runner, harness],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"Song decoupled JS test failed:\n{proc.stderr}\n{proc.stdout}"
