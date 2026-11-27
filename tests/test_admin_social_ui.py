"""Tests for Phase 3C (F10, F11, F10-T): /admin Social UI, APIs, & architecture.html."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
from pydantic import SecretStr

from admin.build import _extract_js_id_refs, _IdCollector, audit_html, build_html
from admin.csp import content_security_policy
from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.social import OwnerBinding
from tests.fakes import FakeGateway
from tests.js_runtime import find_js_engine, run_js_harness
from tests.test_admin import TOKEN
from tests.test_plugin_telegram import (
    BOT_ID,
    GROUP_ID,
    OWNER_UID,
    _FakeTelegramApi,
    _start_connected_plugin,
    _stop_connected_plugin,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_DIR = REPO_ROOT / "admin"
ARCH_HTML = REPO_ROOT / "architecture.html"


def _dual_platform_settings(settings: Any, *, primary: str = "telegram") -> Any:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": SecretStr("x" * 32),
            "onebot_self_id": "10000",
            "owner_qq_id": "20000",
            "onebot_allowed_groups": {"40000"},
            "qq_enabled": False,
            "owner_delivery_platform": primary,
            "proactive_dm_enabled": True,
            "daily_digest_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {
                f"telegram:{GROUP_ID}": "at_only",
                "onebot:40000": "at_only",
            },
        }
    )


def test_admin_social_ui_build_and_markup(tmp_path: Path) -> None:
    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy

    # --check in memory and --verify-file against tmp_path
    proc_check = subprocess.run(
        [sys.executable, str(ADMIN_DIR / "build.py"), "--check"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_check.returncode == 0, proc_check.stderr or proc_check.stdout

    out_path = tmp_path / "admin_page.html"
    proc_write = subprocess.run(
        [sys.executable, str(ADMIN_DIR / "build.py"), "--output", str(out_path)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_write.returncode == 0, proc_write.stderr or proc_write.stdout
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

    # DOM ID integrity
    collector = _IdCollector()
    collector.feed(html)
    assert len(collector.ids) == len(set(collector.ids))
    id_set = set(collector.ids)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(scripts) == 1
    script = scripts[0]
    assert sorted({ref for ref in _extract_js_id_refs(script) if ref not in id_set}) == []

    # Section-level assertions for #proactive, #life, and #screen-watch
    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    proactive_html = panels["proactive"]
    life_html = panels["life"]
    screen_watch_html = panels["screen-watch"]

    assert "投递通道断线时排队" in proactive_html
    assert "QQ 断线时排队" not in proactive_html

    for expected_id in (
        "social-schedule-card",
        "refresh-social-schedule",
        "social-schedule-status",
        "social-schedule-form",
        "social-owner-delivery",
        "social-owner-unlimited",
        "social-group-cooldown",
        "social-group-window",
        "social-group-min-msgs",
        "social-desktop-suppress",
        "social-schedule-save",
        "social-schedule-reset",
    ):
        assert f'id="{expected_id}"' in life_html

    assert 'data-open-tab="modules"' in life_html
    assert 'data-open-tab="plugin-social_presence"' not in life_html

    # #screen-watch remains intentionally scoped to deliver_to_qq_when_no_voice
    assert 'id="sw-qq-fallback"' in screen_watch_html

    # Platform-neutral JS labels and removed legacy hardcoded QQ labels
    assert "social_presence:'社交主动'" in script
    assert "'平台已确认'" in script
    assert "'尚未配置 Owner 投递目标'" in script
    assert "'QQ 已确认'" not in script
    assert "'尚未配置 Owner QQ'" not in script
    assert "{...c,...(c.module_settings?.social_presence||{})}" in script

    # Dedicated SVG icon templates for platform/plugin & system modules (cards + sidebar)
    for icon_id in (
        "social_presence",
        "telegram",
        "song_finder",
        "web_tools",
        "doc_render",
        "memory_hub",
        "voice_gateway",
    ):
        assert f'id="catalog-icon-{icon_id}"' in panels["modules"]
    assert "catalog-icon-'+m.id" in script


def test_admin_social_ui_js_runtime() -> None:
    find_js_engine("No local JS engine (node/jsc) available for admin JS runtime test")

    html = build_html()
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))
    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    life_js = (ADMIN_DIR / "src" / "js" / "05-life-companion.js").read_text(encoding="utf-8")

    harness = f"""
    const ALL_IDS = {all_ids_json};
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
        this.open = false;
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
      showModal() {{ this.open = true; }}
      close() {{ this.open = false; }}
      focus() {{}}
      select() {{}}
      querySelector(sel) {{
        if (sel === '.dirty-badge[data-dirty-indicator]') {{
          return this.children.find(
            c => c.className === 'dirty-badge' && c.dataset.dirtyIndicator === '1'
          ) || null;
        }}
        return null;
      }}
      querySelectorAll(sel) {{
        if (sel === '.row') {{
          return this.children.filter(c => c.className === 'row');
        }}
        return [];
      }}
      closest(sel) {{
        if (sel === 'section.tab') return {{ id: 'life' }};
        return null;
      }}
    }}
    const Node = MockNode;
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    const socialForm = byId.get('social-schedule-form');
    const rowStub = new MockNode('div');
    rowStub.className = 'row';
    socialForm.append(rowStub);

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
    function renderRestHours(){{}}

    const calls = [];
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};

    let nextConfirm = true;
    confirmDialog = async () => nextConfirm;
    let nextProactivePayload = {{ assigned: false }};

    api = async (path, method='GET', data) => {{
      calls.push({{ path, method, data }});
      if (path === 'proactive') return nextProactivePayload;
      if (path === 'life/modules/social_presence' && method === 'GET') {{
        return {{
          id: 'social_presence',
          enabled: true,
          proactive_paused: false,
          scheduling: {{
            values: {{
              owner_delivery_enabled: false,
              owner_dms_unlimited: true,
              group_cooldown_seconds: 1800,
              group_active_window_seconds: 900,
              group_min_recent_messages: 4,
              desktop_active_suppress_seconds: 3600,
            }}
          }}
        }};
      }}
      if (path === 'life/modules/social_presence/schedule' && method === 'PUT') {{
        return {{
          id: 'social_presence',
          enabled: true,
          proactive_paused: false,
          scheduling: {{ values: data }}
        }};
      }}
      if (path === 'life/modules/social_presence/schedule' && method === 'DELETE') {{
        return {{
          id: 'social_presence',
          enabled: true,
          proactive_paused: false,
          scheduling: {{
            values: {{
              owner_delivery_enabled: true,
              owner_dms_unlimited: false,
              group_cooldown_seconds: 1500,
              group_active_window_seconds: 1800,
              group_min_recent_messages: 2,
              desktop_active_suppress_seconds: 7200,
            }}
          }}
        }};
      }}
      return {{}};
    }};

    {life_js}

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. proactive() handles assigned=false
      nextProactivePayload = {{ assigned: false }};
      await proactive();
      assert($('dm-test').disabled === true, 'dm-test must disable when unassigned');
      assert($('dm-now').disabled === true, 'dm-now must disable when unassigned');
      assert(
        $('proactive-state').textContent.includes('Owner 投递已分配给其他社交模块'),
        'unassigned message mismatch'
      );

      // 2. proactive() handles telegram without NapCat or Owner QQ leaks
      nextProactivePayload = {{
        assigned: true,
        platform: 'telegram',
        connected: true,
        configured: false,
        delivery_enabled: true,
        day: '2026-11-26',
        timezone: 'Asia/Shanghai',
        next_digest_at: 1795694400,
        budget: {{ total: 0, total_limit: 0, hard_stop: false }},
        proactive_daily_limit_usd: 1.0,
        queue: [],
        alerts: []
      }};
      await proactive();
      assert(
        $('proactive-stats').textContent.includes('telegram'),
        'proactive stats must show telegram'
      );
      assert(
        !$('proactive-stats').textContent.includes('NapCat'),
        'proactive stats must not leak NapCat for telegram'
      );
      assert(
        $('proactive-state').textContent.includes('尚未配置 Owner 投递目标'),
        'must use neutral missing-target label'
      );
      assert(
        !$('proactive-state').textContent.includes('Owner QQ'),
        'must not mention Owner QQ'
      );

      // 3. proactive() handles onebot cleanly
      nextProactivePayload.platform = 'onebot';
      nextProactivePayload.configured = true;
      await proactive();
      assert(
        $('proactive-stats').textContent.includes('NapCat (QQ)'),
        'onebot must display NapCat (QQ)'
      );
      assert(
        $('proactive-state').textContent.includes('允许投递'),
        'configured onebot must show delivery status'
      );

      // 4. renderLife populates #social-schedule-form, social_presence label, and '平台已确认'
      renderLife({{
        source: 'sqlite',
        timezone: 'Asia/Shanghai',
        config: {{
          enabled: true,
          max_proactive_per_day: 6,
          owner_delivery_enabled: true,
          owner_dms_unlimited: true,
          group_cooldown_seconds: 2400,
          group_active_window_seconds: 600,
          group_min_recent_messages: 3,
          desktop_active_suppress_seconds: 5400,
          module_settings: {{
            social_presence: {{
              owner_delivery_enabled: true,
              owner_dms_unlimited: true,
              group_cooldown_seconds: 2400,
              group_active_window_seconds: 600,
              group_min_recent_messages: 3,
              desktop_active_suppress_seconds: 5400
            }}
          }}
        }},
        world_state: {{
          current_module: 'social_presence',
          focus_level: 'LOW',
          mood: 'calm',
          energy: 80,
          boredom: 20,
          modules: [{{ module_id: 'social_presence', enabled: true, proactive_paused: true }}],
          experience: {{
            plan: {{ outcome: 'accepted', plan_module: 'social_presence', accepted: 1 }},
            actual: {{ state: 'idle' }},
            changes: [],
            news: [{{ source: 'social_presence', label: '共享动态', at: 1795694400 }}],
            news_count: 1
          }}
        }},
        digest_history: [{{
          day: '2026-11-26',
          stage: 'confirmed',
          generated_at: 1795694400,
          queued_at: 1795694410,
          confirmed_at: 1795694420
        }}],
        engine: {{ running: true }},
        nodes: {{}},
        desktop_memory: {{}},
        intents: [],
        rule_receipts: [],
        events: [],
        ledger_events: []
      }});
      assert($('social-owner-delivery').checked === true, 'owner-delivery checked mismatch');
      assert($('social-owner-unlimited').checked === true, 'owner-unlimited checked mismatch');
      assert($('social-group-cooldown').value === 2400, 'group-cooldown value mismatch');
      assert($('social-group-window').value === 600, 'group-window value mismatch');
      assert($('social-group-min-msgs').value === 3, 'group-min-msgs value mismatch');
      assert($('social-desktop-suppress').value === 5400, 'desktop-suppress value mismatch');
      assert($('social-schedule-save').disabled === false, 'save button must enable');
      assert($('social-schedule-reset').disabled === false, 'reset button must enable');
      assert(
        $('social-schedule-status').textContent.includes('高专注已暂停主动'),
        'status must show proactive_paused'
      );
      assert(
        $('life-stats').textContent.includes('社交主动'),
        'renderLife must map social_presence to 社交主动'
      );
      assert(
        $('life-digests').textContent.includes('平台已确认'),
        'life-digests header must say 平台已确认'
      );
      assert(
        !$('life-digests').textContent.includes('QQ 已确认'),
        'life-digests header must not say QQ 已确认'
      );

      // 5. #social-schedule-form rejects out-of-range integer before calling api()
      calls.length = 0;
      $('social-group-cooldown').value = '100';
      $('social-schedule-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 0, 'invalid cooldown must not invoke api()');
      assert(
        $('notice-text').textContent.includes('300～86400'),
        'invalid cooldown must show notice'
      );

      // 6. #social-schedule-form submits valid ints/bools to schedule PUT
      $('social-owner-delivery').checked = false;
      $('social-owner-unlimited').checked = true;
      $('social-group-cooldown').value = '1200';
      $('social-group-window').value = '900';
      $('social-group-min-msgs').value = '5';
      $('social-desktop-suppress').value = '3600';
      $('social-schedule-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 1, 'expected 1 PUT call');
      assert(
        calls[0].path === 'life/modules/social_presence/schedule' && calls[0].method === 'PUT',
        'PUT endpoint mismatch'
      );
      assert(calls[0].data.owner_delivery_enabled === false, 'bool payload mismatch');
      assert(calls[0].data.owner_dms_unlimited === true, 'bool payload mismatch');
      assert(calls[0].data.group_cooldown_seconds === 1200, 'int payload mismatch');
      assert(calls[0].data.group_min_recent_messages === 5, 'int payload mismatch');
      assert($('notice-text').textContent === '共享社交主动排程已保存', 'save notice mismatch');

      // 7. #social-schedule-reset respects confirmDialog cancel and confirm
      calls.length = 0;
      nextConfirm = false;
      $('social-schedule-reset').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 0, 'cancelled reset must not invoke api()');

      nextConfirm = true;
      $('social-schedule-reset').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        calls.length === 1
          && calls[0].method === 'DELETE'
          && calls[0].path === 'life/modules/social_presence/schedule',
        'reset DELETE mismatch'
      );
      assert($('social-group-cooldown').value === 1500, 'reset must restore default cooldown');
      assert(
        $('social-owner-unlimited').checked === false,
        'reset must restore default unlimited'
      );

      // 8. #refresh-social-schedule fetches GET life/modules/social_presence
      calls.length = 0;
      $('refresh-social-schedule').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        calls.length === 1
          && calls[0].method === 'GET'
          && calls[0].path === 'life/modules/social_presence',
        'refresh GET mismatch'
      );
      assert(
        $('social-group-cooldown').value === 1800,
        'refresh must update form from response'
      );

      // 9. logout hook disables buttons and clears status
      for (const fn of logoutHooks) fn();
      assert($('social-schedule-save').disabled === true, 'logout must disable save button');
      assert($('social-schedule-reset').disabled === true, 'logout must disable reset button');
      assert($('social-schedule-status').textContent === '', 'logout must clear status');
    }})().catch(err => {{
      console.error(err && err.stack ? err.stack : err);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"JS runtime test failed:\n{proc.stderr}\n{proc.stdout}"


async def test_admin_social_schedule_api_and_flat_compat(settings: Any) -> None:
    s = _dual_platform_settings(settings, primary="telegram")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        # 1. Directory exposes social_presence with social.speak
        r_mods = await client.get("/admin/api/life/modules")
        assert r_mods.status_code == 200
        mods = {m["id"]: m for m in r_mods.json()}
        assert "social_presence" in mods
        assert mods["social_presence"]["action_count"] == 1
        assert mods["social_presence"]["panel"] is None
        assert mods["social_presence"]["chat_platforms"] == []
        assert mods["social_presence"]["owner_delivery"] is False

        # 2. GET /admin/api/life/modules/social_presence returns 6 SocialScheduleOptions fields
        r_detail = await client.get("/admin/api/life/modules/social_presence")
        assert r_detail.status_code == 200
        detail = r_detail.json()
        assert detail["id"] == "social_presence"
        assert detail["scheduling"]["source"] == "env"
        assert set(detail["scheduling"]["fields"]) == {
            "owner_delivery_enabled",
            "owner_dms_unlimited",
            "group_cooldown_seconds",
            "group_active_window_seconds",
            "group_min_recent_messages",
            "desktop_active_suppress_seconds",
        }
        assert [a["name"] for a in detail["actions"]] == ["social.speak"]

        # 3. PUT /admin/api/life/modules/social_presence/schedule syncs with /admin/api/life
        updated = {
            "owner_delivery_enabled": False,
            "owner_dms_unlimited": True,
            "group_cooldown_seconds": 1200,
            "group_active_window_seconds": 900,
            "group_min_recent_messages": 4,
            "desktop_active_suppress_seconds": 3600,
        }
        r_put = await client.put("/admin/api/life/modules/social_presence/schedule", json=updated)
        assert r_put.status_code == 200
        assert r_put.json()["scheduling"]["values"] == updated
        assert r_put.json()["scheduling"]["source"] == "admin"

        r_life = await client.get("/admin/api/life")
        assert r_life.status_code == 200
        cfg = r_life.json()["config"]
        for k, v in updated.items():
            assert cfg[k] == v
        assert cfg["module_settings"]["social_presence"] == updated

        # 4. Strict validation rejects string/out-of-range/unknown fields
        for bad_payload in (
            {"group_cooldown_seconds": "1500"},
            {"group_cooldown_seconds": 100},
            {"owner_dms_unlimited": 1},
            {"unknown_field": True},
        ):
            r_bad = await client.put(
                "/admin/api/life/modules/social_presence/schedule", json=bad_payload
            )
            assert r_bad.status_code == 400
            assert r_bad.json()["detail"] == "invalid_module_settings"

        # 5. PUT /admin/api/life flat compat updates social_presence canonically
        r_flat = await client.put(
            "/admin/api/life",
            json={"owner_delivery_enabled": True, "group_cooldown_seconds": 2100},
        )
        assert r_flat.status_code == 200
        assert r_flat.json()["config"]["owner_delivery_enabled"] is True
        assert r_flat.json()["config"]["group_cooldown_seconds"] == 2100
        r_detail2 = await client.get("/admin/api/life/modules/social_presence")
        assert r_detail2.status_code == 200
        assert r_detail2.json()["scheduling"]["values"]["owner_delivery_enabled"] is True
        assert r_detail2.json()["scheduling"]["values"]["group_cooldown_seconds"] == 2100

        # 6. DELETE /admin/api/life/modules/social_presence/schedule restores defaults
        r_del = await client.delete("/admin/api/life/modules/social_presence/schedule")
        assert r_del.status_code == 200
        assert r_del.json()["scheduling"]["values"] == {
            "owner_delivery_enabled": True,
            "owner_dms_unlimited": False,
            "group_cooldown_seconds": 1500,
            "group_active_window_seconds": 1800,
            "group_min_recent_messages": 2,
            "desktop_active_suppress_seconds": 7200,
        }


async def test_admin_legacy_sqlite_migration_and_owner_delivery_routes(settings: Any) -> None:
    s = _dual_platform_settings(settings, primary="telegram")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        svc = app.state.service
        # Seed legacy SQLite admin_config (`qq_social` + `qq.speak`)
        await svc.store.runtime.save_config(
            "life_engine",
            {
                "module_settings": {
                    "qq_social": {
                        "owner_delivery_enabled": False,
                        "owner_dms_unlimited": True,
                        "group_cooldown_seconds": 1950,
                    }
                },
                "action_modes": {"qq.speak": "off"},
            },
        )
        svc.life_engine = LifeEngine(svc)
        await svc.life_engine.initialize()

        r_life = await client.get("/admin/api/life")
        assert r_life.status_code == 200
        cfg = r_life.json()["config"]
        assert cfg["owner_delivery_enabled"] is False
        assert cfg["owner_dms_unlimited"] is True
        assert cfg["group_cooldown_seconds"] == 1950
        assert "qq_social" not in cfg.get("module_settings", {})

        r_sp = await client.get("/admin/api/life/modules/social_presence")
        assert r_sp.status_code == 200
        sp_json = r_sp.json()
        assert sp_json["scheduling"]["values"]["owner_delivery_enabled"] is False
        assert sp_json["scheduling"]["values"]["owner_dms_unlimited"] is True
        assert sp_json["scheduling"]["values"]["group_cooldown_seconds"] == 1950
        assert sp_json["actions"][0]["name"] == "social.speak"
        assert sp_json["actions"][0]["mode"] == "off"

        # /admin/api/proactive returns assigned=False when telegram is primary;
        # telegram owner-delivery route returns live status once plugin is connected.
        r_proactive = await client.get("/admin/api/proactive")
        assert r_proactive.status_code == 200
        assert r_proactive.json() == {"assigned": False}

        r_qq_od = await client.get("/admin/api/life/modules/qq_social/owner-delivery")
        assert r_qq_od.status_code == 404
        assert r_qq_od.json()["detail"] == "owner_delivery_not_assigned"

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            r_tg_od = await client.get("/admin/api/life/modules/telegram/owner-delivery")
            assert r_tg_od.status_code == 200
            tg_od = r_tg_od.json()
            assert tg_od["platform"] == "telegram"
            assert tg_od["configured"] is True
            assert tg_od["connected"] is True
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


def test_architecture_html_topology_and_links() -> None:
    html = ARCH_HTML.read_text(encoding="utf-8")
    hrefs = re.findall(r'href="([^"]+)"', html)
    missing = [
        href
        for href in hrefs
        if not href.startswith(("#", "http://", "https://")) and not (REPO_ROOT / href).is_file()
    ]
    assert missing == [], f"Missing local architecture.html links: {missing}"

    for expected in (
        "social_presence",
        "social.speak",
        "SocialScheduleOptions",
        "RecipientResolver",
        "is_verified_owner_scope",
        "boundowner:",
        "owner_chat",
        "apply_presence",
        "src/hakimi/social.py",
        "src/hakimi/social_presence.py",
        "src/hakimi/life_rules.py",
        "src/hakimi/node_transport.py",
    ):
        assert expected in html, f"Expected symbol/link missing from architecture.html: {expected}"

    for stale in (
        "qq.speak 票据",
        "<code>qq.speak</code></small>",
        "<code>telegram.speak</code>",
        "qq/milestones.py",
        "node_agent/mc_bridge_slot.py",
        "QQ 来信到确认回复的先后顺序",
        "QQ 的分享规则",
    ):
        assert stale not in html, f"Stale string still present in architecture.html: {stale}"


def test_admin_telegram_social_hot_config_ui_and_platform_badges() -> None:
    """Verify Telegram first-class tab, inline group policy, QQ purification, and admission bots."""
    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)
    assert len(collector.ids) == len(id_set)

    assert 'data-module="telegram"' in html
    assert 'id="nav-telegram"' in html
    assert 'data-tab="telegram"' in html

    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    tg_html = panels["telegram"]
    gp_html = panels["group-policy"]
    adm_html = panels["admission"]
    mc_html = panels["minecraft"]
    filter_html = panels["filter"]
    sandbox_html = panels["sandbox"]

    # 1. #telegram owns tg-social-card, session privacy, and module actions.
    for expected_id in (
        "tg-social-card",
        "tg-refresh",
        "tg-status-pills",
        "tg-status-meta",
        "tg-capabilities-row",
        "tg-config-form",
        "tg-group-input",
        "tg-add-group",
        "tg-groups-list",
        "tg-private-mode",
        "tg-private-mode-hint",
        "tg-user-input",
        "tg-add-user",
        "tg-users-list",
        "tg-memory-mirror",
        "tg-env-defaults",
        "tg-save-config",
        "tg-reset-config",
        "tg-privacy-note",
        "tg-privacy-table",
        "telegram-actions",
    ):
        assert f'id="{expected_id}"' in tg_html, f"Missing Telegram UI id: {expected_id}"
    assert "group_memory_mirror" in tg_html
    assert "Antigravity_Memory" in tg_html

    # 2. #group-policy is strictly QQ group policy without duplicate bots or tg-social-card.
    assert 'id="groups"' in gp_html
    assert 'id="refresh-group-policy"' in gp_html
    assert 'data-open-tab="admission"' in gp_html
    for removed_id in (
        "tg-social-card",
        "bot-default-threshold-form",
        "bot-default-threshold",
        "bot-id",
        "bot-custom-threshold",
        "add-bot",
        "bots",
    ):
        assert f'id="{removed_id}"' not in gp_html, f"Stale id in #group-policy: {removed_id}"
    assert "bot-default-threshold-form" not in id_set
    assert "bots" not in id_set

    # 3. #admission, #filter, #sandbox, and #minecraft markup contracts.
    assert "Telegram 渠道热配置与群策略" not in adm_html
    assert "QQ 群聊互动策略 →" in adm_html
    assert "各自平台模块页（QQ / Telegram）" in filter_html
    assert "不会向真实聊天发送消息" in sandbox_html
    assert "当前沙盒按 QQ 规则模拟" in sandbox_html
    assert 'id="mc-action-log" class="table"' in mc_html
    assert 'class="mc-control-bar row"' in mc_html
    mc_form_match = re.search(r'<form id="life-mc-form"[^>]*>(.*?)</form>', mc_html, re.S)
    assert mc_form_match is not None
    assert 'id="life-mc-topic"' in mc_form_match.group(1)
    assert 'id="life-mc-build"' in mc_form_match.group(1)
    for btn_id in (
        "life-mc-connect",
        "life-mc-disconnect",
        "life-mc-pause",
        "life-mc-resume",
        "life-mc-idle",
        "life-mc-status-btn",
    ):
        assert f'id="{btn_id}"' in mc_html
        assert f'id="{btn_id}"' not in mc_form_match.group(1)

    find_js_engine("No local JS engine (node/jsc) available for Telegram UI runtime test")

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    adm_js = (ADMIN_DIR / "src" / "js" / "04-admission-memory.js").read_text(encoding="utf-8")
    all_ids_json = json.dumps(sorted(id_set))

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
        this.options = [];
        this.classList = {{ add(){{}}, remove(){{}}, toggle(){{}}, contains(){{ return false; }} }};
      }}
      get firstChild() {{ return this.children[0] || null; }}
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
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt, extra={{}}) {{
        let stopped = false;
        const event = {{
          target: this,
          preventDefault(){{}},
          stopPropagation(){{ stopped = true; }},
          ...extra
        }};
        for (const fn of (this.listeners[evt] || [])) fn(event);
        if (!stopped && this.parentElement) {{
          this.parentElement.dispatchBubble(evt, event);
        }}
      }}
      dispatchBubble(evt, event) {{
        let stopped = false;
        const origStop = event.stopPropagation;
        event.stopPropagation = () => {{ stopped = true; origStop(); }};
        for (const fn of (this.listeners[evt] || [])) fn(event);
        if (!stopped && this.parentElement) {{
          this.parentElement.dispatchBubble(evt, event);
        }}
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest(sel) {{
        if (sel === 'section.tab') {{
          if (this.id && this.id.startsWith('tg-')) return {{ id: 'telegram' }};
          if (this.id && this.id.startsWith('admission-')) return {{ id: 'admission' }};
          return {{ id: 'group-policy' }};
        }}
        return null;
      }}
      showModal() {{ this.open = true; }}
      close() {{ this.open = false; }}
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
    // Nest tg-groups-list inside tg-config-form to verify bubbling isolation of inline selects.
    byId.get('tg-config-form').append(byId.get('tg-groups-list'));

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
    function diagnosticCell(v){{ return String(v||''); }}

    {core_js}
    token = 'test-token';
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};
    let nextConfirm = true;
    confirmDialog = async () => nextConfirm;

    const calls = [];
    let currentGroups = [
      {{ platform: 'onebot', group_id: '40000', mode: 'at_only' }},
      {{ platform: 'telegram', group_id: '-1001234567890', mode: 'smart_reply' }}
    ];
    let currentFilter = {{
      config: {{
        reply_threshold: 0.6,
        group_activity: {{ 'telegram:-1001234567890': 'natural' }}
      }},
      presets: {{ quiet: 0.75, natural: 0.55, active: 0.35 }}
    }};
    let currentTgCfg = {{
      platform: 'telegram',
      plugin_id: 'telegram',
      online: true,
      bot_id: '9101002957',
      bot_username: 'gemini_nyan_bot',
      owner_configured: true,
      source: 'admin',
      capabilities: [
        'voice', 'sticker', 'emoji_like', 'reply_anchor', 'rich_card', 'proactive_speak'
      ],
      config: {{
        allowed_groups: ['-1001234567890'],
        proactive_groups: ['-1001234567890'],
        private_mode: 'owner_only',
        allowed_users: [],
        memory_mirror_enabled: false
      }},
      environment: {{
        allowed_groups: [],
        proactive_groups: [],
        private_mode: 'owner_only',
        allowed_users: [],
        memory_mirror_enabled: false
      }}
    }};

    api = async (path, method='GET', data) => {{
      calls.push({{ path, method, data }});
      if (path === 'groups' && method === 'GET') return currentGroups;
      if (path === 'groups' && method === 'PUT') return {{ status: 'saved' }};
      if (path === 'filter' && method === 'GET') return currentFilter;
      if (path === 'filter/group-activity' && method === 'POST') return {{ status: 'saved' }};
      if (path === 'social-chat/telegram/config' && method === 'GET') return currentTgCfg;
      if (path === 'social-chat/telegram/config' && method === 'PUT') {{
        currentTgCfg = {{ ...currentTgCfg, source: 'admin', config: data, pushed: true }};
        currentGroups = [
          {{ platform: 'onebot', group_id: '40000', mode: 'at_only' }},
          ...data.allowed_groups.map(gid => ({{
            platform: 'telegram', group_id: gid, mode: 'at_only'
          }}))
        ];
        return currentTgCfg;
      }}
      if (path === 'social-chat/telegram/config' && method === 'DELETE') {{
        currentTgCfg = {{
          ...currentTgCfg,
          source: 'env',
          config: {{ ...currentTgCfg.environment }},
          pushed: true
        }};
        currentGroups = [{{ platform: 'onebot', group_id: '40000', mode: 'at_only' }}];
        return currentTgCfg;
      }}
      if (path === 'admission' && method === 'GET') {{
        return {{
          source: 'admin',
          owner_qq_id: '20000',
          onebot_self_id: '10000',
          config: {{
            allowed_groups: ['40000'],
            allowed_users: [],
            ignored_bot_ids: ['60000', '60001'],
            private_mode: 'owner_only',
            bot_reply_threshold: 0.85,
            bot_reply_thresholds: {{ '60000': 0.92 }}
          }},
          environment: {{}}
        }};
      }}
      if (path === 'bots' && method === 'GET') {{
        return [
          {{
            user_id: '60000',
            custom_threshold: 0.92,
            effective_threshold: 0.92,
            groups: [{{
              group_id: '40000', used: 2, max_turns: 2, is_target: true,
              cooldown_seconds: 120, window_seconds: 120
            }}]
          }},
          {{
            user_id: '60001',
            custom_threshold: null,
            effective_threshold: 0.85,
            groups: []
          }}
        ];
      }}
      if (path === 'sessions' && method === 'GET') {{
        return [{{
          id: 'sess-tg-1',
          platform: 'telegram',
          kind: 'group',
          conversation_id: '-1001234567890',
          enabled: true,
          recent_count: 3,
          dual_ready: true,
          dual_entries_count: 2,
          dual_chars: 120
        }}];
      }}
      if (path === 'sessions/sess-tg-1' && method === 'GET') {{
        return {{
          version: 1, recent_count: 3, enabled: true, dual_ready: true,
          dual_entries_count: 2, dual_chars: 120, rolling_summary: ''
        }};
      }}
      if (path === 'sessions/sess-tg-1/memory/search' && method === 'POST') {{
        return {{
          ready: true, version: 1, jobs: {{}}, entries: [],
          mirror_enabled: false, mirror: {{ last_success: 0 }}
        }};
      }}
      return {{}};
    }};

    {adm_js}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. groups() renders ONLY QQ groups in #groups and excludes Telegram groups
      await groups();
      const grpTxt = $('groups').textContent;
      assert(grpTxt.includes('QQ') && grpTxt.includes('40000'), 'QQ group badge missing');
      assert(
        !grpTxt.includes('-1001234567890') && !grpTxt.includes('Telegram'),
        'QQ #groups table must not include Telegram groups: ' + grpTxt
      );

      // 2. telegramSocialConfig() renders status, proactive_speak label, and inline policy selects
      await telegramSocialConfig();
      const pillsTxt = $('tg-status-pills').textContent;
      assert(
        pillsTxt.includes('在线')
          && pillsTxt.includes('@gemini_nyan_bot')
          && pillsTxt.includes('Owner 已绑定')
          && pillsTxt.includes('控制台覆盖'),
        'tg-status-pills mismatch: ' + pillsTxt
      );
      const capsTxt = $('tg-capabilities-row').textContent;
      assert(
        capsTxt.includes('语音')
          && capsTxt.includes('表情包')
          && capsTxt.includes('贴表情')
          && capsTxt.includes('代码卡片')
          && capsTxt.includes('主动冒泡'),
        'tg-capabilities-row mismatch (must translate proactive_speak): ' + capsTxt
      );
      assert($('tg-memory-mirror').checked === false, 'memory mirror defaults false');

      // Saved Telegram group has inline trigger & activity selects inside .tg-item-controls
      const tgControls = walk($('tg-groups-list')).filter(
        n => n.className === 'tg-item-controls'
      );
      assert(tgControls.length === 1, 'expected 1 .tg-item-controls for saved group');
      const modeSelects = walk($('tg-groups-list')).filter(
        n => n.tagName === 'SELECT' && n.dataset.tgPolicy === 'mode'
      );
      const actSelects = walk($('tg-groups-list')).filter(
        n => n.tagName === 'SELECT' && n.dataset.tgPolicy === 'activity'
      );
      assert(
        modeSelects.length === 1 && modeSelects[0].className === 'table-select',
        'mode select'
      );
      assert(modeSelects[0].value === 'smart_reply', 'initial mode must reflect cached groups');
      assert(actSelects.length === 1 && actSelects[0].className === 'table-select', 'act select');
      assert(actSelects[0].value === 'natural', 'initial activity must reflect cached filter');

      // Changing inline mode & activity saves immediately and does NOT dirty #tg-config-form
      calls.length = 0;
      modeSelects[0].value = 'muted';
      modeSelects[0].dispatch('change');
      actSelects[0].value = 'active';
      actSelects[0].dispatch('change');
      await Promise.all(pendingTasks.splice(0));
      assert(
        calls.some(
          c => c.path === 'groups'
            && c.method === 'PUT'
            && c.data.platform === 'telegram'
            && c.data.group_id === '-1001234567890'
            && c.data.mode === 'muted'
        ),
        'inline TG mode select must PUT groups'
      );
      assert(
        calls.some(
          c => c.path === 'filter/group-activity'
            && c.method === 'POST'
            && c.data.platform === 'telegram'
            && c.data.group_id === '-1001234567890'
            && c.data.preset === 'active'
        ),
        'inline TG activity select must POST filter/group-activity'
      );
      assert(
        dirtyRegistry.get($('tg-config-form')).dirty === false,
        'immediate inline selects must NOT mark tg-config-form dirty'
      );

      // 3. Reject invalid group ID without mutating draft
      $('tg-group-input').value = 'not-a-number';
      $('tg-add-group').dispatch('click');
      assert($('notice-text').textContent.includes('Telegram 群号'), 'invalid group notice');
      assert(tgSocialDraft.allowed_groups.length === 1, 'invalid group must not be added');

      // 4. Add a new draft TG group -> shows unsaved hint until saved, then gets inline selects
      $('tg-group-input').value = '-1009988776655';
      $('tg-add-group').dispatch('click');
      assert(tgSocialDraft.allowed_groups.includes('-1009988776655'), 'new group not added');
      assert(
        $('tg-groups-list').textContent.includes('保存后可调触发策略与活跃度'),
        'unsaved draft group must show save-first hint'
      );
      const proCheckboxes = walk($('tg-groups-list')).filter(
        n => n.tagName === 'INPUT' && n.type === 'checkbox'
      );
      assert(proCheckboxes.length === 2, 'expected 2 proactive checkboxes');
      proCheckboxes[1].checked = true;
      proCheckboxes[1].dispatch('change');

      $('tg-private-mode').value = 'owner_and_allowlist';
      $('tg-private-mode').dispatch('change');
      $('tg-user-input').value = '555666777';
      $('tg-add-user').dispatch('click');
      $('tg-memory-mirror').checked = true;
      $('tg-memory-mirror').dispatch('change');

      calls.length = 0;
      $('tg-config-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      const putCall = calls.find(
        c => c.path === 'social-chat/telegram/config' && c.method === 'PUT'
      );
      assert(Boolean(putCall), 'expected PUT social-chat/telegram/config');
      assert(
        JSON.stringify(putCall.data.allowed_groups)
          === JSON.stringify(['-1001234567890', '-1009988776655']),
        'PUT allowed_groups mismatch'
      );
      assert(
        JSON.stringify(putCall.data.proactive_groups)
          === JSON.stringify(['-1001234567890', '-1009988776655']),
        'PUT proactive_groups mismatch'
      );
      assert(putCall.data.private_mode === 'owner_and_allowlist', 'PUT private_mode mismatch');
      assert(
        JSON.stringify(putCall.data.allowed_users) === JSON.stringify(['555666777']),
        'PUT allowed_users mismatch'
      );
      assert(putCall.data.memory_mirror_enabled === true, 'PUT memory_mirror_enabled mismatch');
      assert(
        calls.some(c => c.path === 'groups' && c.method === 'GET'),
        'PUT must automatically refresh groups'
      );
      assert(
        !$('groups').textContent.includes('-1009988776655'),
        'QQ #groups table must continue excluding Telegram groups after save'
      );
      const savedModeSelects = walk($('tg-groups-list')).filter(
        n => n.tagName === 'SELECT' && n.dataset.tgPolicy === 'mode'
      );
      assert(savedModeSelects.length === 2, 'both saved TG groups must now expose inline selects');
      assert($('notice-text').textContent.includes('实时热推送'), 'pushed notice mismatch');

      // 5. admission() merges bot cooldown column into #admission-bots
      await admission();
      const admBotsTxt = $('admission-bots').textContent;
      assert(admBotsTxt.includes('群冷却'), '#admission-bots must include 群冷却 column');
      assert(
        admBotsTxt.includes('40000: 2 / 2轮') && admBotsTxt.includes('冷却 120s'),
        '#admission-bots must display bot group cooldown: ' + admBotsTxt
      );
      assert(admBotsTxt.includes('无冷却'), '#admission-bots must display 无冷却 fallback');

      // 6. sessions() renders platform badge + openSession() prefixes platform in dialog title
      await sessions();
      const sessNodes = walk($('sessions'));
      assert(
        sessNodes.some(n => n.className === 'platform-cell'),
        'sessions() must render .platform-cell with badge'
      );
      assert(
        $('sessions').textContent.includes('Telegram')
          && $('sessions').textContent.includes('group:-1001234567890'),
        'sessions() must display platform badge and conversation id'
      );
      await openSession({{
        id: 'sess-tg-1', platform: 'telegram', kind: 'group', conversation_id: '-1001234567890'
      }});
      assert(
        $('session-title').textContent === 'telegram · group:-1001234567890',
        'openSession must include platform prefix: ' + $('session-title').textContent
      );

      // 7. Reset to .env respects confirmDialog cancel and confirm -> DELETE + refresh groups
      calls.length = 0;
      nextConfirm = false;
      $('tg-reset-config').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(calls.length === 0, 'cancelled reset must not invoke api()');

      nextConfirm = true;
      $('tg-reset-config').dispatch('click');
      await Promise.all(pendingTasks.splice(0));
      assert(
        calls.some(c => c.path === 'social-chat/telegram/config' && c.method === 'DELETE'),
        'expected DELETE social-chat/telegram/config'
      );
      assert($('tg-status-pills').textContent.includes('环境变量'), 'source must switch to env');
      assert($('tg-memory-mirror').checked === false, 'reset must turn mirror off');

      // 8. Saved TG group absent from lastGroupsItems does not duplicate on consecutive mode edits
      lastGroupsItems = [{{ platform: 'onebot', group_id: '40000', mode: 'at_only' }}];
      currentTgCfg = {{
        ...currentTgCfg,
        config: {{ ...currentTgCfg.config, allowed_groups: ['-1007777'], proactive_groups: [] }}
      }};
      renderTelegramSocialConfig(currentTgCfg);
      const uncachedMode = walk($('tg-groups-list')).find(
        n => n.tagName === 'SELECT' && n.dataset.tgPolicy === 'mode'
      );
      assert(Boolean(uncachedMode), 'saved group absent from cache must still show mode select');
      uncachedMode.value = 'smart_reply';
      uncachedMode.dispatch('change');
      await Promise.all(pendingTasks.splice(0));
      uncachedMode.value = 'muted';
      uncachedMode.dispatch('change');
      await Promise.all(pendingTasks.splice(0));
      const cachedTgEntries = lastGroupsItems.filter(
        g => g.platform === 'telegram' && g.group_id === '-1007777'
      );
      assert(
        cachedTgEntries.length === 1 && cachedTgEntries[0].mode === 'muted',
        'consecutive edits must update cached item in place without duplicates'
      );

      // 9. Empty QQ groups renders helpful empty-state pointing to admission
      currentGroups = [{{ platform: 'telegram', group_id: '-1001234567890', mode: 'at_only' }}];
      await groups();
      assert(
        $('groups').textContent.includes('准入与防互刷'),
        'empty QQ groups table must point to 准入与防互刷'
      );

      // 10. Logout clears Telegram UI state, caches, and disables buttons
      for (const fn of logoutHooks) fn();
      assert($('tg-save-config').disabled === true, 'logout must disable tg-save-config');
      assert($('tg-reset-config').disabled === true, 'logout must disable tg-reset-config');
      assert($('tg-groups-list').children.length === 0, 'logout must clear tg-groups-list');
      assert(lastGroupsItems.length === 0 && lastFilterData === null, 'logout must clear caches');

      if (typeof print === 'function') print('TG_SOCIAL_UI_OK');
      else console.log('TG_SOCIAL_UI_OK');
    }})().catch(err => {{
      console.error(err && err.stack ? err.stack : err);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Telegram UI JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "TG_SOCIAL_UI_OK" in proc.stdout, (
        f"Async JS test did not reach completion sentinel:\n{proc.stderr}\n{proc.stdout}"
    )


def test_admin_social_arch_plugins_telegram_offline_and_discard_js_runtime() -> None:
    """Verify login plugin pre-warm, #plugin-* deep link, Telegram offline fallback, and discard."""
    find_js_engine("No local JS engine (node/jsc) available for shell/UX JS runtime test")
    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids_json = json.dumps(sorted(set(collector.ids)))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    routing_js = (ADMIN_DIR / "src" / "js" / "03-routing-reply.js").read_text(encoding="utf-8")
    shell_js = (ADMIN_DIR / "src" / "js" / "08-modules-shell.js").read_text(encoding="utf-8")
    ux_js = (ADMIN_DIR / "src" / "js" / "09-ux.js").read_text(encoding="utf-8")

    # Slice 08-modules-shell.js helpers: module tools, registerPluginPage, telegramTab,
    # moduleDirectory, navigation, and login-form.
    tools_start = shell_js.index("function renderModuleActions(")
    tools_end = shell_js.index("const colabErrorLabels=")
    tools_slice = shell_js[tools_start:tools_end]

    nav_start = shell_js.index("const panelLoads=new WeakMap();")
    nav_end = shell_js.index("$('logout').addEventListener('click',logout);")
    nav_slice = shell_js[nav_start:nav_end]

    harness = f"""
    const ALL_IDS = {all_ids_json};
    class DOMException extends Error {{
      constructor(msg, name='Error') {{ super(msg); this.name = name; }}
    }}
    class MockNode {{
      constructor(tag) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.nodeType = tag === '#text' ? 3 : 1;
        this.children = [];
        this.childNodes = this.children;
        this.parentElement = null;
        this.parentNode = null;
        this.attrs = {{}};
        this.dataset = {{}};
        this.listeners = {{}};
        this._text = '';
        this.data = '';
        this.hidden = false;
        this.disabled = false;
        this.checked = false;
        this.inert = false;
        this.open = false;
        this.value = '';
        this.className = '';
        this.tabIndex = 0;
        this.options = [];
        this.selectedOptions = [];
        this.selectedIndex = 0;
        this.valueAsNumber = 0;
        this.content = {{ firstElementChild: null }};
        this._classes = new Set();
        this.classList = {{
          add: (...cs) => {{
            for (const c of cs) this._classes.add(c);
            this.className = [...this._classes].join(' ');
          }},
          remove: (...cs) => {{
            for (const c of cs) this._classes.delete(c);
            this.className = [...this._classes].join(' ');
          }},
          toggle: (c, f) => {{
            const on = f === undefined ? !this._classes.has(c) : Boolean(f);
            if (on) this._classes.add(c); else this._classes.delete(c);
            this.className = [...this._classes].join(' ');
            return on;
          }},
          contains: c => this._classes.has(c)
        }};
      }}
      get firstChild() {{ return this.children[0] || null; }}
      get firstElementChild() {{ return this.children.find(c => c.nodeType === 1) || null; }}
      get lastElementChild() {{
        for (let i = this.children.length - 1; i >= 0; i--) {{
          if (this.children[i].nodeType === 1) return this.children[i];
        }}
        return null;
      }}
      get textContent() {{
        if (this.nodeType === 3) return this.data;
        return this.children.length
          ? this._text + this.children.map(c => c.textContent).join('')
          : this._text;
      }}
      set textContent(v) {{
        const s = String(v ?? '');
        if (this.nodeType === 3) {{ this.data = s; this._text = s; return; }}
        this._text = s;
        this.children.length = 0;
      }}
      setAttribute(k, v) {{
        if (k === 'class') {{
          this.className = String(v);
          this._classes = new Set(String(v).split(/\\s+/).filter(Boolean));
        }}
        this.attrs[k] = String(v);
      }}
      getAttribute(k) {{ return this.attrs[k] ?? null; }}
      hasAttribute(k) {{ return k in this.attrs; }}
      removeAttribute(k) {{ delete this.attrs[k]; }}
      append(...nodes) {{
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            n.parentElement = this;
            n.parentNode = this;
            this.children.push(n);
            if (n.tagName === 'OPTION') this.options.push(n);
          }} else {{
            const t = new MockNode('#text');
            t.data = String(n); t._text = String(n);
            t.parentElement = this; t.parentNode = this;
            this.children.push(t);
          }}
        }}
      }}
      prepend(...nodes) {{
        const prev = [...this.children];
        this.children.length = 0;
        this.append(...nodes);
        for (const n of prev) this.children.push(n);
      }}
      after(...nodes) {{
        if (!this.parentNode) return;
        const arr = this.parentNode.children;
        const idx = arr.indexOf(this);
        if (idx < 0) return;
        for (const n of nodes) {{
          if (n instanceof MockNode) {{
            n.parentElement = this.parentNode;
            n.parentNode = this.parentNode;
            if (n.id) byId.set(n.id, n);
          }}
        }}
        arr.splice(idx + 1, 0, ...nodes);
      }}
      replaceWith(next) {{
        if (!this.parentNode) return;
        const arr = this.parentNode.children;
        const idx = arr.indexOf(this);
        if (idx < 0) return;
        next.parentElement = this.parentNode;
        next.parentNode = this.parentNode;
        arr.splice(idx, 1, next);
        this.parentNode = null;
        this.parentElement = null;
      }}
      replaceChildren(...nodes) {{
        this.children.length = 0;
        this.options = [];
        this._text = '';
        this.append(...nodes);
      }}
      remove() {{
        if (!this.parentNode) return;
        const idx = this.parentNode.children.indexOf(this);
        if (idx >= 0) this.parentNode.children.splice(idx, 1);
        this.parentNode = null;
        this.parentElement = null;
      }}
      cloneNode(deep=false) {{
        const c = new MockNode(this.tagName);
        c.className = this.className;
        c.attrs = {{ ...this.attrs }};
        c.dataset = {{ ...this.dataset }};
        c._text = this._text;
        if (deep) {{
          for (const ch of this.children) c.append(ch.cloneNode(true));
        }}
        return c;
      }}
      compareDocumentPosition(other) {{
        if (!this.parentNode || this.parentNode !== other.parentNode) return 0;
        const a = this.parentNode.children.indexOf(this);
        const b = this.parentNode.children.indexOf(other);
        return a < b ? 4 : 2;
      }}
      reset() {{}}
      add(opt) {{ this.append(opt); }}
      matches(sel) {{
        const hasCls = c => this.className.split(/\\s+/).includes(c);
        for (const part of sel.split(',').map(s => s.trim())) {{
          if (part === this.tagName.toLowerCase()) return true;
          if (part === 'section.tab' && this.tagName === 'SECTION' && hasCls('tab')) return true;
          if (
            part === 'section.module-schedule'
            && this.tagName === 'SECTION'
            && hasCls('module-schedule')
          ) return true;
          if (part === '[data-module]' && 'module' in this.dataset) return true;
          if (part === '[data-module-tools]' && 'moduleTools' in this.dataset) return true;
          if (
            part === 'input[data-module-switch]'
            && this.tagName === 'INPUT'
            && 'moduleSwitch' in this.dataset
          ) return true;
          if (
            part === 'p.muted:not([id])'
            && this.tagName === 'P'
            && hasCls('muted')
            && !this.id
          ) return true;
          if (part.startsWith('.') && hasCls(part.slice(1))) return true;
        }}
        return false;
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt, extra={{}}) {{
        const ev = {{
          target: this, currentTarget: this, preventDefault(){{}}, stopPropagation(){{}}, ...extra
        }};
        for (const fn of (this.listeners[evt] || [])) fn(ev);
      }}
      querySelector(sel) {{ return this.querySelectorAll(sel)[0] || null; }}
      querySelectorAll(sel) {{
        const out = [];
        const walkNode = (node) => {{
          for (const c of node.children) {{
            if (c.nodeType !== 1) continue;
            if (sel === 'section.module-schedule form') {{
              if (c.tagName === 'FORM' && c.closest('section.module-schedule')) out.push(c);
            }} else if (sel === 'section.tab p.muted:not([id])') {{
              if (c.matches('p.muted:not([id])') && c.closest('section.tab')) out.push(c);
            }} else if (c.matches(sel)) {{
              out.push(c);
            }}
            walkNode(c);
          }}
        }};
        walkNode(this);
        return out;
      }}
      closest(sel) {{
        let cur = this;
        while (cur) {{
          if (cur.nodeType === 1 && cur.matches(sel)) return cur;
          cur = cur.parentElement;
        }}
        return null;
      }}
      focus() {{}}
      select() {{}}
      scrollIntoView() {{}}
    }}
    MockNode.DOCUMENT_POSITION_FOLLOWING = 4;
    const Node = MockNode;
    class Option extends MockNode {{
      constructor(label, value) {{
        super('option'); this._text = String(label); this.value = String(value);
      }}
    }}
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    // Build minimal DOM structure needed by shell, thinking, and modules
    const appEl = byId.get('app');
    const modulesSec = new MockNode('section');
    modulesSec.id = 'modules';
    modulesSec.className = 'tab';
    byId.set('modules', modulesSec);
    const tgSec = new MockNode('section');
    tgSec.id = 'telegram';
    tgSec.className = 'tab';
    byId.set('telegram', tgSec);
    tgSec.append(
      byId.get('telegram-actions'),
      byId.get('tg-privacy-note'),
      byId.get('tg-privacy-table')
    );
    const thinkingSec = new MockNode('section');
    thinkingSec.id = 'thinking';
    thinkingSec.className = 'tab';
    byId.set('thinking', thinkingSec);
    const thinkingForm = byId.get('thinking-policy');
    thinkingForm.tagName = 'FORM';
    const thinkingRow = new MockNode('div');
    thinkingRow.className = 'row';
    thinkingForm.append(thinkingRow);
    const genCapsForm = byId.get('gen-caps-form');
    genCapsForm.tagName = 'FORM';
    thinkingSec.append(thinkingForm, genCapsForm);
    appEl.append(modulesSec, tgSec, thinkingSec);

    const modulesNav = byId.get('modules-nav');
    const platNavGroup = byId.get('platform-nav-group');
    const qqGrp = new MockNode('div');
    qqGrp.dataset.module = 'qq_social';
    const tgGrp = new MockNode('div');
    tgGrp.dataset.module = 'telegram';
    platNavGroup.append(qqGrp, tgGrp);
    modulesNav.append(platNavGroup);

    const navModulesBtn = byId.get('nav-modules');
    navModulesBtn.append(new MockNode('svg'), new MockNode('span'));

    const primarySel = byId.get('primary-model');
    primarySel.tagName = 'SELECT';
    const thinkingAllSel = byId.get('thinking-all');
    thinkingAllSel.tagName = 'SELECT';
    const optMed = new MockNode('option');
    optMed.value = 'medium';
    thinkingAllSel.append(optMed);
    thinkingAllSel.selectedOptions = [optMed];

    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{
        if (byId.has(id)) return byId.get(id);
        for (const n of walk(appEl)) if (n.id === id) return n;
        return null;
      }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{
        const n = new MockNode('#text'); n.data = String(t); n._text = String(t); return n;
      }},
      addEventListener() {{}},
      querySelectorAll(sel) {{
        if (sel === '#app section.tab') return [];
        if (sel === '#app form') return [];
        return [];
      }}
    }};
    const window = {{
      addEventListener(){{}}, scrollTo(){{}}, matchMedia: () => ({{ matches: false }})
    }};
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function setInterval(){{ return 1; }}
    function clearInterval(){{}}
    const location = {{ hash: '#plugin-song_finder', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function parseBotThreshold(v){{ return Number(v); }}
    function updateConnection(){{}}
    function renderMinecraft(){{}}
    function showWeatherLocation(){{}}
    const mcControls = [];
    const regionName = c => c;
    const modulePage = new Map([['qq_social', 'qq'], ['telegram', 'telegram']]);
    const pluginPages = new Map();
    const sections = new Map([
      ['overview', {{ workspace: 'core', title: '运行概览', button: new MockNode('button') }}],
      [
        'telegram',
        {{ workspace: 'modules', module: 'telegram', title: 'Telegram 社交',
          button: new MockNode('button') }}
      ],
      ['thinking', {{ workspace: 'core', title: '模型与思考', button: new MockNode('button') }}]
    ]);
    const tabButtons = [];
    const finderEntries = [];
    const sectionLocation = s => s.title;
    function closeNavigation(){{}}
    function selectTab(k){{ tab = k; updatePageDirtyBadge(); }}
    function syncHash(k){{ location.hash = '#' + k; }}
    function revealSectionFor(){{}}
    let cachedProvidersOverview = null;

    {core_js}
    confirmDialog = async () => true;

    const apiCalls = [];
    let tgOnline = false;
    let listedModules = [
      {{ id: 'qq_social', title: 'QQ 社交', region: 'KR', action_count: 1, enabled: true }},
      {{ id: 'song_finder', title: '找歌插件', region: 'CN', action_count: 2, enabled: true }},
      {{ id: 'web_tools', title: '联网工具', region: 'KR', action_count: 1, enabled: true }},
      {{ id: 'doc_render', title: '文档渲染', region: 'KR', action_count: 1, enabled: true }}
    ];
    let telegramConfigCalls = 0, groupsCalls = 0;
    const dashboardCalls = [];
    const rawDeferred = [];
    let deferRawModules = false;
    async function telegramSocialConfig(){{ telegramConfigCalls++; }}
    async function groups(){{ groupsCalls++; }}
    async function fetchOverviewPayload(){{ return {{ ok: true }}; }}
    async function dashboard(data, opts={{}}){{ dashboardCalls.push({{ data, opts }}); }}
    apiRaw = (path, method='GET', body, opts={{}}) => {{
      const sig = opts.signal || activeRequestSignal(method);
      apiCalls.push({{ path, method, raw: true, sig }});
      if (path === 'life/modules' && deferRawModules) {{
        return new Promise((resolve, reject) => {{
          rawDeferred.push({{
            sig,
            resolve: list => resolve({{ json: async () => list }}),
            reject
          }});
        }});
      }}
      if (path === 'life/modules') {{
        return Promise.resolve({{ json: async () => listedModules }});
      }}
      return Promise.resolve({{ json: async () => ({{}}) }});
    }};

    api = async (path, method='GET', data) => {{
      apiCalls.push({{ path, method, data }});
      if (path === 'life/modules') return listedModules;
      if (path === 'life/modules/telegram') {{
        if (!tgOnline) throw Error('模块当前未注册，请刷新目录。');
        return {{
          id: 'telegram', title: 'Telegram 社交', region: 'KR', enabled: true,
          actions: [{{
            name: 'telegram.ping', description: 'ping',
            audience: 'internal', available: true, mode: 'active'
          }}],
          scheduling: {{ fields: {{}}, values: {{}}, timers: [] }}
        }};
      }}
      if (path === 'life/modules/telegram/privacy') {{
        if (!tgOnline) throw Error('chat_module_not_found');
        return [];
      }}
      if (path === 'life/modules/song_finder') {{
        return {{
          id: 'song_finder', title: '找歌插件', region: 'CN', enabled: true,
          actions: [{{
            name: 'song_finder.search', description: '找歌',
            audience: 'internal', available: true, mode: 'active'
          }}],
          scheduling: {{
            fields: {{
              cooldown: {{ title: '冷却秒数', type: 'integer', minimum: 10, maximum: 600 }}
            }},
            values: {{ cooldown: 60 }},
            timers: []
          }}
        }};
      }}
      if (path === 'models') {{
        return {{
          primary_model: 'hydrogen',
          source: 'env',
          models: {{ hydrogen: 'Hydrogen', neon: 'Neon' }},
          model_levels: {{
            hydrogen: {{ low: 'low', medium: 'medium', high: 'high', xhigh: 'high' }},
            neon: {{ low: 'low', medium: 'medium', high: 'high', xhigh: 'xhigh' }}
          }},
          groups: [{{
            label: '核心对话',
            scenes: [{{
              key: 'chat', label: '群聊回复', value: 'medium', default: 'medium',
              levels: ['low', 'medium', 'high']
            }}]
          }}]
        }};
      }}
      if (path === 'generation-caps') {{
        return {{
          source: 'env',
          fields: {{ chat: {{ title: '聊天上限', minimum: 256, maximum: 4096 }} }},
          config: {{ chat: 1024 }}
        }};
      }}
      return {{}};
    }};

    const tabLoaders = {{
      overview: () => Promise.resolve(),
      thinking: () => Promise.all([thinking(), generationCaps()])
    }};
    function refreshTab(k=tab){{ return tabLoaders[k](); }}

    {ux_js}
    {routing_js}
    {tools_slice}
    tabLoaders.telegram = telegramTab;
    {nav_slice}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}
    async function flush() {{ for (let i = 0; i < 12; i++) await Promise.resolve(); }}

    (async () => {{
      // 1. SECTION_GROUP_LABELS includes #songs secondary tab keys
      assert(SECTION_GROUP_LABELS.song_list === '歌单与试听', 'song_list label');
      assert(SECTION_GROUP_LABELS.song_inbox === '找歌与收件箱', 'song_inbox label');
      assert(SECTION_GROUP_LABELS.song_learn === 'Colab 学歌', 'song_learn label');
      assert(SECTION_GROUP_LABELS.song_upload === '本地上传与目录', 'song_upload label');

      // 2. Login with #plugin-song_finder deep link awaits moduleDirectory() and navigates cleanly
      $('token').value = 'valid-token';
      location.hash = '#plugin-song_finder';
      $('login-form').dispatch('submit');
      await flush();
      assert(tab === 'plugin-song_finder', 'must land on plugin-song_finder, got ' + tab);
      assert(sections.has('plugin-song_finder'), 'song_finder registered');
      assert(sections.has('plugin-web_tools'), 'web_tools registered');
      assert(sections.has('plugin-doc_render'), 'doc_render registered');
      assert(tgGrp.hidden === false, 'telegram nav group must remain visible even when offline');

      // Tool plugin shortcuts
      const sfBtns = walk($('plugin-song_finder')).filter(
        n => n.dataset?.openTab === 'songs'
      );
      const wtBtns = walk($('plugin-web_tools')).filter(
        n => n.dataset?.openTab === 'reply-settings'
      );
      const drBtns = walk($('plugin-doc_render')).filter(
        n => n.dataset?.openTab === 'reply-settings'
      );
      assert(sfBtns.length === 1, 'song_finder must link to #songs');
      assert(wtBtns.length === 1, 'web_tools must link to #reply-settings');
      assert(drBtns.length === 1, 'doc_render must link to #reply-settings');

      // renderModuleActions select uses .table-select & schedule form tracks dirty
      const sfSelects = walk($('plugin-song_finder-actions')).filter(
        n => n.tagName === 'SELECT'
      );
      assert(
        sfSelects.length === 1 && sfSelects[0].className === 'table-select',
        'action select class'
      );
      const schedForms = [...dirtyRegistry.values()].filter(e => e.tabId === 'plugin-song_finder');
      assert(schedForms.length === 1, 'schedule form must register in dirtyRegistry with tabId');

      // Reloading module cleans up old dirtyRegistry form entry (no leak)
      await loadModule('song_finder');
      const schedAfter = [...dirtyRegistry.values()].filter(e => e.tabId === 'plugin-song_finder');
      assert(schedAfter.length === 1, 're-render must not leak duplicate dirtyRegistry entries');

      // 3. Telegram plugin online does NOT register duplicate plugin-telegram page
      listedModules = [
        ...listedModules,
        {{ id: 'telegram', title: 'Telegram 社交', region: 'KR', action_count: 1, enabled: true }}
      ];
      await moduleDirectory();
      assert(!sections.has('plugin-telegram'), 'must not create duplicate plugin-telegram page');

      // 4. Telegram tab offline graceful fallback (no error toast!)
      hideNotice();
      tgOnline = false;
      await navigate('telegram');
      assert($('notice').hidden === true, 'Telegram offline must NOT trigger red error toast');
      assert(
        $('telegram-actions').textContent.includes('前往远程插件总线'),
        'offline #telegram-actions must link to plugin-bus'
      );
      assert(
        $('tg-privacy-table').textContent.includes('前往远程插件总线'),
        'offline #tg-privacy-table must link to plugin-bus'
      );

      // 5. #thinking dirty state & save-bar discard resets sceneEdits + thinkingDirty cleanly
      await navigate('thinking');
      sceneEdits.set('chat', 'high');
      thinkingChanged();
      assert(thinkingDirty === true && sceneEdits.size === 1, 'thinking must be dirty');
      assert($('save-bar').hidden === false, 'save-bar must appear for dirty #thinking');
      const discardBtn = walk($('save-bar-actions')).find(n => n.textContent === '放弃修改');
      assert(Boolean(discardBtn), 'save-bar must render discard button');
      discardBtn.dispatch('click');
      await flush();
      assert(thinkingDirty === false, 'discard must clear thinkingDirty');
      assert(sceneEdits.size === 0, 'discard must clear sceneEdits');
      assert($('thinking-edit-state').textContent === '', 'discard must clear edit-state text');
      assert($('save-bar').hidden === true, 'save-bar must hide after discard');

      // 6. #refresh-thinking uses confirmDiscardTabChanges('thinking', ...) for entire tab
      markDirty('gen-caps-form');
      let confirmCount = 0;
      const origConfirm = confirmDialog;
      confirmDialog = async () => {{ confirmCount++; return true; }};
      $('refresh-thinking').dispatch('click');
      await flush();
      confirmDialog = origConfirm;
      assert(confirmCount === 1, '#refresh-thinking must confirm dirty #gen-caps-form too');
      assert(isTabDirty('thinking') === false, '#refresh-thinking must clear tab dirty state');

      // 7. Static hash login (#overview) pre-warms via sessionRequests.signal so
      //    abortTabRequests() (user switching tabs mid-load) does NOT abort pre-warm!
      for (const remove of [...pluginPages.values()]) remove();
      assert(!sections.has('plugin-song_finder'), 'plugin pages cleared before static login');
      const staticTabBtn = new MockNode('button');
      tabButtons.push(staticTabBtn);
      deferRawModules = true;
      location.hash = '#overview';
      $('token').value = 'valid-token';
      $('login-form').dispatch('submit');
      await flush();
      assert(tab === 'overview', 'static login must land on overview');
      assert(rawDeferred.length === 1, 'static login must start background apiRaw pre-warm');
      const prewarmReq = rawDeferred.shift();
      assert(
        prewarmReq.sig === sessionRequests.signal,
        'background pre-warm must bind to sessionRequests.signal'
      );
      abortTabRequests();
      assert(prewarmReq.sig.aborted === false, 'abortTabRequests must NOT abort pre-warm');
      prewarmReq.resolve(listedModules);
      await flush();
      assert(
        sections.has('plugin-song_finder') && finderEntries.some(e => e.key === 'plugin-web_tools'),
        'pre-warm must register dynamic plugins even after abortTabRequests()'
      );

      // 8. Logout race while moduleDirectory({{background:true}}) is in-flight must NOT leak
      for (const remove of [...pluginPages.values()]) remove();
      const pendingLeak = moduleDirectory({{ background: true }});
      assert(rawDeferred.length === 1, 'expected in-flight moduleDirectory request');
      const leakReq = rawDeferred.shift();
      const prevBtnCount = tabButtons.length;
      token = '';
      sessionRequests.abort();
      sessionRequests = new AbortController();
      // Even if a stale promise resolves after logout, guard must prevent DOM/registry writes.
      leakReq.resolve(listedModules);
      await pendingLeak.catch(() => {{}});
      assert(!sections.has('plugin-song_finder'), 'logout race must not leak sections');
      assert(pluginPages.size === 0, 'logout race must not leak pluginPages');
      assert(
        !finderEntries.some(e => e.key === 'plugin-song_finder'),
        'logout race must not leak finderEntries'
      );
      assert(tabButtons.includes(staticTabBtn), 'static tab button must remain untouched');
      assert(tabButtons.length === prevBtnCount, 'tabButtons length must remain intact');

      // 9. Offline/nonexistent #plugin-* deep link falls back to #overview AND runs full dashboard
      deferRawModules = false;
      dashboardCalls.length = 0;
      location.hash = '#plugin-offline_ghost';
      $('token').value = 'valid-token';
      $('login-form').dispatch('submit');
      await flush();
      assert(tab === 'overview', 'offline #plugin-* deep link must fall back to overview');
      assert(location.hash === '#overview', 'offline #plugin-* must sync hash to #overview');
      assert(
        dashboardCalls.length === 2
          && dashboardCalls[0].opts?.lightweight === true
          && !dashboardCalls[1].opts?.lightweight,
        'offline #plugin-* fallback must run full dashboard(first) after lightweight auth: '
          + JSON.stringify(dashboardCalls)
      );

      if (typeof print === 'function') print('SOCIAL_ARCH_RUNTIME_OK');
      else console.log('SOCIAL_ARCH_RUNTIME_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, (
        f"Social-arch shell/UX JS test failed:\n{proc.stderr}\n{proc.stdout}"
    )
    assert "SOCIAL_ARCH_RUNTIME_OK" in proc.stdout, (
        f"Async JS test did not reach completion sentinel:\n{proc.stderr}\n{proc.stdout}"
    )
