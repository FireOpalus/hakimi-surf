"""Tests for Phase 3C (F10, F11, F10-T): /admin Social UI, APIs, & architecture.html."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from admin.build import _extract_js_id_refs, _IdCollector, audit_html, build_html
from admin.csp import content_security_policy
from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.social import OwnerBinding
from tests.fakes import FakeGateway
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
    jsc_path = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")
    runner: list[str] | None = None
    if shutil.which("node"):
        runner = ["node", "-e"]
    elif jsc_path.is_file():
        runner = [str(jsc_path), "-e"]
    if runner is None:
        pytest.skip("No local JS engine (node/jsc) available for admin JS runtime test")

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

    proc = subprocess.run(
        [*runner, harness],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
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
    """Verify Telegram hot-config card markup, API contract, and cross-platform group badges."""
    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy
    assert "style-src 'sha256-" in policy

    collector = _IdCollector()
    collector.feed(html)
    id_set = set(collector.ids)
    assert len(collector.ids) == len(id_set)

    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    gp_html = panels["group-policy"]
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
    ):
        assert f'id="{expected_id}"' in gp_html, f"Missing Telegram UI id: {expected_id}"
    assert "group_memory_mirror" in gp_html
    assert "Antigravity_Memory" in gp_html

    jsc_path = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")
    runner: list[str] | None = None
    if shutil.which("node"):
        runner = ["node", "-e"]
    elif jsc_path.is_file():
        runner = [str(jsc_path), "-e"]
    if runner is None:
        pytest.skip("No local JS engine (node/jsc) available for Telegram UI runtime test")

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
        for (const fn of (this.listeners[evt] || [])) {{
          fn({{ target: this, preventDefault(){{}}, ...extra }});
        }}
      }}
      querySelector() {{ return null; }}
      querySelectorAll() {{ return []; }}
      closest(sel) {{
        if (sel === 'section.tab') return {{ id: 'group-policy' }};
        return null;
      }}
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
    let currentTgCfg = {{
      platform: 'telegram',
      plugin_id: 'telegram',
      online: true,
      bot_id: '9101002957',
      bot_username: 'gemini_nyan_bot',
      owner_configured: true,
      source: 'admin',
      capabilities: ['voice', 'sticker', 'emoji_like', 'reply_anchor', 'rich_card'],
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
      if (path === 'filter' && method === 'GET') {{
        return {{
          config: {{ reply_threshold: 0.6, group_activity: {{}} }},
          presets: {{ quiet: 0.75, natural: 0.55, active: 0.35 }}
        }};
      }}
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
      // 1. groups() renders QQ and Telegram platform badges cleanly
      await groups();
      const grpTxt = $('groups').textContent;
      assert(grpTxt.includes('QQ') && grpTxt.includes('40000'), 'QQ group badge missing');
      assert(
        grpTxt.includes('Telegram') && grpTxt.includes('-1001234567890'),
        'Telegram group badge missing'
      );

      // 2. telegramSocialConfig() loads and populates status, caps, lists
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
          && capsTxt.includes('代码卡片'),
        'tg-capabilities-row mismatch: ' + capsTxt
      );
      assert($('tg-memory-mirror').checked === false, 'memory mirror defaults false');
      assert($('tg-groups-list').textContent.includes('-1001234567890'), 'initial group missing');

      // 3. Reject invalid group ID without mutating draft
      $('tg-group-input').value = 'not-a-number';
      $('tg-add-group').dispatch('click');
      assert($('notice-text').textContent.includes('Telegram 群号'), 'invalid group notice');
      assert(tgSocialDraft.allowed_groups.length === 1, 'invalid group must not be added');

      // 4. Add a new TG group, toggle proactive, add private user, enable mirror, submit PUT
      $('tg-group-input').value = '-1009988776655';
      $('tg-add-group').dispatch('click');
      assert(tgSocialDraft.allowed_groups.includes('-1009988776655'), 'new group not added');
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
        'PUT must automatically refresh groups table'
      );
      assert($('groups').textContent.includes('-1009988776655'), 'new TG group must appear');
      assert($('notice-text').textContent.includes('实时热推送'), 'pushed notice mismatch');

      // 5. Reset to .env respects confirmDialog cancel and confirm -> DELETE + refresh groups
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

      // 6. Logout clears Telegram UI state and disables buttons
      for (const fn of logoutHooks) fn();
      assert($('tg-save-config').disabled === true, 'logout must disable tg-save-config');
      assert($('tg-reset-config').disabled === true, 'logout must disable tg-reset-config');
      assert($('tg-groups-list').children.length === 0, 'logout must clear tg-groups-list');

      if (typeof print === 'function') print('TG_SOCIAL_UI_OK');
      else console.log('TG_SOCIAL_UI_OK');
    }})().catch(err => {{
      console.error(err && err.stack ? err.stack : err);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = subprocess.run(
        [*runner, harness],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"Telegram UI JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "TG_SOCIAL_UI_OK" in proc.stdout, (
        f"Async JS test did not reach completion sentinel:\n{proc.stderr}\n{proc.stdout}"
    )
