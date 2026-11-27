"""Wave 1+2 regression tests for Admin frontend/backend responsiveness, Nginx/Docker, & deploy."""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from admin.build import _IdCollector, audit_html, build_html
from hakimi.app import create_app
from hakimi.world_state import IntentTicketCreate
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_DIR = REPO_ROOT / "admin"
DEPLOY_SCRIPT = REPO_ROOT / "scripts" / "deploy_prod.sh"


def _js_runner() -> list[str]:
    jsc_path = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")
    if shutil.which("node"):
        return ["node", "-e"]
    if jsc_path.is_file():
        return [str(jsc_path), "-e"]
    pytest.skip("No local JS engine (node/jsc) available for Admin JS runtime test")


def test_deploy_prod_deploys_engine_before_admin_and_valid_bash():
    script = DEPLOY_SCRIPT.read_text(encoding="utf-8")
    proc = subprocess.run(
        ["bash", "-n", str(DEPLOY_SCRIPT)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout

    bot_match = re.search(
        r"^\s*if \[\[ \$mode != admin \]\]; then deploy hakimi-bot\b.*$", script, re.M
    )
    admin_match = re.search(
        r"^\s*if \[\[ \$mode != engine \]\]; then deploy hakimi-admin\b.*$", script, re.M
    )
    assert bot_match is not None, "missing hakimi-bot deploy invocation"
    assert admin_match is not None, "missing hakimi-admin deploy invocation"
    assert bot_match.start() < admin_match.start(), (
        "hakimi-bot (engine) must deploy and pass health checks BEFORE hakimi-admin"
    )


def test_admin_dockerfile_precompresses_html_and_nginx_ref_voices():
    dockerfile = (ADMIN_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert "gzip -9 -n -k admin_page.html" in dockerfile
    assert "COPY --from=page /build/admin_page.html /srv/admin/admin_page.html" in dockerfile
    assert "COPY --from=page /build/admin_page.html.gz /srv/admin/admin_page.html.gz" in dockerfile

    conf = (ADMIN_DIR / "nginx.conf").read_text(encoding="utf-8")
    assert re.search(r"^\s*gzip_static\s+on;", conf, re.M)
    assert 'add_header Cache-Control "no-store" always;' in conf
    m = re.search(r"location = /admin/api/songs/ref-voices\s*\{([^}]*)\}", conf, re.S)
    assert m is not None
    block = m.group(1)
    assert "client_max_body_size 25m;" in block
    assert "proxy_request_buffering off;" in block
    assert "proxy_pass http://engine/admin/api/songs/ref-voices;" in block
    assert conf.index("location = /admin/api/songs/ref-voices") < conf.index("location /admin/api/")


async def test_life_world_admin_parallel_sensors_to_thread_and_fast_mutations(
    settings, monkeypatch
):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    app = create_app(settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        active_mods = [m for m in svc.modules.all() if not m.inactive_reason]
        assert len(active_mods) >= 2, "Need at least 2 active modules to verify concurrency"

        # 1. Verify GET /admin/api/life awaits module sensors in parallel, not serially.
        entered = 0
        all_entered = asyncio.Event()
        sensor_calls: list[str] = []

        def _make_sensor(mod_id: str):
            async def _sensor() -> str:
                nonlocal entered
                sensor_calls.append(mod_id)
                entered += 1
                if entered >= len(active_mods):
                    all_entered.set()
                # Would time out if awaited serially!
                await asyncio.wait_for(all_entered.wait(), timeout=1.5)
                return f"summary:{mod_id}"

            return _sensor

        for mod in active_mods:
            monkeypatch.setattr(mod, "get_sensor_summary", _make_sensor(mod.module_id))

        # 2. Verify cloud_memory.latest_activity_info() runs off the event loop thread.
        loop_thread_id = threading.get_ident()
        observed_threads: list[int] = []

        class _SyncCloudMemory:
            def latest_activity_info(self) -> dict:
                observed_threads.append(threading.get_ident())
                return {"enabled": True, "fresh": True}

        svc.cloud_memory = _SyncCloudMemory()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            r_get = await client.get("/admin/api/life")
            assert r_get.status_code == 200
            data = r_get.json()
            assert len(sensor_calls) == len(active_mods)
            assert observed_threads and observed_threads[-1] != loop_thread_id
            assert data["desktop_memory"] == {"enabled": True, "fresh": True}
            by_id = {m["module_id"]: m["sensor_summary"] for m in data["world_state"]["modules"]}
            for mod in active_mods:
                assert by_id[mod.module_id] == f"summary:{mod.module_id}"

            # 3. Verify ALL Life mutation routes skip sensor RPCs (include_sensors=False).
            sensor_calls.clear()

            async def _forbid_sensor() -> str:
                sensor_calls.append("unexpected")
                return "should_not_be_called"

            for mod in active_mods:
                monkeypatch.setattr(mod, "get_sensor_summary", _forbid_sensor)

            r_put = await client.put("/admin/api/life", json={"max_proactive_per_day": 4})
            assert r_put.status_code == 200
            assert sensor_calls == []

            r_act = await client.put("/admin/api/life/actions/mc.status", json={"mode": "off"})
            assert r_act.status_code == 200
            assert sensor_calls == []

            r_sched_put = await client.put(
                "/admin/api/life/schedule", json={"max_proactive_per_day": 5}
            )
            assert r_sched_put.status_code == 200
            assert sensor_calls == []

            r_sched_del = await client.delete("/admin/api/life/schedule")
            assert r_sched_del.status_code == 200
            assert sensor_calls == []

            r_world = await client.put("/admin/api/life/world", json={"mood": "relaxed"})
            assert r_world.status_code == 200
            assert r_world.json()["world_state"]["mood"] == "relaxed"
            assert sensor_calls == []

            now = time.time()
            intent_id = await svc.world_state.enqueue_intent(
                IntentTicketCreate(
                    target_module="qq_social",
                    target_scope="onebot:40000",
                    action_type="group_share",
                    execute_after=now + 60,
                    expire_at=now + 600,
                    mood="calm",
                ),
                now=now,
            )
            r_cancel = await client.delete(f"/admin/api/life/intents/{intent_id}")
            assert r_cancel.status_code == 200
            assert sensor_calls == []

            r_del = await client.delete("/admin/api/life")
            assert r_del.status_code == 200
            assert sensor_calls == []


async def test_system_modules_endpoint_runs_status_in_parallel(settings, monkeypatch):
    settings.admin_token = SecretStr(TOKEN)
    app = create_app(settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        systems = list(svc.modules.systems.values())
        assert len(systems) >= 3

        entered = 0
        all_entered = asyncio.Event()

        def _make_status(mod_id: str):
            async def _status() -> dict:
                nonlocal entered
                entered += 1
                if entered >= len(systems):
                    all_entered.set()
                await asyncio.wait_for(all_entered.wait(), timeout=1.5)
                return {"detail": f"ok:{mod_id}"}

            return _status

        for mod in systems:
            monkeypatch.setattr(mod, "status", _make_status(mod.module_id))

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            res = await client.get("/admin/api/system-modules")
            assert res.status_code == 200
            rows = res.json()
            assert [r["id"] for r in rows] == [m.module_id for m in systems]
            assert all(r["status"]["detail"] == f"ok:{r['id']}" for r in rows)


def _mock_dom_prelude(all_ids: list[str]) -> str:
    all_ids_json = json.dumps(all_ids)
    return f"""
    const ALL_IDS = {all_ids_json};
    class DOMException extends Error {{
      constructor(msg, name='Error') {{ super(msg); this.name = name; }}
    }}
    class MockNode {{
      constructor(tag, ns) {{
        this.tagName = String(tag || 'div').toUpperCase();
        this.nodeType = tag === '#text' ? 3 : 1;
        this.namespaceURI = ns || null;
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
            t.data = String(n);
            t._text = String(n);
            t.parentElement = this;
            t.parentNode = this;
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
        const wrapped = nodes.map(n => {{
          if (n instanceof MockNode) {{
            n.parentElement = this.parentNode;
            n.parentNode = this.parentNode;
            return n;
          }}
          const t = new MockNode('#text');
          t.data = String(n); t._text = String(n);
          t.parentElement = this.parentNode; t.parentNode = this.parentNode;
          return t;
        }});
        arr.splice(idx + 1, 0, ...wrapped);
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
      compareDocumentPosition(other) {{
        if (!this.parentNode || this.parentNode !== other.parentNode) return 0;
        const a = this.parentNode.children.indexOf(this);
        const b = this.parentNode.children.indexOf(other);
        return a < b ? 4 : 2;
      }}
      matches(sel) {{
        for (const part of sel.split(',').map(s => s.trim())) {{
          if (part === this.tagName.toLowerCase()) return true;
          if (part.startsWith('.') && this.className.split(/\\s+/).includes(part.slice(1))) {{
            return true;
          }}
        }}
        return false;
      }}
      addEventListener(evt, fn) {{ (this.listeners[evt] ||= []).push(fn); }}
      dispatch(evt) {{
        for (const fn of (this.listeners[evt] || [])) fn({{ target: this, preventDefault(){{}} }});
      }}
      querySelector(sel) {{
        const all = this.querySelectorAll(sel);
        return all[0] || null;
      }}
      querySelectorAll(sel) {{
        const parts = sel.split(',').map(s => s.trim());
        const out = [];
        const walk = (node, directOnly) => {{
          for (const c of node.children) {{
            if (c.nodeType !== 1) continue;
            for (const p of parts) {{
              const clean = p.replace(/^:scope\\s*>\\s*/, '');
              if (c.matches(clean)) {{ out.push(c); break; }}
            }}
            if (!directOnly) walk(c, false);
          }}
        }};
        walk(this, parts.every(p => p.startsWith(':scope >')));
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
    const byId = new Map();
    for (const id of ALL_IDS) {{
      const n = new MockNode('div');
      n.id = id;
      byId.set(id, n);
    }}
    class AbortController {{
      constructor() {{ this.signal = {{ aborted: false }}; }}
      abort() {{ this.signal.aborted = true; }}
    }}
    """


def test_songs_and_voice_companion_non_blocking_js_runtime():
    """Verify #songs and #voice-companion render primary data without blocking on slow calls."""
    runner = _js_runner()
    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids = sorted(set(collector.ids))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")
    life_js = (ADMIN_DIR / "src" / "js" / "05-life-companion.js").read_text(encoding="utf-8")

    song_start = media_js.index("let songGeneration=0")
    song_end = media_js.index("$('song-inbox-upload').addEventListener")
    songs_slice = media_js[song_start:song_end]

    vc_start = life_js.index("function renderVoiceCompanion(d)")
    vc_end = life_js.index("$('refresh-vc').addEventListener")
    vc_slice = life_js[vc_start:vc_end]

    harness = f"""
    {_mock_dom_prelude(all_ids)}
    const cfgSourceLabels = {{ sqlite: 'SQLite', env: '.env', default: '默认' }};
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{
        const n = new MockNode('#text'); n.data = String(t); n._text = String(t); return n;
      }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function lifeFmt(v){{ return String(v || ''); }}
    function syncVoiceGatewayFolds(){{}}
    function renderTtsPreview(){{}}
    function clearTtsPreview(){{}}

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

    const deferred = {{}};
    api = (path, method='GET', data) => {{
      if (method === 'PUT' && path === 'life/voice-companion/gateway') {{
        return Promise.resolve({{
          configured: true,
          reachable: true,
          source: 'sqlite',
          gateway: {{
            stt_provider: data.stt_provider,
            tts_provider: data.tts_provider,
            tts_speed: data.tts_speed
          }},
          saved_keys: {{}}
        }});
      }}
      if (method === 'PUT' && path === 'songs/storage') {{
        return Promise.resolve({{
          configured: true,
          source: 'sqlite',
          drive_songs_folder_id: data.drive_songs_folder_id,
          folders: {{ root: data.drive_songs_folder_id }}
        }});
      }}
      return new Promise((resolve, reject) => {{
        (deferred[path] ||= []).push({{ resolve, reject }});
      }});
    }};

    {songs_slice}
    {vc_slice}
    trackDirty('vc-gw-form', '语音网关 API 配置');
    trackDirty('song-storage-form', '歌单云盘目录');

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}
    async function flush() {{ for (let i = 0; i < 8; i++) await Promise.resolve(); }}

    (async () => {{
      // 1. #songs renders song list immediately when api('songs') resolves while Drive is pending.
      const pSongs = songs();
      assert(deferred['songs'].length === 1, 'songs API must start');
      assert(deferred['songs/storage'].length === 1, 'songs/storage must start async');
      assert(deferred['songs/inbox'].length === 1, 'songs/inbox must start async');
      assert(deferred['songs/voices'].length === 1, 'songs/voices must start async');

      deferred['songs'].shift().resolve({{
        items: [{{
          id: 'song1', title: '星之咏叹', duration_ms: 45000, byte_size: 512000,
          drive_backed: true, cached_local: true, source_origin: 'colab'
        }}],
        drive_configured: true,
        colab_configured: true,
        drive_backed_count: 1,
        cached_count: 1,
        loaded_prompt_count: 1
      }});
      await pSongs;
      assert(
        $('song-list').textContent.includes('星之咏叹'),
        'song-list must render BEFORE Drive calls resolve'
      );
      assert(
        $('song-inbox-table').textContent.includes('正在异步加载'),
        'inbox should show non-blocking loading hint'
      );

      // Now resolve the 3 Drive cards asynchronously.
      deferred['songs/storage'].shift().resolve({{
        configured: true,
        source: 'sqlite',
        drive_songs_folder_id: 'root_xyz',
        folders: {{ root: 'root_xyz', models: 'm_1', songs: 's_1', inbox: 'i_1' }}
      }});
      deferred['songs/inbox'].shift().resolve({{
        items: [{{ id: 'in_1', name: '夜空中最亮的星.mp3', size: 2048000 }}]
      }});
      deferred['songs/voices'].shift().resolve({{
        default_ref_voice: 'hakimi_ref.wav',
        ref_voices: [{{ id: 'rv_1', name: 'hakimi_ref.wav', size: 512000, default: true }}],
        rvc_models: [{{ name: 'hakimi', has_index: true }}]
      }});
      await flush();
      assert($('song-drive-details').textContent.includes('m_1'), 'storage card populated');
      assert($('song-inbox-table').textContent.includes('夜空中最亮的星.mp3'), 'inbox populated');
      assert(
        $('song-learn-ref-voice').options.some(o => o.value === 'hakimi_ref.wav'),
        'voices populated'
      );

      // 1b. Saving song-storage-form invalidates a still-in-flight initial GET songs/storage.
      const pSongs2 = songs();
      deferred['songs'].shift().resolve({{ items: [], drive_configured: true }});
      await pSongs2;
      const staleStorageGet = deferred['songs/storage'].shift();
      $('song-drive-folder-id').value = 'fresh_saved_folder';
      $('song-storage-form').dispatch('submit');
      await flush();
      while ((deferred['songs/inbox'] || []).length) {{
        deferred['songs/inbox'].shift().resolve({{ items: [] }});
      }}
      while ((deferred['songs/voices'] || []).length) {{
        deferred['songs/voices'].shift().resolve({{ ref_voices: [], rvc_models: [] }});
      }}
      await Promise.all(pendingTasks.splice(0));
      assert(
        $('song-drive-folder-id').value === 'fresh_saved_folder',
        'saved folder id must apply'
      );
      staleStorageGet.resolve({{
        configured: true,
        source: 'sqlite',
        drive_songs_folder_id: 'stale_old_folder',
        folders: {{ root: 'stale_old_folder' }}
      }});
      await flush();
      assert(
        $('song-drive-folder-id').value === 'fresh_saved_folder',
        'stale GET songs/storage must NOT overwrite freshly saved folder id'
      );

      // 2. #voice-companion renders immediately when life/voice-companion resolves while gateway
      //    is still pending, then updates gateway card asynchronously.
      const pVc = voiceCompanion();
      assert(deferred['life/voice-companion'].length === 1, 'vc API must start');
      assert(deferred['life/voice-companion/gateway'].length === 1, 'gw API must start async');

      deferred['life/voice-companion'].shift().resolve({{
        online: true,
        adapter_id: 'desktop',
        source: 'sqlite',
        config: {{ enabled: true, use_screen_watch: true, save_recap_to_ledger: true }},
        sessions: []
      }});
      await pVc;
      assert(
        $('vc-pills').textContent.includes('伴随端在线'),
        'vc card must render BEFORE gateway resolves'
      );
      assert(
        $('vc-gw-pills').textContent.includes('正在读取语音网关配置'),
        'gateway card should show async loading pill'
      );

      // 2a. If user edits vc-gw-form before slow GET resolves, background GET preserves form!
      $('vc-gw-tts-speed').value = '1.75';
      markDirty('vc-gw-form');
      deferred['life/voice-companion/gateway'].shift().resolve({{
        configured: true,
        reachable: true,
        source: 'sqlite',
        gateway: {{
          stt_provider: 'siliconflow',
          tts_provider: 'breezeblue',
          tts_model: 'breeze-tts-2',
          tts_speed: 1.05
        }},
        saved_keys: {{ breezeblue: true }}
      }});
      await flush();
      assert(
        $('vc-gw-pills').textContent.includes('网关在线'),
        'gateway status pills must still populate asynchronously'
      );
      assert(
        $('vc-gw-tts-speed').value === '1.75',
        'background GET must NOT overwrite unsaved user edit in vc-gw-form'
      );
      assert(
        dirtyRegistry.get($('vc-gw-form')).dirty === true,
        'background GET must NOT clear dirty state on vc-gw-form'
      );

      // 2b. Saving vc-gw-form (PUT) bumps vcGwFetchGen so a stale in-flight GET cannot overwrite.
      clearDirty('vc-gw-form');
      const pVc2 = voiceCompanion();
      deferred['life/voice-companion'].shift().resolve({{
        online: true, adapter_id: 'desktop', source: 'sqlite', config: {{}}, sessions: []
      }});
      await pVc2;
      const staleGwGet = deferred['life/voice-companion/gateway'].shift();
      $('vc-gw-stt-provider').value = 'openai';
      $('vc-gw-tts-provider').value = 'edge';
      $('vc-gw-tts-speed').value = '1.4';
      $('vc-gw-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert($('vc-gw-tts-speed').value === '1.4', 'PUT gateway must render saved speed');
      staleGwGet.resolve({{
        configured: true,
        reachable: true,
        source: 'env',
        gateway: {{ stt_provider: 'fish', tts_provider: 'breezeblue', tts_speed: 0.9 }},
        saved_keys: {{}}
      }});
      await flush();
      assert(
        $('vc-gw-tts-speed').value === '1.4',
        'stale GET gateway must NOT overwrite freshly saved PUT result'
      );

      if (typeof print === 'function') print('NON_BLOCKING_MEDIA_VC_OK');
      else console.log('NON_BLOCKING_MEDIA_VC_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
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
    assert proc.returncode == 0, f"Non-blocking JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "NON_BLOCKING_MEDIA_VC_OK" in proc.stdout


def test_ledger_balance_reuse_per_tab_abort_polling_guard_and_hash_login_runtime():
    """Verify #ledger avoids redundant account-balance, per-tab abort, poll guard & hash login."""
    runner = _js_runner()
    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids = sorted(set(collector.ids))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    ledger_js = (ADMIN_DIR / "src" / "js" / "02-overview-ledger.js").read_text(encoding="utf-8")
    shell_js = (ADMIN_DIR / "src" / "js" / "08-modules-shell.js").read_text(encoding="utf-8")
    nav_start = shell_js.index("const panelLoads=new WeakMap();")
    nav_end = shell_js.index("$('logout').addEventListener('click',logout);")
    nav_login_js = shell_js[nav_start:nav_end]

    harness = f"""
    {_mock_dom_prelude(all_ids)}
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createElementNS(ns, tag) {{ return new MockNode(tag, ns); }},
      createTextNode(t) {{
        const n = new MockNode('#text'); n.data = String(t); n._text = String(t); return n;
      }},
      addEventListener() {{}},
      querySelectorAll() {{ return []; }}
    }};
    const window = {{ addEventListener(){{}}, scrollTo(){{}} }};
    let intervalFn = null;
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function setInterval(fn){{ intervalFn = fn; return 77; }}
    function clearInterval(){{ intervalFn = null; }}
    const location = {{ hash: '#ledger', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};
    function previewWakeWindow(){{}}
    function lifeFmt(v){{ return String(v||''); }}
    function showNotebookAvailability(){{}}
    function stampDashboard(){{}}
    function providerBadge(p){{ return String(p||''); }}

    {core_js}

    // Verify activeRequestSignal splits GET (tabRequests) vs write (sessionRequests).
    const initialTabSig = activeRequestSignal('GET');
    const initialSessSig = activeRequestSignal('PUT');
    assert(initialTabSig === tabRequests.signal, 'GET must bind to tabRequests');
    assert(initialSessSig === sessionRequests.signal, 'PUT must bind to sessionRequests');
    abortTabRequests();
    assert(initialTabSig.aborted === true, 'abortTabRequests must abort GET signal');
    assert(initialSessSig.aborted === false, 'abortTabRequests must NOT abort session signal');
    const secondTabSig = activeRequestSignal('GET');
    assert(secondTabSig.aborted === false, 'new tab signal must be fresh');
    logout();
    assert(secondTabSig.aborted === true, 'logout must abort tab signal');
    assert(initialSessSig.aborted === true, 'logout must abort session signal');

    const apiCalls = [];
    const sampleDash = {{
      primary_model: 'hydrogen',
      uptime_seconds: 1200,
      minute_requests: 1,
      minute_limit: 30,
      daily_limit_usd: 5,
      metrics: [],
      pipeline: {{ gate: 0, generating: 0, image_cache_bytes: 0 }},
      usage: {{
        timezone: 'Asia/Shanghai', day_local: '2026-11-27',
        lanes: [{{ lane: 'chat', amount: 50000, calls: 2 }}],
        categories: [{{ category: 'chat', amount: 50000, calls: 2 }}],
        token_usage: {{ today: null, month: null, lifetime: null }},
        cost_estimates: {{}}, cache_health: {{ prefix_busts_today: 0 }},
        recent_work: [], history: {{}}
      }},
      budget_status: {{
        daily: 50000, daily_limit: 5000000, monthly: 50000, monthly_limit: 50000000,
        total: 50000, total_limit: 0, sub_budgets: {{}},
        account_balance: {{
          mode: 'manual', configured: true, manual_configured: true,
          set_usd: '25.00', remaining_usd: '24.9500', manual_remaining_usd: '24.9500',
          spent_since_set_microdollars: 50000, gcp_synced: false, exhausted: false
        }}
      }},
      rest_hours: null,
      notebook: {{ enabled: false }}
    }};

    const pendingTabLoads = {{}};
    api = (path, method='GET', data) => {{
      const sig = activeRequestSignal(method);
      apiCalls.push({{ path, method, sig }});
      if (path === 'global/overview' || path === 'global/dashboard') {{
        return Promise.resolve(sampleDash);
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
      if (path === 'gcp-billing') {{
        return Promise.resolve({{
          source: 'env',
          config: {{
            bigquery_enabled: false, enforce_credit_hard_stop: false, initial_credits_usd: 0,
            bigquery_sync_interval_seconds: 1800, monitoring_enabled: false,
            monitoring_sync_interval_seconds: 300, credit_grants: []
          }},
          credentials: {{}}, bigquery: {{ synced: false }}, monitoring: {{ status: 'not_synced' }}
        }});
      }}
      if (path === 'colab') {{
        return Promise.resolve({{ configured: false, config: {{}}, worker: {{}}, today: {{}} }});
      }}
      if (path === 'account-balance') {{
        return Promise.resolve({{ mode: 'none' }});
      }}
      return Promise.resolve({{}});
    }};

    {ledger_js}

    // Minimal shell wiring for navigate() + login-form + polling guard tests.
    const sections = new Map([
      ['overview', {{ workspace: 'core', title: '总览' }}],
      ['ledger', {{ workspace: 'core', title: '账本' }}],
      ['logs', {{ workspace: 'core', title: '日志' }}]
    ]);
    function closeNavigation(){{}}
    function selectTab(k){{ tab = k; }}
    function syncHash(k){{ location.hash = '#' + k; }}
    function revealSectionFor(){{}}
    const tabLoaders = {{
      overview: () => dashboard(),
      ledger: () => ledger(),
      logs: () => new Promise((resolve, reject) => {{
        const sig = activeRequestSignal('GET');
        (pendingTabLoads.logs ||= []).push({{ resolve, reject, sig }});
      }})
    }};
    function refreshTab(k=tab){{ return tabLoaders[k](); }}
    let logsCalls = 0;
    let resolvePoll = null;
    function logs(){{
      logsCalls++;
      return new Promise(resolve => {{ resolvePoll = resolve; }});
    }}
    function decisionAudit(){{ return Promise.resolve(); }}

    {nav_login_js}

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}
    async function flush() {{ for (let i = 0; i < 10; i++) await Promise.resolve(); }}

    (async () => {{
      // 1. Hash login (#ledger) must NOT call global/providers, gcp-billing, or colab
      //    from overview before navigate('ledger') runs, AND ledger() must reuse
      //    budget_status.account_balance without fetching account-balance!
      $('token').value = 'valid-token';
      location.hash = '#ledger';
      $('login-form').dispatch('submit');
      await flush();

      const paths = apiCalls.map(c => c.path);
      assert(paths[0] === 'global/overview', 'login must verify token via global/overview');
      assert(
        !paths.includes('global/providers'),
        'hash login must NOT run full overview providers'
      );
      assert(
        !paths.includes('account-balance'),
        'ledger must reuse budget_status.account_balance'
      );
      assert(
        paths.filter(p => p === 'gcp-billing').length === 1,
        'gcp-billing must be fetched once (by ledger), not twice: ' + JSON.stringify(paths)
      );
      assert(
        paths.filter(p => p === 'colab').length === 1,
        'colab must be fetched once (by ledger), not twice: ' + JSON.stringify(paths)
      );
      assert(
        $('unified-balance-strip').textContent.includes('$24.9500'),
        'unified balance must render from dashboard payload'
      );

      // 2. Tab switch aborts previous tab's in-flight GET signal; rapid A->B->A starts fresh load.
      const pLogs1 = navigate('logs');
      assert(pendingTabLoads.logs.length === 1, 'logs load 1 started');
      const sigLogs1 = pendingTabLoads.logs[0].sig;
      assert(sigLogs1.aborted === false, 'logs 1 signal initially live');

      const pOv = navigate('overview');
      assert(sigLogs1.aborted === true, 'switching to overview must abort logs 1 GET signal');
      pendingTabLoads.logs[0].reject(new DOMException('aborted', 'AbortError'));
      await pLogs1;
      await pOv;

      // 3. 15s polling guard prevents overlapping requests while previous poll is in-flight.
      tab = 'logs';
      assert(typeof intervalFn === 'function', 'login must register polling interval');
      logsCalls = 0;
      intervalFn();
      intervalFn();
      intervalFn();
      assert(logsCalls === 1, 'polling guard must block overlapping ticks, got ' + logsCalls);
      resolvePoll();
      await flush();
      intervalFn();
      assert(logsCalls === 2, 'polling guard must release after previous poll settles');
      resolvePoll();
      await flush();

      if (typeof print === 'function') print('SHELL_HYGIENE_OK');
      else console.log('SHELL_HYGIENE_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
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
    assert proc.returncode == 0, f"Shell hygiene JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "SHELL_HYGIENE_OK" in proc.stdout


def test_mutation_observer_scoped_to_forms_not_body_js_runtime():
    """Verify 09-ux.js observes only #app forms (never document.body) and enhances labels."""
    runner = _js_runner()
    html = build_html()
    assert audit_html(html) == []
    collector = _IdCollector()
    collector.feed(html)
    all_ids = sorted(set(collector.ids))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    ux_js = (ADMIN_DIR / "src" / "js" / "09-ux.js").read_text(encoding="utf-8")
    assert ".observe(document.body" not in ux_js, "09-ux.js must not observe document.body"

    harness = f"""
    {_mock_dom_prelude(all_ids)}
    const appNode = byId.get('app');
    const formA = new MockNode('form');
    formA.id = 'sample-form-a';
    const staticLabel = new MockNode('label');
    const staticInput = new MockNode('input');
    staticLabel.append('初始额度 USD (initial_usd)', staticInput);
    formA.append(staticLabel);

    const tableWrap = new MockNode('div');
    tableWrap.className = 'table';
    appNode.append(formA, tableWrap);

    const observedTargets = [];
    class FakeMutationObserver {{
      constructor(cb) {{ this.cb = cb; }}
      observe(target, opts) {{ observedTargets.push({{ target, opts }}); }}
    }}
    const MutationObserver = FakeMutationObserver;
    const window = {{ MutationObserver, matchMedia: () => ({{ matches: false }}) }};
    const document = {{
      body: new MockNode('body'),
      documentElement: new MockNode('html'),
      hidden: false,
      activeElement: null,
      title: '',
      getElementById(id) {{ return byId.get(id) || null; }},
      createElement(tag) {{ return new MockNode(tag); }},
      createTextNode(t) {{
        const n = new MockNode('#text'); n.data = String(t); n._text = String(t); return n;
      }},
      addEventListener() {{}},
      querySelectorAll(sel) {{
        if (sel === '#app form') return [formA];
        if (sel === '#app section.tab') return [];
        return [];
      }}
    }};
    function setTimeout(){{ return 0; }}
    function clearTimeout(){{}}
    function clearInterval(){{}}
    const location = {{ hash: '', pathname: '/admin' }};
    const history = {{ replaceState(){{}}, pushState(){{}} }};

    {core_js}
    {ux_js}

    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    // 1. MutationObserver MUST NOT observe document.body; MUST observe #app form only.
    assert(observedTargets.length === 1, 'expected 1 observed form, got ' + observedTargets.length);
    assert(observedTargets[0].target === formA, 'observer target must be formA');
    assert(
      !observedTargets.some(t => t.target === document.body),
      'document.body must NEVER be observed'
    );

    // 2. Static label enhanced cleanly & idempotently.
    assert(staticLabel.textContent.includes('初始额度 USD'), 'static label main text');
    const hints1 = staticLabel.children.filter(c => c.className === 'field-hint');
    assert(hints1.length === 1 && hints1[0].textContent === 'initial_usd', 'static hint mismatch');
    enhanceLabels(formA);
    const hints2 = staticLabel.children.filter(c => c.className === 'field-hint');
    assert(hints2.length === 1, 'enhanceLabels must be idempotent');

    // 3. Dynamically created form (e.g. identity/plugin/module schedule) enhances cleanly.
    const dynForm = new MockNode('form');
    const dynLabel = new MockNode('label');
    const dynInput = new MockNode('input');
    dynLabel.append('显示名称（display_name）', dynInput);
    dynForm.append(dynLabel);
    enhanceLabels(dynForm);
    const dynHints = dynLabel.children.filter(c => c.className === 'field-hint');
    assert(
      dynHints.length === 1 && dynHints[0].textContent === 'display_name',
      'dynamic form label hint mismatch'
    );

    if (typeof print === 'function') print('UX_OBSERVER_SCOPED_OK');
    else console.log('UX_OBSERVER_SCOPED_OK');
    """

    proc = subprocess.run(
        [*runner, harness],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"UX observer JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "UX_OBSERVER_SCOPED_OK" in proc.stdout
