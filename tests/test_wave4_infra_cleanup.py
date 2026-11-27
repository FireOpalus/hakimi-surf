"""Wave 4 regression tests: cross-platform moderation/UI, CI/Docker/Deploy hardening, & docs."""

from __future__ import annotations

import importlib.util
import re
import subprocess
from pathlib import Path
from typing import Any

import httpx
from pydantic import SecretStr

from admin.build import _IdCollector, audit_html, build_html
from admin.csp import content_security_policy
from hakimi.app import create_app
from hakimi.social import OwnerBinding
from tests.fakes import FakeGateway
from tests.js_runtime import find_js_engine, run_js_harness
from tests.test_admin import TOKEN
from tests.test_wave12_admin_infra import _mock_dom_prelude

REPO_ROOT = Path(__file__).resolve().parents[1]
ADMIN_DIR = REPO_ROOT / "admin"


def _cross_platform_settings(settings: Any) -> Any:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_self_id": "10000",
            "owner_qq_id": "20000",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id="9101002957",
                    user_id="777000111",
                    notebook=False,
                    mirror=False,
                )
            },
        }
    )


async def test_cross_platform_moderation_admin_api_put_and_delete(settings: Any) -> None:
    s = _cross_platform_settings(settings)
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client,
    ):
        svc = app.state.service

        # 1. Auth required before validation on new DELETE route
        r_unauth = await client.delete(
            "/admin/api/moderation/mute/30000", headers={"Authorization": "Bearer wrong"}
        )
        assert r_unauth.status_code == 401

        # 2. Query string rejected on DELETE route
        r_query = await client.delete("/admin/api/moderation/mute/30000?extra=1")
        assert r_query.status_code == 400

        # 3. Invalid target formats rejected with 400 on both PUT and DELETE
        for bad_target in ("invalid", "01234", "onebot:01234", "telegram:", "bad platform:123"):
            r_put_bad = await client.put(
                "/admin/api/moderation/users", json={"user_id": bad_target}
            )
            assert r_put_bad.status_code == 400, (bad_target, r_put_bad.text)
            assert r_put_bad.json()["detail"] == "invalid_admin_input"

        for bad_path in ("invalid", "01234", "onebot:01234", "telegram:"):
            r_del_bad = await client.delete(f"/admin/api/moderation/mute/{bad_path}")
            assert r_del_bad.status_code == 400, (bad_path, r_del_bad.text)
            assert r_del_bad.json()["detail"] == "invalid_admin_input"

        # 4. Protected Owner/Bot identities rejected with 409 across QQ and Telegram
        protected_targets = (
            "10000",
            "20000",
            "onebot:20000",
            "telegram:777000111",
            "telegram:9101002957",
        )
        for protected in protected_targets:
            r_prot_put = await client.put(
                "/admin/api/moderation/users", json={"user_id": protected, "muted": True}
            )
            assert r_prot_put.status_code == 409, (protected, r_prot_put.text)
            assert r_prot_put.json()["detail"] == "protected_identity"

            r_prot_del = await client.delete(f"/admin/api/moderation/mute/{protected}")
            assert r_prot_del.status_code == 409, (protected, r_prot_del.text)
            assert r_prot_del.json()["detail"] == "protected_identity"

        # 5. Legacy numeric QQ mute via PUT and unmute via DELETE
        r_qq_mute = await client.put("/admin/api/moderation/users", json={"user_id": "30000"})
        assert r_qq_mute.status_code == 200
        assert r_qq_mute.json()["muted"] is True
        assert "30000" in svc.s.muted_user_ids

        r_qq_unmute = await client.delete("/admin/api/moderation/mute/30000")
        assert r_qq_unmute.status_code == 200
        assert r_qq_unmute.json()["muted"] is False
        assert "30000" not in svc.s.muted_user_ids

        # 6. Explicit onebot:<qq> prefix also works seamlessly
        r_ob_mute = await client.put(
            "/admin/api/moderation/users", json={"user_id": "onebot:30001"}
        )
        assert r_ob_mute.status_code == 200
        assert "30001" in svc.s.muted_user_ids
        r_ob_del = await client.delete("/admin/api/moderation/mute/onebot%3A30001")
        assert r_ob_del.status_code == 200
        assert "30001" not in svc.s.muted_user_ids

        # 7. Telegram mute via PUT, reject delete_friend=True, unmute via DELETE
        r_tg_df = await client.put(
            "/admin/api/moderation/users",
            json={"user_id": "telegram:555666", "muted": True, "delete_friend": True},
        )
        assert r_tg_df.status_code == 409
        assert r_tg_df.json()["detail"] == "delete_friend_onebot_only"
        assert svc.identity.is_muted("telegram", "555666") is False

        r_tg_mute = await client.put(
            "/admin/api/moderation/users",
            json={"user_id": "telegram:555666", "muted": True},
        )
        assert r_tg_mute.status_code == 200
        tg_body = r_tg_mute.json()
        assert tg_body["platform"] == "telegram"
        assert tg_body["user_id"] == "555666"
        assert tg_body["muted"] is True
        assert tg_body["delete_status"] == "not_supported"
        assert svc.identity.is_muted("telegram", "555666") is True

        # End-to-end check with Moderation.status(): Telegram user appears in platform_muted_users
        r_status_muted = await client.get("/admin/api/moderation")
        assert r_status_muted.status_code == 200
        status_muted = r_status_muted.json()
        assert any(
            item.get("platform") == "telegram"
            and item.get("user_id") == "555666"
            and item.get("target") == "telegram:555666"
            for item in status_muted.get("platform_muted_users", [])
        )

        r_tg_unmute = await client.delete("/admin/api/moderation/mute/telegram%3A555666")
        assert r_tg_unmute.status_code == 200
        assert r_tg_unmute.json()["muted"] is False
        assert svc.identity.is_muted("telegram", "555666") is False

        r_status_after = await client.get("/admin/api/moderation")
        assert r_status_after.status_code == 200
        assert not any(
            item.get("platform") == "telegram" and item.get("user_id") == "555666"
            for item in r_status_after.json().get("platform_muted_users", [])
        )


def test_wave4_admin_js_runtime_table_empty_tg_users_moderation_and_dedup() -> None:
    """Execute assembled JS snippets in node/jsc for Wave 4 UI polish & bugfixes."""
    find_js_engine("No local JS engine (node/jsc) available for Wave 4 JS runtime test")
    html = build_html()
    assert audit_html(html) == []
    policy = content_security_policy(html)
    assert "script-src 'sha256-" in policy and "style-src 'sha256-" in policy

    collector = _IdCollector()
    collector.feed(html)
    all_ids = sorted(set(collector.ids))

    core_js = (ADMIN_DIR / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    adm_js = (ADMIN_DIR / "src" / "js" / "04-admission-memory.js").read_text(encoding="utf-8")
    life_js = (ADMIN_DIR / "src" / "js" / "05-life-companion.js").read_text(encoding="utf-8")
    comm_js = (ADMIN_DIR / "src" / "js" / "06-identity-community.js").read_text(encoding="utf-8")
    media_js = (ADMIN_DIR / "src" / "js" / "07-media-bili.js").read_text(encoding="utf-8")

    # Extract slices cleanly
    mc_start = life_js.index("const mcControls=")
    mc_end = life_js.index("async function sendMcGoal")
    mc_slice = life_js[mc_start:mc_end]

    comm_start = comm_js.index("function parseCommunityMuteTarget(")
    comm_end = comm_js.index("$('sign-config-form').addEventListener")
    comm_slice = comm_js[comm_start:comm_end]

    meme_req_start = media_js.index("function memeRequest(")
    meme_req_end = media_js.index("function pumpMemeQueue(")
    meme_req_slice = media_js[meme_req_start:meme_req_end]

    song_req_start = media_js.index("const songErrorLabels=")
    song_req_end = media_js.index("function setSelectedSongFile(")
    song_req_slice = media_js[song_req_start:song_req_end]

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
    function diagnosticCell(v){{ return String(v || ''); }}
    async function affection(){{}}

    const fetchCalls = [];
    let nextFetchResponse = {{ ok: true, status: 200, json: async () => ({{ ok: true }}) }};
    async function fetch(url, init) {{
      fetchCalls.push({{ url, init }});
      return nextFetchResponse;
    }}

    {core_js}
    token = 'wave4-token';
    const pendingTasks = [];
    const origBusy = busy;
    busy = (btn, fn) => {{
      const p = origBusy(btn, fn);
      pendingTasks.push(p);
      return p;
    }};
    confirmDialog = async () => true;

    const apiCalls = [];
    let modPayload = {{ muted_users: [], platform_muted_users: [], proposals: [] }};
    api = async (path, method='GET', data) => {{
      apiCalls.push({{ path, method, data }});
      if (path === 'moderation' && method === 'GET') return modPayload;
      if (path === 'moderation/users' && method === 'PUT') {{
        return {{
          platform: data.user_id.includes(':') ? data.user_id.split(':')[0] : 'onebot',
          user_id: data.user_id.includes(':') ? data.user_id.split(':')[1] : data.user_id,
          muted: data.muted,
          delete_status: data.user_id.includes(':') ? 'not_supported' : 'not_requested'
        }};
      }}
      if (path.startsWith('moderation/mute/') && method === 'DELETE') {{
        return {{ muted: false, delete_status: 'not_supported' }};
      }}
      return {{}};
    }};

    {adm_js}
    {mc_slice}
    {comm_slice}
    {meme_req_slice}
    {song_req_slice}

    function walk(node, acc=[]) {{
      acc.push(node);
      for (const c of (node.children || [])) walk(c, acc);
      return acc;
    }}
    function assert(cond, msg) {{ if (!cond) throw new Error('ASSERT FAILED: ' + msg); }}

    (async () => {{
      // 1. table() optional 4th emptyText param + renderMinecraft() custom empty state
      table('mute-list', ['A'], []);
      assert(
        $('mute-list').textContent.includes('还没有记录，新的动态会出现在这里。'),
        'default emptyText broken'
      );
      table('mute-list', ['A'], [], '自定义空文案');
      assert($('mute-list').textContent.includes('自定义空文案'), 'custom emptyText ignored');
      renderMinecraft({{ action_log: [] }});
      assert(
        $('mc-action-log').textContent.includes('暂无 MC 动作流水'),
        'renderMinecraft custom emptyText not rendered'
      );

      // 2. TG allowed_users rejects negative IDs & 0; allowed_groups keeps negative IDs
      const norm = normalizeTgDraft({{
        allowed_groups: ['-1001234567890', '12345'],
        proactive_groups: ['-1001234567890'],
        allowed_users: ['-1001234567890', '-42', '0', '555666777']
      }});
      assert(
        JSON.stringify(norm.allowed_groups) === JSON.stringify(['-1001234567890', '12345']),
        'groups must keep negative supergroup IDs'
      );
      assert(
        JSON.stringify(norm.allowed_users) === JSON.stringify(['555666777']),
        'normalizeTgDraft must filter negative/zero users: ' + JSON.stringify(norm.allowed_users)
      );

      tgSocialDraft = normalizeTgDraft({{ allowed_groups: ['-1001234567890'], allowed_users: [] }});
      $('tg-user-input').value = '-1001234567890';
      $('tg-add-user').dispatch('click');
      assert(tgSocialDraft.allowed_users.length === 0, 'negative user ID must be rejected');
      assert($('notice-text').textContent.includes('正整数'), 'negative user notice mismatch');

      $('tg-user-input').value = '987654321';
      $('tg-add-user').dispatch('click');
      assert(
        JSON.stringify(tgSocialDraft.allowed_users) === JSON.stringify(['987654321']),
        'positive TG user ID must be accepted'
      );

      // 3. Cross-platform 小黑屋 renders QQ + TG, unmutes via DELETE, guards delete_friend
      modPayload = {{
        muted_users: [{{
          platform: 'onebot', user_id: '30000', target: 'onebot:30000',
          created: 1700000000, source: 'manual', delete_status: 'not_requested'
        }}],
        platform_muted_users: [{{
          platform: 'telegram', user_id: '555666', target: 'telegram:555666',
          nickname: '捣蛋猫', person_id: 'p_1234567890abcdef',
          created: 1700000100, source: 'owner_manual', delete_status: 'not_supported'
        }}],
        proposals: [
          {{
            id: 'aabbccddeeff', platform: 'telegram', user_id: '888999',
            nickname: 'TG用户', reason: '注入', status: 'pending'
          }},
          {{
            id: '112233445566', platform: 'onebot', user_id: '30002',
            nickname: 'QQ用户', reason: '注入', status: 'pending'
          }}
        ]
      }};
      await community();
      const muteTxt = $('mute-list').textContent;
      assert(muteTxt.includes('Telegram') && muteTxt.includes('555666'), 'TG muted user missing');
      assert(muteTxt.includes('QQ') && muteTxt.includes('30000'), 'QQ muted user missing');
      assert(muteTxt.includes('捣蛋猫'), 'TG muted nickname missing');

      const unmuteBtns = walk($('mute-list')).filter(
        n => n.tagName === 'BUTTON' && n.textContent === '解除屏蔽'
      );
      assert(unmuteBtns.length === 2, 'expected 2 unmute buttons');
      apiCalls.length = 0;
      unmuteBtns[0].dispatch('click'); // newest first -> telegram:555666
      await Promise.all(pendingTasks.splice(0));
      assert(
        apiCalls.some(c => c.method === 'DELETE' && c.path === 'moderation/mute/telegram%3A555666'),
        'TG unmute must call DELETE moderation/mute/telegram%3A555666: ' + JSON.stringify(apiCalls)
      );

      apiCalls.length = 0;
      unmuteBtns[1].dispatch('click'); // onebot 30000
      await Promise.all(pendingTasks.splice(0));
      assert(
        apiCalls.some(c => c.method === 'DELETE' && c.path === 'moderation/mute/30000'),
        'QQ unmute must call DELETE moderation/mute/30000: ' + JSON.stringify(apiCalls)
      );

      // Proposals: TG proposal must NOT show '批准并移除好友', QQ proposal MUST show it
      const propRows = walk($('mute-proposals')).filter(n => n.tagName === 'TR');
      assert(propRows.length === 3, 'expected header + 2 proposal rows');
      assert(
        !propRows[1].textContent.includes('批准并移除好友'),
        'TG proposal must hide delete_friend button'
      );
      assert(
        propRows[2].textContent.includes('批准并移除好友'),
        'QQ proposal must keep delete_friend button'
      );

      // Mute form: reject delete_friend on telegram:888999 without calling API
      apiCalls.length = 0;
      $('mute-qq').value = 'telegram:888999';
      $('mute-delete').checked = true;
      $('mute-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(apiCalls.length === 0, 'delete_friend on TG must fail client-side before API');
      assert(
        $('notice-text').textContent.includes('仅 QQ'),
        'TG delete_friend error notice missing'
      );

      // Mute form: accept telegram:888999 with delete_friend=false
      $('mute-delete').checked = false;
      $('mute-form').dispatch('submit');
      await Promise.all(pendingTasks.splice(0));
      assert(
        apiCalls.some(
          c => c.method === 'PUT'
            && c.path === 'moderation/users'
            && c.data.user_id === 'telegram:888999'
            && c.data.muted === true
            && c.data.delete_friend === false
        ),
        'mute form must submit telegram:888999 cleanly: ' + JSON.stringify(apiCalls)
      );

      // 4. Deduplicated apiRaw / memeRequest / songRequest / uploadInboxAudio / uploadRefVoiceAudio
      fetchCalls.length = 0;
      nextFetchResponse = {{
        ok: true, status: 200, json: async () => ({{ file_id: 'f1', name: 'a.mp3', size: 12 }})
      }};
      await memeRequest('/m1/image');
      assert(fetchCalls[0].url === '/admin/api/memes/m1/image', 'memeRequest URL mismatch');
      assert(
        fetchCalls[0].init.headers.Authorization === 'Bearer wave4-token',
        'memeRequest auth missing'
      );

      await songRequest('/s1/audio', 'GET', undefined, {{ 'X-Test': '1' }});
      assert(fetchCalls[1].url === '/admin/api/songs/s1/audio', 'songRequest URL mismatch');
      assert(fetchCalls[1].init.headers['X-Test'] === '1', 'songRequest extraHeaders missing');

      const fakeFile = {{ name: '猫猫歌.mp3', type: 'audio/mpeg' }};
      const up1 = await uploadInboxAudio(fakeFile);
      assert(up1.file_id === 'f1', 'uploadInboxAudio JSON result mismatch');
      assert(fetchCalls[2].url === '/admin/api/songs/inbox', 'uploadInboxAudio URL mismatch');
      assert(
        fetchCalls[2].init.headers['X-Drive-Filename'] === encodeURIComponent('猫猫歌.mp3'),
        'uploadInboxAudio filename header mismatch'
      );

      const up2 = await uploadRefVoiceAudio({{
        name: 'ref.wav', type: 'application/octet-stream'
      }});
      assert(up2.file_id === 'f1', 'uploadRefVoiceAudio JSON result mismatch');
      assert(
        fetchCalls[3].url === '/admin/api/songs/ref-voices',
        'uploadRefVoiceAudio URL mismatch'
      );
      assert(
        fetchCalls[3].init.headers['Content-Type'] === 'application/octet-stream',
        'uploadRefVoiceAudio fallback content-type mismatch'
      );

      nextFetchResponse = {{
        ok: false, status: 413, json: async () => ({{ detail: 'song_ref_voice_too_large' }})
      }};
      let caught = '';
      try {{
        await uploadRefVoiceAudio(fakeFile);
      }} catch (e) {{
        caught = e.message;
      }}
      assert(caught.includes('20 MiB'), 'uploadRefVoiceAudio must map songErrorLabels: ' + caught);

      if (typeof print === 'function') print('WAVE4_ADMIN_JS_OK');
      else console.log('WAVE4_ADMIN_JS_OK');
    }})().catch(err => {{
      const detail = String(err) + '\\n' + (err && err.stack ? err.stack : '');
      if (typeof print === 'function') print('ERR: ' + detail);
      else console.error(detail);
      if (typeof process !== 'undefined' && process.exit) process.exit(1);
      throw err;
    }});
    """

    proc = run_js_harness(harness, cwd=REPO_ROOT)
    assert proc.returncode == 0, f"Wave 4 JS test failed:\n{proc.stderr}\n{proc.stdout}"
    assert "WAVE4_ADMIN_JS_OK" in proc.stdout


def test_ci_workflow_fast_tests_and_admin_gates() -> None:
    ci_text = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    build_step = "uv run python admin/build.py\n"
    verify_step = "uv run python admin/build.py --check --verify-file"
    csp_step = "uv run python admin/csp.py"
    assert build_step in ci_text
    assert verify_step in ci_text
    assert csp_step in ci_text
    assert ci_text.index(build_step) < ci_text.index(verify_step) < ci_text.index(csp_step)
    assert "bash scripts/test_fast.sh -q" in ci_text
    assert "uv run pytest -q" not in ci_text


def _parse_ci_sections(ci_text: str) -> tuple[dict[str, str], dict[str, str]]:
    """Structurally split top-level workflow sections and per-job blocks without PyYAML."""
    assert "\t" not in ci_text, "YAML must not contain tab indentation"
    top_blocks: dict[str, str] = {}
    cur_top: str | None = None
    for line in ci_text.splitlines():
        if line and not line.startswith(" ") and not line.startswith("#"):
            assert ":" in line, f"Malformed top-level YAML line: {line!r}"
            key = line.split(":", 1)[0].strip()
            assert key not in top_blocks, f"Duplicate top-level YAML key: {key}"
            cur_top = key
            top_blocks[cur_top] = line + "\n"
        elif cur_top is not None:
            top_blocks[cur_top] += line + "\n"

    assert "jobs" in top_blocks, "CI workflow missing top-level jobs section"
    job_blocks: dict[str, str] = {}
    cur_job: str | None = None
    for line in top_blocks["jobs"].splitlines()[1:]:
        if line.startswith("  ") and not line.startswith("   ") and line.rstrip().endswith(":"):
            job_name = line.strip()[:-1]
            assert job_name not in job_blocks, f"Duplicate CI job: {job_name}"
            cur_job = job_name
            job_blocks[cur_job] = ""
        elif cur_job is not None:
            job_blocks[cur_job] += line + "\n"
    return top_blocks, job_blocks


def test_ci_workflow_hardening_and_docker_checks() -> None:
    ci_text = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    if importlib.util.find_spec("yaml") is not None:
        import yaml  # type: ignore[import-not-found]

        parsed = yaml.safe_load(ci_text)
        assert isinstance(parsed, dict)
        assert isinstance(parsed.get("jobs"), dict)

    top_blocks, job_blocks = _parse_ci_sections(ci_text)

    # 1. Workflow-level concurrency cancellation & read-only default permissions
    assert "concurrency" in top_blocks
    concurrency_block = top_blocks["concurrency"]
    assert "group: ${{ github.workflow }}-${{ github.ref }}" in concurrency_block
    assert re.search(r"^\s*cancel-in-progress:\s*true\s*$", concurrency_block, re.M)
    assert re.search(r"^\s*contents:\s*read\s*$", top_blocks["permissions"], re.M)
    assert not re.search(r":\s*write\b", ci_text), "CI workflow must stay read-only"

    # 2. Expected jobs and mandatory timeout-minutes on every job
    expected_timeouts = {
        "catalog": 5,
        "platform": 5,
        "offline-tests": 15,
        "docker": 10,
    }
    assert set(job_blocks) == set(expected_timeouts)
    for job_name, expected_timeout in expected_timeouts.items():
        m = re.search(r"^    timeout-minutes:\s*(\d+)\s*$", job_blocks[job_name], re.M)
        assert m is not None, f"CI job {job_name!r} missing timeout-minutes"
        assert int(m.group(1)) == expected_timeout, (job_name, m.group(1))

    # 3. setup-uv caching on offline-tests + preserved lint/admin/fast-test gates
    offline = job_blocks["offline-tests"]
    assert "uses: astral-sh/setup-uv@v6" in offline
    assert re.search(r"^\s*enable-cache:\s*true\s*$", offline, re.M)
    assert re.search(r"^\s*cache-dependency-glob:\s*['\"]?uv\.lock['\"]?\s*$", offline, re.M)
    for required_cmd in (
        "uv sync --frozen",
        "uv run ruff check .",
        "uv run ruff format --check .",
        "uv run python admin/build.py",
        "uv run python admin/build.py --check --verify-file",
        "uv run python admin/csp.py",
        "bash scripts/test_fast.sh -q",
    ):
        assert required_cmd in offline

    # 4. Parallel secret-free Docker & Compose validation job
    docker_job = job_blocks["docker"]
    assert not re.search(r"^\s*needs:", docker_job, re.M), "docker job must run in parallel"
    assert "secrets." not in docker_job, "docker job must not require private deploy keys"
    assert re.search(r"^    permissions:\s*\n      contents:\s*read\s*$", docker_job, re.M)

    # Clean-checkout env bootstrap + cleanup + all 3 Compose validations
    for env_src in (".env.example", ".env.voice-gateway.example"):
        assert (REPO_ROOT / env_src).is_file(), f"Missing tracked example env file: {env_src}"
        assert env_src in docker_job
    assert "secrets/voice-gateway.env" in docker_job
    assert "trap cleanup EXIT" in docker_job
    for compose_cmd in (
        "docker compose -f compose.yaml config -q",
        "docker compose -f compose.yaml -f compose.plugins.yaml config -q",
        "docker compose -f compose.yaml -f compose.voice-gateway.yaml config -q",
    ):
        assert compose_cmd in docker_job

    # BuildKit --check lint across all 6 Dockerfiles with exact build contexts
    expected_checks = (
        ("Dockerfile", "."),
        ("admin/Dockerfile", "./admin"),
        ("plugins/Dockerfile", "."),
        ("plugins/doc_render/Dockerfile", "."),
        ("plugins/song_finder/Dockerfile", "."),
        ("voice_gateway/Dockerfile", "."),
    )
    for dockerfile, context in expected_checks:
        assert (REPO_ROOT / dockerfile).is_file(), f"Missing Dockerfile: {dockerfile}"
        assert f"docker buildx build --check -f {dockerfile} {context}" in docker_job

    # Self-contained image builds: hakimi-admin (+ nginx -t) and hakimi-voice-gateway
    assert "uses: docker/setup-buildx-action@v3" in docker_job
    assert "uses: docker/build-push-action@v6" in docker_job
    assert "context: ./admin" in docker_job
    assert "file: admin/Dockerfile" in docker_job
    assert "hakimi-admin:ci -t" in docker_job
    assert "file: voice_gateway/Dockerfile" in docker_job
    assert (REPO_ROOT / "voice_gateway" / "Dockerfile.dockerignore").is_file()
    assert "type=gha" in docker_job


def test_compose_plugins_shared_image_cpus_and_healthchecks() -> None:
    compose_text = (REPO_ROOT / "compose.plugins.yaml").read_text(encoding="utf-8")
    blocks: dict[str, str] = {}
    cur: str | None = None
    for line in compose_text.splitlines():
        if line.startswith("  ") and not line.startswith("   ") and line.rstrip().endswith(":"):
            cur = line.strip()[:-1]
            blocks[cur] = ""
        elif cur is not None:
            blocks[cur] += line + "\n"

    common_svcs = {
        "hakimi-plugin-weather",
        "hakimi-plugin-minecraft",
        "hakimi-plugin-bilibili",
        "hakimi-plugin-web-tools",
        "hakimi-plugin-telegram",
    }
    dedicated_svcs = {
        "hakimi-plugin-doc-render": "hakimi-plugin-doc-render:latest",
        "hakimi-plugin-song-finder": "hakimi-plugin-song-finder:latest",
    }
    assert set(blocks) == common_svcs | set(dedicated_svcs)

    for svc in common_svcs:
        assert "image: hakimi-plugin-common:latest" in blocks[svc], svc
    for svc, img in dedicated_svcs.items():
        assert f"image: {img}" in blocks[svc], svc

    for svc, block in blocks.items():
        assert re.search(r"^\s*cpus:\s*[0-9.]+", block, re.M), f"{svc} missing cpus limit"
        assert "healthcheck:" in block, f"{svc} missing healthcheck"
        m = re.search(r'test:\s*\["CMD",\s*"python",\s*"-c",\s*"(.*)"\]', block)
        assert m is not None, f"{svc} healthcheck must be a lightweight Python CMD"
        py_code = m.group(1).encode("utf-8").decode("unicode_escape")
        compile(py_code, f"<{svc}-healthcheck>", "exec")
        assert "os.getpid()" in py_code, f"{svc} healthcheck must exclude its own PID"


def test_deploy_scripts_hardening_and_compose_consolidation() -> None:
    for script_name in ("deploy_prod.sh", "deploy_plugins.sh"):
        script_path = REPO_ROOT / "scripts" / script_name
        proc = subprocess.run(
            ["bash", "-n", str(script_path)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr or proc.stdout
        text = script_path.read_text(encoding="utf-8")
        assert "-f compose.yaml" in text, script_name
        assert "-f docker-compose.yml" not in text, script_name

    plugins_sh = (REPO_ROOT / "scripts" / "deploy_plugins.sh").read_text(encoding="utf-8")
    assert '""|all)' in plugins_sh
    assert "tag_rollback_images" in plugins_sh
    assert ":rollback" in plugins_sh
    assert "verify_running" in plugins_sh
    assert "unique_build_services" in plugins_sh

    # Verify default empty target and 'all' resolve to all 7 plugins in bash directly.
    probe = (
        'eval "$(sed \'/^case "\\$cmd" in/,$d\' scripts/deploy_plugins.sh | grep -v "^cd ")"; '
        'echo "DEFAULT=$(resolve_services)"; '
        'echo "ALL=$(resolve_services all)"; '
        'echo "MULTI=$(resolve_services telegram minecraft telegram)"; '
        'echo "BUILD=$(unique_build_services $(resolve_services all))"'
    )
    res = subprocess.run(
        ["bash", "-c", probe],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    out = dict(line.split("=", 1) for line in res.stdout.strip().splitlines())
    expected_all = (
        "hakimi-plugin-weather hakimi-plugin-minecraft hakimi-plugin-bilibili "
        "hakimi-plugin-web-tools hakimi-plugin-doc-render hakimi-plugin-song-finder "
        "hakimi-plugin-telegram"
    )
    assert out["DEFAULT"] == expected_all
    assert out["ALL"] == expected_all
    assert out["MULTI"] == "hakimi-plugin-telegram hakimi-plugin-minecraft"
    # Only 3 unique images should be built for all 7 services (common + doc-render + song-finder)
    assert len(out["BUILD"].split()) == 3


def test_dockerignore_excludes_bloat_and_no_stale_cn_pycache() -> None:
    lines = [
        ln.strip()
        for ln in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    assert "**/__pycache__" in lines
    assert "__pycache__" not in lines
    for excluded in (
        "docs/",
        "cn/",
        "deploy/",
        "voice_client/",
        "voice_gateway/",
        "architecture.html",
        "README.md",
        "开发手册.md",
    ):
        assert excluded in lines, f"Missing .dockerignore entry: {excluded}"
    for must_keep in ("src", "src/", "plugins", "plugins/", "pyproject.toml", "uv.lock"):
        assert must_keep not in lines, f".dockerignore must NOT exclude {must_keep}"

    cn_dir = REPO_ROOT / "cn"
    if cn_dir.exists():
        stale = list(cn_dir.rglob("__pycache__"))
        assert stale == [], f"Stale __pycache__ left under cn/: {stale}"


def test_docs_and_architecture_wave4_sync() -> None:
    arch = (REPO_ROOT / "architecture.html").read_text(encoding="utf-8")
    for token in (
        "delivery.send_media",
        "delivery.react",
        "delivery.typing",
        "config.update",
        "BaseModule.channel_prompt()",
        "/admin/api/social-chat/{platform}/config",
        "/admin/api/songs/ref-voices",
        "60s TTL",
        "gzip_static",
        "Wave 1–4",
        "hakimi-plugin-common:latest",
    ):
        assert token in arch, f"Missing architecture.html token: {token}"

    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for token in (
        "Telegram",
        "doc_render",
        "900s",
        "bash scripts/test_fast.sh -q",
        "compose.yaml",
        "deploy_plugins.sh",
        "song_studio",
        "voice_gateway",
        "screen_watch",
        "colab_compute",
        "plugin_bus",
    ):
        assert token in readme, f"Missing README.md token: {token}"
    assert "≤480s" not in readme
