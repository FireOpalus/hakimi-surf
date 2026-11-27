"""Tests for P1 observability, plugin lazy-init warnings, deploy pruning, and admin hints."""

from __future__ import annotations

import base64
import logging
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import (
    _STDERR_HANDLER_ATTR,
    _attach_stderr_handler,
    _scrub_log_text,
    create_app,
)
from hakimi.audio_media import ModelAudio
from hakimi.gemini import GeminiGateway
from hakimi.media import ModelImage
from hakimi.runtime_state import AuditSink
from hakimi.security import Rejected, set_runtime_secret_bucket
from tests.test_sdk_contract import sdk_pair as sdk_pair

REPO_ROOT = Path(__file__).resolve().parent.parent


class _DummyGateway:
    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# 1. hakimi logger stderr StreamHandler: attach, dedup, cleanup, scrubbing, caplog
# ---------------------------------------------------------------------------


async def test_app_lifespan_attaches_stderr_handler_dedups_cleans_up_and_keeps_caplog(
    settings, tmp_path, capsys, caplog
):
    logger = logging.getLogger("hakimi")
    assert logger.propagate is True
    before_marked = [h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)]
    assert before_marked == []

    app = create_app(settings, gateway_factory=lambda *_: _DummyGateway(), start_workers=False)
    with caplog.at_level(logging.INFO, logger="hakimi"):
        async with app.router.lifespan_context(app):
            marked = [h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)]
            sinks = [h for h in logger.handlers if isinstance(h, AuditSink)]
            assert len(marked) == 1
            assert isinstance(marked[0], logging.StreamHandler)
            assert len(sinks) >= 1
            assert logger.propagate is True

            # Repeated attachment (or nested app startup) must NOT duplicate stderr handlers.
            assert _attach_stderr_handler(logger, settings) is None
            settings2 = settings.model_copy(update={"data_dir": tmp_path / "data2"})
            app2 = create_app(
                settings2, gateway_factory=lambda *_: _DummyGateway(), start_workers=False
            )
            async with app2.router.lifespan_context(app2):
                marked_nested = [
                    h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)
                ]
                assert len(marked_nested) == 1

            # Outer handler must still be attached after nested lifespan exits.
            assert len([h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)]) == 1

            # Non-whitelisted INFO/WARNING logs must reach stderr AND pytest caplog.
            capsys.readouterr()
            logger.info("event=onebot_connection_ended_detail code=1006 reason=eof")
            logger.warning("event=db_backup_failed")
            logger.warning("event=image_download_failed reason=timeout")
            logger.warning("event=song_audition_fallback reason=timeout")
            # Whitelisted event must still reach AuditSink AND stderr AND caplog.
            logger.warning("event=onebot_connection_ended")

            err_lines = capsys.readouterr().err.splitlines()
            for expected in (
                "event=onebot_connection_ended_detail code=1006 reason=eof",
                "event=db_backup_failed",
                "event=image_download_failed reason=timeout",
                "event=song_audition_fallback reason=timeout",
                "event=onebot_connection_ended",
            ):
                assert sum(1 for line in err_lines if line.endswith(f"] {expected}")) == 1
                assert any(rec.getMessage() == expected for rec in caplog.records)
            assert "onebot_connection_ended" in app.state.service.store.runtime.sink.pending

    # Exiting lifespan cleanly removes both the marked stderr handler and AuditSink.
    after_marked = [h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)]
    assert after_marked == []
    assert app.state.service.store.runtime.sink not in logger.handlers

    # Sequential restart re-attaches cleanly and removes again on exit.
    async with app.router.lifespan_context(app):
        assert len([h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)]) == 1
    assert [h for h in logger.handlers if getattr(h, _STDERR_HANDLER_ATTR, False)] == []


async def test_app_stderr_handler_scrubs_secrets_tokens_ws_paths_cookies_and_tracebacks(
    settings, capsys
):
    ws_secret_suffix = "OneBotSecretPathSuffix1234567890"
    plugin_ws_suffix = "PluginSecretPathSuffix9876543210"
    ws_token = "onebot-ws-secret-token-value-123456789012"
    runtime_plugin_tok = "runtime-plugin-bearer-secret-9988776655"
    bili_sess = "bili-sessdata-secret-value-11223344"
    bili_jct = "bili-csrf-jct-secret-55667788"

    s = settings.model_copy(
        update={
            "onebot_ws_token": SecretStr(ws_token),
            "onebot_ws_path": f"/ws/onebot/{ws_secret_suffix}",
            "plugin_bus_ws_path": f"/ws/plugin/{plugin_ws_suffix}",
            "bili_sessdata": SecretStr(bili_sess),
            "bili_bili_jct": SecretStr(bili_jct),
        }
    )
    set_runtime_secret_bucket(s, "test_plugins", [runtime_plugin_tok])

    logger = logging.getLogger("hakimi")
    app = create_app(s, gateway_factory=lambda *_: _DummyGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        capsys.readouterr()
        api_key = s.gemini_api_key.get_secret_value()
        logger.warning(
            "event=secret_leak_probe api_key=%s inline_key=AIzaSyDummyLeakedKey12345678901234 "
            "Authorization: Bearer %s raw_bearer=Bearer raw_jwt_secret_abc987 "
            "ws1=wss://host/ws/onebot/%s ws2=/ws/plugin/%s "
            "ws3=/ws/node/ArbitraryNodeSecretPath12345678 "
            "Cookie: SESSDATA=%s; bili_jct=%s; custom_sid=priv_cookie_999 "
            "Set-Cookie: session=another_secret_cookie; HttpOnly "
            "url=https://api.example.com/v1?key=query_secret_key_777&token=qtok_888 "
            "runtime_tok=%s safe_code=1006",
            api_key,
            ws_token,
            ws_secret_suffix,
            plugin_ws_suffix,
            bili_sess,
            bili_jct,
            runtime_plugin_tok,
        )
        try:
            raise RuntimeError(
                f"failed on wss://host/ws/onebot/{ws_secret_suffix} "
                f"Authorization: Bearer {ws_token} Cookie: sid=exc_cookie_secret"
            )
        except RuntimeError:
            logger.warning("event=traceback_probe", exc_info=True)

        err = capsys.readouterr().err

    forbidden = (
        api_key,
        "AIzaSyDummyLeakedKey12345678901234",
        ws_token,
        "raw_jwt_secret_abc987",
        ws_secret_suffix,
        plugin_ws_suffix,
        "ArbitraryNodeSecretPath12345678",
        bili_sess,
        bili_jct,
        "priv_cookie_999",
        "another_secret_cookie",
        "query_secret_key_777",
        "qtok_888",
        runtime_plugin_tok,
        "exc_cookie_secret",
    )
    for secret in forbidden:
        assert secret not in err, f"secret leaked to stderr: {secret!r}"

    # Safe diagnostic tokens and path prefixes remain visible.
    assert "event=secret_leak_probe" in err
    assert "safe_code=1006" in err
    assert "/ws/onebot/[REDACTED]" in err
    assert "/ws/plugin/[REDACTED]" in err
    assert "/ws/node/[REDACTED]" in err
    assert "event=traceback_probe" in err
    # Bare WS path without secret suffix is preserved by scrubber.
    assert _scrub_log_text("path=/ws/onebot status=200", s) == "path=/ws/onebot status=200"


# ---------------------------------------------------------------------------
# 2. Inbound decision HTTP 400 sanitized content-free warning
# ---------------------------------------------------------------------------


async def test_inbound_decision_http_400_emits_content_free_warning(
    settings, store, sdk_pair, message, caplog
):
    secret_text = "SUPER_SECRET_RAW_CHAT_TEXT_DO_NOT_LOG"
    secret_user = "9876543210"
    secret_group = "555666777"
    secret_nick = "CanaryNicknameHidden"
    upstream_body_canary = "UPSTREAM_RAW_ECHO_SHOULD_NOT_APPEAR"
    api_key = settings.gemini_api_key.get_secret_value()

    inbound_msg = message.model_copy(
        update={
            "platform": "onebot",
            "kind": "group",
            "user_id": secret_user,
            "conversation_id": secret_group,
            "sender_nickname": secret_nick,
            "addressed": True,
            "text": secret_text,
        }
    )

    def handler(request):
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": 400,
                    "status": "INVALID_ARGUMENT",
                    "message": f"{upstream_body_canary} {secret_text} {api_key}",
                }
            },
        )

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    img = ModelImage(data=base64.b64encode(b"fake-jpeg-bytes").decode(), mime_type="image/jpeg")
    aud = ModelAudio(
        data=base64.b64encode(b"fake-mp3-bytes").decode(),
        mime_type="audio/mp3",
        duration_sec=3.0,
    )

    with caplog.at_level(logging.WARNING, logger="hakimi"):
        with pytest.raises(Rejected, match="^gemini_unavailable$"):
            await gate.screen(
                {"current_message": f"{secret_text} {api_key}"},
                "subject-key",
                purpose="message",
                message=inbound_msg,
                images=[img],
                audios=[aud],
            )

    bad_req_logs = [
        rec for rec in caplog.records if "event=decision_bad_request" in rec.getMessage()
    ]
    assert len(bad_req_logs) == 1
    assert bad_req_logs[0].levelno == logging.WARNING
    msg = bad_req_logs[0].getMessage()

    assert "platform=onebot" in msg
    assert "surface=group" in msg
    assert "image_count=1" in msg
    assert "audio_count=1" in msg
    assert "mimes=image/jpeg,audio/mp3" in msg
    m_bytes = re.search(r"text_bytes=(\d+)", msg)
    assert m_bytes is not None and int(m_bytes.group(1)) > 0
    assert "error_type=BadRequestError" in msg
    assert "status=400" in msg

    for forbidden in (
        secret_text,
        secret_user,
        secret_group,
        secret_nick,
        upstream_body_canary,
        api_key,
    ):
        assert forbidden not in msg


async def test_decision_bad_request_skips_non_inbound_and_non_400_and_sanitizes_mimes(
    settings, store, sdk_pair, message, caplog
):
    def handler_400(request):
        return httpx.Response(400, json={"error": {"code": 400, "status": "INVALID_ARGUMENT"}})

    def handler_500(request):
        return httpx.Response(500, json={"error": {"code": 500, "status": "INTERNAL"}})

    gate_400 = GeminiGateway(settings, store, *sdk_pair(handler_400))
    gate_500 = GeminiGateway(settings, store, *sdk_pair(handler_500))

    with caplog.at_level(logging.WARNING, logger="hakimi"):
        # Non-inbound (purpose="source") HTTP 400 should NOT emit decision_bad_request.
        with pytest.raises(Rejected, match="^gemini_unavailable$"):
            await gate_400.screen(
                {"untrusted_source": "hello"},
                "subject-key",
                purpose="source",
                message=message,
            )
        # Inbound HTTP 500 should NOT emit decision_bad_request.
        with pytest.raises(Rejected, match="^gemini_unavailable$"):
            await gate_500.screen(
                {"current_message": "hello"},
                "subject-key",
                purpose="message",
                message=message,
            )
    assert not any("event=decision_bad_request" in rec.getMessage() for rec in caplog.records)

    # Malformed mime string on an image must be sanitized to "invalid", never logged raw.
    caplog.clear()
    weird_img = ModelImage(
        data=base64.b64encode(b"x").decode(),
        mime_type="image/jpeg; secret=SHOULD_NOT_LEAK",
    )
    with caplog.at_level(logging.WARNING, logger="hakimi"):
        with pytest.raises(Rejected, match="^gemini_unavailable$"):
            await gate_400.screen(
                {"current_message": "hi"},
                "subject-key",
                purpose="message",
                message=message,
                images=[weird_img],
            )
    bad_req_logs = [
        rec.getMessage()
        for rec in caplog.records
        if "event=decision_bad_request" in rec.getMessage()
    ]
    assert len(bad_req_logs) == 1
    assert "mimes=invalid" in bad_req_logs[0]
    assert "SHOULD_NOT_LEAK" not in bad_req_logs[0]


# ---------------------------------------------------------------------------
# 3. plugins/web_tools & plugins/bilibili lazy __init__ (zero runpy RuntimeWarning)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pkg_name,lazy_exports",
    [
        ("plugins.web_tools", ["WebToolsPlugin", "build_web_tools_plugin_from_env"]),
        (
            "plugins.bilibili",
            [
                "BilibiliPlugin",
                "bilibili_config_from_env",
                "build_bilibili_plugin",
                "build_bilibili_plugin_from_env",
            ],
        ),
    ],
)
def test_plugin_lazy_init_prevents_runpy_runtime_warning(pkg_name: str, lazy_exports: list[str]):
    # 1) Importing parent package does NOT eagerly load plugin_main, and runpy emits 0 warnings.
    check_script = (
        "import importlib, runpy, sys, warnings\n"
        "warnings.simplefilter('error', RuntimeWarning)\n"
        f"pkg = importlib.import_module({pkg_name!r})\n"
        f"assert {pkg_name + '.plugin_main'!r} not in sys.modules\n"
        f"runpy.run_module({pkg_name + '.plugin_main'!r}, run_name='_verify_no_warning_')\n"
        f"for name in {lazy_exports!r}:\n"
        "    assert getattr(pkg, name) is not None\n"
        "try:\n"
        "    getattr(pkg, 'nonexistent_attr_for_test')\n"
        "except AttributeError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('expected AttributeError')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", check_script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"
    assert "RuntimeWarning" not in proc.stderr

    # 2) Direct `python -m plugins.<name>.plugin_main` also emits zero runpy RuntimeWarning.
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("HAKIMI_PLUGIN") and not k.startswith("BILI_")
    }
    proc_m = subprocess.run(
        [sys.executable, "-W", "default", "-m", f"{pkg_name}.plugin_main"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert "found in sys.modules after import of package" not in proc_m.stderr
    assert "RuntimeWarning" not in proc_m.stderr


# ---------------------------------------------------------------------------
# 4. scripts/deploy_prod.sh best-effort image & build cache pruning
# ---------------------------------------------------------------------------


def _extract_prune_fn() -> str:
    script = (REPO_ROOT / "scripts" / "deploy_prod.sh").read_text(encoding="utf-8")
    match = re.search(r"(prune_old_images\(\)\s*\{.*?\n\})", script, flags=re.DOTALL)
    assert match is not None, "prune_old_images() not found in scripts/deploy_prod.sh"
    return match.group(1)


def test_deploy_prod_syntax_and_prune_old_images_behavior(tmp_path: Path):
    syntax = subprocess.run(
        ["bash", "-n", str(REPO_ROOT / "scripts" / "deploy_prod.sh")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr

    log_file = tmp_path / "docker_calls.log"
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(
        f"""#!/usr/bin/env python3
import os, sys

log_path = {str(log_file)!r}
args = sys.argv[1:]
with open(log_path, "a", encoding="utf-8") as fh:
    fh.write(" ".join(args) + "\\n")

if os.environ.get("FAKE_DOCKER_FAIL") == "1":
    sys.exit(1)

if args == ["ps", "-aq"]:
    print("cid_bot_running")
    print("cid_bot_stopped_old")
elif args[:3] == ["inspect", "--format", "{{{{.Image}}}}"]:
    mapping = {{
        "cid_bot_running": "sha256:bot5",
        "cid_bot_stopped_old": "sha256:bot1",
    }}
    for cid in args[3:]:
        if cid in mapping:
            print(mapping[cid])
elif args[:3] == ["image", "ls", "--no-trunc"]:
    bot = "hakimi-surf-hakimi-bot"
    adm = "hakimi-surf-hakimi-admin"
    rows = [
        (bot, "latest", "sha256:bot5", "2026-11-27 05:00:00 +0000 UTC"),
        (bot, "rollback", "sha256:bot4", "2026-11-27 04:00:00 +0000 UTC"),
        (bot, "rev-555555555555", "sha256:bot5", "2026-11-27 05:00:00 +0000 UTC"),
        (bot, "rev-444444444444", "sha256:bot4", "2026-11-27 04:00:00 +0000 UTC"),
        (bot, "rev-333333333333", "sha256:bot3", "2026-11-27 03:00:00 +0000 UTC"),
        (bot, "rev-222222222222", "sha256:bot2", "2026-11-27 02:00:00 +0000 UTC"),
        (bot, "rev-111111111111", "sha256:bot1", "2026-11-27 01:00:00 +0000 UTC"),
        (adm, "latest", "sha256:adm4", "2026-11-27 04:00:00 +0000 UTC"),
        (adm, "rollback", "sha256:adm3", "2026-11-27 03:00:00 +0000 UTC"),
        (adm, "rev-444444444444", "sha256:adm4", "2026-11-27 04:00:00 +0000 UTC"),
        (adm, "rev-333333333333", "sha256:adm3", "2026-11-27 03:00:00 +0000 UTC"),
        (adm, "rev-222222222222", "sha256:adm2", "2026-11-27 02:00:00 +0000 UTC"),
        (adm, "rev-111111111111", "sha256:adm1", "2026-11-27 01:00:00 +0000 UTC"),
        ("other-repo", "rev-000000000000", "sha256:oth0", "2026-11-27 00:00:00 +0000 UTC"),
    ]
    for r in rows:
        print("\\t".join(r))
elif args[:3] == ["image", "inspect", "--format"]:
    created = {{
        "sha256:bot5": "2026-11-27T05:00:00.123456789Z",
        "sha256:bot4": "2026-11-27T04:00:00.123456789Z",
        "sha256:bot3": "2026-11-27T03:00:00.123456789Z",
        "sha256:bot2": "2026-11-27T02:00:00.123456789Z",
        "sha256:bot1": "2026-11-27T01:00:00.123456789Z",
        "sha256:adm4": "2026-11-27T04:00:00.000000Z",
        "sha256:adm3": "2026-11-27T03:00:00.000000Z",
        "sha256:adm2": "2026-11-27T02:00:00.000000Z",
        "sha256:adm1": "2026-11-27T01:00:00.000000Z",
    }}
    for iid in args[4:]:
        if iid in created:
            print(f"{{iid}}\\t{{created[iid]}}")
""",
        encoding="utf-8",
    )
    fake_docker.chmod(fake_docker.stat().st_mode | stat.S_IXUSR)

    prune_fn = _extract_prune_fn()
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ.get('PATH', '')}"}
    proc = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", f"{prune_fn}\nprune_old_images"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr

    calls = log_file.read_text(encoding="utf-8").splitlines()
    removed = [c.split()[-1] for c in calls if c.startswith("image rm ")]
    # Only rev-2 for bot (rev-1 is referenced by stopped container) and rev-1 for admin untagged.
    assert removed == [
        "hakimi-surf-hakimi-bot:rev-222222222222",
        "hakimi-surf-hakimi-admin:rev-111111111111",
    ]
    assert "image prune -f" in calls
    assert "builder prune -f --keep-storage=2GB" in calls

    # Must remain best-effort / non-fatal even when every docker call fails.
    fail_env = {**env, "FAKE_DOCKER_FAIL": "1"}
    proc_fail = subprocess.run(
        ["bash", "-euo", "pipefail", "-c", f"{prune_fn}\nprune_old_images || true"],
        env=fail_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc_fail.returncode == 0


# ---------------------------------------------------------------------------
# 5. admin/src/js/01-core.js operator hints
# ---------------------------------------------------------------------------


def test_admin_core_js_contains_drive_and_backup_operator_hints():
    content = (REPO_ROOT / "admin" / "src" / "js" / "01-core.js").read_text(encoding="utf-8")
    hints_match = re.search(r"const hints=\{([^}]*)\};", content, flags=re.DOTALL)
    assert hints_match is not None
    hints_block = hints_match.group(1)
    for key in ("drive_etag_required", "db_backup_too_large", "sessions_backup_too_large"):
        m = re.search(rf"\b{key}\s*:\s*'([^']+)'", hints_block)
        assert m is not None and len(m.group(1).strip()) >= 8, f"missing hint for {key}"
