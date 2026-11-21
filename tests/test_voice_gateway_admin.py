"""Tests for Voice Gateway API configuration inside Voice Companion admin console."""

from __future__ import annotations

import base64
import hashlib
import re

import httpx
from pydantic import SecretStr

from hakimi.app import create_app
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_voice_companion import COMPANION_PATH, COMPANION_TOKEN
from voice_gateway.app import create_gateway_app
from voice_gateway.config import VoiceGatewayConfig

GW_TOKEN = "vg-admin-bridge-token-123456789012"
GW_PATH = "/ws/voice/0123456789abcdef0123456789abcdef"


def _gw_cfg() -> VoiceGatewayConfig:
    return VoiceGatewayConfig(
        ws_path=GW_PATH,
        gateway_token=GW_TOKEN,
        ws_url="ws://127.0.0.1:12141" + COMPANION_PATH,
        ws_token=COMPANION_TOKEN,
        stt_provider="siliconflow",
        tts_provider="siliconflow",
        siliconflow_api_key="sk-base-sf-secret-0000",
        ffmpeg_bin="/nonexistent-ffmpeg",
    )


async def test_voice_gateway_admin_e2e_persistence_resync_test_and_csp(
    admin_frontend, settings, monkeypatch
):
    brz_key = "brz_ui_vault_secret_888888"
    fish_key = "fish_ui_vault_secret_999999"

    def upstream_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/voices":
            return httpx.Response(
                200,
                json={
                    "voices": [
                        {"voice_id": "voc_official", "name": "官方女声", "voice_type": "default"},
                        {
                            "voice_id": "voc_psm85weeq82a",
                            "name": "机灵猫耳少女助手",
                            "voice_type": "personal",
                        },
                    ]
                },
            )
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, content=b"ID3\x03\x00\x00SYNTH-MP3")
        return httpx.Response(200, content=b"ID3\x03\x00\x00OK")

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

        async def _dispatch_to_gateway(request: httpx.Request) -> httpx.Response:
            if request.url.host == "hakimi-voice-gateway" and request.url.port == 12145:
                if request.url.path == "/v1/tts":
                    return httpx.Response(
                        200,
                        content=b"ID3\x03\x00\x00GATEWAY-MP3",
                        headers={
                            "content-type": "audio/mpeg",
                            "x-audio-format": "mp3",
                            "x-audio-duration-ms": "1234",
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
            raise AssertionError(f"Unexpected HTTP call: {request.url}")

        bridge_http = httpx.AsyncClient(transport=httpx.MockTransport(_dispatch_to_gateway))
        svc.voice_gateway_admin.http = bridge_http
        svc.voice_reply.http = bridge_http

        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=bot_app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as client:
                # 1. Auth required on all new routes
                for method, path in (
                    ("GET", "/admin/api/life/voice-companion/gateway"),
                    ("PUT", "/admin/api/life/voice-companion/gateway"),
                    ("DELETE", "/admin/api/life/voice-companion/gateway"),
                    ("GET", "/admin/api/life/voice-companion/gateway/voices"),
                    ("POST", "/admin/api/life/voice-companion/gateway/test"),
                ):
                    r_unauth = await client.request(method, path, headers={"Authorization": ""})
                    assert r_unauth.status_code == 401

                # 2. Initial status comes from env
                r_init = await client.get("/admin/api/life/voice-companion/gateway")
                assert r_init.status_code == 200
                d_init = r_init.json()
                assert d_init["configured"] is True
                assert d_init["reachable"] is True
                assert d_init["source"] == "env"
                assert d_init["gateway"]["tts_provider"] == "siliconflow"

                # 3. Gateway rejects invalid config -> 400 and SQLite untouched
                r_bad = await client.put(
                    "/admin/api/life/voice-companion/gateway",
                    json={
                        "stt_provider": "fish",
                        "tts_provider": "breezeblue",
                        "fish_api_key": fish_key,
                        "breezeblue_api_key": brz_key,
                        "breezeblue_voice_id": "",
                    },
                )
                assert r_bad.status_code == 400
                assert brz_key not in r_bad.text
                row_none = await svc.store.one(
                    "SELECT payload FROM admin_config WHERE key='voice_gateway_override'"
                )
                assert row_none is None

                # 4. Valid save -> encrypted in SQLite, hot-swaps live gateway, zero secret leak
                r_ok = await client.put(
                    "/admin/api/life/voice-companion/gateway",
                    json={
                        "stt_provider": "fish",
                        "tts_provider": "breezeblue",
                        "tts_speed": 1.05,
                        "fish_api_key": fish_key,
                        "breezeblue_api_key": brz_key,
                        "breezeblue_voice_id": "voc_psm85weeq82a",
                        "breezeblue_model_id": "breeze-tts-2",
                        "breezeblue_language_code": "zh",
                        "breezeblue_instructions": "温柔俏皮一点",
                        "breezeblue_guidance_scale": 2.0,
                    },
                )
                assert r_ok.status_code == 200, r_ok.text
                d_ok = r_ok.json()
                assert d_ok["source"] == "sqlite"
                assert d_ok["saved_keys"]["breezeblue"] is True
                assert d_ok["saved_keys"]["fish"] is True
                assert d_ok["gateway"]["tts_provider"] == "breezeblue"
                assert d_ok["gateway"]["breezeblue"]["configured"] is True
                assert brz_key not in r_ok.text and fish_key not in r_ok.text
                assert gw_app1.state.config.tts_provider == "breezeblue"

                # Verify sealed blob in SQLite is encrypted (no plaintext key on disk)
                row_blob = await svc.store.one(
                    "SELECT payload FROM admin_config WHERE key='voice_gateway_override'"
                )
                assert row_blob is not None and brz_key.encode() not in row_blob[0]

                # Blank API keys preserve vaulted secrets
                r_keep = await client.put(
                    "/admin/api/life/voice-companion/gateway",
                    json={
                        "stt_provider": "fish",
                        "tts_provider": "breezeblue",
                        "breezeblue_voice_id": "voc_psm85weeq82a",
                        "breezeblue_instructions": "微调语气",
                    },
                )
                assert r_keep.status_code == 200
                assert r_keep.json()["saved_keys"]["breezeblue"] is True
                assert gw_app1.state.config.breezeblue_api_key == brz_key

                # 5. Fetch BreezeBlue voices & synthesize test
                r_voices = await client.get("/admin/api/life/voice-companion/gateway/voices")
                assert r_voices.status_code == 200
                assert [v["voice_id"] for v in r_voices.json()["voices"]] == [
                    "voc_psm85weeq82a",
                    "voc_official",
                ]

                r_test = await client.post("/admin/api/life/voice-companion/gateway/test")
                assert r_test.status_code == 200
                assert r_test.json()["ok"] is True
                assert r_test.json()["duration_ms"] == 1234

                # 6. Simulated gateway restart auto-resyncs SQLite override
                gw_app2 = create_gateway_app(_gw_cfg())
                async with gw_app2.router.lifespan_context(gw_app2):
                    gw_holder["app"] = gw_app2
                    assert gw_app2.state.config.tts_provider == "siliconflow"
                    r_resync = await client.get("/admin/api/life/voice-companion/gateway")
                    assert r_resync.status_code == 200
                    assert r_resync.json()["gateway"]["tts_provider"] == "breezeblue"
                    assert gw_app2.state.config.tts_provider == "breezeblue"
                    assert gw_app2.state.config.breezeblue_api_key == brz_key

                    # 7. DELETE resets SQLite and restores gateway .env default
                    r_del = await client.delete("/admin/api/life/voice-companion/gateway")
                    assert r_del.status_code == 200
                    assert r_del.json()["source"] == "env"
                    assert r_del.json()["gateway"]["tts_provider"] == "siliconflow"
                    assert gw_app2.state.config.tts_provider == "siliconflow"

                # 8. /admin HTML CSP & DOM security invariants
                page = await client.get("/admin", headers={"Authorization": ""})
                assert page.status_code == 200
                html = page.text
                styles = re.findall(r"<style>(.*?)</style>", html, re.S)
                scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
                assert len(styles) == 1 and len(scripts) == 1
                style_hash = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
                script_hash = base64.b64encode(
                    hashlib.sha256(scripts[0].encode()).digest()
                ).decode()
                csp = page.headers["content-security-policy"]
                assert f"'sha256-{style_hash}'" in csp
                assert f"'sha256-{script_hash}'" in csp
                assert not re.search(r"\sstyle\s*=", html)
                assert not re.search(r"\son[a-z]+\s*=", html, re.I)
                assert "innerHTML" not in html
                for marker in (
                    'id="vc-gw-form"',
                    'id="vc-gw-brz-fetch-voices"',
                    'id="vc-gw-test"',
                    'id="vc-gw-tts-provider"',
                ):
                    assert marker in html
        finally:
            await bridge_http.aclose()
