"""Tests for secret sync, bucketed extra-secrets isolation, and voice gateway lock safety."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.bili_bridge import RemoteBiliWatcher
from hakimi.config import Settings
from hakimi.provider_routing import ProviderCredentialsUpdate, ProviderRoutingManager
from hakimi.security import (
    Rejected,
    configured_secrets,
    safe_output,
    set_runtime_secret_bucket,
)
from hakimi.service import Service
from hakimi.store import Store
from hakimi.voice_gateway_admin import VoiceGatewayAdminManager, VoiceGatewayConfigUpdate
from tests.conftest import cached_rsa_private_key_pem
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN

GW_TOKEN = "vg-lock-test-token-12345678901234"
COLAB_TOKEN = "colab-worker-secret-token-987654321"


def _make_sa_json() -> str:
    return json.dumps(
        {
            "type": "service_account",
            "project_id": "hakimi-sec-test",
            "private_key_id": "kid-1",
            "private_key": cached_rsa_private_key_pem(),
            "client_email": "hakimi-bot@hakimi-sec-test.iam.gserviceaccount.com",
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    )


def test_colab_worker_token_and_all_secret_fields_in_configured_secrets(settings: Settings) -> None:
    secret_fields = [
        name for name, field in Settings.model_fields.items() if field.annotation is SecretStr
    ]
    assert "colab_worker_token" in secret_fields

    updates: dict[str, SecretStr] = {}
    for idx, name in enumerate(secret_fields):
        if name == "encryption_key":
            continue
        updates[name] = SecretStr(f"secret-field-{idx:02d}-{name}-value")
    updates["colab_worker_token"] = SecretStr(COLAB_TOKEN)

    cfg = settings.model_copy(update=updates)
    secrets = configured_secrets(cfg)
    assert secrets[0] == updates["gemini_api_key"].get_secret_value()
    assert COLAB_TOKEN in secrets
    assert cfg.encryption_key.get_secret_value() in secrets
    for name, sec in updates.items():
        assert sec.get_secret_value() in secrets, f"{name} missing from configured_secrets"

    with pytest.raises(Rejected, match="invalid_output"):
        safe_output(f"leaked token {COLAB_TOKEN} here", secrets)


def test_runtime_secret_buckets_isolation_legacy_compat_and_model_copy(settings: Settings) -> None:
    settings._runtime_extra_secrets = ("legacy-direct-secret-000",)
    set_runtime_secret_bucket(settings, "provider_routing", ["prov-free-1111", "prov-vtx-2222"])
    set_runtime_secret_bucket(settings, "bilibili", ["bili-sess-3333", "bili-b4-4444"])
    set_runtime_secret_bucket(settings, "voice_gateway", ["vg-fish-5555", "vg-brz-6666"])

    secrets = configured_secrets(settings)
    assert secrets[0] == settings.gemini_api_key.get_secret_value()
    for expected in (
        "legacy-direct-secret-000",
        "prov-free-1111",
        "prov-vtx-2222",
        "bili-sess-3333",
        "bili-b4-4444",
        "vg-fish-5555",
        "vg-brz-6666",
    ):
        assert expected in secrets

    # Overwriting or clearing one bucket must never clobber other buckets or legacy extras.
    set_runtime_secret_bucket(settings, "provider_routing", ["prov-free-new-9999"])
    secrets_after_update = configured_secrets(settings)
    assert "prov-free-new-9999" in secrets_after_update
    assert "prov-free-1111" not in secrets_after_update
    assert "prov-vtx-2222" not in secrets_after_update
    for preserved in (
        "legacy-direct-secret-000",
        "bili-sess-3333",
        "bili-b4-4444",
        "vg-fish-5555",
        "vg-brz-6666",
    ):
        assert preserved in secrets_after_update

    set_runtime_secret_bucket(settings, "voice_gateway", ())
    secrets_after_clear = configured_secrets(settings)
    assert "vg-fish-5555" not in secrets_after_clear
    assert "vg-brz-6666" not in secrets_after_clear
    assert "bili-sess-3333" in secrets_after_clear
    assert "prov-free-new-9999" in secrets_after_clear
    assert "legacy-direct-secret-000" in secrets_after_clear

    # model_copy must not cross-contaminate original settings buckets.
    copied = settings.model_copy()
    set_runtime_secret_bucket(copied, "bilibili", ["copied-only-cookie-7777"])
    assert "copied-only-cookie-7777" in configured_secrets(copied)
    assert "copied-only-cookie-7777" not in configured_secrets(settings)
    assert "bili-sess-3333" in configured_secrets(settings)

    with pytest.raises(ValueError):
        set_runtime_secret_bucket(settings, "   ", ["abc12345"])


async def test_provider_bili_and_voice_gateway_buckets_do_not_clobber_each_other(
    settings: Settings, store: Store
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    store.s = cfg
    store.runtime.s = cfg
    svc = Service(cfg, store, FakeGateway(), FakeBili(), FakeSender())
    ref = svc.secrets

    # 1. ProviderRoutingManager writes provider_routing bucket
    routing = ProviderRoutingManager(store)
    await routing.update_credentials(
        ProviderCredentialsUpdate(
            gemini_free_api_key="free-bucket-secret-1111",
            vertex_api_key="vtx-bucket-secret-2222",
            gcp_service_account_json=_make_sa_json(),
        )
    )
    svc.refresh_secrets()
    assert svc.secrets is ref

    # 2. RemoteBiliWatcher writes bilibili bucket and refreshes svc.secrets
    watcher = RemoteBiliWatcher(svc)
    await watcher._kv_set(
        "credentials",
        {
            "creds": {
                "sessdata": "bili-sess-bucket-3333",
                "bili_jct": "bili-jct-bucket-4444",
                "buvid3": "bili-b3-bucket-5555",
                "buvid4": "",
            },
            "source": "sqlite",
        },
    )
    await watcher.initialize()
    assert svc.secrets is ref

    # 3. VoiceGatewayAdminManager writes voice_gateway bucket (all 4 keys)
    def gw_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"overridden": True, "tts_provider": "breezeblue"})
        if request.method == "DELETE":
            return httpx.Response(200, json={"overridden": False})
        return httpx.Response(200, json={"overridden": False, "tts_provider": "siliconflow"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(gw_handler)) as gw_http:
        gw = VoiceGatewayAdminManager(cfg, store, http=gw_http)
        await gw.update(
            VoiceGatewayConfigUpdate(
                stt_provider="openai",
                tts_provider="breezeblue",
                siliconflow_api_key="sf-vg-secret-6666",
                fish_api_key="fish-vg-secret-7777",
                breezeblue_api_key="brz-vg-secret-8888",
                openai_stt_api_key="oai-vg-secret-9999",
            )
        )
        svc.refresh_secrets()
        assert svc.secrets is ref

        all_expected = (
            "free-bucket-secret-1111",
            "vtx-bucket-secret-2222",
            "bili-sess-bucket-3333",
            "bili-jct-bucket-4444",
            "bili-b3-bucket-5555",
            "sf-vg-secret-6666",
            "fish-vg-secret-7777",
            "brz-vg-secret-8888",
            "oai-vg-secret-9999",
        )
        for sec in all_expected:
            assert sec in configured_secrets(cfg)
            assert sec in svc.secrets

        # Clearing provider_routing must NOT clobber bilibili or voice_gateway buckets.
        await routing.update_credentials(ProviderCredentialsUpdate(clear=True))
        svc.refresh_secrets()
        assert "free-bucket-secret-1111" not in svc.secrets
        assert "vtx-bucket-secret-2222" not in svc.secrets
        for still_present in (
            "bili-sess-bucket-3333",
            "bili-jct-bucket-4444",
            "bili-b3-bucket-5555",
            "sf-vg-secret-6666",
            "fish-vg-secret-7777",
            "brz-vg-secret-8888",
            "oai-vg-secret-9999",
        ):
            assert still_present in configured_secrets(cfg)
            assert still_present in svc.secrets

        # Partial update_credentials on RemoteBiliWatcher preserves store cookies + submitted cookie
        fake_proxy = SimpleNamespace(
            enabled=True,
            _session=SimpleNamespace(ready=True, closed=False),
        )

        async def _fake_rpc(method: str, payload: dict):
            assert method == "bili.account.update_credentials"
            return {
                "credentials": {
                    "sessdata": True,
                    "bili_jct": True,
                    "buvid3": True,
                    "buvid4": True,
                }
            }

        fake_proxy.call_raw_rpc = _fake_rpc
        watcher._require_online_proxy = lambda **_: fake_proxy  # type: ignore[method-assign]
        await watcher.update_credentials({"buvid4": "bili-b4-partial-1010"})
        for cookie in (
            "bili-sess-bucket-3333",
            "bili-jct-bucket-4444",
            "bili-b3-bucket-5555",
            "bili-b4-partial-1010",
        ):
            assert cookie in configured_secrets(cfg)
            assert cookie in svc.secrets

        # Resetting VoiceGatewayAdminManager clears ONLY voice_gateway bucket
        await gw.reset()
        svc.refresh_secrets()
        for removed in (
            "sf-vg-secret-6666",
            "fish-vg-secret-7777",
            "brz-vg-secret-8888",
            "oai-vg-secret-9999",
        ):
            assert removed not in configured_secrets(cfg)
            assert removed not in svc.secrets
        assert "bili-sess-bucket-3333" in svc.secrets
        assert "bili-b4-partial-1010" in svc.secrets


async def test_admin_routes_and_startup_sync_service_secrets(settings: Settings) -> None:
    gw_state = {"overridden": False, "tts_provider": "siliconflow"}

    def gw_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            gw_state["overridden"] = True
            gw_state["tts_provider"] = body.get("tts_provider", "breezeblue")
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "DELETE":
            gw_state["overridden"] = False
            gw_state["tts_provider"] = "siliconflow"
            return httpx.Response(200, json=dict(gw_state))
        return httpx.Response(200, json=dict(gw_state))

    base_settings = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "colab_worker_token": SecretStr(COLAB_TOKEN),
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )

    sa_json = _make_sa_json()
    sa_pk = json.loads(sa_json)["private_key"].strip()

    app1 = create_app(base_settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app1.router.lifespan_context(app1):
        svc1 = app1.state.service
        ref1 = svc1.secrets
        assert COLAB_TOKEN in svc1.secrets
        gw_http1 = httpx.AsyncClient(transport=httpx.MockTransport(gw_handler))
        svc1.voice_gateway_admin.http = gw_http1
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app1),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as client:
                # 1. Provider credentials route calls svc.refresh_secrets() in-place
                r_prov = await client.post(
                    "/admin/api/providers/credentials",
                    json={
                        "gemini_free_api_key": "route-free-key-1111",
                        "vertex_api_key": "route-vtx-key-2222",
                        "gcp_service_account_json": sa_json,
                    },
                )
                assert r_prov.status_code == 200
                assert svc1.secrets is ref1
                assert "route-free-key-1111" in svc1.secrets
                assert "route-vtx-key-2222" in svc1.secrets
                assert sa_pk in svc1.secrets

                # 2. DeepSeek route calls svc.refresh_secrets() in-place
                r_ds = await client.post(
                    "/admin/api/web-search/deepseek",
                    json={
                        "api_key": "sk-route-deepseek-3333",
                        "base_url": "https://api.deepseek.com",
                        "model": "deepseek-flash",
                    },
                )
                assert r_ds.status_code == 200
                assert svc1.secrets is ref1
                assert "sk-route-deepseek-3333" in svc1.secrets
                with pytest.raises(Rejected, match="invalid_output"):
                    svc1.output("leaked sk-route-deepseek-3333")

                # 3. Voice gateway PUT route syncs all 4 keys into svc.secrets in-place
                r_vg = await client.put(
                    "/admin/api/life/voice-companion/gateway",
                    json={
                        "stt_provider": "openai",
                        "tts_provider": "breezeblue",
                        "siliconflow_api_key": "sf-route-secret-4444",
                        "fish_api_key": "fish-route-secret-5555",
                        "breezeblue_api_key": "brz-route-secret-6666",
                        "openai_stt_api_key": "oai-route-secret-7777",
                        "breezeblue_voice_id": "voc_1",
                    },
                )
                assert r_vg.status_code == 200
                assert svc1.secrets is ref1
                for sec in (
                    "route-free-key-1111",
                    "route-vtx-key-2222",
                    sa_pk,
                    "sk-route-deepseek-3333",
                    "sf-route-secret-4444",
                    "fish-route-secret-5555",
                    "brz-route-secret-6666",
                    "oai-route-secret-7777",
                ):
                    assert sec in svc1.secrets
                    with pytest.raises(Rejected, match="invalid_output"):
                        svc1.output(f"leak {sec}")
        finally:
            await gw_http1.aclose()

    # 4. Restart with clean Settings pointing to same data_dir: boot MUST hydrate svc.secrets!
    fresh_settings = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "colab_worker_token": SecretStr(COLAB_TOKEN),
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    app2 = create_app(fresh_settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app2.router.lifespan_context(app2):
        svc2 = app2.state.service
        ref2 = svc2.secrets
        for restored in (
            COLAB_TOKEN,
            "route-free-key-1111",
            "route-vtx-key-2222",
            sa_pk,
            "sk-route-deepseek-3333",
            "sf-route-secret-4444",
            "fish-route-secret-5555",
            "brz-route-secret-6666",
            "oai-route-secret-7777",
        ):
            assert restored in svc2.secrets, f"Not restored on boot: {restored[:24]}"
            with pytest.raises(Rejected, match="invalid_output"):
                svc2.output(f"leak {restored}")

        # 5. DELETE /voice-companion/gateway removes voice gateway secrets & keeps others
        gw_http2 = httpx.AsyncClient(transport=httpx.MockTransport(gw_handler))
        svc2.voice_gateway_admin.http = gw_http2
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app2),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {TOKEN}"},
            ) as client2:
                r_del = await client2.delete("/admin/api/life/voice-companion/gateway")
                assert r_del.status_code == 200
                assert svc2.secrets is ref2
                for cleared in (
                    "sf-route-secret-4444",
                    "fish-route-secret-5555",
                    "brz-route-secret-6666",
                    "oai-route-secret-7777",
                ):
                    assert cleared not in svc2.secrets
                assert "route-free-key-1111" in svc2.secrets
                assert "sk-route-deepseek-3333" in svc2.secrets

                r_ds_clear = await client2.post(
                    "/admin/api/web-search/deepseek",
                    json={"clear": True},
                )
                assert r_ds_clear.status_code == 200
                assert svc2.secrets is ref2
                assert "sk-route-deepseek-3333" not in svc2.secrets
                assert "route-free-key-1111" in svc2.secrets
        finally:
            await gw_http2.aclose()


async def test_voice_gateway_status_resync_does_not_overwrite_concurrent_reset(
    settings: Settings, store: Store
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    gw_state: dict[str, object] = {"overridden": False, "tts_provider": "siliconflow"}
    calls: list[tuple[str, str]] = []
    first_get_entered = asyncio.Event()
    release_first_get = asyncio.Event()
    get_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_count
        calls.append((request.method, request.url.path))
        if request.method == "GET":
            get_count += 1
            if get_count == 1:
                first_get_entered.set()
                await release_first_get.wait()
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "POST":
            body = json.loads(request.content)
            gw_state["overridden"] = True
            gw_state["tts_provider"] = body.get("tts_provider")
            gw_state["breezeblue_instructions"] = body.get("breezeblue_instructions")
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "DELETE":
            gw_state["overridden"] = False
            gw_state["tts_provider"] = "siliconflow"
            gw_state.pop("breezeblue_instructions", None)
            return httpx.Response(200, json=dict(gw_state))
        raise AssertionError(f"Unexpected {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        mgr = VoiceGatewayAdminManager(cfg, store, http=http)
        await mgr.update(
            VoiceGatewayConfigUpdate(
                stt_provider="siliconflow",
                tts_provider="breezeblue",
                breezeblue_api_key="brz-race-reset-secret-1111",
                breezeblue_voice_id="voc_1",
                breezeblue_instructions="before-reset",
            )
        )
        # Simulate gateway restart losing in-memory override
        gw_state["overridden"] = False
        gw_state["tts_provider"] = "siliconflow"
        gw_state.pop("breezeblue_instructions", None)
        calls.clear()
        get_count = 0

        # Start status(); pause it mid initial GET (which will observe overridden=False)
        status_task = asyncio.create_task(mgr.status())
        await first_get_entered.wait()

        # Run reset() concurrently while status() is in flight
        reset_result = await mgr.reset()
        assert reset_result["source"] == "env"
        assert mgr._override is None

        # Release status()'s initial GET; inside _op_lock it must see _override=None and NOT POST!
        release_first_get.set()
        status_result = await status_task

        assert ("POST", "/v1/admin/config") not in calls
        assert gw_state["overridden"] is False
        assert gw_state["tts_provider"] == "siliconflow"
        assert status_result["source"] == "env"
        assert status_result["saved_keys"]["breezeblue"] is False
        assert "brz-race-reset-secret-1111" not in configured_secrets(cfg)


async def test_voice_gateway_status_resync_does_not_overwrite_concurrent_update(
    settings: Settings, store: Store
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    gw_state: dict[str, object] = {"overridden": False, "tts_provider": "siliconflow"}
    posted_payloads: list[dict] = []
    first_get_entered = asyncio.Event()
    release_first_get = asyncio.Event()
    get_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_count
        if request.method == "GET":
            get_count += 1
            if get_count == 1:
                # Capture stale overridden=False snapshot before update(v2) runs
                snapshot = dict(gw_state)
                first_get_entered.set()
                await release_first_get.wait()
                return httpx.Response(200, json=snapshot)
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "POST":
            body = json.loads(request.content)
            posted_payloads.append(body)
            gw_state["overridden"] = True
            gw_state["tts_provider"] = body.get("tts_provider")
            gw_state["breezeblue_instructions"] = body.get("breezeblue_instructions")
            return httpx.Response(200, json=dict(gw_state))
        raise AssertionError(f"Unexpected {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        mgr = VoiceGatewayAdminManager(cfg, store, http=http)
        await mgr.update(
            VoiceGatewayConfigUpdate(
                stt_provider="siliconflow",
                tts_provider="breezeblue",
                breezeblue_api_key="brz-v1-secret-1111",
                breezeblue_voice_id="voc_1",
                breezeblue_instructions="v1-old",
            )
        )
        # Simulate gateway restart losing in-memory override
        gw_state["overridden"] = False
        gw_state["tts_provider"] = "siliconflow"
        gw_state.pop("breezeblue_instructions", None)
        posted_payloads.clear()
        get_count = 0

        # Start status(); it observes stale overridden=False before update(v2) runs
        status_task = asyncio.create_task(mgr.status())
        await first_get_entered.wait()

        # Concurrent update(v2) pushes newer config to gateway
        await mgr.update(
            VoiceGatewayConfigUpdate(
                stt_provider="fish",
                tts_provider="fish",
                fish_api_key="fish-v2-secret-2222",
                breezeblue_instructions="v2-new",
            )
        )
        assert len(posted_payloads) == 1
        assert posted_payloads[0]["tts_provider"] == "fish"

        # Let status() finish: inside _op_lock it re-checks GET (overridden=True) and skips POST.
        release_first_get.set()
        status_result = await status_task

        assert len(posted_payloads) == 1
        assert gw_state["tts_provider"] == "fish"
        assert gw_state["breezeblue_instructions"] == "v2-new"
        assert status_result["gateway"]["tts_provider"] == "fish"
        assert status_result["saved_keys"]["fish"] is True
        assert status_result["saved_keys"]["breezeblue"] is True


async def test_voice_gateway_concurrent_updates_and_reset_serialize_prev_merge(
    settings: Settings, store: Store
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    first_post_entered = asyncio.Event()
    release_first_post = asyncio.Event()
    reset_delete_entered = asyncio.Event()
    release_reset_delete = asyncio.Event()
    post_bodies: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            post_bodies.append(body)
            if len(post_bodies) == 1:
                first_post_entered.set()
                await release_first_post.wait()
            return httpx.Response(
                200, json={"overridden": True, "tts_provider": body["tts_provider"]}
            )
        if request.method == "DELETE":
            reset_delete_entered.set()
            await release_reset_delete.wait()
            return httpx.Response(200, json={"overridden": False, "tts_provider": "siliconflow"})
        return httpx.Response(200, json={"overridden": False, "tts_provider": "siliconflow"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        mgr = VoiceGatewayAdminManager(cfg, store, http=http)

        # Call A saves fish_api_key and pauses inside _op_lock during POST
        task_a = asyncio.create_task(
            mgr.update(
                VoiceGatewayConfigUpdate(
                    stt_provider="fish",
                    tts_provider="fish",
                    fish_api_key="fish-concurrent-secret-1111",
                )
            )
        )
        await first_post_entered.wait()

        # Call B starts while Call A holds _op_lock and submits only breezeblue_api_key (fish blank)
        task_b = asyncio.create_task(
            mgr.update(
                VoiceGatewayConfigUpdate(
                    stt_provider="fish",
                    tts_provider="breezeblue",
                    breezeblue_api_key="brz-concurrent-secret-2222",
                    breezeblue_voice_id="voc_1",
                )
            )
        )
        # Let task_b reach _op_lock before releasing task_a
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release_first_post.set()
        await asyncio.gather(task_a, task_b)

        # Call B must read Call A's saved fish_api_key inside _op_lock rather than stale prev.
        assert len(post_bodies) == 2
        assert post_bodies[1]["fish_api_key"] == "fish-concurrent-secret-1111"
        assert post_bodies[1]["breezeblue_api_key"] == "brz-concurrent-secret-2222"
        assert mgr._saved_keys()["fish"] is True
        assert mgr._saved_keys()["breezeblue"] is True
        assert "fish-concurrent-secret-1111" in configured_secrets(cfg)
        assert "brz-concurrent-secret-2222" in configured_secrets(cfg)

        # Now start reset() and pause it inside _op_lock during DELETE;
        # a concurrent update() with blank keys must NOT resurrect cleared keys!
        reset_task = asyncio.create_task(mgr.reset())
        await reset_delete_entered.wait()

        update_after_reset = asyncio.create_task(
            mgr.update(
                VoiceGatewayConfigUpdate(
                    stt_provider="siliconflow",
                    tts_provider="edge",
                )
            )
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release_reset_delete.set()
        await asyncio.gather(reset_task, update_after_reset)

        assert len(post_bodies) == 3
        assert post_bodies[2]["fish_api_key"] == ""
        assert post_bodies[2]["breezeblue_api_key"] == ""
        assert mgr._saved_keys()["fish"] is False
        assert mgr._saved_keys()["breezeblue"] is False
        assert "fish-concurrent-secret-1111" not in configured_secrets(cfg)
        assert "brz-concurrent-secret-2222" not in configured_secrets(cfg)


async def test_voice_gateway_legacy_vault_context_migrates_and_syncs_secrets(
    settings: Settings, store: Store
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GW_TOKEN),
        }
    )
    sealed = store.vault.seal(
        "voice-gateway-override:v1",
        {
            "stt_provider": "siliconflow",
            "tts_provider": "breezeblue",
            "breezeblue_api_key": "brz-legacy-ctx-secret-9999",
        },
    )
    async with store.transaction():
        await store.db.execute(
            "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
            ("voice_gateway_override", sealed),
        )

    mgr = VoiceGatewayAdminManager(cfg, store)
    await mgr.initialize()
    assert mgr._saved_keys()["breezeblue"] is True
    assert "brz-legacy-ctx-secret-9999" in configured_secrets(cfg)

    # Row should now be transparently re-sealed under admin:voice_gateway_override
    row = await store.one("SELECT payload FROM admin_config WHERE key='voice_gateway_override'")
    assert row is not None
    opened = store.vault.open("admin:voice_gateway_override", row[0])
    assert opened["breezeblue_api_key"] == "brz-legacy-ctx-secret-9999"


async def test_create_app_boot_migrates_legacy_voice_gateway_override_and_hydrates_secrets(
    settings: Settings,
) -> None:
    cfg = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    seed_store = Store(cfg)
    await seed_store.open()
    try:
        sealed = seed_store.vault.seal(
            "voice-gateway-override:v1",
            {
                "stt_provider": "fish",
                "tts_provider": "breezeblue",
                "fish_api_key": "fish-legacy-boot-secret-7777",
                "breezeblue_api_key": "brz-legacy-boot-secret-8888",
            },
        )
        async with seed_store.transaction():
            await seed_store.db.execute(
                "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
                ("voice_gateway_override", sealed),
            )
    finally:
        await seed_store.close()

    # Booting create_app() must open legacy row via runtime_state fallback, migrate it,
    # and hydrate service.secrets on startup.
    app = create_app(cfg, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert "fish-legacy-boot-secret-7777" in svc.secrets
        assert "brz-legacy-boot-secret-8888" in svc.secrets
        with pytest.raises(Rejected, match="invalid_output"):
            svc.output("leak brz-legacy-boot-secret-8888")

        row = await svc.store.one(
            "SELECT payload FROM admin_config WHERE key='voice_gateway_override'"
        )
        assert row is not None
        migrated = svc.store.vault.open("admin:voice_gateway_override", row[0])
        assert migrated["fish_api_key"] == "fish-legacy-boot-secret-7777"
        assert migrated["breezeblue_api_key"] == "brz-legacy-boot-secret-8888"

    # A wrong context that is neither admin:* nor voice-gateway-override:v1 must still fail.
    bad_store = Store(cfg)
    await bad_store.open()
    try:
        bad_sealed = bad_store.vault.seal("totally-wrong-ctx", {"tts_provider": "edge"})
        async with bad_store.transaction():
            await bad_store.db.execute(
                "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
                ("voice_gateway_override", bad_sealed),
            )
    finally:
        await bad_store.close()

    reopen = Store(cfg)
    with pytest.raises(Rejected, match="ciphertext_context_mismatch"):
        await reopen.open()
    await reopen.close()
