"""Comprehensive unit and integration tests for P2 remote social chat parity."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.admin_policy_routes import GroupActivity, GroupPolicy, allowed_activity_group
from hakimi.app import create_app
from hakimi.models import Message
from hakimi.plugin_bus import RemoteModuleProxy
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected
from hakimi.social import OwnerBinding
from hakimi.social_chat_config import (
    RemoteChatConfig,
    delete_remote_chat_config,
    load_all_remote_chat_configs,
    load_remote_chat_config,
    save_remote_chat_config,
    validate_remote_chat_platform,
)
from plugins.telegram import TelegramPlugin
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_plugin_telegram import (
    BOT_ID,
    BOT_TOKEN,
    FRIEND_UID,
    GROUP_ID,
    OTHER_GROUP_ID,
    OWNER_UID,
    STRANGER_UID,
    _FakeTelegramApi,
    _LoopbackBridge,
    _wait_until,
)

THIRD_GROUP_ID = "-1007778889990"


async def _connect_tg_plugin(
    svc: Any,
    fake_api: _FakeTelegramApi,
    *,
    owner_user_id: str = OWNER_UID,
    allowed_groups: set[str] | None = None,
    proactive_groups: set[str] | None = None,
    allowed_users: set[str] | None = None,
    private_mode: str = "owner_only",
    reuse_token: str | None = None,
) -> tuple[TelegramPlugin, _LoopbackBridge, asyncio.Task[None], str]:
    bus = svc.plugin_bus
    if reuse_token is None:
        token, _ = await bus.create_plugin(
            "telegram",
            "Telegram 社交",
            ["chat", "media", "sensors"],
            node_region="KR",
        )
    else:
        token = reuse_token
    bridge = _LoopbackBridge(bus, "telegram")
    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        bot_token=BOT_TOKEN,
        owner_user_id=owner_user_id,
        allowed_users=set(allowed_users or set()),
        allowed_groups=set(allowed_groups if allowed_groups is not None else {GROUP_ID}),
        proactive_groups=proactive_groups,
        private_mode=private_mode,  # type: ignore[arg-type]
        api=fake_api,  # type: ignore[arg-type]
        connect_factory=bridge.connect_factory,
        enable_polling=False,
    )
    runner = asyncio.create_task(plugin.client.run_forever())
    await _wait_until(
        lambda: (
            plugin.client.connected
            and bus.is_plugin_online("telegram")
            and bool(plugin.bot_id)
            and bool(bus.get_chat_proxy("telegram").status_dict().get("bot_id"))
        )
    )
    return plugin, bridge, runner, token


async def _disconnect_tg_plugin(
    plugin: TelegramPlugin, bridge: _LoopbackBridge, runner: asyncio.Task[None]
) -> None:
    await plugin.close()
    await bridge.shutdown()
    runner.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await runner


def _make_p2_settings(settings: Any, **overrides: Any) -> Any:
    base: dict[str, Any] = {
        "admin_token": SecretStr(TOKEN),
        "owner_qq_id": "20000",
        "onebot_enabled": True,
        "onebot_self_id": "10000",
        "onebot_allowed_groups": {"40000"},
        "qq_enabled": False,
        "owner_delivery_platform": "telegram",
        "social_owner_bindings": {
            "telegram": OwnerBinding(
                bot_id=BOT_ID,
                user_id=OWNER_UID,
                notebook=True,
                mirror=True,
            )
        },
    }
    base.update(overrides)
    return settings.model_copy(update=base)


# ---------------------------------------------------------------------------
# 1. social_chat_config validation & Vault persistence
# ---------------------------------------------------------------------------


def test_remote_chat_config_validation_and_proactive_intersection() -> None:
    cfg = RemoteChatConfig.model_validate(
        {
            "allowed_groups": ["-1002", "-1001", "-1002"],
            "proactive_groups": ["-1001", "-100999", "-1001"],
            "private_mode": "owner_and_allowlist",
            "allowed_users": ["222", "111", "222"],
            "memory_mirror_enabled": True,
        }
    )
    assert cfg.allowed_groups == ("-1001", "-1002")
    assert cfg.proactive_groups == ("-1001",)
    assert cfg.allowed_users == ("111", "222")
    assert cfg.private_mode == "owner_and_allowlist"
    assert cfg.memory_mirror_enabled is True

    # Invalid IDs, >100 items, unknown fields, non-bool mirror, reserved platforms
    for bad in (
        {"allowed_groups": ["abc"]},
        {"allowed_groups": ["-100123\n"]},
        {"allowed_groups": [" -100123 "]},
        {"allowed_groups": ["1" * 25]},
        {"allowed_groups": [12345]},
        {"allowed_groups": [str(i) for i in range(101)]},
        {"private_mode": "everyone"},
        {"memory_mirror_enabled": "yes"},
        {"extra_key": 1},
    ):
        with pytest.raises(ValidationError):
            RemoteChatConfig.model_validate(bad)

    for bad_plat in ("onebot", "qq", "Telegram", "../bad", "123", ""):
        with pytest.raises(Rejected, match="invalid_platform"):
            validate_remote_chat_platform(bad_plat)
    assert validate_remote_chat_platform("telegram") == "telegram"


async def test_remote_chat_config_vault_encryption_and_policy_seeding(store: Any) -> None:
    store.s.group_policies[f"telegram:{GROUP_ID}"] = "smart_reply"
    cfg = RemoteChatConfig(
        allowed_groups=[GROUP_ID, OTHER_GROUP_ID],  # type: ignore[arg-type]
        proactive_groups=[GROUP_ID],  # type: ignore[arg-type]
        private_mode="owner_and_allowlist",
        allowed_users=[FRIEND_UID],  # type: ignore[arg-type]
        memory_mirror_enabled=True,
    )
    saved = await save_remote_chat_config(store, "telegram", cfg, settings=store.s)
    assert saved.allowed_groups == tuple(sorted([GROUP_ID, OTHER_GROUP_ID]))

    # Raw payload in SQLite must be Vault-encrypted (no plaintext group/user IDs)
    row = await store.one(
        "SELECT payload FROM admin_config WHERE key=?", ("remote_chat_config:telegram",)
    )
    assert row is not None
    assert GROUP_ID.encode() not in row["payload"]
    assert FRIEND_UID.encode() not in row["payload"]

    # Existing policy preserved while new group seeded with default "at_only"
    assert store.s.group_policies[f"telegram:{GROUP_ID}"] == "smart_reply"
    assert store.s.group_policies[f"telegram:{OTHER_GROUP_ID}"] == "at_only"

    # RuntimeState.load_config() must cleanly open admin_config without Vault context mismatch
    await store.runtime.load_config()
    assert store.s.group_policies[f"telegram:{OTHER_GROUP_ID}"] == "at_only"

    loaded = await load_remote_chat_config(store, "telegram", settings=store.s)
    assert loaded == saved
    all_loaded = await load_all_remote_chat_configs(store, settings=store.s)
    assert all_loaded == {"telegram": saved}

    assert await delete_remote_chat_config(store, "telegram", settings=store.s) is True
    assert await load_remote_chat_config(store, "telegram", settings=store.s) is None


# ---------------------------------------------------------------------------
# 2. Admin API: social-chat/{platform}/config + hot-push + handshake auto-push
# ---------------------------------------------------------------------------


async def test_social_chat_config_api_offline_handshake_push_and_hot_reload(
    settings: Any,
) -> None:
    s = _make_p2_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            # Reserved / invalid platforms rejected with 400
            for bad in ("onebot", "qq", "BAD_CAPS"):
                assert (await client.get(f"/admin/api/social-chat/{bad}/config")).status_code == 400
                assert (
                    await client.put(f"/admin/api/social-chat/{bad}/config", json={})
                ).status_code == 400
                assert (
                    await client.delete(f"/admin/api/social-chat/{bad}/config")
                ).status_code == 400

            # Auth required & query string forbidden
            assert (
                await client.get(
                    "/admin/api/social-chat/telegram/config",
                    headers={"Authorization": "Bearer wrong"},
                )
            ).status_code == 401
            assert (
                await client.get("/admin/api/social-chat/telegram/config?x=1")
            ).status_code == 400

            # Offline GET baseline
            r0 = await client.get("/admin/api/social-chat/telegram/config")
            assert r0.status_code == 200
            d0 = r0.json()
            assert d0["platform"] == "telegram"
            assert d0["plugin_id"] == "telegram"
            assert d0["online"] is False
            assert d0["bot_id"] == BOT_ID
            assert d0["owner_configured"] is True
            assert d0["source"] == "env"
            assert d0["config"] == {
                "allowed_groups": [],
                "proactive_groups": [],
                "private_mode": "owner_only",
                "allowed_users": [],
                "memory_mirror_enabled": False,
            }

            # Offline PUT persists config, seeds at_only, returns pushed=False
            put_body = {
                "allowed_groups": [OTHER_GROUP_ID, GROUP_ID, GROUP_ID],
                "proactive_groups": [OTHER_GROUP_ID, "-100404404"],
                "private_mode": "owner_and_allowlist",
                "allowed_users": [FRIEND_UID],
                "memory_mirror_enabled": True,
            }
            r_put = await client.put("/admin/api/social-chat/telegram/config", json=put_body)
            assert r_put.status_code == 200
            d_put = r_put.json()
            assert d_put["pushed"] is False
            assert d_put["source"] == "admin"
            assert d_put["config"]["allowed_groups"] == sorted([GROUP_ID, OTHER_GROUP_ID])
            assert d_put["config"]["proactive_groups"] == [OTHER_GROUP_ID]
            assert d_put["config"]["private_mode"] == "owner_and_allowlist"
            assert d_put["config"]["allowed_users"] == [FRIEND_UID]
            assert d_put["config"]["memory_mirror_enabled"] is True

            # Groups endpoint immediately shows persisted Telegram groups even while offline
            g_resp = await client.get("/admin/api/groups")
            assert g_resp.status_code == 200
            g_map = {(g["platform"], g["group_id"]): g["mode"] for g in g_resp.json()}
            assert g_map[("onebot", "40000")] == "at_only"
            assert g_map[("telegram", GROUP_ID)] == "at_only"
            assert g_map[("telegram", OTHER_GROUP_ID)] == "at_only"

            # PUT /admin/api/groups works for Telegram group even while offline
            r_pol = await client.put(
                "/admin/api/groups",
                json={"platform": "telegram", "group_id": GROUP_ID, "mode": "smart_reply"},
            )
            assert r_pol.status_code == 200
            assert svc.s.group_policies[f"telegram:{GROUP_ID}"] == "smart_reply"

            # Unallowlisted Telegram group rejected
            r_pol_bad = await client.put(
                "/admin/api/groups",
                json={"platform": "telegram", "group_id": "-100999", "mode": "smart_reply"},
            )
            assert r_pol_bad.status_code == 409

            # Connect Telegram plugin with different .env defaults -> Bus auto-pushes override
            fake_api = _FakeTelegramApi()
            plugin, bridge, runner, _token = await _connect_tg_plugin(
                svc,
                fake_api,
                allowed_groups={GROUP_ID},
                proactive_groups={GROUP_ID},
                allowed_users=set(),
                private_mode="owner_only",
            )
            try:
                await _wait_until(
                    lambda: (
                        plugin.allowed_groups == {GROUP_ID, OTHER_GROUP_ID}
                        and plugin.proactive_groups == {OTHER_GROUP_ID}
                        and plugin.allowed_users == {FRIEND_UID}
                        and plugin.private_mode == "owner_and_allowlist"
                        and plugin.memory_mirror_enabled is True
                    )
                )

                # Reserved config.update RPC is blocked via custom/raw RPC helpers
                proxy = svc.plugin_bus.get_chat_proxy("telegram")
                assert isinstance(proxy, RemoteModuleProxy)
                with pytest.raises(Rejected, match="plugin_invalid_method"):
                    await proxy.call_custom_rpc("config.update", {})
                with pytest.raises(Rejected, match="plugin_invalid_method"):
                    await proxy.call_raw_rpc("config.update", {})

                # Online GET shows both admin config AND untouched env_defaults
                r_online = await client.get("/admin/api/social-chat/telegram/config")
                assert r_online.status_code == 200
                d_on = r_online.json()
                assert d_on["online"] is True
                assert d_on["bot_id"] == BOT_ID
                assert d_on["bot_username"] == "hakimi_test_bot"
                assert d_on["owner_configured"] is True
                assert d_on["source"] == "admin"
                assert "sticker" in d_on["capabilities"]
                assert "emoji_like" in d_on["capabilities"]
                assert d_on["config"]["allowed_groups"] == sorted([GROUP_ID, OTHER_GROUP_ID])
                assert d_on["environment"] == {
                    "allowed_groups": [GROUP_ID],
                    "proactive_groups": [GROUP_ID],
                    "private_mode": "owner_only",
                    "allowed_users": [],
                    "memory_mirror_enabled": False,
                }

                # Hot-update while online: add THIRD_GROUP_ID, remove OTHER_GROUP_ID
                r_hot = await client.put(
                    "/admin/api/social-chat/telegram/config",
                    json={
                        "allowed_groups": [GROUP_ID, THIRD_GROUP_ID],
                        "proactive_groups": [THIRD_GROUP_ID],
                        "private_mode": "owner_only",
                        "allowed_users": [],
                        "memory_mirror_enabled": False,
                    },
                )
                assert r_hot.status_code == 200
                d_hot = r_hot.json()
                assert d_hot["pushed"] is True
                assert plugin.allowed_groups == {GROUP_ID, THIRD_GROUP_ID}
                assert plugin.proactive_groups == {THIRD_GROUP_ID}
                assert plugin.private_mode == "owner_only"
                assert plugin.allowed_users == set()
                # Existing smart_reply policy on GROUP_ID preserved, THIRD_GROUP_ID seeded at_only
                assert svc.s.group_policies[f"telegram:{GROUP_ID}"] == "smart_reply"
                assert svc.s.group_policies[f"telegram:{THIRD_GROUP_ID}"] == "at_only"

                # Plugin parse_update immediately honors the hot-pushed allowlists
                now_ts = int(time.time())
                assert (
                    plugin.parse_update(
                        {
                            "update_id": 1,
                            "message": {
                                "message_id": 11,
                                "date": now_ts,
                                "chat": {"id": int(THIRD_GROUP_ID), "type": "supergroup"},
                                "from": {"id": int(FRIEND_UID), "is_bot": False},
                                "text": "hello",
                            },
                        }
                    )
                    is not None
                )
                assert (
                    plugin.parse_update(
                        {
                            "update_id": 2,
                            "message": {
                                "message_id": 12,
                                "date": now_ts,
                                "chat": {"id": int(OTHER_GROUP_ID), "type": "supergroup"},
                                "from": {"id": int(FRIEND_UID), "is_bot": False},
                                "text": "hello",
                            },
                        }
                    )
                    is None
                )
                assert (
                    plugin.parse_update(
                        {
                            "update_id": 3,
                            "message": {
                                "message_id": 13,
                                "date": now_ts,
                                "chat": {"id": int(FRIEND_UID), "type": "private"},
                                "from": {"id": int(FRIEND_UID), "is_bot": False},
                                "text": "hi",
                            },
                        }
                    )
                    is None
                )

                # DELETE resets back to .env defaults and pushes reset_to_env=True
                r_del = await client.delete("/admin/api/social-chat/telegram/config")
                assert r_del.status_code == 200
                d_del = r_del.json()
                assert d_del["pushed"] is True
                assert d_del["source"] == "env"
                assert d_del["config"] == d_del["environment"]
                assert plugin.allowed_groups == {GROUP_ID}
                assert plugin.proactive_groups == {GROUP_ID}
                assert proxy.allowed_groups() == [GROUP_ID]
                assert proxy.proactive_groups() == [f"telegram:{GROUP_ID}"]
            finally:
                await _disconnect_tg_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 3. Cross-platform GroupPolicy / GroupActivity / allowed_activity_group
# ---------------------------------------------------------------------------


async def test_group_policy_and_activity_models_accept_remote_platforms(settings: Any) -> None:
    gp = GroupPolicy(platform="telegram", group_id=GROUP_ID, mode="smart_reply")
    assert gp.platform == "telegram"
    ga = GroupActivity(platform="telegram", group_id=GROUP_ID, preset="active")
    assert ga.platform == "telegram"

    with pytest.raises(ValidationError):
        GroupPolicy(platform="INVALID!", group_id=GROUP_ID, mode="at_only")

    s = _make_p2_settings(settings)
    with pytest.raises(Rejected, match="group_not_allowlisted"):
        allowed_activity_group(s, "telegram", GROUP_ID)

    cfg = RemoteChatConfig(allowed_groups=[GROUP_ID])  # type: ignore[arg-type]
    allowed_activity_group(s, "telegram", GROUP_ID, remote_config=cfg)
    with pytest.raises(Rejected, match="group_not_allowlisted"):
        allowed_activity_group(s, "telegram", OTHER_GROUP_ID, remote_config=cfg)


# ---------------------------------------------------------------------------
# 4. Optional group_memory_mirror switch on RemoteModuleProxy
# ---------------------------------------------------------------------------


async def test_remote_memory_mirror_opt_in_and_strict_boundaries(settings: Any) -> None:
    s = _make_p2_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        fake_api = _FakeTelegramApi()
        plugin, bridge, runner, _token = await _connect_tg_plugin(
            svc, fake_api, allowed_groups={GROUP_ID}
        )
        try:
            proxy = svc.plugin_bus.get_chat_proxy("telegram")
            assert isinstance(proxy, RemoteModuleProxy)
            now = time.time()
            tg_group = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="m1",
                user_id="0",
                kind="group",
                conversation_id=GROUP_ID,
                text="",
                addressed=False,
                created_at=now,
            )
            tg_unallowed_group = tg_group.model_copy(update={"conversation_id": OTHER_GROUP_ID})
            tg_owner_dm = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="m2",
                user_id=OWNER_UID,
                kind="private",
                conversation_id=OWNER_UID,
                text="hi",
                addressed=True,
                created_at=now,
            )
            tg_friend_dm = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="m3",
                user_id=FRIEND_UID,
                kind="private",
                conversation_id=FRIEND_UID,
                text="hi",
                addressed=True,
                created_at=now,
            )

            # Default OFF
            assert proxy.permits_memory_mirror(tg_group, svc.s) is False
            assert svc.memory_mirror.allowed(tg_group) is False

            # Trust boundary: forged plugin.status claiming memory_mirror_enabled=True
            # without an admin override must NEVER enable memory mirroring.
            forged_status = {
                **plugin._status_snapshot(connected=True),
                "memory_mirror_enabled": True,
                "env_defaults": {
                    **plugin._env_defaults,
                    "memory_mirror_enabled": True,
                },
            }
            await plugin.client.push_status(forged_status)
            await _wait_until(lambda: proxy.status_dict().get("memory_mirror_enabled") is True)
            assert proxy.permits_memory_mirror(tg_group, svc.s) is False
            assert svc.memory_mirror.allowed(tg_group) is False
            assert (
                RemoteChatConfig.from_untrusted(
                    forged_status, platform="telegram"
                ).memory_mirror_enabled
                is False
            )

            # Enable via Admin API & verify server-side private admission enforcement
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": "Bearer " + TOKEN},
            ) as client:
                r_pre = await client.get("/admin/api/social-chat/telegram/config")
                assert r_pre.status_code == 200
                assert r_pre.json()["environment"]["memory_mirror_enabled"] is False
                assert r_pre.json()["config"]["memory_mirror_enabled"] is False

                r = await client.put(
                    "/admin/api/social-chat/telegram/config",
                    json={
                        "allowed_groups": [GROUP_ID],
                        "proactive_groups": [GROUP_ID],
                        "private_mode": "owner_only",
                        "allowed_users": [FRIEND_UID],
                        "memory_mirror_enabled": True,
                    },
                )
                assert r.status_code == 200 and r.json()["pushed"] is True

                # Server-side private admission defense-in-depth under owner_only
                tg_stranger_dm = tg_friend_dm.model_copy(
                    update={"user_id": STRANGER_UID, "conversation_id": STRANGER_UID}
                )
                assert proxy.authorize_chat(tg_owner_dm, svc.s) is True
                assert proxy.authorize_chat(tg_friend_dm, svc.s) is False
                assert proxy.authorize_chat(tg_stranger_dm, svc.s) is False

                # Switch to owner_and_allowlist: friend admitted, stranger still rejected
                r_allow = await client.put(
                    "/admin/api/social-chat/telegram/config",
                    json={
                        "allowed_groups": [GROUP_ID],
                        "proactive_groups": [GROUP_ID],
                        "private_mode": "owner_and_allowlist",
                        "allowed_users": [FRIEND_UID],
                        "memory_mirror_enabled": True,
                    },
                )
                assert r_allow.status_code == 200 and r_allow.json()["pushed"] is True
                assert proxy.authorize_chat(tg_owner_dm, svc.s) is True
                assert proxy.authorize_chat(tg_friend_dm, svc.s) is True
                assert proxy.authorize_chat(tg_stranger_dm, svc.s) is False

                # Authorized group now permitted; private chats & unallowlisted groups still blocked
                assert proxy.permits_memory_mirror(tg_group, svc.s) is True
                assert svc.memory_mirror.allowed(tg_group) is True
                assert proxy.permits_memory_mirror(tg_unallowed_group, svc.s) is False
                assert proxy.permits_memory_mirror(tg_owner_dm, svc.s) is False
                assert proxy.permits_memory_mirror(tg_friend_dm, svc.s) is False

                # Muted group is blocked even when mirror toggle is True
                svc.s.group_policies[f"telegram:{GROUP_ID}"] = "muted"
                assert proxy.permits_memory_mirror(tg_group, svc.s) is False
                svc.s.group_policies[f"telegram:{GROUP_ID}"] = "at_only"
                assert proxy.permits_memory_mirror(tg_group, svc.s) is True

                # DELETE reverts to env defaults -> mirror disabled again
                r_del = await client.delete("/admin/api/social-chat/telegram/config")
                assert r_del.status_code == 200
                assert proxy.permits_memory_mirror(tg_group, svc.s) is False
                assert svc.memory_mirror.allowed(tg_group) is False
        finally:
            await _disconnect_tg_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 5. Cross-platform Moderation (/mute, /unmute, record_strike, review)
# ---------------------------------------------------------------------------


async def test_cross_platform_mute_commands_and_record_strike(settings: Any) -> None:
    s = _make_p2_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        svc.proactive.alert = AsyncMock()
        fake_api = _FakeTelegramApi()
        plugin, bridge, runner, _token = await _connect_tg_plugin(
            svc, fake_api, allowed_groups={GROUP_ID}
        )
        try:
            proxy = svc.plugin_bus.get_chat_proxy("telegram")
            assert isinstance(proxy, RemoteModuleProxy)
            now = time.time()

            qq_owner_dm = Message(
                platform="onebot",
                bot_id="10000",
                event_id="qq-own-1",
                user_id="20000",
                kind="private",
                conversation_id="20000",
                text="",
                addressed=True,
                created_at=now,
            )
            tg_owner_dm = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="tg-own-1",
                user_id=OWNER_UID,
                kind="private",
                conversation_id=OWNER_UID,
                text="",
                addressed=True,
                created_at=now,
            )
            tg_group_stranger = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="tg-grp-1",
                user_id=STRANGER_UID,
                kind="group",
                conversation_id=GROUP_ID,
                text="哈基米你好",
                addressed=True,
                created_at=now,
                sender_nickname="捣蛋猫",
            )

            # 1) /mute telegram:<uid> from Telegram Owner DM mutes via IdentityService
            reply = await svc.commands.dispatch(
                svc, tg_owner_dm.model_copy(update={"text": f"/mute telegram:{STRANGER_UID}"})
            )
            assert reply and f"telegram:{STRANGER_UID}" in reply
            assert svc.identity.is_muted("telegram", STRANGER_UID) is True
            pid = svc.identity.cached_person_id("telegram", STRANGER_UID)
            assert pid is not None
            assert await svc.identity.person_mute_source(pid) == "owner_manual"
            assert proxy.authorize_chat(tg_group_stranger, svc.s) is False

            # 2) /unmute telegram:<uid> from QQ Owner DM unmutes via IdentityService
            reply_un = await svc.commands.dispatch(
                svc, qq_owner_dm.model_copy(update={"text": f"/unmute telegram:{STRANGER_UID}"})
            )
            assert reply_un and f"telegram:{STRANGER_UID}" in reply_un
            assert svc.identity.is_muted("telegram", STRANGER_UID) is False
            assert proxy.authorize_chat(tg_group_stranger, svc.s) is True

            # 3) Non-onebot target with --delete-friend returns clear refusal and does NOT mute
            for owner_dm in (qq_owner_dm, tg_owner_dm):
                refuse = await svc.commands.dispatch(
                    svc,
                    owner_dm.model_copy(
                        update={"text": f"/mute telegram:{STRANGER_UID} --delete-friend"}
                    ),
                )
                assert refuse and "--delete-friend" in refuse
                assert svc.identity.is_muted("telegram", STRANGER_UID) is False

            # 4) Owner bound ID and Bot self ID are strictly protected on all platforms
            for protected_target in (
                f"telegram:{OWNER_UID}",
                f"telegram:{BOT_ID}",
                "20000",
                "10000",
                "onebot:20000",
                "onebot:10000",
            ):
                with pytest.raises(Rejected, match="protected_identity"):
                    await svc.commands.dispatch(
                        svc,
                        tg_owner_dm.model_copy(update={"text": f"/mute {protected_target}"}),
                    )

            # 5) Pure numeric /mute still defaults to onebot
            qq_mute = await svc.commands.dispatch(
                svc, tg_owner_dm.model_copy(update={"text": "/mute 30000"})
            )
            assert qq_mute and "QQ" in qq_mute
            assert "30000" in svc.s.muted_user_ids
            await svc.commands.dispatch(
                svc, tg_owner_dm.model_copy(update={"text": "/unmute 30000"})
            )
            assert "30000" not in svc.s.muted_user_ids

            # 6) Non-owner or group messages rejected
            for bad_msg in (
                tg_group_stranger.model_copy(update={"text": f"/mute telegram:{FRIEND_UID}"}),
                tg_owner_dm.model_copy(
                    update={"kind": "group", "conversation_id": GROUP_ID, "text": "/mute 30000"}
                ),
            ):
                with pytest.raises(Rejected, match="owner_private_only"):
                    await svc.commands.dispatch(svc, bad_msg)

            # 7) record_strike works for Telegram group messages & alert does NOT hardcode QQ
            assert (
                await svc.moderation.record_strike(
                    tg_group_stranger.model_copy(update={"user_id": OWNER_UID, "event_id": "o1"}),
                    "prompt_injection",
                )
                is None
            )
            assert (
                await svc.moderation.record_strike(
                    tg_group_stranger.model_copy(update={"user_id": BOT_ID, "event_id": "b1"}),
                    "prompt_injection",
                )
                is None
            )
            proposal = None
            for i in range(3):
                proposal = await svc.moderation.record_strike(
                    tg_group_stranger.model_copy(update={"event_id": f"tg-s-{i}"}),
                    "prompt_injection",
                    "捣蛋猫",
                )
            assert proposal is not None
            svc.proactive.alert.assert_awaited_once()
            alert_text = svc.proactive.alert.call_args.args[1]
            assert f"telegram:{STRANGER_UID}" in alert_text
            assert f"QQ {STRANGER_UID}" not in alert_text
            assert "--delete-friend" not in alert_text

            # --delete-friend on Telegram proposal returns explicit refusal without muting
            refuse_prop = await svc.commands.dispatch(
                svc,
                tg_owner_dm.model_copy(
                    update={"text": f"/mute-approve {proposal} --delete-friend"}
                ),
            )
            assert refuse_prop and "--delete-friend" in refuse_prop
            assert svc.identity.is_muted("telegram", STRANGER_UID) is False

            # Atomicity: if identity is unavailable when approving a non-onebot proposal,
            # review() must fail BEFORE committing status='approved' so the proposal stays pending.
            saved_svc_ident = svc.identity
            saved_rt_ident = svc.store.runtime.identity
            try:
                svc.identity = None
                svc.store.runtime.identity = None
                with pytest.raises(Rejected, match="identity_unavailable"):
                    await svc.moderation.review(proposal, True)
                row = await svc.store.one(
                    "SELECT status FROM pending_mute_proposals WHERE id=?", (proposal,)
                )
                assert row is not None and row["status"] == "pending"
            finally:
                svc.identity = saved_svc_ident
                svc.store.runtime.identity = saved_rt_ident

            # Approving Telegram proposal mutes via IdentityService
            approved = await svc.commands.dispatch(
                svc, tg_owner_dm.model_copy(update={"text": f"/mute-approve {proposal}"})
            )
            assert approved and "已批准" in approved
            assert svc.identity.is_muted("telegram", STRANGER_UID) is True
            assert proxy.authorize_chat(tg_group_stranger, svc.s) is False
        finally:
            await _disconnect_tg_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 6. RemoteModuleProxy.channel_prompt reflects declared capabilities accurately
# ---------------------------------------------------------------------------


async def test_remote_module_proxy_channel_prompt_reflects_capabilities(settings: Any) -> None:
    s = _make_p2_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        fake_api = _FakeTelegramApi()
        plugin, bridge, runner, _token = await _connect_tg_plugin(
            svc, fake_api, allowed_groups={GROUP_ID}
        )
        try:
            proxy = svc.plugin_bus.get_chat_proxy("telegram")
            assert isinstance(proxy, RemoteModuleProxy)
            prompt = proxy.channel_prompt(BOT_ID, "telegram")
            assert "telegram" in prompt
            assert BOT_ID in prompt
            assert "meme_labels" in prompt
            assert "emoji_like" in prompt
            assert "anchor.reply_id" in prompt
            assert "voice_text/sing_song" in prompt
            assert "卡片" in prompt
            assert "poke_qq 必须留空" in prompt
            assert "anchor.mentions 必须留空" in prompt
            assert "真实身份与权限一律以末尾服务端验证的 [role=...] 为准。" in prompt

            # A capability-free remote proxy must NOT claim sticker/emoji/voice support
            proxy.supports_sticker = False
            proxy.supports_emoji_like = False
            proxy.supports_voice = False
            proxy.supports_reply_anchor = False
            proxy.supports_rich_card = False
            plain_prompt = proxy.channel_prompt(BOT_ID, "telegram")
            assert "纯文字气泡回复" in plain_prompt
            assert "sticker.label 必须留空" in plain_prompt
            assert "emoji_like 必须留空" in plain_prompt
        finally:
            await _disconnect_tg_plugin(plugin, bridge, runner)
