"""AST architecture boundary guards and unit tests for kernel decoupling.

Covers:
1. AST scan forbidding reverse imports from kernel files into platform modules.
2. `platform_hooks` registry (fail-closed defaults, QQ registration, custom hooks).
3. `CommandRegistry` (registration, aliases, strict protocol-layer Owner gates).
4. `media_turn` (sticker classification, image loading, forward expansion, turn files).
5. `life_rules` dynamic compatibility resolution and `service.py` runtime song decoupling.
"""

from __future__ import annotations

import ast
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hakimi.commands import CommandRegistry, build_default_command_registry
from hakimi.life_engine import DESKTOP_CONTEXT_HINT, JIT_TRIGGER_MESSAGE
from hakimi.life_rules import (
    Reaction,
    Rule,
    build_from_video,
    milestone_sharing,
    register_compat_rule_provider,
)
from hakimi.media_turn import (
    expand_forwards,
    is_sticker_candidate,
    load_message_images,
    load_turn_files,
)
from hakimi.memory_mirror import MemoryMirror
from hakimi.models import FileRef, ImageRef, Message
from hakimi.modules.qq_delivery import JIT_TRIGGER_MESSAGE as QQ_JIT_TRIGGER_MESSAGE
from hakimi.modules.qq_policy import DESKTOP_CONTEXT_HINT as QQ_DESKTOP_CONTEXT_HINT
from hakimi.modules.qq_social import QQSocialModule, register_qq_platform_hooks
from hakimi.outgoing import deliver_proactive_jit
from hakimi.platform_hooks import (
    PlatformPolicyHooks,
    get_platform_hooks,
    register_platform_hooks,
    registered_platforms,
    unregister_platform_hooks,
)
from hakimi.security import Rejected, authorize
from hakimi.service import Service
from hakimi.social import permits_chat_export
from hakimi.world_state import MilestoneEvent
from tests.fakes import FakeBili, FakeGateway, FakeSender

SRC_HAKIMI = Path(__file__).resolve().parents[1] / "src" / "hakimi"

FORBIDDEN_PLATFORM_TARGETS = (
    "hakimi.modules.platforms",
    "hakimi.modules.bilibili",
    "hakimi.modules.bili_watcher",
    "hakimi.modules.qq_delivery",
    "hakimi.modules.qq_policy",
    "hakimi.modules.qq_audience",
)

GUARDED_KERNEL_FILES = (
    "life_engine.py",
    "life_rules.py",
    "security.py",
    "social.py",
    "memory_mirror.py",
    "outgoing.py",
    "commands.py",
    "media_turn.py",
    "platform_hooks.py",
    "gemini.py",
    "rich_message_parser.py",
    "video_downloader.py",
    "admin_session_routes.py",
    "bili_account_admin.py",
)


def _resolve_import_targets(path: Path) -> list[tuple[int, str]]:
    """Collect every imported module/symbol target (top-level and inline) as dotted strings."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    rel = path.relative_to(SRC_HAKIMI.parent).with_suffix("")
    pkg_parts = list(rel.parts[:-1])
    targets: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base_parts = pkg_parts[: len(pkg_parts) - (node.level - 1)]
                mod = ".".join([*base_parts, *([node.module] if node.module else [])])
            else:
                mod = node.module or ""
            if mod:
                targets.append((node.lineno, mod))
            for alias in node.names:
                if alias.name != "*":
                    full = f"{mod}.{alias.name}" if mod else alias.name
                    targets.append((node.lineno, full))
    return targets


def _matches_forbidden(target: str, forbidden: tuple[str, ...]) -> bool:
    return any(target == prefix or target.startswith(prefix + ".") for prefix in forbidden)


@pytest.mark.parametrize("filename", GUARDED_KERNEL_FILES)
def test_kernel_files_have_no_reverse_platform_imports(filename: str):
    path = SRC_HAKIMI / filename
    assert path.is_file(), f"Missing guarded kernel file: {path}"
    offenders = [
        (lineno, target)
        for lineno, target in _resolve_import_targets(path)
        if _matches_forbidden(target, FORBIDDEN_PLATFORM_TARGETS)
    ]
    assert offenders == [], f"{filename} contains forbidden platform reverse imports: {offenders}"


def test_service_decoupled_from_songs_and_qq_and_uses_sdk_extract_bvid():
    path = SRC_HAKIMI / "service.py"
    targets = _resolve_import_targets(path)
    forbidden_in_service = (
        "hakimi.songs",
        "hakimi.modules.platforms",
        "hakimi.modules.qq_delivery",
        "hakimi.modules.qq_policy",
        "hakimi.modules.qq_audience",
        "hakimi.modules.bilibili.extract_bvid",
    )
    offenders = [
        (lineno, target)
        for lineno, target in targets
        if _matches_forbidden(target, forbidden_in_service)
    ]
    assert offenders == [], f"service.py has forbidden imports: {offenders}"
    assert any(target == "hakimi_plugin_sdk.bili_common.extract_bvid" for _, target in targets), (
        "service.py must import extract_bvid from hakimi_plugin_sdk.bili_common"
    )


def test_life_engine_contract_constants_match_qq_platform():
    assert JIT_TRIGGER_MESSAGE == QQ_JIT_TRIGGER_MESSAGE
    assert DESKTOP_CONTEXT_HINT == QQ_DESKTOP_CONTEXT_HINT


# ---------------------------------------------------------------------------
# platform_hooks tests
# ---------------------------------------------------------------------------


def test_platform_hooks_validation_and_registration():
    with pytest.raises(TypeError):
        PlatformPolicyHooks(
            authorize="not-callable",  # type: ignore[arg-type]
            permits_export=lambda *_: False,
            permits_memory_mirror=lambda *_: False,
            allowed_proactive=lambda *_: False,
        )
    hooks = PlatformPolicyHooks(
        authorize=lambda m, s, *, _bot_exempt=False: m.user_id == "ok",
        permits_export=lambda m, consumer: consumer == "patrol_diary",
        permits_memory_mirror=lambda m, s: m.conversation_id == "grp-ok",
        allowed_proactive=lambda m, s: m.user_id == "ok",
    )
    with pytest.raises(ValueError):
        register_platform_hooks("   ", hooks)
    with pytest.raises(TypeError):
        register_platform_hooks("demo_plat", object())  # type: ignore[arg-type]

    register_platform_hooks("demo_plat", hooks)
    try:
        assert "demo_plat" in registered_platforms()
        assert get_platform_hooks("demo_plat", allow_lazy_default=False) is hooks
        assert get_platform_hooks("nonexistent_plat") is None
        assert get_platform_hooks("") is None
    finally:
        unregister_platform_hooks("demo_plat")
    assert get_platform_hooks("demo_plat") is None


def test_platform_hooks_fail_closed_for_unknown_or_unregistered(
    settings, store, message, monkeypatch
):
    import hakimi.platform_hooks as ph

    unknown = message.model_copy(update={"platform": "unknown_chat", "bot_id": "u-bot"})
    unknown_group = unknown.model_copy(update={"kind": "group", "conversation_id": "40000"})

    assert get_platform_hooks("unknown_chat") is None
    assert authorize(unknown, settings) is False
    assert authorize(unknown_group, settings) is False
    assert permits_chat_export(unknown, settings, "patrol_diary") is False

    stub_svc = SimpleNamespace(s=settings, sessions=None, store=store, drive=None)
    mirror = MemoryMirror(stub_svc)
    assert mirror.allowed(unknown_group) is False

    # Even for "onebot", if lazy defaults are disabled and hooks are unregistered, stay fail-closed.
    monkeypatch.setattr(ph, "_DEFAULT_PROVIDER_MODULES", {})
    saved_onebot = get_platform_hooks("onebot", allow_lazy_default=False)
    unregister_platform_hooks("onebot")
    try:
        assert get_platform_hooks("onebot") is None
        assert authorize(message, settings) is False
        assert permits_chat_export(message, settings, "patrol_diary") is False
        group_msg = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
        assert mirror.allowed(group_msg) is False
    finally:
        if saved_onebot is not None:
            register_platform_hooks("onebot", saved_onebot)
        else:
            register_qq_platform_hooks()


def test_qq_social_module_registers_hooks_and_preserves_fail_closed_policy(
    settings, store, message
):
    unregister_platform_hooks("onebot")
    unregister_platform_hooks("qq")
    assert get_platform_hooks("onebot", allow_lazy_default=False) is None
    assert get_platform_hooks("qq", allow_lazy_default=False) is None

    QQSocialModule(settings)

    ob_hooks = get_platform_hooks("onebot", allow_lazy_default=False)
    qq_hooks = get_platform_hooks("qq", allow_lazy_default=False)
    assert ob_hooks is not None and qq_hooks is not None

    # Authorized owner & group pass; unauthorized group / spoofed bot / official qq proactive fail.
    assert authorize(message, settings) is True
    assert authorize(message.model_copy(update={"user_id": "99999"}), settings) is False
    unapproved_group = message.model_copy(
        update={"kind": "group", "conversation_id": "999999", "addressed": True}
    )
    assert authorize(unapproved_group, settings) is False
    approved_group = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "addressed": True}
    )
    assert authorize(approved_group, settings) is True
    assert permits_chat_export(approved_group, settings, "patrol_diary") is True
    assert permits_chat_export(approved_group, settings, "unknown_consumer") is False

    stub_svc = SimpleNamespace(s=settings, sessions=None, store=store, drive=None)
    mirror = MemoryMirror(stub_svc)
    assert mirror.allowed(approved_group) is True
    assert mirror.allowed(unapproved_group) is False
    assert mirror.allowed(message) is False  # Never mirrors private chats.

    assert ob_hooks.allowed_proactive(message, settings) is True
    assert ob_hooks.allowed_proactive(unapproved_group, settings) is False
    official_qq = message.model_copy(
        update={"platform": "qq", "bot_id": settings.qq_app_id, "user_id": "openid-1"}
    )
    assert qq_hooks.allowed_proactive(official_qq, settings) is False


def test_authorize_enforces_kernel_invariants_even_with_permissive_custom_hook(settings, message):
    permissive = PlatformPolicyHooks(
        authorize=lambda m, s, *, _bot_exempt=False: True,
        permits_export=lambda m, c: True,
        permits_memory_mirror=lambda m, s: True,
        allowed_proactive=lambda m, s: True,
    )
    register_platform_hooks("custom_chat", permissive)
    try:
        self_msg = message.model_copy(
            update={"platform": "custom_chat", "bot_id": "bot-1", "user_id": "bot-1"}
        )
        assert authorize(self_msg, settings) is False

        spoofed_private = message.model_copy(
            update={
                "platform": "custom_chat",
                "bot_id": "bot-1",
                "user_id": "user-1",
                "conversation_id": "other-user",
                "kind": "private",
            }
        )
        assert authorize(spoofed_private, settings) is False

        ok_private = message.model_copy(
            update={
                "platform": "custom_chat",
                "bot_id": "bot-1",
                "user_id": "user-1",
                "conversation_id": "user-1",
                "kind": "private",
            }
        )
        assert authorize(ok_private, settings) is True
    finally:
        unregister_platform_hooks("custom_chat")


async def test_deliver_proactive_jit_fails_closed_for_unknown_platform(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        unknown = message.model_copy(update={"platform": "unknown_chat", "bot_id": "u-bot"})
        res = await deliver_proactive_jit(svc, unknown, "hello", reply_id="jit:test-unknown")
        assert res == {"status": "cancelled", "bubbles_sent": 0}
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# CommandRegistry & service.py decoupling tests
# ---------------------------------------------------------------------------


async def test_command_registry_registration_aliases_and_owner_gates(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        reg = CommandRegistry()
        seen: list[tuple[str, str]] = []

        async def ping_handler(_svc, msg: Message, raw_args: str) -> str:
            seen.append((msg.user_id, raw_args))
            return f"pong:{raw_args}"

        async def secret_handler(_svc, _msg: Message, raw_args: str) -> str:
            return f"owner:{raw_args}"

        async def group_owner_handler(_svc, _msg: Message, raw_args: str) -> str:
            return f"group-ok:{raw_args}"

        reg.register("ping", ping_handler, aliases=("p", "/pong"))
        reg.register("/secret", secret_handler, permission="owner_private")
        reg.register("/reset-ctx", group_owner_handler, exact=True, permission="group_owner")

        assert "/ping" in reg.names
        assert await reg.dispatch(svc, message.model_copy(update={"text": "/ping hi"})) == "pong:hi"
        assert (
            await reg.dispatch(svc, message.model_copy(update={"text": "/p world"})) == "pong:world"
        )
        assert await reg.dispatch(svc, message.model_copy(update={"text": "/pong"})) == "pong:"
        assert await reg.dispatch(svc, message.model_copy(update={"text": "/pingpong"})) is None

        # Known bot is always ignored before reaching any handler.
        settings.bot_qq_ids.add("77777")
        bot_msg = message.model_copy(
            update={"user_id": "77777", "conversation_id": "77777", "text": "/ping bot"}
        )
        assert await reg.dispatch(svc, bot_msg) is None
        assert len(seen) == 3

        # owner_private allows ONLY verified OneBot OWNER_QQ_ID in private chat.
        assert (
            await reg.dispatch(svc, message.model_copy(update={"text": "/secret 42"})) == "owner:42"
        )
        # Non-owner with spoofed nickname claiming BOISER / 主人 is rejected!
        spoofed = message.model_copy(
            update={
                "user_id": "30000",
                "conversation_id": "30000",
                "sender_nickname": "BOISER 主人",
                "text": "/secret 42",
            }
        )
        with pytest.raises(Rejected, match="owner_private_only"):
            await reg.dispatch(svc, spoofed)
        # Real owner in group chat is rejected for owner_private!
        owner_group = message.model_copy(
            update={"kind": "group", "conversation_id": "40000", "text": "/secret 42"}
        )
        with pytest.raises(Rejected, match="owner_private_only"):
            await reg.dispatch(svc, owner_group)
        # Official QQ platform with same user_id string cannot forge OneBot owner!
        qq_spoof = message.model_copy(
            update={"platform": "qq", "bot_id": settings.qq_app_id, "text": "/secret 42"}
        )
        with pytest.raises(Rejected, match="owner_private_only"):
            await reg.dispatch(svc, qq_spoof)

        # group_owner: non-owner allowed in private, rejected in group; owner allowed in group.
        friend_priv = message.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "/reset-ctx"}
        )
        assert await reg.dispatch(svc, friend_priv) == "group-ok:"
        friend_grp = message.model_copy(
            update={
                "user_id": "30000",
                "kind": "group",
                "conversation_id": "40000",
                "text": "/reset-ctx",
            }
        )
        with pytest.raises(Rejected, match="group_owner_only"):
            await reg.dispatch(svc, friend_grp)
        owner_grp_reset = message.model_copy(
            update={"kind": "group", "conversation_id": "40000", "text": "/reset-ctx"}
        )
        assert await reg.dispatch(svc, owner_grp_reset) == "group-ok:"

        reg.unregister("/ping")
        assert await reg.dispatch(svc, message.model_copy(update={"text": "/ping hi"})) is None
    finally:
        await svc.stop()


async def test_service_default_commands_and_direct_delegate_guards(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        assert isinstance(svc.commands, CommandRegistry)
        assert "/affection" in svc.commands.names
        assert "/学歌" in svc.commands.names
        assert "/存表情" in svc.commands.names
        assert "/bind" in svc.commands.names

        help_text = await svc.command(message.model_copy(update={"text": "/哈基米 帮助"}))
        assert help_text and "/new-session" in help_text

        non_owner = message.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "/学歌 晴天"}
        )
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.command(non_owner)
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc._learn_song_command(non_owner, "晴天")
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc._affection_command(non_owner, "")
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc._save_meme_command(non_owner, "开心")

        non_owner_group = message.model_copy(
            update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
        )
        with pytest.raises(Rejected, match="group_owner_only"):
            await svc._save_meme_command(non_owner_group, "开心")
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.command(non_owner_group.model_copy(update={"text": "/哈基米 订阅"}))

        # Runtime song request detection without importing hakimi.songs in service.py.
        assert svc._is_explicit_song_request("给我唱一首晴天") is True
        assert svc._is_explicit_song_request("今天天气真好") is False
        svc.songs = SimpleNamespace(explicit_song_request=lambda *texts: "custom-sing" in texts)
        assert svc._is_explicit_song_request("custom-sing") is True
        svc.songs = None
        assert svc._is_explicit_song_request("给我唱一首晴天") is False
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# media_turn unit tests
# ---------------------------------------------------------------------------


async def test_media_turn_sticker_priority_and_omission(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        sticker_ref = ImageRef(file="s1.gif", url="https://img.example/s1.gif", is_sticker=True)
        photo_ref = ImageRef(file="p1.jpg", url="https://img.example/p1.jpg", is_sticker=False)
        extra_ref = ImageRef(file="p2.jpg", url="https://img.example/p2.jpg", is_sticker=False)

        assert is_sticker_candidate(svc, message, sticker_ref) is True
        assert is_sticker_candidate(svc, message, photo_ref) is False

        loaded_order: list[str] = []

        class StubLoader:
            async def load_results(self, refs):
                loaded_order.extend(r.file for r in refs)
                return [
                    SimpleNamespace(digest=f"dig-{r.file}", tiles=(), resolution="high")
                    for r in refs
                ]

        svc.images = StubLoader()
        group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
        m_photo = group.model_copy(update={"event_id": "e1", "images": [photo_ref]})
        m_sticker = group.model_copy(update={"event_id": "e2", "images": [sticker_ref]})
        m_extra = group.model_copy(update={"event_id": "e3", "images": [extra_ref]})

        # In nearby messages, regular photos are prioritized ahead of stickers when capped by limit.
        imgs, sources = await load_message_images(
            svc, group, [], [m_photo, m_extra, m_sticker], limit=2, include_current=False
        )
        assert len(imgs) == 2
        assert loaded_order == ["p2.jpg", "p1.jpg"]
        assert sources[-1] == {"status": "omitted", "count": 1}
    finally:
        await svc.stop()


async def test_media_turn_expand_forwards_bounds_and_secret_rejection(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        # 1. Normal forward expansion deduplicates images/files and caps counts.
        fwd_payload = {
            "messages": [
                {
                    "sender": {"user_id": 20000, "nickname": "Alice"},
                    "message": [
                        {"type": "text", "data": {"text": "看看 BV1xx411c7mD"}},
                        {"type": "image", "data": {"file": "a.jpg", "url": "https://img/a.jpg"}},
                        {"type": "image", "data": {"file": "b.jpg", "url": "https://img/b.jpg"}},
                        {"type": "image", "data": {"file": "c.jpg", "url": "https://img/c.jpg"}},
                    ],
                }
            ]
        }

        async def fake_forward(_fid: str):
            return fwd_payload

        svc.sender.forward = fake_forward
        expanded = await expand_forwards(svc, message.model_copy(update={"forward_ids": ["f1"]}))
        assert expanded.context_expanded is True
        assert expanded.forward_ids == []
        assert len(expanded.images) == 2
        assert "truncated" in expanded.context_status
        assert "BV1xx411c7mD" in expanded.video_ids

        # 2. Oversized combined forward text (> 24000 chars) is rejected.
        async def huge_forward(_fid: str):
            return {"messages": [{"sender": {"user_id": 20000}, "message": "x" * 13000}]}

        svc.sender.forward = huge_forward
        with pytest.raises(Rejected, match="forward_too_large"):
            await expand_forwards(
                svc, message.model_copy(update={"forward_ids": ["f-huge-1", "f-huge-2"]})
            )

        # 3. Secret leaked inside forward text is rejected.
        secret = settings.gemini_api_key.get_secret_value()

        async def secret_forward(_fid: str):
            return {"messages": [{"sender": {"user_id": 20000}, "message": f"key={secret}"}]}

        svc.sender.forward = secret_forward
        with pytest.raises(Rejected, match="credential_in_input"):
            await expand_forwards(svc, message.model_copy(update={"forward_ids": ["f-sec"]}))
    finally:
        await svc.stop()


async def test_media_turn_load_turn_files_filters_unauthorized_and_recalled(
    settings, store, message
):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        good_file = FileRef(file_id="f-ok", name="notes.txt", file_size=128)
        bad_file = FileRef(file_id="f-bad", name="evil.txt", file_size=128)
        captured: list[str] = []

        async def fake_load_for_turn(_msg, candidates, *, limit=1, selected_members=None):
            captured.extend(ref.file_id for ref, _, _ in candidates)
            if selected_members is not None and candidates:
                selected_members.append(candidates[0][1])
            return [{"name": "notes.txt", "content": "ok"}], [{"name": "notes.txt"}]

        svc.files.load_for_turn = fake_load_for_turn
        unauth_member = message.model_copy(
            update={"user_id": "99999", "conversation_id": "99999", "files": [bad_file]}
        )
        ok_member = message.model_copy(update={"event_id": "ev-file", "files": [good_file]})
        attachments, sources, consumed = await load_turn_files(
            svc, ok_member, [unauth_member, ok_member], [], set(), {"ev-file"}
        )
        assert captured == ["f-ok"]
        assert len(attachments) == 1 and len(sources) == 1
        assert consumed == []
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# life_rules dynamic compatibility tests
# ---------------------------------------------------------------------------


def test_life_rules_compat_wrappers_work_without_static_platform_imports(settings):
    ev = MilestoneEvent(
        id="mc-1",
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        importance="high",
        created_at=time.time(),
        payload={"title": "我的世界红石刷铁机教程", "summary": "生电建筑"},
    )
    reaction = build_from_video(ev)
    assert isinstance(reaction, Reaction)
    assert reaction.action == "mc.build_plan"

    settings.owner_qq_id = "20000"
    rule = milestone_sharing(settings)
    assert isinstance(rule, Rule)
    shared = rule.match(ev)
    assert shared is not None and shared.recipient == "owner:20000"

    # Explicit registration via register_compat_rule_provider works and validates args.
    with pytest.raises(ValueError):
        register_compat_rule_provider("", lambda *_: None)
    with pytest.raises(TypeError):
        register_compat_rule_provider("x", "not-callable")  # type: ignore[arg-type]

    default_reg = build_default_command_registry()
    assert "/new-session" in default_reg.names
