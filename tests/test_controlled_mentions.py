import asyncio
import time

import pytest

from hakimi.bot_guard import COOLDOWN_SECONDS
from hakimi.mentions import parse_reply, revalidate_segments, valid_at_targets
from hakimi.persona import OWNER_PERSONA_PROMPT, PUBLIC_PERSONA_PROMPT, identity_anchor
from hakimi.reply_style import clean_chinese_punctuation, public_reply
from hakimi.security import authorize, safe_output
from hakimi.store import Store


def group(message, **changes):
    return message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "command_text": "哈基米看看",
            "addressed": True,
            **changes,
        }
    )


def invitation(message, **changes):
    return group(
        message,
        user_id="20000",
        event_id=changes.pop("event_id", "owner-invite"),
        command_text="哈基米，请和他聊聊这个话题",
        mentioned_ids=["10000", "60000"],
        **changes,
    )


def incoming_bot(message, event="bot-turn", **changes):
    return group(
        message,
        user_id="60000",
        event_id=event,
        command_text="哈基米，继续聊",
        created_at=time.time(),
        **changes,
    )


def test_context_targets_are_real_local_participants(settings, message):
    settings.bot_qq_ids = {"60000"}
    settings.muted_user_ids = {"31000"}
    current = group(message)
    context = [
        group(message, user_id="20000"),
        group(message, user_id="31000"),
        group(message, user_id="60000"),
        group(message, user_id="10000"),
        group(message, user_id="32000", conversation_id="50000"),
        message,
    ]
    assert valid_at_targets(current, context, settings) == {"20000", "30000"}
    assert valid_at_targets(current, [], settings) == {"30000"}  # Owner absent, not inferred.
    assert valid_at_targets(current, context, settings, invited_bots={"60000"}) == {
        "20000",
        "30000",
        "60000",
    }
    assert not valid_at_targets(message, context, settings)


def test_joint_protocol_exact_allowlist_and_spacing(settings, message):
    parsed = parse_reply(
        "[AT:30000] 正常得很,我看你才魔了。[MEME:无语盯]",
        group(message),
        settings,
        allowed_at_qqs={"30000"},
    )
    assert parsed.text == "正常得很，我看你才魔了"
    assert parsed.at_targets == ("30000",)
    assert parsed.meme_tag == "无语盯"
    assert parsed.segments == [
        {"type": "at", "data": {"qq": "30000"}},
        {"type": "text", "data": {"text": " 正常得很，我看你才魔了"}},
    ]
    assert safe_output(parsed.text, []) == parsed.text


@pytest.mark.parametrize(
    "tag", ["all", "0", "10000", "99999", "30000x", "https://example.test/a", "30000\n"]
)
def test_unknown_targets_never_become_at_or_literal_code(settings, message, tag):
    result = parse_reply(
        f"[AT:{tag}]你说的对，但是猫不同意", group(message), settings, allowed_at_qqs={"30000"}
    )
    assert not result.at_targets and "[AT:" not in result.text
    assert all(s["type"] == "text" for s in result.segments)


def test_at_count_is_bounded_and_private_strips_every_protocol(settings, message):
    raw = "[AT:30000] [AT:31000] [AT:32000] 你好。[MEME:无语盯]"
    parsed = parse_reply(raw, group(message), settings, allowed_at_qqs={"30000", "31000", "32000"})
    assert parsed.at_targets == ("30000",)
    parsed = parse_reply(
        raw, group(message), settings, allowed_at_qqs={"30000", "31000", "32000"}, max_targets=99
    )
    assert len(parsed.at_targets) == 2
    private = parse_reply(raw, message, settings, allowed_at_qqs={"30000", "31000"})
    assert not private.at_targets and private.meme_tag is None and "[" not in private.text


def test_sendtime_revocation_drops_at_but_keeps_text(settings, message):
    m = group(message)
    initial = parse_reply("[AT:30000] 你这梗我接住了", m, settings, allowed_at_qqs={"30000"})
    settings.muted_user_ids = {"30000"}
    result = revalidate_segments(initial.segments, m, settings, allowed_at_qqs={"30000"})
    assert all(s["type"] == "text" for s in result)
    assert "你这梗我接住了" in "".join(s["data"]["text"] for s in result)


def test_punctuation_preserves_code_numbers_and_owner_period():
    assert (
        clean_chinese_punctuation("正常,我没事；abc,中文；中文,abc")
        == "正常，我没事；abc，中文；中文，abc"
    )
    assert (
        clean_chinese_punctuation("1,234 hello,world `正常,我` https://x.test/a,b")
        == "1,234 hello,world `正常,我` https://x.test/a,b"
    )
    assert public_reply("正常得很,我看你才魔了。") == "正常得很，我看你才魔了"
    assert public_reply("第一句。第二句。") == "第一句。第二句。"
    assert clean_chinese_punctuation("BOISER，慢慢说。") == "BOISER，慢慢说。"


def test_persona_is_playful_without_obsequious_or_meta_explanation():
    assert "不做冷漠杠精" in PUBLIC_PERSONA_PROMPT and "不懂别硬捧" in PUBLIC_PERSONA_PROMPT
    assert "不献媚护主" in PUBLIC_PERSONA_PROMPT
    assert "不打破第四面墙" in OWNER_PERSONA_PROMPT and "1～3句" in OWNER_PERSONA_PROMPT
    assert "10000" in identity_anchor("10000") and "吉米奈喵" in identity_anchor("10000")


async def test_owner_invite_admits_two_turns_atomically_and_cannot_reset_window(
    store, settings, message
):
    settings.bot_qq_ids = {"60000", "60001"}
    g = store.bot_guard
    assert await g.invite(invitation(message)) == {"60000"}
    before = dict(await store.one("SELECT * FROM bot_dialogue_windows"))
    messages = [incoming_bot(message, f"turn-{n}") for n in range(5)]
    assert all([await g.prepare(m) for m in messages])
    result = await asyncio.gather(*(store.admit(m) for m in messages))
    assert result.count("accepted") == 2 and result.count("ignored") == 3
    assert not await g.prepare(incoming_bot(message, "late"))
    assert not await g.invite(invitation(message, event_id="repeat"))
    after = dict(await store.one("SELECT * FROM bot_dialogue_windows"))
    assert after["used"] == 2 and after["started"] == before["started"]
    assert after["expires"] == before["expires"]
    assert not authorize(incoming_bot(message, "never-prepared"), settings)
    assert authorize(messages[0], settings)
    # Sandbox/copied config and a different scope/user/event never inherit a permission hook.
    assert not authorize(messages[0], settings.model_copy(deep=True))
    assert not authorize(messages[0].model_copy(update={"conversation_id": "50000"}), settings)
    settings.muted_user_ids = {"60000"}
    assert not authorize(messages[0], settings)
    assert not await g.may_process(messages[0], store.event_key(messages[0]))


@pytest.mark.parametrize(
    "text,owner,mentions",
    [
        ("哈基米不要去和它聊天", "20000", ["10000", "60000"]),
        ("哈基米去和它聊天？", "20000", ["10000", "60000"]),
        ("哈基米去和它聊天", "30000", ["10000", "60000"]),
        ("哈基米去和它聊天", "20000", []),
        ("有人说哈基米", "20000", ["10000", "60000"]),
        ("有人让我去和它聊天", "20000", ["10000", "60000"]),
        ("哈基米，如果让你去和它聊天", "20000", ["10000", "60000"]),
    ],
)
async def test_ambiguous_or_untrusted_invites_never_unlock_bot(
    store, settings, message, text, owner, mentions
):
    settings.bot_qq_ids = {"60000"}
    m = group(message, user_id=owner, command_text=text, mentioned_ids=mentions)
    assert not await store.bot_guard.invite(m)
    assert not await store.bot_guard.prepare(incoming_bot(message))
    assert not await store.all("SELECT * FROM bot_authorized_events")


async def test_grant_is_persistent_but_old_legacy_tickets_do_not_authorize(
    store, settings, message
):
    settings.bot_qq_ids = {"60000"}
    owner = invitation(message)
    bot = incoming_bot(message)
    assert await store.bot_guard.invite(owner)
    assert await store.bot_guard.prepare(bot)
    assert await store.admit(bot) == "accepted"
    await store.close()
    recovered = Store(settings)
    await recovered.open()
    try:
        assert not authorize(bot, settings)
        assert await recovered.bot_guard.prepare(bot)
        assert authorize(bot, settings)
        assert await recovered.bot_guard.may_process(bot, recovered.event_key(bot))
        assert not await recovered.bot_guard.prepare(incoming_bot(message, conversation_id="50000"))
        assert COOLDOWN_SECONDS == 600
    finally:
        await recovered.close()


async def test_runtime_metrics_follow_bot_timezone_at_utc_boundary(store, monkeypatch):
    from datetime import UTC, datetime

    instant = datetime(2026, 11, 14, 16, 1, tzinfo=UTC).timestamp()
    monkeypatch.setattr("hakimi.runtime_state.time.time", lambda: instant)
    await store.runtime.event("noise_ignored", "production")
    row = await store.one("SELECT day FROM daily_metrics WHERE code='noise_ignored'")
    assert row[0] == "2026-11-15"
