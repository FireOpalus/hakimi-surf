import time

import pytest

from hakimi.bot_guard import COOLDOWN_SECONDS
from hakimi.models import ImageRef
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.fixture
def bot_service(settings, store):
    settings.owner_qq_id = "20000"
    settings.bot_qq_ids = {"60000", "60001"}
    settings.onebot_allowed_users.update(settings.bot_qq_ids)
    settings.group_policies.update({"onebot:40000": "smart_reply", "onebot:50000": "smart_reply"})
    return Service(settings, store, FakeGateway(), FakeBili(), FakeSender())


def invitation(message, *, event="owner-invitation", group="40000", target="60000"):
    return message.model_copy(
        update={
            "event_id": event,
            "kind": "group",
            "conversation_id": group,
            "command_text": "/bot-interact 讨论天文",
            "text": "/bot-interact [提及群友] 讨论天文",
            "mentioned_ids": ["10000", target],
            "created_at": time.time(),
        }
    )


def bot_message(message, *, event="bot-turn", group="40000", user="60000"):
    return message.model_copy(
        update={
            "event_id": event,
            "user_id": user,
            "kind": "group",
            "conversation_id": group,
            "text": "一起聊聊天文吧",
            "command_text": "一起聊聊天文吧",
            "addressed": False,
            "created_at": time.time(),
        }
    )


async def process(service, message):
    assert await service.accept(message) == "accepted"
    job = await service.store.claim()
    assert job
    await service.process(job)
    return job


@pytest.mark.parametrize(
    "kind,policy,addressed",
    [
        ("group", "smart_reply", False),
        ("group", "smart_reply", True),
        ("group", "at_only", True),
        ("private", "smart_reply", True),
    ],
)
async def test_known_bots_ignored_before_queue_images_or_any_api(
    bot_service, message, kind, policy, addressed
):
    bot_service.s.group_policies["onebot:40000"] = policy
    bot = bot_message(message).model_copy(
        update={
            "kind": kind,
            "conversation_id": "60000" if kind == "private" else "40000",
            "addressed": addressed,
            "images": [ImageRef(url="https://gchat.qpic.cn/example")],
            "forward_ids": ["remote"],
        }
    )
    assert await bot_service.accept(bot) == "ignored"
    assert not bot_service.ai.calls and not bot_service.bili.calls
    assert await bot_service.store.claim() is None
    assert not await bot_service.store.all("SELECT * FROM counters")
    assert not await bot_service.store.all("SELECT * FROM ledger")
    assert not list(bot_service.s.data_dir.glob("sessions/*/*.sqlite3"))


async def test_explicit_owner_invitation_is_the_only_bounded_exception(bot_service, message):
    invite = invitation(message)
    assert await bot_service.bot_guard.invite(invite) == {"60000"}
    first, second, third = [bot_message(message, event=f"bounded-{n}") for n in range(3)]
    for item in (first, second):
        assert await bot_service.bot_guard.prepare(item)
        assert await bot_service.store.admit(item) == "accepted"
    assert not await bot_service.bot_guard.prepare(third)
    assert not bot_service.ai.calls
    assert not await bot_service.bot_guard.invite(invitation(message, event="repeat-owner"))


async def test_known_bot_cannot_invoke_commands_even_as_legacy_admin(bot_service, message):
    bot_service.s.onebot_owner_ids.add("60000")
    bot = bot_message(message).model_copy(
        update={"text": "/new-session", "command_text": "/new-session"}
    )
    await bot_service.sessions.remember(bot, "keep-group-memory", "reply")
    assert await bot_service.accept(bot) == "ignored"
    assert (await bot_service.sessions.history(bot))[0]["user"] == "keep-group-memory"
    assert not bot_service.ai.calls


async def test_legacy_queued_bot_jobs_and_outbox_are_blocked(bot_service, message):
    bot = bot_message(message)
    bot_service.s.bot_qq_ids = set()
    assert await bot_service.accept(bot) == "accepted"
    job = await bot_service.store.claim()
    await bot_service.store.enqueue_reply(job["id"], bot, "old-bot-reply")
    bot_service.s.bot_qq_ids = {"60000"}
    await bot_service.process(job)
    assert not bot_service.ai.calls
    assert await bot_service.deliver_once()
    assert not bot_service.sender.sent
    assert (await bot_service.store.one("SELECT status FROM outbox"))[0] in {"blocked", "cancelled"}


async def test_legacy_sql_tickets_never_reactivate_blacklist(bot_service, message):
    svc = bot_service
    bot = bot_message(message)
    now = time.time()
    scope = svc.store.session_scope(bot)
    async with svc.store.transaction():
        await svc.store.db.execute(
            "INSERT INTO bot_interactions VALUES (?,?,?,?,?,0,0,?)",
            (scope, svc.store.subject(bot), "old-topic", svc.bot_guard.owner_key(), now + 600, now),
        )
        await svc.store.db.execute(
            "INSERT INTO bot_turns VALUES (?,?,?,?,?,?)",
            (
                svc.store.event_key(bot),
                scope,
                svc.store.subject(bot),
                "old-topic",
                svc.bot_guard.owner_key(),
                now + 600,
            ),
        )
    assert not await svc.bot_guard.prepare(bot)
    assert not await svc.bot_guard.may_process(bot, svc.store.event_key(bot))
    assert await svc.accept(bot) == "ignored"
    assert not svc.ai.calls


async def test_fixed_window_expires_without_rearming_and_binds_group(
    bot_service, message, monkeypatch
):
    svc = bot_service
    assert await svc.bot_guard.invite(invitation(message))
    assert not await svc.bot_guard.prepare(bot_message(message, group="50000"))
    assert not await svc.bot_guard.prepare(bot_message(message, user="60001"))
    first = bot_message(message)
    assert await svc.bot_guard.prepare(first)
    assert await svc.store.admit(first) == "accepted"
    later = time.time() + COOLDOWN_SECONDS + 2
    monkeypatch.setattr("hakimi.bot_guard.time.time", lambda: later)
    assert not await svc.bot_guard.prepare(bot_message(message, event="after-window"))
    assert not await svc.bot_guard.may_process(first, svc.store.event_key(first))
    assert not svc.ai.calls
