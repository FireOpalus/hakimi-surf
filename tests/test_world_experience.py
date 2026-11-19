import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from hakimi.planning_journal import PlannerJournal
from hakimi.rest_policy import RestHoursConfig
from hakimi.world_experience import NEWS_TTL
from hakimi.world_state import SCHEMA, WorldStateManager, private_event_audience
from tests.test_social_integration import group

ZONE = ZoneInfo("Asia/Shanghai")


def stamp(hour, minute=0):
    return datetime(2026, 11, 18, hour, minute, tzinfo=ZONE).timestamp()


async def test_migration_elapsed_rest_and_restart(settings, store):
    now = stamp(23)
    await store.runtime.set_rest_hours(RestHoursConfig(rest_hours="23:30-08:30"))
    async with store.lock:
        await store.db.executescript(SCHEMA)
        await store.db.execute(
            "INSERT INTO world_state(singleton,current_module,focus_level,energy,boredom) "
            "VALUES(1,'bilibili_surf','MEDIUM',52,48)"
        )
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    assert world.current.current_module == "idle"
    initial = world.current.energy, world.current.boredom
    assert initial == (52, 48)
    # Half an hour awake, then two hours of scheduled sleep across midnight.
    end = now + 9000
    await world.experience.advance(now + 1800)
    await world.experience.advance(now + 2700)
    restarted = WorldStateManager(settings, store, clock=lambda: end)
    await restarted.initialize()
    await asyncio.gather(*(restarted.experience.advance(end) for _ in range(4)))
    assert (restarted.current.energy, restarted.current.boredom) == (82, 46)
    assert restarted.current.mood == "sleeping"
    rows = await store.all("SELECT reason,energy_after-energy_before AS d FROM world_changes")
    assert sum(r["d"] for r in rows if r["reason"] == "sleep") == 30
    assert sum(r["reason"] == "baseline" for r in rows) == 1


async def test_confirmed_activity_lease_and_history(settings, store):
    now = stamp(15)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await PlannerJournal(store).initialize(now)
    x = world.experience
    await x.bili_reading(True, now)
    assert (await x.view(now))["actual"]["state"] == "bili_read"
    now += 600
    await x.bili_reading(False, now)
    assert (world.current.energy, world.current.boredom) == (80, 20)
    await x.bili_reading(True, now)
    now += 600
    args = (
        "bilibili_surf",
        "bili_video_watched",
        {"bvid": "BV123456789A", "learning_seconds": 600},
    )
    await asyncio.gather(*(world.publish_event(*args, business_key="v1") for _ in range(2)))
    await x.bili_reading(False, now)
    view = await x.view(now)
    assert (world.current.energy, world.current.boredom) == (76, 12)
    assert view["actual"]["last"]["reason"] == "video_learned" and view["news_count"] == 1
    assert view["plan"] is None
    await x.mc_snapshot("mine", now)
    assert world.proactive_suspended()
    now += 1
    await x.mc_snapshot("", now)
    await x.mc_snapshot("mine", now - 1)
    assert not world.proactive_suspended()
    now += 1
    await x.mc_snapshot("gather", now)
    now += 41
    assert not world.proactive_suspended()
    await x.advance(now)
    assert (await x.view(now))["actual"]["state"] == "idle"
    await world.publish_event(
        "minecraft", "mc_goal_completed", {"summary": "已采集木材"}, business_key="task1"
    )
    await world.publish_event(
        "minecraft", "mc_goal_completed", {"summary": "已采集木材"}, business_key="task1"
    )
    assert (await x.view(now))["news_count"] == 2


async def test_private_news_receipts_and_independent_social_charge(settings, store, message):
    now = stamp(16)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await PlannerJournal(store).initialize(now)
    x = world.experience
    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    grp = group(message)
    secret = "BV123456789A"
    public = "BV123456789B"
    await world.publish_event(
        "bilibili_surf",
        "bili_video_watched",
        {
            "bvid": secret,
            "private_first": True,
            "audience": private_event_audience(settings, friend.user_id),
            "title": "私聊标题",
        },
        business_key="friend",
    )
    await world.publish_event(
        "bilibili_surf",
        "bili_video_watched",
        {"bvid": public, "title": "红石小门实验"},
        business_key="public",
    )
    assert [r["ref"] for r in await x.news(friend, now)] == [secret, public]
    assert [r["ref"] for r in await x.news(message, now)] == [public]
    assert [r["ref"] for r in await x.news(grp, now)] == [public]
    serialized = json.dumps(await x.view(now), ensure_ascii=False)
    assert secret not in serialized and "私聊标题" not in serialized
    async with store.transaction():
        await x.confirmed_tx(grp, "same", now, [secret, public])
        await x.confirmed_tx(grp, "same", now, [public])
    assert await x.battery(grp, now) == 88
    assert await x.battery(message, now) == 100
    assert await x.battery(grp, now + 3600) == 100
    assert not await x.news(grp, now)
    assert [r["ref"] for r in await x.news(friend, now)] == [secret]
    digest = {"public_summaries": ["红石小门实验非常好玩", "其他活动"], "world_events": []}
    await x.filter_digest(digest, now)
    assert digest["public_summaries"] == ["其他活动"]
    async with store.transaction():
        await x.confirmed_tx(friend, "answered", now, ["聊点别的"])
    assert len(await x.news(friend, now)) == 1
    assert not await x.news(friend, now + NEWS_TTL)
    async with store.transaction():
        await x.confirmed_tx(friend, "reference", now, [secret])
    assert not await x.news(friend, now)
    assert await x.battery(friend, now) == 76


async def test_low_energy_indicator_is_available_in_status(settings, store):
    now = stamp(16)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await PlannerJournal(store).initialize(now)
    await world.update_state(energy=29, boredom=70)
    assert (await world.experience.view(now))["low_energy"]
    await world.update_state(energy=40)
    assert (await world.experience.view(now))["low_energy"]
    await world.update_state(energy=45)
    assert not (await world.experience.view(now))["low_energy"]


async def test_sharing_reservation_requires_confirmed_delivery(settings, store, message):
    from hakimi.world_state import IntentTicketCreate

    now = stamp(15)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await world.publish_event(
        "minecraft", "mc_goal_completed", {"summary": "完成木屋探索"}, business_key="mc1"
    )
    ids = []
    for target in ("40000", "50000"):
        key = await world.enqueue_intent(
            IntentTicketCreate(
                action_name="qq.speak",
                action_type="group_chat_share",
                target_scope="onebot:" + target,
                target_module="qq_social",
                execute_after=now,
                expire_at=now + 1200,
            )
        )
        await world.claim(key, now)
        ids.append(key)
    grp = group(message)

    async def reserve(key):
        return await world.experience.reserve_news(grp, {"id": key}, now)

    first, second = await asyncio.gather(*(reserve(key) for key in ids))
    assert sum(item is not None for item in (first, second)) == 1
    winner, waiting = ids if first else ids[::-1]
    assert not await world.experience.news(grp, now)
    await world.mark_intent(winner, "gated_skip", expected="executing", reason="abstained", now=now)
    assert (await reserve(waiting))["title"] == "完成木屋探索"
    event = await store.one("SELECT event_id FROM world_news WHERE intent_id=?", (waiting,))
    async with store.transaction():
        await world.experience.confirmed_tx(
            grp, "jit:" + waiting, now, ["小木屋弄好了"], dedicated=event[0]
        )
    assert await reserve(winner) is None


async def test_low_social_charge_only_gates_ambient_group_speech(settings, store, message):
    import httpx

    from hakimi.adapters import Sender
    from hakimi.service import Service
    from tests.fakes import FakeBili, FakeGateway
    from tests.test_social_integration import Wire

    settings.group_policies["onebot:40000"] = "smart_reply"
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, Wire()))
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        grp = group(
            message, addressed=False, text="晚饭吃什么", command_text="晚饭吃什么", mentioned_ids=[]
        )
        now = svc.world_state.clock()
        async with store.transaction():
            for n in range(6):
                await svc.world_state.experience.confirmed_tx(grp, str(n), now, ["群聊回复"])
        assert await svc.accept(grp) == "ignored"
        assert not await store.all("SELECT id FROM jobs")
        assert await svc.accept(message) == "accepted"
        mention = group(message, event_id="g2", text="哈基米，晚饭吃什么", addressed=True)
        assert await svc.accept(mention) == "accepted"
        assert svc.ai.calls == []
