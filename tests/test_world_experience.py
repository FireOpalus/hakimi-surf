import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from hakimi.planning_journal import PlannerJournal
from hakimi.rest_policy import RestHoursConfig
from hakimi.world_experience import (
    CHAT_BOREDOM_DELTA,
    HOURLY_RATES,
    NEWS_TTL,
    OWNER_CHAT_BOREDOM_DELTA,
    WorldExperience,
)
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
    assert (restarted.current.energy, restarted.current.boredom) == (81.0, 52)
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


async def test_private_news_stays_scoped_with_one_shared_social_charge(settings, store, message):
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
    assert await x.battery(grp, now) == await x.battery(message, now) == 98.75
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
    assert await x.battery(friend, now) == await x.battery(grp, now) == 96.25
    assert (await world.refresh()).energy == 75.4


async def test_shared_recharge_excludes_overlapping_chats_then_uses_actual_rest(
    settings, store, message
):
    now = stamp(16)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await store.runtime.set_rest_hours(RestHoursConfig(rest_hours="16:20-16:22"))
    await world.initialize()
    x = world.experience
    async with store.transaction():
        await store.db.execute("UPDATE world_social_battery SET charge=50")
    async with x.chatting():
        now += 300
        with pytest.raises(asyncio.CancelledError):
            async with x.chatting():
                now += 300
                raise asyncio.CancelledError
        assert await x.battery(message, now) == 50
        now += 300
        async with store.transaction():
            await x.confirmed_tx(message, "overlap", now, ["第一句", "第二句"])
    assert await x.battery(group(message), now) == 50
    now = stamp(16, 22)  # Five idle minutes and two sleeping minutes.
    assert await x.battery(message, now) == 51.5
    async with store.transaction():
        assert await x.start_day_rest_tx(now, now + 120)
    now += 120
    reopened = WorldStateManager(settings, store, clock=lambda: now)
    await reopened.initialize()
    assert await reopened.experience.battery(message, now) == 52.5
    assert reopened.current.energy == 81.7  # -0.2 chat, -0.67 idle, +0.5 sleep, +2.0 nap (0-10m).
    now += 24 * 3600
    assert await reopened.experience.battery(message, now) == 100


async def test_legacy_latest_battery_and_signed_energy_migrate_once(settings, store, message):
    now = stamp(16)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    async with store.transaction():
        await store.db.execute("DELETE FROM world_social_battery")
        await store.db.executemany(
            "INSERT INTO world_social VALUES (?,?,?,?)",
            [("group", "群聊", 12, now - 600), ("private", "私聊", 65.5, now - 150)],
        )
        await store.db.execute("UPDATE world_rhythm SET energy_fraction=-0.42")
    for _ in range(2):
        world = WorldStateManager(settings, store, clock=lambda: now)
        await world.initialize()
        assert await world.experience.battery(message, now) == 65.75
        assert world.current.energy == 79.6
        assert (await store.one("SELECT energy_fraction FROM world_rhythm"))[0] == pytest.approx(
            -0.02
        )
    async with store.transaction():
        await store.db.execute("UPDATE world_social_battery SET charge=0.1")
        await world.experience.confirmed_tx(group(message), "floor", now, ["收到"])
    assert await world.experience.battery(message, now) == 0


async def test_interrupted_turn_recharges_offline_and_old_ack_stays_charged(
    settings, store, message
):
    now = stamp(15)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    async with store.transaction():
        await store.db.execute("UPDATE world_social_battery SET charge=50, active_turns=1")
        await world.experience.confirmed_tx(message, "delivered", now, ["收到"])
    now += 600
    reopened = WorldStateManager(settings, store, clock=lambda: now)
    await reopened.initialize()
    async with store.transaction():
        await reopened.experience.confirmed_tx(message, "delivered", now - 600, ["收到"])
    assert await reopened.experience.battery(message, now) == 51.0
    assert reopened.current.energy == 79.5


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
            await store.db.execute("UPDATE world_social_battery SET charge=30.25")
            for n in range(2):
                await svc.world_state.experience.confirmed_tx(grp, str(n), now, ["群聊回复"])
        assert await svc.accept(grp) == "ignored"
        assert not await store.all("SELECT id FROM jobs")
        assert await svc.accept(message) == "accepted"
        mention = group(message, event_id="g2", text="哈基米，晚饭吃什么", addressed=True)
        assert await svc.accept(mention) == "accepted"
        assert svc.ai.calls == []


async def test_living_states_longing_interest_circadian_and_owner_zero_social_cost(
    settings, store, message
):
    now = stamp(7, 40)
    await store.runtime.set_rest_hours(RestHoursConfig(enabled=True, rest_hours="23:20-07:30"))
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    x = world.experience

    # 1. Migrated/initialized at 07:40 -> just_woke_up (< 25 min after 07:30), longing = 0
    prefs0 = await x.preferences(now)
    assert prefs0["circadian_tag"] == "just_woke_up"
    assert prefs0["owner_longing"] == 0
    assert prefs0["interest_lingering"] == ""

    # 2. Private DM with real Owner keeps social_battery at 100.0 (OWNER_CHAT_SOCIAL_COST=0.0)
    async with store.transaction():
        await x.confirmed_tx(message, "owner-dm-1", now, ["早安"])
    assert await x.battery(message, now) == 100.0

    # 3. Public event populates interest_lingering for 2.5h; private_first never leaks
    await world.publish_event(
        "bilibili_surf",
        "bili_video_watched",
        {
            "bvid": "BV1xx411c7mD",
            "private_first": True,
            "audience": private_event_audience(settings, "30000"),
            "title": "私密推荐内容",
        },
        business_key="priv-skip",
    )
    assert (await x.preferences(now))["interest_lingering"] == ""

    await world.publish_event(
        "bilibili_surf",
        "bili_video_watched",
        {"bvid": "BV1Q541167Qg", "title": "手搓异步引擎实录"},
        business_key="pub-ok",
    )
    assert "手搓异步引擎实录" in (await x.preferences(now))["interest_lingering"]
    assert (await x.preferences(now + 9001))["interest_lingering"] == ""

    # 4. Elapsed 10 hours without Owner raises owner_longing; touching Owner resets it to 0
    later = now + 10 * 3600
    prefs_later = await x.preferences(later)
    assert prefs_later["owner_longing"] == 80
    await x.touch_owner(later)
    assert (await x.preferences(later))["owner_longing"] == 0

    # 5. Circadian tags at bedtime / sleep window
    assert x.circadian_tag(stamp(23, 0), energy=80.0) == "bedtime_drowsy"
    assert x.circadian_tag(stamp(23, 30), energy=80.0) == "late_night_catch"
    assert x.circadian_tag(stamp(15, 0), energy=30.0) == ""
    assert x.circadian_tag(stamp(15, 0), energy=20.0) == "bedtime_drowsy"
    assert x.circadian_tag(stamp(15, 0), energy=80.0) == ""


async def test_mc_online_companion_sets_medium_focus_and_releases(settings, store):
    now = stamp(14)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await PlannerJournal(store).initialize(now)
    x = world.experience

    await x.mc_snapshot("online", now)
    assert world.current.current_module == "minecraft"
    assert world.current.focus_level == "MEDIUM"
    assert world.current.mood == "playful"
    assert not world.proactive_suspended()
    assert (await x.view(now))["actual"]["state"] == "mc_play"

    now += 1
    await x.mc_snapshot("mine", now)
    assert world.current.focus_level == "HIGH"
    assert world.current.mood == "focused"
    assert world.proactive_suspended()
    assert (await x.view(now))["actual"]["state"] == "mc_work"

    now += 1
    await x.mc_snapshot("", now, disconnected=True)
    assert world.current.current_module == "idle"
    assert world.current.focus_level == "LOW"
    assert world.current.mood == "calm"
    assert (await x.view(now))["actual"]["state"] == "idle"


async def test_boredom_deltas_idle_rate_and_owner_longing_slope(settings, store, message):
    assert CHAT_BOREDOM_DELTA == -2
    assert OWNER_CHAT_BOREDOM_DELTA == -3
    assert HOURLY_RATES["idle"] == (-2, 24)
    now = stamp(14)
    assert WorldExperience._owner_longing(now - 7200, now) == 0
    assert WorldExperience._owner_longing(now - 5 * 3600, now) == 30
    assert WorldExperience._owner_longing(now - 10 * 3600, now) == 80
    assert WorldExperience._owner_longing(now - 15 * 3600, now) == 100

    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    x = world.experience
    async with store.transaction():
        await x.confirmed_tx(group(message), "grp-1", now, ["嗨"])
    await world.refresh()
    assert world.current.boredom == 18

    async with store.transaction():
        await x.confirmed_tx(message, "owner-1", now, ["在呢"])
    await world.refresh()
    assert world.current.boredom == 15


async def test_mc_goal_completed_ignores_non_substantive_actions_and_preserves_energy(
    settings, store, message
):
    now = stamp(15)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    await PlannerJournal(store).initialize(now)
    x = world.experience
    await world.update_state(energy=70, boredom=40)

    # Non-substantive actions (chat/observe/goto/follow/shelter/chest) are ignored completely.
    for action_type in ("chat", "observe", "goto", "follow", "shelter", "chest"):
        await world.publish_event(
            "minecraft",
            "mc_goal_completed",
            {"action_type": action_type, "summary": f"routine:{action_type}"},
            business_key=f"skip-{action_type}",
        )
    assert world.current.energy == 70
    assert world.current.boredom == 40
    assert world.current.mood != "pleased"
    assert await x.news(message, now) == []

    # Substantive MC goal reduces boredom & posts news WITHOUT double-charging energy.
    await world.publish_event(
        "minecraft",
        "mc_goal_completed",
        {"action_type": "craft", "summary": "合成铁镐 x1"},
        business_key="craft-iron-pick",
    )
    assert world.current.energy == 70
    assert world.current.boredom == 34
    assert world.current.mood == "pleased"
    assert [n["title"] for n in await x.news(message, now)] == ["合成铁镐 x1"]


async def test_mc_world_news_marks_shared_on_paraphrased_or_proactive_reply_and_uses_6h_ttl(
    settings, store, message
):
    now = stamp(8, 12)
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    x = world.experience

    await world.publish_event(
        "minecraft",
        "mc_goal_completed",
        {"action_type": "gather", "summary": "已采集oak_log x2(16,72,11)"},
        business_key="gather-oak-1",
    )
    await world.publish_event(
        "minecraft",
        "mc_goal_completed",
        {"action_type": "craft", "summary": "已合成 stick x4"},
        business_key="craft-stick-1",
    )
    assert len(await x.news(message, now)) == 2
    # Routine mc_goal_completed news expires in 6h instead of lingering 48h.
    assert await x.news(message, now + 6 * 3600 + 1) == []

    # Paraphrased Chinese proactive DM ("刚砍了两块木头搓出四根木棍") marks MC news shared.
    async with store.transaction():
        await x.confirmed_tx(
            message,
            "jit:intent-1",
            now + 60,
            ["BOISER 救命，刚砍了两块木头搓出四根木棍，天一黑就被堵着送回家了"],
        )
    assert await x.news(message, now + 120) == []

    # Rare finds still keep the 48h TTL and are NOT falsely cleared by unrelated non-MC DMs.
    await world.publish_event(
        "minecraft",
        "mc_rare_find",
        {"block": "diamond_ore", "summary": "发现钻石矿"},
        business_key="rare-dia-1",
    )
    async with store.transaction():
        await x.confirmed_tx(message, "jit:intent-2", now + 180, ["今天天气真不错呀"])
    assert len(await x.news(message, now + 12 * 3600)) == 1
