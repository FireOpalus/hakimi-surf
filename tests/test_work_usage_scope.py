import asyncio
from decimal import Decimal

from hakimi.life_rules import build_from_video, milestone_sharing
from hakimi.work_usage import model_work
from hakimi.world_state import MilestoneEvent


async def test_reservations_stay_with_concurrent_actions_and_keep_unknown_cost(store):
    async def run(key):
        with model_work("bili.patrol", key), model_work("bili.video_study", key + "-study"):
            r = await store.reserve("subject", "patrol", Decimal(".0001"))
            await asyncio.sleep(0)
            await store.settle(r, None if key == "b" else Decimal(".00004"))

    await asyncio.gather(run("a"), run("b"))
    work = {w["root_id"]: w for w in (await store.usage())["recent_work"]}
    assert work["a"]["work_id"] == "a-study"
    assert work["a"]["estimated_microdollars"] == work["a"]["charged_microdollars"] == 40
    assert work["b"]["work_id"] == "b-study"
    assert work["b"]["estimated_microdollars"] is None
    assert work["b"]["charged_microdollars"] == 100


def test_private_source_cannot_trigger_build_or_fallback_share(settings):
    event = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        importance="high",
        payload={"private_first": True, "title": "MC 私人灵感"},
    )
    assert build_from_video(event) is None
    assert milestone_sharing(settings).match(event) is None


def test_owner_private_milestone_is_bound_to_original_account(settings):
    from hakimi.world_state import private_event_audience

    settings.owner_qq_id = "20000"
    event = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        importance="high",
        payload={
            "private_first": True,
            "owner_private": True,
            "audience": private_event_audience(settings, "20000"),
        },
    )
    assert milestone_sharing(settings).match(event).recipient == "owner:20000"
    settings.owner_qq_id = "30000"
    assert milestone_sharing(settings).match(event) is None
