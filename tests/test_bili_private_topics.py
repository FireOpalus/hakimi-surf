"""One private video request per selection; receipts contain only an opaque identity."""

import json
import re

from hakimi.bili_watcher import BiliPolicy, BiliWatcher
from hakimi.generation_policy import Scenario
from hakimi.session_manager import SessionManager
from tests.test_bili_watcher import BV, OTHER, Gateway
from tests.test_bili_watcher import watcher as watcher

THIRD = "BV1xx411c7mE"


async def test_served_private_requests_rotate_to_another_friend_and_then_the_public_feed(
    watcher, message, monkeypatch
):
    watcher.s.onebot_allowed_users = {"30000", "40001"}
    sessions = watcher.svc.sessions = SessionManager(watcher.s, watcher.store)
    older = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    newer = message.model_copy(update={"user_id": "40001", "conversation_id": "40001"})
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    await sessions.remember(older, "推荐一个邮票视频", "好")
    await sessions.remember(newer, "帮我找个极光视频", "好")
    await sessions.remember(group, "群里聊火车", "好")
    # Registry order is not a chronological ordering.
    registry = [(watcher.store.session_scope(m), m) for m in (older, newer, group)]

    async def ordered():
        return registry

    monkeypatch.setattr(watcher.store.runtime, "sessions", ordered)
    original_snapshot = sessions.snapshot

    async def group_snapshot_only(target):
        assert target.kind == "group", "private selector must not load summaries or model chains"
        return await original_snapshot(target)

    monkeypatch.setattr(sessions, "snapshot", group_snapshot_only)
    requests = []
    skip = True

    class Select(Gateway):
        async def generate(self, payload, subject, **kwargs):
            if kwargs["scenario"] != Scenario.BILI_SELECTION:
                return await super().generate(payload, subject, **kwargs)
            requests.append(payload["memory_preferences"])
            # The bogus ref cannot choose either a different source or a different receipt.
            return json.dumps(
                {
                    "selected": []
                    if skip
                    else [
                        {
                            "bvid": payload["candidates"][0]["bvid"],
                            "private_topic_ref": "owner:20000",
                        }
                    ]
                }
            )

    watcher.svc.ai = Select()
    await watcher.set_policy(BiliPolicy(enabled=True))

    def recommend(bvid):
        watcher.responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"][0]["bvid"] = bvid
        watcher.responses["/x/web-interface/view"]["bvid"] = bvid

    recommend(BV)
    assert (await watcher.run_once())["learned"] == 0
    assert not await watcher.store.one("SELECT 1 FROM bili_served_private_topics")
    skip = False
    assert (await watcher.run_once())["learned"] == 1
    restarted = BiliWatcher(watcher.svc, watcher.http, sleep=watcher.sleep)
    await restarted.initialize()
    recommend(OTHER)
    assert (await restarted.run_once())["learned"] == 1
    recommend(THIRD)
    assert (await restarted.run_once())["learned"] == 1

    assert [r["recent_chat_topics"] for r in requests] == [
        ["帮我找个极光视频"],
        ["帮我找个极光视频"],
        ["推荐一个邮票视频"],
        ["群里聊火车"],
    ]
    assert all(r["private_topic_refs"] == ["recent_chat_topics:0"] for r in requests[:3])
    assert "private_topic_refs" not in requests[3]
    assert await watcher.lookup_watched(BV, message=newer) and not await watcher.lookup_watched(
        BV, message=older
    )
    assert await watcher.lookup_watched(OTHER, message=older) and not await watcher.lookup_watched(
        OTHER, message=newer
    )
    assert await watcher.lookup_watched(THIRD, message=group)
    rows = await watcher.store.all("SELECT * FROM bili_served_private_topics")
    assert len(rows) == 2 and all(
        row.keys() == ["receipt"] and re.fullmatch(r"[a-f0-9]{64}", row[0]) for row in rows
    )
    assert all(row[0] not in str(requests) for row in rows)

    # Clearing/recreating the same interest is a new request even if row or event IDs repeat.
    await sessions.clear(older)
    await sessions.remember(older, "推荐一个邮票视频", "好")
    prefs, fresh = await restarted._memory_preferences()
    assert prefs["recent_chat_topics"] == ["推荐一个邮票视频"]
    assert fresh.receipt not in {r["receipt"] for r in rows}


async def test_real_source_event_survives_replayed_rows_but_new_event_can_request_same_text(
    watcher, message
):
    sessions = watcher.svc.sessions = SessionManager(watcher.s, watcher.store)
    msg = message.model_copy(update={"text": "帮我找个芯片视频"})
    epoch = await sessions.current_epoch(msg)
    await sessions.receive(msg, expected_epoch=epoch)
    await sessions.admit_history(msg, [msg], expected_epoch=epoch)
    _, first = await watcher._memory_preferences()
    expected = watcher.store.vault.digest(
        "bili-private-request-v1", watcher.store.session_scope(msg), epoch, ("event", msg.event_id)
    )
    assert first.receipt == expected
    await watcher.store.db.execute(
        "INSERT INTO bili_served_private_topics VALUES (?)", (first.receipt,)
    )
    # Even another copy of the same authenticated platform event remains served.
    async with sessions.database(msg) as (db, key):
        await db.execute(
            "INSERT INTO recent_messages(payload,expires) SELECT payload,expires "
            "FROM recent_messages ORDER BY id DESC LIMIT 1"
        )
    assert (await watcher._memory_preferences())[1] is None
    fresh_msg = msg.model_copy(update={"event_id": "new-event"})
    await sessions.receive(fresh_msg, expected_epoch=epoch)
    await sessions.admit_history(fresh_msg, [fresh_msg], expected_epoch=epoch)
    _, fresh = await watcher._memory_preferences()
    assert fresh.receipt != expected


async def test_failed_save_leaves_the_original_private_request_available(watcher, message):
    sessions = watcher.svc.sessions = SessionManager(watcher.s, watcher.store)
    await sessions.remember(message, "帮我找个天文视频", "好")
    _, original = await watcher._memory_preferences()
    await watcher.set_policy(BiliPolicy(enabled=True))
    await watcher.store.db.execute(
        "CREATE TRIGGER reject_private_priority BEFORE INSERT ON bili_private_share_priority "
        "BEGIN SELECT RAISE(ABORT,'simulated write failure'); END"
    )
    result = await watcher.run_once()
    assert result["learned"] == 0 and result["requires_review"]
    assert not await watcher.store.one("SELECT 1 FROM bili_served_private_topics")
    assert not await watcher.lookup_watched(BV, message=message)
    _, pending = await watcher._memory_preferences()
    assert pending.receipt == original.receipt
