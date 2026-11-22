"""Tests for fine-grained usage categories, model_work attribution, and admin label updates."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx

from hakimi.app import create_app
from hakimi.gemini import GeminiGateway
from hakimi.work_usage import model_work, reservation_work
from tests.fakes import FakeGateway
from tests.test_plugin_bus import _get_bus, _plugin_settings, _PluginSocket
from tests.test_screen_watch import _screen_settings
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_voice_companion import TINY_PNG_B64, _companion_settings, _SocketDouble

_ROOT = Path(__file__).resolve().parents[1]


def _offline_gateway(settings, store, sdk_pair):
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=interaction_response())

    return GeminiGateway(settings, store, *sdk_pair(handle))


async def _insert_ledger_row(
    store,
    row_id: str,
    *,
    day: str,
    subject: str,
    lane: str,
    amount: int,
    work_kind: str | None = None,
    root_kind: str | None = None,
) -> None:
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO ledger VALUES (?,?,?,?,?,?,'settled')",
            (row_id, day, subject, lane, amount, amount),
        )
        if work_kind is not None or root_kind is not None:
            await store.db.execute(
                "INSERT INTO ledger_work VALUES (?,?,?,?,?)",
                (
                    row_id,
                    work_kind if work_kind is not None else "",
                    f"w-{row_id}",
                    root_kind if root_kind is not None else "",
                    f"r-{row_id}",
                ),
            )


async def test_usage_categories_classification_and_invariants(store):
    today = store.local_now().date().isoformat()
    yesterday = "2020-01-01"

    empty_overview = await store.usage_overview()
    empty_full = await store.usage()
    assert empty_overview["lanes"] == []
    assert empty_overview["categories"] == []
    assert empty_full["lanes"] == []
    assert empty_full["categories"] == []

    seed_rows = [
        # chat lane
        ("c1", today, "onebot:u1", "chat", 1200, "qq.reply", "qq.reply"),
        ("c2", today, "onebot:u2", "chat", 800, None, None),
        ("c3", today, "voice:owner:20000", "chat", 1500, "voice.companion", "voice.companion"),
        # proactive lane
        ("p1", today, "world:planner", "proactive", 900, "world.plan", "world.plan"),
        ("p2", today, "world:planner", "proactive", 300, "proactive.screen.output", "world.plan"),
        ("p3", today, "plugin:minecraft", "proactive", 2000, "mc.play", "mc.play"),
        (
            "p4",
            today,
            "plugin:minecraft",
            "proactive",
            400,
            "plugin.minecraft.screen",
            "plugin.minecraft.screen",
        ),
        # Legacy today's 42 orphan rows: proactive.screen.output with subject plugin:minecraft
        (
            "p5",
            today,
            "plugin:minecraft",
            "proactive",
            600,
            "proactive.screen.output",
            "proactive.screen.output",
        ),
        ("p6", today, "onebot:g1", "proactive", 700, "qq.speak", ""),
        (
            "p7",
            today,
            "onebot:owner",
            "proactive",
            500,
            "qq.owner_digest",
            "qq.owner_digest",
        ),
        ("p8", today, "screen:owner:20000", "proactive", 350, "screen.watch", "screen.watch"),
        (
            "p9",
            today,
            "voice:owner:20000",
            "proactive",
            450,
            "voice.companion",
            "voice.companion",
        ),
        ("p10", today, "legacy:sub", "proactive", 250, None, None),
        ("p11", today, "other:sub", "proactive", 150, "unknown.work", "unknown.work"),
        # patrol lane
        ("b1", today, "bili:diary", "patrol", 1100, "diary.daily_summary", "diary.daily_summary"),
        ("b2", today, "plugin:bilibili_surf", "patrol", 1300, "plugin.bilibili_surf.video", ""),
        ("b3", today, "bili:patrol", "patrol", 400, None, None),
        # memory / memory_flex / admin / custom lane
        ("m1", today, "mem:1", "memory", 320, "memory.compress", "memory.compress"),
        ("m2", today, "mem:2", "memory_flex", 180, "memory.extract", "memory.extract"),
        ("a1", today, "admin:sandbox", "admin", 220, "admin.sandbox", "admin.sandbox"),
        ("x1", today, "exp:1", "custom_lane", 90, "exp.run", "exp.run"),
        # Excluded rows: search lane & yesterday
        ("s1", today, "onebot:u1", "search", 9999, "search.query", "search.query"),
        ("y1", yesterday, "onebot:u1", "chat", 8888, "qq.reply", "qq.reply"),
    ]
    for row_id, day, subject, lane, amount, work_kind, root_kind in seed_rows:
        await _insert_ledger_row(
            store,
            row_id,
            day=day,
            subject=subject,
            lane=lane,
            amount=amount,
            work_kind=work_kind,
            root_kind=root_kind,
        )

    overview = await store.usage_overview()
    full = await store.usage()

    assert overview["lanes"] == full["lanes"]
    assert overview["categories"] == full["categories"]

    lanes = overview["lanes"]
    categories = overview["categories"]

    # Strict invariants: zero missed or double-counted rows/amounts
    assert sum(c["calls"] for c in categories) == sum(row["calls"] for row in lanes)
    assert sum(c["amount"] for c in categories) == sum(row["amount"] for row in lanes)
    assert len({c["category"] for c in categories}) == len(categories)
    assert all(c["lane"] != "search" and c["category"] != "search" for c in categories)

    by_cat = {c["category"]: c for c in categories}
    assert by_cat["chat"] == {"category": "chat", "lane": "chat", "amount": 2000, "calls": 2}
    assert by_cat["voice_companion"] == {
        "category": "voice_companion",
        "lane": "chat",
        "amount": 1950,
        "calls": 2,
    }
    assert by_cat["world_plan"] == {
        "category": "world_plan",
        "lane": "proactive",
        "amount": 1200,
        "calls": 2,
    }
    assert by_cat["minecraft"] == {
        "category": "minecraft",
        "lane": "proactive",
        "amount": 3000,
        "calls": 3,
    }
    assert by_cat["qq_proactive"] == {
        "category": "qq_proactive",
        "lane": "proactive",
        "amount": 700,
        "calls": 1,
    }
    assert by_cat["owner_digest"] == {
        "category": "owner_digest",
        "lane": "proactive",
        "amount": 500,
        "calls": 1,
    }
    assert by_cat["screen_watch"] == {
        "category": "screen_watch",
        "lane": "proactive",
        "amount": 350,
        "calls": 1,
    }
    assert by_cat["proactive_other"] == {
        "category": "proactive_other",
        "lane": "proactive",
        "amount": 400,
        "calls": 2,
    }
    assert by_cat["patrol_diary"] == {
        "category": "patrol_diary",
        "lane": "patrol",
        "amount": 1100,
        "calls": 1,
    }
    assert by_cat["patrol"] == {
        "category": "patrol",
        "lane": "patrol",
        "amount": 1700,
        "calls": 2,
    }
    assert by_cat["memory"] == {"category": "memory", "lane": "memory", "amount": 320, "calls": 1}
    assert by_cat["memory_flex"] == {
        "category": "memory_flex",
        "lane": "memory_flex",
        "amount": 180,
        "calls": 1,
    }
    assert by_cat["admin"] == {"category": "admin", "lane": "admin", "amount": 220, "calls": 1}
    assert by_cat["custom_lane"] == {
        "category": "custom_lane",
        "lane": "custom_lane",
        "amount": 90,
        "calls": 1,
    }


async def test_plugin_bus_model_work_attribution_and_no_orphan_screen_output(settings, sdk_pair):
    s = _plugin_settings(settings)
    app = create_app(
        s,
        gateway_factory=lambda cfg, st: _offline_gateway(cfg, st, sdk_pair),
        start_workers=False,
    )
    async with app.router.lifespan_context(app):
        svc = app.state.service
        bus = _get_bus(svc)
        svc.modules.unregister("minecraft")
        svc.modules.unregister("bilibili_surf")
        await bus.create_plugin(
            "minecraft",
            display_name="Minecraft",
            scopes=["actions", "sensors", "thinking_scenes"],
            node_region="KR",
        )
        await bus.create_plugin(
            "bilibili_surf",
            display_name="B站巡逻",
            scopes=["actions", "sensors", "thinking_scenes", "media", "node_bridge"],
            node_region="KR",
        )

        mc_sock = _PluginSocket()
        mc_run = asyncio.create_task(
            bus.serve(
                mc_sock.receive,
                mc_sock.send,
                mc_sock.close,
                source_ip="127.0.0.1",
                plugin_id="minecraft",
            )
        )
        bili_sock = _PluginSocket()
        bili_run = asyncio.create_task(
            bus.serve(
                bili_sock.receive,
                bili_sock.send,
                bili_sock.close,
                source_ip="127.0.0.1",
                plugin_id="bilibili_surf",
            )
        )
        try:
            await mc_sock.push(
                {
                    "type": "plugin.hello",
                    "protocol_version": 1,
                    "display_name": "Minecraft",
                    "scopes": ["actions", "sensors", "thinking_scenes"],
                    "thinking_scenes": [{"key": "mc.play", "label": "MC 游玩"}],
                }
            )
            await bili_sock.push(
                {
                    "type": "plugin.hello",
                    "protocol_version": 1,
                    "display_name": "B站巡逻",
                    "scopes": ["actions", "sensors", "thinking_scenes", "media", "node_bridge"],
                    "thinking_scenes": [
                        {"key": "bilibili_summary", "label": "视频摘要"},
                        {"key": "bili_learning", "label": "B站学习"},
                    ],
                }
            )
            await mc_sock.recv_until(lambda m: m["type"] == "plugin.ready")
            await bili_sock.recv_until(lambda m: m["type"] == "plugin.ready")

            # 1. ai.generate("mc.play") attributes both generation and inner output screen
            await mc_sock.push(
                {
                    "type": "ai.generate",
                    "id": "mc-gen-1",
                    "scenario": "mc.play",
                    "budget_lane": "proactive",
                    "payload": {"state": "exploring cave"},
                }
            )
            gen_res = (await mc_sock.recv_until(lambda m: m.get("id") == "mc-gen-1"))[-1]
            assert gen_res["ok"] is True

            # 2. ai.screen attributes to plugin.minecraft.screen
            await mc_sock.push(
                {
                    "type": "ai.screen",
                    "id": "mc-scr-1",
                    "budget_lane": "proactive",
                    "payload": {"chat": "hi"},
                }
            )
            scr_res = (await mc_sock.recv_until(lambda m: m.get("id") == "mc-scr-1"))[-1]
            assert scr_res["ok"] is True

            # 3. ai.video_analyze via video_watch.analyze -> plugin.bilibili_surf.video
            await bili_sock.push(
                {
                    "type": "ai.video_analyze",
                    "id": "bili-vw-1",
                    "bvid": "BV1xx411c7mD",
                    "budget_lane": "patrol",
                    "material": {"title": "测试视频", "subtitle_excerpt": "一段字幕内容"},
                }
            )
            vw_res = (await bili_sock.recv_until(lambda m: m.get("id") == "bili-vw-1"))[-1]
            assert vw_res["ok"] is True

            # 4. ai.video_analyze fallback path (video_watch=None) -> plugin.bilibili_surf.video
            orig_vw = getattr(svc, "video_watch", None)
            svc.video_watch = None
            try:
                await bili_sock.push(
                    {
                        "type": "ai.video_analyze",
                        "id": "bili-vid-1",
                        "bvid": "BV1xx411c7mD",
                        "budget_lane": "patrol",
                        "material": {"title": "测试视频", "subtitle_excerpt": "一段字幕"},
                    }
                )
                vid_res = (await bili_sock.recv_until(lambda m: m.get("id") == "bili-vid-1"))[-1]
                assert vid_res["ok"] is True
            finally:
                svc.video_watch = orig_vw

            rows = await svc.store.all(
                "SELECT l.lane, l.subject, w.kind, w.work_id, w.root_kind, w.root_id "
                "FROM ledger l LEFT JOIN ledger_work w ON w.id=l.id"
            )
            kinds = [r["kind"] for r in rows]
            assert "proactive.screen.output" not in kinds
            assert "patrol.screen.output" not in kinds

            mc_gen_rows = [r for r in rows if r["work_id"] == "mc-gen-1"]
            assert len(mc_gen_rows) >= 2  # main generation + inner output screen
            assert {(r["lane"], r["kind"], r["root_kind"], r["root_id"]) for r in mc_gen_rows} == {
                ("proactive", "mc.play", "mc.play", "mc-gen-1")
            }

            mc_scr_rows = [r for r in rows if r["work_id"] == "mc-scr-1"]
            assert len(mc_scr_rows) == 1
            assert (mc_scr_rows[0]["kind"], mc_scr_rows[0]["root_kind"]) == (
                "plugin.minecraft.screen",
                "plugin.minecraft.screen",
            )

            bili_vw_rows = [r for r in rows if r["work_id"] == "bili-vw-1"]
            assert len(bili_vw_rows) >= 2
            assert {(r["lane"], r["kind"], r["root_kind"], r["root_id"]) for r in bili_vw_rows} == {
                (
                    "patrol",
                    "plugin.bilibili_surf.video",
                    "plugin.bilibili_surf.video",
                    "bili-vw-1",
                )
            }

            bili_vid_rows = [r for r in rows if r["work_id"] == "bili-vid-1"]
            assert len(bili_vid_rows) >= 3  # source screen + generation + output screen
            assert {
                (r["lane"], r["kind"], r["root_kind"], r["root_id"]) for r in bili_vid_rows
            } == {
                (
                    "patrol",
                    "plugin.bilibili_surf.video",
                    "plugin.bilibili_surf.video",
                    "bili-vid-1",
                )
            }

            overview = await svc.store.usage_overview()
            by_cat = {c["category"]: c for c in overview["categories"]}
            assert set(by_cat) == {"minecraft", "patrol"}
            assert by_cat["minecraft"]["calls"] == len(mc_gen_rows) + len(mc_scr_rows)
            assert by_cat["patrol"]["calls"] == len(bili_vw_rows) + len(bili_vid_rows)
            assert sum(c["calls"] for c in overview["categories"]) == sum(
                row["calls"] for row in overview["lanes"]
            )
            assert sum(c["amount"] for c in overview["categories"]) == sum(
                row["amount"] for row in overview["lanes"]
            )
        finally:
            await mc_sock.close()
            await bili_sock.close()
            await asyncio.gather(mc_run, bili_run, return_exceptions=True)


async def test_plugin_bus_concurrent_ai_requests_contextvars_isolated(settings):
    """Overlapping plugin AI calls must never leak model_work contextvars across tasks."""
    s = _plugin_settings(settings)
    entered = asyncio.Event()
    release = asyncio.Event()
    observed: dict[str, tuple[str, str, str, str]] = {}
    inflight_seen = 0

    class _BarrierGateway(FakeGateway):
        async def generate(self, payload: dict[str, Any], subject: str, **kwargs: Any) -> str:
            nonlocal inflight_seen
            tag = str(payload["tag"])
            before = reservation_work("proactive", "fallback")
            inflight_seen += 1
            if inflight_seen >= 2:
                entered.set()
            await asyncio.wait_for(release.wait(), timeout=2.0)
            after = reservation_work("proactive", "fallback")
            assert before == after
            observed[tag] = (after.kind, after.key, after.root_kind, after.root_key)
            return f"ok:{tag}"

        async def screen(self, payload: dict[str, Any], subject: str, **kwargs: Any) -> dict:
            work = reservation_work("proactive", "fallback")
            observed[f"screen:{payload['tag']}"] = (
                work.kind,
                work.key,
                work.root_kind,
                work.root_key,
            )
            return {"ok": True}

    gw = _BarrierGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        bus = _get_bus(svc)
        svc.modules.unregister("minecraft")
        await bus.create_plugin(
            "minecraft",
            display_name="Minecraft",
            scopes=["thinking_scenes"],
            node_region="KR",
        )
        sock = _PluginSocket()
        run_task = asyncio.create_task(
            bus.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                plugin_id="minecraft",
            )
        )
        try:
            await sock.push(
                {
                    "type": "plugin.hello",
                    "protocol_version": 1,
                    "display_name": "Minecraft",
                    "scopes": ["thinking_scenes"],
                    "thinking_scenes": [
                        {"key": "mc.play", "label": "游玩"},
                        {"key": "mc.build", "label": "建造"},
                    ],
                }
            )
            await sock.recv_until(lambda m: m["type"] == "plugin.ready")

            with model_work("parent.unrelated", "outer-key"):
                await sock.push(
                    {
                        "type": "ai.generate",
                        "id": "req-a",
                        "scenario": "mc.play",
                        "budget_lane": "proactive",
                        "payload": {"tag": "A"},
                    }
                )
                await sock.push(
                    {
                        "type": "ai.generate",
                        "id": "req-b",
                        "scenario": "mc.build",
                        "budget_lane": "proactive",
                        "payload": {"tag": "B"},
                    }
                )
                await asyncio.wait_for(entered.wait(), timeout=2.0)
                await sock.push(
                    {
                        "type": "ai.screen",
                        "id": "req-c",
                        "budget_lane": "proactive",
                        "payload": {"tag": "C"},
                    }
                )
                release.set()

                target_ids = {"req-a", "req-b", "req-c"}
                seen_ids: set[str] = set()
                while len(seen_ids) < 3:
                    msgs = await sock.recv_until(lambda m: m.get("id") in target_ids)
                    for msg in msgs:
                        if msg.get("id") in target_ids:
                            assert msg["ok"] is True
                            seen_ids.add(msg["id"])

                # Outer context must remain completely untouched
                outer = reservation_work("chat", "fb")
                assert (outer.kind, outer.key) == ("parent.unrelated", "outer-key")

            assert observed["A"] == ("mc.play", "req-a", "mc.play", "req-a")
            assert observed["B"] == ("mc.build", "req-b", "mc.build", "req-b")
            assert observed["screen:C"] == (
                "plugin.minecraft.screen",
                "req-c",
                "plugin.minecraft.screen",
                "req-c",
            )
        finally:
            release.set()
            await sock.close()
            await asyncio.gather(run_task, return_exceptions=True)


async def test_screen_watch_and_voice_companion_model_work_attribution(settings, sdk_pair):
    now = [1_795_500_000.0]
    s = _companion_settings(_screen_settings(settings))
    app = create_app(
        s,
        gateway_factory=lambda cfg, st: _offline_gateway(cfg, st, sdk_pair),
        start_workers=False,
    )
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.screen_watch.clock = lambda: now[0]
        svc.voice_companion.clock = lambda: now[0]

        # 1. Standalone screen_watch commentary -> kind="screen.watch" (no orphan screen.output)
        sw_sock = _SocketDouble()
        sw_task = asyncio.create_task(
            svc.screen_watch.serve(
                sw_sock.receive,
                sw_sock.send,
                sw_sock.close,
                source_ip="127.0.0.1",
                adapter_id="win_cap",
                client_id="sw-test",
            )
        )
        try:
            await sw_sock.push({"type": "watch.start", "game_title": "原神"})
            await sw_sock.recv_until(lambda m: m["type"] == "watch.ready")
            now[0] += 30.0
            await sw_sock.push(
                {
                    "type": "frame.push",
                    "turn_id": "sw-turn-1",
                    "request_commentary": True,
                    "frames": [TINY_PNG_B64],
                }
            )
            sw_res = await sw_sock.recv_until(
                lambda m: m.get("type") == "commentary.result" and m.get("turn_id") == "sw-turn-1"
            )
            assert sw_res[-1]["status"] in {"overlay", "sent_qq", "silent"}
        finally:
            await sw_sock.close()
            await asyncio.gather(sw_task, return_exceptions=True)

        # 2. Voice companion speech (chat) + screen_pulse (proactive) -> kind="voice.companion"
        vc_sock = _SocketDouble()
        vc_task = asyncio.create_task(
            svc.voice_companion.serve(
                vc_sock.receive,
                vc_sock.send,
                vc_sock.close,
                source_ip="127.0.0.1",
                adapter_id="desktop",
                client_id="vc-test",
            )
        )
        try:
            await vc_sock.push(
                {"type": "session.start", "mode": "owner_private", "game_title": "原神"}
            )
            await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
            await vc_sock.push(
                {
                    "type": "speech.turn",
                    "turn_id": "vc-speech-1",
                    "text": "这波怎么样",
                }
            )
            ev_speech = await vc_sock.recv_until(
                lambda m: m.get("type") == "turn.end" and m.get("turn_id") == "vc-speech-1"
            )
            assert ev_speech[-1]["status"] == "spoken"

            outcome = await svc.voice_companion.trigger_screen_commentary(
                frames=[],
                game_title="原神",
                turn_id="vc-pulse-1",
            )
            assert outcome == "dispatched"
            ev_pulse = await vc_sock.recv_until(
                lambda m: m.get("type") == "turn.end" and m.get("turn_id") == "vc-pulse-1"
            )
            assert ev_pulse[-1]["status"] in {"spoken", "silent"}

            # 3. Room-public voice companion triggers output screening -> still voice.companion!
            await vc_sock.push(
                {"type": "session.start", "mode": "room_public", "game_title": "原神"}
            )
            await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
            await vc_sock.push(
                {
                    "type": "speech.turn",
                    "turn_id": "vc-pub-1",
                    "text": "大家看我操作",
                    "speaker_id": "friend-1",
                    "speaker_name": "群友",
                }
            )
            ev_pub = await vc_sock.recv_until(
                lambda m: m.get("type") == "turn.end" and m.get("turn_id") == "vc-pub-1"
            )
            assert ev_pub[-1]["status"] == "spoken"
        finally:
            await vc_sock.close()
            await asyncio.gather(vc_task, return_exceptions=True)

        rows = await svc.store.all(
            "SELECT l.lane, w.kind, w.work_id, w.root_kind, w.root_id "
            "FROM ledger l LEFT JOIN ledger_work w ON w.id=l.id"
        )
        kinds = [r["kind"] for r in rows]
        assert "proactive.screen.output" not in kinds
        assert "chat.screen.output" not in kinds

        sw_rows = [r for r in rows if r["kind"] == "screen.watch"]
        assert len(sw_rows) == 1
        assert all(
            r["root_kind"] == "screen.watch" and r["work_id"] == "sw-turn-1" for r in sw_rows
        )

        vc_rows = [r for r in rows if r["kind"] == "voice.companion"]
        assert {r["lane"] for r in vc_rows} == {"chat", "proactive"}
        # owner speech (1) + owner pulse (1) + room_public speech + output screen (2) = 4
        assert len(vc_rows) >= 4
        assert all(r["root_kind"] == "voice.companion" for r in vc_rows)

        overview = await svc.store.usage_overview()
        by_cat = {c["category"]: c for c in overview["categories"]}
        assert set(by_cat) == {"screen_watch", "voice_companion"}
        assert by_cat["screen_watch"]["calls"] == len(sw_rows)
        assert by_cat["voice_companion"]["calls"] == len(vc_rows)
        assert sum(c["calls"] for c in overview["categories"]) == sum(
            row["calls"] for row in overview["lanes"]
        )
        assert sum(c["amount"] for c in overview["categories"]) == sum(
            row["amount"] for row in overview["lanes"]
        )


def test_admin_js_usage_category_and_proactive_label_source():
    overview_js = (_ROOT / "admin/src/js/02-overview-ledger.js").read_text(encoding="utf-8")
    routing_js = (_ROOT / "admin/src/js/03-routing-reply.js").read_text(encoding="utf-8")

    assert "主动私信" not in overview_js
    assert "主动私信" not in routing_js
    assert "proactive:'世界引擎与主动生成'" in overview_js
    assert "'世界引擎与主动生成今日'" in overview_js
    assert "budget_exhausted_proactive_daily:'budget_exhausted · 主动生成日上限'" in routing_js
    assert "usage.categories" in overview_js

    for key, label in [
        ("chat", "主对话"),
        ("world_plan", "世界引擎规划"),
        ("minecraft", "Minecraft 游玩"),
        ("qq_proactive", "主动社交（群/私信）"),
        ("owner_digest", "Owner 晚间日报"),
        ("screen_watch", "屏幕伴看"),
        ("voice_companion", "语音伴随"),
        ("proactive_other", "其他主动生成"),
        ("patrol", "B 站巡逻"),
        ("patrol_diary", "巡逻日记"),
        ("memory", "记忆提炼"),
        ("memory_flex", "记忆 Flex"),
        ("admin", "管理测试"),
        ("search", "联网检索"),
    ]:
        assert f"{key}:'{label}'" in overview_js
