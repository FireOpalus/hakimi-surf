"""Mock Gemini's HTTP transport while exercising the real SDK, budget lanes and work ledger."""

import asyncio
import re

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.memory_compressor import MemoryCompressor
from hakimi.session_manager import SessionManager
from hakimi.work_usage import model_work, reservation_work
from tests.test_admin import admin_api as admin_api
from tests.test_dual_memory_v100 import facts, group, mirror_fixture, seed
from tests.test_rolling_memory import fill
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


def offline_gateway(settings, store, sdk_pair, *, extraction=False):
    def handle(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        response = interaction_response()
        if extraction:
            response["steps"][0]["content"][0]["text"] = facts().model_dump_json()
        return httpx.Response(200, json=response)

    return GeminiGateway(settings, store, *sdk_pair(handle))


async def billed(store):
    return await store.all(
        "SELECT lane,kind,work_id,root_kind,root_id FROM ledger JOIN ledger_work USING(id) "
        "ORDER BY ledger.rowid"
    )


@pytest.mark.parametrize("archive", [False, True])
async def test_background_compression_owns_its_paid_checks_and_generation(
    settings, store, message, sdk_pair, archive
):
    settings.prefix_history_enabled = archive
    sessions = SessionManager(settings, store)
    gateway = offline_gateway(settings, store, sdk_pair, extraction=archive)
    compressor = MemoryCompressor(settings, sessions, gateway, store)
    msg = group(message) if archive else message
    if archive:
        await seed(sessions, msg)
    else:
        await fill(sessions, msg)
    with model_work("qq.reply", "parent-chat") as chat:
        if archive:
            task = asyncio.create_task(compressor.extract(msg, force=True))
        else:
            compressor.schedule(msg)
            task = compressor.tasks[sessions.key(msg)]
        result = await task
        assert result["status"] == "compressed"
        assert reservation_work("chat", "") == chat
    rows = await billed(store)
    assert len(rows) >= 2
    kind = "memory.extract" if archive else "memory.compress"
    if archive:
        key = result["batch_id"]
    else:
        async with sessions.database(msg) as (db, _):
            async with db.execute("SELECT attempt FROM rolling_state") as cur:
                key = (await cur.fetchone())[0]
    assert {tuple(row) for row in rows} == {("memory", kind, key, kind, key)}
    assert re.fullmatch(r"(?:flex-[0-9a-f]{32}|[0-9a-f]{64})", key)


async def test_sandbox_requests_have_distinct_ids_shared_by_gate_and_preview(admin_api, sdk_pair):
    app, client, _ = admin_api
    svc = app.state.service
    svc.ai = offline_gateway(svc.s, svc.store, sdk_pair)
    payload = {"identity": "OWNER", "scene": "private", "text": "同一段测试消息"}
    keys = set()
    for _ in range(2):
        response = await client.post("/admin/api/sandbox", json=payload)
        assert response.is_success and response.json()["preview"]["text"]
        rows = [r for r in await billed(svc.store) if r["work_id"] not in keys]
        assert len(rows) >= 2
        key = rows[0]["work_id"]
        assert re.fullmatch(r"[0-9a-f]{32}", key)
        assert {tuple(r) for r in rows} == {("admin", "admin.sandbox", key, "admin.sandbox", key)}
        keys.add(key)
    assert len(keys) == 2


async def test_drive_mirror_batches_all_review_chunks_under_one_private_safe_id(
    settings, store, message, sdk_pair
):
    mirror, drive, _, msg, _ = await mirror_fixture(settings, store, message)
    mirror.svc.ai = offline_gateway(settings, store, sdk_pair)
    await mirror.sync(msg, push=True)
    drive.content += "\n" * 5100
    drive.version += 1
    await mirror.sync(msg)
    rows = await billed(store)
    assert len(rows) >= 2
    key = rows[0]["work_id"]
    assert re.fullmatch(r"[0-9a-f]{32}", key)
    assert {tuple(row) for row in rows} == {
        ("admin", "memory.mirror_review", key, "memory.mirror_review", key)
    }
