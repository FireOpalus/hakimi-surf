"""Tests for normal-reply reconnect wait, audit detail, and disconnect diagnostics."""

import asyncio
import base64
import logging
import time

import httpx
import pytest

from hakimi.models import Message
from hakimi.modules.adapters import Sender
from hakimi.modules.onebot_transport import WebSocketTransport
from hakimi.outgoing import deliver_actions
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_crossborder_transport import Peer, response, ws_settings

TINY_PNG_B64 = "base64://" + base64.b64encode(
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
    b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
).decode("ascii")


def _private_owner_msg(settings, *, event_id="ev-1", text="你好"):
    return Message(
        platform="onebot",
        bot_id=settings.onebot_self_id,
        event_id=event_id,
        user_id=settings.owner_qq_id,
        kind="private",
        conversation_id=settings.owner_qq_id,
        text=text,
        addressed=True,
        created_at=time.time(),
    )


async def test_sender_normal_reply_and_meme_wait_for_reconnect_and_send_once(
    settings, message, monkeypatch
):
    wire = WebSocketTransport(ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0))
    sender_settings = ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0)
    monkeypatch.setattr(Sender, "REPLY_RECONNECT_WAIT_SECONDS", 1.0)
    async with httpx.AsyncClient() as http:
        sender = Sender(sender_settings, http, wire)
        peer = Peer()

        async def _login_later():
            await asyncio.sleep(0.05)
            await peer.login(wire)
            for expected_id in (101, 102):
                req = await asyncio.wait_for(peer.outgoing.get(), 1)
                if req["action"] == "get_status":
                    await peer.incoming.put(response(req, {"online": True, "good": True}))
                    req = await asyncio.wait_for(peer.outgoing.get(), 1)
                assert req["action"] == "send_private_msg"
                await peer.incoming.put(response(req, {"message_id": expected_id}))

        driver = asyncio.create_task(_login_later())
        try:

            async def targets():
                return set()

            async def valid():
                return True

            r1 = await sender.send_action_bubble(
                message,
                [{"type": "text", "data": {"text": "嗨，我在呢"}}],
                targets=targets,
                before_send=valid,
            )
            assert r1 == {"message_id": 101}

            r2 = await sender.send_meme(message, TINY_PNG_B64, before_send=valid)
            assert r2 == {"message_id": 102}
            await asyncio.wait_for(driver, 1)
            assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 2
        finally:
            await wire.close()


async def test_sender_revoked_during_reconnect_wait_raises_reply_invalidated(
    settings, message, monkeypatch
):
    wire = WebSocketTransport(ws_settings(settings))
    monkeypatch.setattr(Sender, "REPLY_RECONNECT_WAIT_SECONDS", 1.0)
    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, wire)
        allowed = True
        calls = 0

        async def valid():
            nonlocal calls
            calls += 1
            return allowed

        async def targets():
            return set()

        send_task = asyncio.create_task(
            sender.send_action_bubble(
                message,
                [{"type": "text", "data": {"text": "不要发出"}}],
                targets=targets,
                before_send=valid,
            )
        )
        await asyncio.sleep(0.02)
        allowed = False
        peer = Peer()
        await peer.login(wire)
        try:
            with pytest.raises(Rejected, match="^reply_invalidated$"):
                await asyncio.wait_for(send_task, 1)
            assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 0
            assert calls >= 2
        finally:
            await wire.close()


async def test_dispatched_failure_never_retries_and_records_onebot_delivery_uncertain(
    settings, store, monkeypatch
):
    wire = WebSocketTransport(ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0))
    monkeypatch.setattr(Sender, "REPLY_RECONNECT_WAIT_SECONDS", 1.0)
    peer = Peer()
    await peer.login(wire)
    peer.send_error = True
    s = ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0)
    async with httpx.AsyncClient() as http:
        sender = Sender(s, http, wire)
        svc = Service(s, store, FakeGateway(), FakeBili(), sender)
        msg = _private_owner_msg(s, event_id="ev-uncertain")
        audit_id = await store.runtime.filters.record(
            msg,
            {"current_message": msg.text},
            {"result": "chat", "reason": "ok", "answers": {}},
            "production",
            audit_id="audit-uncertain-1",
        )
        await store.enqueue_reply(
            "job-uncertain-1",
            msg,
            "要发送的消息",
            session_epoch=0,
            audit_id=audit_id,
            actions={
                "abstain": False,
                "bubbles": ["要发送的消息"],
                "anchor": {"reply_id": None, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "folded": None,
                "watermark": 0,
                "public": False,
            },
        )
        try:
            assert await deliver_actions(svc, await store.claim_reply())
            assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 1
            assert svc.counters["delivery_uncertain"] == 1
            audit = (await store.runtime.filters.recent_audit())[0]
            assert audit["execution_status"] == "uncertain"
            assert audit["execution_detail"] == "onebot_delivery_uncertain"
            row = await store.one("SELECT status FROM outbox WHERE id='job-uncertain-1'")
            assert row[0] == "uncertain"
        finally:
            await svc.stop()
            await wire.close()


async def test_pre_dispatch_vs_partial_bubble_audit_status_and_detail(settings, store, monkeypatch):
    wire = WebSocketTransport(ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0))
    monkeypatch.setattr(Sender, "REPLY_RECONNECT_WAIT_SECONDS", 0.03)
    s = ws_settings(settings, send_delay_min=0.0, send_delay_max=0.0)
    async with httpx.AsyncClient() as http:
        sender = Sender(s, http, wire)
        svc = Service(s, store, FakeGateway(), FakeBili(), sender)
        try:
            # Case A: disconnected pre-dispatch -> failed + onebot_not_dispatched
            msg_a = _private_owner_msg(s, event_id="ev-pre")
            audit_a = await store.runtime.filters.record(
                msg_a,
                {"current_message": msg_a.text},
                {"result": "chat", "reason": "ok", "answers": {}},
                "production",
                audit_id="audit-pre-1",
            )
            await store.enqueue_reply(
                "job-pre-1",
                msg_a,
                "第一句",
                session_epoch=0,
                audit_id=audit_a,
                actions={
                    "abstain": False,
                    "bubbles": ["第一句"],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "folded": None,
                    "watermark": 0,
                    "public": False,
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            assert svc.counters["delivery_uncertain"] == 0
            recent = await store.runtime.filters.recent_audit()
            assert recent[0]["execution_status"] == "failed"
            assert recent[0]["execution_detail"] == "onebot_not_dispatched"
            assert (await store.one("SELECT status FROM outbox WHERE id='job-pre-1'"))[
                0
            ] == "failed"

            # Case B: bubble 0 confirmed, then disconnect before bubble 1 -> uncertain (not failed)
            peer = Peer()
            await peer.login(wire)

            async def _approve_first_then_drop():
                req = await asyncio.wait_for(peer.outgoing.get(), 1)
                assert req["action"] == "send_private_msg"
                await peer.incoming.put(response(req, {"message_id": 201}))
                await wire.close()

            driver = asyncio.create_task(_approve_first_then_drop())
            msg_b = _private_owner_msg(s, event_id="ev-partial")
            audit_b = await store.runtime.filters.record(
                msg_b,
                {"current_message": msg_b.text},
                {"result": "chat", "reason": "ok", "answers": {}},
                "production",
                audit_id="audit-partial-1",
            )
            await store.enqueue_reply(
                "job-partial-1",
                msg_b,
                "第一句\n第二句",
                session_epoch=0,
                audit_id=audit_b,
                actions={
                    "abstain": False,
                    "bubbles": ["第一句", "第二句"],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "folded": None,
                    "watermark": 0,
                    "public": False,
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            await asyncio.wait_for(driver, 1)
            assert svc.counters["delivery_uncertain"] == 1
            recent = await store.runtime.filters.recent_audit()
            assert recent[0]["execution_status"] == "uncertain"
            assert recent[0]["execution_detail"] == "onebot_not_dispatched"
            rcpt = await store.one(
                "SELECT applied FROM outbox_bubble_receipts "
                "WHERE reply_id='job-partial-1' AND position=0"
            )
            assert rcpt and rcpt[0] == 1
        finally:
            await svc.stop()
            await wire.close()


async def test_legacy_deliver_reply_records_pre_dispatch_failed_not_uncertain(settings, store):
    wire = WebSocketTransport(ws_settings(settings))
    s = ws_settings(settings)

    class InstantSender(Sender):
        REPLY_RECONNECT_WAIT_SECONDS = 0.0

    async with httpx.AsyncClient() as http:
        sender = InstantSender(s, http, wire)
        svc = Service(s, store, FakeGateway(), FakeBili(), sender)
        msg = _private_owner_msg(s, event_id="ev-legacy")
        audit_id = await store.runtime.filters.record(
            msg,
            {"current_message": msg.text},
            {"result": "chat", "reason": "ok", "answers": {}},
            "production",
            audit_id="audit-legacy-1",
        )
        await store.enqueue_reply(
            "job-legacy-1",
            msg,
            "旧路径消息",
            session_epoch=0,
            audit_id=audit_id,
            actions={"farewell_bypass_mute": True},
        )
        try:
            assert await svc.deliver_once()
            assert svc.counters["delivery_uncertain"] == 0
            recent = await store.runtime.filters.recent_audit()
            assert recent[0]["execution_status"] == "failed"
            assert recent[0]["execution_detail"] == "onebot_disconnected"
        finally:
            await svc.stop()
            await wire.close()


async def test_disconnect_diagnostic_log_preserves_audit_sink(settings, store, caplog):
    wire = WebSocketTransport(ws_settings(settings, ws_ping_interval=0.02))
    peer = Peer()
    hakimi_logger = logging.getLogger("hakimi")
    hakimi_logger.addHandler(store.runtime.sink)
    try:
        with caplog.at_level(logging.INFO, logger="hakimi"):
            await peer.login(wire)
            req = await asyncio.wait_for(peer.outgoing.get(), 1)
            assert req["action"] == "get_status"
            await peer.incoming.put(response(req, {"online": False, "good": False}))
            await asyncio.wait_for(peer.task, 1)
        assert "onebot_connection_ended" in list(store.runtime.sink.pending)
        assert any(rec.getMessage() == "event=onebot_connection_ended" for rec in caplog.records)
        assert any(
            rec.getMessage() == "onebot_connection_ended_detail reason=onebot_offline"
            for rec in caplog.records
        )
    finally:
        hakimi_logger.removeHandler(store.runtime.sink)
        await wire.close()
