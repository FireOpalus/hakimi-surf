import asyncio

import pytest

from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.mark.parametrize("phase", ["forward", "images", "session"])
@pytest.mark.parametrize("revocation", ["group", "private_user", "bot", "mute"])
async def test_revocation_during_preparation_prevents_further_model_calls(
    settings, store, message, monkeypatch, phase, revocation
):
    gateway = FakeGateway()
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    target = message.model_copy(
        update={
            "user_id": "30000",
            "kind": "group" if revocation == "group" else "private",
            "conversation_id": "40000" if revocation == "group" else "30000",
        }
    )
    entered, resume = asyncio.Event(), asyncio.Event()
    owner, method = {
        "forward": (svc.context_resolver, "resolve"),
        "images": (svc, "message_images"),
        "session": (svc.sessions, "snapshot"),
    }[phase]
    original = getattr(owner, method)

    async def preparation(*args, **kwargs):
        entered.set()
        await resume.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(owner, method, preparation)
    if phase == "forward":
        processing = asyncio.create_task(svc.accept(target))
    else:
        assert await svc.accept(target) == "accepted"
        job = await store.claim()
        processing = asyncio.create_task(svc.handle_message(job))
    try:
        async with asyncio.timeout(3):
            await entered.wait()
        # These are the same persistent hot-update paths called by the admin API.
        if revocation == "group":
            await store.runtime.change_admission("allowed_groups", "remove", "40000")
        elif revocation == "private_user":
            await store.runtime.change_admission("allowed_users", "remove", "30000")
        elif revocation == "bot":
            await store.runtime.change_admission("ignored_bot_ids", "add", "30000")
        else:
            await svc.moderation.set_muted("30000")
        resume.set()
        async with asyncio.timeout(3):
            await processing
        # Current attachments now enter the same inbound gate; preparation revocation
        # is caught before any paid call in both public and private conversations.
        assert gateway.calls == []
        assert not await store.all("SELECT * FROM ledger")
        assert not await store.all("SELECT * FROM outbox")
    finally:
        resume.set()
        if not processing.done():
            processing.cancel()
        await asyncio.gather(processing, return_exceptions=True)
        await svc.stop()
