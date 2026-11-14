"""Video job deadlines stay separate from ordinary chat replay windows."""

import pytest

from hakimi.video_policy import VideoWatchConfig
from tests.test_service import run_message
from tests.test_service import service as service
from tests.test_service import videos as videos


@pytest.mark.parametrize("video_request", [False, True])
async def test_only_validated_video_jobs_get_a_bounded_longer_reply_deadline(
    service, message, videos, video_request
):
    await service.video_watch.set_config(VideoWatchConfig(enabled=True, daily_limit=0))
    current = message.model_copy(update={"text": videos[0].url if video_request else "你好"})
    job = await run_message(service, current)
    persisted = await service.store.one("SELECT expires FROM jobs WHERE id=?", (job["id"],))
    reply = await service.store.one("SELECT expires FROM outbox WHERE id=?", (job["id"],))
    seconds = 900 if video_request else service.s.event_ttl_seconds
    assert persisted["expires"] == pytest.approx(message.created_at + seconds)
    assert reply["expires"] == pytest.approx(message.created_at + seconds)
    assert not await service.store.all("SELECT * FROM video_watch_attempts")
