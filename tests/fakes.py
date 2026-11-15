import copy
import inspect

from hakimi.models import Message
from hakimi.security import Rejected


def recorded_kwargs(kwargs):
    return {
        k: v.model_dump() if isinstance(v, Message) else copy.deepcopy(v)
        for k, v in kwargs.items()
        if k not in {"before_call", "cancel_event", "audit_id", "audit_job_id"}
    }


async def call_guard(kwargs):
    guard = kwargs.get("before_call")
    if guard:
        result = guard()
        if inspect.isawaitable(result):
            result = await result
        if result is False:
            raise Rejected("video_watch_invalidated")


class FakeGateway:
    def __init__(self, *args):
        self.calls = []
        self.block_purpose = None
        self.intent = "chat"

    async def screen(self, payload, subject, **kwargs):
        await call_guard(kwargs)
        self.calls.append(("screen", copy.deepcopy(payload), subject, recorded_kwargs(kwargs)))
        if kwargs.get("purpose") == self.block_purpose:
            raise Rejected("content_blocked")
        return self.intent

    async def generate(self, payload, subject, **kwargs):
        await call_guard(kwargs)
        self.calls.append(("generate", copy.deepcopy(payload), subject, recorded_kwargs(kwargs)))
        return "这是基于已有信息的简短回复。"

    async def close(self):
        pass


class FakeSender:
    def __init__(self):
        self.sent = []
        self.fail = False

    async def send(self, message, text, proactive=False, *, before_send=None):
        if before_send is not None and not await before_send():
            raise Rejected("reply_invalidated")
        self.sent.append((message, text, proactive))
        if self.fail:
            raise TimeoutError
        return {"message_id": len(self.sent)}


class FakeBili:
    def __init__(self, videos=None):
        self.videos = videos or []
        self.calls = []

    async def video(self, bvid):
        self.calls.append(bvid)
        return self.videos[0]

    async def updates(self, mid):
        self.calls.append(mid)
        return self.videos

    async def material(self, video, guard=None):
        if guard:
            await guard()
        return {
            "bvid": video.bvid,
            "title": video.title,
            "description": video.description,
            "duration": None,
            "subtitle_excerpt": "",
            "subtitle_complete": False,
            "subtitle_status": "missing",
            "hot_comments_unverified": [],
            "material_scope": "metadata_only",
        }
