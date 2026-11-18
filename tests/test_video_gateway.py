import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from google import genai

from hakimi.gemini import GeminiGateway
from hakimi.security import Rejected
from hakimi.video_files import ModelVideo, VideoFiles, video_tokens
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


class Files:
    def __init__(self):
        self.calls = []
        self.states = ["PROCESSING", "ACTIVE"]
        self.delete_error = None
        self.upload_error = None
        self.bad_uri = False
        self.entered = asyncio.Event()
        self.hold_upload = False

    def result(self, name):
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return SimpleNamespace(
            name=name,
            state=SimpleNamespace(name=state),
            uri="https://generativelanguage.googleapis.com/v1beta/" + name
            if not self.bad_uri
            else "http://127.0.0.1/private",
            mime_type="video/mp4",
        )

    async def upload(self, *, file, config):
        self.calls.append(("upload", config))
        self.entered.set()
        if self.hold_upload:
            await asyncio.Future()
        if self.upload_error:
            raise self.upload_error
        return self.result(config["name"])

    async def get(self, *, name, config):
        self.calls.append(("get", {"name": name}))
        return self.result(name)

    async def delete(self, *, name, config):
        self.calls.append(("delete", {"name": name}))
        if self.delete_error:
            raise self.delete_error


@pytest.fixture
def local_video(tmp_path):
    path = tmp_path / "download.mp4"
    path.write_bytes(b"mock-mp4-content")
    return path


@pytest.fixture
def files():
    return Files()


def manager(store, files):
    value = VideoFiles(store, lambda: files)

    async def no_delay(_):
        await asyncio.sleep(0)

    value.sleep = no_delay
    return value


async def test_video_sdk_versions_wire_safety_gate_and_video_usage(
    settings, store, sdk_pair, files, local_video, message
):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        response = interaction_response()
        response["usage"].update(total_input_tokens=6000, total_tokens=6050)
        return httpx.Response(200, json=response)

    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(handler),
        files_client=SimpleNamespace(aio=SimpleNamespace(files=files)),
    )
    gateway.video_files = manager(store, files)
    text = await gateway.watch_video(
        local_video,
        {"current_message": "画面里的猫在做什么？", "title": "猫猫"},
        "owner",
        duration=60,
        message=message,
    )
    assert text == "你好喵。"
    assert [r.url.path for r in calls] == [
        "/v1beta/decisions",
        "/v1/interactions",
        "/v1beta/decisions",
    ]
    for request in calls[:2]:
        blocks = json.loads(request.content)["input"]
        if blocks[0]["type"] == "user_input":
            blocks = blocks[0]["content"]
        assert [b["type"] for b in blocks] == ["text", "video"]
        assert blocks[1]["mime_type"] == "video/mp4"
        assert blocks[1]["resolution"] == "low"
        assert blocks[1]["processing"] == {"type": "static", "fps": 1}
        assert "data" not in blocks[1]
    body = json.loads(calls[1].content)
    assert body["store"] is False and body["model"] == "gemini-4-hydrogen"
    assert body["generation_config"]["thinking_level"] == "medium"
    assert body.get("tools", []) == []
    assert calls[0].extensions["timeout"]["read"] == 60
    assert calls[1].extensions["timeout"]["read"] == 120
    assert [c[0] for c in files.calls] == ["upload", "get", "delete"]
    assert files.calls[0][1]["http_options"]["timeout"] == 120000
    assert files.calls[0][1]["name"] == files.calls[-1][1]["name"]
    assert await store.all("SELECT * FROM gemini_video_files") == []
    rows = await store.all("SELECT amount,status FROM ledger")
    assert sum(r["amount"] for r in rows) == 9395
    assert all(r["status"] == "settled" for r in rows)


async def test_injected_video_never_reaches_interactions_but_file_is_deleted(
    settings, store, sdk_pair, files, local_video
):
    calls = []

    def handler(request):
        calls.append(request)
        value = decision_response()
        value["answers"]["is_prompt_injection"]["noul"] = 0.99
        return httpx.Response(200, json=value)

    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(handler),
        files_client=SimpleNamespace(aio=SimpleNamespace(files=files)),
    )
    gateway.video_files = manager(store, files)
    with pytest.raises(Rejected, match="prompt_injection"):
        await gateway.watch_video(local_video, {}, "subject", duration=10)
    assert len(calls) == 1 and calls[0].url.path.endswith("decisions")
    assert files.calls[-1][0] == "delete"


@pytest.mark.parametrize("failure", ["failed", "timeout", "upload", "uri"])
async def test_lifecycle_failures_clean_owned_remote_file(store, files, local_video, failure):
    lifecycle = manager(store, files)
    if failure == "failed":
        files.states = ["FAILED"]
    elif failure == "timeout":
        files.states = ["PROCESSING"]
        lifecycle.processing_timeout = 0.005
    elif failure == "upload":
        files.upload_error = RuntimeError("private upstream exception")
    else:
        files.bad_uri = True
    with pytest.raises(Rejected):
        async with lifecycle.uploaded(local_video, 30):
            raise AssertionError("invalid file reached caller")
    assert files.calls[-1][0] == "delete"
    assert await store.all("SELECT * FROM gemini_video_files") == []


async def test_upload_cancellation_uses_predetermined_name_and_deletes(store, files, local_video):
    lifecycle = manager(store, files)
    files.hold_upload = True

    async def upload():
        async with lifecycle.uploaded(local_video, 30):
            raise AssertionError("must never become ready")

    task = asyncio.create_task(upload())
    await files.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert files.calls[0][1]["name"] == files.calls[-1][1]["name"]
    assert files.calls[-1][0] == "delete"
    assert not lifecycle.active
    assert await store.all("SELECT * FROM gemini_video_files") == []


async def test_delete_failure_persists_only_encrypted_owned_name_for_restart(
    store, files, local_video
):
    lifecycle = manager(store, files)
    files.delete_error = TimeoutError()
    async with lifecycle.uploaded(local_video, 30) as video:
        name = video.name
        await lifecycle.cleanup()
        assert not any(c[0] == "delete" for c in files.calls)
    rows = await store.all("SELECT * FROM gemini_video_files")
    assert len(rows) == 1 and rows[0]["attempts"] == 1
    assert name.encode() not in rows[0]["payload"]
    files.delete_error = None
    async with store.transaction():
        await store.db.execute("UPDATE gemini_video_files SET retry_after=0")
    restored = manager(store, files)
    await restored.cleanup()
    assert files.calls[-1] == ("delete", {"name": name})
    assert await store.all("SELECT * FROM gemini_video_files") == []


async def test_file_deleted_after_model_exception_and_guard_revocation(
    settings, store, sdk_pair, files, local_video
):
    allowed = [True]
    calls = []

    def handler(request):
        calls.append(request)
        allowed[0] = False
        return httpx.Response(200, json=decision_response())

    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(handler),
        files_client=SimpleNamespace(aio=SimpleNamespace(files=files)),
    )
    gateway.video_files = manager(store, files)
    with pytest.raises(Rejected, match="video_watch_invalidated"):
        await gateway.watch_video(
            local_video, {}, "subject", duration=30, before_call=lambda: allowed[0]
        )
    assert len(calls) == 1
    assert files.calls[-1][0] == "delete"


async def test_budget_stops_upload_and_video_call_reservation_is_not_uri_sized(
    settings, store, sdk_pair, files, local_video
):
    settings.daily_usd = Decimal("0.000001")
    calls = []
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(lambda request: calls.append(request)),
        files_client=SimpleNamespace(aio=SimpleNamespace(files=files)),
    )
    gateway.video_files = manager(store, files)
    with pytest.raises(Rejected, match="budget_exhausted"):
        await gateway.watch_video(local_video, {}, "subject", duration=30)
    assert calls == []
    assert files.calls[-1][0] == "delete"
    async with store.transaction():
        await store.db.execute("INSERT OR REPLACE INTO meta VALUES ('budget_overrun','1')")
    files.calls.clear()
    with pytest.raises(Rejected, match="budget_hard_stop"):
        await gateway.watch_video(local_video, {}, "subject", duration=30)
    assert files.calls == []


@pytest.mark.parametrize("duration", [0, -1, float("nan"), float("inf"), True, 901])
def test_video_duration_reservation_is_bounded(duration):
    with pytest.raises(Rejected):
        video_tokens(duration)


def test_video_reference_cannot_be_external_or_unrelated_file():
    name = "files/hv-" + "a" * 32
    assert video_tokens(900) > video_tokens(480) > video_tokens(60) > 60 * (66 + 32)
    for uri in [
        "https://evil.example/v1beta/" + name,
        "https://generativelanguage.googleapis.com/v1beta/files/foreign",
        "https://generativelanguage.googleapis.com/v1beta/" + name + "?key=secret",
        "https://generativelanguage.googleapis.com:invalid/v1beta/" + name,
    ]:
        with pytest.raises(Rejected):
            ModelVideo(name, uri, 60).block()


async def test_real_files_sdk_async_upload_uses_beta_and_overrides_short_timeout(
    store, local_video
):
    calls = []
    name = [None]

    def handler(request):
        calls.append(request)
        if request.url.path == "/upload/v1beta/files":
            name[0] = json.loads(request.content)["file"]["name"]
            return httpx.Response(
                200,
                headers={
                    "x-goog-upload-url": "https://generativelanguage.googleapis.com/upload/testing"
                },
                json={},
            )
        if request.url.path == "/upload/testing":
            return httpx.Response(
                200,
                headers={"x-goog-upload-status": "final"},
                json={
                    "file": {
                        "name": name[0],
                        "mimeType": "video/mp4",
                        "state": "ACTIVE",
                        "uri": "https://generativelanguage.googleapis.com/v1beta/" + name[0],
                    }
                },
            )
        assert request.method == "DELETE"
        return httpx.Response(200, json={})

    transport = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=8)
    client = genai.Client(
        api_key="test-only",
        http_options={
            "api_version": "v1beta",
            "timeout": 8000,
            "httpx_async_client": transport,
            "retry_options": {"attempts": 1},
        },
    )
    try:
        lifecycle = VideoFiles(store, lambda: client.aio.files)
        async with lifecycle.uploaded(local_video, 60) as video:
            assert video.name == name[0]
        assert [r.method for r in calls] == ["POST", "POST", "DELETE"]
        assert calls[0].url.path == "/upload/v1beta/files"
        assert calls[0].extensions["timeout"]["read"] == 120
        assert calls[1].extensions["timeout"]["read"] == 120
        assert calls[-1].extensions["timeout"]["read"] == 5
    finally:
        await client.aio.aclose()
        client.close()
        await transport.aclose()
