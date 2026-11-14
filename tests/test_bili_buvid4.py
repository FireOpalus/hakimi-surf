"""Device-cookie completion uses only the standard endpoint, never a challenge bypass."""

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from hakimi.bili_watcher import SPI_PATH, USER_AGENT, BiliPolicy, BiliWatcher, CredentialUpdate
from hakimi.security import Rejected, configured_secrets
from hakimi.video_downloader import cookie_file
from tests.fakes import FakeGateway
from tests.test_bili_watcher import NAV


@pytest.fixture
async def device_watcher(settings, store, message):
    calls, naps, alerts = [], [], []

    async def alert(*args):
        alerts.append(args)

    async def sleep(seconds):
        naps.append(seconds)

    def handler(request):
        calls.append(request)
        if request.url.path == SPI_PATH:
            return httpx.Response(200, json={"code": 0, "data": {"b_4": "device-four-secret"}})
        assert request.url.path == "/x/web-interface/nav"
        return httpx.Response(200, json={"code": 0, "data": NAV})

    service = SimpleNamespace(
        s=settings,
        store=store,
        secrets=[],
        ai=FakeGateway(),
        proactive=SimpleNamespace(alert=alert, target=lambda: message),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        watcher = BiliWatcher(service, client, sleep=sleep)
        watcher.calls, watcher.naps, watcher.alerts = calls, naps, alerts
        await watcher.initialize()
        await watcher.update_credentials(
            CredentialUpdate(sessdata="session-secret", bili_jct="csrf-secret")
        )
        yield watcher


async def test_missing_optional_cookie_causes_no_startup_network(device_watcher):
    w = device_watcher
    assert not w.calls
    status = await w.status()
    assert not status["credentials"]["buvid4"]
    assert status["buvid4_autofill"]["status"] == "not_attempted"
    assert not w.svc.ai.calls


async def test_spi_uses_same_ua_and_encrypted_hot_cookie_then_survives_restart(device_watcher):
    w = device_watcher
    assert (await w.nav())["status"] == "valid"
    assert [str(call.url) for call in w.calls] == [
        "https://api.bilibili.com" + SPI_PATH,
        "https://api.bilibili.com/x/web-interface/nav",
    ]
    assert all(call.headers["User-Agent"] == USER_AGENT for call in w.calls)
    assert "buvid4=" not in w.calls[0].headers["Cookie"]
    assert "buvid4=device-four-secret" in w.calls[1].headers["Cookie"]
    assert 0 <= w.naps[0] <= 6
    saved = (await w.store.one("SELECT payload FROM bili_credentials"))[0]
    assert b"device-four-secret" not in saved
    assert "device-four-secret" in configured_secrets(w.s)
    assert "device-four-secret" not in json.dumps(await w.status())
    again = BiliWatcher(w.svc, w.http, sleep=w.sleep)
    await again.initialize()
    assert again.creds["buvid4"] == "device-four-secret"
    await again.nav()
    assert sum(call.url.path == SPI_PATH for call in w.calls) == 1


async def test_old_three_cookie_sqlite_row_migrates_without_network(device_watcher):
    w = device_watcher
    old = {key: value for key, value in w.creds.items() if key != "buvid4"}
    await w.store.db.execute(
        "UPDATE bili_credentials SET payload=?", (w.store.vault.seal("bili-credentials", old),)
    )
    again = BiliWatcher(w.svc, w.http, sleep=w.sleep)
    await again.initialize()
    assert again.creds == {**old, "buvid4": ""}
    assert not w.calls
    await again.nav()
    assert again.creds["buvid4"] == "device-four-secret"


async def test_concurrent_nav_and_download_only_request_device_once(device_watcher):
    w = device_watcher
    nav, creds = await asyncio.gather(w.nav(), w.download_credentials())
    assert nav["status"] == "valid" and creds["buvid4"] == "device-four-secret"
    assert sum(call.url.path == SPI_PATH for call in w.calls) == 1
    assert not w.svc.ai.calls


@pytest.mark.parametrize(
    "failure",
    [
        "timeout",
        "http500",
        "bad_json",
        "bad_cookie",
        "missing",
        "api_error",
        "malformed_code",
        "boolean_code",
    ],
)
async def test_optional_lookup_failure_falls_back_once_and_attempt_persists(
    device_watcher, failure
):
    w = device_watcher
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path != SPI_PATH:
            return httpx.Response(200, json={"code": 0, "data": NAV})
        if failure == "timeout":
            raise httpx.ReadTimeout("private upstream detail")
        if failure == "http500":
            return httpx.Response(500)
        if failure == "bad_json":
            return httpx.Response(200, json=[])
        if failure in {"malformed_code", "boolean_code"}:
            return httpx.Response(
                200,
                json={
                    "code": {} if failure == "malformed_code" else False,
                    "data": {"b_4": "should-not-be-accepted"},
                },
            )
        return httpx.Response(
            200,
            json={
                "code": -400 if failure == "api_error" else 0,
                "data": {"b_4": "secret;injected=x" if failure == "bad_cookie" else ""},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        w.http = client
        assert (await w.nav())["status"] == "valid"
        assert (await w.nav())["status"] == "valid"
        again = BiliWatcher(w.svc, client, sleep=w.sleep)
        await again.initialize()
        assert (await again.nav())["status"] == "valid"
        assert sum(call.url.path == SPI_PATH for call in calls) == 1
        assert not again.creds["buvid4"]
        assert (await again.status())["buvid4_autofill"]["status"] == "unavailable"
        assert not w.alerts and not w.svc.ai.calls
        await again.update_credentials(CredentialUpdate(bili_jct="explicit-new-csrf"))
        await again.nav()
        assert sum(call.url.path == SPI_PATH for call in calls) == 2


@pytest.mark.parametrize(
    "body",
    [
        {"code": -101},
        {"code": -352},
        {"code": -412},
        {"code": 0, "data": {"v_voucher": "challenge", "b_4": "do-not-use"}},
    ],
)
async def test_spi_risk_stops_before_nav_and_enters_existing_long_cooldown(device_watcher, body):
    w, calls = device_watcher, []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        w.http = client
        with pytest.raises(Rejected, match="bili_cookie_expired|bili_account_risk"):
            await w.nav()
        assert len(calls) == 1 and calls[0].url.path == SPI_PATH
        assert w.blocked_until > time.time() + 43000
        assert not w.creds["buvid4"] and len(w.alerts) == 1
        with pytest.raises(Rejected, match="cooling_down"):
            await w.nav()
        assert len(calls) == 1 and not w.svc.ai.calls


async def test_http_412_is_not_treated_as_optional_cookie_failure(device_watcher):
    w, calls = device_watcher, []

    def handler(request):
        calls.append(request)
        return httpx.Response(412)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        w.http = client
        with pytest.raises(Rejected, match="bili_account_risk"):
            await w.nav()
        assert len(calls) == 1 and w.blocked_until > time.time() + 43000
        assert len(w.alerts) == 1 and not w.svc.ai.calls


async def test_existing_buvid4_or_missing_login_parts_skip_spi(device_watcher):
    w = device_watcher
    await w.update_credentials(CredentialUpdate(buvid4="explicit-device"))
    await w.nav()
    assert len(w.calls) == 1 and w.calls[0].url.path != SPI_PATH
    await w.update_credentials(CredentialUpdate(clear=True))
    await w.update_credentials(CredentialUpdate(sessdata="session-only"))
    await w.nav()
    assert all(call.url.path != SPI_PATH for call in w.calls)
    await w.update_credentials(CredentialUpdate(clear=True))
    with pytest.raises(Rejected, match="credentials_missing"):
        await w.nav()


async def test_queued_credential_update_cannot_be_overwritten_by_old_spi(device_watcher):
    w = device_watcher
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        if request.url.path == SPI_PATH:
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"code": 0, "data": {"b_4": "old-device"}})
        return httpx.Response(200, json={"code": 0, "data": NAV})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        w.http = client
        first = asyncio.create_task(w.download_credentials())
        await entered.wait()
        update = asyncio.create_task(
            w.update_credentials(
                CredentialUpdate(
                    sessdata="new-session",
                    buvid4="new-device",
                )
            )
        )
        await asyncio.sleep(0)
        assert not update.done()
        release.set()
        await first
        await update
        assert w.creds["sessdata"] == "new-session" and w.creds["buvid4"] == "new-device"
        row = await w.store.one("SELECT payload FROM bili_credentials")
        assert w.store.vault.open("bili-credentials", row[0])["buvid4"] == "new-device"


async def test_patrol_revoked_during_spi_does_not_persist_or_continue(device_watcher):
    w = device_watcher
    await w.set_policy(BiliPolicy(enabled=True))
    entered, release, calls = asyncio.Event(), asyncio.Event(), []

    async def handler(request):
        calls.append(request)
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"code": 0, "data": {"b_4": "stale-device"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        w.http = client
        token = w.patrol_context.set((w.policy_epoch, w.credentials_epoch))
        try:
            task = asyncio.create_task(w.nav())
        finally:
            w.patrol_context.reset(token)
        await entered.wait()
        await w.set_policy(BiliPolicy(enabled=False))
        release.set()
        with pytest.raises(Rejected, match="bili_settings_changed"):
            await task
        assert len(calls) == 1 and not w.creds["buvid4"]


async def test_download_guard_stops_lookup_before_any_http(device_watcher):
    with pytest.raises(Rejected, match="bili_request_revoked"):
        await device_watcher.download_credentials(guard=lambda: False)
    assert not device_watcher.calls


def test_download_cookie_jar_contains_scoped_buvid4():
    jar = cookie_file({"buvid4": "device-value"})
    assert ".bilibili.com\tTRUE\t/\tTRUE\t0\tbuvid4\tdevice-value" in jar
    with pytest.raises(Rejected, match="credentials_invalid"):
        cookie_file({"buvid4": "bad\nother-cookie"})
