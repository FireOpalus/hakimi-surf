import time

import pytest
from cryptography.fernet import Fernet

from hakimi.config import Settings
from hakimi.models import Message
from hakimi.store import Store

pytest_plugins = ["tests.admin_frontend"]


@pytest.fixture
def settings(tmp_path):
    return Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        gemini_api_key="test-gemini-key-never-live",
        encryption_key=Fernet.generate_key().decode(),
        onebot_enabled=True,
        onebot_self_id="10000",
        onebot_secret="test-onebot-signing-secret",
        onebot_access_token="test-onebot-access-token",
        onebot_owner_ids={"20000"},
        owner_qq_id="20000",
        onebot_private_mode="owner_and_allowlist",
        onebot_allowed_users={"30000"},
        onebot_allowed_groups={"40000", "50000"},
        qq_enabled=True,
        qq_app_id="99999",
        qq_app_secret="test-qq-application-secret",
        qq_owner_ids={"user-openid"},
        qq_allowed_groups={"group-openid"},
        user_requests_per_minute=100,
        global_requests_per_minute=1000,
        group_debounce_seconds=0,
        group_debounce_max_seconds=0,
        send_delay_min=0,
        send_delay_max=0,
        rest_hours_enabled=False,
        affection_enabled=False,
    )


@pytest.fixture
async def store(settings):
    instance = Store(settings)
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


@pytest.fixture
def message():
    return Message(
        platform="onebot",
        bot_id="10000",
        event_id="event-1",
        user_id="20000",
        kind="private",
        conversation_id="20000",
        text="你好，哈基米",
        addressed=True,
        created_at=time.time(),
    )


@pytest.fixture(autouse=True)
def forbid_external_test_connections(monkeypatch):
    """All provider tests are mocks. Real sockets are limited to loopback test servers."""
    import ipaddress
    import socket

    for name in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        monkeypatch.delenv(name, raising=False)

    original = socket.socket.connect

    def connect(sock, address):
        if isinstance(address, tuple):
            try:
                permitted = ipaddress.ip_address(address[0]).is_loopback
            except ValueError:
                permitted = address[0] == "localhost"
            if not permitted:
                raise AssertionError("External network forbidden in offline tests")
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", connect)
