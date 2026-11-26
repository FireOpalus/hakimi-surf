import functools
import time
from typing import Any

import aiosqlite
import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

import hakimi.session_manager as hakimi_session_manager
import hakimi.store as hakimi_store
from hakimi.config import Settings
from hakimi.models import Message
from hakimi.store import Store

pytest_plugins = ["tests.admin_frontend"]


@functools.lru_cache(maxsize=1)
def cached_rsa_private_key() -> rsa.RSAPrivateKey:
    """Reuse one 2048-bit RSA keypair per worker process to avoid repeated slow keygen."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@functools.lru_cache(maxsize=1)
def cached_rsa_private_key_pem() -> str:
    return (
        cached_rsa_private_key()
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode("ascii")
    )


class _FastConnectAwaitable:
    def __init__(self, orig_connect: Any, *args: Any, **kwargs: Any) -> None:
        self._inner = orig_connect(*args, **kwargs)

    def __await__(self):
        async def _open() -> Any:
            conn = await self._inner
            orig_execute = conn.execute

            def _fast_execute(sql: str, *a: Any, **kw: Any) -> Any:
                if sql == "PRAGMA synchronous=FULL":
                    sql = "PRAGMA synchronous=OFF"
                elif sql == "PRAGMA secure_delete=ON":
                    sql = "PRAGMA secure_delete=OFF"
                    conn.__dict__.pop("execute", None)
                return orig_execute(sql, *a, **kw)

            conn.__dict__["execute"] = _fast_execute
            return conn

        return _open().__await__()

    async def __aenter__(self) -> Any:
        return await self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        return await self._inner.__aexit__(exc_type, exc, tb)


class _FastAiosqliteProxy:
    def __getattr__(self, name: str) -> Any:
        return getattr(aiosqlite, name)

    def connect(self, *args: Any, **kwargs: Any) -> _FastConnectAwaitable:
        return _FastConnectAwaitable(aiosqlite.connect, *args, **kwargs)


_FAST_AIOSQLITE_PROXY = _FastAiosqliteProxy()


@pytest.fixture(autouse=True)
def sqlite_test_fast_path(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable fsync/zero-fill on Store and Session DBs unless marked sqlite_durable."""
    if request.node.get_closest_marker("sqlite_durable") is not None:
        return
    monkeypatch.setattr(hakimi_store, "aiosqlite", _FastAiosqliteProxy())
    monkeypatch.setattr(hakimi_session_manager, "aiosqlite", _FastAiosqliteProxy())
    orig_open = Store.open

    async def _fast_open(self: Store) -> None:
        try:
            await orig_open(self)
        finally:
            db = getattr(self, "db", None)
            if db is not None:
                db.__dict__.pop("execute", None)

    monkeypatch.setattr(Store, "open", _fast_open)


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
