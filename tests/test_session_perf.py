"""Performance & safety invariants for SessionManager, current_epoch, and RuntimeState.register."""

from pathlib import Path

import aiosqlite
import pytest

from hakimi.outgoing import deliver_actions, deliver_proactive_jit
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.session_manager import SessionManager, session_epoch
from tests.fakes import FakeBili, FakeGateway
from tests.test_streaming_v0100 import ActionSender, action


class _SQLSpyConnection:
    """Wrap an aiosqlite connection to record SQL statements and executescript calls."""

    def __init__(self, inner, log: list[tuple[str, str]]):
        self._inner = inner
        self._log = log

    def execute(self, sql, parameters=None):
        self._log.append(("execute", str(sql)))
        if parameters is None:
            return self._inner.execute(sql)
        return self._inner.execute(sql, parameters)

    def executescript(self, sql):
        self._log.append(("executescript", str(sql)))
        return self._inner.executescript(sql)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _SQLSpyConnect:
    def __init__(self, orig_connect, log: list[tuple[str, str]], connect_calls: list[dict]):
        self._orig = orig_connect
        self._log = log
        self._connect_calls = connect_calls

    def __call__(self, database, *args, **kwargs):
        self._connect_calls.append({"database": str(database), "kwargs": dict(kwargs)})
        ctx = self._orig(database, *args, **kwargs)
        log = self._log

        class _Ctx:
            async def __aenter__(self):
                conn = await ctx.__aenter__()
                return _SQLSpyConnection(conn, log)

            async def __aexit__(self, exc_type, exc, tb):
                return await ctx.__aexit__(exc_type, exc, tb)

        return _Ctx()


async def test_initialized_session_skips_schema_migrations_lock_glob_and_registry_writes(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    sql_log: list[tuple[str, str]] = []
    connect_calls: list[dict] = []
    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        _SQLSpyConnect(aiosqlite.connect, sql_log, connect_calls),
    )

    glob_calls = 0
    orig_glob = Path.glob

    def spy_glob(self, pattern, *args, **kwargs):
        nonlocal glob_calls
        if self == sessions.root and pattern == "*/*.sqlite3":
            glob_calls += 1
        return orig_glob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", spy_glob)

    registry_writes = 0
    orig_store_tx = store.transaction

    def spy_tx():
        cm = orig_store_tx()

        class _Tx:
            async def __aenter__(self):
                nonlocal registry_writes
                res = await cm.__aenter__()
                orig_exec = store.db.execute

                def _exec(sql, *a, **kw):
                    nonlocal registry_writes
                    if "session_registry" in str(sql):
                        registry_writes += 1
                    return orig_exec(sql, *a, **kw)

                store.db.__dict__["execute"] = _exec
                return res

            async def __aexit__(self, exc_type, exc, tb):
                store.db.__dict__.pop("execute", None)
                return await cm.__aexit__(exc_type, exc, tb)

        return _Tx()

    monkeypatch.setattr(store, "transaction", spy_tx)

    # First open initializes file, schema, migrations, glob count, and session_registry once.
    assert await sessions.remember(message, "hello", "world")
    assert glob_calls == 1
    assert registry_writes == 1
    assert any(kind == "executescript" for kind, _ in sql_log)
    assert any("PRAGMA table_info(rolling_state)" in sql for _, sql in sql_log)

    # Subsequent opens on the initialized session must take the fast path.
    sql_log.clear()
    create_lock_entered = False
    orig_create_lock = sessions.create_lock

    class _FailLock:
        async def __aenter__(self):
            nonlocal create_lock_entered
            create_lock_entered = True
            return await orig_create_lock.__aenter__()

        async def __aexit__(self, exc_type, exc, tb):
            return await orig_create_lock.__aexit__(exc_type, exc, tb)

    sessions.create_lock = _FailLock()

    snap = await sessions.snapshot(message)
    assert len(snap["recent_messages"]) == 2
    assert not create_lock_entered
    assert glob_calls == 1
    assert registry_writes == 1

    # Fast path skipped schema, migrations, journal_mode, and redundant expiry sweeps.
    assert not any(kind == "executescript" for kind, _ in sql_log)
    assert not any("PRAGMA table_info" in sql for _, sql in sql_log)
    assert not any("PRAGMA journal_mode=WAL" in sql for _, sql in sql_log)
    assert not any("DELETE FROM recent_messages WHERE expires<=" in sql for _, sql in sql_log)
    assert not any("DELETE FROM history_folds WHERE expires<=" in sql for _, sql in sql_log)
    assert not any("DELETE FROM interaction_chains WHERE expires<=" in sql for _, sql in sql_log)
    # Durability PRAGMAs remain enforced on write-capable database() opens.
    assert any("PRAGMA synchronous=FULL" in sql for _, sql in sql_log)
    assert any("PRAGMA secure_delete=ON" in sql for _, sql in sql_log)
    assert any("PRAGMA busy_timeout=5000" in sql for _, sql in sql_log)


async def test_fast_path_preserves_identity_tamper_and_symlink_rejection(
    settings, store, message, tmp_path
):
    sessions = SessionManager(settings, store)
    await sessions.remember(message, "secret", "reply")
    path = sessions.path(message)
    key = sessions.key(message)
    assert path in sessions._initialized_paths

    # 1. Tampering session_meta.identity in-place is caught on BOTH database() and current_epoch().
    wrong_parts = ("other-ns", message.platform, message.bot_id, message.kind, "99999")
    async with aiosqlite.connect(path) as db:
        await db.execute(
            "UPDATE session_meta SET identity=?",
            (sessions.vault.seal(key, wrong_parts),),
        )
        await db.commit()
    with pytest.raises(Rejected, match="session_identity_mismatch"):
        await sessions.snapshot(message)
    with pytest.raises(Rejected, match="session_identity_mismatch"):
        await sessions.current_epoch(message)

    # Restore valid identity, then delete session_meta row in-place.
    async with aiosqlite.connect(path) as db:
        await db.execute("DELETE FROM session_meta")
        await db.commit()
    with pytest.raises(Rejected, match="session_identity_mismatch"):
        await sessions.snapshot(message)
    assert path not in sessions._initialized_paths

    # 2. Symlink replacement on file, parent dir, or root is always rejected on both paths.
    msg2 = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    await sessions.remember(msg2, "hi", "there")
    path2 = sessions.path(msg2)
    assert path2 in sessions._initialized_paths
    decoy = tmp_path / "decoy.sqlite3"
    decoy.write_bytes(path2.read_bytes())
    path2.unlink()
    path2.symlink_to(decoy)
    with pytest.raises(Rejected, match="invalid_session_path"):
        await sessions.snapshot(msg2)
    with pytest.raises(Rejected, match="invalid_session_path"):
        await sessions.current_epoch(msg2)

    # Parent symlink rejection
    path2.unlink()
    other_dir = tmp_path / "elsewhere_private"
    other_dir.mkdir()
    parent = path2.parent
    for child in list(parent.iterdir()):
        child.unlink()
    parent.rmdir()
    parent.symlink_to(other_dir, target_is_directory=True)
    with pytest.raises(Rejected, match="invalid_session_path"):
        await sessions.snapshot(msg2)
    with pytest.raises(Rejected, match="invalid_session_path"):
        await sessions.current_epoch(msg2)


async def test_session_capacity_cached_count_external_delete_and_prune_eviction(
    settings, store, message, monkeypatch
):
    settings.session_max_files = 2
    sessions = SessionManager(settings, store)

    glob_calls = 0
    orig_glob = Path.glob

    def spy_glob(self, pattern, *args, **kwargs):
        nonlocal glob_calls
        if self == sessions.root and pattern == "*/*.sqlite3":
            glob_calls += 1
        return orig_glob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", spy_glob)

    m1 = message.model_copy(update={"kind": "group", "conversation_id": "40001"})
    m2 = message.model_copy(update={"kind": "group", "conversation_id": "40002"})
    m3 = message.model_copy(update={"kind": "group", "conversation_id": "40003"})

    await sessions.remember(m1, "a", "b")
    assert glob_calls == 1
    assert sessions._file_count == 1

    # Second new file uses cached count without re-globbing.
    await sessions.remember(m2, "c", "d")
    assert glob_calls == 1
    assert sessions._file_count == 2

    # Third file hits capacity (re-globs once before rejecting).
    with pytest.raises(Rejected, match="session_capacity"):
        await sessions.remember(m3, "e", "f")
    assert glob_calls == 2

    # Externally deleting m1 allows m3 after recount.
    sessions.path(m1).unlink()
    await sessions.remember(m3, "e", "f")
    assert sessions._file_count == 2
    assert glob_calls == 3

    # Re-opening deleted m1 detects missing file, evicts cache, and enforces capacity.
    assert sessions.path(m1) in sessions._initialized_paths
    with pytest.raises(Rejected, match="session_capacity"):
        await sessions.snapshot(m1)
    assert sessions.path(m1) not in sessions._initialized_paths

    # Deleting m3 and running prune() evicts stale entries and resets file count.
    sessions.path(m3).unlink()
    await sessions.prune()
    assert sessions.path(m3) not in sessions._initialized_paths
    assert sessions._file_count is None
    assert (await sessions.snapshot(m1))["recent_messages"] == []
    assert sessions.path(m1) in sessions._initialized_paths


async def test_expiry_watermark_exact_ttl_trim_version_expiry_slice_and_time_monkeypatch(
    settings, store, message, monkeypatch
):
    now = [1_800_000_000.0]
    monkeypatch.setattr("hakimi.session_manager.time.time", lambda: now[0])
    monkeypatch.setattr("hakimi.dual_memory.time.time", lambda: now[0])

    settings.prefix_history_enabled = True
    settings.memory_ttl_seconds = 10
    settings.interaction_chain_ttl_seconds = 5
    settings.interaction_store_enabled = True
    sessions = SessionManager(settings, store)

    await sessions.remember(message, "短命消息", "短命回复")
    snap0 = await sessions.snapshot(message)
    assert len(snap0["recent_messages"]) == 2
    assert snap0["history_revision"] == 0
    assert await sessions.commit_chain(
        message,
        lane="hydrogen",
        interaction_id="inter-1",
        expected_epoch=snap0["epoch"],
        trim_version=snap0["history_revision"],
        system_digest="sys",
        covered_seq=snap0["recent_messages"][-1]["seq"],
    )

    # At T + 1s (before either expiry): fast path skips sweep and returns live chain + messages.
    now[0] += 1.0
    sql_log: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        _SQLSpyConnect(aiosqlite.connect, sql_log, []),
    )
    snap1 = await sessions.snapshot(message)
    assert len(snap1["recent_messages"]) == 2
    assert "hydrogen" in snap1["interaction_chains"]
    assert not any("DELETE FROM interaction_chains WHERE expires<=" in s for _, s in sql_log)
    assert not any("DELETE FROM recent_messages WHERE expires<=" in s for _, s in sql_log)

    # At T + 5.1s (chain expired, still well within 60s sweep interval): watermark forces sweep!
    now[0] += 4.1
    sql_log.clear()
    snap2 = await sessions.snapshot(message)
    assert snap2["interaction_chains"] == {}
    assert len(snap2["recent_messages"]) == 2
    assert snap2["history_revision"] == 0
    assert any("DELETE FROM interaction_chains WHERE expires<=" in s for _, s in sql_log)

    # At T + 10.1s (messages expired, still well within 60s interval): watermark forces sweep,
    # bumps trim_version (history_revision), and writes cold_history_slices(kind='expiry').
    now[0] += 5.0
    snap3 = await sessions.snapshot(message)
    assert snap3["recent_messages"] == []
    assert snap3["history_revision"] == 1
    async with sessions.database(message, preserve_history=True) as (db, _):
        async with db.execute("SELECT kind FROM cold_history_slices WHERE kind='expiry'") as cur:
            assert await cur.fetchone() == ("expiry",)

    # Direct write via preserve_history=True invalidates expiry watermark immediately.
    settings.memory_ttl_seconds = 3600
    await sessions.remember(message, "新消息", "新回复")
    async with sessions.database(message, preserve_history=True) as (db, _):
        await db.execute("UPDATE recent_messages SET expires=?", (now[0] - 1.0,))
    snap4 = await sessions.snapshot(message)
    assert snap4["recent_messages"] == []
    assert snap4["history_revision"] == 2

    # Rolling summary expiry under legacy mode triggers at exact summary_ttl_seconds.
    settings.prefix_history_enabled = False
    settings.summary_ttl_seconds = 8
    await sessions.edit_summary(message, "短期摘要", snap4["version"])
    snap_sum = await sessions.snapshot(message)
    assert snap_sum["rolling_summary"] == "短期摘要"
    now[0] += 8.1
    snap_after = await sessions.snapshot(message)
    assert snap_after["rolling_summary"] == ""
    assert snap_after["version"] == snap_sum["version"] + 1


async def test_current_epoch_fast_path_skips_writes_folds_and_reflects_clear_and_edit(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    # Seed enough messages that snapshot() would trigger _fold_history once threshold drops.
    await sessions.remember(message, "一" * 400, "二" * 400)
    await sessions.remember(message, "三" * 400, "四" * 400)
    settings.history_high_tokens = 20

    sql_log: list[tuple[str, str]] = []
    connect_calls: list[dict] = []
    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        _SQLSpyConnect(aiosqlite.connect, sql_log, connect_calls),
    )

    register_calls = 0
    orig_register = store.runtime.register

    async def spy_register(msg):
        nonlocal register_calls
        register_calls += 1
        return await orig_register(msg)

    monkeypatch.setattr(store.runtime, "register", spy_register)

    # Fast read-only current_epoch: NO BEGIN IMMEDIATE, NO DELETE, NO fold, NO register().
    assert await sessions.current_epoch(message) == 0
    assert register_calls == 0
    assert len(connect_calls) == 1
    assert connect_calls[0]["kwargs"].get("uri") is True
    assert "mode=ro" in connect_calls[0]["database"]
    assert not any("BEGIN IMMEDIATE" in sql for _, sql in sql_log)
    assert not any("DELETE" in sql for _, sql in sql_log)
    assert not any("UPDATE" in sql for _, sql in sql_log)
    assert not any("INSERT" in sql for _, sql in sql_log)
    assert not any(kind == "executescript" for kind, _ in sql_log)

    # Verify _fold_history was NOT triggered by current_epoch().
    async with sessions.database(message, preserve_history=True) as (db, _):
        async with db.execute("SELECT COUNT(*) FROM history_folds") as cur:
            assert (await cur.fetchone())[0] == 0

    # clear() and edit_summary() epoch increments are immediately reflected.
    await sessions.clear(message)
    assert await sessions.current_epoch(message) == 1
    snap = await sessions.snapshot(message)
    await sessions.edit_summary(message, "手工摘要", snap["version"])
    assert await sessions.current_epoch(message) == 2

    # Missing file falls back cleanly to database() and re-initializes.
    sessions.path(message).unlink()
    assert await sessions.current_epoch(message) == 0
    assert sessions.path(message) in sessions._initialized_paths


async def test_session_epoch_helper_and_epoch_only_callers_avoid_snapshot(
    settings, store, message, monkeypatch
):
    # 1. Helper falls back cleanly for stub sessions lacking current_epoch().
    stub_calls = 0

    class _StubSessions:
        async def snapshot(self, _msg):
            nonlocal stub_calls
            stub_calls += 1
            return {"epoch": 7}

    assert await session_epoch(_StubSessions(), message) == 7
    assert stub_calls == 1

    # 2. End-to-end turn + multi-bubble deliver_actions + legacy _deliver_reply + proactive JIT.
    class _ActionGateway(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return action(bubbles=["第一句", "第二句"])

    class _RealLikeActionSender(ActionSender):
        async def send_action_bubble(self, msg, segments, *, targets, before_send):
            if not await before_send():
                raise Rejected("reply_invalidated")
            self.sent.append(segments)
            return {"message_id": len(self.sent) + 100}

    ai = _ActionGateway()
    sender = _RealLikeActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    await svc.world_state.initialize()
    try:
        snapshot_calls = 0
        epoch_calls = 0
        orig_snapshot = svc.sessions.snapshot
        orig_epoch = svc.sessions.current_epoch

        async def spy_snapshot(msg):
            nonlocal snapshot_calls
            snapshot_calls += 1
            return await orig_snapshot(msg)

        async def spy_epoch(msg):
            nonlocal epoch_calls
            epoch_calls += 1
            return await orig_epoch(msg)

        monkeypatch.setattr(svc.sessions, "snapshot", spy_snapshot)
        monkeypatch.setattr(svc.sessions, "current_epoch", spy_epoch)

        # Action reply (2 bubbles) -> turn uses snapshot only for context (2x) and current_epoch
        # for guards; delivery uses ONLY current_epoch and ZERO snapshot calls.
        m1 = message.model_copy(update={"event_id": "ev-1", "text": "聊点啥"})
        assert await svc.accept(m1) == "accepted"
        await svc.process(await store.claim())
        assert snapshot_calls == 2
        assert epoch_calls >= 1

        snapshot_calls = 0
        epoch_calls = 0
        reply = await store.claim_reply()
        assert await deliver_actions(svc, reply)
        assert snapshot_calls == 0
        assert epoch_calls >= 2
        assert len(sender.sent) == 2

        # Legacy _deliver_reply path also uses ONLY current_epoch and ZERO snapshot calls.
        snapshot_calls = 0
        epoch_calls = 0
        m2 = message.model_copy(update={"event_id": "ev-2", "text": "旧通道"})
        await store.admit(m2)
        await store.enqueue_reply(
            "legacy-1",
            m2,
            "旧版回复",
            session_epoch=await orig_epoch(m2),
        )
        assert await svc.deliver_once()
        assert snapshot_calls == 0
        assert epoch_calls >= 2

        # Proactive JIT delivery uses ONLY current_epoch and cancels if clear() bumps epoch.
        snapshot_calls = 0
        epoch_calls = 0
        res = await deliver_proactive_jit(
            svc,
            m2,
            action(bubbles=["主动问候"]),
            reply_id="jit-1",
        )
        assert res["status"] == "sent"
        assert snapshot_calls == 0
        assert epoch_calls >= 2

        # Epoch bump mid-JIT via before_delivery is detected by current_epoch and cancels send.
        async def _reset_midway():
            await svc.sessions.clear(m2)
            return True

        res2 = await deliver_proactive_jit(
            svc,
            m2,
            action(bubbles=["第一条", "第二条"]),
            reply_id="jit-2",
            before_delivery=_reset_midway,
        )
        assert res2["status"] == "cancelled"
        assert res2["bubbles_sent"] == 0
    finally:
        await svc.stop()


async def test_runtime_register_deduplicates_writes_and_handles_reload_and_errors(
    settings, store, message, monkeypatch
):
    writes = 0
    orig_tx = store.transaction

    def spy_tx():
        cm = orig_tx()

        class _Tx:
            async def __aenter__(self):
                nonlocal writes
                writes += 1
                return await cm.__aenter__()

            async def __aexit__(self, exc_type, exc, tb):
                return await cm.__aexit__(exc_type, exc, tb)

        return _Tx()

    monkeypatch.setattr(store, "transaction", spy_tx)

    k1 = await store.runtime.register(message)
    for _ in range(5):
        assert await store.runtime.register(message) == k1
    assert writes == 1
    assert (await store.runtime.session(k1)).conversation_id == message.conversation_id

    # load_config() invalidates the in-memory set cleanly.
    await store.runtime.load_config()
    assert store.runtime._registered_sessions == set()
    writes_before = writes
    assert await store.runtime.register(message) == k1
    assert writes == writes_before + 1
    assert await store.runtime.register(message) == k1
    assert writes == writes_before + 1

    # Transaction failure must not cache the session key.
    other = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})

    def failing_tx():
        class _FailTx:
            async def __aenter__(self):
                raise RuntimeError("db unavailable")

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return _FailTx()

    monkeypatch.setattr(store, "transaction", failing_tx)
    with pytest.raises(RuntimeError, match="db unavailable"):
        await store.runtime.register(other)
    assert store.session_scope(other) not in store.runtime._registered_sessions

    monkeypatch.setattr(store, "transaction", spy_tx)
    writes_before_other = writes
    k2 = await store.runtime.register(other)
    assert k2 in store.runtime._registered_sessions
    assert await store.runtime.session(k2)
    assert writes == writes_before_other + 1
