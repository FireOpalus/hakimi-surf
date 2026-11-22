"""Targeted unit tests for Wave 2 Tier-2 resident system modules:
affection, memory_compressor, and meme_library.
"""

from __future__ import annotations

import asyncio
import io
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.affection import AffectionManager
from hakimi.app import create_app
from hakimi.memes import MemeLibrary
from hakimi.memory_compressor import MemoryCompressor
from hakimi.modules import ModuleRegistry
from hakimi.session_manager import SessionManager
from hakimi.system_modules import SystemResources, install_system_modules
from hakimi.system_modules.affection import Module as AffectionSystemModule
from hakimi.system_modules.identity import Module as IdentitySystemModule
from hakimi.system_modules.meme_library import Module as MemeLibrarySystemModule
from hakimi.system_modules.memory_compressor import Module as MemoryCompressorSystemModule
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN


def _sample_png(color: str = "purple", size: tuple[int, int] = (32, 24)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, format="PNG")
    return buf.getvalue()


async def test_tier2_system_modules_topology_and_admin_contract(settings):
    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN), "affection_enabled": True})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        names = list(svc.modules.systems.keys())

        for mod_id in ("affection", "memory_compressor", "meme_library"):
            assert mod_id in names
            assert svc.modules.get(mod_id) is None
            assert not hasattr(svc.modules.systems[mod_id], "enabled")
            assert mod_id not in svc.modules.catalog.owners.values()
            assert svc.modules.systems[mod_id].drive_usage is None

        assert AffectionSystemModule.requires == ("identity",)
        assert MemoryCompressorSystemModule.requires == ("memory_hub",)
        assert MemeLibrarySystemModule.requires == ()

        assert names.index("identity") < names.index("affection")
        assert names.index("memory_hub") < names.index("memory_compressor")

        aff_mod = svc.modules.systems["affection"]
        comp_mod = svc.modules.systems["memory_compressor"]
        meme_mod = svc.modules.systems["meme_library"]

        assert aff_mod.manager is svc.store.runtime.affection
        assert aff_mod.manager._identity is svc.modules.systems["identity"].identity
        assert aff_mod.manager.initialized is True
        assert aff_mod.manager._migrated_to_person is True

        assert comp_mod.memory_hub is svc.modules.systems["memory_hub"].hub
        assert isinstance(comp_mod.compressor, MemoryCompressor)
        assert comp_mod.workers() == (comp_mod.run_archive,)

        assert meme_mod.library is svc.store.runtime.memes
        assert meme_mod.library.initialized is True
        assert meme_mod.workers() == ()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert sys_mods["affection"]["panel"] == "community"
            assert sys_mods["memory_compressor"]["panel"] == "memory"
            assert sys_mods["meme_library"]["panel"] == "memes"

            assert sys_mods["affection"]["status"]["identity_bound"] is True
            assert sys_mods["affection"]["status"]["state"] == "ready"
            assert sys_mods["memory_compressor"]["status"]["state"] == "ready"
            assert sys_mods["meme_library"]["status"]["state"] == "ready"
            assert sys_mods["meme_library"]["status"]["storage_mode"] == "local"

            for mod_id in ("affection", "memory_compressor", "meme_library"):
                assert "enabled" not in sys_mods[mod_id]
                deny = await client.put(
                    f"/admin/api/life/modules/{mod_id}/enabled", json={"enabled": False}
                )
                assert deny.status_code == 404


async def test_affection_module_forward_bind_identity_and_runtime_edges(settings, store):
    settings.affection_enabled = True
    settings.owner_qq_id = "20000"
    await store.runtime.load_config()

    # 1. Identity module alone no longer performs reverse bind_identity glue.
    existing_aff = store.runtime.affection
    assert existing_aff._identity is None

    fake_svc = SimpleNamespace(s=settings, store=store, sessions=SessionManager(settings, store))
    res = SystemResources(settings, store, FakeGateway(), None, fake_svc)
    reg = ModuleRegistry()

    ident_mod = IdentitySystemModule(res, reg)
    reg.register_system(ident_mod)
    assert store.runtime.identity is ident_mod.identity
    assert existing_aff._identity is None

    # 2. Affection module reuses store.runtime.affection and forward-binds identity.
    aff_mod = AffectionSystemModule(res, reg)
    reg.register_system(aff_mod)
    assert aff_mod.manager is existing_aff
    assert aff_mod.manager._identity is ident_mod.identity
    assert aff_mod.workers() == ()

    # Initialize in dependency order: identity -> affection triggers person migration.
    assert aff_mod.manager._migrated_to_person is False
    await ident_mod.initialize()
    await aff_mod.initialize()
    assert aff_mod.manager.initialized is True
    assert aff_mod.manager._migrated_to_person is True

    ready_status = await aff_mod.status()
    assert ready_status["state"] == "ready"
    assert ready_status["enabled"] is True
    assert ready_status["identity_bound"] is True
    assert ready_status["blacklisted_count"] == 0
    assert "身份已绑定" in ready_status["detail"]

    # Blacklisting a non-owner reflects immediately in non-blocking status summary.
    await aff_mod.manager.set_user_score("30001", 0, platform="onebot", reason="test_block")
    blocked_status = await aff_mod.status()
    assert blocked_status["state"] == "ready"
    assert blocked_status["blacklisted_count"] == 1
    assert "已拉黑 1 人" in blocked_status["detail"]

    # Disabling affection updates status state cleanly.
    await aff_mod.manager.set_config(aff_mod.manager.config.model_copy(update={"enabled": False}))
    disabled_status = await aff_mod.status()
    assert disabled_status["state"] == "disabled"
    assert disabled_status["enabled"] is False
    assert "已关闭" in disabled_status["detail"]

    # 3. Runtime without pre-attached affection creates & mounts one; pre-init state is starting.
    bare_store = SimpleNamespace(s=settings, runtime=SimpleNamespace())
    bare_res = SystemResources(settings, bare_store, FakeGateway(), None, SimpleNamespace())
    bare_aff = AffectionSystemModule(bare_res, reg)
    assert isinstance(bare_aff.manager, AffectionManager)
    assert bare_store.runtime.affection is bare_aff.manager
    assert bare_aff.manager._identity is ident_mod.identity
    pre_status = await bare_aff.status()
    assert pre_status["state"] == "starting"
    assert pre_status["initialized"] is False
    assert pre_status["identity_bound"] is True

    null_runtime_store = SimpleNamespace(s=settings, runtime=None)
    null_res = SystemResources(settings, null_runtime_store, FakeGateway(), None, SimpleNamespace())
    null_aff = AffectionSystemModule(null_res, reg)
    assert isinstance(null_aff.manager, AffectionManager)
    assert null_aff.manager._identity is ident_mod.identity


async def test_memory_compressor_module_worker_lifecycle_cancel_and_status(
    settings, store, monkeypatch
):
    fake_hub = object()
    reg = ModuleRegistry()
    reg.systems["memory_hub"] = SimpleNamespace(module_id="memory_hub", hub=fake_hub)

    # Fallback SessionManager when service lacks .sessions
    svc_no_sessions = SimpleNamespace()
    res_fallback = SystemResources(settings, store, FakeGateway(), None, svc_no_sessions)
    mod_fallback = MemoryCompressorSystemModule(res_fallback, reg)
    assert isinstance(mod_fallback.compressor.sessions, SessionManager)
    assert mod_fallback.memory_hub is fake_hub

    # Uses existing service.sessions when available
    sessions = SessionManager(settings, store)
    svc = SimpleNamespace(sessions=sessions)
    res = SystemResources(settings, store, FakeGateway(), None, svc)
    mod = MemoryCompressorSystemModule(res, reg)
    assert mod.compressor.sessions is sessions
    assert mod.workers() == (mod.run_archive,)
    assert await mod.initialize() is None

    # Status checks across active, backoff, rolling mode, disabled mode, and runtime=None.
    mono_now = 100.0
    mod.compressor.clock = lambda: mono_now
    mod.compressor.slice_backoff["active-slice"] = (1, mono_now + 60.0)
    mod.compressor.slice_backoff["expired-slice"] = (1, mono_now - 5.0)
    st = await mod.status()
    assert st["state"] == "ready"
    assert st["prefix_history_enabled"] is True
    assert st["compression_enabled"] is True
    assert st["backoff_slices"] == 1
    assert "双轨归档" in st["detail"]
    assert "退避 1 片" in st["detail"]

    settings.prefix_history_enabled = False
    st_rolling = await mod.status()
    assert st_rolling["state"] == "ready"
    assert "滚动摘要" in st_rolling["detail"]
    settings.prefix_history_enabled = True

    store.runtime.compression = store.runtime.compression.model_copy(update={"enabled": False})
    st_disabled = await mod.status()
    assert st_disabled["state"] == "disabled"
    assert st_disabled["compression_enabled"] is False
    assert "已暂停压缩" in st_disabled["detail"]
    store.runtime.compression = store.runtime.compression.model_copy(update={"enabled": True})

    # Worker loop tolerates transient errors, records an audit event, and propagates cancellation.
    recorded_events: list[tuple[str, str]] = []
    loop_entered = asyncio.Event()
    calls = 0

    async def fake_sessions():
        nonlocal calls
        calls += 1
        loop_entered.set()
        if calls == 1:
            raise RuntimeError("transient_sqlite_glitch")
        return []

    async def fake_event(code: str, source: str = "system", *, diagnostics=None):
        recorded_events.append((code, source))

    monkeypatch.setattr(store.runtime, "sessions", fake_sessions)
    monkeypatch.setattr(store.runtime, "event", fake_event)

    worker_task = asyncio.create_task(mod.run_archive(), name="memory_compressor")
    await asyncio.wait_for(loop_entered.wait(), timeout=2.0)
    # Give the exception handler one tick to record the event and enter wakeup.wait().
    await asyncio.sleep(0.02)
    assert ("memory_worker_failed", "memory") in recorded_events
    assert not worker_task.done()

    worker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker_task

    # Dynamic service.compressor test-double dispatch for run_archive() and status().
    stub_ran = False

    class _StubCompressor:
        tasks = {"k1": object()}
        slice_backoff = {}

        async def run(self) -> None:
            nonlocal stub_ran
            stub_ran = True
            raise asyncio.CancelledError

    svc.compressor = _StubCompressor()
    with pytest.raises(asyncio.CancelledError):
        await mod.run_archive()
    assert stub_ran is True

    stub_status = await mod.status()
    assert stub_status["state"] == "ready"
    assert stub_status["active_tasks"] == 1
    assert "在途 1 项" in stub_status["detail"]

    # Runtime=None does not break status().
    null_store = SimpleNamespace(s=settings, vault=store.vault, runtime=None)
    null_mod = MemoryCompressorSystemModule(
        SystemResources(settings, null_store, FakeGateway(), None, SimpleNamespace()),
        reg,
    )
    null_status = await null_mod.status()
    assert null_status["state"] == "ready"
    assert null_status["compression_enabled"] is True


async def test_meme_library_module_lifecycle_status_and_empty_state(settings, store, tmp_path):
    reg = ModuleRegistry()
    res = SystemResources(settings, store, FakeGateway(), None, SimpleNamespace())

    # 1. Creates and mounts MemeLibrary onto store.runtime.memes when absent.
    assert getattr(store.runtime, "memes", None) is None
    mod = MemeLibrarySystemModule(res, reg)
    assert isinstance(mod.library, MemeLibrary)
    assert store.runtime.memes is mod.library
    assert mod.workers() == ()
    assert mod.drive_usage is None

    # Pre-init status
    pre = await mod.status()
    assert pre["state"] == "starting"
    assert pre["initialized"] is False
    assert pre["labels_count"] == 0
    assert pre["variants_count"] == 0
    assert pre["total_count"] == 0
    assert pre["total_bytes"] == 0
    assert pre["storage_mode"] == "local"
    assert pre["drive_backup_enabled"] is False

    # Empty initialized status
    await mod.initialize()
    empty = await mod.status()
    assert empty["state"] == "ready"
    assert empty["initialized"] is True
    assert empty["directory_ready"] is True
    assert empty["total_count"] == 0
    assert "暂无本地表情" in empty["detail"]
    assert "目录就绪" in empty["detail"]

    # Reuses existing store.runtime.memes on subsequent module construction.
    mod_reused = MemeLibrarySystemModule(res, reg)
    assert mod_reused.library is mod.library

    # Upload primary + variant under same label and verify counts/bytes.
    await mod.library.upload("开心.png", _sample_png("purple"))
    await mod.library.upload("开心.png", _sample_png("gold"))
    populated = await mod.status()
    assert populated["state"] == "ready"
    assert populated["labels_count"] == 1
    assert populated["variants_count"] == 1
    assert populated["total_count"] == 2
    assert populated["total_bytes"] > 0
    assert populated["directory_ready"] is True
    assert populated["storage_mode"] == "local"
    assert populated["drive_backup_enabled"] is False
    assert "标签 1 个" in populated["detail"]
    assert "素材 2 张" in populated["detail"]
    assert "变体 1 张" in populated["detail"]

    # Symlink root is treated as directory not ready (O_NOFOLLOW semantics).
    real_target = tmp_path / "symlink_target"
    real_target.mkdir()
    symlink_root = tmp_path / "memes_link"
    symlink_root.symlink_to(real_target)
    mod.library.root = symlink_root
    sym_status = await mod.status()
    assert sym_status["directory_ready"] is False
    assert "目录未就绪" in sym_status["detail"]

    # Runtime=None compatibility.
    null_store = SimpleNamespace(s=settings, runtime=None)
    null_mod = MemeLibrarySystemModule(
        SystemResources(settings, null_store, FakeGateway(), None, SimpleNamespace()),
        reg,
    )
    assert isinstance(null_mod.library, MemeLibrary)
    assert (await null_mod.status())["state"] == "starting"


async def test_install_system_modules_standalone_order(settings, store):
    """Standalone install_system_modules resolves full DAG including Tier-2 dependencies."""
    reg = ModuleRegistry()
    svc = SimpleNamespace(
        s=settings,
        store=store,
        sessions=SessionManager(settings, store),
        sender=SimpleNamespace(onebot=None),
        bili=SimpleNamespace(),
    )
    install_system_modules(
        reg,
        SystemResources(settings, store, FakeGateway(), None, svc),
    )
    names = list(reg.systems.keys())
    assert {"identity", "affection", "memory_hub", "memory_compressor", "meme_library"} <= set(
        names
    )
    assert names.index("identity") < names.index("affection")
    assert names.index("memory_hub") < names.index("memory_compressor")
    assert reg.systems["affection"].manager._identity is reg.systems["identity"].identity
    assert reg.systems["memory_compressor"].memory_hub is reg.systems["memory_hub"].hub
    assert store.runtime.memes is reg.systems["meme_library"].library
