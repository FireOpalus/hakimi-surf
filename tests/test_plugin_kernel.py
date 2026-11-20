"""Unit tests for PluginKernel and ModuleRegistry/ActionCatalog uninstall."""

from __future__ import annotations

import asyncio
import logging

import pytest

from hakimi.life_rules import Rule
from hakimi.modules.actions import Action, ActionCatalog, DailyAction, Outcome
from hakimi.modules.base import BaseModule, ModuleRegistry
from hakimi.plugin_kernel import PluginFiber, PluginKernel
from hakimi.thinking_policy import CORE_THINKING_SCENES, ThinkingScene


async def test_fiber_lifo_effects_sync_async_and_idempotency():
    fiber = PluginFiber("plugin_alpha")
    assert fiber.owner_id == "plugin_alpha"
    assert fiber.state == "active"
    assert isinstance(fiber.created_at, float)
    assert fiber.disposed_at is None

    order: list[str] = []

    def sync_first() -> None:
        order.append("sync_1")

    async def async_second() -> None:
        await asyncio.sleep(0)
        order.append("async_2")

    def sync_third() -> None:
        order.append("sync_3")

    async def async_fourth() -> None:
        await asyncio.sleep(0)
        order.append("async_4")

    fiber.effect(sync_first)
    fiber.effect(async_second)
    fiber.effect(sync_third)
    fiber.effect(async_fourth)

    await fiber.dispose()
    assert order == ["async_4", "sync_3", "async_2", "sync_1"]
    assert fiber.state == "disposed"
    assert isinstance(fiber.disposed_at, float)
    assert fiber.disposed_at >= fiber.created_at

    # Idempotent dispose does not re-run disposers or alter disposed_at.
    first_disposed_at = fiber.disposed_at
    await fiber.dispose()
    assert order == ["async_4", "sync_3", "async_2", "sync_1"]
    assert fiber.disposed_at == first_disposed_at

    # Registering effects or spawning tasks after disposal is rejected cleanly.
    with pytest.raises(RuntimeError):
        fiber.effect(lambda: None)

    async def never_runs() -> None:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        fiber.spawn(never_runs())


async def test_fiber_dispose_error_isolation_and_concurrent_dispose(caplog):
    fiber = PluginFiber("resilient")
    started = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def slow_cleanup() -> None:
        started.set()
        await release.wait()
        order.append("slow_1")

    def broken_sync() -> None:
        order.append("broken_sync_2")
        raise RuntimeError("sync boom")

    async def broken_async() -> None:
        order.append("broken_async_3")
        raise ValueError("async boom")

    def tail_cleanup() -> None:
        order.append("tail_4")

    fiber.effect(tail_cleanup)
    fiber.effect(slow_cleanup)
    fiber.effect(broken_sync)
    fiber.effect(broken_async)

    with caplog.at_level(logging.WARNING, logger="hakimi"):
        t1 = asyncio.create_task(fiber.dispose())
        await started.wait()
        assert fiber.state == "disposing"
        with pytest.raises(RuntimeError):
            fiber.effect(lambda: None)
        t2 = asyncio.create_task(fiber.dispose())
        release.set()
        await asyncio.gather(t1, t2)

    assert fiber.state == "disposed"
    assert order == ["broken_async_3", "broken_sync_2", "slow_1", "tail_4"]
    assert any("fiber_disposer_failed" in rec.message for rec in caplog.records)


async def test_fiber_spawn_tracks_discards_and_cancels_in_lifo_order():
    fiber = PluginFiber("spawner")
    events: list[str] = []

    async def quick_job() -> str:
        return "ok"

    quick = fiber.spawn(quick_job(), name="quick")
    assert await quick == "ok"
    # Give done callback a tick to discard completed task.
    await asyncio.sleep(0)
    assert quick not in fiber._tasks

    started_1 = asyncio.Event()
    started_2 = asyncio.Event()
    blocker = asyncio.Event()

    async def worker(tag: str, ready: asyncio.Event) -> None:
        ready.set()
        try:
            await blocker.wait()
        finally:
            events.append(f"task_finally:{tag}")

    t1 = fiber.spawn(worker("1", started_1), name="w1")
    t2 = fiber.spawn(worker("2", started_2), name="w2")
    await asyncio.gather(started_1.wait(), started_2.wait())

    fiber.effect(lambda: events.append("effect_after_tasks"))

    await fiber.dispose()
    assert t1.cancelled() and t2.cancelled()
    assert events == ["task_finally:2", "task_finally:1", "effect_after_tasks"]
    assert fiber._tasks == []


async def test_kernel_provide_core_protection_and_owner_isolation():
    kernel = PluginKernel()
    core_db = object()
    kernel.provide_core("db", core_db)
    kernel.provide_core("nullable", None)

    assert kernel.has("db")
    assert kernel.get("db") is core_db
    assert kernel.has("nullable")
    assert kernel.get("nullable", "missing") is None

    ctx_a = kernel.fork("plugin_a")
    ctx_b = kernel.fork("plugin_b")

    assert ctx_a.has("db")
    assert ctx_a.get("db") is core_db

    # Duplicate active owner_id is rejected.
    with pytest.raises(ValueError, match="active fiber"):
        kernel.fork("plugin_a")

    # Plugins cannot overwrite core services.
    with pytest.raises(ValueError, match="core"):
        ctx_a.provide("db", object())

    # Plugin A provides service; plugin B cannot overwrite active non-owned service.
    svc_a_v1 = {"v": 1}
    svc_a_v2 = {"v": 2}
    ctx_a.provide("cache", svc_a_v1)
    assert ctx_b.get("cache") is svc_a_v1

    with pytest.raises(ValueError, match="already provided"):
        ctx_b.provide("cache", {"v": 99})

    # Owning fiber can update its own provided service cleanly.
    ctx_a.provide("cache", svc_a_v2)
    assert kernel.get("cache") is svc_a_v2

    # Disposing A unprovides "cache" and allows re-forking "plugin_a" and re-providing "cache".
    assert await kernel.dispose_fiber("plugin_a") is True
    assert await kernel.dispose_fiber("plugin_a") is False
    assert not kernel.has("cache")
    assert "plugin_a" not in kernel.fibers()

    ctx_b.provide("cache", {"v": 3})
    ctx_a_reborn = kernel.fork("plugin_a")
    assert ctx_a_reborn.get("cache") == {"v": 3}

    summary = kernel.status_summary()
    assert summary["fiber_count"] == 2
    assert "db" in summary["core_services"]
    assert "cache" in summary["services"]

    # Core services survive dispose_all().
    await kernel.dispose_all()
    assert kernel.fibers() == {}
    assert kernel.has("db")
    assert kernel.get("db") is core_db
    assert not kernel.has("cache")


async def test_kernel_dispose_all_runs_in_reverse_creation_order():
    kernel = PluginKernel()
    disposed: list[str] = []

    for name in ("first", "second", "third"):
        ctx = kernel.fork(name)
        ctx.effect(lambda n=name: disposed.append(n))

    await kernel.dispose_all()
    assert disposed == ["third", "second", "first"]


async def test_reactive_on_service_ready_lost_async_and_unsubscribe(caplog):
    kernel = PluginKernel()
    events: list[str] = []

    provider = kernel.fork("provider")
    consumer_early = kernel.fork("consumer_early")
    consumer_late = kernel.fork("consumer_late")
    consumer_unsub = kernel.fork("consumer_unsub")

    # Subscribe before service exists (sync on_ready + async on_lost).
    async def early_lost() -> None:
        await asyncio.sleep(0)
        events.append("early_lost")

    consumer_early.on_service(
        "bus",
        on_ready=lambda svc: events.append(f"early_ready:{svc}"),
        on_lost=early_lost,
    )
    consumer_unsub.on_service(
        "bus",
        on_ready=lambda svc: events.append(f"unsub_ready:{svc}"),
        on_lost=lambda: events.append("unsub_lost"),
    )

    # Provide service -> notifies both active listeners.
    provider.provide("bus", "bus-v1")
    assert "early_ready:bus-v1" in events
    assert "unsub_ready:bus-v1" in events

    # Subscribe AFTER service is already present (both sync and async on_ready).
    async_ready_done = asyncio.Event()

    async def late_ready(svc: str) -> None:
        events.append(f"late_async_ready:{svc}")
        async_ready_done.set()

    consumer_late.on_service(
        "bus",
        on_ready=late_ready,
        on_lost=lambda: events.append("late_lost"),
    )
    await asyncio.wait_for(async_ready_done.wait(), timeout=2.0)
    assert "late_async_ready:bus-v1" in events

    # Broken listener on_lost should not prevent other listeners from receiving on_lost.
    consumer_broken = kernel.fork("consumer_broken")

    def broken_lost() -> None:
        raise RuntimeError("listener boom")

    consumer_broken.on_service("bus", on_lost=broken_lost)

    # Disposing consumer_unsub unsubscribes it before provider disposes.
    await consumer_unsub.fiber.dispose()

    with caplog.at_level(logging.WARNING, logger="hakimi"):
        await provider.fiber.dispose()

    assert "early_lost" in events
    assert "late_lost" in events
    assert "unsub_lost" not in events
    assert any("service_on_lost_failed" in rec.message for rec in caplog.records)

    # A new provider bringing the service back triggers on_ready again for active consumers.
    provider2 = kernel.fork("provider2")
    async_ready_done_2 = asyncio.Event()

    async def late_ready_v2(svc: str) -> None:
        events.append(f"late_async_ready:{svc}")
        async_ready_done_2.set()

    consumer_re = kernel.fork("consumer_re")
    consumer_re.on_service("bus", on_ready=late_ready_v2)

    provider2.provide("bus", "bus-v2")
    await asyncio.wait_for(async_ready_done_2.wait(), timeout=2.0)
    assert "early_ready:bus-v2" in events
    assert "late_async_ready:bus-v2" in events
    assert "unsub_ready:bus-v2" not in events

    await kernel.dispose_all()


class _DemoModule(BaseModule):
    def __init__(
        self,
        module_id: str,
        *,
        chat_platforms: tuple[str, ...] = (),
        recipient_namespaces: tuple[str, ...] = (),
        thinking_scenes: tuple[ThinkingScene, ...] = (),
        action_specs: tuple[Action, ...] = (),
        rules: tuple[Rule, ...] = (),
    ) -> None:
        super().__init__(None)
        self.module_id = module_id
        self.display_name = module_id
        self.description = f"desc:{module_id}"
        self.chat_platforms = chat_platforms
        self.recipient_namespaces = recipient_namespaces
        self.thinking_scenes = thinking_scenes
        self._action_specs = action_specs
        self._rules = rules

    def actions(self) -> tuple[Action, ...]:
        return self._action_specs

    def event_rules(self) -> tuple[Rule, ...]:
        return self._rules

    async def _raw_sensor_summary(self) -> str:
        return f"ok:{self.module_id}"


class _FakeSystemModule:
    module_id = "sys_core"
    thinking_scenes = (ThinkingScene("sys.scene", "系统场景"),)

    def public_event_observers(self):
        async def observe(event, now):
            return False

        return {"sys.observer": observe}


class _FakeOwnerDelivery:
    def module_timer(self, module: BaseModule):
        if "remote_chat" not in module.chat_platforms:
            return None

        async def run(call):
            return Outcome("done", "ok")

        action = Action(
            name=f"{module.module_id}.owner_digest",
            description="owner digest",
            run=run,
            social=True,
            owner_only=True,
            origins=frozenset({"timer"}),
        )
        slot = DailyAction(action.name, at=None, target_scope="boundowner:1")
        return action, slot


def test_action_catalog_uninstall_and_reinstall():
    async def run(call):
        return Outcome("done", "ok")

    catalog = ActionCatalog()
    a1 = Action(
        name="alpha.ping",
        description="ping",
        run=run,
        aliases=(("alpha", "ping", False),),
    )
    a2 = Action(
        name="alpha.say",
        description="say",
        run=run,
        social=True,
        aliases=(("alpha", "say", True),),
    )
    b1 = Action(
        name="beta.ping",
        description="beta ping",
        run=run,
        aliases=(("beta", "ping", False),),
    )

    catalog.install("alpha", (a1, a2))
    catalog.install("beta", (b1,))

    assert set(catalog.actions) == {"alpha.ping", "alpha.say", "beta.ping"}
    assert ("alpha", "ping", False) in catalog.aliases
    assert ("alpha", "say", True) in catalog.aliases

    catalog.uninstall("alpha")
    # Idempotent when module has no remaining actions.
    catalog.uninstall("alpha")

    assert set(catalog.actions) == {"beta.ping"}
    assert catalog.owners == {"beta.ping": "beta"}
    assert catalog.aliases == {("beta", "ping", False): "beta.ping"}

    # Reinstalling identical action & alias names succeeds cleanly.
    catalog.install("alpha", (a1, a2))
    assert set(catalog.actions) == {"alpha.ping", "alpha.say", "beta.ping"}


def test_module_registry_register_and_unregister_symmetrical_cleanup():
    async def run(call):
        return Outcome("done", "ok")

    registry = ModuleRegistry(owner_delivery=_FakeOwnerDelivery())
    sys_mod = _FakeSystemModule()
    registry.register_system(sys_mod)

    remote_mod = _DemoModule(
        "remote_plugin",
        chat_platforms=("remote_chat",),
        recipient_namespaces=("remote_user",),
        thinking_scenes=(ThinkingScene("remote.reason", "远程推理"),),
        action_specs=(
            Action(
                name="remote_plugin.act",
                description="remote action",
                run=run,
                aliases=(("remote_plugin", "act", False),),
            ),
        ),
        rules=(Rule("remote.rule", lambda ev: None),),
    )
    other_mod = _DemoModule(
        "other_plugin",
        chat_platforms=("other_chat",),
        recipient_namespaces=("other_user",),
        thinking_scenes=(ThinkingScene("other.reason", "其他推理"),),
        action_specs=(
            Action(
                name="other_plugin.act",
                description="other action",
                run=run,
                aliases=(("other_plugin", "act", False),),
            ),
        ),
        rules=(Rule("other.rule", lambda ev: None),),
    )

    registry.register(remote_mod)
    registry.register(other_mod)

    assert remote_mod._registry is registry
    assert "remote_plugin" in registry._owner_slots
    assert "remote.reason" in registry.thinking_scenes
    assert "remote_chat" in registry.chat_platforms
    assert "remote_user" in registry._recipients
    assert "remote.rule" in registry.rules
    assert "remote_plugin.act" in registry.catalog.actions
    assert "remote_plugin.owner_digest" in registry.catalog.actions

    # System modules cannot be unregistered.
    with pytest.raises(ValueError, match="system"):
        registry.unregister("sys_core")

    # Unregistering unknown module returns False.
    assert registry.unregister("nonexistent") is False

    # Unregistering target module removes all its entries while preserving core/system/other.
    assert registry.unregister("remote_plugin") is True
    assert registry.unregister("remote_plugin") is False

    assert remote_mod._registry is None
    assert registry.get("remote_plugin") is None
    assert [m.module_id for m in registry.all()] == ["other_plugin"]
    assert "remote_plugin" not in registry._owner_slots

    assert "remote_plugin.act" not in registry.catalog.actions
    assert "remote_plugin.owner_digest" not in registry.catalog.actions
    assert ("remote_plugin", "act", False) not in registry.catalog.aliases
    assert "remote_plugin" not in registry.catalog.owners.values()
    assert "other_plugin.act" in registry.catalog.actions

    assert "remote.reason" not in registry.thinking_scenes
    assert "other.reason" in registry.thinking_scenes
    assert "sys.scene" in registry.thinking_scenes
    for core_scene in CORE_THINKING_SCENES:
        assert core_scene.key in registry.thinking_scenes

    assert "remote_chat" not in registry.chat_platforms
    assert registry.chat_platforms["other_chat"] is other_mod

    assert "remote_user" not in registry._recipients
    assert registry._recipients["other_user"] is other_mod

    assert "remote.rule" not in registry.rules
    assert "remote.rule" not in registry.rule_owners
    assert "other.rule" in registry.rules
    assert registry.rule_owners["other.rule"] == "other_plugin"
    assert "sys.observer" in registry.public_event_observers

    # Re-registering the same module succeeds cleanly.
    registry.register(remote_mod)
    assert registry.get("remote_plugin") is remote_mod
    assert "remote.reason" in registry.thinking_scenes
    assert "remote_plugin.act" in registry.catalog.actions
