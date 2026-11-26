"""Structural, re-export, boundary, and circular-import guards for `hakimi.plugin_bus`."""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from pathlib import Path

import pytest

import hakimi.plugin_bus as plugin_bus_pkg
from hakimi.plugin_bus import (
    _BILI_PRIVATE_HANDLE_MAX_COMMITS,
    _BILI_PRIVATE_HANDLE_TTL_SECONDS,
    _BILI_PRIVATE_SHARE_PRIORITY_SECONDS,
    _DEFAULT_MAX_FRAME_BYTES,
    _MAX_BILI_PRIVATE_HANDLES,
    _NODE_BRIDGE_ALLOWED_ACTIONS,
    _SCHEMA,
    BLOB_CHUNK_RAW_BYTES,
    BLOB_HARD_MAX_BYTES,
    BLOB_TRANSFER_TTL_SECONDS,
    HOTPLUG_PILOT_PLUGIN_IDS,
    PLATFORM_ID_RE,
    PLUGIN_ID_RE,
    RESERVED_CHAT_PLATFORMS,
    RESERVED_PLUGIN_IDS,
    SCHEMA,
    VALID_SCOPES,
    PluginBus,
    PluginBusAIGatewayMixin,
    PluginBusBlobMixin,
    PluginBusDomainBridgeMixin,
    RemoteModuleProxy,
    ToolCallResult,
    _PendingRpc,
    _PluginActionDecl,
    _PluginHelloFrame,
    _PluginSession,
    _PluginThinkingSceneDecl,
    ai_gateway,
    blob,
    bus,
    domain_bridge,
    protocol,
    proxy,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PKG_DIR = REPO_ROOT / "src" / "hakimi" / "plugin_bus"
SUBMODULES = ("protocol", "blob", "proxy", "ai_gateway", "domain_bridge", "bus")


def _runtime_relative_imports(path: Path) -> set[str]:
    """Collect relative sibling submodule names imported at runtime (outside TYPE_CHECKING)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()

    def _is_type_checking_guard(node: ast.If) -> bool:
        test = node.test
        return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
            isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
        )

    def _visit(stmts: list[ast.stmt]) -> None:
        for stmt in stmts:
            if isinstance(stmt, ast.If) and _is_type_checking_guard(stmt):
                _visit(stmt.orelse)
                continue
            if isinstance(stmt, ast.ImportFrom) and stmt.level == 1 and stmt.module:
                imported.add(stmt.module.split(".", 1)[0])
            for child in ast.iter_child_nodes(stmt):
                if isinstance(child, ast.If | ast.With | ast.Try | ast.For | ast.While):
                    body = getattr(child, "body", [])
                    orelse = getattr(child, "orelse", [])
                    finalbody = getattr(child, "finalbody", [])
                    if isinstance(child, ast.If) and _is_type_checking_guard(child):
                        _visit(orelse)
                    else:
                        _visit(body)
                        _visit(orelse)
                        _visit(finalbody)

    _visit(tree.body)
    return imported


def test_plugin_bus_subpackage_layout_and_monolith_removed() -> None:
    assert not (REPO_ROOT / "src" / "hakimi" / "plugin_bus.py").exists()
    assert PKG_DIR.is_dir()
    assert hasattr(plugin_bus_pkg, "__path__")
    for name in ("__init__", *SUBMODULES):
        assert (PKG_DIR / f"{name}.py").is_file(), f"missing plugin_bus/{name}.py"


def test_plugin_bus_reexports_preserve_identity_and_all_completeness() -> None:
    expected_identity = {
        "PluginBus": bus.PluginBus,
        "PluginBusBlobMixin": blob.PluginBusBlobMixin,
        "PluginBusAIGatewayMixin": ai_gateway.PluginBusAIGatewayMixin,
        "PluginBusDomainBridgeMixin": domain_bridge.PluginBusDomainBridgeMixin,
        "RemoteModuleProxy": proxy.RemoteModuleProxy,
        "_PendingRpc": proxy._PendingRpc,
        "_PluginSession": proxy._PluginSession,
        "VALID_SCOPES": protocol.VALID_SCOPES,
        "RESERVED_PLUGIN_IDS": protocol.RESERVED_PLUGIN_IDS,
        "HOTPLUG_PILOT_PLUGIN_IDS": protocol.HOTPLUG_PILOT_PLUGIN_IDS,
        "RESERVED_CHAT_PLATFORMS": protocol.RESERVED_CHAT_PLATFORMS,
        "PLUGIN_ID_RE": protocol.PLUGIN_ID_RE,
        "PLATFORM_ID_RE": protocol.PLATFORM_ID_RE,
        "SCHEMA": protocol.SCHEMA,
        "_SCHEMA": protocol._SCHEMA,
        "_NODE_BRIDGE_ALLOWED_ACTIONS": protocol._NODE_BRIDGE_ALLOWED_ACTIONS,
        "_PluginActionDecl": protocol._PluginActionDecl,
        "_PluginThinkingSceneDecl": protocol._PluginThinkingSceneDecl,
        "_PluginHelloFrame": protocol._PluginHelloFrame,
        "_BILI_PRIVATE_HANDLE_TTL_SECONDS": domain_bridge._BILI_PRIVATE_HANDLE_TTL_SECONDS,
        "_BILI_PRIVATE_HANDLE_MAX_COMMITS": domain_bridge._BILI_PRIVATE_HANDLE_MAX_COMMITS,
        "_BILI_PRIVATE_SHARE_PRIORITY_SECONDS": domain_bridge._BILI_PRIVATE_SHARE_PRIORITY_SECONDS,
        "_MAX_BILI_PRIVATE_HANDLES": domain_bridge._MAX_BILI_PRIVATE_HANDLES,
        "ToolCallResult": blob.ToolCallResult,
    }
    for name, target in expected_identity.items():
        assert getattr(plugin_bus_pkg, name) is target
        assert name in plugin_bus_pkg.__all__

    # Direct imports brought into this module match package attributes.
    assert PluginBus is bus.PluginBus
    assert RemoteModuleProxy is proxy.RemoteModuleProxy
    assert _PendingRpc is proxy._PendingRpc
    assert _PluginSession is proxy._PluginSession
    assert _PluginActionDecl is protocol._PluginActionDecl
    assert _PluginThinkingSceneDecl is protocol._PluginThinkingSceneDecl
    assert _PluginHelloFrame is protocol._PluginHelloFrame
    assert VALID_SCOPES is protocol.VALID_SCOPES
    assert RESERVED_PLUGIN_IDS is protocol.RESERVED_PLUGIN_IDS
    assert HOTPLUG_PILOT_PLUGIN_IDS is protocol.HOTPLUG_PILOT_PLUGIN_IDS
    assert RESERVED_CHAT_PLATFORMS is protocol.RESERVED_CHAT_PLATFORMS
    assert PLUGIN_ID_RE is protocol.PLUGIN_ID_RE
    assert PLATFORM_ID_RE is protocol.PLATFORM_ID_RE
    assert SCHEMA == _SCHEMA and "plugin_credentials" in SCHEMA and "plugin_kv_state" in SCHEMA
    assert _NODE_BRIDGE_ALLOWED_ACTIONS is protocol._NODE_BRIDGE_ALLOWED_ACTIONS
    assert ToolCallResult is blob.ToolCallResult


def test_plugin_bus_submodule_boundaries_are_strictly_acyclic() -> None:
    graph = {name: _runtime_relative_imports(PKG_DIR / f"{name}.py") for name in SUBMODULES}

    assert graph["protocol"] == set()
    assert graph["proxy"] <= {"protocol"}
    assert graph["blob"] <= {"protocol", "proxy"}
    assert graph["ai_gateway"] <= {"protocol", "proxy"}
    assert graph["domain_bridge"] <= {"protocol", "proxy"}
    assert graph["bus"] <= {"protocol", "proxy", "blob", "ai_gateway", "domain_bridge"}

    # Verify full graph has no cycles via DFS.
    visiting: set[str] = set()
    visited: set[str] = set()

    def _dfs(node: str) -> None:
        assert node not in visiting, f"cycle detected involving {node}"
        if node in visited:
            return
        visiting.add(node)
        for dep in graph[node]:
            if dep in graph:
                _dfs(dep)
        visiting.remove(node)
        visited.add(node)

    for mod in SUBMODULES:
        _dfs(mod)


@pytest.mark.parametrize(
    ("entry_module", "verify_modules"),
    [
        (
            "hakimi.plugin_bus.protocol",
            (
                "hakimi.plugin_bus",
                "hakimi.plugin_bus.protocol",
                "hakimi.plugin_bus.proxy",
                "hakimi.plugin_bus.blob",
                "hakimi.plugin_bus.ai_gateway",
                "hakimi.plugin_bus.domain_bridge",
                "hakimi.plugin_bus.bus",
            ),
        ),
        ("hakimi.app", ("hakimi.app", "hakimi.plugin_bus")),
    ],
)
def test_plugin_bus_submodules_import_cleanly_in_isolated_process(
    entry_module: str, verify_modules: tuple[str, ...]
) -> None:
    script = (
        f"import importlib, sys; "
        f"importlib.import_module({entry_module!r}); "
        f"mods = {verify_modules!r}; "
        f"[importlib.import_module(m) for m in mods]; "
        f"assert all(m in sys.modules for m in mods)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"failed importing {entry_module}: {proc.stderr}"


def test_plugin_bus_security_limits_and_model_work_isolation_preserved() -> None:
    assert _DEFAULT_MAX_FRAME_BYTES == 262_144
    assert BLOB_CHUNK_RAW_BYTES == 128 * 1024
    assert BLOB_HARD_MAX_BYTES == 40 * 1024 * 1024
    assert BLOB_TRANSFER_TTL_SECONDS == 60.0
    assert _BILI_PRIVATE_HANDLE_TTL_SECONDS == 1800.0
    assert _BILI_PRIVATE_HANDLE_MAX_COMMITS == 2
    assert _BILI_PRIVATE_SHARE_PRIORITY_SECONDS == 24 * 3600
    assert _MAX_BILI_PRIVATE_HANDLES == 64

    # Verify PluginBus composes all three mixins and keeps key methods intact.
    assert issubclass(PluginBus, PluginBusBlobMixin)
    assert issubclass(PluginBus, PluginBusAIGatewayMixin)
    assert issubclass(PluginBus, PluginBusDomainBridgeMixin)
    for method_name in (
        "create_plugin",
        "rotate_token",
        "authenticate",
        "serve",
        "invoke_tool",
        "overview",
        "status_summary",
        "_upload_blob_chunks",
        "_download_blob_chunks",
        "_best_effort_release_blob",
        "_handle_ai_budget_check",
        "_handle_ai_screen",
        "_handle_ai_generate",
        "_handle_ai_video_analyze",
        "_handle_world_event",
        "_handle_bili_memory_preferences",
        "_handle_bili_private_topic_commit",
        "_handle_state_get",
        "_handle_node_call",
        "relay_node_event",
        "broadcast_world_event",
    ):
        assert hasattr(PluginBus, method_name), f"PluginBus missing {method_name}"

    # Verify every model_work(...) call in ai_gateway.py preserves independent=True.
    tree = ast.parse((PKG_DIR / "ai_gateway.py").read_text(encoding="utf-8"))
    mw_calls: list[ast.Call] = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "model_work"
    ]
    assert len(mw_calls) == 4
    for call in mw_calls:
        indep = [kw.value for kw in call.keywords if kw.arg == "independent"]
        assert len(indep) == 1
        assert isinstance(indep[0], ast.Constant) and indep[0].value is True

    # Ensure dynamic import via importlib works identically.
    mod = importlib.import_module("hakimi.plugin_bus")
    assert mod.PluginBus is PluginBus
