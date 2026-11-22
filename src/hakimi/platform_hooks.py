"""Kernel-owned platform policy hook registry.

Decouples core security, social export, memory mirror, and proactive delivery
from platform-specific modules. Platform modules register their policy hooks
when initialized; detached callers (such as standalone tests or the isolated
admin sandbox without a ModuleRegistry) resolve built-in default providers
through a single centralized lazy loader while remaining strictly fail-closed
for any unregistered or unknown platform.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class PlatformPolicyHooks:
    """Policy callbacks registered by a chat platform module."""

    authorize: Callable[..., bool]
    permits_export: Callable[[Any, str], bool]
    permits_memory_mirror: Callable[[Any, Any], bool]
    allowed_proactive: Callable[[Any, Any], bool]

    def __post_init__(self) -> None:
        for field_name in (
            "authorize",
            "permits_export",
            "permits_memory_mirror",
            "allowed_proactive",
        ):
            if not callable(getattr(self, field_name)):
                raise TypeError(f"PlatformPolicyHooks.{field_name} must be callable")


_HOOKS: dict[str, PlatformPolicyHooks] = {}

# Single centralized lazy provider map for detached callers without ModuleRegistry.
_DEFAULT_PROVIDER_MODULES: dict[str, tuple[str, str]] = {
    "onebot": ("hakimi.modules.qq_social", "register_qq_platform_hooks"),
    "qq": ("hakimi.modules.qq_social", "register_qq_platform_hooks"),
}


def register_platform_hooks(platform: str, hooks: PlatformPolicyHooks) -> None:
    """Register or replace policy hooks for a canonical platform identifier."""
    key = str(platform or "").strip()
    if not key:
        raise ValueError("platform must be a non-empty string")
    if not isinstance(hooks, PlatformPolicyHooks):
        raise TypeError("hooks must be a PlatformPolicyHooks instance")
    _HOOKS[key] = hooks


def unregister_platform_hooks(platform: str) -> None:
    """Remove registered policy hooks for a platform (primarily for tests)."""
    _HOOKS.pop(str(platform or "").strip(), None)


def registered_platforms() -> tuple[str, ...]:
    """Return sorted platform identifiers currently registered in the hook table."""
    return tuple(sorted(_HOOKS))


def _resolve_lazy_default(platform: str) -> PlatformPolicyHooks | None:
    entry = _DEFAULT_PROVIDER_MODULES.get(platform)
    if entry is None:
        return None
    module_path, installer_name = entry
    try:
        mod = import_module(module_path)
    except Exception:
        return None
    installer = getattr(mod, installer_name, None)
    if callable(installer):
        installer()
    return _HOOKS.get(platform)


def get_platform_hooks(
    platform: str, *, allow_lazy_default: bool = True
) -> PlatformPolicyHooks | None:
    """Return policy hooks for a platform, or None (fail-closed) if unknown."""
    key = str(platform or "").strip()
    if not key:
        return None
    hooks = _HOOKS.get(key)
    if hooks is not None:
        return hooks
    if not allow_lazy_default:
        return None
    return _resolve_lazy_default(key)
