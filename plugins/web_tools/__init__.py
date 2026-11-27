"""Standalone out-of-process web search and link preview remote plugin."""

from __future__ import annotations

from typing import Any

__all__ = ["WebToolsPlugin", "build_web_tools_plugin_from_env"]


def __getattr__(name: str) -> Any:
    if name in {"WebToolsPlugin", "build_web_tools_plugin_from_env"}:
        from . import plugin_main

        return getattr(plugin_main, name)
    raise AttributeError(name)
