"""Standalone out-of-process web search and link preview remote plugin."""

from .plugin_main import WebToolsPlugin, build_web_tools_plugin_from_env

__all__ = ["WebToolsPlugin", "build_web_tools_plugin_from_env"]
