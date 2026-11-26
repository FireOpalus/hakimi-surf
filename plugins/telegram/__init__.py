"""Standalone out-of-process Telegram Bot remote plugin."""

from __future__ import annotations

from typing import Any

from .tg_api import TelegramApiError, TelegramBotApi

__all__ = [
    "TelegramApiError",
    "TelegramBotApi",
    "TelegramPlugin",
    "build_telegram_plugin_from_env",
]


def __getattr__(name: str) -> Any:
    if name in {"TelegramPlugin", "build_telegram_plugin_from_env"}:
        from . import plugin_main

        return getattr(plugin_main, name)
    raise AttributeError(name)
