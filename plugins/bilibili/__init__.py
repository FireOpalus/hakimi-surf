"""Standalone hot-plug Bilibili surf & patrol remote plugin."""

from __future__ import annotations

from typing import Any

from .client import BV_RE, BVID, URL_RE, BiliClientError, BiliPublicClient, extract_bvid
from .patrol import (
    BiliPatrolEngine,
    BiliPolicy,
    CredentialUpdate,
    Pick,
    Selection,
    has_voucher,
    subtitle_text,
    subtitle_timestamp,
    validate_cookie,
    video_duration,
    wbi_keys,
    wbi_sign,
)

__all__ = [
    "BVID",
    "BV_RE",
    "URL_RE",
    "BiliClientError",
    "BiliPatrolEngine",
    "BiliPolicy",
    "BiliPublicClient",
    "BilibiliPlugin",
    "CredentialUpdate",
    "Pick",
    "Selection",
    "bilibili_config_from_env",
    "build_bilibili_plugin",
    "build_bilibili_plugin_from_env",
    "extract_bvid",
    "has_voucher",
    "subtitle_text",
    "subtitle_timestamp",
    "validate_cookie",
    "video_duration",
    "wbi_keys",
    "wbi_sign",
]

_PLUGIN_MAIN_EXPORTS = frozenset(
    {
        "BilibiliPlugin",
        "bilibili_config_from_env",
        "build_bilibili_plugin",
        "build_bilibili_plugin_from_env",
    }
)


def __getattr__(name: str) -> Any:
    if name in _PLUGIN_MAIN_EXPORTS:
        from . import plugin_main

        return getattr(plugin_main, name)
    raise AttributeError(name)
