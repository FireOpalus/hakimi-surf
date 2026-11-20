"""Standalone hot-plug Bilibili surf & patrol remote plugin."""

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
from .plugin_main import (
    BilibiliPlugin,
    bilibili_config_from_env,
    build_bilibili_plugin,
    build_bilibili_plugin_from_env,
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
