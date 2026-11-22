"""Standalone hot-plug music source finder plugin (`song_finder`).

Physically isolated from any patrol modules and strictly anonymous (zero account cookies).
Exposes `song_finder.search` and `song_finder.fetch_source` via the Hakimi plugin bus.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from collections.abc import Callable, Sequence
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hakimi_plugin_sdk import (
    BLOB_HARD_MAX_BYTES,
    ActionSpec,
    BlobResult,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
)
from plugins.song_finder.providers import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_MAX_DOWNLOAD_BYTES,
    DEFAULT_MAX_OUTPUT_BYTES,
    HARD_MAX_DURATION_SEC,
    HARD_MIN_DURATION_SEC,
    MAX_QUERY_LEN,
    MAX_SEARCH_LIMIT,
    MIN_OUTPUT_BYTES_LIMIT,
    BilibiliPublicProvider,
    ProviderCircuitBreaker,
    SongFinderEngine,
    SongFinderError,
    YouTubeProvider,
)

log = logging.getLogger("hakimi_plugin_song_finder")

SONG_FINDER_MAX_BLOB_BYTES = min(DEFAULT_MAX_OUTPUT_BYTES, BLOB_HARD_MAX_BYTES)

SEARCH_ACTION_ARGS: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "query": {"type": "string", "min_length": 1, "max_length": MAX_QUERY_LEN},
        "title": {"type": "string", "max_length": 80, "default": ""},
        "artist": {"type": "string", "max_length": 60, "default": ""},
        "strategy": {
            "type": "string",
            "enum": ["auto", "youtube", "bilibili_public"],
            "default": "auto",
        },
        "limit": {"type": "integer", "ge": 1, "le": MAX_SEARCH_LIMIT, "default": 5},
    },
    "required": ["query"],
}

FETCH_SOURCE_ACTION_ARGS: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "query": {"type": "string", "min_length": 1, "max_length": MAX_QUERY_LEN},
        "title": {"type": "string", "max_length": 80, "default": ""},
        "artist": {"type": "string", "max_length": 60, "default": ""},
        "strategy": {
            "type": "string",
            "enum": ["auto", "youtube", "bilibili_public"],
            "default": "auto",
        },
        "preferred_format": {
            "type": "string",
            "enum": ["m4a", "mp3", "flac"],
            "default": "m4a",
        },
        "max_bytes": {
            "type": "integer",
            "ge": MIN_OUTPUT_BYTES_LIMIT,
            "le": SONG_FINDER_MAX_BLOB_BYTES,
            "default": SONG_FINDER_MAX_BLOB_BYTES,
        },
        "min_duration_sec": {
            "type": "integer",
            "ge": HARD_MIN_DURATION_SEC,
            "le": 300,
            "default": 60,
        },
        "max_duration_sec": {
            "type": "integer",
            "ge": 60,
            "le": HARD_MAX_DURATION_SEC,
            "default": 600,
        },
    },
    "required": ["query"],
}


class SearchArgs(BaseModel):
    """Strict input validation for `song_finder.search`."""

    model_config = ConfigDict(extra="forbid", strict=True)

    query: str = Field(min_length=1, max_length=MAX_QUERY_LEN)
    title: str = Field(default="", max_length=80)
    artist: str = Field(default="", max_length=60)
    strategy: Literal["auto", "youtube", "bilibili_public"] = "auto"
    limit: int = Field(default=5, ge=1, le=MAX_SEARCH_LIMIT)


class FetchSourceArgs(BaseModel):
    """Strict input validation for `song_finder.fetch_source`."""

    model_config = ConfigDict(extra="forbid", strict=True)

    query: str = Field(min_length=1, max_length=MAX_QUERY_LEN)
    title: str = Field(default="", max_length=80)
    artist: str = Field(default="", max_length=60)
    strategy: Literal["auto", "youtube", "bilibili_public"] = "auto"
    preferred_format: Literal["m4a", "mp3", "flac"] = "m4a"
    max_bytes: int = Field(
        default=SONG_FINDER_MAX_BLOB_BYTES,
        ge=MIN_OUTPUT_BYTES_LIMIT,
        le=SONG_FINDER_MAX_BLOB_BYTES,
    )
    min_duration_sec: int = Field(default=60, ge=HARD_MIN_DURATION_SEC, le=300)
    max_duration_sec: int = Field(default=600, ge=60, le=HARD_MAX_DURATION_SEC)


class SongFinderPlugin:
    """Out-of-process music source finder wrapping `HakimiPluginClient`."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        engine: SongFinderEngine | None = None,
        provider_order: str | Sequence[str] | None = None,
        proxy_url: str | None = None,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        yt_dlp_bin: str = "",
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
    ) -> None:
        bounded_cap = min(
            max(MIN_OUTPUT_BYTES_LIMIT, int(max_download_bytes)), SONG_FINDER_MAX_BLOB_BYTES
        )
        if engine is not None:
            self.engine = engine
        else:
            breaker = ProviderCircuitBreaker(
                base_cooldown_seconds=cooldown_seconds,
                clock=clock,
            )
            youtube = YouTubeProvider(
                yt_dlp_bin=yt_dlp_bin,
                proxy_url=proxy_url,
                max_download_bytes=bounded_cap,
            )
            bilibili_public = BilibiliPublicProvider(
                http=http,
                max_download_bytes=bounded_cap,
                clock=clock,
            )
            self.engine = SongFinderEngine(
                youtube=youtube,
                bilibili_public=bilibili_public,
                provider_order=provider_order,
                circuit_breaker=breaker,
                max_download_bytes=bounded_cap,
                max_output_bytes=bounded_cap,
                ffmpeg_bin=ffmpeg_bin,
                ffprobe_bin=ffprobe_bin,
            )

        self._fetch_slots = asyncio.Semaphore(2)

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id="song_finder",
            token=token,
            display_name="音源检索器",
            description="独立多策略干净原曲检索与音频封装插件（风控与冷却完全隔离）",
            scopes=("actions",),
            node_region="KR",
            admin_panel=None,
            attention_level="LOW",
            reconnect_min=reconnect_min,
            reconnect_max=reconnect_max,
        )
        self.client = HakimiPluginClient(
            config,
            actions=(
                ActionSpec(
                    name="song_finder.search",
                    description="按歌名与歌手检索干净原曲候选列表（支持多策略切换与独立风控冷却）",
                    arguments=SEARCH_ACTION_ARGS,
                    origins=["admin"],
                    timeout_seconds=60.0,
                    handler=self._handle_search_action,
                ),
                ActionSpec(
                    name="song_finder.fetch_source",
                    description="检索、下载并封装干净原曲音频返回给歌单工坊（独立隔离，无共享账号凭据）",
                    arguments=FETCH_SOURCE_ACTION_ARGS,
                    origins=["admin"],
                    timeout_seconds=240.0,
                    handler=self._handle_fetch_source_action,
                ),
            ),
            initial_status=self.engine.status_snapshot(),
            connect_factory=connect_factory,
            max_blob_bytes=SONG_FINDER_MAX_BLOB_BYTES,
        )
        self.client.on_connect(self._on_connect)

    async def close(self) -> None:
        await self.client.stop()
        await self.engine.close()

    async def _push_status_best_effort(self) -> None:
        if not self.client.connected:
            return
        with contextlib.suppress(Exception):
            await self.client.push_status(self.engine.status_snapshot())

    async def _on_connect(self, _welcome: dict[str, Any]) -> None:
        await self._push_status_best_effort()

    async def _handle_search_action(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("invalid_query", "arguments must be an object")
        try:
            args = SearchArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("invalid_query", "invalid search arguments") from None

        try:
            result = await self.engine.search(
                args.query,
                title=args.title,
                artist=args.artist,
                strategy=args.strategy,
                limit=args.limit,
            )
        except SongFinderError as exc:
            await self._push_status_best_effort()
            raise PluginRpcError(exc.code, exc.message) from None

        await self._push_status_best_effort()
        return result

    async def _handle_fetch_source_action(self, params: dict[str, Any]) -> BlobResult:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("invalid_query", "arguments must be an object")
        try:
            args = FetchSourceArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("invalid_query", "invalid fetch_source arguments") from None

        async with self._fetch_slots:
            try:
                fetched = await self.engine.fetch_source(
                    args.query,
                    title=args.title,
                    artist=args.artist,
                    strategy=args.strategy,
                    preferred_format=args.preferred_format,
                    max_bytes=args.max_bytes,
                    min_duration_sec=args.min_duration_sec,
                    max_duration_sec=args.max_duration_sec,
                )
            except SongFinderError as exc:
                await self._push_status_best_effort()
                raise PluginRpcError(exc.code, exc.message) from None

        await self._push_status_best_effort()
        return BlobResult(
            data={
                "provider": fetched.provider,
                "title": fetched.title,
                "uploader": fetched.uploader,
                "duration_sec": round(fetched.duration_sec, 2),
                "format": fetched.audio_format,
                "mime_type": fetched.mime_type,
                "filename": fetched.filename,
                "byte_size": fetched.byte_size,
                "sha256": fetched.sha256,
                "skipped_providers": dict(fetched.skipped_providers),
            },
            blob=fetched.data,
            mime_type=fetched.mime_type,
            filename=fetched.filename,
        )


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        val = int(raw)
        return val if val > 0 else default
    except ValueError:
        return default


def _parse_positive_float_env(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        val = float(raw)
        return val if val > 0 else default
    except ValueError:
        return default


def build_song_finder_plugin_from_env(**overrides: Any) -> SongFinderPlugin:
    """Create a SongFinderPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_SONG_FINDER_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_SONG_FINDER_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "provider_order": os.getenv("SONG_FINDER_PROVIDER_ORDER") or None,
        "proxy_url": os.getenv("SONG_FINDER_PROXY_URL") or None,
        "cooldown_seconds": _parse_positive_float_env(
            "SONG_FINDER_COOLDOWN_SECONDS", DEFAULT_COOLDOWN_SECONDS
        ),
        "max_download_bytes": _parse_positive_int_env(
            "SONG_FINDER_MAX_DOWNLOAD_BYTES", DEFAULT_MAX_DOWNLOAD_BYTES
        ),
        "yt_dlp_bin": os.getenv("SONG_FINDER_YT_DLP_BIN") or "",
        "ffmpeg_bin": os.getenv("SONG_FINDER_FFMPEG_BIN") or "ffmpeg",
        "ffprobe_bin": os.getenv("SONG_FINDER_FFPROBE_BIN") or "ffprobe",
    }
    kwargs.update(overrides)
    return SongFinderPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the song_finder remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    plugin = build_song_finder_plugin_from_env()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        asyncio.create_task(plugin.client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await plugin.client.run_forever()
    finally:
        await plugin.close()


if __name__ == "__main__":
    asyncio.run(main())
