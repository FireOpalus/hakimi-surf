"""Standalone hot-plug document parsing and Pillow card rendering plugin."""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hakimi_plugin_sdk import (
    ActionSpec,
    BlobResult,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
)

from .parser import (
    ALLOWED_RICH_DOC_EXTENSIONS,
    DEFAULT_MAX_CHARS,
    MAX_DOCUMENT_BYTES,
    MAX_MAX_CHARS,
    MIN_MAX_CHARS,
    DocumentParseError,
    parse_document_bytes,
)
from .renderer import (
    CARD_THEMES,
    CARD_WIDTH,
    HARD_MAX_CARD_HEIGHT,
    MAX_CARD_BYTES,
    MAX_CARD_HEIGHT,
    MAX_CARD_WIDTH,
    MAX_INPUT_CHARS,
    MAX_RENDER_PIXELS,
    MIN_CARD_HEIGHT,
    MIN_CARD_WIDTH,
    CardRenderError,
    apply_pillow_pixel_limit,
    render_card_bytes,
)

log = logging.getLogger("hakimi_plugin_doc_render")

PLUGIN_ID = "doc_render"
ACTION_READ_FILE = "doc_render.read_file"
ACTION_RENDER_CARD = "doc_render.render_card"

MAX_BLOB_BYTES = 16 * 1024 * 1024
DEFAULT_PARSE_TIMEOUT_SECONDS = 15.0
DEFAULT_RENDER_TIMEOUT_SECONDS = 15.0


def _has_pypdf() -> bool:
    try:
        import pypdf  # noqa: F401
    except ImportError:
        return False
    return True


class ReadFileArgs(BaseModel):
    """Strict input schema for `doc_render.read_file` (payload-only, never paths)."""

    model_config = ConfigDict(extra="forbid", strict=True)

    filename: str = Field(min_length=1, max_length=255)
    mime_type: str = Field(default="", max_length=128)
    max_chars: int = Field(default=DEFAULT_MAX_CHARS, ge=MIN_MAX_CHARS, le=MAX_MAX_CHARS)


class RenderCardArgs(BaseModel):
    """Strict input schema for `doc_render.render_card`."""

    model_config = ConfigDict(extra="forbid", strict=True)

    text: str = Field(min_length=1, max_length=MAX_INPUT_CHARS)
    format: Literal["png", "jpeg"] = "png"
    width: int = Field(default=CARD_WIDTH, ge=MIN_CARD_WIDTH, le=MAX_CARD_WIDTH)
    max_height: int = Field(default=MAX_CARD_HEIGHT, ge=MIN_CARD_HEIGHT, le=HARD_MAX_CARD_HEIGHT)
    theme: Literal["lavender", "light", "dark"] = "lavender"


_READ_FILE_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "filename": {
            "type": "string",
            "min_length": 1,
            "max_length": 255,
            "description": "文档文件名（仅文件名，禁止传本地路径）",
        },
        "mime_type": {
            "type": "string",
            "max_length": 128,
            "default": "",
            "description": "可选 MIME 类型提示",
        },
        "max_chars": {
            "type": "integer",
            "minimum": MIN_MAX_CHARS,
            "maximum": MAX_MAX_CHARS,
            "default": DEFAULT_MAX_CHARS,
            "description": "返回文本摘要最大字符数",
        },
    },
    "required": ["filename"],
}

_RENDER_CARD_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "text": {
            "type": "string",
            "min_length": 1,
            "max_length": 4096,
            "description": "待渲染 Markdown 或纯文本内容",
        },
        "format": {
            "type": "string",
            "enum": ["png", "jpeg"],
            "default": "png",
            "description": "输出图片格式",
        },
        "width": {
            "type": "integer",
            "minimum": MIN_CARD_WIDTH,
            "maximum": MAX_CARD_WIDTH,
            "default": CARD_WIDTH,
            "description": "卡片宽度（像素）",
        },
        "max_height": {
            "type": "integer",
            "minimum": MIN_CARD_HEIGHT,
            "maximum": HARD_MAX_CARD_HEIGHT,
            "default": MAX_CARD_HEIGHT,
            "description": "卡片最大高度（像素）",
        },
        "theme": {
            "type": "string",
            "enum": list(CARD_THEMES.keys()),
            "default": "lavender",
            "description": "卡片配色主题",
        },
    },
    "required": ["text"],
}


class DocRenderPlugin:
    """Sandboxed out-of-process document parser and Pillow card renderer plugin."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        parse_timeout_seconds: float = DEFAULT_PARSE_TIMEOUT_SECONDS,
        render_timeout_seconds: float = DEFAULT_RENDER_TIMEOUT_SECONDS,
        max_workers: int = 2,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        max_frame_bytes: int = 262_144,
    ) -> None:
        self.parse_timeout_seconds = max(0.05, float(parse_timeout_seconds))
        self.render_timeout_seconds = max(0.05, float(render_timeout_seconds))
        workers = max(1, min(int(max_workers), 4))
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="doc-render")
        self._sem = asyncio.Semaphore(workers)

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id=PLUGIN_ID,
            token=token,
            display_name="文档解析与卡片渲染",
            description="沙盒化富文档解析（文本/PDF/Office）与 Pillow Markdown 卡片渲染",
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
                    name=ACTION_READ_FILE,
                    description=(
                        "在隔离沙盒中解析传入字节载荷的纯文本、PDF 或 Office（DOCX/XLSX/PPTX）文档"
                    ),
                    arguments=_READ_FILE_ACTION_SCHEMA,
                    origins=["admin"],
                    timeout_seconds=30.0,
                    handler=self._handle_read_file,
                ),
                ActionSpec(
                    name=ACTION_RENDER_CARD,
                    description="使用 Pillow 将 Markdown/代码/表格文本渲染为有界 PNG 或 JPEG 卡片",
                    arguments=_RENDER_CARD_ACTION_SCHEMA,
                    origins=["admin"],
                    timeout_seconds=30.0,
                    handler=self._handle_render_card,
                ),
            ),
            initial_status=self.status_snapshot(),
            connect_factory=connect_factory,
            max_frame_bytes=max_frame_bytes,
            max_blob_bytes=MAX_BLOB_BYTES,
        )

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "rich_formats": sorted(ext.lstrip(".") for ext in ALLOWED_RICH_DOC_EXTENSIONS),
            "card_themes": sorted(CARD_THEMES.keys()),
            "max_doc_bytes": MAX_DOCUMENT_BYTES,
            "max_card_bytes": MAX_CARD_BYTES,
            "max_card_pixels": MAX_RENDER_PIXELS,
            "pypdf_available": _has_pypdf(),
        }

    async def close(self) -> None:
        await self.client.stop()
        self._executor.shutdown(wait=False, cancel_futures=True)

    async def _handle_read_file(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("invalid_document", "invalid arguments")
        try:
            args = ReadFileArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("invalid_document", "invalid read_file arguments") from None

        blob = params.get("blob")
        if not isinstance(blob, (bytes, bytearray)):
            raise PluginRpcError("invalid_document", "missing document payload")
        raw_bytes = bytes(blob)
        if len(raw_bytes) > MAX_DOCUMENT_BYTES:
            raise PluginRpcError("oversized", "document payload exceeds limit")

        loop = asyncio.get_running_loop()
        async with self._sem:
            try:
                async with asyncio.timeout(self.parse_timeout_seconds):
                    parsed = await loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            parse_document_bytes,
                            raw_bytes,
                            args.filename,
                            mime_type=args.mime_type,
                            max_chars=args.max_chars,
                            max_bytes=MAX_DOCUMENT_BYTES,
                            timeout_seconds=self.parse_timeout_seconds,
                        ),
                    )
            except TimeoutError:
                raise PluginRpcError("parse_timeout", "document parsing timed out") from None
            except DocumentParseError as exc:
                raise PluginRpcError(exc.code, exc.message) from None
        return parsed.to_result_dict()

    async def _handle_render_card(self, params: dict[str, Any]) -> BlobResult:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("card_text_invalid", "invalid arguments")
        try:
            args = RenderCardArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("card_text_invalid", "invalid render_card arguments") from None

        if not args.text.strip():
            raise PluginRpcError("card_text_invalid", "empty card text")
        if args.width * args.max_height > MAX_RENDER_PIXELS:
            raise PluginRpcError("card_too_large", "card dimensions exceed pixel limit")

        loop = asyncio.get_running_loop()
        async with self._sem:
            try:
                async with asyncio.timeout(self.render_timeout_seconds):
                    rendered = await loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            render_card_bytes,
                            args.text,
                            fmt=args.format,
                            width=args.width,
                            max_height=args.max_height,
                            theme=args.theme,
                            max_bytes=MAX_CARD_BYTES,
                        ),
                    )
            except TimeoutError:
                raise PluginRpcError("render_failed", "card rendering timed out") from None
            except CardRenderError as exc:
                raise PluginRpcError(exc.code, exc.message) from None

        return BlobResult(
            data=rendered.to_metadata(),
            blob=rendered.image_bytes,
            mime_type=rendered.mime_type,
            filename=rendered.filename,
        )


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except ValueError:
        return default
    return value if value > 0 else default


def build_doc_render_plugin_from_env(**overrides: Any) -> DocRenderPlugin:
    """Create a DocRenderPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_DOC_RENDER_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_DOC_RENDER_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "parse_timeout_seconds": _float_env(
            "DOC_RENDER_PARSE_TIMEOUT", DEFAULT_PARSE_TIMEOUT_SECONDS
        ),
        "render_timeout_seconds": _float_env(
            "DOC_RENDER_RENDER_TIMEOUT", DEFAULT_RENDER_TIMEOUT_SECONDS
        ),
    }
    kwargs.update(overrides)
    return DocRenderPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the doc_render remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    apply_pillow_pixel_limit()
    plugin = build_doc_render_plugin_from_env()
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
