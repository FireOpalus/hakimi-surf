"""Standalone hot-plug web search and link preview remote plugin."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from hakimi_plugin_sdk import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
)

from .preview import (
    PREVIEW_DEFAULT_BUDGET_SECONDS,
    PREVIEW_MAX_BUDGET_SECONDS,
    PREVIEW_MAX_URLS,
    PREVIEW_MIN_BUDGET_SECONDS,
    extract_urls,
    preview_urls,
)
from .search import (
    SEARCH_DEFAULT_TIMEOUT_SECONDS,
    SEARCH_MAX_TIMEOUT_SECONDS,
    SEARCH_MIN_TIMEOUT_SECONDS,
    ProviderChoice,
    SearchError,
    SearchProviderConfig,
    execute_search,
)
from .ssrf import (
    DEFAULT_ALLOWED_PORTS,
    DnsResolver,
    PinnedPublicTransport,
    get_default_ssl_context,
)

log = logging.getLogger("hakimi_plugin_web_tools")

SEARCH_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["query"],
    "properties": {
        "query": {
            "type": "string",
            "minLength": 2,
            "maxLength": 500,
            "description": "公开检索词",
        },
        "provider": {
            "type": "string",
            "enum": ["auto", "deepseek", "searxng", "tavily", "brave", "duckduckgo"],
            "default": "auto",
            "description": "检索源策略",
        },
        "max_results": {
            "type": "integer",
            "minimum": 1,
            "maximum": 10,
            "default": 5,
            "description": "最多返回来源条数",
        },
        "timeout_seconds": {
            "type": "number",
            "minimum": SEARCH_MIN_TIMEOUT_SECONDS,
            "maximum": SEARCH_MAX_TIMEOUT_SECONDS,
            "default": SEARCH_DEFAULT_TIMEOUT_SECONDS,
            "description": "检索超时预算（秒）",
        },
    },
}

PREVIEW_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [],
    "properties": {
        "urls": {
            "type": "array",
            "description": "待预览的公开链接列表（最多 2 条）",
        },
        "text": {
            "type": "string",
            "minLength": 0,
            "maxLength": 4000,
            "description": "含链接的原始文本",
        },
        "budget_seconds": {
            "type": "number",
            "minimum": PREVIEW_MIN_BUDGET_SECONDS,
            "maximum": PREVIEW_MAX_BUDGET_SECONDS,
            "default": PREVIEW_DEFAULT_BUDGET_SECONDS,
            "description": "链接预览总超时预算（秒）",
        },
    },
}


class SearchActionArgs(BaseModel):
    """Strict input validation for `web_tools.search`."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    query: str = Field(min_length=2, max_length=500)
    provider: ProviderChoice = "auto"
    max_results: int = Field(default=5, ge=1, le=10)
    timeout_seconds: float = Field(
        default=SEARCH_DEFAULT_TIMEOUT_SECONDS,
        ge=SEARCH_MIN_TIMEOUT_SECONDS,
        le=SEARCH_MAX_TIMEOUT_SECONDS,
    )

    @field_validator("max_results", "timeout_seconds", mode="before")
    @classmethod
    def _reject_bool_numbers(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("boolean not allowed for numeric field")
        return value


class PreviewActionArgs(BaseModel):
    """Strict input validation for `web_tools.preview`."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    urls: list[str] | None = None
    text: str | None = Field(default=None, max_length=4000)
    budget_seconds: float = Field(
        default=PREVIEW_DEFAULT_BUDGET_SECONDS,
        ge=PREVIEW_MIN_BUDGET_SECONDS,
        le=PREVIEW_MAX_BUDGET_SECONDS,
    )

    @field_validator("budget_seconds", mode="before")
    @classmethod
    def _reject_bool_budget(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("boolean not allowed for budget_seconds")
        if isinstance(value, int | float) and not math.isfinite(float(value)):
            raise ValueError("budget_seconds must be finite")
        return value

    @field_validator("urls")
    @classmethod
    def _validate_urls(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if len(value) > PREVIEW_MAX_URLS:
            raise ValueError("too many urls")
        cleaned: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip() or len(item) > 2048:
                raise ValueError("invalid url item")
            cleaned.append(item.strip())
        return cleaned


class WebToolsPlugin:
    """Out-of-process remote plugin serving `web_tools.search` and `web_tools.preview`."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        search_config: SearchProviderConfig | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        dns_resolver: DnsResolver | None = None,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
    ) -> None:
        self.search_config = (
            search_config if search_config is not None else SearchProviderConfig.from_env()
        )
        self._owns_transports = transport is None
        if transport is not None:
            self._transport: httpx.AsyncBaseTransport = transport
            self._search_transport: httpx.AsyncBaseTransport = transport
            self._preview_transport: httpx.AsyncBaseTransport = transport
        else:
            ssl_ctx = get_default_ssl_context()
            self._search_transport = PinnedPublicTransport(
                dns_resolver=dns_resolver,
                allowed_ports=frozenset({443}),
                max_connections=8,
                ssl_context=ssl_ctx,
            )
            self._preview_transport = PinnedPublicTransport(
                dns_resolver=dns_resolver,
                allowed_ports=DEFAULT_ALLOWED_PORTS,
                max_connections=8,
                ssl_context=ssl_ctx,
            )
            self._transport = self._preview_transport
        self._dns_resolver = dns_resolver

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id="web_tools",
            token=token,
            display_name="网页检索与链接预览",
            description="隔离进程外公网检索（多检索源策略切换）与安全链接标题预览",
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
                    name="web_tools.search",
                    description="隔离进程外执行公网检索（支持多检索源策略切换与 SSRF 拦截）",
                    arguments=SEARCH_ACTION_SCHEMA,
                    origins=["admin"],
                    timeout_seconds=35.0,
                    handler=self._handle_search_action,
                ),
                ActionSpec(
                    name="web_tools.preview",
                    description="隔离进程外执行公开链接标题与摘要预览（逐跳 SSRF 与字节上限防护）",
                    arguments=PREVIEW_ACTION_SCHEMA,
                    origins=["admin"],
                    timeout_seconds=8.0,
                    handler=self._handle_preview_action,
                ),
            ),
            initial_status=self.status_payload(),
            connect_factory=connect_factory,
            max_blob_bytes=0,
        )

    def status_payload(self) -> dict[str, Any]:
        return {
            "search": self.search_config.public_summary(),
            "preview": {
                "max_urls": PREVIEW_MAX_URLS,
                "max_budget_seconds": PREVIEW_MAX_BUDGET_SECONDS,
            },
        }

    async def close(self) -> None:
        await self.client.stop()
        if self._owns_transports:
            await self._search_transport.aclose()
            await self._preview_transport.aclose()

    async def _handle_search_action(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("search_invalid_output", "invalid_arguments")
        try:
            args = SearchActionArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("search_invalid_output", "invalid_arguments") from None

        try:
            return await execute_search(
                args.query,
                self.search_config,
                provider=args.provider,
                max_results=args.max_results,
                timeout_seconds=args.timeout_seconds,
                transport=self._search_transport,
                dns_resolver=self._dns_resolver,
            )
        except SearchError as exc:
            raise PluginRpcError(exc.code, exc.code) from None
        except Exception as exc:
            log.warning("event=web_tools_search_unexpected type=%s", type(exc).__name__)
            raise PluginRpcError("web_search_failed", "web_search_failed") from None

    async def _handle_preview_action(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        if not isinstance(raw_args, dict):
            raise PluginRpcError("preview_failed", "invalid_arguments")
        try:
            args = PreviewActionArgs.model_validate(raw_args)
        except ValidationError:
            raise PluginRpcError("preview_failed", "invalid_arguments") from None

        if args.urls is not None:
            target_urls = list(args.urls)
        elif args.text:
            target_urls = extract_urls(args.text, limit=PREVIEW_MAX_URLS)
        else:
            target_urls = []

        if not target_urls:
            return {"previews": []}

        try:
            outcome = await preview_urls(
                target_urls,
                budget_seconds=args.budget_seconds,
                transport=self._preview_transport,
                dns_resolver=self._dns_resolver,
            )
        except Exception as exc:
            log.warning("event=web_tools_preview_unexpected type=%s", type(exc).__name__)
            raise PluginRpcError("preview_failed", "preview_failed") from None

        if outcome.previews:
            return {"previews": outcome.previews}
        if outcome.total_urls > 0 and outcome.ssrf_blocked_count == outcome.total_urls:
            raise PluginRpcError("ssrf_blocked", "ssrf_blocked")
        if outcome.total_urls > 0 and outcome.oversized_count == outcome.total_urls:
            raise PluginRpcError("preview_failed", "preview_failed")
        return {"previews": []}


def build_web_tools_plugin_from_env(**overrides: Any) -> WebToolsPlugin:
    """Create a WebToolsPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_WEB_TOOLS_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_WEB_TOOLS_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "search_config": SearchProviderConfig.from_env(),
    }
    kwargs.update(overrides)
    return WebToolsPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the web_tools remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    plugin = build_web_tools_plugin_from_env()
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
