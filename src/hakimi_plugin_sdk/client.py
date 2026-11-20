"""Standalone reverse WebSocket runtime client for Hakimi remote plugins."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator
from websockets.asyncio.client import connect as ws_connect

log = logging.getLogger("hakimi_plugin_sdk")

_PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}\Z")
_ACTION_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,30}\.[a-z][a-z0-9_]{0,30}\Z")
_CODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}\Z")
_VALID_WINDOWS = frozenset({"any", "active", "closing"})
_DEFAULT_MAX_FRAME_BYTES = 262_144

ThinkingLevel = Literal["low", "medium", "high", "xhigh"]


class PluginRpcError(Exception):
    """Constant-code RPC error propagated across the plugin bus."""

    def __init__(self, code: str, message: str = "") -> None:
        clean_code = str(code or "").strip() or "plugin_rpc_failed"
        self.code = clean_code
        self.message = str(message) if message else clean_code
        super().__init__(self.message if not message else f"{self.code}: {self.message}")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate_key")
        obj[key] = value
    return obj


def _reject_constant(token: str) -> Any:
    raise ValueError(f"invalid_json_constant:{token}")


def decode_ws_json_frame(
    raw: str | bytes, max_bytes: int = _DEFAULT_MAX_FRAME_BYTES
) -> dict[str, Any]:
    """Decode one bounded JSON object frame while rejecting duplicate keys or NaN/Infinity."""
    if isinstance(raw, bytes):
        if len(raw) > max_bytes:
            raise PluginRpcError("plugin_frame_too_large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise PluginRpcError("plugin_invalid_json") from None
    elif isinstance(raw, str):
        if len(raw.encode("utf-8")) > max_bytes:
            raise PluginRpcError("plugin_frame_too_large")
        text = raw
    else:
        raise PluginRpcError("plugin_invalid_json")

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (ValueError, TypeError, RecursionError):
        raise PluginRpcError("plugin_invalid_json") from None
    if not isinstance(parsed, dict):
        raise PluginRpcError("plugin_invalid_json")
    return parsed


def compute_backoff_step(
    delay: float,
    *,
    max_delay: float,
    min_delay: float = 0.01,
    rng: random.Random | None = None,
) -> tuple[float, float]:
    """Return `(sleep_seconds, next_delay)` for exponential backoff with half-full jitter."""
    ceiling = max(float(max_delay), float(min_delay))
    bounded = min(max(float(delay), float(min_delay)), ceiling)
    sampler = rng if rng is not None else random
    sleep_for = sampler.uniform(bounded / 2.0, bounded)
    next_delay = min(bounded * 2.0, ceiling)
    return sleep_for, next_delay


def _extract_close_code(exc: BaseException | None, ws: Any = None) -> int | None:
    if exc is not None:
        rcvd = getattr(exc, "rcvd", None)
        rcvd_code = getattr(rcvd, "code", None)
        if isinstance(rcvd_code, int):
            return rcvd_code
        exc_code = getattr(exc, "code", None)
        if isinstance(exc_code, int):
            return exc_code
        resp = getattr(exc, "response", None)
        status_code = getattr(resp, "status_code", None)
        if status_code == 403:
            return 1008
    if ws is not None:
        ws_code = getattr(ws, "close_code", None)
        if isinstance(ws_code, int):
            return ws_code
        rcvd = getattr(ws, "close_rcvd", None)
        rcvd_code = getattr(rcvd, "code", None)
        if isinstance(rcvd_code, int):
            return rcvd_code
    return None


async def _invoke_callback(fn: Callable[..., Any], *args: Any) -> Any:
    sig = inspect.signature(fn)
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    if has_varargs:
        res = fn(*args)
    else:
        res = fn(*args[: len(positional)])
    if inspect.isawaitable(res):
        return await res
    return res


class PluginRuntimeConfig(BaseModel):
    """Runtime connection, identity, capability, and timing configuration for a remote plugin."""

    ws_url: str
    plugin_id: str
    token: str = Field(repr=False, min_length=1)
    display_name: str = Field(min_length=1, max_length=80)
    description: str = ""
    scopes: tuple[str, ...] = ()
    node_region: Literal["KR", "CN"] = "KR"
    admin_panel: str | None = None
    attention_level: Literal["LOW", "MEDIUM", "HIGH"] = "LOW"
    capabilities: tuple[str, ...] = ()
    event_subscriptions: tuple[str, ...] = ()
    chat_platforms: tuple[str, ...] = ()
    reconnect_min: float = Field(default=1.0, gt=0.0)
    reconnect_max: float = Field(default=30.0, gt=0.0)
    heartbeat_seconds: float = Field(default=20.0, gt=0.0)
    rpc_timeout: float = Field(default=15.0, gt=0.0)

    @field_validator("ws_url")
    @classmethod
    def _validate_ws_url(cls, value: str) -> str:
        parsed = urlsplit(value.strip())
        if parsed.scheme not in ("ws", "wss") or not parsed.netloc:
            raise ValueError("ws_url must be a valid ws:// or wss:// URL")
        if parsed.query:
            raise ValueError("ws_url must not contain query parameters")
        return value.strip()

    @field_validator("plugin_id")
    @classmethod
    def _validate_plugin_id(cls, value: str) -> str:
        if not _PLUGIN_ID_RE.fullmatch(value):
            raise ValueError("invalid plugin_id")
        return value


class ThinkingSceneSpec(BaseModel):
    """Remote thinking scene declaration sent in `plugin.hello`."""

    key: str = Field(min_length=2, max_length=64)
    label: str = Field(min_length=1, max_length=80)
    default: ThinkingLevel = "medium"
    levels: tuple[ThinkingLevel, ...] = ("low", "medium", "high", "xhigh")
    output_tokens: int = Field(default=2048, ge=128, le=16384)

    @field_validator("levels")
    @classmethod
    def _validate_levels(cls, value: tuple[ThinkingLevel, ...]) -> tuple[ThinkingLevel, ...]:
        if not value or len(value) > 4 or len(set(value)) != len(value):
            raise ValueError("invalid thinking levels")
        return value

    def model_post_init(self, __context: Any) -> None:
        if self.default not in self.levels:
            raise ValueError("default level must be present in levels")

    def to_hello_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "default": self.default,
            "levels": list(self.levels),
            "output_tokens": self.output_tokens,
        }


class ActionSpec(BaseModel):
    """Remote action declaration and handler bound to a HakimiPluginClient.

    The `handler` receives the full `action.run` params dict
    (`{"action", "arguments", "origin", "target_scope", "now"}`).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    description: str = Field(min_length=1, max_length=240)
    arguments: dict[str, Any] = Field(default_factory=dict)
    origins: list[str] = Field(default_factory=lambda: ["planner"])
    effortful: bool = False
    interrupts_focus: bool = False
    window: str = "any"
    social: bool = False
    owner_only: bool = False
    aliases: list[list[Any]] = Field(default_factory=list)
    handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if not _ACTION_NAME_RE.fullmatch(value):
            raise ValueError("invalid action name")
        return value

    @field_validator("window")
    @classmethod
    def _validate_window(cls, value: str) -> str:
        if value not in _VALID_WINDOWS:
            raise ValueError("invalid action window")
        return value

    def to_hello_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "arguments": self.arguments,
            "origins": list(self.origins),
            "effortful": self.effortful,
            "interrupts_focus": self.interrupts_focus,
            "window": self.window,
            "social": self.social,
            "owner_only": self.owner_only,
            "aliases": [list(alias) for alias in self.aliases],
        }


class HakimiPluginClient:
    """Persistent reverse-WebSocket client for an out-of-process Hakimi plugin."""

    def __init__(
        self,
        config: PluginRuntimeConfig,
        *,
        actions: Sequence[ActionSpec] = (),
        thinking_scenes: Sequence[ThinkingSceneSpec] = (),
        chat_platforms: Sequence[str] = (),
        initial_status: dict[str, Any] | None = None,
        configuration_issue: str | None = None,
        connect_factory: Callable[..., Any] | None = None,
        max_frame_bytes: int = _DEFAULT_MAX_FRAME_BYTES,
        handshake_timeout: float = 15.0,
        idle_timeout: float | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config
        self._connect_factory = connect_factory
        self.max_frame_bytes = max(16_384, int(max_frame_bytes))
        self.handshake_timeout = max(0.05, float(handshake_timeout))
        self.idle_timeout = (
            float(idle_timeout)
            if idle_timeout is not None
            else max(float(config.heartbeat_seconds) * 3.0, 60.0)
        )
        self._rng = rng

        self._actions: dict[str, ActionSpec] = {}
        for spec in actions:
            self.register_action(spec)
        self._thinking_scenes: dict[str, ThinkingSceneSpec] = {}
        for scene in thinking_scenes:
            self.register_thinking_scene(scene)
        self._chat_platforms: list[str] = list(
            dict.fromkeys([*config.chat_platforms, *chat_platforms])
        )
        self._last_status: dict[str, Any] = dict(initial_status or {})
        self._configuration_issue: str | None = configuration_issue

        self._sensor_handler: Callable[..., Any] | None = None
        self._rpc_handlers: dict[str, Callable[..., Any]] = {}
        self._on_connect_handlers: list[Callable[..., Any]] = []
        self._on_world_event_handlers: list[Callable[..., Any]] = []
        self._on_switch_handlers: list[Callable[..., Any]] = []
        self._on_node_event_handlers: list[Callable[..., Any]] = []
        self._on_node_state_handlers: list[Callable[..., Any]] = []

        self._ws: Any = None
        self._connecting_ws: Any = None
        self._connected: bool = False
        self._connected_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._runner_task: asyncio.Task[None] | None = None
        self._rpc_tasks: set[asyncio.Task[None]] = set()
        self._pending_kernel_rpcs: dict[str, asyncio.Future[Any]] = {}
        self.welcome_payload: dict[str, Any] | None = None
        self.backoff_history: list[float] = []

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    @property
    def actions(self) -> dict[str, ActionSpec]:
        return dict(self._actions)

    @property
    def thinking_scenes(self) -> dict[str, ThinkingSceneSpec]:
        return dict(self._thinking_scenes)

    def register_action(self, spec: ActionSpec) -> ActionSpec:
        self._actions[spec.name] = spec
        return spec

    def action(
        self,
        name: str,
        description: str,
        *,
        arguments: dict[str, Any] | None = None,
        origins: Sequence[str] = ("planner",),
        effortful: bool = False,
        interrupts_focus: bool = False,
        window: str = "any",
        social: bool = False,
        owner_only: bool = False,
        aliases: Sequence[Sequence[Any]] = (),
    ) -> Callable[[Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]], Callable[..., Any]]:
        """Decorator registering an ActionSpec handler."""

        def _decorator(fn: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]) -> Any:
            self.register_action(
                ActionSpec(
                    name=name,
                    description=description,
                    arguments=dict(arguments or {}),
                    origins=list(origins),
                    effortful=effortful,
                    interrupts_focus=interrupts_focus,
                    window=window,
                    social=social,
                    owner_only=owner_only,
                    aliases=[list(item) for item in aliases],
                    handler=fn,
                )
            )
            return fn

        return _decorator

    def register_thinking_scene(self, spec: ThinkingSceneSpec) -> ThinkingSceneSpec:
        self._thinking_scenes[spec.key] = spec
        return spec

    def sensor(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering the `sensor.read` handler."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._sensor_handler = target
            return target

        return _decorator(fn) if fn is not None else _decorator

    def rpc(self, method: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator registering a custom `rpc.call` method handler."""

        def _decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.register_rpc(method, fn)
            return fn

        return _decorator

    def register_rpc(self, method: str, handler: Callable[..., Any]) -> None:
        if not method or not isinstance(method, str):
            raise ValueError("rpc method must be a non-empty string")
        self._rpc_handlers[method] = handler

    def on_connect(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering a callback executed after `plugin.welcome`."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._on_connect_handlers.append(target)
            return target

        return _decorator(fn) if fn is not None else _decorator

    def on_world_event(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering a handler for `world.event.broadcast`."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._on_world_event_handlers.append(target)
            return target

        return _decorator(fn) if fn is not None else _decorator

    def on_switch(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering a handler for `module.switch` (`enabled: bool`)."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._on_switch_handlers.append(target)
            return target

        return _decorator(fn) if fn is not None else _decorator

    def on_node_event(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering a handler for `node.event` (`frame: dict[str, Any]`)."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._on_node_event_handlers.append(target)
            return target

        return _decorator(fn) if fn is not None else _decorator

    def on_node_state(self, fn: Callable[..., Any] | None = None) -> Any:
        """Decorator registering a handler for `node.state` (`connected: bool`)."""

        def _decorator(target: Callable[..., Any]) -> Callable[..., Any]:
            self._on_node_state_handlers.append(target)
            return target

        return _decorator(fn) if fn is not None else _decorator

    def set_status_cache(
        self, status: dict[str, Any], *, configuration_issue: str | None = None
    ) -> None:
        """Update cached initial status and configuration issue sent on next handshake."""
        self._last_status = dict(status)
        self._configuration_issue = configuration_issue

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.token}",
            "X-Plugin-Id": self.config.plugin_id,
        }

    def hello_payload(self) -> dict[str, Any]:
        return {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": self.config.display_name,
            "description": self.config.description,
            "scopes": list(self.config.scopes),
            "actions": [spec.to_hello_dict() for spec in self._actions.values()],
            "thinking_scenes": [spec.to_hello_dict() for spec in self._thinking_scenes.values()],
            "chat_platforms": list(self._chat_platforms),
            "capabilities": list(self.config.capabilities),
            "admin_panel": self.config.admin_panel,
            "attention_level": self.config.attention_level,
            "configuration_issue": self._configuration_issue,
            "initial_status": dict(self._last_status),
            "event_subscriptions": list(self.config.event_subscriptions),
        }

    async def _ws_send(self, ws: Any, frame: dict[str, Any]) -> None:
        raw = json.dumps(frame, ensure_ascii=False, allow_nan=False)
        if len(raw.encode("utf-8")) > self.max_frame_bytes:
            raise PluginRpcError("plugin_frame_too_large")
        async with self._send_lock:
            if hasattr(ws, "send"):
                await ws.send(raw)
            elif hasattr(ws, "send_text"):
                await ws.send_text(raw)
            else:
                raise AttributeError("WebSocket object must implement send() or send_text()")

    async def _ws_recv(self, ws: Any) -> str | bytes:
        if hasattr(ws, "recv"):
            return await ws.recv()
        if hasattr(ws, "receive"):
            return await ws.receive()
        if hasattr(ws, "receive_text"):
            return await ws.receive_text()
        raise AttributeError("WebSocket object must implement recv() or receive()")

    async def _ws_close(self, ws: Any, code: int = 1000) -> None:
        close_fn = getattr(ws, "close", None)
        if close_fn is None:
            return
        with contextlib.suppress(Exception):
            sig = inspect.signature(close_fn)
            if "code" in sig.parameters or len(sig.parameters) >= 1:
                res = close_fn(code)
            else:
                res = close_fn()
            if inspect.isawaitable(res):
                await res

    async def _send_live_frame(self, frame: dict[str, Any]) -> None:
        ws = self._ws
        if not self._connected or ws is None:
            raise PluginRpcError("plugin_disconnected", "not connected")
        try:
            await self._ws_send(ws, frame)
        except PluginRpcError:
            raise
        except Exception:
            raise PluginRpcError("plugin_disconnected", "send failed") from None

    async def _kernel_request(
        self,
        frame: dict[str, Any],
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> Any:
        ws = self._ws
        if not self._connected or ws is None:
            raise PluginRpcError("plugin_disconnected", "not connected")

        req_id = uuid.uuid4().hex
        outbound = dict(frame)
        outbound["id"] = req_id
        effective_timeout = float(self.config.rpc_timeout if timeout is None else timeout)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_kernel_rpcs[req_id] = future
        try:
            await self._send_live_frame(outbound)
            async with asyncio.timeout(effective_timeout):
                return await future
        except TimeoutError:
            raise PluginRpcError(
                "plugin_rpc_timeout", f"timed out waiting for {frame.get(type)}"
            ) from None
        finally:
            self._pending_kernel_rpcs.pop(req_id, None)

    async def state_get(self, key: str) -> Any:
        """Read plugin-scoped KV state from the kernel."""
        res = await self._kernel_request({"type": "state.get", "key": key})
        if isinstance(res, dict):
            if res.get("found") is False:
                return None
            return res.get("value")
        return res

    async def state_set(self, key: str, value: Any) -> None:
        """Persist plugin-scoped KV state in the kernel."""
        await self._kernel_request({"type": "state.set", "key": key, "value": value})

    async def state_delete(self, key: str) -> None:
        """Delete a plugin-scoped KV key in the kernel."""
        await self._kernel_request({"type": "state.delete", "key": key})

    async def node_call(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        """Invoke a CN node bridge action through the kernel (`node_bridge` scope)."""
        res = await self._kernel_request(
            {"type": "node.call", "action": action, "params": dict(params or {})},
            timeout=timeout,
        )
        if res is None:
            return {}
        if isinstance(res, dict):
            return res
        return {"result": res}

    async def push_status(
        self, status: dict[str, Any], *, configuration_issue: str | None = None
    ) -> None:
        """Push updated admin/runtime status and optional configuration issue."""
        self.set_status_cache(status, configuration_issue=configuration_issue)
        await self._send_live_frame(
            {
                "type": "plugin.status",
                "status": dict(status),
                "configuration_issue": configuration_issue,
            }
        )

    async def emit_world_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        importance: str = "normal",
        business_key: str | None = None,
        summary: str | None = None,
    ) -> None:
        """Emit a one-way `world.event` frame (`sensors` scope)."""
        frame: dict[str, Any] = {
            "type": "world.event",
            "event_type": event_type,
            "payload": dict(payload),
            "importance": importance,
        }
        if business_key is not None:
            frame["business_key"] = business_key
        if summary is not None:
            frame["summary"] = summary
        await self._send_live_frame(frame)

    async def report_activity(
        self,
        active_task: str,
        *,
        disconnected: bool = False,
        updated_at: float | None = None,
    ) -> None:
        """Report current world activity snapshot (`sensors` scope)."""
        await self._send_live_frame(
            {
                "type": "world.activity",
                "active_task": active_task,
                "disconnected": bool(disconnected),
                "updated_at": time.time() if updated_at is None else float(updated_at),
            }
        )

    async def enqueue_reaction(
        self,
        action_name: str,
        note: str,
        *,
        ttl: int = 7200,
        arguments: dict[str, Any] | None = None,
        legacy_module: str = "",
        legacy_verb: str = "",
        source_event_id: str = "",
    ) -> None:
        """Enqueue a reaction intent in the kernel (`actions` + `sensors` scopes)."""
        await self._send_live_frame(
            {
                "type": "world.reaction",
                "action_name": action_name,
                "note": note,
                "ttl": int(ttl),
                "arguments": dict(arguments or {}),
                "legacy_module": legacy_module,
                "legacy_verb": legacy_verb,
                "source_event_id": source_event_id,
            }
        )

    def _fail_pending_kernel_rpcs(self, code: str = "plugin_disconnected") -> None:
        for future in list(self._pending_kernel_rpcs.values()):
            if not future.done():
                future.set_exception(PluginRpcError(code))
        self._pending_kernel_rpcs.clear()

    async def _send_rpc_result(
        self,
        ws: Any,
        req_id: str,
        *,
        ok: bool,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        frame: dict[str, Any]
        if ok:
            frame = {"type": "rpc.result", "id": req_id, "ok": True, "result": result}
        else:
            code = str(error or "plugin_rpc_failed")
            if not _CODE_RE.fullmatch(code):
                code = "plugin_rpc_failed"
            frame = {"type": "rpc.result", "id": req_id, "ok": False, "error": code}
        try:
            await self._ws_send(ws, frame)
        except PluginRpcError as exc:
            if exc.code == "plugin_frame_too_large" and ok:
                with contextlib.suppress(Exception):
                    await self._ws_send(
                        ws,
                        {
                            "type": "rpc.result",
                            "id": req_id,
                            "ok": False,
                            "error": "plugin_frame_too_large",
                        },
                    )
        except Exception:
            pass

    async def _handle_rpc_call(self, ws: Any, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        method = frame.get("method")
        raw_params = frame.get("params")
        if not isinstance(req_id, str) or not req_id:
            return
        if not isinstance(method, str) or not method:
            await self._send_rpc_result(ws, req_id, ok=False, error="plugin_invalid_schema")
            return
        if raw_params is None:
            params: dict[str, Any] = {}
        elif isinstance(raw_params, dict):
            params = dict(raw_params)
        else:
            await self._send_rpc_result(ws, req_id, ok=False, error="plugin_invalid_schema")
            return

        try:
            if method == "action.run":
                action_name = params.get("action")
                if not isinstance(action_name, str) or action_name not in self._actions:
                    await self._send_rpc_result(ws, req_id, ok=False, error="action_not_found")
                    return
                spec = self._actions[action_name]
                if not isinstance(params.get("arguments"), dict):
                    params["arguments"] = {}
                result = await _invoke_callback(spec.handler, params)
                await self._send_rpc_result(
                    ws, req_id, ok=True, result={} if result is None else result
                )
                return

            if method == "sensor.read":
                if self._sensor_handler is None:
                    await self._send_rpc_result(ws, req_id, ok=False, error="sensor_not_configured")
                    return
                raw_sensor = await _invoke_callback(self._sensor_handler, params)
                if isinstance(raw_sensor, dict):
                    sensor_res = raw_sensor
                elif raw_sensor is None:
                    sensor_res = {"summary": ""}
                else:
                    sensor_res = {"summary": str(raw_sensor)}
                await self._send_rpc_result(ws, req_id, ok=True, result=sensor_res)
                return

            handler = self._rpc_handlers.get(method)
            if handler is None:
                await self._send_rpc_result(ws, req_id, ok=False, error="rpc_method_not_found")
                return
            result = await _invoke_callback(handler, params)
            await self._send_rpc_result(
                ws, req_id, ok=True, result={} if result is None else result
            )
        except asyncio.CancelledError:
            raise
        except PluginRpcError as exc:
            await self._send_rpc_result(ws, req_id, ok=False, error=exc.code)
        except Exception as exc:
            log.warning(
                "event=plugin_rpc_handler_error plugin=%s method=%s err=%s",
                self.config.plugin_id,
                method,
                type(exc).__name__,
            )
            await self._send_rpc_result(ws, req_id, ok=False, error="plugin_internal_error")

    async def _run_notifications(self, queue: asyncio.Queue[tuple[str, Any]]) -> None:
        while True:
            kind, payload = await queue.get()
            try:
                if kind == "on_connect":
                    for cb in self._on_connect_handlers:
                        await _invoke_callback(cb, payload)
                elif kind == "world_event":
                    for cb in self._on_world_event_handlers:
                        await _invoke_callback(cb, payload)
                elif kind == "switch":
                    for cb in self._on_switch_handlers:
                        await _invoke_callback(cb, payload)
                elif kind == "node_event":
                    for cb in self._on_node_event_handlers:
                        await _invoke_callback(cb, payload)
                elif kind == "node_state":
                    for cb in self._on_node_state_handlers:
                        await _invoke_callback(cb, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "event=plugin_notification_error plugin=%s kind=%s err=%s",
                    self.config.plugin_id,
                    kind,
                    type(exc).__name__,
                )

    async def _serve_session(self, ws: Any) -> None:
        await self._ws_send(ws, self.hello_payload())

        loop = asyncio.get_running_loop()
        welcome_frame: dict[str, Any]
        async with asyncio.timeout(self.handshake_timeout):
            while True:
                raw = await self._ws_recv(ws)
                frame = decode_ws_json_frame(raw, self.max_frame_bytes)
                ftype = frame.get("type")
                if ftype == "ping":
                    pong: dict[str, Any] = {"type": "pong", "ts": frame.get("ts")}
                    if isinstance(frame.get("id"), str):
                        pong["id"] = frame["id"]
                    await self._ws_send(ws, pong)
                    continue
                if ftype == "error":
                    raise PluginRpcError(str(frame.get("reason") or "plugin_handshake_rejected"))
                if ftype in {"plugin.welcome", "plugin.ready"}:
                    welcome_frame = frame
                    break
                raise PluginRpcError("plugin_invalid_handshake")

        self.welcome_payload = welcome_frame
        self._ws = ws
        self._connected = True
        self._connected_event.set()
        last_rx = loop.time()

        notif_queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        notif_queue.put_nowait(("on_connect", welcome_frame))

        async def _reader() -> None:
            nonlocal last_rx
            while not self._stop_event.is_set():
                remaining = max(0.01, self.idle_timeout - (loop.time() - last_rx))
                try:
                    async with asyncio.timeout(remaining):
                        raw = await self._ws_recv(ws)
                except TimeoutError:
                    if (loop.time() - last_rx) >= self.idle_timeout:
                        raise PluginRpcError("plugin_idle_timeout") from None
                    continue

                last_rx = loop.time()
                frame = decode_ws_json_frame(raw, self.max_frame_bytes)
                ftype = frame.get("type")

                if ftype == "ping":
                    pong_frame: dict[str, Any] = {"type": "pong", "ts": frame.get("ts")}
                    if isinstance(frame.get("id"), str):
                        pong_frame["id"] = frame["id"]
                    await self._ws_send(ws, pong_frame)
                elif ftype == "pong":
                    continue
                elif ftype in {"rpc.call", "rpc.request"}:
                    task = asyncio.create_task(self._handle_rpc_call(ws, frame))
                    self._rpc_tasks.add(task)
                    task.add_done_callback(self._rpc_tasks.discard)
                elif ftype == "kernel.result":
                    req_id = frame.get("id")
                    fut = self._pending_kernel_rpcs.get(req_id) if isinstance(req_id, str) else None
                    if fut is not None and not fut.done():
                        if frame.get("ok", False):
                            fut.set_result(frame.get("result"))
                        else:
                            fut.set_exception(
                                PluginRpcError(
                                    str(frame.get("error") or "kernel_rpc_failed"),
                                    str(frame.get("message") or ""),
                                )
                            )
                elif ftype == "world.event.broadcast":
                    raw_ev = frame.get("event")
                    ev = (
                        raw_ev
                        if isinstance(raw_ev, dict)
                        else {k: v for k, v in frame.items() if k != "type"}
                    )
                    notif_queue.put_nowait(("world_event", ev))
                elif ftype == "module.switch":
                    notif_queue.put_nowait(("switch", bool(frame.get("enabled", True))))
                elif ftype == "node.event":
                    raw_inner = frame.get("frame")
                    inner = (
                        raw_inner
                        if isinstance(raw_inner, dict)
                        else {k: v for k, v in frame.items() if k != "type"}
                    )
                    notif_queue.put_nowait(("node_event", inner))
                elif ftype == "node.state":
                    notif_queue.put_nowait(("node_state", bool(frame.get("connected", False))))
                elif ftype in {
                    "plugin.welcome",
                    "plugin.ready",
                    "world.event.ack",
                    "chat.inbound.ack",
                    "owner.notify.ack",
                }:
                    continue
                elif ftype == "error":
                    log.warning(
                        "event=plugin_server_error plugin=%s reason=%s",
                        self.config.plugin_id,
                        frame.get("reason"),
                    )

        async def _heartbeat() -> None:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.config.heartbeat_seconds
                    )
                    return
                except TimeoutError:
                    pass
                await self._ws_send(ws, {"type": "ping", "ts": time.time()})

        notif_task = asyncio.create_task(self._run_notifications(notif_queue))
        reader_task = asyncio.create_task(_reader())
        heartbeat_task = asyncio.create_task(_heartbeat())
        try:
            done, _ = await asyncio.wait(
                [reader_task, heartbeat_task, notif_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in done:
                if not t.cancelled():
                    t.result()
        finally:
            self._connected = False
            self._connected_event.clear()
            self._ws = None
            self._fail_pending_kernel_rpcs("plugin_disconnected")
            for task in (reader_task, heartbeat_task, notif_task, *list(self._rpc_tasks)):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                reader_task,
                heartbeat_task,
                notif_task,
                *list(self._rpc_tasks),
                return_exceptions=True,
            )
            self._rpc_tasks.clear()

    def _open_connection(self) -> Any:
        headers = self._auth_headers()
        if self._connect_factory is None:
            return ws_connect(
                self.config.ws_url,
                additional_headers=headers,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=self.handshake_timeout,
                close_timeout=3.0,
                max_size=self.max_frame_bytes,
                max_queue=16,
                compression=None,
                proxy=None,
            )

        sig = inspect.signature(self._connect_factory)
        params = sig.parameters
        has_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        positional = [
            p
            for p in params.values()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if "headers" in params:
            return self._connect_factory(self.config.ws_url, headers=headers)
        if "additional_headers" in params or has_kwargs:
            return self._connect_factory(self.config.ws_url, additional_headers=headers)
        if len(positional) >= 2 or any(
            p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values()
        ):
            return self._connect_factory(self.config.ws_url, headers)
        return self._connect_factory(self.config.ws_url)

    async def run_forever(self) -> None:
        """Connect and maintain the reverse-WS session with bounded exponential backoff."""
        self._stop_event.clear()
        min_delay = min(self.config.reconnect_min, self.config.reconnect_max)
        max_delay = max(self.config.reconnect_min, self.config.reconnect_max)
        delay = min_delay

        while not self._stop_event.is_set():
            connected_at = asyncio.get_running_loop().time()
            close_code: int | None = None
            ws_ref: Any = None
            try:
                conn = self._open_connection()
                if hasattr(conn, "__aenter__"):
                    async with conn as ws:
                        ws_ref = ws
                        self._connecting_ws = ws
                        try:
                            await self._serve_session(ws)
                        finally:
                            self._connecting_ws = None
                else:
                    ws = await conn if inspect.isawaitable(conn) else conn
                    ws_ref = ws
                    self._connecting_ws = ws
                    try:
                        await self._serve_session(ws)
                    finally:
                        self._connecting_ws = None
                        await self._ws_close(ws, 1000)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                close_code = _extract_close_code(exc, ws_ref)
                if close_code == 1008:
                    log.warning(
                        "event=plugin_ws_rejected plugin=%s code=1008",
                        self.config.plugin_id,
                    )
                else:
                    log.info(
                        "event=plugin_ws_reconnect plugin=%s err=%s code=%s",
                        self.config.plugin_id,
                        type(exc).__name__,
                        close_code,
                    )
            else:
                close_code = _extract_close_code(None, ws_ref)
                if close_code == 1008:
                    log.warning(
                        "event=plugin_ws_rejected plugin=%s code=1008",
                        self.config.plugin_id,
                    )

            if self._stop_event.is_set():
                break

            if (asyncio.get_running_loop().time() - connected_at) > max(60.0, max_delay * 2.0):
                delay = min_delay

            sleep_for, next_delay = compute_backoff_step(
                delay,
                max_delay=max_delay,
                min_delay=min_delay,
                rng=self._rng,
            )
            self.backoff_history.append(sleep_for)
            if len(self.backoff_history) > 64:
                self.backoff_history.pop(0)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)
            except TimeoutError:
                pass
            delay = next_delay

    run = run_forever

    async def start(self) -> None:
        """Start the background connection loop if not already running."""
        if self._runner_task is not None and not self._runner_task.done():
            return
        self._stop_event.clear()
        self._runner_task = asyncio.create_task(
            self.run_forever(),
            name=f"hakimi-plugin:{self.config.plugin_id}",
        )

    async def wait_connected(self, timeout: float | None = None) -> bool:  # noqa: ASYNC109
        """Wait until the WebSocket handshake completes and the session is connected."""
        if self.connected:
            return True
        if timeout is None:
            await self._connected_event.wait()
            return self.connected
        try:
            await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)
            return self.connected
        except TimeoutError:
            return False

    async def stop(self) -> None:
        """Gracefully close active session and stop reconnect loop."""
        self._stop_event.set()
        self._connected = False
        self._connected_event.clear()
        self._fail_pending_kernel_rpcs("plugin_disconnected")
        ws = self._ws or self._connecting_ws
        if ws is not None:
            await self._ws_close(ws, 1000)
        task = self._runner_task
        if task is not None:
            if task is not asyncio.current_task() and not task.done():
                task.cancel()
            if task is not asyncio.current_task():
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self._runner_task = None

    async def __aenter__(self) -> HakimiPluginClient:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()
