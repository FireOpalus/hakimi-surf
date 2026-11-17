"""Reverse WebSocket trunk transport for the domestic CN collector node."""

import asyncio
import inspect
import json
import logging
import math
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import anyio

from .config import Settings
from .security import Rejected

ALLOWED_NODE_ACTIONS = frozenset(
    {
        "node.ping",
        "bili.public_get",
        "bili.resolve_short",
        "bili.request",
        "bili.cover_thumb",
        "bili.video_storyboard",
        "mc.status",
        "mc.dispatch_goal",
        "mc.pause",
        "mc.resume",
    }
)

ALLOWED_NODE_EVENTS = frozenset(
    {
        "node.hello",
        "node.heartbeat",
        "mc.snapshot",
        "mc.milestone",
    }
)

_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_DETAIL_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
_MAX_CAPABILITIES = 64
_MAX_PENDING_RPCS = 16
_MAX_EVENT_QUEUE = 32
_DEFAULT_HANDSHAKE_TIMEOUT = 10.0
_HEARTBEAT_RPC_TIMEOUT = 10.0

NodeEventHandler = Callable[[dict[str, Any]], Any]
NodeStateHandler = Callable[[bool], Any]


def decode_node_frame(raw: str | bytes, limit: int) -> dict[str, Any]:
    """Decode and strictly validate one incoming JSON object frame."""
    try:
        if isinstance(raw, str):
            size = len(raw.encode("utf-8"))
        elif isinstance(raw, (bytes, bytearray)):
            size = len(raw)
        else:
            raise Rejected("node_invalid_json")
    except UnicodeError:
        raise Rejected("node_invalid_json") from None
    if size > limit:
        raise Rejected("node_frame_too_large")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise Rejected("node_invalid_json")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise Rejected("node_invalid_json") from None


def _normalize_capabilities(raw_caps: Any) -> list[str]:
    if not isinstance(raw_caps, list):
        raise Rejected("node_invalid_frame")
    return [c for c in raw_caps if isinstance(c, str) and c and len(c) <= 64][:_MAX_CAPABILITIES]


@dataclass
class _NodePeer:
    receive: Callable[[], Awaitable[str | bytes]]
    send: Callable[[str], Awaitable[None]]
    close: Callable[[int], Awaitable[None]]
    runner: asyncio.Task[Any]
    source_ip: str
    node_id: str = ""
    region: str = "CN"
    capabilities: list[str] = field(default_factory=list)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    hello_received: asyncio.Event = field(default_factory=asyncio.Event)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    closed: bool = False


class NodeTransport:
    """Single-active-peer reverse WebSocket RPC/event trunk to the CN collector node."""

    def __init__(
        self,
        settings: Settings,
        *,
        on_event: NodeEventHandler | None = None,
        on_state: NodeStateHandler | None = None,
    ) -> None:
        self.s = settings
        self.on_event = on_event
        self.on_state = on_state
        self.ready = asyncio.Event()
        self.peer_lock = asyncio.Lock()
        self._peer: _NodePeer | None = None
        self._retired: set[asyncio.Task[Any]] = set()
        self._handshake_timeout: float = _DEFAULT_HANDSHAKE_TIMEOUT
        self.active_node_id: str | None = None
        self.capabilities: list[str] = []
        self.source_ip: str | None = None
        self.connected_at: float | None = None
        self.last_heartbeat_at: float | None = None
        self.rpc_calls: int = 0
        self.rpc_errors: int = 0

    def connected(self) -> bool:
        return self.ready.is_set()

    def status(self) -> dict[str, Any]:
        return {
            "configured": bool(self.s.node_ws_enabled),
            "online": self.ready.is_set(),
            "node_id": self.active_node_id or self.s.node_id,
            "region": "CN",
            "capabilities": list(self.capabilities),
            "source_ip": self.source_ip,
            "connected_at": self.connected_at,
            "last_heartbeat_at": self.last_heartbeat_at,
            "rpc_calls": self.rpc_calls,
            "rpc_errors": self.rpc_errors,
        }

    async def call(
        self,
        action: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        if action not in ALLOWED_NODE_ACTIONS:
            raise Rejected("node_action_forbidden")
        if not isinstance(params, dict):
            raise Rejected("node_invalid_params")
        if not self.ready.is_set():
            raise Rejected("node_disconnected")
        effective_timeout = self.s.node_ws_rpc_timeout if timeout is None else float(timeout)
        try:
            data = await self._rpc(action, params, self._peer, timeout=effective_timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.rpc_errors += 1
            raise
        self.rpc_calls += 1
        return data

    async def _rpc(
        self,
        action: str,
        params: dict[str, Any],
        peer: _NodePeer | None,
        *,
        timeout: float,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        if action not in ALLOWED_NODE_ACTIONS:
            raise Rejected("node_action_forbidden")
        if not isinstance(params, dict):
            raise Rejected("node_invalid_params")
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("timeout must be positive")
        if peer is None or peer is not self._peer:
            raise Rejected("node_not_dispatched")
        if len(peer.pending) >= _MAX_PENDING_RPCS:
            raise Rejected("node_unavailable")

        req_id = uuid.uuid4().hex
        try:
            frame = json.dumps(
                {
                    "type": "rpc_request",
                    "id": req_id,
                    "action": action,
                    "params": params,
                },
                allow_nan=False,
            )
        except (TypeError, ValueError):
            raise Rejected("node_invalid_params") from None
        if len(frame.encode("utf-8")) > self.s.node_ws_max_frame_bytes:
            raise Rejected("node_frame_too_large")

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        peer.pending[req_id] = future
        dispatched = False
        try:
            async with asyncio.timeout(timeout):
                async with peer.send_lock:
                    if peer is not self._peer:
                        raise Rejected("node_not_dispatched")
                    dispatched = True
                    await peer.send(frame)
                resp = await future
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise Rejected("node_rpc_timeout") from None
        except Rejected:
            raise
        except Exception:
            raise Rejected("node_disconnected" if dispatched else "node_not_dispatched") from None
        finally:
            peer.pending.pop(req_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

        if not isinstance(resp, dict) or resp.get("type") != "rpc_response":
            raise Rejected("node_invalid_response")
        ok = resp.get("ok")
        if ok is False:
            raw_err = resp.get("error")
            err_code = (
                raw_err
                if isinstance(raw_err, str) and _ERROR_CODE_RE.fullmatch(raw_err)
                else "node_rpc_failed"
            )
            detail_val = resp.get("upstream_code")
            if detail_val is None:
                detail_val = resp.get("upstream_status")
            if isinstance(detail_val, int) and not isinstance(detail_val, bool):
                detail = str(detail_val)
            elif isinstance(detail_val, str) and _DETAIL_RE.fullmatch(detail_val):
                detail = detail_val
            else:
                detail = ""
            raise Rejected(err_code, detail=detail)
        if ok is not True:
            raise Rejected("node_invalid_response")
        data = resp.get("data")
        if not isinstance(data, dict):
            raise Rejected("node_invalid_response")
        return data

    async def _retire(self, peer: _NodePeer) -> None:
        peer.ready.clear()
        for future in list(peer.pending.values()):
            if not future.done():
                future.set_exception(Rejected("node_disconnected"))
        if not peer.closed:
            peer.closed = True
            try:
                async with asyncio.timeout(3):
                    await peer.close(1012)
            except Exception:
                pass
        if peer.runner is not asyncio.current_task() and not peer.runner.done():
            peer.runner.cancel()

    async def serve(
        self,
        receive: Callable[[], Awaitable[str | bytes]],
        send: Callable[[str], Awaitable[None]],
        close: Callable[[int], Awaitable[None]],
        *,
        source_ip: str = "unknown",
        node_id: str = "",
        region: str = "CN",
    ) -> None:
        if (node_id and node_id != self.s.node_id) or region != "CN":
            try:
                async with asyncio.timeout(3):
                    await close(1008)
            except Exception:
                pass
            logging.getLogger("hakimi").info("event=node_connection_rejected")
            return

        current_task = asyncio.current_task()
        assert current_task is not None
        peer = _NodePeer(
            receive=receive,
            send=send,
            close=close,
            runner=current_task,
            source_ip=source_ip,
            node_id=node_id,
            region=region,
        )
        async with self.peer_lock:
            old = self._peer
            self._peer = peer
            self.ready.clear()
            self.active_node_id = None
            self.capabilities = []
            self.source_ip = source_ip
            self.connected_at = None
            self.last_heartbeat_at = None
            if old:
                # Never await a half-open socket before replacing it with the new connection.
                retire = asyncio.create_task(self._retire(old))
                self._retired.add(retire)
                retire.add_done_callback(self._retired.discard)

        events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_MAX_EVENT_QUEUE)

        async def reader() -> None:
            while True:
                raw = await receive()
                value = decode_node_frame(raw, self.s.node_ws_max_frame_bytes)
                frame_type = value.get("type")
                if frame_type == "rpc_response":
                    req_id = value.get("id")
                    future = peer.pending.get(req_id) if isinstance(req_id, str) else None
                    if future is not None and not future.done():
                        future.set_result(value)
                elif frame_type == "event":
                    event_name = value.get("event")
                    if not isinstance(event_name, str) or event_name not in ALLOWED_NODE_EVENTS:
                        raise Rejected("node_event_forbidden")
                    if value.get("node_id") != self.s.node_id or value.get("region") != "CN":
                        raise Rejected("wrong_node")
                    payload = value.get("payload", {})
                    if not isinstance(payload, dict):
                        raise Rejected("node_invalid_frame")
                    if event_name == "node.hello":
                        peer.capabilities = _normalize_capabilities(payload.get("capabilities", []))
                        peer.node_id = self.s.node_id
                        peer.region = "CN"
                        if peer is self._peer and peer.ready.is_set():
                            self.capabilities = list(peer.capabilities)
                        peer.hello_received.set()
                    elif (
                        event_name == "node.heartbeat"
                        and peer is self._peer
                        and peer.ready.is_set()
                    ):
                        self.last_heartbeat_at = time.time()
                    try:
                        events.put_nowait(value)
                    except asyncio.QueueFull:
                        raise Rejected("node_backpressure") from None
                else:
                    raise Rejected("node_invalid_frame")

        async def event_consumer() -> None:
            await peer.ready.wait()
            while True:
                value = await events.get()
                try:
                    if peer is not self._peer:
                        return
                    if self.on_event is not None:
                        res = self.on_event(value)
                        if inspect.isawaitable(res):
                            res = await res
                        if res == "full":
                            raise Rejected("node_backpressure")
                finally:
                    events.task_done()

        async def await_handshake() -> None:
            deadline = asyncio.get_running_loop().time() + self._handshake_timeout
            grace = min(0.5, self._handshake_timeout / 2)
            try:
                async with asyncio.timeout(grace):
                    await peer.hello_received.wait()
                return
            except TimeoutError:
                pass

            remaining = max(0.05, deadline - asyncio.get_running_loop().time())
            hello_waiter = asyncio.create_task(peer.hello_received.wait())
            ping_task = asyncio.create_task(
                self._rpc("node.ping", {}, peer, timeout=remaining)
            )
            try:
                done_hs, pending_hs = await asyncio.wait(
                    {hello_waiter, ping_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for pending_task in pending_hs:
                    pending_task.cancel()
                await asyncio.gather(*pending_hs, return_exceptions=True)
                if hello_waiter in done_hs:
                    hello_waiter.result()
                    return
                ping_data = ping_task.result()
                resp_node_id = ping_data.get("node_id") or node_id
                resp_region = ping_data.get("region") or region
                if resp_node_id != self.s.node_id or resp_region != "CN":
                    raise Rejected("wrong_node")
                if "capabilities" in ping_data:
                    peer.capabilities = _normalize_capabilities(ping_data.get("capabilities"))
            except BaseException:
                for task in (hello_waiter, ping_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(hello_waiter, ping_task, return_exceptions=True)
                raise

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self.s.ws_ping_interval)
                await self._rpc("node.ping", {}, peer, timeout=_HEARTBEAT_RPC_TIMEOUT)
                if peer is self._peer:
                    self.last_heartbeat_at = time.time()

        reader_task = asyncio.create_task(reader())
        consumer_task = asyncio.create_task(event_consumer())
        handshake_task = asyncio.create_task(await_handshake())
        tasks: list[asyncio.Task[Any]] = [reader_task, consumer_task]
        try:
            done_init, _ = await asyncio.wait(
                [reader_task, consumer_task, handshake_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done_init:
                task.result()
            if not handshake_task.done():
                # reader or consumer exited before handshake completed
                raise Rejected("node_disconnected")

            async with self.peer_lock:
                if peer is not self._peer:
                    return
                peer.ready.set()
                self.active_node_id = self.s.node_id
                self.capabilities = list(peer.capabilities)
                self.connected_at = time.time()
                self.ready.set()
                if self.on_state is not None:
                    res = self.on_state(True)
                    if inspect.isawaitable(res):
                        await res

            tasks.append(asyncio.create_task(heartbeat()))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger("hakimi").info("event=node_connection_ended")
        finally:
            if not handshake_task.done():
                handshake_task.cancel()
            peer.ready.clear()
            for future in list(peer.pending.values()):
                if not future.done():
                    future.set_exception(Rejected("node_disconnected"))
            for task in tasks:
                task.cancel()
            with anyio.CancelScope(shield=True):
                async with self.peer_lock:
                    if peer is self._peer:
                        self._peer = None
                        self.ready.clear()
                        self.active_node_id = None
                        self.capabilities = []
                        if self.on_state is not None:
                            try:
                                res = self.on_state(False)
                                if inspect.isawaitable(res):
                                    await res
                            except Exception:
                                logging.getLogger("hakimi").info(
                                    "event=node_state_callback_error"
                                )
                await asyncio.gather(handshake_task, *tasks, return_exceptions=True)
                if not peer.closed:
                    peer.closed = True
                    try:
                        async with asyncio.timeout(3):
                            await close(1012)
                    except Exception:
                        pass

    async def close(self) -> None:
        peer = self._peer
        if peer:
            await self._retire(peer)
        tasks = {
            t
            for t in (peer.runner if peer else None, *self._retired)
            if t and t is not asyncio.current_task()
        }
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
