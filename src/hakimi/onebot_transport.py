"""OneBot v11 HTTP / universal forward and reverse WebSocket transports."""

import asyncio
import inspect
import ipaddress
import json
import logging
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

import anyio
import httpx
from websockets.asyncio.client import connect

from .bilibili import bounded_json
from .config import Settings
from .outbound_actions import ALLOWED_EMOJI_IDS
from .security import Rejected

ACTIONS = {"send_private_msg", "send_group_msg", "get_forward_msg", "get_login_info", "get_status"}
ACTIONS |= {
    "get_friend_list",
    "delete_friend",
    "set_self_longnick",
    "get_image",
    "get_file",
    "get_msg",
    "set_input_status",
    "send_group_forward_msg",
    "set_msg_emoji_like",
    "group_poke",
    "friend_poke",
    "send_poke",
}
EventHandler = Callable[[dict], Awaitable[str]]


class OneBotTransport(Protocol):
    async def call(self, action: str, params: dict) -> dict: ...


def response_data(value: dict, action: str = "") -> dict | list:
    if value.get("status") != "ok" or value.get("retcode") != 0:
        if action == "send_group_forward_msg" and value.get("retcode") in {404, 1404}:
            raise Rejected("onebot_action_unsupported")
        raise Rejected("onebot_rpc_failed")
    data = value.get("data")
    if action == "get_friend_list" and isinstance(data, list):
        return data
    if (
        action
        in {
            "delete_friend",
            "set_self_longnick",
            "set_input_status",
            "set_msg_emoji_like",
            "group_poke",
            "friend_poke",
            "send_poke",
        }
        and data is None
    ):
        return {}
    if not isinstance(data, dict):
        raise Rejected("onebot_invalid_response")
    return data


def validate_action(settings, action, params):
    if action not in ACTIONS:
        raise Rejected("onebot_action_forbidden")
    if action == "send_group_forward_msg":
        target = str(params.get("group_id", ""))
        nodes = params.get("messages", [])
        if (
            target not in settings.onebot_allowed_groups
            or settings.group_policies.get("onebot:" + target, "at_only") == "muted"
            or not isinstance(nodes, list)
            or len(nodes) != 1
        ):
            raise Rejected("onebot_action_forbidden")
        node = nodes[0]
        data = node.get("data", {}) if isinstance(node, dict) else {}
        content = data.get("content", [])
        if (
            node.get("type") != "node"
            or str(data.get("user_id")) != settings.onebot_self_id
            or not isinstance(content, list)
            or len(content) != 1
            or content[0].get("type") != "text"
            or len(content[0].get("data", {}).get("text", "")) > 6000
        ):
            raise Rejected("onebot_action_forbidden")
    if action == "set_input_status":
        target = str(params.get("user_id", ""))
        if target != settings.owner_qq_id and target not in settings.onebot_allowed_users:
            raise Rejected("onebot_action_forbidden")
    if action == "delete_friend":
        value = params.get("user_id")
        if (
            not settings.owner_qq_id
            or isinstance(value, bool)
            or not str(value).isdecimal()
            or int(value) <= 0
            or int(value) in {int(settings.owner_qq_id), int(settings.onebot_self_id)}
        ):
            raise Rejected("reserved_identity")
    if action == "set_msg_emoji_like":
        msg_id = params.get("message_id")
        emoji_id = str(params.get("emoji_id", ""))
        if (
            isinstance(msg_id, bool)
            or not re.fullmatch(r"-?[0-9]{1,30}", str(msg_id or ""))
            or emoji_id not in ALLOWED_EMOJI_IDS
        ):
            raise Rejected("onebot_action_forbidden")
    if action in {"group_poke", "friend_poke", "send_poke"}:
        uid = params.get("user_id")
        uid_str = str(uid)
        if (
            isinstance(uid, bool)
            or not uid_str.isdecimal()
            or int(uid_str) <= 0
            or uid_str == settings.onebot_self_id
            or uid_str in settings.muted_user_ids
        ):
            raise Rejected("onebot_action_forbidden")
        if action == "group_poke" or (action == "send_poke" and "group_id" in params):
            gid = str(params.get("group_id", ""))
            if (
                gid not in settings.onebot_allowed_groups
                or settings.group_policies.get("onebot:" + gid, "at_only") == "muted"
            ):
                raise Rejected("onebot_action_forbidden")
        else:
            allowed_private = uid_str == settings.owner_qq_id or (
                settings.onebot_private_mode == "owner_and_allowlist"
                and uid_str in settings.onebot_allowed_users
            )
            if not allowed_private:
                raise Rejected("onebot_action_forbidden")


class HTTPTransport:
    def __init__(self, settings: Settings, http: httpx.AsyncClient):
        self.s, self.http = settings, http

    async def call(self, action: str, params: dict) -> dict:
        validate_action(self.s, action, params)
        value = await bounded_json(
            self.http,
            "POST",
            self.s.onebot_api_base.rstrip("/") + "/" + action,
            max_bytes=self.s.max_body_bytes,
            headers={"Authorization": "Bearer " + self.s.onebot_access_token.get_secret_value()},
            json=params,
        )
        return response_data(value, action)


def decode_frame(raw: str | bytes, limit: int) -> dict:
    if len(raw.encode() if isinstance(raw, str) else raw) > limit:
        raise Rejected("onebot_frame_too_large")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise Rejected("onebot_invalid_json")
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
    except (ValueError, RecursionError, UnicodeError):
        raise Rejected("onebot_invalid_json") from None


def websocket_source_ip(websocket, settings) -> str:
    """Only an explicitly trusted immediate proxy may supply X-Real-IP."""
    immediate = websocket.client.host if websocket.client else "unknown"
    try:
        immediate = str(ipaddress.ip_address(immediate))
    except ValueError:
        return "unknown"
    if immediate in settings.onebot_trusted_proxy_ips:
        try:
            return str(ipaddress.ip_address(websocket.headers.get("x-real-ip", immediate)))
        except ValueError:
            pass
    return immediate


@dataclass
class _Peer:
    receive: Callable
    send: Callable
    close: Callable
    runner: asyncio.Task
    source_ip: str
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: dict = field(default_factory=dict)


class WebSocketTransport:
    """A newly authenticated connection replaces its predecessor without sharing RPC state.

    Pending echoes, readers and cleanup belong to one peer only. Once a write may
    have reached NapCat, an uncertain send is never replayed on the new peer.
    """

    def __init__(self, settings: Settings):
        self.s = settings
        self.ready = asyncio.Event()
        self.peer_lock = asyncio.Lock()  # Serializes handoff only, never the connection lifetime.
        self._peer: _Peer | None = None
        self._forward: asyncio.Task | None = None
        self._retired: set[asyncio.Task] = set()
        self.on_state = None
        self.last_heartbeat_at = None
        self.source_ip = None
        self.connected_at = None

    def status(self):
        return {
            "online": self.ready.is_set(),
            "last_heartbeat_at": self.last_heartbeat_at,
            "source_ip": self.source_ip,
            "connected_at": self.connected_at,
        }

    async def call(self, action: str, params: dict) -> dict:
        if not self.ready.is_set():
            raise Rejected("onebot_disconnected")
        return await self._rpc(action, params, self._peer)

    async def _rpc(self, action: str, params: dict, peer: _Peer | None) -> dict:
        validate_action(self.s, action, params)
        if peer is None or peer is not self._peer:
            raise Rejected("onebot_not_dispatched")
        if len(peer.pending) >= 8:
            raise Rejected("onebot_unavailable")
        echo = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        peer.pending[echo] = future
        dispatched = False
        try:
            async with asyncio.timeout(self.s.ws_rpc_timeout):
                async with peer.send_lock:
                    if peer is not self._peer:
                        raise Rejected("onebot_not_dispatched")
                    # Mark before await: even a transport exception can follow a delivered frame.
                    dispatched = True
                    await peer.send(json.dumps({"action": action, "params": params, "echo": echo}))
                return response_data(await future, action)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if dispatched and action in {
                "send_private_msg",
                "send_group_msg",
                "send_group_forward_msg",
            }:
                if isinstance(exc, Rejected) and str(exc) == "onebot_action_unsupported":
                    raise
                raise Rejected("onebot_delivery_uncertain") from None
            if isinstance(exc, TimeoutError):
                raise Rejected("onebot_rpc_timeout") from None
            raise
        finally:
            peer.pending.pop(echo, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()  # Consume peer shutdown errors even when send itself failed.

    async def send_onebot_action_with_retry(
        self, action, params, *, before_send=None, max_wait=60, attempts=3
    ):
        """Wait for a usable connection; retry only when nothing was dispatched.

        The durable owner-DM queue remains responsible for expiry/recovery after this
        bounded wait. This helper never makes an ambiguous send exactly-once claim.
        """
        validate_action(self.s, action, params)
        attempts = max(1, min(int(attempts), 3))
        wait = max(0.0, min(float(max_wait), 60.0))
        deadline = asyncio.get_running_loop().time() + wait
        for turn in range(attempts):
            if not self.ready.is_set():
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    async with asyncio.timeout(remaining / (attempts - turn)):
                        await self.ready.wait()
                except TimeoutError:
                    continue
            if before_send is not None:
                valid = before_send()
                valid = await valid if inspect.isawaitable(valid) else valid
                if valid is False:
                    raise Rejected("onebot_not_dispatched")
            peer = self._peer
            if not self.ready.is_set() or peer is None or not peer.ready.is_set():
                continue
            try:
                return await self._rpc(action, params, peer)
            except Rejected as exc:
                if str(exc) != "onebot_not_dispatched":
                    raise
        raise Rejected("onebot_not_dispatched")

    async def _retire(self, peer):
        peer.ready.clear()
        try:
            async with asyncio.timeout(3):
                await peer.close(1012)
        except Exception:
            pass
        finally:
            if peer.runner is not asyncio.current_task() and not peer.runner.done():
                peer.runner.cancel()

    async def serve(self, receive, send, close, on_event: EventHandler, *, source_ip="unknown"):
        peer = _Peer(receive, send, close, asyncio.current_task(), source_ip)
        async with self.peer_lock:
            old = self._peer
            self._peer = peer
            self.ready.clear()
            self.source_ip = source_ip
            self.connected_at = None
            if old:
                # Never await the half-open socket before accepting the new one.
                retire = asyncio.create_task(self._retire(old))
                self._retired.add(retire)
                retire.add_done_callback(self._retired.discard)
        events: asyncio.Queue = asyncio.Queue(maxsize=32)
        notices: asyncio.Queue = asyncio.Queue(maxsize=32)

        async def reader():
            while True:
                raw = await receive()
                value = decode_frame(raw, self.s.ws_max_frame_bytes)
                if "echo" in value:
                    echo = value["echo"]
                    future = peer.pending.get(echo) if isinstance(echo, str) else None
                    if future is not None and not future.done():
                        future.set_result(value)
                elif "post_type" in value:
                    # Large base64 RPC responses are allowed; untrusted event floods are not.
                    if len(raw.encode() if isinstance(raw, str) else raw) > self.s.max_body_bytes:
                        raise Rejected("onebot_event_too_large")
                    if str(value.get("self_id")) != self.s.onebot_self_id:
                        raise Rejected("wrong_bot")
                    if value.get("post_type") in {"message", "message_sent", "notice"}:
                        try:
                            (notices if value.get("post_type") == "notice" else events).put_nowait(
                                value
                            )
                        except asyncio.QueueFull:
                            raise Rejected("onebot_backpressure") from None

        async def consumer(queue):
            await peer.ready.wait()
            while True:
                value = await queue.get()
                try:
                    if peer is not self._peer:
                        return
                    if await on_event(value) == "full":
                        raise Rejected("onebot_backpressure")
                finally:
                    queue.task_done()

        async def heartbeat():
            while True:
                await asyncio.sleep(self.s.ws_ping_interval)
                status = await self._rpc("get_status", {}, peer)
                if status.get("online") is not True or status.get("good") is not True:
                    raise Rejected("onebot_offline")
                if peer is self._peer:
                    self.last_heartbeat_at = time.time()

        tasks = [
            asyncio.create_task(reader()),
            asyncio.create_task(consumer(events)),
            asyncio.create_task(consumer(notices)),
        ]
        try:
            info = await self._rpc("get_login_info", {}, peer)
            if str(info.get("user_id")) != self.s.onebot_self_id:
                raise Rejected("wrong_bot")
            async with self.peer_lock:
                if peer is not self._peer:
                    return
                peer.ready.set()
                self.ready.set()
                self.connected_at = time.time()
                if self.on_state:
                    await self.on_state(True)
            tasks.append(asyncio.create_task(heartbeat()))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger("hakimi").info("event=onebot_connection_ended")
        finally:
            peer.ready.clear()
            for future in list(peer.pending.values()):
                if not future.done():
                    future.set_exception(Rejected("onebot_disconnected"))
            for task in tasks:
                task.cancel()
            with anyio.CancelScope(shield=True):
                async with self.peer_lock:
                    if peer is self._peer:
                        self._peer = None
                        self.ready.clear()
                        if self.on_state:
                            try:
                                await self.on_state(False)
                            except Exception:
                                logging.getLogger("hakimi").info("event=owner_dm_worker_error")
                await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    async with asyncio.timeout(3):
                        await close(1012)
                except Exception:
                    pass

    def start_forward(self, on_event: EventHandler):
        self._forward = asyncio.create_task(self._reconnect(on_event), name="onebot-forward")

    async def _reconnect(self, on_event: EventHandler):
        delay = 1.0
        while True:
            connected_at = asyncio.get_running_loop().time()
            try:
                async with connect(
                    self.s.onebot_ws_url,
                    additional_headers={
                        "Authorization": "Bearer " + self.s.onebot_ws_token.get_secret_value()
                    },
                    ping_interval=self.s.ws_ping_interval,
                    ping_timeout=self.s.ws_ping_timeout,
                    open_timeout=10,
                    close_timeout=3,
                    max_size=self.s.ws_max_frame_bytes,
                    max_queue=2,
                    compression=None,
                    proxy=None,
                ) as ws:
                    await self.serve(ws.recv, ws.send, ws.close, on_event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("hakimi").info("event=onebot_reconnect")
            if asyncio.get_running_loop().time() - connected_at > 60:
                delay = 1.0
            await asyncio.sleep(random.uniform(delay / 2, delay))
            delay = min(delay * 2, self.s.ws_reconnect_max)

    async def close(self):
        peer = self._peer
        if peer:
            await self._retire(peer)
        tasks = {
            t
            for t in (peer.runner if peer else None, self._forward, *self._retired)
            if t and t is not asyncio.current_task()
        }
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
