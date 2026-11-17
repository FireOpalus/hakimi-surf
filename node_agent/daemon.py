"""Standalone Shanghai CN WebSocket node agent daemon."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import signal
import time
from typing import Any

from websockets.asyncio.client import connect

from . import __version__
from .bili_collector import BiliCollector, CollectorError
from .config import NodeAgentSettings
from .mc_bridge_slot import McBridgeSlot

log = logging.getLogger("node_agent.daemon")

CAPABILITIES = [
    "bili.public_get",
    "bili.resolve_short",
    "bili.request",
    "bili.cover_thumb",
    "bili.video_storyboard",
    "mc.slot",
]


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate_key")
        obj[key] = value
    return obj


def _reject_constant(token: str) -> Any:
    raise ValueError(f"invalid_json_constant:{token}")


def decode_node_frame(raw: str | bytes, max_bytes: int) -> dict[str, Any]:
    """Decode one bounded JSON object frame while rejecting duplicate keys or NaN/Infinity."""
    if isinstance(raw, bytes):
        if len(raw) > max_bytes:
            raise CollectorError("node_frame_too_large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise CollectorError("node_invalid_json") from None
    elif isinstance(raw, str):
        encoded = raw.encode("utf-8")
        if len(encoded) > max_bytes:
            raise CollectorError("node_frame_too_large")
        text = raw
    else:
        raise CollectorError("node_invalid_json")

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except ValueError:
        raise CollectorError("node_invalid_json") from None
    if not isinstance(parsed, dict):
        raise CollectorError("node_invalid_json")
    return parsed


def compute_backoff_step(
    delay: float,
    *,
    max_delay: float,
    rng: random.Random | None = None,
) -> tuple[float, float]:
    """Return `(sleep_seconds, next_delay)` for exponential backoff with half-full jitter."""
    bounded = min(max(delay, 0.01), max_delay)
    sampler = rng if rng is not None else random
    sleep_for = sampler.uniform(bounded / 2.0, bounded)
    next_delay = min(bounded * 2.0, max_delay)
    return sleep_for, next_delay


class NodeDaemon:
    """Outbound WSS reverse-RPC daemon running on the Shanghai CN node."""

    def __init__(
        self,
        settings: NodeAgentSettings,
        *,
        collector: BiliCollector | None = None,
        mc_slot: McBridgeSlot | None = None,
        reconnect_base: float = 1.0,
    ) -> None:
        self.settings = settings
        self.collector = collector or BiliCollector(
            min_interval=settings.bili_min_interval,
            temp_dir=settings.temp_dir,
        )
        self.mc_slot = mc_slot or McBridgeSlot()
        self.reconnect_base = max(0.01, float(reconnect_base))
        self._rpc_semaphore = asyncio.Semaphore(4)
        self._send_lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._active_ws: Any = None
        self.backoff_history: list[float] = []

    def hello_event(self) -> dict[str, Any]:
        return {
            "type": "event",
            "event": "node.hello",
            "node_id": self.settings.node_id,
            "region": self.settings.node_region,
            "ts": time.time(),
            "payload": {
                "version": __version__,
                "capabilities": list(CAPABILITIES),
            },
        }

    async def send_json(self, ws: Any, obj: dict[str, Any]) -> None:
        raw = json.dumps(obj, ensure_ascii=False)
        if len(raw.encode("utf-8")) > self.settings.max_frame_bytes:
            if obj.get("type") == "rpc_response":
                raw = json.dumps(
                    {
                        "type": "rpc_response",
                        "id": obj.get("id", ""),
                        "ok": False,
                        "data": {},
                        "error": "node_frame_too_large",
                        "upstream_status": None,
                        "upstream_code": None,
                    },
                    ensure_ascii=False,
                )
            else:
                raise CollectorError("node_frame_too_large")
        async with self._send_lock:
            await ws.send(raw)

    async def _execute_action(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if action == "bili.public_get":
            return await self.collector.public_get(params)
        if action == "bili.resolve_short":
            return await self.collector.resolve_short(params)
        if action == "bili.request":
            return await self.collector.request(params)
        if action == "bili.cover_thumb":
            return await self.collector.cover_thumb(params)
        if action == "bili.video_storyboard":
            return await self.collector.video_storyboard(params)
        if action == "mc.status":
            return await self.mc_slot.status()
        if action == "mc.dispatch_goal":
            return await self.mc_slot.dispatch_goal(params)
        if action == "mc.pause":
            return await self.mc_slot.pause()
        if action == "mc.resume":
            return await self.mc_slot.resume()
        raise CollectorError("node_action_forbidden")

    async def _dispatch_rpc(
        self,
        ws: Any,
        req_id: str,
        action: str,
        params: Any,
    ) -> None:
        response: dict[str, Any]
        try:
            if not isinstance(params, dict):
                if params is None:
                    params = {}
                else:
                    raise CollectorError("invalid_params")

            if action == "node.ping":
                data = {
                    "pong": True,
                    "node_id": self.settings.node_id,
                    "region": self.settings.node_region,
                    "ts": time.time(),
                }
            else:
                async with self._rpc_semaphore:
                    data = await self._execute_action(action, params)

            response = {
                "type": "rpc_response",
                "id": req_id,
                "ok": True,
                "data": data,
                "error": None,
                "upstream_status": None,
                "upstream_code": None,
            }
        except asyncio.CancelledError:
            raise
        except CollectorError as exc:
            response = {
                "type": "rpc_response",
                "id": req_id,
                "ok": False,
                "data": {},
                "error": exc.code,
                "upstream_status": exc.upstream_status,
                "upstream_code": exc.upstream_code,
            }
        except Exception as exc:
            log.warning(
                "event=node_rpc_internal_error action=%s err=%s",
                action,
                type(exc).__name__,
            )
            response = {
                "type": "rpc_response",
                "id": req_id,
                "ok": False,
                "data": {},
                "error": "node_internal_error",
                "upstream_status": None,
                "upstream_code": None,
            }

        with contextlib.suppress(Exception):
            await self.send_json(ws, response)

    async def serve_session(self, ws: Any) -> None:
        """Handle one connected WSS session until closed or stopped."""
        self._active_ws = ws
        try:
            await self.send_json(ws, self.hello_event())
            async for raw in ws:
                if self._stop.is_set():
                    break
                try:
                    frame = decode_node_frame(raw, self.settings.max_frame_bytes)
                except CollectorError:
                    log.info("event=node_bad_frame")
                    continue
                if frame.get("type") != "rpc_request":
                    continue
                req_id = frame.get("id")
                action = frame.get("action")
                if (
                    not isinstance(req_id, str)
                    or not req_id
                    or len(req_id) > 128
                    or not isinstance(action, str)
                    or not action
                ):
                    continue
                task = asyncio.create_task(
                    self._dispatch_rpc(ws, req_id, action, frame.get("params"))
                )
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        finally:
            self._active_ws = None
            pending = list(self._tasks)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    def _ws_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.settings.node_ws_token.get_secret_value()}",
            "X-Node-ID": self.settings.node_id,
            "X-Node-Region": self.settings.node_region,
        }

    async def run(self) -> None:
        """Maintain persistent outbound WSS connection with exponential backoff."""
        await self.collector.cleanup_stale()
        delay = min(self.reconnect_base, self.settings.reconnect_max)
        try:
            while not self._stop.is_set():
                connected_at = asyncio.get_running_loop().time()
                try:
                    async with connect(
                        self.settings.node_ws_url,
                        additional_headers=self._ws_headers(),
                        ping_interval=self.settings.ws_ping_interval,
                        ping_timeout=self.settings.ws_ping_timeout,
                        open_timeout=10.0,
                        close_timeout=3.0,
                        max_size=self.settings.max_frame_bytes,
                        max_queue=8,
                        compression=None,
                        proxy=None,
                    ) as ws:
                        await self.serve_session(ws)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.info("event=node_ws_reconnect err=%s", type(exc).__name__)

                if self._stop.is_set():
                    break
                if asyncio.get_running_loop().time() - connected_at > 60.0:
                    delay = min(self.reconnect_base, self.settings.reconnect_max)

                sleep_for, next_delay = compute_backoff_step(
                    delay, max_delay=self.settings.reconnect_max
                )
                self.backoff_history.append(sleep_for)
                if len(self.backoff_history) > 64:
                    self.backoff_history.pop(0)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
                except TimeoutError:
                    pass
                delay = next_delay
        finally:
            await self.collector.close()

    async def stop(self) -> None:
        """Signal shutdown and close active socket/tasks cleanly."""
        self._stop.set()
        ws = self._active_ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        for task in list(self._tasks):
            task.cancel()


async def _run_cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = NodeAgentSettings()
    daemon = NodeDaemon(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(daemon.stop()))
    await daemon.run()


def main() -> None:
    try:
        asyncio.run(_run_cli())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
