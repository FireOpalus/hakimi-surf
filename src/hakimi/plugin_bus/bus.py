"""Core PluginBus: credentials, WS session lifecycle, fiber coordination, RPC, and tool routing."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import math
import secrets
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import anyio
from pydantic import ValidationError

from hakimi_plugin_sdk.protocol import BLOB_FIELD, BLOB_HARD_MAX_BYTES, ToolCallResult

from ..budget_policy import PATROL_LANE
from ..config import Settings
from ..modules.actions import Action, ActionCall, NoArgs, Outcome
from ..modules.base import sanitize_sensor_text
from ..modules.scheduling import ActivityQuota
from ..plugin_kernel import PluginContext, PluginKernel
from ..screen_watch import decode_ws_json_frame
from ..security import Rejected, clean_text, configured_secrets
from ..thinking_policy import ThinkingScene
from .ai_gateway import PluginBusAIGatewayMixin
from .blob import PluginBusBlobMixin
from .domain_bridge import PluginBusDomainBridgeMixin
from .protocol import (
    _ACTION_NAME_RE,
    _ALIAS_VERB_RE,
    _CAPABILITY_RE,
    _CODE_RE,
    _CORE_ADMIN_PANELS,
    _DEFAULT_HANDSHAKE_TIMEOUT_SECONDS,
    _DEFAULT_HEARTBEAT_SECONDS,
    _DEFAULT_IDLE_TIMEOUT_SECONDS,
    _DEFAULT_MAX_FRAME_BYTES,
    _DEFAULT_RPC_TIMEOUT_SECONDS,
    _FIELD_NAME_RE,
    _HOTPLUG_ADMIN_PANELS,
    _MAX_PENDING_RPCS,
    _NOTIFY_MAX_PER_WINDOW,
    _NOTIFY_WINDOW_SECONDS,
    _RESERVED_ACTION_PREFIXES,
    _RESERVED_SCENE_KEYS,
    _SCHEMA,
    _SCOPED_ACTION_PREFIX_OWNERS,
    _SCOPED_SCENE_OWNERS,
    _VALID_OUTCOME_STATUSES,
    _VAULT_CRED_PREFIX,
    PLATFORM_ID_RE,
    PLUGIN_ID_RE,
    RESERVED_CHAT_PLATFORMS,
    RESERVED_PLUGIN_IDS,
    VALID_SCOPES,
    _build_action_args_model,
    _hash_token,
    _PluginActionDecl,
    _PluginHelloFrame,
    _PluginThinkingSceneDecl,
    _sanitize_payload,
    _scrub_json_secrets,
    _validate_display_name,
    _validate_node_region,
    _validate_plugin_id,
    _validate_scopes,
)
from .proxy import RemoteModuleProxy, _PendingRpc, _PluginSession

log = logging.getLogger("hakimi")

__all__ = ["PluginBus", "log"]


class PluginBus(
    PluginBusBlobMixin,
    PluginBusAIGatewayMixin,
    PluginBusDomainBridgeMixin,
):
    """Credential store and reverse WebSocket transport for remote plugin modules."""

    def __init__(
        self,
        settings: Settings,
        store: Any,
        service: Any,
        *,
        clock: Callable[[], float] = time.time,
        heartbeat_interval: float | None = None,
        idle_timeout: float | None = None,
        handshake_timeout: float | None = None,
        rpc_timeout: float | None = None,
        notify_cooldown: float = _NOTIFY_WINDOW_SECONDS,
        notify_limit: int = _NOTIFY_MAX_PER_WINDOW,
    ) -> None:
        self.s = settings
        self.store = store
        self.service = service
        self.clock = clock
        self.heartbeat_interval = float(
            getattr(settings, "plugin_bus_ws_heartbeat_seconds", _DEFAULT_HEARTBEAT_SECONDS)
            if heartbeat_interval is None
            else heartbeat_interval
        )
        self.idle_timeout = float(
            getattr(settings, "plugin_bus_ws_idle_timeout_seconds", _DEFAULT_IDLE_TIMEOUT_SECONDS)
            if idle_timeout is None
            else idle_timeout
        )
        self.handshake_timeout = float(
            _DEFAULT_HANDSHAKE_TIMEOUT_SECONDS if handshake_timeout is None else handshake_timeout
        )
        self.rpc_timeout = float(
            getattr(settings, "plugin_bus_rpc_timeout", _DEFAULT_RPC_TIMEOUT_SECONDS)
            if rpc_timeout is None
            else rpc_timeout
        )
        self.notify_cooldown = float(notify_cooldown)
        self.notify_limit = int(notify_limit)
        self.initialized = False
        self._init_lock = asyncio.Lock()
        self._connect_locks: dict[str, asyncio.Lock] = {}
        self._sessions: dict[str, _PluginSession] = {}
        self._fallback_kernel = PluginKernel()
        self._notify_history: dict[str, deque[float]] = {}
        self.uncertain_deliveries: deque[str] = deque(maxlen=128)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._blob_transfer_sem = asyncio.Semaphore(4)

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop | None:
        """Return the active event loop bound to this PluginBus, if running."""
        loop = self._loop
        if loop is not None and not loop.is_closed() and loop.is_running():
            return loop
        return None

    def _connect_lock(self, plugin_id: str) -> asyncio.Lock:
        lock = self._connect_locks.get(plugin_id)
        if lock is None:
            lock = asyncio.Lock()
            self._connect_locks[plugin_id] = lock
        return lock

    def _kernel(self) -> PluginKernel:
        k = getattr(self.service, "kernel", None)
        return k if isinstance(k, PluginKernel) else self._fallback_kernel

    def _all_secrets(self) -> list[str]:
        seen: dict[str, None] = {}
        for raw in configured_secrets(self.s):
            if raw:
                seen[raw] = None
        for raw in getattr(self.service, "secrets", ()) or ():
            if isinstance(raw, str) and raw:
                seen[raw] = None
        return list(seen)

    def _ensure_plugin_not_reserved(self, pid: str) -> None:
        if pid in RESERVED_PLUGIN_IDS:
            raise Rejected("reserved_plugin_id")
        modules = getattr(self.service, "modules", None)
        if modules is not None:
            if pid in getattr(modules, "systems", {}):
                raise Rejected("reserved_plugin_id")
            existing = modules.get(pid) if hasattr(modules, "get") else None
            if existing is not None and not isinstance(existing, RemoteModuleProxy):
                raise Rejected("reserved_plugin_id")

    def _max_frame_bytes(self) -> int:
        return int(getattr(self.s, "plugin_bus_ws_max_frame_bytes", _DEFAULT_MAX_FRAME_BYTES))

    def _ws_enabled(self) -> bool:
        return bool(getattr(self.s, "plugin_bus_ws_enabled", False))

    def _ws_path(self) -> str:
        return str(getattr(self.s, "plugin_bus_ws_path", "/ws/plugin"))

    async def initialize(self) -> None:
        self._loop = asyncio.get_running_loop()
        if self.initialized:
            return
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                await self.store.db.executescript(_SCHEMA)
            self.initialized = True

    def _open_cred_row(self, row: Any) -> dict[str, Any]:
        plugin_id = str(row["plugin_id"])
        data = self.store.vault.open(_VAULT_CRED_PREFIX + plugin_id, row["payload"])
        if not isinstance(data, dict):
            raise Rejected("plugin_credential_corrupt")
        return {
            "plugin_id": plugin_id,
            "enabled": bool(row["enabled"]),
            "display_name": str(data.get("display_name") or plugin_id),
            "allowed_scopes": list(data.get("allowed_scopes") or []),
            "node_region": data.get("node_region", "KR"),
            "created_at": float(data.get("created_at") or row["updated"]),
            "last_seen_at": (
                float(data["last_seen_at"]) if data.get("last_seen_at") is not None else None
            ),
            "updated": float(row["updated"]),
        }

    async def _load_cred(self, plugin_id: str) -> dict[str, Any] | None:
        await self.initialize()
        async with self.store.lock:
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (plugin_id,),
            )
        if row is None:
            return None
        return self._open_cred_row(row)

    async def create_plugin(
        self,
        plugin_id: str,
        display_name: str,
        scopes: list[str],
        *,
        node_region: Literal["KR", "CN"] = "KR",
    ) -> tuple[str, dict[str, Any]]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        self._ensure_plugin_not_reserved(pid)
        clean_name = _validate_display_name(display_name, self.s)
        clean_scopes = _validate_scopes(scopes)
        region = _validate_node_region(node_region)

        token = secrets.token_urlsafe(32)
        salt = secrets.token_hex(16)
        token_hash = _hash_token(self.store.vault.index_key, salt, token)
        now = self.clock()
        payload_blob = self.store.vault.seal(
            _VAULT_CRED_PREFIX + pid,
            {
                "display_name": clean_name,
                "allowed_scopes": clean_scopes,
                "node_region": region,
                "created_at": now,
                "last_seen_at": None,
            },
        )
        async with self.store.transaction():
            existing = await self.store.one(
                "SELECT 1 FROM plugin_credentials WHERE plugin_id=?", (pid,)
            )
            if existing is not None:
                raise Rejected("plugin_already_exists")
            await self.store.db.execute(
                "INSERT INTO plugin_credentials"
                "(plugin_id, token_salt, token_hash, enabled, payload, updated) "
                "VALUES (?,?,?,?,?,?)",
                (pid, salt, token_hash, 1, payload_blob, now),
            )
        return token, await self.overview()

    async def update_plugin(
        self,
        plugin_id: str,
        *,
        display_name: str | None = None,
        scopes: list[str] | None = None,
        enabled: bool | None = None,
        node_region: Literal["KR", "CN"] | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        clean_name = (
            _validate_display_name(display_name, self.s) if display_name is not None else None
        )
        clean_scopes = _validate_scopes(scopes) if scopes is not None else None
        region = _validate_node_region(node_region) if node_region is not None else None
        if enabled is not None and not isinstance(enabled, bool):
            raise Rejected("invalid_admin_input")

        now = self.clock()
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (pid,),
            )
            if row is None:
                raise Rejected("plugin_not_found")
            cred = self._open_cred_row(row)
            new_enabled = cred["enabled"] if enabled is None else enabled
            new_name = cred["display_name"] if clean_name is None else clean_name
            new_scopes = cred["allowed_scopes"] if clean_scopes is None else clean_scopes
            new_region = cred["node_region"] if region is None else region
            blob = self.store.vault.seal(
                _VAULT_CRED_PREFIX + pid,
                {
                    "display_name": new_name,
                    "allowed_scopes": new_scopes,
                    "node_region": new_region,
                    "created_at": cred["created_at"],
                    "last_seen_at": cred["last_seen_at"],
                },
            )
            await self.store.db.execute(
                "UPDATE plugin_credentials SET enabled=?, payload=?, updated=? WHERE plugin_id=?",
                (int(new_enabled), blob, now, pid),
            )

        session = self._sessions.get(pid)
        if session is not None:
            scopes_narrowed = clean_scopes is not None and not session.scopes.issubset(
                set(new_scopes)
            )
            if not new_enabled or scopes_narrowed:
                await self._dispose_session(pid, close_code=1008)
            elif session.proxy is not None:
                if clean_name is not None:
                    session.proxy.display_name = new_name
                if region is not None:
                    session.proxy.node_region = new_region
                    session.node_region = new_region
        return await self.overview()

    async def rotate_token(self, plugin_id: str) -> tuple[str, dict[str, Any]]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        token = secrets.token_urlsafe(32)
        salt = secrets.token_hex(16)
        token_hash = _hash_token(self.store.vault.index_key, salt, token)
        now = self.clock()
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE plugin_credentials "
                "SET token_salt=?, token_hash=?, updated=? WHERE plugin_id=?",
                (salt, token_hash, now, pid),
            )
            if cur.rowcount == 0:
                raise Rejected("plugin_not_found")
        await self._dispose_session(pid, close_code=1008)
        return token, await self.overview()

    async def delete_plugin(self, plugin_id: str) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        await self._dispose_session(pid, close_code=1008)
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "DELETE FROM plugin_credentials WHERE plugin_id=?", (pid,)
            )
            if cur.rowcount == 0:
                raise Rejected("plugin_not_found")
        return await self.overview()

    async def disconnect_plugin(self, plugin_id: str) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        if await self._load_cred(pid) is None:
            raise Rejected("plugin_not_found")
        await self._dispose_session(pid, close_code=1012)
        return await self.overview()

    async def authenticate(self, plugin_id: str, token: str) -> dict[str, Any] | None:
        await self.initialize()
        if not isinstance(plugin_id, str) or not isinstance(token, str) or not token:
            return None
        try:
            pid = _validate_plugin_id(plugin_id)
            self._ensure_plugin_not_reserved(pid)
        except Rejected:
            return None
        async with self.store.lock:
            row = await self.store.one(
                "SELECT plugin_id, token_salt, token_hash, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (pid,),
            )
        if row is None or not row["enabled"]:
            return None
        expected = _hash_token(self.store.vault.index_key, row["token_salt"], token)
        if not hmac.compare_digest(expected, row["token_hash"]):
            return None
        return self._open_cred_row(row)

    async def status_summary(self) -> dict[str, Any]:
        await self.initialize()
        async with self.store.lock:
            row = await self.store.one("SELECT COUNT(*) FROM plugin_credentials")
        registered_count = int(row[0]) if row else 0
        online_count = sum(1 for s in self._sessions.values() if s.ready and not s.closed)
        return {
            "ws_enabled": self._ws_enabled(),
            "ws_path": self._ws_path(),
            "registered_count": registered_count,
            "online_count": online_count,
        }

    async def overview(self) -> dict[str, Any]:
        await self.initialize()
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials ORDER BY plugin_id ASC"
            )
        plugins: list[dict[str, Any]] = []
        for row in rows:
            cred = self._open_cred_row(row)
            pid = cred["plugin_id"]
            sess = self._sessions.get(pid)
            online = bool(sess is not None and sess.ready and not sess.closed)
            proxy = sess.proxy if sess is not None and online else None
            plugins.append(
                {
                    "plugin_id": pid,
                    "display_name": cred["display_name"],
                    "enabled": cred["enabled"],
                    "online": online,
                    "scopes": list(cred["allowed_scopes"]),
                    "node_region": cred["node_region"],
                    "connected_at": sess.connected_at if online and sess is not None else None,
                    "last_seen_at": (
                        sess.last_seen_at if online and sess is not None else cred["last_seen_at"]
                    ),
                    "remote_ip": sess.source_ip if online and sess is not None else None,
                    "actions_count": len(proxy.actions()) if proxy is not None else 0,
                    "chat_platforms": list(proxy.chat_platforms) if proxy is not None else [],
                    "thinking_scenes": (
                        [s.key for s in proxy.thinking_scenes] if proxy is not None else []
                    ),
                }
            )
        return {
            "ws_enabled": self._ws_enabled(),
            "ws_path": self._ws_path(),
            "allowed_scopes_catalog": list(VALID_SCOPES),
            "plugins": plugins,
        }

    async def _touch_last_seen(self, plugin_id: str, ts: float) -> None:
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (plugin_id,),
            )
            if row is None:
                return
            cred = self._open_cred_row(row)
            blob = self.store.vault.seal(
                _VAULT_CRED_PREFIX + plugin_id,
                {
                    "display_name": cred["display_name"],
                    "allowed_scopes": cred["allowed_scopes"],
                    "node_region": cred["node_region"],
                    "created_at": cred["created_at"],
                    "last_seen_at": ts,
                },
            )
            await self.store.db.execute(
                "UPDATE plugin_credentials SET payload=?, updated=? WHERE plugin_id=?",
                (blob, ts, plugin_id),
            )

    async def _load_module_switch(self, module_id: str) -> bool:
        async with self.store.lock:
            row = await self.store.one(
                "SELECT enabled FROM module_switches WHERE module_id=?",
                (module_id,),
            )
        return True if row is None else bool(row[0])

    async def _dispose_session(self, plugin_id: str, *, close_code: int = 1012) -> None:
        sess = self._sessions.get(plugin_id)
        if sess is not None:
            sess.close_code = close_code
            await sess.fiber.dispose()
        else:
            kernel = self._kernel()
            await kernel.dispose_fiber(f"remote:{plugin_id}")

    async def close(self) -> None:
        for pid in list(self._sessions):
            await self._dispose_session(pid, close_code=1012)

    async def _send_raw(
        self,
        send: Callable[[str], Awaitable[None]],
        send_lock: asyncio.Lock,
        payload: dict[str, Any],
    ) -> None:
        wire = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        async with send_lock:
            await send(wire)

    async def _send_frame(self, session: _PluginSession, payload: dict[str, Any]) -> None:
        if session.closed:
            return
        try:
            await self._send_raw(session.send, session.send_lock, payload)
        except Exception:
            pass

    async def _rpc(
        self,
        session: _PluginSession,
        method: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        delivery_id: str | None = None,
    ) -> dict[str, Any]:
        if session.closed or not session.ready:
            raise Rejected("plugin_disconnected")
        if len(session.pending) >= _MAX_PENDING_RPCS:
            raise Rejected("plugin_backpressure")
        effective_timeout = self.rpc_timeout if timeout is None else float(timeout)
        req_id = uuid.uuid4().hex
        try:
            frame = json.dumps(
                {
                    "type": "rpc.request",
                    "id": req_id,
                    "method": method,
                    "params": params,
                },
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError):
            raise Rejected("plugin_invalid_params") from None
        if len(frame.encode("utf-8")) > self._max_frame_bytes():
            raise Rejected("plugin_frame_too_large")

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        session.pending[req_id] = _PendingRpc(future=future, method=method, delivery_id=delivery_id)
        dispatched = False
        try:
            async with asyncio.timeout(effective_timeout):
                async with session.send_lock:
                    if session.closed or not session.ready:
                        raise Rejected("plugin_disconnected")
                    dispatched = True
                    await session.send(frame)
                return await future
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise Rejected("plugin_rpc_timeout") from None
        except Rejected:
            raise
        except Exception:
            raise Rejected(
                "plugin_disconnected" if dispatched else "plugin_not_dispatched"
            ) from None
        finally:
            session.pending.pop(req_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    def is_plugin_online(self, plugin_id: str, *, require_enabled: bool = True) -> bool:
        """Return True when `plugin_id` has an active ready session (and is enabled if required)."""
        if not isinstance(plugin_id, str) or not plugin_id:
            return False
        sess = self._sessions.get(plugin_id)
        if sess is None or not sess.ready or sess.closed:
            return False
        if require_enabled and (sess.proxy is None or not sess.proxy.enabled):
            return False
        return True

    def has_tool(self, plugin_id: str, action_name: str, *, require_enabled: bool = True) -> bool:
        """Return True when `plugin_id` is online and declares `action_name` under `actions`."""
        if not isinstance(action_name, str) or not action_name:
            return False
        if not self.is_plugin_online(plugin_id, require_enabled=require_enabled):
            return False
        sess = self._sessions.get(plugin_id)
        if sess is None or "actions" not in sess.scopes or sess.proxy is None:
            return False
        return any(spec.name == action_name for spec in sess.proxy.actions())

    def get_plugin_runtime_status(
        self, plugin_id: str, *, require_enabled: bool = True
    ) -> dict[str, Any] | None:
        """Return sanitized runtime region and pushed status dict for an online plugin."""
        if not self.is_plugin_online(plugin_id, require_enabled=require_enabled):
            return None
        sess = self._sessions.get(plugin_id)
        if sess is None or sess.proxy is None:
            return None
        return {
            "online": True,
            "node_region": sess.node_region,
            "status": sess.proxy.status_dict(),
        }

    async def invoke_tool(
        self,
        plugin_id: str,
        action_name: str,
        arguments: dict[str, Any] | None = None,
        *,
        blob: bytes | None = None,
        blob_mime_type: str = "application/octet-stream",
        blob_filename: str = "",
        max_request_blob_bytes: int = 15 * 1024 * 1024,
        max_response_blob_bytes: int = 0,
        timeout: float | None = None,  # noqa: ASYNC109
        require_enabled: bool = True,
    ) -> ToolCallResult:
        """Invoke a declared remote plugin tool with bounded bidirectional chunked blob transfer."""
        if not isinstance(plugin_id, str) or not PLUGIN_ID_RE.fullmatch(plugin_id):
            raise Rejected("invalid_plugin_id")
        if not isinstance(action_name, str) or not _ACTION_NAME_RE.fullmatch(action_name):
            raise Rejected("plugin_invalid_method")
        if arguments is not None and not isinstance(arguments, dict):
            raise Rejected("plugin_invalid_params")
        sess = self._sessions.get(plugin_id)
        if sess is None or not sess.ready or sess.closed:
            raise Rejected("plugin_offline")
        if "actions" not in sess.scopes:
            raise Rejected("plugin_scope_rejected")
        proxy = sess.proxy
        if proxy is None or (require_enabled and not proxy.enabled):
            raise Rejected("plugin_disabled")
        if not any(spec.name == action_name for spec in proxy.actions()):
            raise Rejected("action_not_found")

        req_cap = min(max(0, int(max_request_blob_bytes)), BLOB_HARD_MAX_BYTES)
        resp_cap = min(max(0, int(max_response_blob_bytes)), BLOB_HARD_MAX_BYTES)
        declared_timeout = proxy._action_timeouts.get(action_name)
        effective_timeout = float(
            timeout
            if timeout is not None
            else (declared_timeout if declared_timeout is not None else self.rpc_timeout)
        )
        if not math.isfinite(effective_timeout) or effective_timeout <= 0:
            effective_timeout = self.rpc_timeout

        loop = asyncio.get_running_loop()
        deadline = loop.time() + effective_timeout
        rpc_params: dict[str, Any] = {
            "action": action_name,
            "arguments": dict(arguments or {}),
            "origin": "kernel",
            "target_scope": "",
            "now": self.clock(),
        }

        uploaded_transfer_id: str | None = None
        if blob is not None:
            blob_desc, uploaded_transfer_id = await self._prepare_request_blob(
                sess,
                blob,
                rpc_params=rpc_params,
                blob_mime_type=blob_mime_type,
                blob_filename=blob_filename,
                req_cap=req_cap,
                deadline=deadline,
            )
            rpc_params[BLOB_FIELD] = blob_desc

        try:
            rem = deadline - loop.time()
            if rem <= 0:
                raise Rejected("plugin_rpc_timeout")
            resp = await self._rpc(sess, "action.run", rpc_params, timeout=rem)
        except Exception:
            if uploaded_transfer_id is not None:
                await self._best_effort_release_blob(sess, transfer_id=uploaded_transfer_id)
            raise

        if not resp.get("ok", False):
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_action_failed"
            raise Rejected(code)

        raw_res = resp.get("result") if "result" in resp else resp.get("data")
        if raw_res is None:
            res_dict: dict[str, Any] = {}
        elif isinstance(raw_res, dict):
            res_dict = dict(raw_res)
        else:
            res_dict = {"value": raw_res}

        resp_blob, resp_mime, resp_filename = await self._extract_response_blob(
            sess,
            res_dict,
            resp_cap=resp_cap,
            deadline=deadline,
        )

        clean_data = _scrub_json_secrets(res_dict, self._all_secrets())
        if not isinstance(clean_data, dict):
            clean_data = {"value": clean_data}
        return ToolCallResult(
            data=clean_data,
            blob=resp_blob,
            blob_mime_type=resp_mime,
            blob_filename=resp_filename,
        )

    def _build_actions(
        self,
        session: _PluginSession,
        decls: list[_PluginActionDecl],
    ) -> tuple[tuple[Action, ...], dict[str, float]]:
        registry = getattr(self.service, "modules", None)
        existing_actions = set(registry.catalog.actions) if registry is not None else set()
        existing_aliases = set(registry.catalog.aliases) if registry is not None else set()
        seen_names: set[str] = set()
        seen_aliases: set[tuple[str, str, bool]] = set()
        actions: list[Action] = []
        action_timeouts: dict[str, float] = {}
        for decl in decls:
            prefix = decl.name.split(".", 1)[0]
            required_owner = _SCOPED_ACTION_PREFIX_OWNERS.get(prefix)
            if prefix in _RESERVED_ACTION_PREFIXES or (
                required_owner is not None and session.plugin_id != required_owner
            ):
                raise Rejected("plugin_scope_rejected")
            if decl.budget_lane == PATROL_LANE and session.plugin_id != "bilibili_surf":
                raise Rejected("plugin_scope_rejected")
            if decl.name in seen_names or decl.name in existing_actions:
                raise Rejected("plugin_duplicate_action")
            seen_names.add(decl.name)
            if decl.social and "chat" not in session.scopes:
                raise Rejected("plugin_scope_rejected")
            args_model = _build_action_args_model(decl.name, decl.arguments)
            origins = (
                frozenset(decl.origins)
                if decl.origins is not None
                else frozenset({"planner", "admin"})
            )
            if not origins:
                raise Rejected("plugin_invalid_schema")
            if "query" in origins and (
                decl.social
                or decl.owner_only
                or decl.budget_lane is not None
                or bool(decl.quotas)
                or args_model is not NoArgs
            ):
                raise Rejected("plugin_invalid_schema")
            parsed_quotas: tuple[ActivityQuota, ...] | None
            if decl.quotas is None:
                parsed_quotas = () if "query" in origins else None
            else:
                if len(decl.quotas) > 4:
                    raise Rejected("plugin_invalid_schema")
                q_list: list[ActivityQuota] = []
                for q_item in decl.quotas:
                    if not isinstance(q_item, dict):
                        raise Rejected("plugin_invalid_schema")
                    if set(q_item) - {"name", "unlimited_flag"} or "name" not in q_item:
                        raise Rejected("plugin_invalid_schema")
                    q_name = q_item["name"]
                    if (
                        not isinstance(q_name, str)
                        or not _FIELD_NAME_RE.fullmatch(q_name)
                        or q_name in {"total", "counted"}
                    ):
                        raise Rejected("plugin_invalid_schema")
                    q_flag = q_item.get("unlimited_flag", "")
                    if not isinstance(q_flag, str) or (
                        q_flag and not _FIELD_NAME_RE.fullmatch(q_flag)
                    ):
                        raise Rejected("plugin_invalid_schema")
                    q_list.append(ActivityQuota(counter=q_name, unlimited_option=q_flag))
                parsed_quotas = tuple(q_list)
            parsed_aliases: list[tuple[str, str, bool]] = []
            for raw_alias in decl.aliases:
                if not isinstance(raw_alias, list) or len(raw_alias) != 3:
                    raise Rejected("plugin_invalid_schema")
                target_mod, action_type, has_recipient = raw_alias
                if (
                    not isinstance(target_mod, str)
                    or not isinstance(action_type, str)
                    or not isinstance(has_recipient, bool)
                ):
                    raise Rejected("plugin_invalid_schema")
                if target_mod != session.plugin_id:
                    raise Rejected("plugin_scope_rejected")
                if not _ALIAS_VERB_RE.fullmatch(action_type) or has_recipient != decl.social:
                    raise Rejected("plugin_invalid_schema")
                alias = (target_mod, action_type, has_recipient)
                if alias in seen_aliases or alias in existing_aliases:
                    raise Rejected("plugin_duplicate_action")
                seen_aliases.add(alias)
                parsed_aliases.append(alias)
            desc = sanitize_sensor_text(clean_text(decl.description), self.s, max_tokens=80)
            if not desc:
                raise Rejected("plugin_invalid_schema")
            if decl.timeout_seconds is not None:
                action_timeouts[decl.name] = float(decl.timeout_seconds)

            async def _run(
                call: ActionCall,
                action_name: str = decl.name,
                action_timeout: float | None = decl.timeout_seconds,
            ) -> Outcome:
                proxy = session.proxy
                if proxy is None or not proxy.enabled:
                    return Outcome("gated_skip", "module_disabled")
                if not call.still_allowed():
                    return Outcome("gated_skip", "action_no_longer_eligible")
                args_dict = call.args.model_dump(mode="json")
                rpc_params: dict[str, Any] = {
                    "action": action_name,
                    "arguments": args_dict,
                    "origin": str(call.ticket.get("origin") or "planner"),
                    "target_scope": str(call.ticket.get("target_scope") or ""),
                    "now": call.now,
                }
                if exit_reason := str(call.ticket.get("exit_reason") or "").strip():
                    clean_exit_reason = sanitize_sensor_text(exit_reason, self.s, max_tokens=12)[
                        :32
                    ]
                    if clean_exit_reason:
                        rpc_params["exit_reason"] = clean_exit_reason
                try:
                    resp = await self._rpc(
                        session,
                        "action.run",
                        rpc_params,
                        timeout=action_timeout,
                    )
                except Rejected as exc:
                    code = str(exc)
                    if code in {"plugin_disconnected", "plugin_rpc_timeout"}:
                        return Outcome("uncertain", code)
                    return Outcome("not_dispatched", code)
                if not resp.get("ok", False):
                    raw_err = str(resp.get("error") or "")
                    reason = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_action_failed"
                    return Outcome("not_dispatched", reason)
                if "result" in resp and resp["result"] is not None:
                    raw_res = resp["result"]
                elif "data" in resp and resp["data"] is not None:
                    raw_res = resp["data"]
                elif any(k in resp for k in ("status", "reason", "details")):
                    raw_res = {k: v for k, v in resp.items() if k not in {"type", "id", "ok"}}
                else:
                    raw_res = resp.get("result")
                if isinstance(raw_res, dict):
                    raw_status = str(raw_res.get("status") or "done")
                    status = raw_status if raw_status in _VALID_OUTCOME_STATUSES else "done"
                    raw_reason = str(raw_res.get("reason") or "ok")
                    reason = raw_reason if _CODE_RE.fullmatch(raw_reason) else "ok"
                    raw_details = raw_res.get("details")
                    if isinstance(raw_details, dict):
                        details = _sanitize_payload(raw_details, self.s)
                    else:
                        extra = {
                            k: v
                            for k, v in raw_res.items()
                            if k not in {"status", "reason", "details"}
                        }
                        details = _sanitize_payload(extra, self.s) if extra else {}
                    return Outcome(status=status, reason=reason, details=details)  # type: ignore[arg-type]
                if raw_res is None:
                    return Outcome("done", "ok")
                return Outcome(
                    "done",
                    "ok",
                    details={"summary": sanitize_sensor_text(str(raw_res), self.s, max_tokens=90)},
                )

            actions.append(
                Action(
                    name=decl.name,
                    description=desc,
                    run=_run,
                    arguments=args_model,
                    social=decl.social,
                    owner_only=decl.owner_only,
                    budget_lane=decl.budget_lane,
                    effortful=decl.effortful,
                    interrupts_focus=decl.interrupts_focus,
                    window=decl.window,
                    quotas=parsed_quotas,
                    origins=origins,
                    aliases=tuple(parsed_aliases),
                    unavailable=lambda s=session, n=decl.name: (
                        "plugin_offline"
                        if not (s.ready and not s.closed)
                        else (s.proxy._action_unavailable.get(n) if s.proxy is not None else None)
                    ),
                )
            )
        return tuple(actions), action_timeouts

    def _build_thinking_scenes(
        self,
        plugin_id: str,
        decls: list[_PluginThinkingSceneDecl],
    ) -> tuple[ThinkingScene, ...]:
        registry = getattr(self.service, "modules", None)
        existing_keys = set(registry.thinking_scenes) if registry is not None else set()
        seen: set[str] = set()
        scenes: list[ThinkingScene] = []
        for decl in decls:
            if decl.key in _SCOPED_SCENE_OWNERS:
                if (
                    plugin_id != _SCOPED_SCENE_OWNERS[decl.key]
                    or decl.key in seen
                    or decl.key in existing_keys
                ):
                    raise Rejected("plugin_scope_rejected")
            else:
                prefix = decl.key.split(".", 1)[0]
                required_owner = _SCOPED_ACTION_PREFIX_OWNERS.get(prefix)
                if (
                    decl.key in _RESERVED_SCENE_KEYS
                    or decl.key in seen
                    or decl.key in existing_keys
                    or prefix in _RESERVED_ACTION_PREFIXES
                    or (required_owner is not None and plugin_id != required_owner)
                ):
                    raise Rejected("plugin_scope_rejected")
            seen.add(decl.key)
            clean_label = sanitize_sensor_text(clean_text(decl.label), self.s, max_tokens=32)[:80]
            if not clean_label:
                raise Rejected("plugin_invalid_schema")
            try:
                scene = ThinkingScene(
                    key=decl.key,
                    label=clean_label,
                    default=decl.default,
                    levels=tuple(decl.levels),
                    output_tokens=decl.output_tokens,
                    output_cap=decl.output_cap,
                )
            except ValueError:
                raise Rejected("plugin_invalid_schema") from None
            scenes.append(scene)
        return tuple(scenes)

    def _validate_hello(
        self,
        raw_frame: dict[str, Any],
        cred: dict[str, Any],
        *,
        plugin_id: str,
    ) -> tuple[_PluginHelloFrame, frozenset[str], tuple[str, ...], tuple[str, ...]]:
        try:
            hello = _PluginHelloFrame.model_validate(raw_frame)
        except ValidationError:
            raise Rejected("plugin_invalid_schema") from None

        try:
            requested_scopes = _validate_scopes(hello.scopes)
        except Rejected:
            raise Rejected("plugin_scope_rejected") from None

        allowed_scopes = set(cred["allowed_scopes"])
        scope_set = frozenset(requested_scopes)
        if not scope_set.issubset(allowed_scopes):
            raise Rejected("plugin_scope_rejected")
        if hello.actions and "actions" not in scope_set:
            raise Rejected("plugin_scope_rejected")
        if hello.thinking_scenes and "thinking_scenes" not in scope_set:
            raise Rejected("plugin_scope_rejected")
        if hello.chat_platforms and "chat" not in scope_set:
            raise Rejected("plugin_scope_rejected")

        registry = getattr(self.service, "modules", None)
        existing_platforms = set(registry.chat_platforms) if registry is not None else set()
        seen_platforms: set[str] = set()
        clean_platforms: list[str] = []
        for plat in hello.chat_platforms:
            if not isinstance(plat, str) or not PLATFORM_ID_RE.fullmatch(plat):
                raise Rejected("plugin_invalid_schema")
            if (
                plat in RESERVED_CHAT_PLATFORMS
                or plat in seen_platforms
                or plat in existing_platforms
            ):
                raise Rejected("plugin_scope_rejected")
            seen_platforms.add(plat)
            clean_platforms.append(plat)

        clean_caps: list[str] = []
        seen_caps: set[str] = set()
        for cap in hello.capabilities:
            if not isinstance(cap, str) or not _CAPABILITY_RE.fullmatch(cap):
                raise Rejected("plugin_invalid_schema")
            if cap not in seen_caps:
                seen_caps.add(cap)
                clean_caps.append(cap)

        if hello.admin_panel is not None:
            if _HOTPLUG_ADMIN_PANELS.get(plugin_id) == hello.admin_panel:
                pass
            elif hello.admin_panel in _CORE_ADMIN_PANELS:
                raise Rejected("plugin_scope_rejected")

        return hello, scope_set, tuple(clean_platforms), tuple(clean_caps)

    async def serve(
        self,
        receive: Callable[[], Awaitable[str | bytes]],
        send: Callable[[str], Awaitable[None]],
        close: Callable[[int], Awaitable[None]],
        *,
        source_ip: str,
        plugin_id: str,
    ) -> None:
        await self.initialize()
        try:
            pid = _validate_plugin_id(plugin_id)
            self._ensure_plugin_not_reserved(pid)
        except Rejected as exc:
            log.warning("event=plugin_scope_rejected plugin=%s reason=%s", plugin_id, str(exc))
            try:
                await close(1008)
            except Exception:
                pass
            return

        cred = await self._load_cred(pid)
        if cred is None or not cred["enabled"]:
            log.warning("event=plugin_scope_rejected plugin=%s reason=unauthorized", pid)
            try:
                await close(1008)
            except Exception:
                pass
            return

        max_frame_bytes = self._max_frame_bytes()
        hs_lock = asyncio.Lock()
        loop = asyncio.get_running_loop()
        last_rx = loop.time()

        # Phase 1: Wait for valid plugin.hello (allowing ping/pong during handshake).
        hello_raw: dict[str, Any] | None = None
        hs_deadline = loop.time() + self.handshake_timeout
        try:
            while True:
                remaining = max(0.01, hs_deadline - loop.time())
                async with asyncio.timeout(remaining):
                    raw = await receive()
                last_rx = loop.time()
                frame = decode_ws_json_frame(raw, max_frame_bytes, error_prefix="plugin")
                ftype = frame.get("type")
                if ftype == "ping":
                    await self._send_raw(send, hs_lock, {"type": "pong", "ts": self.clock()})
                    continue
                if ftype != "plugin.hello":
                    raise Rejected("plugin_scope_rejected")
                hello_raw = frame
                break
        except asyncio.CancelledError:
            raise
        except (TimeoutError, Rejected) as exc:
            reason = "plugin_handshake_timeout" if isinstance(exc, TimeoutError) else str(exc)
            log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, reason)
            try:
                await self._send_raw(send, hs_lock, {"type": "error", "reason": reason})
            except Exception:
                pass
            try:
                await close(1008)
            except Exception:
                pass
            return
        except Exception:
            try:
                await close(1012)
            except Exception:
                pass
            return

        # Phase 2: Validate credential, scope enforcement, and mount RemoteModuleProxy under fiber.
        session: _PluginSession | None = None
        ctx: PluginContext | None = None
        async with self._connect_lock(pid):
            cred = await self._load_cred(pid)
            if cred is None or not cred["enabled"]:
                log.warning("event=plugin_scope_rejected plugin=%s reason=disabled", pid)
                try:
                    await self._send_raw(
                        send, hs_lock, {"type": "error", "reason": "plugin_disabled"}
                    )
                except Exception:
                    pass
                try:
                    await close(1008)
                except Exception:
                    pass
                return

            if pid in self._sessions:
                await self._dispose_session(pid, close_code=1012)

            try:
                self._ensure_plugin_not_reserved(pid)
                hello, scope_set, chat_platforms, capabilities = self._validate_hello(
                    hello_raw, cred, plugin_id=pid
                )
            except Rejected as exc:
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, str(exc))
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                try:
                    await self._send_raw(send, hs_lock, {"type": "error", "reason": str(exc)})
                except Exception:
                    pass
                try:
                    await close(1008)
                except Exception:
                    pass
                return

            kernel = self._kernel()
            if f"remote:{pid}" in kernel.fibers():
                await kernel.dispose_fiber(f"remote:{pid}")

            ctx = kernel.fork(f"remote:{pid}")
            now = self.clock()
            session = _PluginSession(
                plugin_id=pid,
                receive=receive,
                send=send,
                close=close,
                source_ip=source_ip,
                fiber=ctx.fiber,
                ctx=ctx,
                scopes=scope_set,
                node_region=cred["node_region"],
                connected_at=now,
                last_seen_at=now,
                event_subscriptions=frozenset(hello.event_subscriptions),
            )

            # Register LIFO cleanup effects (reverse execution order:
            # 3rd registered -> runs 1st: cancel pending RPCs + mark in-flight deliveries uncertain
            # 2nd registered -> runs 2nd: unregister proxy & refresh _thinking_scenes
            # 1st registered -> runs 3rd: clear session entry, close WS, log disconnected)
            async def _cleanup_session_and_close(sess: _PluginSession = session) -> None:
                if self._sessions.get(sess.plugin_id) is sess:
                    self._sessions.pop(sess.plugin_id, None)
                sess._bili_private_handles.clear()
                sess.ready = False
                if not sess.closed:
                    sess.closed = True
                    try:
                        async with asyncio.timeout(2.0):
                            await sess.close(sess.close_code)
                    except Exception:
                        pass
                log.info("event=plugin_disconnected plugin=%s", sess.plugin_id)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_disconnected", emit_audit=False)

            def _unregister_proxy(sess: _PluginSession = session) -> None:
                modules = getattr(self.service, "modules", None)
                if modules is not None and sess.proxy is not None:
                    try:
                        if modules.get(sess.plugin_id) is sess.proxy:
                            modules.unregister(sess.plugin_id)
                    finally:
                        self.s._thinking_scenes = modules.thinking_scenes

            def _cancel_pending_rpcs(sess: _PluginSession = session) -> None:
                sess.ready = False
                for pending in list(sess.pending.values()):
                    if pending.delivery_id is not None:
                        sess.uncertain_deliveries.add(pending.delivery_id)
                        self.uncertain_deliveries.append(pending.delivery_id)
                    if not pending.future.done():
                        err = (
                            "plugin_delivery_uncertain"
                            if pending.delivery_id is not None
                            else "plugin_disconnected"
                        )
                        pending.future.set_exception(Rejected(err))

            ctx.effect(_cleanup_session_and_close)
            ctx.effect(_unregister_proxy)
            ctx.effect(_cancel_pending_rpcs)

            try:
                actions, action_timeouts = self._build_actions(session, hello.actions)
                if hello.session_exit_action is not None:
                    exit_spec = next(
                        (spec for spec in actions if spec.name == hello.session_exit_action),
                        None,
                    )
                    if exit_spec is None or exit_spec.social:
                        raise Rejected("plugin_invalid_schema")
                    try:
                        exit_spec.arguments()
                    except (ValidationError, TypeError, ValueError):
                        raise Rejected("plugin_invalid_schema") from None
                scenes = self._build_thinking_scenes(pid, hello.thinking_scenes)
                saved_enabled = await self._load_module_switch(pid)
                clean_disp = sanitize_sensor_text(
                    clean_text(hello.display_name), self.s, max_tokens=32
                )[:64]
                clean_desc = sanitize_sensor_text(
                    clean_text(hello.description), self.s, max_tokens=80
                )[:240]
                proxy = RemoteModuleProxy(
                    self.s,
                    self,
                    session,
                    plugin_id=pid,
                    display_name=clean_disp or cred["display_name"],
                    description=clean_desc,
                    node_region=cred["node_region"],
                    scopes=scope_set,
                    chat_platforms=chat_platforms,
                    thinking_scenes=scenes,
                    actions=actions,
                    capabilities=capabilities,
                    enabled=saved_enabled,
                    attention_level=hello.attention_level,
                    admin_panel=hello.admin_panel,
                    configuration_issue=hello.configuration_issue,
                    initial_status=hello.initial_status,
                    action_timeouts=action_timeouts,
                    session_exit_action=hello.session_exit_action,
                    voice_format=hello.voice_format,
                )
                session.proxy = proxy
                modules = getattr(self.service, "modules", None)
                if modules is not None:
                    modules.register(proxy)
                    self.s._thinking_scenes = modules.thinking_scenes
                self._sessions[pid] = session
                session.ready = True
                await self._touch_last_seen(pid, now)
            except (Rejected, ValueError) as exc:
                reason = str(exc) if isinstance(exc, Rejected) else "plugin_registration_conflict"
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, reason)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                session.close_code = 1008
                await self._send_frame(session, {"type": "error", "reason": reason})
                await ctx.fiber.dispose()
                return

        enabled_now = session.proxy.enabled if session.proxy else True
        scopes_now = sorted(session.scopes)
        await self._send_frame(
            session,
            {
                "type": "plugin.welcome",
                "plugin_id": pid,
                "scopes": scopes_now,
                "enabled": enabled_now,
            },
        )
        await self._send_frame(
            session,
            {
                "type": "plugin.ready",
                "plugin_id": pid,
                "protocol_version": 1,
                "enabled": enabled_now,
                "scopes": scopes_now,
                "node_region": session.node_region,
            },
        )
        nt = getattr(self.service, "node_transport", None)
        if (
            nt is not None
            and "node_bridge" in session.scopes
            and callable(getattr(nt, "connected", None))
        ):
            await self._send_frame(
                session,
                {"type": "node.state", "connected": bool(nt.connected())},
            )

        # Phase 3: Active message loop, 20s heartbeat, and 60s idle timeout on tracked fiber tasks.
        async def _reader() -> None:
            nonlocal last_rx
            while True:
                remaining = max(0.01, self.idle_timeout - (loop.time() - last_rx))
                try:
                    async with asyncio.timeout(remaining):
                        raw = await receive()
                except TimeoutError:
                    if (loop.time() - last_rx) >= self.idle_timeout:
                        session.close_code = 1000
                        return
                    continue
                last_rx = loop.time()
                session.last_seen_at = self.clock()
                frame = decode_ws_json_frame(raw, max_frame_bytes, error_prefix="plugin")
                ftype = frame.get("type")
                if ftype == "ping":
                    pong: dict[str, Any] = {"type": "pong", "ts": self.clock()}
                    if isinstance(frame.get("id"), str):
                        pong["id"] = frame["id"]
                    await self._send_frame(session, pong)
                elif ftype == "pong":
                    continue
                elif ftype == "rpc.result":
                    req_id = frame.get("id")
                    pending = session.pending.get(req_id) if isinstance(req_id, str) else None
                    if pending is not None and not pending.future.done():
                        pending.future.set_result(frame)
                elif ftype == "world.event":
                    await self._handle_world_event(session, frame)
                elif ftype == "chat.inbound":
                    await self._handle_chat_inbound(session, frame)
                elif ftype == "owner.notify":
                    await self._handle_owner_notify(session, frame)
                elif ftype == "ai.budget_check":
                    await self._handle_ai_budget_check(session, frame)
                elif ftype == "ai.screen":
                    await self._handle_ai_screen(session, frame)
                elif ftype == "ai.generate":
                    await self._handle_ai_generate(session, frame)
                elif ftype == "ai.video_analyze":
                    await self._handle_ai_video_analyze(session, frame)
                elif ftype == "state.get":
                    await self._handle_state_get(session, frame)
                elif ftype == "state.set":
                    await self._handle_state_set(session, frame)
                elif ftype == "state.delete":
                    await self._handle_state_delete(session, frame)
                elif ftype == "node.call":
                    await self._handle_node_call(session, frame)
                elif ftype == "plugin.status":
                    await self._handle_plugin_status(session, frame)
                elif ftype == "world.activity":
                    await self._handle_world_activity(session, frame)
                elif ftype == "world.reaction":
                    await self._handle_world_reaction(session, frame)
                elif ftype == "bili.memory_preferences":
                    await self._handle_bili_memory_preferences(session, frame)
                elif ftype == "bili.private_topic_commit":
                    await self._handle_bili_private_topic_commit(session, frame)
                else:
                    raise Rejected("plugin_unknown_frame")

        async def _heartbeat() -> None:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                await self._send_frame(session, {"type": "ping", "ts": self.clock()})

        reader_task = ctx.spawn(_reader(), name=f"plugin-reader:{pid}")
        heartbeat_task = ctx.spawn(_heartbeat(), name=f"plugin-heartbeat:{pid}")
        try:
            done, _ = await asyncio.wait(
                [reader_task, heartbeat_task], return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if not t.cancelled():
                    t.result()
        except asyncio.CancelledError:
            raise
        except Rejected as exc:
            code = str(exc)
            if code == "plugin_scope_rejected":
                log.warning("event=plugin_scope_rejected plugin=%s reason=frame_scope", pid)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                session.close_code = 1008
            elif code in {"plugin_invalid_schema", "plugin_invalid_json", "plugin_frame_too_large"}:
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, code)
                session.close_code = 1008
            else:
                session.close_code = 1012
            await self._send_frame(session, {"type": "error", "reason": code})
        except Exception:
            session.close_code = 1012
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await self._touch_last_seen(pid, session.last_seen_at)
                except Exception:
                    pass
                await ctx.fiber.dispose()
