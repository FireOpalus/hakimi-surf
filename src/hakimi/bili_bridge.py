"""Kernel bridge proxies for external `bilibili_surf` hot-plug plugin."""

from __future__ import annotations

import asyncio
import base64
import inspect
import logging
import math
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from hakimi_plugin_sdk.bili_common import (
    BVID,
    BVID_RE,
    COVER_URL_RE,
    CREDENTIAL_KEYS,
    MAX_COVER_THUMB_BYTES,
    MID_RE,
    PUBLIC_DEGRADE_ERRORS,
    RISK_CODES,
    RISK_COOLDOWN_SECONDS,
    validate_cookie,
)

from .models import Message, Video
from .plugin_bus import RemoteModuleProxy
from .roaming_logger import redact_public_summary
from .security import Rejected, clean_text, configured_secrets, safe_output

log = logging.getLogger("hakimi")

_RISK_CODES = RISK_CODES
_RPC_TRANSPORT_ERRORS = frozenset({"plugin_disconnected", "plugin_rpc_timeout"})
_PUBLIC_DEGRADE_ERRORS = (
    PUBLIC_DEGRADE_ERRORS | _RPC_TRANSPORT_ERRORS | frozenset({"bilibili_unavailable"})
)
_REVOCATION_ERRORS = frozenset(
    {
        "module_disabled",
        "bili_settings_changed",
        "bili_request_revoked",
        "video_watch_invalidated",
    }
)
_validate_cookie_str = validate_cookie

__all__ = [
    "BVID",
    "BVID_RE",
    "COVER_URL_RE",
    "CREDENTIAL_KEYS",
    "MAX_COVER_THUMB_BYTES",
    "MID_RE",
    "RISK_COOLDOWN_SECONDS",
    "RemoteBiliClient",
    "RemoteBiliWatcher",
]


def _secret_str(raw: Any) -> str:
    return raw.get_secret_value() if hasattr(raw, "get_secret_value") else str(raw or "")


def _to_video(raw: Any, *, expected_bvid: str | None = None) -> Video:
    if not isinstance(raw, dict):
        raise Rejected("bilibili_schema_changed")
    try:
        bv = str(raw["bvid"])
        if expected_bvid is not None and bv != expected_bvid:
            raise ValueError("bvid_mismatch")
        raw_pub = raw.get("published_at", 0)
        if isinstance(raw_pub, bool):
            raise ValueError("invalid_published_at")
        return Video(
            bvid=bv,
            mid=str(raw["mid"]),
            title=clean_text(str(raw["title"]))[:200],
            description=clean_text(str(raw.get("description", "")))[:2000],
            author=clean_text(str(raw["author"]))[:100],
            published_at=int(raw_pub),
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise Rejected("bilibili_schema_changed") from None


def _clean_policy_dict(raw: dict[str, Any], env_policy: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabled": bool(raw.get("enabled", env_policy["enabled"])),
        "allow_like": bool(raw.get("allow_like", False)),
        "allow_favorite": bool(raw.get("allow_favorite", False)),
        "favorite_folder_id": str(raw.get("favorite_folder_id") or ""),
        "daily_action_limit": int(raw.get("daily_action_limit", 1)),
    }


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    fval = float(value)
    return fval if math.isfinite(fval) else None


def _private_priority_visible(
    meta: dict[str, Any] | None, *, session_scope: str, now: float
) -> tuple[bool, bool]:
    if not isinstance(meta, dict):
        return True, False
    p_scope = str(meta.get("private_scope") or "")
    exp = _finite_float(meta.get("private_expires_at")) or 0.0
    if p_scope and exp > now:
        if session_scope and session_scope == p_scope:
            return True, True
        return False, False
    return True, False


class RemoteBiliClient:
    """Kernel-side proxy delegating public Bilibili lookups to external `bilibili_surf`."""

    def __init__(self, service: Any) -> None:
        self.svc = service
        self.s = service.s
        self.last_request: float = 0.0
        self.blocked_until: float = 0.0

    def _resolve_proxy(self) -> RemoteModuleProxy:
        mod = self.svc.modules.get("bilibili_surf")
        if mod is not None and not getattr(mod, "enabled", True):
            raise Rejected("module_disabled")
        if not getattr(self.svc.s, "bilibili_enabled", True):
            raise Rejected("bilibili_disabled")
        if not isinstance(mod, RemoteModuleProxy) or not mod._session.ready or mod._session.closed:
            raise Rejected("bilibili_unavailable")
        return mod

    async def _call_rpc(
        self, method: str, params: dict[str, Any], *, rpc_timeout: float = 15.0
    ) -> Any:
        proxy = self._resolve_proxy()
        try:
            return await proxy.call_raw_rpc(method, params, timeout=rpc_timeout)
        except Rejected as exc:
            if str(exc) in _RPC_TRANSPORT_ERRORS:
                raise Rejected("bilibili_unavailable") from None
            raise

    async def resolve_short(self, url: str) -> str | None:
        self._resolve_proxy()
        if not isinstance(url, str) or not url.strip():
            raise Rejected("invalid_bilibili_short_link")
        res = await self._call_rpc("bili.resolve_short", {"url": url})
        if res is None:
            return None
        if isinstance(res, dict):
            raw_bvid = res.get("bvid")
        elif isinstance(res, str):
            raw_bvid = res
        else:
            raise Rejected("bilibili_short_link_unavailable")
        if raw_bvid is None or raw_bvid == "":
            return None
        if not isinstance(raw_bvid, str) or not BVID_RE.fullmatch(raw_bvid):
            raise Rejected("bilibili_short_link_target_forbidden")
        return raw_bvid

    async def video(self, bvid: str) -> Video:
        if not isinstance(bvid, str) or not BVID_RE.fullmatch(bvid):
            raise Rejected("invalid_bvid")
        res = await self._call_rpc("bili.video", {"bvid": bvid})
        return _to_video(res, expected_bvid=bvid)

    async def updates(self, mid: str) -> list[Video]:
        if not isinstance(mid, str) or not MID_RE.fullmatch(mid):
            raise Rejected("invalid_mid")
        res = await self._call_rpc("bili.updates", {"mid": mid})
        if not isinstance(res, dict) or not isinstance(res.get("videos"), list):
            raise Rejected("bilibili_schema_changed")
        videos: dict[str, Video] = {}
        for item in res["videos"][:50]:
            video = _to_video(item)
            if video.mid != mid:
                raise Rejected("bilibili_schema_changed")
            videos[video.bvid] = video
        return sorted(videos.values(), key=lambda v: v.published_at, reverse=True)

    async def live_info(self, room_id: str) -> dict[str, Any]:
        if not isinstance(room_id, str) or not MID_RE.fullmatch(room_id):
            raise Rejected("invalid_room_id")
        res = await self._call_rpc("bili.live_info", {"room_id": room_id})
        if not isinstance(res, dict):
            raise Rejected("bilibili_schema_changed")
        status = res.get("live_status")
        if isinstance(status, bool) or status not in {0, 1, 2}:
            raise Rejected("invalid_live_status")
        return {
            "room_id": room_id,
            "live_status": int(status),
            "title": clean_text(str(res.get("title", "")))[:120],
            "area": clean_text(str(res.get("area", "")))[:40],
        }

    async def live_status(self, room_id: str) -> int:
        return int((await self.live_info(room_id))["live_status"])


class RemoteBiliWatcher:
    """Kernel-side proxy for Bilibili small-account patrol, KV credentials, and admin RPCs."""

    def __init__(self, service: Any, *, clock: Callable[[], float] = time.time) -> None:
        self.svc = service
        self.s = service.s
        self.store = service.store
        self.clock = clock
        self.zone = ZoneInfo(getattr(self.s, "bot_timezone", "Asia/Shanghai"))
        self.credentials_epoch: int = 0
        self.policy_epoch: int = 0
        self.blocked_until: float = 0.0
        self.last_request: float = 0.0
        self._cred_update_seq: int = 0
        self._cred_lock = asyncio.Lock()

    def _proxy(self) -> RemoteModuleProxy | None:
        mod = self.svc.modules.get("bilibili_surf")
        return mod if isinstance(mod, RemoteModuleProxy) else None

    def _online_proxy(self) -> RemoteModuleProxy | None:
        proxy = self._proxy()
        if proxy is not None and proxy._session.ready and not proxy._session.closed:
            return proxy
        return None

    def _inside_own_reader(self) -> bool:
        task = asyncio.current_task()
        return task is not None and task.get_name() == "plugin-reader:bilibili_surf"

    def _require_online_proxy(
        self,
        *,
        check_enabled: bool = False,
        unavailable_code: str = "bilibili_module_unavailable",
    ) -> RemoteModuleProxy:
        proxy = self._proxy()
        if check_enabled and proxy is not None and not proxy.enabled:
            raise Rejected("module_disabled")
        if proxy is None or not proxy._session.ready or proxy._session.closed:
            raise Rejected(unavailable_code)
        if check_enabled and not proxy.enabled:
            raise Rejected("module_disabled")
        return proxy

    def _all_secrets(self) -> list[str]:
        svc_secrets = getattr(self.svc, "secrets", None)
        base = list(svc_secrets) if isinstance(svc_secrets, list) else []
        for item in configured_secrets(self.s):
            if item and item not in base:
                base.append(item)
        return base

    def _protect_secrets(self, creds: dict[str, str]) -> None:
        cookie_vals = [
            str(creds.get(k) or "")
            for k in CREDENTIAL_KEYS
            if isinstance(creds.get(k), str) and creds.get(k)
        ]
        if not cookie_vals:
            return
        existing_extra = tuple(getattr(self.s, "_runtime_extra_secrets", ()) or ())
        self.s._runtime_extra_secrets = tuple(dict.fromkeys([*existing_extra, *cookie_vals]))
        svc_secrets = getattr(self.svc, "secrets", None)
        if isinstance(svc_secrets, list):
            for val in cookie_vals:
                if val not in svc_secrets:
                    svc_secrets.append(val)

    async def _table_exists(self, table_name: str) -> bool:
        row = await self.store.one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return row is not None

    async def _kv_get(self, state_key: str) -> Any | None:
        async with self.store.lock:
            if not await self._table_exists("plugin_kv_state"):
                return None
            row = await self.store.one(
                "SELECT payload FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
                ("bilibili_surf", state_key),
            )
        if row is None:
            return None
        try:
            return self.store.vault.open(f"plugin-kv:bilibili_surf:{state_key}", row[0])
        except Exception:
            return None

    async def _kv_set(self, state_key: str, value: Any) -> None:
        blob = self.store.vault.seal(f"plugin-kv:bilibili_surf:{state_key}", value)
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS plugin_kv_state ("
                "plugin_id TEXT NOT NULL, state_key TEXT NOT NULL, "
                "payload BLOB NOT NULL, updated REAL NOT NULL, "
                "PRIMARY KEY(plugin_id, state_key))"
            )
            await self.store.db.execute(
                "INSERT INTO plugin_kv_state(plugin_id, state_key, payload, updated) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(plugin_id, state_key) DO UPDATE SET "
                "payload=excluded.payload, updated=excluded.updated",
                ("bilibili_surf", state_key, blob, now),
            )

    async def _load_credentials_from_store(self) -> tuple[dict[str, str], str]:
        kv_creds = await self._kv_get("credentials")
        if isinstance(kv_creds, dict):
            creds_obj = (
                kv_creds.get("creds") if isinstance(kv_creds.get("creds"), dict) else kv_creds
            )
            creds: dict[str, str] = {}
            for key in CREDENTIAL_KEYS:
                raw_val = str(creds_obj.get(key) or "")
                try:
                    creds[key] = _validate_cookie_str(raw_val)
                except ValueError:
                    creds[key] = ""
            return creds, str(kv_creds.get("source") or "sqlite")

        async with self.store.lock:
            has_legacy = await self._table_exists("bili_credentials")
            row = (
                await self.store.one("SELECT payload FROM bili_credentials WHERE id=1")
                if has_legacy
                else None
            )
        if row is not None:
            try:
                saved = self.store.vault.open("bili-credentials", row[0])
            except Exception:
                saved = None
            if isinstance(saved, dict):
                creds = {}
                for key in CREDENTIAL_KEYS:
                    raw_val = str(saved.get(key) or "")
                    try:
                        creds[key] = _validate_cookie_str(raw_val)
                    except ValueError:
                        creds[key] = ""
                return creds, "sqlite"

        env_creds = {
            "sessdata": _secret_str(getattr(self.s, "bili_sessdata", "")),
            "bili_jct": _secret_str(
                getattr(self.s, "bili_bili_jct", getattr(self.s, "bili_jct", ""))
            ),
            "buvid3": _secret_str(getattr(self.s, "bili_buvid3", "")),
            "buvid4": _secret_str(getattr(self.s, "bili_buvid4", "")),
        }
        cleaned_env: dict[str, str] = {}
        for key, raw_val in env_creds.items():
            try:
                cleaned_env[key] = _validate_cookie_str(raw_val)
            except ValueError:
                cleaned_env[key] = ""
        return cleaned_env, "env"

    async def _load_account_state_from_store(self) -> dict[str, Any] | None:
        kv_state = await self._kv_get("account_state")
        if isinstance(kv_state, dict):
            return dict(kv_state)
        async with self.store.lock:
            has_acc = await self._table_exists("bili_account_state")
            has_dev = await self._table_exists("bili_device_state")
            acc_row = (
                await self.store.one(
                    "SELECT blocked_until, last_error, checked_at, account "
                    "FROM bili_account_state WHERE id=1"
                )
                if has_acc
                else None
            )
            dev_row = (
                await self.store.one(
                    "SELECT fingerprint, attempted_at, status FROM bili_device_state WHERE id=1"
                )
                if has_dev
                else None
            )
        if acc_row is None and dev_row is None:
            return None
        opened_account: Any = None
        if acc_row is not None and acc_row["account"] is not None:
            try:
                opened_account = self.store.vault.open("bili-account", acc_row["account"])
            except Exception:
                opened_account = None
        return {
            "blocked_until": float(acc_row["blocked_until"]) if acc_row else 0.0,
            "last_error": acc_row["last_error"] if acc_row else None,
            "checked_at": (
                float(acc_row["checked_at"])
                if acc_row and acc_row["checked_at"] is not None
                else None
            ),
            "account": opened_account,
            "buvid4_device": (
                {
                    "fingerprint": str(dev_row["fingerprint"]),
                    "attempted_at": float(dev_row["attempted_at"]),
                    "status": str(dev_row["status"]),
                }
                if dev_row is not None
                else None
            ),
        }

    async def _sync_cooldown_from_store(self, *, default_risk: bool = False) -> None:
        state = await self._load_account_state_from_store()
        if isinstance(state, dict):
            val = _finite_float(state.get("blocked_until"))
            if val is not None and val > self.blocked_until:
                self.blocked_until = val
        if default_risk and self.blocked_until <= self.clock():
            self.blocked_until = self.clock() + RISK_COOLDOWN_SECONDS

    async def initialize(self) -> None:
        creds, _ = await self._load_credentials_from_store()
        self._protect_secrets(creds)
        state = await self._load_account_state_from_store()
        if isinstance(state, dict):
            val = _finite_float(state.get("blocked_until"))
            if val is not None and val >= 0.0:
                self.blocked_until = val

    async def close(self) -> None:
        return None

    def _ensure_current(self, *, epoch: int | None = None, allow_cooldown: bool = False) -> None:
        proxy = self._proxy()
        if proxy is not None and not proxy.enabled:
            raise Rejected("module_disabled")
        if not allow_cooldown and self.blocked_until > self.clock():
            raise Rejected("bili_account_cooling_down")
        if epoch is not None and epoch != self.credentials_epoch:
            raise Rejected("bili_settings_changed")

    async def _check_guard(self, guard: Callable[[], Any] | None) -> None:
        self._ensure_current()
        if guard is not None:
            result = guard()
            if inspect.isawaitable(result):
                result = await result
            if result is False:
                raise Rejected("bili_request_revoked")
        self._ensure_current()

    async def download_credentials(self, guard: Callable[[], Any] | None = None) -> dict[str, str]:
        """Refresh optional `buvid4` via plugin RPC and read decrypted cookies from kernel KV."""
        await self._check_guard(guard)
        async with self._cred_lock:
            await self._check_guard(guard)
            before_creds, _ = await self._load_credentials_from_store()
            epoch_before = self.credentials_epoch
            seq_before = self._cred_update_seq

            proxy = self._online_proxy()
            if proxy is not None and proxy.enabled:
                try:
                    await proxy.call_raw_rpc("bili.ensure_buvid4", {})
                except Rejected as exc:
                    code = str(exc)
                    if code in _RISK_CODES or code == "bili_account_cooling_down":
                        await self._sync_cooldown_from_store(default_risk=(code in _RISK_CODES))
                        raise
                    if code in _REVOCATION_ERRORS:
                        raise

            creds, _ = await self._load_credentials_from_store()
            self._protect_secrets(creds)

            # `buvid4` SPI autofill updates KV `"credentials"` without rotating account session;
            # keep `credentials_epoch` unchanged so an active VideoWatch download stays valid.
            if (
                self._cred_update_seq == seq_before
                and self.credentials_epoch > epoch_before
                and not before_creds.get("buvid4")
                and bool(creds.get("buvid4"))
                and all(
                    creds.get(k) == before_creds.get(k) for k in ("sessdata", "bili_jct", "buvid3")
                )
            ):
                self.credentials_epoch = epoch_before

            await self._check_guard(guard)
            return dict(creds)

    async def _risk(self, reason: str) -> None:
        safe_reason = reason if reason in _RISK_CODES else "bili_account_risk"
        self.blocked_until = self.clock() + RISK_COOLDOWN_SECONDS
        persisted = False
        proxy = self._online_proxy()
        if proxy is not None:
            try:
                res = await proxy.call_raw_rpc("bili.account.risk", {"reason": safe_reason})
                if isinstance(res, dict):
                    val = _finite_float(res.get("cooldown_until"))
                    if val is not None and val > self.blocked_until:
                        self.blocked_until = val
                persisted = True
            except Exception:
                persisted = False
        if not persisted:
            existing = await self._kv_get("account_state")
            state = dict(existing) if isinstance(existing, dict) else {}
            state["blocked_until"] = self.blocked_until
            state["last_error"] = safe_reason
            await self._kv_set("account_state", state)
        try:
            await self.svc.proactive.alert(
                "bili_account_" + reason,
                "BOISER，小号巡逻遇到凭证过期或平台风控，已暂停 12 小时喵。"
                "请先登录 B 站检查状态，再在后台更新凭证喵。",
            )
        except Exception:
            log.info("event=bili_account_alert_failed")
        raise Rejected(reason)

    async def public_material(
        self, video: Video | dict[str, Any], guard: Callable[[], Any] | None = None
    ) -> dict[str, Any]:
        if isinstance(video, dict):
            bvid = str(video.get("bvid") or "")
            title = str(video.get("title") or "")
            description = str(video.get("description") or "")
            video_payload = dict(video)
        else:
            bvid = video.bvid
            title = video.title
            description = video.description
            video_payload = video.model_dump(mode="json")

        fallback: dict[str, Any] = {
            "bvid": bvid,
            "title": clean_text(title)[:150],
            "description": clean_text(description)[:1500],
            "duration": None,
            "subtitle_excerpt": "",
            "subtitle_complete": False,
            "subtitle_status": "unavailable",
            "hot_comments_unverified": [],
            "material_scope": "metadata_only",
        }
        await self._check_guard(guard)
        proxy = self._online_proxy()
        if proxy is None:
            await self._check_guard(guard)
            fallback["material_error"] = "bilibili_unavailable"
            return fallback

        try:
            res = await proxy.call_raw_rpc(
                "bili.public_material",
                {"video": video_payload},
                timeout=30.0,
            )
        except Rejected as exc:
            code = str(exc)
            if code in _RISK_CODES or code == "bili_account_cooling_down":
                await self._sync_cooldown_from_store(default_risk=(code in _RISK_CODES))
                raise
            if code in _REVOCATION_ERRORS:
                raise
            if code in _PUBLIC_DEGRADE_ERRORS:
                await self._check_guard(guard)
                fallback["material_error"] = code
                return fallback
            raise

        await self._check_guard(guard)
        if not isinstance(res, dict):
            fallback["material_error"] = "bili_public_schema_changed"
            return fallback
        return dict(res)

    async def fetch_cover_thumbnail(self, cover_url: str) -> dict[str, Any] | None:
        """Fetch a bounded JPEG cover thumbnail via external `bilibili_surf` plugin."""
        if (
            not isinstance(cover_url, str)
            or ".." in cover_url
            or not COVER_URL_RE.fullmatch(cover_url)
        ):
            return None
        proxy = self._online_proxy()
        if proxy is None or not proxy.enabled or self._inside_own_reader():
            return None
        try:
            res = await proxy.call_raw_rpc("bili.cover_thumb", {"url": cover_url}, timeout=15.0)
        except (Rejected, TimeoutError, ValueError):
            return None
        candidate = res.get("thumbnail") if isinstance(res, dict) and "thumbnail" in res else res
        if not isinstance(candidate, dict) or candidate.get("mime_type") != "image/jpeg":
            return None
        n_bytes = candidate.get("bytes")
        data_b64 = candidate.get("data_b64")
        if (
            isinstance(n_bytes, bool)
            or not isinstance(n_bytes, int)
            or not (1 <= n_bytes <= MAX_COVER_THUMB_BYTES)
            or not isinstance(data_b64, str)
            or not data_b64
        ):
            return None
        try:
            raw = base64.b64decode(data_b64, validate=True)
        except ValueError:
            return None
        if not (1 <= len(raw) <= MAX_COVER_THUMB_BYTES):
            return None
        return dict(candidate)

    async def _active_private_priority_map(self, current: float) -> dict[str, str]:
        async with self.store.lock:
            if not await self._table_exists("bili_private_share_priority"):
                return {}
            rows = await self.store.all(
                "SELECT bvid, session_scope FROM bili_private_share_priority WHERE expires_at>?",
                (current,),
            )
        return {str(r[0]): str(r[1]) for r in rows}

    async def _kv_actions_by_bvid(self, bvids: list[str]) -> dict[str, list[str]]:
        if not bvids:
            return {}
        wanted = set(bvids)
        grouped: dict[str, set[str]] = {}
        actions_log = await self._kv_get("actions_log")
        if isinstance(actions_log, list):
            for act in actions_log:
                if (
                    isinstance(act, dict)
                    and act.get("status") == "done"
                    and act.get("bvid") in wanted
                    and act.get("kind")
                ):
                    grouped.setdefault(str(act["bvid"]), set()).add(str(act["kind"]))
        watched_index = await self._kv_get("watched_index")
        if isinstance(watched_index, dict):
            items = watched_index.get("items")
            if isinstance(items, dict):
                for bv in bvids:
                    for kind in (items.get(bv) or {}).get("actions") or []:
                        grouped.setdefault(bv, set()).add(str(kind))
        return {k: sorted(v) for k, v in grouped.items()}

    async def _legacy_actions_by_bvid(self, bvids: list[str]) -> dict[str, list[str]]:
        if not bvids or not await self._table_exists("bili_actions"):
            return {}
        slots = ",".join("?" for _ in bvids)
        rows = await self.store.all(
            f"SELECT bvid, kind FROM bili_actions WHERE status='done' AND bvid IN ({slots})",
            tuple(bvids),
        )
        grouped: dict[str, set[str]] = {}
        for row in rows:
            grouped.setdefault(str(row[0]), set()).add(str(row[1]))
        return {k: sorted(v) for k, v in grouped.items()}

    async def _lookup_watched_fallback(
        self, bvid: str, *, message: Message | None, notes_limit: int, secrets: list[str]
    ) -> dict[str, Any] | None:
        now = self.clock()
        scope = (
            self.store.session_scope(message)
            if message is not None and message.kind == "private"
            else ""
        )
        prio_map = await self._active_private_priority_map(now)
        if bvid in prio_map and prio_map[bvid] != scope:
            return None

        watched_index = await self._kv_get("watched_index")
        item: dict[str, Any] | None = None
        if isinstance(watched_index, dict):
            items = watched_index.get("items")
            if isinstance(items, dict):
                raw_item = items.get(bvid)
                if isinstance(raw_item, dict):
                    if raw_item.get("status") != "done" or not raw_item.get("has_summary", True):
                        return None
                    item_visible, _ = _private_priority_visible(
                        raw_item, session_scope=scope, now=now
                    )
                    if not item_visible:
                        return None
                    item = raw_item
        doc = await self._kv_get(f"watched.{bvid}")
        if isinstance(doc, dict) and doc.get("summary"):
            doc_visible, _ = _private_priority_visible(doc, session_scope=scope, now=now)
            if not doc_visible:
                return None
            try:
                notes = redact_public_summary(
                    safe_output(str(doc["summary"]), secrets, limit=notes_limit)
                )
            except Rejected:
                return None
            if not notes:
                return None
            created = (
                _finite_float(doc.get("created"))
                or (_finite_float(item.get("created")) if item else None)
                or 0.0
            )
            kv_acts = (await self._kv_actions_by_bvid([bvid])).get(bvid, [])
            doc_acts = [str(a) for a in (doc.get("actions") or [])]
            return {
                "already_watched_in_patrol": True,
                "watched_at": datetime.fromtimestamp(created, self.zone).strftime("%Y-%m-%d %H:%M"),
                "actions_taken": sorted(set([*kv_acts, *doc_acts])),
                "prior_patrol_notes": notes,
            }

        async with self.store.lock:
            if not await self._table_exists("bili_watched_history"):
                return None
            has_prio = await self._table_exists("bili_private_share_priority")
            if has_prio:
                row = await self.store.one(
                    "SELECT h.created, h.summary FROM bili_watched_history h "
                    "LEFT JOIN bili_private_share_priority p ON p.bvid=h.bvid AND p.expires_at>? "
                    "WHERE h.bvid=? AND h.status='done' AND h.summary IS NOT NULL "
                    "AND (p.bvid IS NULL OR p.session_scope=?)",
                    (now, bvid, scope),
                )
            else:
                row = await self.store.one(
                    "SELECT created, summary FROM bili_watched_history "
                    "WHERE bvid=? AND status='done' AND summary IS NOT NULL",
                    (bvid,),
                )
            if row is None:
                return None
            actions = (await self._legacy_actions_by_bvid([bvid])).get(bvid, [])
        try:
            raw_summary = self.store.vault.open("bili-learn:" + bvid, row[1])
            notes = redact_public_summary(safe_output(str(raw_summary), secrets, limit=notes_limit))
        except Exception:
            return None
        if not notes:
            return None
        return {
            "already_watched_in_patrol": True,
            "watched_at": datetime.fromtimestamp(float(row[0]), self.zone).strftime(
                "%Y-%m-%d %H:%M"
            ),
            "actions_taken": actions,
            "prior_patrol_notes": notes,
        }

    async def lookup_watched(
        self, bvid: str, *, message: Message | None = None, limit: int = 360
    ) -> dict[str, Any] | None:
        if not isinstance(bvid, str) or not BVID_RE.fullmatch(bvid):
            return None
        notes_limit = min(1200, max(1, int(limit)))
        scope = (
            self.store.session_scope(message)
            if message is not None and message.kind == "private"
            else ""
        )
        prio_map = await self._active_private_priority_map(self.clock())
        if bvid in prio_map and prio_map[bvid] != scope:
            return None
        secrets = self._all_secrets()
        proxy = self._online_proxy()
        if proxy is not None and not self._inside_own_reader():
            try:
                res = await proxy.call_raw_rpc(
                    "bili.lookup_watched",
                    {"bvid": bvid, "limit": notes_limit, "session_scope": scope},
                )
            except Rejected as exc:
                if str(exc) in _RPC_TRANSPORT_ERRORS:
                    return await self._lookup_watched_fallback(
                        bvid, message=message, notes_limit=notes_limit, secrets=secrets
                    )
                raise
            if not isinstance(res, dict) or not res.get("found"):
                return None
            entry = res.get("entry")
            if not isinstance(entry, dict):
                return None
            try:
                notes = redact_public_summary(
                    safe_output(
                        str(entry.get("prior_patrol_notes") or ""),
                        secrets,
                        limit=notes_limit,
                    )
                )
            except Rejected:
                return None
            if not notes:
                return None
            return {
                "already_watched_in_patrol": True,
                "watched_at": str(entry.get("watched_at") or ""),
                "actions_taken": [str(a) for a in (entry.get("actions_taken") or [])],
                "prior_patrol_notes": notes,
            }

        return await self._lookup_watched_fallback(
            bvid, message=message, notes_limit=notes_limit, secrets=secrets
        )

    async def _recent_surfed_fallback(
        self,
        *,
        limit: int,
        max_age: float,
        current: float,
        message: Message | None,
        secrets: list[str],
    ) -> list[dict[str, Any]]:
        scope = (
            self.store.session_scope(message)
            if message is not None and message.kind == "private"
            else ""
        )
        prio_map = await self._active_private_priority_map(current)
        watched_index = await self._kv_get("watched_index")
        if isinstance(watched_index, dict) and isinstance(watched_index.get("items"), dict):
            eligible: list[tuple[str, dict[str, Any], bool]] = []
            for bv, item in watched_index["items"].items():
                if (
                    not isinstance(item, dict)
                    or item.get("status") != "done"
                    or not item.get("has_summary", True)
                    or (_finite_float(item.get("created")) or 0.0) < current - max_age
                ):
                    continue
                if bv in prio_map and prio_map[bv] != scope:
                    continue
                item_visible, item_prio = _private_priority_visible(
                    item, session_scope=scope, now=current
                )
                if not item_visible:
                    continue
                is_prio = item_prio or (bool(scope) and prio_map.get(bv) == scope)
                eligible.append((bv, item, is_prio))
            eligible.sort(
                key=lambda triple: (
                    1 if triple[2] else 0,
                    _finite_float(triple[1].get("created")) or 0.0,
                ),
                reverse=True,
            )
            actions_map = await self._kv_actions_by_bvid([bv for bv, _, _ in eligible[: limit * 2]])
            results: list[dict[str, Any]] = []
            for bv, item, _ in eligible:
                if len(results) >= limit:
                    break
                doc = await self._kv_get(f"watched.{bv}")
                if not isinstance(doc, dict) or not doc.get("summary"):
                    continue
                doc_visible, _ = _private_priority_visible(doc, session_scope=scope, now=current)
                if not doc_visible:
                    continue
                try:
                    notes = redact_public_summary(
                        safe_output(str(doc["summary"]), secrets, limit=220)
                    )
                except Rejected:
                    continue
                if not notes:
                    continue
                created = (
                    _finite_float(doc.get("created")) or _finite_float(item.get("created")) or 0.0
                )
                entry: dict[str, Any] = {
                    "bvid": bv,
                    "watched_at": datetime.fromtimestamp(created, self.zone).strftime(
                        "%m-%d %H:%M"
                    ),
                    "notes": notes,
                }
                doc_acts = [str(a) for a in (doc.get("actions") or [])]
                acts = sorted(set([*doc_acts, *actions_map.get(bv, [])]))
                if acts:
                    entry["actions"] = acts
                results.append(entry)
            return results

        async with self.store.lock:
            if not await self._table_exists("bili_watched_history"):
                return []
            has_prio = await self._table_exists("bili_private_share_priority")
            if has_prio:
                rows = await self.store.all(
                    "SELECT h.bvid, h.created, h.summary FROM bili_watched_history h "
                    "LEFT JOIN bili_private_share_priority p ON p.bvid=h.bvid AND p.expires_at>? "
                    "WHERE h.status='done' AND h.summary IS NOT NULL AND h.created>=? "
                    "AND (p.bvid IS NULL OR p.session_scope=?) "
                    "ORDER BY (p.bvid IS NOT NULL) DESC, h.created DESC LIMIT ?",
                    (current, current - max_age, scope, limit),
                )
            else:
                rows = await self.store.all(
                    "SELECT bvid, created, summary FROM bili_watched_history "
                    "WHERE status='done' AND summary IS NOT NULL AND created>=? "
                    "ORDER BY created DESC LIMIT ?",
                    (current - max_age, limit),
                )
            actions = await self._legacy_actions_by_bvid([str(r[0]) for r in rows])

        items_out: list[dict[str, Any]] = []
        for bvid_val, created, sealed in rows:
            bv = str(bvid_val)
            try:
                raw_summary = self.store.vault.open("bili-learn:" + bv, sealed)
                notes = redact_public_summary(safe_output(str(raw_summary), secrets, limit=220))
            except Exception:
                continue
            if not notes:
                continue
            entry = {
                "bvid": bv,
                "watched_at": datetime.fromtimestamp(float(created), self.zone).strftime(
                    "%m-%d %H:%M"
                ),
                "notes": notes,
            }
            if actions.get(bv):
                entry["actions"] = actions[bv]
            items_out.append(entry)
        return items_out

    async def recent_surfed(
        self,
        *,
        limit: int = 4,
        max_age: float = 172800.0,
        now: float | None = None,
        message: Message | None = None,
    ) -> list[dict[str, Any]]:
        current = self.clock() if now is None else float(now)
        max_items = max(1, int(limit))
        scope = (
            self.store.session_scope(message)
            if message is not None and message.kind == "private"
            else ""
        )
        prio_map = await self._active_private_priority_map(current)
        secrets = self._all_secrets()
        proxy = self._online_proxy()
        if proxy is not None and not self._inside_own_reader():
            try:
                res = await proxy.call_raw_rpc(
                    "bili.recent_surfed",
                    {
                        "limit": max_items,
                        "max_age": float(max_age),
                        "now": current,
                        "session_scope": scope,
                    },
                )
            except Rejected as exc:
                if str(exc) in _RPC_TRANSPORT_ERRORS:
                    return await self._recent_surfed_fallback(
                        limit=max_items,
                        max_age=float(max_age),
                        current=current,
                        message=message,
                        secrets=secrets,
                    )
                raise
            raw_items = res.get("items") if isinstance(res, dict) else None
            if not isinstance(raw_items, list):
                return []
            out: list[dict[str, Any]] = []
            for item in raw_items:
                if len(out) >= max_items:
                    break
                if not isinstance(item, dict):
                    continue
                bv = str(item.get("bvid") or "")
                if not BVID_RE.fullmatch(bv):
                    continue
                if bv in prio_map and prio_map[bv] != scope:
                    continue
                try:
                    notes = redact_public_summary(
                        safe_output(str(item.get("notes") or ""), secrets, limit=220)
                    )
                except Rejected:
                    continue
                if not notes:
                    continue
                cleaned: dict[str, Any] = {
                    "bvid": bv,
                    "watched_at": str(item.get("watched_at") or ""),
                    "notes": notes,
                }
                if isinstance(item.get("actions"), list) and item["actions"]:
                    cleaned["actions"] = [str(a) for a in item["actions"]]
                out.append(cleaned)
            return out

        return await self._recent_surfed_fallback(
            limit=max_items,
            max_age=float(max_age),
            current=current,
            message=message,
            secrets=secrets,
        )

    async def _offline_status_snapshot(self) -> dict[str, Any]:
        env_policy = {
            "enabled": bool(getattr(self.s, "bili_autonomous_enabled", False)),
            "allow_like": False,
            "allow_favorite": False,
            "favorite_folder_id": "",
            "daily_action_limit": 1,
        }
        policy = dict(env_policy)
        policy_source = "env"

        kv_policy = await self._kv_get("policy")
        if isinstance(kv_policy, dict):
            cleaned = {k: v for k, v in kv_policy.items() if k not in {"times", "jitter_minutes"}}
            policy = _clean_policy_dict(cleaned, env_policy)
            policy_source = "sqlite"
        else:
            async with self.store.lock:
                has_admin = await self._table_exists("admin_config")
                row = (
                    await self.store.one(
                        "SELECT payload FROM admin_config WHERE key='bili_account'"
                    )
                    if has_admin
                    else None
                )
            if row is not None:
                try:
                    saved = self.store.vault.open("admin:bili_account", row[0])
                except Exception:
                    saved = None
                if isinstance(saved, dict):
                    cleaned = {
                        k: v for k, v in saved.items() if k not in {"times", "jitter_minutes"}
                    }
                    policy = _clean_policy_dict(cleaned, env_policy)
                    policy_source = "sqlite"

        creds, cred_source = await self._load_credentials_from_store()
        account_state = await self._load_account_state_from_store() or {}
        blocked_until = _finite_float(account_state.get("blocked_until")) or 0.0
        if blocked_until > self.blocked_until:
            self.blocked_until = blocked_until
        account_obj = account_state.get("account")
        mid = str(account_obj.get("mid") or "") if isinstance(account_obj, dict) else ""
        account_mask = ("…" + mid[-4:]) if mid else None
        dev = account_state.get("buvid4_device")
        buvid4_status = (
            "configured"
            if creds.get("buvid4")
            else str(dev.get("status") or "not_attempted")
            if isinstance(dev, dict)
            else "not_attempted"
        )
        buvid4_attempted_at = (
            _finite_float(dev.get("attempted_at")) if isinstance(dev, dict) else None
        )

        tz_name = str(getattr(self.s, "bot_timezone", "Asia/Shanghai"))
        day = datetime.fromtimestamp(self.clock(), self.zone).date().isoformat()

        runs: list[dict[str, Any]] = []
        kv_runs = await self._kv_get("recent_runs")
        if isinstance(kv_runs, list):
            valid_runs = [dict(r) for r in kv_runs if isinstance(r, dict)]
            valid_runs.sort(key=lambda r: _finite_float(r.get("created")) or 0.0, reverse=True)
            runs = [
                {
                    "day": str(r.get("day") or ""),
                    "slot": str(r.get("slot") or ""),
                    "status": str(r.get("status") or ""),
                    "created": _finite_float(r.get("created")) or 0.0,
                    "reason": r.get("reason"),
                }
                for r in valid_runs[:10]
            ]
        else:
            async with self.store.lock:
                if await self._table_exists("bili_patrol_runs"):
                    rows = await self.store.all(
                        "SELECT day, slot, status, created, reason "
                        "FROM bili_patrol_runs ORDER BY created DESC LIMIT 10"
                    )
                    runs = [dict(r) for r in rows]

        actions_today = 0
        uncertain_actions = 0
        kv_actions = await self._kv_get("actions_log")
        if isinstance(kv_actions, list):
            for act in kv_actions:
                if not isinstance(act, dict):
                    continue
                if act.get("day") == day:
                    actions_today += 1
                if act.get("status") == "uncertain":
                    uncertain_actions += 1
        else:
            async with self.store.lock:
                if await self._table_exists("bili_actions"):
                    actions_today = (
                        await self.store.one(
                            "SELECT COUNT(*) FROM bili_actions WHERE day=?", (day,)
                        )
                    )[0]
                    uncertain_actions = (
                        await self.store.one(
                            "SELECT COUNT(*) FROM bili_actions WHERE status='uncertain'"
                        )
                    )[0]

        watched_counts = {
            "done": 0,
            "processing": 0,
            "retryable": 0,
            "uncertain": 0,
            "exhausted": 0,
        }
        kv_index = await self._kv_get("watched_index")
        if isinstance(kv_index, dict) and isinstance(kv_index.get("items"), dict):
            for item in kv_index["items"].values():
                if not isinstance(item, dict):
                    continue
                st = item.get("status")
                if st == "done" and item.get("has_summary", True):
                    watched_counts["done"] += 1
                elif st in {"reading", "analyzing"}:
                    watched_counts["processing"] += 1
                elif st == "retryable":
                    watched_counts["retryable"] += 1
                    if int(item.get("attempts") or 0) >= 3:
                        watched_counts["exhausted"] += 1
                elif st in {"uncertain", "attempted"} or (
                    st == "done" and not item.get("has_summary", True)
                ):
                    watched_counts["uncertain"] += 1
        else:
            async with self.store.lock:
                if await self._table_exists("bili_watched_history"):
                    has_prog = await self._table_exists("bili_watch_progress")
                    if has_prog:
                        row = await self.store.one(
                            "SELECT "
                            "COUNT(*) FILTER "
                            "(WHERE h.status='done' AND length(h.summary)>0) AS done, "
                            "COUNT(*) FILTER "
                            "(WHERE h.status IN ('reading','analyzing')) AS processing, "
                            "COUNT(*) FILTER (WHERE h.status='retryable') AS retryable, "
                            "COUNT(*) FILTER (WHERE h.status IN ('uncertain','attempted') OR "
                            "(h.status='done' AND COALESCE(length(h.summary),0)=0)) AS uncertain, "
                            "COUNT(*) FILTER "
                            "(WHERE h.status='retryable' AND p.attempts>=3) AS exhausted "
                            "FROM bili_watched_history h "
                            "LEFT JOIN bili_watch_progress p ON p.bvid=h.bvid"
                        )
                    else:
                        row = await self.store.one(
                            "SELECT "
                            "COUNT(*) FILTER "
                            "(WHERE status='done' AND length(summary)>0) AS done, "
                            "COUNT(*) FILTER "
                            "(WHERE status IN ('reading','analyzing')) AS processing, "
                            "COUNT(*) FILTER (WHERE status='retryable') AS retryable, "
                            "COUNT(*) FILTER (WHERE status IN ('uncertain','attempted') OR "
                            "(status='done' AND COALESCE(length(summary),0)=0)) AS uncertain, "
                            "0 AS exhausted FROM bili_watched_history"
                        )
                    if row is not None:
                        watched_counts = dict(row)

        return {
            "config": policy,
            "source": policy_source,
            "environment": env_policy,
            "credentials": {key: bool(creds.get(key)) for key in CREDENTIAL_KEYS},
            "credential_source": cred_source,
            "buvid4_autofill": {
                "status": buvid4_status,
                "attempted_at": buvid4_attempted_at,
            },
            "account_mask": account_mask,
            "checked_at": _finite_float(account_state.get("checked_at")),
            "cooldown_until": blocked_until,
            "last_error": account_state.get("last_error"),
            "recent_runs": runs,
            "actions_today": actions_today,
            "watched_count": watched_counts["done"],
            "watch_history_counts": watched_counts,
            "uncertain_actions": uncertain_actions,
            "scheduler": "life_engine",
            "timezone": tz_name,
            "heartbeat_supported": False,
            "plugin_offline": True,
        }

    async def status(self) -> dict[str, Any]:
        proxy = self._online_proxy()
        if proxy is not None and not self._inside_own_reader():
            try:
                res = await proxy.call_raw_rpc("bili.account.status", {})
            except Rejected as exc:
                if str(exc) in _RPC_TRANSPORT_ERRORS:
                    return await self._offline_status_snapshot()
                raise
            if isinstance(res, dict):
                out = dict(res)
                if isinstance(out.get("credentials"), dict):
                    out["credentials"] = {
                        k: bool(out["credentials"].get(k)) for k in CREDENTIAL_KEYS
                    }
                cooldown = _finite_float(out.get("cooldown_until"))
                if cooldown is not None and cooldown >= 0.0:
                    self.blocked_until = cooldown
                return out
        return await self._offline_status_snapshot()

    async def set_policy(self, value: Any) -> dict[str, Any]:
        proxy = self._require_online_proxy(unavailable_code="bilibili_module_unavailable")
        if value is None:
            res = await proxy.call_raw_rpc("bili.account.reset_policy", {})
        else:
            payload = value.model_dump() if hasattr(value, "model_dump") else dict(value)
            res = await proxy.call_raw_rpc("bili.account.set_policy", payload)
        self.policy_epoch += 1
        if isinstance(res, dict):
            out = dict(res)
            if isinstance(out.get("credentials"), dict):
                out["credentials"] = {k: bool(out["credentials"].get(k)) for k in CREDENTIAL_KEYS}
            return out
        return await self.status()

    async def update_credentials(self, update: Any) -> dict[str, Any]:
        proxy = self._require_online_proxy(unavailable_code="bilibili_module_unavailable")
        payload = update.model_dump() if hasattr(update, "model_dump") else dict(update)
        try:
            res = await proxy.call_raw_rpc("bili.account.update_credentials", payload)
        except Rejected:
            raise
        except Exception:
            raise Rejected("bilibili_module_unavailable") from None
        self._cred_update_seq += 1
        self.credentials_epoch += 1
        creds, _ = await self._load_credentials_from_store()
        self._protect_secrets(creds)
        if isinstance(payload, dict) and not payload.get("clear"):
            submitted = {
                k: str(payload[k])
                for k in CREDENTIAL_KEYS
                if isinstance(payload.get(k), str) and payload.get(k)
            }
            if submitted:
                self._protect_secrets(submitted)
        if isinstance(res, dict):
            out = dict(res)
            if isinstance(out.get("credentials"), dict):
                out["credentials"] = {k: bool(out["credentials"].get(k)) for k in CREDENTIAL_KEYS}
            return out
        return await self.status()

    async def nav(self) -> dict[str, Any]:
        proxy = self._require_online_proxy(
            check_enabled=True, unavailable_code="bilibili_module_unavailable"
        )
        try:
            res = await proxy.call_raw_rpc("bili.account.nav", {}, timeout=30.0)
        except Rejected as exc:
            code = str(exc)
            if code in _RISK_CODES or code == "bili_account_cooling_down":
                await self._sync_cooldown_from_store(default_risk=(code in _RISK_CODES))
            raise
        return dict(res) if isinstance(res, dict) else {}

    async def run_once(self, slot: str | None = None) -> dict[str, Any]:
        proxy = self._require_online_proxy(
            check_enabled=True, unavailable_code="bilibili_module_unavailable"
        )
        try:
            res = await proxy.call_raw_rpc("bili.account.run_once", {"slot": slot}, timeout=1500.0)
        except Rejected as exc:
            code = str(exc)
            if code in _RISK_CODES or code == "bili_account_cooling_down":
                await self._sync_cooldown_from_store(default_risk=(code in _RISK_CODES))
            raise
        if isinstance(res, dict) and res.get("status") == "failed":
            reason = res.get("reason")
            if reason in _RISK_CODES or reason == "bili_account_cooling_down":
                await self._sync_cooldown_from_store(default_risk=(reason in _RISK_CODES))
        return dict(res) if isinstance(res, dict) else {}
