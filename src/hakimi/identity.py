"""Encrypted cross-platform unified identity, /bind codes, and Owner cross-surface recaps."""

from __future__ import annotations

import asyncio
import html
import json
import re
import secrets
import time
import uuid
from collections import deque
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from .affection_rules import (
    GENERIC_USER_RE as GENERIC_USER_RE,
)
from .affection_rules import (
    ONEBOT_USER_RE as ONEBOT_USER_RE,
)
from .affection_rules import (
    PLATFORM_ID_RE as PLATFORM_ID_RE,
)
from .affection_rules import (
    QQ_USER_RE as QQ_USER_RE,
)
from .affection_rules import (
    validate_platform_user as validate_platform_user,
)
from .bot_server import is_persona_owner
from .config import Settings
from .context_budget import estimate_tokens
from .models import Message
from .security import Rejected, clean_text, configured_secrets
from .social import chat_registry
from .social_chat_config import cached_remote_chat_config, load_remote_chat_config

SCHEMA = """
CREATE TABLE IF NOT EXISTS identity_persons (
  person_id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  muted INTEGER NOT NULL DEFAULT 0,
  payload BLOB NOT NULL,
  updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS identity_persons_scope_updated
  ON identity_persons(scope, updated DESC);
CREATE TABLE IF NOT EXISTS identity_links (
  account_key TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  person_id TEXT NOT NULL,
  platform TEXT NOT NULL,
  payload BLOB NOT NULL,
  updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS identity_links_person ON identity_links(scope, person_id);
CREATE INDEX IF NOT EXISTS identity_links_scope_updated
  ON identity_links(scope, updated ASC);
"""

PERSON_ID_RE = re.compile(r"^(?:owner|p_[a-f0-9]{16})$")

# 32 unambiguous Base32 characters (excludes 0, 1, I, O).
BIND_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
BIND_CODE_RE = re.compile(r"^HK-[2-9A-HJ-NP-Z]{8}$")
BIND_CODE_TTL_SECONDS = 300.0
BIND_GEN_WINDOW_SECONDS = 600.0
BIND_GEN_LIMIT = 3
BIND_MAX_WRONG_TRIES = 5
BIND_COOLDOWN_SECONDS = 900.0

OWNER_RECAP_TTL_SECONDS = 43200.0
OWNER_RECAP_MAX_ENTRIES = 6
OWNER_RECAP_MAX_CHARS = 80
OWNER_RECAP_MAX_TOKENS = 240

MAX_L2_TAGS = 8
MAX_L2_TAG_CHARS = 32
VALID_LINK_SOURCES = frozenset({"auto", "admin", "bind_code", "owner_config"})

_AUTH_HEADER_RE = re.compile(r"(?i)\bauthorization\b\s*[:=]\s*Bearer\s+[A-Za-z0-9._~+/=-]{4,}")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{4,}")
_SK_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{6,}")
_AIZA_RE = re.compile(r"\bAIza[0-9A-Za-z_-]{8,}")
_CREDENTIAL_KEY_RE = re.compile(
    r"(?i)(?:[\"']?)"
    r"(?:api[_-]?key|apikey|password|passwd|pwd|secret|token|"
    r"sessdata|bili_jct|buvid3|buvid4|access[_-]?token|refresh[_-]?token|authorization)"
    r"(?:[\"']?)\s*[:=]\s*(?:[\"']?)[^\s,;}\"'\]]+(?:[\"']?)"
)
_PEM_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")
_ROLE_RE = re.compile(r"\[(?:untrusted_)?role=", re.IGNORECASE)
_CQ_RE = re.compile(r"\[cq:", re.IGNORECASE)
_AT_BROADCAST_RE = re.compile(r"@(everyone|all|全体成员)", re.IGNORECASE)
_INJECTION_RE = re.compile(
    r"(?:\[(?:untrusted_)?role=|\[cq:|</?(?:untrusted_|memory_summary|system)|"
    r"@(?:everyone|all|全体成员)|ignore\s+(?:previous|all)\s+instructions|"
    r"system\s+prompt|忽略系统规则|忽略以上指令|系统提示词|越权|覆盖人设)",
    re.IGNORECASE,
)
_GROUP_EVENT_TAG_RE = re.compile(
    r"(?:^(?:group|群名|群聊|event|事件)\s*[:：#\-]|"
    r"群聊事件|群事件|群名称|group_name|group_event)",
    re.IGNORECASE,
)
_COLLAPSE_WS_RE = re.compile(r"[\r\n\t ]+")


@dataclass(frozen=True)
class _PendingBindCode:
    code_digest: str
    person_id: str
    platform: str
    user_id: str
    expires_at: float


class IdentityService:
    """Cross-platform encrypted identity store, /bind codes, and Owner recap ring."""

    def __init__(
        self,
        settings: Settings,
        store: Any,
        service: Any = None,
        *,
        clock: Any = time.time,
    ) -> None:
        self.s = settings
        self.store = store
        self.service = service
        self.clock = clock
        self.scope = self.store.vault.digest("identity", self.s.namespace)
        self.initialized = False
        self._tables_ready = False
        self._init_task: asyncio.Task[Any] | None = None
        self._init_lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self._bind_lock = asyncio.Lock()

        # Fast in-memory caches populated during initialize() and updated atomically on mutation.
        self._account_to_person: dict[tuple[str, str], str] = {}
        self._person_muted: dict[str, bool] = {}
        self._person_accounts: dict[str, set[tuple[str, str]]] = {}
        self._syncing_accounts: set[tuple[str, str]] = set()

        # RAM-only /bind code state (never persisted, never logged).
        self._bind_codes: dict[str, _PendingBindCode] = {}
        self._bind_gen_history: dict[tuple[str, str], deque[float]] = {}
        self._bind_fail_state: dict[tuple[str, str], tuple[int, float]] = {}

        # RAM-only Owner cross-surface recap ring.
        self._owner_recaps: deque[dict[str, Any]] = deque(maxlen=OWNER_RECAP_MAX_ENTRIES)

    def _person_seal_ctx(self, person_id: str) -> str:
        return f"identity-person:{self.scope}:{person_id}"

    def _link_seal_ctx(self, account_key: str) -> str:
        return f"identity-link:{self.scope}:{account_key}"

    def account_key(self, platform: str, user_id: str) -> str:
        return self.store.vault.digest("identity-account", self.scope, platform, user_id)

    def _secret_candidates(self) -> list[str]:
        candidates = {
            val
            for raw in configured_secrets(self.s)
            if raw
            for val in (raw, clean_text(raw))
            if len(val) >= 6
        }
        return sorted(candidates, key=len, reverse=True)

    def _contains_secret(self, text: str) -> bool:
        if any(secret in text for secret in self._secret_candidates()):
            return True
        return bool(
            _AUTH_HEADER_RE.search(text)
            or _BEARER_RE.search(text)
            or _SK_RE.search(text)
            or _AIZA_RE.search(text)
            or _CREDENTIAL_KEY_RE.search(text)
            or _PEM_RE.search(text)
        )

    def _scrub_secrets(self, text: str) -> str:
        cleaned = text
        for secret in self._secret_candidates():
            if secret in cleaned:
                cleaned = cleaned.replace(secret, "[REDACTED]")
        cleaned = _AUTH_HEADER_RE.sub("[REDACTED]", cleaned)
        cleaned = _BEARER_RE.sub("[REDACTED]", cleaned)
        cleaned = _SK_RE.sub("[REDACTED]", cleaned)
        cleaned = _AIZA_RE.sub("[REDACTED]", cleaned)
        cleaned = _CREDENTIAL_KEY_RE.sub("[REDACTED]", cleaned)
        cleaned = _PEM_RE.sub("[REDACTED]", cleaned)
        return cleaned

    def _sanitize_nickname(self, value: Any) -> str:
        raw = clean_text(str(value or ""))
        raw = _COLLAPSE_WS_RE.sub(" ", raw).strip()[:80]
        if not raw or self._contains_secret(raw) or _INJECTION_RE.search(raw):
            return ""
        cleaned = re.sub(r"[<>]", "", raw)[:64].strip()
        return cleaned

    def _sanitize_display_name(self, value: Any, *, fallback: str = "") -> str:
        cleaned = self._sanitize_nickname(value)
        if cleaned:
            return cleaned
        return fallback[:64]

    def _needs_init(self) -> bool:
        if self.initialized:
            return False
        return not (self._tables_ready and self._init_task is asyncio.current_task())

    def _sanitize_l2_tags(self, tags: list[str] | tuple[str, ...] | None) -> list[str]:
        if not tags:
            return []
        known_groups: set[str] = {
            str(g).strip()
            for g in (
                *getattr(self.s, "onebot_allowed_groups", ()),
                *getattr(self.s, "qq_allowed_groups", ()),
            )
            if str(g).strip()
        }
        remote_platforms: set[str] = {"telegram"}
        reg = chat_registry(self.s)
        if reg is not None:
            for plat, mod in (getattr(reg, "chat_platforms", None) or {}).items():
                if plat not in {"onebot", "qq"}:
                    remote_platforms.add(str(plat))
                    if hasattr(mod, "allowed_groups"):
                        for g in mod.allowed_groups():
                            if str(g).strip():
                                known_groups.add(str(g).strip())
        for plat in remote_platforms:
            cfg = cached_remote_chat_config(self.s, plat)
            if cfg is not None:
                for g in (*cfg.allowed_groups, *cfg.proactive_groups):
                    if str(g).strip():
                        known_groups.add(str(g).strip())

        prefixes = ("group:", "onebot:", "qq:", *(f"{plat}:" for plat in sorted(remote_platforms)))
        out: list[str] = []
        seen: set[str] = set()
        for raw in tags:
            if isinstance(raw, bool) or not isinstance(raw, str):
                continue
            text = _COLLAPSE_WS_RE.sub(" ", clean_text(raw)).strip()
            if not text:
                continue
            if self._contains_secret(text):
                continue
            if _INJECTION_RE.search(text) or "<" in text or ">" in text:
                continue
            if _GROUP_EVENT_TAG_RE.search(text):
                continue
            if any(
                len(gid) >= 3
                and (
                    text == gid
                    or f"群{gid}" in text
                    or any(f"{prefix}{gid}" in text for prefix in prefixes)
                )
                for gid in known_groups
            ):
                continue
            tag = text[:MAX_L2_TAG_CHARS].strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
            if len(out) >= MAX_L2_TAGS:
                break
        return out

    def _configured_owner_accounts(self) -> list[tuple[str, str]]:
        accounts: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for uid in {self.s.owner_qq_id, *self.s.onebot_owner_ids} - {""}:
            try:
                norm = validate_platform_user("onebot", str(uid))
            except Rejected:
                continue
            if ("onebot", norm) not in seen:
                seen.add(("onebot", norm))
                accounts.append(("onebot", norm))
        for uid in set(self.s.qq_owner_ids) - {""}:
            try:
                norm = validate_platform_user("qq", str(uid))
            except Rejected:
                continue
            if ("qq", norm) not in seen:
                seen.add(("qq", norm))
                accounts.append(("qq", norm))
        for plat, binding in self.s.social_owner_bindings.items():
            try:
                norm = validate_platform_user(plat, str(binding.user_id))
            except Rejected:
                continue
            if (plat, norm) not in seen:
                seen.add((plat, norm))
                accounts.append((plat, norm))
        return accounts

    def _is_configured_owner_account(self, platform: str, user_id: str) -> bool:
        uid = str(user_id or "").strip()
        if not uid:
            return False
        if platform == "onebot":
            return uid in ({self.s.owner_qq_id, *self.s.onebot_owner_ids} - {""})
        if platform == "qq":
            return uid in (set(self.s.qq_owner_ids) - {""})
        binding = self.s.social_owner_bindings.get(platform)
        return binding is not None and uid == binding.user_id

    def is_owner_account(self, platform: str, user_id: str) -> bool:
        uid = str(user_id or "").strip()
        if not uid:
            return False
        if self._is_configured_owner_account(platform, uid):
            return True
        return self._account_to_person.get((platform, uid)) == "owner"

    def _is_known_bot(self, platform: str, user_id: str, bot_id: str = "") -> bool:
        uid = str(user_id or "").strip()
        if not uid:
            return False
        if bot_id and uid == bot_id:
            return True
        if platform == "onebot" and (
            uid in self.s.bot_qq_ids or (self.s.onebot_self_id and uid == self.s.onebot_self_id)
        ):
            return True
        if platform == "qq" and self.s.qq_app_id and uid == self.s.qq_app_id:
            return True
        binding = self.s.social_owner_bindings.get(platform)
        return binding is not None and uid == binding.bot_id

    def is_muted(self, platform: str, user_id: str) -> bool:
        uid = str(user_id or "").strip()
        if not uid or self.is_owner_account(platform, uid):
            return False
        pid = self._account_to_person.get((platform, uid))
        if pid is not None:
            if pid == "owner":
                return False
            if self._person_muted.get(pid, False):
                return True
        if platform == "onebot" and uid in getattr(self.s, "muted_user_ids", set()):
            return True
        return False

    def cached_person_id(self, platform: str, user_id: str) -> str | None:
        uid = str(user_id or "").strip()
        if not uid:
            return None
        pid = self._account_to_person.get((platform, uid))
        if pid is not None:
            return pid
        if self._is_configured_owner_account(platform, uid):
            return "owner"
        return None

    def linked_accounts_cached(self, person_id: str) -> list[tuple[str, str]]:
        return sorted(self._person_accounts.get(person_id, ()))

    def muted_onebot_user_ids(self) -> set[str]:
        out: set[str] = set()
        for pid, muted in self._person_muted.items():
            if not muted or pid == "owner":
                continue
            for plat, uid in self._person_accounts.get(pid, ()):
                if (
                    plat == "onebot"
                    and not self.is_owner_account(plat, uid)
                    and not self._is_known_bot(plat, uid)
                ):
                    out.add(uid)
        return out

    def _cache_put_link(self, person_id: str, platform: str, user_id: str) -> None:
        old_pid = self._account_to_person.get((platform, user_id))
        if old_pid is not None and old_pid != person_id:
            old_set = self._person_accounts.get(old_pid)
            if old_set is not None:
                old_set.discard((platform, user_id))
        self._account_to_person[(platform, user_id)] = person_id
        self._person_accounts.setdefault(person_id, set()).add((platform, user_id))

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            self._init_task = asyncio.current_task()
            try:
                now = float(self.clock())
                async with self.store.transaction():
                    for stmt in SCHEMA.split(";"):
                        sql = stmt.strip()
                        if sql:
                            await self.store.db.execute(sql)

                    # Seed or sanitize reserved "owner" person.
                    owner_row = await self.store.one(
                        "SELECT muted, payload FROM identity_persons WHERE person_id='owner'"
                    )
                    if not owner_row:
                        owner_payload = {
                            "display_name": "BOISER",
                            "l2_tags": [],
                            "created_at": now,
                            "updated_at": now,
                            "mute_source": "",
                        }
                        await self.store.db.execute(
                            "INSERT INTO identity_persons"
                            "(person_id, scope, muted, payload, updated) "
                            "VALUES ('owner', ?, 0, ?, ?)",
                            (
                                self.scope,
                                self.store.vault.seal(
                                    self._person_seal_ctx("owner"), owner_payload
                                ),
                                now,
                            ),
                        )
                    elif int(owner_row["muted"]) != 0:
                        await self.store.db.execute(
                            "UPDATE identity_persons SET muted=0, scope=?, updated=? "
                            "WHERE person_id='owner'",
                            (self.scope, now),
                        )

                    # Link all configured owner accounts to "owner".
                    for plat, uid in self._configured_owner_accounts():
                        acc_key = self.account_key(plat, uid)
                        existing = await self.store.one(
                            "SELECT person_id, payload FROM identity_links "
                            "WHERE account_key=? AND scope=?",
                            (acc_key, self.scope),
                        )
                        if not existing:
                            link_payload = {
                                "platform": plat,
                                "user_id": uid,
                                "nickname": "BOISER",
                                "source": "owner_config",
                                "linked_at": now,
                            }
                            await self.store.db.execute(
                                "INSERT INTO identity_links"
                                "(account_key, scope, person_id, platform, payload, updated) "
                                "VALUES (?,?,?,?,?,?)",
                                (
                                    acc_key,
                                    self.scope,
                                    "owner",
                                    plat,
                                    self.store.vault.seal(
                                        self._link_seal_ctx(acc_key), link_payload
                                    ),
                                    now,
                                ),
                            )
                        elif existing["person_id"] != "owner":
                            opened = self.store.vault.open(
                                self._link_seal_ctx(acc_key), existing["payload"]
                            )
                            link_payload = dict(opened) if isinstance(opened, dict) else {}
                            link_payload.update(
                                platform=plat,
                                user_id=uid,
                                source="owner_config",
                                linked_at=float(link_payload.get("linked_at", now) or now),
                            )
                            await self.store.db.execute(
                                "UPDATE identity_links "
                                "SET person_id='owner', payload=?, updated=? "
                                "WHERE account_key=? AND scope=?",
                                (
                                    self.store.vault.seal(
                                        self._link_seal_ctx(acc_key), link_payload
                                    ),
                                    now,
                                    acc_key,
                                    self.scope,
                                ),
                            )

                    # Sync any already-muted onebot user IDs from settings into identity tables.
                    for uid in sorted(set(getattr(self.s, "muted_user_ids", set()))):
                        try:
                            norm_uid = validate_platform_user("onebot", uid)
                        except Rejected:
                            continue
                        if self._is_configured_owner_account(
                            "onebot", norm_uid
                        ) or self._is_known_bot("onebot", norm_uid):
                            continue
                        acc_key = self.account_key("onebot", norm_uid)
                        existing = await self.store.one(
                            "SELECT person_id FROM identity_links WHERE account_key=? AND scope=?",
                            (acc_key, self.scope),
                        )
                        if existing:
                            pid = str(existing["person_id"])
                            if pid != "owner":
                                await self.store.db.execute(
                                    "UPDATE identity_persons SET muted=1, updated=? "
                                    "WHERE person_id=? AND scope=?",
                                    (now, pid, self.scope),
                                )
                        else:
                            pid = "p_" + uuid.uuid4().hex[:16]
                            person_payload = {
                                "display_name": f"onebot:{norm_uid}"[:48],
                                "l2_tags": [],
                                "created_at": now,
                                "updated_at": now,
                                "mute_source": "manual",
                            }
                            link_payload = {
                                "platform": "onebot",
                                "user_id": norm_uid,
                                "nickname": "",
                                "source": "auto",
                                "linked_at": now,
                            }
                            await self.store.db.execute(
                                "INSERT INTO identity_persons"
                                "(person_id, scope, muted, payload, updated) VALUES (?,?,?,?,?)",
                                (
                                    pid,
                                    self.scope,
                                    1,
                                    self.store.vault.seal(
                                        self._person_seal_ctx(pid), person_payload
                                    ),
                                    now,
                                ),
                            )
                            await self.store.db.execute(
                                "INSERT INTO identity_links"
                                "(account_key, scope, person_id, platform, payload, updated) "
                                "VALUES (?,?,?,?,?,?)",
                                (
                                    acc_key,
                                    self.scope,
                                    pid,
                                    "onebot",
                                    self.store.vault.seal(
                                        self._link_seal_ctx(acc_key), link_payload
                                    ),
                                    now,
                                ),
                            )

                await self._reload_caches()
                self._tables_ready = True

                # Warm cached Telegram RemoteChatConfig if persisted, for sync _sanitize_l2_tags.
                if cached_remote_chat_config(self.s, "telegram") is None:
                    try:
                        await load_remote_chat_config(self.store, "telegram", settings=self.s)
                    except Exception:
                        pass

                # Ensure runtime reference and complete affection migration BEFORE releasing
                # _init_lock and exposing initialized=True (same-task re-entry uses _init_task).
                if (
                    self.store.runtime is not None
                    and getattr(self.store.runtime, "identity", None) is None
                ):
                    self.store.runtime.identity = self
                aff = getattr(self.store.runtime, "affection", None)
                if aff is not None:
                    if hasattr(aff, "bind_identity"):
                        aff.bind_identity(self)
                    if getattr(aff, "initialized", False) and hasattr(aff, "migrate_to_identity"):
                        try:
                            await aff.migrate_to_identity(_identity_ready=True)
                        except TypeError:
                            await aff.migrate_to_identity()

                self.initialized = True
            finally:
                if not self.initialized:
                    self._tables_ready = False
                self._init_task = None

    async def _reload_caches(self) -> None:
        async with self.store.lock:
            person_rows = await self.store.all(
                "SELECT person_id, muted FROM identity_persons WHERE scope=?",
                (self.scope,),
            )
            link_rows = await self.store.all(
                "SELECT account_key, person_id, payload FROM identity_links WHERE scope=?",
                (self.scope,),
            )
        next_muted: dict[str, bool] = {}
        next_accounts: dict[str, set[tuple[str, str]]] = {}
        next_acc_to_person: dict[tuple[str, str], str] = {}
        for row in person_rows:
            pid = str(row["person_id"])
            next_muted[pid] = False if pid == "owner" else bool(row["muted"])
            next_accounts[pid] = set()
        for row in link_rows:
            opened = self.store.vault.open(self._link_seal_ctx(row["account_key"]), row["payload"])
            if not isinstance(opened, dict):
                continue
            plat = str(opened.get("platform", ""))
            uid = str(opened.get("user_id", ""))
            pid = str(row["person_id"])
            if not plat or not uid:
                continue
            next_acc_to_person[(plat, uid)] = pid
            next_accounts.setdefault(pid, set()).add((plat, uid))
        self._person_muted = next_muted
        self._person_accounts = next_accounts
        self._account_to_person = next_acc_to_person
        muted_onebot = self.muted_onebot_user_ids()
        if muted_onebot:
            self.s.muted_user_ids = set(getattr(self.s, "muted_user_ids", set())) | muted_onebot

    async def resolve_person_id(
        self,
        platform: str,
        user_id: str,
        nickname: str = "",
        *,
        create: bool = True,
        source: str = "auto",
    ) -> str | None:
        if self._needs_init():
            await self.initialize()
        uid = validate_platform_user(platform, user_id)
        if source not in VALID_LINK_SOURCES:
            raise Rejected("invalid_link_source")

        cached = self._account_to_person.get((platform, uid))
        if cached is not None:
            return cached

        is_owner = self._is_configured_owner_account(platform, uid)
        if is_owner and not create:
            return "owner"

        async with self._lock:
            cached = self._account_to_person.get((platform, uid))
            if cached is not None:
                return cached
            acc_key = self.account_key(platform, uid)
            if not create:
                async with self.store.lock:
                    row = await self.store.one(
                        "SELECT person_id FROM identity_links WHERE account_key=? AND scope=?",
                        (acc_key, self.scope),
                    )
                if row:
                    pid = str(row["person_id"])
                    self._cache_put_link(pid, platform, uid)
                    return pid
                return "owner" if is_owner else None

            now = float(self.clock())
            clean_nick = self._sanitize_nickname(nickname)
            created_new = False
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT person_id FROM identity_links WHERE account_key=? AND scope=?",
                    (acc_key, self.scope),
                )
                if row:
                    pid = str(row["person_id"])
                elif is_owner:
                    pid = "owner"
                    link_payload = {
                        "platform": platform,
                        "user_id": uid,
                        "nickname": clean_nick or "BOISER",
                        "source": "owner_config",
                        "linked_at": now,
                    }
                    await self.store.db.execute(
                        "INSERT INTO identity_links"
                        "(account_key, scope, person_id, platform, payload, updated) "
                        "VALUES (?,?,?,?,?,?)",
                        (
                            acc_key,
                            self.scope,
                            "owner",
                            platform,
                            self.store.vault.seal(self._link_seal_ctx(acc_key), link_payload),
                            now,
                        ),
                    )
                else:
                    pid = "p_" + uuid.uuid4().hex[:16]
                    display_name = clean_nick or f"{platform}:{uid}"[:48]
                    person_payload = {
                        "display_name": display_name,
                        "l2_tags": [],
                        "created_at": now,
                        "updated_at": now,
                        "mute_source": "",
                    }
                    link_payload = {
                        "platform": platform,
                        "user_id": uid,
                        "nickname": clean_nick,
                        "source": source,
                        "linked_at": now,
                    }
                    await self.store.db.execute(
                        "INSERT INTO identity_persons(person_id, scope, muted, payload, updated) "
                        "VALUES (?,?,?,?,?)",
                        (
                            pid,
                            self.scope,
                            0,
                            self.store.vault.seal(self._person_seal_ctx(pid), person_payload),
                            now,
                        ),
                    )
                    await self.store.db.execute(
                        "INSERT INTO identity_links"
                        "(account_key, scope, person_id, platform, payload, updated) "
                        "VALUES (?,?,?,?,?,?)",
                        (
                            acc_key,
                            self.scope,
                            pid,
                            platform,
                            self.store.vault.seal(self._link_seal_ctx(acc_key), link_payload),
                            now,
                        ),
                    )
                    created_new = True
            if created_new:
                self._person_muted[pid] = False
            self._cache_put_link(pid, platform, uid)
            return pid

    async def get_l2_tags(self, platform: str, user_id: str) -> list[str]:
        if self._needs_init():
            await self.initialize()
        pid = await self.resolve_person_id(platform, user_id, create=False)
        if pid is None:
            return []
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM identity_persons WHERE person_id=? AND scope=?",
                (pid, self.scope),
            )
        if not row:
            return []
        opened = self.store.vault.open(self._person_seal_ctx(pid), row["payload"])
        if not isinstance(opened, dict):
            return []
        return self._sanitize_l2_tags(opened.get("l2_tags"))

    async def person_mute_source(self, person_id: str) -> str:
        if self._needs_init():
            await self.initialize()
        async with self.store.lock:
            row = await self.store.one(
                "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                (person_id, self.scope),
            )
        if not row or not row["muted"]:
            return ""
        opened = self.store.vault.open(self._person_seal_ctx(person_id), row["payload"])
        if not isinstance(opened, dict):
            return ""
        return str(opened.get("mute_source", ""))

    async def _load_person_dict_unlocked(
        self, person_id: str, *, affection: Any = None
    ) -> dict[str, Any]:
        async with self.store.lock:
            prow = await self.store.one(
                "SELECT person_id, muted, payload, updated FROM identity_persons "
                "WHERE person_id=? AND scope=?",
                (person_id, self.scope),
            )
            if not prow:
                raise Rejected("person_not_found")
            lrows = await self.store.all(
                "SELECT account_key, payload FROM identity_links "
                "WHERE scope=? AND person_id=? ORDER BY updated ASC",
                (self.scope, person_id),
            )
        opened_p = self.store.vault.open(self._person_seal_ctx(person_id), prow["payload"])
        pdata = dict(opened_p) if isinstance(opened_p, dict) else {}
        accounts: list[dict[str, Any]] = []
        for lrow in lrows:
            opened_l = self.store.vault.open(
                self._link_seal_ctx(lrow["account_key"]), lrow["payload"]
            )
            if not isinstance(opened_l, dict):
                continue
            accounts.append(
                {
                    "platform": str(opened_l.get("platform", "")),
                    "user_id": str(opened_l.get("user_id", "")),
                    "nickname": str(opened_l.get("nickname", "")),
                    "source": str(opened_l.get("source", "auto")),
                    "linked_at": float(opened_l.get("linked_at", 0.0) or 0.0),
                }
            )
        is_owner = person_id == "owner"
        score, lv, tier_label = await self._score_summary_for_person(
            person_id, is_owner=is_owner, affection=affection
        )
        return {
            "person_id": person_id,
            "display_name": str(pdata.get("display_name", "BOISER" if is_owner else person_id)),
            "is_owner": is_owner,
            "muted": False if is_owner else bool(prow["muted"]),
            "score": score,
            "lv": lv,
            "tier_label": tier_label,
            "l2_tags": self._sanitize_l2_tags(pdata.get("l2_tags")),
            "accounts": accounts,
            "updated_at": float(prow["updated"]),
        }

    async def _score_summary_for_person(
        self, person_id: str, *, is_owner: bool, affection: Any = None
    ) -> tuple[int, str, str]:
        from .affection import capabilities_for_score

        aff = affection if affection is not None else getattr(self.store.runtime, "affection", None)
        score = 120 if is_owner else 50
        if aff is not None:
            if hasattr(aff, "person_scores_snapshot"):
                scores = await aff.person_scores_snapshot()
                score = scores.get(person_id, 120 if is_owner else aff.config.initial_score)
            else:
                score = 120 if is_owner else int(getattr(aff.config, "initial_score", 50))
        caps = capabilities_for_score(score, is_owner=is_owner)
        return score, caps.lv, caps.tier_label

    async def _sync_moderation_for_accounts(
        self,
        accounts: list[tuple[str, str]],
        muted: bool,
        *,
        moderation: Any = None,
        source: str = "manual",
    ) -> None:
        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        if mod is None:
            return
        for plat, uid in accounts:
            if (
                plat != "onebot"
                or self.is_owner_account(plat, uid)
                or self._is_known_bot(plat, uid)
            ):
                continue
            if (plat, uid) in self._syncing_accounts:
                continue
            self._syncing_accounts.add((plat, uid))
            try:
                if muted:
                    if source == "ai_affection_zero" and hasattr(mod, "mute_by_affection"):
                        await mod.mute_by_affection(uid)
                    elif hasattr(mod, "set_muted"):
                        await mod.set_muted(uid, True)
                elif hasattr(mod, "set_muted"):
                    await mod.set_muted(uid, False)
            finally:
                self._syncing_accounts.discard((plat, uid))

    async def set_person_muted(
        self,
        person_id: str,
        muted: bool,
        *,
        moderation: Any = None,
        source: str = "manual",
    ) -> bool:
        if self._needs_init():
            await self.initialize()
        if person_id == "owner":
            raise Rejected("protected_identity")
        now = float(self.clock())
        target_muted = bool(muted)
        onebot_uids: set[str] = set()
        linked_list: list[tuple[str, str]] = []

        async with self._lock:
            async with self.store.transaction():
                prow = await self.store.one(
                    "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                    (person_id, self.scope),
                )
                if not prow:
                    raise Rejected("person_not_found")
                lrows = await self.store.all(
                    "SELECT account_key, payload FROM identity_links WHERE scope=? AND person_id=?",
                    (self.scope, person_id),
                )
                for lrow in lrows:
                    opened_l = self.store.vault.open(
                        self._link_seal_ctx(lrow["account_key"]), lrow["payload"]
                    )
                    if isinstance(opened_l, dict):
                        plat = str(opened_l.get("platform", ""))
                        uid = str(opened_l.get("user_id", ""))
                        if plat and uid:
                            if self._is_configured_owner_account(plat, uid):
                                raise Rejected("protected_identity")
                            linked_list.append((plat, uid))
                            if plat == "onebot" and not self._is_known_bot(plat, uid):
                                onebot_uids.add(uid)

                opened_p = self.store.vault.open(self._person_seal_ctx(person_id), prow["payload"])
                pdata = dict(opened_p) if isinstance(opened_p, dict) else {}
                pdata["updated_at"] = now
                pdata["mute_source"] = source if target_muted else ""
                await self.store.db.execute(
                    "UPDATE identity_persons SET muted=?, payload=?, updated=? "
                    "WHERE person_id=? AND scope=?",
                    (
                        1 if target_muted else 0,
                        self.store.vault.seal(self._person_seal_ctx(person_id), pdata),
                        now,
                        person_id,
                        self.scope,
                    ),
                )
            self._person_muted[person_id] = target_muted
            current_muted = set(getattr(self.s, "muted_user_ids", set()))
            if target_muted:
                self.s.muted_user_ids = current_muted | onebot_uids
            else:
                self.s.muted_user_ids = current_muted - onebot_uids

        await self._sync_moderation_for_accounts(
            linked_list, target_muted, moderation=moderation, source=source
        )
        if not target_muted and not onebot_uids:
            aff = getattr(self.store.runtime, "affection", None)
            if aff is not None and hasattr(aff, "on_unmute_person"):
                await aff.on_unmute_person(person_id)
        return target_muted

    async def set_account_muted(
        self,
        platform: str,
        user_id: str,
        muted: bool,
        *,
        moderation: Any = None,
        source: str = "manual",
    ) -> bool:
        if self._needs_init():
            await self.initialize()
        uid = validate_platform_user(platform, user_id)
        if self.is_owner_account(platform, uid):
            raise Rejected("protected_identity")
        pid = await self.resolve_person_id(platform, uid, create=True, source="auto")
        if pid == "owner" or pid is None:
            raise Rejected("protected_identity")
        return await self.set_person_muted(pid, muted, moderation=moderation, source=source)

    async def sync_account_muted(
        self,
        platform: str,
        user_id: str,
        muted: bool,
        *,
        source: str = "manual",
        nickname: str = "",
        moderation: Any = None,
    ) -> bool:
        """Sync a moderation mute/unmute into identity without recursing back into moderation."""
        if self._needs_init():
            await self.initialize()
        uid = validate_platform_user(platform, user_id)
        if self.is_owner_account(platform, uid):
            return False
        if (platform, uid) in self._syncing_accounts:
            return bool(muted)
        pid = await self.resolve_person_id(
            platform, uid, nickname, create=bool(muted), source="auto"
        )
        if pid is None or pid == "owner":
            return False
        self._syncing_accounts.add((platform, uid))
        try:
            return await self.set_person_muted(pid, muted, moderation=moderation, source=source)
        finally:
            self._syncing_accounts.discard((platform, uid))

    async def update_person(
        self,
        person_id: str,
        *,
        display_name: str | None = None,
        l2_tags: list[str] | None = None,
        muted: bool | None = None,
        moderation: Any = None,
    ) -> dict[str, Any]:
        if self._needs_init():
            await self.initialize()
        if muted is True and person_id == "owner":
            raise Rejected("protected_identity")
        now = float(self.clock())
        async with self._lock:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                    (person_id, self.scope),
                )
                if not row:
                    raise Rejected("person_not_found")
                opened = self.store.vault.open(self._person_seal_ctx(person_id), row["payload"])
                pdata = dict(opened) if isinstance(opened, dict) else {}
                if display_name is not None:
                    clean_name = self._sanitize_display_name(display_name)
                    if not clean_name:
                        raise Rejected("invalid_display_name")
                    pdata["display_name"] = clean_name
                if l2_tags is not None:
                    pdata["l2_tags"] = self._sanitize_l2_tags(l2_tags)
                pdata["updated_at"] = now
                await self.store.db.execute(
                    "UPDATE identity_persons SET payload=?, updated=? "
                    "WHERE person_id=? AND scope=?",
                    (
                        self.store.vault.seal(self._person_seal_ctx(person_id), pdata),
                        now,
                        person_id,
                        self.scope,
                    ),
                )
        if muted is not None:
            if person_id == "owner":
                if muted:
                    raise Rejected("protected_identity")
            else:
                await self.set_person_muted(
                    person_id, bool(muted), moderation=moderation, source="admin"
                )
        return await self._load_person_dict_unlocked(person_id)

    async def link_account(
        self,
        person_id: str,
        platform: str,
        user_id: str,
        *,
        nickname: str = "",
        source: str = "admin",
        affection: Any = None,
        moderation: Any = None,
    ) -> dict[str, Any]:
        if self._needs_init():
            await self.initialize()
        uid = validate_platform_user(platform, user_id)
        if source not in VALID_LINK_SOURCES:
            raise Rejected("invalid_link_source")
        if self._is_configured_owner_account(platform, uid) and person_id != "owner":
            raise Rejected("protected_identity")

        aff = affection if affection is not None else getattr(self.store.runtime, "affection", None)
        # Ensure affection is initialized BEFORE taking identity._lock (breaks AB-BA deadlock
        # with AffectionManager.init_lock -> identity._lock).
        fallback_scores: dict[str, int] | None = None
        if aff is not None:
            if hasattr(aff, "initialize"):
                await aff.initialize()
            if hasattr(aff, "_ensure_identity_ready"):
                await aff._ensure_identity_ready()
            if not hasattr(aff, "_person_score_in_tx") and hasattr(aff, "person_scores_snapshot"):
                fallback_scores = await aff.person_scores_snapshot()

        acc_key = self.account_key(platform, uid)
        now = float(self.clock())
        clean_nick = self._sanitize_nickname(nickname)
        same_person_noop = False
        old_pid: str | None = None
        old_remaining = 0
        target_muted = False
        force_zero = False
        merged_in_tx = False
        merge_cache_meta: dict[str, Any] = {}

        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._lock)
            if aff is not None and hasattr(aff, "_lock"):
                await stack.enter_async_context(aff._lock)

            async with self.store.transaction():
                target_row = await self.store.one(
                    "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                    (person_id, self.scope),
                )
                if not target_row:
                    raise Rejected("person_not_found")
                existing_link = await self.store.one(
                    "SELECT person_id, payload FROM identity_links WHERE account_key=? AND scope=?",
                    (acc_key, self.scope),
                )
                old_pid = str(existing_link["person_id"]) if existing_link else None
                if old_pid == person_id:
                    same_person_noop = True
                    if existing_link:
                        opened_l = self.store.vault.open(
                            self._link_seal_ctx(acc_key), existing_link["payload"]
                        )
                        ldata = dict(opened_l) if isinstance(opened_l, dict) else {}
                        if clean_nick:
                            ldata["nickname"] = clean_nick
                        ldata["source"] = source
                        await self.store.db.execute(
                            "UPDATE identity_links SET payload=?, updated=? "
                            "WHERE account_key=? AND scope=?",
                            (
                                self.store.vault.seal(self._link_seal_ctx(acc_key), ldata),
                                now,
                                acc_key,
                                self.scope,
                            ),
                        )
                else:
                    if old_pid == "owner" and self._is_configured_owner_account(platform, uid):
                        raise Rejected("protected_identity")

                    old_was_muted = False
                    old_mute_source = ""
                    old_tags: list[str] = []
                    if old_pid is not None:
                        old_prow = await self.store.one(
                            "SELECT muted, payload FROM identity_persons "
                            "WHERE person_id=? AND scope=?",
                            (old_pid, self.scope),
                        )
                        cnt_row = await self.store.one(
                            "SELECT COUNT(*) FROM identity_links "
                            "WHERE scope=? AND person_id=? AND account_key!=?",
                            (self.scope, old_pid, acc_key),
                        )
                        old_remaining = int(cnt_row[0]) if cnt_row else 0
                        if old_prow:
                            old_was_muted = bool(old_prow["muted"])
                            opened_op = self.store.vault.open(
                                self._person_seal_ctx(old_pid), old_prow["payload"]
                            )
                            if isinstance(opened_op, dict):
                                old_tags = self._sanitize_l2_tags(opened_op.get("l2_tags"))
                                old_mute_source = str(opened_op.get("mute_source", ""))

                    target_muted = False if person_id == "owner" else bool(target_row["muted"])
                    opened_tp = self.store.vault.open(
                        self._person_seal_ctx(person_id), target_row["payload"]
                    )
                    tp_data = dict(opened_tp) if isinstance(opened_tp, dict) else {}
                    if old_tags and old_remaining == 0:
                        tp_data["l2_tags"] = self._sanitize_l2_tags(
                            [*self._sanitize_l2_tags(tp_data.get("l2_tags")), *old_tags]
                        )
                    tp_data["updated_at"] = now

                    if person_id != "owner" and old_was_muted:
                        target_muted = True
                        force_zero = old_mute_source == "ai_affection_zero"
                        tp_data["mute_source"] = old_mute_source or "manual"
                    if (
                        person_id != "owner"
                        and platform == "onebot"
                        and uid in getattr(self.s, "muted_user_ids", set())
                    ):
                        target_muted = True
                        tp_data.setdefault("mute_source", "manual")

                    if (
                        aff is not None
                        and old_pid is not None
                        and old_remaining == 0
                        and person_id != "owner"
                    ):
                        if hasattr(aff, "_person_score_in_tx"):
                            old_score = await aff._person_score_in_tx(self.store.db, old_pid)
                            dst_score = await aff._person_score_in_tx(self.store.db, person_id)
                            zero_hit = (old_score == 0) or (dst_score == 0)
                        elif fallback_scores is not None:
                            zero_hit = (
                                fallback_scores.get(old_pid) == 0
                                or fallback_scores.get(person_id) == 0
                            )
                        else:
                            zero_hit = False
                        if zero_hit:
                            target_muted = True
                            force_zero = True
                            tp_data["mute_source"] = "ai_affection_zero"

                    link_payload = {
                        "platform": platform,
                        "user_id": uid,
                        "nickname": clean_nick or ("BOISER" if person_id == "owner" else ""),
                        "source": source,
                        "linked_at": now,
                    }

                    await self.store.db.execute(
                        "INSERT INTO identity_links"
                        "(account_key, scope, person_id, platform, payload, updated) "
                        "VALUES (?,?,?,?,?,?) "
                        "ON CONFLICT(account_key) DO UPDATE SET "
                        "person_id=excluded.person_id, platform=excluded.platform, "
                        "payload=excluded.payload, updated=excluded.updated",
                        (
                            acc_key,
                            self.scope,
                            person_id,
                            platform,
                            self.store.vault.seal(self._link_seal_ctx(acc_key), link_payload),
                            now,
                        ),
                    )
                    await self.store.db.execute(
                        "UPDATE identity_persons SET muted=?, payload=?, updated=? "
                        "WHERE person_id=? AND scope=?",
                        (
                            0 if person_id == "owner" else int(target_muted),
                            self.store.vault.seal(self._person_seal_ctx(person_id), tp_data),
                            now,
                            person_id,
                            self.scope,
                        ),
                    )
                    if old_pid is not None and old_pid != "owner" and old_remaining == 0:
                        await self.store.db.execute(
                            "DELETE FROM identity_persons WHERE person_id=? AND scope=?",
                            (old_pid, self.scope),
                        )
                    if (
                        aff is not None
                        and old_pid is not None
                        and old_remaining == 0
                        and hasattr(aff, "merge_person_scores_in_tx")
                    ):
                        linked_after = [
                            *self.linked_accounts_cached(person_id),
                            (platform, uid),
                        ]
                        await aff.merge_person_scores_in_tx(
                            self.store.db,
                            old_pid,
                            person_id,
                            force_zero=force_zero,
                            now=now,
                            linked_accounts=linked_after,
                            _cache_out=merge_cache_meta,
                        )
                        merged_in_tx = True

            if not same_person_noop:
                self._cache_put_link(person_id, platform, uid)
                self._person_muted[person_id] = False if person_id == "owner" else target_muted
                if old_pid is not None and old_pid != "owner" and old_remaining == 0:
                    self._person_muted.pop(old_pid, None)
                    self._person_accounts.pop(old_pid, None)
                if merged_in_tx and hasattr(aff, "_finalize_merged_person_caches"):
                    aff._finalize_merged_person_caches(
                        old_pid, person_id, merge_cache_meta, identity=self
                    )

        if same_person_noop:
            return await self._load_person_dict_unlocked(person_id, affection=aff)

        if aff is not None:
            if (
                not merged_in_tx
                and old_pid is not None
                and old_remaining == 0
                and hasattr(aff, "merge_person_scores")
            ):
                await aff.merge_person_scores(old_pid, person_id, force_zero=force_zero)
            elif not merged_in_tx and hasattr(aff, "refresh_person_caches"):
                await aff.refresh_person_caches(person_id)

        target_accounts = self.linked_accounts_cached(person_id)
        if person_id == "owner":
            if platform == "onebot" and uid in getattr(self.s, "muted_user_ids", set()):
                self.s.muted_user_ids = set(self.s.muted_user_ids) - {uid}
                await self._sync_moderation_for_accounts(
                    [(platform, uid)], False, moderation=moderation
                )
        elif target_muted:
            onebot_uids = {
                u
                for p, u in target_accounts
                if p == "onebot"
                and not self.is_owner_account(p, u)
                and not self._is_known_bot(p, u)
            }
            self.s.muted_user_ids = set(getattr(self.s, "muted_user_ids", set())) | onebot_uids
            await self._sync_moderation_for_accounts(
                target_accounts,
                True,
                moderation=moderation,
                source="ai_affection_zero" if force_zero else "manual",
            )
        elif platform == "onebot" and uid in getattr(self.s, "muted_user_ids", set()):
            self.s.muted_user_ids = set(self.s.muted_user_ids) - {uid}
            await self._sync_moderation_for_accounts(
                [(platform, uid)], False, moderation=moderation
            )

        return await self._load_person_dict_unlocked(person_id, affection=aff)

    async def unlink_account(
        self,
        platform: str,
        user_id: str,
        *,
        moderation: Any = None,
    ) -> dict[str, Any]:
        if self._needs_init():
            await self.initialize()
        uid = validate_platform_user(platform, user_id)
        configured_owners = self._configured_owner_accounts()
        if (platform, uid) in configured_owners:
            same_platform_owners = [acc for acc in configured_owners if acc[0] == platform]
            if len(configured_owners) <= 1 or (
                platform == "onebot"
                and uid == self.s.owner_qq_id
                and len(same_platform_owners) <= 1
            ):
                raise Rejected("protected_identity")

        acc_key = self.account_key(platform, uid)
        now = float(self.clock())
        aff = getattr(self.store.runtime, "affection", None)

        async with self._lock:
            async with self.store.transaction():
                lrow = await self.store.one(
                    "SELECT person_id, payload FROM identity_links WHERE account_key=? AND scope=?",
                    (acc_key, self.scope),
                )
                if not lrow:
                    raise Rejected("account_not_found")
                old_pid = str(lrow["person_id"])
                if old_pid == "owner":
                    cnt_row = await self.store.one(
                        "SELECT COUNT(*) FROM identity_links WHERE scope=? AND person_id='owner'",
                        (self.scope,),
                    )
                    owner_link_count = int(cnt_row[0]) if cnt_row else 0
                    if (platform, uid) in configured_owners and owner_link_count <= 1:
                        raise Rejected("protected_identity")
                cnt_old = await self.store.one(
                    "SELECT COUNT(*) FROM identity_links "
                    "WHERE scope=? AND person_id=? AND account_key!=?",
                    (self.scope, old_pid, acc_key),
                )
                old_remaining = int(cnt_old[0]) if cnt_old else 0

                opened_l = self.store.vault.open(self._link_seal_ctx(acc_key), lrow["payload"])
                ldata = dict(opened_l) if isinstance(opened_l, dict) else {}
                clean_nick = self._sanitize_nickname(ldata.get("nickname", ""))

                new_pid = "p_" + uuid.uuid4().hex[:16]
                new_person_payload = {
                    "display_name": clean_nick or f"{platform}:{uid}"[:48],
                    "l2_tags": [],
                    "created_at": now,
                    "updated_at": now,
                    "mute_source": "",
                }
                ldata.update(platform=platform, user_id=uid, source="admin", linked_at=now)

                await self.store.db.execute(
                    "INSERT INTO identity_persons(person_id, scope, muted, payload, updated) "
                    "VALUES (?,?,?,?,?)",
                    (
                        new_pid,
                        self.scope,
                        0,
                        self.store.vault.seal(self._person_seal_ctx(new_pid), new_person_payload),
                        now,
                    ),
                )
                await self.store.db.execute(
                    "UPDATE identity_links SET person_id=?, payload=?, updated=? "
                    "WHERE account_key=? AND scope=?",
                    (
                        new_pid,
                        self.store.vault.seal(self._link_seal_ctx(acc_key), ldata),
                        now,
                        acc_key,
                        self.scope,
                    ),
                )
                if old_pid != "owner" and old_remaining == 0:
                    await self.store.db.execute(
                        "DELETE FROM identity_persons WHERE person_id=? AND scope=?",
                        (old_pid, self.scope),
                    )

            self._person_muted[new_pid] = False
            self._cache_put_link(new_pid, platform, uid)
            if old_pid != "owner" and old_remaining == 0:
                self._person_muted.pop(old_pid, None)
                self._person_accounts.pop(old_pid, None)

        if platform == "onebot" and uid in getattr(self.s, "muted_user_ids", set()):
            self.s.muted_user_ids = set(self.s.muted_user_ids) - {uid}
            await self._sync_moderation_for_accounts(
                [(platform, uid)], False, moderation=moderation
            )
        if aff is not None and hasattr(aff, "refresh_person_caches"):
            await aff.refresh_person_caches(new_pid)
            if old_remaining > 0:
                await aff.refresh_person_caches(old_pid)

        result = await self._load_person_dict_unlocked(new_pid, affection=aff)
        result["previous_person_id"] = old_pid
        return result

    async def merge_persons(
        self,
        source_person_id: str,
        target_person_id: str,
        *,
        allow_owner: bool = False,
        affection: Any = None,
        moderation: Any = None,
    ) -> dict[str, Any]:
        if self._needs_init():
            await self.initialize()
        if source_person_id == target_person_id:
            raise Rejected("cannot_merge_same_person")
        if "owner" in {source_person_id, target_person_id}:
            if not allow_owner:
                raise Rejected("protected_identity")
            # Always preserve "owner" as the canonical surviving person_id.
            if source_person_id == "owner":
                source_person_id, target_person_id = target_person_id, "owner"

        aff = affection if affection is not None else getattr(self.store.runtime, "affection", None)
        # Ensure affection is initialized BEFORE taking identity._lock (breaks AB-BA deadlock).
        fallback_scores: dict[str, int] | None = None
        if aff is not None:
            if hasattr(aff, "initialize"):
                await aff.initialize()
            if hasattr(aff, "_ensure_identity_ready"):
                await aff._ensure_identity_ready()
            if not hasattr(aff, "_person_score_in_tx") and hasattr(aff, "person_scores_snapshot"):
                fallback_scores = await aff.person_scores_snapshot()

        now = float(self.clock())
        merged_in_tx = False
        merge_cache_meta: dict[str, Any] = {}

        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._lock)
            if aff is not None and hasattr(aff, "_lock"):
                await stack.enter_async_context(aff._lock)

            async with self.store.transaction():
                src_row = await self.store.one(
                    "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                    (source_person_id, self.scope),
                )
                dst_row = await self.store.one(
                    "SELECT muted, payload FROM identity_persons WHERE person_id=? AND scope=?",
                    (target_person_id, self.scope),
                )
                if not src_row or not dst_row:
                    raise Rejected("person_not_found")
                src_links = await self.store.all(
                    "SELECT account_key, payload FROM identity_links WHERE scope=? AND person_id=?",
                    (self.scope, source_person_id),
                )
                dst_links = await self.store.all(
                    "SELECT account_key, payload FROM identity_links WHERE scope=? AND person_id=?",
                    (self.scope, target_person_id),
                )

                src_opened = self.store.vault.open(
                    self._person_seal_ctx(source_person_id), src_row["payload"]
                )
                dst_opened = self.store.vault.open(
                    self._person_seal_ctx(target_person_id), dst_row["payload"]
                )
                src_data = dict(src_opened) if isinstance(src_opened, dict) else {}
                dst_data = dict(dst_opened) if isinstance(dst_opened, dict) else {}

                src_accounts: list[tuple[str, str]] = []
                dst_accounts: list[tuple[str, str]] = []
                for lrow in src_links:
                    ol = self.store.vault.open(
                        self._link_seal_ctx(lrow["account_key"]), lrow["payload"]
                    )
                    if isinstance(ol, dict) and ol.get("platform") and ol.get("user_id"):
                        src_accounts.append((str(ol["platform"]), str(ol["user_id"])))
                for lrow in dst_links:
                    ol = self.store.vault.open(
                        self._link_seal_ctx(lrow["account_key"]), lrow["payload"]
                    )
                    if isinstance(ol, dict) and ol.get("platform") and ol.get("user_id"):
                        dst_accounts.append((str(ol["platform"]), str(ol["user_id"])))

                all_accounts = [*dst_accounts, *src_accounts]
                is_owner_target = target_person_id == "owner"

                src_score_zero = False
                dst_score_zero = False
                if aff is not None:
                    if hasattr(aff, "_person_score_in_tx"):
                        src_score_zero = (
                            await aff._person_score_in_tx(self.store.db, source_person_id) == 0
                        )
                        dst_score_zero = (
                            await aff._person_score_in_tx(self.store.db, target_person_id) == 0
                        )
                    elif fallback_scores is not None:
                        src_score_zero = fallback_scores.get(source_person_id) == 0
                        dst_score_zero = fallback_scores.get(target_person_id) == 0

                any_onebot_muted = any(
                    p == "onebot" and u in getattr(self.s, "muted_user_ids", set())
                    for p, u in all_accounts
                )
                either_muted = (
                    bool(src_row["muted"])
                    or bool(dst_row["muted"])
                    or any_onebot_muted
                    or src_score_zero
                    or dst_score_zero
                )
                merged_muted = False if is_owner_target else either_muted
                force_zero = (not is_owner_target) and either_muted

                merged_tags = self._sanitize_l2_tags(
                    [
                        *self._sanitize_l2_tags(dst_data.get("l2_tags")),
                        *self._sanitize_l2_tags(src_data.get("l2_tags")),
                    ]
                )
                merged_display = (
                    self._sanitize_display_name(dst_data.get("display_name"))
                    or self._sanitize_display_name(src_data.get("display_name"))
                    or ("BOISER" if is_owner_target else target_person_id)
                )
                mute_source = ""
                if merged_muted:
                    if src_score_zero or dst_score_zero:
                        mute_source = "ai_affection_zero"
                    else:
                        mute_source = str(
                            dst_data.get("mute_source") or src_data.get("mute_source") or "manual"
                        )

                merged_payload = {
                    "display_name": merged_display,
                    "l2_tags": merged_tags,
                    "created_at": float(dst_data.get("created_at", now) or now),
                    "updated_at": now,
                    "mute_source": mute_source,
                }

                await self.store.db.execute(
                    "UPDATE identity_links SET person_id=?, updated=? "
                    "WHERE scope=? AND person_id=?",
                    (target_person_id, now, self.scope, source_person_id),
                )
                await self.store.db.execute(
                    "UPDATE identity_persons SET muted=?, payload=?, updated=? "
                    "WHERE person_id=? AND scope=?",
                    (
                        1 if merged_muted else 0,
                        self.store.vault.seal(
                            self._person_seal_ctx(target_person_id), merged_payload
                        ),
                        now,
                        target_person_id,
                        self.scope,
                    ),
                )
                await self.store.db.execute(
                    "DELETE FROM identity_persons WHERE person_id=? AND scope=?",
                    (source_person_id, self.scope),
                )
                if aff is not None and hasattr(aff, "merge_person_scores_in_tx"):
                    await aff.merge_person_scores_in_tx(
                        self.store.db,
                        source_person_id,
                        target_person_id,
                        force_zero=force_zero,
                        now=now,
                        linked_accounts=all_accounts,
                        _cache_out=merge_cache_meta,
                    )
                    merged_in_tx = True

            for plat, uid in src_accounts:
                self._cache_put_link(target_person_id, plat, uid)
            self._person_muted[target_person_id] = merged_muted
            self._person_muted.pop(source_person_id, None)
            self._person_accounts.pop(source_person_id, None)
            if merged_in_tx and hasattr(aff, "_finalize_merged_person_caches"):
                aff._finalize_merged_person_caches(
                    source_person_id, target_person_id, merge_cache_meta, identity=self
                )

        if not merged_in_tx and aff is not None and hasattr(aff, "merge_person_scores"):
            await aff.merge_person_scores(source_person_id, target_person_id, force_zero=force_zero)

        onebot_uids = {
            u
            for p, u in all_accounts
            if p == "onebot" and not self.is_owner_account(p, u) and not self._is_known_bot(p, u)
        }
        if is_owner_target:
            if onebot_uids:
                self.s.muted_user_ids = set(getattr(self.s, "muted_user_ids", set())) - onebot_uids
                await self._sync_moderation_for_accounts(
                    [("onebot", u) for u in onebot_uids], False, moderation=moderation
                )
        elif merged_muted:
            self.s.muted_user_ids = set(getattr(self.s, "muted_user_ids", set())) | onebot_uids
            await self._sync_moderation_for_accounts(
                all_accounts,
                True,
                moderation=moderation,
                source="ai_affection_zero" if force_zero else "manual",
            )

        result = await self._load_person_dict_unlocked(target_person_id, affection=aff)
        result["merged_from"] = source_person_id
        return result

    def _prune_bind_state(self, now: float) -> None:
        expired_keys = [k for k, v in self._bind_codes.items() if v.expires_at <= now]
        for k in expired_keys:
            self._bind_codes.pop(k, None)
        for acc, hist in list(self._bind_gen_history.items()):
            while hist and (now - hist[0]) >= BIND_GEN_WINDOW_SECONDS:
                hist.popleft()
            if not hist:
                self._bind_gen_history.pop(acc, None)
        for acc, (fails, locked_until) in list(self._bind_fail_state.items()):
            if locked_until > 0 and now >= locked_until:
                self._bind_fail_state.pop(acc, None)
            elif fails == 0 and locked_until == 0:
                self._bind_fail_state.pop(acc, None)

    def _validate_bind_caller(self, message: Message) -> tuple[str, str]:
        if message.kind != "private":
            raise Rejected("bind_private_only")
        uid = validate_platform_user(message.platform, message.user_id)
        if message.conversation_id != uid:
            raise Rejected("bind_private_only")
        if self._is_known_bot(message.platform, uid, message.bot_id):
            raise Rejected("bind_bot_forbidden")
        if self.is_owner_account(message.platform, uid) or is_persona_owner(message, self.s):
            raise Rejected("protected_identity")
        if self.is_muted(message.platform, uid):
            raise Rejected("bind_muted_forbidden")
        return message.platform, uid

    async def create_bind_code(self, message: Message) -> str:
        if self._needs_init():
            await self.initialize()
        platform, uid = self._validate_bind_caller(message)
        pid = await self.resolve_person_id(
            platform, uid, message.sender_nickname, create=True, source="auto"
        )
        if pid is None or pid == "owner":
            raise Rejected("protected_identity")
        if self._person_muted.get(pid, False):
            raise Rejected("bind_muted_forbidden")

        async with self._bind_lock:
            now = float(self.clock())
            self._prune_bind_state(now)
            hist = self._bind_gen_history.setdefault((platform, uid), deque())
            if len(hist) >= BIND_GEN_LIMIT:
                raise Rejected("bind_rate_limited")

            for _ in range(8):
                suffix = "".join(secrets.choice(BIND_ALPHABET) for _ in range(8))
                code = f"HK-{suffix}"
                code_digest = self.store.vault.digest("bind-code", code)
                if code_digest not in self._bind_codes:
                    break
            else:
                raise Rejected("bind_code_generation_failed")

            hist.append(now)
            self._bind_codes[code_digest] = _PendingBindCode(
                code_digest=code_digest,
                person_id=pid,
                platform=platform,
                user_id=uid,
                expires_at=now + BIND_CODE_TTL_SECONDS,
            )
            return code

    async def redeem_bind_code(
        self,
        message: Message,
        raw_code: str,
        *,
        affection: Any = None,
        moderation: Any = None,
    ) -> str:
        if self._needs_init():
            await self.initialize()
        platform, uid = self._validate_bind_caller(message)

        async with self._bind_lock:
            now = float(self.clock())
            self._prune_bind_state(now)
            fails, locked_until = self._bind_fail_state.get((platform, uid), (0, 0.0))
            if locked_until > now:
                raise Rejected("bind_cooldown")

            normalized = str(raw_code or "").strip().upper()
            entry: _PendingBindCode | None = None
            code_digest = ""
            if BIND_CODE_RE.fullmatch(normalized):
                code_digest = self.store.vault.digest("bind-code", normalized)
                entry = self._bind_codes.get(code_digest)

            if entry is None or entry.expires_at <= now:
                if entry is not None:
                    self._bind_codes.pop(code_digest, None)
                fails += 1
                next_lock = (now + BIND_COOLDOWN_SECONDS) if fails >= BIND_MAX_WRONG_TRIES else 0.0
                self._bind_fail_state[(platform, uid)] = (fails, next_lock)
                raise Rejected("bind_cooldown" if next_lock > 0 else "invalid_bind_code")

            if entry.platform == platform and entry.user_id == uid:
                raise Rejected("bind_same_account")
            if entry.person_id == "owner" or self.is_owner_account(entry.platform, entry.user_id):
                raise Rejected("protected_identity")
            if self._person_muted.get(entry.person_id, False) or self.is_muted(
                entry.platform, entry.user_id
            ):
                raise Rejected("bind_muted_forbidden")

            redeemer_pid = await self.resolve_person_id(
                platform, uid, message.sender_nickname, create=True, source="auto"
            )
            if redeemer_pid is None or redeemer_pid == "owner":
                raise Rejected("protected_identity")
            if self._person_muted.get(redeemer_pid, False):
                raise Rejected("bind_muted_forbidden")
            if redeemer_pid == entry.person_id:
                raise Rejected("bind_already_linked")

            # Consume immediately so concurrent redemptions cannot reuse this one-time code.
            self._bind_codes.pop(code_digest, None)
            self._bind_fail_state.pop((platform, uid), None)

        await self.merge_persons(
            redeemer_pid,
            entry.person_id,
            allow_owner=False,
            affection=affection,
            moderation=moderation,
        )
        # Record that the redeemed account was attached via bind_code.
        acc_key = self.account_key(platform, uid)
        async with self._lock:
            async with self.store.transaction():
                lrow = await self.store.one(
                    "SELECT payload FROM identity_links WHERE account_key=? AND scope=?",
                    (acc_key, self.scope),
                )
                if lrow:
                    opened_l = self.store.vault.open(self._link_seal_ctx(acc_key), lrow["payload"])
                    ldata = dict(opened_l) if isinstance(opened_l, dict) else {}
                    ldata["source"] = "bind_code"
                    await self.store.db.execute(
                        "UPDATE identity_links SET payload=?, updated=? "
                        "WHERE account_key=? AND scope=?",
                        (
                            self.store.vault.seal(self._link_seal_ctx(acc_key), ldata),
                            float(self.clock()),
                            acc_key,
                            self.scope,
                        ),
                    )
        return entry.person_id

    def _prune_owner_recaps(self, now: float) -> None:
        cutoff = now - OWNER_RECAP_TTL_SECONDS
        while self._owner_recaps and self._owner_recaps[0]["created_at"] <= cutoff:
            self._owner_recaps.popleft()

    def _is_private_owner_message(self, message: Message | None) -> bool:
        if message is None or message.kind != "private":
            return False
        return self.is_owner_account(message.platform, message.user_id) or is_persona_owner(
            message, self.s
        )

    def record_owner_recap(
        self,
        platform: str,
        summary: str,
        *,
        now: float | None = None,
        message: Message | None = None,
        private_owner: bool | None = None,
    ) -> None:
        if private_owner is False:
            return
        if message is not None and not self._is_private_owner_message(message):
            return
        if not isinstance(platform, str) or not PLATFORM_ID_RE.fullmatch(platform):
            return
        ts = float(self.clock() if now is None else now)
        self._prune_owner_recaps(ts)

        cleaned = _COLLAPSE_WS_RE.sub(" ", clean_text(str(summary or ""))).strip()
        if not cleaned:
            return
        cleaned = self._scrub_secrets(cleaned)
        cleaned = _ROLE_RE.sub("[untrusted_role=", cleaned)
        cleaned = _CQ_RE.sub("［CQ:", cleaned)
        cleaned = _AT_BROADCAST_RE.sub(r"＠\1", cleaned)
        cleaned = html.escape(cleaned, quote=False)
        cleaned = _COLLAPSE_WS_RE.sub(" ", cleaned).strip()
        if not cleaned:
            return
        if len(cleaned) > OWNER_RECAP_MAX_CHARS:
            cleaned = cleaned[: OWNER_RECAP_MAX_CHARS - 1].rstrip() + "…"
        self._owner_recaps.append({"platform": platform, "summary": cleaned, "created_at": ts})

    def owner_other_surface_recent(
        self,
        current_platform: str,
        *,
        limit: int = 6,
        now: float | None = None,
        message: Message | None = None,
        private_owner: bool | None = None,
    ) -> list[dict[str, Any]]:
        if private_owner is False:
            return []
        if message is not None and not self._is_private_owner_message(message):
            return []
        ts = float(self.clock() if now is None else now)
        self._prune_owner_recaps(ts)
        capped_limit = max(0, min(int(limit), OWNER_RECAP_MAX_ENTRIES))
        if capped_limit == 0:
            return []

        candidates = [
            dict(item)
            for item in reversed(self._owner_recaps)
            if item["platform"] != current_platform
        ]
        selected: list[dict[str, Any]] = []
        for item in candidates:
            if len(selected) >= capped_limit:
                break
            tentative = [item, *selected]
            if estimate_tokens(json.dumps(tentative, ensure_ascii=False)) > OWNER_RECAP_MAX_TOKENS:
                break
            selected = tentative
        return selected

    async def list_persons(self, *, affection: Any = None) -> dict[str, Any]:
        if self._needs_init():
            await self.initialize()
        now = float(self.clock())
        self._prune_bind_state(now)
        self._prune_owner_recaps(now)

        aff = affection if affection is not None else getattr(self.store.runtime, "affection", None)
        scores: dict[str, int] = {}
        default_member_score = 50
        if aff is not None:
            default_member_score = int(getattr(aff.config, "initial_score", 50))
            if hasattr(aff, "person_scores_snapshot"):
                scores = await aff.person_scores_snapshot()

        from .affection import capabilities_for_score

        async with self.store.lock:
            person_rows = await self.store.all(
                "SELECT person_id, muted, payload, updated FROM identity_persons "
                "WHERE scope=? ORDER BY updated DESC LIMIT 1000",
                (self.scope,),
            )
            person_ids = [str(r["person_id"]) for r in person_rows]
            link_rows: list[Any] = []
            if person_ids:
                for start in range(0, len(person_ids), 500):
                    batch = person_ids[start : start + 500]
                    placeholders = ",".join("?" for _ in batch)
                    batch_rows = await self.store.all(
                        "SELECT account_key, person_id, payload, updated FROM identity_links "
                        f"WHERE scope=? AND person_id IN ({placeholders}) ORDER BY updated ASC",
                        (self.scope, *batch),
                    )
                    link_rows.extend(batch_rows)
                if len(person_ids) > 500:
                    link_rows.sort(key=lambda r: float(r["updated"]))

        accounts_by_person: dict[str, list[dict[str, Any]]] = {}
        for lrow in link_rows:
            opened_l = self.store.vault.open(
                self._link_seal_ctx(lrow["account_key"]), lrow["payload"]
            )
            if not isinstance(opened_l, dict):
                continue
            pid = str(lrow["person_id"])
            accounts_by_person.setdefault(pid, []).append(
                {
                    "platform": str(opened_l.get("platform", "")),
                    "user_id": str(opened_l.get("user_id", "")),
                    "nickname": str(opened_l.get("nickname", "")),
                    "source": str(opened_l.get("source", "auto")),
                    "linked_at": float(opened_l.get("linked_at", 0.0) or 0.0),
                }
            )

        persons: list[dict[str, Any]] = []
        for prow in person_rows:
            pid = str(prow["person_id"])
            is_owner = pid == "owner"
            opened_p = self.store.vault.open(self._person_seal_ctx(pid), prow["payload"])
            pdata = dict(opened_p) if isinstance(opened_p, dict) else {}
            score = scores.get(pid, 120 if is_owner else default_member_score)
            caps = capabilities_for_score(score, is_owner=is_owner)
            persons.append(
                {
                    "person_id": pid,
                    "display_name": str(pdata.get("display_name", "BOISER" if is_owner else pid)),
                    "is_owner": is_owner,
                    "muted": False if is_owner else bool(prow["muted"]),
                    "score": score,
                    "lv": caps.lv,
                    "tier_label": caps.tier_label,
                    "l2_tags": self._sanitize_l2_tags(pdata.get("l2_tags")),
                    "accounts": accounts_by_person.get(pid, []),
                    "updated_at": float(prow["updated"]),
                }
            )

        persons.sort(key=lambda item: (0 if item["is_owner"] else 1, -item["updated_at"]))
        return {
            "persons": persons,
            "owner_recap_count": len(self._owner_recaps),
            "pending_bind_codes": len(self._bind_codes),
        }

    def status_summary(self) -> dict[str, Any]:
        now = float(self.clock())
        self._prune_bind_state(now)
        self._prune_owner_recaps(now)
        return {
            "initialized": self.initialized,
            "persons_count": len(self._person_muted),
            "accounts_count": len(self._account_to_person),
            "muted_persons_count": sum(1 for muted in self._person_muted.values() if muted),
            "owner_recap_count": len(self._owner_recaps),
            "pending_bind_codes": len(self._bind_codes),
        }
