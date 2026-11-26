"""Encrypted persistence and validation for remote social chat platform configs."""

from __future__ import annotations

import re
import weakref
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .security import Rejected

REMOTE_CHAT_CONFIG_PREFIX = "remote_chat_config:"
RESERVED_SOCIAL_PLATFORMS = frozenset({"onebot", "qq"})
PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}\Z")
REMOTE_CHAT_ID_RE = re.compile(r"^-?[0-9]{1,24}\Z")
MAX_REMOTE_CHAT_IDS = 100

PrivateMode = Literal["owner_only", "owner_and_allowlist"]

# Weakly-keyed per-Settings in-memory cache so synchronous policy checks
# (e.g. permits_memory_mirror / allowed_activity_group) observe saved overrides
# without mutating Settings attributes or leaking references across tests.
_SETTINGS_CONFIG_CACHE: dict[int, tuple[weakref.ref[Any], dict[str, RemoteChatConfig]]] = {}


def validate_remote_chat_platform(platform: str) -> str:
    """Validate that `platform` is a legal non-QQ remote social platform ID."""
    if (
        not isinstance(platform, str)
        or not PLATFORM_ID_RE.fullmatch(platform)
        or platform in RESERVED_SOCIAL_PLATFORMS
    ):
        raise Rejected("invalid_platform")
    return platform


def config_storage_key(platform: str) -> str:
    """Return the `admin_config` primary key for a remote social chat platform."""
    return f"{REMOTE_CHAT_CONFIG_PREFIX}{validate_remote_chat_platform(platform)}"


def normalize_remote_chat_ids(
    values: Any,
    *,
    strict: bool = True,
    platform: str | None = None,
) -> tuple[str, ...]:
    """Validate, deduplicate, and sort numeric chat/user IDs (max 100)."""
    if values is None:
        if strict:
            raise ValueError("Expected a list of numeric identifiers")
        return ()
    if not isinstance(values, (list, tuple)):
        if strict:
            raise ValueError("Expected a list of numeric identifiers")
        return ()
    if strict and len(values) > MAX_REMOTE_CHAT_IDS:
        raise ValueError(f"Expected at most {MAX_REMOTE_CHAT_IDS} identifiers")

    deduped: set[str] = set()
    plat_prefix = f"{platform}:" if platform else ""
    for raw in values:
        if not isinstance(raw, str):
            if strict:
                raise ValueError("Identifiers must be strings")
            continue
        candidate = raw
        if not strict:
            candidate = candidate.strip()
            if plat_prefix and candidate.startswith(plat_prefix):
                candidate = candidate[len(plat_prefix) :].strip()
        if not REMOTE_CHAT_ID_RE.fullmatch(candidate):
            if strict:
                raise ValueError("Invalid remote chat identifier")
            continue
        deduped.add(candidate)
        if len(deduped) > MAX_REMOTE_CHAT_IDS:
            if strict:
                raise ValueError(f"Expected at most {MAX_REMOTE_CHAT_IDS} identifiers")
            break
    return tuple(sorted(deduped))


class RemoteChatConfig(BaseModel):
    """Server-owned admission and policy overrides for a remote social chat plugin."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    allowed_groups: tuple[str, ...] = ()
    proactive_groups: tuple[str, ...] = ()
    private_mode: PrivateMode = "owner_only"
    allowed_users: tuple[str, ...] = ()
    memory_mirror_enabled: bool = False

    @field_validator("allowed_groups", "proactive_groups", "allowed_users", mode="before")
    @classmethod
    def _validate_ids(cls, value: Any) -> tuple[str, ...]:
        return normalize_remote_chat_ids(value, strict=True)

    @model_validator(mode="after")
    def _intersect_proactive_groups(self) -> RemoteChatConfig:
        allowed_set = set(self.allowed_groups)
        intersected = tuple(sorted(gid for gid in self.proactive_groups if gid in allowed_set))
        if intersected != self.proactive_groups:
            return self.model_copy(update={"proactive_groups": intersected})
        return self

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain JSON-compatible dict matching the Admin API contract."""
        return self.model_dump(mode="json")

    def to_rpc_params(self, *, platform: str | None = None) -> dict[str, Any]:
        """Build the `config.update` RPC parameter dict sent over PluginBus."""
        params: dict[str, Any] = {
            "allowed_groups": list(self.allowed_groups),
            "proactive_groups": list(self.proactive_groups),
            "private_mode": self.private_mode,
            "allowed_users": list(self.allowed_users),
            "memory_mirror_enabled": bool(self.memory_mirror_enabled),
            "reset_to_env": False,
        }
        if platform is not None:
            params["platform"] = platform
        return params

    @classmethod
    def from_untrusted(cls, raw: Any, *, platform: str | None = None) -> RemoteChatConfig:
        """Build a safe RemoteChatConfig from a plugin-reported status or env_defaults dict."""
        if not isinstance(raw, dict):
            return cls()
        allowed_groups = normalize_remote_chat_ids(
            raw.get("allowed_groups"), strict=False, platform=platform
        )
        proactive_groups = normalize_remote_chat_ids(
            raw.get("proactive_groups"), strict=False, platform=platform
        )
        allowed_users = normalize_remote_chat_ids(
            raw.get("allowed_users"), strict=False, platform=platform
        )
        raw_mode = raw.get("private_mode")
        private_mode: PrivateMode = (
            "owner_and_allowlist" if raw_mode == "owner_and_allowlist" else "owner_only"
        )
        # Never trust a remote plugin's reported status/env_defaults to enable memory mirroring.
        return cls(
            allowed_groups=list(allowed_groups),
            proactive_groups=list(proactive_groups),
            private_mode=private_mode,
            allowed_users=list(allowed_users),
            memory_mirror_enabled=False,
        )


def _settings_cache_bucket(
    settings: Any, *, create: bool = False
) -> dict[str, RemoteChatConfig] | None:
    if settings is None:
        return None
    key = id(settings)
    entry = _SETTINGS_CONFIG_CACHE.get(key)
    if entry is not None and entry[0]() is settings:
        return entry[1]
    if not create:
        return None
    bucket: dict[str, RemoteChatConfig] = {}
    try:
        ref = weakref.ref(settings, lambda _r, k=key: _SETTINGS_CONFIG_CACHE.pop(k, None))
    except TypeError:
        return None
    _SETTINGS_CONFIG_CACHE[key] = (ref, bucket)
    return bucket


def cache_remote_chat_config(settings: Any, platform: str, config: RemoteChatConfig | None) -> None:
    """Update or clear the in-memory cached override for `(settings, platform)`."""
    if settings is None:
        return
    try:
        plat = validate_remote_chat_platform(platform)
    except Rejected:
        return
    bucket = _settings_cache_bucket(settings, create=config is not None)
    if bucket is None:
        return
    if config is None:
        bucket.pop(plat, None)
    else:
        bucket[plat] = config


def cached_remote_chat_config(settings: Any, platform: str) -> RemoteChatConfig | None:
    """Return the cached RemoteChatConfig for `(settings, platform)`, if any."""
    if settings is None:
        return None
    bucket = _settings_cache_bucket(settings, create=False)
    if bucket is None:
        return None
    return bucket.get(platform)


async def _admin_config_table_exists(store: Any) -> bool:
    if store is None or getattr(store, "db", None) is None:
        return False
    row = await store.one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='admin_config'")
    return row is not None


async def load_remote_chat_config(
    store: Any, platform: str, *, settings: Any = None
) -> RemoteChatConfig | None:
    """Load the Vault-encrypted RemoteChatConfig override for `platform`, or None."""
    plat = validate_remote_chat_platform(platform)
    key = f"{REMOTE_CHAT_CONFIG_PREFIX}{plat}"
    if store is None or getattr(store, "db", None) is None:
        return None
    async with store.lock:
        if not await _admin_config_table_exists(store):
            return None
        row = await store.one("SELECT payload FROM admin_config WHERE key=?", (key,))
    if row is None:
        if settings is not None:
            cache_remote_chat_config(settings, plat, None)
        return None
    data = store.vault.open("admin:" + key, row["payload"])
    cfg = RemoteChatConfig.model_validate(data)
    if settings is not None:
        cache_remote_chat_config(settings, plat, cfg)
    return cfg


async def load_all_remote_chat_configs(
    store: Any, *, settings: Any = None
) -> dict[str, RemoteChatConfig]:
    """Load all Vault-encrypted remote social chat config overrides from `admin_config`."""
    if store is None or getattr(store, "db", None) is None:
        return {}
    async with store.lock:
        if not await _admin_config_table_exists(store):
            return {}
        rows = await store.all(
            "SELECT key, payload FROM admin_config WHERE key LIKE ? ORDER BY key ASC",
            (f"{REMOTE_CHAT_CONFIG_PREFIX}%",),
        )
    configs: dict[str, RemoteChatConfig] = {}
    for row in rows:
        key = str(row["key"])
        plat = key[len(REMOTE_CHAT_CONFIG_PREFIX) :]
        try:
            validate_remote_chat_platform(plat)
            data = store.vault.open("admin:" + key, row["payload"])
            cfg = RemoteChatConfig.model_validate(data)
        except Exception:
            continue
        configs[plat] = cfg
        if settings is not None:
            cache_remote_chat_config(settings, plat, cfg)
    return configs


async def save_remote_chat_config(
    store: Any,
    platform: str,
    config: RemoteChatConfig | dict[str, Any],
    *,
    settings: Any = None,
) -> RemoteChatConfig:
    """Persist encrypted `remote_chat_config:<platform>` and seed default `at_only` policies."""
    plat = validate_remote_chat_platform(platform)
    cfg = (
        config if isinstance(config, RemoteChatConfig) else RemoteChatConfig.model_validate(config)
    )
    key = f"{REMOTE_CHAT_CONFIG_PREFIX}{plat}"
    sealed_cfg = store.vault.seal("admin:" + key, cfg.to_dict())
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO admin_config VALUES (?,?) ON CONFLICT(key) "
            "DO UPDATE SET payload=excluded.payload",
            (key, sealed_cfg),
        )
        for gid in cfg.allowed_groups:
            policy_key = f"policy:{plat}:{gid}"
            await store.db.execute(
                "INSERT OR IGNORE INTO admin_config VALUES (?,?)",
                (policy_key, store.vault.seal("admin:" + policy_key, "at_only")),
            )
    target_settings = settings if settings is not None else getattr(store, "s", None)
    if target_settings is not None:
        policies = getattr(target_settings, "group_policies", None)
        if isinstance(policies, dict):
            for gid in cfg.allowed_groups:
                policies.setdefault(f"{plat}:{gid}", "at_only")
        cache_remote_chat_config(target_settings, plat, cfg)
    return cfg


async def delete_remote_chat_config(store: Any, platform: str, *, settings: Any = None) -> bool:
    """Delete the persisted `remote_chat_config:<platform>` override to revert to env defaults."""
    plat = validate_remote_chat_platform(platform)
    key = f"{REMOTE_CHAT_CONFIG_PREFIX}{plat}"
    async with store.transaction():
        cur = await store.db.execute("DELETE FROM admin_config WHERE key=?", (key,))
        deleted = cur.rowcount > 0
    target_settings = settings if settings is not None else getattr(store, "s", None)
    if target_settings is not None:
        cache_remote_chat_config(target_settings, plat, None)
    return deleted
