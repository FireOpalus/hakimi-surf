"""Encrypted mutable settings and bounded, content-free operational telemetry."""

import asyncio
import json
import logging
import re
import time
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from .admission_policy import AdmissionConfig
from .affection import AffectionManager
from .attention import AttentionRouter
from .bot_server import is_persona_owner
from .compression_policy import CompressionConfig
from .debounce_policy import DebounceConfig
from .gateway import FilterManager
from .gcp_billing import GcpBillingManager
from .limits_policy import (
    ContentToolsConfig,
    GenerationCapsConfig,
    ImageLimitsConfig,
    PatrolLimitConfig,
    RateLimitConfig,
)
from .models import Message
from .policy_registry import SIMPLE_LIMIT_BY_KEY, SIMPLE_LIMIT_SPECS, build_env_policy
from .provider_diagnostics import safe_attempts
from .provider_routing import ProviderRoutingManager
from .rest_policy import RestHoursConfig
from .security import Rejected
from .thinking_policy import (
    CORE_THINKING_SCENES,
    PRIMARY_MODELS,
    SETTING_FIELDS,
    THINKING_LEVELS,
    ModelPreferences,
    ThinkingConfig,
    preferences,
    supported_level,
)
from .web_search import WebSearch

SCHEMA = """
CREATE TABLE IF NOT EXISTS module_switches (
  module_id TEXT PRIMARY KEY, enabled INTEGER NOT NULL CHECK(enabled IN (0, 1))
);
CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS session_registry (id TEXT PRIMARY KEY, identity BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY, at REAL NOT NULL, code TEXT NOT NULL, source TEXT NOT NULL,
  diagnostics TEXT);
CREATE TABLE IF NOT EXISTS daily_metrics (
  day TEXT NOT NULL, code TEXT NOT NULL, source TEXT NOT NULL, n INTEGER NOT NULL,
  PRIMARY KEY(day,code,source));
CREATE TABLE IF NOT EXISTS live_watches (
  room_id TEXT PRIMARY KEY, target BLOB NOT NULL, last_status INTEGER,
  next_poll REAL NOT NULL DEFAULT 0);
"""
CODES = frozenset(
    {
        "prompt_injection",
        "content_blocked",
        "noise_ignored",
        "invalid_decision",
        "onebot_connection_ended",
        "onebot_reconnect",
        "onebot_backpressure",
        "patrol_source_unavailable",
        "bilibili_http_403",
        "bilibili_http_412",
        "bilibili_http_429",
        "bili_account_failed",
        "bili_account_alert_failed",
        "video_file_cleanup_pending",
        "diary_sync_pending",
        "diary_summary_unavailable",
        "diary_worker_error",
        "delivery_uncertain",
        "delivery_worker_error",
        "job_failed",
        "worker_error",
        "patrol_worker_error",
        "budget_exhausted",
        "gemini_unavailable",
        "gemini_circuit_open",
        "generation_incomplete",
        "stream_provider_error",
        "web_search_failed",
        "compression_failed",
        "memory_flex_retry_scheduled",
        "memory_flex_retry_exhausted",
        "memory_flex_retry_succeeded",
        "memory_archive_unconfirmed",
        "memory_worker_failed",
        "memory_mirror_failed",
        "cache_prefix_busted",
        "compression_stale",
        "notebook_refresh_failed",
        "admin_action_failed",
        "owner_dm_uncertain",
        "owner_dm_failed",
        "owner_dm_worker_error",
        "moderation_notice_failed",
        "friend_delete_unavailable",
        "longnick_update_failed",
        "meme_send_failed",
        "gcp_bigquery_sync_failed",
        "gcp_monitoring_sync_failed",
        "affection_blacklisted",
        "companion_connected",
        "companion_disconnected",
        "companion_turn_failed",
        "screen_watch_connected",
        "screen_watch_disconnected",
        "screen_watch_pulse_failed",
        "plugin_connected",
        "plugin_disconnected",
        "plugin_auth_rejected",
        "plugin_scope_rejected",
        "plugin_rpc_failed",
        "plugin_node_bridge_rejected",
        "plugin_state_error",
        "plugin_reaction_enqueued",
        "plugin_reaction_rejected",
        "identity_bound",
        "identity_merged",
        "identity_unlinked",
        "identity_muted",
    }
)


class AuditSink(logging.Handler):
    def __init__(self):
        super().__init__()
        self.pending = deque(maxlen=200)

    def emit(self, record):
        # Accept only the application's fixed event label, never exception text or arguments.
        match = re.fullmatch(r"event=([a-z_0-9]+)", record.getMessage())
        if match and match[1] in CODES:
            self.pending.append(match[1])


class RuntimeState:
    def __init__(self, store):
        self.store, self.s = store, store.s
        self.sink = AuditSink()
        self.env_compression = CompressionConfig.from_settings(self.s)
        self.compression = self.env_compression
        self.compression_overridden = False
        self.s.history_high_tokens = self.env_compression.history_high_tokens
        self.compression_lock = asyncio.Lock()
        self.env_admission = AdmissionConfig.from_settings(self.s)
        self.admission = self.env_admission
        self.admission_overridden = False
        self.admission_lock = asyncio.Lock()
        self.attention = AttentionRouter(store)
        self.filters = FilterManager(store)
        self.env_thinking = ThinkingConfig.from_settings(self.s)
        self.thinking_lock = asyncio.Lock()
        self.env_debounce = DebounceConfig.from_settings(self.s)
        self.debounce_override = None
        self.debounce_lock = asyncio.Lock()
        for spec in SIMPLE_LIMIT_SPECS:
            env_val = build_env_policy(spec, self.s)
            setattr(self, f"env_{spec.attr}", env_val)
            setattr(self, spec.attr, env_val)
            setattr(self, f"{spec.attr}_overridden", False)
            setattr(self, f"{spec.attr}_lock", asyncio.Lock())
        self.clock = time.time
        self.env_rest_hours = RestHoursConfig.from_settings(self.s)
        self.rest_hours = self.env_rest_hours
        self.rest_hours_overridden = False
        self.rest_hours_lock = asyncio.Lock()
        self.day_rest = (0.0, 0.0)
        self.web_search = WebSearch(store)
        self.provider_routing = ProviderRoutingManager(store)
        self.gcp_billing = GcpBillingManager(store)
        self.affection = AffectionManager(store)
        self._registered_sessions: set[str] = set()

    async def event(self, code, source="system", *, diagnostics=None):
        if code not in CODES or source not in {"system", "production", "sandbox", "memory"}:
            return
        now = time.time()
        day = datetime.fromtimestamp(now, ZoneInfo(self.s.bot_timezone)).date().isoformat()
        details = safe_attempts(diagnostics)
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO audit_events(at,code,source,diagnostics) VALUES (?,?,?,?)",
                (
                    now,
                    code,
                    source,
                    json.dumps(details, separators=(",", ":")) if details else None,
                ),
            )
            await self.store.db.execute(
                "INSERT INTO daily_metrics VALUES (?,?,?,1) ON "
                "CONFLICT(day,code,source) DO UPDATE SET n=n+1",
                (day, code, source),
            )
            await self.store.db.execute(
                "DELETE FROM audit_events WHERE id NOT IN (SELECT id FROM "
                "audit_events ORDER BY id DESC LIMIT 500)"
            )
            await self.store.db.execute(
                "DELETE FROM daily_metrics WHERE day<?",
                (
                    datetime.fromtimestamp(now - 32 * 86400, ZoneInfo(self.s.bot_timezone))
                    .date()
                    .isoformat(),
                ),
            )

    async def flush(self):
        while self.sink.pending:
            await self.event(self.sink.pending.popleft())

    async def save_config(self, key, value):
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO admin_config VALUES (?,?) ON CONFLICT(key) DO "
                "UPDATE SET payload=excluded.payload",
                (key, self.store.vault.seal("admin:" + key, value)),
            )

    async def load_config(self):
        self._registered_sessions.clear()
        self.provider_routing.initialized = False
        self.affection.initialized = False
        self.debounce_override = None
        self.s._thinking_override = None
        self.compression = self.env_compression
        self.compression_overridden = False
        self.s.history_high_tokens = self.env_compression.history_high_tokens
        for spec in SIMPLE_LIMIT_SPECS:
            env_val = getattr(self, f"env_{spec.attr}")
            setattr(self, spec.attr, env_val)
            setattr(self, f"{spec.attr}_overridden", False)
            self._apply_fields(env_val)
        self._apply_rest_hours(self.env_rest_hours)
        self.rest_hours_overridden = False
        admission = self.env_admission
        self.admission_overridden = False
        legacy_bots = None
        old_schedule = None
        model_values = {}
        rows = await self.store.all("SELECT * FROM admin_config")
        for row in rows:
            try:
                value = self.store.vault.open("admin:" + row["key"], row["payload"])
            except Rejected as exc:
                if (
                    row["key"] == "voice_gateway_override"
                    and str(exc) == "ciphertext_context_mismatch"
                ):
                    value = self.store.vault.open("voice-gateway-override:v1", row["payload"])
                else:
                    raise
            if row["key"] == "life_engine" and isinstance(value, dict):
                old_schedule = value
            elif row["key"] == "bot_ids":
                legacy_bots = value
            elif row["key"] == "admission":
                merged = (
                    {**self.env_admission.model_dump(mode="json"), **value}
                    if isinstance(value, dict)
                    else value
                )
                if (
                    isinstance(merged, dict)
                    and isinstance(merged.get("ignored_bot_ids"), (list, tuple))
                    and isinstance(merged.get("bot_reply_thresholds"), dict)
                ):
                    listed = set(merged["ignored_bot_ids"])
                    merged["bot_reply_thresholds"] = {
                        k: v for k, v in merged["bot_reply_thresholds"].items() if k in listed
                    }
                admission = AdmissionConfig.model_validate(merged)
                self.admission_overridden = True
            elif row["key"] == "compression":
                merged = (
                    {**self.env_compression.model_dump(), **value}
                    if isinstance(value, dict)
                    else value
                )
                self.compression = CompressionConfig.model_validate(merged)
                self.compression_overridden = True
                self.s.history_high_tokens = self.compression.history_high_tokens
            elif row["key"] in {"thinking", "model_preferences"}:
                model_values.update(value)
            elif row["key"] == "debounce":
                self.debounce_override = DebounceConfig.model_validate(value)
            elif (spec := SIMPLE_LIMIT_BY_KEY.get(row["key"])) is not None:
                loaded = self._validate_loaded(getattr(self, f"env_{spec.attr}"), value)
                setattr(self, spec.attr, loaded)
                setattr(self, f"{spec.attr}_overridden", True)
                self._apply_fields(loaded)
            elif row["key"] == "rest_hours":
                self._apply_rest_hours(self._validate_loaded(self.env_rest_hours, value))
                self.rest_hours_overridden = True
            elif row["key"].startswith("policy:"):
                self.s.group_policies[row["key"][7:]] = value
        if model_values:
            self.s._thinking_override = ModelPreferences.model_validate(
                ModelPreferences.from_settings(self.s).model_dump() | model_values
            )
        if old_schedule and (
            "morning_thinking_level" in old_schedule or "active_hours" in old_schedule
        ):
            policy = preferences(self.s)
            level = old_schedule.pop("morning_thinking_level", None)
            old_schedule.pop("active_hours", None)
            migrated = level in ("medium", "high") and "world.morning" not in policy.scene_levels
            async with self.store.transaction():
                if migrated:
                    policy = policy.model_copy(
                        update={"scene_levels": {**policy.scene_levels, "world.morning": level}}
                    )
                    await self._persist_model_preferences(policy)
                await self.store.db.execute(
                    "UPDATE admin_config SET payload=? WHERE key='life_engine'",
                    (self.store.vault.seal("admin:life_engine", old_schedule),),
                )
            if migrated:
                self.s._thinking_override = policy
        if (
            not self.image_limits_overridden
            and (self.env_image_limits.image_max_bytes, self.env_image_limits.image_max_pixels)
            == (4_194_304, 12_000_000)
            and not await self.store.one("SELECT 1 FROM meta WHERE key='image_limits_v140'")
            and await self.store.one("SELECT 1 FROM ledger LIMIT 1")
        ):
            upgraded = ImageLimitsConfig()
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO admin_config VALUES ('image_limits',?)",
                    (self.store.vault.seal("admin:image_limits", upgraded.model_dump()),),
                )
                await self.store.db.execute("INSERT INTO meta VALUES ('image_limits_v140','1')")
            self.image_limits = upgraded
            self.image_limits_overridden = True
            self._apply_fields(upgraded)
        if not self.admission_overridden and legacy_bots is not None:
            data = {**admission.model_dump(mode="json"), "ignored_bot_ids": legacy_bots}
            if isinstance(legacy_bots, (list, tuple)):
                listed = set(legacy_bots)
                data["bot_reply_thresholds"] = {
                    k: v for k, v in data.get("bot_reply_thresholds", {}).items() if k in listed
                }
            admission = AdmissionConfig.model_validate(data)
            self.admission_overridden = True
        self._validate_admission(admission)
        self._apply_admission(admission)
        if self.s.bot_qq_ids & {self.s.owner_qq_id, self.s.onebot_self_id}:
            raise Rejected("bot_identity_config_conflict")
        await self.attention.initialize()
        await self.filters.initialize()
        await self.web_search.initialize()
        await self.provider_routing.initialize()
        await self.gcp_billing.initialize()
        await self.affection.initialize()

    def debounce_status(self):
        return {
            "config": self.store.groups.config.model_dump(),
            "source": "sqlite" if self.debounce_override is not None else "env",
            "environment": self.env_debounce.model_dump(),
        }

    async def set_debounce(self, value: DebounceConfig | None):
        if value is not None and not isinstance(value, DebounceConfig):
            raise ValueError("Expected a validated DebounceConfig")
        async with self.debounce_lock:
            async with self.store.transaction():
                if value is None:
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='debounce'")
                else:
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO admin_config VALUES ('debounce',?)",
                        (self.store.vault.seal("admin:debounce", value.model_dump()),),
                    )
                await self.store.groups.retime_pending_in_transaction(value or self.env_debounce)
            self.debounce_override = value
        return self.debounce_status()

    def thinking_status(self):
        return {
            "config": (self.s._thinking_override or self.env_thinking).model_dump(
                include=set(SETTING_FIELDS)
            ),
            "source": "sqlite" if self.s._thinking_override is not None else "env",
            "environment": self.env_thinking.model_dump(),
            "levels": list(THINKING_LEVELS),
            "chat_levels": ["auto", *THINKING_LEVELS],
            "carbon_enabled": self.s.carbon_enabled,
        }

    async def _persist_model_preferences(self, value):
        extra = value.model_dump(include={"primary_model", "scene_levels"})
        await self.store.db.execute(
            "INSERT OR REPLACE INTO admin_config VALUES ('model_preferences',?)",
            (self.store.vault.seal("admin:model_preferences", extra),),
        )

    async def _write_thinking(self, value):
        # The original row remains readable by the previous engine during a rollback.
        async with self.store.transaction():
            if value is None:
                await self.store.db.execute(
                    "DELETE FROM admin_config WHERE key IN ('thinking','model_preferences')"
                )
            else:
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO admin_config VALUES ('thinking',?)",
                    (
                        self.store.vault.seal(
                            "admin:thinking", value.model_dump(include=set(SETTING_FIELDS))
                        ),
                    ),
                )
                await self._persist_model_preferences(value)
        self.s._thinking_override = value

    async def set_thinking(self, value: ThinkingConfig | None):
        # Older consoles only own the original flat scene settings.
        if value is not None and not isinstance(value, ThinkingConfig):
            raise ValueError("Expected a validated ThinkingConfig")
        async with self.thinking_lock:
            policy = preferences(self.s).model_copy(
                update=(value or self.env_thinking).model_dump()
            )
            await self._write_thinking(
                None
                if value is None and policy == ModelPreferences.from_settings(self.s)
                else policy
            )
        return self.thinking_status()

    def model_settings_status(self, registry):
        policy = preferences(self.s)
        env = ModelPreferences.from_settings(self.s)
        groups = []
        for key, label, scenes in [
            ("core", "世界引擎与公共服务", CORE_THINKING_SCENES),
            *(
                (m.module_id, m.display_name or m.module_id, m.thinking_scenes)
                for m in (*registry.systems.values(), *registry.all())
            ),
        ]:
            if scenes:
                groups.append(
                    {
                        "id": key,
                        "label": label,
                        "scenes": [
                            {
                                "key": scene.key,
                                "label": scene.label,
                                "levels": scene.levels,
                                "value": policy.level(scene),
                                "default": env.level(scene),
                            }
                            for scene in scenes
                        ],
                    }
                )
        return {
            "primary_model": policy.primary_model,
            "models": PRIMARY_MODELS,
            "model_levels": {
                m: {level: supported_level(m, level) for level in ("auto", *THINKING_LEVELS)}
                for m in PRIMARY_MODELS
            },
            "groups": groups,
            "source": "sqlite" if self.s._thinking_override else "env",
        }

    async def set_model_settings(self, registry, update):
        async with self.thinking_lock:
            policy = preferences(self.s)
            levels = dict(policy.scene_levels)
            fields = {}
            for key, level in update.levels.items() if update else ():
                scene = registry.thinking_scenes.get(key)
                choices = (
                    [
                        pref
                        for pref in scene.levels
                        if supported_level(update.primary_model, pref) == level
                    ]
                    if scene
                    else []
                )
                if not choices:
                    raise ValueError("Invalid thinking scene or level")
                (fields if key in SETTING_FIELDS else levels)[key] = (
                    level if level in choices else choices[0]
                )
            if update:
                policy = policy.model_copy(
                    update={**fields, "primary_model": update.primary_model, "scene_levels": levels}
                )
            else:
                # Preserve dormant plugin settings so reinstalling restores those preferences.
                env = ModelPreferences.from_settings(self.s)
                dormant = {k: v for k, v in levels.items() if k not in registry.thinking_scenes}
                missing = {
                    k: getattr(policy, k)
                    for k in SETTING_FIELDS
                    if k not in registry.thinking_scenes
                }
                policy = env.model_copy(update={**missing, "scene_levels": dormant})
            await self._write_thinking(
                None
                if update is None and policy == ModelPreferences.from_settings(self.s)
                else policy
            )
        return self.model_settings_status(registry)

    @staticmethod
    def _validate_loaded(env_model, saved):
        data = {**env_model.model_dump(), **saved} if isinstance(saved, dict) else saved
        return type(env_model).model_validate(data)

    def _apply_fields(self, model):
        # No await here so concurrent readers never observe partial limit updates.
        for name in type(model).model_fields:
            setattr(self.s, name, getattr(model, name))

    def _limits_status(self, current, env_model, overridden: bool):
        return {
            "config": current.model_dump(),
            "source": "admin" if overridden else "env",
            "environment": env_model.model_dump(),
        }

    async def _save_limits(self, key: str, value, expected_cls, lock, env_model, attr: str):
        if value is not None and not isinstance(value, expected_cls):
            raise ValueError(f"Expected a validated {expected_cls.__name__}")
        effective = env_model if value is None else value
        async with lock:
            async with self.store.transaction():
                if value is None:
                    await self.store.db.execute("DELETE FROM admin_config WHERE key=?", (key,))
                else:
                    await self.store.db.execute(
                        "INSERT INTO admin_config VALUES (?,?) ON CONFLICT(key) "
                        "DO UPDATE SET payload=excluded.payload",
                        (key, self.store.vault.seal("admin:" + key, value.model_dump())),
                    )
                if key == "patrol_limits":
                    deadline = time.time() + effective.patrol_interval_seconds
                    await self.store.db.execute(
                        "UPDATE subscriptions SET next_poll=? WHERE next_poll>?",
                        (deadline, deadline),
                    )
                    await self.store.db.execute(
                        "UPDATE live_watches SET next_poll=? WHERE next_poll>?",
                        (deadline, deadline),
                    )
            self._apply_fields(effective)
            setattr(self, attr, effective)
            setattr(self, attr + "_overridden", value is not None)
            return self._limits_status(effective, env_model, value is not None)

    def policy_status(self, key: str):
        spec = SIMPLE_LIMIT_BY_KEY[key]
        status = self._limits_status(
            getattr(self, spec.attr),
            getattr(self, f"env_{spec.attr}"),
            getattr(self, f"{spec.attr}_overridden"),
        )
        if key == "generation_caps":
            used = {scene.output_cap for scene in self.s._thinking_scenes.values()} | {
                "high_output_tokens"
            }
            status["fields"] = {
                k: v for k, v in spec.model.model_json_schema()["properties"].items() if k in used
            }
        return status

    async def set_policy(self, key: str, value):
        spec = SIMPLE_LIMIT_BY_KEY[key]
        return await self._save_limits(
            spec.key,
            value,
            spec.model,
            getattr(self, f"{spec.attr}_lock"),
            getattr(self, f"env_{spec.attr}"),
            spec.attr,
        )

    def rate_limits_status(self):
        return self.policy_status("rate_limits")

    async def set_rate_limits(self, value: RateLimitConfig | None):
        return await self.set_policy("rate_limits", value)

    def generation_caps_status(self):
        return self.policy_status("generation_caps")

    async def set_generation_caps(self, value: GenerationCapsConfig | None):
        return await self.set_policy("generation_caps", value)

    def patrol_limits_status(self):
        return self.policy_status("patrol_limits")

    async def set_patrol_limits(self, value: PatrolLimitConfig | None):
        return await self.set_policy("patrol_limits", value)

    def image_limits_status(self):
        return self.policy_status("image_limits")

    async def set_image_limits(self, value: ImageLimitsConfig | None):
        return await self.set_policy("image_limits", value)

    def content_tools_status(self):
        return self.policy_status("content_tools")

    async def set_content_tools(self, value: ContentToolsConfig | None):
        return await self.set_policy("content_tools", value)

    def _apply_rest_hours(self, value: RestHoursConfig):
        # Synchronous update: readers never observe partial rest-hour settings.
        self.s.rest_hours_enabled = value.enabled
        self.s.rest_hours = value.rest_hours
        self.s.rest_allow_owner_private = value.allow_owner_private
        self.s.day_rest_enabled = value.day_rest_enabled
        self.rest_hours = value

    def _local_time(self, now: float | None = None) -> datetime:
        stamp = self.clock() if now is None else now
        return datetime.fromtimestamp(stamp, ZoneInfo(self.s.bot_timezone))

    def is_resting(self, now: float | None = None) -> bool:
        """Scheduled sleep. Obligatory closing-time deliveries use this gate."""
        return self.rest_hours.active(self._local_time(now))

    def day_rest_end(self, now: float) -> float:
        start, end = self.day_rest
        return end if self.rest_hours.day_rest_enabled and start <= now < end else 0.0

    def rest_kind(self, now: float | None = None) -> str:
        now = self.clock() if now is None else now
        if self.is_resting(now):
            return "sleep"
        return "day_rest" if self.day_rest_end(now) else "awake"

    def is_resting_for(self, message: Message | None, now: float | None = None) -> bool:
        if self.rest_kind(now) == "awake":
            return False
        if (
            message is not None
            and self.rest_hours.allow_owner_private
            and message.kind == "private"
            and is_persona_owner(message, self.s)
        ):
            return False
        return True

    def rest_hours_status(self, now: float | None = None):
        now = self.clock() if now is None else now
        local = self._local_time(now)
        kind = self.rest_kind(now)
        return {
            "config": self.rest_hours.model_dump(),
            "source": "admin" if self.rest_hours_overridden else "env",
            "environment": self.env_rest_hours.model_dump(),
            "timezone": self.s.bot_timezone,
            "resting_now": kind != "awake",
            "rest_kind": kind,
            "day_rest_until": self.day_rest_end(now) or None,
            "local_time": local.strftime("%H:%M"),
        }

    async def set_rest_hours(self, value: RestHoursConfig | None):
        if value is not None and not isinstance(value, RestHoursConfig):
            raise ValueError("Expected a validated RestHoursConfig")
        async with self.rest_hours_lock:
            async with self.store.transaction():
                await self.persist_rest_hours(value)
            self.apply_rest_override(value)
            return self.rest_hours_status()

    async def persist_rest_hours(self, value):
        if value is None:
            await self.store.db.execute("DELETE FROM admin_config WHERE key='rest_hours'")
        else:
            await self.store.db.execute(
                "INSERT OR REPLACE INTO admin_config VALUES ('rest_hours',?)",
                (self.store.vault.seal("admin:rest_hours", value.model_dump()),),
            )

    def apply_rest_override(self, value):
        self._apply_rest_hours(value if value is not None else self.env_rest_hours)
        self.rest_hours_overridden = value is not None

    def _validate_admission(self, value):
        protected = {self.s.owner_qq_id, self.s.onebot_self_id} - {""}
        if protected.intersection(value.ignored_bot_ids):
            raise Rejected("reserved_identity")
        if protected.intersection(value.bot_reply_thresholds):
            raise Rejected("reserved_identity")
        if protected.intersection(value.allowed_users):
            raise Rejected("reserved_identity")

    def _apply_admission(self, value):
        # No await in this section: coroutine routing cannot observe a half-applied rule set.
        self.s.onebot_allowed_groups = set(value.allowed_groups)
        self.s.onebot_allowed_users = set(value.allowed_users)
        self.s.bot_qq_ids = set(value.ignored_bot_ids)
        self.s.bot_reply_threshold = value.bot_reply_threshold
        self.s.bot_reply_thresholds = dict(value.bot_reply_thresholds)
        self.s.onebot_private_mode = value.private_mode
        self.admission = value

    async def _save_admission(self, value):
        effective = self.env_admission if value is None else value
        self._validate_admission(effective)
        added = set(effective.allowed_groups) - self.s.onebot_allowed_groups
        async with self.store.transaction():
            await self.store.db.execute("DELETE FROM admin_config WHERE key='bot_ids'")
            if value is None:
                await self.store.db.execute("DELETE FROM admin_config WHERE key='admission'")
            else:
                await self.store.db.execute(
                    "INSERT INTO admin_config VALUES ('admission',?) ON CONFLICT(key) "
                    "DO UPDATE SET payload=excluded.payload",
                    (self.store.vault.seal("admin:admission", value.model_dump(mode="json")),),
                )
            for gid in added:
                key = "policy:onebot:" + gid
                await self.store.db.execute(
                    "INSERT INTO admin_config VALUES (?,?) ON CONFLICT(key) "
                    "DO UPDATE SET payload=excluded.payload",
                    (key, self.store.vault.seal("admin:" + key, "at_only")),
                )
        for gid in added:
            self.s.group_policies["onebot:" + gid] = "at_only"
        self._apply_admission(effective)
        self.admission_overridden = value is not None

    async def set_admission(self, value: AdmissionConfig | None):
        async with self.admission_lock:
            await self._save_admission(value)

    async def change_admission(self, field, action, identifier):
        if field not in {"allowed_groups", "allowed_users", "ignored_bot_ids"}:
            raise Rejected("invalid_admission_list")
        async with self.admission_lock:
            # Both add and remove of a protected identity are refused, to avoid a misleading UI.
            if field != "allowed_groups" and identifier in {
                self.s.owner_qq_id,
                self.s.onebot_self_id,
            }:
                raise Rejected("reserved_identity")
            values = set(getattr(self.admission, field))
            if action == "add" and identifier in values:
                raise Rejected("already_listed")
            if action == "remove" and identifier not in values:
                raise Rejected("not_listed")
            values.add(identifier) if action == "add" else values.discard(identifier)
            data = self.admission.model_dump(mode="json")
            data[field] = sorted(values)
            if field == "ignored_bot_ids" and action == "remove":
                overrides = dict(data.get("bot_reply_thresholds", {}))
                overrides.pop(identifier, None)
                data["bot_reply_thresholds"] = overrides
            await self._save_admission(AdmissionConfig.model_validate(data))

    async def set_bot_thresholds(
        self,
        *,
        default_threshold: float | None = None,
        user_id: str | None = None,
        threshold: float | None = None,
        clear_user_override: bool = False,
    ):
        """Atomically update default or per-bot reply thresholds under admission_lock."""
        async with self.admission_lock:
            protected = {self.s.owner_qq_id, self.s.onebot_self_id} - {""}
            if user_id is not None:
                if user_id in protected:
                    raise Rejected("reserved_identity")
                if user_id not in self.admission.ignored_bot_ids:
                    raise Rejected("not_listed")
            data = self.admission.model_dump(mode="json")
            if default_threshold is not None:
                data["bot_reply_threshold"] = default_threshold
            if user_id is not None:
                overrides = dict(data.get("bot_reply_thresholds", {}))
                if clear_user_override or threshold is None:
                    overrides.pop(user_id, None)
                else:
                    overrides[user_id] = threshold
                data["bot_reply_thresholds"] = overrides
            await self._save_admission(AdmissionConfig.model_validate(data))

    async def provider_result(self, provider: str, failed: bool):
        if provider not in {"bilibili_412", "gemini_decision", "gemini_chat", "gemini_memory"}:
            return
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO operational_health VALUES (?,?,?) ON CONFLICT(provider) "
                "DO UPDATE SET failures=CASE WHEN excluded.failures=0 THEN 0 "
                "ELSE operational_health.failures+1 END,changed=excluded.changed",
                (provider, int(failed), time.time()),
            )

    async def set_compression(self, value: CompressionConfig | None):
        # Serialize with compression commits, not with the slow model calls.
        async with self.compression_lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='compression'")
                self.compression = self.env_compression
                self.compression_overridden = False
                self.s.history_high_tokens = self.env_compression.history_high_tokens
            else:
                await self.save_config("compression", value.model_dump())
                # Swap one immutable object: consumers never see half-updated thresholds.
                self.compression = value
                self.compression_overridden = True
                self.s.history_high_tokens = value.history_high_tokens

    async def register(self, message):
        key = self.store.session_scope(message)
        if key in self._registered_sessions:
            return key
        # Registry holds only an encrypted delivery/session identity, never message content.
        identity = message.model_copy(
            update={
                "event_id": "admin-session",
                "text": "",
                "command_text": "",
                "images": [],
                "files": [],
                "forward_ids": [],
                "video_ids": [],
                "short_urls": [],
                "mentioned_ids": [],
                "user_id": message.conversation_id if message.kind == "private" else "0",
            }
        )
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR IGNORE INTO session_registry VALUES (?,?)",
                (key, self.store.vault.seal(key, identity.model_dump())),
            )
        if len(self._registered_sessions) >= 4096:
            self._registered_sessions.clear()
        self._registered_sessions.add(key)
        return key

    async def session(self, key):
        async with self.store.lock:
            row = await self.store.one("SELECT identity FROM session_registry WHERE id=?", (key,))
        if not row:
            raise Rejected("session_not_found")
        return Message.model_validate(self.store.vault.open(key, row[0]))

    async def sessions(self):
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id,identity FROM session_registry ORDER BY id LIMIT 1000"
            )
        return [
            (row["id"], Message.model_validate(self.store.vault.open(row["id"], row["identity"])))
            for row in rows
        ]
