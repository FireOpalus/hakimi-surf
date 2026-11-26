"""Privacy-tiered decision routing, Vertex AI policy, and encrypted provider credentials."""

import asyncio
import json
import re
from typing import Any, Literal, Self

from google.oauth2 import service_account
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
from .security import Rejected, set_runtime_secret_bucket

SESSION_KEY_RE = re.compile(r"^(?:onebot:[1-9][0-9]{0,19}|qq:[A-Za-z0-9_-]{1,128})$")
ONEBOT_TARGET_RE = re.compile(r"^[1-9][0-9]{0,19}$")
QQ_TARGET_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SA_EMAIL_RE = re.compile(r"^[A-Za-z0-9._+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com$")
VERTEX_PROJECT_RE = re.compile(r"^$|^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
VERTEX_LOCATION_RE = re.compile(r"^[a-z0-9-]{1,64}$")
VERTEX_MODEL_RE = re.compile(r"^[A-Za-z0-9._:/-]{1,128}$")
TOKEN_URI = "https://oauth2.googleapis.com/token"
MAX_SERVICE_ACCOUNT_BYTES = 65536


def validate_service_account_json(raw: str) -> dict[str, Any]:
    """Strictly validate a GCP service account JSON document without ADC or impersonation."""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("Service account JSON must be a non-empty JSON object")
    if len(raw.encode("utf-8")) > MAX_SERVICE_ACCOUNT_BYTES:
        raise ValueError("Service account JSON exceeds 65536-byte limit")
    try:
        info = json.loads(raw)
    except ValueError as exc:
        raise ValueError("Invalid JSON in service account payload") from exc
    if not isinstance(info, dict):
        raise ValueError("Service account JSON must decode to an object")
    email = info.get("client_email")
    private_key = info.get("private_key")
    if (
        info.get("type") != "service_account"
        or info.get("token_uri") != TOKEN_URI
        or not isinstance(email, str)
        or not SA_EMAIL_RE.fullmatch(email)
        or not isinstance(private_key, str)
        or not private_key.strip()
    ):
        raise ValueError("Service account JSON missing required fields or invalid domain")
    try:
        service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    except Exception as exc:
        raise ValueError("Service account JSON failed credential validation") from exc
    return info


class PrivacyDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    default_group_privacy: Literal["high", "low"]
    default_private_privacy: Literal["high", "low"]


QQ_PRIVACY_FIELDS = (
    "default_group_privacy",
    "default_private_privacy",
    "group_privacy",
    "private_privacy",
)

ProviderView = Literal["global", "full"]


class ProviderRoutingConfig(PrivacyDefaults):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    default_group_privacy: Literal["high", "low"] = "low"
    default_private_privacy: Literal["high", "low"] = "high"
    group_privacy: dict[str, Literal["high", "low"]] = Field(default_factory=dict)
    private_privacy: dict[str, Literal["high", "low"]] = Field(default_factory=dict)
    decision_context_tokens: int = Field(default=2500, ge=600, le=4200)
    free_tier_fallback_to_paid: bool = True

    vertex_enabled: bool = False
    vertex_auth_mode: Literal["api_key", "service_account"] = "api_key"
    vertex_project: str = Field(default="", max_length=128)
    vertex_location: str = Field(default="global", max_length=64)
    vertex_hydrogen_model: str = Field(default="gemini-4-hydrogen", max_length=128)
    vertex_protocol: Literal["auto", "interactions", "generate_content"] = "auto"
    vertex_fallback_to_gemini: bool = True

    def for_admin(self, view: ProviderView) -> dict[str, Any]:
        return self.model_dump(
            mode="json", exclude=set(QQ_PRIVACY_FIELDS) if view == "global" else None
        )

    @field_validator("group_privacy", "private_privacy")
    @classmethod
    def validate_session_keys(cls, values: dict[str, Any]) -> dict[str, Any]:
        if len(values) > 1000 or any(not SESSION_KEY_RE.fullmatch(key) for key in values):
            raise ValueError("Invalid session key in provider routing map")
        return values

    @field_validator("vertex_project")
    @classmethod
    def validate_project(cls, value: str) -> str:
        cleaned = value.strip()
        if not VERTEX_PROJECT_RE.fullmatch(cleaned):
            raise ValueError("Invalid Vertex project identifier")
        return cleaned

    @field_validator("vertex_location")
    @classmethod
    def validate_location(cls, value: str) -> str:
        cleaned = value.strip() or "global"
        if not VERTEX_LOCATION_RE.fullmatch(cleaned):
            raise ValueError("Invalid Vertex location")
        return cleaned

    @field_validator("vertex_hydrogen_model")
    @classmethod
    def validate_model(cls, value: str) -> str:
        cleaned = value.strip() or "gemini-4-hydrogen"
        if not VERTEX_MODEL_RE.fullmatch(cleaned):
            raise ValueError("Invalid Vertex model identifier")
        return cleaned

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            decision_context_tokens=settings.decision_context_tokens,
            vertex_enabled=settings.vertex_enabled,
            vertex_project=settings.vertex_project,
            vertex_location=settings.vertex_location or "global",
            vertex_hydrogen_model=settings.vertex_hydrogen_model or "gemini-4-hydrogen",
        )


def _validate_api_key_input(value: str) -> str:
    if value == "":
        return ""
    cleaned = value.strip()
    if not 8 <= len(cleaned) <= 512 or re.search(r"\s", cleaned):
        raise ValueError("API key must be 8..512 non-whitespace characters")
    return cleaned


class ProviderCredentialsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    gemini_free_api_key: str = Field(default="", max_length=512)
    vertex_api_key: str = Field(default="", max_length=512)
    gcp_service_account_json: str = Field(default="", max_length=MAX_SERVICE_ACCOUNT_BYTES)
    clear_gemini_free_api_key: bool = False
    clear_vertex_api_key: bool = False
    clear_gcp_service_account_json: bool = False
    clear: bool = False

    @field_validator("gemini_free_api_key", "vertex_api_key")
    @classmethod
    def check_api_keys(cls, value: str) -> str:
        return _validate_api_key_input(value)

    @field_validator("gcp_service_account_json")
    @classmethod
    def check_service_account(cls, value: str) -> str:
        if value == "":
            return ""
        validate_service_account_json(value)
        return value


class SessionPrivacyUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["group", "private"]
    platform: Literal["onebot", "qq"] = "onebot"
    target_id: str = Field(min_length=1, max_length=128)
    privacy: Literal["default", "high", "low"]

    @model_validator(mode="after")
    def validate_target_and_changes(self) -> Self:
        pattern = ONEBOT_TARGET_RE if self.platform == "onebot" else QQ_TARGET_RE
        if not pattern.fullmatch(self.target_id):
            raise ValueError("Invalid target_id for platform")
        return self


class ProviderRoutingManager:
    """Manages session privacy routing, Vertex AI settings, and encrypted credentials."""

    def __init__(self, store):
        self.store = store
        self.s: Settings = store.s
        self.plugin_session_tiers = {}
        self.env_config = ProviderRoutingConfig.from_settings(self.s)
        self.config = self.env_config
        self.overridden = False
        self.env_gemini_free_api_key = self.s.gemini_free_api_key.get_secret_value()
        self.env_vertex_api_key = self.s.vertex_api_key.get_secret_value()
        self.credentials_override: dict[str, str] = {}
        self._sa_info: dict[str, Any] | None = None
        self.initialized = False
        self.generation = 0
        self.init_lock = asyncio.Lock()
        self.lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self.init_lock:
            if self.initialized:
                return
            async with self.store.lock:
                routing_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='provider_routing'"
                )
                cred_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='provider_credentials'"
                )
                tiers = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='chat_session_tiers'"
                )
            if tiers:
                self.plugin_session_tiers = self.store.vault.open(
                    "admin:chat_session_tiers", tiers[0]
                )
            if routing_row:
                saved = self.store.vault.open("admin:provider_routing", routing_row[0])
                legacy_channels = {"group_hydrogen_provider", "private_hydrogen_provider"}
                retire_channels = isinstance(saved, dict) and bool(
                    legacy_channels.intersection(saved)
                )
                if retire_channels:
                    saved = {
                        key: value for key, value in saved.items() if key not in legacy_channels
                    }
                merged = (
                    {**self.env_config.model_dump(mode="json"), **saved}
                    if isinstance(saved, dict)
                    else saved
                )
                self.config = ProviderRoutingConfig.model_validate(merged)
                if retire_channels:
                    await self.store.runtime.save_config(
                        "provider_routing", self.config.model_dump()
                    )
                self.overridden = True
            else:
                self.config = self.env_config
                self.overridden = False

            next_creds: dict[str, str] = {}
            next_sa: dict[str, Any] | None = None
            if cred_row:
                opened = self.store.vault.open("admin:provider_credentials", cred_row[0])
                if isinstance(opened, dict):
                    for key in (
                        "gemini_free_api_key",
                        "vertex_api_key",
                        "gcp_service_account_json",
                    ):
                        val = opened.get(key)
                        if isinstance(val, str) and val.strip():
                            next_creds[key] = (
                                val.strip() if key != "gcp_service_account_json" else val
                            )
                    raw_sa = next_creds.get("gcp_service_account_json")
                    if raw_sa:
                        try:
                            next_sa = validate_service_account_json(raw_sa)
                        except ValueError:
                            next_creds.pop("gcp_service_account_json", None)
                            next_sa = None
            self.credentials_override = next_creds
            self._sa_info = next_sa
            self._sync_runtime_secrets()
            self.generation += 1
            self.initialized = True

    def _sync_runtime_secrets(self) -> None:
        extras: list[str] = []
        for key in ("gemini_free_api_key", "vertex_api_key"):
            val = self.credentials_override.get(key, "").strip()
            if val:
                extras.append(val)
        if self._sa_info:
            pk = str(self._sa_info.get("private_key") or "").strip()
            if pk:
                extras.append(pk)
        set_runtime_secret_bucket(self.s, "provider_routing", extras)

    def gemini_free_api_key(self) -> str:
        return self.credentials_override.get("gemini_free_api_key") or self.env_gemini_free_api_key

    def gemini_free_source(self) -> Literal["sqlite", "env", "none"]:
        if self.credentials_override.get("gemini_free_api_key"):
            return "sqlite"
        if self.env_gemini_free_api_key:
            return "env"
        return "none"

    def vertex_api_key(self) -> str:
        return self.credentials_override.get("vertex_api_key") or self.env_vertex_api_key

    def vertex_api_key_source(self) -> Literal["sqlite", "env", "none"]:
        if self.credentials_override.get("vertex_api_key"):
            return "sqlite"
        if self.env_vertex_api_key:
            return "env"
        return "none"

    def service_account_info(self) -> tuple[dict[str, Any] | None, str]:
        if self._sa_info is not None:
            return dict(self._sa_info), "sqlite"
        return None, "none"

    def effective_vertex_project(self) -> str:
        if self.config.vertex_project:
            return self.config.vertex_project
        if self.config.vertex_auth_mode == "service_account" and self._sa_info is not None:
            return str(self._sa_info.get("project_id") or "").strip()
        return ""

    def has_vertex_credentials(self) -> bool:
        if self.config.vertex_auth_mode == "service_account":
            return self._sa_info is not None and bool(self.effective_vertex_project())
        return bool(self.vertex_api_key())

    def privacy_for(
        self, message: Message | None, settings: Settings | None = None
    ) -> Literal["high", "low"]:
        if message is None:
            return "high"
        active_settings = settings or self.s
        if message.platform not in {"onebot", "qq"}:
            return self.plugin_session_tiers.get(self.store.session_scope(message), "high")
        key = f"{message.platform}:{message.conversation_id}"
        if message.kind == "group":
            return self.config.group_privacy.get(key, self.config.default_group_privacy)
        if key in self.config.private_privacy:
            return self.config.private_privacy[key]
        if is_persona_owner(message, active_settings):
            return "high"
        return self.config.default_private_privacy

    def hydrogen_provider_for(self, message: Message | None = None) -> Literal["gemini", "vertex"]:
        if self.config.vertex_enabled and self.has_vertex_credentials():
            return "vertex"
        return "gemini"

    async def set_config(
        self, value: ProviderRoutingConfig | dict[str, Any] | None, *, view: ProviderView = "full"
    ) -> dict:
        await self.initialize()
        async with self.lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key='provider_routing'"
                    )
                self.config = self.env_config
                self.overridden = False
            else:
                cfg = ProviderRoutingConfig.model_validate(value)
                await self.store.runtime.save_config(
                    "provider_routing", cfg.model_dump(mode="json")
                )
                self.config = cfg
                self.overridden = True
            self.generation += 1
        return await self.status(view=view)

    async def update_credentials(
        self, value: ProviderCredentialsUpdate | dict[str, Any], *, view: ProviderView = "full"
    ) -> dict:
        await self.initialize()
        upd = ProviderCredentialsUpdate.model_validate(value)
        async with self.lock:
            if upd.clear:
                next_creds: dict[str, str] = {}
                next_sa: dict[str, Any] | None = None
            else:
                next_creds = dict(self.credentials_override)
                next_sa = dict(self._sa_info) if self._sa_info is not None else None
                if upd.clear_gemini_free_api_key:
                    next_creds.pop("gemini_free_api_key", None)
                elif upd.gemini_free_api_key:
                    next_creds["gemini_free_api_key"] = upd.gemini_free_api_key.strip()

                if upd.clear_vertex_api_key:
                    next_creds.pop("vertex_api_key", None)
                elif upd.vertex_api_key:
                    next_creds["vertex_api_key"] = upd.vertex_api_key.strip()

                if upd.clear_gcp_service_account_json:
                    next_creds.pop("gcp_service_account_json", None)
                    next_sa = None
                elif upd.gcp_service_account_json:
                    try:
                        next_sa = validate_service_account_json(upd.gcp_service_account_json)
                    except ValueError as exc:
                        raise Rejected("invalid_service_account_json") from exc
                    next_creds["gcp_service_account_json"] = json.dumps(next_sa, ensure_ascii=False)

            next_creds = {k: v for k, v in next_creds.items() if v}
            if not next_creds:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key='provider_credentials'"
                    )
            else:
                await self.store.runtime.save_config("provider_credentials", next_creds)
            self.credentials_override = next_creds
            self._sa_info = next_sa
            self._sync_runtime_secrets()
            self.generation += 1
        return await self.status(view=view)

    async def set_session_policy(self, value: SessionPrivacyUpdate | dict[str, Any]) -> None:
        await self.initialize()
        upd = SessionPrivacyUpdate.model_validate(value)
        async with self.lock:
            data = self.config.model_dump(mode="json")
            key = f"{upd.platform}:{upd.target_id}"
            privacy_field = "group_privacy" if upd.kind == "group" else "private_privacy"
            if upd.privacy == "default":
                data[privacy_field].pop(key, None)
            else:
                data[privacy_field][key] = upd.privacy
            await self._save_privacy(ProviderRoutingConfig.model_validate(data))

    async def set_privacy_defaults(self, value: PrivacyDefaults):
        await self.initialize()
        async with self.lock:
            await self._save_privacy(self.config.model_copy(update=value.model_dump()))

    async def _save_privacy(self, config):
        await self.store.runtime.save_config("provider_routing", config.model_dump())
        self.config = config
        self.overridden = True

    async def chat_privacy(self, protocols):
        await self.initialize()
        sessions = []
        for key, message in await self.store.runtime.sessions():
            if message.platform not in protocols:
                continue
            sessions.append(
                {
                    "key": key,
                    "platform": message.platform,
                    "bot_id": message.bot_id,
                    "kind": message.kind,
                    "target_id": message.conversation_id,
                    "override_privacy": self.plugin_session_tiers.get(key, "default"),
                    "effective_privacy": self.privacy_for(message),
                }
            )
        return sessions

    async def set_chat_privacy(self, key, tier, protocols):
        await self.initialize()
        message = await self.store.runtime.session(key)
        if message.platform not in protocols or message.platform in {"onebot", "qq"}:
            raise Rejected("session_not_found")
        async with self.lock:
            values = dict(self.plugin_session_tiers)
            if tier == "default":
                values.pop(key, None)
            elif tier in {"high", "low"}:
                values[key] = tier
            else:
                raise Rejected("invalid_privacy")
            await self.store.runtime.save_config("chat_session_tiers", values)
            self.plugin_session_tiers = values

    async def privacy_status(self):
        await self.initialize()
        return {
            "defaults": self.config.model_dump(include=set(PrivacyDefaults.model_fields)),
            "sessions": await self._sessions_matrix(),
        }

    async def _sessions_matrix(self) -> list[dict[str, Any]]:
        entries: dict[tuple[str, str, str], None] = {}

        def add_entry(kind: str, platform: str, target_id: str) -> None:
            if not target_id or platform not in {"onebot", "qq"}:
                return
            pattern = ONEBOT_TARGET_RE if platform == "onebot" else QQ_TARGET_RE
            if pattern.fullmatch(target_id):
                entries[(kind, platform, target_id)] = None

        if self.s.onebot_enabled:
            if self.s.owner_qq_id:
                add_entry("private", "onebot", self.s.owner_qq_id)
            for uid in sorted(self.s.onebot_owner_ids | self.s.onebot_allowed_users):
                add_entry("private", "onebot", uid)
            for gid in sorted(self.s.onebot_allowed_groups):
                add_entry("group", "onebot", gid)
        if self.s.qq_enabled:
            for uid in sorted(self.s.qq_owner_ids | self.s.qq_allowed_users):
                add_entry("private", "qq", uid)
            for gid in sorted(self.s.qq_allowed_groups):
                add_entry("group", "qq", gid)

        for _, msg in await self.store.runtime.sessions():
            add_entry(msg.kind, msg.platform, msg.conversation_id)

        for mapping, kind in (
            (self.config.group_privacy, "group"),
            (self.config.private_privacy, "private"),
        ):
            for session_key in mapping:
                platform, _, target_id = session_key.partition(":")
                add_entry(kind, platform, target_id)

        has_free_key = bool(self.gemini_free_api_key())
        rows: list[dict[str, Any]] = []
        for kind, platform, target_id in sorted(
            entries,
            key=lambda item: (
                0
                if (item[0] == "private" and item[1] == "onebot" and item[2] == self.s.owner_qq_id)
                else 1,
                0 if item[0] == "private" else 1,
                item[1],
                item[2],
            ),
        ):
            key = f"{platform}:{target_id}"
            bot_id = self.s.onebot_self_id if platform == "onebot" else self.s.qq_app_id
            synthetic = Message(
                platform=platform,  # type: ignore[arg-type]
                bot_id=bot_id,
                event_id="admin-matrix",
                user_id=target_id if kind == "private" else "0",
                kind=kind,  # type: ignore[arg-type]
                conversation_id=target_id,
                text="",
                addressed=True,
                created_at=0.0,
            )
            privacy_map = (
                self.config.group_privacy if kind == "group" else self.config.private_privacy
            )
            override_privacy = privacy_map.get(key, "default")
            effective_privacy = self.privacy_for(synthetic, self.s)
            effective_tier = "free" if (effective_privacy == "low" and has_free_key) else "paid"
            is_owner = kind == "private" and is_persona_owner(synthetic, self.s)
            rows.append(
                {
                    "kind": kind,
                    "platform": platform,
                    "target_id": target_id,
                    "key": key,
                    "is_owner": is_owner,
                    "override_privacy": override_privacy,
                    "effective_privacy": effective_privacy,
                    "effective_decision_tier": effective_tier,
                }
            )
        return rows

    async def status(self, *, view: ProviderView = "full") -> dict[str, Any]:
        await self.initialize()
        sa_info, _ = self.service_account_info()
        snapshot = {
            "config": self.config.for_admin(view),
            "environment": self.env_config.for_admin(view),
            "source": "sqlite" if self.overridden else "env",
            "credentials": {
                "gemini_paid_configured": bool(self.s.gemini_api_key.get_secret_value()),
                "gemini_free_configured": bool(self.gemini_free_api_key()),
                "gemini_free_source": self.gemini_free_source(),
                "vertex_api_key_configured": bool(self.vertex_api_key()),
                "vertex_api_key_source": self.vertex_api_key_source(),
                "gcp_service_account_configured": sa_info is not None,
                "gcp_service_account_email": (
                    str(sa_info.get("client_email") or "") if sa_info is not None else ""
                ),
                "gcp_service_account_project": (
                    str(sa_info.get("project_id") or "") if sa_info is not None else ""
                ),
            },
        }
        if view == "full":
            snapshot["sessions_matrix"] = [
                {
                    **row,
                    "override_hydrogen_provider": "default",
                    "effective_hydrogen_provider": self.hydrogen_provider_for(),
                }
                for row in await self._sessions_matrix()
            ]
        return snapshot
