import ipaddress
import re
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from pydantic import AliasChoices, Field, PrivateAttr, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .compression_policy import CompressionConfig
from .proactive_policy import ProactiveConfig
from .thinking_policy import ThinkingConfig, ThinkingLevel


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HAKIMI_",
        env_file=".env",
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    namespace: str = Field(default="home", pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    gemini_api_key: SecretStr
    encryption_key: SecretStr
    data_dir: Path = Path("data")
    admin_token: SecretStr = Field(
        default=SecretStr(""), validation_alias=AliasChoices("ADMIN_TOKEN", "HAKIMI_ADMIN_TOKEN")
    )
    enable_notebook_snapshot: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "ENABLE_NOTEBOOK_SNAPSHOT", "HAKIMI_ENABLE_NOTEBOOK_SNAPSHOT"
        ),
    )
    gdrive_notebook_snapshot_file_id: str = Field(
        default="",
        pattern=r"^[A-Za-z0-9_-]{0,128}$",
        validation_alias=AliasChoices(
            "GDRIVE_NOTEBOOK_SNAPSHOT_FILE_ID", "HAKIMI_GDRIVE_NOTEBOOK_SNAPSHOT_FILE_ID"
        ),
    )
    notebook_snapshot_path: Path = Field(
        default=Path("data/owner_notebook_snapshot.md"),
        validation_alias=AliasChoices("NOTEBOOK_SNAPSHOT_PATH", "HAKIMI_NOTEBOOK_SNAPSHOT_PATH"),
    )
    compression_enabled: bool = True
    group_compression_messages: int = 60
    group_compression_chars: int = 6000
    group_compression_keep: int = 25
    group_compression_min_chars: int = 1200
    group_compression_max_messages: int = 90
    private_compression_messages: int = 40
    private_compression_chars: int = 6000
    private_compression_keep: int = 16
    summary_ttl_seconds: int = Field(default=604800, ge=3600, le=2592000)
    memory_daily_usd: Decimal = Field(default=Decimal("0.20"), ge=0, le=100)
    admin_daily_usd: Decimal = Field(default=Decimal("0.20"), ge=0, le=100)
    onebot_enabled: bool = False
    onebot_self_id: str = Field(default="", pattern=r"^[0-9]{0,20}$")
    onebot_secret: SecretStr = SecretStr("")
    # Separate HTTP's internal field name from the new public WS alias. Otherwise
    # case-insensitive BaseSettings can consume ONEBOT_ACCESS_TOKEN for two fields.
    onebot_http_access_token: SecretStr = Field(
        default=SecretStr(""), validation_alias="HAKIMI_ONEBOT_ACCESS_TOKEN"
    )
    onebot_api_base: str = "http://127.0.0.1:3000"
    onebot_owner_ids: set[str] = Field(default_factory=set)
    onebot_allowed_users: set[str] = Field(default_factory=set)
    onebot_allowed_groups: set[str] = Field(default_factory=set)
    onebot_private_mode: Literal["owner_only", "owner_and_allowlist"] = "owner_only"
    muted_user_ids: set[str] = Field(default_factory=set, exclude=True)
    enable_daily_longnick: bool = Field(default=False, validation_alias="ENABLE_DAILY_LONGNICK")
    longnick_cooldown_hours: int = Field(
        default=20, ge=20, le=168, validation_alias="LONGNICK_COOLDOWN_HOURS"
    )
    bili_sessdata: SecretStr = Field(default=SecretStr(""), validation_alias="BILI_SESSDATA")
    bili_bili_jct: SecretStr = Field(default=SecretStr(""), validation_alias="BILI_BILI_JCT")
    bili_buvid3: SecretStr = Field(default=SecretStr(""), validation_alias="BILI_BUVID3")
    bili_buvid4: SecretStr = Field(default=SecretStr(""), validation_alias="BILI_BUVID4")
    bili_autonomous_enabled: bool = Field(default=False, validation_alias="BILI_AUTONOMOUS_ENABLED")
    bili_proxy: SecretStr = Field(default=SecretStr(""), validation_alias="BILI_PROXY")
    bili_patrol_times: list[str] = Field(
        default_factory=lambda: ["13:00", "20:30"], validation_alias="BILI_PATROL_TIMES"
    )
    enable_video_file_watch: bool = Field(default=False, validation_alias="ENABLE_VIDEO_FILE_WATCH")
    max_video_watch_seconds: int = Field(
        default=480, ge=30, le=480, validation_alias="MAX_VIDEO_WATCH_SECONDS"
    )
    daily_video_watch_limit: int = Field(
        default=3, ge=0, le=20, validation_alias="DAILY_VIDEO_WATCH_LIMIT"
    )
    owner_qq_id: str = Field(
        default="",
        pattern=r"^[0-9]{0,20}$",
        validation_alias=AliasChoices("OWNER_QQ_ID", "HAKIMI_OWNER_QQ_ID"),
    )
    bot_qq_ids: set[str] = Field(
        default_factory=set,
        max_length=1000,
        validation_alias=AliasChoices("BOT_QQ_IDS", "HAKIMI_BOT_QQ_IDS", "IGNORED_BOT_QQ_IDS"),
    )
    onebot_mode: Literal["http", "ws_forward", "ws_reverse"] = "http"
    onebot_webhook_enabled: bool = True
    onebot_ws_url: str = "ws://127.0.0.1:3001/"
    onebot_ws_token: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("ONEBOT_ACCESS_TOKEN", "HAKIMI_ONEBOT_WS_TOKEN"),
    )
    onebot_ws_path: str = Field(
        default="/ws/onebot",
        validation_alias=AliasChoices("ONEBOT_WS_PATH", "HAKIMI_ONEBOT_WS_PATH"),
        pattern=r"^/ws/onebot(?:/[A-Za-z0-9_-]{24,128})?$",
    )
    onebot_trusted_proxy_ips: set[str] = Field(
        default_factory=lambda: {"127.0.0.1", "::1"},
        validation_alias="ONEBOT_TRUSTED_PROXY_IPS",
    )
    ws_max_frame_bytes: int = Field(default=16777216, ge=65536, le=16777216)
    ws_ping_interval: float = Field(default=25, ge=1, le=60)
    ws_ping_timeout: float = Field(default=30, ge=1, le=60)
    ws_rpc_timeout: float = Field(default=10, ge=1, le=30)
    ws_reconnect_max: float = Field(default=30, ge=1, le=120)
    group_policies: dict[str, Literal["at_only", "smart_reply", "muted"]] = Field(
        default_factory=dict
    )
    send_delay_min: float = Field(default=0.8, ge=0, le=5)
    send_delay_max: float = Field(default=2.0, ge=0, le=10)
    send_max_chars: int = Field(default=1800, ge=200, le=2000)
    session_turns: int = Field(default=20, ge=15, le=20)
    session_default_enabled: bool = True
    session_max_files: int = Field(default=1000, ge=10, le=10000)
    bot_timezone: str = Field(default="Asia/Shanghai", validation_alias="BOT_TIMEZONE")
    proactive_dm_enabled: bool = Field(default=True, validation_alias="PROACTIVE_DM_ENABLED")
    daily_digest_enabled: bool = Field(default=True, validation_alias="DAILY_DIGEST_ENABLED")
    daily_digest_time: str = Field(default="22:30", validation_alias="DAILY_DIGEST_TIME")
    companion_ping_enabled: bool = Field(default=True, validation_alias="COMPANION_PING_ENABLED")
    proactive_chat_max_per_day: int = Field(
        default=1, ge=1, le=2, validation_alias="PROACTIVE_CHAT_MAX_PER_DAY"
    )
    quiet_hours: str = Field(default="23:30-08:30", validation_alias="QUIET_HOURS")
    proactive_daily_usd: Decimal = Field(default=Decimal("0.10"), ge=0, le=100)
    total_usd: Decimal = Field(default=Decimal("40"), ge=0, le=10000)
    proactive_alert_cooldown_seconds: int = Field(default=21600, ge=3600, le=86400)
    proactive_ws_alert_after_seconds: int = Field(default=300, ge=60, le=86400)
    images_enabled: bool = True
    image_max_bytes: int = Field(default=4_194_304, ge=1024, le=8_388_608)
    image_max_pixels: int = Field(default=12_000_000, ge=65536, le=20_000_000)
    image_token_reserve: int = Field(default=8192, ge=4096, le=32768)

    roaming_enabled: bool = False
    gdrive_auth_mode: Literal["service_account", "oauth"] = Field(
        default="service_account",
        validation_alias=AliasChoices("GDRIVE_AUTH_MODE", "HAKIMI_GDRIVE_AUTH_MODE"),
    )
    gdrive_roaming_folder_id: str = Field(
        default="",
        validation_alias=AliasChoices(
            "GDRIVE_ROAMING_FOLDER_ID", "HAKIMI_GDRIVE_ROAMING_FOLDER_ID"
        ),
    )
    gdrive_service_account_file: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "GDRIVE_SERVICE_ACCOUNT_FILE", "HAKIMI_GDRIVE_SERVICE_ACCOUNT_FILE"
        ),
    )
    gdrive_oauth_credentials_file: Path | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "GDRIVE_OAUTH_CREDENTIALS_FILE", "HAKIMI_GDRIVE_OAUTH_CREDENTIALS_FILE"
        ),
    )
    roaming_timezone: str = "Asia/Shanghai"
    roaming_interval_seconds: int = Field(default=7200, ge=3600, le=86400)
    roaming_group_ids: set[str] = Field(default_factory=set)
    roaming_max_events_per_day: int = Field(default=200, ge=10, le=1000)
    qq_enabled: bool = False
    qq_app_id: str = Field(default="", pattern=r"^[0-9]{0,32}$")
    qq_app_secret: SecretStr = SecretStr("")
    qq_owner_ids: set[str] = Field(default_factory=set)
    qq_allowed_users: set[str] = Field(default_factory=set)
    qq_allowed_groups: set[str] = Field(default_factory=set)
    qq_allow_proactive: bool = False
    # Accepted for old .env files only; group admission never uses private member lists.
    allow_group_members: bool = False
    command_prefix: Literal["/哈基米"] = "/哈基米"

    max_body_bytes: int = Field(default=65536, ge=1024, le=262144)
    max_message_chars: int = Field(default=2000, ge=100, le=8000)
    signature_max_age: int = Field(default=300, ge=30, le=600)
    queue_capacity: int = Field(default=200, ge=1, le=10000)
    group_debounce_seconds: float = Field(default=1.8, ge=0, le=3.5)
    group_debounce_max_seconds: float = Field(default=3.5, ge=0, le=3.5)
    workers: int = Field(default=2, ge=1, le=8)
    user_requests_per_minute: int = Field(default=6, ge=1, le=100)
    global_requests_per_minute: int = Field(default=30, ge=1, le=1000)
    event_ttl_seconds: int = Field(default=240, ge=30, le=240)

    decision_model: Literal["gemini-decision-1"] = "gemini-decision-1"
    chat_model: Literal["gemini-4-hydrogen"] = "gemini-4-hydrogen"
    # Retain validation of the old env key, but allow only the unified generation model.
    summary_model: Literal["gemini-4-hydrogen"] = "gemini-4-hydrogen"
    # Only the authenticated control plane may replace this immutable policy snapshot.
    _thinking_override: ThinkingConfig | None = PrivateAttr(default=None)
    fast_chat_thinking_level: ThinkingLevel = "medium"
    owner_thinking_level: ThinkingLevel = Field(
        default="medium",
        validation_alias=AliasChoices(
            "HAKIMI_OWNER_PRIVATE_THINKING_LEVEL", "HAKIMI_OWNER_THINKING_LEVEL"
        ),
    )
    summary_thinking_level: ThinkingLevel = Field(
        default="medium",
        validation_alias=AliasChoices(
            "HAKIMI_BILIBILI_SUMMARY_THINKING_LEVEL", "HAKIMI_SUMMARY_THINKING_LEVEL"
        ),
    )
    diary_thinking_level: ThinkingLevel = Field(
        default="medium",
        validation_alias=AliasChoices(
            "HAKIMI_DAILY_DIARY_THINKING_LEVEL", "HAKIMI_DIARY_THINKING_LEVEL"
        ),
    )
    memory_compression_thinking_level: ThinkingLevel = "medium"
    owner_digest_thinking_level: ThinkingLevel = "medium"
    owner_companion_thinking_level: ThinkingLevel = "medium"
    bili_selection_thinking_level: ThinkingLevel = "medium"
    bili_learning_thinking_level: ThinkingLevel = "medium"
    decision_timeout: float = Field(default=8, ge=1, le=30)
    generation_timeout: float = Field(default=45, ge=1, le=90)
    injection_threshold: float = Field(default=0.20, ge=0.01, le=0.99)
    unsafe_threshold: float = Field(default=0.50, ge=0.01, le=0.99)
    # Incoming conversation policy is separate from stricter source/output screening.
    message_injection_threshold: float = Field(default=0.78, ge=0.01, le=0.99)
    message_unsafe_threshold: float = Field(default=0.80, ge=0.01, le=0.99)
    message_reply_threshold: float = Field(default=0.42, ge=0.01, le=0.99)
    message_owner_injection_threshold: float = Field(default=0.90, ge=0.01, le=0.99)
    message_owner_unsafe_threshold: float = Field(default=0.90, ge=0.01, le=0.99)
    reply_threshold: float = Field(default=0.70, ge=0.01, le=0.99)
    owner_reply_threshold: float = Field(default=0.55, ge=0.01, le=0.99)
    min_confidence: float = Field(default=0.70, ge=0.01, le=0.99)
    chat_output_tokens: int = Field(default=1536, ge=128, le=4096)
    owner_output_tokens: int = Field(default=4096, ge=128, le=8192)
    summary_output_tokens: int = Field(default=4096, ge=128, le=8192)
    diary_output_tokens: int = Field(default=4096, ge=128, le=8192)
    high_output_tokens: int = Field(default=8192, ge=128, le=16384)
    max_context_bytes: int = Field(default=16000, ge=2048, le=64000)
    daily_usd: Decimal = Field(default=Decimal("2"), ge=0, le=1000)
    monthly_usd: Decimal = Field(default=Decimal("40"), ge=0, le=10000)
    user_daily_usd: Decimal = Field(default=Decimal("1"), ge=0, le=1000)
    patrol_daily_usd: Decimal = Field(default=Decimal("0.50"), ge=0, le=1000)
    # Standard text prices, verified 2026-11-14; explicitly editable for future pricing.
    decision_input_usd_per_m: Decimal = Field(default=Decimal("0.10"), gt=0)
    chat_input_usd_per_m: Decimal = Field(default=Decimal("1.50"), gt=0)
    chat_cached_input_usd_per_m: Decimal = Field(default=Decimal("0.075"), ge=0)
    chat_output_usd_per_m: Decimal = Field(default=Decimal("7.50"), gt=0)

    memory_ttl_seconds: int = Field(default=86400, ge=60, le=604800)
    memory_turns: int = Field(default=4, ge=1, le=12)
    bilibili_enabled: bool = True
    patrol_interval_seconds: int = Field(default=1800, ge=900, le=86400)
    max_subscriptions: int = Field(default=10, ge=1, le=50)
    patrol_batch_size: int = Field(default=3, ge=1, le=5)
    bilibili_min_interval_seconds: float = Field(default=3, ge=1, le=30)

    @property
    def onebot_access_token(self) -> SecretStr:
        return self.onebot_http_access_token

    def __init__(self, **values):
        if "onebot_access_token" in values:
            values.setdefault("onebot_http_access_token", values.pop("onebot_access_token"))
        super().__init__(**values)

    @model_validator(mode="after")
    def check_secrets_and_destinations(self) -> Self:
        CompressionConfig.from_settings(self)
        ProactiveConfig.from_settings(self)
        ZoneInfo(self.bot_timezone)
        for proxy_ip in self.onebot_trusted_proxy_ips:
            ipaddress.ip_address(proxy_ip)

        def valid_token(token: SecretStr) -> bool:
            raw = token.get_secret_value()
            return len(raw) >= 16 and not raw.startswith(("GENERATE_", "REPLACE_"))

        Fernet(self.encryption_key.get_secret_value().encode("ascii"))
        ZoneInfo(self.roaming_timezone)
        if self.owner_qq_id:
            self.onebot_owner_ids.add(self.owner_qq_id)
        if self.owner_qq_id and self.owner_qq_id == self.onebot_self_id:
            raise ValueError("OWNER_QQ_ID must differ from the bot's own QQ ID")
        if any(not re.fullmatch(r"[0-9]{1,20}", identity) for identity in self.bot_qq_ids):
            raise ValueError("BOT_QQ_IDS must contain numeric IDs as strings")
        if self.bot_qq_ids & {self.owner_qq_id, self.onebot_self_id}:
            raise ValueError("BOT_QQ_IDS must exclude OWNER_QQ_ID and the assistant itself")
        if self.owner_reply_threshold > self.reply_threshold:
            raise ValueError("owner_reply_threshold must be <= reply_threshold")
        if self.chat_cached_input_usd_per_m > self.chat_input_usd_per_m:
            raise ValueError("cached input price must not exceed standard input price")
        if self.send_delay_max < self.send_delay_min:
            raise ValueError("send_delay_max must be >= send_delay_min")
        if self.onebot_mode != "http":
            if (
                not self.onebot_enabled
                or not valid_token(self.onebot_ws_token)
                or len(self.onebot_ws_token.get_secret_value()) < 32
            ):
                raise ValueError("WebSocket requires OneBot and a >=32-character access token")
            ws = urlsplit(self.onebot_ws_url)
            if (
                ws.scheme not in {"ws", "wss"}
                or not ws.hostname
                or ws.username
                or ws.password
                or ws.query
                or ws.fragment
                or ws.path not in {"", "/"}
            ):
                raise ValueError("Use a universal OneBot WS URL without query credentials")
            if ws.scheme == "ws" and ws.hostname not in {"127.0.0.1", "localhost", "::1", "onebot"}:
                raise ValueError("Remote OneBot WS requires wss")
        if any(
            not re.fullmatch(r"(onebot|qq):[A-Za-z0-9_-]{1,128}", key)
            for key in self.group_policies | dict.fromkeys(self.roaming_group_ids)
        ):
            raise ValueError("Group keys must use platform:group_id")
        if self.roaming_enabled:
            credential_path = (
                self.gdrive_oauth_credentials_file
                if self.gdrive_auth_mode == "oauth"
                else self.gdrive_service_account_file
            )
            if (
                not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", self.gdrive_roaming_folder_id)
                or self.gdrive_roaming_folder_id == "root"
                or credential_path is None
            ):
                raise ValueError("Roaming requires one folder ID and the selected credential path")
        if len(
            self.gemini_api_key.get_secret_value()
        ) < 8 or self.gemini_api_key.get_secret_value().startswith("REPLACE_"):
            raise ValueError("HAKIMI_GEMINI_API_KEY is required")
        if self.onebot_enabled:
            if not self.onebot_self_id or not self.onebot_owner_ids:
                raise ValueError("OneBot requires self_id and owner_ids")
            if self.onebot_webhook_enabled and not valid_token(self.onebot_secret):
                raise ValueError("OneBot webhook secret must contain at least 16 characters")
            if self.onebot_mode == "http" and not valid_token(self.onebot_access_token):
                raise ValueError("OneBot HTTP access token must contain at least 16 characters")
            ids = self.onebot_owner_ids | self.onebot_allowed_users | self.onebot_allowed_groups
            if any(not re.fullmatch(r"[0-9]{1,20}", identity) for identity in ids):
                raise ValueError("OneBot allowlists must contain numeric IDs as strings")
        if self.qq_enabled and (
            not self.qq_app_id or not self.qq_owner_ids or not valid_token(self.qq_app_secret)
        ):
            raise ValueError("QQ requires app_id, app_secret and owner_ids")
        u = urlsplit(self.onebot_api_base)
        if u.scheme not in {"http", "https"} or not u.hostname:
            raise ValueError("invalid OneBot API base")
        if u.username or u.password or u.query or u.fragment or u.path not in {"", "/"}:
            raise ValueError("OneBot API base must be an origin without credentials")
        if u.scheme == "http" and u.hostname not in {"localhost", "127.0.0.1", "::1", "onebot"}:
            raise ValueError(
                "remote OneBot APIs require HTTPS; 'onebot' is a private Docker service"
            )
        return self
