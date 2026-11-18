"""Validated configuration for the standalone domestic node agent."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOCAL_WS_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_LOCAL_MC_WS_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "hakimi-mc-agent"})


def _validate_secret_token(token: str, *, env_name: str) -> None:
    if (
        len(token) < 32
        or token.startswith(("GENERATE_", "REPLACE_"))
        or any(ord(ch) < 33 or ord(ch) > 126 for ch in token)
    ):
        raise ValueError(
            f"{env_name} must be at least 32 printable ASCII non-placeholder characters"
        )


def _validate_ws_url(
    raw_url: str,
    *,
    env_name: str,
    allowed_ws_hosts: frozenset[str],
) -> None:
    if any(ord(ch) <= 32 or ord(ch) == 127 for ch in raw_url):
        raise ValueError(f"{env_name} must not contain whitespace or control characters")
    parsed = urlsplit(raw_url)
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme not in {"ws", "wss"}
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{env_name} must be a ws/wss URL without query credentials")
    if parsed.scheme == "ws" and host not in allowed_ws_hosts:
        raise ValueError(f"Remote {env_name} must use wss://")


class NodeAgentSettings(BaseSettings):
    """Settings loaded from `.env.node` or environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env.node",
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    node_ws_url: str = Field(
        validation_alias=AliasChoices("NODE_WS_URL", "HAKIMI_NODE_WS_URL", "node_ws_url"),
    )
    node_ws_token: SecretStr = Field(
        validation_alias=AliasChoices("NODE_WS_TOKEN", "HAKIMI_NODE_WS_TOKEN", "node_ws_token"),
    )
    node_id: str = Field(
        default="shanghai-cn-1",
        pattern=r"^[a-zA-Z0-9_-]{1,64}$",
        validation_alias=AliasChoices("NODE_ID", "HAKIMI_NODE_ID", "node_id"),
    )
    node_region: Literal["CN"] = Field(
        default="CN",
        validation_alias=AliasChoices("NODE_REGION", "HAKIMI_NODE_REGION", "node_region"),
    )
    ws_ping_interval: float = Field(
        default=25.0,
        ge=1.0,
        le=120.0,
        validation_alias=AliasChoices("WS_PING_INTERVAL", "ws_ping_interval"),
    )
    ws_ping_timeout: float = Field(
        default=30.0,
        ge=1.0,
        le=120.0,
        validation_alias=AliasChoices("WS_PING_TIMEOUT", "ws_ping_timeout"),
    )
    reconnect_max: float = Field(
        default=30.0,
        ge=0.1,
        le=300.0,
        validation_alias=AliasChoices("RECONNECT_MAX", "reconnect_max"),
    )
    max_frame_bytes: int = Field(
        default=1_048_576,
        ge=65_536,
        le=1_048_576,
        validation_alias=AliasChoices("MAX_FRAME_BYTES", "max_frame_bytes"),
    )
    bili_min_interval: float = Field(
        default=1.2,
        ge=0.0,
        le=30.0,
        validation_alias=AliasChoices("BILI_MIN_INTERVAL", "bili_min_interval"),
    )
    temp_dir: Path = Field(
        default=Path("/tmp/node_bili_jobs"),
        validation_alias=AliasChoices(
            "NODE_TEMP_DIR",
            "TEMP_DIR",
            "HAKIMI_NODE_TEMP_DIR",
            "temp_dir",
        ),
    )
    mc_bridge_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("MC_BRIDGE_ENABLED", "mc_bridge_enabled"),
    )
    mc_bridge_url: str = Field(
        default="ws://hakimi-mc-agent:8791/ws",
        validation_alias=AliasChoices("MC_BRIDGE_URL", "mc_bridge_url"),
    )
    mc_bridge_token: SecretStr = Field(
        default=SecretStr(""),
        validation_alias=AliasChoices("MC_BRIDGE_TOKEN", "mc_bridge_token"),
    )
    mc_rpc_timeout: float = Field(
        default=15.0,
        ge=1.0,
        le=60.0,
        validation_alias=AliasChoices("MC_RPC_TIMEOUT", "mc_rpc_timeout"),
    )

    @model_validator(mode="after")
    def validate_transport_and_token(self) -> Self:
        _validate_secret_token(self.node_ws_token.get_secret_value(), env_name="NODE_WS_TOKEN")
        _validate_ws_url(
            self.node_ws_url,
            env_name="NODE_WS_URL",
            allowed_ws_hosts=_LOCAL_WS_HOSTS,
        )
        if self.mc_bridge_enabled:
            _validate_secret_token(
                self.mc_bridge_token.get_secret_value(),
                env_name="MC_BRIDGE_TOKEN",
            )
            _validate_ws_url(
                self.mc_bridge_url,
                env_name="MC_BRIDGE_URL",
                allowed_ws_hosts=_LOCAL_MC_WS_HOSTS,
            )
        return self
