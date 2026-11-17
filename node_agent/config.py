"""Validated configuration for the standalone Shanghai CN node agent."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOCAL_WS_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


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

    @model_validator(mode="after")
    def validate_transport_and_token(self) -> Self:
        token = self.node_ws_token.get_secret_value()
        if (
            len(token) < 32
            or token.startswith(("GENERATE_", "REPLACE_"))
            or any(ord(ch) < 33 or ord(ch) > 126 for ch in token)
        ):
            raise ValueError(
                "NODE_WS_TOKEN must be at least 32 printable ASCII non-placeholder characters"
            )

        parsed = urlsplit(self.node_ws_url)
        host = (parsed.hostname or "").lower()
        if (
            parsed.scheme not in {"ws", "wss"}
            or not host
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("NODE_WS_URL must be a ws/wss URL without query credentials")
        if parsed.scheme == "ws" and host not in _LOCAL_WS_HOSTS:
            raise ValueError("Remote NODE_WS_URL must use wss://")
        return self
