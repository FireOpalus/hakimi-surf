"""Configuration loader, validation, and secret redaction for hakimi-voice-gateway."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from voice_client.config import (
    _ADAPTER_ID_RE,
    _CLIENT_ID_RE,
    VoiceClientConfig,
    _clamp,
    _non_empty_or_default,
    _parse_bool,
    _parse_float,
    _parse_int,
    load_dotenv_file,
)

OutputFormat = Literal["mp3", "wav", "pcm_16k", "pcm_48k_stereo"]

_VALID_OUTPUT_FORMATS: frozenset[str] = frozenset({"mp3", "wav", "pcm_16k", "pcm_48k_stereo"})
_WS_PATH_RE = re.compile(r"^/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*$")
_BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_SK_TOKEN_RE = re.compile(r"sk-[A-Za-z0-9_-]{8,}")


def redact_secrets(text: str, config: VoiceGatewayConfig | None = None) -> str:
    """Scrub gateway tokens, hidden WS paths, companion URLs, and provider API keys."""
    out = str(text)
    if config is not None:
        ws_url = config.ws_url.strip()
        if ws_url:
            out = out.replace(ws_url, "[REDACTED_URL]")
            parsed = urlparse(ws_url)
            if parsed.path and len(parsed.path) > 1:
                out = out.replace(parsed.path, "/[REDACTED_PATH]")

        ws_path = config.ws_path.strip()
        if ws_path and len(ws_path) > 1:
            out = out.replace(ws_path, "/[REDACTED_PATH]")

        raw_secrets = [
            config.gateway_token.strip(),
            config.ws_token.strip(),
            config.siliconflow_api_key.strip(),
            config.fish_api_key.strip(),
            config.openai_stt_api_key.strip(),
        ]
        # Replace longer secret values before shorter ones to prevent partial leakage.
        for secret in sorted({s for s in raw_secrets if s}, key=len, reverse=True):
            out = out.replace(secret, "[REDACTED]")

    out = _BEARER_RE.sub("Bearer [REDACTED]", out)
    out = _SK_TOKEN_RE.sub("sk-[REDACTED]", out)
    return out


@dataclass(frozen=True)
class VoiceGatewayConfig:
    """Immutable runtime configuration for the standalone KR universal audio gateway."""

    # Gateway HTTP / WebSocket server
    host: str = "0.0.0.0"
    port: int = 12145
    ws_path: str = "/ws/voice"
    gateway_token: str = ""
    max_audio_bytes: int = 10_485_760
    max_ws_frame_bytes: int = 16_777_216
    default_output_format: OutputFormat = "mp3"
    ffmpeg_bin: str = "ffmpeg"

    # Upstream /ws/companion connection & session defaults
    ws_url: str = ""
    ws_token: str = ""
    adapter_id: str = "voice-gateway"
    client_id: str = "default"
    session_mode: Literal["owner_private", "room_public"] = "owner_private"
    game_title: str = ""
    speaker_id: str = "owner"
    speaker_name: str = "BOISER"
    ping_interval_seconds: float = 20.0

    # STT & TTS provider configuration
    stt_provider: str = "siliconflow"
    tts_provider: str = "siliconflow"

    siliconflow_api_key: str = ""
    siliconflow_base_url: str = "https://api.siliconflow.cn/v1"
    siliconflow_stt_model: str = "FunAudioLLM/SenseVoiceSmall"
    siliconflow_tts_model: str = "FunAudioLLM/CosyVoice2-0.5B"
    siliconflow_tts_voice: str = "FunAudioLLM/CosyVoice2-0.5B:diana"
    tts_speed: float = 1.05
    tts_gain: float = 0.0
    tts_format: Literal["mp3", "wav"] = "mp3"
    tts_buffer_mode: Literal["turn", "chunk"] = "turn"
    tts_streaming: bool = True

    fish_api_key: str = ""
    fish_base_url: str = "https://api.fish.audio"
    fish_reference_id: str = ""
    fish_model: str = ""
    fish_latency: Literal["balanced", "normal"] = "balanced"
    fish_asr_language: str = "zh"
    fish_asr_model: str = ""

    openai_stt_api_key: str = ""
    openai_stt_base_url: str = "https://api.groq.com/openai/v1"
    openai_stt_model: str = "whisper-large-v3-turbo"
    edge_tts_voice: str = "zh-CN-XiaoyiNeural"

    def require_gateway_auth(self) -> None:
        """Enforce >=32-character gateway Bearer token and valid WS path."""
        if len(self.gateway_token.strip()) < 32:
            raise ValueError("VOICE_GATEWAY_TOKEN must be at least 32 characters")
        ws_path = self.ws_path.strip()
        if not _WS_PATH_RE.fullmatch(ws_path):
            raise ValueError("VOICE_GATEWAY_WS_PATH must be a valid clean path starting with '/'")

    def to_client_config(
        self,
        adapter_id: str | None = None,
        client_id: str | None = None,
        tts_format_override: str | None = None,
    ) -> VoiceClientConfig:
        """Convert gateway config into a VoiceClientConfig for provider/upstream helpers."""
        resolved_adapter = (
            adapter_id.strip() if adapter_id is not None and adapter_id.strip() else self.adapter_id
        )
        resolved_client = (
            client_id.strip() if client_id is not None and client_id.strip() else self.client_id
        )

        resolved_tts_format: Literal["mp3", "wav"] = self.tts_format
        if tts_format_override is not None:
            fmt = tts_format_override.strip().lower()
            if fmt not in _VALID_OUTPUT_FORMATS:
                raise ValueError(f"Unsupported tts_format_override: {tts_format_override!r}")
            resolved_tts_format = "mp3" if fmt == "mp3" else "wav"

        return VoiceClientConfig(
            ws_url=self.ws_url,
            ws_token=self.ws_token,
            adapter_id=resolved_adapter,
            client_id=resolved_client,
            session_mode=self.session_mode,
            game_title=self.game_title,
            speaker_id=self.speaker_id,
            speaker_name=self.speaker_name,
            stt_provider=self.stt_provider,
            tts_provider=self.tts_provider,
            siliconflow_api_key=self.siliconflow_api_key,
            siliconflow_base_url=self.siliconflow_base_url,
            siliconflow_stt_model=self.siliconflow_stt_model,
            siliconflow_tts_model=self.siliconflow_tts_model,
            siliconflow_tts_voice=self.siliconflow_tts_voice,
            tts_speed=self.tts_speed,
            tts_gain=self.tts_gain,
            tts_format=resolved_tts_format,
            tts_buffer_mode=self.tts_buffer_mode,
            tts_streaming=self.tts_streaming,
            fish_api_key=self.fish_api_key,
            fish_base_url=self.fish_base_url,
            fish_reference_id=self.fish_reference_id,
            fish_model=self.fish_model,
            fish_latency=self.fish_latency,
            fish_asr_language=self.fish_asr_language,
            fish_asr_model=self.fish_asr_model,
            openai_stt_api_key=self.openai_stt_api_key,
            openai_stt_base_url=self.openai_stt_base_url,
            openai_stt_model=self.openai_stt_model,
            edge_tts_voice=self.edge_tts_voice,
            ping_interval_seconds=self.ping_interval_seconds,
        )

    def resolved_tts_model(self) -> str:
        """Return the active TTS model or voice identifier for session metadata."""
        provider = self.tts_provider.strip().lower()
        if provider == "siliconflow":
            return self.siliconflow_tts_model
        if provider in {"fish", "fish_audio"}:
            return self.fish_model or self.fish_reference_id or "default"
        if provider in {"edge", "edge_tts"}:
            return self.edge_tts_voice
        return ""

    def redact_secrets(self, text: str) -> str:
        """Scrub configured secrets and hidden paths from arbitrary text."""
        return redact_secrets(text, self)


def load_gateway_config(
    env: Mapping[str, str] | None = None,
    *,
    env_file: Path | None = None,
) -> VoiceGatewayConfig:
    """Load VoiceGatewayConfig from environment variables and an optional dotenv file."""
    dotenv_values = load_dotenv_file(env_file) if env_file is not None else {}
    source_env = os.environ if env is None else env
    merged: dict[str, str] = {
        **dotenv_values,
        **{str(k): str(v) for k, v in source_env.items()},
    }

    host = _non_empty_or_default(merged, "VOICE_GATEWAY_HOST", "0.0.0.0")
    port = _parse_int(merged.get("VOICE_GATEWAY_PORT"), 12145, "VOICE_GATEWAY_PORT")
    if not (1 <= port <= 65535):
        raise ValueError(f"VOICE_GATEWAY_PORT 超出范围 (1..65535): {port}")

    ws_path = _non_empty_or_default(merged, "VOICE_GATEWAY_WS_PATH", "/ws/voice")
    if not _WS_PATH_RE.fullmatch(ws_path):
        raise ValueError("VOICE_GATEWAY_WS_PATH 格式无效：必须为合法的 '/' 开头路径")

    gateway_token = merged.get("VOICE_GATEWAY_TOKEN", "").strip()
    max_audio_bytes = max(
        1024,
        _parse_int(
            merged.get("VOICE_GATEWAY_MAX_AUDIO_BYTES"),
            10_485_760,
            "VOICE_GATEWAY_MAX_AUDIO_BYTES",
        ),
    )
    max_ws_frame_bytes = max(
        65536,
        _parse_int(
            merged.get("VOICE_GATEWAY_MAX_WS_FRAME_BYTES"),
            16_777_216,
            "VOICE_GATEWAY_MAX_WS_FRAME_BYTES",
        ),
    )
    raw_output_fmt = _non_empty_or_default(
        merged, "VOICE_GATEWAY_DEFAULT_OUTPUT_FORMAT", "mp3"
    ).lower()
    if raw_output_fmt not in _VALID_OUTPUT_FORMATS:
        raise ValueError(
            f"VOICE_GATEWAY_DEFAULT_OUTPUT_FORMAT 无效 ({raw_output_fmt!r})："
            "支持 mp3, wav, pcm_16k, pcm_48k_stereo。"
        )
    default_output_format: OutputFormat = raw_output_fmt  # type: ignore[assignment]

    adapter_id = _non_empty_or_default(merged, "COMPANION_ADAPTER_ID", "voice-gateway")
    if not _ADAPTER_ID_RE.fullmatch(adapter_id):
        raise ValueError(
            f"COMPANION_ADAPTER_ID 格式无效 ({adapter_id!r})：需匹配 ^[a-z][a-z0-9_-]{{1,31}}$。"
        )

    client_id = _non_empty_or_default(merged, "COMPANION_CLIENT_ID", "default")
    if not _CLIENT_ID_RE.fullmatch(client_id):
        raise ValueError(
            f"COMPANION_CLIENT_ID 格式无效 ({client_id!r})：需匹配 ^[A-Za-z0-9_.-]{{1,64}}$。"
        )

    raw_mode = _non_empty_or_default(merged, "COMPANION_MODE", "owner_private")
    if raw_mode not in {"owner_private", "room_public"}:
        raise ValueError(
            f"COMPANION_MODE 无效 ({raw_mode!r})：必须为 owner_private 或 room_public。"
        )
    session_mode: Literal["owner_private", "room_public"] = raw_mode  # type: ignore[assignment]

    raw_tts_fmt = _non_empty_or_default(merged, "TTS_FORMAT", "mp3").lower()
    if raw_tts_fmt not in {"mp3", "wav"}:
        raise ValueError(f"TTS_FORMAT 无效 ({raw_tts_fmt!r})：必须为 mp3 或 wav。")
    tts_format: Literal["mp3", "wav"] = raw_tts_fmt  # type: ignore[assignment]

    raw_tts_buf = _non_empty_or_default(merged, "TTS_BUFFER_MODE", "turn").lower()
    if raw_tts_buf not in {"turn", "chunk"}:
        raise ValueError(f"TTS_BUFFER_MODE 无效 ({raw_tts_buf!r})：必须为 turn 或 chunk。")
    tts_buffer_mode: Literal["turn", "chunk"] = raw_tts_buf  # type: ignore[assignment]

    raw_fish_latency = _non_empty_or_default(merged, "FISH_LATENCY", "balanced").lower()
    if raw_fish_latency not in {"balanced", "normal"}:
        raise ValueError(f"FISH_LATENCY 无效 ({raw_fish_latency!r})：必须为 balanced 或 normal。")
    fish_latency: Literal["balanced", "normal"] = raw_fish_latency  # type: ignore[assignment]

    tts_speed = _clamp(_parse_float(merged.get("TTS_SPEED"), 1.05, "TTS_SPEED"), 0.25, 4.0)
    tts_gain = _clamp(_parse_float(merged.get("TTS_GAIN"), 0.0, "TTS_GAIN"), -10.0, 10.0)
    tts_streaming = _parse_bool(merged.get("TTS_STREAMING"), True, "TTS_STREAMING")
    ping_interval_seconds = max(
        0.0,
        _parse_float(merged.get("PING_INTERVAL_SECONDS"), 20.0, "PING_INTERVAL_SECONDS"),
    )

    return VoiceGatewayConfig(
        host=host,
        port=port,
        ws_path=ws_path,
        gateway_token=gateway_token,
        max_audio_bytes=max_audio_bytes,
        max_ws_frame_bytes=max_ws_frame_bytes,
        default_output_format=default_output_format,
        ffmpeg_bin=_non_empty_or_default(merged, "FFMPEG_BIN", "ffmpeg"),
        ws_url=merged.get("COMPANION_WS_URL", "").strip(),
        ws_token=merged.get("COMPANION_WS_TOKEN", "").strip(),
        adapter_id=adapter_id,
        client_id=client_id,
        session_mode=session_mode,
        game_title=merged.get("COMPANION_GAME_TITLE", "").strip(),
        speaker_id=_non_empty_or_default(merged, "COMPANION_SPEAKER_ID", "owner"),
        speaker_name=_non_empty_or_default(merged, "COMPANION_SPEAKER_NAME", "BOISER"),
        ping_interval_seconds=ping_interval_seconds,
        stt_provider=_non_empty_or_default(merged, "STT_PROVIDER", "siliconflow").lower(),
        tts_provider=_non_empty_or_default(merged, "TTS_PROVIDER", "siliconflow").lower(),
        siliconflow_api_key=merged.get("SILICONFLOW_API_KEY", "").strip(),
        siliconflow_base_url=_non_empty_or_default(
            merged, "SILICONFLOW_BASE_URL", "https://api.siliconflow.cn/v1"
        ).rstrip("/"),
        siliconflow_stt_model=_non_empty_or_default(
            merged, "SILICONFLOW_STT_MODEL", "FunAudioLLM/SenseVoiceSmall"
        ),
        siliconflow_tts_model=_non_empty_or_default(
            merged, "SILICONFLOW_TTS_MODEL", "FunAudioLLM/CosyVoice2-0.5B"
        ),
        siliconflow_tts_voice=_non_empty_or_default(
            merged, "SILICONFLOW_TTS_VOICE", "FunAudioLLM/CosyVoice2-0.5B:diana"
        ),
        tts_speed=tts_speed,
        tts_gain=tts_gain,
        tts_format=tts_format,
        tts_buffer_mode=tts_buffer_mode,
        tts_streaming=tts_streaming,
        fish_api_key=merged.get("FISH_API_KEY", "").strip(),
        fish_base_url=_non_empty_or_default(
            merged, "FISH_BASE_URL", "https://api.fish.audio"
        ).rstrip("/"),
        fish_reference_id=merged.get("FISH_REFERENCE_ID", "").strip(),
        fish_model=merged.get("FISH_MODEL", "").strip(),
        fish_latency=fish_latency,
        fish_asr_language=(
            merged["FISH_ASR_LANGUAGE"].strip() if "FISH_ASR_LANGUAGE" in merged else "zh"
        ),
        fish_asr_model=merged.get("FISH_ASR_MODEL", "").strip(),
        openai_stt_api_key=merged.get("OPENAI_STT_API_KEY", "").strip(),
        openai_stt_base_url=_non_empty_or_default(
            merged, "OPENAI_STT_BASE_URL", "https://api.groq.com/openai/v1"
        ).rstrip("/"),
        openai_stt_model=_non_empty_or_default(
            merged, "OPENAI_STT_MODEL", "whisper-large-v3-turbo"
        ),
        edge_tts_voice=_non_empty_or_default(merged, "EDGE_TTS_VOICE", "zh-CN-XiaoyiNeural"),
    )
