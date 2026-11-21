"""Configuration loader and validation for the Mac real-time voice client."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DEFAULT_ENV_FILE = Path.home() / ".config/hakimi-voice/.env"

_ADAPTER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,31}$")
_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_EXPORT_PREFIX_RE = re.compile(r"^export\s+")
_INLINE_COMMENT_RE = re.compile(r"\s+#")

_DOUBLE_QUOTE_ESCAPES = {
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "\\": "\\",
    '"': '"',
}


def _parse_dotenv_value(raw_val: str) -> str:
    """Parse a dotenv value supporting single quotes, double quotes, and unquoted text."""
    val = raw_val.lstrip()
    if not val:
        return ""
    quote = val[0]
    if quote == '"':
        chars: list[str] = []
        idx = 1
        while idx < len(val):
            ch = val[idx]
            if ch == "\\" and idx + 1 < len(val):
                nxt = val[idx + 1]
                chars.append(_DOUBLE_QUOTE_ESCAPES.get(nxt, nxt))
                idx += 2
                continue
            if ch == '"':
                return "".join(chars)
            chars.append(ch)
            idx += 1
        return "".join(chars)
    if quote == "'":
        end_idx = val.find("'", 1)
        if end_idx != -1:
            return val[1:end_idx]
        return val[1:]
    return _INLINE_COMMENT_RE.split(val, maxsplit=1)[0].strip()


def load_dotenv_file(path: Path) -> dict[str, str]:
    """Load key=value pairs from a UTF-8 .env file, returning {} if missing."""
    if not path.is_file():
        return {}
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = _EXPORT_PREFIX_RE.sub("", line, count=1).lstrip()
        key, sep, raw_val = line.partition("=")
        if not sep:
            continue
        clean_key = key.strip()
        if not clean_key:
            continue
        result[clean_key] = _parse_dotenv_value(raw_val)
    return result


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_float(raw: str | None, default: float, name: str) -> float:
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} 必须是数字 (must be a float): {raw!r}") from exc


def _parse_int(raw: str | None, default: int, name: str) -> int:
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} 必须是整数 (must be an integer): {raw!r}") from exc


def _parse_bool(raw: str | None, default: bool, name: str) -> bool:
    if raw is None or not raw.strip():
        return default
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} 必须是布尔值 (true/false): {raw!r}")


def _non_empty_or_default(merged: Mapping[str, str], key: str, default: str) -> str:
    raw = merged.get(key)
    if raw is None:
        return default
    stripped = raw.strip()
    return stripped if stripped else default


@dataclass(frozen=True)
class VoiceClientConfig:
    """Immutable runtime configuration for the Mac voice companion client."""

    # WebSocket connection & companion session metadata
    ws_url: str = ""
    ws_token: str = ""
    adapter_id: str = "mac-voice"
    client_id: str = "boiser-m4"
    session_mode: Literal["owner_private", "room_public"] = "owner_private"
    game_title: str = ""
    speaker_id: str = "owner"
    speaker_name: str = "BOISER"

    # Provider selection
    stt_provider: str = "siliconflow"
    tts_provider: str = "siliconflow"

    # SiliconFlow (Plan A)
    siliconflow_api_key: str = ""
    siliconflow_base_url: str = "https://api.siliconflow.cn/v1"
    siliconflow_stt_model: str = "FunAudioLLM/SenseVoiceSmall"
    siliconflow_tts_model: str = "FunAudioLLM/CosyVoice2-0.5B"
    siliconflow_tts_voice: str = "FunAudioLLM/CosyVoice2-0.5B:diana"
    tts_speed: float = 1.05
    tts_gain: float = 0.0
    tts_format: Literal["mp3", "wav"] = "mp3"
    tts_buffer_mode: Literal["turn", "chunk"] = "turn"

    # Optional alternate providers
    fish_api_key: str = ""
    fish_base_url: str = "https://api.fish.audio"
    fish_reference_id: str = ""
    fish_model: str = ""
    fish_latency: Literal["balanced", "normal"] = "balanced"
    openai_stt_api_key: str = ""
    openai_stt_base_url: str = "https://api.groq.com/openai/v1"
    openai_stt_model: str = "whisper-large-v3-turbo"
    edge_tts_voice: str = "zh-CN-XiaoyiNeural"

    # Audio capture, VAD, PTT, and keepalive
    voice_mode: Literal["vad", "ptt"] = "vad"
    audio_input_device: str = "0"
    sample_rate: int = 16000
    vad_threshold: float = 0.018
    vad_silence_ms: int = 750
    vad_min_speech_ms: int = 320
    vad_max_speech_ms: int = 20000
    vad_barge_in_multiplier: float = 2.2
    barge_in_enabled: bool = True
    ping_interval_seconds: float = 20.0

    def require_ws(self) -> None:
        """Validate WebSocket URL and token without exposing secret values."""
        url = self.ws_url.strip()
        if not url:
            raise ValueError(
                "缺少 COMPANION_WS_URL：请在 ~/.config/hakimi-voice/.env "
                "或环境变量中设置 WebSocket 地址。"
            )
        if not (url.startswith("ws://") or url.startswith("wss://")):
            raise ValueError(
                "COMPANION_WS_URL 格式无效：必须以 ws:// 或 wss:// 开头 (must start with ws:// or wss://)。"
            )
        if not self.ws_token.strip():
            raise ValueError(
                "缺少 COMPANION_WS_TOKEN：请在 ~/.config/hakimi-voice/.env "
                "或环境变量中设置鉴权 Token。"
            )

    def require_stt(self) -> None:
        """Validate STT provider selection and required API key."""
        provider = self.stt_provider.strip().lower()
        if provider == "siliconflow":
            if not self.siliconflow_api_key.strip():
                raise ValueError(
                    "缺少 SILICONFLOW_API_KEY：当前 STT_PROVIDER=siliconflow，"
                    "请配置硅基流动 API Key。"
                )
            return
        if provider in {"openai", "openai_compatible", "groq"}:
            if not self.openai_stt_api_key.strip():
                raise ValueError(
                    "缺少 OPENAI_STT_API_KEY：当前 STT_PROVIDER 使用 OpenAI 兼容接口，"
                    "请配置 API Key。"
                )
            return
        raise ValueError(
            f"不支持的 STT_PROVIDER={self.stt_provider!r}（支持: siliconflow, openai）。"
        )

    def require_tts(self) -> None:
        """Validate TTS provider selection and required credentials."""
        provider = self.tts_provider.strip().lower()
        if provider == "siliconflow":
            if not self.siliconflow_api_key.strip():
                raise ValueError(
                    "缺少 SILICONFLOW_API_KEY：当前 TTS_PROVIDER=siliconflow，"
                    "请配置硅基流动 API Key。"
                )
            return
        if provider in {"fish", "fish_audio"}:
            if not self.fish_api_key.strip():
                raise ValueError(
                    "缺少 FISH_API_KEY：当前 TTS_PROVIDER=fish，请配置 Fish Audio API Key。"
                )
            return
        if provider in {"edge", "edge_tts"}:
            if not self.edge_tts_voice.strip():
                raise ValueError(
                    "缺少 EDGE_TTS_VOICE：当前 TTS_PROVIDER=edge，请配置 Edge TTS 音色名称。"
                )
            return
        raise ValueError(
            f"不支持的 TTS_PROVIDER={self.tts_provider!r}（支持: siliconflow, fish, edge）。"
        )


def load_config(
    env: Mapping[str, str] | None = None,
    *,
    env_file: Path | None = None,
) -> VoiceClientConfig:
    """Load VoiceClientConfig by merging dotenv file and environment mapping."""
    dotenv_values = load_dotenv_file(env_file or DEFAULT_ENV_FILE)
    source_env = os.environ if env is None else env
    merged: dict[str, str] = {
        **dotenv_values,
        **{str(k): str(v) for k, v in source_env.items()},
    }

    adapter_id = (
        merged["COMPANION_ADAPTER_ID"].strip() if "COMPANION_ADAPTER_ID" in merged else "mac-voice"
    )
    if not _ADAPTER_ID_RE.fullmatch(adapter_id):
        raise ValueError(
            f"COMPANION_ADAPTER_ID 格式无效 ({adapter_id!r})：需匹配 ^[a-z][a-z0-9_-]{{1,31}}$。"
        )

    client_id = (
        merged["COMPANION_CLIENT_ID"].strip() if "COMPANION_CLIENT_ID" in merged else "boiser-m4"
    )
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

    raw_voice_mode = _non_empty_or_default(merged, "VOICE_MODE", "vad").lower()
    if raw_voice_mode not in {"vad", "ptt"}:
        raise ValueError(f"VOICE_MODE 无效 ({raw_voice_mode!r})：必须为 vad 或 ptt。")
    voice_mode: Literal["vad", "ptt"] = raw_voice_mode  # type: ignore[assignment]

    tts_speed = _clamp(_parse_float(merged.get("TTS_SPEED"), 1.05, "TTS_SPEED"), 0.25, 4.0)
    tts_gain = _clamp(_parse_float(merged.get("TTS_GAIN"), 0.0, "TTS_GAIN"), -10.0, 10.0)

    vad_threshold = _clamp(
        _parse_float(merged.get("VAD_THRESHOLD"), 0.018, "VAD_THRESHOLD"), 0.0005, 1.0
    )
    vad_silence_ms = max(60, _parse_int(merged.get("VAD_SILENCE_MS"), 750, "VAD_SILENCE_MS"))
    vad_min_speech_ms = max(
        30, _parse_int(merged.get("VAD_MIN_SPEECH_MS"), 320, "VAD_MIN_SPEECH_MS")
    )
    vad_max_speech_ms = max(
        500, _parse_int(merged.get("VAD_MAX_SPEECH_MS"), 20000, "VAD_MAX_SPEECH_MS")
    )
    vad_barge_in_multiplier = max(
        1.0,
        _parse_float(merged.get("VAD_BARGE_IN_MULTIPLIER"), 2.2, "VAD_BARGE_IN_MULTIPLIER"),
    )
    barge_in_enabled = _parse_bool(merged.get("BARGE_IN_ENABLED"), True, "BARGE_IN_ENABLED")

    return VoiceClientConfig(
        ws_url=merged.get("COMPANION_WS_URL", "").strip(),
        ws_token=merged.get("COMPANION_WS_TOKEN", "").strip(),
        adapter_id=adapter_id,
        client_id=client_id,
        session_mode=session_mode,
        game_title=merged.get("COMPANION_GAME_TITLE", "").strip(),
        speaker_id=_non_empty_or_default(merged, "COMPANION_SPEAKER_ID", "owner"),
        speaker_name=_non_empty_or_default(merged, "COMPANION_SPEAKER_NAME", "BOISER"),
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
        fish_api_key=merged.get("FISH_API_KEY", "").strip(),
        fish_base_url=_non_empty_or_default(
            merged, "FISH_BASE_URL", "https://api.fish.audio"
        ).rstrip("/"),
        fish_reference_id=merged.get("FISH_REFERENCE_ID", "").strip(),
        fish_model=merged.get("FISH_MODEL", "").strip(),
        fish_latency=fish_latency,
        openai_stt_api_key=merged.get("OPENAI_STT_API_KEY", "").strip(),
        openai_stt_base_url=_non_empty_or_default(
            merged, "OPENAI_STT_BASE_URL", "https://api.groq.com/openai/v1"
        ).rstrip("/"),
        openai_stt_model=_non_empty_or_default(
            merged, "OPENAI_STT_MODEL", "whisper-large-v3-turbo"
        ),
        edge_tts_voice=_non_empty_or_default(merged, "EDGE_TTS_VOICE", "zh-CN-XiaoyiNeural"),
        voice_mode=voice_mode,
        audio_input_device=_non_empty_or_default(merged, "AUDIO_INPUT_DEVICE", "0"),
        vad_threshold=vad_threshold,
        vad_silence_ms=vad_silence_ms,
        vad_min_speech_ms=vad_min_speech_ms,
        vad_max_speech_ms=vad_max_speech_ms,
        vad_barge_in_multiplier=vad_barge_in_multiplier,
        barge_in_enabled=barge_in_enabled,
    )
