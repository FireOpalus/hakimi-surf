"""Lightweight Mac real-time voice companion client package."""

from __future__ import annotations

from .audio import (
    AfplayAudioPlayer,
    EnergyVadDetector,
    FfmpegMicCapture,
    list_avfoundation_audio_devices,
    pcm16_rms,
    pcm16_to_wav_bytes,
)
from .config import DEFAULT_ENV_FILE, VoiceClientConfig, load_config, load_dotenv_file
from .providers import (
    EdgeFallbackTTS,
    FishAudioTTS,
    OpenAICompatibleSTT,
    ProviderError,
    SiliconFlowSTT,
    SiliconFlowTTS,
    STTProvider,
    TTSProvider,
    build_stt_provider,
    build_tts_provider,
    strip_stt_markup,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_ENV_FILE",
    "AfplayAudioPlayer",
    "EdgeFallbackTTS",
    "EnergyVadDetector",
    "FfmpegMicCapture",
    "FishAudioTTS",
    "OpenAICompatibleSTT",
    "ProviderError",
    "STTProvider",
    "SiliconFlowSTT",
    "SiliconFlowTTS",
    "TTSProvider",
    "VoiceClientConfig",
    "__version__",
    "build_stt_provider",
    "build_tts_provider",
    "list_avfoundation_audio_devices",
    "load_config",
    "load_dotenv_file",
    "pcm16_rms",
    "pcm16_to_wav_bytes",
    "strip_stt_markup",
]
