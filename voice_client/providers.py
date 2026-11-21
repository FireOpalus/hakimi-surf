"""STT and TTS provider implementations for the Mac voice companion client."""

from __future__ import annotations

import asyncio
import mimetypes
import re
import tempfile
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import quote

import httpx

from .config import VoiceClientConfig

_STT_MARKUP_RE = re.compile(r"<\|[^|>]{1,32}\|>")
_BEARER_RE = re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_SK_TOKEN_RE = re.compile(r"sk-[A-Za-z0-9_-]{8,}")
_BRZ_TOKEN_RE = re.compile(r"brz_[A-Za-z0-9_-]{8,}")


def _read_existing_file(path: Path) -> bytes | None:
    if not path.is_file():
        return None
    return path.read_bytes()


class ProviderError(RuntimeError):
    """Safe provider error that never includes raw API keys or auth headers."""


def strip_stt_markup(text: str) -> str:
    """Remove SenseVoice <|...|> metadata tags and normalize whitespace."""
    cleaned = _STT_MARKUP_RE.sub("", text)
    return " ".join(cleaned.split())


def _sanitize_error_detail(detail: str, secrets: Sequence[str] = ()) -> str:
    """Scrub secret substrings and token patterns from HTTP error text."""
    out = detail
    for secret in secrets:
        if secret:
            out = out.replace(secret, "[REDACTED]")
    out = _BEARER_RE.sub("Bearer [REDACTED]", out)
    out = _SK_TOKEN_RE.sub("sk-[REDACTED]", out)
    out = _BRZ_TOKEN_RE.sub("brz_[REDACTED]", out)
    out = " ".join(out.split())
    if len(out) > 220:
        out = out[:217] + "..."
    return out


def _raise_for_bad_status(
    provider_label: str,
    resp: httpx.Response,
    *,
    secrets: Sequence[str] = (),
) -> None:
    if resp.status_code < 400:
        return
    body = _sanitize_error_detail(resp.text, secrets=secrets)
    suffix = f": {body}" if body else ""
    raise ProviderError(f"{provider_label} 请求失败 (HTTP {resp.status_code}){suffix}")


@runtime_checkable
class STTProvider(Protocol):
    """Speech-to-text provider protocol."""

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        """Transcribe WAV audio bytes into plain text."""

    async def aclose(self) -> None:
        """Release underlying resources."""


@runtime_checkable
class TTSProvider(Protocol):
    """Text-to-speech provider protocol."""

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        """Synthesize text into (audio_bytes, file_extension)."""

    async def aclose(self) -> None:
        """Release underlying resources."""


def _make_default_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(30.0, connect=10.0),
    )


async def _post_transcription(
    *,
    http: httpx.AsyncClient,
    provider_label: str,
    url: str,
    api_key: str,
    model: str,
    wav_bytes: bytes,
    filename: str,
) -> str:
    if not wav_bytes:
        return ""
    try:
        resp = await http.post(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
            files={"file": (filename, wav_bytes, "audio/wav")},
            data={"model": model},
        )
    except httpx.HTTPError as exc:
        raise ProviderError(f"{provider_label} 网络请求失败 ({type(exc).__name__})") from None
    _raise_for_bad_status(provider_label, resp, secrets=(api_key,))
    try:
        payload = resp.json()
    except ValueError:
        raise ProviderError(f"{provider_label} 返回了无效 JSON 响应") from None
    if not isinstance(payload, dict) or "text" not in payload:
        raise ProviderError(f"{provider_label} 响应缺少 text 字段")
    return str(payload.get("text") or "")


class FishAudioSTT:
    """Fish Audio /v1/asr client."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.fish.audio",
        language: str = "zh",
        model: str = "",
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.language = language.strip()
        self.model = model.strip()
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        if not wav_bytes:
            return ""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if self.model:
            headers["model"] = self.model
        data = {"language": self.language} if self.language else {}
        try:
            resp = await self._http.post(
                f"{self.base_url}/v1/asr",
                headers=headers,
                files={"audio": (filename, wav_bytes, "audio/wav")},
                data=data,
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"Fish Audio STT 网络请求失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("Fish Audio STT", resp, secrets=(self.api_key,))
        try:
            payload = resp.json()
        except ValueError:
            raise ProviderError("Fish Audio STT 返回了无效 JSON 响应") from None
        if not isinstance(payload, dict) or "text" not in payload:
            raise ProviderError("Fish Audio STT 响应缺少 text 字段")
        return strip_stt_markup(str(payload.get("text") or ""))

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class SiliconFlowSTT:
    """SiliconFlow /audio/transcriptions client (SenseVoiceSmall)."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.siliconflow.cn/v1",
        model: str = "FunAudioLLM/SenseVoiceSmall",
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        raw_text = await _post_transcription(
            http=self._http,
            provider_label="SiliconFlow STT",
            url=f"{self.base_url}/audio/transcriptions",
            api_key=self.api_key,
            model=self.model,
            wav_bytes=wav_bytes,
            filename=filename,
        )
        return strip_stt_markup(raw_text)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class OpenAICompatibleSTT:
    """OpenAI-compatible /audio/transcriptions STT client (e.g., Groq Whisper)."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.groq.com/openai/v1",
        model: str = "whisper-large-v3-turbo",
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        raw_text = await _post_transcription(
            http=self._http,
            provider_label="OpenAI-compatible STT",
            url=f"{self.base_url}/audio/transcriptions",
            api_key=self.api_key,
            model=self.model,
            wav_bytes=wav_bytes,
            filename=filename,
        )
        return raw_text.strip()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class SiliconFlowTTS:
    """SiliconFlow /audio/speech TTS client (CosyVoice2) and custom voice uploader."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.siliconflow.cn/v1",
        model: str = "FunAudioLLM/CosyVoice2-0.5B",
        voice: str = "FunAudioLLM/CosyVoice2-0.5B:diana",
        response_format: Literal["mp3", "wav"] = "mp3",
        speed: float = 1.05,
        gain: float = 0.0,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.voice = voice
        self.response_format: Literal["mp3", "wav"] = response_format
        self.speed = speed
        self.gain = gain
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        if not text.strip():
            raise ProviderError("SiliconFlow TTS 输入文本为空")
        fmt = self.response_format
        sample_rate = 44100 if fmt == "mp3" else 32000
        try:
            resp = await self._http.post(
                f"{self.base_url}/audio/speech",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "input": text,
                    "voice": self.voice,
                    "response_format": fmt,
                    "sample_rate": sample_rate,
                    "speed": self.speed,
                    "gain": self.gain,
                },
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"SiliconFlow TTS 网络请求失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("SiliconFlow TTS", resp, secrets=(self.api_key,))
        if not resp.content:
            raise ProviderError("SiliconFlow TTS 返回了空音频数据")
        return resp.content, fmt

    async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
        if not text.strip():
            raise ProviderError("SiliconFlow TTS 输入文本为空")
        fmt = self.response_format
        sample_rate = 44100 if fmt == "mp3" else 32000
        try:
            async with self._http.stream(
                "POST",
                f"{self.base_url}/audio/speech",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model,
                    "input": text,
                    "voice": self.voice,
                    "response_format": fmt,
                    "sample_rate": sample_rate,
                    "speed": self.speed,
                    "gain": self.gain,
                },
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    _raise_for_bad_status("SiliconFlow TTS", resp, secrets=(self.api_key,))
                emitted = False
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if chunk:
                        emitted = True
                        yield chunk
                if not emitted:
                    raise ProviderError("SiliconFlow TTS 返回了空音频数据")
        except httpx.HTTPError as exc:
            raise ProviderError(f"SiliconFlow TTS 流式请求失败 ({type(exc).__name__})") from None

    async def upload_custom_voice(
        self,
        audio_path: Path,
        custom_name: str,
        ref_text: str,
    ) -> str:
        """Upload a local reference audio sample and return the resulting voice URI."""
        if not custom_name.strip():
            raise ProviderError("custom_name 不能为空")
        if not ref_text.strip():
            raise ProviderError("ref_text 参考文本不能为空")
        audio_bytes = await asyncio.to_thread(_read_existing_file, audio_path)
        if audio_bytes is None:
            raise ProviderError(f"参考音频文件不存在: {audio_path}")
        guessed_mime, _ = mimetypes.guess_type(audio_path.name)
        mime = guessed_mime or "application/octet-stream"
        try:
            resp = await self._http.post(
                f"{self.base_url}/uploads/audio/voice",
                headers={"Authorization": f"Bearer {self.api_key}"},
                files={"file": (audio_path.name, audio_bytes, mime)},
                data={
                    "model": self.model,
                    "customName": custom_name,
                    "text": ref_text,
                },
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"SiliconFlow 音色上传失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("SiliconFlow 音色上传", resp, secrets=(self.api_key,))
        try:
            payload = resp.json()
        except ValueError:
            raise ProviderError("SiliconFlow 音色上传返回了无效 JSON 响应") from None
        uri = str(payload.get("uri") or "").strip() if isinstance(payload, dict) else ""
        if not uri:
            raise ProviderError("SiliconFlow 音色上传响应缺少 uri 字段")
        return uri

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class FishAudioTTS:
    """Fish Audio /v1/tts client."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://api.fish.audio",
        reference_id: str = "",
        model: str = "",
        response_format: Literal["mp3", "wav"] = "mp3",
        latency: str = "balanced",
        speed: float = 1.0,
        volume: float = 0.0,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.reference_id = reference_id.strip()
        self.model = model.strip()
        self.response_format = response_format if response_format in {"mp3", "wav"} else "mp3"
        self.latency = latency if latency in {"balanced", "normal"} else "balanced"
        self.speed = max(0.5, min(2.0, float(speed)))
        self.volume = max(-20.0, min(20.0, float(volume)))
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        if not text.strip():
            raise ProviderError("Fish Audio TTS 输入文本为空")
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if self.model:
            headers["model"] = self.model
        body: dict[str, Any] = {
            "text": text,
            "format": self.response_format,
            "latency": self.latency,
            "normalize": True,
            "prosody": {
                "speed": self.speed,
                "volume": self.volume,
            },
        }
        if self.reference_id:
            body["reference_id"] = self.reference_id
        if self.response_format == "mp3":
            body["mp3_bitrate"] = 128
        try:
            resp = await self._http.post(
                f"{self.base_url}/v1/tts",
                headers=headers,
                json=body,
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"Fish Audio TTS 网络请求失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("Fish Audio TTS", resp, secrets=(self.api_key,))
        if not resp.content:
            raise ProviderError("Fish Audio TTS 返回了空音频数据")
        return resp.content, self.response_format

    async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
        if not text.strip():
            raise ProviderError("Fish Audio TTS 输入文本为空")
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if self.model:
            headers["model"] = self.model
        body: dict[str, Any] = {
            "text": text,
            "format": self.response_format,
            "latency": self.latency,
            "normalize": True,
            "prosody": {
                "speed": self.speed,
                "volume": self.volume,
            },
        }
        if self.reference_id:
            body["reference_id"] = self.reference_id
        if self.response_format == "mp3":
            body["mp3_bitrate"] = 128
        try:
            async with self._http.stream(
                "POST",
                f"{self.base_url}/v1/tts",
                headers=headers,
                json=body,
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    _raise_for_bad_status("Fish Audio TTS", resp, secrets=(self.api_key,))
                emitted = False
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if chunk:
                        emitted = True
                        yield chunk
                if not emitted:
                    raise ProviderError("Fish Audio TTS 返回了空音频数据")
        except httpx.HTTPError as exc:
            raise ProviderError(f"Fish Audio TTS 流式请求失败 ({type(exc).__name__})") from None

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class BreezeBlueTTS:
    """BreezeBlue (Breeze TTS 2) /v1/text-to-speech/{voice_id} client."""

    def __init__(
        self,
        *,
        api_key: str,
        voice_id: str,
        base_url: str = "https://api.breeze.blue",
        model_id: str = "breeze-tts-2",
        language_code: str = "zh",
        instructions: str = "",
        response_format: Literal["mp3", "wav"] = "mp3",
        speed: float = 1.0,
        guidance_scale: float | None = None,
        use_speaker_boost: bool | None = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.voice_id = voice_id.strip()
        cleaned_url = base_url.strip().rstrip("/") or "https://api.breeze.blue"
        if cleaned_url.endswith("/v1"):
            cleaned_url = cleaned_url[:-3].rstrip("/")
        self.base_url = cleaned_url
        self.model_id = model_id.strip()
        self.language_code = language_code.strip()
        self.instructions = instructions.strip()
        self.response_format = response_format if response_format in {"mp3", "wav"} else "mp3"
        self.speed = max(0.5, min(2.0, float(speed)))
        self.guidance_scale = (
            max(1.0, min(10.0, float(guidance_scale))) if guidance_scale is not None else None
        )
        self.use_speaker_boost = use_speaker_boost
        self._owns_http = http is None
        self._http = http or _make_default_http_client()

    def _output_format_param(self) -> str:
        return "mp3" if self.response_format == "mp3" else "wav_24000"

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def _body(self, text: str) -> dict[str, Any]:
        body: dict[str, Any] = {"text": text}
        if self.model_id:
            body["model_id"] = self.model_id
        if self.language_code:
            body["language_code"] = self.language_code
        if self.instructions:
            body["instructions"] = self.instructions
        voice_settings: dict[str, Any] = {}
        if abs(self.speed - 1.0) > 1e-6:
            voice_settings["speed"] = self.speed
        if self.guidance_scale is not None:
            voice_settings["guidance_scale"] = self.guidance_scale
        if self.use_speaker_boost is not None:
            voice_settings["use_speaker_boost"] = self.use_speaker_boost
        if voice_settings:
            body["voice_settings"] = voice_settings
        return body

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        if not text.strip():
            raise ProviderError("BreezeBlue TTS 输入文本为空")
        if not self.voice_id:
            raise ProviderError("BreezeBlue TTS 未配置 voice_id")
        voice_seg = quote(self.voice_id, safe="")
        try:
            resp = await self._http.post(
                f"{self.base_url}/v1/text-to-speech/{voice_seg}",
                params={"output_format": self._output_format_param()},
                headers=self._headers(),
                json=self._body(text),
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"BreezeBlue TTS 网络请求失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("BreezeBlue TTS", resp, secrets=(self.api_key,))
        if not resp.content:
            raise ProviderError("BreezeBlue TTS 返回了空音频数据")
        return resp.content, self.response_format

    async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
        if not text.strip():
            raise ProviderError("BreezeBlue TTS 输入文本为空")
        if not self.voice_id:
            raise ProviderError("BreezeBlue TTS 未配置 voice_id")
        voice_seg = quote(self.voice_id, safe="")
        try:
            async with self._http.stream(
                "POST",
                f"{self.base_url}/v1/text-to-speech/{voice_seg}/stream",
                params={"output_format": self._output_format_param()},
                headers=self._headers(),
                json=self._body(text),
            ) as resp:
                if resp.status_code >= 400:
                    await resp.aread()
                    _raise_for_bad_status("BreezeBlue TTS", resp, secrets=(self.api_key,))
                emitted = False
                async for chunk in resp.aiter_bytes(chunk_size=4096):
                    if chunk:
                        emitted = True
                        yield chunk
                if not emitted:
                    raise ProviderError("BreezeBlue TTS 返回了空音频数据")
        except httpx.HTTPError as exc:
            raise ProviderError(f"BreezeBlue TTS 流式请求失败 ({type(exc).__name__})") from None

    async def list_voices(self) -> list[dict[str, Any]]:
        try:
            resp = await self._http.get(
                f"{self.base_url}/v1/voices",
                headers=self._headers(),
            )
        except httpx.HTTPError as exc:
            raise ProviderError(f"BreezeBlue 音色列表请求失败 ({type(exc).__name__})") from None
        _raise_for_bad_status("BreezeBlue 音色列表", resp, secrets=(self.api_key,))
        try:
            payload = resp.json()
        except ValueError:
            raise ProviderError("BreezeBlue 音色列表返回了无效 JSON 响应") from None
        if isinstance(payload, dict) and isinstance(payload.get("voices"), list):
            return [v for v in payload["voices"] if isinstance(v, dict)]
        if isinstance(payload, list):
            return [v for v in payload if isinstance(v, dict)]
        raise ProviderError("BreezeBlue 音色列表响应格式无效")

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()


class EdgeFallbackTTS:
    """Fallback TTS invoking the local edge-tts CLI into a temporary MP3 file."""

    def __init__(
        self,
        *,
        voice: str = "zh-CN-XiaoyiNeural",
        edge_tts_bin: str = "edge-tts",
    ) -> None:
        self.voice = voice
        self.edge_tts_bin = edge_tts_bin

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        if not text.strip():
            raise ProviderError("Edge TTS 输入文本为空")
        with tempfile.TemporaryDirectory(prefix="hakimi_edge_tts_") as tmpdir:
            out_path = Path(tmpdir) / "speech.mp3"
            try:
                proc = await asyncio.create_subprocess_exec(
                    self.edge_tts_bin,
                    "--voice",
                    self.voice,
                    "--text",
                    text,
                    "--write-media",
                    str(out_path),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError:
                raise ProviderError(
                    f"未找到 {self.edge_tts_bin!r} 命令，如需使用 Edge TTS 请先安装 edge-tts。"
                ) from None
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                err_text = _sanitize_error_detail(stderr.decode("utf-8", errors="replace"))
                raise ProviderError(
                    f"edge-tts 执行失败 (exit={proc.returncode}): {err_text or 'unknown error'}"
                )
            audio_bytes = await asyncio.to_thread(_read_existing_file, out_path)
            if audio_bytes is None:
                raise ProviderError("edge-tts 未生成音频文件")
            if not audio_bytes:
                raise ProviderError("edge-tts 生成了空音频文件")
            return audio_bytes, "mp3"

    async def aclose(self) -> None:
        return None


def build_stt_provider(
    config: VoiceClientConfig,
    *,
    http: httpx.AsyncClient | None = None,
) -> STTProvider:
    """Construct the configured STTProvider after validating credentials."""
    config.require_stt()
    provider = config.stt_provider.strip().lower()
    if provider == "siliconflow":
        return SiliconFlowSTT(
            api_key=config.siliconflow_api_key,
            base_url=config.siliconflow_base_url,
            model=config.siliconflow_stt_model,
            http=http,
        )
    if provider in {"fish", "fish_audio"}:
        return FishAudioSTT(
            api_key=config.fish_api_key,
            base_url=config.fish_base_url,
            language=config.fish_asr_language,
            model=config.fish_asr_model,
            http=http,
        )
    return OpenAICompatibleSTT(
        api_key=config.openai_stt_api_key,
        base_url=config.openai_stt_base_url,
        model=config.openai_stt_model,
        http=http,
    )


def build_tts_provider(
    config: VoiceClientConfig,
    *,
    http: httpx.AsyncClient | None = None,
) -> TTSProvider:
    """Construct the configured TTSProvider after validating credentials."""
    config.require_tts()
    provider = config.tts_provider.strip().lower()
    if provider == "siliconflow":
        return SiliconFlowTTS(
            api_key=config.siliconflow_api_key,
            base_url=config.siliconflow_base_url,
            model=config.siliconflow_tts_model,
            voice=config.siliconflow_tts_voice,
            response_format=config.tts_format,
            speed=config.tts_speed,
            gain=config.tts_gain,
            http=http,
        )
    if provider in {"fish", "fish_audio"}:
        return FishAudioTTS(
            api_key=config.fish_api_key,
            base_url=config.fish_base_url,
            reference_id=config.fish_reference_id,
            model=config.fish_model,
            response_format=config.tts_format,
            latency=config.fish_latency,
            speed=config.tts_speed,
            volume=config.tts_gain,
            http=http,
        )
    if provider in {"breezeblue", "breeze_blue", "breeze"}:
        return BreezeBlueTTS(
            api_key=config.breezeblue_api_key,
            voice_id=config.breezeblue_voice_id,
            base_url=config.breezeblue_base_url,
            model_id=config.breezeblue_model_id,
            language_code=config.breezeblue_language_code,
            instructions=config.breezeblue_instructions,
            response_format=config.tts_format,
            speed=config.tts_speed,
            guidance_scale=config.breezeblue_guidance_scale,
            use_speaker_boost=config.breezeblue_speaker_boost,
            http=http,
        )
    return EdgeFallbackTTS(voice=config.edge_tts_voice)
