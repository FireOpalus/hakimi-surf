"""Bounded QQ voice fetching, SSRF-pinned HTTPS download, and ffmpeg audio normalization.

Never reads local filesystem paths returned by NapCat/OneBot. Accepts only base64 payloads or
HTTPS URLs pinned to validated global IP addresses at connect time, then probes and transcodes
audio to 16 kHz mono 32 kbps MP3 (with stripped metadata) via safe argv-list subprocess calls.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import ipaddress
import json
import logging
import math
import re
import shutil
import subprocess
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import aiohttp

from .config import Settings
from .media import BROWSER_AGENT, PublicResolver
from .models import AudioRef
from .security import Rejected

MAX_AUDIO_BYTES = 20 * 1024 * 1024
MAX_INLINE_AUDIO_BYTES = 20 * 1024 * 1024
HTTP_AUDIO_TIMEOUT = 15
RPC_AUDIO_TIMEOUT = 15
FFPROBE_TIMEOUT = 10
FFMPEG_TIMEOUT = 30
AUDIO_TOKENS_PER_SECOND = 35
TARGET_SAMPLE_RATE = 16000
TARGET_CHANNELS = 1
TARGET_BITRATE = "32k"

VALID_RECORD_FILE_RE = re.compile(r"^[A-Za-z0-9_{}=+\-][A-Za-z0-9_.{}=+\-]{0,1023}$")
ALLOWED_INLINE_AUDIO_MIMES = frozenset({"audio/mp3", "audio/mpeg", "audio/wav", "audio/ogg"})
_ALLOWED_INPUT_CODECS = frozenset(
    {
        "mp3",
        "mp2",
        "mp1",
        "opus",
        "vorbis",
        "speex",
        "amr_nb",
        "amr_wb",
        "aac",
        "alac",
        "flac",
        "wmav1",
        "wmav2",
        "wmapro",
        "silk",
    }
)
_BLOCKED_FORMATS = frozenset(
    {
        "hls",
        "applehttp",
        "concat",
        "dash",
        "sdp",
        "m3u8",
        "image2",
        "image2pipe",
        "pipe",
        "tty",
        "bin",
        "rtp",
        "rtsp",
    }
)
_REJECTED_HTTP_CONTENT_TYPES = frozenset(
    {
        "text/html",
        "text/plain",
        "text/xml",
        "application/json",
        "application/xml",
        "application/xhtml+xml",
        "application/javascript",
    }
)
_FALLBACK_OUT_FORMATS = ("mp3", "wav", "ogg", "amr")

log = logging.getLogger("hakimi")


def valid_record_file_id(raw: Any) -> str:
    """Validate an opaque OneBot record identifier; reject paths, URLs, and traversals."""
    if not isinstance(raw, str):
        return ""
    value = raw.strip()
    if (
        not value
        or value in {".", ".."}
        or ".." in value
        or not VALID_RECORD_FILE_RE.fullmatch(value)
    ):
        return ""
    return value


def validate_audio_url(url: str) -> str:
    """Accept only HTTPS public URLs with no credentials, fragments, or non-443 ports."""
    if not isinstance(url, str) or len(url) > 4096:
        raise Rejected("audio_origin_forbidden")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise Rejected("audio_origin_forbidden") from None
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme != "https"
        or port not in {None, 443}
        or parsed.username
        or parsed.password
        or parsed.fragment
        or not host
        or host == "localhost"
        or host.endswith((".local", ".localhost", ".internal", ".invalid", ".home.arpa"))
    ):
        raise Rejected("audio_origin_forbidden")
    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global or ip.is_multicast or (ip.version == 6 and ip.ipv4_mapped):
            raise Rejected("audio_origin_forbidden")
    except ValueError:
        if "." not in host:
            raise Rejected("audio_origin_forbidden") from None
    return url


def audio_tokens(duration_sec: float) -> int:
    """Estimate Gemini audio input tokens at 35 tok/s rounded up (~32 tok/s + safety margin)."""
    if (
        isinstance(duration_sec, bool)
        or not isinstance(duration_sec, (int, float))
        or not math.isfinite(duration_sec)
        or duration_sec <= 0
    ):
        raise Rejected("audio_duration_invalid")
    return math.ceil(duration_sec * AUDIO_TOKENS_PER_SECOND)


def format_audio_memory_tag(duration_sec: float | None, gist: str | None = None) -> str:
    """Format audio history retention marker: '[语音 12s: <gist>]' or '[语音 12s]'."""
    secs = (
        max(1, round(duration_sec))
        if isinstance(duration_sec, (int, float))
        and not isinstance(duration_sec, bool)
        and math.isfinite(duration_sec)
        and duration_sec > 0
        else None
    )
    head = f"语音 {secs}s" if secs is not None else "语音"
    cleaned = (gist or "").strip()
    return f"[{head}: {cleaned}]" if cleaned else f"[{head}]"


@dataclass(frozen=True)
class ModelAudio:
    data: str
    mime_type: str = "audio/mp3"
    duration_sec: float = 0.0
    byte_size: int = 0
    digest: str = ""

    def validate(self) -> int:
        audio_tokens(self.duration_sec)
        if self.mime_type not in ALLOWED_INLINE_AUDIO_MIMES:
            raise Rejected("invalid_audio")
        if not isinstance(self.data, str) or not self.data:
            raise Rejected("invalid_audio")
        if len(self.data) > 4 * ((MAX_AUDIO_BYTES + 2) // 3):
            raise Rejected("audio_too_large")
        try:
            raw = base64.b64decode(self.data, validate=True)
        except (ValueError, binascii.Error):
            raise Rejected("invalid_audio") from None
        if not raw:
            raise Rejected("invalid_audio")
        if len(raw) > MAX_AUDIO_BYTES:
            raise Rejected("audio_too_large")
        return len(raw)

    @property
    def tokens(self) -> int:
        return audio_tokens(self.duration_sec)

    def gate(self) -> ModelAudio:
        return self

    def block(self) -> dict[str, Any]:
        return {
            "type": "audio",
            "data": self.data,
            "mime_type": self.mime_type,
        }

    def raw_bytes(self) -> bytes:
        return base64.b64decode(self.data, validate=True)

    def memory_bytes(self) -> int:
        return len(self.data)


def _probe_audio(probe_bin: str, path: Path) -> dict[str, Any]:
    cmd = [
        probe_bin,
        "-v",
        "error",
        "-protocol_whitelist",
        "file",
        "-show_entries",
        (
            "format=duration,format_name:"
            "stream=codec_type,codec_name,sample_rate,channels,duration:"
            "stream_disposition=attached_pic"
        ),
        "-of",
        "json",
        str(path),
    ]
    try:
        proc = subprocess.run(
            cmd,
            shell=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=FFPROBE_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise Rejected("audio_transcode_timeout") from None
    except OSError:
        raise Rejected("audio_transcoder_unavailable") from None
    if proc.returncode != 0:
        raise Rejected("invalid_audio")
    try:
        parsed = json.loads(proc.stdout.decode("utf-8", errors="strict"))
    except (ValueError, UnicodeDecodeError):
        raise Rejected("invalid_audio") from None
    if not isinstance(parsed, dict):
        raise Rejected("invalid_audio")
    return parsed


def _parse_duration(info: dict[str, Any]) -> float | None:
    candidates: list[Any] = []
    fmt = info.get("format")
    if isinstance(fmt, dict):
        candidates.append(fmt.get("duration"))
    streams = info.get("streams")
    if isinstance(streams, list):
        for stream in streams:
            if isinstance(stream, dict) and stream.get("codec_type") == "audio":
                candidates.append(stream.get("duration"))
    for raw in candidates:
        if raw is None or isinstance(raw, bool):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(val) and val > 0:
            return val
    return None


def _validate_probe_input(info: dict[str, Any]) -> None:
    fmt = info.get("format")
    if not isinstance(fmt, dict):
        raise Rejected("invalid_audio")
    format_names = {
        part.strip().lower()
        for part in str(fmt.get("format_name") or "").split(",")
        if part.strip()
    }
    if not format_names or (format_names & _BLOCKED_FORMATS):
        raise Rejected("invalid_audio")
    streams = info.get("streams")
    if not isinstance(streams, list) or not streams:
        raise Rejected("invalid_audio")
    has_audio = False
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        ctype = str(stream.get("codec_type") or "").lower()
        if ctype == "video":
            disp = stream.get("disposition")
            if not (isinstance(disp, dict) and disp.get("attached_pic")):
                raise Rejected("invalid_audio")
        elif ctype == "audio":
            codec = str(stream.get("codec_name") or "").lower()
            if codec in _ALLOWED_INPUT_CODECS or codec.startswith(("pcm_", "adpcm_")):
                has_audio = True
    if not has_audio:
        raise Rejected("invalid_audio")


def _run_ffmpeg_transcode(
    enc_bin: str,
    in_path: Path,
    out_path: Path,
    *,
    codec: str,
    container: str,
    bitrate: str | None = TARGET_BITRATE,
) -> subprocess.CompletedProcess[bytes]:
    cmd = [
        enc_bin,
        "-v",
        "error",
        "-nostdin",
        "-y",
        "-protocol_whitelist",
        "file",
        "-i",
        str(in_path),
        "-vn",
        "-sn",
        "-dn",
        "-map",
        "0:a:0",
        "-map_metadata",
        "-1",
        "-fflags",
        "+bitexact",
        "-ac",
        str(TARGET_CHANNELS),
        "-ar",
        str(TARGET_SAMPLE_RATE),
    ]
    if bitrate:
        cmd.extend(["-b:a", bitrate])
    cmd.extend(["-c:a", codec, "-f", container, str(out_path)])
    try:
        return subprocess.run(
            cmd,
            shell=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=FFMPEG_TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise Rejected("audio_transcode_timeout") from None
    except OSError:
        raise Rejected("audio_transcoder_unavailable") from None


def normalize_audio(
    raw: bytes,
    *,
    ffmpeg_bin: str | None = None,
    ffprobe_bin: str | None = None,
    max_bytes: int = MAX_AUDIO_BYTES,
) -> ModelAudio:
    """Validate and transcode untrusted audio bytes into 16 kHz mono 32 kbps MP3."""
    if not isinstance(raw, (bytes, bytearray)):
        raise Rejected("invalid_audio")
    data = bytes(raw)
    if not data:
        raise Rejected("invalid_audio")
    limit = min(int(max_bytes), MAX_AUDIO_BYTES)
    if limit <= 0 or len(data) > limit:
        raise Rejected("audio_too_large")

    enc_bin = ffmpeg_bin or shutil.which("ffmpeg")
    probe_bin = ffprobe_bin or shutil.which("ffprobe")
    if not enc_bin or not probe_bin:
        raise Rejected("audio_transcoder_unavailable")

    with tempfile.TemporaryDirectory(prefix="hakimi-audio-") as tmpdir:
        base = Path(tmpdir)
        in_path = base / "input.bin"
        in_path.write_bytes(data)

        in_info = _probe_audio(probe_bin, in_path)
        _validate_probe_input(in_info)

        out_path = base / "output.mp3"
        out_mime = "audio/mp3"
        proc = _run_ffmpeg_transcode(
            enc_bin,
            in_path,
            out_path,
            codec="libmp3lame",
            container="mp3",
            bitrate=TARGET_BITRATE,
        )
        if proc.returncode != 0:
            stderr_lower = proc.stderr.decode("utf-8", errors="ignore").lower()
            if "unknown encoder" in stderr_lower and "libmp3lame" in stderr_lower:
                # Fallback to Opus/OGG or WAV only if the build lacks libmp3lame.
                out_path = base / "output.ogg"
                out_mime = "audio/ogg"
                proc = _run_ffmpeg_transcode(
                    enc_bin,
                    in_path,
                    out_path,
                    codec="libopus",
                    container="ogg",
                    bitrate=TARGET_BITRATE,
                )
                if proc.returncode != 0:
                    out_path = base / "output.wav"
                    out_mime = "audio/wav"
                    proc = _run_ffmpeg_transcode(
                        enc_bin,
                        in_path,
                        out_path,
                        codec="pcm_s16le",
                        container="wav",
                        bitrate=None,
                    )
            if proc.returncode != 0:
                raise Rejected("invalid_audio")

        if not out_path.is_file():
            raise Rejected("invalid_audio")
        encoded = out_path.read_bytes()
        if not encoded:
            raise Rejected("invalid_audio")
        if len(encoded) > limit:
            raise Rejected("audio_too_large")

        out_info = _probe_audio(probe_bin, out_path)
        duration_sec = _parse_duration(out_info) or _parse_duration(in_info)
        if duration_sec is None:
            raise Rejected("audio_duration_invalid")

        audio = ModelAudio(
            data=base64.b64encode(encoded).decode("ascii"),
            mime_type=out_mime,
            duration_sec=round(duration_sec, 3),
            byte_size=len(encoded),
            digest=hashlib.sha256(data).hexdigest(),
        )
        audio.validate()
        return audio


class AudioHTTPFailure(Rejected):
    def __init__(self, status: int):
        super().__init__("audio_http_failed")
        self.status = status


class AudioLoader:
    """Fetch QQ voice audio over OneBot get_record or public-IP-pinned HTTPS and normalize."""

    def __init__(
        self,
        settings: Settings,
        transport: Any = None,
        *,
        plugin_bus: Any = None,
        ffmpeg_bin: str | None = None,
        ffprobe_bin: str | None = None,
    ):
        self.s, self.transport = settings, transport
        self.plugin_bus = plugin_bus
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        # Serializes OneBot WS audio transfers so domestic uplink stays responsive.
        self.ws_audios = asyncio.Semaphore(1)
        self.decode_slots = asyncio.Semaphore(2)
        self.download_slots = asyncio.Semaphore(2)
        self.cache: OrderedDict[tuple[str, str], ModelAudio] = OrderedDict()
        self.aliases: OrderedDict[tuple[str, str], tuple[str, str]] = OrderedDict()
        self.cache_bytes = 0
        self.cache_limit = 32 * 1024 * 1024
        self.fetch_locks = [asyncio.Lock() for _ in range(64)]

    def failure_status(self, ref: AudioRef) -> str:
        return "oversized" if ref.file_size and ref.file_size > MAX_AUDIO_BYTES else "unavailable"

    def _client(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(resolver=PublicResolver(), use_dns_cache=False, limit=2)
        return aiohttp.ClientSession(
            connector=connector,
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            timeout=aiohttp.ClientTimeout(total=HTTP_AUDIO_TIMEOUT, connect=5),
        )

    @staticmethod
    def _warn(stage: str, exc: BaseException) -> None:
        # Never log exception text, file IDs, signed URLs, QQ IDs, or raw bytes.
        status = getattr(exc, "status", None)
        log.warning(
            "event=audio_download_failed stage=%s exception=%s status=%s",
            stage,
            type(exc).__name__,
            status if isinstance(status, int) else "none",
        )

    async def _napcat(self, ref: AudioRef) -> tuple[bytes | None, list[str]]:
        file_id = valid_record_file_id(ref.file)
        if self.transport is None or not file_id:
            return None, []
        urls: list[str] = []
        rpc_calls: list[tuple[str, dict[str, Any]]] = [
            ("get_record", {"file": file_id, "out_format": fmt}) for fmt in _FALLBACK_OUT_FORMATS
        ]
        rpc_calls.append(("get_file", {"file": file_id}))
        async with self.ws_audios:
            for action, params in rpc_calls:
                try:
                    async with asyncio.timeout(RPC_AUDIO_TIMEOUT):
                        value = await self.transport.call(action, params)
                    if not isinstance(value, dict):
                        raise Rejected("audio_rpc_invalid")
                    declared = value.get("file_size", value.get("size"))
                    if (
                        declared is not None
                        and not isinstance(declared, bool)
                        and str(declared).isdecimal()
                        and int(declared) > MAX_AUDIO_BYTES
                    ):
                        raise Rejected("audio_too_large")
                    # Strictly ignore any local path keys ("file", "path", "file_path", etc.).
                    raw = value.get("base64")
                    if isinstance(raw, str) and raw:
                        if raw.startswith("base64://"):
                            raw = raw[9:]
                        if len(raw) > 4 * ((MAX_AUDIO_BYTES + 2) // 3):
                            raise Rejected("audio_too_large")
                        decoded = base64.b64decode(raw, validate=True)
                        if not decoded or len(decoded) > MAX_AUDIO_BYTES:
                            raise Rejected("audio_too_large")
                        return decoded, urls
                    url = value.get("url")
                    if isinstance(url, str) and url:
                        checked = validate_audio_url(url)
                        urls.append(checked)
                        break
                except (Rejected, TimeoutError, OSError, ValueError, binascii.Error) as exc:
                    self._warn("napcat", exc)
                    if isinstance(exc, Rejected) and str(exc) == "audio_too_large":
                        raise
        return None, urls

    async def _http_one(
        self, client: aiohttp.ClientSession, url: str, *, maximum: int = MAX_AUDIO_BYTES
    ) -> bytes:
        current = url
        async with asyncio.timeout(HTTP_AUDIO_TIMEOUT):
            for _ in range(3):
                checked = validate_audio_url(current)
                async with client.get(
                    checked,
                    allow_redirects=False,
                    headers={"Accept": "audio/*,*/*;q=0.5", "User-Agent": BROWSER_AGENT},
                ) as response:
                    if response.status in {301, 302, 303, 307, 308}:
                        loc = response.headers.get("Location", "").strip()
                        if not loc:
                            raise AudioHTTPFailure(response.status)
                        current = urljoin(checked, loc)
                        continue
                    if response.status != 200:
                        raise AudioHTTPFailure(response.status)
                    content_type = (response.content_type or "").lower()
                    if (
                        response.headers.get("Content-Encoding", "identity") != "identity"
                        or content_type in _REJECTED_HTTP_CONTENT_TYPES
                        or (response.content_length or 0) > maximum
                    ):
                        raise Rejected("audio_download_failed")
                    raw = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        raw.extend(chunk)
                        if len(raw) > maximum:
                            raise Rejected("audio_too_large")
                    if not raw:
                        raise Rejected("audio_download_failed")
                    return bytes(raw)
        raise Rejected("audio_download_failed")

    async def _fetch_raw(
        self, client: aiohttp.ClientSession, ref: AudioRef, *, maximum: int = MAX_AUDIO_BYTES
    ) -> bytes:
        if ref.file_size and ref.file_size > maximum:
            raise Rejected("audio_too_large")
        if ref.file.startswith("plugin_media:"):
            if self.plugin_bus is None or not hasattr(self.plugin_bus, "fetch_plugin_media"):
                raise Rejected("audio_url_unavailable")
            raw = await self.plugin_bus.fetch_plugin_media(
                ref.file, kind="audio", max_bytes=maximum
            )
            if not raw or len(raw) > maximum:
                raise Rejected("audio_too_large" if raw else "audio_download_failed")
            return raw
        raw, refreshed = await self._napcat(ref)
        if raw is not None:
            if len(raw) > maximum:
                raise Rejected("audio_too_large")
            return raw
        url = next(iter(refreshed), ref.url)
        if not url:
            raise Rejected("audio_url_unavailable")
        raw = await self._http_one(client, url, maximum=maximum)
        if len(raw) > maximum:
            raise Rejected("audio_too_large")
        return raw

    async def normalize(self, raw: bytes) -> ModelAudio:
        async with self.decode_slots:
            return await asyncio.to_thread(
                normalize_audio,
                raw,
                ffmpeg_bin=self.ffmpeg_bin,
                ffprobe_bin=self.ffprobe_bin,
                max_bytes=MAX_AUDIO_BYTES,
            )

    async def _one(self, client: aiohttp.ClientSession, ref: AudioRef) -> ModelAudio:
        raw = await self._fetch_raw(client, ref, maximum=MAX_AUDIO_BYTES)
        return await self.normalize(raw)

    async def cached_one(
        self, client: aiohttp.ClientSession, ref: AudioRef, scope: str
    ) -> ModelAudio:
        async with self.fetch_locks[hash((scope, ref.identity)) % len(self.fetch_locks)]:
            alias = self.aliases.get((scope, ref.identity))
            if alias in self.cache:
                self.cache.move_to_end(alias)
                return self.cache[alias]
            audio = await self._one(client, ref)
            key = (scope, audio.digest or hashlib.sha256(audio.data.encode()).hexdigest())
            if key not in self.cache:
                self.cache[key] = audio
                self.cache_bytes += audio.memory_bytes()
            self.aliases[(scope, ref.identity)] = key

            def evict(victim: tuple[str, str]) -> None:
                old = self.cache.pop(victim)
                self.cache_bytes -= old.memory_bytes()

            own = [k for k in self.cache if k[0] == scope]
            for victim in own[:-30]:
                evict(victim)
            while self.cache_bytes > self.cache_limit or len(self.cache) > 512:
                evict(next(iter(self.cache)))
            self.aliases = OrderedDict((k, v) for k, v in self.aliases.items() if v in self.cache)
            while len(self.aliases) > 2048:
                self.aliases.popitem(last=False)
            return audio

    async def load_results(
        self,
        refs: list[AudioRef],
        *,
        scope: str = "legacy",
        max_total_bytes: int = MAX_INLINE_AUDIO_BYTES,
    ) -> list[ModelAudio | None]:
        """Load audio refs without business count/duration caps, bounded by inline byte cap."""
        if not refs:
            return []
        cap = min(max(0, int(max_total_bytes)), MAX_INLINE_AUDIO_BYTES)
        results: list[ModelAudio | None] = []
        total_bytes = 0
        async with self.download_slots, self._client() as client:
            for ref in refs:
                try:
                    loaded = await self.cached_one(client, ref, scope)
                    size = loaded.byte_size or len(loaded.raw_bytes())
                    if total_bytes + size > cap:
                        results.append(None)
                        continue
                    total_bytes += size
                    results.append(loaded)
                except asyncio.CancelledError:
                    raise
                except (Rejected, TimeoutError, aiohttp.ClientError, OSError, ValueError) as exc:
                    self._warn("fallback", exc)
                    results.append(None)
        return results
