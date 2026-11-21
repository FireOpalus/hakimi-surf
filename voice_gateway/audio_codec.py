"""Audio normalization and live stream transcoding helpers for hakimi-voice-gateway."""

from __future__ import annotations

import asyncio
import contextlib
import io
import math
import os
import tempfile
import wave
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from pathlib import Path

from voice_client.audio import pcm16_to_wav_bytes

OUTPUT_FORMATS: tuple[str, ...] = ("mp3", "wav", "ogg_opus", "pcm_16k", "pcm_48k_stereo")

_OUTPUT_FORMAT_META: dict[str, tuple[str, int, int]] = {
    "mp3": ("audio/mpeg", 44100, 1),
    "wav": ("audio/wav", 32000, 1),
    "ogg_opus": ("audio/ogg", 48000, 1),
    "pcm_16k": ("audio/L16;rate=16000;channels=1", 16000, 1),
    "pcm_48k_stereo": ("audio/L16;rate=48000;channels=2", 48000, 2),
}

_FFMPEG_INPUT_ARGS: dict[str, list[str]] = {
    "mp3": ["-f", "mp3"],
    "wav": ["-f", "wav"],
    "ogg": ["-f", "ogg"],
    "webm": ["-f", "webm"],
    "opus": [],
    "pcm_16k": ["-f", "s16le", "-ar", "16000", "-ac", "1"],
    "pcm_48k_stereo": ["-f", "s16le", "-ar", "48000", "-ac", "2"],
}

_FFMPEG_OUTPUT_ARGS: dict[str, list[str]] = {
    "mp3": ["-vn", "-ac", "1", "-ar", "44100", "-f", "mp3", "pipe:1"],
    "wav": ["-vn", "-ac", "1", "-ar", "32000", "-f", "wav", "pipe:1"],
    "ogg_opus": [
        "-vn",
        "-ac",
        "1",
        "-ar",
        "48000",
        "-c:a",
        "libopus",
        "-b:a",
        "32k",
        "-vbr",
        "on",
        "-application",
        "voip",
        "-f",
        "ogg",
        "pipe:1",
    ],
    "pcm_16k": ["-vn", "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1"],
    "pcm_48k_stereo": ["-vn", "-ac", "2", "-ar", "48000", "-f", "s16le", "pipe:1"],
}


def output_format_meta(fmt: str) -> tuple[str, int, int]:
    """Return (mime_type, sample_rate, channels) for a supported gateway output format."""
    key = fmt.strip().lower()
    meta = _OUTPUT_FORMAT_META.get(key)
    if meta is None:
        raise ValueError(f"Unsupported output format: {fmt!r}")
    return meta


async def _terminate_process(proc: asyncio.subprocess.Process) -> None:
    """Terminate a subprocess gracefully, escalating to SIGKILL on timeout or cancellation."""
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=0.5)
    except TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            return
        with contextlib.suppress(BaseException):
            await proc.wait()
    except asyncio.CancelledError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        raise


async def _run_ffmpeg_bytes(
    cmd: list[str],
    raw_bytes: bytes,
    *,
    timeout: float,  # noqa: ASYNC109
) -> bytes:
    """Run an ffmpeg conversion piping raw_bytes to stdin and returning stdout bytes."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(timeout):
            stdout, stderr = await proc.communicate(raw_bytes)
    except (TimeoutError, asyncio.CancelledError):
        await _terminate_process(proc)
        raise

    if proc.returncode != 0:
        err = stderr.decode("utf-8", errors="replace").strip()
        summary = " ".join(err.split())[:200] or "unknown ffmpeg error"
        raise RuntimeError(f"ffmpeg exited with code {proc.returncode}: {summary}")
    if not stdout:
        raise RuntimeError("ffmpeg produced empty audio output")
    return stdout


async def normalize_input_to_wav(
    raw_bytes: bytes,
    fmt: str,
    *,
    sample_rate: int = 16000,
    channels: int = 1,
    ffmpeg_bin: str = "ffmpeg",
    timeout: float = 15.0,  # noqa: ASYNC109
) -> bytes:
    """Normalize supported client audio bytes into 16kHz mono WAV bytes for STT."""
    if not raw_bytes:
        raise ValueError("Audio payload must not be empty")
    if not (1 <= sample_rate <= 192000):
        raise ValueError(f"Invalid sample_rate: {sample_rate}")
    if not (1 <= channels <= 8):
        raise ValueError(f"Invalid channels: {channels}")

    fmt_norm = fmt.strip().lower().lstrip(".")

    if fmt_norm == "wav":
        if len(raw_bytes) < 12 or raw_bytes[:4] != b"RIFF" or raw_bytes[8:12] != b"WAVE":
            raise ValueError("Invalid WAV audio: expected RIFF/WAVE header")
        return raw_bytes

    if fmt_norm == "pcm_16k":
        return pcm16_to_wav_bytes(raw_bytes, sample_rate=16000, channels=1)

    if fmt_norm in {"pcm_s16le", "pcm_48k_stereo", "pcm"}:
        sr, ch = (48000, 2) if fmt_norm == "pcm_48k_stereo" else (sample_rate, channels)
        if sr == 16000 and ch == 1:
            return pcm16_to_wav_bytes(raw_bytes, sample_rate=16000, channels=1)
        cmd = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "s16le",
            "-ar",
            str(sr),
            "-ac",
            str(ch),
            "-i",
            "pipe:0",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "wav",
            "pipe:1",
        ]
        try:
            return await _run_ffmpeg_bytes(cmd, raw_bytes, timeout=timeout)
        except FileNotFoundError:
            # Allow unit tests without ffmpeg installed to proceed with PCM header wrapping.
            return pcm16_to_wav_bytes(raw_bytes, sample_rate=sr, channels=ch)

    if fmt_norm in {"opus", "ogg", "mp3", "webm"}:
        in_args: list[str]
        if fmt_norm == "opus":
            in_args = ["-f", "ogg"] if raw_bytes.startswith(b"OggS") else []
        else:
            in_args = ["-f", fmt_norm]
        cmd = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            *in_args,
            "-i",
            "pipe:0",
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-f",
            "wav",
            "pipe:1",
        ]
        try:
            return await _run_ffmpeg_bytes(cmd, raw_bytes, timeout=timeout)
        except FileNotFoundError:
            raise RuntimeError(f"ffmpeg binary not found: {ffmpeg_bin!r}") from None

    raise ValueError(f"Unsupported input audio format: {fmt!r}")


async def _iter_chunks(
    source_iter: AsyncIterable[bytes] | Iterable[bytes],
) -> AsyncIterator[bytes]:
    if hasattr(source_iter, "__aiter__"):
        async for chunk in source_iter:  # type: ignore[union-attr]
            if chunk:
                yield bytes(chunk)
        return
    for chunk in source_iter:  # type: ignore[union-attr]
        if chunk:
            yield bytes(chunk)


async def transcode_audio_stream(
    source_iter: AsyncIterable[bytes] | Iterable[bytes],
    *,
    src_fmt: str,
    dst_fmt: str,
    ffmpeg_bin: str = "ffmpeg",
    chunk_size: int = 16384,
) -> AsyncIterator[bytes]:
    """Stream-transcode audio chunks via ffmpeg, or pass through zero-copy when formats match."""
    src = src_fmt.strip().lower().lstrip(".")
    dst = dst_fmt.strip().lower().lstrip(".")
    output_format_meta(dst)

    if dst == src:
        async for chunk in _iter_chunks(source_iter):
            yield chunk
        return

    in_args = _FFMPEG_INPUT_ARGS.get(src)
    if in_args is None:
        raise ValueError(f"Unsupported source audio format: {src_fmt!r}")
    out_args = _FFMPEG_OUTPUT_ARGS[dst]

    chunks = _iter_chunks(source_iter)
    try:
        first_chunk = await anext(chunks)
    except StopAsyncIteration:
        return

    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        *in_args,
        "-i",
        "pipe:0",
        *out_args,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        with contextlib.suppress(Exception):
            await chunks.aclose()
        raise RuntimeError(f"ffmpeg binary not found: {ffmpeg_bin!r}") from None

    feeder_error: BaseException | None = None

    async def _feed_stdin() -> None:
        nonlocal feeder_error
        assert proc.stdin is not None
        try:
            proc.stdin.write(first_chunk)
            await proc.stdin.drain()
            async for chunk in chunks:
                proc.stdin.write(chunk)
                await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            feeder_error = exc
        finally:
            with contextlib.suppress(Exception):
                proc.stdin.close()
                await proc.stdin.wait_closed()

    assert proc.stdout is not None
    assert proc.stderr is not None
    feeder_task = asyncio.create_task(_feed_stdin())
    stderr_task = asyncio.create_task(proc.stderr.read())

    try:
        while True:
            data = await proc.stdout.read(chunk_size)
            if not data:
                break
            yield data

        await feeder_task
        stderr_bytes = await stderr_task
        rc = await proc.wait()

        if feeder_error is not None:
            raise feeder_error
        if rc != 0:
            err = stderr_bytes.decode("utf-8", errors="replace").strip()
            summary = " ".join(err.split())[:200] or "unknown ffmpeg error"
            raise RuntimeError(f"ffmpeg transcode failed (exit={rc}): {summary}")
    finally:
        for task in (feeder_task, stderr_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(feeder_task, stderr_task, return_exceptions=True)
        if proc.returncode is None:
            await _terminate_process(proc)
        with contextlib.suppress(Exception):
            await chunks.aclose()


def _derive_ffprobe_bin(ffmpeg_bin: str) -> str:
    raw = str(ffmpeg_bin or "").strip()
    if not raw or raw == "ffmpeg":
        return "ffprobe"
    path = Path(raw)
    if path.name.startswith("ffmpeg"):
        return str(path.with_name("ffprobe" + path.name[6:]))
    return str(path.with_name("ffprobe"))


def _write_temp_audio(audio_bytes: bytes, suffix: str) -> str:
    fd, temp_path = tempfile.mkstemp(prefix="vg-dur-", suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(audio_bytes)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temp_path)
        raise
    return temp_path


async def probe_audio_duration_ms(
    audio_bytes: bytes,
    fmt: str,
    *,
    ffmpeg_bin: str = "ffmpeg",
    timeout: float = 5.0,  # noqa: ASYNC109
) -> int | None:
    """Return positive audio duration in milliseconds, or None when unavailable."""
    if not audio_bytes:
        return None
    fmt_norm = fmt.strip().lower().lstrip(".")
    if fmt_norm == "wav":
        try:
            with wave.open(io.BytesIO(audio_bytes), "rb") as wf:
                frames = wf.getnframes()
                rate = wf.getframerate()
        except (wave.Error, EOFError, ValueError):
            return None
        if frames <= 0 or rate <= 0:
            return None
        return max(1, round(frames / rate * 1000.0))
    if fmt_norm == "pcm_16k":
        if len(audio_bytes) < 2:
            return None
        return max(1, round(len(audio_bytes) / (16000 * 2) * 1000.0))
    if fmt_norm == "pcm_48k_stereo":
        if len(audio_bytes) < 4:
            return None
        return max(1, round(len(audio_bytes) / (48000 * 2 * 2) * 1000.0))

    ffprobe_bin = _derive_ffprobe_bin(ffmpeg_bin)
    suffix = ".ogg" if fmt_norm == "ogg_opus" else f".{fmt_norm}" if fmt_norm.isalnum() else ".bin"
    temp_path: str | None = None
    try:
        temp_path = await asyncio.to_thread(_write_temp_audio, audio_bytes, suffix)
        proc = await asyncio.create_subprocess_exec(
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            temp_path,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(timeout):
                stdout, _ = await proc.communicate()
        except TimeoutError:
            await _terminate_process(proc)
            return None
        except asyncio.CancelledError:
            await _terminate_process(proc)
            raise
        if proc.returncode != 0:
            return None
        raw_val = stdout.decode("utf-8", errors="replace").strip().splitlines()
        if not raw_val:
            return None
        seconds = float(raw_val[0].strip())
        if not math.isfinite(seconds) or seconds <= 0:
            return None
        return max(1, round(seconds * 1000.0))
    except (FileNotFoundError, OSError, ValueError):
        return None
    finally:
        if temp_path is not None:
            with contextlib.suppress(OSError):
                await asyncio.to_thread(os.unlink, temp_path)
