"""Pure-Python PCM helpers, energy VAD, ffmpeg mic capture, and afplay queue player."""

from __future__ import annotations

import asyncio
import contextlib
import io
import logging
import math
import re
import shutil
import subprocess
import sys
import tempfile
import time
import wave
from array import array
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from .providers import TTSProvider

log = logging.getLogger("voice_client.audio")

_AVFOUNDATION_DEVICE_RE = re.compile(r"\[(\d+)\]\s+(.+?)\s*$")
_EXT_CLEAN_RE = re.compile(r"[^a-zA-Z0-9]")


def pcm16_rms(pcm_bytes: bytes) -> float:
    """Compute normalized RMS amplitude [0.0, 1.0+] for little-endian signed 16-bit PCM."""
    usable = len(pcm_bytes) - (len(pcm_bytes) % 2)
    if usable < 2:
        return 0.0
    samples = array("h")
    samples.frombytes(pcm_bytes[:usable])
    if sys.byteorder == "big":
        samples.byteswap()
    mean_square = sum(sample * sample for sample in samples) / len(samples)
    return math.sqrt(mean_square) / 32768.0


def pcm16_to_wav_bytes(
    pcm_bytes: bytes,
    *,
    sample_rate: int = 16000,
    channels: int = 1,
) -> bytes:
    """Wrap raw signed 16-bit little-endian PCM bytes in a WAV container."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_bytes)
    return buf.getvalue()


class EnergyVadDetector:
    """Frame-by-frame energy VAD with pre-roll, hangover silence, min/max cut, and barge-in."""

    def __init__(
        self,
        sample_rate: int = 16000,
        frame_ms: int = 30,
        threshold: float = 0.018,
        silence_ms: int = 480,
        min_speech_ms: int = 320,
        max_speech_ms: int = 20000,
        barge_in_multiplier: float = 2.2,
        pre_roll_ms: int = 300,
        speech_start_frames: int = 2,
    ) -> None:
        self.sample_rate = sample_rate
        self.frame_ms = max(1, frame_ms)
        self.threshold = threshold
        self.silence_ms = silence_ms
        self.min_speech_ms = min_speech_ms
        self.max_speech_ms = max_speech_ms
        self.barge_in_multiplier = max(1.0, barge_in_multiplier)
        self.pre_roll_ms = max(0, pre_roll_ms)
        self.speech_start_frames = max(1, speech_start_frames)

        self._pre_roll_max_frames = (
            max(1, math.ceil(self.pre_roll_ms / self.frame_ms)) if self.pre_roll_ms > 0 else 0
        )
        self._silence_hangover_frames = max(1, math.ceil(self.silence_ms / self.frame_ms))
        self._max_speech_frames = max(1, math.ceil(self.max_speech_ms / self.frame_ms))

        self._pre_roll: deque[bytes] = deque(maxlen=self._pre_roll_max_frames or None)
        self._onset_frames: list[bytes] = []
        self._in_speech = False
        self._utterance = bytearray()
        self._active_speech_frames = 0
        self._speech_frames_since_onset = 0
        self._trailing_silence_frames = 0

    def reset(self) -> None:
        """Reset detector state and discard any buffered audio."""
        self._pre_roll.clear()
        self._onset_frames.clear()
        self._in_speech = False
        self._utterance.clear()
        self._active_speech_frames = 0
        self._speech_frames_since_onset = 0
        self._trailing_silence_frames = 0

    def _emit_if_long_enough(self) -> bytes | None:
        active_ms = self._active_speech_frames * self.frame_ms
        pcm = bytes(self._utterance) if active_ms >= self.min_speech_ms else None
        self.reset()
        return pcm

    def flush(self) -> bytes | None:
        """Finalize and return the active utterance if it meets min_speech_ms, else None."""
        if not self._in_speech:
            self.reset()
            return None
        return self._emit_if_long_enough()

    def feed(
        self,
        frame: bytes,
        *,
        playback_active: bool = False,
    ) -> tuple[bool, bytes | None]:
        """Process one PCM16 frame and return (barge_in_triggered, completed_pcm)."""
        if not frame:
            return False, None

        rms = pcm16_rms(frame)
        if not self._in_speech:
            onset_threshold = (
                self.threshold * self.barge_in_multiplier if playback_active else self.threshold
            )
            if rms >= onset_threshold:
                self._onset_frames.append(frame)
                if len(self._onset_frames) < self.speech_start_frames:
                    return False, None

                # Speech onset confirmed: prepend pre-roll, then onset frames.
                self._in_speech = True
                self._utterance.clear()
                if self._pre_roll_max_frames > 0:
                    for pre_frame in self._pre_roll:
                        self._utterance.extend(pre_frame)
                    self._pre_roll.clear()
                onset_count = len(self._onset_frames)
                for onset_frame in self._onset_frames:
                    self._utterance.extend(onset_frame)
                self._onset_frames.clear()

                self._active_speech_frames = onset_count
                self._speech_frames_since_onset = onset_count
                self._trailing_silence_frames = 0
                barge_in = playback_active

                if self._speech_frames_since_onset >= self._max_speech_frames:
                    return barge_in, self._emit_if_long_enough()
                return barge_in, None

            # Abort partial onset spike and preserve audio in pre-roll ring buffer.
            if self._onset_frames:
                if self._pre_roll_max_frames > 0:
                    self._pre_roll.extend(self._onset_frames)
                self._onset_frames.clear()
            if self._pre_roll_max_frames > 0:
                self._pre_roll.append(frame)
            return False, None

        # Currently inside an active speech segment.
        self._utterance.extend(frame)
        self._speech_frames_since_onset += 1
        if rms >= self.threshold:
            self._active_speech_frames += 1
            self._trailing_silence_frames = 0
        else:
            self._trailing_silence_frames += 1

        if self._speech_frames_since_onset >= self._max_speech_frames:
            return False, self._emit_if_long_enough()
        if self._trailing_silence_frames >= self._silence_hangover_frames:
            return False, self._emit_if_long_enough()
        return False, None


def list_avfoundation_audio_devices(ffmpeg_bin: str = "ffmpeg") -> list[tuple[str, str]]:
    """Query macOS AVFoundation audio input devices via ffmpeg -list_devices."""
    try:
        proc = subprocess.run(
            [ffmpeg_bin, "-hide_banner", "-f", "avfoundation", "-list_devices", "true", "-i", ""],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"未找到 {ffmpeg_bin!r}，请先在 macOS 安装 ffmpeg（例如: brew install ffmpeg）。"
        ) from None
    except subprocess.TimeoutExpired:
        raise RuntimeError("枚举 AVFoundation 音频设备超时。") from None

    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part)
    devices: list[tuple[str, str]] = []
    in_audio_section = False
    for raw_line in output.splitlines():
        if "AVFoundation audio devices:" in raw_line:
            in_audio_section = True
            continue
        if "AVFoundation video devices:" in raw_line:
            in_audio_section = False
            continue
        if not in_audio_section:
            continue
        match = _AVFOUNDATION_DEVICE_RE.search(raw_line)
        if match:
            devices.append((match.group(1), match.group(2).strip()))
        elif raw_line.strip().startswith("[") and "[AVFoundation" not in raw_line:
            break
    return devices


class FfmpegMicCapture:
    """Async microphone frame reader backed by ffmpeg AVFoundation s16le pipe."""

    def __init__(
        self,
        *,
        device: str = "0",
        sample_rate: int = 16000,
        frame_bytes: int = 960,
        ffmpeg_bin: str = "ffmpeg",
        cmd_factory: Callable[[], Sequence[str]] | None = None,
    ) -> None:
        self.device = device
        self.sample_rate = sample_rate
        self.frame_bytes = frame_bytes
        self.ffmpeg_bin = ffmpeg_bin
        self._cmd_factory = cmd_factory
        self._proc: asyncio.subprocess.Process | None = None
        self._stopping = False

    def _build_command(self) -> list[str]:
        if self._cmd_factory is not None:
            return [str(part) for part in self._cmd_factory()]
        return [
            self.ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "avfoundation",
            "-i",
            f":{self.device}",
            "-ac",
            "1",
            "-ar",
            str(self.sample_rate),
            "-f",
            "s16le",
            "pipe:1",
        ]

    async def start(self) -> None:
        """Start the ffmpeg capture subprocess."""
        if self._proc is not None:
            return
        self._stopping = False
        cmd = self._build_command()
        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            raise RuntimeError(
                f"未找到 {self.ffmpeg_bin!r}，请先在 macOS 安装 ffmpeg"
                "（例如: brew install ffmpeg）。"
            ) from None

    async def read_frame(self) -> bytes:
        """Read one exact PCM frame, returning b'' on clean EOF."""
        proc = self._proc
        if proc is None or proc.stdout is None:
            return b""
        try:
            return await proc.stdout.readexactly(self.frame_bytes)
        except asyncio.IncompleteReadError:
            if self._stopping:
                return b""
            rc = await proc.wait()
            stderr_msg = ""
            if proc.stderr is not None:
                stderr_msg = (await proc.stderr.read()).decode("utf-8", errors="replace").strip()
            self._proc = None
            if rc != 0 and stderr_msg:
                raise RuntimeError(f"麦克风采集进程异常退出 (exit={rc}): {stderr_msg}") from None
            return b""

    async def stop(self) -> None:
        """Terminate the ffmpeg capture subprocess cleanly."""
        self._stopping = True
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        await _terminate_process(proc)


async def _terminate_process(proc: asyncio.subprocess.Process) -> None:
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
        await proc.wait()


def _consume_task_exception(task: asyncio.Task[tuple[bytes, str]]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except BaseException:
        pass


def _default_ffplay_stream_cmd(fmt: str) -> list[str]:
    clean_fmt = _EXT_CLEAN_RE.sub("", fmt.lstrip(".")) or "mp3"
    return [
        "ffplay",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nodisp",
        "-autoexit",
        "-probesize",
        "4096",
        "-analyzeduration",
        "100000",
        "-f",
        clean_fmt,
        "-i",
        "pipe:0",
    ]


@dataclass
class _QueuedChunk:
    turn_id: str
    seq: int
    text: str
    synth_task: asyncio.Task[tuple[bytes, str]] | None = None
    enqueued_at: float = field(default_factory=time.perf_counter)


class AfplayAudioPlayer:
    """Ordered TTS playback queue with streaming ffplay pipe, afplay fallback, and cancel."""

    def __init__(
        self,
        tts_provider: TTSProvider,
        *,
        play_cmd_factory: Callable[[Path], Sequence[str]] | None = None,
        stream_cmd_factory: Callable[[str], Sequence[str]] | None = None,
        enable_streaming: bool = False,
        on_first_audio: Callable[[str, float], None] | None = None,
    ) -> None:
        self._tts = tts_provider
        self._play_cmd_factory = play_cmd_factory or (lambda path: ["afplay", str(path)])
        if stream_cmd_factory is not None:
            self._stream_cmd_factory: Callable[[str], Sequence[str]] | None = stream_cmd_factory
        elif enable_streaming and play_cmd_factory is None and shutil.which("ffplay") is not None:
            self._stream_cmd_factory = _default_ffplay_stream_cmd
        else:
            self._stream_cmd_factory = None
        self._enable_streaming = bool(enable_streaming)
        self._on_first_audio = on_first_audio
        self._queue: deque[_QueuedChunk] = deque()
        self._cancelled_turns: deque[str] = deque(maxlen=64)
        self._wake_event = asyncio.Event()
        self._idle_event = asyncio.Event()
        self._idle_event.set()

        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._active_item: _QueuedChunk | None = None
        self._active_stream_task: asyncio.Task[None] | None = None
        self._active_proc: asyncio.subprocess.Process | None = None
        self._active_cancelled = False
        self._is_playing = False
        self._stop_requested = False
        self._file_seq = 0

    @property
    def streaming_active(self) -> bool:
        """Return True when live chunk streaming into ffplay is enabled and supported."""
        return (
            self._enable_streaming
            and self._stream_cmd_factory is not None
            and callable(getattr(self._tts, "synthesize_stream", None))
        )

    def _fire_first_audio(self, turn_id: str, elapsed_ms: float) -> None:
        if self._on_first_audio is None:
            return
        try:
            self._on_first_audio(turn_id, elapsed_ms)
        except Exception:
            pass

    @property
    def is_playing(self) -> bool:
        """Return True while audio is actively playing out through the audio subprocess."""
        return self._is_playing

    async def start(self) -> None:
        """Start the background playback worker and allocate temp directory."""
        if self._worker_task is not None:
            return
        self._stop_requested = False
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix="hakimi_voice_play_")
        self._worker_task = asyncio.create_task(self._worker_loop())

    def enqueue_chunk(self, turn_id: str, seq: int, text: str) -> None:
        """Queue a chunk for streaming ffplay or parallel TTS prefetch + afplay."""
        if self._stop_requested or not text.strip():
            return
        if turn_id in self._cancelled_turns:
            return
        if self.streaming_active:
            task = None
        else:
            task = asyncio.create_task(self._tts.synthesize(text))
            task.add_done_callback(_consume_task_exception)
        self._queue.append(_QueuedChunk(turn_id=turn_id, seq=seq, text=text, synth_task=task))
        self._idle_event.clear()
        self._wake_event.set()

    async def cancel_turn(self, turn_id: str | None = None) -> None:
        """Cancel queued/active synthesis and kill playback for one turn or all turns."""
        if turn_id is not None and turn_id not in self._cancelled_turns:
            self._cancelled_turns.append(turn_id)

        remaining: deque[_QueuedChunk] = deque()
        while self._queue:
            item = self._queue.popleft()
            if turn_id is None or item.turn_id == turn_id:
                if item.synth_task is not None:
                    item.synth_task.cancel()
            else:
                remaining.append(item)
        self._queue = remaining

        active = self._active_item
        if active is not None and (turn_id is None or active.turn_id == turn_id):
            self._active_cancelled = True
            if active.synth_task is not None:
                active.synth_task.cancel()
            if self._active_stream_task is not None and not self._active_stream_task.done():
                self._active_stream_task.cancel()
            proc = self._active_proc
            if proc is not None:
                await _terminate_process(proc)

        if not self._queue and self._active_item is None:
            self._idle_event.set()

    async def wait_idle(self) -> None:
        """Wait until all queued chunks have finished playing or been cancelled."""
        await self._idle_event.wait()

    async def stop(self) -> None:
        """Cancel pending audio, stop the playback worker, and clean up temp files."""
        self._stop_requested = True
        await self.cancel_turn(None)
        self._wake_event.set()
        worker = self._worker_task
        self._worker_task = None
        if worker is not None:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        self._is_playing = False
        self._idle_event.set()
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None

    async def play_once(self, audio_bytes: bytes, ext: str = "mp3") -> None:
        """Play a single synthesized audio buffer to completion (used by --test-tts)."""
        clean_ext = _EXT_CLEAN_RE.sub("", ext.lstrip(".")) or "mp3"
        with tempfile.TemporaryDirectory(prefix="hakimi_voice_once_") as tmpdir:
            out_path = Path(tmpdir) / f"once.{clean_ext}"
            await asyncio.to_thread(out_path.write_bytes, audio_bytes)
            cmd = [str(part) for part in self._play_cmd_factory(out_path)]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError:
                raise RuntimeError(f"未找到播放命令 {cmd[0]!r}。") from None
            try:
                self._is_playing = True
                _, stderr = await proc.communicate()
                if proc.returncode != 0:
                    err = stderr.decode("utf-8", errors="replace").strip()
                    raise RuntimeError(f"音频播放失败 (exit={proc.returncode}): {err}")
            except asyncio.CancelledError:
                await _terminate_process(proc)
                raise
            finally:
                self._is_playing = False

    async def _play_bytes_in_tmpdir(
        self,
        audio_bytes: bytes,
        ext: str,
        item: _QueuedChunk,
    ) -> None:
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix="hakimi_voice_play_")
        clean_ext = _EXT_CLEAN_RE.sub("", ext.lstrip(".")) or "mp3"
        self._file_seq += 1
        out_path = Path(self._tmpdir.name) / f"chunk_{self._file_seq}_{item.seq}.{clean_ext}"
        await asyncio.to_thread(out_path.write_bytes, audio_bytes)
        cmd = [str(part) for part in self._play_cmd_factory(out_path)]
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            self._active_proc = proc
            if (
                self._active_cancelled
                or self._stop_requested
                or item.turn_id in self._cancelled_turns
            ):
                await _terminate_process(proc)
                return
            self._is_playing = True
            self._fire_first_audio(
                item.turn_id,
                (time.perf_counter() - item.enqueued_at) * 1000.0,
            )
            _, stderr = await proc.communicate()
            if (
                proc.returncode not in (0, None)
                and not self._active_cancelled
                and not self._stop_requested
            ):
                err = stderr.decode("utf-8", errors="replace").strip()
                log.warning("Playback exited non-zero (%s): %s", proc.returncode, err)
        except FileNotFoundError:
            log.warning("Playback command not found: %s", cmd[0])
        except asyncio.CancelledError:
            if proc is not None:
                await _terminate_process(proc)
            raise
        finally:
            self._is_playing = False
            self._active_proc = None
            out_path.unlink(missing_ok=True)

    async def _worker_loop(self) -> None:
        while not self._stop_requested:
            if not self._queue:
                if self._active_item is None:
                    self._idle_event.set()
                self._wake_event.clear()
                if self._stop_requested:
                    break
                await self._wake_event.wait()
                continue

            item = self._queue.popleft()
            if item.turn_id in self._cancelled_turns:
                if item.synth_task is not None:
                    item.synth_task.cancel()
                if not self._queue:
                    self._idle_event.set()
                continue

            self._active_item = item
            self._active_cancelled = False
            try:
                if item.synth_task is None:
                    stream_task = asyncio.create_task(self._play_stream_item(item))
                    self._active_stream_task = stream_task
                    try:
                        await stream_task
                    except asyncio.CancelledError:
                        if self._stop_requested or not self._active_cancelled:
                            raise
                        continue
                    finally:
                        self._active_stream_task = None
                    continue

                try:
                    audio_bytes, ext = await item.synth_task
                except asyncio.CancelledError:
                    if self._stop_requested or not self._active_cancelled:
                        raise
                    continue
                if (
                    self._stop_requested
                    or self._active_cancelled
                    or item.turn_id in self._cancelled_turns
                ):
                    continue
                await self._play_bytes_in_tmpdir(audio_bytes, ext, item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "TTS chunk failed (turn=%s seq=%s): %s",
                    item.turn_id,
                    item.seq,
                    type(exc).__name__,
                )
            finally:
                self._active_item = None
                self._active_cancelled = False
                if not self._queue:
                    self._idle_event.set()

    async def play_stream_once(self, text: str) -> tuple[float, float, int, str]:
        """Play one phrase via streaming ffplay when supported, else download + afplay."""
        if self.streaming_active:
            return await self._stream_to_player(text, turn_id=None, raise_on_error=True)
        t0 = time.perf_counter()
        audio_bytes, fmt = await self._tts.synthesize(text)
        ttfb_ms = (time.perf_counter() - t0) * 1000.0
        await self.play_once(audio_bytes, fmt)
        total_ms = (time.perf_counter() - t0) * 1000.0
        return ttfb_ms, total_ms, len(audio_bytes), fmt

    async def _play_stream_item(self, item: _QueuedChunk) -> None:
        await self._stream_to_player(
            item.text,
            turn_id=item.turn_id,
            enqueued_at=item.enqueued_at,
            raise_on_error=False,
        )

    async def _stream_to_player(
        self,
        text: str,
        *,
        turn_id: str | None,
        enqueued_at: float | None = None,
        raise_on_error: bool,
    ) -> tuple[float, float, int, str]:
        assert self._stream_cmd_factory is not None
        synth_stream = getattr(self._tts, "synthesize_stream", None)
        assert callable(synth_stream)
        raw_fmt = str(getattr(self._tts, "response_format", "mp3") or "mp3")
        fmt = _EXT_CLEAN_RE.sub("", raw_fmt.lstrip(".")) or "mp3"
        t0 = enqueued_at if enqueued_at is not None else time.perf_counter()
        proc: asyncio.subprocess.Process | None = None
        gen = synth_stream(text)
        total_bytes = 0
        ttfb_ms = 0.0
        stderr_data = b""

        def _is_aborted() -> bool:
            return self._stop_requested or (
                turn_id is not None and (self._active_cancelled or turn_id in self._cancelled_turns)
            )

        try:
            try:
                first_chunk = await anext(gen)
            except StopAsyncIteration:
                return 0.0, (time.perf_counter() - t0) * 1000.0, 0, fmt
            ttfb_ms = (time.perf_counter() - t0) * 1000.0
            total_bytes += len(first_chunk)
            if _is_aborted():
                return ttfb_ms, ttfb_ms, total_bytes, fmt

            cmd = [str(part) for part in self._stream_cmd_factory(fmt)]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
            except FileNotFoundError:
                if raise_on_error:
                    raise RuntimeError(f"未找到流式播放命令 {cmd[0]!r}。") from None
                log.warning("Streaming playback command not found: %s", cmd[0])
                return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, fmt

            self._active_proc = proc
            if _is_aborted():
                await _terminate_process(proc)
                return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, fmt

            self._is_playing = True
            if turn_id is not None:
                self._fire_first_audio(turn_id, ttfb_ms)

            assert proc.stdin is not None
            try:
                proc.stdin.write(first_chunk)
                await proc.stdin.drain()
                async for chunk in gen:
                    if _is_aborted():
                        break
                    if chunk:
                        total_bytes += len(chunk)
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    proc.stdin.close()
                    await proc.stdin.wait_closed()

            if _is_aborted():
                await _terminate_process(proc)
                return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, fmt

            if proc.stderr is not None:
                stderr_data = await proc.stderr.read()
            await proc.wait()
            if proc.returncode not in (0, None) and not _is_aborted():
                err = stderr_data.decode("utf-8", errors="replace").strip()
                if raise_on_error:
                    raise RuntimeError(f"流式音频播放失败 (exit={proc.returncode}): {err}")
                log.warning("Streaming playback exited non-zero (%s): %s", proc.returncode, err)
            return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, fmt
        finally:
            self._is_playing = False
            if proc is not None:
                self._active_proc = None
                if proc.returncode is None:
                    with contextlib.suppress(Exception):
                        await _terminate_process(proc)
            with contextlib.suppress(Exception):
                await gen.aclose()
