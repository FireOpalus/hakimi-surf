"""Pure-Python PCM helpers, energy VAD, ffmpeg mic capture, and afplay queue player."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
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
from collections import OrderedDict, deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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


class _StderrDrain:
    """Asynchronously drain a subprocess stderr pipe into a small bounded tail buffer."""

    def __init__(
        self,
        stream: asyncio.StreamReader | None,
        *,
        max_chunks: int = 32,
        chunk_size: int = 1024,
    ) -> None:
        self._chunks: deque[bytes] = deque(maxlen=max_chunks)
        self._task: asyncio.Task[None] | None = (
            asyncio.create_task(self._drain(stream, chunk_size)) if stream is not None else None
        )

    async def _drain(self, stream: asyncio.StreamReader, chunk_size: int) -> None:
        try:
            while True:
                data = await stream.read(chunk_size)
                if not data:
                    break
                self._chunks.append(data)
        except (asyncio.CancelledError, OSError, ValueError):
            return

    async def finish(self, *, timeout: float = 0.5) -> str:  # noqa: ASYNC109
        task = self._task
        self._task = None
        if task is not None:
            if not task.done():
                try:
                    async with asyncio.timeout(timeout):
                        await task
                except TimeoutError:
                    task.cancel()
                    with contextlib.suppress(BaseException):
                        await task
            else:
                with contextlib.suppress(BaseException):
                    await task
        return b"".join(self._chunks).decode("utf-8", errors="replace").strip()

    async def cancel(self) -> None:
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task


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
        self._stderr_drain: _StderrDrain | None = None
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
        self._stderr_drain = _StderrDrain(self._proc.stderr)

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
            drain = self._stderr_drain
            self._stderr_drain = None
            if drain is not None:
                stderr_msg = await drain.finish()
            elif proc.stderr is not None:
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
        drain = self._stderr_drain
        self._stderr_drain = None
        try:
            if proc is not None:
                await _terminate_process(proc)
        finally:
            if drain is not None:
                await drain.cancel()


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
    lowered = fmt.strip().lower()
    if lowered == "pcm_16k":
        return [
            "ffplay",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nodisp",
            "-autoexit",
            "-f",
            "s16le",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-i",
            "pipe:0",
        ]
    if lowered == "pcm_48k_stereo":
        return [
            "ffplay",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nodisp",
            "-autoexit",
            "-f",
            "s16le",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-i",
            "pipe:0",
        ]
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


def _audio_bytes_for_file(audio_bytes: bytes, fmt: str) -> tuple[bytes, str]:
    """Convert raw PCM formats to WAV for file-based playback (afplay), else return ext."""
    lowered = (fmt or "mp3").strip().lower()
    if lowered == "pcm_16k":
        return pcm16_to_wav_bytes(audio_bytes, sample_rate=16000, channels=1), "wav"
    if lowered == "pcm_48k_stereo":
        return pcm16_to_wav_bytes(audio_bytes, sample_rate=48000, channels=2), "wav"
    ext = _EXT_CLEAN_RE.sub("", lowered.lstrip(".")) or "mp3"
    return audio_bytes, ext


@dataclass
class _TurnPlayCursor:
    started_seq: int = 0
    completed_seq: int = 0
    active_started_at: float = 0.0
    played_ms: int = 0


class _PlaybackCursorTracker:
    """Bounded per-turn playout cursor tracking started/completed chunk sequence and ms."""

    def __init__(self, max_turns: int = 64) -> None:
        self._max_turns = max_turns
        self._turns: OrderedDict[str, _TurnPlayCursor] = OrderedDict()
        self._last_turn_id: str = ""

    def ensure_turn(self, turn_id: str) -> _TurnPlayCursor:
        clean = str(turn_id or "").strip()
        cur = self._turns.get(clean)
        if cur is None:
            cur = _TurnPlayCursor()
            self._turns[clean] = cur
            while len(self._turns) > self._max_turns:
                self._turns.popitem(last=False)
        else:
            self._turns.move_to_end(clean)
        if clean:
            self._last_turn_id = clean
        return cur

    def mark_started(self, turn_id: str | None, seq: int | None) -> None:
        if not turn_id:
            return
        cur = self.ensure_turn(turn_id)
        if seq is not None and seq > cur.started_seq:
            cur.started_seq = int(seq)
        cur.active_started_at = time.perf_counter()

    def mark_completed(self, turn_id: str | None, seq: int | None) -> None:
        if not turn_id:
            return
        cur = self.ensure_turn(turn_id)
        if cur.active_started_at > 0.0:
            cur.played_ms += max(0, int((time.perf_counter() - cur.active_started_at) * 1000.0))
            cur.active_started_at = 0.0
        if seq is not None and seq > cur.completed_seq:
            cur.completed_seq = int(seq)

    def mark_stopped(self, turn_id: str | None) -> None:
        if not turn_id:
            return
        cur = self._turns.get(turn_id)
        if cur is not None and cur.active_started_at > 0.0:
            cur.played_ms += max(0, int((time.perf_counter() - cur.active_started_at) * 1000.0))
            cur.active_started_at = 0.0

    def snapshot(
        self,
        turn_id: str | None = None,
        *,
        active_turn_id: str = "",
        is_playing: bool = False,
    ) -> dict[str, Any]:
        tid = (turn_id or "").strip() or active_turn_id or self._last_turn_id
        cur = self._turns.get(tid) if tid else None
        if cur is None:
            return {"turn_id": tid, "played_seq": 0, "completed_seq": 0, "played_ms": 0}
        played_ms = cur.played_ms
        if (
            is_playing
            and cur.active_started_at > 0.0
            and (not active_turn_id or active_turn_id == tid)
        ):
            played_ms += max(0, int((time.perf_counter() - cur.active_started_at) * 1000.0))
        return {
            "turn_id": tid,
            "played_seq": cur.started_seq,
            "completed_seq": cur.completed_seq,
            "played_ms": played_ms,
        }


class BaseQueuedAudioPlayer:
    """Shared ordered playback queue, cursor tracking, temp-file afplay, and ffplay pipe logic."""

    def __init__(
        self,
        *,
        play_cmd_factory: Callable[[Path], Sequence[str]] | None = None,
        stream_cmd_factory: Callable[[str], Sequence[str]] | None = None,
        enable_streaming: bool = False,
        on_first_audio: Callable[[str, float], None] | None = None,
        tmpdir_prefix: str = "hakimi_voice_play_",
    ) -> None:
        self._play_cmd_factory = play_cmd_factory or (lambda path: ["afplay", str(path)])
        if stream_cmd_factory is not None:
            self._stream_cmd_factory: Callable[[str], Sequence[str]] | None = stream_cmd_factory
        elif enable_streaming and play_cmd_factory is None and shutil.which("ffplay") is not None:
            self._stream_cmd_factory = _default_ffplay_stream_cmd
        else:
            self._stream_cmd_factory = None
        self._enable_streaming = bool(enable_streaming)
        self._on_first_audio = on_first_audio
        self._tmpdir_prefix = tmpdir_prefix

        self._queue: deque[Any] = deque()
        self._cancelled_turns: deque[str] = deque(maxlen=64)
        self._wake_event = asyncio.Event()
        self._idle_event = asyncio.Event()
        self._idle_event.set()

        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._active_item: Any = None
        self._active_task: asyncio.Task[None] | None = None
        self._active_proc: asyncio.subprocess.Process | None = None
        self._active_once_proc: asyncio.subprocess.Process | None = None
        self._active_cancelled = False
        self._is_playing = False
        self._stop_requested = False
        self._file_seq = 0
        self._cursor = _PlaybackCursorTracker()

    def playback_cursor(self, turn_id: str | None = None) -> dict[str, Any]:
        """Return playout cursor (played_seq, completed_seq, played_ms) for turn_id."""
        active_tid = self._active_item.turn_id if self._active_item is not None else ""
        return self._cursor.snapshot(
            turn_id, active_turn_id=active_tid, is_playing=self._is_playing
        )

    @property
    def streaming_active(self) -> bool:
        """Return True when live chunk streaming into ffplay is enabled and supported."""
        return self._enable_streaming and self._stream_cmd_factory is not None

    @property
    def is_playing(self) -> bool:
        """Return True while audio is actively playing out through the audio subprocess."""
        return self._is_playing

    def _fire_first_audio(self, turn_id: str, elapsed_ms: float) -> None:
        if self._on_first_audio is None:
            return
        try:
            self._on_first_audio(turn_id, elapsed_ms)
        except Exception:
            pass

    def _is_aborted(self, turn_id: str | None) -> bool:
        if self._stop_requested:
            return True
        if turn_id is None:
            return False
        return self._active_cancelled or turn_id in self._cancelled_turns

    async def start(self) -> None:
        """Start the background playback worker and allocate temp directory."""
        if self._worker_task is not None:
            return
        self._stop_requested = False
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix=self._tmpdir_prefix)
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def wait_idle(self) -> None:
        """Wait until all queued items have finished playing or been cancelled."""
        await self._idle_event.wait()

    def _before_cancel_turn(self, turn_id: str | None) -> None:
        return None

    def _discard_item(self, item: Any) -> None:
        return None

    async def cancel_turn(self, turn_id: str | None = None) -> None:
        """Cancel queued/active audio and kill active playback immediately."""
        if turn_id is not None and turn_id not in self._cancelled_turns:
            self._cancelled_turns.append(turn_id)

        self._before_cancel_turn(turn_id)

        remaining: deque[Any] = deque()
        while self._queue:
            item = self._queue.popleft()
            if turn_id is None or item.turn_id == turn_id:
                self._discard_item(item)
            else:
                remaining.append(item)
        self._queue = remaining

        active = self._active_item
        if active is not None and (turn_id is None or active.turn_id == turn_id):
            self._active_cancelled = True
            self._discard_item(active)
            active_task = self._active_task
            if active_task is not None and not active_task.done():
                active_task.cancel()
            proc = self._active_proc
            if proc is not None:
                await _terminate_process(proc)
            self._is_playing = False
            if active_task is not None and active_task is not asyncio.current_task():
                with contextlib.suppress(BaseException):
                    await active_task

        if turn_id is None and self._active_once_proc is not None:
            await _terminate_process(self._active_once_proc)
            self._is_playing = False

        if not self._queue and self._active_item is None:
            self._idle_event.set()

    async def stop(self) -> None:
        """Cancel pending audio, stop the playback worker, and clean up temp files."""
        self._stop_requested = True
        await self.cancel_turn(None)
        self._wake_event.set()
        worker = self._worker_task
        self._worker_task = None
        if worker is not None:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        self._is_playing = False
        self._idle_event.set()
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None

    async def play_once(self, audio_bytes: bytes, ext: str = "mp3") -> None:
        """Play a single complete audio buffer via play_cmd_factory (afplay)."""
        if not audio_bytes:
            return
        payload, clean_ext = _audio_bytes_for_file(audio_bytes, ext)
        with tempfile.TemporaryDirectory(prefix=f"{self._tmpdir_prefix}once_") as tmpdir:
            out_path = Path(tmpdir) / f"once.{clean_ext}"
            await asyncio.to_thread(out_path.write_bytes, payload)
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
            self._active_once_proc = proc
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
                if self._active_once_proc is proc:
                    self._active_once_proc = None

    async def _play_bytes_in_tmpdir(
        self,
        audio_bytes: bytes,
        fmt: str,
        item: Any,
        *,
        file_prefix: str = "chunk",
    ) -> None:
        if not audio_bytes or self._is_aborted(item.turn_id):
            return
        if self._tmpdir is None:
            self._tmpdir = tempfile.TemporaryDirectory(prefix=self._tmpdir_prefix)
        payload, clean_ext = _audio_bytes_for_file(audio_bytes, fmt)
        self._file_seq += 1
        out_path = (
            Path(self._tmpdir.name) / f"{file_prefix}_{self._file_seq}_{item.seq}.{clean_ext}"
        )
        await asyncio.to_thread(out_path.write_bytes, payload)
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
            if self._is_aborted(item.turn_id):
                await _terminate_process(proc)
                return
            self._is_playing = True
            self._cursor.mark_started(item.turn_id, item.seq)
            self._fire_first_audio(
                item.turn_id,
                (time.perf_counter() - item.enqueued_at) * 1000.0,
            )
            _, stderr = await proc.communicate()
            if proc.returncode not in (0, None) and not self._is_aborted(item.turn_id):
                err = stderr.decode("utf-8", errors="replace").strip()
                log.warning("Playback exited non-zero (%s): %s", proc.returncode, err)
            elif proc.returncode == 0 and not self._is_aborted(item.turn_id):
                self._cursor.mark_completed(item.turn_id, item.seq)
        except FileNotFoundError:
            log.warning("Playback command not found: %s", cmd[0])
        except asyncio.CancelledError:
            if proc is not None:
                await _terminate_process(proc)
            raise
        finally:
            self._is_playing = False
            self._cursor.mark_stopped(item.turn_id)
            if self._active_proc is proc:
                self._active_proc = None
            out_path.unlink(missing_ok=True)

    async def _pipe_to_stream_player(
        self,
        first_chunk: bytes,
        rest_chunks: AsyncIterator[bytes],
        *,
        fmt: str,
        turn_id: str | None,
        seq: int | None,
        ttfb_ms: float,
        raise_on_error: bool,
    ) -> int:
        """Spawn stream_cmd_factory, drain stderr asynchronously, and pipe audio chunks."""
        assert self._stream_cmd_factory is not None
        total_bytes = len(first_chunk)
        cmd = [str(part) for part in self._stream_cmd_factory(fmt)]
        proc: asyncio.subprocess.Process | None = None
        stderr_drain: _StderrDrain | None = None
        try:
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
                return total_bytes

            stderr_drain = _StderrDrain(proc.stderr)
            if turn_id is None:
                self._active_once_proc = proc
            else:
                self._active_proc = proc
            if self._is_aborted(turn_id):
                await _terminate_process(proc)
                return total_bytes

            self._is_playing = True
            if turn_id is not None:
                self._cursor.mark_started(turn_id, seq)
                self._fire_first_audio(turn_id, ttfb_ms)

            assert proc.stdin is not None
            try:
                proc.stdin.write(first_chunk)
                await proc.stdin.drain()
                async for chunk in rest_chunks:
                    if self._is_aborted(turn_id):
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
                    async with asyncio.timeout(0.5):
                        await proc.stdin.wait_closed()

            if self._is_aborted(turn_id):
                await _terminate_process(proc)
                return total_bytes

            await proc.wait()
            err = await stderr_drain.finish()
            stderr_drain = None
            if proc.returncode not in (0, None) and not self._is_aborted(turn_id):
                if raise_on_error:
                    raise RuntimeError(f"流式音频播放失败 (exit={proc.returncode}): {err}")
                log.warning("Streaming playback exited non-zero (%s): %s", proc.returncode, err)
            elif proc.returncode == 0 and not self._is_aborted(turn_id) and turn_id is not None:
                self._cursor.mark_completed(turn_id, seq)
            return total_bytes
        except asyncio.CancelledError:
            if proc is not None:
                await _terminate_process(proc)
            raise
        finally:
            self._is_playing = False
            if turn_id is not None:
                self._cursor.mark_stopped(turn_id)
            if proc is not None:
                if self._active_proc is proc:
                    self._active_proc = None
                if self._active_once_proc is proc:
                    self._active_once_proc = None
                if proc.returncode is None:
                    with contextlib.suppress(Exception):
                        await _terminate_process(proc)
            if stderr_drain is not None:
                await stderr_drain.cancel()

    async def _execute_item(self, item: Any) -> None:
        raise NotImplementedError

    def _item_failure_label(self) -> str:
        return "TTS playback failed"

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
                self._discard_item(item)
                if not self._queue:
                    self._idle_event.set()
                continue

            self._active_item = item
            self._active_cancelled = False
            try:
                await self._execute_item(item)
            except asyncio.CancelledError:
                if self._stop_requested or not self._active_cancelled:
                    raise
                continue
            except Exception as exc:
                log.warning(
                    "%s (turn=%s seq=%s): %s",
                    self._item_failure_label(),
                    item.turn_id,
                    item.seq,
                    type(exc).__name__,
                )
            finally:
                self._active_task = None
                self._active_item = None
                self._active_cancelled = False
                if not self._queue:
                    self._idle_event.set()


@dataclass
class _QueuedChunk:
    turn_id: str
    seq: int
    text: str
    synth_task: asyncio.Task[tuple[bytes, str]] | None = None
    enqueued_at: float = field(default_factory=time.perf_counter)


class AfplayAudioPlayer(BaseQueuedAudioPlayer):
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
        super().__init__(
            play_cmd_factory=play_cmd_factory,
            stream_cmd_factory=stream_cmd_factory,
            enable_streaming=enable_streaming,
            on_first_audio=on_first_audio,
            tmpdir_prefix="hakimi_voice_play_",
        )

    @property
    def streaming_active(self) -> bool:
        """Return True when live chunk streaming into ffplay is enabled and supported."""
        return super().streaming_active and callable(getattr(self._tts, "synthesize_stream", None))

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
        self._cursor.ensure_turn(turn_id)
        self._queue.append(_QueuedChunk(turn_id=turn_id, seq=seq, text=text, synth_task=task))
        self._idle_event.clear()
        self._wake_event.set()

    def _discard_item(self, item: Any) -> None:
        if item.synth_task is not None:
            item.synth_task.cancel()

    def _item_failure_label(self) -> str:
        return "TTS chunk failed"

    async def _execute_item(self, item: _QueuedChunk) -> None:
        if item.synth_task is None:
            stream_task = asyncio.create_task(self._play_stream_item(item))
            self._active_task = stream_task
            await stream_task
            return
        audio_bytes, ext = await item.synth_task
        if self._is_aborted(item.turn_id):
            return
        await self._play_bytes_in_tmpdir(audio_bytes, ext, item, file_prefix="chunk")

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
            seq=item.seq,
            enqueued_at=item.enqueued_at,
            raise_on_error=False,
        )

    async def _stream_to_player(
        self,
        text: str,
        *,
        turn_id: str | None,
        seq: int | None = None,
        enqueued_at: float | None = None,
        raise_on_error: bool,
    ) -> tuple[float, float, int, str]:
        synth_stream = getattr(self._tts, "synthesize_stream", None)
        assert callable(synth_stream)
        raw_fmt = str(getattr(self._tts, "response_format", "mp3") or "mp3")
        fmt = _EXT_CLEAN_RE.sub("", raw_fmt.lstrip(".")) or "mp3"
        t0 = enqueued_at if enqueued_at is not None else time.perf_counter()
        gen = synth_stream(text)
        try:
            try:
                first_chunk = await anext(gen)
            except StopAsyncIteration:
                return 0.0, (time.perf_counter() - t0) * 1000.0, 0, fmt
            ttfb_ms = (time.perf_counter() - t0) * 1000.0
            if self._is_aborted(turn_id):
                return ttfb_ms, ttfb_ms, len(first_chunk), fmt
            total_bytes = await self._pipe_to_stream_player(
                first_chunk,
                gen,
                fmt=fmt,
                turn_id=turn_id,
                seq=seq,
                ttfb_ms=ttfb_ms,
                raise_on_error=raise_on_error,
            )
            return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, fmt
        finally:
            with contextlib.suppress(Exception):
                await gen.aclose()


@dataclass
class _RawStreamItem:
    turn_id: str
    seq: int
    fmt: str
    chunks: asyncio.Queue[bytes | None] = field(default_factory=asyncio.Queue)
    enqueued_at: float = field(default_factory=time.perf_counter)
    closed: bool = False


class RawStreamAudioPlayer(BaseQueuedAudioPlayer):
    """Ordered TTS player for live binary audio chunks pushed by VoiceGatewayWsClient."""

    def __init__(
        self,
        *,
        play_cmd_factory: Callable[[Path], Sequence[str]] | None = None,
        stream_cmd_factory: Callable[[str], Sequence[str]] | None = None,
        enable_streaming: bool = True,
        on_first_audio: Callable[[str, float], None] | None = None,
    ) -> None:
        super().__init__(
            play_cmd_factory=play_cmd_factory,
            stream_cmd_factory=stream_cmd_factory,
            enable_streaming=enable_streaming,
            on_first_audio=on_first_audio,
            tmpdir_prefix="hakimi_voice_raw_",
        )
        self._open_streams: dict[str, _RawStreamItem] = {}

    def begin_stream(self, turn_id: str, seq: int = 1, fmt: str = "mp3") -> None:
        """Open a new ordered audio chunk stream for (turn_id, seq)."""
        if self._stop_requested or turn_id in self._cancelled_turns:
            return
        prev = self._open_streams.pop(turn_id, None)
        if prev is not None and not prev.closed:
            prev.closed = True
            prev.chunks.put_nowait(None)
        clean_fmt = (fmt or "mp3").strip().lower() or "mp3"
        item = _RawStreamItem(turn_id=turn_id, seq=seq, fmt=clean_fmt)
        self._cursor.ensure_turn(turn_id)
        self._open_streams[turn_id] = item
        self._queue.append(item)
        self._idle_event.clear()
        self._wake_event.set()

    def push_chunk(self, turn_id: str, chunk: bytes) -> None:
        """Append incoming binary audio bytes to the open stream for turn_id."""
        if self._stop_requested or not chunk or turn_id in self._cancelled_turns:
            return
        item = self._open_streams.get(turn_id)
        if item is None or item.closed:
            return
        item.chunks.put_nowait(bytes(chunk))

    def end_stream(self, turn_id: str, seq: int = 0) -> None:
        """Close the open audio stream for turn_id so playback can finish cleanly."""
        item = self._open_streams.get(turn_id)
        if item is None:
            return
        if seq and item.seq != seq:
            return
        self._open_streams.pop(turn_id, None)
        if not item.closed:
            item.closed = True
            item.chunks.put_nowait(None)

    def _before_cancel_turn(self, turn_id: str | None) -> None:
        for key in list(self._open_streams):
            if turn_id is None or key == turn_id:
                stream_item = self._open_streams.pop(key)
                if not stream_item.closed:
                    stream_item.closed = True
                    stream_item.chunks.put_nowait(None)

    def _discard_item(self, item: Any) -> None:
        if not item.closed:
            item.closed = True
            item.chunks.put_nowait(None)

    def _item_failure_label(self) -> str:
        return "Raw TTS playback failed"

    async def play_stream_once(
        self,
        stream_fn: Any,
        *,
        fmt: str = "mp3",
    ) -> tuple[float, float, int, str]:
        """Play a single incoming audio stream live via ffplay or buffered afplay."""
        t0 = time.perf_counter()
        active_fmt = (fmt or "mp3").strip().lower() or "mp3"
        total_bytes = 0
        ttfb_ms = 0.0
        proc: asyncio.subprocess.Process | None = None
        stderr_drain: _StderrDrain | None = None
        buffered = bytearray()

        async def _consume_chunk(chunk: bytes, chunk_fmt: str | None = None) -> None:
            nonlocal active_fmt, total_bytes, ttfb_ms, proc, stderr_drain
            if chunk_fmt:
                active_fmt = chunk_fmt.strip().lower() or active_fmt
            if not chunk:
                return
            if total_bytes == 0:
                ttfb_ms = (time.perf_counter() - t0) * 1000.0
            total_bytes += len(chunk)

            if self.streaming_active:
                assert self._stream_cmd_factory is not None
                if proc is None:
                    cmd = [str(part) for part in self._stream_cmd_factory(active_fmt)]
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            *cmd,
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.PIPE,
                        )
                    except FileNotFoundError:
                        raise RuntimeError(f"未找到流式播放命令 {cmd[0]!r}。") from None
                    stderr_drain = _StderrDrain(proc.stderr)
                    self._active_once_proc = proc
                    self._is_playing = True
                assert proc.stdin is not None
                try:
                    proc.stdin.write(chunk)
                    await proc.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            else:
                buffered.extend(chunk)

        try:
            if hasattr(stream_fn, "__aiter__"):
                async for piece in stream_fn:
                    if isinstance(piece, tuple):
                        await _consume_chunk(piece[0], str(piece[1]))
                    else:
                        await _consume_chunk(piece)
            elif callable(stream_fn):
                sig = inspect.signature(stream_fn)
                if len(sig.parameters) == 0:
                    res = stream_fn()
                    if hasattr(res, "__aiter__"):
                        async for piece in res:
                            if isinstance(piece, tuple):
                                await _consume_chunk(piece[0], str(piece[1]))
                            else:
                                await _consume_chunk(piece)
                    elif inspect.isawaitable(res):
                        val = await res
                        if isinstance(val, tuple) and len(val) >= 2:
                            await _consume_chunk(val[0], str(val[1]))
                        elif isinstance(val, (bytes, bytearray)):
                            await _consume_chunk(bytes(val))
                else:
                    res = stream_fn(_consume_chunk)
                    if inspect.isawaitable(res):
                        val = await res
                        if isinstance(val, dict) and val.get("format"):
                            active_fmt = str(val["format"]).strip().lower() or active_fmt
                        elif isinstance(val, tuple) and len(val) >= 2 and isinstance(val[1], str):
                            active_fmt = val[1].strip().lower() or active_fmt
                        elif isinstance(val, str) and val.strip():
                            active_fmt = val.strip().lower()
            else:
                raise TypeError("stream_fn must be an async iterable or callable")

            if proc is not None:
                if proc.stdin is not None:
                    with contextlib.suppress(Exception):
                        proc.stdin.close()
                        async with asyncio.timeout(0.5):
                            await proc.stdin.wait_closed()
                await proc.wait()
                err = ""
                if stderr_drain is not None:
                    err = await stderr_drain.finish()
                    stderr_drain = None
                if proc.returncode not in (0, None):
                    raise RuntimeError(f"流式音频播放失败 (exit={proc.returncode}): {err}")
            elif buffered:
                await self.play_once(bytes(buffered), active_fmt)

            return ttfb_ms, (time.perf_counter() - t0) * 1000.0, total_bytes, active_fmt
        except asyncio.CancelledError:
            if proc is not None:
                await _terminate_process(proc)
            raise
        finally:
            self._is_playing = False
            if proc is not None:
                if self._active_once_proc is proc:
                    self._active_once_proc = None
                if proc.returncode is None:
                    with contextlib.suppress(Exception):
                        await _terminate_process(proc)
            if stderr_drain is not None:
                await stderr_drain.cancel()

    async def _iter_raw_chunks(self, item: _RawStreamItem) -> AsyncIterator[bytes]:
        while True:
            chunk = await item.chunks.get()
            if chunk is None or self._is_aborted(item.turn_id):
                return
            if chunk:
                yield chunk

    async def _play_raw_item(self, item: _RawStreamItem) -> None:
        if not self.streaming_active:
            buf = bytearray()
            while True:
                chunk = await item.chunks.get()
                if chunk is None or self._is_aborted(item.turn_id):
                    break
                buf.extend(chunk)
            if not buf or self._is_aborted(item.turn_id):
                return
            await self._play_bytes_in_tmpdir(bytes(buf), item.fmt, item, file_prefix="raw")
            return

        first_chunk = b""
        while True:
            chunk = await item.chunks.get()
            if chunk is None or self._is_aborted(item.turn_id):
                return
            if chunk:
                first_chunk = chunk
                break

        ttfb_ms = (time.perf_counter() - item.enqueued_at) * 1000.0
        await self._pipe_to_stream_player(
            first_chunk,
            self._iter_raw_chunks(item),
            fmt=item.fmt,
            turn_id=item.turn_id,
            seq=item.seq,
            ttfb_ms=ttfb_ms,
            raise_on_error=False,
        )

    async def _execute_item(self, item: _RawStreamItem) -> None:
        play_task = asyncio.create_task(self._play_raw_item(item))
        self._active_task = play_task
        await play_task
