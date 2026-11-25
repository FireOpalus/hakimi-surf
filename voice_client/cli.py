"""CLI and interactive dual-mode (VAD / PTT) loop for the Hakimi voice companion."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import inspect
import os
import signal
import sys
import termios
import time
import tty
import uuid
from pathlib import Path
from typing import Any

from .audio import (
    AfplayAudioPlayer,
    EnergyVadDetector,
    FfmpegMicCapture,
    RawStreamAudioPlayer,
    list_avfoundation_audio_devices,
    pcm16_to_wav_bytes,
)
from .companion_ws import CompanionWsClient
from .config import DEFAULT_ENV_FILE, VoiceClientConfig, load_config
from .gateway_ws import VoiceGatewayError, VoiceGatewayWsClient
from .providers import (
    ProviderError,
    SiliconFlowTTS,
    build_stt_provider,
    build_tts_provider,
)


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser for `python -m voice_client`."""
    parser = argparse.ArgumentParser(
        prog="hakimi-voice",
        description="哈基米 Mac 实时语音伴随客户端 (Voice Gateway / /ws/companion)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help=f".env 配置文件路径 (默认: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="忽略 VOICE_GATEWAY_URL，强制直连 /ws/companion 与本地 STT/TTS",
    )
    parser.add_argument(
        "--mode",
        choices=("vad", "ptt"),
        default=None,
        help="覆盖语音模式: vad (自动能量检测) 或 ptt (空格按键说话)",
    )
    parser.add_argument(
        "--game",
        default=None,
        help="覆盖当前游玩游戏标题 (会发送至服务端 session.start)",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="覆盖 AVFoundation 音频输入设备序号或名称",
    )
    parser.add_argument(
        "--tts-buffer-mode",
        choices=("turn", "chunk"),
        default=None,
        help="覆盖 TTS 缓冲策略: turn (整轮说完一次合成, 默认) 或 chunk (逐句切片合成)",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--list-devices",
        action="store_true",
        help="列出本机 AVFoundation 音频输入设备并退出",
    )
    group.add_argument(
        "--check-ws",
        action="store_true",
        help="测试 /ws/companion 鉴权握手与 ping RTT 后退出 (零 LLM 消耗)",
    )
    group.add_argument(
        "--test-tts",
        metavar="TEXT",
        default=None,
        help="使用配置的 TTS 合成并播放测试文本",
    )
    group.add_argument(
        "--test-stt",
        action="store_true",
        help="录制 4 秒麦克风音频并测试 STT 识别",
    )
    group.add_argument(
        "--upload-voice",
        metavar="AUDIO_PATH",
        default=None,
        help="上传参考音频到 SiliconFlow 自定义音色并输出 speech:... URI",
    )

    parser.add_argument(
        "--voice-name",
        default=None,
        help="配合 --upload-voice 使用的音色名称",
    )
    parser.add_argument(
        "--ref-text",
        default=None,
        help="配合 --upload-voice 使用的参考音频对应文本",
    )
    return parser


def run_list_devices() -> int:
    """Print available AVFoundation audio input devices."""
    devices = list_avfoundation_audio_devices()
    if not devices:
        print("未检测到可用的 AVFoundation 音频输入设备。")
        return 0
    print("可用 AVFoundation 音频输入设备:")
    for dev_id, name in devices:
        print(f"  [{dev_id}] {name}")
    return 0


async def check_ws(
    config: VoiceClientConfig,
    *,
    connect_factory: Any = None,
) -> int:
    """Verify gateway or /ws/companion handshake, ping RTT, and clean teardown."""
    if config.use_gateway:
        if connect_factory is None:
            config.require_gateway()
        gw_client = VoiceGatewayWsClient(config, connect_factory=connect_factory)
        await gw_client.connect()
        try:
            ready = await gw_client.start_session()
            pong = await gw_client.ping_info(timeout=10.0)
            ended = await gw_client.stop_session(timeout=5.0)
            mode = ready.get("mode", config.session_mode)
            sid = str(ready.get("session_id") or ended.get("session_id") or "")[:8]
            rtt_ms = float(pong.get("rtt_ms") or 0.0)
            upstream_val = pong.get("upstream_ms")
            upstream_str = f"{float(upstream_val):.1f}" if upstream_val is not None else "-"
            stt_name = str(ready.get("stt_provider") or config.stt_provider or "-")
            tts_name = str(ready.get("tts_provider") or config.tts_provider or "-")
            tts_model = str(ready.get("tts_model") or ready.get("output_format") or "-")
            print(
                f"✅ 语音网关握手正常: mode={mode} session={sid or '-'} "
                f"rtt={rtt_ms:.1f}ms (upstream={upstream_str}ms, "
                f"stt={stt_name}, tts={tts_name}/{tts_model})"
            )
            return 0
        finally:
            await gw_client.close()

    if connect_factory is None:
        config.require_ws()
    client = CompanionWsClient(config, connect_factory=connect_factory)
    await client.connect()
    try:
        ready = await client.start_session()
        rtt_ms = await client.ping(timeout=10.0)
        ended = await client.stop_session(timeout=5.0)
        mode = ready.get("mode", config.session_mode)
        sid = str(ready.get("session_id") or ended.get("session_id") or "")[:8]
        print(f"✅ WS 握手正常: mode={mode} session={sid or '-'} rtt={rtt_ms:.1f}ms")
        return 0
    finally:
        await client.close()


async def test_tts(
    config: VoiceClientConfig,
    text: str,
    *,
    connect_factory: Any = None,
) -> int:
    """Synthesize and play one test phrase via gateway or local TTS provider."""
    if config.use_gateway:
        if connect_factory is None:
            config.require_gateway()
        gw_client = VoiceGatewayWsClient(config, connect_factory=connect_factory)
        try:
            player = RawStreamAudioPlayer(enable_streaming=config.tts_streaming)
        except TypeError:
            player = RawStreamAudioPlayer()
        await gw_client.connect()
        try:
            ttfb_ms, total_ms, total_bytes, fmt = await player.play_stream_once(
                lambda on_chunk: gw_client.synthesize_once(
                    text,
                    output_format=config.gateway_output_format or None,
                    on_chunk=on_chunk,
                )
            )
            mode = "流式ffplay" if getattr(player, "streaming_active", False) else "afplay"
            print(
                f"✅ TTS 播放完成 ({mode}, 首字节={ttfb_ms:.0f}ms, "
                f"总计={total_ms:.0f}ms, {total_bytes} bytes, {fmt})"
            )
            return 0
        finally:
            await player.stop()
            await gw_client.close()

    config.require_tts()
    tts = build_tts_provider(config)
    try:
        player = AfplayAudioPlayer(tts, enable_streaming=config.tts_streaming)
    except TypeError:
        player = AfplayAudioPlayer(tts)
    try:
        if callable(getattr(player, "play_stream_once", None)):
            ttfb_ms, total_ms, total_bytes, fmt = await player.play_stream_once(text)
            mode = "流式ffplay" if getattr(player, "streaming_active", False) else "afplay"
            print(
                f"✅ TTS 播放完成 ({mode}, 首字节={ttfb_ms:.0f}ms, "
                f"总计={total_ms:.0f}ms, {total_bytes} bytes, {fmt})"
            )
        else:
            audio_bytes, fmt = await tts.synthesize(text)
            await player.play_once(audio_bytes, fmt)
            print(f"✅ TTS 合成并播放完成 ({len(audio_bytes)} bytes, {fmt})")
        return 0
    finally:
        await player.stop()
        await tts.aclose()


async def _capture_mic_wav(config: VoiceClientConfig, duration_seconds: float) -> bytes | None:
    mic = FfmpegMicCapture(
        device=config.audio_input_device,
        sample_rate=config.sample_rate,
    )
    try:
        await mic.start()
        print(f"🎙️ 开始录音 {duration_seconds:.0f} 秒，请说话...", flush=True)
        target_bytes = max(1, int(config.sample_rate * 2 * duration_seconds))
        chunks: list[bytes] = []
        collected = 0
        async with asyncio.timeout(duration_seconds + 5.0):
            while collected < target_bytes:
                frame = await mic.read_frame()
                if not frame:
                    break
                chunks.append(frame)
                collected += len(frame)
        pcm = b"".join(chunks)[:target_bytes]
        if not pcm:
            print("⚠️ 未采集到麦克风音频。")
            return None
        return pcm16_to_wav_bytes(pcm, sample_rate=config.sample_rate)
    finally:
        await mic.stop()


async def test_stt(
    config: VoiceClientConfig,
    *,
    duration_seconds: float = 4.0,
    connect_factory: Any = None,
) -> int:
    """Capture a short microphone clip and print STT transcription."""
    if config.use_gateway:
        if connect_factory is None:
            config.require_gateway()
        wav = await _capture_mic_wav(config, duration_seconds)
        if wav is None:
            return 1
        gw_client = VoiceGatewayWsClient(config, connect_factory=connect_factory)
        await gw_client.connect()
        try:
            t0 = time.perf_counter()
            text = (
                await gw_client.transcribe_once(
                    wav,
                    fmt="wav",
                    sample_rate=config.sample_rate,
                    channels=1,
                )
            ).strip()
            stt_ms = (time.perf_counter() - t0) * 1000.0
            print(f"📝 识别文本 ({stt_ms:.0f}ms): {text or '(未识别到有效语音)'}")
            return 0
        finally:
            await gw_client.close()

    config.require_stt()
    stt = build_stt_provider(config)
    try:
        wav = await _capture_mic_wav(config, duration_seconds)
        if wav is None:
            return 1
        t0 = time.perf_counter()
        text = (await stt.transcribe(wav)).strip()
        stt_ms = (time.perf_counter() - t0) * 1000.0
        print(f"📝 识别文本 ({stt_ms:.0f}ms): {text or '(未识别到有效语音)'}")
        return 0
    finally:
        await stt.aclose()


async def upload_voice(
    config: VoiceClientConfig,
    audio_path: Path,
    voice_name: str,
    ref_text: str,
) -> int:
    """Upload a custom reference voice clip to SiliconFlow and print its URI."""
    if not config.siliconflow_api_key.strip():
        raise ValueError("上传自定义音色需要配置 SILICONFLOW_API_KEY。")
    if not await asyncio.to_thread(audio_path.is_file):
        raise ValueError(f"参考音频文件不存在: {audio_path}")
    sf_config = dataclasses.replace(config, tts_provider="siliconflow")
    tts = build_tts_provider(sf_config)
    if not isinstance(tts, SiliconFlowTTS):
        raise RuntimeError("SiliconFlowTTS provider unavailable")
    try:
        uri = await tts.upload_custom_voice(audio_path, voice_name, ref_text)
        print(f"✅ 上传成功! 请将以下 URI 填入 SILICONFLOW_TTS_VOICE:\n{uri}")
        return 0
    finally:
        await tts.aclose()


def _join_turn_chunks(parts: list[str]) -> str:
    """Join streamed chunks cleanly while avoiding extra spaces between Chinese chars."""
    out: list[str] = []
    for raw in parts:
        piece = str(raw or "").strip()
        if not piece:
            continue
        if out:
            prev = out[-1][-1]
            curr = piece[0]
            if prev.isascii() and prev.isalnum() and curr.isascii() and curr.isalnum():
                out.append(" ")
        out.append(piece)
    return "".join(out)


def _print_banner(
    config: VoiceClientConfig,
    ready: dict[str, Any],
    voice_mode: str,
    *,
    gateway_mode: bool = False,
) -> None:
    mode_label = ready.get("mode", config.session_mode)
    game_label = config.game_title or "日常连麦"
    buf_mode = str(ready.get("tts_buffer_mode") or config.tts_buffer_mode)
    buf_label = "整轮合播(不切片)" if buf_mode == "turn" else "逐句流式"
    stream_label = "流式ffplay" if config.tts_streaming else "afplay"
    stt_label = str(ready.get("stt_provider") or config.stt_provider)
    tts_label = str(ready.get("tts_provider") or config.tts_provider)
    conn_label = "Voice Gateway" if gateway_mode else "直连 /ws/companion"
    print("=" * 64)
    print(f"🐱 哈基米实时语音伴随客户端已连接 ({conn_label})")
    print(
        f"会话模式: {mode_label} | 拾音: {voice_mode.upper()} | "
        f"STT: {stt_label} | TTS: {tts_label}/{buf_label}/{stream_label} | "
        f"设备: [{config.audio_input_device}] | 场景: {game_label}"
    )
    print("操作说明:")
    print("  [Space] (PTT 模式) 开始/停止录音并发送")
    print("  [m]     切换 VAD 自动检测 <-> PTT 按键说话")
    print("  [x]     立即打断/取消当前回复与播放")
    print("  [t]     输入一行文字消息发送")
    print("  [q]     优雅退出并结束连麦")
    print("=" * 64, flush=True)


async def run_interactive_session(
    config: VoiceClientConfig,
    *,
    connect_factory: Any = None,
    stt_provider: Any = None,
    tts_provider: Any = None,
    mic_capture: Any = None,
    audio_player: Any = None,
) -> int:
    """Run the interactive dual-mode (VAD/PTT) voice companion session."""
    use_gateway = config.use_gateway and stt_provider is None and tts_provider is None
    if use_gateway:
        if connect_factory is None:
            config.require_gateway()
        stt = None
        tts = None
        owns_stt = False
        owns_tts = False
    else:
        if connect_factory is None:
            config.require_ws()
        if stt_provider is None:
            config.require_stt()
        if tts_provider is None:
            config.require_tts()
        stt = stt_provider or build_stt_provider(config)
        tts = tts_provider or build_tts_provider(config)
        owns_stt = stt_provider is None
        owns_tts = tts_provider is None

    turn_metrics: dict[str, dict[str, float]] = {}

    def _format_timing_line(
        *,
        stt_ms: float | None = None,
        llm_ms: float | None = None,
        tts_ms: float | None = None,
    ) -> str:
        parts: list[str] = []
        if stt_ms is not None:
            parts.append(f"STT={stt_ms:.0f}ms")
        if llm_ms is not None:
            parts.append(f"LLM={llm_ms:.0f}ms")
        if tts_ms is not None:
            parts.append(f"TTS首音={tts_ms:.0f}ms")
        return f"⏱️ 耗时: {' | '.join(parts)}"

    def _on_first_audio(turn_id: str, tts_ttfa_ms: float) -> None:
        if use_gateway:
            entry = turn_metrics.get(turn_id)
            if entry is not None:
                entry["tts_ttfa_ms"] = tts_ttfa_ms
            return
        info = turn_metrics.pop(turn_id, None)
        if not info:
            return
        llm_ms = info.get("llm_ms")
        if llm_ms is None and "sent_at" in info:
            llm_ms = (time.perf_counter() - info["sent_at"]) * 1000.0
        print(
            _format_timing_line(
                stt_ms=info.get("stt_ms"),
                llm_ms=llm_ms,
                tts_ms=tts_ttfa_ms,
            ),
            flush=True,
        )

    def _record_turn_metric(turn_id: str, *, stt_ms: float | None = None) -> None:
        if len(turn_metrics) >= 32:
            oldest = next(iter(turn_metrics))
            turn_metrics.pop(oldest, None)
        entry: dict[str, float] = {"sent_at": time.perf_counter()}
        if stt_ms is not None:
            entry["stt_ms"] = stt_ms
        turn_metrics[turn_id] = entry

    if use_gateway:
        player: Any = audio_player or RawStreamAudioPlayer(
            enable_streaming=config.tts_streaming,
            on_first_audio=_on_first_audio,
        )
    else:
        player = audio_player or AfplayAudioPlayer(
            tts,
            enable_streaming=config.tts_streaming,
            on_first_audio=_on_first_audio,
        )
    mic = mic_capture or FfmpegMicCapture(
        device=config.audio_input_device,
        sample_rate=config.sample_rate,
    )
    vad = EnergyVadDetector(
        sample_rate=config.sample_rate,
        threshold=config.vad_threshold,
        silence_ms=config.vad_silence_ms,
        min_speech_ms=config.vad_min_speech_ms,
        max_speech_ms=config.vad_max_speech_ms,
        barge_in_multiplier=config.vad_barge_in_multiplier,
    )

    stop_event = asyncio.Event()
    voice_mode: str = config.voice_mode
    ptt_recording = False
    ptt_buffer = bytearray()
    max_ptt_bytes = max(1, config.sample_rate * 2 * 60)
    bg_tasks: set[asyncio.Task[None]] = set()
    turn_buffers: dict[str, list[str]] = {}
    loop = asyncio.get_running_loop()

    def spawn_task(coro: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        bg_tasks.add(task)

        def _done(done_task: asyncio.Task[None]) -> None:
            bg_tasks.discard(done_task)
            if not done_task.cancelled():
                exc = done_task.exception()
                if exc is not None:
                    print(f"⚠️ 后台任务异常: {exc}", flush=True)

        task.add_done_callback(_done)
        return task

    async def on_disconnect() -> None:
        if not stop_event.is_set():
            print("\n🔌 与哈基米服务端连接已断开。", flush=True)
            stop_event.set()

    async def on_error(frame: dict[str, Any]) -> None:
        reason = str(frame.get("detail") or frame.get("reason") or "unknown")
        print(f"⚠️ 服务端错误: {reason}", flush=True)

    ws: Any
    if use_gateway:

        async def gw_on_stt_final(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            text = str(frame.get("text") or "").strip()
            if not text:
                return
            raw_stt_ms = frame.get("stt_ms")
            if raw_stt_ms is not None:
                stt_val = float(raw_stt_ms)
                if tid in turn_metrics:
                    turn_metrics[tid]["stt_ms"] = stt_val
                print(f"🎤 你: {text}  (STT {stt_val:.0f}ms)", flush=True)
            else:
                print(f"🎤 你: {text}", flush=True)

        async def gw_on_text_chunk(frame: dict[str, Any]) -> None:
            text = str(frame.get("text") or "").strip()
            if text:
                print(f"🐱 哈基米: {text}", flush=True)

        async def gw_on_tts_start(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            seq = int(frame.get("seq") or 0)
            fmt = str(frame.get("format") or config.gateway_output_format or "mp3")
            res = player.begin_stream(tid, seq, fmt)
            if asyncio.iscoroutine(res):
                await res

        async def gw_on_tts_chunk(turn_id: str, seq: int, fmt: str, chunk: bytes) -> None:
            _ = seq, fmt
            res = player.push_chunk(turn_id, chunk)
            if asyncio.iscoroutine(res):
                await res

        async def gw_on_tts_end(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            seq = int(frame.get("seq") or 0)
            res = player.end_stream(tid, seq)
            if asyncio.iscoroutine(res):
                await res

        async def gw_on_turn_end(frame: dict[str, Any]) -> None:
            status = str(frame.get("status") or "").lower()
            tid = str(frame.get("turn_id") or "")
            if status == "cancelled":
                turn_metrics.pop(tid, None)
                await player.cancel_turn(tid or None)
                return
            if status == "error":
                turn_metrics.pop(tid, None)
                reason = str(frame.get("reason") or "unknown")
                print(f"⚠️ 本轮回复异常: {reason}", flush=True)
                return
            if status in {"silent", "skipped"}:
                turn_metrics.pop(tid, None)
                return
            info = turn_metrics.pop(tid, {})
            metrics = frame.get("metrics") if isinstance(frame.get("metrics"), dict) else {}
            stt_ms = metrics.get("stt_ms") if "stt_ms" in metrics else info.get("stt_ms")
            llm_ms = metrics.get("llm_ms") if "llm_ms" in metrics else info.get("llm_ms")
            tts_ms = info.get("tts_ttfa_ms")
            if tts_ms is None and "tts_ttfb_ms" in metrics:
                tts_ms = metrics.get("tts_ttfb_ms")
            if stt_ms is not None or llm_ms is not None or tts_ms is not None:
                print(
                    _format_timing_line(
                        stt_ms=float(stt_ms) if stt_ms is not None else None,
                        llm_ms=float(llm_ms) if llm_ms is not None else None,
                        tts_ms=float(tts_ms) if tts_ms is not None else None,
                    ),
                    flush=True,
                )

        ws = VoiceGatewayWsClient(
            config,
            connect_factory=connect_factory,
            on_stt_final=gw_on_stt_final,
            on_text_chunk=gw_on_text_chunk,
            on_tts_start=gw_on_tts_start,
            on_tts_chunk=gw_on_tts_chunk,
            on_tts_end=gw_on_tts_end,
            on_turn_end=gw_on_turn_end,
            on_error=on_error,
            on_disconnect=on_disconnect,
        )

        async def send_text_turn(text: str, *, stt_ms: float | None = None) -> None:
            _ = stt_ms
            tid = f"t-{uuid.uuid4().hex[:12]}"
            _record_turn_metric(tid)
            await ws.send_text_turn(text, turn_id=tid)

        async def submit_pcm(pcm_bytes: bytes) -> None:
            if not pcm_bytes:
                return
            wav = pcm16_to_wav_bytes(pcm_bytes, sample_rate=config.sample_rate)
            tid = f"t-{uuid.uuid4().hex[:12]}"
            _record_turn_metric(tid)
            try:
                await ws.send_audio_turn(
                    wav,
                    fmt="wav",
                    sample_rate=config.sample_rate,
                    channels=1,
                    turn_id=tid,
                )
            except VoiceGatewayError as exc:
                turn_metrics.pop(tid, None)
                print(f"⚠️ 语音发送失败: {exc}", flush=True)

    else:

        async def on_turn_start(frame: dict[str, Any]) -> None:
            if config.tts_buffer_mode == "turn":
                turn_buffers[str(frame.get("turn_id") or "")] = []

        async def on_speak_chunk(frame: dict[str, Any]) -> None:
            turn_id = str(frame.get("turn_id") or "")
            seq = int(frame.get("seq") or 0)
            text = str(frame.get("text") or "").strip()
            if not text:
                return
            print(f"🐱 哈基米: {text}", flush=True)
            if config.tts_buffer_mode == "turn":
                turn_buffers.setdefault(turn_id, []).append(text)
                return
            res = player.enqueue_chunk(turn_id, seq, text)
            if asyncio.iscoroutine(res):
                await res

        async def on_turn_end(frame: dict[str, Any]) -> None:
            status = str(frame.get("status") or "").lower()
            turn_id = str(frame.get("turn_id") or "")
            parts = turn_buffers.pop(turn_id, [])
            if status == "cancelled":
                turn_metrics.pop(turn_id, None)
                await player.cancel_turn(turn_id or None)
            elif status == "error":
                turn_metrics.pop(turn_id, None)
                reason = str(frame.get("reason") or "unknown")
                print(f"⚠️ 本轮回复异常: {reason}", flush=True)
            elif status in {"silent", "skipped"}:
                turn_metrics.pop(turn_id, None)
            else:
                metric = turn_metrics.get(turn_id)
                if metric is not None and "sent_at" in metric:
                    metric["llm_ms"] = (time.perf_counter() - metric["sent_at"]) * 1000.0
                if config.tts_buffer_mode == "turn" and parts:
                    full_text = _join_turn_chunks(parts)
                    if full_text:
                        res = player.enqueue_chunk(turn_id, 1, full_text)
                        if asyncio.iscoroutine(res):
                            await res

        ws = CompanionWsClient(
            config,
            connect_factory=connect_factory,
            on_turn_start=on_turn_start,
            on_speak_chunk=on_speak_chunk,
            on_turn_end=on_turn_end,
            on_error=on_error,
            on_disconnect=on_disconnect,
        )

        async def send_text_turn(text: str, *, stt_ms: float | None = None) -> None:
            tid = f"t-{uuid.uuid4().hex[:12]}"
            _record_turn_metric(tid, stt_ms=stt_ms)
            await ws.send_speech_turn(text, turn_id=tid)

        async def submit_pcm(pcm_bytes: bytes) -> None:
            if not pcm_bytes:
                return
            assert stt is not None
            wav = pcm16_to_wav_bytes(pcm_bytes, sample_rate=config.sample_rate)
            t0 = time.perf_counter()
            try:
                text = (await stt.transcribe(wav)).strip()
            except ProviderError as exc:
                print(f"⚠️ 语音识别失败: {exc}", flush=True)
                return
            stt_ms = (time.perf_counter() - t0) * 1000.0
            if not text:
                return
            print(f"🎤 你: {text}  (STT {stt_ms:.0f}ms)", flush=True)
            await send_text_turn(text, stt_ms=stt_ms)

    def _supports_cancel_cursor(fn: Any) -> bool:
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            return False
        return "played_seq" in sig.parameters or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )

    async def interrupt_now() -> None:
        cursor: dict[str, Any] = {}
        if callable(getattr(player, "playback_cursor", None)):
            cursor = player.playback_cursor() or {}
        target_tid = str(cursor.get("turn_id") or ws.active_turn_id or "")
        turn_buffers.clear()
        turn_metrics.clear()
        await player.cancel_turn()
        if target_tid and _supports_cancel_cursor(ws.cancel_turn):
            await ws.cancel_turn(
                target_tid,
                played_seq=cursor.get("played_seq"),
                played_ms=cursor.get("played_ms"),
            )
        else:
            await ws.cancel_turn()

    def toggle_mode() -> None:
        nonlocal voice_mode, ptt_recording
        ptt_recording = False
        ptt_buffer.clear()
        vad.reset()
        voice_mode = "ptt" if voice_mode == "vad" else "vad"
        label = "PTT 按键说话 ([Space] 开始/发送)" if voice_mode == "ptt" else "VAD 自动检测"
        print(f"🔄 已切换拾音模式 -> {label}", flush=True)

    def handle_space() -> None:
        nonlocal ptt_recording
        if voice_mode != "ptt":
            print("ℹ️ 当前为 VAD 模式，按 [m] 可切换为 PTT 模式。", flush=True)
            return
        if not ptt_recording:
            ptt_buffer.clear()
            ptt_recording = True
            print("🎙️ PTT 录音中... 再按 [Space] 停止并发送", flush=True)
            return
        ptt_recording = False
        pcm = bytes(ptt_buffer)
        ptt_buffer.clear()
        if pcm:
            print("⏹️ 录音结束，正在识别...", flush=True)
            spawn_task(submit_pcm(pcm))
        else:
            print("ℹ️ 录音为空，未发送。", flush=True)

    async def mic_loop() -> None:
        nonlocal ptt_recording
        try:
            while not stop_event.is_set():
                frame = await mic.read_frame()
                if not frame:
                    stop_event.set()
                    break
                if voice_mode == "vad":
                    barge_in, completed_pcm = vad.feed(frame, playback_active=player.is_playing)
                    if barge_in and config.barge_in_enabled:
                        if player.is_playing or ws.active_turn_id:
                            print("⚡ 检测到插话，已打断当前回复", flush=True)
                            spawn_task(interrupt_now())
                    if completed_pcm:
                        spawn_task(submit_pcm(completed_pcm))
                elif ptt_recording:
                    ptt_buffer.extend(frame)
                    if len(ptt_buffer) >= max_ptt_bytes:
                        ptt_recording = False
                        pcm = bytes(ptt_buffer)
                        ptt_buffer.clear()
                        print("⏹️ PTT 达到最长时长上限，自动发送中...", flush=True)
                        spawn_task(submit_pcm(pcm))
        except asyncio.CancelledError:
            raise

    stdin_fd: int = -1
    is_tty = False
    try:
        stdin_fd = sys.stdin.fileno()
        is_tty = os.isatty(stdin_fd)
    except Exception:
        stdin_fd = -1
        is_tty = False

    old_termios: list[Any] | None = None
    reader_registered = False
    text_input_lock = asyncio.Lock()

    async def handle_tty_text_input() -> None:
        nonlocal reader_registered
        if text_input_lock.locked() or old_termios is None:
            return
        async with text_input_lock:
            if reader_registered:
                loop.remove_reader(stdin_fd)
                reader_registered = False
            termios.tcsetattr(stdin_fd, termios.TCSADRAIN, old_termios)
            try:
                line = await asyncio.to_thread(input, "\n💬 输入文字 (回车发送，留空取消): ")
            except (EOFError, KeyboardInterrupt):
                line = ""
            finally:
                if not stop_event.is_set():
                    tty.setcbreak(stdin_fd)
                    loop.add_reader(stdin_fd, on_tty_stdin_ready)
                    reader_registered = True
            clean = line.strip()
            if clean and not stop_event.is_set():
                print(f"💬 你: {clean}", flush=True)
                await send_text_turn(clean)

    def on_tty_stdin_ready() -> None:
        try:
            raw = os.read(stdin_fd, 64)
        except OSError:
            stop_event.set()
            return
        if not raw:
            stop_event.set()
            return
        chars = raw.decode("utf-8", errors="ignore")
        if chars.startswith("\x1b"):
            return
        for ch in chars:
            if ch in ("q", "Q", "\x03", "\x04"):
                stop_event.set()
                return
            if ch in ("m", "M"):
                toggle_mode()
            elif ch in ("x", "X"):
                print("⏹️ 已手动打断当前回复", flush=True)
                spawn_task(interrupt_now())
            elif ch == " ":
                handle_space()
            elif ch in ("t", "T"):
                spawn_task(handle_tty_text_input())
                return

    async def non_tty_stdin_loop() -> None:
        had_commands = False
        try:
            while not stop_event.is_set():
                line = await asyncio.to_thread(sys.stdin.readline)
                if line == "":
                    if had_commands:
                        if bg_tasks:
                            await asyncio.gather(*list(bg_tasks), return_exceptions=True)
                        stop_event.set()
                    break
                raw_cmd = line.rstrip("\r\n")
                stripped = raw_cmd.strip()
                lower = stripped.lower()
                if not stripped:
                    if raw_cmd == " ":
                        had_commands = True
                        handle_space()
                    continue
                had_commands = True
                if lower in ("q", "quit", "exit", "/quit"):
                    stop_event.set()
                    break
                if lower in ("m", "/mode"):
                    toggle_mode()
                elif lower in ("x", "/cancel"):
                    print("⏹️ 已手动打断当前回复", flush=True)
                    spawn_task(interrupt_now())
                elif lower == "t":
                    next_line = await asyncio.to_thread(sys.stdin.readline)
                    if next_line == "":
                        stop_event.set()
                        break
                    msg = next_line.strip()
                    if msg:
                        print(f"💬 你: {msg}", flush=True)
                        await send_text_turn(msg)
                else:
                    msg = stripped[2:].strip() if stripped.startswith("t ") else stripped
                    if msg:
                        print(f"💬 你: {msg}", flush=True)
                        await send_text_turn(msg)
        except asyncio.CancelledError:
            raise

    installed_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    mic_task: asyncio.Task[None] | None = None
    stdin_task: asyncio.Task[None] | None = None

    try:
        await ws.connect()
        ready = await ws.start_session()
        await player.start()
        await mic.start()
        _print_banner(config, ready, voice_mode, gateway_mode=use_gateway)

        mic_task = asyncio.create_task(mic_loop(), name="voice-mic-loop")
        if is_tty and stdin_fd >= 0:
            old_termios = termios.tcgetattr(stdin_fd)
            tty.setcbreak(stdin_fd)
            loop.add_reader(stdin_fd, on_tty_stdin_ready)
            reader_registered = True
        else:
            stdin_task = asyncio.create_task(non_tty_stdin_loop(), name="voice-stdin-loop")

        await stop_event.wait()
        return 0
    finally:
        stop_event.set()
        if reader_registered and stdin_fd >= 0:
            with contextlib.suppress(Exception):
                loop.remove_reader(stdin_fd)
        if old_termios is not None and stdin_fd >= 0:
            with contextlib.suppress(Exception):
                termios.tcsetattr(stdin_fd, termios.TCSADRAIN, old_termios)
        for sig in installed_signals:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)

        for task in (mic_task, stdin_task, *list(bg_tasks)):
            if task is not None and not task.done():
                task.cancel()
        pending = [t for t in (mic_task, stdin_task, *list(bg_tasks)) if t is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        with contextlib.suppress(Exception):
            await mic.stop()
        with contextlib.suppress(Exception):
            await player.cancel_turn()
        with contextlib.suppress(Exception):
            await player.stop()
        with contextlib.suppress(Exception):
            if ws.connected:
                await ws.stop_session(timeout=3.0)
        with contextlib.suppress(Exception):
            await ws.close()
        if owns_stt and hasattr(stt, "aclose"):
            with contextlib.suppress(Exception):
                await stt.aclose()
        if owns_tts and hasattr(tts, "aclose"):
            with contextlib.suppress(Exception):
                await tts.aclose()
        print("\n👋 已退出哈基米语音伴随客户端。", flush=True)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for `python -m voice_client`."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.upload_voice and (not args.voice_name or not args.ref_text):
        parser.error("--upload-voice 需要同时指定 --voice-name 和 --ref-text")

    if args.list_devices:
        return run_list_devices()

    try:
        config = load_config(env_file=args.env_file)
        updates: dict[str, Any] = {}
        if args.mode is not None:
            updates["voice_mode"] = args.mode
        if args.game is not None:
            updates["game_title"] = args.game
        if args.device is not None:
            updates["audio_input_device"] = args.device
        if args.tts_buffer_mode is not None:
            updates["tts_buffer_mode"] = args.tts_buffer_mode
        if args.direct:
            updates["gateway_url"] = ""
        if updates:
            config = dataclasses.replace(config, **updates)

        if args.check_ws:
            return asyncio.run(check_ws(config))
        if args.test_tts is not None:
            return asyncio.run(test_tts(config, args.test_tts))
        if args.test_stt:
            return asyncio.run(test_stt(config))
        if args.upload_voice is not None:
            return asyncio.run(
                upload_voice(
                    config,
                    Path(args.upload_voice),
                    args.voice_name,
                    args.ref_text,
                )
            )
        return asyncio.run(run_interactive_session(config))
    except KeyboardInterrupt:
        return 0
    except (ValueError, ProviderError, VoiceGatewayError, RuntimeError, OSError) as exc:
        print(f"❌ 错误: {exc}", file=sys.stderr)
        return 1
