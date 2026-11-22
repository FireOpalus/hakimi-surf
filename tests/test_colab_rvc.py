"""Tests for KR-to-Colab RVC training, cover generation, and SongLibrary upload helpers."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import threading
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, rel_path: str):
    path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


colab_remote = _load_script("colab_remote_rvc", "scripts/colab_remote_rvc.py")
colab_rvc = _load_script("colab_rvc", "scripts/colab_rvc.py")


def _make_wav_bytes(duration_sec: float = 5.0, sample_rate: int = 24000) -> bytes:
    buf = io.BytesIO()
    nframes = int(duration_sec * sample_rate)
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(b"\x00\x00" * nframes)
    return buf.getvalue()


def test_parse_env_file_handles_comments_quotes_and_exports(tmp_path: Path) -> None:
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "# comment\n"
        "PLAIN=abc123\n"
        "export QUOTED='hello world'\n"
        'DOUBLE="foo=bar"\n'
        "  SPACED =  value  \n",
        encoding="utf-8",
    )
    parsed = colab_rvc.parse_env_file(env_file)
    assert parsed == {
        "PLAIN": "abc123",
        "QUOTED": "hello world",
        "DOUBLE": "foo=bar",
        "SPACED": "value",
    }
    assert colab_rvc.parse_env_file(tmp_path / "missing.env") == {}


def test_chunk_split_reassemble_and_sha_mismatch(tmp_path: Path) -> None:
    src = tmp_path / "weights.bin"
    payload = (b"hakimi-rvc-bytes-" * 150_000)[:2_500_000]
    src.write_bytes(payload)
    expected_sha = hashlib.sha256(payload).hexdigest()

    chunk_dir = tmp_path / "chunks"
    manifest = colab_remote.split_file(src, chunk_dir, chunk_mb=1)
    assert len(manifest["chunks"]) == 3
    assert manifest["sha256"] == expected_sha
    assert (chunk_dir / "manifest.json").is_file()

    bad_out = tmp_path / "bad.bin"
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        colab_remote.reassemble_chunks(
            chunk_dir,
            bad_out,
            expected_sha256="0" * 64,
            remove_chunks=False,
        )
    assert not bad_out.exists()

    good_out = tmp_path / "good.bin"
    colab_remote.reassemble_chunks(
        chunk_dir,
        good_out,
        expected_sha256=expected_sha,
        remove_chunks=True,
    )
    assert good_out.read_bytes() == payload
    assert not chunk_dir.exists()


def test_corpus_utterances_and_idempotent_dataset_build(tmp_path: Path) -> None:
    utterances = colab_rvc.CORPUS_UTTERANCES
    assert len(utterances) >= 60
    texts = [u.text.strip() for u in utterances]
    assert all(texts)
    assert len(set(texts)) == len(texts)
    assert {u.style for u in utterances} == {"ballad", "cheerful", "expressive", "lyrical"}

    voice_env = tmp_path / "voice.env"
    voice_env.write_text(
        "BREEZEBLUE_API_KEY=brz_dummy_test_key\n"
        "BREEZEBLUE_VOICE_ID=voc_dummy_voice\n"
        "BREEZEBLUE_MODEL_ID=breeze-tts-2\n",
        encoding="utf-8",
    )
    dataset_dir = tmp_path / "dataset"
    wav_5s = _make_wav_bytes(duration_sec=5.0, sample_rate=24000)

    calls: list[tuple[str, str]] = []

    def fake_tts(**kwargs) -> bytes:
        calls.append((kwargs["output_format"], kwargs["text"]))
        if kwargs["output_format"] == "wav_48000":
            raise colab_rvc._BreezeHttpError(400, "unsupported format wav_48000")
        return wav_5s

    manifest = colab_rvc.build_dataset(
        dataset_dir=dataset_dir,
        voice_env_path=voice_env,
        min_seconds=300.0,
        tts_fn=fake_tts,
        sleep_fn=lambda _: None,
    )
    assert manifest["clip_count"] == len(utterances)
    assert manifest["output_format"] == "wav"
    assert manifest["sample_rate"] == 24000
    assert manifest["total_duration_sec"] >= 300.0
    assert (dataset_dir / "dataset.tar.gz").is_file()
    with tarfile.open(dataset_dir / "dataset.tar.gz", "r:gz") as tf:
        assert len(tf.getnames()) == len(utterances)

    first_call_count = len(calls)
    assert first_call_count == len(utterances) + 1  # 1 probe fallback + N clips

    # Second run skips everything completely without hitting TTS again.
    manifest2 = colab_rvc.build_dataset(
        dataset_dir=dataset_dir,
        voice_env_path=voice_env,
        min_seconds=300.0,
        tts_fn=fake_tts,
        sleep_fn=lambda _: None,
    )
    assert manifest2 == manifest
    assert len(calls) == first_call_count


def test_upload_song_to_admin_encodes_headers_without_query_params(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            captured["path"] = self.path
            captured["auth"] = self.headers.get("Authorization")
            captured["content_type"] = self.headers.get("Content-Type")
            captured["title"] = unquote(self.headers.get("X-Song-Title", ""))
            captured["artist"] = unquote(self.headers.get("X-Song-Artist", ""))
            captured["tags"] = unquote(self.headers.get("X-Song-Tags", ""))
            captured["start"] = self.headers.get("X-Song-Start")
            captured["duration"] = self.headers.get("X-Song-Duration")
            captured["body"] = self.rfile.read(length)
            resp = json.dumps({"id": "abc123", "title": captured["title"]}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        audio = tmp_path / "yu_ai_chorus.mp3"
        audio.write_bytes(b"ID3fake-mp3-data")
        env_file = tmp_path / ".env"
        env_file.write_text("ADMIN_TOKEN=secret_admin_tok\n", encoding="utf-8")

        res = colab_rvc.upload_song_to_admin(
            audio_path=audio,
            title="雨爱",
            artist="杨丞琳",
            tags="温柔 抒情",
            start_sec=0.0,
            duration_sec=50.0,
            admin_base_url=f"http://127.0.0.1:{server.server_port}",
            env_path=env_file,
        )
        assert res["title"] == "雨爱"
        assert captured["path"] == "/admin/api/songs/upload"
        assert "?" not in str(captured["path"])
        assert captured["auth"] == "Bearer secret_admin_tok"
        assert captured["content_type"] == "audio/mpeg"
        assert captured["title"] == "雨爱"
        assert captured["artist"] == "杨丞琳"
        assert captured["tags"] == "温柔 抒情"
        assert captured["start"] == "0.0"
        assert captured["duration"] == "50.0"
        assert captured["body"] == b"ID3fake-mp3-data"
    finally:
        server.shutdown()
        server.server_close()


def test_find_chorus_start_sec_matches_trad_simp_and_fallback() -> None:
    segments = [
        {"start": 12.0, "end": 18.0, "text": "聽見下雨的聲音"},
        {"start": 63.4, "end": 69.0, "text": "窗外的雨滴一滴滴累積"},
        {"start": 70.0, "end": 75.0, "text": "屋內的濕氣像儲存愛你的記憶"},
    ]
    start, reason = colab_remote.find_chorus_start_sec(
        segments,
        anchor="窗外的雨滴",
        fallback_start_sec=55.0,
        total_duration_sec=262.0,
        clip_duration_sec=50.0,
    )
    assert start == pytest.approx(63.05, abs=0.01)
    assert reason.startswith("matched_anchor:")

    # Traditional anchor vs simplified transcript
    start2, reason2 = colab_remote.find_chorus_start_sec(
        [{"start": 64.0, "end": 70.0, "text": "爱你的记忆"}],
        anchor="愛你的記憶",
        fallback_start_sec=55.0,
        total_duration_sec=262.0,
        clip_duration_sec=50.0,
    )
    assert start2 == pytest.approx(63.65, abs=0.01)
    assert reason2.startswith("matched_anchor:")

    # Fallback clamped cleanly inside track duration
    fb_start, fb_reason = colab_remote.find_chorus_start_sec(
        segments,
        anchor="完全不存在的歌词",
        fallback_start_sec=240.0,
        total_duration_sec=262.0,
        clip_duration_sec=50.0,
    )
    assert fb_start == 212.0
    assert fb_reason == "fallback"


def test_find_trained_artifacts_selects_exported_weights_and_index(tmp_path: Path) -> None:
    logs_dir = tmp_path / "Applio" / "logs"
    exp_dir = logs_dir / "hakimi"
    exp_dir.mkdir(parents=True)
    # Generator / discriminator checkpoints must be excluded
    (exp_dir / "G_2500.pth").write_bytes(b"gen")
    (exp_dir / "D_2500.pth").write_bytes(b"disc")
    (exp_dir / "f0G40k.pth").write_bytes(b"f0")
    (exp_dir / "hakimi_100e_500s.pth").write_bytes(b"old")
    final_pth = exp_dir / "hakimi_250e_1250s.pth"
    final_pth.write_bytes(b"final")
    idx = exp_dir / "added_IVF256_Flat_nprobe_1_hakimi_v2.index"
    idx.write_bytes(b"index")

    found_pth, found_idx = colab_remote.find_trained_artifacts(
        logs_dir, "hakimi", target_epochs=250
    )
    assert found_pth == final_pth
    assert found_idx == idx


def test_run_colab_kills_hung_connection_lost_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time

    fake_colab = tmp_path / "colab"
    fake_colab.write_text(
        f"#!{sys.executable}\n"
        "import sys, time\n"
        "print('RuntimeError: Connection was lost.', flush=True)\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    fake_colab.chmod(0o755)
    fake_adc = tmp_path / "adc.json"
    fake_adc.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(colab_rvc, "COLAB_BIN", fake_colab)
    monkeypatch.setattr(colab_rvc, "COLAB_PYTHON", Path(sys.executable))
    monkeypatch.setattr(colab_rvc, "COLAB_ADC_PATH", fake_adc)

    orig = colab_rvc._stream_process_with_watchdog

    def fast_grace(*args, **kwargs):
        kwargs["conn_lost_grace"] = 0.2
        return orig(*args, **kwargs)

    monkeypatch.setattr(colab_rvc, "_stream_process_with_watchdog", fast_grace)

    started = time.monotonic()
    with pytest.raises(colab_rvc.ColabConnectionLostError):
        colab_rvc._run_colab(["exec", "-s", "test"], stream=False, idle_timeout=30.0)
    elapsed = time.monotonic() - started
    assert elapsed < 5.0


def test_build_mix_filter_complex_with_and_without_backing() -> None:
    fg_no_bg = colab_remote.build_mix_filter_complex(-1.25, has_backing=False)
    assert (
        "[0:a]volume=-1.25dB,highpass=f=85,"
        "equalizer=f=3200:t=q:w=1.4:g=1.8,"
        "equalizer=f=6800:t=q:w=2.0:g=-2.0," in fg_no_bg
    )
    assert "acompressor=threshold=-18dB:ratio=2.2:attack=12:release=140:makeup=1dB," in fg_no_bg
    assert "aformat=channel_layouts=stereo,asplit=2[vdry][vwet];" in fg_no_bg
    assert (
        "[vwet]highpass=f=300,lowpass=f=3800,aecho=0.8:0.6:23|41|67:0.18|0.12|0.08,volume=0.12[vamb];"
        in fg_no_bg
    )
    assert "[1:a]volume=-0.8dB[inst];" in fg_no_bg
    assert "[v][inst]amix=inputs=2:duration=longest:normalize=0," in fg_no_bg
    assert "alimiter=level_in=1:level_out=0.92:limit=0.92:attack=5:release=60[m]" in fg_no_bg
    assert "[2:a]" not in fg_no_bg

    # Default backing mix: -14.0 dB + 220..3200 Hz bandpass
    fg_with_bg = colab_remote.build_mix_filter_complex(2.5, has_backing=True)
    assert "[0:a]volume=2.50dB," in fg_with_bg
    assert "equalizer=f=3200:t=q:w=1.4:g=1.8," in fg_with_bg
    assert "[2:a]highpass=f=220,lowpass=f=3200,volume=-14.0dB[bg];" in fg_with_bg
    assert "[v][inst][bg]amix=inputs=3:duration=longest:normalize=0," in fg_with_bg
    assert "alimiter=level_in=1:level_out=0.92:limit=0.92:attack=5:release=60[m]" in fg_with_bg

    # skip_backing=True forces 2-input mix even when has_backing=True
    fg_skip_bg = colab_remote.build_mix_filter_complex(
        0.0, has_backing=True, backing_gain_db=-12.0, skip_backing=True
    )
    assert "[2:a]" not in fg_skip_bg
    assert "[v][inst]amix=inputs=2:duration=longest:normalize=0," in fg_skip_bg

    # Clamping boundaries: [-24.0, -6.0]
    fg_low = colab_remote.build_mix_filter_complex(0.0, has_backing=True, backing_gain_db=-40.0)
    assert "[2:a]highpass=f=220,lowpass=f=3200,volume=-24.0dB[bg];" in fg_low
    fg_high = colab_remote.build_mix_filter_complex(0.0, has_backing=True, backing_gain_db=-1.0)
    assert "[2:a]highpass=f=220,lowpass=f=3200,volume=-6.0dB[bg];" in fg_high
    fg_custom = colab_remote.build_mix_filter_complex(0.0, has_backing=True, backing_gain_db=-16.5)
    assert "[2:a]highpass=f=220,lowpass=f=3200,volume=-16.5dB[bg];" in fg_custom

    # Strict validation: reject bool, non-finite, and injection strings
    for bad_bg in (True, False, float("nan"), float("inf"), float("-inf"), "-10;aeval=0"):
        with pytest.raises(ValueError):
            colab_remote.build_mix_filter_complex(
                0.0,
                has_backing=True,
                backing_gain_db=bad_bg,  # type: ignore[arg-type]
            )
    for bad_gain in (True, False, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            colab_remote.build_mix_filter_complex(bad_gain)  # type: ignore[arg-type]
    for bad_skip in (1, 0, "true"):
        with pytest.raises(ValueError):
            colab_remote.build_mix_filter_complex(
                0.0,
                has_backing=True,
                skip_backing=bad_skip,  # type: ignore[arg-type]
            )


def test_action_setup_restores_onnxruntime_gpu_and_dereverb_dry_keyword(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work_dir = tmp_path / "work"
    applio_dir = tmp_path / "Applio"
    applio_dir.mkdir(parents=True)
    (applio_dir / "core.py").write_text("# stub\n", encoding="utf-8")
    (applio_dir / "requirements-engine.txt").write_text("", encoding="utf-8")
    rvc_venv = tmp_path / "rvc-venv"
    (rvc_venv / "bin").mkdir(parents=True)
    (rvc_venv / "bin" / "python").write_text("", encoding="utf-8")
    sep_venv = tmp_path / "sep-venv"
    (sep_venv / "bin").mkdir(parents=True)
    (sep_venv / "bin" / "python").write_text("", encoding="utf-8")
    uv_bin = tmp_path / "uv"
    uv_bin.write_text("", encoding="utf-8")

    monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", work_dir)
    monkeypatch.setattr(colab_remote, "SETUP_MARKER", tmp_path / ".setup_done")
    monkeypatch.setattr(colab_remote, "_ensure_ffmpeg", lambda: None)
    monkeypatch.setattr(colab_remote, "_ensure_uv", lambda: str(uv_bin))

    recorded_cmds: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        del kwargs
        recorded_cmds.append(list(cmd))
        return colab_remote.subprocess.CompletedProcess(cmd, 0, stdout="GPU OK\n", stderr="")

    monkeypatch.setattr(colab_remote, "_run", fake_run)
    colab_remote.action_setup(applio_dir=applio_dir, rvc_venv=rvc_venv, sep_venv=sep_venv)

    fw_idx = next(i for i, c in enumerate(recorded_cmds) if "faster-whisper" in c)
    uninst_idx = next(
        i for i, c in enumerate(recorded_cmds) if "uninstall" in c and "onnxruntime" in c
    )
    reinst_idx = next(
        i for i, c in enumerate(recorded_cmds) if "--reinstall" in c and "onnxruntime-gpu" in c
    )
    assert fw_idx < uninst_idx < reinst_idx

    # Verify dereverb stem selection recognizes "dry" keyword
    sep_out = tmp_path / "dereverb_out"
    sep_out.mkdir()
    reverb_wav = sep_out / "lead_(Reverb).wav"
    dry_wav = sep_out / "lead_(Dry).wav"
    reverb_wav.write_bytes(b"RIFF")
    dry_wav.write_bytes(b"RIFF")

    def fake_sep_run(cmd, **kwargs):
        del kwargs
        return colab_remote.subprocess.CompletedProcess(
            cmd,
            0,
            stdout="__SEP_FILES__=" + json.dumps([str(reverb_wav), str(dry_wav)]) + "\n",
            stderr="",
        )

    monkeypatch.setattr(colab_remote, "_run", fake_sep_run)
    picked, second = colab_remote._separate_with_sep_venv(
        "python",
        tmp_path / "in.wav",
        sep_out,
        colab_remote.DEREVERB_MODELS,
        is_dereverb=True,
    )
    assert picked == dry_wav
    assert second is None


def test_action_cover_karaoke_split_audio_and_graceful_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _run_cover_case(
        *,
        case_name: str,
        fail_karaoke: bool,
        backing_gain_db: float = -14.0,
        skip_backing: bool = False,
    ) -> tuple[dict[str, object], list[list[str]], list[tuple[str, tuple[str, ...]]]]:
        case_dir = tmp_path / case_name
        work_dir = case_dir / "work"
        out_dir = case_dir / "out"
        chunks_dir = case_dir / "chunks"
        applio_dir = case_dir / "Applio"
        applio_dir.mkdir(parents=True)
        (applio_dir / "core.py").write_text("# stub\n", encoding="utf-8")
        rvc_venv = case_dir / "rvc-venv"
        (rvc_venv / "bin").mkdir(parents=True)
        (rvc_venv / "bin" / "python").write_text("", encoding="utf-8")
        sep_venv = case_dir / "sep-venv"
        (sep_venv / "bin").mkdir(parents=True)
        (sep_venv / "bin" / "python").write_text("", encoding="utf-8")

        src_song = case_dir / "song.mp3"
        src_song.write_bytes(b"ID3fake")
        pth_path = case_dir / "hakimi.pth"
        pth_path.write_bytes(b"pth")
        idx_path = case_dir / "hakimi.index"
        idx_path.write_bytes(b"idx")

        monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", work_dir)
        monkeypatch.setattr(colab_remote, "_probe_duration_sec", lambda _p: 120.0)
        monkeypatch.setattr(
            colab_remote,
            "_detect_mean_volume_db",
            lambda p: -16.0 if "hakimi" in p.name else -18.0,
        )
        monkeypatch.setattr(colab_remote, "_transcribe_for_chorus", lambda *_a: [])

        sep_calls: list[tuple[str, tuple[str, ...]]] = []

        def fake_separate(sep_py, input_wav, sep_out_dir, model_names, *, is_dereverb=False):
            del sep_py, input_wav
            sep_out_dir.mkdir(parents=True, exist_ok=True)
            sep_calls.append((sep_out_dir.name, tuple(model_names)))
            if model_names == colab_remote.PRIMARY_SEP_MODELS:
                v = sep_out_dir / "vocals.wav"
                inst = sep_out_dir / "inst.wav"
                v.write_bytes(_make_wav_bytes(2.0, 44100))
                inst.write_bytes(_make_wav_bytes(2.0, 44100))
                return v, inst
            if model_names == colab_remote.KARAOKE_SEP_MODELS:
                if fail_karaoke:
                    raise RuntimeError("karaoke model download failed")
                lead = sep_out_dir / "lead.wav"
                backing = sep_out_dir / "backing.wav"
                lead.write_bytes(_make_wav_bytes(2.0, 44100))
                backing.write_bytes(_make_wav_bytes(2.0, 44100))
                return lead, backing
            assert is_dereverb is True
            dry = sep_out_dir / "dry.wav"
            dry.write_bytes(_make_wav_bytes(2.0, 44100))
            return dry, None

        monkeypatch.setattr(colab_remote, "_separate_with_sep_venv", fake_separate)

        cmds: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            del kwargs
            cmds.append(list(cmd))
            if "infer" in cmd and "--output-path" in cmd:
                Path(cmd[cmd.index("--output-path") + 1]).write_bytes(_make_wav_bytes(2.0, 44100))
            elif cmd[0] == "ffmpeg":
                Path(cmd[-1]).write_bytes(b"ID3fake-mp3")
            return colab_remote.subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(colab_remote, "_run", fake_run)

        meta = colab_remote.action_cover(
            source_audio=src_song,
            pth_path=pth_path,
            index_path=idx_path,
            backing_gain_db=backing_gain_db,
            skip_backing=skip_backing,
            applio_dir=applio_dir,
            rvc_venv=rvc_venv,
            sep_venv=sep_venv,
            out_dir=out_dir,
            chunks_dir=chunks_dir,
        )
        return meta, cmds, sep_calls

    meta_ok, cmds_ok, seps_ok = _run_cover_case(case_name="karaoke", fail_karaoke=False)
    assert [m for _, m in seps_ok] == [
        colab_remote.PRIMARY_SEP_MODELS,
        colab_remote.KARAOKE_SEP_MODELS,
        colab_remote.DEREVERB_MODELS,
    ]
    infer_cmd = next(c for c in cmds_ok if "infer" in c)
    assert "--split-audio" not in infer_cmd
    assert infer_cmd[infer_cmd.index("--index-rate") + 1] == "0.25"
    assert infer_cmd[infer_cmd.index("--protect") + 1] == "0.28"
    assert infer_cmd[infer_cmd.index("--volume-envelope") + 1] == "0.25"
    assert infer_cmd[infer_cmd.index("--f0-method") + 1] == "rmvpe"
    assert infer_cmd[infer_cmd.index("--embedder-model") + 1] == "contentvec"
    assert infer_cmd[infer_cmd.index("--export-format") + 1] == "WAV"
    mix_cmd = next(c for c in cmds_ok if "-filter_complex" in c)
    mix_graph = mix_cmd[mix_cmd.index("-filter_complex") + 1]
    assert mix_cmd.count("-i") == 3
    assert "volume=-2.00dB" in mix_graph
    assert "equalizer=f=3200:t=q:w=1.4:g=1.8" in mix_graph
    assert "[2:a]highpass=f=220,lowpass=f=3200,volume=-14.0dB[bg];" in mix_graph
    assert "[v][inst][bg]amix=inputs=3" in mix_graph
    assert meta_ok["karaoke_backing_separated"] is True
    assert meta_ok["backing_gain_db"] == -14.0
    assert meta_ok["skip_backing"] is False
    assert meta_ok["auto_pipeline_split"] is True
    assert "split_audio" not in meta_ok
    assert meta_ok["loudness_gain_db"] == -2.0

    # skip_backing=True omits backing stem input from ffmpeg even when karaoke separation succeeds
    meta_skip, cmds_skip, _ = _run_cover_case(
        case_name="skip_bg", fail_karaoke=False, backing_gain_db=-18.0, skip_backing=True
    )
    mix_skip = next(c for c in cmds_skip if "-filter_complex" in c)
    assert mix_skip.count("-i") == 2
    assert "[v][inst]amix=inputs=2" in mix_skip[mix_skip.index("-filter_complex") + 1]
    assert "[2:a]" not in mix_skip[mix_skip.index("-filter_complex") + 1]
    assert meta_skip["karaoke_backing_separated"] is True
    assert meta_skip["skip_backing"] is True
    assert meta_skip["backing_gain_db"] == -18.0

    # Out-of-range backing_gain_db is clamped to [-24.0, -6.0] in both ffmpeg graph and meta
    meta_clamped, cmds_clamped, _ = _run_cover_case(
        case_name="clamped_bg", fail_karaoke=False, backing_gain_db=-32.0, skip_backing=False
    )
    mix_clamped = next(c for c in cmds_clamped if "-filter_complex" in c)
    assert "volume=-24.0dB[bg];" in mix_clamped[mix_clamped.index("-filter_complex") + 1]
    assert meta_clamped["backing_gain_db"] == -24.0

    meta_fb, cmds_fb, _ = _run_cover_case(case_name="fallback", fail_karaoke=True)
    mix_fb = next(c for c in cmds_fb if "-filter_complex" in c)
    assert mix_fb.count("-i") == 2
    assert "[v][inst]amix=inputs=2" in mix_fb[mix_fb.index("-filter_complex") + 1]
    assert meta_fb["karaoke_backing_separated"] is False
    assert meta_fb["skip_backing"] is False
    assert meta_fb["auto_pipeline_split"] is True


def test_action_train_prepares_songs_and_dry_at_48khz(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work_dir = tmp_path / "work"
    out_dir = tmp_path / "out"
    chunks_dir = tmp_path / "chunks"
    applio_dir = tmp_path / "Applio"
    applio_dir.mkdir(parents=True)
    (applio_dir / "core.py").write_text("# stub\n", encoding="utf-8")
    rvc_venv = tmp_path / "rvc-venv"
    (rvc_venv / "bin").mkdir(parents=True)
    (rvc_venv / "bin" / "python").write_text("", encoding="utf-8")
    sep_venv = tmp_path / "sep-venv"
    (sep_venv / "bin").mkdir(parents=True)
    (sep_venv / "bin" / "python").write_text("", encoding="utf-8")

    src_tts = tmp_path / "utt_001.wav"
    src_dry = tmp_path / "dry_extra.flac"
    src_song = tmp_path / "full_song.mp3"
    src_tts.write_bytes(_make_wav_bytes(2.0, 24000))
    src_dry.write_bytes(b"fLaC")
    src_song.write_bytes(b"ID3song")
    tar_path = tmp_path / "dataset.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tf:
        tf.add(src_tts, arcname="utt_001.wav")
        tf.add(src_dry, arcname="dry/001_dry_extra.flac")
        tf.add(src_song, arcname="songs/001_full_song.mp3")

    monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", work_dir)
    sep_models_called: list[tuple[str, ...]] = []

    def fake_sep(sep_py, input_wav, sep_out_dir, model_names, *, is_dereverb=False):
        del sep_py, input_wav
        sep_out_dir.mkdir(parents=True, exist_ok=True)
        sep_models_called.append(tuple(model_names))
        if model_names == colab_remote.PRIMARY_SEP_MODELS:
            v = sep_out_dir / "vocals.wav"
            inst = sep_out_dir / "inst.wav"
            v.write_bytes(b"RIFF")
            inst.write_bytes(b"RIFF")
            return v, inst
        if model_names == colab_remote.KARAOKE_SEP_MODELS:
            lead = sep_out_dir / "lead.wav"
            bg = sep_out_dir / "bg.wav"
            lead.write_bytes(b"RIFF")
            bg.write_bytes(b"RIFF")
            return lead, bg
        assert is_dereverb is True
        dry = sep_out_dir / "dry.wav"
        dry.write_bytes(b"RIFF")
        return dry, None

    monkeypatch.setattr(colab_remote, "_separate_with_sep_venv", fake_sep)

    cmds: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        del kwargs
        cmds.append(list(cmd))
        if cmd[0] == "ffmpeg":
            Path(cmd[-1]).write_bytes(_make_wav_bytes(1.0, 48000))
        elif "preprocess" in cmd:
            exp_dir = applio_dir / "logs" / "hakimi" / "sliced_audios"
            exp_dir.mkdir(parents=True, exist_ok=True)
            (exp_dir / "0_0.wav").write_bytes(b"RIFF")
        elif "extract" in cmd:
            exp_dir = applio_dir / "logs" / "hakimi"
            (exp_dir / "extracted").mkdir(parents=True, exist_ok=True)
            (exp_dir / "extracted" / "0_0.npy").write_bytes(b"npy")
            (exp_dir / "filelist.txt").write_text("0\n", encoding="utf-8")
        elif "train" in cmd:
            exp_dir = applio_dir / "logs" / "hakimi"
            (exp_dir / "hakimi_150e_900s.pth").write_bytes(b"pth-weights")
            (exp_dir / "added_hakimi.index").write_bytes(b"idx-data")
        return colab_remote.subprocess.CompletedProcess(cmd, 0, stdout="ok\n", stderr="")

    monkeypatch.setattr(colab_remote, "_run", fake_run)

    pth, idx = colab_remote.action_train(
        dataset_tar=tar_path,
        applio_dir=applio_dir,
        rvc_venv=rvc_venv,
        sep_venv=sep_venv,
        out_dir=out_dir,
        chunks_dir=chunks_dir,
    )
    assert pth.read_bytes() == b"pth-weights"
    assert idx.read_bytes() == b"idx-data"
    assert sep_models_called == [
        colab_remote.PRIMARY_SEP_MODELS,
        colab_remote.KARAOKE_SEP_MODELS,
        colab_remote.DEREVERB_MODELS,
    ]
    dataset_wavs = sorted(p.name for p in (work_dir / "dataset").glob("*.wav"))
    assert dataset_wavs == ["clip_0001.wav", "clip_0002.wav", "song_0001.wav"]
    ffmpeg_cmds = [c for c in cmds if c[0] == "ffmpeg"]
    assert len(ffmpeg_cmds) == 3
    for fc in ffmpeg_cmds:
        assert fc[fc.index("-ar") + 1] == "48000"
        assert fc[fc.index("-ac") + 1] == "1"
    train_cmd = next(c for c in cmds if "train" in c)
    assert train_cmd[train_cmd.index("--total-epoch") + 1] == "150"
    assert train_cmd[train_cmd.index("--save-every-epoch") + 1] == "30"
    assert train_cmd[train_cmd.index("--batch-size") + 1] == "12"
    assert train_cmd[train_cmd.index("--sample-rate") + 1] == "48000"


def test_build_dataset_extra_corpus_no_tts_and_cache_invalidation(tmp_path: Path) -> None:
    voice_env = tmp_path / "voice.env"
    voice_env.write_text(
        "BREEZEBLUE_API_KEY=brz_dummy\nBREEZEBLUE_VOICE_ID=voc_dummy\n",
        encoding="utf-8",
    )
    dataset_dir = tmp_path / "dataset"
    extra_dry = tmp_path / "extra_dry"
    extra_songs = tmp_path / "extra_songs"
    extra_dry.mkdir()
    extra_songs.mkdir()

    # Empty extra dirs + include_tts=False must raise ValueError
    with pytest.raises(ValueError, match="include_tts=False"):
        colab_rvc.build_dataset(
            dataset_dir=dataset_dir,
            voice_env_path=tmp_path / "missing.env",
            extra_dry_dir=extra_dry,
            extra_songs_dir=extra_songs,
            include_tts=False,
        )

    dry1 = extra_dry / "甜美干声 01.wav"
    dry1.write_bytes(_make_wav_bytes(3.0, 48000))
    song1 = extra_songs / "翻唱曲目.mp3"
    song1.write_bytes(b"ID3song-bytes-1")

    tts_calls = 0

    def fake_tts(**_kwargs) -> bytes:
        nonlocal tts_calls
        tts_calls += 1
        return _make_wav_bytes(5.0, 48000)

    manifest = colab_rvc.build_dataset(
        dataset_dir=dataset_dir,
        voice_env_path=voice_env,
        min_seconds=300.0,
        tts_fn=fake_tts,
        sleep_fn=lambda _: None,
        extra_dry_dir=extra_dry,
        extra_songs_dir=extra_songs,
        include_tts=True,
    )
    assert manifest["include_tts"] is True
    assert manifest["tts_clip_count"] == len(colab_rvc.CORPUS_UTTERANCES)
    assert manifest["clip_count"] == len(colab_rvc.CORPUS_UTTERANCES) + 2
    assert len(manifest["extra_dry_files"]) == 1
    assert len(manifest["extra_songs_files"]) == 1
    assert tts_calls == len(colab_rvc.CORPUS_UTTERANCES)

    with tarfile.open(dataset_dir / "dataset.tar.gz", "r:gz") as tf:
        names = tf.getnames()
    assert "utt_001.wav" in names
    assert "dry/001_甜美干声_01.wav" in names
    assert "songs/001_翻唱曲目.mp3" in names

    # Modifying an extra song file invalidates manifest cache without re-calling TTS
    song1.write_bytes(b"ID3song-bytes-modified-longer")
    manifest_rebuilt = colab_rvc.build_dataset(
        dataset_dir=dataset_dir,
        voice_env_path=voice_env,
        min_seconds=300.0,
        tts_fn=fake_tts,
        sleep_fn=lambda _: None,
        extra_dry_dir=extra_dry,
        extra_songs_dir=extra_songs,
        include_tts=True,
    )
    assert tts_calls == len(colab_rvc.CORPUS_UTTERANCES)
    assert manifest_rebuilt["dataset_tar_sha256"] != manifest["dataset_tar_sha256"]

    # include_tts=False works without voice.env or TTS calls when extra files exist
    no_tts_dir = tmp_path / "dataset_no_tts"
    manifest_no_tts = colab_rvc.build_dataset(
        dataset_dir=no_tts_dir,
        voice_env_path=tmp_path / "nonexistent.env",
        extra_dry_dir=extra_dry,
        extra_songs_dir=extra_songs,
        include_tts=False,
    )
    assert manifest_no_tts["include_tts"] is False
    assert manifest_no_tts["tts_clip_count"] == 0
    assert manifest_no_tts["clip_count"] == 2
    with tarfile.open(no_tts_dir / "dataset.tar.gz", "r:gz") as tf:
        assert sorted(tf.getnames()) == ["dry/001_甜美干声_01.wav", "songs/001_翻唱曲目.mp3"]

    # Cache hit on include_tts=False
    manifest_no_tts_2 = colab_rvc.build_dataset(
        dataset_dir=no_tts_dir,
        voice_env_path=tmp_path / "nonexistent.env",
        extra_dry_dir=extra_dry,
        extra_songs_dir=extra_songs,
        include_tts=False,
    )
    assert manifest_no_tts_2 == manifest_no_tts

    # CLI flags present on build-dataset, train, and all
    parser = colab_rvc._build_parser()
    for subcmd in ("build-dataset", "train"):
        ns = parser.parse_args(
            [
                subcmd,
                "--extra-dry-dir",
                str(extra_dry),
                "--extra-songs-dir",
                str(extra_songs),
                "--no-tts",
            ]
        )
        assert ns.extra_dry_dir == extra_dry
        assert ns.extra_songs_dir == extra_songs
        assert ns.no_tts is True
    ns_all = parser.parse_args(
        [
            "all",
            "--source",
            str(song1),
            "--title",
            "t",
            "--extra-dry-dir",
            str(extra_dry),
            "--extra-songs-dir",
            str(extra_songs),
            "--no-tts",
        ]
    )
    assert ns_all.extra_dry_dir == extra_dry
    assert ns_all.extra_songs_dir == extra_songs
    assert ns_all.no_tts is True


def test_p0_drive_direct_cover_fallback_and_zero_token_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret_tok = "ya29.super-secret-direct-token-999999"

    # 1) _redact_cmd & _redact_text must never expose secret env values or Bearer tokens
    redacted = colab_rvc._redact_cmd(
        [
            "exec",
            "-s",
            "sess1",
            "--env",
            f"HAKIMI_DRIVE_ACCESS_TOKEN={secret_tok}",
            "--env",
            "HAKIMI_PITCH=0",
        ],
        secret_tok,
    )
    assert secret_tok not in redacted
    assert "HAKIMI_DRIVE_ACCESS_TOKEN=[REDACTED]" in redacted
    assert "HAKIMI_PITCH=0" in redacted
    assert secret_tok not in colab_rvc._redact_text(f"Authorization: Bearer {secret_tok}")

    # 2) Direct Drive happy path: zero WS uploads, zero WS downloads
    ws_uploads: list[str] = []
    ws_downloads: list[str] = []
    stopped: list[str] = []
    captured_envs: list[tuple[str, dict[str, str]]] = []

    monkeypatch.setattr(colab_rvc, "provision_gpu", lambda s, gpu="L4", **kw: "L4")
    monkeypatch.setattr(colab_rvc, "stop_session", stopped.append)
    monkeypatch.setattr(
        colab_rvc,
        "upload_file_chunked",
        lambda s, lp, rp, **kw: ws_uploads.append(rp),
    )
    monkeypatch.setattr(
        colab_rvc,
        "download_chunked_dir",
        lambda s, rdir, lout: ws_downloads.append(rdir) or lout,
    )

    def fake_exec_ok(session: str, action: str, env_vars=None) -> str:
        env_copy = dict(env_vars or {})
        captured_envs.append((action, env_copy))
        if action == "setup":
            return (
                colab_rvc.CACHE_STATUS_PREFIX
                + json.dumps({"weight_cache_hit": True, "weight_cache_has_sep_models": True})
                + "\n"
                + colab_rvc.STATUS_OK_MARKER
            )
        assert action == "cover"
        out_payload = {
            "chorus": {
                "file_id": "drv_chorus_001",
                "name": "yu_ai_chorus.mp3",
                "sha256": "a" * 64,
                "size": 123456,
                "duration_ms": 50000,
            },
            "full": {"file_id": "drv_full_001", "name": "yu_ai_full.mp3"},
            "meta": {"chorus_start_sec": 72.5, "chorus_start_reason": "dsp_vocal_rms"},
        }
        return (
            colab_rvc.DRIVE_OUTPUTS_PREFIX
            + json.dumps(out_payload)
            + "\n"
            + colab_rvc.STATUS_OK_MARKER
        )

    monkeypatch.setattr(colab_rvc, "exec_remote_worker", fake_exec_ok)
    res = colab_rvc.run_cover_pipeline(
        session="sess-direct",
        gpu_spec="L4,T4",
        cover_dir=tmp_path / "cover_ok",
        drive_access_token=secret_tok,
        drive_source_file_id="drv_src_1",
        drive_source_ext=".flac",
        drive_pth_file_id="drv_pth_1",
        drive_index_file_id="drv_idx_1",
        drive_songs_folder_id="drv_songs_folder_1",
        cover_prefix="yu_ai",
        song_title="雨爱",
        cache_folder_id="drv_cache_folder_1",
        cache_weights_file_id="drv_wcache_1",
        cache_weights_sha256="b" * 64,
        keep_session_on_success=True,
    )
    assert ws_uploads == []
    assert ws_downloads == []
    assert stopped == []
    assert res["session_kept"] is True
    assert res["drive_outputs"]["chorus"]["file_id"] == "drv_chorus_001"
    assert res["acceleration"]["drive_direct"] is True
    assert res["acceleration"]["drive_direct_fallback"] is False
    assert res["acceleration"]["weight_cache_hit"] is True
    cover_env = captured_envs[1][1]
    assert cover_env["HAKIMI_DRIVE_SOURCE_FILE_ID"] == "drv_src_1"
    assert cover_env["HAKIMI_DRIVE_PTH_FILE_ID"] == "drv_pth_1"
    assert cover_env["HAKIMI_DRIVE_SONGS_FOLDER_ID"] == "drv_songs_folder_1"

    # 3) Direct input failure + direct output failure -> transparent WS fallback
    local_src = tmp_path / "fallback_src.m4a"
    local_pth = tmp_path / "hakimi.pth"
    local_idx = tmp_path / "hakimi.index"
    local_src.write_bytes(b"M4A")
    local_pth.write_bytes(b"PTH")
    local_idx.write_bytes(b"IDX")

    cover_calls = 0

    def fake_exec_fallback(session: str, action: str, env_vars=None) -> str:
        nonlocal cover_calls
        if action == "setup":
            return colab_rvc.STATUS_OK_MARKER
        cover_calls += 1
        if cover_calls == 1:
            raise RuntimeError(
                "Remote worker action 'cover' failed: drive_direct_input_failed:source:HTTPError"
            )
        # Second attempt succeeds without direct Drive outputs -> triggers WS download fallback
        assert "HAKIMI_DRIVE_SOURCE_FILE_ID" not in (env_vars or {})
        assert "HAKIMI_DRIVE_PTH_FILE_ID" not in (env_vars or {})
        return colab_rvc.STATUS_OK_MARKER

    def fake_dl_chunks(session: str, rdir: str, lout: Path) -> Path:
        ws_downloads.append(rdir)
        lout.parent.mkdir(parents=True, exist_ok=True)
        lout.write_bytes(b"ID3mp3")
        return lout

    def fake_run_colab_meta(args: list[str], **kwargs):
        del kwargs
        Path(args[-1]).write_text(json.dumps({"chorus_start_sec": 60.0}), encoding="utf-8")
        return 0, ""

    monkeypatch.setattr(colab_rvc, "exec_remote_worker", fake_exec_fallback)
    monkeypatch.setattr(colab_rvc, "download_chunked_dir", fake_dl_chunks)
    monkeypatch.setattr(colab_rvc, "_run_colab", fake_run_colab_meta)

    fb_res = colab_rvc.run_cover_pipeline(
        session="sess-fb",
        gpu_spec="L4",
        cover_dir=tmp_path / "cover_fb",
        ensure_local_source_fn=lambda: local_src,
        ensure_local_model_fn=lambda: (local_pth, local_idx),
        drive_access_token=secret_tok,
        drive_source_file_id="drv_src_bad",
        drive_pth_file_id="drv_pth_bad",
        drive_index_file_id="drv_idx_bad",
        drive_songs_folder_id="drv_songs_folder_1",
        keep_session_on_success=False,
    )
    assert cover_calls == 2
    assert len(ws_uploads) == 3  # pth + idx + source
    assert "/content/out_chunks/cover_chorus" in ws_downloads
    assert fb_res["drive_outputs"] is None
    assert fb_res["chorus_mp3"].is_file()
    assert fb_res["acceleration"]["drive_direct_fallback"] is True
    assert "sess-fb" in stopped

    # 4) Remote _dispatch_env pops HAKIMI_DRIVE_ACCESS_TOKEN so child processes never inherit it
    #    and scrubs token from logs and STATUS ERROR line.
    seen_child_env: list[str | None] = []
    monkeypatch.setenv("HAKIMI_COLAB_ACTION", "setup")
    monkeypatch.setenv("HAKIMI_DRIVE_ACCESS_TOKEN", secret_tok)

    def fake_action_setup(**kwargs):
        seen_child_env.append(os.environ.get("HAKIMI_DRIVE_ACCESS_TOKEN"))
        assert kwargs["drive_token"] == secret_tok
        raise RuntimeError(f"simulated error carrying {secret_tok}")

    monkeypatch.setattr(colab_remote, "action_setup", fake_action_setup)
    buf = io.StringIO()
    import contextlib
    import os

    with contextlib.redirect_stdout(buf):
        with pytest.raises(RuntimeError):
            colab_remote.main(argv=None)
    assert seen_child_env == [None]
    assert secret_tok not in buf.getvalue()
    assert "[REDACTED]" in buf.getvalue()
    assert os.environ.get("HAKIMI_DRIVE_ACCESS_TOKEN") is None
    monkeypatch.delenv("HAKIMI_COLAB_ACTION", raising=False)


def test_p1_run_cover_pipeline_warm_reuse_and_high_gpu_never_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provision_calls: list[str] = []
    exec_actions: list[str] = []
    stopped: list[str] = []

    def fake_provision(session: str, gpu: str = "L4", **kw) -> str:
        provision_calls.append(gpu)
        return gpu.split(",")[0]

    def fake_exec(session: str, action: str, env_vars=None) -> str:
        exec_actions.append(action)
        out = {
            "chorus": {
                "file_id": "drv_c1",
                "name": "c.mp3",
                "sha256": "c" * 64,
                "size": 1000,
                "duration_ms": 50000,
            },
            "full": {"file_id": "drv_f1", "name": "f.mp3"},
            "meta": {"chorus_start_sec": 30.0},
        }
        if action == "cover":
            return (
                colab_rvc.DRIVE_OUTPUTS_PREFIX + json.dumps(out) + "\n" + colab_rvc.STATUS_OK_MARKER
            )
        return colab_rvc.STATUS_OK_MARKER

    monkeypatch.setattr(colab_rvc, "provision_gpu", fake_provision)
    monkeypatch.setattr(colab_rvc, "exec_remote_worker", fake_exec)
    monkeypatch.setattr(colab_rvc, "stop_session", stopped.append)

    # Warm reuse on L4 with setup_ready skips BOTH provision_gpu and setup!
    warm_res = colab_rvc.run_cover_pipeline(
        session="warm-l4",
        gpu_spec="L4,T4",
        cover_dir=tmp_path / "warm",
        drive_access_token="ya29.tok",
        drive_source_file_id="src1",
        drive_pth_file_id="pth1",
        drive_index_file_id="idx1",
        drive_songs_folder_id="songs1",
        reuse_session=True,
        reused_gpu="L4",
        reused_setup_ready=True,
        keep_session_on_success=True,
    )
    assert provision_calls == []
    assert exec_actions == ["cover"]
    assert stopped == []
    assert warm_res["session_kept"] is True
    assert warm_res["acceleration"]["warm_reused"] is True

    # High-price GPUs (H100/A100/G4) MUST ALWAYS be stopped even if keep_session_on_success=True
    for banned_gpu in ("H100", "A100", "G4"):
        stopped.clear()
        hi_res = colab_rvc.run_cover_pipeline(
            session=f"sess-{banned_gpu}",
            gpu_spec=banned_gpu,
            cover_dir=tmp_path / banned_gpu,
            drive_access_token="ya29.tok",
            drive_source_file_id="src1",
            drive_pth_file_id="pth1",
            drive_index_file_id="idx1",
            drive_songs_folder_id="songs1",
            keep_session_on_success=True,
        )
        assert hi_res["session_kept"] is False
        assert f"sess-{banned_gpu}" in stopped

    # probe_warm_session parses healthy vs broken
    monkeypatch.setattr(
        colab_rvc,
        "_run_colab",
        lambda args, **kw: (0, '__HAKIMI_WARM_PROBE__={"ok": true, "setup_ready": true}\n'),
    )
    assert colab_rvc.probe_warm_session("warm-l4") == {"healthy": True, "setup_ready": True}
    monkeypatch.setattr(colab_rvc, "_run_colab", lambda args, **kw: (1, "connection refused"))
    assert colab_rvc.probe_warm_session("warm-l4") == {"healthy": False, "setup_ready": False}


def test_p2_weight_and_venv_cache_pack_restore_and_setup_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1) Weight cache pack -> restore roundtrip + checksum & traversal guards
    src_applio = tmp_path / "src_applio"
    src_sep = tmp_path / "src_sep"
    (src_applio / "rvc" / "models" / "predictors").mkdir(parents=True)
    (src_applio / "rvc" / "models" / "embedders" / "contentvec").mkdir(parents=True)
    (src_applio / "rvc" / "models" / "predictors" / "rmvpe.pt").write_bytes(b"RMVPE-WEIGHTS")
    (src_applio / "rvc" / "models" / "embedders" / "contentvec" / "pytorch_model.bin").write_bytes(
        b"CONTENTVEC"
    )
    src_sep.mkdir(parents=True)
    (src_sep / colab_remote.PRIMARY_SEP_MODELS[0]).write_bytes(b"BS-ROFORMER-CKPT")
    (src_sep / colab_remote.KARAOKE_SEP_MODELS[0]).write_bytes(b"KARAOKE-CKPT")
    (src_sep / colab_remote.DEREVERB_MODELS[0]).write_bytes(b"DEREVERB-CKPT")

    w_tar = tmp_path / "weights.tar"
    packed_w = colab_remote.pack_weight_cache(w_tar, applio_dir=src_applio, sep_models_dir=src_sep)
    assert packed_w["file_count"] == 5
    assert packed_w["sep_model_files"] == 3
    assert packed_w["applio_model_files"] == 2

    dst_applio = tmp_path / "dst_applio"
    dst_sep = tmp_path / "dst_sep"
    with pytest.raises(ValueError, match="weight_cache_sha256_mismatch"):
        colab_remote.restore_weight_cache(
            w_tar, expected_sha256="0" * 64, applio_dir=dst_applio, sep_models_dir=dst_sep
        )

    restored_w = colab_remote.restore_weight_cache(
        w_tar,
        expected_sha256=packed_w["sha256"],
        applio_dir=dst_applio,
        sep_models_dir=dst_sep,
    )
    assert restored_w["file_count"] == 5
    assert colab_remote._has_applio_pretrained_weights(dst_applio, mode="cover") is True
    assert (dst_sep / colab_remote.PRIMARY_SEP_MODELS[0]).read_bytes() == b"BS-ROFORMER-CKPT"

    # Malicious symlink inside weight tar must be rejected
    bad_w_tar = tmp_path / "bad_weights.tar"
    with tarfile.open(bad_w_tar, "w") as tf:
        ti = tarfile.TarInfo("sep_models/evil.ckpt")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "/etc/passwd"
        tf.addfile(ti)
    with pytest.raises(ValueError, match="Unsafe member"):
        colab_remote.restore_weight_cache(bad_w_tar, applio_dir=dst_applio, sep_models_dir=dst_sep)

    # 2) Venv cache pack -> strict fingerprint gate -> restore + unsafe symlink rejection
    src_rvc = tmp_path / "src_rvc_venv"
    src_sep_v = tmp_path / "src_sep_venv"
    (src_rvc / "bin").mkdir(parents=True)
    (src_sep_v / "bin").mkdir(parents=True)
    (src_rvc / "bin" / "python").write_text("#!/bin/sh\n", encoding="utf-8")
    (src_rvc / "bin" / "python3").symlink_to("python")
    (src_sep_v / "bin" / "python").write_text("#!/bin/sh\n", encoding="utf-8")

    fp_ok = colab_remote.compute_runtime_fingerprint(
        python_version="3.12", cuda_version="12.4", os_id="ubuntu22.04", arch="x86_64"
    )
    fp_other = colab_remote.compute_runtime_fingerprint(
        python_version="3.12", cuda_version="12.1", os_id="ubuntu22.04", arch="x86_64"
    )
    v_tar = tmp_path / "venv.tar.gz"
    packed_v = colab_remote.pack_venv_cache(
        v_tar, rvc_venv=src_rvc, sep_venv=src_sep_v, fingerprint=fp_ok
    )

    dst_rvc = tmp_path / "dst_rvc_venv"
    dst_sep_v = tmp_path / "dst_sep_venv"
    with pytest.raises(ValueError, match="venv_fingerprint_mismatch"):
        colab_remote.restore_venv_cache(
            v_tar,
            expected_sha256=packed_v["sha256"],
            expected_fingerprint=fp_ok,
            actual_fingerprint=fp_other,
            rvc_venv=dst_rvc,
            sep_venv=dst_sep_v,
        )

    colab_remote.restore_venv_cache(
        v_tar,
        expected_sha256=packed_v["sha256"],
        expected_fingerprint=fp_ok,
        actual_fingerprint=fp_ok,
        rvc_venv=dst_rvc,
        sep_venv=dst_sep_v,
    )
    assert (dst_rvc / "bin" / "python").is_file()
    assert (dst_sep_v / "bin" / "python").is_file()

    # Unsafe symlink escaping venv is rejected
    bad_v_tar = tmp_path / "bad_venv.tar.gz"
    with tarfile.open(bad_v_tar, "w:gz") as tf:
        ti = tarfile.TarInfo("rvc-venv/bin/evil")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "/etc/passwd"
        tf.addfile(ti)
    bad_v_sha = colab_remote.sha256_file(bad_v_tar)
    with pytest.raises(ValueError, match="Unsafe symlink"):
        colab_remote.restore_venv_cache(
            bad_v_tar,
            expected_sha256=bad_v_sha,
            expected_fingerprint=fp_ok,
            actual_fingerprint=fp_ok,
            rvc_venv=tmp_path / "bad_dst_rvc",
            sep_venv=tmp_path / "bad_dst_sep",
        )

    # 3) action_setup() integration: HIT skips prerequisites/pip; MISS falls back + atomic writeback
    setup_root = tmp_path / "setup_hit"
    applio_hit = setup_root / "Applio"
    applio_hit.mkdir(parents=True)
    (applio_hit / "core.py").write_text("# stub\n", encoding="utf-8")
    (applio_hit / "requirements-engine.txt").write_text("torch\n", encoding="utf-8")

    monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", setup_root / "work")
    monkeypatch.setattr(colab_remote, "SETUP_MARKER", setup_root / ".setup_done")
    monkeypatch.setattr(colab_remote, "_ensure_ffmpeg", lambda: None)
    monkeypatch.setattr(colab_remote, "_ensure_uv", lambda: "uv")
    monkeypatch.setattr(colab_remote, "compute_runtime_fingerprint", lambda **kw: fp_ok)

    hit_cmds: list[list[str]] = []

    def fake_run_hit(cmd, **kwargs):
        del kwargs
        hit_cmds.append(list(cmd))
        return colab_remote.subprocess.CompletedProcess(
            cmd, 0, stdout="NVIDIA L4 | CUDA Version: 12.4\n", stderr=""
        )

    monkeypatch.setattr(colab_remote, "_run", fake_run_hit)
    monkeypatch.setattr(
        colab_remote,
        "_drive_download_to_path",
        lambda tok, fid, dest: (
            dest.parent.mkdir(parents=True, exist_ok=True)
            or dest.write_bytes(w_tar.read_bytes() if fid == "w_fid" else v_tar.read_bytes())
            or dest
        ),
    )

    hit_status = colab_remote.action_setup(
        applio_dir=applio_hit,
        rvc_venv=setup_root / "rvc-venv",
        sep_venv=setup_root / "sep-venv",
        sep_models_dir=setup_root / "sep-models",
        mode="cover",
        drive_token="ya29.cache-tok",
        cache_folder_id="cache_folder_1",
        cache_weights_file_id="w_fid",
        cache_weights_sha256=packed_w["sha256"],
        cache_venv_enabled=True,
        cache_venv_file_id="v_fid",
        cache_venv_sha256=packed_v["sha256"],
        cache_venv_fingerprint=fp_ok,
    )
    assert hit_status["weight_cache_hit"] is True
    assert hit_status["venv_cache_hit"] is True
    assert not any("prerequisites" in c for c in hit_cmds)
    assert not any("pip" in c for c in hit_cmds)

    # Corrupt checksum + fingerprint mismatch -> transparent fallback + atomic write-back order
    fb_root = tmp_path / "setup_fb"
    applio_fb = fb_root / "Applio"
    applio_fb.mkdir(parents=True)
    (applio_fb / "core.py").write_text("# stub\n", encoding="utf-8")
    (applio_fb / "requirements-engine.txt").write_text("torch\n", encoding="utf-8")
    (fb_root / "rvc-venv" / "bin").mkdir(parents=True)
    (fb_root / "rvc-venv" / "bin" / "python").write_text("", encoding="utf-8")
    (fb_root / "sep-venv" / "bin").mkdir(parents=True)
    (fb_root / "sep-venv" / "bin" / "python").write_text("", encoding="utf-8")

    monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", fb_root / "work")
    monkeypatch.setattr(colab_remote, "SETUP_MARKER", fb_root / ".setup_done")
    fb_cmds: list[list[str]] = []

    def fake_run_fb(cmd, **kwargs):
        del kwargs
        fb_cmds.append(list(cmd))
        if "prerequisites" in cmd:
            (applio_fb / "rvc" / "models" / "predictors").mkdir(parents=True, exist_ok=True)
            (applio_fb / "rvc" / "models" / "embedders" / "contentvec").mkdir(
                parents=True, exist_ok=True
            )
            (applio_fb / "rvc" / "models" / "predictors" / "rmvpe.pt").write_bytes(b"RMVPE")
            (applio_fb / "rvc" / "models" / "embedders" / "contentvec" / "model.bin").write_bytes(
                b"CV"
            )
        return colab_remote.subprocess.CompletedProcess(
            cmd, 0, stdout="NVIDIA L4 | CUDA Version: 12.4\n", stderr=""
        )

    upload_order: list[str] = []

    def fake_up_path(tok, folder, local_path, drive_name, mime, **kw):
        del tok, folder, local_path, mime, kw
        upload_order.append(drive_name)
        return {"file_id": f"up_{len(upload_order)}", "name": drive_name, "size": 100}

    monkeypatch.setattr(colab_remote, "_run", fake_run_fb)
    monkeypatch.setattr(colab_remote, "_drive_upload_from_path", fake_up_path)

    fb_status = colab_remote.action_setup(
        applio_dir=applio_fb,
        rvc_venv=fb_root / "rvc-venv",
        sep_venv=fb_root / "sep-venv",
        sep_models_dir=fb_root / "sep-models",
        mode="cover",
        drive_token="ya29.cache-tok",
        cache_folder_id="cache_folder_1",
        cache_weights_file_id="w_fid",
        cache_weights_sha256="f" * 64,  # intentional mismatch -> fallback!
        cache_venv_enabled=True,
        cache_venv_file_id="v_fid",
        cache_venv_sha256=packed_v["sha256"],
        cache_venv_fingerprint=fp_other,  # intentional fingerprint mismatch -> fallback!
        cache_writeback=True,
    )
    assert fb_status["weight_cache_hit"] is False
    assert fb_status["venv_cache_hit"] is False
    assert fb_status["venv_fallback_reason"] == "fingerprint_mismatch"
    assert fb_status["weight_cache_written"] is True
    assert fb_status["venv_cache_written"] is True
    assert any("prerequisites" in c for c in fb_cmds)
    assert any("pip" in c for c in fb_cmds)
    # Atomic write-back order: archive uploaded BEFORE its .manifest.json commit!
    w_key = colab_remote.compute_weight_cache_key("cover")
    w_archive_idx = next(
        i for i, n in enumerate(upload_order) if n.startswith(f"{w_key}_") and n.endswith(".tar")
    )
    w_manifest_idx = upload_order.index(f"{w_key}.manifest.json")
    assert w_archive_idx < w_manifest_idx
    v_manifest_idx = next(
        i
        for i, n in enumerate(upload_order)
        if n.startswith("venv_") and n.endswith(".manifest.json")
    )
    v_archive_idx = next(
        i for i, n in enumerate(upload_order) if n.startswith("venv_") and n.endswith(".tar.gz")
    )
    assert v_archive_idx < v_manifest_idx


def test_p2_venv_cache_rejects_symlink_directory_traversal_and_prefix_collision(
    tmp_path: Path,
) -> None:
    import os

    fp = "ubuntu22.04_x86_64_py3.12_cuda12.4"

    def _build_tar(path: Path, entries: list[tuple[str, str, bytes | str]]) -> str:
        with tarfile.open(path, "w:gz") as tf:
            for name, kind, data in entries:
                ti = tarfile.TarInfo(name)
                if kind == "sym":
                    ti.type = tarfile.SYMTYPE
                    ti.linkname = str(data)
                    tf.addfile(ti)
                else:
                    raw = data if isinstance(data, bytes) else data.encode("utf-8")
                    ti.size = len(raw)
                    tf.addfile(ti, io.BytesIO(raw))
        return colab_remote.sha256_file(path)

    # 1) Absolute symlink pointing to directory prefix + write through symlink -> MUST reject
    t_dir_esc = tmp_path / "dir_escape.tar.gz"
    sha1 = _build_tar(
        t_dir_esc,
        [
            ("rvc-venv/link", "sym", "/root/.local/share/uv/python/foo"),
            ("rvc-venv/link/pwned", "file", b"evil"),
            ("rvc-venv/bin/python", "file", b"py"),
            ("sep-venv/bin/python", "file", b"py"),
        ],
    )
    with pytest.raises(ValueError, match="Unsafe symlink"):
        colab_remote.restore_venv_cache(
            t_dir_esc,
            expected_sha256=sha1,
            expected_fingerprint=fp,
            actual_fingerprint=fp,
            rvc_venv=tmp_path / "out1_rvc",
            sep_venv=tmp_path / "out1_sep",
        )

    # 2) Prefix collision (/usr/bin/python_evil) -> MUST reject
    t_prefix = tmp_path / "prefix_collision.tar.gz"
    sha2 = _build_tar(
        t_prefix,
        [
            ("rvc-venv/bin/python", "sym", "/usr/bin/python_evil"),
            ("sep-venv/bin/python", "file", b"py"),
        ],
    )
    with pytest.raises(ValueError, match="Unsafe symlink"):
        colab_remote.restore_venv_cache(
            t_prefix,
            expected_sha256=sha2,
            expected_fingerprint=fp,
            actual_fingerprint=fp,
            rvc_venv=tmp_path / "out2_rvc",
            sep_venv=tmp_path / "out2_sep",
        )

    # 3) Valid bin python symlink + regular file traversing through that symlink -> MUST reject
    t_bin_trav = tmp_path / "bin_traverse.tar.gz"
    sha3 = _build_tar(
        t_bin_trav,
        [
            ("rvc-venv/bin/python", "sym", "/usr/bin/python3.12"),
            ("rvc-venv/bin/python/pwned", "file", b"evil"),
            ("sep-venv/bin/python", "file", b"py"),
        ],
    )
    with pytest.raises(ValueError, match="Path traverses symlink"):
        colab_remote.restore_venv_cache(
            t_bin_trav,
            expected_sha256=sha3,
            expected_fingerprint=fp,
            actual_fingerprint=fp,
            rvc_venv=tmp_path / "out3_rvc",
            sep_venv=tmp_path / "out3_sep",
        )

    # 4) Relative symlink + regular file traversing through it -> MUST reject
    t_rel_trav = tmp_path / "rel_traverse.tar.gz"
    sha4 = _build_tar(
        t_rel_trav,
        [
            ("rvc-venv/lib64", "sym", "lib"),
            ("rvc-venv/lib64/pwned", "file", b"evil"),
            ("rvc-venv/bin/python", "file", b"py"),
            ("sep-venv/bin/python", "file", b"py"),
        ],
    )
    with pytest.raises(ValueError, match="Path traverses symlink"):
        colab_remote.restore_venv_cache(
            t_rel_trav,
            expected_sha256=sha4,
            expected_fingerprint=fp,
            actual_fingerprint=fp,
            rvc_venv=tmp_path / "out4_rvc",
            sep_venv=tmp_path / "out4_sep",
        )

    # 5) Valid bin-only absolute Python symlinks + safe relative symlink -> SUCCEEDS
    t_ok = tmp_path / "valid_venv.tar.gz"
    sha5 = _build_tar(
        t_ok,
        [
            ("rvc-venv/bin/python", "sym", "/usr/bin/python3.12"),
            ("rvc-venv/bin/python3", "sym", "python"),
            (
                "sep-venv/bin/python",
                "sym",
                "/root/.local/share/uv/python/cpython-3.12.7-linux-x86_64/bin/python3.12",
            ),
        ],
    )
    dst_rvc = tmp_path / "ok_rvc"
    dst_sep = tmp_path / "ok_sep"
    colab_remote.restore_venv_cache(
        t_ok,
        expected_sha256=sha5,
        expected_fingerprint=fp,
        actual_fingerprint=fp,
        rvc_venv=dst_rvc,
        sep_venv=dst_sep,
    )
    assert os.readlink(dst_rvc / "bin" / "python") == "/usr/bin/python3.12"
    assert os.readlink(dst_rvc / "bin" / "python3") == "python"
    assert os.readlink(dst_sep / "bin" / "python").endswith("/bin/python3.12")


def test_p0_remote_exception_scrubs_custom_opaque_token_in_stdout_and_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import contextlib
    import traceback

    opaque_tok = "opaque-custom-token-99887766"
    monkeypatch.setenv("HAKIMI_COLAB_ACTION", "setup")
    monkeypatch.setenv("HAKIMI_DRIVE_ACCESS_TOKEN", opaque_tok)

    def boom(**kwargs):
        del kwargs
        raise RuntimeError(f"upstream rejected {opaque_tok} at step 1")

    monkeypatch.setattr(colab_remote, "action_setup", boom)
    out_buf = io.StringIO()
    tb_text = ""
    exc_text = ""
    with contextlib.redirect_stdout(out_buf):
        try:
            colab_remote.main(argv=None)
        except RuntimeError as exc:
            exc_text = str(exc)
            tb_text = traceback.format_exc()

    assert opaque_tok not in out_buf.getvalue()
    assert opaque_tok not in exc_text
    assert opaque_tok not in tb_text
    assert "[REDACTED]" in out_buf.getvalue()
    assert "[REDACTED]" in exc_text
    assert opaque_tok not in colab_remote._ACTIVE_SECRETS
    monkeypatch.delenv("HAKIMI_COLAB_ACTION", raising=False)


def test_backing_mix_params_env_and_cli_passthrough(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1) colab_rvc CLI parser exposes --backing-gain-db and --skip-backing on cover & all
    parser = colab_rvc._build_parser()
    ns_default = parser.parse_args(["cover", "--source", "s.mp3", "--title", "枝垂桜"])
    assert ns_default.backing_gain_db == -14.0
    assert ns_default.skip_backing is False
    ns_custom = parser.parse_args(
        [
            "cover",
            "--source",
            "s.mp3",
            "--title",
            "枝垂桜",
            "--backing-gain-db",
            "-16.5",
            "--skip-backing",
        ]
    )
    assert ns_custom.backing_gain_db == -16.5
    assert ns_custom.skip_backing is True

    # 2) run_cover_pipeline forwards HAKIMI_BACKING_GAIN_DB & HAKIMI_SKIP_BACKING & validates inputs
    captured_envs: list[dict[str, str]] = []
    monkeypatch.setattr(colab_rvc, "provision_gpu", lambda s, gpu="L4", **kw: "L4")
    monkeypatch.setattr(colab_rvc, "stop_session", lambda _s: None)

    def fake_exec(_session: str, action: str, env_vars=None) -> str:
        if action == "cover":
            captured_envs.append(dict(env_vars or {}))
            payload = {
                "chorus": {
                    "file_id": "drv_c",
                    "name": "c.mp3",
                    "sha256": "a" * 64,
                    "size": 1000,
                    "duration_ms": 50000,
                },
                "full": {"file_id": "drv_f", "name": "f.mp3"},
                "meta": {"chorus_start_sec": 30.0},
            }
            return (
                colab_rvc.DRIVE_OUTPUTS_PREFIX
                + json.dumps(payload)
                + "\n"
                + colab_rvc.STATUS_OK_MARKER
            )
        return colab_rvc.STATUS_OK_MARKER

    monkeypatch.setattr(colab_rvc, "exec_remote_worker", fake_exec)
    colab_rvc.run_cover_pipeline(
        session="sess-mix",
        gpu_spec="L4",
        cover_dir=tmp_path / "mix",
        drive_access_token="ya29.tok",
        drive_source_file_id="src1",
        drive_pth_file_id="pth1",
        drive_index_file_id="idx1",
        drive_songs_folder_id="songs1",
        backing_gain_db=-30.0,  # clamped to -24.0
        skip_backing=True,
    )
    assert captured_envs[-1]["HAKIMI_BACKING_GAIN_DB"] == "-24.0"
    assert captured_envs[-1]["HAKIMI_SKIP_BACKING"] == "1"

    for bad_bg in (True, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            colab_rvc.run_cover_pipeline(
                session="sess-bad",
                gpu_spec="L4",
                cover_dir=tmp_path / "bad",
                backing_gain_db=bad_bg,  # type: ignore[arg-type]
            )
    with pytest.raises(ValueError):
        colab_rvc.run_cover_pipeline(
            session="sess-bad2",
            gpu_spec="L4",
            cover_dir=tmp_path / "bad2",
            skip_backing="1",  # type: ignore[arg-type]
        )

    # 3) Remote _dispatch_env reads HAKIMI_BACKING_GAIN_DB (clamped) & HAKIMI_SKIP_BACKING
    seen_cover_kwargs: list[dict[str, object]] = []

    def fake_action_cover(**kwargs):
        seen_cover_kwargs.append(dict(kwargs))
        return {
            "backing_gain_db": kwargs["backing_gain_db"],
            "skip_backing": kwargs["skip_backing"],
        }

    monkeypatch.setattr(colab_remote, "action_cover", fake_action_cover)
    monkeypatch.setenv("HAKIMI_BACKING_GAIN_DB", "-2.0")
    monkeypatch.setenv("HAKIMI_SKIP_BACKING", "1")
    colab_remote._dispatch_env("cover", drive_token="")
    assert seen_cover_kwargs[-1]["backing_gain_db"] == -6.0
    assert seen_cover_kwargs[-1]["skip_backing"] is True

    monkeypatch.setenv("HAKIMI_BACKING_GAIN_DB", "nan")
    with pytest.raises(ValueError, match="finite"):
        colab_remote._dispatch_env("cover", drive_token="")
    monkeypatch.delenv("HAKIMI_BACKING_GAIN_DB", raising=False)
    monkeypatch.delenv("HAKIMI_SKIP_BACKING", raising=False)
