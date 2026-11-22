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
    assert "[0:a]volume=-1.25dB,highpass=f=85,equalizer=f=6800:t=q:w=2.0:g=-2.0," in fg_no_bg
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

    fg_with_bg = colab_remote.build_mix_filter_complex(2.5, has_backing=True)
    assert "[0:a]volume=2.50dB," in fg_with_bg
    assert "[2:a]volume=-2.5dB[bg];" in fg_with_bg
    assert "[v][inst][bg]amix=inputs=3:duration=longest:normalize=0," in fg_with_bg
    assert "alimiter=level_in=1:level_out=0.92:limit=0.92:attack=5:release=60[m]" in fg_with_bg


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
        *, fail_karaoke: bool
    ) -> tuple[dict[str, object], list[list[str]], list[tuple[str, tuple[str, ...]]]]:
        case_dir = tmp_path / ("fallback" if fail_karaoke else "karaoke")
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
            applio_dir=applio_dir,
            rvc_venv=rvc_venv,
            sep_venv=sep_venv,
            out_dir=out_dir,
            chunks_dir=chunks_dir,
        )
        return meta, cmds, sep_calls

    meta_ok, cmds_ok, seps_ok = _run_cover_case(fail_karaoke=False)
    assert [m for _, m in seps_ok] == [
        colab_remote.PRIMARY_SEP_MODELS,
        colab_remote.KARAOKE_SEP_MODELS,
        colab_remote.DEREVERB_MODELS,
    ]
    infer_cmd = next(c for c in cmds_ok if "infer" in c)
    assert "--split-audio" in infer_cmd
    assert infer_cmd[infer_cmd.index("--index-rate") + 1] == "0.25"
    assert infer_cmd[infer_cmd.index("--protect") + 1] == "0.28"
    assert infer_cmd[infer_cmd.index("--volume-envelope") + 1] == "0.25"
    mix_cmd = next(c for c in cmds_ok if "-filter_complex" in c)
    assert mix_cmd.count("-i") == 3
    assert "volume=-2.00dB" in mix_cmd[mix_cmd.index("-filter_complex") + 1]
    assert "[v][inst][bg]amix=inputs=3" in mix_cmd[mix_cmd.index("-filter_complex") + 1]
    assert meta_ok["karaoke_backing_separated"] is True
    assert meta_ok["split_audio"] is True
    assert meta_ok["loudness_gain_db"] == -2.0

    meta_fb, cmds_fb, _ = _run_cover_case(fail_karaoke=True)
    mix_fb = next(c for c in cmds_fb if "-filter_complex" in c)
    assert mix_fb.count("-i") == 2
    assert "[v][inst]amix=inputs=2" in mix_fb[mix_fb.index("-filter_complex") + 1]
    assert meta_fb["karaoke_backing_separated"] is False
    assert meta_fb["split_audio"] is True


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
