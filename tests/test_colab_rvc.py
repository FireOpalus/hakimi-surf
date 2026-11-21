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
