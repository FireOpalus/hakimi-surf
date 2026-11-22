"""Remote worker executed on a Google Colab GPU VM via `google-colab-cli`.

Designed to be self-contained (stdlib-only) and runnable both via:
  1) `colab exec --env HAKIMI_COLAB_ACTION=<action> ... -f scripts/colab_remote_rvc.py`
  2) Direct Python invocation with CLI subcommands (for local unit tests).
"""

from __future__ import annotations

import argparse
import array
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
import wave
from pathlib import Path
from typing import Any

DEFAULT_APPLIO_DIR = Path("/content/Applio")
DEFAULT_RVC_VENV = Path("/content/rvc-venv")
DEFAULT_SEP_VENV = Path("/content/sep-venv")
DEFAULT_WORK_DIR = Path("/content/work")
DEFAULT_OUT_DIR = Path("/content/out")
DEFAULT_CHUNKS_DIR = Path("/content/out_chunks")
SETUP_MARKER = Path("/content/.hakimi_setup_done")

PRIMARY_SEP_MODELS = (
    "model_bs_roformer_ep_317_sdr_12.9755.ckpt",
    "mel_band_roformer_karaoke_aufr33_viperx_sdr_10.1956.ckpt",
    "UVR-MDX-NET-Inst_HQ_3.onnx",
)
DEREVERB_MODEL = "UVR-DeEcho-DeReverb.pth"
MAX_CHORUS_BYTES = 1_750_000
STATUS_PREFIX = "__HAKIMI_COLAB_STATUS__="

_TRAD_TO_SIMP = str.maketrans(
    "愛淚夢聲憶雲風裏這麼為說還點樣覺過來時間對會沒開關氣離聽見給讓頭臉邊難誰錯變濕積儲記",
    "爱泪梦声忆云风里这么为说还点样觉过来时间对会没开关气离听见给让头脸边难谁错变湿积储记",
)


def log(msg: str) -> None:
    print(f"[hakimi-colab] {msg}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def split_file(input_path: Path, chunk_dir: Path, chunk_mb: int = 6) -> dict[str, Any]:
    """Split a file into sequential parts with a SHA-256 manifest."""
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found for split: {input_path}")
    chunk_bytes = max(1, int(chunk_mb)) * 1024 * 1024
    if chunk_dir.exists():
        shutil.rmtree(chunk_dir)
    chunk_dir.mkdir(parents=True, exist_ok=True)

    chunks: list[str] = []
    total_size = input_path.stat().st_size
    digest = hashlib.sha256()
    with input_path.open("rb") as src:
        idx = 0
        while True:
            data = src.read(chunk_bytes)
            if not data:
                break
            digest.update(data)
            part_name = f"part_{idx:03d}"
            (chunk_dir / part_name).write_bytes(data)
            chunks.append(part_name)
            idx += 1

    if not chunks:
        part_name = "part_000"
        (chunk_dir / part_name).write_bytes(b"")
        chunks.append(part_name)

    manifest = {
        "filename": input_path.name,
        "size": total_size,
        "sha256": digest.hexdigest(),
        "chunks": chunks,
    }
    (chunk_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    log(f"Split {input_path.name} ({total_size} B) into {len(chunks)} chunk(s) at {chunk_dir}")
    return manifest


def reassemble_chunks(
    chunk_dir: Path,
    output_path: Path,
    expected_sha256: str = "",
    *,
    remove_chunks: bool = True,
) -> Path:
    """Reassemble `part_XXX` files into `output_path` and verify SHA-256."""
    if not chunk_dir.is_dir():
        raise FileNotFoundError(f"Chunk directory not found: {chunk_dir}")
    part_files = sorted(
        p for p in chunk_dir.iterdir() if p.is_file() and p.name.startswith("part_")
    )
    if not part_files:
        raise FileNotFoundError(f"No chunk parts found in {chunk_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    digest = hashlib.sha256()
    with tmp_path.open("wb") as dst:
        for part in part_files:
            with part.open("rb") as src:
                while buf := src.read(1024 * 1024):
                    digest.update(buf)
                    dst.write(buf)

    actual = digest.hexdigest()
    if expected_sha256 and actual.lower() != expected_sha256.strip().lower():
        tmp_path.unlink(missing_ok=True)
        raise ValueError(
            f"SHA-256 mismatch for {output_path}: expected {expected_sha256}, got {actual}"
        )

    tmp_path.replace(output_path)
    if remove_chunks:
        shutil.rmtree(chunk_dir, ignore_errors=True)
    log(f"Reassembled {output_path} ({output_path.stat().st_size} B, sha256={actual[:12]}...)")
    return output_path


def _run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    merged_env["MPLBACKEND"] = "Agg"
    if env:
        merged_env.update(env)
    log(f"$ {' '.join(cmd)}")
    if capture:
        return subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=merged_env,
            text=True,
            errors="replace",
            capture_output=True,
            check=check,
        )
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        lines.append(line)
    rc = proc.wait()
    output = "".join(lines)
    if check and rc != 0:
        raise subprocess.CalledProcessError(rc, cmd, output=output)
    return subprocess.CompletedProcess(cmd, rc, stdout=output, stderr="")


def _find_uv() -> str:
    for candidate in (
        shutil.which("uv"),
        str(Path.home() / ".local" / "bin" / "uv"),
        str(Path.home() / ".cargo" / "bin" / "uv"),
    ):
        if candidate and Path(candidate).is_file():
            return candidate
    return ""


def _ensure_uv() -> str:
    uv_bin = _find_uv()
    if uv_bin:
        return uv_bin
    log("Installing uv...")
    proc = _run(
        ["bash", "-lc", "curl -LsSf https://astral.sh/uv/install.sh | sh"],
        check=False,
    )
    if proc.returncode == 0:
        uv_bin = _find_uv()
        if uv_bin:
            return uv_bin
    _run([sys.executable, "-m", "pip", "install", "-q", "uv"])
    uv_bin = _find_uv()
    if not uv_bin:
        raise RuntimeError("Failed to install uv on Colab runtime")
    return uv_bin


def _ensure_ffmpeg() -> None:
    if shutil.which("ffmpeg") and shutil.which("ffprobe"):
        return
    log("Installing ffmpeg via apt-get...")
    _run(["apt-get", "update", "-qq"])
    _run(["apt-get", "install", "-y", "-qq", "ffmpeg"])


def _safe_extract_tar(tar_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_root = dest_dir.resolve()
    with tarfile.open(tar_path, "r:*") as tf:
        for member in tf.getmembers():
            if member.issym() or member.islnk():
                raise ValueError(f"Refusing symlink in dataset archive: {member.name}")
            target = (dest_root / member.name).resolve()
            if not target.is_relative_to(dest_root):
                raise ValueError(f"Unsafe path traversal in tar archive: {member.name}")
        if hasattr(tarfile, "data_filter"):
            tf.extractall(dest_root, filter="data")
        else:
            tf.extractall(dest_root)


def _epoch_from_pth_name(name: str, model_name: str) -> int:
    match = re.match(rf"^{re.escape(model_name)}_(\d+)e_\d+s\.pth$", name)
    if match:
        return int(match.group(1))
    match_alt = re.search(r"_(\d+)e(?:_|\.)", name)
    return int(match_alt.group(1)) if match_alt else -1


def find_trained_artifacts(
    logs_dir: Path,
    model_name: str,
    target_epochs: int | None = None,
) -> tuple[Path, Path | None]:
    """Locate the final exported `.pth` (excluding G_/D_/f0 checkpoints) and `.index`."""
    exp_dir = logs_dir / model_name
    search_roots = [exp_dir] if exp_dir.is_dir() else []
    weights_dir = logs_dir.parent / "assets" / "weights"
    if weights_dir.is_dir():
        search_roots.append(weights_dir)

    candidates: list[Path] = []
    for root in search_roots:
        for p in root.rglob("*.pth"):
            fname = p.name
            if fname.startswith(("G_", "D_", "f0", "pretrained")):
                continue
            if fname == f"{model_name}.pth" or fname.startswith(f"{model_name}_"):
                candidates.append(p)

    if not candidates:
        raise FileNotFoundError(
            f"No trained RVC .pth model found for '{model_name}' under {logs_dir}"
        )

    def _score(path: Path) -> tuple[int, int, float]:
        ep = _epoch_from_pth_name(path.name, model_name)
        exact = 1 if (target_epochs is not None and ep == target_epochs) else 0
        return (exact, ep, path.stat().st_mtime)

    best_pth = max(candidates, key=_score)

    index_candidates: list[Path] = []
    if exp_dir.is_dir():
        for p in exp_dir.glob("*.index"):
            if p.name.startswith("added_") or p.name == f"{model_name}.index":
                index_candidates.append(p)
        if not index_candidates:
            index_candidates = list(exp_dir.glob("*.index"))

    best_index: Path | None = None
    if index_candidates:
        best_index = max(
            index_candidates,
            key=lambda p: (
                1 if p.name.startswith("added_") else 0,
                p.stat().st_mtime,
            ),
        )
    return best_pth, best_index


def normalize_zh_text(text: str) -> str:
    """Normalize Chinese lyric snippet for simplified/traditional substring matching."""
    translated = text.translate(_TRAD_TO_SIMP)
    return re.sub(r"[\s\W_]+", "", translated).lower()


def compute_wav_rms_envelope(
    wav_path: Path,
    *,
    bin_sec: float = 1.0,
) -> tuple[list[float], float]:
    """Compute per-bin RMS vocal energy envelope from a PCM WAV without external deps."""
    with wave.open(str(wav_path), "rb") as wf:
        channels = max(1, wf.getnchannels())
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        if framerate <= 0 or nframes <= 0:
            return [], 0.0
        total_sec = nframes / float(framerate)
        frames_per_bin = max(1, int(round(framerate * max(0.25, bin_sec))))
        rms_bins: list[float] = []
        # Stride keeps pure-Python RMS fast even on long 44.1 kHz stereo stems.
        stride = max(1, (framerate * channels) // 1000)

        while True:
            raw = wf.readframes(frames_per_bin)
            if not raw:
                break
            if sampwidth == 2:
                samples = array.array("h")
                samples.frombytes(raw[: len(raw) - (len(raw) % 2)])
                if sys.byteorder == "big":
                    samples.byteswap()
                sampled = samples[::stride]
                if not sampled:
                    rms_bins.append(0.0)
                    continue
                sq_sum = sum(float(s) * float(s) for s in sampled)
                rms_bins.append(math.sqrt(sq_sum / len(sampled)))
            elif sampwidth == 1:
                sampled_u8 = raw[::stride]
                if not sampled_u8:
                    rms_bins.append(0.0)
                    continue
                sq_sum = sum(float(b - 128) * float(b - 128) * 65536.0 for b in sampled_u8)
                rms_bins.append(math.sqrt(sq_sum / len(sampled_u8)))
            else:
                step = sampwidth * stride
                total_samples = 0
                sq_sum = 0.0
                for idx in range(0, len(raw) - sampwidth + 1, step):
                    val = int.from_bytes(raw[idx : idx + sampwidth], "little", signed=True)
                    norm = float(val) / float(1 << max(0, (sampwidth - 2) * 8))
                    sq_sum += norm * norm
                    total_samples += 1
                rms_bins.append(math.sqrt(sq_sum / total_samples) if total_samples else 0.0)
    return rms_bins, total_sec


def detect_vocal_energy_chorus_start(
    vocals_wav: Path,
    total_duration_sec: float,
    clip_duration_sec: float,
    *,
    intro_skip_ratio: float = 0.12,
) -> float | None:
    """Locate the highest sustained vocal RMS energy window as a DSP chorus fallback."""
    try:
        bins, wav_dur = compute_wav_rms_envelope(vocals_wav, bin_sec=1.0)
    except Exception as exc:
        log(f"DSP vocal RMS envelope unavailable ({type(exc).__name__}: {exc})")
        return None
    if not bins:
        return None
    peak_rms = max(bins)
    if peak_rms <= 10.0:
        return None

    effective_total = total_duration_sec if total_duration_sec > 0 else wav_dur
    win_bins = max(8, min(len(bins), int(round(min(clip_duration_sec, effective_total)))))
    if len(bins) <= win_bins:
        return 0.0

    max_idx = len(bins) - win_bins
    min_idx = min(max_idx, int(round(min(15.0, len(bins) * intro_skip_ratio))))
    if max_idx <= 4:
        min_idx = 0

    prefix = [0.0] * (len(bins) + 1)
    for idx, val in enumerate(bins):
        prefix[idx + 1] = prefix[idx] + val

    best_idx = min_idx
    best_score = -1.0
    for start_idx in range(min_idx, max_idx + 1):
        score = prefix[start_idx + win_bins] - prefix[start_idx]
        if score > best_score:
            best_score = score
            best_idx = start_idx

    # Trim leading near-silence inside the selected window up to 5 seconds forward.
    win_peak = max(bins[best_idx : best_idx + win_bins])
    onset_thresh = win_peak * 0.35
    chosen_idx = best_idx
    for offset in range(min(5, max_idx - best_idx + 1)):
        if bins[best_idx + offset] >= onset_thresh:
            chosen_idx = best_idx + offset
            break

    return _clamp_start(float(chosen_idx), effective_total, clip_duration_sec)


def find_chorus_start_sec(
    segments: list[dict[str, Any]],
    anchor: str,
    fallback_start_sec: float,
    total_duration_sec: float,
    clip_duration_sec: float,
    *,
    vocal_wav: Path | None = None,
) -> tuple[float, str]:
    """Resolve chorus trim start timestamp from ASR segments, DSP vocal RMS, or safe fallback."""
    clean_anchor = normalize_zh_text(anchor)
    if clean_anchor and segments:
        for seg in segments:
            seg_text = normalize_zh_text(str(seg.get("text", "")))
            if clean_anchor in seg_text:
                raw_start = max(0.0, float(seg.get("start", 0.0)) - 0.35)
                return _clamp_start(raw_start, total_duration_sec, clip_duration_sec), (
                    f"matched_anchor:{seg.get('text', '')}"
                )

        if len(clean_anchor) >= 3:
            substrings = [clean_anchor[i : i + 3] for i in range(len(clean_anchor) - 2)]
            for seg in segments:
                seg_text = normalize_zh_text(str(seg.get("text", "")))
                if any(sub in seg_text for sub in substrings):
                    raw_start = max(0.0, float(seg.get("start", 0.0)) - 0.35)
                    return _clamp_start(raw_start, total_duration_sec, clip_duration_sec), (
                        f"fuzzy_anchor:{seg.get('text', '')}"
                    )

    if vocal_wav is not None and vocal_wav.is_file():
        dsp_start = detect_vocal_energy_chorus_start(
            vocal_wav,
            total_duration_sec,
            clip_duration_sec,
        )
        if dsp_start is not None:
            return dsp_start, "dsp_vocal_rms"

    return _clamp_start(fallback_start_sec, total_duration_sec, clip_duration_sec), "fallback"


def _clamp_start(start_sec: float, total_duration_sec: float, clip_duration_sec: float) -> float:
    if total_duration_sec <= 0:
        return max(0.0, float(start_sec))
    max_start = max(0.0, total_duration_sec - min(clip_duration_sec, total_duration_sec))
    return round(min(max(0.0, float(start_sec)), max_start), 2)


def _probe_duration_sec(path: Path) -> float:
    proc = _run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        capture=True,
    )
    return float(proc.stdout.strip())


def _detect_mean_volume_db(path: Path) -> float:
    proc = _run(
        [
            "ffmpeg",
            "-hide_banner",
            "-i",
            str(path),
            "-af",
            "volumedetect",
            "-f",
            "null",
            "-",
        ],
        check=False,
        capture=True,
    )
    text = (proc.stderr or "") + "\n" + (proc.stdout or "")
    match = re.search(r"mean_volume:\s*([-+]?\d+(?:\.\d+)?)\s*dB", text)
    if not match:
        return -18.0
    return float(match.group(1))


def action_setup(
    *,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
    force: bool = False,
    mode: str = "all",
) -> None:
    """Provision GPU check, ffmpeg, uv, Applio + rvc-venv, and audio-separator sep-venv."""
    if SETUP_MARKER.is_file() and not force and rvc_venv.exists() and sep_venv.exists():
        log("Setup marker already present; skipping environment reinstall.")
        return

    gpu_proc = _run(["nvidia-smi"], check=False, capture=True)
    if gpu_proc.returncode != 0:
        raise RuntimeError(
            "CUDA GPU not available on this Colab runtime (`nvidia-smi` failed). "
            "Please allocate a GPU runtime (T4/L4)."
        )
    log((gpu_proc.stdout or "").splitlines()[0] if gpu_proc.stdout else "GPU verified.")

    _ensure_ffmpeg()
    uv_bin = _ensure_uv()

    if not (applio_dir / "core.py").is_file():
        log(f"Cloning Applio into {applio_dir}...")
        _run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "https://github.com/IAHispano/Applio.git",
                str(applio_dir),
            ]
        )

    if not (rvc_venv / "bin" / "python").is_file():
        log(f"Creating {rvc_venv} (Python 3.12)...")
        _run([uv_bin, "venv", "--python", "3.12", str(rvc_venv)])
    rvc_py = str(rvc_venv / "bin" / "python")
    req_file = applio_dir / "requirements-engine.txt"
    log("Installing Applio engine dependencies into rvc-venv...")
    _run([uv_bin, "pip", "install", "--python", rvc_py, "-r", str(req_file), "scikit-learn"])

    pretrain_flag = "--no-pretraineds-hifigan" if mode == "cover" else "--pretraineds-hifigan"
    log("Downloading Applio pretrained weights & RMVPE/ContentVec...")
    _run(
        [
            rvc_py,
            str(applio_dir / "core.py"),
            "prerequisites",
            pretrain_flag,
            "--models",
            "--no-exe",
        ],
        cwd=applio_dir,
        env={"APPLIO_ROOT": str(applio_dir), "APPLIO_LOGS_DIR": str(applio_dir / "logs")},
    )

    if not (sep_venv / "bin" / "python").is_file():
        log(f"Creating {sep_venv} (Python 3.12)...")
        _run([uv_bin, "venv", "--python", "3.12", str(sep_venv)])
    sep_py = str(sep_venv / "bin" / "python")
    log("Installing audio-separator[gpu] into sep-venv...")
    _run(
        [
            uv_bin,
            "pip",
            "install",
            "--python",
            sep_py,
            "audio-separator[gpu]",
            "audioread",
            "librosa<1.0",
            "soundfile",
            "numpy",
        ]
    )
    _run([sep_py, "-c", "from audio_separator.separator import Separator; print('sep_import_ok')"])
    log("Installing optional faster-whisper into sep-venv for lyric anchor detection...")
    fw_proc = _run(
        [uv_bin, "pip", "install", "--python", sep_py, "faster-whisper"],
        check=False,
    )
    if fw_proc.returncode != 0:
        log("Warning: faster-whisper install failed; cover will use fallback chorus timestamp.")

    SETUP_MARKER.write_text("ok\n", encoding="utf-8")
    log("Colab GPU environment setup complete.")


def action_train(
    *,
    model_name: str = "hakimi",
    dataset_tar: Path = DEFAULT_WORK_DIR / "dataset.tar.gz",
    epochs: int = 250,
    save_every: int = 50,
    batch_size: int = 8,
    sample_rate: int = 40000,
    chunk_mb: int = 6,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    out_dir: Path = DEFAULT_OUT_DIR,
    chunks_dir: Path = DEFAULT_CHUNKS_DIR,
) -> tuple[Path, Path]:
    """Run Applio preprocess -> extract -> train -> index -> chunked export."""
    if not dataset_tar.is_file():
        raise FileNotFoundError(f"Dataset archive not found: {dataset_tar}")

    dataset_dir = DEFAULT_WORK_DIR / "dataset"
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    _safe_extract_tar(dataset_tar, dataset_dir)
    wav_files = sorted(p for p in dataset_dir.rglob("*.wav") if p.is_file())
    if len(wav_files) < 10:
        raise ValueError(f"Expected >= 10 .wav files in dataset, found {len(wav_files)}")
    log(f"Extracted {len(wav_files)} training WAV files into {dataset_dir}")

    rvc_py = str(rvc_venv / "bin" / "python")
    logs_dir = applio_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    cpu_cores = str(max(1, min(os.cpu_count() or 2, 8)))
    applio_env = {
        "APPLIO_ROOT": str(applio_dir),
        "APPLIO_LOGS_DIR": str(logs_dir),
    }

    log(f"Step 1/3: Applio preprocess ({model_name}, {sample_rate}Hz)...")
    prep_proc = _run(
        [
            rvc_py,
            str(applio_dir / "core.py"),
            "preprocess",
            "--model-name",
            model_name,
            "--dataset-path",
            str(dataset_dir),
            "--sample-rate",
            str(sample_rate),
            "--cpu-cores",
            cpu_cores,
            "--cut-preprocess",
            "Automatic",
            "--chunk-len",
            "3.0",
            "--overlap-len",
            "0.3",
            "--normalization-mode",
            "post",
        ],
        cwd=applio_dir,
        env=applio_env,
    )
    exp_dir = logs_dir / model_name
    sliced_wavs = list((exp_dir / "sliced_audios").glob("*.wav"))
    if "preprocessing failed" in prep_proc.stdout.lower() or not sliced_wavs:
        raise RuntimeError(
            f"Applio preprocess produced {len(sliced_wavs)} sliced clips.\n"
            f"Tail:\n{prep_proc.stdout[-1500:]}"
        )
    log(f"Preprocess produced {len(sliced_wavs)} sliced WAV clips.")

    log("Step 2/3: Applio extract (RMVPE + ContentVec)...")
    ext_proc = _run(
        [
            rvc_py,
            str(applio_dir / "core.py"),
            "extract",
            "--model-name",
            model_name,
            "--f0-method",
            "rmvpe",
            "--cpu-cores",
            cpu_cores,
            "--gpu",
            "0",
            "--sample-rate",
            str(sample_rate),
            "--embedder-model",
            "contentvec",
            "--include-mutes",
            "2",
        ],
        cwd=applio_dir,
        env=applio_env,
    )
    extracted_npys = list((exp_dir / "extracted").glob("*.npy"))
    if (
        "extraction failed" in ext_proc.stdout.lower()
        or not (exp_dir / "filelist.txt").is_file()
        or not extracted_npys
    ):
        raise RuntimeError(
            f"Applio extract failed (npy={len(extracted_npys)}, "
            f"filelist={(exp_dir / 'filelist.txt').is_file()}).\n"
            f"Tail:\n{ext_proc.stdout[-1500:]}"
        )
    log(f"Extract produced {len(extracted_npys)} feature vectors + filelist.txt.")

    log(f"Step 3/3: Applio train (epochs={epochs}, batch_size={batch_size})...")
    train_proc = _run(
        [
            rvc_py,
            str(applio_dir / "core.py"),
            "train",
            "--model-name",
            model_name,
            "--vocoder",
            "HiFi-GAN",
            "--save-every-epoch",
            str(save_every),
            "--save-only-latest",
            "--save-every-weights",
            "--total-epoch",
            str(epochs),
            "--sample-rate",
            str(sample_rate),
            "--batch-size",
            str(batch_size),
            "--gpu",
            "0",
            "--pretrained",
            "--cache-data-in-gpu",
            "--index-algorithm",
            "Auto",
        ],
        cwd=applio_dir,
        env=applio_env,
    )
    if "training failed" in train_proc.stdout.lower():
        raise RuntimeError(f"Applio train failed.\nTail:\n{train_proc.stdout[-1500:]}")

    pth_src, idx_src = find_trained_artifacts(logs_dir, model_name, target_epochs=epochs)
    if idx_src is None:
        log("Index file not emitted automatically; running explicit `core.py index`...")
        _run(
            [
                rvc_py,
                str(applio_dir / "core.py"),
                "index",
                "--model-name",
                model_name,
                "--index-algorithm",
                "Auto",
            ],
            cwd=applio_dir,
            env=applio_env,
        )
        pth_src, idx_src = find_trained_artifacts(logs_dir, model_name, target_epochs=epochs)
    if idx_src is None:
        raise FileNotFoundError(f"Failed to locate .index file for {model_name}")

    out_dir.mkdir(parents=True, exist_ok=True)
    final_pth = out_dir / f"{model_name}.pth"
    final_idx = out_dir / f"{model_name}.index"
    shutil.copy2(pth_src, final_pth)
    shutil.copy2(idx_src, final_idx)
    log(f"Saved trained model: {final_pth} ({final_pth.stat().st_size} B)")
    log(f"Saved trained index: {final_idx} ({final_idx.stat().st_size} B)")

    split_file(final_pth, chunks_dir / f"{model_name}_pth", chunk_mb=chunk_mb)
    split_file(final_idx, chunks_dir / f"{model_name}_index", chunk_mb=chunk_mb)
    return final_pth, final_idx


def _separate_with_sep_venv(
    sep_py: str,
    input_wav: Path,
    sep_out_dir: Path,
    model_names: tuple[str, ...],
    *,
    is_dereverb: bool = False,
) -> tuple[Path, Path | None]:
    sep_out_dir.mkdir(parents=True, exist_ok=True)
    helper = DEFAULT_WORK_DIR / "_run_audio_sep.py"
    helper.parent.mkdir(parents=True, exist_ok=True)
    helper.write_text(
        """import json, sys
from pathlib import Path
from audio_separator.separator import Separator

audio_path, out_dir, models_json, model_dir = sys.argv[1:5]
models = json.loads(models_json)
last_err = None
for m in models:
    try:
        sep = Separator(output_dir=out_dir, output_format="WAV", model_file_dir=model_dir)
        sep.load_model(model_filename=m)
        files = sep.separate(audio_path)
        resolved = [str(Path(out_dir, f)) if not Path(f).is_absolute() else f for f in files]
        print("__SEP_FILES__=" + json.dumps(resolved))
        print("__SEP_MODEL__=" + m)
        sys.exit(0)
    except Exception as exc:
        last_err = exc
        print(f"[sep] Model {m} failed: {exc}", file=sys.stderr)
raise RuntimeError(f"All separation models failed: {last_err}")
""",
        encoding="utf-8",
    )
    proc = _run(
        [
            sep_py,
            str(helper),
            str(input_wav),
            str(sep_out_dir),
            json.dumps(list(model_names)),
            "/tmp/audio-separator-models",
        ],
        capture=True,
        check=False,
    )
    if proc.stdout:
        print(proc.stdout, end="", flush=True)
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr, flush=True)
    if proc.returncode != 0:
        raise RuntimeError(f"audio-separator failed with exit {proc.returncode}")

    files: list[Path] = []
    for line in proc.stdout.splitlines():
        if line.startswith("__SEP_FILES__="):
            files = [Path(p) for p in json.loads(line.split("=", 1)[1])]
            break
    if not files:
        files = sorted(sep_out_dir.glob("*.wav"))
    if not files:
        raise FileNotFoundError(f"No separated stems written in {sep_out_dir}")

    if is_dereverb:
        dry = next(
            (
                p
                for p in files
                if any(k in p.name.lower() for k in ("no reverb", "no echo", "noreverb", "noecho"))
            ),
            files[0],
        )
        return dry, None

    vocals = next((p for p in files if "vocal" in p.name.lower()), None)
    inst = next(
        (p for p in files if "instrumental" in p.name.lower() or "no vocal" in p.name.lower()),
        None,
    )
    if vocals is None or inst is None:
        if len(files) >= 2:
            vocals = vocals or files[0]
            inst = inst or files[1]
        else:
            raise FileNotFoundError(f"Could not resolve vocals/instrumental stems from {files}")
    return vocals, inst


def _transcribe_for_chorus(sep_py: str, dry_vocals_wav: Path) -> list[dict[str, Any]]:
    script = """import json, subprocess, sys
import numpy as np
from faster_whisper import WhisperModel

def _load_audio_16k(path):
    proc = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path,
            "-ar",
            "16000",
            "-ac",
            "1",
            "-f",
            "f32le",
            "-",
        ],
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout:
        err_tail = (proc.stderr or b"").decode("utf-8", errors="replace").strip()[-240:]
        raise RuntimeError(f"ffmpeg 16k decode failed (rc={proc.returncode}): {err_tail}")
    return np.frombuffer(proc.stdout, dtype=np.float32).copy()

audio = _load_audio_16k(sys.argv[1])
if audio.size == 0:
    print("[whisper] empty audio after 16k decode.", file=sys.stderr)
    print("__WHISPER_SEGS__=[]")
    sys.exit(0)

def _run_model(device, compute_type):
    model = WhisperModel("small", device=device, compute_type=compute_type)
    try:
        segments, info = model.transcribe(audio, vad_filter=True)
        return list(segments), info
    except Exception as vad_exc:
        print(
            f"[whisper] vad_filter=True failed on {device} "
            f"({type(vad_exc).__name__}: {vad_exc}); retrying without VAD.",
            file=sys.stderr,
        )
        segments, info = model.transcribe(audio, vad_filter=False)
        return list(segments), info

seg_list = None
info = None
used_device = "cuda"
try:
    seg_list, info = _run_model("cuda", "float16")
except Exception as cuda_exc:
    print(
        f"[whisper] CUDA transcribe failed ({type(cuda_exc).__name__}: {cuda_exc}); "
        "falling back to CPU int8.",
        file=sys.stderr,
    )
    used_device = "cpu"
    seg_list, info = _run_model("cpu", "int8")

out = [{"start": float(s.start), "end": float(s.end), "text": s.text} for s in seg_list]
detected_lang = getattr(info, "language", "unknown") or "unknown"
print(
    f"[whisper] transcribed {len(out)} segment(s) on {used_device} (lang={detected_lang}).",
    file=sys.stderr,
)
print("__WHISPER_SEGS__=" + json.dumps(out, ensure_ascii=False))
"""
    proc = _run([sep_py, "-c", script, str(dry_vocals_wav)], check=False, capture=True)
    if proc.stderr:
        for line in proc.stderr.strip().splitlines():
            log(f"whisper stderr: {line}")
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()[-400:]
        log(
            f"faster-whisper skipped/failed (exit={proc.returncode}); "
            f"proceeding to DSP vocal RMS / fallback. detail={tail}"
        )
        return []
    for line in proc.stdout.splitlines():
        if line.startswith("__WHISPER_SEGS__="):
            return json.loads(line.split("=", 1)[1])
    log("faster-whisper produced no __WHISPER_SEGS__ marker; proceeding to DSP / fallback.")
    return []


def action_cover(
    *,
    source_audio: Path,
    pth_path: Path,
    index_path: Path,
    pitch: int = 0,
    index_rate: float = 0.55,
    protect: float = 0.33,
    volume_envelope: float = 0.85,
    start_sec: str = "auto",
    duration_sec: float = 50.0,
    chorus_anchor: str = "",
    fallback_start_sec: float = 60.0,
    chunk_mb: int = 6,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
    out_dir: Path = DEFAULT_OUT_DIR,
    chunks_dir: Path = DEFAULT_CHUNKS_DIR,
) -> dict[str, Any]:
    """Separate stems, run RVC voice conversion, loudness-match mix, and trim chorus."""
    if not source_audio.is_file():
        raise FileNotFoundError(f"Source song audio not found: {source_audio}")
    if not pth_path.is_file():
        raise FileNotFoundError(f"RVC .pth model not found: {pth_path}")
    if not index_path.is_file():
        raise FileNotFoundError(f"RVC .index file not found: {index_path}")

    work_cover = DEFAULT_WORK_DIR / "cover_work"
    if work_cover.exists():
        shutil.rmtree(work_cover)
    work_cover.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    source_wav = work_cover / "source.wav"
    log(f"Normalizing source audio {source_audio} -> {source_wav}...")
    _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(source_audio),
            "-ar",
            "44100",
            "-ac",
            "2",
            "-c:a",
            "pcm_s16le",
            str(source_wav),
        ]
    )
    total_duration = _probe_duration_sec(source_wav)
    log(f"Source track duration: {total_duration:.2f}s")

    sep_py = str(sep_venv / "bin" / "python")
    rvc_py = str(rvc_venv / "bin" / "python")

    log("Separating vocals and instrumental...")
    vocals_wav, inst_wav = _separate_with_sep_venv(
        sep_py, source_wav, work_cover / "stems", PRIMARY_SEP_MODELS
    )
    assert inst_wav is not None

    dry_vocals_wav = vocals_wav
    try:
        log("Running best-effort de-echo/de-reverb pass on vocals...")
        dry_vocals_wav, _ = _separate_with_sep_venv(
            sep_py, vocals_wav, work_cover / "dereverb", (DEREVERB_MODEL,), is_dereverb=True
        )
    except Exception as exc:
        log(f"De-reverb skipped ({exc}); proceeding with primary vocals stem.")
        dry_vocals_wav = vocals_wav

    converted_wav = work_cover / "vocals_hakimi.wav"
    log(f"Running Applio RVC inference (pitch={pitch}, index_rate={index_rate})...")
    _run(
        [
            rvc_py,
            str(applio_dir / "core.py"),
            "infer",
            "--input-path",
            str(dry_vocals_wav),
            "--output-path",
            str(converted_wav),
            "--pth-path",
            str(pth_path),
            "--index-path",
            str(index_path),
            "--pitch",
            str(pitch),
            "--index-rate",
            str(index_rate),
            "--volume-envelope",
            str(volume_envelope),
            "--protect",
            str(protect),
            "--f0-method",
            "rmvpe",
            "--embedder-model",
            "contentvec",
            "--export-format",
            "WAV",
        ],
        cwd=applio_dir,
        env={"APPLIO_ROOT": str(applio_dir), "APPLIO_LOGS_DIR": str(applio_dir / "logs")},
    )
    if not converted_wav.is_file():
        raise FileNotFoundError(f"RVC inference output missing: {converted_wav}")

    orig_db = _detect_mean_volume_db(dry_vocals_wav)
    conv_db = _detect_mean_volume_db(converted_wav)
    gain_db = max(-12.0, min(12.0, (orig_db - conv_db) + 1.0))
    log(f"Loudness match: orig={orig_db:.1f} dB, conv={conv_db:.1f} dB -> gain={gain_db:+.2f} dB")

    cover_full_mp3 = out_dir / "cover_full.mp3"
    filter_complex = (
        f"[0:a]volume={gain_db:.2f}dB,highpass=f=80,aecho=0.8:0.88:45:0.18[v];"
        "[v][1:a]amix=inputs=2:duration=longest:normalize=0,alimiter=limit=0.95[m]"
    )
    _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(converted_wav),
            "-i",
            str(inst_wav),
            "-filter_complex",
            filter_complex,
            "-map",
            "[m]",
            "-map_metadata",
            "-1",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "192k",
            "-ar",
            "44100",
            "-ac",
            "2",
            str(cover_full_mp3),
        ]
    )

    clamped_dur = round(max(5.0, min(56.0, float(duration_sec))), 2)
    if total_duration > 0:
        clamped_dur = round(min(clamped_dur, max(5.0, total_duration)), 2)

    segments: list[dict[str, Any]] = []
    if str(start_sec).strip().lower() == "auto":
        segments = _transcribe_for_chorus(sep_py, dry_vocals_wav)
        if segments:
            (out_dir / "lyrics_timestamps.json").write_text(
                json.dumps(segments, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        chosen_start, start_reason = find_chorus_start_sec(
            segments,
            chorus_anchor,
            fallback_start_sec,
            total_duration,
            clamped_dur,
            vocal_wav=dry_vocals_wav,
        )
    else:
        chosen_start = _clamp_start(float(start_sec), total_duration, clamped_dur)
        start_reason = "explicit"

    fade_out_start = max(0.0, clamped_dur - 1.5)
    cover_chorus_mp3 = out_dir / "cover_chorus.mp3"
    _run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            f"{chosen_start:.2f}",
            "-t",
            f"{clamped_dur:.2f}",
            "-i",
            str(cover_full_mp3),
            "-af",
            f"afade=t=in:st=0:d=0.3,afade=t=out:st={fade_out_start:.2f}:d=1.5",
            "-map_metadata",
            "-1",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "128k",
            "-ar",
            "44100",
            "-ac",
            "2",
            str(cover_chorus_mp3),
        ]
    )
    if cover_chorus_mp3.stat().st_size > MAX_CHORUS_BYTES:
        log("Chorus MP3 exceeded 1.75 MB; re-encoding at 96k mono for SongLibrary headroom...")
        tmp_mono = work_cover / "cover_chorus_mono.mp3"
        _run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(cover_chorus_mp3),
                "-map_metadata",
                "-1",
                "-c:a",
                "libmp3lame",
                "-b:a",
                "96k",
                "-ar",
                "44100",
                "-ac",
                "1",
                str(tmp_mono),
            ]
        )
        tmp_mono.replace(cover_chorus_mp3)

    meta = {
        "source_duration_sec": round(total_duration, 2),
        "chorus_start_sec": chosen_start,
        "chorus_duration_sec": clamped_dur,
        "chorus_start_reason": start_reason,
        "pitch": pitch,
        "index_rate": index_rate,
        "protect": protect,
        "volume_envelope": volume_envelope,
        "loudness_gain_db": round(gain_db, 2),
        "cover_full_bytes": cover_full_mp3.stat().st_size,
        "cover_chorus_bytes": cover_chorus_mp3.stat().st_size,
    }
    (out_dir / "cover_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    split_file(cover_full_mp3, chunks_dir / "cover_full", chunk_mb=chunk_mb)
    split_file(cover_chorus_mp3, chunks_dir / "cover_chorus", chunk_mb=chunk_mb)
    log(f"Cover ready: start={chosen_start}s dur={clamped_dur}s ({start_reason})")
    return meta


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


DRIVE_OUTPUTS_PREFIX = "__HAKIMI_DRIVE_OUTPUTS__="


def _drive_download_to_path(access_token: str, file_id: str, dest_path: Path) -> Path:
    """Download a Drive file directly from Colab using a short-lived Bearer token."""
    clean_id = re.sub(r"[^A-Za-z0-9_-]", "", file_id.strip())
    if not clean_id or clean_id != file_id.strip():
        raise ValueError("Invalid Drive file ID")
    url = f"https://www.googleapis.com/drive/v3/files/{clean_id}?alt=media&supportsAllDrives=true"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    with urllib.request.urlopen(req, timeout=120) as resp, tmp_path.open("wb") as handle:
        while chunk := resp.read(1024 * 1024):
            handle.write(chunk)
    tmp_path.replace(dest_path)
    log(f"Direct Drive download complete -> {dest_path} ({dest_path.stat().st_size} B)")
    return dest_path


def _drive_upload_from_path(
    access_token: str,
    folder_id: str,
    local_path: Path,
    drive_name: str,
    mime_type: str,
) -> dict[str, Any]:
    """Upload a local output artifact directly from Colab to Google Drive."""
    clean_folder = re.sub(r"[^A-Za-z0-9_-]", "", folder_id.strip())
    if not clean_folder or clean_folder != folder_id.strip():
        raise ValueError("Invalid Drive folder ID")
    data = local_path.read_bytes()
    if len(data) <= 4 * 1024 * 1024:
        boundary = f"hakimi_{uuid.uuid4().hex}"
        meta = json.dumps(
            {"name": drive_name, "parents": [clean_folder]}, ensure_ascii=False
        ).encode("utf-8")
        body = b"".join(
            [
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode(),
                meta,
                f"\r\n--{boundary}\r\nContent-Type: {mime_type}\r\n\r\n".encode(),
                data,
                f"\r\n--{boundary}--\r\n".encode(),
            ]
        )
        req = urllib.request.Request(
            "https://www.googleapis.com/upload/drive/v3/files"
            "?uploadType=multipart&supportsAllDrives=true&fields=id,name,size",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": f"multipart/related; boundary={boundary}",
            },
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        log(f"Direct Drive upload complete: {drive_name} ({len(data)} B)")
        return {"file_id": str(result["id"]), "name": str(result.get("name") or drive_name)}

    init_req = urllib.request.Request(
        "https://www.googleapis.com/upload/drive/v3/files"
        "?uploadType=resumable&supportsAllDrives=true&fields=id,name,size",
        data=json.dumps({"name": drive_name, "parents": [clean_folder]}, ensure_ascii=False).encode(
            "utf-8"
        ),
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": mime_type,
            "X-Upload-Content-Length": str(len(data)),
        },
    )
    with urllib.request.urlopen(init_req, timeout=60) as resp:
        upload_url = resp.headers.get("Location", "")
    if not upload_url.startswith("https://www.googleapis.com/"):
        raise RuntimeError("Drive resumable upload init returned invalid Location")

    chunk_size = 4 * 1024 * 1024
    offset = 0
    total = len(data)
    while offset < total:
        end = min(total, offset + chunk_size)
        chunk = data[offset:end]
        put_req = urllib.request.Request(
            upload_url,
            data=chunk,
            method="PUT",
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {offset}-{end - 1}/{total}",
                "Content-Type": mime_type,
            },
        )
        try:
            with urllib.request.urlopen(put_req, timeout=120) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                log(f"Direct Drive upload complete: {drive_name} ({total} B)")
                return {
                    "file_id": str(result["id"]),
                    "name": str(result.get("name") or drive_name),
                }
        except urllib.error.HTTPError as exc:
            if exc.code == 308:
                offset = end
                continue
            raise
    raise RuntimeError("Direct Drive resumable upload ended without completion")


def _dispatch_env(action: str) -> None:
    drive_token = os.environ.get("HAKIMI_DRIVE_ACCESS_TOKEN", "").strip()
    if action == "reassemble":
        reassemble_chunks(
            Path(os.environ["HAKIMI_CHUNK_DIR"]),
            Path(os.environ["HAKIMI_OUTPUT_PATH"]),
            os.environ.get("HAKIMI_EXPECTED_SHA256", ""),
        )
    elif action == "split":
        split_file(
            Path(os.environ["HAKIMI_INPUT_PATH"]),
            Path(os.environ["HAKIMI_CHUNK_DIR"]),
            _env_int("HAKIMI_CHUNK_MB", 6),
        )
    elif action == "setup":
        action_setup(
            force=os.environ.get("HAKIMI_FORCE", "").lower() in {"1", "true", "yes"},
            mode=os.environ.get("HAKIMI_SETUP_MODE", "all").strip().lower() or "all",
        )
    elif action == "train":
        model_name = os.environ.get("HAKIMI_MODEL_NAME", "hakimi")
        final_pth, final_idx = action_train(
            model_name=model_name,
            dataset_tar=Path(os.environ.get("HAKIMI_DATASET_TAR", "/content/work/dataset.tar.gz")),
            epochs=_env_int("HAKIMI_EPOCHS", 250),
            save_every=_env_int("HAKIMI_SAVE_EVERY", 50),
            batch_size=_env_int("HAKIMI_BATCH_SIZE", 8),
            sample_rate=_env_int("HAKIMI_SAMPLE_RATE", 40000),
            chunk_mb=_env_int("HAKIMI_CHUNK_MB", 6),
        )
        models_folder = os.environ.get("HAKIMI_DRIVE_MODELS_FOLDER_ID", "").strip()
        if drive_token and models_folder:
            pth_up = _drive_upload_from_path(
                drive_token, models_folder, final_pth, final_pth.name, "application/octet-stream"
            )
            idx_up = _drive_upload_from_path(
                drive_token, models_folder, final_idx, final_idx.name, "application/octet-stream"
            )
            print(
                DRIVE_OUTPUTS_PREFIX
                + json.dumps({"pth": pth_up, "index": idx_up}, ensure_ascii=False),
                flush=True,
            )
    elif action == "cover":
        source_path = Path(os.environ.get("HAKIMI_SOURCE_AUDIO", "/content/work/source_song"))
        pth_path = Path(os.environ.get("HAKIMI_PTH_PATH", "/content/out/hakimi.pth"))
        index_path = Path(os.environ.get("HAKIMI_INDEX_PATH", "/content/out/hakimi.index"))
        if drive_token:
            if src_fid := os.environ.get("HAKIMI_DRIVE_SOURCE_FILE_ID", "").strip():
                _drive_download_to_path(drive_token, src_fid, source_path)
            if pth_fid := os.environ.get("HAKIMI_DRIVE_PTH_FILE_ID", "").strip():
                _drive_download_to_path(drive_token, pth_fid, pth_path)
            if idx_fid := os.environ.get("HAKIMI_DRIVE_INDEX_FILE_ID", "").strip():
                _drive_download_to_path(drive_token, idx_fid, index_path)
        meta = action_cover(
            source_audio=source_path,
            pth_path=pth_path,
            index_path=index_path,
            pitch=_env_int("HAKIMI_PITCH", 0),
            index_rate=_env_float("HAKIMI_INDEX_RATE", 0.55),
            protect=_env_float("HAKIMI_PROTECT", 0.33),
            volume_envelope=_env_float("HAKIMI_VOLUME_ENVELOPE", 0.85),
            start_sec=os.environ.get("HAKIMI_START_SEC", "auto"),
            duration_sec=_env_float("HAKIMI_DURATION_SEC", 50.0),
            chorus_anchor=os.environ.get("HAKIMI_CHORUS_ANCHOR", ""),
            fallback_start_sec=_env_float("HAKIMI_FALLBACK_START_SEC", 60.0),
            chunk_mb=_env_int("HAKIMI_CHUNK_MB", 6),
        )
        songs_folder = os.environ.get("HAKIMI_DRIVE_SONGS_FOLDER_ID", "").strip()
        if drive_token and songs_folder:
            raw_prefix = os.environ.get("HAKIMI_COVER_PREFIX", "cover").strip()
            prefix = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", raw_prefix).strip("_") or "cover"
            full_up = _drive_upload_from_path(
                drive_token,
                songs_folder,
                DEFAULT_OUT_DIR / "cover_full.mp3",
                f"{prefix}_full.mp3",
                "audio/mpeg",
            )
            chorus_up = _drive_upload_from_path(
                drive_token,
                songs_folder,
                DEFAULT_OUT_DIR / "cover_chorus.mp3",
                f"{prefix}_chorus.mp3",
                "audio/mpeg",
            )
            payload_json = json.dumps(
                {"full": full_up, "chorus": chorus_up, "meta": meta},
                ensure_ascii=False,
            )
            print(DRIVE_OUTPUTS_PREFIX + payload_json, flush=True)
    else:
        raise ValueError(f"Unsupported HAKIMI_COLAB_ACTION: {action!r}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Colab GPU RVC worker.")
    sub = parser.add_subparsers(dest="action", required=True)

    p_split = sub.add_parser("split")
    p_split.add_argument("--input", type=Path, required=True)
    p_split.add_argument("--chunk-dir", type=Path, required=True)
    p_split.add_argument("--chunk-mb", type=int, default=6)

    p_reasm = sub.add_parser("reassemble")
    p_reasm.add_argument("--chunk-dir", type=Path, required=True)
    p_reasm.add_argument("--output", type=Path, required=True)
    p_reasm.add_argument("--expected-sha256", default="")
    p_reasm.add_argument("--keep-chunks", action="store_true")

    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--force", action="store_true")

    p_train = sub.add_parser("train")
    p_train.add_argument("--model-name", default="hakimi")
    p_train.add_argument("--dataset-tar", type=Path, default=DEFAULT_WORK_DIR / "dataset.tar.gz")
    p_train.add_argument("--epochs", type=int, default=250)
    p_train.add_argument("--save-every", type=int, default=50)
    p_train.add_argument("--batch-size", type=int, default=8)
    p_train.add_argument("--sample-rate", type=int, default=40000)

    p_cover = sub.add_parser("cover")
    p_cover.add_argument("--source", type=Path, required=True)
    p_cover.add_argument("--pth", type=Path, required=True)
    p_cover.add_argument("--index", type=Path, required=True)
    p_cover.add_argument("--pitch", type=int, default=0)
    p_cover.add_argument("--index-rate", type=float, default=0.55)
    p_cover.add_argument("--protect", type=float, default=0.33)
    p_cover.add_argument("--volume-envelope", type=float, default=0.85)
    p_cover.add_argument("--start", default="auto")
    p_cover.add_argument("--duration", type=float, default=50.0)
    p_cover.add_argument("--chorus-anchor", default="")
    p_cover.add_argument("--fallback-start", type=float, default=60.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    actions = {"reassemble", "split", "setup", "train", "cover"}
    env_action = os.environ.get("HAKIMI_COLAB_ACTION", "").strip()
    effective_argv = sys.argv[1:] if argv is None else argv

    try:
        if env_action and (
            argv is None and (not effective_argv or effective_argv[0] not in actions)
        ):
            _dispatch_env(env_action)
        else:
            args = _build_parser().parse_args(effective_argv)
            if args.action == "split":
                split_file(args.input, args.chunk_dir, chunk_mb=args.chunk_mb)
            elif args.action == "reassemble":
                reassemble_chunks(
                    args.chunk_dir,
                    args.output,
                    args.expected_sha256,
                    remove_chunks=not args.keep_chunks,
                )
            elif args.action == "setup":
                action_setup(force=args.force)
            elif args.action == "train":
                action_train(
                    model_name=args.model_name,
                    dataset_tar=args.dataset_tar,
                    epochs=args.epochs,
                    save_every=args.save_every,
                    batch_size=args.batch_size,
                    sample_rate=args.sample_rate,
                )
            elif args.action == "cover":
                action_cover(
                    source_audio=args.source,
                    pth_path=args.pth,
                    index_path=args.index,
                    pitch=args.pitch,
                    index_rate=args.index_rate,
                    protect=args.protect,
                    volume_envelope=args.volume_envelope,
                    start_sec=args.start,
                    duration_sec=args.duration,
                    chorus_anchor=args.chorus_anchor,
                    fallback_start_sec=args.fallback_start,
                )
        print(f"{STATUS_PREFIX}OK", flush=True)
        return 0
    except Exception as exc:
        print(f"{STATUS_PREFIX}ERROR:{exc}", flush=True)
        raise


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
