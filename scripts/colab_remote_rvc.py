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
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
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
DEFAULT_SEP_MODELS_DIR = Path("/tmp/audio-separator-models")
DEFAULT_WORK_DIR = Path("/content/work")
DEFAULT_OUT_DIR = Path("/content/out")
DEFAULT_CHUNKS_DIR = Path("/content/out_chunks")
SETUP_MARKER = Path("/content/.hakimi_setup_done")

WEIGHT_CACHE_SCHEMA_VERSION = "v1"
VENV_CACHE_SCHEMA_VERSION = "v1"
_WEIGHT_CACHE_EXTS = frozenset(
    {".ckpt", ".onnx", ".pth", ".pt", ".bin", ".safetensors", ".yaml", ".yml", ".json", ".th"}
)
_ALLOWED_VENV_ABS_PYTHON_RE = re.compile(
    r"^(?:/usr/bin|/usr/local/bin|/opt/conda/bin)/python(?:\d+(?:\.\d+)?)?$"
    r"|^/root/\.local/share/uv/python/[A-Za-z0-9._-]+/bin/python(?:\d+(?:\.\d+)?)?$"
)

PRIMARY_SEP_MODELS = (
    "model_bs_roformer_ep_317_sdr_12.9755.ckpt",
    "UVR-MDX-NET-Inst_HQ_3.onnx",
)
KARAOKE_SEP_MODELS = ("mel_band_roformer_karaoke_aufr33_viperx_sdr_10.1956.ckpt",)
DEREVERB_MODELS = (
    "dereverb_mel_band_roformer_less_aggressive_anvuew_sdr_18.8050.ckpt",
    "dereverb_mel_band_roformer_anvuew_sdr_19.1729.ckpt",
    "UVR-DeEcho-DeReverb.pth",
)
DEREVERB_MODEL = DEREVERB_MODELS[0]
SUPPORTED_AUDIO_EXTS = frozenset({".wav", ".flac", ".mp3", ".m4a", ".ogg", ".opus"})
MAX_CHORUS_BYTES = 1_750_000
STATUS_PREFIX = "__HAKIMI_COLAB_STATUS__="
DRIVE_OUTPUTS_PREFIX = "__HAKIMI_DRIVE_OUTPUTS__="
DRIVE_DIRECT_ERROR_PREFIX = "__HAKIMI_DRIVE_DIRECT_ERROR__="
CACHE_STATUS_PREFIX = "__HAKIMI_CACHE_STATUS__="

_OAUTH_TOKEN_RE = re.compile(r"(?:ya29\.[A-Za-z0-9._-]+|1//[A-Za-z0-9._-]+)")
_BEARER_RE = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
_SHA256_RE = re.compile(r"^[a-fA-F0-9]{64}$")
_ACTIVE_SECRETS: set[str] = set()

_TRAD_TO_SIMP = str.maketrans(
    "愛淚夢聲憶雲風裏這麼為說還點樣覺過來時間對會沒開關氣離聽見給讓頭臉邊難誰錯變濕積儲記",
    "爱泪梦声忆云风里这么为说还点样觉过来时间对会没开关气离听见给让头脸边难谁错变湿积储记",
)


def scrub_secrets(text: Any) -> str:
    """Redact active short-lived tokens and Google OAuth patterns from logs/errors."""
    out = str(text or "")
    out = _OAUTH_TOKEN_RE.sub("[REDACTED]", out)
    out = _BEARER_RE.sub(r"\1[REDACTED]", out)
    for sec in sorted((s for s in _ACTIVE_SECRETS if len(s) >= 6), key=len, reverse=True):
        out = out.replace(sec, "[REDACTED]")
    return out


def log(msg: str) -> None:
    print(f"[hakimi-colab] {scrub_secrets(msg)}", flush=True)


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
    merged_env.pop("HAKIMI_DRIVE_ACCESS_TOKEN", None)
    merged_env.setdefault("PYTHONUNBUFFERED", "1")
    merged_env["MPLBACKEND"] = "Agg"
    if env:
        merged_env.update(env)
        merged_env.pop("HAKIMI_DRIVE_ACCESS_TOKEN", None)
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


def _sanitize_cache_token(value: str, fallback: str = "unknown") -> str:
    cleaned = re.sub(r"[^a-z0-9._-]+", "", value.strip().lower())
    return cleaned[:32] or fallback


def compute_weight_cache_key(mode: str = "cover") -> str:
    """Deterministic version key for high-frequency RVC + audio-separator weights."""
    norm_mode = "cover" if str(mode or "").strip().lower() == "cover" else "all"
    spec = {
        "schema": WEIGHT_CACHE_SCHEMA_VERSION,
        "mode": norm_mode,
        "primary_sep": list(PRIMARY_SEP_MODELS),
        "karaoke_sep": list(KARAOKE_SEP_MODELS),
        "dereverb": list(DEREVERB_MODELS),
        "applio_f0": "rmvpe",
        "applio_embedder": "contentvec",
    }
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode("utf-8")).hexdigest()
    return f"weights_{norm_mode}_{digest[:12]}"


def _detect_os_id() -> str:
    os_release = Path("/etc/os-release")
    if os_release.is_file():
        vals: dict[str, str] = {}
        for line in os_release.read_text(encoding="utf-8", errors="ignore").splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip().strip('"').strip("'")
        distro = vals.get("ID", "")
        ver = vals.get("VERSION_ID", "")
        if distro:
            return _sanitize_cache_token(f"{distro}{ver}")
    return _sanitize_cache_token(platform.system(), "unknown")


def _detect_cuda_version(nvidia_smi_output: str = "") -> str:
    if nvidia_smi_output:
        match = re.search(r"CUDA Version:\s*([0-9]+\.[0-9]+)", nvidia_smi_output)
        if match:
            return _sanitize_cache_token(match.group(1))
    cuda_json = Path("/usr/local/cuda/version.json")
    if cuda_json.is_file():
        try:
            data = json.loads(cuda_json.read_text(encoding="utf-8"))
            ver = str((data.get("cuda") or {}).get("version") or "")
            if ver:
                parts = ver.split(".")
                return _sanitize_cache_token(".".join(parts[:2]))
        except Exception:
            pass
    try:
        proc = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=10,
            check=False,
        )
        if proc.returncode == 0:
            match = re.search(r"CUDA Version:\s*([0-9]+\.[0-9]+)", proc.stdout or "")
            if match:
                return _sanitize_cache_token(match.group(1))
    except Exception:
        pass
    return "unknown"


def compute_runtime_fingerprint(
    *,
    python_version: str = "",
    cuda_version: str = "",
    os_id: str = "",
    arch: str = "",
    nvidia_smi_output: str = "",
) -> str:
    """Compute base-image fingerprint (`os_arch_pyX.Y_cudaA.B`) gating venv cache reuse."""
    py = _sanitize_cache_token(
        python_version or f"{sys.version_info.major}.{sys.version_info.minor}"
    )
    os_tok = _sanitize_cache_token(os_id or _detect_os_id())
    arch_tok = _sanitize_cache_token(arch or platform.machine())
    cuda_tok = _sanitize_cache_token(cuda_version or _detect_cuda_version(nvidia_smi_output))
    return f"{os_tok}_{arch_tok}_py{py}_cuda{cuda_tok}"


def compute_venv_cache_key(fingerprint: str, *, req_hash: str = "") -> str:
    """Deterministic version key for Python virtualenv cache bound to runtime fingerprint."""
    safe_fp = re.sub(r"[^a-z0-9._-]+", "_", fingerprint.strip().lower()).strip("_") or "unknown"
    spec = {
        "schema": VENV_CACHE_SCHEMA_VERSION,
        "fingerprint": safe_fp,
        "req_hash": req_hash.strip().lower(),
    }
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode("utf-8")).hexdigest()
    return f"venv_{safe_fp[:48]}_{digest[:10]}"


def _requirements_fingerprint(applio_dir: Path) -> str:
    req_file = applio_dir / "requirements-engine.txt"
    if not req_file.is_file():
        return ""
    return sha256_file(req_file)[:16]


def _has_applio_pretrained_weights(applio_dir: Path, mode: str = "cover") -> bool:
    """Check whether essential RMVPE + ContentVec (and HiFi-GAN for train) weights exist."""
    search_roots = [applio_dir / "rvc" / "models", applio_dir / "assets"]
    found_names: list[str] = []
    for root in search_roots:
        if not root.is_dir():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.stat().st_size > 0:
                found_names.append(p.as_posix().lower())
    has_rmvpe = any(
        "rmvpe" in name and name.endswith((".pt", ".pth", ".onnx")) for name in found_names
    )
    has_contentvec = any(
        "contentvec" in name and name.endswith((".bin", ".pt", ".pth", ".safetensors"))
        for name in found_names
    )
    if not (has_rmvpe and has_contentvec):
        return False
    if str(mode or "").strip().lower() != "cover":
        has_hifigan = any("pretrained" in name and name.endswith(".pth") for name in found_names)
        if not has_hifigan:
            return False
    return True


def pack_weight_cache(
    output_tar: Path,
    *,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    sep_models_dir: Path = DEFAULT_SEP_MODELS_DIR,
) -> dict[str, Any]:
    """Pack high-frequency separation + Applio pretrained weights into an uncompressed tar."""
    entries: list[tuple[Path, str]] = []
    sep_count = 0
    applio_count = 0

    if sep_models_dir.is_dir():
        for p in sorted(sep_models_dir.rglob("*")):
            if (
                p.is_file()
                and not p.is_symlink()
                and not p.name.startswith(".")
                and p.suffix.lower() in _WEIGHT_CACHE_EXTS
            ):
                rel = p.relative_to(sep_models_dir).as_posix()
                entries.append((p, f"sep_models/{rel}"))
                sep_count += 1

    applio_rvc_models = applio_dir / "rvc" / "models"
    if applio_rvc_models.is_dir():
        for p in sorted(applio_rvc_models.rglob("*")):
            if (
                p.is_file()
                and not p.is_symlink()
                and not p.name.startswith(".")
                and p.suffix.lower() in _WEIGHT_CACHE_EXTS
            ):
                rel = p.relative_to(applio_rvc_models).as_posix()
                entries.append((p, f"applio_rvc_models/{rel}"))
                applio_count += 1

    applio_assets = applio_dir / "assets"
    if applio_assets.is_dir():
        for p in sorted(applio_assets.rglob("*")):
            if (
                p.is_file()
                and not p.is_symlink()
                and not p.name.startswith(".")
                and p.suffix.lower() in _WEIGHT_CACHE_EXTS
            ):
                rel = p.relative_to(applio_assets).as_posix()
                entries.append((p, f"applio_assets/{rel}"))
                applio_count += 1

    if not entries:
        raise FileNotFoundError("No weight files found to pack into cache archive")

    output_tar.parent.mkdir(parents=True, exist_ok=True)
    tmp_tar = output_tar.with_suffix(output_tar.suffix + ".tmp")
    try:
        with tarfile.open(tmp_tar, "w") as tf:
            for src_path, arcname in entries:
                tf.add(src_path, arcname=arcname, recursive=False)
        tmp_tar.replace(output_tar)
    finally:
        tmp_tar.unlink(missing_ok=True)

    return {
        "path": output_tar,
        "sha256": sha256_file(output_tar),
        "size": output_tar.stat().st_size,
        "file_count": len(entries),
        "sep_model_files": sep_count,
        "applio_model_files": applio_count,
    }


def restore_weight_cache(
    tar_path: Path,
    *,
    expected_sha256: str = "",
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    sep_models_dir: Path = DEFAULT_SEP_MODELS_DIR,
) -> dict[str, Any]:
    """Verify SHA-256 and safely restore cached separation + Applio weights."""
    if not tar_path.is_file():
        raise FileNotFoundError(f"Weight cache archive missing: {tar_path}")
    actual_sha = sha256_file(tar_path)
    if expected_sha256:
        if not _SHA256_RE.fullmatch(expected_sha256.strip()) or (
            actual_sha.lower() != expected_sha256.strip().lower()
        ):
            raise ValueError("weight_cache_sha256_mismatch")

    allowed_roots = {
        "sep_models": sep_models_dir,
        "applio_rvc_models": applio_dir / "rvc" / "models",
        "applio_assets": applio_dir / "assets",
    }
    restored = 0
    sep_restored = 0
    applio_restored = 0

    with tempfile.TemporaryDirectory(prefix="hakimi_wcache_", dir=tar_path.parent) as stage_str:
        stage_root = Path(stage_str).resolve()
        with tarfile.open(tar_path, "r:*") as tf:
            members = tf.getmembers()
            for member in members:
                if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                    raise ValueError(f"Unsafe member in weight cache archive: {member.name}")
                clean_parts = Path(member.name).parts
                if (
                    not clean_parts
                    or clean_parts[0] not in allowed_roots
                    or any(part in {"..", "/"} for part in clean_parts)
                ):
                    raise ValueError(f"Disallowed path in weight cache archive: {member.name}")
                target = (stage_root / member.name).resolve()
                if not target.is_relative_to(stage_root):
                    raise ValueError(f"Path traversal in weight cache archive: {member.name}")
            if hasattr(tarfile, "data_filter"):
                tf.extractall(stage_root, filter="data")
            else:
                tf.extractall(stage_root)

        for prefix, dest_base in allowed_roots.items():
            src_sub = stage_root / prefix
            if not src_sub.is_dir():
                continue
            for item in sorted(src_sub.rglob("*")):
                if not item.is_file():
                    continue
                rel = item.relative_to(src_sub)
                dst = dest_base / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(item), str(dst))
                restored += 1
                if prefix == "sep_models":
                    sep_restored += 1
                else:
                    applio_restored += 1

    if restored == 0:
        raise ValueError("weight_cache_empty")
    return {
        "file_count": restored,
        "sep_model_files": sep_restored,
        "applio_model_files": applio_restored,
        "sha256": actual_sha,
    }


def _normalize_venv_member_parts(raw_name: str) -> tuple[str, ...]:
    if not raw_name or raw_name.startswith("/") or "\\" in raw_name:
        raise ValueError(f"Disallowed path in venv cache archive: {raw_name}")
    raw_parts = raw_name.split("/")
    cleaned: list[str] = []
    for part in raw_parts:
        if part in {"", "."}:
            continue
        if part == "..":
            raise ValueError(f"Disallowed path in venv cache archive: {raw_name}")
        cleaned.append(part)
    if not cleaned or cleaned[0] not in {"rvc-venv", "sep-venv"}:
        raise ValueError(f"Disallowed path in venv cache archive: {raw_name}")
    return tuple(cleaned)


def _is_safe_venv_symlink(
    parts: tuple[str, ...],
    linkname: str,
    stage_root: Path,
) -> bool:
    if not linkname or "\x00" in linkname or "\\" in linkname:
        return False
    if os.path.isabs(linkname):
        # Absolute symlinks are allowed ONLY directly under <venv>/bin/ pointing to an exact
        # system/uv Python executable (never a directory prefix or arbitrary binary).
        if len(parts) != 3 or parts[1] != "bin":
            return False
        if ".." in linkname.split("/") or linkname.endswith("/"):
            return False
        norm = os.path.normpath(linkname)
        return bool(_ALLOWED_VENV_ABS_PYTHON_RE.fullmatch(norm))
    top_root = (stage_root / parts[0]).resolve()
    link_parent = stage_root.joinpath(*parts[:-1])
    resolved = Path(os.path.normpath(str(link_parent / linkname)))
    return resolved.is_relative_to(top_root)


def pack_venv_cache(
    output_tar: Path,
    *,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
    fingerprint: str,
) -> dict[str, Any]:
    """Pack `rvc-venv` and `sep-venv` into a gzipped tarball bound to runtime fingerprint."""
    if not fingerprint or "unknown" in fingerprint:
        raise ValueError("venv_fingerprint_unknown")
    if not (rvc_venv / "bin" / "python").exists() or not (sep_venv / "bin" / "python").exists():
        raise FileNotFoundError("Virtualenvs incomplete for cache packaging")
    output_tar.parent.mkdir(parents=True, exist_ok=True)
    tmp_tar = output_tar.with_suffix(output_tar.suffix + ".tmp")
    try:
        with tarfile.open(tmp_tar, "w:gz", compresslevel=1) as tf:
            tf.add(rvc_venv, arcname="rvc-venv", recursive=True)
            tf.add(sep_venv, arcname="sep-venv", recursive=True)
        tmp_tar.replace(output_tar)
    finally:
        tmp_tar.unlink(missing_ok=True)
    return {
        "path": output_tar,
        "sha256": sha256_file(output_tar),
        "size": output_tar.stat().st_size,
        "fingerprint": fingerprint,
    }


def restore_venv_cache(
    tar_path: Path,
    *,
    expected_sha256: str,
    expected_fingerprint: str,
    actual_fingerprint: str,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
) -> dict[str, Any]:
    """Restore cached virtualenvs ONLY when base image fingerprint & SHA-256 match exactly."""
    if (
        not actual_fingerprint
        or not expected_fingerprint
        or "unknown" in actual_fingerprint
        or actual_fingerprint != expected_fingerprint
    ):
        raise ValueError("venv_fingerprint_mismatch")
    if not tar_path.is_file():
        raise FileNotFoundError(f"Venv cache archive missing: {tar_path}")
    actual_sha = sha256_file(tar_path)
    if not _SHA256_RE.fullmatch(expected_sha256.strip()) or (
        actual_sha.lower() != expected_sha256.strip().lower()
    ):
        raise ValueError("venv_cache_sha256_mismatch")

    with tempfile.TemporaryDirectory(prefix="hakimi_vcache_", dir=tar_path.parent) as stage_str:
        stage_root = Path(stage_str).resolve()
        with tarfile.open(tar_path, "r:*") as tf:
            members = tf.getmembers()
            validated: list[tuple[tarfile.TarInfo, tuple[str, ...]]] = []
            symlink_paths: set[tuple[str, ...]] = set()
            non_symlink_paths: set[tuple[str, ...]] = set()

            for member in members:
                if member.islnk() or member.isdev() or member.isfifo():
                    raise ValueError(f"Unsafe member in venv cache archive: {member.name}")
                if not (member.isdir() or member.isreg() or member.issym()):
                    raise ValueError(f"Unsafe member in venv cache archive: {member.name}")
                parts = _normalize_venv_member_parts(member.name)
                target = Path(os.path.normpath(str(stage_root.joinpath(*parts))))
                if not target.is_relative_to(stage_root):
                    raise ValueError(f"Path traversal in venv cache archive: {member.name}")
                if member.issym():
                    if not _is_safe_venv_symlink(parts, member.linkname or "", stage_root):
                        raise ValueError(f"Unsafe symlink in venv cache archive: {member.name}")
                    if parts in non_symlink_paths:
                        raise ValueError(f"Symlink collision in venv cache archive: {member.name}")
                    symlink_paths.add(parts)
                else:
                    if parts in symlink_paths:
                        raise ValueError(f"Symlink collision in venv cache archive: {member.name}")
                    non_symlink_paths.add(parts)
                validated.append((member, parts))

            # Reject any member whose parent directory traverses a symlink entry.
            for member, parts in validated:
                for depth in range(1, len(parts)):
                    if parts[:depth] in symlink_paths:
                        raise ValueError(
                            f"Path traverses symlink in venv cache archive: {member.name}"
                        )

            # Extract directories and regular files first; materialize validated symlinks last.
            for member, parts in validated:
                if member.issym():
                    continue
                dest = stage_root.joinpath(*parts)
                if member.isdir():
                    dest.mkdir(parents=True, exist_ok=True)
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.parent.resolve().is_relative_to(stage_root):
                    raise ValueError(f"Path traversal in venv cache archive: {member.name}")
                src_fp = tf.extractfile(member)
                if src_fp is None:
                    raise ValueError(f"Unreadable member in venv cache archive: {member.name}")
                with src_fp, dest.open("wb") as out_fp:
                    shutil.copyfileobj(src_fp, out_fp, length=1024 * 1024)
                mode = 0o755 if (member.mode & 0o111) else 0o644
                os.chmod(dest, mode)

            for member, parts in validated:
                if not member.issym():
                    continue
                dest = stage_root.joinpath(*parts)
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.parent.resolve().is_relative_to(stage_root):
                    raise ValueError(f"Path traversal in venv cache archive: {member.name}")
                if os.path.lexists(dest):
                    dest.unlink()
                os.symlink(member.linkname, dest)

        staged_rvc = stage_root / "rvc-venv"
        staged_sep = stage_root / "sep-venv"
        if not os.path.lexists(staged_rvc / "bin" / "python") or not os.path.lexists(
            staged_sep / "bin" / "python"
        ):
            raise ValueError("venv_cache_incomplete")

        for staged, dst in ((staged_rvc, rvc_venv), (staged_sep, sep_venv)):
            if dst.exists():
                shutil.rmtree(dst)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(staged), str(dst))

    return {"fingerprint": actual_fingerprint, "sha256": actual_sha}


def _try_restore_weight_cache_from_drive(
    *,
    drive_token: str,
    cache_folder_id: str,
    weight_cache_key: str,
    applio_dir: Path,
    sep_models_dir: Path,
    work_dir: Path,
    explicit_file_id: str = "",
    explicit_sha256: str = "",
) -> tuple[bool, str | None, dict[str, Any]]:
    """Attempt direct Drive restore of version-keyed weights cache; returns (hit, reason, info)."""
    archive_id = explicit_file_id.strip()
    expected_sha = explicit_sha256.strip().lower()
    manifest_info: dict[str, Any] = {}
    try:
        if not archive_id:
            if not cache_folder_id:
                return False, "cache_folder_unconfigured", {}
            entry = _drive_find_file_in_folder(
                drive_token, cache_folder_id, f"{weight_cache_key}.manifest.json"
            )
            if entry is None:
                return False, "cache_miss", {}
            manifest_info = _drive_download_json(drive_token, entry["id"])
            if str(manifest_info.get("version_key") or "") != weight_cache_key:
                return False, "version_mismatch", {}
            if str(manifest_info.get("kind") or "") != "weights":
                return False, "kind_mismatch", {}
            archive_id = str(manifest_info.get("archive_file_id") or "").strip()
            expected_sha = str(manifest_info.get("sha256") or "").strip().lower()
        if not archive_id or not _SHA256_RE.fullmatch(expected_sha):
            return False, "manifest_invalid", {}

        cache_tmp = work_dir / "_cache_dl"
        cache_tmp.mkdir(parents=True, exist_ok=True)
        tar_path = cache_tmp / f"{weight_cache_key}.tar"
        try:
            _drive_download_to_path(drive_token, archive_id, tar_path)
            info = restore_weight_cache(
                tar_path,
                expected_sha256=expected_sha,
                applio_dir=applio_dir,
                sep_models_dir=sep_models_dir,
            )
            log(
                f"Weight cache HIT ({weight_cache_key}): restored {info['file_count']} file(s) "
                f"(sep={info['sep_model_files']}, applio={info['applio_model_files']})."
            )
            return True, None, info
        finally:
            tar_path.unlink(missing_ok=True)
    except Exception as exc:
        reason = scrub_secrets(f"{type(exc).__name__}:{exc}")[:120]
        log(f"Weight cache fallback ({weight_cache_key}): {reason}")
        return False, reason, {}


def _try_restore_venv_cache_from_drive(
    *,
    drive_token: str,
    cache_folder_id: str,
    venv_cache_key: str,
    actual_fingerprint: str,
    rvc_venv: Path,
    sep_venv: Path,
    work_dir: Path,
    explicit_file_id: str = "",
    explicit_sha256: str = "",
    explicit_fingerprint: str = "",
) -> tuple[bool, str | None]:
    """Attempt direct Drive restore of fingerprint-matched virtualenv cache."""
    if not actual_fingerprint or "unknown" in actual_fingerprint:
        return False, "fingerprint_unknown"
    archive_id = explicit_file_id.strip()
    expected_sha = explicit_sha256.strip().lower()
    expected_fp = explicit_fingerprint.strip()
    try:
        if archive_id:
            if expected_fp != actual_fingerprint:
                log(
                    f"Venv cache fingerprint mismatch: expected={expected_fp}, "
                    f"actual={actual_fingerprint}; falling back to clean install."
                )
                return False, "fingerprint_mismatch"
        else:
            if not cache_folder_id:
                return False, "cache_folder_unconfigured"
            entry = _drive_find_file_in_folder(
                drive_token, cache_folder_id, f"{venv_cache_key}.manifest.json"
            )
            if entry is None:
                return False, "cache_miss"
            manifest = _drive_download_json(drive_token, entry["id"])
            if str(manifest.get("version_key") or "") != venv_cache_key:
                return False, "version_mismatch"
            if str(manifest.get("kind") or "") != "venv":
                return False, "kind_mismatch"
            expected_fp = str(manifest.get("fingerprint") or "").strip()
            if expected_fp != actual_fingerprint:
                return False, "fingerprint_mismatch"
            archive_id = str(manifest.get("archive_file_id") or "").strip()
            expected_sha = str(manifest.get("sha256") or "").strip().lower()

        if not archive_id or not _SHA256_RE.fullmatch(expected_sha):
            return False, "manifest_invalid"

        cache_tmp = work_dir / "_cache_dl"
        cache_tmp.mkdir(parents=True, exist_ok=True)
        tar_path = cache_tmp / f"{venv_cache_key}.tar.gz"
        try:
            _drive_download_to_path(drive_token, archive_id, tar_path)
            restore_venv_cache(
                tar_path,
                expected_sha256=expected_sha,
                expected_fingerprint=expected_fp,
                actual_fingerprint=actual_fingerprint,
                rvc_venv=rvc_venv,
                sep_venv=sep_venv,
            )
            log(f"Venv cache HIT ({venv_cache_key}, fp={actual_fingerprint}).")
            return True, None
        finally:
            tar_path.unlink(missing_ok=True)
    except Exception as exc:
        reason = scrub_secrets(f"{type(exc).__name__}:{exc}")[:120]
        log(f"Venv cache fallback ({venv_cache_key}): {reason}")
        return False, reason


def _writeback_weight_cache_to_drive(
    *,
    drive_token: str,
    cache_folder_id: str,
    weight_cache_key: str,
    applio_dir: Path,
    sep_models_dir: Path,
    work_dir: Path,
) -> bool:
    """Atomically write weight tarball first, then commit manifest JSON to Hakimi_Songs/cache/."""
    if not drive_token or not cache_folder_id:
        return False
    cache_tmp = work_dir / "_cache_wb"
    cache_tmp.mkdir(parents=True, exist_ok=True)
    tar_path = cache_tmp / f"{weight_cache_key}.tar"
    try:
        packed = pack_weight_cache(
            tar_path,
            applio_dir=applio_dir,
            sep_models_dir=sep_models_dir,
        )
        archive_name = f"{weight_cache_key}_{packed['sha256'][:8]}.tar"
        up = _drive_upload_from_path(
            drive_token,
            cache_folder_id,
            tar_path,
            archive_name,
            "application/x-tar",
            replace_existing=True,
            app_properties={
                "hakimi_songs_role": "cache",
                "cache_kind": "weights",
                "cache_key": weight_cache_key[:100],
            },
        )
        manifest = {
            "schema": WEIGHT_CACHE_SCHEMA_VERSION,
            "kind": "weights",
            "version_key": weight_cache_key,
            "archive_file_id": up["file_id"],
            "archive_name": up["name"],
            "sha256": packed["sha256"],
            "size": packed["size"],
            "file_count": packed["file_count"],
            "sep_model_files": packed["sep_model_files"],
            "applio_model_files": packed["applio_model_files"],
            "created_at": int(time.time()),
        }
        _drive_upload_json(
            drive_token,
            cache_folder_id,
            f"{weight_cache_key}.manifest.json",
            manifest,
            replace_existing=True,
            app_properties={"hakimi_songs_role": "cache", "cache_key": weight_cache_key[:100]},
        )
        log(
            f"Weight cache atomic writeback committed: {weight_cache_key} "
            f"({packed['size']} B, files={packed['file_count']})"
        )
        return True
    except Exception as exc:
        log(f"Warning: weight cache writeback skipped ({type(exc).__name__}: {exc})")
        return False
    finally:
        tar_path.unlink(missing_ok=True)


def _writeback_venv_cache_to_drive(
    *,
    drive_token: str,
    cache_folder_id: str,
    venv_cache_key: str,
    fingerprint: str,
    rvc_venv: Path,
    sep_venv: Path,
    work_dir: Path,
) -> bool:
    """Atomically write fingerprint-bound venv tarball first, then commit manifest JSON."""
    if not drive_token or not cache_folder_id or not fingerprint or "unknown" in fingerprint:
        return False
    cache_tmp = work_dir / "_cache_wb"
    cache_tmp.mkdir(parents=True, exist_ok=True)
    tar_path = cache_tmp / f"{venv_cache_key}.tar.gz"
    try:
        packed = pack_venv_cache(
            tar_path,
            rvc_venv=rvc_venv,
            sep_venv=sep_venv,
            fingerprint=fingerprint,
        )
        archive_name = f"{venv_cache_key}_{packed['sha256'][:8]}.tar.gz"
        up = _drive_upload_from_path(
            drive_token,
            cache_folder_id,
            tar_path,
            archive_name,
            "application/gzip",
            replace_existing=True,
            app_properties={
                "hakimi_songs_role": "cache",
                "cache_kind": "venv",
                "cache_key": venv_cache_key[:100],
            },
        )
        manifest = {
            "schema": VENV_CACHE_SCHEMA_VERSION,
            "kind": "venv",
            "version_key": venv_cache_key,
            "fingerprint": fingerprint,
            "archive_file_id": up["file_id"],
            "archive_name": up["name"],
            "sha256": packed["sha256"],
            "size": packed["size"],
            "created_at": int(time.time()),
        }
        _drive_upload_json(
            drive_token,
            cache_folder_id,
            f"{venv_cache_key}.manifest.json",
            manifest,
            replace_existing=True,
            app_properties={"hakimi_songs_role": "cache", "cache_key": venv_cache_key[:100]},
        )
        log(f"Venv cache atomic writeback committed: {venv_cache_key} ({packed['size']} B)")
        return True
    except Exception as exc:
        log(f"Warning: venv cache writeback skipped ({type(exc).__name__}: {exc})")
        return False
    finally:
        tar_path.unlink(missing_ok=True)


def action_setup(
    *,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
    sep_models_dir: Path = DEFAULT_SEP_MODELS_DIR,
    force: bool = False,
    mode: str = "all",
    drive_token: str = "",
    cache_folder_id: str = "",
    cache_weights_file_id: str = "",
    cache_weights_sha256: str = "",
    cache_venv_enabled: bool = False,
    cache_venv_file_id: str = "",
    cache_venv_sha256: str = "",
    cache_venv_fingerprint: str = "",
    cache_writeback: bool = False,
) -> dict[str, Any]:
    """Provision GPU check, ffmpeg, uv, Applio + rvc-venv, and audio-separator sep-venv."""
    weight_cache_key = compute_weight_cache_key(mode)
    cache_status: dict[str, Any] = {
        "setup_skipped_warm": False,
        "weight_cache_key": weight_cache_key,
        "weight_cache_hit": None,
        "weight_cache_written": False,
        "weight_cache_has_sep_models": False,
        "weight_fallback_reason": None,
        "venv_cache_hit": None,
        "venv_cache_written": False,
        "venv_fallback_reason": None,
        "runtime_fingerprint": "",
    }
    if SETUP_MARKER.is_file() and not force and rvc_venv.exists() and sep_venv.exists():
        log("Setup marker already present; skipping environment reinstall.")
        cache_status["setup_skipped_warm"] = True
        return cache_status

    gpu_proc = _run(["nvidia-smi"], check=False, capture=True)
    if gpu_proc.returncode != 0:
        raise RuntimeError(
            "CUDA GPU not available on this Colab runtime (`nvidia-smi` failed). "
            "Please allocate a GPU runtime (T4/L4)."
        )
    log((gpu_proc.stdout or "").splitlines()[0] if gpu_proc.stdout else "GPU verified.")

    actual_fp = compute_runtime_fingerprint(nvidia_smi_output=gpu_proc.stdout or "")
    cache_status["runtime_fingerprint"] = actual_fp

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

    venv_cache_key = compute_venv_cache_key(
        actual_fp, req_hash=_requirements_fingerprint(applio_dir)
    )
    venv_hit = False
    if cache_venv_enabled and drive_token and (cache_folder_id or cache_venv_file_id):
        venv_hit, venv_reason = _try_restore_venv_cache_from_drive(
            drive_token=drive_token,
            cache_folder_id=cache_folder_id,
            venv_cache_key=venv_cache_key,
            actual_fingerprint=actual_fp,
            rvc_venv=rvc_venv,
            sep_venv=sep_venv,
            work_dir=DEFAULT_WORK_DIR,
            explicit_file_id=cache_venv_file_id,
            explicit_sha256=cache_venv_sha256,
            explicit_fingerprint=cache_venv_fingerprint,
        )
        cache_status["venv_cache_hit"] = venv_hit
        cache_status["venv_fallback_reason"] = venv_reason

    weight_hit = False
    weight_info: dict[str, Any] = {}
    if drive_token and (cache_folder_id or cache_weights_file_id):
        weight_hit, weight_reason, weight_info = _try_restore_weight_cache_from_drive(
            drive_token=drive_token,
            cache_folder_id=cache_folder_id,
            weight_cache_key=weight_cache_key,
            applio_dir=applio_dir,
            sep_models_dir=sep_models_dir,
            work_dir=DEFAULT_WORK_DIR,
            explicit_file_id=cache_weights_file_id,
            explicit_sha256=cache_weights_sha256,
        )
        cache_status["weight_cache_hit"] = weight_hit
        cache_status["weight_fallback_reason"] = weight_reason
        cache_status["weight_cache_has_sep_models"] = bool(weight_info.get("sep_model_files"))

    if not venv_hit:
        if not (rvc_venv / "bin" / "python").is_file():
            log(f"Creating {rvc_venv} (Python 3.12)...")
            _run([uv_bin, "venv", "--python", "3.12", str(rvc_venv)])
        rvc_py = str(rvc_venv / "bin" / "python")
        req_file = applio_dir / "requirements-engine.txt"
        log("Installing Applio engine dependencies into rvc-venv...")
        _run([uv_bin, "pip", "install", "--python", rvc_py, "-r", str(req_file), "scikit-learn"])
    else:
        rvc_py = str(rvc_venv / "bin" / "python")

    if weight_hit and _has_applio_pretrained_weights(applio_dir, mode=mode):
        log("Applio pretrained weights restored from Drive cache; skipping prerequisites download.")
    else:
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

    if not venv_hit:
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
        _run(
            [
                sep_py,
                "-c",
                "from audio_separator.separator import Separator; print('sep_import_ok')",
            ]
        )
        log("Installing optional faster-whisper into sep-venv for lyric anchor detection...")
        fw_proc = _run(
            [uv_bin, "pip", "install", "--python", sep_py, "faster-whisper"],
            check=False,
        )
        if fw_proc.returncode != 0:
            log("Warning: faster-whisper install failed; cover will use fallback chorus timestamp.")

        log("Restoring onnxruntime-gpu in sep-venv after faster-whisper install...")
        _run([uv_bin, "pip", "uninstall", "--python", sep_py, "onnxruntime"], check=False)
        _run(
            [uv_bin, "pip", "install", "--python", sep_py, "--reinstall", "onnxruntime-gpu"],
            check=False,
        )
    else:
        sep_py = str(sep_venv / "bin" / "python")
        _run(
            [
                sep_py,
                "-c",
                "from audio_separator.separator import Separator; print('sep_import_ok')",
            ]
        )

    SETUP_MARKER.write_text("ok\n", encoding="utf-8")

    if cache_writeback and drive_token and cache_folder_id:
        if cache_venv_enabled and not venv_hit:
            cache_status["venv_cache_written"] = _writeback_venv_cache_to_drive(
                drive_token=drive_token,
                cache_folder_id=cache_folder_id,
                venv_cache_key=venv_cache_key,
                fingerprint=actual_fp,
                rvc_venv=rvc_venv,
                sep_venv=sep_venv,
                work_dir=DEFAULT_WORK_DIR,
            )
        if not weight_hit and _has_applio_pretrained_weights(applio_dir, mode=mode):
            cache_status["weight_cache_written"] = _writeback_weight_cache_to_drive(
                drive_token=drive_token,
                cache_folder_id=cache_folder_id,
                weight_cache_key=weight_cache_key,
                applio_dir=applio_dir,
                sep_models_dir=sep_models_dir,
                work_dir=DEFAULT_WORK_DIR,
            )

    log("Colab GPU environment setup complete.")
    return cache_status


def _normalize_mono_wav(src_path: Path, dst_path: Path, sample_rate: int) -> None:
    """Transcode an audio clip into mono PCM16 WAV at the target training sample rate."""
    _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(src_path),
            "-ar",
            str(sample_rate),
            "-ac",
            "1",
            "-c:a",
            "pcm_s16le",
            str(dst_path),
        ]
    )


def action_train(
    *,
    model_name: str = "hakimi",
    dataset_tar: Path = DEFAULT_WORK_DIR / "dataset.tar.gz",
    epochs: int = 150,
    save_every: int = 30,
    batch_size: int = 12,
    sample_rate: int = 48000,
    chunk_mb: int = 6,
    applio_dir: Path = DEFAULT_APPLIO_DIR,
    rvc_venv: Path = DEFAULT_RVC_VENV,
    sep_venv: Path = DEFAULT_SEP_VENV,
    out_dir: Path = DEFAULT_OUT_DIR,
    chunks_dir: Path = DEFAULT_CHUNKS_DIR,
) -> tuple[Path, Path]:
    """Run dataset preparation/separation -> Applio preprocess -> extract -> train -> export."""
    if not dataset_tar.is_file():
        raise FileNotFoundError(f"Dataset archive not found: {dataset_tar}")

    raw_dir = DEFAULT_WORK_DIR / "dataset_raw"
    if raw_dir.exists():
        shutil.rmtree(raw_dir)
    _safe_extract_tar(dataset_tar, raw_dir)

    dataset_dir = DEFAULT_WORK_DIR / "dataset"
    if dataset_dir.exists():
        shutil.rmtree(dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    train_sep_dir = DEFAULT_WORK_DIR / "train_sep"
    if train_sep_dir.exists():
        shutil.rmtree(train_sep_dir)

    songs_root = raw_dir / "songs"
    song_inputs: list[Path] = []
    dry_inputs: list[Path] = []
    for path in sorted(raw_dir.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.suffix.lower() not in SUPPORTED_AUDIO_EXTS:
            continue
        if path.is_relative_to(songs_root):
            song_inputs.append(path)
        else:
            dry_inputs.append(path)

    sep_py = str(sep_venv / "bin" / "python")
    for idx, song_src in enumerate(song_inputs, start=1):
        log(f"Separating vocals from training song {idx}/{len(song_inputs)}: {song_src.name}...")
        dry_stem, _, _ = _separate_clean_lead_vocals(
            sep_py, song_src, train_sep_dir / f"song_{idx:04d}", label=song_src.name
        )
        _normalize_mono_wav(dry_stem, dataset_dir / f"song_{idx:04d}.wav", sample_rate)

    for idx, dry_src in enumerate(dry_inputs, start=1):
        _normalize_mono_wav(dry_src, dataset_dir / f"clip_{idx:04d}.wav", sample_rate)

    prepared_wavs = sorted(p for p in dataset_dir.glob("*.wav") if p.is_file())
    if len(prepared_wavs) < 1:
        raise ValueError(
            f"Expected >= 1 prepared .wav files in dataset, found {len(prepared_wavs)}"
        )
    log(
        f"Prepared {len(prepared_wavs)} training WAV files "
        f"({len(dry_inputs)} dry, {len(song_inputs)} songs) in {dataset_dir}"
    )

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
            str(DEFAULT_SEP_MODELS_DIR),
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
                if any(
                    k in p.name.lower()
                    for k in ("no reverb", "no echo", "noreverb", "noecho", "dry")
                )
            ),
            files[0],
        )
        return dry, None

    inst = next(
        (
            p
            for p in files
            if any(k in p.name.lower() for k in ("instrumental", "no vocal", "no_vocal", "backing"))
        ),
        None,
    )
    vocals = next(
        (p for p in files if "vocal" in p.name.lower() and p != inst),
        None,
    )
    if vocals is None or inst is None:
        if len(files) >= 2:
            if vocals is None:
                vocals = files[0] if inst != files[0] else files[1]
            if inst is None:
                inst = files[1] if vocals != files[1] else files[0]
        else:
            raise FileNotFoundError(f"Could not resolve vocals/instrumental stems from {files}")
    return vocals, inst


def _separate_clean_lead_vocals(
    sep_py: str,
    input_audio: Path,
    work_dir: Path,
    *,
    label: str = "cover",
) -> tuple[Path, Path, Path | None]:
    """Run primary separation -> best-effort karaoke -> best-effort dereverb.

    Returns ``(dry_lead_vocals_wav, instrumental_wav, backing_vocals_wav)``.
    """
    vocals_wav, inst_wav = _separate_with_sep_venv(
        sep_py, input_audio, work_dir / "stems", PRIMARY_SEP_MODELS
    )
    assert inst_wav is not None

    lead_vocals_wav = vocals_wav
    backing_vocals_wav: Path | None = None
    try:
        log(f"Running best-effort karaoke lead/backing vocal separation ({label})...")
        lead_vocals_wav, backing_vocals_wav = _separate_with_sep_venv(
            sep_py, vocals_wav, work_dir / "karaoke", KARAOKE_SEP_MODELS
        )
    except Exception as exc:
        log(f"Warning: karaoke separation skipped ({label}: {exc}); keeping full vocals stem.")
        lead_vocals_wav = vocals_wav
        backing_vocals_wav = None

    dry_vocals_wav = lead_vocals_wav
    try:
        log(f"Running best-effort de-echo/de-reverb pass on lead vocals ({label})...")
        dry_vocals_wav, _ = _separate_with_sep_venv(
            sep_py,
            lead_vocals_wav,
            work_dir / "dereverb",
            DEREVERB_MODELS,
            is_dereverb=True,
        )
    except Exception as exc:
        log(f"De-reverb skipped ({label}: {exc}); proceeding with lead vocals stem.")
        dry_vocals_wav = lead_vocals_wav

    return dry_vocals_wav, inst_wav, backing_vocals_wav


def build_mix_filter_complex(gain_db: float, *, has_backing: bool = False) -> str:
    """Build ffmpeg filter_complex graph for vocal de-harshing, ambience, and mastering."""
    vocal_bus = (
        f"[0:a]volume={gain_db:.2f}dB,highpass=f=85,"
        "equalizer=f=6800:t=q:w=2.0:g=-2.0,"
        "acompressor=threshold=-18dB:ratio=2.2:attack=12:release=140:makeup=1dB,"
        "aformat=channel_layouts=stereo,asplit=2[vdry][vwet];"
        "[vwet]highpass=f=300,lowpass=f=3800,"
        "aecho=0.8:0.6:23|41|67:0.18|0.12|0.08,volume=0.12[vamb];"
        "[vdry][vamb]amix=inputs=2:duration=longest:normalize=0[v];"
    )
    inst_bus = "[1:a]volume=-0.8dB[inst];"
    if has_backing:
        return (
            vocal_bus
            + inst_bus
            + "[2:a]volume=-2.5dB[bg];"
            + "[v][inst][bg]amix=inputs=3:duration=longest:normalize=0,"
            + "alimiter=level_in=1:level_out=0.92:limit=0.92:attack=5:release=60[m]"
        )
    return (
        vocal_bus
        + inst_bus
        + "[v][inst]amix=inputs=2:duration=longest:normalize=0,"
        + "alimiter=level_in=1:level_out=0.92:limit=0.92:attack=5:release=60[m]"
    )


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
    index_rate: float = 0.25,
    protect: float = 0.28,
    volume_envelope: float = 0.25,
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
    dry_vocals_wav, inst_wav, backing_vocals_wav = _separate_clean_lead_vocals(
        sep_py, source_wav, work_cover, label="cover"
    )

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
    gain_db = max(-12.0, min(12.0, orig_db - conv_db))
    log(f"Loudness match: orig={orig_db:.1f} dB, conv={conv_db:.1f} dB -> gain={gain_db:+.2f} dB")

    cover_full_mp3 = out_dir / "cover_full.mp3"
    has_backing = backing_vocals_wav is not None
    filter_complex = build_mix_filter_complex(gain_db, has_backing=has_backing)
    mix_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(converted_wav),
        "-i",
        str(inst_wav),
    ]
    if backing_vocals_wav is not None:
        mix_cmd.extend(["-i", str(backing_vocals_wav)])
    mix_cmd.extend(
        [
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
    _run(mix_cmd)

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
        "karaoke_backing_separated": backing_vocals_wav is not None,
        "auto_pipeline_split": True,
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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, D102
        raise urllib.error.HTTPError(req.full_url, code, "Redirects are forbidden", headers, fp)


_DRIVE_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _drive_urlopen(req: urllib.request.Request, *, timeout: float = 120.0):
    parsed = urllib.parse.urlsplit(req.full_url)
    if parsed.scheme != "https" or parsed.netloc != "www.googleapis.com":
        raise ValueError("Disallowed Drive API endpoint")
    return _DRIVE_OPENER.open(req, timeout=timeout)


def _escape_drive_q(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _drive_find_file_in_folder(
    access_token: str,
    folder_id: str,
    filename: str,
) -> dict[str, Any] | None:
    clean_folder = re.sub(r"[^A-Za-z0-9_-]", "", folder_id.strip())
    if not clean_folder or clean_folder != folder_id.strip():
        raise ValueError("Invalid Drive folder ID")
    q = (
        f"'{_escape_drive_q(clean_folder)}' in parents "
        f"and name = '{_escape_drive_q(filename)}' "
        "and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    )
    query = urllib.parse.urlencode(
        {
            "q": q,
            "fields": "files(id,name,size,modifiedTime,appProperties)",
            "pageSize": "5",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
    )
    req = urllib.request.Request(
        f"https://www.googleapis.com/drive/v3/files?{query}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with _drive_urlopen(req, timeout=30.0) as resp:
        data = json.loads(resp.read(1_048_576).decode("utf-8"))
    files = data.get("files") or []
    if not files:
        return None
    first = files[0]
    return {
        "id": str(first["id"]),
        "name": str(first.get("name") or filename),
        "size": int(first["size"]) if str(first.get("size") or "").isdigit() else 0,
        "app_properties": dict(first.get("appProperties") or {}),
    }


def _drive_download_json(
    access_token: str,
    file_id: str,
    *,
    max_bytes: int = 262_144,
) -> dict[str, Any]:
    clean_id = re.sub(r"[^A-Za-z0-9_-]", "", file_id.strip())
    if not clean_id or clean_id != file_id.strip():
        raise ValueError("Invalid Drive file ID")
    url = f"https://www.googleapis.com/drive/v3/files/{clean_id}?alt=media&supportsAllDrives=true"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
    with _drive_urlopen(req, timeout=30.0) as resp:
        raw = resp.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("Drive JSON file exceeded limit")
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("Drive JSON payload is not an object")
    return parsed


def _drive_download_to_path(access_token: str, file_id: str, dest_path: Path) -> Path:
    """Download a Drive file directly from Colab using a short-lived Bearer token."""
    clean_id = re.sub(r"[^A-Za-z0-9_-]", "", file_id.strip())
    if not clean_id or clean_id != file_id.strip():
        raise ValueError("Invalid Drive file ID")
    url = f"https://www.googleapis.com/drive/v3/files/{clean_id}?alt=media&supportsAllDrives=true"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    try:
        with _drive_urlopen(req, timeout=180.0) as resp, tmp_path.open("wb") as handle:
            while chunk := resp.read(1024 * 1024):
                handle.write(chunk)
        tmp_path.replace(dest_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    log(f"Direct Drive download complete -> {dest_path} ({dest_path.stat().st_size} B)")
    return dest_path


def _drive_upload_from_path(
    access_token: str,
    folder_id: str,
    local_path: Path,
    drive_name: str,
    mime_type: str,
    *,
    replace_existing: bool = False,
    app_properties: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Upload a local artifact directly from Colab to Google Drive using bounded streaming."""
    clean_folder = re.sub(r"[^A-Za-z0-9_-]", "", folder_id.strip())
    if not clean_folder or clean_folder != folder_id.strip():
        raise ValueError("Invalid Drive folder ID")
    total = local_path.stat().st_size
    if total <= 0:
        raise ValueError(f"Cannot upload empty file: {local_path}")

    existing_id: str | None = None
    if replace_existing:
        existing = _drive_find_file_in_folder(access_token, clean_folder, drive_name)
        if existing is not None:
            existing_id = existing["id"]

    meta_obj: dict[str, Any] = {"name": drive_name}
    if existing_id is None:
        meta_obj["parents"] = [clean_folder]
    if app_properties:
        meta_obj["appProperties"] = {
            str(k)[:40]: str(v)[:120] for k, v in app_properties.items() if k and v is not None
        }

    if total <= 4 * 1024 * 1024:
        data = local_path.read_bytes()
        if existing_id is not None:
            req = urllib.request.Request(
                f"https://www.googleapis.com/upload/drive/v3/files/{existing_id}"
                "?uploadType=media&supportsAllDrives=true&fields=id,name,size",
                data=data,
                method="PATCH",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": mime_type,
                },
            )
            with _drive_urlopen(req, timeout=120.0) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        else:
            boundary = f"hakimi_{uuid.uuid4().hex}"
            meta = json.dumps(meta_obj, ensure_ascii=False).encode("utf-8")
            meta_hdr = (
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            ).encode()
            body = b"".join(
                [
                    meta_hdr,
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
            with _drive_urlopen(req, timeout=120.0) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        log(f"Direct Drive upload complete: {drive_name} ({total} B)")
        return {
            "file_id": str(result["id"]),
            "name": str(result.get("name") or drive_name),
            "size": total,
        }

    init_method = "PATCH" if existing_id else "POST"
    init_base = (
        f"https://www.googleapis.com/upload/drive/v3/files/{existing_id}"
        if existing_id
        else "https://www.googleapis.com/upload/drive/v3/files"
    )
    init_req = urllib.request.Request(
        f"{init_base}?uploadType=resumable&supportsAllDrives=true&fields=id,name,size",
        data=json.dumps(meta_obj, ensure_ascii=False).encode("utf-8"),
        method=init_method,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": mime_type,
            "X-Upload-Content-Length": str(total),
        },
    )
    with _drive_urlopen(init_req, timeout=60.0) as resp:
        upload_url = resp.headers.get("Location", "")
    if not upload_url.startswith("https://www.googleapis.com/"):
        raise RuntimeError("Drive resumable upload init returned invalid Location")

    chunk_size = 4 * 1024 * 1024
    offset = 0
    with local_path.open("rb") as handle:
        while offset < total:
            end = min(total, offset + chunk_size)
            expected_len = end - offset
            if handle.tell() != offset:
                handle.seek(offset)
            chunk = handle.read(expected_len)
            if len(chunk) != expected_len:
                raise RuntimeError("Short read during Drive resumable upload")
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
                with _drive_urlopen(put_req, timeout=180.0) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                    log(f"Direct Drive upload complete: {drive_name} ({total} B)")
                    return {
                        "file_id": str(result["id"]),
                        "name": str(result.get("name") or drive_name),
                        "size": total,
                    }
            except urllib.error.HTTPError as exc:
                if exc.code == 308:
                    offset = end
                    continue
                raise
    raise RuntimeError("Direct Drive resumable upload ended without completion")


def _drive_upload_json(
    access_token: str,
    folder_id: str,
    filename: str,
    payload: dict[str, Any],
    *,
    replace_existing: bool = True,
    app_properties: dict[str, str] | None = None,
) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(prefix="hakimi_json_", suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        tmp.write(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    try:
        return _drive_upload_from_path(
            access_token,
            folder_id,
            tmp_path,
            filename,
            "application/json",
            replace_existing=replace_existing,
            app_properties=app_properties,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def _download_drive_model_if_needed(
    access_token: str,
    file_id: str,
    dest_path: Path,
) -> bool:
    """Download model file directly from Drive unless warm VM already holds the exact file_id."""
    marker = dest_path.with_suffix(dest_path.suffix + ".drive_id")
    if (
        dest_path.is_file()
        and dest_path.stat().st_size > 0
        and marker.is_file()
        and marker.read_text(encoding="utf-8").strip() == file_id
    ):
        log(f"Reusing warm model artifact already present: {dest_path.name}")
        return False
    _drive_download_to_path(access_token, file_id, dest_path)
    marker.write_text(file_id, encoding="utf-8")
    return True


def _dispatch_env(action: str, drive_token: str | None = None) -> None:
    token = (
        os.environ.pop("HAKIMI_DRIVE_ACCESS_TOKEN", "").strip()
        if drive_token is None
        else drive_token
    )
    registered = bool(token and token not in _ACTIVE_SECRETS)
    if registered:
        _ACTIVE_SECRETS.add(token)
    try:
        _dispatch_env_inner(action, token)
    except Exception as exc:
        safe_msg = scrub_secrets(exc)
        if safe_msg != str(exc):
            raise RuntimeError(safe_msg) from None
        raise
    finally:
        if registered:
            _ACTIVE_SECRETS.discard(token)


def _dispatch_env_inner(action: str, drive_token: str) -> None:
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
        cache_status = action_setup(
            force=_env_bool("HAKIMI_FORCE", False),
            mode=os.environ.get("HAKIMI_SETUP_MODE", "all").strip().lower() or "all",
            drive_token=drive_token,
            cache_folder_id=os.environ.get("HAKIMI_CACHE_FOLDER_ID", "").strip(),
            cache_weights_file_id=os.environ.get("HAKIMI_CACHE_WEIGHTS_FILE_ID", "").strip(),
            cache_weights_sha256=os.environ.get("HAKIMI_CACHE_WEIGHTS_SHA256", "").strip(),
            cache_venv_enabled=_env_bool("HAKIMI_CACHE_VENV_ENABLED", False),
            cache_venv_file_id=os.environ.get("HAKIMI_CACHE_VENV_FILE_ID", "").strip(),
            cache_venv_sha256=os.environ.get("HAKIMI_CACHE_VENV_SHA256", "").strip(),
            cache_venv_fingerprint=os.environ.get("HAKIMI_CACHE_VENV_FINGERPRINT", "").strip(),
            cache_writeback=_env_bool("HAKIMI_CACHE_WRITEBACK", False),
        )
        print(
            CACHE_STATUS_PREFIX + json.dumps(cache_status, ensure_ascii=False),
            flush=True,
        )
    elif action == "train":
        model_name = os.environ.get("HAKIMI_MODEL_NAME", "hakimi")
        final_pth, final_idx = action_train(
            model_name=model_name,
            dataset_tar=Path(os.environ.get("HAKIMI_DATASET_TAR", "/content/work/dataset.tar.gz")),
            epochs=_env_int("HAKIMI_EPOCHS", 150),
            save_every=_env_int("HAKIMI_SAVE_EVERY", 30),
            batch_size=_env_int("HAKIMI_BATCH_SIZE", 12),
            sample_rate=_env_int("HAKIMI_SAMPLE_RATE", 48000),
            chunk_mb=_env_int("HAKIMI_CHUNK_MB", 6),
        )
        models_folder = os.environ.get("HAKIMI_DRIVE_MODELS_FOLDER_ID", "").strip()
        if drive_token and models_folder:
            try:
                pth_sha = sha256_file(final_pth)
                idx_sha = sha256_file(final_idx)
                pth_up = _drive_upload_from_path(
                    drive_token,
                    models_folder,
                    final_pth,
                    final_pth.name,
                    "application/octet-stream",
                    replace_existing=True,
                    app_properties={"hakimi_songs_role": "models"},
                )
                idx_up = _drive_upload_from_path(
                    drive_token,
                    models_folder,
                    final_idx,
                    final_idx.name,
                    "application/octet-stream",
                    replace_existing=True,
                    app_properties={"hakimi_songs_role": "models"},
                )
                pth_up["sha256"] = pth_sha
                idx_up["sha256"] = idx_sha
                print(
                    DRIVE_OUTPUTS_PREFIX
                    + json.dumps({"pth": pth_up, "index": idx_up}, ensure_ascii=False),
                    flush=True,
                )
            except Exception as exc:
                log(
                    "Warning: direct Drive train upload failed "
                    f"({type(exc).__name__}: {exc}); host will fall back to chunk download."
                )
    elif action == "cover":
        source_path = Path(os.environ.get("HAKIMI_SOURCE_AUDIO", "/content/work/source_song"))
        pth_path = Path(os.environ.get("HAKIMI_PTH_PATH", "/content/out/hakimi.pth"))
        index_path = Path(os.environ.get("HAKIMI_INDEX_PATH", "/content/out/hakimi.index"))
        direct_inputs: list[str] = []
        if drive_token:
            for env_key, target_name, dest, is_model in (
                ("HAKIMI_DRIVE_SOURCE_FILE_ID", "source", source_path, False),
                ("HAKIMI_DRIVE_PTH_FILE_ID", "pth", pth_path, True),
                ("HAKIMI_DRIVE_INDEX_FILE_ID", "index", index_path, True),
            ):
                fid = os.environ.get(env_key, "").strip()
                if not fid:
                    continue
                try:
                    if is_model:
                        _download_drive_model_if_needed(drive_token, fid, dest)
                    else:
                        _drive_download_to_path(drive_token, fid, dest)
                    direct_inputs.append(target_name)
                except Exception as exc:
                    err_payload = {
                        "stage": "input_download",
                        "target": target_name,
                        "error": scrub_secrets(f"{type(exc).__name__}: {exc}")[:160],
                    }
                    print(
                        DRIVE_DIRECT_ERROR_PREFIX + json.dumps(err_payload, ensure_ascii=False),
                        flush=True,
                    )
                    raise RuntimeError(
                        f"drive_direct_input_failed:{target_name}:{type(exc).__name__}"
                    ) from None
        meta = action_cover(
            source_audio=source_path,
            pth_path=pth_path,
            index_path=index_path,
            pitch=_env_int("HAKIMI_PITCH", 0),
            index_rate=_env_float("HAKIMI_INDEX_RATE", 0.25),
            protect=_env_float("HAKIMI_PROTECT", 0.28),
            volume_envelope=_env_float("HAKIMI_VOLUME_ENVELOPE", 0.25),
            start_sec=os.environ.get("HAKIMI_START_SEC", "auto"),
            duration_sec=_env_float("HAKIMI_DURATION_SEC", 50.0),
            chorus_anchor=os.environ.get("HAKIMI_CHORUS_ANCHOR", ""),
            fallback_start_sec=_env_float("HAKIMI_FALLBACK_START_SEC", 60.0),
            chunk_mb=_env_int("HAKIMI_CHUNK_MB", 6),
        )
        songs_folder = os.environ.get("HAKIMI_DRIVE_SONGS_FOLDER_ID", "").strip()
        if drive_token and songs_folder:
            try:
                raw_prefix = os.environ.get("HAKIMI_COVER_PREFIX", "cover").strip()
                prefix = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", raw_prefix).strip("_") or "cover"
                song_title = os.environ.get("HAKIMI_SONG_TITLE", raw_prefix).strip()[:80]
                full_mp3 = DEFAULT_OUT_DIR / "cover_full.mp3"
                chorus_mp3 = DEFAULT_OUT_DIR / "cover_chorus.mp3"
                chorus_sha = sha256_file(chorus_mp3)
                chorus_size = chorus_mp3.stat().st_size
                try:
                    chorus_dur_ms = int(round(_probe_duration_sec(chorus_mp3) * 1000.0))
                except Exception:
                    fallback_dur = float(meta.get("chorus_duration_sec", 50.0))
                    chorus_dur_ms = int(round(fallback_dur * 1000.0))
                chorus_up = _drive_upload_from_path(
                    drive_token,
                    songs_folder,
                    chorus_mp3,
                    f"{prefix}_{chorus_sha[:8]}_chorus.mp3",
                    "audio/mpeg",
                    replace_existing=True,
                    app_properties={
                        "hakimi_songs_role": "songs",
                        "song_title": song_title,
                        "kind": "chorus",
                    },
                )
                chorus_up["sha256"] = chorus_sha
                chorus_up["size"] = chorus_size
                chorus_up["duration_ms"] = chorus_dur_ms
                full_up = _drive_upload_from_path(
                    drive_token,
                    songs_folder,
                    full_mp3,
                    f"{prefix}_{chorus_sha[:8]}_full.mp3",
                    "audio/mpeg",
                    replace_existing=True,
                    app_properties={
                        "hakimi_songs_role": "songs",
                        "song_title": song_title,
                        "kind": "full",
                    },
                )
                payload_json = json.dumps(
                    {
                        "full": full_up,
                        "chorus": chorus_up,
                        "meta": meta,
                        "drive_direct_inputs": direct_inputs,
                    },
                    ensure_ascii=False,
                )
                print(DRIVE_OUTPUTS_PREFIX + payload_json, flush=True)
            except Exception as exc:
                log(
                    "Warning: direct Drive cover upload failed "
                    f"({type(exc).__name__}: {exc}); host will fall back to chunk download."
                )

        cache_folder = os.environ.get("HAKIMI_CACHE_FOLDER_ID", "").strip()
        if (
            drive_token
            and cache_folder
            and _env_bool("HAKIMI_CACHE_WRITEBACK", False)
            and _env_bool("HAKIMI_CACHE_WEIGHTS_WRITEBACK_AFTER_COVER", False)
        ):
            w_key = compute_weight_cache_key("cover")
            written = _writeback_weight_cache_to_drive(
                drive_token=drive_token,
                cache_folder_id=cache_folder,
                weight_cache_key=w_key,
                applio_dir=DEFAULT_APPLIO_DIR,
                sep_models_dir=DEFAULT_SEP_MODELS_DIR,
                work_dir=DEFAULT_WORK_DIR,
            )
            print(
                CACHE_STATUS_PREFIX
                + json.dumps(
                    {
                        "weight_cache_key": w_key,
                        "weight_cache_written": written,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
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
    p_train.add_argument("--epochs", type=int, default=150)
    p_train.add_argument("--save-every", type=int, default=30)
    p_train.add_argument("--batch-size", type=int, default=12)
    p_train.add_argument("--sample-rate", type=int, default=48000)

    p_cover = sub.add_parser("cover")
    p_cover.add_argument("--source", type=Path, required=True)
    p_cover.add_argument("--pth", type=Path, required=True)
    p_cover.add_argument("--index", type=Path, required=True)
    p_cover.add_argument("--pitch", type=int, default=0)
    p_cover.add_argument("--index-rate", type=float, default=0.25)
    p_cover.add_argument("--protect", type=float, default=0.28)
    p_cover.add_argument("--volume-envelope", type=float, default=0.25)
    p_cover.add_argument("--start", default="auto")
    p_cover.add_argument("--duration", type=float, default=50.0)
    p_cover.add_argument("--chorus-anchor", default="")
    p_cover.add_argument("--fallback-start", type=float, default=60.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    actions = {"reassemble", "split", "setup", "train", "cover"}
    env_action = os.environ.get("HAKIMI_COLAB_ACTION", "").strip()
    effective_argv = sys.argv[1:] if argv is None else argv
    drive_token = os.environ.pop("HAKIMI_DRIVE_ACCESS_TOKEN", "").strip()
    if drive_token:
        _ACTIVE_SECRETS.add(drive_token)

    try:
        if env_action and (
            argv is None and (not effective_argv or effective_argv[0] not in actions)
        ):
            _dispatch_env(env_action, drive_token=drive_token)
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
        safe_err = scrub_secrets(exc)
        print(f"{STATUS_PREFIX}ERROR:{safe_err}", flush=True)
        if safe_err != str(exc):
            raise RuntimeError(safe_err) from None
        raise
    finally:
        if drive_token:
            _ACTIVE_SECRETS.discard(drive_token)


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
