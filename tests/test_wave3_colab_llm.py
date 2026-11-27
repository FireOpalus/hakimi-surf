"""Wave 3+4 regression tests for Colab reliability, SVC cache, Drive retries & LLM hot paths."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import inspect
import json
import os
import sys
import tarfile
import threading
import time
import urllib.error
from datetime import datetime
from email.message import Message as EmailMessage
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from hakimi import bot_server, colab_compute, context_budget, life_engine
from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.memes import MemeLibrary
from hakimi.models import Message
from hakimi.outbound_actions import Anchor, ChatActions, Sticker
from hakimi.prefix_runtime import MAX_CHAIN_TURNS, PrefixRuntime
from hakimi.rest_policy import RestHoursConfig
from hakimi.routing import RouteDecision
from hakimi.service import Service
from hakimi.song_drive import SongDriveStore, compute_weight_cache_key
from hakimi.songs import _INBOX_STREAM_THRESHOLD_BYTES, SongLibrary
from hakimi.system_modules import SystemResources
from hakimi.system_modules.song_studio import Module as SongStudioModule
from hakimi.world_experience import MAX_DAY_REST_MINUTES
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_colab_compute import (
    FakeDriveBackend,
    _build_test_worker,
    _fake_mp3_bytes,
    _make_drive_dispatch_context,
    _write_fake_oauth,
)
from tests.test_proactive_dm import dm as dm  # noqa: F401
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair  # noqa: F401
from tests.test_song_drive import _FakeGoogleDriveServer, _write_oauth_json
from tests.test_v20_life_engine import LifeGateway

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, rel_path: str):
    if name in sys.modules:
        return sys.modules[name]
    path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


colab_drive = _load_script("colab_drive", "scripts/colab_drive.py")
colab_remote = _load_script("colab_remote_rvc", "scripts/colab_remote_rvc.py")
colab_rvc = _load_script("colab_rvc", "scripts/colab_rvc.py")
colab_worker = _load_script("colab_worker", "scripts/colab_worker.py")


# ---------------------------------------------------------------------------
# 1. Colab Worker cancellation, draining guard, orphan watchdog, token safety
# ---------------------------------------------------------------------------


def test_worker_cancel_event_and_draining_guard(tmp_path: Path) -> None:
    started = threading.Event()
    saw_cancel = threading.Event()
    allow_exit = threading.Event()

    def slow_cover(**kwargs):
        cancel_ev: threading.Event | None = kwargs.get("cancel_event")
        assert cancel_ev is not None
        started.set()
        if cancel_ev.wait(timeout=3.0):
            saw_cancel.set()
        # Simulate slow teardown while thread is still draining.
        allow_exit.wait(timeout=3.0)
        raise colab_rvc.JobCancelledError("cancelled")

    app, cfg, drive_client, tree, _usage, _backend = _build_test_worker(
        tmp_path, cover_runner=slow_cover
    )
    inbox = drive_client.upload_bytes(
        "inbox", "song.mp3", b"ID3-fake-audio", mime_type="audio/mpeg"
    )
    ref = drive_client.upload_bytes(
        "models", "hakimi_ref.wav", b"RIFF-ref-wav", mime_type="audio/wav"
    )
    headers = {"Authorization": f"Bearer {cfg.token}"}
    jid_1 = "1" * 32
    jid_2 = "2" * 32
    dctx = _make_drive_dispatch_context(
        tree,
        inbox_file_id=inbox["file_id"],
        inbox_name="song.mp3",
        ref_audio_file_id=ref["file_id"],
        ref_audio_name="hakimi_ref.wav",
    )
    payload = {
        "title": "DrainSong",
        "engine": "dual",
        "inbox_file_id": inbox["file_id"],
        "ref_audio_file_id": ref["file_id"],
    }
    body_1 = {
        "job_id": jid_1,
        "job_type": "rvc_cover",
        "payload": payload,
        "dispatch_context": dctx,
    }
    body_2 = {**body_1, "job_id": jid_2}

    with TestClient(app) as client:
        r1 = client.post("/v1/jobs", json=body_1, headers=headers)
        assert r1.status_code == 202
        assert started.wait(timeout=3.0)

        rc = client.post(f"/v1/jobs/{jid_1}/cancel", headers=headers)
        assert rc.status_code == 200
        assert saw_cancel.wait(timeout=3.0)

        # While the cancelled thread is still exiting, status reports worker_draining
        # and a second job MUST be rejected with 409 so no second VM can launch.
        st = client.get("/v1/status", headers=headers).json()
        assert st["worker_draining"] is True
        r2 = client.post("/v1/jobs", json=body_2, headers=headers)
        assert r2.status_code == 409
        assert r2.json()["detail"] in {"worker_draining", "worker_busy"}

        # Once the background thread finishes exiting, draining clears.
        allow_exit.set()
        st2: dict[str, Any] = {}
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            st2 = client.get("/v1/status", headers=headers).json()
            if not st2["worker_draining"] and st2["running_job_id"] is None:
                break
            time.sleep(0.05)
        assert st2["worker_draining"] is False
        assert st2["running_job_id"] is None


def test_run_cover_pipeline_cancels_before_provision_gpu(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provision_called = False

    def fake_provision(*args, **kwargs):
        nonlocal provision_called
        provision_called = True
        return "sid-should-not-run", True

    monkeypatch.setattr(colab_rvc, "provision_gpu", fake_provision)
    cancel_ev = threading.Event()
    cancel_ev.set()

    src_audio = tmp_path / "in.mp3"
    src_audio.write_bytes(b"ID3-fake")
    ref_audio = tmp_path / "ref.wav"
    ref_audio.write_bytes(b"RIFF-fake")
    with pytest.raises(colab_rvc.JobCancelledError):
        colab_rvc.run_cover_pipeline(
            session="sess-cancel-1",
            gpu_spec="L4",
            source_path=src_audio,
            ref_audio_path=ref_audio,
            cover_dir=tmp_path / "cover",
            engine="dual",
            cancel_event=cancel_ev,
        )
    assert provision_called is False


def test_stream_process_terminates_subprocess_on_cancel_event() -> None:
    cancel_ev = threading.Event()
    timer = threading.Timer(0.1, cancel_ev.set)
    timer.start()
    t0 = time.monotonic()
    try:
        with pytest.raises(colab_rvc.JobCancelledError):
            colab_rvc._stream_process_with_watchdog(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                env=os.environ.copy(),
                idle_timeout=30.0,
                stream=False,
                cancel_event=cancel_ev,
            )
    finally:
        timer.cancel()
    elapsed = time.monotonic() - t0
    assert elapsed < 3.0


def test_parse_colab_usage_runs_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    adc_file = tmp_path / "colab-adc.json"
    adc_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(colab_rvc, "_ensure_colab_installed", lambda: None)
    monkeypatch.setattr(colab_rvc, "COLAB_ADC_PATH", adc_file)

    barrier = threading.Barrier(2, timeout=2.0)

    def fake_run_colab(args, **kwargs):
        # Will raise BrokenBarrierError if usage & sessions run sequentially.
        barrier.wait()
        if args[0] == "usage":
            return 0, "Current balance: 42.5\nUsage rate: 1.2\n"
        if args[0] == "sessions":
            return 0, "hakimi-s1\n"
        return 0, ""

    monkeypatch.setattr(colab_rvc, "_run_colab", fake_run_colab)
    parsed = colab_rvc.parse_colab_usage()
    assert parsed["reachable"] is True
    assert parsed["compute_units"] == pytest.approx(42.5)
    assert parsed["active_sessions"] == 1
    assert parsed["sessions"] == ["hakimi-s1"]


def test_exec_remote_worker_hides_drive_token_and_remote_consumes_secret_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorded_calls: list[list[str]] = []
    uploaded_payload: dict[str, Any] = {}
    secret_token = "ya29.super-secret-oauth-token-xyz987"

    def fake_run_colab(args, **kwargs):
        recorded_calls.append(list(args))
        if args and args[0] == "upload":
            local_file = Path(args[3])
            assert local_file.stat().st_mode & 0o777 == 0o600
            uploaded_payload["data"] = json.loads(local_file.read_text(encoding="utf-8"))
            uploaded_payload["remote"] = args[4]
            return 0, ""
        return 0, f"ok\n{colab_rvc.STATUS_OK_MARKER}\n"

    monkeypatch.setattr(colab_rvc, "_run_colab", fake_run_colab)
    colab_rvc.exec_remote_worker(
        "sess-sec-1",
        "setup",
        {
            "HAKIMI_DRIVE_ACCESS_TOKEN": secret_token,
            "HAKIMI_DRIVE_SONGS_FOLDER_ID": "drv_songs_123",
        },
    )

    # Secret token MUST NOT appear anywhere in any colab CLI argv (host /proc/<pid>/cmdline).
    for argv in recorded_calls:
        joined = " ".join(argv)
        assert secret_token not in joined
        assert "HAKIMI_DRIVE_ACCESS_TOKEN=" not in joined

    exec_calls = [a for a in recorded_calls if a and a[0] == "exec"]
    assert len(exec_calls) == 1
    assert any(arg.startswith("HAKIMI_SECRETS_FILE=/tmp/.hakimi_sec_") for arg in exec_calls[0])
    assert uploaded_payload["data"] == {"HAKIMI_DRIVE_ACCESS_TOKEN": secret_token}

    # Now test remote consumption & immediate unlink of the secrets file.
    sec_file = tmp_path / ".hakimi_sec_test.json"
    sec_file.write_text(json.dumps({"HAKIMI_DRIVE_ACCESS_TOKEN": secret_token}), encoding="utf-8")
    os.chmod(sec_file, 0o600)
    monkeypatch.setenv("HAKIMI_SECRETS_FILE", str(sec_file))
    monkeypatch.delenv("HAKIMI_DRIVE_ACCESS_TOKEN", raising=False)

    consumed_tok = colab_remote._consume_remote_secrets_file()
    assert consumed_tok == secret_token
    assert "HAKIMI_DRIVE_ACCESS_TOKEN" not in os.environ
    assert not sec_file.exists()
    assert "HAKIMI_SECRETS_FILE" not in os.environ


async def test_colab_compute_narrow_lock_reconcile_backoff_and_warm_orphan_guard(
    settings, store
) -> None:
    pre_dispatch_entered = asyncio.Event()
    release_pre_dispatch = asyncio.Event()

    class MockTransport(httpx.AsyncBaseTransport):
        def __init__(self) -> None:
            self.post_attempts = 0
            self.post_mode = "timeout_then_accepted"
            self.get_job_status = 200
            self.status_payload: dict[str, Any] = {
                "reachable": True,
                "compute_units": 50.0,
                "active_sessions": 1,
                "untracked_active_sessions": 0,
                "warm_sessions": [{"session_name": "hakimi-warm-1", "gpu": "L4"}],
                "running_job_id": None,
            }
            self.terminate_calls = 0

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "GET" and path == "/v1/status":
                return httpx.Response(200, json=self.status_payload)
            if request.method == "POST" and path == "/v1/sessions/terminate":
                self.terminate_calls += 1
                return httpx.Response(200, json={"terminated_sessions": 1})
            if request.method == "POST" and path == "/v1/jobs":
                self.post_attempts += 1
                if self.post_mode == "timeout_then_accepted":
                    raise httpx.ReadTimeout("simulated timeout after worker accepted")
                if self.post_mode == "409":
                    return httpx.Response(409, json={"detail": "worker_draining"})
                return httpx.Response(202, json={"accepted": True})
            if request.method == "GET" and path.startswith("/v1/jobs/"):
                if self.get_job_status == 200:
                    return httpx.Response(
                        200,
                        json={"status": "running", "assigned_gpu": "L4", "stage": "setup"},
                    )
                return httpx.Response(404, json={"detail": "not_found"})
            if request.method == "POST" and path.endswith("/cancel"):
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(404, json={"detail": "unknown"})

    now_box = [1_750_000_000.0]
    transport = MockTransport()
    s = settings.model_copy(
        update={
            "colab_worker_url": "http://127.0.0.1:12146",
            "colab_worker_token": SecretStr("test-worker-token"),
        }
    )

    hub = colab_compute.ColabComputeHub(
        s,
        store,
        SimpleNamespace(),
        clock=lambda: now_box[0],
        http=httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:12146"),
    )

    async def slow_pre_dispatch(job: dict[str, Any]) -> dict[str, Any]:
        del job
        pre_dispatch_entered.set()
        await release_pre_dispatch.wait()
        return {"drive": {"access_token": "ya29.tok"}}

    hub.register_job_type(
        "test_cover",
        validator=lambda p: dict(p),
        pre_dispatch=slow_pre_dispatch,
        default_gpu="L4",
    )
    await hub.initialize()

    # 1. Warm session alone (untracked_active_sessions=0) MUST NOT trigger orphan termination.
    await hub.scheduler_step()
    assert hub._orphan_seen_since is None
    now_box[0] += 3600.0
    await hub.scheduler_step()
    assert hub._orphan_seen_since is None
    assert transport.terminate_calls == 0

    # 2. Untracked active session (untracked_active_sessions=1) triggers orphan termination.
    transport.status_payload = {
        "reachable": True,
        "compute_units": 50.0,
        "active_sessions": 2,
        "untracked_active_sessions": 1,
        "warm_sessions": [{"session_name": "hakimi-warm-1", "gpu": "L4"}],
        "running_job_id": None,
    }
    now_box[0] += 120.0
    await hub.scheduler_step()
    assert hub._orphan_seen_since is not None
    now_box[0] += hub.config.auto_terminate_orphan_minutes * 60.0 + 5.0
    await hub.scheduler_step()
    assert transport.terminate_calls == 1

    # Restore clean idle worker status for dispatch tests.
    transport.status_payload = {
        "reachable": True,
        "compute_units": 50.0,
        "active_sessions": 0,
        "untracked_active_sessions": 0,
        "warm_sessions": [],
        "running_job_id": None,
    }

    # 3. Narrow _op_lock: slow pre_dispatch must NOT block concurrent get_job / submit_job.
    job1 = await hub.submit_job("test_cover", {"song": "A"})
    step_task = asyncio.create_task(hub.scheduler_step())
    await asyncio.wait_for(pre_dispatch_entered.wait(), timeout=2.0)

    # While pre_dispatch is blocked, DB shows 'dispatching' and submit_job does NOT block.
    cur1 = next((j for j in await hub.list_jobs() if j["id"] == job1["id"]), None)
    assert cur1 is not None and cur1["status"] == "dispatching"
    job2 = await asyncio.wait_for(hub.submit_job("test_cover", {"song": "B"}), timeout=1.0)
    assert job2["status"] == "queued"

    # Release pre_dispatch -> POST times out -> GET /v1/jobs/{id} (200) reconciles to running.
    release_pre_dispatch.set()
    await asyncio.wait_for(step_task, timeout=2.0)
    cur1_after = next((j for j in await hub.list_jobs() if j["id"] == job1["id"]), None)
    assert cur1_after is not None and cur1_after["status"] == "running"
    assert transport.post_attempts == 1

    # Cancel job1, then test 409 exponential backoff on job2.
    await hub.cancel_job(job1["id"])
    transport.post_mode = "409"
    transport.post_attempts = 0

    async def fast_pre_dispatch(job: dict[str, Any]) -> dict[str, Any]:
        del job
        return {}

    hub.register_job_type(
        "test_cover",
        validator=lambda p: dict(p),
        pre_dispatch=fast_pre_dispatch,
        default_gpu="L4",
    )
    await hub.scheduler_step()
    assert transport.post_attempts == 1
    assert job2["id"] in hub._dispatch_backoff_until
    # Immediate second step MUST respect backoff and not hammer POST /v1/jobs.
    await hub.scheduler_step()
    assert transport.post_attempts == 1
    await hub.close()


# ---------------------------------------------------------------------------
# 2. Default `dual` SVC Drive Tier-3 Weight Cache + Redundant GPU VM Work
# ---------------------------------------------------------------------------


def test_svc_weight_cache_key_parity_and_tar_zst_roundtrip(tmp_path: Path) -> None:
    key_drive = compute_weight_cache_key("svc")
    key_remote = colab_remote.compute_weight_cache_key("svc")
    assert key_drive == key_remote
    assert key_drive.startswith("svc_weights_")

    # Prepare synthetic sep_models + svc_hf_cache (including relative HF symlink)
    sep_dir = tmp_path / "sep_models"
    hf_dir = tmp_path / "svc_hf_cache"
    (sep_dir / "bs_roformer").mkdir(parents=True)
    (sep_dir / "bs_roformer" / "model.ckpt").write_bytes(b"BS-ROFORMER-WEIGHTS")

    blob_dir = hf_dir / "models--Plachta--Seed-VC" / "blobs"
    snap_dir = hf_dir / "models--Plachta--Seed-VC" / "snapshots" / "rev1"
    blob_dir.mkdir(parents=True)
    snap_dir.mkdir(parents=True)
    blob_file = blob_dir / "abc123blob"
    blob_file.write_bytes(b"SEED-VC-DIT-WEIGHTS")
    (snap_dir / "dit.pth").symlink_to("../../blobs/abc123blob")

    archive_path = tmp_path / f"{key_remote}.tar.zst"
    packed_meta = colab_remote.pack_svc_weight_cache(
        archive_path,
        sep_models_dir=sep_dir,
        svc_hf_cache=hf_dir,
    )
    assert archive_path.is_file()
    assert packed_meta["file_count"] >= 2
    assert packed_meta["sha256"] == hashlib.sha256(archive_path.read_bytes()).hexdigest()

    # Restore into fresh directories
    sep_out = tmp_path / "restored_sep"
    hf_out = tmp_path / "restored_hf"
    restored = colab_remote.restore_weight_cache(
        archive_path,
        expected_sha256=packed_meta["sha256"],
        sep_models_dir=sep_out,
        svc_hf_cache=hf_out,
        only_prefixes=("sep_models", "svc_hf_cache"),
    )
    assert restored["file_count"] >= 2
    assert restored["sep_model_files"] >= 1
    assert restored["svc_model_files"] >= 1
    assert (sep_out / "bs_roformer" / "model.ckpt").read_bytes() == b"BS-ROFORMER-WEIGHTS"
    restored_link = hf_out / "models--Plachta--Seed-VC" / "snapshots" / "rev1" / "dit.pth"
    assert restored_link.is_symlink()
    assert restored_link.read_bytes() == b"SEED-VC-DIT-WEIGHTS"

    # Security: escaping symlink inside svc_hf_cache MUST be rejected.
    evil_tar = tmp_path / "evil.tar.gz"
    with tarfile.open(evil_tar, "w:gz") as tf:
        info = tarfile.TarInfo("svc_hf_cache/models--evil/snapshots/r1/escape")
        info.type = tarfile.SYMTYPE
        info.linkname = "../../../../../../../etc/passwd"
        tf.addfile(info)
    with pytest.raises(ValueError, match="Unsafe member"):
        colab_remote.restore_weight_cache(
            evil_tar,
            sep_models_dir=sep_out,
            svc_hf_cache=hf_out,
            only_prefixes=("svc_hf_cache",),
        )


async def test_song_studio_prefers_svc_weight_cache_and_falls_back_to_cover(
    settings, store, tmp_path: Path
) -> None:
    oauth = _write_oauth_json(tmp_path / "roaming-oauth.json")
    s = settings.model_copy(update={"song_drive_oauth_file": oauth})
    srv = _FakeGoogleDriveServer()
    cover_key = compute_weight_cache_key("cover")
    svc_key = compute_weight_cache_key("svc")
    cover_sha = hashlib.sha256(b"COVER_ARC").hexdigest()
    svc_sha = hashlib.sha256(b"SVC_ARC").hexdigest()
    srv.add_file("arc_cover", name=f"{cover_key}.tar", parent_id=srv.cache_id, data=b"COVER_ARC")
    srv.add_file(
        "man_cover",
        name=f"{cover_key}.manifest.json",
        parent_id=srv.cache_id,
        data=json.dumps(
            {
                "version_key": cover_key,
                "kind": "weights",
                "archive_file_id": "arc_cover",
                "sha256": cover_sha,
            }
        ).encode(),
    )

    from hakimi.modules import ModuleRegistry

    reg = ModuleRegistry()
    resources = SystemResources(
        settings=s,
        store=store,
        gateway=FakeGateway(),  # type: ignore[arg-type]
        http=None,
        service=SimpleNamespace(),  # type: ignore[arg-type]
    )
    async with srv.client() as http:
        mod = SongStudioModule(resources, reg)
        mod.drive._injected_http = http
        await mod.initialize()
        # Before svc cache exists, fallback resolves cover_weights (still restores BS-RoFormer).
        _, fid1, sha1 = await mod._resolve_weight_cache(mod.drive, "svc", fallback_mode="cover")
        assert (fid1, sha1) == ("arc_cover", cover_sha)

        # Once svc_weights_<hash> exists, SVC mode prefers it directly!
        srv.add_file("arc_svc", name=f"{svc_key}.tar.zst", parent_id=srv.cache_id, data=b"SVC_ARC")
        srv.add_file(
            "man_svc",
            name=f"{svc_key}.manifest.json",
            parent_id=srv.cache_id,
            data=json.dumps(
                {
                    "version_key": svc_key,
                    "kind": "weights",
                    "archive_file_id": "arc_svc",
                    "sha256": svc_sha,
                }
            ).encode(),
        )
        _, fid2, sha2 = await mod._resolve_weight_cache(mod.drive, "svc", fallback_mode="cover")
        assert (fid2, sha2) == ("arc_svc", svc_sha)


def test_dual_cover_hoists_volume_probe_skips_chunk_split_and_avoids_reprobe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audio_in = tmp_path / "song.mp3"
    audio_in.write_bytes(b"ID3-input")
    ref_wav = tmp_path / "hakimi_ref.wav"
    ref_wav.write_bytes(b"RIFF-ref")
    out_dir = tmp_path / "out"
    chunks_dir = tmp_path / "chunks"
    work_dir = tmp_path / "work"

    volume_calls: list[Path] = []
    split_calls: list[Path] = []
    lead_holder: dict[str, Path] = {}

    def fake_run(cmd, **kwargs):
        del kwargs
        # ffmpeg writes to last argv path
        if cmd and cmd[0] == "ffmpeg":
            out_p = Path(cmd[-1])
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_bytes(_fake_mp3_bytes(b"mix-out"))
        return ""

    def fake_norm_ref(src, dst):
        del src
        Path(dst).write_bytes(b"RIFF-ref44k")
        return Path(dst)

    def fake_sep(sep_py, src_wav, work_cover, **kwargs):
        del sep_py, src_wav, kwargs
        lead = Path(work_cover) / "lead.wav"
        inst = Path(work_cover) / "inst.wav"
        lead.write_bytes(b"RIFF-lead")
        inst.write_bytes(b"RIFF-inst")
        lead_holder["lead"] = lead
        return lead, inst, None

    def fake_ying(svc_py, *, work_dir, **kwargs):
        del svc_py, kwargs
        out = Path(work_dir) / "vocals_ying.wav"
        out.write_bytes(b"RIFF-ying")
        return out

    def fake_seed(svc_py, *, work_dir, **kwargs):
        del svc_py, kwargs
        out = Path(work_dir) / "vocals_seed.wav"
        out.write_bytes(b"RIFF-seed")
        return out

    def fake_detect_vol(path):
        volume_calls.append(Path(path))
        return -18.0

    def fake_split(src, dest, **kwargs):
        del dest, kwargs
        split_calls.append(Path(src))
        return {"chunks": [], "sha256": "0" * 64}

    monkeypatch.setattr(colab_remote, "DEFAULT_WORK_DIR", work_dir)
    monkeypatch.setattr(colab_remote, "_run", fake_run)
    monkeypatch.setattr(colab_remote, "_normalize_ref_voice", fake_norm_ref)
    monkeypatch.setattr(colab_remote, "_separate_clean_lead_vocals", fake_sep)
    monkeypatch.setattr(colab_remote, "_transcribe_for_chorus", lambda *a, **kw: [])
    monkeypatch.setattr(colab_remote, "find_chorus_start_sec", lambda *a, **kw: (60.0, "dsp"))
    monkeypatch.setattr(colab_remote, "_run_yingmusic_svc_inference", fake_ying)
    monkeypatch.setattr(colab_remote, "_run_seed_vc_inference", fake_seed)
    monkeypatch.setattr(colab_remote, "_detect_mean_volume_db", fake_detect_vol)
    monkeypatch.setattr(colab_remote, "compute_acoustic_metrics", lambda *a, **kw: {"ok": True})
    monkeypatch.setattr(
        colab_remote,
        "_probe_duration_sec",
        lambda p: 180.0 if Path(p).name == "source.wav" else 45.0,
    )
    monkeypatch.setattr(colab_remote, "split_file", fake_split)

    res = colab_remote.action_cover(
        source_audio=audio_in,
        ref_audio=ref_wav,
        engine="dual",
        out_dir=out_dir,
        chunks_dir=chunks_dir,
        skip_chunk_split=True,
    )
    assert res["engine_requested"] == "dual"
    assert len(res["candidates"]) == 2
    # Hoisted: lead_vocals_wav volume detected ONCE (not twice for 2 candidates)!
    assert sum(1 for p in volume_calls if p == lead_holder["lead"]) == 1
    # Skipped when Drive Direct upload is configured!
    assert split_calls == []
    # Candidates carry pre-probed duration_ms for _dispatch_env_inner reuse.
    assert all(c["duration_ms"] == 45000 for c in res["candidates"])


# ---------------------------------------------------------------------------
# 3. Drive HTTP retries, deduplicated verification, streaming upload & LRU
# ---------------------------------------------------------------------------


async def test_song_drive_retries_429_and_503_with_retry_after_and_dedups_verify(
    settings, store, tmp_path: Path
) -> None:
    oauth_path = _write_oauth_json(tmp_path / "oauth.json")
    s = settings.model_copy(update={"song_drive_oauth_file": oauth_path})
    server = _FakeGoogleDriveServer()

    api_failures = {"remaining": 2}
    put_failures = {"remaining": 1}
    verify_gets = {"count": 0}
    sleeps: list[float] = []

    async def flaky_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path.startswith("/drive/v3/files/"):
            if api_failures["remaining"] > 0:
                api_failures["remaining"] -= 1
                if api_failures["remaining"] == 1:
                    return httpx.Response(429, headers={"Retry-After": "0.02"}, json={"err": 429})
                return httpx.Response(503, json={"err": 503})
            if request.url.params.get("alt") != "media":
                verify_gets["count"] += 1
        if request.method == "PUT":
            if put_failures["remaining"] > 0:
                put_failures["remaining"] -= 1
                return httpx.Response(503, json={"err": 503})
        return server._handle(request)

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(flaky_handler),
        follow_redirects=False,
        trust_env=False,
    ) as http:
        drive = SongDriveStore(s, store, http=http, sleep_fn=fake_sleep)
        await drive.initialize()
        await drive.ensure_folder_tree()

        async def _chunks():
            yield b"ID3-retry-payload"

        uploaded = await drive.upload_inbox_stream(
            "retry-test.mp3",
            _chunks(),
            mime_type="audio/mpeg",
            content_length=len(b"ID3-retry-payload"),
        )
        assert put_failures["remaining"] == 0
        assert len(sleeps) >= 1

        # First verify hits 429 + 503 + 200 -> cached; second verify hits cache (0 new GETs).
        drive._verified_file_cache.clear()
        verify_gets["count"] = 0
        meta1 = await drive._verify_file_in_role(uploaded["file_id"], "inbox")
        meta2 = await drive._verify_file_in_role(uploaded["file_id"], "inbox")
        assert meta1["id"] == meta2["id"] == uploaded["file_id"]
        assert api_failures["remaining"] == 0
        assert verify_gets["count"] == 1
        assert any(abs(d - 0.02) < 1e-6 for d in sleeps)


def test_ephemeral_drive_client_retries_and_caches_verified_meta(tmp_path: Path) -> None:
    cred = tmp_path / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()

    seed = colab_drive.ColabDriveClient(cred, transport=backend.transport)
    tree = seed.ensure_folder_tree()
    item = seed.upload_bytes("inbox", "t.mp3", b"AUDIO-BYTES", mime_type="audio/mpeg")
    fid = item["file_id"]

    meta_get_count = 0
    injected_503 = True

    def flaky_sync(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        nonlocal meta_get_count, injected_503
        if method == "GET" and f"/drive/v3/files/{fid}" in url and "alt=media" not in url:
            if injected_503:
                injected_503 = False
                return 503, {"content-type": "application/json"}, b'{"error":"transient"}'
            meta_get_count += 1
        return backend.transport(method, url, headers, body, timeout)

    sleeps: list[float] = []
    client = colab_worker.EphemeralDriveClient(
        "ya29.fake-access-token",
        tree.as_dict(),
        transport=flaky_sync,
        sleep_fn=sleeps.append,
    )
    m1 = client.verify_file_in_folders(fid, allowed_roles=("inbox",))
    m2 = client.verify_file_in_folders(fid, allowed_roles=("inbox",))
    assert m1["id"] == m2["id"] == fid
    assert meta_get_count == 1
    assert len(sleeps) == 1


def test_colab_remote_drive_urlopen_retries_429_and_503(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}
    sleeps: list[float] = []

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"id":"ok"}'

    def fake_open(req, timeout=60):
        del timeout
        calls["n"] += 1
        if calls["n"] == 1:
            hdrs = EmailMessage()
            hdrs["Retry-After"] = "0"
            raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", hdrs, None)
        if calls["n"] == 2:
            raise urllib.error.HTTPError(req.full_url, 503, "Unavailable", EmailMessage(), None)
        return _FakeResp()

    monkeypatch.setattr(colab_remote._DRIVE_OPENER, "open", fake_open)

    req = colab_remote.urllib.request.Request("https://www.googleapis.com/drive/v3/files/abc")
    with colab_remote._drive_urlopen(req, timeout=10, sleep_fn=sleeps.append) as resp:
        assert resp.read() == b'{"id":"ok"}'
    assert calls["n"] == 3
    assert len(sleeps) == 2


async def test_songs_large_inbox_streaming_and_take_preview_lru(settings, store) -> None:
    lib = SongLibrary(store)
    await lib.initialize()

    stream_calls: list[tuple[str, int]] = []
    bytes_calls: list[str] = []
    download_calls: list[str] = []
    active_mp3 = _fake_mp3_bytes(b"active-take-chorus")
    seed_mp3 = _fake_mp3_bytes(b"seed-take-preview")
    active_sha = hashlib.sha256(active_mp3).hexdigest()
    seed_sha = hashlib.sha256(seed_mp3).hexdigest()

    class _FakeDrive:
        def configured(self) -> bool:
            return True

        async def upload_inbox_stream(self, name, chunks, *, mime_type, total_size=None, **kw):
            del mime_type, total_size, kw
            total = 0
            async for chunk in chunks:
                total += len(chunk)
            stream_calls.append((name, total))
            return {"id": "drv_stream_1", "name": name, "size": total}

        async def upload_inbox_bytes(self, name, data, *, mime_type, **kw):
            del mime_type, kw
            bytes_calls.append(name)
            return {"id": "drv_bytes_1", "name": name, "size": len(data)}

        async def download_song(self, file_id, *, max_bytes=None):
            del max_bytes
            download_calls.append(file_id)
            return seed_mp3

    lib.drive = _FakeDrive()

    # Large payload (> 4 MiB) streams via upload_inbox_stream instead of upload_inbox_bytes.
    big_buf = b"A" * (_INBOX_STREAM_THRESHOLD_BYTES + 1024)
    res = await lib._upload_inbox_bytes("big.mp3", big_buf, mime_type="audio/mpeg")
    assert res["id"] == "drv_stream_1"
    assert len(stream_calls) == 1 and stream_calls[0][1] == len(big_buf)
    assert bytes_calls == []

    # Take preview bounded LRU cache avoids re-downloading the same non-active take.
    entry = await lib.register_colab_cover(
        title="TestTakeSong",
        artist="Artist",
        digest=active_sha,
        byte_size=len(active_mp3),
        duration_ms=30000,
        drive_chorus_id="drv_take_active",
        drive_full_id="drv_full_1",
        chorus_bytes=active_mp3,
        active_engine="yingmusic_svc",
        active_take="yingmusic_svc",
        takes=[
            {
                "key": "yingmusic_svc",
                "engine": "yingmusic_svc",
                "status": "succeeded",
                "digest": active_sha,
                "byte_size": len(active_mp3),
                "duration_ms": 30000,
                "drive_chorus_id": "drv_take_active",
                "drive_full_id": "drv_full_1",
            },
            {
                "key": "seed_vc",
                "engine": "seed_vc",
                "status": "succeeded",
                "digest": seed_sha,
                "byte_size": len(seed_mp3),
                "duration_ms": 30000,
                "drive_chorus_id": "drv_take_seed",
                "drive_full_id": "drv_full_2",
            },
        ],
    )
    b1, _ = await lib.take_audio_bytes(entry["id"], "seed_vc")
    b2, _ = await lib.take_audio_bytes(entry["id"], "seed_vc")
    assert b1 == b2 == seed_mp3
    assert download_calls == ["drv_take_seed"]

    # Deleting the song evicts cached take previews.
    await lib.delete(entry["id"])
    assert not any(k[0] == entry["id"] for k in lib._take_preview_cache)


# ---------------------------------------------------------------------------
# 4. LLM / Chat hot-path efficiency & confirmed dead-code removal
# ---------------------------------------------------------------------------


async def test_gemini_generate_skips_output_screen_and_chain_commit_on_search_turn_1(
    settings, store, message, sdk_pair
) -> None:
    gateway = GeminiGateway(settings, store)
    turn1_json = ChatActions(
        abstain=False,
        bubbles=[""],
        anchor=Anchor(),
        sticker=Sticker(),
        image_digest=None,
        search_query="今天上海天气",
    ).model_dump_json()
    turn2_json = ChatActions(
        abstain=False,
        bubbles=["上海今天晴天喵~"],
        anchor=Anchor(),
        sticker=Sticker(),
        image_digest=None,
    ).model_dump_json()
    outputs = iter([turn1_json, turn2_json])
    screen_purposes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/interactions"):
            raw = interaction_response()
            raw["id"] = f"inter-{len(screen_purposes)}"
            raw["steps"][0]["content"][0]["text"] = next(outputs)
            return httpx.Response(200, json=raw)
        return httpx.Response(200, json=decision_response("chat"))

    gateway.chat_client, gateway.decision_client = sdk_pair(handler)
    orig_screen = gateway.screen

    async def spy_screen(*args, **kwargs):
        screen_purposes.append(str(kwargs.get("purpose", "")))
        return await orig_screen(*args, **kwargs)

    gateway.screen = spy_screen  # type: ignore[method-assign]

    payload = {
        "current_message": "查一下天气",
        "current_sender": {"nickname": "BOISER", "qq": message.user_id},
        "surface": "private",
        "directly_addressed": True,
        "recent_messages": [],
        "rolling_summary": "",
        "session_epoch": 1,
        "session_enabled": True,
        "history_revision": 0,
        "interaction_chains": {},
    }
    route = RouteDecision("chat", tier=1)

    trace1: dict[str, Any] = {}
    out1 = await gateway.generate(
        payload,
        "subj",
        scenario=Scenario.OWNER_PRIVATE,
        route=route,
        message=message,
        generation_trace=trace1,
    )
    assert "今天上海天气" in out1
    assert "output" not in screen_purposes
    assert "chain_commit" not in trace1

    trace2: dict[str, Any] = {}
    out2 = await gateway.generate(
        payload,
        "subj",
        scenario=Scenario.OWNER_PRIVATE,
        route=route,
        message=message,
        generation_trace=trace2,
    )
    assert "上海今天晴天" in out2
    assert screen_purposes == ["output"]
    assert "chain_commit" in trace2


def test_assemble_chat_input_fast_path_serializes_tail_once(
    settings, message, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    orig_est = context_budget.estimate_tokens

    def spy_estimate(text: str) -> int:
        nonlocal calls
        calls += 1
        return orig_est(text)

    monkeypatch.setattr(context_budget, "estimate_tokens", spy_estimate)
    payload = {
        "current_message": "早安喵",
        "current_sender": {"nickname": "BOISER", "qq": message.user_id},
        "surface": "private",
        "directly_addressed": True,
        "recent_messages": [],
        "rolling_summary": "",
        "session_epoch": 1,
    }
    assembled = bot_server.assemble_chat_input(payload, message, settings)
    assert "<transient_turn_context>" in assembled
    # Fast-path: estimate_tokens called ONCE on tail() (instead of 20+ times)!
    assert calls == 1


async def test_service_turn_ignore_skips_second_snapshot_and_filters_platform_bot_id(
    settings, store, message
) -> None:
    await store.runtime.set_rest_hours(RestHoursConfig(enabled=False))
    gid = next(iter(settings.onebot_allowed_groups))
    settings.group_policies = {f"onebot:{gid}": "at_only"}
    gw = FakeGateway()
    gw.intent = RouteDecision("ignore", tier=1)
    svc = Service(settings, store, gw, FakeBili(), FakeSender())
    try:
        grp_msg = message.model_copy(
            update={
                "kind": "group",
                "event_id": "evt-ign-1",
                "conversation_id": gid,
                "text": "路过冒泡",
                "addressed": True,
                "created_at": time.time(),
            }
        )
        assert await svc.accept(grp_msg) == "accepted"
        job = await store.claim()
        assert job is not None

        snap_calls = 0
        orig_snapshot = svc.sessions.snapshot

        async def spy_snapshot(msg):
            nonlocal snap_calls
            snap_calls += 1
            return await orig_snapshot(msg)

        svc.sessions.snapshot = spy_snapshot  # type: ignore[method-assign]
        await svc.process(job)
        # Only the initial snapshot before gatekeeper; second snapshot skipped on ignore!
        assert snap_calls == 1

        # Verify non-QQ platform uses m.bot_id (not onebot_self_id) when filtering mentioned_ids.
        gw.intent = RouteDecision("chat", tier=1)
        captured_related: list[list[str]] = []
        orig_recall = svc.memory_hub.recall_context

        async def spy_recall(*args, **kwargs):
            captured_related.append(list(kwargs.get("related_user_ids") or []))
            return await orig_recall(*args, **kwargs)

        svc.memory_hub.recall_context = spy_recall  # type: ignore[method-assign]
        tg_msg = Message(
            platform="telegram",
            bot_id="tg_bot_999",
            kind="private",
            event_id="evt-tg-1",
            conversation_id="555001",
            user_id="555001",
            sender_nickname="TGUser",
            text="hello",
            addressed=True,
            created_at=time.time(),
            mentioned_ids=["tg_bot_999", settings.onebot_self_id, "777888"],
        )
        from hakimi import service_turn

        orig_auth = service_turn.authorize
        service_turn.authorize = lambda m, s: True  # type: ignore[assignment]
        orig_batch_auth = svc.batch_authorized
        svc.batch_authorized = lambda m, ms: True  # type: ignore[method-assign]
        try:
            assert await store.admit(tg_msg) == "accepted"
            job2 = await store.claim()
            assert job2 is not None
            await svc.process(job2)
        finally:
            service_turn.authorize = orig_auth  # type: ignore[assignment]
            svc.batch_authorized = orig_batch_auth  # type: ignore[method-assign]
        assert captured_related
        assert "tg_bot_999" not in captured_related[0]
        assert "777888" in captured_related[0]
        assert settings.onebot_self_id in captured_related[0]
    finally:
        await svc.stop()


async def test_life_engine_single_agenda_query_parallel_ctx_and_analytical_day_rest(
    dm, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = dm.service.life_engine
    dm.service.ai = LifeGateway()

    # 1. Verify agenda_items() is queried ONCE per plan_once().
    agenda_calls = 0
    orig_agenda_items = life_engine.agenda_items

    async def spy_agenda_items(store_arg, day_iso):
        nonlocal agenda_calls
        agenda_calls += 1
        return await orig_agenda_items(store_arg, day_iso)

    monkeypatch.setattr(life_engine, "agenda_items", spy_agenda_items)
    res = await engine.trigger_plan_once()
    assert res["status"] == "planned"
    assert agenda_calls == 1

    # 2. Verify module.planning_context() runs concurrently via asyncio.gather().
    entered = 0
    both_entered = asyncio.Event()

    class _ConcurrentModule:
        def __init__(self, mid: str) -> None:
            self.module_id = mid

        async def get_sensor_summary(self) -> str:
            return ""

        async def context_data(self, **kwargs):
            del kwargs
            return {}

        async def planning_context(self, service, cfg, ts):
            del service, cfg, ts
            nonlocal entered
            entered += 1
            if entered >= 2:
                both_entered.set()
            await asyncio.wait_for(both_entered.wait(), timeout=2.0)
            return {f"ctx_{self.module_id}": True}

        def planning_fingerprint(self) -> str:
            return f"fp-{self.module_id}"

    m1, m2 = _ConcurrentModule("mod_a"), _ConcurrentModule("mod_b")
    monkeypatch.setattr(dm.service.modules, "active", lambda: [m1, m2])
    monkeypatch.setattr(dm.service.modules, "inactive_reason", lambda mid: None)
    monkeypatch.setattr(dm.service.modules, "schedules_for", lambda m: [])
    res2 = await engine.trigger_plan_once()
    assert res2["status"] in {"planned", "resting"}
    assert both_entered.is_set()

    # 3. Brute-force equivalence test for analytical day_rest_limit() across edge cases.
    def brute_force_day_rest_limit(eng, now_ts: float) -> float:
        runtime = eng.store.runtime
        if not eng.config.enabled or not runtime.rest_hours.day_rest_enabled:
            return now_ts
        end = now_ts + MAX_DAY_REST_MINUTES * 60
        point = now_ts
        while point < end:
            if runtime.is_resting(point) or eng.closing_due(point):
                return point
            point = (int(point // 60) + 1) * 60
        return end

    cases = [
        ("Asia/Shanghai", RestHoursConfig(rest_hours="23:00-08:00"), "2026-11-14T14:00:00"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="23:00-08:00"), "2026-11-14T22:04:00"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="23:00-08:00"), "2026-11-14T22:44:30"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="23:00-08:00"), "2026-11-14T22:45:15"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="23:00-08:00"), "2026-11-14T23:10:00"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="00:15-08:00"), "2026-11-14T23:30:00"),
        ("Asia/Shanghai", RestHoursConfig(rest_hours="12:10-13:00"), "2026-11-14T11:30:27"),
        ("Asia/Shanghai", RestHoursConfig(enabled=False), "2026-11-14T22:40:00"),
        ("Asia/Shanghai", RestHoursConfig(day_rest_enabled=False), "2026-11-14T14:00:00"),
        # DST spring-forward and fall-back in America/New_York
        ("America/New_York", RestHoursConfig(rest_hours="01:30-03:30"), "2026-03-08T01:00:00"),
        ("America/New_York", RestHoursConfig(rest_hours="01:30-01:45"), "2026-11-01T01:15:00"),
    ]
    orig_tz = dm.s.bot_timezone
    try:
        for tz_name, rest_cfg, iso_local in cases:
            dm.s.bot_timezone = tz_name
            await engine.set_rest_policy(rest_cfg)
            dt_local = datetime.fromisoformat(iso_local).replace(tzinfo=ZoneInfo(tz_name))
            ts_val = dt_local.timestamp()
            for offset in (0.0, 0.5, 30.25, 59.9):
                t = ts_val + offset
                assert engine.day_rest_limit(t) == brute_force_day_rest_limit(engine, t), (
                    f"Mismatch for tz={tz_name}, rest={rest_cfg}, t={t}"
                )
    finally:
        dm.s.bot_timezone = orig_tz


async def test_confirmed_dead_code_removed(store, dm) -> None:
    # 1. PrefixRuntime drops chains/get_chain/put_chain, keeps MAX_CHAIN_TURNS & invalidate_chain.
    pr = PrefixRuntime()
    assert MAX_CHAIN_TURNS == 12
    assert not hasattr(pr, "chains")
    assert not hasattr(pr, "get_chain")
    assert not hasattr(pr, "put_chain")
    pr.invalidate_chain("scope", "hydrogen")  # compatibility no-op

    # 2. MemeLibrary has no static_prompt while retaining labels & protocol_prompt.
    ml = MemeLibrary(store)
    assert not hasattr(ml, "static_prompt")
    assert isinstance(ml.labels, tuple)
    assert "meme_labels" in ml.protocol_prompt

    # 3. colab_worker has no clean_search_fn parameter.
    assert "clean_search_fn" not in inspect.signature(colab_worker.WorkerState.__init__).parameters
    assert "clean_search_fn" not in inspect.signature(colab_worker.create_worker_app).parameters

    # 4. social_speak no longer calls dead proactive.companion_context().
    from hakimi import social_speak

    assert "companion_context" not in inspect.getsource(social_speak.speak_proactive)
    assert dm.service.proactive is not None
