import asyncio
import json
import stat
from types import SimpleNamespace

import pytest

import hakimi.video_download_worker as worker
import hakimi.video_downloader as module
from hakimi.security import Rejected
from hakimi.video_download_worker import (
    MAX_BYTES,
    WorkerError,
    allowed_url,
    anonymous_nav_keys,
    check_risk_payload,
    classify_error,
    inspect_media,
    select_format,
    validate_info,
)
from hakimi.video_downloader import VideoDownloader, cookie_file, valid_proxy

BV = "BV1xx411c7mD"
CDN = "https://cn-test.bilivideo.com/upgcxcode/video.mp4"


def video(**kw):
    return {
        "format_id": "360",
        "ext": "mp4",
        "url": CDN,
        "height": 360,
        "width": 640,
        "vcodec": "avc1",
        "acodec": "mp4a",
        "tbr": 500,
        **kw,
    }


def test_smallest_low_resolution_format_with_audio_only():
    formats = [
        video(format_id="1080", height=1080, width=1920, tbr=100),
        video(format_id="480", height=480, width=854, tbr=700),
        video(acodec="none", tbr=300),
        video(format_id="audio", ext="m4a", vcodec="none", acodec="mp4a", tbr=64),
        video(format_id="unsafe", url="https://127.0.0.1/v.mp4", tbr=1),
    ]
    chosen = select_format({"duration": 60, "formats": formats})
    assert chosen["format_id"] == "360+audio"
    assert chosen["ext"] == "mp4"
    assert len(chosen["requested_formats"]) == 2
    formats[2] = video(filesize=100)
    assert select_format({"duration": 60, "formats": formats})["format_id"] == "360"


@pytest.mark.parametrize(
    "fmt",
    [
        video(height=720),
        video(ext="webm"),
        video(has_drm=True),
        video(fragments=[{}]),
        video(protocol="m3u8_native"),
        video(filesize=MAX_BYTES + 1),
    ],
)
def test_no_high_resolution_or_unbounded_fallback(fmt):
    with pytest.raises(WorkerError, match="video_download_format"):
        select_format({"duration": 60, "formats": [fmt]})


def test_silent_mp4_only_when_no_available_audio_track():
    silent = video(acodec="none", filesize=100)
    chosen = select_format({"duration": 60, "formats": [silent]})
    assert chosen["format_id"] == "360" and chosen["acodec"] == "none"
    assert "requested_formats" not in chosen
    muxed = video(format_id="480", height=480, width=854, filesize=2000)
    assert select_format({"duration": 60, "formats": [silent, muxed]})["format_id"] == "480"
    audio = video(format_id="audio", ext="m4a", vcodec="none", filesize=500)
    assert select_format({"duration": 60, "formats": [silent, audio]})["format_id"] == "360+audio"
    audio["filesize"] = MAX_BYTES + 1
    with pytest.raises(WorkerError, match="video_download_format"):
        select_format({"duration": 60, "formats": [silent, audio]})


@pytest.mark.parametrize("audio_tracks,accepted", [(0, True), (1, True), (2, False)])
def test_ffprobe_accepts_silent_video_but_not_extra_audio_tracks(
    tmp_path, monkeypatch, audio_tracks, accepted
):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"mock-video")
    payload = {
        "format": {"duration": "2.0", "format_name": "mov,mp4,m4a,3gp,3g2,mj2"},
        "streams": [{"codec_type": "video", "width": 640, "height": 360}]
        + [{"codec_type": "audio"}] * audio_tracks,
    }
    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *_a, **_k: SimpleNamespace(stdout=json.dumps(payload).encode()),
    )
    if accepted:
        assert inspect_media(path, 480) == 2.0
    else:
        with pytest.raises(WorkerError, match="video_download_invalid_media"):
            inspect_media(path, 480)


@pytest.mark.parametrize("duration", [None, True, 0, -1, 481, float("nan"), float("inf")])
def test_duration_must_be_known_before_download(duration):
    with pytest.raises(WorkerError, match="video_download_duration"):
        validate_info({"id": BV, "duration": duration}, BV, 480)


@pytest.mark.parametrize(
    "update",
    [
        {"id": BV + "_p1"},
        {"_type": "playlist"},
        {"entries": []},
        {"is_live": True},
        {"has_drm": True},
        {"availability": "premium_only"},
    ],
)
def test_single_public_video_only(update):
    with pytest.raises(WorkerError, match="video_download_restricted"):
        validate_info({"id": BV, "duration": 60, **update}, BV, 480)


@pytest.mark.parametrize(
    "url",
    [
        "http://cn-test.bilivideo.com/v.mp4",
        "https://127.0.0.1/v.mp4",
        "https://api.bilibili.com.evil.test/x/nav",
        "https://api.bilibili.com:8080/x/nav",
        "https://user:secret@api.bilibili.com/x/nav",
        "file:///etc/passwd",
        "https://www.bilibili.com/bangumi/play/ep1",
        "https://api.bilibili.com/x/nav#x",
    ],
)
def test_fixed_network_boundary(url):
    with pytest.raises(WorkerError, match="video_download_network_forbidden"):
        allowed_url(url)


def test_domain_bound_cookies_and_proxy_validation():
    jar = cookie_file({"sessdata": "example-value", "bili_jct": "csrf-value"})
    assert ".bilibili.com\tTRUE\t/\tTRUE\t0\tSESSDATA\texample-value" in jar
    assert "bilivideo.com" not in jar
    for value in ["x\ny", "x;y", "x\ty", "x y", "汉字"]:
        with pytest.raises(Rejected):
            cookie_file({"sessdata": value})
    valid_proxy("https://proxy.example")
    valid_proxy("http://user:pass@127.0.0.1:8080")
    for proxy in ["file:///tmp/x", "http://proxy:bad", "http://proxy/path", "http://x\n"]:
        with pytest.raises(Rejected):
            valid_proxy(proxy)


@pytest.mark.parametrize(
    "text,code",
    [
        ("HTTP Error 412", "video_download_risk"),
        ("-352: blocked", "video_download_risk"),
        ("v_voucher", "video_download_risk"),
        ("captcha challenge", "video_download_risk"),
        ("You have exceeded the rate limit", "video_download_risk"),
        ("API code -101", "video_download_cookie_expired"),
        ("supporter-only preview", "video_download_restricted"),
        ("private-cookie-value in unknown error", "video_download_unavailable"),
    ],
)
def test_errors_are_fixed_categories(text, code):
    assert classify_error(text) == code


@pytest.mark.parametrize(
    "body,code",
    [
        ({"code": -101}, "video_download_cookie_expired"),
        ({"code": -352}, "video_download_risk"),
        ({"data": {"code": -412}}, "video_download_risk"),
        ({"data": [{"risk": {"v_voucher": "challenge"}}]}, "video_download_risk"),
    ],
)
def test_structured_risk_aborts_without_waiting_for_upstream_warnings(body, code):
    with pytest.raises(WorkerError, match=code):
        check_risk_payload(body)


def test_only_normal_anonymous_nav_may_return_keys_with_minus_101():
    url = "https://api.bilibili.com/x/web-interface/nav"
    payload = {
        "code": -101,
        "data": {
            "isLogin": False,
            "wbi_img": {
                "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
                "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
            },
        },
    }
    assert anonymous_nav_keys(url, payload)
    assert not anonymous_nav_keys(url, payload, authenticated=True)
    assert not anonymous_nav_keys(url, payload, method="POST")
    assert not anonymous_nav_keys(url + "?x=1", payload)
    assert not anonymous_nav_keys(url.replace("/nav", "/view"), payload)
    assert not anonymous_nav_keys(url, {**payload, "code": -412})
    assert not anonymous_nav_keys(url, {**payload, "v_voucher": "challenge"})
    payload["data"]["wbi_img"]["img_url"] = "https://i0.hdslb.com/not-a-key.png"
    assert not anonymous_nav_keys(url, payload)


@pytest.fixture
def runner(tmp_path, monkeypatch):
    worker = tmp_path / "video_download_worker.py"
    monkeypatch.setattr(module, "__file__", str(tmp_path / "video_downloader.py"))
    downloader = VideoDownloader(tmp_path / "cache", timeout=2, poll_interval=0.01)

    def configure(script):
        worker.write_text("import json,os,sys,time\nfrom pathlib import Path\n" + script)
        return downloader

    return configure


SUCCESS = """\
assert 'HAKIMI_GEMINI_API_KEY' not in os.environ
assert 'HTTP_PROXY' not in os.environ
assert len(sys.argv) == 2 and sys.argv[1] == 'request.json'
assert oct(Path('request.json').stat().st_mode & 0o777) == '0o600'
assert oct(Path('cookies.txt').stat().st_mode & 0o777) == '0o600'
Path('video.mp4').write_bytes(b'mock-video-content')
print(json.dumps({'status':'ok','duration':60}))
"""


async def test_subprocess_environment_credentials_and_cleanup(runner, monkeypatch):
    monkeypatch.setenv("HAKIMI_GEMINI_API_KEY", "must-not-reach-child")
    monkeypatch.setenv("HTTP_PROXY", "http://untrusted-env-proxy")
    downloader = runner(SUCCESS)
    async with downloader.download(
        BV, cookies={"sessdata": "fake-secret"}, proxy="http://user:pass@127.0.0.1:8080"
    ) as result:
        assert result.path.exists() and result.duration == 60
        assert result.bytes == len(b"mock-video-content")
        assert stat.S_IMODE(result.path.parent.stat().st_mode) == 0o700
        assert json.loads((result.path.parent / "request.json").read_text())["proxy"].startswith(
            "http:"
        )
    assert not list(downloader.root.iterdir())


async def test_consumer_failure_always_removes_local_video(runner):
    downloader = runner(SUCCESS)
    with pytest.raises(RuntimeError):
        async with downloader.download(BV) as result:
            assert result.path.exists()
            raise RuntimeError("model upload failed")
    assert not list(downloader.root.iterdir())


async def test_failure_only_returns_known_error_no_stderr(runner):
    downloader = runner("print(json.dumps({'status':'error','error':'secret-from-server'}))\n")
    with pytest.raises(Rejected, match="^video_download_unavailable$"):
        async with downloader.download(BV):
            pytest.fail("must not yield")
    assert not list(downloader.root.iterdir())


@pytest.mark.parametrize(
    "script,code",
    [
        ("print('x'*20000)\ntime.sleep(5)\n", "video_download_unavailable"),
        ("time.sleep(5)\n", "video_download_timeout"),
        ("Path('large.part').write_bytes(b'x'*100000)\ntime.sleep(5)\n", "video_download_size"),
        (
            "Path('video.mp4').symlink_to('/etc/passwd')\ntime.sleep(5)\n",
            "video_download_invalid_media",
        ),
    ],
)
async def test_worker_resource_limits_and_cleanup(runner, monkeypatch, script, code):
    downloader = runner(script)
    downloader.timeout = 0.25
    monkeypatch.setattr(module, "MAX_JOB_BYTES", 65536)
    with pytest.raises(Rejected, match=code):
        async with downloader.download(BV):
            pytest.fail("must not yield")
    assert not list(downloader.root.iterdir())


async def test_hot_revocation_during_download_kills_worker(runner):
    downloader = runner("time.sleep(5)\n")
    calls = 0

    def guard():
        nonlocal calls
        calls += 1
        return calls < 4

    with pytest.raises(Rejected, match="video_download_revoked"):
        async with downloader.download(BV, guard=guard):
            pytest.fail("must not yield")
    assert not list(downloader.root.iterdir())


async def test_cancelled_download_still_cleans_cache(runner):
    downloader = runner("time.sleep(5)\n")
    started = asyncio.Event()

    def guard():
        started.set()

    async def work():
        async with downloader.download(BV, guard=guard):
            pytest.fail("must not yield")

    task = asyncio.create_task(work())
    await started.wait()
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list(downloader.root.iterdir())


async def test_stale_cleanup_scoped_to_own_job_directories(tmp_path):
    downloader = VideoDownloader(tmp_path / "cache")
    downloader._root()
    old = downloader.root / ("job-" + "a" * 32)
    old.mkdir()
    (old / "video.mp4").write_text("old")
    active = downloader.root / ("job-" + "b" * 32)
    active.mkdir()
    downloader.active.add(active)
    unrelated = downloader.root / "unrelated.txt"
    unrelated.write_text("keep")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep")
    link = downloader.root / ("job-" + "c" * 32)
    link.symlink_to(outside, target_is_directory=True)
    await downloader.cleanup_stale()
    assert not old.exists() and not link.exists()
    assert active.exists() and unrelated.exists() and (outside / "keep").exists()


async def test_invalid_bvid_never_spawns(runner):
    downloader = runner("raise Exception('should not start')")
    for bvid in ["https://evil.test", BV + "; touch /tmp/pwn", "../video", ""]:
        with pytest.raises(Rejected, match="invalid_bvid"):
            async with downloader.download(bvid):
                pytest.fail("must not yield")
    assert not downloader.root.exists()
