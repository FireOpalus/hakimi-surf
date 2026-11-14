import asyncio
import base64
import io
import os
from urllib.parse import quote

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.memes import MemeLibrary, extract_meme, label_from_filename, normalize_meme
from hakimi.security import Rejected
from tests.fakes import FakeGateway


def picture(fmt="PNG", size=(40, 30), animated=False):
    target = io.BytesIO()
    image = Image.new("RGB", size, "purple")
    if animated:
        image.save(
            target,
            format=fmt,
            save_all=True,
            append_images=[Image.new("RGB", size, "yellow")],
            duration=120,
            loop=0,
        )
    else:
        image.save(target, format=fmt)
    return target.getvalue()


@pytest.mark.parametrize("fmt", ["JPEG", "PNG", "GIF", "WEBP"])
def test_upload_decodes_real_images_and_removes_metadata(fmt):
    normalized = normalize_meme(picture(fmt, (1600, 800)))
    with Image.open(io.BytesIO(normalized.data)) as image:
        assert image.format == "JPEG"
        assert image.size == (1024, 512)
        assert "exif" not in image.info and "icc_profile" not in image.info


def test_animation_first_frame_is_explicit():
    result = normalize_meme(picture("GIF", animated=True))
    assert result.was_animated
    with Image.open(io.BytesIO(result.data)) as image:
        assert not getattr(image, "is_animated", False)


@pytest.mark.parametrize(
    "filename",
    [
        "../x.jpg",
        "x/y.jpg",
        "x\\y.jpg",
        "[MEME:x].jpg",
        "x\n.jpg",
        ".jpg",
        "https://evil.invalid/x.jpg",
        "无语<script>.jpg",
        "a" * 25 + ".jpg",
        "x.svg",
    ],
)
def test_label_rejects_paths_and_prompt_markup(filename):
    with pytest.raises(Rejected, match="invalid_meme_filename"):
        label_from_filename(filename)


def test_rejects_oversize_bombs_wrong_formats_and_invalid_images(monkeypatch):
    for raw in (b"not an image", picture("BMP")):
        with pytest.raises(Rejected):
            normalize_meme(raw)
    monkeypatch.setattr("hakimi.memes.MAX_PIXELS", 10)
    with pytest.raises(Rejected, match="meme_image_dimensions"):
        normalize_meme(picture())
    monkeypatch.setattr("hakimi.memes.MAX_UPLOAD_BYTES", 1)
    with pytest.raises(Rejected, match="meme_upload_too_large"):
        normalize_meme(picture())


async def test_library_stable_static_prefix_restart_hot_revoke_and_base64(store):
    library = MemeLibrary(store)
    await library.initialize()
    first = await library.upload("无语盯.png", picture())
    await library.upload("A.webp", picture("WEBP"))
    assert library.labels == ("A", "无语盯")
    assert '表情标签白名单：["A","无语盯"]' in library.static_prompt
    restart = MemeLibrary(store)
    await restart.initialize()
    assert restart.static_prompt == library.static_prompt
    assert await restart.list() == await library.list()
    raw, mime = await library.preview(first["id"])
    assert mime == "image/jpeg"
    assert base64.b64decode((await library.get_base64("无语盯"))[9:]) == raw
    assert library.root.stat().st_mode & 0o777 == 0o700
    assert (library.root / (first["id"] + ".jpg")).stat().st_mode & 0o777 == 0o600
    (library.root / "manual.jpg").write_bytes(picture())
    assert await library.get_base64("manual") is None
    await library.delete(first["id"])
    assert await library.get_base64("无语盯") is None
    assert "无语盯" not in library.static_prompt
    assert not (library.root / (first["id"] + ".jpg")).exists()
    await restart.delete((await restart.list())[0]["id"])


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "tamper", "fifo"])
async def test_file_substitution_cannot_send_unregistered_bytes(store, tmp_path, attack):
    library = MemeLibrary(store)
    item = await library.upload("无语.png", picture())
    path = library.root / (item["id"] + ".jpg")
    outside = tmp_path / "sensitive.jpg"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    if attack == "symlink":
        path.symlink_to(outside)
    elif attack == "hardlink":
        os.link(outside, path)
    elif attack == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(b"x" * outside.stat().st_size)
    assert await asyncio.wait_for(library.get_base64("无语"), 2) is None
    with pytest.raises(Rejected, match="meme_file_unavailable"):
        await library.preview(item["id"])
    assert outside.read_bytes()


async def test_root_symlink_is_never_followed(store, tmp_path):
    library = MemeLibrary(store)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    library.root.symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(OSError):
        await library.initialize()
    assert not list(elsewhere.iterdir())


async def test_duplicate_concurrent_uploads_and_quotas(store, monkeypatch):
    library = MemeLibrary(store)
    results = await asyncio.gather(
        library.upload("微笑.png", picture()),
        library.upload("微笑.png", picture()),
        return_exceptions=True,
    )
    assert sum(isinstance(r, dict) for r in results) == 1
    assert sum(isinstance(r, Rejected) for r in results) == 1
    assert len(await library.list()) == 1
    monkeypatch.setattr("hakimi.memes.MAX_MEMES", 1)
    with pytest.raises(Rejected, match="meme_library_full"):
        await library.upload("另一个.png", picture())
    monkeypatch.setattr("hakimi.memes.MAX_MEMES", 100)
    monkeypatch.setattr("hakimi.memes.MAX_LIBRARY_BYTES", 1)
    with pytest.raises(Rejected, match="meme_library_full"):
        await library.upload("另一个.png", picture())


@pytest.mark.parametrize(
    "value,clean,tag",
    [
        ("有点离谱[MEME:无语盯]", "有点离谱", "无语盯"),
        ("[MEME:不存在]", "", "不存在"),
        ("[MEME:https://example.invalid/x.jpg]", "", None),
        ("[MEME:../秘密]", "", None),
        ("[MEME:无语]后面还有话", "后面还有话", None),
        ("[MEME:无语][MEME:盯]", "", None),
        ("[meme:无语]", "", None),
        ("文本[MEME:无语", "文本", None),
    ],
)
def test_strict_single_trailing_protocol(value, clean, tag):
    assert extract_meme(value) == (clean, tag)


async def test_frequency_counts_all_replies_persistently_and_hides_ids(store):
    library = MemeLibrary(store)
    await library.upload("无语.png", picture())
    assert await library.reserve_reply("group:40000", "r1", "无语")
    assert not await library.reserve_reply("group:40000", "r1", "无语")
    assert not await library.reserve_reply("group:40000", "r2", "无语")
    assert not await library.reserve_reply("group:40000", "r3", None)
    restart = MemeLibrary(store)
    assert not await restart.reserve_reply("group:40000", "r4", "不存在")
    assert await restart.reserve_reply("group:40000", "r5", "无语")
    assert await restart.reserve_reply("group:50000", "r1", "无语")
    state = str([dict(row) for row in await store.all("SELECT * FROM meme_reply_frequency")])
    assert "40000" not in state and "50000" not in state


@pytest.fixture
async def meme_api(settings):
    settings.admin_token = SecretStr("test-meme-admin-token")
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer test-meme-admin-token"},
        ) as client:
            yield app, client, gateway


async def test_admin_upload_preview_delete_authenticated_without_model_calls(meme_api):
    app, client, gateway = meme_api
    upload = await client.post(
        "/admin/api/memes/upload/" + quote("无语.png"),
        content=picture(),
        headers={"Content-Type": "image/png"},
    )
    assert upload.status_code == 200, upload.text
    key = upload.json()["id"]
    listing = (await client.get("/admin/api/memes")).json()
    assert listing["items"][0]["label"] == "无语"
    assert "filename" not in listing["items"][0]
    image = await client.get(f"/admin/api/memes/{key}/image")
    assert image.status_code == 200 and image.headers["content-type"] == "image/jpeg"
    assert image.headers["cache-control"] == "no-store"
    assert "img-src blob:" in image.headers["content-security-policy"]
    assert (
        await client.get(f"/admin/api/memes/{key}/image", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (await client.delete(f"/admin/api/memes/{key}")).status_code == 200
    assert (await client.get(f"/admin/api/memes/{key}/image")).status_code == 404
    assert not gateway.calls
    assert not app.state.service.memes.labels


async def test_admin_guards_stream_uploads_before_processing(meme_api):
    app, client, _ = meme_api
    url = "/admin/api/memes/upload/x.png"
    assert (
        await client.post(
            url,
            content=b"x",
            headers={"Authorization": "Bearer wrong", "Content-Type": "image/png"},
        )
    ).status_code == 401
    assert (
        await client.post(
            url,
            content=b"x",
            headers={"Content-Type": "image/png", "Content-Length": str(6 * 1024 * 1024)},
        )
    ).status_code == 413
    assert (
        await client.post(url, content=b"x", headers={"Content-Type": "image/svg+xml"})
    ).status_code == 415
    app.state.settings.admin_token = SecretStr("")
    # AdminAuth snapshots token at construction; create a fresh disabled guard.
    from hakimi.admin_auth import AdminAuth

    app.state.admin_auth = AdminAuth(app.state.settings)
    assert (await client.get("/admin/api/memes")).status_code == 403
