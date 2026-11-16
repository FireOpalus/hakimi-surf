import asyncio
import base64
import io
import socket

import pytest
from PIL import Image

from hakimi.media import ImageLoader, PublicResolver, normalize_image, validate_image_url
from hakimi.models import ImageRef
from hakimi.security import Rejected


def test_napcat_nt_image_origin_is_allowed():
    url = "https://multimedia.nt.qq.com.cn/download?appid=1407&fileid=test&rkey=test"
    assert validate_image_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/a",
        "https://gchat.qpic.cn.evil.test/a",
        "https://gchat.qpic.cn@evil.test/a",
        "http://gchat.qpic.cn/a",
        "https://gchat.qpic.cn:444/a",
        "file:///etc/passwd",
        "base64://YWJj",
    ],
)
def test_reject_image_ssrf_origins(url):
    with pytest.raises(Rejected):
        validate_image_url(url)


@pytest.mark.parametrize(
    "ip", ["127.0.0.1", "169.254.169.254", "10.0.0.1", "::1", "::ffff:127.0.0.1"]
)
async def test_resolver_rejects_internal_dns_results(monkeypatch, ip):
    import asyncio

    async def lookup(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    with pytest.raises(Rejected, match="image_address_forbidden"):
        await PublicResolver().resolve("gchat.qpic.cn", 443)


async def test_resolver_returns_pinned_public_addresses(monkeypatch):
    import asyncio

    async def lookup(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    result = await PublicResolver().resolve("gchat.qpic.cn", 443)
    assert result[0]["host"] == "1.1.1.1" and result[0]["hostname"] == "gchat.qpic.cn"
    assert result[0]["flags"] == socket.AI_NUMERICHOST


def test_image_is_bounded_and_metadata_removed():
    source = Image.new("RGB", (2400, 1800), "red")
    exif = Image.Exif()
    exif[270] = "secret-camera-comment"
    raw = io.BytesIO()
    source.save(raw, format="JPEG", exif=exif)
    result = normalize_image(raw.getvalue(), 5_000_000)
    decoded = base64.b64decode(result.data)
    assert b"secret-camera-comment" not in decoded
    with Image.open(io.BytesIO(decoded)) as output:
        assert output.size == (1600, 1200) and not output.getexif()
    assert result.block()["resolution"] == "high"
    assert result.gate().block()["resolution"] == "low"
    with pytest.raises(Rejected):
        normalize_image(raw.getvalue(), 1_000_000)
    with pytest.raises(Rejected):
        normalize_image(b"<svg><script/></svg>", 2_000_000)


def test_gif_uses_only_first_frame_as_jpeg():
    data = io.BytesIO()
    Image.new("RGB", (80, 40), "red").save(
        data,
        format="GIF",
        save_all=True,
        append_images=[Image.new("RGB", (80, 40), "blue")],
        comment=b"do not forward this metadata",
    )
    result = normalize_image(data.getvalue(), 10000)
    with Image.open(io.BytesIO(base64.b64decode(result.data))) as decoded:
        assert decoded.format == "JPEG"
        with pytest.raises(EOFError):
            decoded.seek(1)
        assert decoded.getpixel((10, 10))[0] > 200
        assert not decoded.info.get("comment")


class Response:
    def __init__(self, data, *, status=200, content_type="image/png", content_length=None, delay=0):
        self.data, self.status, self.content_type = data, status, content_type
        self.content_length, self.delay = content_length, delay
        self.headers, self.content = {}, self

    async def __aenter__(self):
        await asyncio.sleep(self.delay)
        return self

    async def __aexit__(self, *_):
        pass

    async def iter_chunked(self, size):
        for n in range(0, len(self.data), size):
            yield self.data[n : n + size]


class Client:
    def __init__(self, responses):
        self.responses, self.requests = list(responses), []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return self.responses.pop(0)


async def test_failed_download_keeps_position_and_success_is_resized(settings, monkeypatch):
    data = io.BytesIO()
    Image.new("RGB", (2400, 1800), "white").save(data, format="PNG")
    loader = ImageLoader(settings)
    client = Client([Response(b"", status=302), Response(data.getvalue())])
    monkeypatch.setattr(loader, "_client", lambda: client)
    refs = [
        ImageRef(url="https://gchat.qpic.cn/first"),
        ImageRef(url="https://gchat.qpic.cn/second"),
    ]
    result = await loader.load_results(refs)
    assert result[0] is None and result[1] is not None
    with Image.open(io.BytesIO(base64.b64decode(result[1].data))) as output:
        assert output.format == "JPEG" and output.size == (1600, 1200)
    assert all(not kwargs["allow_redirects"] for _, kwargs in client.requests)


async def test_image_download_never_exceeds_max_image_bytes_even_if_env_larger(
    settings, monkeypatch
):
    settings.image_max_bytes = 25 * 1024 * 1024
    loader = ImageLoader(settings)
    client = Client([Response(b"x" * (20 * 1024 * 1024 + 1))])
    monkeypatch.setattr(loader, "_client", lambda: client)
    assert await loader.load_results([ImageRef(url="https://gchat.qpic.cn/huge")]) == [None]


async def test_timeout_falls_back_without_cancelling_following_image(settings, monkeypatch):
    loader = ImageLoader(settings)
    client = Client([Response(b"", delay=1), Response(b"", delay=1)])
    monkeypatch.setattr(loader, "_client", lambda: client)
    original = asyncio.timeout

    def short_timeout(seconds):
        assert seconds == 12
        return original(0.01)

    monkeypatch.setattr(asyncio, "timeout", short_timeout)
    assert await loader.load_results([ImageRef(url="https://gchat.qpic.cn/slow")]) == [None]
    assert [url.split(":", 1)[0] for url, _ in client.requests] == ["https", "http"]


async def test_images_disabled_do_not_download_and_cancel_is_not_swallowed(settings, monkeypatch):
    loader = ImageLoader(settings)
    refs = [ImageRef(url="https://gchat.qpic.cn/one")]
    settings.images_enabled = False
    assert await loader.load_results(refs) == [None]
    settings.images_enabled = True
    client = Client([])
    monkeypatch.setattr(loader, "_client", lambda: client)

    async def cancel(*_):
        raise asyncio.CancelledError

    monkeypatch.setattr(loader, "_one", cancel)
    with pytest.raises(asyncio.CancelledError):
        await loader.load_results(refs)
