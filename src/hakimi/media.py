"""Fetch only bounded public QQ CDN images; pin validated DNS results at connect time."""

import asyncio
import base64
import hashlib
import io
import ipaddress
import logging
import re
import socket
import ssl
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver
from PIL import Image, ImageOps

from .config import Settings
from .models import ImageRef
from .security import Rejected

MAX_IMAGE_BYTES = 20 * 1024 * 1024
HTTP_IMAGE_TIMEOUT = 12
MAX_ENCODED_JPEG_BYTES = 800 * 1024
HTTP_FALLBACK_HOSTS = {"multimedia.nt.qq.com.cn", "gchat.qpic.cn"}
BROWSER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
log = logging.getLogger("hakimi")


def validate_image_url(url: str, *, allow_http: bool = False) -> str:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    http = allow_http and parsed.scheme == "http" and host in HTTP_FALLBACK_HOSTS
    if (
        (parsed.scheme != "https" and not http)
        or parsed.port not in ({None, 80} if http else {None, 443})
        or parsed.username
        or parsed.password
        or parsed.fragment
        or len(url) > 4096
        or not (
            host.endswith(".qpic.cn") or host in {"multimedia.nt.qq.com", "multimedia.nt.qq.com.cn"}
        )
    ):
        raise Rejected("image_origin_forbidden")
    return url


class PublicResolver(AbstractResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        entries = await asyncio.get_running_loop().getaddrinfo(
            host, port, family=family, type=socket.SOCK_STREAM
        )
        results = []
        for address_family, _, protocol, _, address in entries:
            ip = ipaddress.ip_address(address[0])
            if not ip.is_global or ip.is_multicast or (ip.version == 6 and ip.ipv4_mapped):
                raise Rejected("image_address_forbidden")
            results.append(
                {
                    "hostname": host,
                    "host": str(ip),
                    "port": port,
                    "family": address_family,
                    "proto": protocol,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        if not results:
            raise Rejected("image_dns_failed")
        return results

    async def close(self):
        pass


@dataclass(frozen=True)
class ModelImage:
    data: str
    mime_type: str = "image/jpeg"
    thumbnail: str = ""
    digest: str = ""
    resolution: str = "low"
    tiles: tuple[str, ...] = ()
    tile_thumbnails: tuple[str, ...] = ()

    def gate(self):
        return ModelImage(self.thumbnail or self.data, self.mime_type, digest=self.digest)

    def block(self) -> dict:
        return {
            "type": "image",
            "data": self.data,
            "mime_type": self.mime_type,
            "resolution": self.resolution,
        }

    def tile_images(self) -> list["ModelImage"]:
        if len(self.tiles) != 2:
            return [self]
        return [
            ModelImage(
                self.tiles[idx],
                self.mime_type,
                thumbnail=(
                    self.tile_thumbnails[idx] if idx < len(self.tile_thumbnails) else self.thumbnail
                ),
                digest=self.digest if idx == 0 else "",
                resolution=self.resolution,
            )
            for idx in range(2)
        ]

    def memory_bytes(self) -> int:
        return (
            len(self.data)
            + len(self.thumbnail)
            + sum(len(t) for t in self.tiles)
            + sum(len(t) for t in self.tile_thumbnails)
        )


def _scale_box(img: Image.Image, max_long: int, max_short: int) -> Image.Image:
    w, h = img.size
    scale = min(1.0, max_long / max(1, max(w, h)), max_short / max(1, min(w, h)))
    if scale < 1.0:
        return img.resize(
            (max(1, round(w * scale)), max(1, round(h * scale))),
            Image.Resampling.LANCZOS,
        )
    return img


def _encode_jpeg(
    img: Image.Image,
    quality: int,
    *,
    subsampling: int = 0,
    max_bytes: int = MAX_ENCODED_JPEG_BYTES,
) -> bytes:
    img.info.clear()
    attempts = [(quality, subsampling), (78, 0), (72, 2), (65, 2)]
    best = b""
    seen = set()
    for q, sub in attempts:
        if (q, sub) in seen:
            continue
        seen.add((q, sub))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=q, subsampling=sub, optimize=True)
        raw = buf.getvalue()
        if not best or len(raw) < len(best):
            best = raw
        if len(raw) <= max_bytes:
            return raw
    return best


def _make_thumb(img: Image.Image, edge: int) -> str:
    tiny = img.copy()
    tiny.thumbnail((edge, edge), Image.Resampling.LANCZOS)
    tiny.info.clear()
    buf = io.BytesIO()
    tiny.save(buf, format="JPEG", quality=72, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def normalize_image(
    raw: bytes,
    max_pixels: int,
    *,
    standard_max_edge: int = 1600,
    tall_max_edge: int = 2560,
) -> ModelImage:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as source:
                if (
                    source.format not in {"JPEG", "PNG", "WEBP", "GIF"}
                    or source.width * source.height > max_pixels
                    or source.width < 1
                    or source.height < 1
                ):
                    raise Rejected("image_format_or_size")
                orig_raw_w, orig_raw_h = source.width, source.height
                raw_long = max(orig_raw_w, orig_raw_h)
                raw_aspect = raw_long / max(1, min(orig_raw_w, orig_raw_h))
                animated = bool(getattr(source, "is_animated", False))
                if (
                    not animated
                    and source.format == "JPEG"
                    and raw_aspect <= 4.5
                    and raw_long > tall_max_edge * 2
                ):
                    source.draft("RGB", (tall_max_edge, tall_max_edge))
                source.seek(0)
                source.load()
                full = ImageOps.exif_transpose(source).convert("RGB")
                orig_w, orig_h = full.size
                aspect = max(orig_w, orig_h) / max(1, min(orig_w, orig_h))
                tiles: tuple[str, ...] = ()
                tile_thumbnails: tuple[str, ...] = ()
                if animated:
                    if (
                        source.n_frames > 300
                        or source.n_frames * orig_raw_w * orig_raw_h > 64_000_000
                    ):
                        raise Rejected("image_animation_too_long")
                    full.thumbnail((768, 768), Image.Resampling.LANCZOS)
                    source.seek(source.n_frames - 1)
                    last = ImageOps.exif_transpose(source).convert("RGB")
                    last.thumbnail((768, 768), Image.Resampling.LANCZOS)
                    stitched = Image.new(
                        "RGB",
                        (full.width + last.width, max(full.height, last.height)),
                        "white",
                    )
                    stitched.paste(full, (0, 0))
                    stitched.paste(last, (full.width, 0))
                    resized = stitched
                    resized.thumbnail((1152, 768), Image.Resampling.LANCZOS)
                    encoded = _encode_jpeg(resized, 82, subsampling=2)
                    resolution = "medium"
                elif aspect <= 2.2:
                    short_cap = max(384, round(standard_max_edge * 0.75))
                    resized = _scale_box(full, standard_max_edge, short_cap)
                    encoded = _encode_jpeg(resized, 85, subsampling=0)
                    resolution = "high" if raw_long >= 1200 else "medium"
                else:
                    tall_short_cap = min(1080, tall_max_edge)
                    resized = _scale_box(full, tall_max_edge, tall_short_cap)
                    encoded = _encode_jpeg(resized, 86, subsampling=0)
                    resolution = "high"
                    if aspect > 4.5 and max(orig_w, orig_h) > tall_max_edge:
                        overlap = min(96, max(32, max(orig_w, orig_h) // 40))
                        if orig_h >= orig_w:
                            mid = orig_h // 2
                            crops = (
                                full.crop((0, 0, orig_w, min(orig_h, mid + overlap // 2))),
                                full.crop((0, max(0, mid - overlap // 2), orig_w, orig_h)),
                            )
                        else:
                            mid = orig_w // 2
                            crops = (
                                full.crop((0, 0, min(orig_w, mid + overlap // 2), orig_h)),
                                full.crop((max(0, mid - overlap // 2), 0, orig_w, orig_h)),
                            )
                        tile_list = []
                        tile_thumb_list = []
                        for crop in crops:
                            scaled_tile = _scale_box(crop, tall_max_edge, tall_short_cap)
                            tile_bytes = _encode_jpeg(scaled_tile, 86, subsampling=0)
                            tile_list.append(base64.b64encode(tile_bytes).decode("ascii"))
                            tile_thumb_list.append(_make_thumb(scaled_tile, 512))
                        tiles = tuple(tile_list)
                        tile_thumbnails = tuple(tile_thumb_list)
                thumb_b64 = _make_thumb(resized, 512 if aspect > 2.2 else 448)
        return ModelImage(
            base64.b64encode(encoded).decode("ascii"),
            thumbnail=thumb_b64,
            digest=hashlib.sha256(raw).hexdigest(),
            resolution=resolution,
            tiles=tiles,
            tile_thumbnails=tile_thumbnails,
        )
    except (OSError, ValueError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise Rejected("invalid_image") from None


class ImageHTTPFailure(Rejected):
    def __init__(self, status):
        super().__init__("image_http_failed")
        self.status = status


class ImageLoader:
    def __init__(self, settings: Settings, transport=None):
        self.s, self.transport = settings, transport
        # Shared by all sessions; only one image transfer can occupy the domestic uplink.
        self.ws_images = asyncio.Semaphore(1)
        self.decode_slots = asyncio.Semaphore(1)
        self.download_slots = asyncio.Semaphore(2)
        self.cache = OrderedDict()
        self.aliases = OrderedDict()
        self.cache_bytes = 0
        self.cache_limit = 48 * 1024 * 1024
        self.fetch_locks = [asyncio.Lock() for _ in range(64)]

    def failure_status(self, ref):
        limit = min(self.s.image_max_bytes, MAX_IMAGE_BYTES)
        return "oversized" if ref.file_size and ref.file_size > limit else "unavailable"

    async def load(self, refs: list[ImageRef]) -> list[ModelImage]:
        if not refs:
            return []
        if not self.s.images_enabled or len(refs) > 2:
            raise Rejected("images_disabled_or_excessive")
        images = []
        async with self.download_slots, self._client() as client:
            for ref in refs:
                images.append(await self.cached_one(client, ref, "legacy"))
        return images

    def _client(self):
        # Keep the existing DNS pinning resolver: httpx's high-level API cannot pin a
        # validated DNS result while preserving TLS SNI. Compatibility TLS is restricted
        # to this QQ-CDN-only client; it never affects Gemini/Bilibili/Drive clients.
        legacy_tls = ssl.create_default_context()
        legacy_tls.check_hostname = False
        legacy_tls.verify_mode = ssl.CERT_NONE
        legacy_tls.set_ciphers("DEFAULT:@SECLEVEL=1")
        connector = aiohttp.TCPConnector(
            resolver=PublicResolver(), use_dns_cache=False, limit=2, ssl=legacy_tls
        )
        return aiohttp.ClientSession(
            connector=connector,
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            timeout=aiohttp.ClientTimeout(total=HTTP_IMAGE_TIMEOUT, connect=5),
        )

    @staticmethod
    def _warn(stage, exc):
        # Exception class and numeric status are sufficient diagnostics. Never log
        # exception text, file IDs, signed URLs, QQ identities or raw image bytes.
        status = getattr(exc, "status", None)
        log.warning(
            "event=image_download_failed stage=%s exception=%s status=%s",
            stage,
            type(exc).__name__,
            status if isinstance(status, int) else "none",
        )

    async def _napcat(self, ref):
        if self.transport is None or not re.fullmatch(r"[A-Za-z0-9_.=+\-]{1,1024}", ref.file):
            return None, []
        if ref.file in {".", ".."}:
            return None, []
        urls = []
        maximum = min(self.s.image_max_bytes, MAX_IMAGE_BYTES)
        async with self.ws_images:
            for action in ("get_image", "get_file"):
                try:
                    async with asyncio.timeout(15):
                        value = await self.transport.call(action, {"file": ref.file})
                    if not isinstance(value, dict):
                        raise Rejected("image_rpc_invalid")
                    declared = value.get("file_size")
                    if str(declared).isdecimal() and int(declared) > maximum:
                        raise Rejected("image_too_large")
                    raw = value.get("base64")
                    if isinstance(raw, str) and raw:
                        if raw.startswith("base64://"):
                            raw = raw[9:]
                        if len(raw) > 4 * ((maximum + 2) // 3):
                            raise Rejected("image_too_large")
                        decoded = base64.b64decode(raw, validate=True)
                        if not decoded or len(decoded) > maximum:
                            raise Rejected("image_too_large")
                        return decoded, urls
                    url = value.get("url")
                    if isinstance(url, str) and url.startswith("https://"):
                        validate_image_url(url)
                        urls.append(url)
                except (Rejected, TimeoutError, OSError, ValueError) as exc:
                    self._warn("napcat", exc)
                    if str(exc) == "image_too_large":
                        raise
        return None, urls

    async def _http_one(self, client, url, *, allow_http=False):
        maximum = min(self.s.image_max_bytes, MAX_IMAGE_BYTES)
        async with asyncio.timeout(HTTP_IMAGE_TIMEOUT):
            async with client.get(
                validate_image_url(url, allow_http=allow_http),
                allow_redirects=False,
                headers={
                    "Accept": "image/jpeg,image/png,image/webp,image/gif",
                    "User-Agent": BROWSER_AGENT,
                },
            ) as response:
                if response.status != 200:
                    raise ImageHTTPFailure(response.status)
                if (
                    response.headers.get("Content-Encoding", "identity") != "identity"
                    or response.content_type
                    not in {"image/jpeg", "image/png", "image/webp", "image/gif"}
                    or (response.content_length or 0) > maximum
                ):
                    raise Rejected("image_download_failed")
                raw = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    raw.extend(chunk)
                    if len(raw) > maximum:
                        raise Rejected("image_too_large")
            return bytes(raw)

    async def _fetch_raw_with_client(self, client, ref, *, maximum: int) -> bytes:
        if ref.file_size and ref.file_size > maximum:
            raise Rejected("image_too_large")
        raw, refreshed = await self._napcat(ref)
        if raw is not None:
            if len(raw) > maximum:
                raise Rejected("image_too_large")
            return raw
        # Prefer a freshly resolved CDN URL, otherwise use the authenticated event's URL.
        url = next(iter(refreshed), ref.url)
        if not url:
            raise Rejected("image_url_unavailable")
        try:
            raw = await self._http_one(client, url)
        except (Rejected, TimeoutError, aiohttp.ClientError, OSError, ValueError) as exc:
            self._warn("https", exc)
            # Never downgrade redirects, oversized/invalid bodies or rejected origins.
            retryable = isinstance(exc, (TimeoutError, aiohttp.ClientError, OSError)) or (
                isinstance(exc, ImageHTTPFailure) and (exc.status == 403 or exc.status >= 500)
            )
            if not retryable or urlsplit(url).hostname not in HTTP_FALLBACK_HOSTS:
                raise
            raw = await self._http_one(
                client, "http://" + url.removeprefix("https://"), allow_http=True
            )
        if len(raw) > maximum:
            raise Rejected("image_too_large")
        return raw

    async def fetch_raw(self, ref: ImageRef, *, max_bytes: int | None = None) -> bytes:
        limit = min(max_bytes or MAX_IMAGE_BYTES, self.s.image_max_bytes, MAX_IMAGE_BYTES)
        async with self.download_slots, self._client() as client:
            return await self._fetch_raw_with_client(client, ref, maximum=limit)

    async def _one(self, client, ref):
        maximum = min(self.s.image_max_bytes, MAX_IMAGE_BYTES)
        raw = await self._fetch_raw_with_client(client, ref, maximum=maximum)
        return await self.normalize(raw)

    async def normalize(self, raw):
        async with self.decode_slots:
            return await asyncio.to_thread(
                normalize_image,
                raw,
                self.s.image_max_pixels,
                standard_max_edge=self.s.image_standard_max_edge,
                tall_max_edge=self.s.image_tall_max_edge,
            )

    def _current_edges(self):
        return (self.s.image_standard_max_edge, self.s.image_tall_max_edge)

    def cache_get(self, scope, ref):
        key = self.aliases.get((scope, ref.identity))
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    async def cached_one(self, client, ref, scope):
        edges = self._current_edges()
        async with self.fetch_locks[hash((scope, ref.identity)) % len(self.fetch_locks)]:
            alias = self.aliases.get((scope, ref.identity))
            if alias in self.cache and alias[2:] == edges:
                self.cache.move_to_end(alias)
                return self.cache[alias]
            image = await self._one(client, ref)
            key = (scope, image.digest or hashlib.sha256(image.data.encode()).hexdigest(), *edges)
            if key not in self.cache:
                self.cache[key] = image
                self.cache_bytes += image.memory_bytes()
            self.aliases[(scope, ref.identity)] = key

            def evict(victim):
                old = self.cache.pop(victim)
                self.cache_bytes -= old.memory_bytes()

            own = [k for k in self.cache if k[0] == scope]
            for victim in own[:-30]:
                evict(victim)
            while self.cache_bytes > self.cache_limit or len(self.cache) > 1024:
                evict(next(iter(self.cache)))
            # Remove aliases to evicted data; cap aliases independently of repeated identities.
            self.aliases = OrderedDict((k, v) for k, v in self.aliases.items() if v in self.cache)
            while len(self.aliases) > 4096:
                self.aliases.popitem(last=False)
            return image

    async def load_results(
        self, refs: list[ImageRef], *, scope="legacy"
    ) -> list[ModelImage | None]:
        """Keep positions for sender attribution; a failed image never aborts ordinary chat."""
        if len(refs) > 2:
            raise Rejected("images_disabled_or_excessive")
        if not refs or not self.s.images_enabled:
            return [None for _ in refs]
        results = []
        async with self.download_slots, self._client() as client:
            for ref in refs:
                try:
                    results.append(await self.cached_one(client, ref, scope))
                except (Rejected, TimeoutError, aiohttp.ClientError, OSError, ValueError) as exc:
                    self._warn("fallback", exc)
                    results.append(None)
        return results
