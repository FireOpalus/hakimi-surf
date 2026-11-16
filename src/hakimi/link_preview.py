"""Bounded public-web title probe; validates every redirect hop and pins public DNS."""

import asyncio
import ipaddress
import re
import zlib
from contextlib import nullcontext
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import aiohttp

from .media import PublicResolver

REDIRECT_STATUSES = {301, 302, 303, 307, 308}


def _clean_preview_text(value: str, limit: int) -> str:
    return re.sub(r"\s+", " ", value).strip()[:limit]


class Head(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_title = False
        self.title = ""
        self.og_title = ""
        self.description = ""
        self.og_description = ""

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs = {str(k).lower(): str(v) for k, v in attrs if k is not None and v is not None}
        if tag == "title":
            self.in_title = True
        elif tag == "meta":
            key = (attrs.get("property") or attrs.get("name") or "").lower()
            content = attrs.get("content", "")
            if key in {"og:title", "twitter:title"} and not self.og_title:
                self.og_title = _clean_preview_text(content, 120)
            elif key == "description" and not self.description:
                self.description = _clean_preview_text(content, 160)
            elif key in {"og:description", "twitter:description"} and not self.og_description:
                self.og_description = _clean_preview_text(content, 160)

    def handle_endtag(self, tag):
        if tag.lower() == "title":
            self.in_title = False

    def handle_data(self, text):
        if self.in_title and len(self.title) < 120:
            self.title = (self.title + text)[:120]

    def result(self) -> dict | None:
        title = _clean_preview_text(self.title, 120) or self.og_title
        if not title:
            return None
        description = self.description or self.og_description
        return {"title": title, "description": description}


def public_url(url):
    p = urlsplit(url)
    host = (p.hostname or "").lower().rstrip(".")
    if (
        p.scheme not in {"https", "http"}
        or p.port not in {None, 80, 443}
        or p.username
        or p.password
        or not host
        or len(url) > 2048
        or host in {"localhost", "b23.tv", "bilibili.com"}
        or host.endswith((".local", ".localhost", ".internal", ".invalid", ".bilibili.com"))
    ):
        return False
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_global and not ip.is_multicast and not (ip.version == 6 and ip.ipv4_mapped)
    except ValueError:
        return "." in host


def _decode_html(raw: bytes, encoding: str, content_type: str) -> str:
    enc = encoding.strip().lower()
    if enc == "gzip":
        raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw, max_length=65536)
    elif enc == "deflate":
        try:
            raw = zlib.decompressobj().decompress(raw, max_length=65536)
        except zlib.error:
            raw = zlib.decompressobj(-zlib.MAX_WBITS).decompress(raw, max_length=65536)
    elif enc not in {"", "identity"}:
        return ""
    charset = "utf-8"
    match = re.search(r"charset=([a-z0-9_\-]+)", content_type, re.I)
    if match:
        candidate = match[1].lower()
        if candidate in {"gbk", "gb2312", "gb18030"}:
            charset = "gb18030"
    return raw.decode(charset, "replace")


def _extract_urls(text: str) -> list[str]:
    matches = re.findall(r'https?://[^\s<>"\'\]，。！？；、（）]+', text)
    cleaned = [u.rstrip("。，！？；、）》)") for u in matches]
    return list(dict.fromkeys(u for u in cleaned if u))[:2]


async def preview_links(text, slots=None, *, budget_seconds: float = 0.8):
    urls = _extract_urls(text)
    headers = {
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
        "Accept-Encoding": "gzip, deflate, identity",
        "User-Agent": "Mozilla/5.0 (compatible; HakimiPreview/1.4)",
    }

    async def fetch(client, url):
        current = url
        try:
            for _ in range(4):
                if not public_url(current):
                    return None
                async with client.get(current, allow_redirects=False, headers=headers) as response:
                    if response.status in REDIRECT_STATUSES:
                        location = response.headers.get("Location", "").strip()
                        if not location:
                            return None
                        current = urljoin(current, location)
                        continue
                    ctype = response.headers.get("Content-Type", "").lower()
                    if response.status != 200 or not any(
                        token in ctype for token in ("text/html", "application/xhtml+xml")
                    ):
                        return None
                    raw = await response.content.read(32768)
                    text_body = _decode_html(
                        raw, response.headers.get("Content-Encoding", "identity"), ctype
                    )
                    if not text_body:
                        return None
                    head = Head()
                    head.feed(text_body)
                    return head.result()
            return None
        except Exception:
            return None

    try:
        async with asyncio.timeout(budget_seconds), slots or nullcontext():
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    resolver=PublicResolver(), use_dns_cache=False, limit=2
                ),
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
                timeout=aiohttp.ClientTimeout(total=budget_seconds),
            ) as client:
                return [p for p in await asyncio.gather(*(fetch(client, u) for u in urls)) if p]
    except TimeoutError:
        return []
