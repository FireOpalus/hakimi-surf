"""Optional 800 ms public-web title probe; no cookies, redirects, or private addresses."""

import asyncio
import ipaddress
import re
from contextlib import nullcontext
from html.parser import HTMLParser
from urllib.parse import urlsplit

import aiohttp

from .media import PublicResolver


class Head(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_title, self.title, self.description = False, "", ""

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "title":
            self.in_title = True
        if tag == "meta" and attrs.get("name", "").lower() == "description":
            self.description = attrs.get("content", "")[:100]

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False

    def handle_data(self, text):
        if self.in_title:
            self.title = (self.title + text)[:100]


def public_url(url):
    p = urlsplit(url)
    host = (p.hostname or "").lower()
    if (
        p.scheme not in {"https", "http"}
        or p.port not in {None, 80, 443}
        or p.username
        or p.password
        or not host
        or len(url) > 2048
        or host in {"localhost", "b23.tv", "bilibili.com"}
        or host.endswith((".local", ".bilibili.com"))
    ):
        return False
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_global and not ip.is_multicast and not (ip.version == 6 and ip.ipv4_mapped)
    except ValueError:
        return "." in host


async def preview_links(text, slots=None):
    urls = list(dict.fromkeys(re.findall(r'https?://[^\s<>"\]]+', text)))[:2]

    async def fetch(client, url):
        try:
            if not public_url(url):
                return None
            async with client.get(url, allow_redirects=False) as response:
                if response.status != 200 or "text/html" not in response.headers.get(
                    "Content-Type", ""
                ):
                    return None
                raw = await response.content.read(32768)
                head = Head()
                head.feed(raw.decode("utf-8", "replace"))
                return (
                    {"title": head.title, "description": head.description} if head.title else None
                )
        except Exception:
            return None

    try:
        async with asyncio.timeout(0.8), slots or nullcontext():
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    resolver=PublicResolver(), use_dns_cache=False, limit=2
                ),
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
                timeout=aiohttp.ClientTimeout(total=0.8),
            ) as client:
                return [p for p in await asyncio.gather(*(fetch(client, u) for u in urls)) if p]
    except TimeoutError:
        return []
