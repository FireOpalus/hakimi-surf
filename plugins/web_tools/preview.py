"""Hardened public-web link preview with hop-by-hop SSRF & DNS-pinned transport."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import httpx

from .ssrf import (
    REDIRECT_STATUSES,
    BodyTooLargeError,
    DnsResolver,
    PinnedPublicTransport,
    SsrfBlockedError,
    build_pinned_client,
    read_bounded_body,
    resolve_public_ips,
    validate_public_url,
)

log = logging.getLogger("hakimi_plugin_web_tools")

PREVIEW_MAX_URLS = 2
PREVIEW_MAX_HOPS = 4
PREVIEW_MAX_RAW_BYTES = 32_768
PREVIEW_MAX_DECODED_BYTES = 65_536
PREVIEW_DEFAULT_BUDGET_SECONDS = 1.8
PREVIEW_MIN_BUDGET_SECONDS = 0.2
PREVIEW_MAX_BUDGET_SECONDS = 5.0

PREVIEW_BLOCKED_HOSTS = frozenset({"localhost", "b23.tv", "bilibili.com"})
PREVIEW_BLOCKED_SUFFIXES = (".bilibili.com",)

PREVIEW_HEADERS = {
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
    "Accept-Encoding": "gzip, deflate, identity",
    "User-Agent": "Mozilla/5.0 (compatible; HakimiPreview/1.4)",
}


def _clean_preview_text(value: str, limit: int) -> str:
    return re.sub(r"\s+", " ", value).strip()[:limit]


class HeadParser(HTMLParser):
    """Lightweight HTML <head> title and OpenGraph/meta description extractor."""

    def __init__(self) -> None:
        super().__init__()
        self.in_title = False
        self.title = ""
        self.og_title = ""
        self.description = ""
        self.og_description = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_l = tag.lower()
        attr_map = {str(k).lower(): str(v) for k, v in attrs if k is not None and v is not None}
        if tag_l == "title":
            self.in_title = True
        elif tag_l == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").lower()
            content = attr_map.get("content", "")
            if key in {"og:title", "twitter:title"} and not self.og_title:
                self.og_title = _clean_preview_text(content, 120)
            elif key == "description" and not self.description:
                self.description = _clean_preview_text(content, 160)
            elif key in {"og:description", "twitter:description"} and not self.og_description:
                self.og_description = _clean_preview_text(content, 160)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.in_title and len(self.title) < 120:
            self.title = (self.title + data)[:120]

    def result(self) -> dict[str, str] | None:
        title = _clean_preview_text(self.title, 120) or self.og_title
        if not title:
            return None
        description = self.description or self.og_description
        return {"title": title, "description": description}


def extract_urls(text: str, *, limit: int = PREVIEW_MAX_URLS) -> list[str]:
    """Extract up to `limit` unique HTTP(S) URLs from chat text."""
    if not isinstance(text, str) or not text:
        return []
    matches = re.findall(r'https?://[^\s<>"\'\]，。！？；、（）]+', text)
    cleaned = [u.rstrip("。，！？；、）》)") for u in matches]
    return list(dict.fromkeys(u for u in cleaned if u))[:limit]


def _decode_html_bytes(raw: bytes, content_type: str) -> str:
    if not raw:
        return ""
    charset = "utf-8"
    match = re.search(r"charset=([a-z0-9_\-]+)", content_type, re.I)
    if match:
        candidate = match[1].lower()
        if candidate in {"gbk", "gb2312", "gb18030"}:
            charset = "gb18030"
    return raw.decode(charset, "replace")


@dataclass(frozen=True)
class PreviewOutcome:
    previews: list[dict[str, str]]
    ssrf_blocked_count: int
    total_urls: int
    oversized_count: int = 0


def _validate_preview_url(url: str) -> str:
    return validate_public_url(
        url,
        allow_http=True,
        allow_ip_literals=True,
        max_length=2048,
        blocked_hosts=PREVIEW_BLOCKED_HOSTS,
        blocked_suffixes=PREVIEW_BLOCKED_SUFFIXES,
    )


async def _fetch_single_preview(
    client: httpx.AsyncClient,
    url: str,
    *,
    pre_check_dns: bool,
    dns_resolver: DnsResolver | None,
) -> tuple[dict[str, str] | None, str | None]:
    """Fetch one URL with hop-by-hop redirect SSRF enforcement.

    Returns `(preview_dict_or_none, failure_kind)` where `failure_kind` is
    `"ssrf"`, `"oversized"`, or `None`.
    """
    current = url
    try:
        for _ in range(PREVIEW_MAX_HOPS):
            _validate_preview_url(current)
            if pre_check_dns:
                parsed = urlsplit(current)
                port = parsed.port or (80 if parsed.scheme.lower() == "http" else 443)
                await resolve_public_ips(parsed.hostname or "", port, resolver=dns_resolver)

            async with client.stream("GET", current, headers=PREVIEW_HEADERS) as response:
                if response.status_code in REDIRECT_STATUSES:
                    location = response.headers.get("Location", "").strip()
                    if not location:
                        return None, None
                    next_url = urljoin(current, location)
                    _validate_preview_url(next_url)
                    current = next_url
                    continue

                ctype = response.headers.get("Content-Type", "").lower()
                if response.status_code != 200 or not any(
                    token in ctype for token in ("text/html", "application/xhtml+xml")
                ):
                    return None, None

                raw = await read_bounded_body(
                    response,
                    max_raw_bytes=PREVIEW_MAX_RAW_BYTES,
                    max_decoded_bytes=PREVIEW_MAX_DECODED_BYTES,
                    strict_size=False,
                )
                text_body = _decode_html_bytes(raw, ctype)
                if not text_body:
                    return None, None
                parser = HeadParser()
                parser.feed(text_body)
                parsed_head = parser.result()
                if not parsed_head:
                    return None, None
                return {
                    "url": current,
                    "title": parsed_head["title"],
                    "description": parsed_head["description"],
                }, None
        return None, None
    except SsrfBlockedError:
        log.info("event=web_tools_preview_ssrf_blocked")
        return None, "ssrf"
    except BodyTooLargeError:
        log.info("event=web_tools_preview_body_too_large")
        return None, "oversized"
    except Exception as exc:
        log.debug("event=web_tools_preview_error type=%s", type(exc).__name__)
        return None, None


async def _fetch_single_preview_bounded(
    client: httpx.AsyncClient,
    url: str,
    *,
    budget: float,
    pre_check_dns: bool,
    dns_resolver: DnsResolver | None,
) -> tuple[dict[str, str] | None, str | None]:
    try:
        async with asyncio.timeout(budget):
            return await _fetch_single_preview(
                client,
                url,
                pre_check_dns=pre_check_dns,
                dns_resolver=dns_resolver,
            )
    except TimeoutError:
        log.debug("event=web_tools_preview_timeout")
        return None, None


async def preview_urls(
    urls: list[str],
    *,
    budget_seconds: float = PREVIEW_DEFAULT_BUDGET_SECONDS,
    transport: httpx.AsyncBaseTransport | None = None,
    dns_resolver: DnsResolver | None = None,
) -> PreviewOutcome:
    """Preview up to 2 public HTTP(S) URLs with hard timeout and SSRF isolation."""
    bounded_urls = [u.strip() for u in urls if isinstance(u, str) and u.strip()][:PREVIEW_MAX_URLS]
    if not bounded_urls:
        return PreviewOutcome(previews=[], ssrf_blocked_count=0, total_urls=0, oversized_count=0)

    budget = min(max(float(budget_seconds), PREVIEW_MIN_BUDGET_SECONDS), PREVIEW_MAX_BUDGET_SECONDS)
    pre_check_dns = (
        transport is not None
        and not isinstance(transport, PinnedPublicTransport)
        and dns_resolver is not None
    )

    async with build_pinned_client(
        timeout=budget,
        transport=transport,
        dns_resolver=dns_resolver,
        max_connections=4,
    ) as client:
        raw_results = await asyncio.gather(
            *(
                _fetch_single_preview_bounded(
                    client,
                    u,
                    budget=budget,
                    pre_check_dns=pre_check_dns,
                    dns_resolver=dns_resolver,
                )
                for u in bounded_urls
            ),
            return_exceptions=True,
        )

    previews: list[dict[str, str]] = []
    ssrf_count = 0
    oversized_count = 0
    for entry in raw_results:
        if isinstance(entry, asyncio.CancelledError):
            raise entry
        if isinstance(entry, BaseException):
            log.debug("event=web_tools_preview_error type=%s", type(entry).__name__)
            continue
        item, failure = entry
        if item is not None:
            previews.append(item)
        elif failure == "ssrf":
            ssrf_count += 1
        elif failure == "oversized":
            oversized_count += 1

    return PreviewOutcome(
        previews=previews,
        ssrf_blocked_count=ssrf_count,
        total_urls=len(bounded_urls),
        oversized_count=oversized_count,
    )
