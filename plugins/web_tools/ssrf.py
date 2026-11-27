"""Hardened SSRF guard, anti-DNS-rebinding transport, and bounded HTTP helpers."""

from __future__ import annotations

import asyncio
import functools
import ipaddress
import re
import socket
import ssl
import typing
import zlib
from collections.abc import Awaitable, Callable, Iterable, Sequence
from urllib.parse import urlsplit

import httpx

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
DnsResolver = Callable[[str, int], Awaitable[Sequence[typing.Any]]]

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
DEFAULT_ALLOWED_PORTS = frozenset({80, 443})
HARD_MAX_CONTENT_LENGTH = 10 * 1024 * 1024

_CGNAT_NET = ipaddress.ip_network("100.64.0.0/10")
_BENCHMARK_NET = ipaddress.ip_network("198.18.0.0/15")
_6TO4_RELAY_NET = ipaddress.ip_network("192.88.99.0/24")
_BAD_URL_CHARS_RE = re.compile(r"[\s<>\\\x00-\x1f\x7f]")
_DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_TLD_LABEL_RE = re.compile(r"^(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})\Z")
_URL_CREDS_RE = re.compile(r"(https?://)([^/@\s]+)@", re.I)

_ALWAYS_BLOCKED_HOSTS = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata",
        "metadata.goog",
        "metadata.google.internal",
        "instance-data",
    }
)
_ALWAYS_BLOCKED_SUFFIXES = (
    ".local",
    ".localhost",
    ".internal",
    ".invalid",
    ".arpa",
    ".lan",
    ".localdomain",
    ".intranet",
    ".corp",
    ".home",
)


class SsrfBlockedError(Exception):
    """Raised when a URL, port, hostname, or resolved IP fails SSRF checks."""

    def __init__(self, code: str = "ssrf_blocked") -> None:
        self.code = code
        super().__init__(code)


class BodyTooLargeError(Exception):
    """Raised when an upstream response exceeds configured raw or decoded limits."""

    def __init__(self, code: str = "response_body_too_large") -> None:
        self.code = code
        super().__init__(code)


def is_public_ip(ip: IPAddress) -> bool:
    """Return True only for globally routable unicast public IPv4/IPv6 addresses."""
    if (
        not ip.is_global
        or ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        return False
    if isinstance(ip, ipaddress.IPv4Address):
        if ip in _CGNAT_NET or ip in _BENCHMARK_NET or ip in _6TO4_RELAY_NET:
            return False
        return True
    if (
        ip.is_site_local
        or ip.ipv4_mapped is not None
        or ip.sixtofour is not None
        or ip.teredo is not None
    ):
        return False
    return True


def _is_valid_public_domain(host: str) -> bool:
    if not host or len(host) > 253 or "." not in host or ".." in host:
        return False
    labels = host.split(".")
    if len(labels) < 2:
        return False
    if not all(_DOMAIN_LABEL_RE.fullmatch(label) for label in labels):
        return False
    return bool(_TLD_LABEL_RE.fullmatch(labels[-1]))


def validate_public_url(
    url: str,
    *,
    allow_http: bool = True,
    allowed_ports: frozenset[int] | None = None,
    max_length: int = 2048,
    blocked_hosts: frozenset[str] = frozenset(),
    blocked_suffixes: tuple[str, ...] = (),
    allow_ip_literals: bool = True,
    forbid_query_or_fragment: bool = False,
) -> str:
    """Strictly validate a URL before any DNS resolution or network connection."""
    if not isinstance(url, str) or not url or len(url) > max_length:
        raise SsrfBlockedError("ssrf_blocked")
    if _BAD_URL_CHARS_RE.search(url):
        raise SsrfBlockedError("ssrf_blocked")

    try:
        parsed = urlsplit(url)
    except ValueError:
        raise SsrfBlockedError("ssrf_blocked") from None

    scheme = parsed.scheme.lower()
    valid_schemes = {"https", "http"} if allow_http else {"https"}
    if scheme not in valid_schemes:
        raise SsrfBlockedError("ssrf_blocked")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise SsrfBlockedError("ssrf_blocked")
    if forbid_query_or_fragment and (parsed.query or parsed.fragment):
        raise SsrfBlockedError("ssrf_blocked")

    # Reject stray square brackets outside IPv6 host syntax.
    path_rest = parsed.path + parsed.query + parsed.fragment
    if "[" in path_rest or "]" in path_rest:
        raise SsrfBlockedError("ssrf_blocked")

    try:
        port = parsed.port
    except ValueError:
        raise SsrfBlockedError("ssrf_blocked") from None

    if allowed_ports is not None:
        if port is not None and port not in allowed_ports:
            raise SsrfBlockedError("ssrf_blocked")
    else:
        expected_port = 80 if scheme == "http" else 443
        if port is not None and port != expected_port:
            raise SsrfBlockedError("ssrf_blocked")

    raw_host = parsed.hostname or ""
    if not raw_host or "%" in raw_host or raw_host.startswith("."):
        raise SsrfBlockedError("ssrf_blocked")
    host = raw_host.lower().rstrip(".")
    if not host or len(host) > 253 or ".." in host:
        raise SsrfBlockedError("ssrf_blocked")

    if host in _ALWAYS_BLOCKED_HOSTS or host in blocked_hosts:
        raise SsrfBlockedError("ssrf_blocked")
    if host.endswith(_ALWAYS_BLOCKED_SUFFIXES) or (
        blocked_suffixes and host.endswith(blocked_suffixes)
    ):
        raise SsrfBlockedError("ssrf_blocked")

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        if not _is_valid_public_domain(host):
            raise SsrfBlockedError("ssrf_blocked") from None
    else:
        if not allow_ip_literals or not is_public_ip(ip):
            raise SsrfBlockedError("ssrf_blocked")

    return url


def _extract_ip_from_entry(entry: typing.Any) -> IPAddress:
    if isinstance(entry, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return entry
    if isinstance(entry, str):
        return ipaddress.ip_address(entry.strip())
    if isinstance(entry, tuple) and len(entry) >= 5 and isinstance(entry[4], tuple) and entry[4]:
        raw = str(entry[4][0])
        if "%" in raw:
            raise SsrfBlockedError("ssrf_blocked")
        return ipaddress.ip_address(raw)
    raise SsrfBlockedError("ssrf_blocked")


async def resolve_public_ips(
    host: str,
    port: int,
    *,
    resolver: DnsResolver | None = None,
) -> tuple[IPAddress, ...]:
    """Resolve `host` and require every returned IP address to be globally public."""
    clean_host = (host or "").strip().lower().rstrip(".")
    if not clean_host or "%" in clean_host:
        raise SsrfBlockedError("ssrf_blocked")
    if clean_host in _ALWAYS_BLOCKED_HOSTS or clean_host.endswith(_ALWAYS_BLOCKED_SUFFIXES):
        raise SsrfBlockedError("ssrf_blocked")

    try:
        literal = ipaddress.ip_address(clean_host)
    except ValueError:
        literal = None
    if literal is not None:
        if not is_public_ip(literal):
            raise SsrfBlockedError("ssrf_blocked")
        return (literal,)

    try:
        if resolver is not None:
            entries = await resolver(clean_host, port)
        else:
            entries = await asyncio.get_running_loop().getaddrinfo(
                clean_host, port, type=socket.SOCK_STREAM
            )
    except SsrfBlockedError:
        raise
    except OSError:
        raise SsrfBlockedError("ssrf_dns_failed") from None

    seen: dict[IPAddress, None] = {}
    for entry in entries or ():
        try:
            ip = _extract_ip_from_entry(entry)
        except (ValueError, SsrfBlockedError):
            raise SsrfBlockedError("ssrf_blocked") from None
        if not is_public_ip(ip):
            raise SsrfBlockedError("ssrf_blocked")
        seen[ip] = None

    if not seen:
        raise SsrfBlockedError("ssrf_dns_failed")
    return tuple(seen)


def build_pinned_request(
    request: httpx.Request,
    pinned_ip: IPAddress,
    *,
    original_host: str,
    port: int,
) -> httpx.Request:
    """Clone an httpx request to connect directly to a validated public IP with Host + TLS SNI."""
    scheme = request.url.scheme.lower()
    default_port = 80 if scheme == "http" else 443
    host_display = (
        f"[{original_host}]"
        if ":" in original_host and not original_host.startswith("[")
        else original_host
    )
    host_header = host_display if port == default_port else f"{host_display}:{port}"
    pinned_url = request.url.copy_with(
        host=str(pinned_ip),
        port=None if port == default_port else port,
    )

    headers = httpx.Headers(request.headers)
    headers["Host"] = host_header
    headers["Connection"] = "close"

    extensions = dict(request.extensions)
    if scheme == "https":
        extensions["sni_hostname"] = original_host

    return httpx.Request(
        method=request.method,
        url=pinned_url,
        headers=headers,
        stream=request.stream,
        extensions=extensions,
    )


@functools.lru_cache(maxsize=1)
def get_default_ssl_context() -> ssl.SSLContext:
    """Return a process-wide cached default SSLContext so CA bundles aren't reloaded per request."""
    return ssl.create_default_context()


class _NonClosingTransport(httpx.AsyncBaseTransport):
    """Wrap a caller-owned transport so AsyncClient exit does not close shared pools."""

    def __init__(self, wrapped: httpx.AsyncBaseTransport) -> None:
        self._wrapped = wrapped

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self._wrapped.handle_async_request(request)

    async def aclose(self) -> None:
        return None


class PinnedPublicTransport(httpx.AsyncBaseTransport):
    """httpx transport pinning every TCP socket to validated public IPs with full TLS SNI."""

    def __init__(
        self,
        *,
        inner: httpx.AsyncBaseTransport | None = None,
        dns_resolver: DnsResolver | None = None,
        allowed_ports: frozenset[int] = DEFAULT_ALLOWED_PORTS,
        max_connections: int = 8,
        ssl_context: ssl.SSLContext | None = None,
        dns_timeout: float | None = None,
    ) -> None:
        self._owns_inner = inner is None
        self._ssl_context = ssl_context if ssl_context is not None else get_default_ssl_context()
        self._inner = (
            inner
            if inner is not None
            else httpx.AsyncHTTPTransport(
                verify=self._ssl_context,
                trust_env=False,
                http1=True,
                http2=False,
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=0,
                    keepalive_expiry=0.0,
                ),
                retries=0,
            )
        )
        self._dns_resolver = dns_resolver
        self._allowed_ports = allowed_ports
        self._dns_timeout = (
            float(dns_timeout) if dns_timeout is not None and dns_timeout > 0 else None
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        scheme = request.url.scheme.lower()
        if scheme not in {"http", "https"}:
            raise SsrfBlockedError("ssrf_blocked")
        default_port = 80 if scheme == "http" else 443
        port = request.url.port if request.url.port is not None else default_port
        if port not in self._allowed_ports or port != default_port:
            raise SsrfBlockedError("ssrf_blocked")

        original_host = (request.url.host or "").strip().lower().rstrip(".")
        if not original_host:
            raise SsrfBlockedError("ssrf_blocked")

        connect_timeout: float | None = None
        timeout_ext = request.extensions.get("timeout")
        if isinstance(timeout_ext, dict):
            raw_ct = timeout_ext.get("connect")
            if isinstance(raw_ct, (int, float)) and not isinstance(raw_ct, bool) and raw_ct > 0:
                connect_timeout = float(raw_ct)
        if connect_timeout is None:
            connect_timeout = self._dns_timeout

        try:
            if connect_timeout is not None:
                async with asyncio.timeout(connect_timeout):
                    ips = await resolve_public_ips(original_host, port, resolver=self._dns_resolver)
            else:
                ips = await resolve_public_ips(original_host, port, resolver=self._dns_resolver)
        except TimeoutError:
            raise httpx.ConnectTimeout("DNS resolution timed out", request=request) from None

        replayable = isinstance(request.stream, httpx.ByteStream)
        candidates = ips if replayable else ips[:1]
        last_exc: Exception | None = None
        for ip in candidates:
            pinned = build_pinned_request(
                request,
                ip,
                original_host=original_host,
                port=port,
            )
            try:
                return await self._inner.handle_async_request(pinned)
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                last_exc = exc
        if last_exc is not None:
            raise last_exc
        raise SsrfBlockedError("ssrf_dns_failed")

    async def aclose(self) -> None:
        if self._owns_inner:
            await self._inner.aclose()


def build_pinned_client(
    *,
    timeout: float,
    transport: httpx.AsyncBaseTransport | None = None,
    dns_resolver: DnsResolver | None = None,
    allowed_ports: frozenset[int] = DEFAULT_ALLOWED_PORTS,
    max_connections: int = 8,
    ssl_context: ssl.SSLContext | None = None,
) -> httpx.AsyncClient:
    """Create a hardened httpx AsyncClient with trust_env=False and redirects disabled."""
    effective_transport = (
        _NonClosingTransport(transport)
        if transport is not None
        else PinnedPublicTransport(
            dns_resolver=dns_resolver,
            allowed_ports=allowed_ports,
            max_connections=max_connections,
            ssl_context=ssl_context,
        )
    )
    return httpx.AsyncClient(
        transport=effective_transport,
        timeout=httpx.Timeout(timeout),
        trust_env=False,
        follow_redirects=False,
    )


def decompress_bounded(
    raw: bytes,
    content_encoding: str,
    *,
    max_decoded_bytes: int,
    strict_size: bool,
) -> bytes:
    """Bounded gzip/deflate decompression resilient to decompression bombs."""
    enc = (content_encoding or "").strip().lower()
    if enc in {"", "identity"}:
        if len(raw) > max_decoded_bytes:
            if strict_size:
                raise BodyTooLargeError("response_body_too_large")
            return raw[:max_decoded_bytes]
        return raw

    limit = max_decoded_bytes + (1 if strict_size else 0)
    if enc == "gzip":
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            out = decompressor.decompress(raw, max_length=limit)
        except zlib.error:
            return b"" if not strict_size else _raise_invalid_compression()
        if strict_size and (len(out) > max_decoded_bytes or decompressor.unconsumed_tail):
            raise BodyTooLargeError("response_body_too_large")
        return out[:max_decoded_bytes]

    if enc == "deflate":
        decompressor = zlib.decompressobj()
        try:
            out = decompressor.decompress(raw, max_length=limit)
        except zlib.error:
            decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
            try:
                out = decompressor.decompress(raw, max_length=limit)
            except zlib.error:
                return b"" if not strict_size else _raise_invalid_compression()
        if strict_size and (len(out) > max_decoded_bytes or decompressor.unconsumed_tail):
            raise BodyTooLargeError("response_body_too_large")
        return out[:max_decoded_bytes]

    if strict_size:
        raise ValueError("unsupported_content_encoding")
    return b""


def _raise_invalid_compression() -> typing.NoReturn:
    raise ValueError("invalid_compressed_body")


async def read_bounded_body(
    response: httpx.Response,
    *,
    max_raw_bytes: int,
    max_decoded_bytes: int,
    strict_size: bool = True,
    max_content_length: int = HARD_MAX_CONTENT_LENGTH,
) -> bytes:
    """Read an httpx response with strict wire-byte and post-decompression limits."""
    raw_cl = response.headers.get("Content-Length", "").strip()
    if raw_cl:
        try:
            declared_len = int(raw_cl)
        except ValueError:
            declared_len = -1
        if declared_len > max_content_length or (strict_size and declared_len > max_raw_bytes):
            raise BodyTooLargeError("response_body_too_large")

    encoding = response.headers.get("Content-Encoding", "identity")
    if response.is_stream_consumed:
        # Synthetic MockTransport responses constructed with `content=` already decoded bytes.
        data = response.content
        if len(data) > max_decoded_bytes:
            if strict_size:
                raise BodyTooLargeError("response_body_too_large")
            return data[:max_decoded_bytes]
        return data

    buf = bytearray()
    async for chunk in response.aiter_raw(chunk_size=8192):
        if not chunk:
            continue
        remaining = max_raw_bytes - len(buf)
        if len(chunk) > remaining:
            if strict_size:
                raise BodyTooLargeError("response_body_too_large")
            buf.extend(chunk[:remaining])
            break
        buf.extend(chunk)

    return decompress_bounded(
        bytes(buf),
        encoding,
        max_decoded_bytes=max_decoded_bytes,
        strict_size=strict_size,
    )


def scrub_secrets(text: str, secrets: Iterable[str] = ()) -> str:
    """Redact configured secrets and embedded URL credentials from strings/logs."""
    if not text:
        return ""
    out = _URL_CREDS_RE.sub(r"\1[REDACTED]@", text)
    candidates = sorted(
        {s for s in secrets if isinstance(s, str) and len(s) >= 6},
        key=len,
        reverse=True,
    )
    for secret in candidates:
        if secret in out:
            out = out.replace(secret, "[REDACTED]")
    return out
