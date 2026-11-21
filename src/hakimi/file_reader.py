"""Bounded plain-text / code / error-log file reader with strict allowlist & SSRF checks."""

import asyncio
import base64
import binascii
import ipaddress
import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlsplit

import aiohttp

from .media import PublicResolver
from .memory_compressor import check_memory_text
from .models import FileRef, Message
from .security import Rejected, authorize, clean_text, configured_secrets

MAX_FILE_HARD_BYTES = 262_144
MAX_AUDIO_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_EXCERPT_CHARS = 3200
HTTP_TIMEOUT = 12
ALLOWED_AUDIO_EXTENSIONS = frozenset({".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus"})
ALLOWED_TEXT_EXTENSIONS = frozenset(
    {
        ".txt",
        ".log",
        ".md",
        ".markdown",
        ".rst",
        ".csv",
        ".tsv",
        ".py",
        ".pyi",
        ".js",
        ".mjs",
        ".cjs",
        ".ts",
        ".tsx",
        ".jsx",
        ".json",
        ".jsonl",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
        ".conf",
        ".sh",
        ".bash",
        ".zsh",
        ".sql",
        ".html",
        ".htm",
        ".css",
        ".xml",
        ".c",
        ".h",
        ".cpp",
        ".hpp",
        ".rs",
        ".go",
        ".java",
        ".kt",
        ".swift",
        ".rb",
        ".php",
        ".lua",
    }
)
ALLOWED_SPECIAL_FILENAMES = frozenset({"dockerfile", ".gitignore", ".env.example"})

SCHEMA = """
CREATE TABLE IF NOT EXISTS file_read_attempts (
 id TEXT PRIMARY KEY, day TEXT NOT NULL, subject TEXT NOT NULL, created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS file_read_day_subj ON file_read_attempts(day,subject);
"""

log = logging.getLogger("hakimi")


def _allowed_filename(name: str) -> bool:
    clean = PurePosixPath(str(name or "").replace("\\", "/")).name.strip().lower()
    if not clean or clean in {".", ".."}:
        return False
    if clean == ".env" or (clean.startswith(".env.") and clean != ".env.example"):
        return False
    if clean in ALLOWED_SPECIAL_FILENAMES:
        return True
    suffix = PurePosixPath(clean).suffix.lower()
    return suffix in ALLOWED_TEXT_EXTENSIONS


def _sanitize_audio_filename(name: str) -> str | None:
    clean = PurePosixPath(str(name or "").replace("\\", "/")).name.strip()
    if not clean or clean in {".", ".."} or any(ord(ch) < 32 for ch in clean):
        return None
    suffix = PurePosixPath(clean.lower()).suffix
    if suffix not in ALLOWED_AUDIO_EXTENSIONS:
        return None
    return clean[:120]


def _public_download_url(url: str) -> str | None:
    if not isinstance(url, str) or len(url) > 4096:
        return None
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme not in {"https", "http"}
        or parsed.port not in ({None, 443} if parsed.scheme == "https" else {None, 80})
        or parsed.username
        or parsed.password
        or parsed.fragment
        or not host
        or host == "localhost"
        or host.endswith((".local", ".localhost", ".internal", ".invalid"))
    ):
        return None
    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global or ip.is_multicast or (ip.version == 6 and ip.ipv4_mapped):
            return None
    except ValueError:
        if "." not in host:
            return None
    return url


@dataclass(frozen=True)
class LoadedTextFile:
    name: str
    content: str
    char_count: int
    byte_size: int
    encoding: str
    truncated: bool


def decode_text_bytes(raw: bytes, *, secrets: tuple[str, ...], settings) -> tuple[str, str, bool]:
    if not raw or b"\x00" in raw[:8192]:
        raise Rejected("binary_unsupported")
    text = ""
    encoding = ""
    for candidate in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            text = raw.decode(candidate, errors="strict")
            encoding = "utf-8" if candidate.startswith("utf-8") else candidate
            break
        except UnicodeDecodeError:
            continue
    if not encoding:
        raise Rejected("encoding_unsupported")
    text = clean_text(text.replace("\r\n", "\n").replace("\r", "\n"))
    if any(secret and len(secret) >= 8 and secret in text for secret in secrets):
        raise Rejected("credential_in_input")
    check_memory_text(text, settings)
    total = len(text)
    if total > MAX_EXCERPT_CHARS:
        head = text[:1000]
        tail = text[-2100:]
        excerpt = f"{head}\n... [中间省略 {total - 3100} 字] ...\n{tail}"
        return excerpt, encoding, True
    return text, encoding, False


class FileReader:
    def __init__(self, settings, store, transport=None):
        self.s, self.store, self.transport = settings, store, transport
        self.slots = asyncio.Semaphore(2)
        self.init_lock = asyncio.Lock()
        self.initialized = False

    async def initialize(self):
        if self.initialized:
            return
        async with self.init_lock:
            if not self.initialized:
                async with self.store.transaction():
                    await self.store.db.executescript(SCHEMA)
                self.initialized = True

    def _max_bytes(self) -> int:
        return min(self.s.file_max_bytes, MAX_FILE_HARD_BYTES)

    def _display_name(self, ref: FileRef) -> str:
        if ref.name:
            return ref.name
        if ref.url:
            tail = PurePosixPath(urlsplit(ref.url).path).name
            if tail:
                return tail[:120]
        return "未命名文件"

    def _client(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(resolver=PublicResolver(), use_dns_cache=False, limit=2)
        return aiohttp.ClientSession(
            connector=connector,
            trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT, connect=5),
        )

    async def _via_transport(self, ref: FileRef, context_msg: Message, maximum: int):
        if self.transport is None or not ref.file_id:
            return None, []
        actions: list[tuple[str, dict]] = []
        if context_msg.kind == "group":
            params = {"group_id": int(context_msg.conversation_id), "file_id": ref.file_id}
            if ref.busid is not None:
                params["busid"] = ref.busid
            actions.append(("get_group_file_url", params))
        else:
            actions.append(
                (
                    "get_private_file_url",
                    {"user_id": int(context_msg.conversation_id), "file_id": ref.file_id},
                )
            )
        actions.append(("get_file", {"file_id": ref.file_id}))

        urls: list[str] = []
        for action, params in actions:
            try:
                async with asyncio.timeout(10):
                    value = await self.transport.call(action, params)
                if not isinstance(value, dict):
                    continue
                declared = value.get("file_size", value.get("size"))
                if str(declared).isdecimal() and int(declared) > maximum:
                    raise Rejected("oversized")
                b64 = value.get("base64")
                if isinstance(b64, str) and b64:
                    if b64.startswith("base64://"):
                        b64 = b64[9:]
                    if len(b64) > 4 * ((maximum + 2) // 3):
                        raise Rejected("oversized")
                    decoded = base64.b64decode(b64, validate=True)
                    if len(decoded) > maximum:
                        raise Rejected("oversized")
                    return decoded, urls
                # Deliberately ignore local path keys ("file" / "path") on disk.
                url = value.get("url")
                checked = _public_download_url(url) if isinstance(url, str) else None
                if checked:
                    urls.append(checked)
            except Rejected as exc:
                if str(exc) == "oversized":
                    raise
            except (TimeoutError, OSError, ValueError, TypeError, binascii.Error) as exc:
                log.warning("event=file_rpc_failed exception=%s", type(exc).__name__)
        return None, urls

    async def _download_http(self, url: str, maximum: int) -> bytes:
        current = url
        async with self.slots, self._client() as client:
            for _ in range(3):
                checked = _public_download_url(current)
                if not checked:
                    raise Rejected("unavailable")
                async with client.get(checked, allow_redirects=False) as resp:
                    if resp.status in {301, 302, 303, 307, 308}:
                        loc = resp.headers.get("Location", "").strip()
                        if not loc:
                            raise Rejected("unavailable")
                        current = urljoin(checked, loc)
                        continue
                    if resp.status != 200:
                        raise Rejected("unavailable")
                    if (resp.content_length or 0) > maximum:
                        raise Rejected("oversized")
                    buf = bytearray()
                    async for chunk in resp.content.iter_chunked(32768):
                        buf.extend(chunk)
                        if len(buf) > maximum:
                            raise Rejected("oversized")
                    return bytes(buf)
        raise Rejected("unavailable")

    async def read_one(self, ref: FileRef, context_msg: Message, subject: str) -> LoadedTextFile:
        await self.initialize()
        if not self.s.file_read_enabled:
            raise Rejected("disabled")
        name = self._display_name(ref)
        if not _allowed_filename(name):
            raise Rejected("unsupported_extension")
        maximum = self._max_bytes()
        if ref.file_size is not None and ref.file_size > maximum:
            raise Rejected("oversized")
        day = self.store.local_now().date().isoformat()
        limit_per_user = self.s.file_daily_limit_per_user
        async with self.store.lock:
            used = (
                await self.store.one(
                    "SELECT COUNT(*) FROM file_read_attempts WHERE day=? AND subject=?",
                    (day, subject),
                )
            )[0]
        if limit_per_user == 0 or used >= limit_per_user:
            raise Rejected("daily_limit")

        raw, urls = await self._via_transport(ref, context_msg, maximum)
        if raw is None:
            url = next(iter(urls), _public_download_url(ref.url) if ref.url else None)
            if not url:
                raise Rejected("unavailable")
            raw = await self._download_http(url, maximum)
        if not raw:
            raise Rejected("unavailable")
        if len(raw) > maximum:
            raise Rejected("oversized")
        excerpt, encoding, truncated = decode_text_bytes(
            raw, secrets=configured_secrets(self.s), settings=self.s
        )
        now = time.time()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO file_read_attempts VALUES (?,?,?,?)",
                (uuid.uuid4().hex, day, subject, now),
            )
            await self.store.db.execute(
                "DELETE FROM file_read_attempts WHERE created<?", (now - 7 * 86400,)
            )
        return LoadedTextFile(
            name=name,
            content=excerpt,
            char_count=len(excerpt),
            byte_size=len(raw),
            encoding=encoding,
            truncated=truncated,
        )

    async def load_for_turn(
        self,
        message: Message,
        candidates: list[tuple[FileRef, Message, str]],
        *,
        limit: int = 1,
        selected_members: list[Message] | None = None,
    ) -> tuple[list[dict], list[dict]]:
        """Load up to `limit` text files cleanly, returning (attachments, file_sources)."""
        attachments: list[dict] = []
        sources: list[dict] = []
        seen: set[str] = set()
        for ref, owner_msg, origin in candidates:
            if ref.identity in seen:
                continue
            seen.add(ref.identity)
            candidate_check = (
                owner_msg.model_copy(update={"addressed": True})
                if owner_msg.kind == "group"
                else owner_msg
            )
            if not authorize(candidate_check, self.s):
                continue
            name = self._display_name(ref)
            source_entry = {
                "filename": name,
                "sender": owner_msg.sender_nickname.strip()[:40] or "群友",
                "origin": origin if ref.origin in {"", "当前消息"} else f"{origin} / {ref.origin}",
            }
            if len(attachments) >= limit:
                sources.append({**source_entry, "status": "omitted"})
                continue
            try:
                loaded = await self.read_one(
                    ref,
                    message,
                    self.store.subject(owner_msg),
                )
            except Rejected as exc:
                code = str(exc)
                if code == "credential_in_input":
                    raise
                sources.append({**source_entry, "status": code})
                continue
            except (TimeoutError, aiohttp.ClientError, OSError, ValueError) as exc:
                log.warning("event=file_download_failed exception=%s", type(exc).__name__)
                sources.append({**source_entry, "status": "unavailable"})
                continue
            attachments.append(
                {
                    "filename": loaded.name,
                    "encoding": loaded.encoding,
                    "byte_size": loaded.byte_size,
                    "truncated": loaded.truncated,
                    "content": loaded.content,
                }
            )
            sources.append({**source_entry, "status": "available", "byte_size": loaded.byte_size})
            if selected_members is not None:
                selected_members.append(owner_msg)
        return attachments, sources

    async def read_audio_attachment(
        self,
        ref: FileRef,
        context_msg: Message,
        *,
        max_bytes: int = MAX_AUDIO_ATTACHMENT_BYTES,
    ) -> tuple[bytes, str]:
        """Download a bounded audio attachment using strict extension & SSRF checks."""
        if not isinstance(ref, FileRef):
            raise Rejected("invalid_song_audio")
        filename = _sanitize_audio_filename(self._display_name(ref))
        if not filename:
            raise Rejected("invalid_song_audio")
        if isinstance(max_bytes, bool) or max_bytes <= 0:
            raise Rejected("oversized")
        maximum = min(int(max_bytes), MAX_AUDIO_ATTACHMENT_BYTES * 2)
        if ref.file_size is not None and ref.file_size > maximum:
            raise Rejected("oversized")

        raw, urls = await self._via_transport(ref, context_msg, maximum)
        if raw is None:
            url = next(iter(urls), _public_download_url(ref.url) if ref.url else None)
            if not url:
                raise Rejected("unavailable")
            try:
                raw = await self._download_http(url, maximum)
            except Rejected:
                raise
            except (TimeoutError, aiohttp.ClientError, OSError, ValueError):
                raise Rejected("unavailable") from None
        if not raw:
            raise Rejected("unavailable")
        if len(raw) > maximum:
            raise Rejected("oversized")
        return raw, filename
