"""Bounded file reader with strict allowlist, SSRF checks, and optional doc_render remote bridge."""

import asyncio
import base64
import binascii
import ipaddress
import logging
import os
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
MAX_RICH_DOCUMENT_BYTES = 15 * 1024 * 1024
MAX_AUDIO_ATTACHMENT_BYTES = 100 * 1024 * 1024
MAX_EXCERPT_CHARS = 3200
HTTP_TIMEOUT = 12
DOC_RENDER_PLUGIN_ID = "doc_render"
DOC_RENDER_READ_ACTION = "doc_render.read_file"
_DOC_RENDER_FALLBACK_ERRORS = frozenset(
    {
        "plugin_offline",
        "plugin_disabled",
        "plugin_disconnected",
        "plugin_not_dispatched",
        "plugin_rpc_timeout",
        "action_not_found",
        "plugin_internal_error",
        "plugin_blob_backpressure",
    }
)
ALLOWED_AUDIO_EXTENSIONS = frozenset({".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus"})
ALLOWED_RICH_DOCUMENT_EXTENSIONS = frozenset({".pdf", ".docx", ".xlsx", ".pptx"})
_RICH_MIME_TYPES: dict[str, str] = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}
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


def _clean_basename(name: str) -> str:
    clean = PurePosixPath(str(name or "").replace("\\", "/")).name.strip()
    if not clean or clean in {".", ".."} or any(ord(ch) < 32 or ord(ch) == 127 for ch in clean):
        return ""
    return clean[:120]


def _allowed_filename(name: str) -> bool:
    clean = _clean_basename(name).lower()
    if not clean:
        return False
    if clean == ".env" or (clean.startswith(".env.") and clean != ".env.example"):
        return False
    if clean in ALLOWED_SPECIAL_FILENAMES:
        return True
    suffix = PurePosixPath(clean).suffix.lower()
    return suffix in ALLOWED_TEXT_EXTENSIONS


def _is_rich_document(name: str) -> bool:
    clean = _clean_basename(name).lower()
    if not clean:
        return False
    suffix = PurePosixPath(clean).suffix.lower()
    return suffix in ALLOWED_RICH_DOCUMENT_EXTENSIONS


def _guess_doc_mime(name: str) -> str:
    suffix = PurePosixPath(_clean_basename(name).lower()).suffix
    return _RICH_MIME_TYPES.get(suffix, "text/plain")


def _env_doc_render_remote_only() -> bool:
    return os.getenv("DOC_RENDER_REMOTE_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}


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
    doc_format: str = "text"
    pages: int | None = None


def _screen_and_excerpt_text(text: str, *, secrets: tuple[str, ...], settings) -> tuple[str, bool]:
    cleaned = clean_text(text.replace("\r\n", "\n").replace("\r", "\n"))
    if any(secret and len(secret) >= 8 and secret in cleaned for secret in secrets):
        raise Rejected("credential_in_input")
    check_memory_text(cleaned, settings)
    total = len(cleaned)
    if total > MAX_EXCERPT_CHARS:
        head = cleaned[:1000]
        tail = cleaned[-2100:]
        excerpt = f"{head}\n... [中间省略 {total - 3100} 字] ...\n{tail}"
        return excerpt, True
    return cleaned, False


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
    excerpt, truncated = _screen_and_excerpt_text(text, secrets=secrets, settings=settings)
    return excerpt, encoding, truncated


class FileReader:
    def __init__(
        self,
        settings,
        store,
        transport=None,
        *,
        plugin_bus=None,
        remote_only: bool | None = None,
    ):
        self.s, self.store, self.transport = settings, store, transport
        self.plugin_bus = plugin_bus
        self.remote_only = remote_only
        self.slots = asyncio.Semaphore(2)
        self.init_lock = asyncio.Lock()
        self.initialized = False

    def _is_remote_only(self) -> bool:
        if self.remote_only is not None:
            return bool(self.remote_only)
        cfg_val = getattr(self.s, "doc_render_remote_only", None)
        if cfg_val is not None:
            return bool(cfg_val)
        return _env_doc_render_remote_only()

    def _doc_render_online(self) -> bool:
        bus = self.plugin_bus
        if bus is None:
            return False
        has_tool = getattr(bus, "has_tool", None)
        if callable(has_tool):
            return bool(has_tool(DOC_RENDER_PLUGIN_ID, DOC_RENDER_READ_ACTION))
        is_online = getattr(bus, "is_plugin_online", None)
        if callable(is_online):
            return bool(is_online(DOC_RENDER_PLUGIN_ID))
        return False

    async def _parse_via_remote(
        self,
        raw: bytes,
        name: str,
        *,
        secrets: tuple[str, ...],
    ) -> tuple[str, str, bool, str, int | None]:
        basename = _clean_basename(name)
        if not basename:
            raise Rejected("invalid_document")
        mime_type = _guess_doc_mime(basename)
        res = await self.plugin_bus.invoke_tool(
            DOC_RENDER_PLUGIN_ID,
            DOC_RENDER_READ_ACTION,
            {
                "filename": basename,
                "mime_type": mime_type,
                "max_chars": MAX_EXCERPT_CHARS,
            },
            blob=raw,
            blob_mime_type=mime_type,
            blob_filename=basename,
            max_request_blob_bytes=MAX_RICH_DOCUMENT_BYTES,
            max_response_blob_bytes=0,
            timeout=30.0,
        )
        data = res.data if isinstance(getattr(res, "data", None), dict) else {}
        raw_content = data.get("content")
        if not isinstance(raw_content, str):
            raise Rejected("invalid_document")
        excerpt, post_truncated = _screen_and_excerpt_text(
            raw_content, secrets=secrets, settings=self.s
        )
        encoding = str(data.get("encoding") or "utf-8")[:32]
        doc_format = str(data.get("format") or "text")[:16]
        truncated = bool(data.get("truncated")) or post_truncated
        raw_pages = data.get("pages")
        pages = (
            raw_pages
            if isinstance(raw_pages, int) and not isinstance(raw_pages, bool) and raw_pages >= 0
            else None
        )
        return excerpt, encoding, truncated, doc_format, pages

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
        is_text = _allowed_filename(name)
        is_rich = _is_rich_document(name)
        if not is_text and not is_rich:
            raise Rejected("unsupported_extension")

        remote_only = self._is_remote_only()
        remote_online = self._doc_render_online()
        if is_rich:
            if not remote_online:
                if remote_only or self.plugin_bus is not None:
                    raise Rejected("doc_render_remote_unavailable")
                raise Rejected("unsupported_extension")
            maximum = MAX_RICH_DOCUMENT_BYTES
        else:
            if remote_only and not remote_online:
                raise Rejected("doc_render_remote_unavailable")
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

        if ref.file_id.startswith("plugin_media:"):
            if self.plugin_bus is None or not hasattr(self.plugin_bus, "fetch_plugin_media"):
                raise Rejected("unavailable")
            try:
                raw = await self.plugin_bus.fetch_plugin_media(
                    ref.file_id, kind="file", max_bytes=maximum
                )
            except Rejected as exc:
                raise Rejected("oversized" if "too_large" in str(exc) else "unavailable") from None
        else:
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

        secrets_tuple = configured_secrets(self.s)
        if is_text:
            for secret in secrets_tuple:
                if secret and len(secret) >= 8 and secret.encode("utf-8") in raw:
                    raise Rejected("credential_in_input")

        doc_format = "text"
        pages: int | None = None
        if remote_online:
            try:
                excerpt, encoding, truncated, doc_format, pages = await self._parse_via_remote(
                    raw, name, secrets=secrets_tuple
                )
            except Rejected as exc:
                if str(exc) in _DOC_RENDER_FALLBACK_ERRORS:
                    if remote_only or is_rich:
                        raise Rejected("doc_render_remote_unavailable") from None
                    log.warning("event=doc_render_file_fallback reason=%s", str(exc))
                    excerpt, encoding, truncated = decode_text_bytes(
                        raw, secrets=secrets_tuple, settings=self.s
                    )
                else:
                    raise
        else:
            excerpt, encoding, truncated = decode_text_bytes(
                raw, secrets=secrets_tuple, settings=self.s
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
            doc_format=doc_format,
            pages=pages,
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
            attachment_entry: dict[str, object] = {
                "filename": loaded.name,
                "encoding": loaded.encoding,
                "byte_size": loaded.byte_size,
                "truncated": loaded.truncated,
                "content": loaded.content,
            }
            if loaded.doc_format != "text":
                attachment_entry["format"] = loaded.doc_format
            if loaded.pages is not None:
                attachment_entry["pages"] = loaded.pages
            attachments.append(attachment_entry)
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

        if ref.file_id.startswith("plugin_media:"):
            if self.plugin_bus is None or not hasattr(self.plugin_bus, "fetch_plugin_media"):
                raise Rejected("unavailable")
            try:
                raw = await self.plugin_bus.fetch_plugin_media(
                    ref.file_id, kind="file", max_bytes=maximum
                )
            except Rejected as exc:
                raise Rejected("oversized" if "too_large" in str(exc) else "unavailable") from None
        else:
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
