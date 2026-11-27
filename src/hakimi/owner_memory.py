"""Zone 2 physically isolated Owner semantic memory with bi-temporal history and 1-hop entity graph.

Stored strictly at ``settings.data_dir / "owner_memory.sqlite3"`` (never inside
``Antigravity_Memory``). Every payload, entity label, and vector blob is sealed at rest
with Fernet via ``Vault``; no plaintext text or plaintext vectors are ever written to disk.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import logging
import math
import os
import re
import sqlite3
import stat
import struct
import sys
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .config import Settings
from .modules.base import sanitize_sensor_text
from .security import Rejected, Vault, clean_text, configured_secrets, redact_secrets, safe_output

log = logging.getLogger("hakimi")

SEMANTIC_MAGIC = b"E1"
LEGACY_HASH_DIM = 192
LEGACY_HASH_BYTES = LEGACY_HASH_DIM * 4

MAX_SOURCE_CHARS = 64
MAX_CATEGORY_CHARS = 32
MAX_TOPIC_CHARS = 80
MAX_TEXT_CHARS = 600
MAX_TAGS = 8
MAX_TAG_CHARS = 32
MAX_ENTITIES = 16
MAX_ENTITY_CHARS = 48
MAX_LINKS = 16
MAX_RELATION_CHARS = 32
MAX_CACHE_BYTES = 16 * 1024 * 1024
MAX_RECALL_SCAN = 2048

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS owner_memory_entries (
        id TEXT PRIMARY KEY,
        dedupe_key TEXT UNIQUE,
        source TEXT NOT NULL,
        category TEXT NOT NULL DEFAULT 'event',
        topic_digest TEXT NOT NULL,
        payload_blob BLOB NOT NULL,
        vector_blob BLOB NOT NULL,
        vector_kind TEXT NOT NULL DEFAULT 'legacy_hash',
        heat INTEGER NOT NULL DEFAULT 1,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        valid_from REAL NOT NULL,
        valid_until REAL,
        superseded_by TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_active
        ON owner_memory_entries(superseded_by, valid_from, valid_until, updated_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_topic
        ON owner_memory_entries(topic_digest, superseded_by)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_vkind
        ON owner_memory_entries(vector_kind, superseded_by, updated_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS owner_memory_entities (
        entry_id TEXT NOT NULL,
        entity_norm TEXT NOT NULL,
        entity_label BLOB NOT NULL,
        PRIMARY KEY(entry_id, entity_norm)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_entities_norm
        ON owner_memory_entities(entity_norm, entry_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS owner_memory_links (
        src_id TEXT NOT NULL,
        dst_id TEXT NOT NULL,
        relation TEXT NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0,
        created_at REAL NOT NULL,
        PRIMARY KEY(src_id, dst_id, relation)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_links_src
        ON owner_memory_links(src_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_owner_memory_links_dst
        ON owner_memory_links(dst_id)
    """,
)

_MIGRATION_COLUMNS: dict[str, str] = {
    "dedupe_key": "TEXT",
    "category": "TEXT NOT NULL DEFAULT 'event'",
    "topic_digest": "TEXT NOT NULL DEFAULT ''",
    "vector_kind": "TEXT NOT NULL DEFAULT 'legacy_hash'",
    "heat": "INTEGER NOT NULL DEFAULT 1",
    "valid_from": "REAL NOT NULL DEFAULT 0",
    "valid_until": "REAL",
    "superseded_by": "TEXT",
}


# ---------------------------------------------------------------------------
# Shared vector codec (uses .embeddings when available, exact fallback otherwise)
# ---------------------------------------------------------------------------


def _fallback_pack_semantic_vector(values: Sequence[float]) -> bytes:
    if not values:
        raise ValueError("empty_embedding_vector")
    dim = len(values)
    if dim > 65535:
        raise ValueError("embedding_dim_too_large")
    floats: list[float] = []
    sq_sum = 0.0
    for raw in values:
        if isinstance(raw, bool):
            raise ValueError("invalid_embedding_value")
        val = float(raw)
        if not math.isfinite(val):
            raise ValueError("non_finite_embedding_value")
        floats.append(val)
        sq_sum += val * val
    norm = math.sqrt(sq_sum)
    normed = [v / norm for v in floats] if norm > 0.0 else floats
    return SEMANTIC_MAGIC + struct.pack("<H", dim) + struct.pack(f"<{dim}e", *normed)


def _fallback_is_semantic_blob(blob: bytes | bytearray | memoryview | None) -> bool:
    if not isinstance(blob, bytes | bytearray | memoryview):
        return False
    raw = bytes(blob)
    if len(raw) < 6 or raw[:2] != SEMANTIC_MAGIC:
        return False
    dim = struct.unpack_from("<H", raw, 2)[0]
    return dim > 0 and len(raw) == 4 + dim * 2


def _fallback_unpack_semantic_vector(
    blob: bytes | bytearray | memoryview | None,
) -> tuple[str, tuple[float, ...]] | None:
    if not isinstance(blob, bytes | bytearray | memoryview):
        return None
    raw = bytes(blob)
    if len(raw) >= 6 and raw[:2] == SEMANTIC_MAGIC:
        dim = struct.unpack_from("<H", raw, 2)[0]
        if dim <= 0 or len(raw) != 4 + dim * 2:
            return None
        vals = struct.unpack_from(f"<{dim}e", raw, 4)
        if not all(math.isfinite(v) for v in vals):
            return None
        return ("semantic", vals)
    if len(raw) == LEGACY_HASH_BYTES:
        vals = struct.unpack("<192f", raw)
        if not all(math.isfinite(v) for v in vals):
            return None
        return ("legacy_hash", vals)
    return None


def _fallback_cosine_similarity_blobs(a: bytes | None, b: bytes | None) -> float | None:
    ua = _fallback_unpack_semantic_vector(a)
    ub = _fallback_unpack_semantic_vector(b)
    if ua is None or ub is None:
        return None
    kind_a, va = ua
    kind_b, vb = ub
    if kind_a != kind_b or len(va) != len(vb):
        return None
    dot = float(sum(x * y for x, y in zip(va, vb, strict=True)))
    return max(-1.0, min(1.0, dot))


try:
    from .embeddings import (
        cosine_similarity_blobs as cosine_similarity_blobs,
    )
    from .embeddings import (
        is_semantic_blob as is_semantic_blob,
    )
    from .embeddings import (
        pack_semantic_vector as pack_semantic_vector,
    )
    from .embeddings import (
        unpack_semantic_vector as unpack_semantic_vector,
    )
except ImportError:
    pack_semantic_vector = _fallback_pack_semantic_vector
    unpack_semantic_vector = _fallback_unpack_semantic_vector
    cosine_similarity_blobs = _fallback_cosine_similarity_blobs
    is_semantic_blob = _fallback_is_semantic_blob


def _legacy_terms(text: str) -> set[str]:
    words = re.findall(r"[a-z0-9_]{2,}|[\u3400-\u9fff]", text.lower())
    han = re.findall(r"[\u3400-\u9fff]{2,}", text)
    return set(words + [s[i : i + 2] for s in han for i in range(len(s) - 1)])


def _legacy_vector(text: str) -> bytes:
    values = [0.0] * LEGACY_HASH_DIM
    for word in _legacy_terms(text):
        digest = hashlib.blake2s(word.encode(), digest_size=4).digest()
        values[int.from_bytes(digest[:2], "little") % LEGACY_HASH_DIM] += 1 if digest[2] & 1 else -1
    norm = math.sqrt(sum(x * x for x in values)) or 1.0
    return struct.pack("<192f", *(x / norm for x in values))


def _supports_kwarg(fn: Any, name: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        or (
            p.name == name
            and p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        )
        for p in sig.parameters.values()
    )


async def embed_one_safe(
    embedder: Any,
    text: str,
    *,
    task_type: str = "RETRIEVAL_DOCUMENT",
    budget_lane: str = "memory",
    subject: str = "embeddings",
    purpose: str = "memory_embed",
) -> bytes | None:
    """Invoke an optional EmbeddingClient safely, returning a valid semantic blob or None."""
    if embedder is None or not str(text or "").strip():
        return None
    fn = getattr(embedder, "embed_one", None)
    if callable(fn):
        kwargs: dict[str, Any] = {}
        for k, v in (
            ("task_type", task_type),
            ("budget_lane", budget_lane),
            ("subject", subject),
            ("purpose", purpose),
        ):
            if _supports_kwarg(fn, k):
                kwargs[k] = v
        try:
            res = fn(text, **kwargs)
            blob = await res if inspect.isawaitable(res) else res
        except Exception as exc:
            log.debug("event=embed_one_failed purpose=%s exc=%s", purpose, type(exc).__name__)
            return None
        return bytes(blob) if is_semantic_blob(blob) else None
    batch = await embed_texts_safe(
        embedder,
        [text],
        task_type=task_type,
        budget_lane=budget_lane,
        subject=subject,
        purpose=purpose,
    )
    return batch[0] if batch else None


async def embed_texts_safe(
    embedder: Any,
    texts: Sequence[str],
    *,
    task_type: str = "RETRIEVAL_DOCUMENT",
    budget_lane: str = "memory",
    subject: str = "embeddings",
    purpose: str = "memory_embed",
) -> list[bytes | None]:
    """Batch-embed texts via an optional EmbeddingClient, returning None entries on fallback."""
    items = list(texts)
    if not items or embedder is None:
        return [None] * len(items)
    fn = getattr(embedder, "embed_texts", None)
    if callable(fn):
        kwargs: dict[str, Any] = {}
        for k, v in (
            ("task_type", task_type),
            ("budget_lane", budget_lane),
            ("subject", subject),
            ("purpose", purpose),
        ):
            if _supports_kwarg(fn, k):
                kwargs[k] = v
        try:
            res = fn(items, **kwargs)
            blobs = await res if inspect.isawaitable(res) else res
        except Exception as exc:
            log.debug("event=embed_texts_failed purpose=%s exc=%s", purpose, type(exc).__name__)
            return [None] * len(items)
        if not isinstance(blobs, Sequence) or isinstance(blobs, str | bytes):
            return [None] * len(items)
        out: list[bytes | None] = []
        for idx in range(len(items)):
            raw = blobs[idx] if idx < len(blobs) else None
            out.append(bytes(raw) if is_semantic_blob(raw) else None)
        return out
    out_single: list[bytes | None] = []
    for item in items:
        out_single.append(
            await embed_one_safe(
                embedder,
                item,
                task_type=task_type,
                budget_lane=budget_lane,
                subject=subject,
                purpose=purpose,
            )
        )
    return out_single


# ---------------------------------------------------------------------------
# Bounded LRU Cache for Decoded Entries
# ---------------------------------------------------------------------------


class _EntryLRUCache:
    """Bounded byte-budget LRU cache keyed by (cache_key, fingerprint)."""

    def __init__(self, max_bytes: int = MAX_CACHE_BYTES) -> None:
        self._entries: OrderedDict[str, tuple[tuple[int, ...], Any, int]] = OrderedDict()
        self._bytes = 0
        self._limit = max(65536, int(max_bytes))

    @staticmethod
    def _estimate_size(obj: Any) -> int:
        if isinstance(obj, dict):
            return sys.getsizeof(obj) + sum(
                _EntryLRUCache._estimate_size(k) + _EntryLRUCache._estimate_size(v)
                for k, v in obj.items()
            )
        if isinstance(obj, list | tuple | set | frozenset):
            return sys.getsizeof(obj) + sum(_EntryLRUCache._estimate_size(x) for x in obj)
        return sys.getsizeof(obj)

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0

    def discard(self, key: str) -> None:
        item = self._entries.pop(key, None)
        if item is not None:
            self._bytes -= item[2]

    def get(self, key: str, fingerprint: tuple[int, ...]) -> Any | None:
        item = self._entries.get(key)
        if item is not None and item[0] == fingerprint:
            self._entries.move_to_end(key)
            return item[1]
        return None

    def put(self, key: str, fingerprint: tuple[int, ...], value: Any) -> None:
        self.discard(key)
        size = self._estimate_size(value) + sys.getsizeof(key) + 256
        if size > self._limit:
            return
        while self._entries and self._bytes + size > self._limit:
            self.discard(next(iter(self._entries)))
        self._entries[key] = (fingerprint, value, size)
        self._bytes += size


# ---------------------------------------------------------------------------
# Zone 2 OwnerMemoryStore
# ---------------------------------------------------------------------------


def _resolve_vault(settings: Settings, vault: Any) -> Vault:
    if isinstance(vault, Vault):
        return vault
    nested = getattr(vault, "vault", None)
    if isinstance(nested, Vault):
        return nested
    return Vault(settings.encryption_key.get_secret_value())


class OwnerMemoryStore:
    """Physically isolated encrypted Zone 2 Owner memory with bi-temporal & 1-hop graph recall."""

    def __init__(
        self,
        settings: Settings,
        vault: Vault | Any | None = None,
        *,
        db_path: Path | None = None,
        embedder: Any = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.settings = settings
        self.vault = _resolve_vault(settings, vault)
        self.db_path = (
            Path(db_path) if db_path is not None else (settings.data_dir / "owner_memory.sqlite3")
        )
        self.embedder = embedder
        self.clock = clock
        self._thread_lock = threading.Lock()
        self._initialized = False
        self._cache = _EntryLRUCache()

    def _check_not_symlink(self) -> None:
        targets = (self.db_path, self.db_path.parent, Path(self.s.data_dir))
        for candidate in targets:
            if candidate.is_symlink():
                raise Rejected("invalid_owner_memory_path")
        # Never allow owner_memory DB to live inside read-only Antigravity_Memory.
        cloud_dir = getattr(self.s, "antigravity_memory_dir", None)
        if cloud_dir:
            try:
                cloud_resolved = Path(cloud_dir).resolve()
                db_resolved = self.db_path.resolve()
                if db_resolved == cloud_resolved or cloud_resolved in db_resolved.parents:
                    raise Rejected("invalid_owner_memory_path")
            except OSError:
                pass

    def _db_fingerprint(self) -> tuple[int, ...] | None:
        try:
            st = os.lstat(self.db_path)
        except OSError:
            return None
        if stat.S_ISLNK(st.st_mode):
            raise Rejected("invalid_owner_memory_path")
        if not stat.S_ISREG(st.st_mode):
            return None
        wal_ino = -1
        wal_size = -1
        wal_mtime = -1
        try:
            wst = os.lstat(f"{self.db_path}-wal")
            wal_ino = wst.st_ino
            wal_size = wst.st_size
            wal_mtime = wst.st_mtime_ns
        except OSError:
            pass
        return (
            st.st_dev,
            st.st_ino,
            st.st_size,
            st.st_mtime_ns,
            st.st_ctime_ns,
            wal_ino,
            wal_size,
            wal_mtime,
        )

    def _connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        self._check_not_symlink()
        if readonly:
            uri = f"{self.db_path.absolute().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=5000")
            return conn
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA secure_delete=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _initialize_sync(self) -> None:
        with self._thread_lock:
            self._check_not_symlink()
            if self._initialized and self.db_path.exists():
                return
            self.db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                self.db_path.parent.chmod(0o700)
            except OSError:
                pass
            fd = os.open(self.db_path, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self.db_path.chmod(0o600)
            conn = self._connect(readonly=False)
            try:
                conn.execute("BEGIN IMMEDIATE")
                for stmt in SCHEMA_STATEMENTS:
                    conn.execute(stmt)
                cols = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(owner_memory_entries)").fetchall()
                }
                for col_name, col_def in _MIGRATION_COLUMNS.items():
                    if col_name not in cols:
                        conn.execute(
                            f"ALTER TABLE owner_memory_entries ADD COLUMN {col_name} {col_def}"
                        )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            self._initialized = True

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    async def close(self) -> None:
        with self._thread_lock:
            self._cache.clear()
            self._initialized = False

    def _scrub_field(
        self,
        raw: Any,
        *,
        secrets: list[str],
        char_limit: int,
        max_tokens: int,
        reject_on_secret: bool = True,
    ) -> str:
        text = clean_text(str(raw or ""))
        if not text:
            return ""
        if reject_on_secret:
            scrubbed = safe_output(text, secrets, limit=char_limit)
        else:
            scrubbed = redact_secrets(text, self.s, neutralize_role_tags=True)[:char_limit]
        redacted = redact_secrets(scrubbed, self.s, neutralize_role_tags=True)
        return sanitize_sensor_text(redacted, self.s, max_tokens=max_tokens)[:char_limit].strip()

    def _prepare_write_fields(
        self,
        source: str,
        topic: str,
        text: str,
        *,
        category: str,
        tags: Sequence[str],
        entities: Sequence[str],
        links: Sequence[Any],
        valid_from: float | None,
        valid_until: float | None,
        supersedes_topic: str | None,
        supersede_id: str | None,
        dedupe_key: str | None,
        now: float | None,
    ) -> dict[str, Any] | None:
        ts = float(self.clock() if now is None else now)
        if not math.isfinite(ts):
            return None
        secrets = configured_secrets(self.s)
        try:
            clean_source = self._scrub_field(
                source, secrets=secrets, char_limit=MAX_SOURCE_CHARS, max_tokens=24
            )
            clean_topic = self._scrub_field(
                topic, secrets=secrets, char_limit=MAX_TOPIC_CHARS, max_tokens=30
            )
            clean_body = self._scrub_field(
                text, secrets=secrets, char_limit=MAX_TEXT_CHARS, max_tokens=220
            )
        except Rejected:
            return None
        if not clean_source or not clean_topic or not clean_body:
            return None

        clean_category = (
            self._scrub_field(
                category or "event",
                secrets=secrets,
                char_limit=MAX_CATEGORY_CHARS,
                max_tokens=12,
                reject_on_secret=False,
            )
            or "event"
        )

        clean_tags: list[str] = []
        if not isinstance(tags, str | bytes) and isinstance(tags, Sequence):
            for raw_tag in tags:
                if not isinstance(raw_tag, str):
                    continue
                try:
                    t = self._scrub_field(
                        raw_tag, secrets=secrets, char_limit=MAX_TAG_CHARS, max_tokens=12
                    )
                except Rejected:
                    continue
                if t and t not in clean_tags:
                    clean_tags.append(t)
                    if len(clean_tags) >= MAX_TAGS:
                        break

        clean_entities: list[str] = []
        seen_ent_norms: set[str] = set()
        if not isinstance(entities, str | bytes) and isinstance(entities, Sequence):
            for raw_ent in entities:
                if not isinstance(raw_ent, str):
                    continue
                try:
                    ent = self._scrub_field(
                        raw_ent, secrets=secrets, char_limit=MAX_ENTITY_CHARS, max_tokens=16
                    )
                except Rejected:
                    continue
                ent_key = ent.lower().strip()
                if ent and ent_key and ent_key not in seen_ent_norms:
                    seen_ent_norms.add(ent_key)
                    clean_entities.append(ent)
                    if len(clean_entities) >= MAX_ENTITIES:
                        break

        clean_links: list[tuple[str, str, float]] = []
        if not isinstance(links, str | bytes) and isinstance(links, Sequence):
            for item in links:
                dst_id = ""
                relation = "related"
                weight = 1.0
                if isinstance(item, dict):
                    dst_id = str(item.get("dst_id") or item.get("target_id") or "").strip()
                    relation = str(item.get("relation") or "related").strip()
                    raw_w = item.get("weight", 1.0)
                    weight = (
                        float(raw_w)
                        if isinstance(raw_w, int | float) and not isinstance(raw_w, bool)
                        else 1.0
                    )
                elif isinstance(item, tuple | list) and len(item) >= 2:
                    dst_id = str(item[0] or "").strip()
                    relation = str(item[1] or "related").strip()
                    if (
                        len(item) >= 3
                        and isinstance(item[2], int | float)
                        and not isinstance(item[2], bool)
                    ):
                        weight = float(item[2])
                if not dst_id or len(dst_id) > 64:
                    continue
                rel_clean = (
                    self._scrub_field(
                        relation,
                        secrets=secrets,
                        char_limit=MAX_RELATION_CHARS,
                        max_tokens=10,
                        reject_on_secret=False,
                    )
                    or "related"
                )
                if not math.isfinite(weight):
                    weight = 1.0
                weight = max(0.05, min(5.0, weight))
                clean_links.append((dst_id, rel_clean, weight))
                if len(clean_links) >= MAX_LINKS:
                    break

        v_from = ts if valid_from is None else float(valid_from)
        if not math.isfinite(v_from):
            v_from = ts
        v_until: float | None = None
        if valid_until is not None:
            cand = float(valid_until)
            if not math.isfinite(cand):
                return None
            v_until = cand

        clean_sup_topic: str | None = None
        if isinstance(supersedes_topic, str) and supersedes_topic.strip():
            try:
                clean_sup_topic = self._scrub_field(
                    supersedes_topic,
                    secrets=secrets,
                    char_limit=MAX_TOPIC_CHARS,
                    max_tokens=30,
                )
            except Rejected:
                clean_sup_topic = None

        clean_sup_id = (
            supersede_id.strip()[:64]
            if isinstance(supersede_id, str) and supersede_id.strip()
            else None
        )

        clean_dedupe: str | None = None
        if isinstance(dedupe_key, str) and dedupe_key.strip():
            raw_dk = clean_text(dedupe_key).strip()[:128]
            if raw_dk:
                clean_dedupe = self.vault.digest("owner-memory-dedupe", raw_dk)

        return {
            "ts": ts,
            "source": clean_source,
            "category": clean_category,
            "topic": clean_topic,
            "text": clean_body,
            "tags": clean_tags,
            "entities": clean_entities,
            "links": clean_links,
            "valid_from": v_from,
            "valid_until": v_until,
            "supersedes_topic": clean_sup_topic,
            "supersede_id": clean_sup_id,
            "dedupe_key": clean_dedupe,
        }

    def _seal_vector(self, entry_id: str, raw_vec: bytes) -> bytes:
        return self.vault.seal(
            f"owner-memory:v:{entry_id}",
            base64.b64encode(raw_vec).decode("ascii"),
        )

    def _open_vector(self, entry_id: str, blob: bytes) -> bytes | None:
        try:
            encoded = self.vault.open(f"owner-memory:v:{entry_id}", blob)
            return base64.b64decode(encoded)
        except Exception:
            return None

    def _write_record_sync(self, prepared: dict[str, Any], embedding: bytes | None) -> str:
        self._initialize_sync()
        ts = prepared["ts"]
        doc_text = " ".join(
            part
            for part in (
                prepared["topic"],
                prepared["text"],
                " ".join(prepared["tags"]),
                " ".join(prepared["entities"]),
            )
            if part
        )
        if is_semantic_blob(embedding):
            raw_vec = bytes(embedding)  # type: ignore[arg-type]
            vector_kind = "semantic"
        else:
            raw_vec = _legacy_vector(doc_text)
            vector_kind = "legacy_hash"

        topic_digest = self.vault.digest("owner-memory-topic", prepared["topic"].lower())
        sup_topic_digest = (
            self.vault.digest("owner-memory-topic", prepared["supersedes_topic"].lower())
            if prepared["supersedes_topic"]
            else None
        )
        dedupe_key = prepared["dedupe_key"]
        superseding = bool(sup_topic_digest or prepared["supersede_id"])

        with self._thread_lock:
            conn = self._connect(readonly=False)
            try:
                conn.execute("BEGIN IMMEDIATE")
                existing_id: str | None = None
                existing_heat = 1
                if dedupe_key is not None:
                    row = conn.execute(
                        "SELECT id, heat FROM owner_memory_entries WHERE dedupe_key=?",
                        (dedupe_key,),
                    ).fetchone()
                    if row is not None:
                        if superseding:
                            # Release dedupe_key on historical row so new truth preserves history.
                            conn.execute(
                                "UPDATE owner_memory_entries SET dedupe_key=NULL WHERE id=?",
                                (row["id"],),
                            )
                        else:
                            existing_id = str(row["id"])
                            existing_heat = min(100, int(row["heat"]) + 1)

                entry_id = existing_id or f"om_{uuid.uuid4().hex[:20]}"
                payload_obj = {
                    "source": prepared["source"],
                    "category": prepared["category"],
                    "topic": prepared["topic"],
                    "text": prepared["text"],
                    "tags": prepared["tags"],
                    "entities": prepared["entities"],
                }
                payload_blob = self.vault.seal(f"owner-memory:e:{entry_id}", payload_obj)
                vector_blob = self._seal_vector(entry_id, raw_vec)

                if existing_id is not None:
                    conn.execute(
                        "UPDATE owner_memory_entries SET "
                        "source=?, category=?, topic_digest=?, payload_blob=?, vector_blob=?, "
                        "vector_kind=?, heat=?, updated_at=?, valid_from=?, valid_until=?, "
                        "superseded_by=NULL WHERE id=?",
                        (
                            prepared["source"],
                            prepared["category"],
                            topic_digest,
                            payload_blob,
                            vector_blob,
                            vector_kind,
                            existing_heat,
                            ts,
                            prepared["valid_from"],
                            prepared["valid_until"],
                            entry_id,
                        ),
                    )
                    conn.execute("DELETE FROM owner_memory_entities WHERE entry_id=?", (entry_id,))
                else:
                    conn.execute(
                        "INSERT INTO owner_memory_entries("
                        "id, dedupe_key, source, category, topic_digest, payload_blob, "
                        "vector_blob, vector_kind, heat, created_at, updated_at, valid_from, "
                        "valid_until, superseded_by"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                        (
                            entry_id,
                            dedupe_key,
                            prepared["source"],
                            prepared["category"],
                            topic_digest,
                            payload_blob,
                            vector_blob,
                            vector_kind,
                            1,
                            ts,
                            ts,
                            prepared["valid_from"],
                            prepared["valid_until"],
                        ),
                    )

                # Bi-temporal superseding: preserve old rows with superseded_by + valid_until.
                superseded_ids: list[str] = []
                if sup_topic_digest is not None:
                    old_rows = conn.execute(
                        "SELECT id FROM owner_memory_entries "
                        "WHERE topic_digest=? AND id!=? AND superseded_by IS NULL",
                        (sup_topic_digest, entry_id),
                    ).fetchall()
                    for old_row in old_rows:
                        old_id = str(old_row["id"])
                        superseded_ids.append(old_id)
                        conn.execute(
                            "UPDATE owner_memory_entries SET superseded_by=?, "
                            "valid_until=CASE WHEN valid_until IS NULL OR valid_until>? "
                            "THEN ? ELSE valid_until END, updated_at=? WHERE id=?",
                            (entry_id, ts, ts, ts, old_id),
                        )
                if prepared["supersede_id"] and prepared["supersede_id"] != entry_id:
                    cur = conn.execute(
                        "UPDATE owner_memory_entries SET superseded_by=?, "
                        "valid_until=CASE WHEN valid_until IS NULL OR valid_until>? "
                        "THEN ? ELSE valid_until END, updated_at=? "
                        "WHERE id=? AND superseded_by IS NULL",
                        (entry_id, ts, ts, ts, prepared["supersede_id"]),
                    )
                    if cur.rowcount:
                        superseded_ids.append(prepared["supersede_id"])

                for ent_label in prepared["entities"]:
                    ent_norm = self.vault.digest("owner-memory-entity", ent_label.lower().strip())
                    ent_sealed = self.vault.seal(
                        f"owner-memory:ent:{entry_id}:{ent_norm}", ent_label
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO owner_memory_entities("
                        "entry_id, entity_norm, entity_label) VALUES (?,?,?)",
                        (entry_id, ent_norm, ent_sealed),
                    )

                for dst_id, relation, weight in prepared["links"]:
                    if dst_id == entry_id:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO owner_memory_links("
                        "src_id, dst_id, relation, weight, created_at) VALUES (?,?,?,?,?)",
                        (entry_id, dst_id, relation, weight, ts),
                    )
                for old_id in superseded_ids:
                    conn.execute(
                        "INSERT OR REPLACE INTO owner_memory_links("
                        "src_id, dst_id, relation, weight, created_at) VALUES (?,?,?,?,?)",
                        (entry_id, old_id, "supersedes", 1.0, ts),
                    )

                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            self._cache.clear()
        return entry_id

    def record_sync(
        self,
        source: str,
        topic: str,
        text: str,
        *,
        category: str = "event",
        tags: Sequence[str] = (),
        entities: Sequence[str] = (),
        links: Sequence[Any] = (),
        valid_from: float | None = None,
        valid_until: float | None = None,
        supersedes_topic: str | None = None,
        supersede_id: str | None = None,
        embedding: bytes | None = None,
        dedupe_key: str | None = None,
        now: float | None = None,
    ) -> str | None:
        """Synchronous write helper used by sync recap callers; always Vault-seals at rest."""
        prepared = self._prepare_write_fields(
            source,
            topic,
            text,
            category=category,
            tags=tags,
            entities=entities,
            links=links,
            valid_from=valid_from,
            valid_until=valid_until,
            supersedes_topic=supersedes_topic,
            supersede_id=supersede_id,
            dedupe_key=dedupe_key,
            now=now,
        )
        if prepared is None:
            return None
        return self._write_record_sync(prepared, embedding)

    async def record(
        self,
        source: str,
        topic: str,
        text: str,
        *,
        category: str = "event",
        tags: Sequence[str] = (),
        entities: Sequence[str] = (),
        links: Sequence[Any] = (),
        valid_from: float | None = None,
        valid_until: float | None = None,
        supersedes_topic: str | None = None,
        supersede_id: str | None = None,
        embedding: bytes | None = None,
        dedupe_key: str | None = None,
        now: float | None = None,
    ) -> str | None:
        """Record an encrypted Owner memory entry, optionally superseding older topic/entry."""
        prepared = self._prepare_write_fields(
            source,
            topic,
            text,
            category=category,
            tags=tags,
            entities=entities,
            links=links,
            valid_from=valid_from,
            valid_until=valid_until,
            supersedes_topic=supersedes_topic,
            supersede_id=supersede_id,
            dedupe_key=dedupe_key,
            now=now,
        )
        if prepared is None:
            return None

        resolved_embedding = embedding if is_semantic_blob(embedding) else None
        if resolved_embedding is None and self.embedder is not None:
            doc_text = " ".join(
                part
                for part in (
                    prepared["topic"],
                    prepared["text"],
                    " ".join(prepared["tags"]),
                    " ".join(prepared["entities"]),
                )
                if part
            )
            resolved_embedding = await embed_one_safe(
                self.embedder,
                doc_text,
                task_type="RETRIEVAL_DOCUMENT",
                budget_lane="memory",
                subject="owner_memory",
                purpose="owner_memory_record",
            )
        return await asyncio.to_thread(self._write_record_sync, prepared, resolved_embedding)

    def _load_snapshot_sync(self) -> dict[str, Any] | None:
        with self._thread_lock:
            self._check_not_symlink()
            fp = self._db_fingerprint()
            if fp is None:
                return None
            cached = self._cache.get("snapshot", fp)
            if cached is not None:
                return cached
            conn = self._connect(readonly=True)
            try:
                rows = conn.execute(
                    "SELECT id, source, category, payload_blob, vector_blob, vector_kind, "
                    "heat, created_at, updated_at, valid_from, valid_until, superseded_by "
                    "FROM owner_memory_entries ORDER BY updated_at DESC LIMIT ?",
                    (MAX_RECALL_SCAN,),
                ).fetchall()
                ent_rows = conn.execute(
                    "SELECT entry_id, entity_norm FROM owner_memory_entities"
                ).fetchall()
                link_rows = conn.execute(
                    "SELECT src_id, dst_id, relation, weight FROM owner_memory_links"
                ).fetchall()
            except sqlite3.OperationalError:
                return None
            finally:
                conn.close()

            entry_entities: dict[str, set[str]] = {}
            norm_to_entries: dict[str, set[str]] = {}
            for erow in ent_rows:
                eid = str(erow["entry_id"])
                enorm = str(erow["entity_norm"])
                entry_entities.setdefault(eid, set()).add(enorm)
                norm_to_entries.setdefault(enorm, set()).add(eid)

            links_by_node: dict[str, list[tuple[str, str, float]]] = {}
            for lrow in link_rows:
                src = str(lrow["src_id"])
                dst = str(lrow["dst_id"])
                rel = str(lrow["relation"])
                w = float(lrow["weight"])
                links_by_node.setdefault(src, []).append((dst, rel, w))
                links_by_node.setdefault(dst, []).append((src, rel, w))

            entries: dict[str, dict[str, Any]] = {}
            has_semantic = False
            for row in rows:
                eid = str(row["id"])
                try:
                    data = self.vault.open(f"owner-memory:e:{eid}", row["payload_blob"])
                except Exception:
                    continue
                if not isinstance(data, dict):
                    continue
                raw_vec = self._open_vector(eid, row["vector_blob"])
                if raw_vec is None:
                    continue
                if is_semantic_blob(raw_vec):
                    has_semantic = True
                topic = str(data.get("topic") or "")
                body = str(data.get("text") or "")
                tags = tuple(str(t) for t in (data.get("tags") or ()) if isinstance(t, str))
                ents = tuple(str(e) for e in (data.get("entities") or ()) if isinstance(e, str))
                search_text = " ".join(
                    p for p in (topic, body, " ".join(tags), " ".join(ents)) if p
                )
                entries[eid] = {
                    "id": eid,
                    "source": str(row["source"]),
                    "category": str(row["category"]),
                    "topic": topic,
                    "text": body,
                    "tags": tags,
                    "entities": ents,
                    "entity_norms": frozenset(entry_entities.get(eid, ())),
                    "vector": raw_vec,
                    "vector_kind": "semantic" if is_semantic_blob(raw_vec) else "legacy_hash",
                    "heat": int(row["heat"]),
                    "created_at": float(row["created_at"]),
                    "updated_at": float(row["updated_at"]),
                    "valid_from": float(row["valid_from"]),
                    "valid_until": (
                        float(row["valid_until"]) if row["valid_until"] is not None else None
                    ),
                    "superseded_by": (
                        str(row["superseded_by"]) if row["superseded_by"] is not None else None
                    ),
                    "terms": frozenset(_legacy_terms(search_text)),
                }

            snapshot = {
                "entries": entries,
                "norm_to_entries": {k: frozenset(v) for k, v in norm_to_entries.items()},
                "links_by_node": {k: tuple(v) for k, v in links_by_node.items()},
                "has_semantic": has_semantic,
            }
            self._cache.put("snapshot", fp, snapshot)
            return snapshot

    @staticmethod
    def _is_entry_active(entry: dict[str, Any], ts: float) -> bool:
        if entry["superseded_by"] is not None:
            return False
        if entry["valid_from"] > ts:
            return False
        if entry["valid_until"] is not None and entry["valid_until"] <= ts:
            return False
        return True

    def _format_entry(
        self,
        entry: dict[str, Any],
        *,
        score: float,
        ts: float,
        via_hop: int = 0,
    ) -> dict[str, Any]:
        return {
            "id": entry["id"],
            "source": entry["source"],
            "category": entry["category"],
            "topic": entry["topic"],
            "text": entry["text"],
            "tags": list(entry["tags"]),
            "entities": list(entry["entities"]),
            "score": round(max(0.0, min(1.0, float(score))), 4),
            "heat": entry["heat"],
            "created_at": entry["created_at"],
            "updated_at": entry["updated_at"],
            "valid_from": entry["valid_from"],
            "valid_until": entry["valid_until"],
            "superseded_by": entry["superseded_by"],
            "is_active": self._is_entry_active(entry, ts),
            "via_hop": via_hop,
        }

    async def recall(
        self,
        query: str,
        *,
        query_vector: bytes | None = None,
        limit: int = 4,
        include_superseded: bool = False,
        expand_hops: int = 1,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """Recall Owner memories with bi-temporal filtering, hybrid scoring, and 1-hop expansion."""
        bounded_limit = max(1, min(int(limit), 32))
        ts = float(self.clock() if now is None else now)
        snapshot = await asyncio.to_thread(self._load_snapshot_sync)
        if not snapshot or not snapshot["entries"]:
            return []

        all_entries: dict[str, dict[str, Any]] = snapshot["entries"]
        eligible: dict[str, dict[str, Any]] = {
            eid: entry
            for eid, entry in all_entries.items()
            if include_superseded or self._is_entry_active(entry, ts)
        }
        if not eligible:
            return []

        q_clean = str(query or "").strip()
        sem_qvec = query_vector if is_semantic_blob(query_vector) else None
        if not q_clean and sem_qvec is None:
            ordered = sorted(
                eligible.values(),
                key=lambda e: (e["updated_at"], e["heat"]),
                reverse=True,
            )[:bounded_limit]
            return [self._format_entry(e, score=0.5, ts=ts, via_hop=0) for e in ordered]

        if sem_qvec is None and snapshot["has_semantic"] and self.embedder is not None and q_clean:
            sem_qvec = await embed_one_safe(
                self.embedder,
                q_clean,
                task_type="RETRIEVAL_QUERY",
                budget_lane="memory",
                subject="owner_memory",
                purpose="owner_memory_recall",
            )

        q_lower = q_clean.lower()
        q_terms = _legacy_terms(q_clean) if q_clean else set()
        leg_qvec = _legacy_vector(q_clean) if q_clean else None

        direct_scores: dict[str, float] = {}
        seed_ids: list[str] = []
        for eid, entry in eligible.items():
            overlap = len(q_terms & entry["terms"]) if q_terms else 0
            lex_norm = (
                min(1.0, overlap / max(1, min(len(q_terms), 6))) if q_terms and overlap > 0 else 0.0
            )
            topic_hit = bool(
                q_lower
                and entry["topic"]
                and (entry["topic"].lower() in q_lower or q_lower in entry["topic"].lower())
            )
            entity_hit = any(
                q_lower and ent.lower() in q_lower or (len(q_lower) >= 2 and q_lower in ent.lower())
                for ent in entry["entities"]
            )
            if topic_hit:
                lex_norm = max(lex_norm, 0.72)
            if entity_hit:
                lex_norm = max(lex_norm, 0.78)

            vec_sim = 0.0
            used_semantic = False
            if entry["vector_kind"] == "semantic" and sem_qvec is not None:
                sim = cosine_similarity_blobs(sem_qvec, entry["vector"])
                if sim is not None:
                    vec_sim = max(0.0, sim)
                    used_semantic = True
            elif entry["vector_kind"] != "semantic" and leg_qvec is not None:
                sim = cosine_similarity_blobs(leg_qvec, entry["vector"])
                if sim is not None:
                    vec_sim = max(0.0, sim)

            if used_semantic:
                base = max(vec_sim, 0.78 * vec_sim + 0.22 * lex_norm)
                is_hit = vec_sim >= 0.35 or overlap > 0 or topic_hit or entity_hit
            else:
                base = max(vec_sim, 0.55 * vec_sim + 0.45 * lex_norm) if overlap or vec_sim else 0.0
                is_hit = overlap > 0 or vec_sim >= 0.35 or topic_hit or entity_hit

            if not is_hit or base <= 0.0:
                continue

            age_sec = max(0.0, ts - entry["updated_at"])
            recency_bonus = 0.06 * math.exp(-age_sec / 2_592_000.0)
            heat_bonus = min(entry["heat"], 10) * 0.005
            score = min(1.0, base + recency_bonus + heat_bonus)
            direct_scores[eid] = score

        ranked_direct = sorted(
            direct_scores.items(),
            key=lambda kv: (kv[1], eligible[kv[0]]["updated_at"]),
            reverse=True,
        )
        seed_ids = [eid for eid, _ in ranked_direct[: max(2, bounded_limit)]]

        # 1-hop entity graph & explicit link expansion
        expanded_scores: dict[str, tuple[float, int]] = {
            eid: (score, 0) for eid, score in direct_scores.items()
        }
        if expand_hops >= 1 and seed_ids:
            norm_to_entries: dict[str, frozenset[str]] = snapshot["norm_to_entries"]
            links_by_node: dict[str, tuple[tuple[str, str, float], ...]] = snapshot["links_by_node"]
            for sid in seed_ids:
                seed_score = direct_scores[sid]
                seed_entry = eligible[sid]
                # 1a. Shared entity neighbors
                for enorm in seed_entry["entity_norms"]:
                    for neighbor_id in norm_to_entries.get(enorm, ()):
                        if neighbor_id == sid or neighbor_id not in eligible:
                            continue
                        hop_score = min(0.95, seed_score * 0.72)
                        prev = expanded_scores.get(neighbor_id)
                        if prev is None or hop_score > prev[0]:
                            expanded_scores[neighbor_id] = (
                                hop_score,
                                prev[1] if prev is not None and prev[1] == 0 else 1,
                            )
                # 1b. Explicit link neighbors
                for neighbor_id, rel, weight in links_by_node.get(sid, ()):
                    if neighbor_id == sid or neighbor_id not in eligible:
                        continue
                    if rel == "supersedes" and not include_superseded:
                        continue
                    w_factor = min(1.1, max(0.35, 0.70 * float(weight)))
                    hop_score = min(0.95, seed_score * w_factor)
                    prev = expanded_scores.get(neighbor_id)
                    if prev is None or hop_score > prev[0]:
                        expanded_scores[neighbor_id] = (
                            hop_score,
                            prev[1] if prev is not None and prev[1] == 0 else 1,
                        )

        final_ids = sorted(
            expanded_scores.items(),
            key=lambda kv: (kv[1][0], -kv[1][1], eligible[kv[0]]["updated_at"]),
            reverse=True,
        )[:bounded_limit]
        return [
            self._format_entry(eligible[eid], score=sc, ts=ts, via_hop=hop)
            for eid, (sc, hop) in final_ids
        ]

    async def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """Probe active Owner memories for reverse semantic/lexical resonance."""
        if not str(query_text or "").strip() and not is_semantic_blob(query_vector):
            return []
        hits = await self.recall(
            query_text,
            query_vector=query_vector,
            limit=limit,
            include_superseded=False,
            expand_hops=1,
            now=now,
        )
        return [
            {
                "id": item["id"],
                "source": item["source"],
                "topic": item["topic"],
                "text": item["text"],
                "score": item["score"],
                "entities": list(item["entities"]),
            }
            for item in hits
        ]

    def _select_backfill_candidates_sync(self, limit: int) -> list[tuple[str, str, float]]:
        with self._thread_lock:
            self._check_not_symlink()
            if not self.db_path.exists():
                return []
            conn = self._connect(readonly=True)
            try:
                rows = conn.execute(
                    "SELECT id, payload_blob, updated_at FROM owner_memory_entries "
                    "WHERE superseded_by IS NULL AND vector_kind != 'semantic' "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (max(1, min(int(limit), 100)),),
                ).fetchall()
            except sqlite3.OperationalError:
                return []
            finally:
                conn.close()
        candidates: list[tuple[str, str, float]] = []
        for row in rows:
            eid = str(row["id"])
            try:
                data = self.vault.open(f"owner-memory:e:{eid}", row["payload_blob"])
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            doc_text = " ".join(
                part
                for part in (
                    str(data.get("topic") or ""),
                    str(data.get("text") or ""),
                    " ".join(t for t in (data.get("tags") or ()) if isinstance(t, str)),
                    " ".join(e for e in (data.get("entities") or ()) if isinstance(e, str)),
                )
                if part
            ).strip()
            if doc_text:
                candidates.append((eid, doc_text, float(row["updated_at"])))
        return candidates

    def _apply_backfill_sync(self, updates: list[tuple[str, bytes, float]]) -> int:
        if not updates:
            return 0
        updated = 0
        with self._thread_lock:
            self._check_not_symlink()
            if not self.db_path.exists():
                return 0
            conn = self._connect(readonly=False)
            try:
                conn.execute("BEGIN IMMEDIATE")
                for eid, raw_vec, expected_updated in updates:
                    sealed = self._seal_vector(eid, raw_vec)
                    cur = conn.execute(
                        "UPDATE owner_memory_entries SET vector_blob=?, vector_kind='semantic' "
                        "WHERE id=? AND superseded_by IS NULL AND updated_at=?",
                        (sealed, eid, expected_updated),
                    )
                    updated += max(0, int(cur.rowcount or 0))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            if updated:
                self._cache.clear()
        return updated

    async def backfill_embeddings(self, embedder: Any, *, limit: int = 20) -> int:
        """Backfill legacy hashed vectors to Vault-sealed semantic embeddings in bounded batches."""
        if embedder is None or limit <= 0:
            return 0
        candidates = await asyncio.to_thread(self._select_backfill_candidates_sync, limit)
        if not candidates:
            return 0
        blobs = await embed_texts_safe(
            embedder,
            [doc for _, doc, _ in candidates],
            task_type="RETRIEVAL_DOCUMENT",
            budget_lane="memory",
            subject="owner_memory",
            purpose="owner_memory_backfill",
        )
        updates: list[tuple[str, bytes, float]] = []
        for (eid, _doc, old_updated), blob in zip(candidates, blobs, strict=True):
            if is_semantic_blob(blob):
                updates.append((eid, bytes(blob), old_updated))  # type: ignore[arg-type]
        if not updates:
            return 0
        return await asyncio.to_thread(self._apply_backfill_sync, updates)

    def _stats_sync(self, *, now: float | None = None) -> dict[str, Any]:
        with self._thread_lock:
            self._check_not_symlink()
            if not self.db_path.exists():
                return {
                    "db_exists": False,
                    "total_entries": 0,
                    "active_entries": 0,
                    "superseded_entries": 0,
                    "semantic_entries": 0,
                    "legacy_entries": 0,
                    "entities_count": 0,
                    "links_count": 0,
                }
            ts = float(self.clock() if now is None else now)
            conn = self._connect(readonly=True)
            try:
                row = conn.execute(
                    "SELECT "
                    "COUNT(*) AS total, "
                    "COALESCE(SUM(CASE WHEN superseded_by IS NULL AND valid_from <= ? "
                    "AND (valid_until IS NULL OR valid_until > ?) THEN 1 ELSE 0 END), 0) "
                    "AS active, "
                    "COALESCE(SUM(CASE WHEN superseded_by IS NOT NULL THEN 1 ELSE 0 END), 0) "
                    "AS superseded, "
                    "COALESCE(SUM(CASE WHEN vector_kind = 'semantic' THEN 1 ELSE 0 END), 0) "
                    "AS semantic, "
                    "COALESCE(SUM(CASE WHEN vector_kind != 'semantic' THEN 1 ELSE 0 END), 0) "
                    "AS legacy "
                    "FROM owner_memory_entries",
                    (ts, ts),
                ).fetchone()
                ent_count = conn.execute("SELECT COUNT(*) FROM owner_memory_entities").fetchone()[0]
                link_count = conn.execute("SELECT COUNT(*) FROM owner_memory_links").fetchone()[0]
            except sqlite3.OperationalError:
                return {
                    "db_exists": True,
                    "total_entries": 0,
                    "active_entries": 0,
                    "superseded_entries": 0,
                    "semantic_entries": 0,
                    "legacy_entries": 0,
                    "entities_count": 0,
                    "links_count": 0,
                }
            finally:
                conn.close()
        return {
            "db_exists": True,
            "total_entries": int(row["total"] or 0),
            "active_entries": int(row["active"] or 0),
            "superseded_entries": int(row["superseded"] or 0),
            "semantic_entries": int(row["semantic"] or 0),
            "legacy_entries": int(row["legacy"] or 0),
            "entities_count": int(ent_count or 0),
            "links_count": int(link_count or 0),
        }

    async def stats(self, *, now: float | None = None) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats_sync, now=now)
