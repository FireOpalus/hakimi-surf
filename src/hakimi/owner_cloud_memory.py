"""Read-only owner memory loader & local recall for Antigravity_Memory.

Antigravity_Memory is STRICTLY READ-ONLY. Any optional cached semantic embeddings
live in a Vault-sealed local SQLite cache under ``settings.data_dir`` (never inside
``antigravity_memory_dir``).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import sqlite3
import stat
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .owner_memory import (
    cosine_similarity_blobs,
    embed_texts_safe,
    is_semantic_blob,
)
from .reply_style import trim_complete_text
from .security import CREDENTIAL_RE, ROLE_TAG_RE, Rejected, Vault, clean_text, redact_secrets
from .social import bound_owner

MAX_FILE_BYTES = 262_144
MAX_PINNED_CHARS = 1800
MAX_RECALLED_CHARS = 1800
MAX_RECENT_CORE_TURNS = 4
MAX_CACHED_EMBEDDINGS = 4096

_CLOUD_CACHE_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS cloud_chunk_embeddings (
        chunk_sha256 TEXT PRIMARY KEY,
        vector_blob BLOB NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_cloud_chunk_embeddings_updated
        ON cloud_chunk_embeddings(updated_at DESC)
    """,
)

BROAD_RECALL_RE = re.compile(
    r"记得|忘|想起来|以前|之前|过去|从前|当初|相遇|约定|契约|小本本|回忆|记忆"
)
CJK_STOP_CLASS = (
    "的了吗呢吧啊哦呀是我你他她它这那在有就都也还说要会想看"
    "很把被让给和与或但去来着过好吗嘛哈喵呜么啥怎"
)
CJK_SPLIT_RE = re.compile(rf"[{CJK_STOP_CLASS}]+")
CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]+")
ASCII_TOKEN_RE = re.compile(r"[a-z0-9_]{2,}")

__all__ = ["CREDENTIAL_RE", "ROLE_TAG_RE", "OwnerCloudMemory"]


def _sanitize(text: str, settings) -> str:
    cleaned = clean_text(str(text or ""))
    return redact_secrets(cleaned, settings, neutralize_role_tags=True)


def _read_regular_file(path: Path, max_bytes: int = MAX_FILE_BYTES) -> str | None:
    if path.is_symlink():
        return None
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        with os.fdopen(fd, "rb") as file:
            if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                return None
            raw = file.read(max_bytes + 1)
    except OSError:
        return None
    return raw[:max_bytes].decode("utf-8", errors="ignore")


def _tokenize(text: str) -> tuple[set[str], set[str], list[str]]:
    """Return (all_tokens, strong_ascii_tokens, clean_cjk_phrases)."""
    lowered = text.lower()
    ascii_all = set(ASCII_TOKEN_RE.findall(lowered))
    strong_ascii = {tok for tok in ascii_all if len(tok) >= 3}
    cjk_bigrams: set[str] = set()
    phrases: list[str] = []
    for run in CJK_RUN_RE.findall(text):
        for seg in CJK_SPLIT_RE.split(run):
            if len(seg) >= 2:
                for idx in range(len(seg) - 1):
                    cjk_bigrams.add(seg[idx : idx + 2])
            if len(seg) >= 3:
                phrases.append(seg)
    return (ascii_all | cjk_bigrams), strong_ascii, phrases


def _extract_pinned_memos(notebook_md: str) -> str:
    if not notebook_md.strip():
        return ""
    marker = "## 🌟"
    idx = notebook_md.find(marker)
    section = notebook_md[idx:] if idx >= 0 else notebook_md
    end = section.find("\n## 📑")
    if end >= 0:
        section = section[:end]
    section = re.sub(r"\n---\s*$", "", section).strip()
    return trim_complete_text(section, MAX_PINNED_CHARS)


def _parse_notebook_index(notebook_md: str) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    idx = notebook_md.find("## 📑")
    if idx < 0:
        return chunks
    for line in notebook_md[idx:].splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 3:
            continue
        date_range, tags, summary = cells[0], cells[1], cells[2]
        if not date_range or date_range.startswith(("归档日期", ":---", "---")):
            continue
        body = trim_complete_text(f"[{date_range}] {tags}：{summary}", 360)
        tokens, strong_ascii, _ = _tokenize(f"{date_range} {tags} {summary}")
        chunks.append(
            {
                "source": "notebook_index",
                "date": date_range,
                "title": tags[:80],
                "text": body,
                "tokens": tokens,
                "strong_ascii": strong_ascii,
                "tags": {t.lower() for t in re.findall(r"#[^\s|]+", tags)},
            }
        )
    return chunks


def _parse_markdown_sections(text: str, *, source: str, title_prefix: str) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    parts = re.split(r"(?=^#{2,3}\s+)", text, flags=re.MULTILINE)
    for part in parts:
        block = part.strip()
        if not block or block.startswith("# 长期记忆"):
            continue
        lines = block.splitlines()
        heading = lines[0].lstrip("#").strip() if lines else title_prefix
        cleaned_lines = [
            ln for ln in lines if not ln.startswith(("<a id=", "摘要方式：", "[查看本批"))
        ]
        body = trim_complete_text("\n".join(cleaned_lines).strip(), 420)
        if len(body) < 20:
            continue
        tokens, strong_ascii, _ = _tokenize(f"{title_prefix} {body}")
        chunks.append(
            {
                "source": source,
                "date": heading[:40],
                "title": f"{title_prefix} · {heading}"[:90],
                "text": body,
                "tokens": tokens,
                "strong_ascii": strong_ascii,
                "tags": {t.lower() for t in re.findall(r"#[^\s，。]+", body)},
            }
        )
    return chunks


def _chunk_sha256(chunk: dict[str, Any]) -> str:
    material = (
        f"{chunk.get('source', '')}|{chunk.get('date', '')}|"
        f"{chunk.get('title', '')}|{chunk.get('text', '')}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class OwnerCloudMemory:
    """Thread-safe, mtime-cached read-only memory & local search for private Owner chats."""

    def __init__(
        self,
        settings,
        *,
        vault: Vault | Any | None = None,
        embedder: Any = None,
        cache_path: Path | None = None,
    ):
        self.s = settings
        self._vault: Vault | None = (
            vault
            if isinstance(vault, Vault)
            else getattr(vault, "vault", None)
            if vault is not None
            else None
        )
        self.embedder = embedder
        self._cache_path = (
            Path(cache_path)
            if cache_path is not None
            else (Path(settings.data_dir) / "owner_cloud_embed_cache.sqlite3")
        )
        self._lock = threading.Lock()
        self._signature: tuple[tuple[str, int, int], ...] = ()
        self._pinned = ""
        self._recent_core: list[dict[str, str]] = []
        self._chunks: list[dict[str, Any]] = []
        self._index_chunks: list[dict[str, Any]] = []
        self._latest_ts: float = 0.0
        self._latest_iso: str = ""
        self._latest_tag: str = ""
        self._chunk_vectors: dict[str, bytes] = {}
        self._cache_fp: tuple[int, ...] | None = None

    def _get_vault(self) -> Vault:
        if self._vault is None:
            self._vault = Vault(self.s.encryption_key.get_secret_value())
        return self._vault

    def _check_cache_path_safe(self) -> None:
        for candidate in (self._cache_path, self._cache_path.parent, Path(self.s.data_dir)):
            if candidate.is_symlink():
                raise Rejected("invalid_cloud_cache_path")
        cloud_dir = getattr(self.s, "antigravity_memory_dir", None)
        if cloud_dir:
            try:
                cloud_resolved = Path(cloud_dir).resolve()
                cache_resolved = self._cache_path.resolve()
                if cache_resolved == cloud_resolved or cloud_resolved in cache_resolved.parents:
                    raise Rejected("invalid_cloud_cache_path")
            except OSError:
                pass

    def _cache_fingerprint(self) -> tuple[int, ...] | None:
        try:
            st = os.lstat(self._cache_path)
        except OSError:
            return None
        if stat.S_ISLNK(st.st_mode):
            raise Rejected("invalid_cloud_cache_path")
        if not stat.S_ISREG(st.st_mode):
            return None
        wal_size = -1
        wal_mtime = -1
        try:
            wst = os.lstat(f"{self._cache_path}-wal")
            wal_size = wst.st_size
            wal_mtime = wst.st_mtime_ns
        except OSError:
            pass
        return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, wal_size, wal_mtime)

    def _load_cached_vectors_locked(self) -> None:
        try:
            self._check_cache_path_safe()
            fp = self._cache_fingerprint()
        except Rejected:
            self._chunk_vectors = {}
            self._cache_fp = None
            return
        if fp is None:
            self._chunk_vectors = {}
            self._cache_fp = None
            return
        if fp == self._cache_fp and self._chunk_vectors:
            return
        wanted = {chunk["chunk_hash"] for chunk in self._chunks if chunk.get("chunk_hash")}
        if not wanted:
            self._chunk_vectors = {}
            self._cache_fp = fp
            return
        vault = self._get_vault()
        uri = f"{self._cache_path.absolute().as_uri()}?mode=ro"
        loaded: dict[str, bytes] = {}
        try:
            conn = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5.0)
            try:
                conn.execute("PRAGMA busy_timeout=5000")
                rows = conn.execute(
                    "SELECT chunk_sha256, vector_blob FROM cloud_chunk_embeddings"
                ).fetchall()
            finally:
                conn.close()
        except sqlite3.Error:
            self._chunk_vectors = {}
            self._cache_fp = None
            return
        for chunk_hash, blob in rows:
            if chunk_hash not in wanted:
                continue
            try:
                raw_vec = base64.b64decode(vault.open(f"owner-cloud-embed:{chunk_hash}", blob))
            except Exception:
                continue
            if is_semantic_blob(raw_vec):
                loaded[str(chunk_hash)] = raw_vec
        self._chunk_vectors = loaded
        self._cache_fp = fp

    def cache_embeddings(self, mapping: dict[str, bytes]) -> int:
        """Persist Vault-sealed semantic vectors keyed by chunk SHA-256 into local cache DB."""
        clean_items = [
            (str(k).strip(), bytes(v))
            for k, v in mapping.items()
            if isinstance(k, str) and len(k.strip()) == 64 and is_semantic_blob(v)
        ]
        if not clean_items:
            return 0
        with self._lock:
            self._check_cache_path_safe()
            self._cache_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                self._cache_path.parent.chmod(0o700)
            except OSError:
                pass
            fd = os.open(self._cache_path, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self._cache_path.chmod(0o600)
            vault = self._get_vault()
            now_ts = time.time()
            conn = sqlite3.connect(self._cache_path, isolation_level=None, timeout=5.0)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("PRAGMA secure_delete=ON")
                conn.execute("PRAGMA busy_timeout=5000")
                conn.execute("BEGIN IMMEDIATE")
                for stmt in _CLOUD_CACHE_SCHEMA:
                    conn.execute(stmt)
                for chunk_hash, raw_vec in clean_items:
                    sealed = vault.seal(
                        f"owner-cloud-embed:{chunk_hash}",
                        base64.b64encode(raw_vec).decode("ascii"),
                    )
                    conn.execute(
                        "INSERT INTO cloud_chunk_embeddings(chunk_sha256, vector_blob, updated_at) "
                        "VALUES (?,?,?) ON CONFLICT(chunk_sha256) DO UPDATE SET "
                        "vector_blob=excluded.vector_blob, updated_at=excluded.updated_at",
                        (chunk_hash, sealed, now_ts),
                    )
                conn.execute(
                    "DELETE FROM cloud_chunk_embeddings WHERE chunk_sha256 NOT IN ("
                    "SELECT chunk_sha256 FROM cloud_chunk_embeddings "
                    "ORDER BY updated_at DESC LIMIT ?)",
                    (MAX_CACHED_EMBEDDINGS,),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
            for chunk_hash, raw_vec in clean_items:
                self._chunk_vectors[chunk_hash] = raw_vec
            self._cache_fp = self._cache_fingerprint()
        return len(clean_items)

    async def sync_embeddings(self, embedder: Any = None, *, limit: int = 40) -> int:
        """Compute and Vault-seal semantic embeddings for uncached read-only cloud chunks."""
        active = embedder if embedder is not None else self.embedder
        if active is None or not self.s.enable_antigravity_memory or limit <= 0:
            return 0
        with self._lock:
            self._rebuild_if_needed()
            missing = [
                (chunk["chunk_hash"], f"{chunk['title']} {chunk['text']}".strip())
                for chunk in self._chunks
                if chunk.get("chunk_hash") and chunk["chunk_hash"] not in self._chunk_vectors
            ][: max(1, min(int(limit), 100))]
        if not missing:
            return 0
        blobs = await embed_texts_safe(
            active,
            [txt for _, txt in missing],
            task_type="RETRIEVAL_DOCUMENT",
            budget_lane="memory",
            subject="owner_cloud_memory",
            purpose="owner_cloud_embed",
        )
        mapping = {
            chunk_hash: bytes(blob)
            for (chunk_hash, _), blob in zip(missing, blobs, strict=True)
            if is_semantic_blob(blob)
        }
        if not mapping:
            return 0
        return await asyncio.to_thread(self.cache_embeddings, mapping)

    def _collect_files(self) -> list[tuple[str, Path]]:
        root = Path(self.s.antigravity_memory_dir)
        if not root.is_dir():
            return []
        files: list[tuple[str, Path]] = []
        for name in ("notebook.md", "short_term_memory.json"):
            candidate = root / name
            if candidate.is_file() and not candidate.is_symlink():
                files.append((name, candidate))
        for subdir in ("long_term", "notes"):
            subpath = root / subdir
            if not subpath.is_dir() or subpath.is_symlink():
                continue
            for item in sorted(subpath.iterdir()):
                if (
                    item.suffix.lower() == ".md"
                    and not (subdir == "notes" and item.name.lower() == "readme.md")
                    and item.is_file()
                    and not item.is_symlink()
                ):
                    files.append((f"{subdir}/{item.name}", item))
        return files

    def _signature_for(self, files: list[tuple[str, Path]]) -> tuple[tuple[str, int, int], ...]:
        sig: list[tuple[str, int, int]] = []
        for rel, path in files:
            try:
                st = path.stat(follow_symlinks=False)
                if stat.S_ISREG(st.st_mode):
                    sig.append((rel, int(st.st_mtime_ns), int(st.st_size)))
            except OSError:
                continue
        return tuple(sig)

    def _rebuild_if_needed(self) -> None:
        files = self._collect_files()
        sig = self._signature_for(files)
        if sig == self._signature and (sig or not files):
            self._load_cached_vectors_locked()
            return

        pinned = ""
        recent_core: list[dict[str, str]] = []
        chunks: list[dict[str, Any]] = []
        index_chunks: list[dict[str, Any]] = []
        latest_ts = 0.0
        latest_iso = ""
        latest_tag = ""
        try:
            bot_tz = ZoneInfo(self.s.bot_timezone)
        except ZoneInfoNotFoundError:
            bot_tz = ZoneInfo("UTC")

        for rel, path in files:
            raw = _read_regular_file(path)
            if raw is None:
                continue
            if rel == "short_term_memory.json":
                try:
                    items = json.loads(raw)
                except ValueError:
                    items = []
                if isinstance(items, list):
                    for item in items[-MAX_RECENT_CORE_TURNS:]:
                        if not isinstance(item, dict):
                            continue
                        dlg = item.get("dialogue") or {}
                        recent_core.append(
                            {
                                "at": _sanitize(str(item.get("timestamp", ""))[:16], self.s),
                                "tag": _sanitize(str(item.get("tag", ""))[:24], self.s),
                                "boiser": trim_complete_text(
                                    _sanitize(str(dlg.get("user", "")), self.s), 130
                                ),
                                "hakimi": trim_complete_text(
                                    _sanitize(str(dlg.get("assistant", "")), self.s), 150
                                ),
                            }
                        )
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        dlg = item.get("dialogue") or {}
                        ts = _sanitize(str(item.get("timestamp", ""))[:16], self.s)
                        tag = _sanitize(str(item.get("tag", ""))[:24], self.s)
                        u_txt = trim_complete_text(_sanitize(str(dlg.get("user", "")), self.s), 200)
                        a_txt = trim_complete_text(
                            _sanitize(str(dlg.get("assistant", "")), self.s), 220
                        )
                        body = f"[{ts} | {tag}] BOISER: {u_txt} / 哈基米: {a_txt}"
                        tokens, strong_ascii, _ = _tokenize(f"{tag} {u_txt} {a_txt}")
                        chunks.append(
                            {
                                "source": "recent_dialogue",
                                "date": ts,
                                "title": f"近期对话 · {tag}",
                                "text": body,
                                "tokens": tokens,
                                "strong_ascii": strong_ascii,
                                "tags": set(),
                            }
                        )
                        raw_ts = str(item.get("timestamp", "")).strip()
                        if len(raw_ts) >= 16:
                            parsed = None
                            try:
                                parsed = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S").replace(
                                    tzinfo=bot_tz
                                )
                            except ValueError:
                                try:
                                    parsed = datetime.fromisoformat(raw_ts)
                                    if parsed.tzinfo is None:
                                        parsed = parsed.replace(tzinfo=bot_tz)
                                except ValueError:
                                    parsed = None
                            if parsed is not None:
                                ts_val = parsed.timestamp()
                                if ts_val >= latest_ts:
                                    latest_ts = ts_val
                                    latest_iso = parsed.astimezone(bot_tz).isoformat(
                                        timespec="seconds"
                                    )
                                    latest_tag = _sanitize(str(item.get("tag", "")), self.s)[:24]
                continue

            text = _sanitize(raw, self.s)
            if rel == "notebook.md":
                pinned = _extract_pinned_memos(text)
                index_chunks = _parse_notebook_index(text)
                chunks.extend(index_chunks)
            elif rel.startswith("long_term/"):
                chunks.extend(
                    _parse_markdown_sections(text, source="long_term", title_prefix=Path(rel).stem)
                )
            elif rel.startswith("notes/"):
                chunks.extend(
                    _parse_markdown_sections(text, source="notes", title_prefix=Path(rel).stem)
                )

        for chunk in chunks:
            chunk["chunk_hash"] = _chunk_sha256(chunk)

        self._signature = sig
        self._pinned = pinned
        self._recent_core = recent_core
        self._chunks = chunks
        self._index_chunks = index_chunks
        self._latest_ts = latest_ts
        self._latest_iso = latest_iso
        self._latest_tag = latest_tag
        self._cache_fp = None
        self._load_cached_vectors_locked()

    def _search_chunks(
        self,
        query_text: str,
        *,
        recall: bool,
        query_vector: bytes | None = None,
    ) -> list[dict[str, str]]:
        if not self._chunks:
            return []
        q_tokens, q_strong, q_phrases = _tokenize(query_text)
        broad = bool(BROAD_RECALL_RE.search(query_text))
        sem_qvec = bytes(query_vector) if is_semantic_blob(query_vector) else None
        if not q_tokens and not recall and not broad and sem_qvec is None:
            return []

        scored: list[tuple[float, int, dict[str, Any]]] = []
        for idx, chunk in enumerate(self._chunks):
            overlap = q_tokens & chunk["tokens"]
            sem_sim = 0.0
            if sem_qvec is not None:
                chunk_vec = self._chunk_vectors.get(chunk.get("chunk_hash", ""))
                if chunk_vec is not None:
                    sim_val = cosine_similarity_blobs(sem_qvec, chunk_vec)
                    if sim_val is not None:
                        sem_sim = max(0.0, sim_val)
            if not overlap and not broad and sem_sim < 0.40:
                continue
            score = float(len(overlap))
            score += float(len(q_strong & chunk["strong_ascii"]))
            if any(tag in query_text.lower() for tag in chunk["tags"]):
                score += 1.5
            lowered_body = chunk["text"].lower()
            for phrase in q_phrases:
                if phrase in chunk["text"] or phrase.lower() in lowered_body:
                    score += 2.5
            if overlap and chunk["source"] in {"notebook_index", "notes"}:
                score += 0.5
            if sem_sim >= 0.40:
                score += sem_sim * 4.5
            if score >= 1.0:
                scored.append((score, idx, chunk))

        scored.sort(key=lambda item: (-item[0], -item[1]))
        best_score = scored[0][0] if scored else 0.0
        strong_hits = sum(1 for item in scored if item[0] >= 2.0)
        if not (recall or broad or best_score >= 2.0):
            return []

        selected: list[dict[str, str]] = []
        seen_texts: set[str] = set()
        total_chars = 0

        def add_chunk(chunk: dict[str, Any]) -> None:
            nonlocal total_chars
            text = chunk["text"]
            if text in seen_texts or len(selected) >= 4:
                return
            if total_chars + len(text) > MAX_RECALLED_CHARS and selected:
                return
            seen_texts.add(text)
            total_chars += len(text)
            selected.append(
                {
                    "source": chunk["source"],
                    "date": chunk["date"],
                    "title": chunk["title"],
                    "excerpt": trim_complete_text(text, MAX_RECALLED_CHARS),
                }
            )

        for _, _, chunk in scored[:6]:
            add_chunk(chunk)

        if broad and strong_hits < 2 and self._index_chunks:
            anchors = list(self._index_chunks[:2])
            if len(self._index_chunks) > 2:
                anchors.append(self._index_chunks[-1])
            for anchor in anchors:
                add_chunk(anchor)

        return selected

    def for_message(
        self,
        message,
        query_text: str = "",
        *,
        recall: bool = False,
        query_vector: bytes | None = None,
    ) -> dict[str, Any]:
        if (
            not self.s.enable_antigravity_memory
            or message is None
            or message.kind != "private"
            or not bound_owner(message, self.s, "notebook")
        ):
            return {}
        with self._lock:
            self._rebuild_if_needed()
            result: dict[str, Any] = {}
            core: dict[str, Any] = {}
            if self._pinned:
                core["pinned_vows_and_memos"] = self._pinned
            if self._recent_core:
                core["recent_computer_chats"] = list(self._recent_core)
            if core:
                result["owner_core_memory"] = core
            recalled = self._search_chunks(query_text, recall=recall, query_vector=query_vector)
            if recalled:
                result["owner_recalled_memories"] = recalled
            return result

    def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        message: Any = None,
    ) -> list[dict[str, Any]]:
        """Probe read-only Owner cloud chunks for semantic/lexical resonance (Owner-only)."""
        if not self.s.enable_antigravity_memory:
            return []
        if message is not None:
            if message.kind != "private" or not bound_owner(message, self.s, "notebook"):
                return []
        elif not (self.s.owner_qq_id or getattr(self.s, "social_owner_bindings", None)):
            return []
        q_clean = str(query_text or "").strip()
        sem_qvec = bytes(query_vector) if is_semantic_blob(query_vector) else None
        if not q_clean and sem_qvec is None:
            return []

        with self._lock:
            self._rebuild_if_needed()
            chunks = list(self._chunks)
            chunk_vectors = dict(self._chunk_vectors)
        if not chunks:
            return []

        q_tokens, q_strong, q_phrases = _tokenize(q_clean) if q_clean else (set(), set(), [])
        q_lower = q_clean.lower()
        scored: list[tuple[float, int, dict[str, Any]]] = []
        for idx, chunk in enumerate(chunks):
            overlap = q_tokens & chunk["tokens"] if q_tokens else set()
            raw_lex = float(len(overlap)) + float(len(q_strong & chunk["strong_ascii"]))
            if q_lower and any(tag in q_lower for tag in chunk["tags"]):
                raw_lex += 1.5
            lowered_body = chunk["text"].lower()
            for phrase in q_phrases:
                if phrase in chunk["text"] or phrase.lower() in lowered_body:
                    raw_lex += 2.5
            if overlap and chunk["source"] in {"notebook_index", "notes"}:
                raw_lex += 0.5
            lex_norm = min(1.0, raw_lex / 5.0) if raw_lex > 0 else 0.0

            sem_sim: float | None = None
            if sem_qvec is not None:
                cvec = chunk_vectors.get(chunk.get("chunk_hash", ""))
                if cvec is not None:
                    val = cosine_similarity_blobs(sem_qvec, cvec)
                    if val is not None:
                        sem_sim = max(0.0, val)

            if sem_sim is not None:
                if sem_sim < 0.35 and raw_lex < 2.0:
                    continue
                score = max(sem_sim, 0.78 * sem_sim + 0.22 * lex_norm)
            else:
                if raw_lex < 2.0:
                    continue
                score = lex_norm
            scored.append((min(1.0, score), idx, chunk))

        scored.sort(key=lambda item: (-item[0], -item[1]))
        bounded_limit = max(1, min(int(limit), 10))
        out: list[dict[str, Any]] = []
        seen_texts: set[str] = set()
        for score, _idx, chunk in scored:
            text = chunk["text"]
            if text in seen_texts:
                continue
            seen_texts.add(text)
            chash = chunk.get("chunk_hash") or _chunk_sha256(chunk)
            out.append(
                {
                    "id": f"cloud:{chash[:16]}",
                    "source": f"cloud:{chunk['source']}",
                    "topic": chunk["title"],
                    "text": trim_complete_text(text, 360),
                    "score": round(score, 4),
                    "entities": sorted(chunk["tags"]),
                }
            )
            if len(out) >= bounded_limit:
                break
        return out

    def latest_activity_info(self) -> dict[str, Any]:
        enabled = bool(self.s.enable_antigravity_memory)
        with self._lock:
            if enabled:
                self._rebuild_if_needed()
            return {
                "enabled": enabled,
                "latest_timestamp": self._latest_ts if enabled else 0.0,
                "latest_iso": self._latest_iso if enabled else "",
                "latest_tag": self._latest_tag if enabled else "",
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            if self.s.enable_antigravity_memory:
                self._rebuild_if_needed()
            return {
                "enabled": self.s.enable_antigravity_memory,
                "loaded": bool(self._pinned or self._recent_core or self._chunks),
                "pinned_chars": len(self._pinned),
                "recent_turns": len(self._recent_core),
                "indexed_chunks": len(self._chunks),
                "cached_embeddings": len(self._chunk_vectors),
            }

    def admin_preview(self) -> dict[str, Any]:
        """Return sanitized read-only Owner Antigravity_Memory bridge status for /admin."""
        enabled = bool(self.s.enable_antigravity_memory)
        with self._lock:
            if enabled:
                self._rebuild_if_needed()
            return {
                "enabled": enabled,
                "loaded": bool(self._pinned or self._recent_core or self._chunks)
                if enabled
                else False,
                "pinned_text": self._pinned if enabled else "",
                "pinned_chars": len(self._pinned) if enabled else 0,
                "recent_turns": len(self._recent_core) if enabled else 0,
                "indexed_chunks": len(self._chunks) if enabled else 0,
                "latest_timestamp": self._latest_ts if enabled else 0.0,
                "latest_iso": self._latest_iso if enabled else "",
                "latest_tag": self._latest_tag if enabled else "",
            }
