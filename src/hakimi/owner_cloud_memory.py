"""Read-only owner memory loader & zero-LLM local recall for Antigravity_Memory."""

from __future__ import annotations

import json
import os
import re
import stat
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .bot_server import is_persona_owner
from .reply_style import trim_complete_text
from .security import clean_text, configured_secrets

MAX_FILE_BYTES = 262_144
MAX_PINNED_CHARS = 1800
MAX_RECALLED_CHARS = 1800
MAX_RECENT_CORE_TURNS = 4

BROAD_RECALL_RE = re.compile(
    r"记得|忘|想起来|以前|之前|过去|从前|当初|相遇|约定|契约|小本本|回忆|记忆"
)
CREDENTIAL_RE = re.compile(
    r"(?i)(?:(?:api[_ -]?key|password|passwd|token|secret|sessdata|bili_jct|密钥|密码)"
    r"\s*[:=：]\s*\S{6,}|bearer\s+\S+|\bsk-[A-Za-z0-9_-]{16,}|\bAIza[0-9A-Za-z_-]{20,})"
)
ROLE_TAG_RE = re.compile(r"\[role=", re.IGNORECASE)
CJK_STOP_CLASS = (
    "的了吗呢吧啊哦呀是我你他她它这那在有就都也还说要会想看"
    "很把被让给和与或但去来着过好吗嘛哈喵呜么啥怎"
)
CJK_SPLIT_RE = re.compile(rf"[{CJK_STOP_CLASS}]+")
CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]+")
ASCII_TOKEN_RE = re.compile(r"[a-z0-9_]{2,}")


def _sanitize(text: str, settings) -> str:
    cleaned = clean_text(str(text or ""))
    for secret in configured_secrets(settings):
        if secret and len(secret) >= 6:
            cleaned = cleaned.replace(secret, "[凭证已隐去]")
    cleaned = CREDENTIAL_RE.sub("[凭证已隐去]", cleaned)
    return ROLE_TAG_RE.sub("[untrusted_role=", cleaned)


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


class OwnerCloudMemory:
    """Thread-safe, mtime-cached read-only memory & local search for private Owner chats."""

    def __init__(self, settings):
        self.s = settings
        self._lock = threading.Lock()
        self._signature: tuple[tuple[str, int, int], ...] = ()
        self._pinned = ""
        self._recent_core: list[dict[str, str]] = []
        self._chunks: list[dict[str, Any]] = []
        self._index_chunks: list[dict[str, Any]] = []
        self._latest_ts: float = 0.0
        self._latest_iso: str = ""
        self._latest_tag: str = ""

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
                                parsed = datetime.strptime(
                                    raw_ts, "%Y-%m-%d %H:%M:%S"
                                ).replace(tzinfo=bot_tz)
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

        self._signature = sig
        self._pinned = pinned
        self._recent_core = recent_core
        self._chunks = chunks
        self._index_chunks = index_chunks
        self._latest_ts = latest_ts
        self._latest_iso = latest_iso
        self._latest_tag = latest_tag

    def _search_chunks(self, query_text: str, *, recall: bool) -> list[dict[str, str]]:
        if not self._chunks:
            return []
        q_tokens, q_strong, q_phrases = _tokenize(query_text)
        broad = bool(BROAD_RECALL_RE.search(query_text))
        if not q_tokens and not recall and not broad:
            return []

        scored: list[tuple[float, int, dict[str, Any]]] = []
        for idx, chunk in enumerate(self._chunks):
            overlap = q_tokens & chunk["tokens"]
            if not overlap and not broad:
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

    def for_message(self, message, query_text: str = "", *, recall: bool = False) -> dict[str, Any]:
        if (
            not self.s.enable_antigravity_memory
            or message is None
            or message.kind != "private"
            or not is_persona_owner(message, self.s)
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
            recalled = self._search_chunks(query_text, recall=recall)
            if recalled:
                result["owner_recalled_memories"] = recalled
            return result

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
            }
