"""Zero-LLM read-only Avatar -> Owner Markdown mirror for hakimi-surf."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import stat
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .drive_client import API, FILE_FIELDS, UPLOAD, FolderDrive
from .models import Message
from .security import Rejected, clean_text, configured_secrets

log = logging.getLogger("hakimi")

MAX_MIRROR_BYTES = 131_072
MAX_RECENT_TURNS = 12
MAX_CHRONICLE_ITEMS = 20
MAX_GLOBAL_EVENTS = 12
MAX_ROAMING_EVENTS = 30
DRIVE_LATEST_NAME = "surf-mirror-latest.md"
CREDENTIAL_RE = re.compile(
    r"(?i)(?:(?:api[_ -]?key|password|passwd|token|secret|sessdata|bili_jct|密钥|密码)"
    r"\s*[:=：]\s*\S{6,}|bearer\s+\S+|\bsk-[A-Za-z0-9_-]{16,}|\bAIza[0-9A-Za-z_-]{20,}"
    r"|-----BEGIN [^-]+PRIVATE KEY-----)"
)
ROLE_TAG_RE = re.compile(r"\[role=", re.IGNORECASE)


def sanitize_mirror_text(text: Any, settings: Any, *, limit: int = 2000) -> str:
    """Normalize text, scrub secrets/credentials, and neutralize role/protocol tags."""
    cleaned = clean_text(str(text or ""))
    candidates = {
        val
        for secret in configured_secrets(settings)
        if secret and len(secret) >= 6
        for val in (secret, clean_text(secret))
        if len(val) >= 6
    }
    for secret in sorted(candidates, key=len, reverse=True):
        if secret in cleaned:
            cleaned = cleaned.replace(secret, "[凭证已隐去]")
    cleaned = CREDENTIAL_RE.sub("[凭证已隐去]", cleaned)
    cleaned = ROLE_TAG_RE.sub("[untrusted_role=", cleaned)
    cleaned = (
        cleaned.replace("[CQ:", "［CQ:")
        .replace("@everyone", "＠everyone")
        .replace("@all", "＠all")
        .replace("@全体成员", "＠全体成员")
        .replace("<", "＜")
        .replace(">", "＞")
    )
    if limit > 0 and len(cleaned) > limit:
        cleaned = cleaned[: max(0, limit - 1)].rstrip() + "…"
    return cleaned


def sanitize_mirror_text_post(markdown: str, settings: Any) -> str:
    """Final full-document secret and role-tag scrub without altering valid Markdown headings."""
    cleaned = markdown
    candidates = {
        val
        for secret in configured_secrets(settings)
        if secret and len(secret) >= 6
        for val in (secret, clean_text(secret))
        if len(val) >= 6
    }
    for secret in sorted(candidates, key=len, reverse=True):
        if secret in cleaned:
            cleaned = cleaned.replace(secret, "[凭证已隐去]")
    cleaned = CREDENTIAL_RE.sub("[凭证已隐去]", cleaned)
    return ROLE_TAG_RE.sub("[untrusted_role=", cleaned)


def _cap_markdown_bytes(markdown: str, max_bytes: int = MAX_MIRROR_BYTES) -> str:
    encoded = markdown.encode("utf-8")
    if len(encoded) <= max_bytes:
        return markdown
    notice = "\n\n> （内容超出 128 KiB 镜像上限，已截断尾部记录）\n"
    budget = max(0, max_bytes - len(notice.encode("utf-8")))
    lines = markdown.splitlines(keepends=True)
    kept: list[str] = []
    used = 0
    for line in lines:
        line_bytes = len(line.encode("utf-8"))
        if used + line_bytes > budget:
            break
        kept.append(line)
        used += line_bytes
    if not kept and budget > 0:
        partial = encoded[:budget].decode("utf-8", errors="ignore")
        return (partial.rstrip() + notice)[:max_bytes]
    return "".join(kept).rstrip() + notice


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ensure_dir_no_symlink(path: Path) -> None:
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise Rejected("surf_mirror_symlink_forbidden")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _atomic_write_text(path: Path, content: str) -> None:
    _ensure_dir_no_symlink(path.parent)
    if path.is_symlink():
        raise Rejected("surf_mirror_symlink_forbidden")
    fd, tmp_name = tempfile.mkstemp(prefix=".mirror-", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _read_state_sync(state_path: Path) -> dict[str, Any]:
    if not state_path.exists() or state_path.is_symlink() or not state_path.is_file():
        return {"local_digests": {}, "drive_digests": {}, "drive_file_ids": {}}
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"local_digests": {}, "drive_digests": {}, "drive_file_ids": {}}
    if not isinstance(data, dict):
        return {"local_digests": {}, "drive_digests": {}, "drive_file_ids": {}}
    return {
        "local_digests": dict(data.get("local_digests") or {}),
        "drive_digests": dict(data.get("drive_digests") or {}),
        "drive_file_ids": dict(data.get("drive_file_ids") or {}),
    }


def _write_mirror_outputs_sync(
    mirror_dir: Path,
    day: str,
    latest_md: str,
    daily_md: str,
    latest_digest: str,
    daily_digest: str,
) -> dict[str, Any]:
    _ensure_dir_no_symlink(mirror_dir)
    daily_dir = mirror_dir / "daily"
    _ensure_dir_no_symlink(daily_dir)
    latest_path = mirror_dir / "latest.md"
    daily_path = daily_dir / f"{day}.md"
    state_path = mirror_dir / ".mirror_state.json"

    state = _read_state_sync(state_path)
    local_digests = state["local_digests"]
    latest_changed = local_digests.get("latest") != latest_digest or not latest_path.is_file()
    daily_changed = local_digests.get(day) != daily_digest or not daily_path.is_file()

    if latest_changed:
        _atomic_write_text(latest_path, latest_md)
        local_digests["latest"] = latest_digest
    if daily_changed:
        _atomic_write_text(daily_path, daily_md)
        local_digests[day] = daily_digest

    if latest_changed or daily_changed or not state_path.is_file():
        state["local_digests"] = local_digests
        _atomic_write_text(state_path, json.dumps(state, ensure_ascii=False, indent=2) + "\n")

    return {
        "latest_path": str(latest_path),
        "daily_path": str(daily_path),
        "local_changed": bool(latest_changed or daily_changed),
        "state": state,
    }


def _save_state_sync(mirror_dir: Path, state: dict[str, Any]) -> None:
    _ensure_dir_no_symlink(mirror_dir)
    state_path = mirror_dir / ".mirror_state.json"
    _atomic_write_text(state_path, json.dumps(state, ensure_ascii=False, indent=2) + "\n")


def _read_owner_session_sync(
    sessions_root: Path,
    db_path: Path,
    key: str,
    vault: Any,
    owner_qq_id: str,
    now: float,
) -> dict[str, Any]:
    empty: dict[str, Any] = {
        "enabled": False,
        "rolling_summary": "",
        "recent_turns": [],
        "profiles": [],
        "chronicles": [],
    }
    for candidate in (sessions_root, db_path.parent, db_path):
        if candidate.is_symlink():
            return empty
    if not db_path.exists() or not db_path.is_file():
        return empty
    try:
        st = db_path.stat(follow_symlinks=False)
    except OSError:
        return empty
    if not stat.S_ISREG(st.st_mode):
        return empty

    conn = sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "session_meta" not in tables:
            return empty
        meta = conn.execute("SELECT enabled FROM session_meta").fetchone()
        if not meta or not meta["enabled"]:
            return empty

        rolling_summary = ""
        if "rolling_state" in tables:
            row = conn.execute("SELECT summary, expires FROM rolling_state").fetchone()
            if row and row["summary"] is not None:
                exp = row["expires"] or 0
                if exp == 0 or exp > now:
                    try:
                        rolling_summary = str(vault.open(key, row["summary"]) or "")
                    except Exception:
                        rolling_summary = ""

        recent_messages: list[dict[str, Any]] = []
        if "recent_messages" in tables:
            rows = conn.execute(
                "SELECT id, payload FROM recent_messages "
                "WHERE (expires = 0 OR expires > ?) ORDER BY id DESC LIMIT ?",
                (now, MAX_RECENT_TURNS * 2),
            ).fetchall()
            for row in reversed(rows):
                try:
                    item = vault.open(key, row["payload"])
                    if isinstance(item, dict):
                        recent_messages.append(item)
                except Exception:
                    continue

        turns: list[list[dict[str, Any]]] = []
        for item in recent_messages:
            role = item.get("role")
            if role == "user" or not turns:
                turns.append([item])
            else:
                turns[-1].append(item)
        recent_turns = turns[-MAX_RECENT_TURNS:]

        profiles: list[dict[str, Any]] = []
        if "member_profiles" in tables:
            p_rows = conn.execute(
                "SELECT member_id, slot, payload, updated FROM member_profiles "
                "WHERE member_id=? AND deleted=0 AND (expires=0 OR expires>?) "
                "ORDER BY updated DESC LIMIT 16",
                (owner_qq_id, now),
            ).fetchall()
            for row in p_rows:
                ident = f"p:{row['member_id']}:{row['slot']}"
                try:
                    data = vault.open(key + ":" + ident, row["payload"])
                    if isinstance(data, dict) and data.get("text"):
                        profiles.append(
                            {
                                "slot": row["slot"],
                                "text": str(data["text"]),
                                "updated": row["updated"],
                            }
                        )
                except Exception:
                    continue

        chronicles: list[dict[str, Any]] = []
        if "chronicle_events" in tables:
            c_rows = conn.execute(
                "SELECT id, payload, heat, updated FROM chronicle_events "
                "WHERE deleted=0 ORDER BY heat DESC, updated DESC LIMIT ?",
                (MAX_CHRONICLE_ITEMS,),
            ).fetchall()
            for row in c_rows:
                try:
                    data = vault.open(key + ":e:" + row["id"], row["payload"])
                    if isinstance(data, dict) and data.get("text"):
                        chronicles.append(
                            {
                                "topic": str(data.get("topic", "")),
                                "category": str(data.get("category", "")),
                                "text": str(data["text"]),
                                "tags": list(data.get("tags") or []),
                                "heat": row["heat"],
                            }
                        )
                except Exception:
                    continue

        return {
            "enabled": True,
            "rolling_summary": rolling_summary,
            "recent_turns": recent_turns,
            "profiles": profiles,
            "chronicles": chronicles,
        }
    finally:
        conn.close()


def _parse_json_safe(raw: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw}


class SurfMirror:
    """Periodic read-only Avatar -> Owner Markdown snapshot & Drive uploader."""

    def __init__(
        self,
        service: Any,
        drive: FolderDrive | None = None,
        *,
        interval_seconds: int = 1800,
    ):
        self.svc = service
        self.s = service.s
        self.store = service.store
        self.sessions = service.sessions
        self.drive = drive
        self.interval_seconds = interval_seconds
        self.zone = ZoneInfo(self.s.bot_timezone)
        self._lock = asyncio.Lock()

    def _format_ts(self, ts: Any) -> str:
        if not isinstance(ts, (int, float)) or isinstance(ts, bool) or ts <= 0:
            return ""
        try:
            return datetime.fromtimestamp(float(ts), self.zone).strftime("%m-%d %H:%M")
        except (OSError, OverflowError, ValueError):
            return ""

    async def _read_owner_session(self, now: float) -> dict[str, Any]:
        empty: dict[str, Any] = {
            "enabled": False,
            "rolling_summary": "",
            "recent_turns": [],
            "profiles": [],
            "chronicles": [],
        }
        owner_id = str(getattr(self.s, "owner_qq_id", "") or "")
        bot_id = str(getattr(self.s, "onebot_self_id", "") or "")
        if not owner_id or not bot_id or self.sessions is None:
            return empty
        owner_msg = Message(
            platform="onebot",
            bot_id=bot_id,
            event_id="surf-mirror-ro",
            user_id=owner_id,
            kind="private",
            conversation_id=owner_id,
            text="",
            addressed=True,
            created_at=now,
        )
        key = self.sessions.key(owner_msg)
        db_path = self.sessions.path(owner_msg)
        lock = self.sessions.locks[int(key[:8], 16) % len(self.sessions.locks)]
        async with lock:
            return await asyncio.to_thread(
                _read_owner_session_sync,
                self.sessions.root,
                db_path,
                key,
                self.sessions.vault,
                owner_id,
                now,
            )

    async def _read_store_world_and_roaming(self, day: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "world_state": None,
            "global_events": [],
            "roaming_counts": {},
            "roaming_events": [],
        }
        if self.store is None or getattr(self.store, "db", None) is None:
            return result

        async with self.store.lock:
            tables = {
                row[0]
                for row in await self.store.all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "world_state" in tables:
                row = await self.store.one("SELECT * FROM world_state WHERE singleton=1")
                if row is not None:
                    data = dict(row)
                    data["snapshot"] = _parse_json_safe(data.get("snapshot_json"))
                    result["world_state"] = data

            if "global_events" in tables:
                rows = await self.store.all(
                    "SELECT * FROM global_events ORDER BY created_at DESC LIMIT ?",
                    (MAX_GLOBAL_EVENTS,),
                )
                events = []
                for row in rows:
                    item = dict(row)
                    item["payload"] = _parse_json_safe(item.get("payload_json"))
                    events.append(item)
                result["global_events"] = events

            if "roaming_counts" in tables:
                c_rows = await self.store.all(
                    "SELECT kind, n FROM roaming_counts WHERE day=?", (day,)
                )
                result["roaming_counts"] = {str(r[0]): int(r[1]) for r in c_rows}

            if "roaming_events" in tables:
                e_rows = await self.store.all(
                    "SELECT * FROM roaming_events WHERE day=? ORDER BY rowid DESC LIMIT ?",
                    (day, MAX_ROAMING_EVENTS),
                )
                roaming_events = []
                for row in reversed(e_rows):
                    item = dict(row)
                    try:
                        text = self.store.vault.open(item["id"], item["payload"])
                    except Exception:
                        continue
                    roaming_events.append(
                        {
                            "event_kind": str(item.get("event_kind") or "patrol"),
                            "text": str(text),
                        }
                    )
                result["roaming_events"] = roaming_events
        return result

    async def _read_recent_surfed(self, now: float) -> list[dict[str, Any]]:
        watcher = getattr(self.svc, "bili_watcher", None)
        if watcher is None or not hasattr(watcher, "recent_surfed"):
            return []
        try:
            items = await watcher.recent_surfed(limit=6, now=now)
        except TypeError:
            items = await watcher.recent_surfed(limit=6)
        except Exception:
            return []
        return list(items or [])

    def _render_markdown(
        self,
        *,
        title: str,
        day: str,
        owner_data: dict[str, Any],
        store_data: dict[str, Any],
        surfed: list[dict[str, Any]],
    ) -> str:
        tz_label = sanitize_mirror_text(self.s.bot_timezone, self.s, limit=64)
        lines = [
            f"# {title}",
            "",
            f"镜像日期：{day} | 时区：{tz_label}",
            "",
            "## 世界状态看板",
            "",
        ]

        world = store_data.get("world_state")
        if world:
            mod = sanitize_mirror_text(world.get("current_module", "idle"), self.s, limit=64)
            focus = sanitize_mirror_text(world.get("focus_level", "LOW"), self.s, limit=32)
            mood = sanitize_mirror_text(world.get("mood", "calm"), self.s, limit=64)
            energy = world.get("energy", "-")
            boredom = world.get("boredom", "-")
            updated = self._format_ts(world.get("updated_at"))
            lines.append(
                f"- 当前模块：`{mod}` | 专注度：`{focus}` | 心情：`{mood}` | "
                f"精力：{energy} | 无聊度：{boredom}" + (f" | 更新于：{updated}" if updated else "")
            )
            snapshot = world.get("snapshot")
            if isinstance(snapshot, dict) and snapshot:
                snap_text = sanitize_mirror_text(
                    json.dumps(snapshot, ensure_ascii=False), self.s, limit=600
                )
                lines.append(f"- 状态快照：{snap_text}")
        else:
            lines.append("- 当前暂无世界状态记录（处于基础待机模式）。")

        global_events = store_data.get("global_events") or []
        if global_events:
            lines.append("- 近期全局事件：")
            for ev in global_events:
                ts_label = self._format_ts(ev.get("created_at"))
                src = sanitize_mirror_text(ev.get("source_module", ""), self.s, limit=48)
                etype = sanitize_mirror_text(ev.get("event_type", ""), self.s, limit=48)
                imp = sanitize_mirror_text(ev.get("importance", "normal"), self.s, limit=24)
                payload_obj = ev.get("payload")
                payload_str = (
                    sanitize_mirror_text(
                        json.dumps(payload_obj, ensure_ascii=False), self.s, limit=300
                    )
                    if payload_obj
                    else ""
                )
                prefix = f"[{ts_label}] " if ts_label else ""
                suffix = f"：{payload_str}" if payload_str else ""
                lines.append(f"  - {prefix}[{imp}] `{src}` / `{etype}`{suffix}")
        lines.append("")

        lines += ["## 近期与 BOISER 的私聊记忆与对话摘要", ""]
        if not owner_data.get("enabled"):
            lines.append("- 暂无活跃私聊记忆或未开启会话记忆。")
        else:
            summary = sanitize_mirror_text(
                owner_data.get("rolling_summary", ""), self.s, limit=1800
            )
            if summary:
                lines += ["### 滚动摘要", "", *("> " + ln for ln in summary.splitlines()), ""]

            profiles = owner_data.get("profiles") or []
            if profiles:
                lines += ["### 主人双轨画像", ""]
                for prof in profiles:
                    slot = sanitize_mirror_text(prof.get("slot", ""), self.s, limit=32)
                    text = sanitize_mirror_text(prof.get("text", ""), self.s, limit=400)
                    lines.append(f"- `{slot}`：{text}")
                lines.append("")

            chronicles = owner_data.get("chronicles") or []
            if chronicles:
                lines += ["### 纪事记忆", ""]
                for ch in chronicles:
                    topic = sanitize_mirror_text(ch.get("topic", ""), self.s, limit=60)
                    cat = sanitize_mirror_text(ch.get("category", ""), self.s, limit=32)
                    text = sanitize_mirror_text(ch.get("text", ""), self.s, limit=600)
                    tags = " ".join(
                        "#" + sanitize_mirror_text(t, self.s, limit=32)
                        for t in (ch.get("tags") or [])[:6]
                    )
                    meta = f"[{cat}] {topic}".strip()
                    tag_suffix = " " + tags if tags else ""
                    lines.append(f"- **{meta}**{tag_suffix}：{text}")
                lines.append("")

            turns = owner_data.get("recent_turns") or []
            if turns:
                lines += [f"### 近期私聊片段（最近 {len(turns)} 轮）", ""]
                for turn in turns:
                    for msg in turn:
                        role = msg.get("role", "")
                        default_speaker = "BOISER" if role == "user" else "哈基米"
                        speaker = sanitize_mirror_text(
                            msg.get("speaker") or default_speaker, self.s, limit=40
                        )
                        ts_label = self._format_ts(msg.get("created_at"))
                        body = sanitize_mirror_text(msg.get("text", ""), self.s, limit=800).replace(
                            "\n", " "
                        )
                        if not body:
                            continue
                        head = f"[{ts_label}] " if ts_label else ""
                        lines.append(f"- {head}**{speaker}**：{body}")
                lines.append("")
            elif not (summary or profiles or chronicles):
                lines.append("- 会话记忆已开启，当前暂无近期对话记录。")
        lines.append("")

        lines += ["## 今日冲浪与生活轨迹", ""]
        counts = store_data.get("roaming_counts") or {}
        if counts:
            parts = [
                f"{sanitize_mirror_text(k, self.s, limit=32)}={v}"
                for k, v in sorted(counts.items())
            ]
            lines.append(f"- 今日活动计数：{', '.join(parts)}")

        roaming_events = store_data.get("roaming_events") or []
        if roaming_events:
            lines.append("- 今日漫游与互动日志：")
            for ev in roaming_events:
                kind = sanitize_mirror_text(ev.get("event_kind", "patrol"), self.s, limit=32)
                text = sanitize_mirror_text(ev.get("text", ""), self.s, limit=600)
                if text:
                    lines.append(f"  - [{kind}] {text}")
        elif not counts:
            lines.append("- 今日暂无漫游日志记录。")

        if surfed:
            lines.append("- 近期 B 站冲浪记录：")
            for item in surfed:
                bvid = sanitize_mirror_text(item.get("bvid", ""), self.s, limit=32)
                watched_at = sanitize_mirror_text(item.get("watched_at", ""), self.s, limit=32)
                notes = sanitize_mirror_text(item.get("notes", ""), self.s, limit=320)
                actions = item.get("actions") or []
                clean_actions = [sanitize_mirror_text(a, self.s, limit=24) for a in actions]
                act_str = " [" + ", ".join(clean_actions) + "]" if clean_actions else ""
                lines.append(f"  - `{bvid}` ({watched_at}){act_str}：{notes}")
        else:
            lines.append("- 近期暂无 B 站主动冲浪笔记。")
        lines.append("")

        raw_md = "\n".join(lines)
        raw_md = sanitize_mirror_text_post(raw_md, self.s)
        return _cap_markdown_bytes(raw_md, MAX_MIRROR_BYTES)

    def _drive_marker(self) -> str:
        return self.store.vault.digest("surf-mirror", self.s.namespace)

    def _validate_drive_file(self, item: dict[str, Any], filename: str, marker: str) -> str:
        file_id = str(item.get("id", ""))
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", file_id)
            or item.get("parents") != [self.drive.folder]
            or item.get("trashed", False)
            or item.get("name") != filename
            or item.get("mimeType") != "text/markdown"
            or (item.get("appProperties") or {}).get("hakimi_surf_mirror") != marker
        ):
            raise Rejected("drive_file_outside_scope")
        return file_id

    async def _drive_json(self, method: str, url: str, token: str, **kwargs: Any) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {token}", **kwargs.pop("headers", {})}
        async with self.drive.http.stream(
            method, url, headers=headers, follow_redirects=False, **kwargs
        ) as response:
            if response.status_code not in {200, 201}:
                raise Rejected("drive_request_failed")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 65536:
                    raise Rejected("drive_response_too_large")
            value = json.loads(body)
            if not isinstance(value, dict):
                raise Rejected("drive_invalid_response")
            return value

    async def _upload_drive_file(
        self, filename: str, markdown: str, marker: str, token: str
    ) -> str:
        if len(markdown.encode("utf-8")) > MAX_MIRROR_BYTES:
            raise Rejected("surf_mirror_too_large")
        listing = await self._drive_json(
            "GET",
            API,
            token,
            params={
                "q": (
                    f"'{self.drive.folder}' in parents and trashed = false "
                    f"and name = '{filename}' "
                    f"and appProperties has {{ key='hakimi_surf_mirror' and value='{marker}' }}"
                ),
                "fields": f"nextPageToken,incompleteSearch,files({FILE_FIELDS})",
                "pageSize": "10",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                "corpora": "user",
            },
        )
        files = listing.get("files")
        if (
            not isinstance(files, list)
            or len(files) > 1
            or listing.get("nextPageToken")
            or listing.get("incompleteSearch")
        ):
            raise Rejected("drive_ambiguous_file")

        if files:
            file_id = self._validate_drive_file(files[0], filename, marker)
            result = await self._drive_json(
                "PATCH",
                f"{UPLOAD}/{file_id}",
                token,
                params={"uploadType": "media", "supportsAllDrives": "true", "fields": FILE_FIELDS},
                headers={"Content-Type": "text/markdown; charset=UTF-8"},
                content=markdown.encode("utf-8"),
            )
        else:
            metadata = {
                "name": filename,
                "mimeType": "text/markdown",
                "parents": [self.drive.folder],
                "appProperties": {"hakimi_surf_mirror": marker},
            }
            boundary = "hakimi_" + uuid.uuid4().hex
            body = (
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                + json.dumps(metadata)
                + f"\r\n--{boundary}\r\nContent-Type: text/markdown; charset=UTF-8\r\n\r\n"
                + markdown
                + f"\r\n--{boundary}--\r\n"
            ).encode("utf-8")
            result = await self._drive_json(
                "POST",
                UPLOAD,
                token,
                params={
                    "uploadType": "multipart",
                    "supportsAllDrives": "true",
                    "fields": FILE_FIELDS,
                },
                headers={"Content-Type": f"multipart/related; boundary={boundary}"},
                content=body,
            )
        return self._validate_drive_file(result, filename, marker)

    async def export_once(
        self, now: float | None = None, upload_drive: bool = True
    ) -> dict[str, Any]:
        async with self._lock:
            current = time.time() if now is None else float(now)
            day = datetime.fromtimestamp(current, self.zone).date().isoformat()
            owner_data, store_data, surfed = await asyncio.gather(
                self._read_owner_session(current),
                self._read_store_world_and_roaming(day),
                self._read_recent_surfed(current),
            )
            latest_md = self._render_markdown(
                title="🐾 哈基米分身实时镜像 (hakimi-surf Mirror)",
                day=day,
                owner_data=owner_data,
                store_data=store_data,
                surfed=surfed,
            )
            daily_md = self._render_markdown(
                title=f"🐾 哈基米分身每日镜像 · {day}",
                day=day,
                owner_data=owner_data,
                store_data=store_data,
                surfed=surfed,
            )
            latest_digest = _sha256_text(latest_md)
            daily_digest = _sha256_text(daily_md)

            mirror_dir = Path(self.s.data_dir) / "surf_mirror"
            write_res = await asyncio.to_thread(
                _write_mirror_outputs_sync,
                mirror_dir,
                day,
                latest_md,
                daily_md,
                latest_digest,
                daily_digest,
            )
            state = write_res["state"]
            drive_digests = state["drive_digests"]
            drive_file_ids = state["drive_file_ids"]

            need_latest = drive_digests.get("latest") != latest_digest
            need_daily = drive_digests.get(day) != daily_digest
            uploaded: list[str] = []
            skipped_drive = True

            if (
                upload_drive
                and self.drive is not None
                and self.s.roaming_enabled
                and (need_latest or need_daily)
            ):
                skipped_drive = False
                marker = self._drive_marker()
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", marker):
                    raise Rejected("surf_mirror_marker_invalid")
                await self.drive.validate_folder()
                token = await self.drive._access_token()
                daily_drive_name = f"surf-mirror-{day}.md"
                if need_latest:
                    fid = await self._upload_drive_file(DRIVE_LATEST_NAME, latest_md, marker, token)
                    drive_digests["latest"] = latest_digest
                    drive_file_ids[DRIVE_LATEST_NAME] = fid
                    uploaded.append(DRIVE_LATEST_NAME)
                if need_daily:
                    fid = await self._upload_drive_file(daily_drive_name, daily_md, marker, token)
                    drive_digests[day] = daily_digest
                    drive_file_ids[daily_drive_name] = fid
                    uploaded.append(daily_drive_name)
                if uploaded:
                    state["drive_digests"] = drive_digests
                    state["drive_file_ids"] = drive_file_ids
                    await asyncio.to_thread(_save_state_sync, mirror_dir, state)

            return {
                "day": day,
                "latest_path": write_res["latest_path"],
                "daily_path": write_res["daily_path"],
                "latest_digest": latest_digest,
                "daily_digest": daily_digest,
                "local_changed": write_res["local_changed"],
                "uploaded": uploaded,
                "skipped_drive": skipped_drive,
            }

    async def run(self) -> None:
        while True:
            try:
                await self.export_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.info("event=surf_mirror_error")
            await asyncio.sleep(self.interval_seconds)
