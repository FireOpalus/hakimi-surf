"""Subtitle-first analysis with a durable, global quota for bounded video uploads."""

import asyncio
import copy
import inspect
import math
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from .bot_server import untrusted_input
from .security import Rejected, authorize, safe_output
from .video_downloader import VideoDownloader
from .video_policy import VideoWatchConfig

SCHEMA = """
CREATE TABLE IF NOT EXISTS video_watch_attempts (
 id TEXT PRIMARY KEY, day TEXT NOT NULL, bvid TEXT NOT NULL, subject TEXT NOT NULL,
 status TEXT NOT NULL, reason TEXT, created REAL NOT NULL,
 duration REAL, bytes INTEGER
);
CREATE INDEX IF NOT EXISTS video_watch_day ON video_watch_attempts(day);
"""

REASONS = {
    "disabled": "原片看片未开启",
    "daily_limit": "今日原片次数已用完",
    "too_long": "视频超过本地时长上限",
    "busy": "另一个原片任务正在处理",
    "cleanup_pending": "先前上传的文件正在等待清理",
    "download_failed": "原片下载不可用或不符合大小／格式限制",
    "risk": "平台风控或凭证过期，已停止访问并冷却",
    "files_unavailable": "视频上传或处理暂不可用",
    "deadline": "原片处理超时",
}


@dataclass(frozen=True)
class VideoAnalysis:
    text: str
    evidence: str
    mode: str
    reason: str | None = None


def explicitly_visual(text: str) -> bool:
    # Only the top-level user text is supplied, never card/forward/source metadata.
    if re.search(r"(?:不要|不用|无需|不必|别).{0,8}(?:画面|镜头|声音|音频|音乐)", text):
        return False
    return bool(
        re.search(
            r"(?:看|分析|检查|描述|解读).{0,8}(?:画面|镜头|视觉|动作)"
            r"|(?:听|分析|辨认).{0,8}(?:声音|音频|音轨|音乐|对白)"
            r"|(?:^|\s)/watch-video(?:\s|$)",
            text,
        )
    )


async def guard_call(callback):
    if callback is not None:
        result = callback()
        if inspect.isawaitable(result):
            result = await result
        if result is False:
            raise Rejected("video_watch_invalidated")


class VideoWatch:
    def __init__(self, service, *, downloader=None, clock=time.time):
        self.svc, self.s, self.store = service, service.s, service.store
        self.clock = clock
        self.zone = ZoneInfo(self.s.bot_timezone)
        self.env_config = VideoWatchConfig.from_settings(self.s)
        self.config = self.env_config
        self.overridden = False
        self.epoch = 0
        self.init_lock, self.config_lock, self.video_lock = (
            asyncio.Lock(),
            asyncio.Lock(),
            asyncio.Lock(),
        )
        self.initialized = False
        self.downloader = downloader or VideoDownloader()

    async def initialize(self):
        async with self.init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                await self.store.db.executescript(SCHEMA)
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='video_watch'"
                )
                if row:
                    self.config = VideoWatchConfig.model_validate(
                        self.store.vault.open("admin:video_watch", row[0])
                    )
                    self.overridden = True
                # A crash cannot release the day's attempt and cause an automatic replay.
                await self.store.db.execute(
                    "UPDATE video_watch_attempts SET status='uncertain',reason='interrupted' "
                    "WHERE status='working'"
                )
            await self.downloader.cleanup_stale()
            self.initialized = True

    def day(self):
        return datetime.fromtimestamp(self.clock(), self.zone).date().isoformat()

    async def set_config(self, value):
        await self.initialize()
        async with self.config_lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='video_watch'")
                self.config, self.overridden = self.env_config, False
            else:
                value = VideoWatchConfig.model_validate(value)
                await self.store.runtime.save_config("video_watch", value.model_dump())
                self.config, self.overridden = value, True
            self.epoch += 1
        return await self.status()

    async def status(self):
        await self.initialize()
        async with self.store.lock:
            used = (
                await self.store.one(
                    "SELECT COUNT(*) FROM video_watch_attempts WHERE day=?", (self.day(),)
                )
            )[0]
            recent = await self.store.all(
                "SELECT day,bvid,status,reason,created,duration,bytes "
                "FROM video_watch_attempts ORDER BY created DESC LIMIT 10"
            )
            has_files = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gemini_video_files'"
            )
            pending = (
                (await self.store.one("SELECT COUNT(*) FROM gemini_video_files"))[0]
                if has_files
                else 0
            )
        return {
            "config": self.config.model_dump(),
            "environment": self.env_config.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "attempts_today": used,
            "remaining_today": max(0, self.config.daily_limit - used),
            "recent_attempts": [dict(r) for r in recent],
            "remote_cleanup_pending": pending,
        }

    def bounded(self, material, query=""):
        payload = copy.deepcopy(material)
        payload["current_message"] = query[:2000] or "概括这个视频中能够确认的内容。"
        while len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            key = max(
                ("subtitle_excerpt", "description", "current_message"),
                key=lambda k: len(payload.get(k, "")),
            )
            if not payload.get(key):
                raise Rejected("context_too_large")
            payload[key] = payload[key][: len(payload[key]) // 2]
            if key == "subtitle_excerpt":
                payload["subtitle_complete"] = False
                payload["subtitle_status"] = "truncated"
                payload["evidence"] = "仅阅读简介、字幕节选和评论；字幕已裁剪，没有播放视频流"
        return payload

    async def _identity_guard(self, message, guard, *, check_budget=True):
        if message is not None and not authorize(message, self.s):
            raise Rejected("video_watch_invalidated")
        await guard_call(guard)
        if check_budget and (await self.store.budget_status())["hard_stop"]:
            raise Rejected("budget_hard_stop")

    async def analyze(
        self,
        bvid,
        material,
        subject,
        lane,
        *,
        message=None,
        deep=False,
        force_video=False,
        guard=None,
    ):
        await self.initialize()
        if not re.fullmatch(r"BV[1-9A-HJ-NP-Za-km-z]{10}", bvid):
            raise Rejected("invalid_bvid")
        await self._identity_guard(message, guard)
        direct = (
            (message.command_text if message.command_text is not None else message.text)
            if message is not None
            else ""
        )
        payload = self.bounded(material, direct)

        async def valid_text():
            # Gemini reserves the budget atomically; a request's own reservation is
            # not a reason to discard its already-paid response after dispatch.
            await self._identity_guard(message, guard, check_budget=False)

        # All text is screened before it can cause a download, even without subtitles.
        await self.svc.ai.screen(
            payload, subject, purpose="source", budget_lane=lane, before_call=valid_text
        )
        await self._identity_guard(message, guard)
        needs_video = (
            force_video
            or explicitly_visual(direct)
            or not payload.get("subtitle_excerpt", "").strip()
        )
        reason = None
        if needs_video:
            duration = payload.get("duration")
            if not self.config.enabled:
                reason = "disabled"
            elif self.config.daily_limit == 0:
                reason = "daily_limit"
            elif (
                isinstance(duration, (int, float))
                and not isinstance(duration, bool)
                and math.isfinite(duration)
                and duration > self.config.max_seconds
            ):
                reason = "too_long"
            elif self.video_lock.locked():
                reason = "busy"
            elif (await self.status())["remote_cleanup_pending"] >= 3:
                reason = "cleanup_pending"
            else:
                result, reason = await self._video(
                    bvid, payload, subject, lane, message, deep, guard
                )
                if result is not None:
                    return result
        await self._identity_guard(message, guard)
        text = await self.svc.ai.generate(
            payload,
            subject,
            summary=True,
            deep=deep,
            budget_lane=lane,
            message=message,
            before_call=valid_text,
        )
        await self._identity_guard(message, guard, check_budget=False)
        evidence = payload.get("evidence", "仅依据公开文字资料，未观看视频画面或收听音轨")
        if needs_video and reason:
            evidence += "；" + REASONS.get(reason, "原片不可用") + "，已退回文字资料"
        return VideoAnalysis(text, evidence, "text", reason)

    async def _video(self, bvid, payload, subject, lane, message, deep, guard):
        async with self.video_lock:
            epoch, config = self.epoch, self.config
            watcher = self.svc.bili_watcher
            credentials_epoch = watcher.credentials_epoch

            async def valid(*, check_budget=True):
                if self.epoch != epoch or not self.config.enabled:
                    raise Rejected("video_watch_invalidated")
                if watcher.credentials_epoch != credentials_epoch:
                    raise Rejected("video_watch_invalidated")
                watcher._ensure_current()
                await self._identity_guard(message, guard, check_budget=check_budget)

            await valid()
            key = uuid.uuid4().hex
            async with self.store.transaction():
                count = (
                    await self.store.one(
                        "SELECT COUNT(*) FROM video_watch_attempts WHERE day=?", (self.day(),)
                    )
                )[0]
                if count >= config.daily_limit:
                    return None, "daily_limit"
                await self.store.db.execute(
                    "INSERT INTO video_watch_attempts VALUES (?,?,?,?,'working',NULL,?,NULL,NULL)",
                    (key, self.day(), bvid, subject, self.clock()),
                )
            state, reason = "failed", "download_failed"
            try:
                async with asyncio.timeout(660):
                    cookies = (
                        await watcher.download_credentials(guard=valid)
                        if hasattr(watcher, "download_credentials")
                        else dict(watcher.creds)
                    )
                    async with self.downloader.download(
                        bvid,
                        max_seconds=config.max_seconds,
                        cookies=cookies,
                        proxy=self.s.bili_proxy.get_secret_value(),
                        guard=valid,
                    ) as video:
                        await valid()
                        async with self.store.transaction():
                            await self.store.db.execute(
                                "UPDATE video_watch_attempts SET duration=?,bytes=? WHERE id=?",
                                (video.duration, video.bytes, key),
                            )
                        result = await self.svc.ai.watch_video(
                            video.path,
                            {
                                **payload,
                                "evidence": "已提供低清原片及可用音轨，画面以1fps采样",
                                "analysis_mode": "video_file",
                            },
                            subject,
                            duration=video.duration,
                            budget_lane=lane,
                            message=message,
                            deep=deep,
                            before_call=valid,
                        )
                        await valid(check_budget=False)
                        state, reason = "done", None
                        return VideoAnalysis(
                            result,
                            "已分析低清原片画面与可用音轨；视频按低帧率采样，细小快速动作可能遗漏",
                            "video",
                        ), None
            except asyncio.CancelledError:
                state, reason = "uncertain", "interrupted"
                raise
            except TimeoutError:
                reason = "deadline"
            except Rejected as exc:
                code = str(exc)
                if code in {"video_download_risk", "video_download_cookie_expired"}:
                    reason = "risk"
                    try:
                        await watcher._risk(
                            "bili_cookie_expired"
                            if code.endswith("cookie_expired")
                            else "bili_account_risk"
                        )
                    except Rejected:
                        pass
                elif code.startswith("video_download_"):
                    reason = "download_failed"
                elif code.startswith(("video_file_", "video_processing_")):
                    reason = "files_unavailable"
                else:
                    reason = "request_rejected"
                    raise
            finally:

                async def finish():
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE video_watch_attempts SET status=?,reason=? WHERE id=?",
                            (state, reason, key),
                        )

                await asyncio.shield(finish())
            # Already-screened metadata provides a deterministic graceful fallback. No
            # second model charge or Bili request after risk/upload/download failures.
            text = "标题：" + payload.get("title", bvid)[:150]
            if payload.get("description"):
                text += "\n公开简介摘录：" + payload["description"][:300]
            comments = payload.get("hot_comments_unverified") or []
            if comments and isinstance(comments[0], str):
                text += "\n热评摘录（仅代表评论者观点）：" + comments[0][:120]
            text = re.sub(r"https?://\S+", "[链接已省略]", text)
            text = safe_output(text, self.svc.secrets, limit=800)
            evidence = (
                REASONS.get(reason, "原片处理不可用") + "；未完成视频分析，仅提供已取得的公开文字"
            )
            return VideoAnalysis(text, evidence, "metadata", reason), reason
