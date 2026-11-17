"""Subtitle-first analysis with a durable, global quota for bounded video uploads."""

import asyncio
import base64
import copy
import hashlib
import inspect
import math
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from .bot_server import untrusted_input
from .media import ModelImage
from .security import Rejected, authorize
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

LOCAL_DOWNLOAD_REASONS = (
    "download_failed",
    "download_format",
    "download_size",
    "download_restricted",
    "download_network",
)
DOWNLOAD_CODE_MAP = {
    "video_download_format": "download_format",
    "video_download_size": "download_size",
    "video_download_restricted": "download_restricted",
    "video_download_network_forbidden": "download_network",
}
LANE2_FALLBACK_CODES = frozenset(
    {
        "video_download_restricted",
        "video_download_unavailable",
        "video_download_timeout",
        "video_download_network_forbidden",
    }
)
MAX_STORYBOARD_BYTES = 262_144
STORYBOARD_NOTE = "原片由上海节点本地提取 3x3 关键帧拼图回传（未传输大体积原片）"
LOCAL_FAILURE_SQL = (
    "NOT (status='failed' AND reason IN ('" + "','".join(LOCAL_DOWNLOAD_REASONS) + "'))"
)
REASONS = {
    "disabled": "原片看片未开启",
    "daily_limit": "今日原片次数已用完",
    "too_long": "视频超过本地时长上限",
    "busy": "另一个原片任务正在处理",
    "cleanup_pending": "先前上传的文件正在等待清理",
    "download_failed": "原片下载暂不可用",
    "download_format": "视频格式或分辨率不支持",
    "download_size": "原片体积超过 80 MiB 上限",
    "download_restricted": "视频受限或需付费解锁",
    "download_network": "视频 CDN 地址未通过安全校验",
    "risk": "平台风控或凭证过期，已停止访问并冷却",
    "files_unavailable": "视频上传或处理暂不可用",
    "deadline": "原片处理超时",
    "interrupted": "服务重启中断，不自动重放",
}


@dataclass(frozen=True)
class VideoAnalysis:
    text: str
    material_scope: str
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
                    "SELECT COUNT(*) FROM video_watch_attempts "
                    f"WHERE day=? AND {LOCAL_FAILURE_SQL}",
                    (self.day(),),
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
            "reasons": REASONS,
            "remote_cleanup_pending": pending,
        }

    def bounded(self, material, query="", *, patrol=False):
        payload = copy.deepcopy(material)
        default_prompt = (
            "用哈基米逛B站的冲浪笔记口吻记下：①正片核心亮点；"
            "②若 hot_comments_unverified 有神评或烂梗顺手记一笔（注明评论区玩梗）；"
            "③一句猫猫自己的主观吐槽或锐评。"
            if patrol
            else "概括这个视频中能够确认的内容。"
        )
        payload["current_message"] = query[:2000] or default_prompt
        while len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            key = max(
                ("subtitle_excerpt", "description", "current_message"),
                key=lambda k: len(payload.get(k, "")),
            )
            if not payload.get(key):
                if "patrol_deja_vu" in payload:
                    payload.pop("patrol_deja_vu", None)
                    continue
                raise Rejected("context_too_large")
            payload[key] = payload[key][: len(payload[key]) // 2]
            if key == "subtitle_excerpt":
                payload["subtitle_complete"] = False
                payload["subtitle_status"] = "truncated"
                payload["material_scope"] = "partial_subtitle" if payload[key] else "metadata_only"
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
        payload = self.bounded(material, direct, patrol=(lane == "patrol"))

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
                try:
                    result, reason = await self._video(
                        bvid, payload, subject, lane, message, deep, guard
                    )
                except Rejected as exc:
                    if str(exc) != "bili_account_cooling_down":
                        raise
                    result, reason = None, "risk"
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
        return VideoAnalysis(text, payload.get("material_scope", "metadata_only"), "text", reason)

    async def _try_cn_storyboard(
        self, bvid, payload, subject, lane, message, deep, config, cookies, key, valid
    ) -> VideoAnalysis | None:
        nt = getattr(self.svc, "node_transport", None)
        if not (
            nt is not None
            and nt.connected()
            and getattr(self.s, "node_bili_delegate_enabled", True)
        ):
            return None
        await valid()
        req_cookies = {k: v for k, v in cookies.items() if v} if isinstance(cookies, dict) else {}
        try:
            sb = await nt.call(
                "bili.video_storyboard",
                {"bvid": bvid, "max_seconds": config.max_seconds, "cookies": req_cookies},
                timeout=120.0,
            )
        except (Rejected, TimeoutError, ValueError):
            return None
        await valid()
        if not isinstance(sb, dict) or sb.get("mime_type") != "image/jpeg":
            return None
        data_b64 = sb.get("data_b64")
        if not isinstance(data_b64, str) or not data_b64:
            return None
        try:
            raw_bytes = base64.b64decode(data_b64, validate=True)
        except ValueError:
            return None
        if not (1 <= len(raw_bytes) <= MAX_STORYBOARD_BYTES):
            return None
        raw_dur = sb.get("duration", payload.get("duration"))
        sb_duration = (
            float(raw_dur)
            if isinstance(raw_dur, (int, float))
            and not isinstance(raw_dur, bool)
            and math.isfinite(raw_dur)
            and raw_dur > 0
            else None
        )
        storyboard_img = ModelImage(
            data=base64.b64encode(raw_bytes).decode("ascii"),
            mime_type="image/jpeg",
            digest=hashlib.sha256(raw_bytes).hexdigest(),
            resolution="high",
        )
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE video_watch_attempts SET duration=?,bytes=? WHERE id=?",
                (sb_duration, len(raw_bytes), key),
            )
        await self.svc.ai.screen(
            {"storyboard_bvid": bvid},
            subject,
            purpose="source",
            budget_lane=lane,
            images=[storyboard_img.gate()],
            before_call=valid,
        )
        result = await self.svc.ai.generate(
            {
                **payload,
                "analysis_mode": "cn_keyframe_storyboard",
                "storyboard_note": STORYBOARD_NOTE,
            },
            subject,
            summary=True,
            deep=deep,
            budget_lane=lane,
            message=message,
            images=[storyboard_img],
            before_call=valid,
        )
        await valid(check_budget=False)
        return VideoAnalysis(
            result,
            payload.get("material_scope", "metadata_only"),
            "cn_storyboard",
        )

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
                        "SELECT COUNT(*) FROM video_watch_attempts "
                        f"WHERE day=? AND {LOCAL_FAILURE_SQL}",
                        (self.day(),),
                    )
                )[0]
                total_attempts = (
                    await self.store.one(
                        "SELECT COUNT(*) FROM video_watch_attempts WHERE day=?",
                        (self.day(),),
                    )
                )[0]
                if count >= config.daily_limit or total_attempts >= max(config.daily_limit * 4, 12):
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
                    try:
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
                                payload.get("material_scope", "metadata_only"),
                                "video",
                            ), None
                    except Rejected as lane1_exc:
                        if str(lane1_exc) not in LANE2_FALLBACK_CODES:
                            raise
                        analysis = await self._try_cn_storyboard(
                            bvid,
                            payload,
                            subject,
                            lane,
                            message,
                            deep,
                            config,
                            cookies,
                            key,
                            valid,
                        )
                        if analysis is None:
                            raise lane1_exc from None
                        state, reason = "done", None
                        return analysis, None
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
                    reason = DOWNLOAD_CODE_MAP.get(code, "download_failed")
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
            # Reuse only already-screened material. Bili remains in cooldown; the outer
            # text generation still checks identity, revocation and the Gemini ledger.
            return None, reason
