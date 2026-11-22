"""Subtitle-first analysis with a durable, global quota for bounded video uploads."""

import asyncio
import base64
import copy
import hashlib
import inspect
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

from .bot_server import untrusted_input
from .media import ModelImage
from .security import Rejected, authorize
from .video_downloader import VideoDownloader
from .video_policy import VideoWatchConfig

log = logging.getLogger("hakimi")

_BILI_URL_RE = re.compile(r"https?://[^\s<>\"'，。！？）]+", re.IGNORECASE)
_BILI_VIDEO_HOSTS = frozenset({"www.bilibili.com", "bilibili.com", "m.bilibili.com"})
_BILI_FESTIVAL_PATH_RE = re.compile(r"^/festival(?:/[A-Za-z0-9_-]{1,80})?/?$")
_REASON_CODE_RE = re.compile(r"^[a-z0-9_]{1,48}$")

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
    "timeout",
    "invalid_media",
    "cdn_http_567",
    "cdn_http_403",
    "cdn_http_5xx",
    "cdn_host_blocked",
    "no_audio_track",
    "part_not_found",
    "storyboard_err",
)
DOWNLOAD_CODE_MAP = {
    "video_download_format": "download_format",
    "video_download_size": "download_size",
    "video_download_restricted": "download_restricted",
    "video_download_network_forbidden": "download_network",
    "video_download_timeout": "timeout",
    "video_download_invalid_media": "invalid_media",
    "video_download_cdn_http_567": "cdn_http_567",
    "video_download_cdn_http_403": "cdn_http_403",
    "video_download_cdn_http_5xx": "cdn_http_5xx",
    "video_download_cdn_host_blocked": "cdn_host_blocked",
    "video_download_no_audio_track": "no_audio_track",
    "video_download_part_not_found": "part_not_found",
    "video_download_storyboard_err": "storyboard_err",
}
LANE2_FALLBACK_CODES = frozenset(
    {
        "video_download_restricted",
        "video_download_unavailable",
        "video_download_timeout",
        "video_download_network_forbidden",
        "video_download_cdn_http_567",
        "video_download_cdn_http_403",
        "video_download_cdn_http_5xx",
        "video_download_cdn_host_blocked",
        "video_download_no_audio_track",
    }
)
MAX_STORYBOARD_BYTES = 262_144
STORYBOARD_NOTE = "原片由境内节点本地提取 3x3 关键帧拼图回传（未传输大体积原片）"
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
    "timeout": "原片下载超时",
    "invalid_media": "原片封装校验失败",
    "cdn_http_567": "视频 CDN 返回 HTTP 567（边缘拦截）",
    "cdn_http_403": "视频 CDN 返回 HTTP 403（已跳过账号风控冷却）",
    "cdn_http_5xx": "视频 CDN 临时服务异常",
    "cdn_host_blocked": "视频 CDN 域名未通过安全白名单校验",
    "no_audio_track": "视频缺少可用音轨，已拒绝静默降级",
    "part_not_found": "请求的视频分 P 不存在",
    "storyboard_err": "境内关键帧拼图回退失败",
    "risk": "平台风控或凭证过期，已停止访问并冷却",
    "files_unavailable": "视频上传或处理暂不可用",
    "deadline": "原片处理超时",
    "interrupted": "服务重启中断，不自动重放",
}


def _sanitize_reason_code(raw: str, *, default: str = "storyboard_err") -> str:
    cleaned = str(raw or "").strip().lower()
    return cleaned if _REASON_CODE_RE.fullmatch(cleaned) else default


def extract_video_page(text: str, bvid: str) -> int:
    """Extract bounded 1-based `?p=N` part index for `bvid` from trusted Bilibili URLs."""
    if not isinstance(text, str) or not text or not bvid:
        return 1
    for raw in _BILI_URL_RE.findall(text):
        cleaned = raw.rstrip("。，）)")
        try:
            u = urlsplit(cleaned)
            port = u.port
        except ValueError:
            continue
        host = (u.hostname or "").lower()
        if (
            u.scheme not in {"https", "http"}
            or host not in _BILI_VIDEO_HOSTS
            or u.username is not None
            or u.password is not None
            or (u.scheme == "https" and port not in {None, 443})
            or (u.scheme == "http" and port not in {None, 80})
        ):
            continue
        qs = parse_qs(u.query, keep_blank_values=False)
        matched_bvid = None
        path_match = re.fullmatch(r"/video/(BV[1-9A-HJ-NP-Za-km-z]{10})/?", u.path)
        if path_match:
            matched_bvid = path_match[1]
        elif _BILI_FESTIVAL_PATH_RE.fullmatch(u.path):
            q_bvids = qs.get("bvid")
            if q_bvids and re.fullmatch(r"BV[1-9A-HJ-NP-Za-km-z]{10}", q_bvids[0]):
                matched_bvid = q_bvids[0]
        if matched_bvid != bvid:
            continue
        raw_p = (qs.get("p") or [""])[0].strip()
        if re.fullmatch(r"[1-9][0-9]{0,2}", raw_p):
            return int(raw_p)
    return 1


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
    def __init__(self, service, *, downloader=None, clock=time.time, settings=None, store=None):
        self.svc = service
        self.s = settings if settings is not None else service.s
        self.store = store if store is not None else service.store
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
            "remaining_today": (
                None
                if self.config.daily_limit == "unlimited"
                else max(0, self.config.daily_limit - used)
            ),
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

    def _cn_delegate_ready(self) -> bool:
        nt = getattr(self.svc, "node_transport", None)
        return bool(
            nt is not None
            and nt.connected()
            and getattr(self.s, "node_bili_delegate_enabled", True)
        )

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

        raw_page = material.get("page") if isinstance(material, dict) else None
        if isinstance(raw_page, int) and not isinstance(raw_page, bool) and 1 <= raw_page <= 999:
            page = raw_page
        elif message is not None:
            page = extract_video_page(message.text, bvid)
        else:
            page = 1
        raw_cid = material.get("cid") if isinstance(material, dict) else None
        cid = (
            raw_cid
            if isinstance(raw_cid, int) and not isinstance(raw_cid, bool) and raw_cid > 0
            else None
        )

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
            is_too_long = (
                isinstance(duration, (int, float))
                and not isinstance(duration, bool)
                and math.isfinite(duration)
                and duration > self.config.max_seconds
            )
            if not self.config.enabled:
                reason = "disabled"
            elif self.config.daily_limit == 0:
                reason = "daily_limit"
            elif is_too_long and not self._cn_delegate_ready():
                reason = "too_long"
            elif self.video_lock.locked():
                reason = "busy"
            elif (await self.status())["remote_cleanup_pending"] >= 3:
                reason = "cleanup_pending"
            else:
                try:
                    result, reason = await self._video(
                        bvid,
                        payload,
                        subject,
                        lane,
                        message,
                        deep,
                        guard,
                        page=page,
                        cid=cid,
                        videoshot_only=is_too_long,
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
        self,
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
        *,
        page: int = 1,
        cid: int | None = None,
        videoshot_only: bool = False,
    ) -> tuple[VideoAnalysis | None, str | None]:
        if not self._cn_delegate_ready():
            return None, None
        nt = self.svc.node_transport
        await valid()
        req_cookies = {k: v for k, v in cookies.items() if v} if isinstance(cookies, dict) else {}
        call_params = {
            "bvid": bvid,
            "max_seconds": config.max_seconds,
            "cookies": req_cookies,
        }
        if page > 1:
            call_params["page"] = page
        if cid is not None:
            call_params["cid"] = cid
        if videoshot_only:
            call_params["videoshot_only"] = True
            dur_val = payload.get("duration")
            if (
                isinstance(dur_val, (int, float))
                and not isinstance(dur_val, bool)
                and math.isfinite(dur_val)
                and dur_val > 0
            ):
                call_params["duration"] = float(dur_val)
        try:
            sb = await nt.call(
                "bili.video_storyboard",
                call_params,
                timeout=120.0,
            )
        except TimeoutError:
            return None, "timeout"
        except Rejected as exc:
            return None, _sanitize_reason_code(str(exc), default="storyboard_err")
        except ValueError:
            return None, "invalid_response"
        await valid()
        if not isinstance(sb, dict) or sb.get("mime_type") != "image/jpeg":
            return None, "invalid_mime"
        is_videoshot = sb.get("source") == "videoshot"
        if videoshot_only and not is_videoshot:
            return None, "videoshot_required"
        data_b64 = sb.get("data_b64")
        if not isinstance(data_b64, str) or not data_b64:
            return None, "missing_data"
        try:
            raw_bytes = base64.b64decode(data_b64, validate=True)
        except ValueError:
            return None, "invalid_base64"
        if not (1 <= len(raw_bytes) <= MAX_STORYBOARD_BYTES):
            return None, "oversized"
        raw_dur = sb.get("duration") if sb.get("duration") is not None else payload.get("duration")
        sb_duration = (
            float(raw_dur)
            if isinstance(raw_dur, (int, float))
            and not isinstance(raw_dur, bool)
            and math.isfinite(raw_dur)
            and raw_dur > 0
            else None
        )
        if not is_videoshot and (sb_duration is None or sb_duration > config.max_seconds):
            return None, "invalid_duration"
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
        return (
            VideoAnalysis(
                result,
                payload.get("material_scope", "metadata_only"),
                "cn_storyboard",
            ),
            None,
        )

    async def _video(
        self,
        bvid,
        payload,
        subject,
        lane,
        message,
        deep,
        guard,
        *,
        page: int = 1,
        cid: int | None = None,
        videoshot_only: bool = False,
    ):
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
                day = self.day()
                if config.daily_limit != "unlimited":
                    used, total = await self.store.one(
                        f"SELECT COUNT(*) FILTER (WHERE {LOCAL_FAILURE_SQL}), COUNT(*) "
                        "FROM video_watch_attempts WHERE day=?",
                        (day,),
                    )
                    if used >= config.daily_limit or total >= max(config.daily_limit * 4, 12):
                        return None, "daily_limit"
                await self.store.db.execute(
                    "INSERT INTO video_watch_attempts VALUES (?,?,?,?,'working',NULL,?,NULL,NULL)",
                    (key, day, bvid, subject, self.clock()),
                )
            state, reason = "failed", "download_failed"
            try:
                async with asyncio.timeout(660):
                    cookies = (
                        await watcher.download_credentials(guard=valid)
                        if hasattr(watcher, "download_credentials")
                        else dict(watcher.creds)
                    )
                    if videoshot_only:
                        analysis, sb_err = await self._try_cn_storyboard(
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
                            page=page,
                            cid=cid,
                            videoshot_only=True,
                        )
                        if analysis is not None:
                            state, reason = "done", None
                            return analysis, None
                        log.warning(
                            "event=video_watch_storyboard_failed bvid=%s "
                            "lane1_reason=too_long storyboard_reason=%s",
                            bvid,
                            sb_err or "node_offline",
                        )
                        raise Rejected("video_download_storyboard_err")

                    try:
                        dl_kwargs = {
                            "max_seconds": config.max_seconds,
                            "cookies": cookies,
                            "proxy": self.s.bili_proxy.get_secret_value(),
                            "guard": valid,
                        }
                        if page > 1:
                            dl_kwargs["page"] = page
                        async with self.downloader.download(bvid, **dl_kwargs) as video:
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
                        lane1_code = str(lane1_exc)
                        lane1_reason = DOWNLOAD_CODE_MAP.get(lane1_code, "download_failed")
                        log.info(
                            "event=video_watch_lane1_failed bvid=%s code=%s reason=%s",
                            bvid,
                            _sanitize_reason_code(lane1_code, default="unknown"),
                            lane1_reason,
                        )
                        if lane1_code not in LANE2_FALLBACK_CODES:
                            raise
                        analysis, sb_err = await self._try_cn_storyboard(
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
                            page=page,
                            cid=cid,
                        )
                        if analysis is not None:
                            state, reason = "done", None
                            return analysis, None
                        if sb_err is not None:
                            log.warning(
                                "event=video_watch_storyboard_failed bvid=%s "
                                "lane1_reason=%s storyboard_reason=%s",
                                bvid,
                                lane1_reason,
                                sb_err,
                            )
                            if lane1_code == "video_download_unavailable":
                                raise Rejected("video_download_storyboard_err") from None
                        raise lane1_exc from None
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
