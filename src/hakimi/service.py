import asyncio
import dataclasses
import inspect
import logging
import random
import re
import time
import uuid
from collections import Counter
from contextlib import nullcontext
from typing import Any, Literal

from .adapters import Sender, farewell_send_bypass, identifier, onebot_message
from .affection import _validate_platform_user
from .attention import can_watch_video
from .bili_watcher import BiliWatcher
from .bilibili import Bilibili, extract_bvid
from .bot_server import is_persona_owner, untrusted_input
from .config import Settings
from .db_backup import DatabaseBackup
from .drive_client import FolderDrive
from .file_reader import FileReader
from .gateway import redact_excerpt
from .gemini import GeminiGateway
from .generation_policy import Scenario
from .group_pipeline import other_mentions
from .hot_ring import HotRing
from .life_engine import LifeEngine
from .link_preview import preview_links
from .longnick import Longnick
from .media import ImageLoader
from .memes import MAX_UPLOAD_BYTES, MemeLibrary, validate_meme_label
from .memory_compressor import MemoryCompressor, check_memory_text
from .memory_mirror import MemoryMirror
from .mentions import parse_reply, valid_at_targets
from .message_context import MessageContextResolver
from .models import Message, Video
from .moderation import Moderation
from .modules import BilibiliSurfModule, MinecraftSlotModule, ModuleRegistry, QQSocialModule
from .modules.base import sanitize_sensor_text
from .node_transport import NodeTransport
from .outbound_actions import normalize_emoji_like, parse_actions
from .output_links import filter_reply_links
from .owner_cloud_memory import OwnerCloudMemory
from .proactive_dm import OwnerProactive
from .public_memory import PublicMemory
from .rich_message_parser import parse_forward
from .roaming_logger import RoamingLogger, redact_public_summary
from .routing import RouteDecision
from .security import Rejected, authorize, configured_secrets, owner, safe_output
from .session_manager import SessionManager
from .session_scheduler import SessionScheduler
from .store import Store
from .streaming import cancellable
from .surf_mirror import SurfMirror
from .video_watch import VideoWatch
from .web_search import explicit_search, search_query
from .world_state import WorldStateManager

log = logging.getLogger("hakimi")
PRIOR_IMAGE_REF_RE = re.compile(
    r"(?:这|那|上|刚|前|原).{0,6}(?:张|幅|个)?(?:图|截图|照片|画面|报错图)"
    r"|(?:图|截图|照片)(?:里|中|上)"
    r"|(?:看|瞅|瞧|分析|解释|推导)(?:下|一下|看)?.{0,6}(?:图|截图|照片)"
)
PRIOR_FILE_REF_RE = re.compile(
    r"(?:文件|日志|代码|脚本|配置|报错)"
    r"|\.(?:log|txt|py|json|ya?ml|toml|md|js|ts|sh|sql)\b",
    re.I,
)
HELP = """哈基米在这里。私聊可直接提问或发 B 站视频；群聊也支持按语境看图、联网和聊视频。
/哈基米 或 /哈基米 帮助：查看本指令说明
/new-session：清空当前会话上下文，保留记忆开关
/search <内容> 或 /搜索 <内容>：显式联网检索公开信息（日常对话也会按需自动检索）
/watch-video（随 B 站链接或引用视频发送）：请求原片画面/音频分析（默认优先读字幕与热评摘要）
/哈基米 记忆 开：为当前私聊或本群启用短期与双轨记忆
/哈基米 记忆 关 或 /哈基米 清空：关闭并删除当前会话记忆
外部 Bot 默认按阈值限流；群内记忆与策略由配置中的主人管理，私聊记忆绝不进入群聊。
以下指令需真实主人权限（群内可用）：
/存表情 <标签>（或 /偷表情、/save-meme）：引用或附带图片（静图/透明PNG/GIF）存进本地表情包库
/bot-interact：群内 @ 外部 Bot 授权其本群临时互动（10 分钟最多 2 轮）
以下功能仅限配置中的主人私聊使用：
/deep <问题或视频链接>：本次启用 high 档深度分析
/set-sign <签名>：手动设置 50 字以内纯文本 QQ 个签
/mute <QQ> 或 /unmute <QQ>：应用层屏蔽或解除（默认不删好友）
/mute-approve <编号> 或 /mute-reject <编号>：批准或驳回屏蔽申请
（如需同时删除好友，须在 /mute 或 /mute-approve 末尾加 --delete-friend）
/affection [QQ] [分数]（或 /好感度）：查看或调整指定用户好感度档案（仅主人私聊）
/哈基米 订阅 <UP主UID> / /哈基米 退订 <UP主UID>：UP 更新私聊提醒（直播开播监控可在 /admin 添加）
/哈基米 订阅列表 / /哈基米 报告 / /哈基米 状态：查看订阅、近 7 天巡逻报告与今日额度
链接与小号巡逻优先阅读公开字幕与热评资料；原片分析受后台开关、时长、体积和每日次数限制。"""


class Service:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        gateway: GeminiGateway,
        bili: Bilibili,
        sender: Sender,
        *,
        image_loader: ImageLoader | None = None,
        diary: RoamingLogger | None = None,
        notebook=None,
        drive: FolderDrive | None = None,
        node_transport: NodeTransport | None = None,
    ):
        self.s, self.store, self.ai, self.bili, self.sender = settings, store, gateway, bili, sender
        self.node_transport = (
            node_transport if node_transport is not None else NodeTransport(settings)
        )
        if node_transport is not None or getattr(self.bili, "node_transport", None) is None:
            self.bili.node_transport = self.node_transport
        self.sessions = SessionManager(settings, store)
        self.hot = HotRing()
        store.runtime.hot_ring = self.hot
        store.runtime.filters.hot_ring = self.hot
        self.enrichments = {}
        self.preview_slots = asyncio.Semaphore(4)
        self.background_tasks = set()
        self.bot_guard = store.bot_guard
        self.images = image_loader or ImageLoader(
            settings, transport=getattr(sender, "onebot", None)
        )
        self.files = FileReader(settings, store, transport=getattr(sender, "onebot", None))
        self.context_resolver = MessageContextResolver(
            settings, getattr(sender, "onebot", None), lookup=self.lookup_received
        )
        self.ingress_scheduler = SessionScheduler()
        self.dialogue_scheduler = SessionScheduler()
        self.public_memory = PublicMemory(store)
        self.diary = diary
        self.notebook = notebook
        self.cloud_memory = OwnerCloudMemory(settings)
        self.compressor = MemoryCompressor(settings, self.sessions, gateway, store)
        self.memory_mirror = MemoryMirror(self)
        self.compressor.mirror = self.memory_mirror
        self.recovery_task = None
        self.poll_lock = asyncio.Lock()
        self.owner_dialogue_lock = asyncio.Lock()
        self.proactive = OwnerProactive(self)
        self.started_at = time.monotonic()
        self.counters: Counter = Counter()
        self.tasks: list[asyncio.Task] = []
        self.secrets = configured_secrets(settings)
        self.moderation = Moderation(self)
        store.runtime.moderation = self.moderation
        self.longnick = Longnick(self)
        self.bili_watcher = BiliWatcher(self)
        self.video_watch = VideoWatch(self)
        self.memes = MemeLibrary(store)
        store.runtime.memes = self.memes
        self._lurk_react_at: dict[str, float] = {}
        self.modules = ModuleRegistry()
        self.modules.register(
            QQSocialModule(
                settings,
                hot_ring=self.hot,
                transport=getattr(sender, "onebot", None),
                service=self,
            )
        )
        self.modules.register(
            BilibiliSurfModule(
                settings,
                watcher=self.bili_watcher,
                bili=bili,
                node_transport=self.node_transport,
            )
        )
        self.world_state = WorldStateManager(settings, store, self.modules)
        self.modules.register(MinecraftSlotModule(settings, world_state=self.world_state))
        if getattr(self.node_transport, "on_event", None) is None:
            self.node_transport.on_event = self._handle_node_event
        resolved_drive = drive if drive is not None else getattr(diary, "drive", None)
        active_drive = resolved_drive if settings.roaming_enabled else None
        self.db_backup = DatabaseBackup(settings, store, active_drive)
        self.surf_mirror = SurfMirror(self, active_drive)
        self.life_engine = LifeEngine(self)

    def refresh_secrets(self):
        self.secrets = configured_secrets(self.s)

    def record(self, event: str, *, emit_audit: bool = True):
        # Only internal constant labels. Never log exception repr, payloads, URLs or IDs.
        self.counters[event] += 1
        if emit_audit:
            log.info("event=%s", event)

    async def lookup_received(self, message, event_id):
        if await self.recalled([message.model_copy(update={"event_id": event_id})]):
            raise Rejected("recalled_message")
        return self.hot.lookup(message, event_id) or await self.sessions.lookup_received(
            message, event_id
        )

    async def recalled(self, members):
        for member in members:
            for ref in [member.event_id, *member.reply_ids]:
                if self.hot.recalled(member, ref):
                    return True
                event = self.store.event_key(member.model_copy(update={"event_id": ref}))
                async with self.store.lock:
                    if await self.store.one(
                        "SELECT 1 FROM message_recalls WHERE event=?", (event,)
                    ):
                        return True
        return False

    def background(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task

    async def enrich(self, message):
        try:

            async def hydrate():
                if message.images and isinstance(self.images, ImageLoader):
                    await self.images.load_results(
                        message.images, scope=self.store.session_scope(message)
                    )

            _, links = await asyncio.gather(
                hydrate(),
                preview_links(
                    message.command_text or message.text, self.preview_slots, budget_seconds=1.8
                ),
            )
            return links
        except Exception:
            return []

    async def accept_event(self, payload):
        if str(payload.get("self_id")) != self.s.onebot_self_id:
            raise Rejected("wrong_bot")
        typ = payload.get("post_type")
        if (
            typ in {"message", "message_sent"}
            and str(payload.get("user_id")) == self.s.onebot_self_id
        ):
            # Echoes only confirm an outgoing protocol ID; they cannot enter debounce or B.
            echo = onebot_message({**payload, "post_type": "message"}, self.s)
            if echo:
                await self.store.runtime.attention.record_sent(echo, {"message_id": echo.event_id})
            return "ignored"
        if typ == "notice":
            from .adapters import notice_message

            message = notice_message(payload, self.s)
            if message is None:
                return "ignored"
            if payload.get("notice_type") in {"group_recall", "friend_recall"}:
                if not authorize(message.model_copy(update={"addressed": True}), self.s):
                    return "ignored"
                ref = identifier(payload.get("message_id"))
                self.hot.recall(message, ref)
                self.store.groups.recall(message, ref)
                await self.sessions.invalidate_chains(message, recalled_event_id=ref)
                prefix_runtime = getattr(self.ai, "prefix_runtime", None)
                if prefix_runtime is not None:
                    prefix_runtime.invalidate_chain(
                        self.store.vault.digest(
                            "generation-cache", "production", self.store.session_scope(message)
                        )
                    )
                async with self.store.transaction():
                    event = self.store.event_key(message.model_copy(update={"event_id": ref}))
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO message_recalls VALUES (?,?)", (event, time.time())
                    )
                    rows = await self.store.all(
                        "SELECT o.id,o.payload FROM outbox o JOIN jobs j ON j.id=o.id "
                        "WHERE j.scope=? AND o.status IN ('pending','sending')",
                        (self.store.session_scope(message),),
                    )
                    for row in rows:
                        data = self.store.vault.open(row["id"], row["payload"])
                        originals = data.get("participants") or [data["message"]]
                        if any(ref in {m["event_id"], *m.get("reply_ids", [])} for m in originals):
                            await self.store.db.execute(
                                "UPDATE outbox SET status='cancelled',payload=NULL WHERE id=?",
                                (row["id"],),
                            )
                return "recalled"
            return await self.accept(message)
        return await self.accept(onebot_message(payload, self.s))

    async def accept(self, message: Message | None) -> str:
        if other_mentions(message):
            self.counters["addressed_to_others"] += 1
            return "ignored"
        message = await self.store.runtime.attention.resolve(message)
        if message is not None and self.bot_guard.known_bot(message):
            if not await self.bot_guard.prepare(message):
                return "ignored"
        eligible_context = bool(
            message is not None
            and message.kind == "group"
            and authorize(message.model_copy(update={"addressed": True}), self.s)
        )
        if message is None or (not authorize(message, self.s) and not eligible_context):
            self.counters["ignored"] += 1
            return "ignored"
        await self.proactive.note_activity(message)
        if any(secret and len(secret) >= 8 and secret in message.text for secret in self.secrets):
            self.counters["credential_in_input"] += 1
            return "ignored"
        try:
            check_memory_text(message.text, self.s)
        except Rejected:
            self.counters["credential_in_input"] += 1
            return "ignored"
        async with self.ingress_scheduler.hold(self.store.session_scope(message)):
            async with self.store.lock:
                if await self.store.one(
                    "SELECT 1 FROM jobs WHERE id=?", (self.store.event_key(message),)
                ):
                    return "duplicate"
            epoch = await self.sessions.receive(message)
            message = await self.context_resolver.resolve(message)
            check_memory_text(message.text, self.s)
            if any(
                secret and len(secret) >= 8 and secret in message.text for secret in self.secrets
            ):
                return "ignored"
            if await self.sessions.receive(message, expected_epoch=epoch) is None:
                return "ignored"
            if self.store.runtime.is_resting_for(message):
                if message.kind == "group":
                    await self.store.groups.observe(message)
                    if eligible_context and not self.bot_guard.known_bot(message):
                        await self.store.runtime.filters.remember_group(message)
                self.hot.observe(message)
                self.counters["admission_resting"] += 1
                return "resting"
            # Start hydration before opening the debounce window. Context-only images
            # in an admitted group can support the next addressed message; no LLM call.
            if (message.images or "http" in message.text) and len(self.background_tasks) < 32:
                if len(self.enrichments) >= 256:
                    oldest = next(iter(self.enrichments))
                    self.enrichments.pop(oldest).cancel()
                self.enrichments[self.store.event_key(message)] = self.background(
                    self.enrich(message)
                )
            if not authorize(message, self.s):
                # Context-only messages remain local; only an existing addressed batch can wake.
                await self.store.groups.observe(message)
                result = await self.store.admit(message, continuation_only=True)
            else:
                result = await self.store.admit(message)
            if message.kind == "group" and result in {"accepted", "ignored"}:
                await self.store.groups.observe(message)
                if result == "accepted" or (
                    eligible_context and not self.bot_guard.known_bot(message)
                ):
                    await self.store.runtime.filters.remember_group(message)
        if result in {"accepted", "ignored"}:
            self.hot.observe(message)
        if result == "accepted":
            self.store.groups.interrupt(message)
        if (
            result == "limited"
            and self.store.runtime.affection.config.enabled
            and not self.bot_guard.known_bot(message)
            and not owner(message, self.s)
        ):
            update = await self.store.runtime.affection.record_annoyance(message, "rate_limit")
            if update.blacklisted_now:
                await self._alert_affection_blacklisted(message, update)
        self.counters["admission_" + result] += 1
        return result

    def start(self):
        self.recovery_task = asyncio.create_task(
            self.resume_compression(), name="compression-recovery"
        )
        self.tasks = [
            asyncio.create_task(self.worker(), name=f"worker-{i}") for i in range(self.s.workers)
        ]
        self.tasks += [
            asyncio.create_task(self.delivery(), name="delivery"),
            asyncio.create_task(self.compressor.run(), name="memory-archive"),
            asyncio.create_task(self.patrol(), name="patrol"),
            asyncio.create_task(self.janitor(), name="janitor"),
            asyncio.create_task(self.proactive.run(), name="owner-proactive"),
            asyncio.create_task(self.proactive.delivery(), name="owner-dm-delivery"),
            asyncio.create_task(self.bili_watcher.run(), name="bili-account-patrol"),
            asyncio.create_task(self.gcp_billing_worker(), name="gcp-billing-sync"),
            asyncio.create_task(self.life_engine.run(), name="life-engine"),
        ]
        if self.diary and self.s.roaming_enabled:
            self.tasks.append(asyncio.create_task(self.diary.run(), name="roaming-diary"))
        if self.s.roaming_enabled:
            self.tasks.append(asyncio.create_task(self.db_backup.run(), name="db-backup"))
            self.tasks.append(asyncio.create_task(self.surf_mirror.run(), name="surf-mirror"))
        if self.notebook:
            self.tasks.append(asyncio.create_task(self.notebook.run(), name="notebook-refresh"))

    async def resume_compression(self):
        # Persisted, unattempted folds survive restarts. Paid ambiguous attempts never replay.
        for _, message in await self.store.runtime.sessions():
            self.compressor.schedule(message)
            await asyncio.sleep(0)

    async def stop(self):
        for task in list(self.background_tasks):
            task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        tasks = [*self.tasks, *([self.recovery_task] if self.recovery_task is not None else [])]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.compressor.close()
        await self.bili_watcher.close()
        close_nt = getattr(self.node_transport, "close", None)
        if close_nt is not None:
            res = close_nt()
            if inspect.isawaitable(res):
                await res

    async def _handle_node_event(self, frame: dict[str, Any]) -> str:
        event_name = frame.get("event")
        payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
        if event_name == "mc.snapshot":
            mc_mod = self.modules.get("minecraft")
            if isinstance(mc_mod, MinecraftSlotModule):
                await mc_mod.update_snapshot(payload)
        elif event_name == "mc.milestone":
            raw_type = payload.get("event_type") or payload.get("type") or "mc_milestone"
            clean_type = (
                sanitize_sensor_text(str(raw_type), self.s, max_tokens=16) or "mc_milestone"
            )
            raw_imp = payload.get("importance", "normal")
            importance: Literal["low", "normal", "high"] = (
                raw_imp if raw_imp in ("low", "normal", "high") else "normal"
            )
            event_data = (
                dict(payload["payload"])
                if isinstance(payload.get("payload"), dict)
                else {
                    k: v
                    for k, v in payload.items()
                    if k not in {"event_type", "type", "importance"}
                }
            )
            await self.world_state.publish_event(
                "minecraft",
                clean_type,
                event_data,
                importance=importance,
            )
        return "accepted"

    async def command(self, message: Message) -> str | None:
        if self.bot_guard.known_bot(message):
            return None
        prefix = self.s.command_prefix
        direct = message.command_text if message.command_text is not None else message.text
        for module in (self.moderation, self.longnick):
            result = await module.command(message)
            if result is not None:
                return result
        if re.match(r"^/bot-interact(?:\s|$)", direct):
            return await self.bot_guard.grant(message)
        if direct == "/new-session":
            if message.kind == "group" and not owner(message, self.s):
                raise Rejected("group_owner_only")
            await self.sessions.clear(message)
            return "已清空当前会话上下文，记忆开关保持原设置。"
        meme_match = re.match(
            r"^(?:/(?:存表情|偷表情|save-meme)|" + re.escape(prefix) + r"\s*(?:存表情|偷表情))"
            r"(?:\s+(.*))?$",
            direct.strip(),
            re.DOTALL,
        )
        if meme_match:
            return await self._save_meme_command(message, meme_match[1] or "")
        aff_match = re.match(
            r"^(?:/(?:affection|好感度)|(?:"
            + re.escape(prefix)
            + r"|/哈基米)\s+好感度)(?:\s+(.*))?$",
            direct.strip(),
        )
        if aff_match:
            return await self._affection_command(message, aff_match[1] or "")
        if not direct.startswith(prefix):
            return None
        command = direct[len(prefix) :].strip()
        subject = self.store.subject(message)
        if command in {"", "帮助"}:
            return HELP
        if (
            command in {"清空", "记忆 关", "记忆 开"}
            and message.kind == "group"
            and not owner(message, self.s)
        ):
            raise Rejected("group_owner_only")
        if command in {"清空", "记忆 关"}:
            await self.sessions.clear(message, enabled=False)
            return "已关闭并删除当前会话中的记忆。"
        if command == "记忆 开":
            await self.sessions.enable(message)
            return "已启用当前会话的短期记忆；本群成员共用本群上下文，其他群和私聊均独立。"
        privileged = command in {"订阅列表", "报告", "状态"} or command.startswith(("订阅", "退订"))
        if not privileged:
            return None
        if message.kind != "private" or not owner(message, self.s):
            raise Rejected("owner_private_only")
        if command == "订阅列表":
            subscriptions = await self.store.subscriptions(subject)
            return "已订阅 UP：" + ("、".join(s["mid"] for s in subscriptions) or "暂无")
        if command == "报告":
            reports = await self.store.latest_reports(subject)
            return "\n\n".join(reports) if reports else "最近七天暂无巡逻摘要。"
        if command == "状态":
            usage = await self.store.usage()
            amount = sum(x["amount"] for x in usage["lanes"]) / 1_000_000
            return (
                f"{self.s.bot_timezone} {usage['day_local']}：已计入费用（含预留）${amount:.4f}；"
                f"每日全局上限 ${self.s.daily_usd}。"
            )
        match = re.fullmatch(r"(订阅|退订) ([1-9][0-9]{0,19})", command)
        if not match:
            return "格式：/哈基米 订阅 <数字UID> 或 /哈基米 退订 <数字UID>"
        if match[1] == "退订":
            await self.store.remove_subscription(subject, match[2])
            return "已取消该 UP 主的巡逻订阅。"
        if not self.s.bilibili_enabled:
            return "B 站读取功能尚未开启。"
        await self.store.add_subscription(message, match[2])
        tip = (
            "发现更新后会私聊汇报。"
            if is_persona_owner(message, self.s) and self.proactive.config.enabled
            else "新摘要会保存在 /哈基米 报告 中。"
        )
        return "已订阅；首次巡逻仅建立基线。" + tip

    async def _affection_command(self, message: Message, raw_args: str) -> str:
        if message.kind != "private" or not owner(message, self.s):
            raise Rejected("owner_private_only")
        aff = self.store.runtime.affection
        args = raw_args.strip().split()
        if not args:
            st = await aff.status()
            owner_prof = await aff.profile(
                message.platform, message.user_id, message.sender_nickname
            )
            users = st["users"]
            blacklisted = [u for u in users if u["score"] == 0 and not u["is_owner"]]
            bl_text = (
                "、".join(f"{u['nickname']}({u['user_id']})" for u in blacklisted[:10]) or "暂无"
            )
            cap = st["config"]["daily_positive_cap"]
            init_score = st["config"]["initial_score"]
            return (
                f"好感度系统：{'已启用' if st['config']['enabled'] else '已停用'}"
                f"（初始 {init_score}，日正增长上限 +{cap}）。\n"
                f"BOISER 当前羁绊：{owner_prof['score']} 分"
                f"（{owner_prof['lv']} {owner_prof['tier_label']}）。\n"
                f"已建档用户：{len(users)} 人；拉黑(0分)：{len(blacklisted)} 人（{bl_text}）。\n"
                "用法：/affection <QQ> 查看档案，或 /affection <QQ> <0..120> 调整分数。"
            )
        if len(args) > 2:
            return "用法：/affection [QQ] [0..120]"
        raw_target = args[0].strip()
        if raw_target.startswith("qq:"):
            target_platform, target_uid = "qq", raw_target[3:]
        elif raw_target.startswith("onebot:"):
            target_platform, target_uid = "onebot", raw_target[7:]
        else:
            target_platform, target_uid = "onebot", raw_target
        try:
            target_uid = _validate_platform_user(target_platform, target_uid)
        except Rejected:
            return "用法：/affection [QQ] [0..120]，目标账号格式不正确。"
        if len(args) == 1:
            prof = await aff.profile(target_platform, target_uid)
            return (
                f"{prof['nickname']}（{prof['platform']}:{prof['user_id']}）：{prof['score']} 分｜"
                f"{prof['lv']} {prof['tier_label']}｜"
                f"今日正向 +{prof['today_positive_gain']}/{prof['daily_positive_cap']}"
                f"{'｜已拉黑静音' if prof['muted'] else ''}"
            )
        if not re.fullmatch(r"-?[0-9]{1,4}", args[1]):
            return "分数需为 0～120 之间的整数（Owner 下限 100，普通成员上限 100）。"
        target_score = int(args[1])
        if not 0 <= target_score <= 120:
            return "分数需为 0～120 之间的整数（Owner 下限 100，普通成员上限 100）。"
        try:
            updated = await aff.set_user_score(
                target_uid,
                target_score,
                platform=target_platform,
                reason="owner_cmd",
                moderation=self.moderation,
            )
        except Rejected as exc:
            return f"设置失败：{exc}"
        return (
            f"已将 {updated['nickname']}（{updated['user_id']}）好感度设为 {updated['score']} 分"
            f"（{updated['lv']} {updated['tier_label']}）"
            + ("，并已同步应用层静音。" if updated["muted"] else "。")
        )

    async def _save_meme_command(self, message: Message, raw_label: str) -> str:
        if not owner(message, self.s):
            raise Rejected("group_owner_only" if message.kind == "group" else "owner_private_only")
        raw_label = raw_label.strip()
        if not raw_label:
            return "用法：引用带图消息（或附图同发）并发送 /存表情 <1~24字标签>"
        try:
            label = validate_meme_label(raw_label)
        except Rejected:
            return "标签格式不对，请用 1～24 个汉字、字母、数字、下划线或短横线。"
        if not message.images and message.forward_ids and not message.context_expanded:
            try:
                message = await self.expand_forwards(message)
            except Rejected:
                return "转发内容展开失败，无法获取其中的图片。"
        if not message.images:
            return "未找到图片，请引用包含表情包的消息或随指令附带图片。"
        if not hasattr(self.images, "fetch_raw"):
            return "当前图片加载器不支持原始下载，请在管理后台上传。"
        try:
            raw = await self.images.fetch_raw(message.images[0], max_bytes=MAX_UPLOAD_BYTES)
            saved = await self.memes.upload_bytes(label, raw)
        except Rejected as exc:
            code = str(exc)
            messages = {
                "image_too_large": "图片太大啦，单张表情不能超过 5 MiB。",
                "meme_upload_too_large": "图片太大啦，单张表情不能超过 5 MiB。",
                "meme_label_exists": f"标签「{label}」里已经有这张图啦，换个标签或直接用就行~",
                "meme_library_full": (
                    "表情包库已经满啦（最多 100 张 / 50 MiB），去后台删几张再来吧。"
                ),
                "invalid_meme_label": (
                    "标签格式不对，请用 1～24 个汉字、字母、数字、下划线或短横线。"
                ),
            }
            if code in messages:
                return messages[code]
            if code.startswith(("meme_image_", "invalid_meme_")):
                return "这张图格式或尺寸不支持呢（支持 jpg / png / gif / webp）。"
            return f"保存表情失败（{code}）。"
        extra = "（动画已取第一帧）" if saved.get("first_frame_only") else ""
        return f"表情已存好啦：标签「{saved['label']}」，随时可以用~{extra}"

    async def _auto_save_meme(
        self, message: Message, candidates: list[Message], label: str
    ) -> bool:
        if not hasattr(self.images, "fetch_raw"):
            return False
        ref = next(
            (img for item in reversed(candidates) for img in reversed(item.images)),
            None,
        )
        if ref is None and message.reply_ids:
            for reply_id in reversed(message.reply_ids):
                replied = await self.lookup_received(message, reply_id)
                if replied and replied.images:
                    ref = replied.images[-1]
                    break
        if ref is None:
            return False
        try:
            raw = await self.images.fetch_raw(ref, max_bytes=MAX_UPLOAD_BYTES)
            await self.memes.upload_bytes(label, raw)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    def output(self, text: str) -> str:
        return safe_output(text, self.secrets, limit=self.s.send_max_chars)

    @staticmethod
    def _pick_lurk_emoji(members: list[Message]) -> str | None:
        combined = " ".join(str(m.text or "") for m in members)
        for pattern, label in (
            (r"(?:哈哈哈|笑死|绷不住|笑疯|草|2333|乐死)", "笑哭"),
            (r"(?:好耶|太强了|牛逼|牛哇|出货了|不歪|搞定了|跑通了)", "庆祝"),
            (r"(?:晚安|先睡了|困死|熬不住了)", "晚安"),
            (r"(?:猫猫|喵喵|修猫|小猫)", "猫猫"),
        ):
            if re.search(pattern, combined, re.I):
                return normalize_emoji_like(label)
        if any(m.images for m in members):
            return normalize_emoji_like("围观")
        return None

    def _authorize_for_batch(
        self, candidate: Message, *, bypass_mute_user_id: str | None = None
    ) -> bool:
        if (
            bypass_mute_user_id
            and candidate.user_id == bypass_mute_user_id
            and candidate.user_id in self.s.muted_user_ids
            and not self.bot_guard.known_bot(candidate)
        ):
            cfg = self.s.model_copy(
                update={"muted_user_ids": self.s.muted_user_ids - {bypass_mute_user_id}}
            )
            return authorize(candidate, cfg)
        return authorize(candidate, self.s)

    def batch_authorized(self, message, members, *, bypass_mute_user_id: str | None = None):
        # An accepted @Bot batch may include subsequent unaddressed follow-up text.
        # This relaxes attention only, never group admission, identity or blacklists.
        addressed_batch = (
            message.kind == "group" and len(members) > 1 and any(m.addressed for m in members)
        )
        return self._authorize_for_batch(message, bypass_mute_user_id=bypass_mute_user_id) and all(
            self._authorize_for_batch(
                m.model_copy(update={"addressed": True}) if addressed_batch else m,
                bypass_mute_user_id=bypass_mute_user_id,
            )
            and not other_mentions(m)
            for m in members
        )

    def image_sender(self, message):
        nickname = message.sender_nickname.strip()[:40] or "群友"
        token = self.store.vault.digest("image-speaker", *message.subject_parts(self.s.namespace))[
            :6
        ]
        return f"{nickname}（成员标识 {token}）"

    def _is_sticker_candidate(self, message, ref) -> bool:
        if getattr(ref, "is_sticker", False):
            return True
        if isinstance(self.images, ImageLoader):
            cached = self.images.cache_get(self.store.session_scope(message), ref)
            if cached is not None and cached.digest and self.memes.semantic_label(cached.digest):
                return True
        return False

    async def message_images(
        self,
        message,
        members,
        nearby,
        *,
        limit=2,
        include_current=True,
        nearby_origin="紧邻群消息",
        selected_members=None,
    ):
        selected, seen = [], set()
        for origin, messages in (
            ("本批消息", reversed(members)),
            (nearby_origin, reversed(nearby)),
        ):
            entries = []
            for item in messages:
                candidate = (
                    item.model_copy(update={"addressed": True}) if item.kind == "group" else item
                )
                if not authorize(candidate, self.s) or other_mentions(item):
                    continue
                for ref in item.images:
                    if ref.identity not in seen:
                        seen.add(ref.identity)
                        entries.append((ref, item, origin))
            if origin == nearby_origin and entries:
                regular = [e for e in entries if not self._is_sticker_candidate(message, e[0])]
                stickers = [e for e in entries if self._is_sticker_candidate(message, e[0])]
                entries = regular + stickers
            selected.extend(entries)
        # Expanded forward images can lack an original-user mapping; never invent authorship.
        for ref in message.images if include_current else []:
            if ref.identity not in seen:
                selected.append((ref, message, "转发材料，原始作者未核实"))
                seen.add(ref.identity)
        omitted = max(0, len(selected) - limit)
        selected = selected[:limit]
        if not selected:
            return [], []
        refs = [entry[0] for entry in selected]
        if hasattr(self.images, "load_results"):
            results = await self.images.load_results(
                refs,
                **(
                    {"scope": self.store.session_scope(message)}
                    if isinstance(self.images, ImageLoader)
                    else {}
                ),
            )
        else:
            results = await self.images.load(refs)  # Compatible injected loaders in integrations.
        images, sources = [], []
        valid_loaded = sum(1 for item in results if item is not None)
        for (ref, member, origin), loaded in zip(selected, results, strict=True):
            data = {
                "sender": f"{ref.sender_nickname or '群友'}(QQ:{ref.sender_id})"
                if ref.sender_id
                else self.image_sender(member),
                "origin": origin if ref.origin in {"", "当前消息"} else origin + " / " + ref.origin,
            }
            if loaded is None:
                data["status"] = (
                    self.images.failure_status(ref)
                    if hasattr(self.images, "failure_status")
                    else "unavailable"
                )
            elif valid_loaded == 1 and len(loaded.tiles) == 2 and len(images) + 2 <= limit:
                split_imgs = loaded.tile_images()
                if selected_members is not None:
                    selected_members.append(member)
                semantic = self.memes.semantic_label(loaded.digest)
                for part_idx, part_img in enumerate(split_imgs, 1):
                    images.append(part_img)
                    part_data = {
                        **data,
                        "image_number": len(images),
                        "status": "available",
                        "slice": f"{part_idx}/2",
                    }
                    if semantic and part_idx == 1:
                        part_data["local_sticker_label"] = semantic
                    sources.append(part_data)
                continue
            else:
                images.append(loaded)
                data.update(image_number=len(images), status="available")
                if selected_members is not None:
                    selected_members.append(member)
                semantic = self.memes.semantic_label(loaded.digest)
                if semantic:
                    data["local_sticker_label"] = semantic
            sources.append(data)
        if omitted:
            sources.append({"status": "omitted", "count": omitted})
        return images, sources

    async def _load_turn_files(
        self,
        message: Message,
        members: list[Message],
        filtered_nearby: list[Message],
        history_events: set[str],
        current_events: set[str],
    ) -> tuple[list[dict], list[dict], list[str]]:
        candidates: list[tuple] = []
        seen_ids: set[str] = set()
        for item in reversed(members):
            check = item.model_copy(update={"addressed": True}) if item.kind == "group" else item
            if not authorize(check, self.s) or other_mentions(item):
                continue
            for ref in item.files:
                if ref.identity not in seen_ids:
                    seen_ids.add(ref.identity)
                    candidates.append((ref, item, "本批消息"))
        for ref in message.files:
            if ref.identity not in seen_ids:
                seen_ids.add(ref.identity)
                candidates.append((ref, message, "引用或转发材料"))
        if not candidates and (message.kind == "group" or PRIOR_FILE_REF_RE.search(message.text)):
            consumed = (
                await self.store.groups.consumed_file_events(message)
                if message.kind == "group"
                else set()
            )
            mention_any = bool(PRIOR_FILE_REF_RE.search(message.text))
            nearby_pool = [m for m in filtered_nearby if m.files and m.event_id not in consumed]
            hot_pool = [
                m
                for m in self.hot.recent_with_files(
                    message, exclude_events={*current_events, *consumed}
                )
                if m.event_id not in history_events
            ]
            seen_events: set[str] = set()
            for item in reversed([*hot_pool, *nearby_pool]):
                if item.event_id in seen_events:
                    continue
                seen_events.add(item.event_id)
                if item.user_id != message.user_id and (message.kind != "group" or not mention_any):
                    continue
                if await self.recalled([item]):
                    continue
                check = (
                    item.model_copy(update={"addressed": True}) if item.kind == "group" else item
                )
                if not authorize(check, self.s) or other_mentions(item):
                    continue
                for ref in item.files:
                    if ref.identity not in seen_ids:
                        seen_ids.add(ref.identity)
                        candidates.append((ref, item, "近期群聊文件"))
        if not candidates:
            return [], [], []
        selected: list[Message] = []
        attachments, sources = await self.files.load_for_turn(
            message, candidates, limit=1, selected_members=selected
        )
        consumed_events = [
            m.event_id for m in selected if m.event_id and m.event_id not in current_events
        ]
        return attachments, sources, consumed_events

    async def expand_forwards(self, message: Message) -> Message:
        texts, images = [message.text], list(message.images)
        seen_ids = {i.identity for i in images}
        files = list(message.files)
        seen_file_ids = {f.identity for f in files}
        videos, shorts = list(message.video_ids), list(message.short_urls)
        statuses = set(message.context_status)
        for forward_id in message.forward_ids:
            rich = parse_forward(await self.sender.forward(forward_id), message.bot_id)
            if rich.truncated:
                statuses.add("truncated")
            texts.append(rich.text)
            for img in rich.images:
                if img.identity not in seen_ids:
                    images.append(img)
                    seen_ids.add(img.identity)
            for fref in rich.files:
                if fref.identity not in seen_file_ids:
                    files.append(fref)
                    seen_file_ids.add(fref.identity)
            videos.extend(v for v in rich.video_ids if v not in videos)
            shorts.extend(u for u in rich.short_urls if u not in shorts)
            # No recursive get_forward_msg calls: a quoted reference cannot fan out requests.
        text = "\n".join(texts)
        if len(text) > 24000:
            raise Rejected("forward_too_large")
        if len(images) > 2:
            images = images[:2]
            statuses.add("truncated")
        if len(files) > 2:
            files = files[:2]
            statuses.add("truncated")
        if any(secret and len(secret) >= 8 and secret in text for secret in self.secrets):
            raise Rejected("credential_in_input")
        return message.model_copy(
            update={
                "text": text,
                "images": images,
                "files": files,
                "video_ids": videos[:5],
                "short_urls": shorts[:5],
                "forward_ids": [],
                "context_expanded": True,
                "context_status": sorted(statuses),
            }
        )

    async def summarize(
        self,
        videos: list[Video],
        subject: str,
        lane: str,
        *,
        deep: bool = False,
        message: Message | None = None,
        patrol_notify: bool = False,
    ) -> str:
        # No chat memory is ever included in public-content summarization.
        payload = {
            "task": (
                "notify_owner_subscribed_up_new_video"
                if patrol_notify
                else "summarize_public_metadata"
            ),
            "material_scope": "metadata_only",
            "untrusted_sources": [v.model_dump(exclude={"mid"}) for v in videos],
        }
        # Truncate before screening; the generator receives precisely the same screened excerpts.
        while len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            longest = max(payload["untrusted_sources"], key=lambda v: len(v["description"]))
            if not longest["description"]:
                raise Rejected("context_too_large")
            longest["description"] = longest["description"][: len(longest["description"]) // 2]
        await self.ai.screen(payload, subject, purpose="source", budget_lane=lane)
        text = await self.ai.generate(
            payload, subject, summary=True, deep=deep, budget_lane=lane, message=message
        )
        # Links come only from validated BVIDs, never model output or fetched text.
        text = filter_reply_links(text, public=False)
        sources = "\n".join(v.url for v in videos)
        return self.output(f"{text}\n{sources}")

    async def handle_message(self, job: dict):
        message = Message.model_validate(job["payload"])
        async with self.dialogue_scheduler.hold(self.store.session_scope(message)):
            batch = self.store.groups.begin(job)
            try:

                async def run():
                    if message.kind == "private" and is_persona_owner(message, self.s):
                        async with self.owner_dialogue_lock:
                            return await self._handle_message(job)
                    return await self._handle_message(job)

                return await cancellable(run, batch.cancel)
            except asyncio.CancelledError:
                if batch.cancel.is_set():
                    raise Rejected(
                        "batch_interrupted" if batch.reason == "followup" else "batch_recalled"
                    ) from None
                raise
            finally:
                self.store.groups.active.pop(job["scope"], None)

    def _select_affection_target(
        self, message: Message, members: list[Message], actions
    ) -> tuple[Message, int, str]:
        target = message
        delta = int(actions.affection_delta or 0)
        if len(members) > 1:
            if actions.anchor.reply_id:
                matched = next((m for m in members if m.event_id == actions.anchor.reply_id), None)
                if matched is not None:
                    target = matched
            elif actions.anchor.mentions:
                matched = next(
                    (m for m in members if m.user_id == actions.anchor.mentions[0]), None
                )
                if matched is not None:
                    target = matched
        if is_persona_owner(target, self.s) and (
            delta <= -10 or (delta < 0 and any(not is_persona_owner(m, self.s) for m in members))
        ):
            non_owners = [m for m in members if not is_persona_owner(m, self.s)]
            unique_non_owners = {m.user_id for m in non_owners}
            if len(unique_non_owners) == 1:
                target = non_owners[-1]
            else:
                delta = 0
        reason = "attack_owner" if delta <= -10 else "chat"
        return target, delta, reason

    async def _alert_affection_blacklisted(self, message: Message, update) -> None:
        self.record("affection_blacklisted")
        nick = self.moderation.nickname(message.sender_nickname or getattr(update, "nickname", ""))
        code = f"affection_zero_{message.platform}_{message.user_id}"
        recovery = self.store.runtime.affection.config.unmute_recovery_score
        hint = (
            f"如需恢复互动，可私聊发送 /unmute {message.user_id}"
            f"（恢复至 {recovery} 分）或前往后台调整。"
            if message.platform == "onebot"
            else "如需恢复互动，请前往后台好感度面板调整分值。"
        )
        text = (
            f"BOISER，{nick}（{message.platform}:{message.user_id}）因「{update.reason}」"
            f"好感度已降至 0 分，哈基米已发告别语并在应用层静音（绝不删好友）。\n{hint}"
        )
        try:
            await self.proactive.alert(code, text)
        except Exception:
            pass

    async def _enqueue_affection_farewell(
        self, job, message: Message, members: list[Message], session, watermark, audit_id, update
    ) -> None:
        if session is None:
            session = await self.sessions.snapshot(message)
        aff = self.store.runtime.affection
        farewell = self.output(update.farewell_message or aff.config.farewell_message)
        public = not (message.kind == "private" and is_persona_owner(message, self.s))
        target_id = next(
            (m.event_id for m in reversed(members) if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)),
            None,
        )
        await self.store.enqueue_reply(
            job["id"],
            message,
            farewell,
            session_epoch=session["epoch"],
            participants=members,
            allowed_at_qqs=set(),
            audit_id=audit_id,
            actions={
                "abstain": False,
                "bubbles": [farewell],
                "anchor": {"reply_id": target_id, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "emoji_like": None,
                "poke_qq": None,
                "save_meme_label": None,
                "set_longnick": None,
                "affection_delta": update.delta,
                "folded": None,
                "watermark": watermark,
                "source_ids": [m.event_id for m in members],
                "public": public,
                "allow_text_card": False,
                "farewell_bypass_mute": True,
            },
            consumed_image_events=[],
            consumed_file_events=[],
        )
        if audit_id:
            await self.store.runtime.filters.update_audit(audit_id, "queued")

    async def _handle_message(self, job: dict):
        message = Message.model_validate(job["payload"])
        members = [Message.model_validate(m) for m in job.get("batch_messages", [])] or [message]
        for original in members:
            if self.bot_guard.known_bot(original):
                await self.bot_guard.prepare(original)
        neighbour_members = []
        recalled_members = []
        if await self.recalled(members):
            return
        watermark = self.hot.watermark(message)
        if (
            not self.batch_authorized(message, members)
            or time.time() > job["expires"]
            or self.store.runtime.is_resting_for(message)
        ):
            return
        # Recheck persisted permits before image downloads, forward RPCs, memory or paid calls.
        if not await self.bot_guard.may_process(message, job["id"]):
            return
        aff = self.store.runtime.affection
        caps = await aff.capabilities_for(message)
        aff_active = aff.config.enabled and not self.bot_guard.known_bot(message)
        if aff_active and caps.level == 0:
            return
        fixed = await self.command(message)
        if fixed is not None:
            await self.store.enqueue_reply(job["id"], message, self.output(fixed))
            return
        direct = message.command_text if message.command_text is not None else message.text
        deep_match = re.fullmatch(r"/deep(?:\s+(.*))?", direct, re.DOTALL)
        deep = deep_match is not None and not self.bot_guard.known_bot(message)
        if deep:
            if message.kind != "private" or not owner(message, self.s):
                raise Rejected("owner_private_only")
            if not deep_match[1] or not deep_match[1].strip():
                await self.store.enqueue_reply(job["id"], message, "用法：/deep <问题或视频链接>")
                return
        if (
            aff_active
            and not caps.allow_poke_inbound
            and message.text.strip().startswith("[协议事件：戳了戳你]")
        ):
            update = await aff.record_annoyance(message, "poke_ignored")
            if update.blacklisted_now:
                await self._enqueue_affection_farewell(
                    job, message, members, None, watermark, None, update
                )
                await self._alert_affection_blacklisted(message, update)
            return
        if (
            aff_active
            and not aff.is_owner_id(message.platform, message.user_id)
            and caps.direct_reply_prob < 1.0
        ):
            if random.random() > caps.direct_reply_prob:
                self.counters["affection_prob_ignored"] += 1
                return
        if (
            message.platform == "onebot"
            and message.kind == "private"
            and hasattr(self.sender, "typing")
        ):
            await self.sender.typing(message)
        if not message.context_expanded:
            message = await self.expand_forwards(message)
        img_limit = (
            0
            if (aff_active and caps.max_images <= 0)
            else (min(2, caps.max_images) if aff_active else 2)
        )
        images, image_sources = [], []
        if img_limit > 0:
            images, image_sources = await self.message_images(
                message, members or [message], [], limit=img_limit
            )
            if aff_active and caps.low_res_images_only and images:
                images = [dataclasses.replace(i, resolution="low") for i in images]
        # Open exactly one physical database; never search other conversations.
        session = await self.sessions.snapshot(message)
        history = session["recent_messages"]
        if message.kind == "group":
            frozen_relevance = await self.store.runtime.filters.group_context(message, limit=24)
        elif message.kind == "private" and session.get("enabled") and history:
            frozen_relevance = [
                redact_excerpt(
                    f"{item.get('role', 'user')}: {item.get('text', '')}",
                    self.s,
                    limit=240,
                )
                for item in history[-16:]
                if isinstance(item, dict) and str(item.get("text", "")).strip()
            ]
        else:
            frozen_relevance = []
        payload = {
            "current_message": message.text,
            "current_batch_items": [
                {
                    "event_id": m.event_id,
                    "sender_id": m.user_id,
                    "sender_nickname": m.sender_nickname or "群友",
                    "text": message.text if len(members) == 1 else m.text,
                }
                for m in members
            ],
            "current_sender": {
                "nickname": message.sender_nickname or "群友",
                "qq": message.user_id,
            },
            "surface": message.kind,
            "directly_addressed": message.addressed,
            "recent_messages": history,
            "rolling_summary": session["rolling_summary"],
            "session_epoch": session["epoch"],
            "session_enabled": session.get("enabled", False),
            "interaction_chains": session.get("interaction_chains", {}),
            "allowed_reply_ids": [
                m.event_id
                for m in members
                if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)
                and (not self.bot_guard.known_bot(m) or self.bot_guard.is_owner_invited(m))
            ],
            "history_revision": session.get("history_revision", 0),
            "context_status": [
                {"event_id": m.event_id, "states": m.context_status}
                for m in members or [message]
                if m.context_status
            ],
        }
        previews = []
        for m in members:
            task = self.enrichments.pop(self.store.event_key(m), None)
            if task:
                previews.extend(await task)
        if previews:
            payload["web_preview_untrusted"] = previews[:2]
        if image_sources:
            payload["image_sources"] = image_sources
        if self.notebook:
            snapshot = self.notebook.for_message(message)
            if snapshot:
                payload["owner_notebook_snapshot"] = snapshot
        public_history = await self.public_memory.recent_for(message)
        while (
            public_history
            and len(untrusted_input({"background": public_history}).encode())
            > self.s.max_context_bytes // 2
        ):
            public_history.pop()
        if public_history:
            payload["近期群聊巡逻记忆（只读背景）"] = public_history
        # Attribute strikes to this incoming content, never to another member's history.
        # Stored history, summaries and model outputs were screened on entry/creation.
        current = {
            key: payload[key]
            for key in ("current_message", "current_sender", "surface", "directly_addressed")
        }
        if previews:
            current["web_preview_untrusted"] = previews[:2]
        # Forward/image/session preparation yields to admin updates. A revocation
        # during those waits must take effect before the first paid gateway call.
        if not authorize(message, self.s) or self.store.runtime.is_resting_for(message):
            return

        async def message_guard():
            if await self.recalled(members) or (
                recalled_members and await self.recalled(recalled_members)
            ):
                raise Rejected("reply_invalidated")
            if (
                not self.batch_authorized(message, members)
                or any(
                    not authorize(m.model_copy(update={"addressed": True}), self.s)
                    or other_mentions(m)
                    for m in neighbour_members
                )
                or not authorize(message, self.s)
                or time.time() > job["expires"]
                or any(
                    self.hot.recalled(m) or any(self.hot.recalled(m, r) for r in m.reply_ids)
                    for m in members
                )
            ):
                raise Rejected("reply_invalidated")
            if (await self.sessions.snapshot(message))["epoch"] != session["epoch"]:
                raise Rejected("reply_invalidated")

        audit_id = uuid.uuid4().hex
        job["_audit_id"] = audit_id
        try:
            relevance = frozen_relevance
            intent = await self.ai.screen(
                current,
                job["subject"],
                purpose="message",
                group=message.kind == "group",
                message=message,
                before_call=message_guard,
                audit_id=audit_id,
                audit_job_id=job["id"],
                **({"relevance_context": relevance} if relevance else {}),
                **({"images": [i.gate() for i in images]} if images else {}),
            )
        except Rejected as exc:
            if (
                len(members) == 1
                and not previews
                and str(exc) in {"prompt_injection", "content_blocked"}
            ):
                await self.moderation.record_strike(
                    message, str(exc), nickname=message.sender_nickname
                )
                if aff_active:
                    update = await aff.record_violation(message, str(exc))
                    if update.blacklisted_now:
                        await self._enqueue_affection_farewell(
                            job, message, members, session, watermark, audit_id, update
                        )
                        await self._alert_affection_blacklisted(message, update)
            raise
        if not authorize(message, self.s):
            await self.store.runtime.filters.update_audit(audit_id, "cancelled", "unauthorized")
            return
        sandbox = getattr(intent, "sandbox_mode", False)
        if self.diary and not sandbox:
            await self.diary.group(job["id"], message, members=members)
        if not sandbox:
            if not await self.sessions.admit_history(
                message, members, expected_epoch=session["epoch"]
            ):
                await self.store.runtime.filters.update_audit(
                    audit_id, "cancelled", "epoch_changed"
                )
                return
            self.compressor.schedule(message)
            # Admission may atomically fold B. Reload before constructing the model history.
            fresh = await self.sessions.snapshot(message)
            current_events = {m.event_id for m in members}
            history = [
                item
                for item in fresh["recent_messages"]
                if item.get("event_id") not in current_events
            ]
            payload.update(
                recent_messages=history,
                rolling_summary=fresh["rolling_summary"],
                history_revision=fresh.get("history_revision", 0),
                session_enabled=fresh.get("enabled", False),
                interaction_chains=fresh.get("interaction_chains", {}),
            )
            if self.cloud_memory:
                cloud_mem = await asyncio.to_thread(
                    self.cloud_memory.for_message,
                    message,
                    message.text,
                    recall=getattr(intent, "need_memory_recall", False),
                )
                if cloud_mem:
                    payload.update(cloud_mem)
        else:
            payload.update(
                recent_messages=[],
                rolling_summary="",
                session_enabled=False,
                interaction_chains={},
            )
            payload.pop("owner_notebook_snapshot", None)
            payload.pop("owner_core_memory", None)
            payload.pop("owner_recalled_memories", None)
            payload.pop("近期群聊巡逻记忆（只读背景）", None)
        if intent == "ignore":
            if (
                not sandbox
                and message.platform == "onebot"
                and message.kind == "group"
                and not message.addressed
                and not self.bot_guard.known_bot(message)
                and (not aff_active or caps.allow_emoji_like)
            ):
                r_score = getattr(intent, "reply_score", None)
                r_thresh = getattr(intent, "reply_threshold", None)
                if (
                    isinstance(r_score, (int, float))
                    and isinstance(r_thresh, (int, float))
                    and max(0.20, r_thresh * 0.55) <= r_score < r_thresh
                ):
                    scope_key = self.store.session_scope(message)
                    now_ts = time.time()
                    if now_ts - self._lurk_react_at.get(scope_key, 0.0) >= 180:
                        emoji_id = self._pick_lurk_emoji(members)
                        target_id = next(
                            (
                                m.event_id
                                for m in reversed(members)
                                if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)
                            ),
                            None,
                        )
                        if emoji_id and target_id:
                            self._lurk_react_at[scope_key] = now_ts
                            if len(self._lurk_react_at) > 1024:
                                self._lurk_react_at.pop(next(iter(self._lurk_react_at)))
                            await self.store.enqueue_reply(
                                job["id"],
                                message,
                                "",
                                session_epoch=session["epoch"],
                                participants=members,
                                allowed_at_qqs=set(),
                                audit_id=audit_id,
                                actions={
                                    "abstain": True,
                                    "bubbles": [],
                                    "anchor": {"reply_id": target_id, "mentions": []},
                                    "sticker": {"label": None, "timing": "after_text"},
                                    "image_digest": None,
                                    "emoji_like": emoji_id,
                                    "poke_qq": None,
                                    "save_meme_label": None,
                                    "set_longnick": None,
                                    "folded": None,
                                    "watermark": watermark,
                                    "source_ids": [m.event_id for m in members],
                                    "public": True,
                                },
                                consumed_image_events=[],
                                consumed_file_events=[],
                            )
                            await self.store.runtime.filters.update_audit(audit_id, "queued")
            return
        invited_bots = set()
        if not sandbox and len(members) == 1 and owner(message, self.s):
            invited_bots = await self.bot_guard.invite(message)
        if self.bot_guard.known_bot(message) and await self.bot_guard.allowed_reply_target(
            message, message.user_id
        ):
            invited_bots.add(message.user_id)
        filtered_nearby = []
        image_nearby = []
        history_events: set[str] = set()
        selected_neighbour_members = []
        neighbour_image_events = []
        late_images = False
        digested_events = {
            match.group(1)
            for entry in history
            if (match := re.match(r"^\[附图摘要，消息([^：\]]+)：", str(entry.get("text", ""))))
        }
        current_events = {m.event_id for m in members}
        if message.kind == "group" and not sandbox:
            nearby = await self.store.groups.previous_messages(message, members, limit=6)
            consumed_events = await self.store.groups.consumed_image_events(message)
            history_events = {item.get("event_id") for item in history if item.get("event_id")}
            filtered_nearby = [
                m
                for m in nearby
                if m.event_id not in history_events and m.event_id not in current_events
            ]
            image_nearby = [
                m
                for m in filtered_nearby
                if m.event_id not in digested_events and m.event_id not in consumed_events
            ]
            neighbour_members[:] = filtered_nearby
            nearby_context = [
                {"sender": self.image_sender(m), "text": m.text[:160]} for m in filtered_nearby
            ]
            if nearby_context:
                payload["nearby_group_context"] = nearby_context
            neighbour_images, neighbour_sources = [], []
            if img_limit > 0 and len(images) < img_limit and image_nearby:
                neighbour_images, neighbour_sources = await self.message_images(
                    message,
                    [],
                    image_nearby,
                    limit=img_limit - len(images),
                    include_current=False,
                    selected_members=selected_neighbour_members,
                )
                if aff_active and caps.low_res_images_only and neighbour_images:
                    neighbour_images = [
                        dataclasses.replace(i, resolution="low") for i in neighbour_images
                    ]
            await message_guard()
            if neighbour_images:
                late_images = True
                # Adjacent users' images remain an independent strict SOURCE gate.
                await self.ai.screen(
                    {"image_sources": neighbour_sources, "nearby_group_context": nearby_context},
                    job["subject"],
                    purpose="source",
                    message=message,
                    images=[i.gate() for i in neighbour_images],
                    before_call=message_guard,
                )
                neighbour_image_events = [
                    m.event_id for m in selected_neighbour_members if m.event_id
                ]
                for source in neighbour_sources:
                    if "image_number" in source:
                        source = {**source, "image_number": source["image_number"] + len(images)}
                    image_sources.append(source)
                images.extend(neighbour_images)
                if neighbour_sources:
                    payload["image_sources"] = image_sources
            payload["context_status"].extend(
                {"event_id": m.event_id, "states": m.context_status}
                for m in filtered_nearby
                if m.context_status
            )
        if img_limit > 0 and not sandbox and not images and PRIOR_IMAGE_REF_RE.search(message.text):
            candidates = self.hot.recent_with_images(
                message,
                exclude_events={*current_events, *(m.event_id for m in image_nearby)},
            )
            valid_candidates = []
            for item in candidates:
                if not await self.recalled([item]):
                    valid_candidates.append(item)
            if valid_candidates:
                mentioned_peers = {
                    uid for m in members for uid in m.mentioned_ids if uid != self.s.onebot_self_id
                }
                tier_hint = getattr(intent, "tier", 2)

                def _recall_rank(idx_item):
                    idx, item = idx_item
                    speaker_match = (item.user_id in mentioned_peers) or bool(
                        item.sender_nickname and item.sender_nickname in message.text
                    )
                    undigested = item.event_id not in digested_events
                    return (
                        1 if speaker_match else 0,
                        1 if (tier_hint < 4 and undigested) else 0,
                        idx,
                    )

                ordered_candidates = [
                    item for _, item in sorted(enumerate(valid_candidates), key=_recall_rank)
                ]
                recalled_members.clear()
                recalled_images, recalled_sources = await self.message_images(
                    message,
                    [],
                    ordered_candidates,
                    limit=img_limit,
                    include_current=False,
                    nearby_origin="历史消息附图",
                    selected_members=recalled_members,
                )
                if aff_active and caps.low_res_images_only and recalled_images:
                    recalled_images = [
                        dataclasses.replace(i, resolution="low") for i in recalled_images
                    ]
                await message_guard()
                if recalled_images:
                    late_images = True
                    neighbour_members.extend(recalled_members)
                    await self.ai.screen(
                        {"image_sources": recalled_sources, "historical_image_recall": True},
                        job["subject"],
                        purpose="source",
                        message=message,
                        images=[i.gate() for i in recalled_images],
                        before_call=message_guard,
                    )
                    image_sources.extend(recalled_sources)
                    images.extend(recalled_images)
                    payload["image_sources"] = image_sources
        file_attachments, file_sources, consumed_file_events = [], [], []
        if (
            not sandbox
            and not self.bot_guard.known_bot(message)
            and (not aff_active or caps.allow_file_read)
        ):
            file_attachments, file_sources, consumed_file_events = await self._load_turn_files(
                message, members, filtered_nearby, history_events, current_events
            )
            await message_guard()
            if file_sources:
                payload["file_sources"] = file_sources
            if file_attachments:
                await self.ai.screen(
                    {"file_sources": file_sources, "file_attachments": file_attachments},
                    job["subject"],
                    purpose="source",
                    message=message,
                    before_call=message_guard,
                )
                payload["file_attachments"] = file_attachments
        if not sandbox:
            related_user_ids = [
                *(m.user_id for m in members),
                *(uid for m in members for uid in m.mentioned_ids if uid != self.s.onebot_self_id),
                *(m.user_id for m in filtered_nearby),
                *(str(item.get("user_id", "")) for item in history[-8:]),
            ]
            dual_data = await self.sessions.dual.prefetch(
                message,
                message.text,
                recall=getattr(intent, "need_memory_recall", False),
                related_user_ids=related_user_ids,
            )
            if aff_active and not caps.allow_dual_profile and isinstance(dual_data, dict):
                dual_data = {
                    k: v
                    for k, v in dual_data.items()
                    if k not in {"speaker_profile", "member_profiles"}
                }
            payload["dual_memory_data"] = dual_data
            if aff_active:
                sender_prof = await aff.profile(
                    message.platform, message.user_id, message.sender_nickname
                )
                payload["sender_affection"] = {
                    "score": sender_prof["score"],
                    "lv": sender_prof["lv"],
                    "tier": sender_prof["tier"],
                    "attitude": sender_prof["attitude"],
                }
                batch_aff: dict[str, dict] = {}
                for m in members:
                    if m.user_id in batch_aff or self.bot_guard.known_bot(m):
                        continue
                    m_prof = (
                        sender_prof
                        if (m.user_id == message.user_id and m.platform == message.platform)
                        else await aff.profile(m.platform, m.user_id, m.sender_nickname)
                    )
                    batch_aff[m.user_id] = {
                        "score": m_prof["score"],
                        "lv": m_prof["lv"],
                        "tier": m_prof["tier"],
                        "attitude": m_prof["attitude"],
                    }
                if batch_aff:
                    payload["batch_affection"] = batch_aff
            if not self.bot_guard.known_bot(message) and hasattr(
                self.bili_watcher, "recent_surfed"
            ):
                surfed = await self.bili_watcher.recent_surfed(limit=4)
                if surfed:
                    payload["recent_bili_surfed"] = surfed
            if not self.bot_guard.known_bot(message) and hasattr(self, "world_state"):
                ws_summary = await self.world_state.chat_sensor_payload()
                if ws_summary:
                    payload["world_state_summary"] = ws_summary
        context_members = list(members)
        if message.kind == "group":
            context_members.extend(neighbour_members if images else [])
            for entry in history:
                if entry.get("role") == "user" and re.fullmatch(
                    r"[1-9][0-9]{0,19}", str(entry.get("user_id", ""))
                ):
                    context_members.append(
                        message.model_copy(
                            update={
                                "user_id": entry["user_id"],
                                "sender_nickname": entry.get("speaker", "")[:80],
                                "text": entry["text"][:8000],
                                "event_id": entry.get("event_id", "history"),
                                "mentioned_ids": [],
                                "command_text": "",
                                "forward_ids": [],
                                "images": [],
                                "files": [],
                            }
                        )
                    )
        allowed_at = valid_at_targets(message, context_members, self.s, invited_bots=invited_bots)
        if sandbox:
            allowed_at = set()
        if allowed_at:
            payload["allowed_at_qqs"] = sorted(allowed_at)
        parsed = None
        meme_tag = None
        public = not (message.kind == "private" and is_persona_owner(message, self.s))
        is_watch_cmd = bool(re.search(r"(?:^|\s)/watch-video(?:\s|$)", direct))
        allow_video = (
            not sandbox
            and not self.bot_guard.known_bot(message)
            and can_watch_video(message, members)
            and (
                not aff_active
                or (caps.allow_video_watch if is_watch_cmd else caps.allow_bili_summary)
            )
        )
        bvid = (
            (message.video_ids[0] if message.video_ids else extract_bvid(message.text))
            if allow_video
            else None
        )
        if allow_video and not bvid and message.short_urls:
            bvid = await self.bili.resolve_short(message.short_urls[0])
        if bvid:
            # Only a validated, admitted video request gets a longer processing deadline.
            # Ordinary chat retains its existing replay/freshness window.
            deadline = message.created_at + (
                900 if self.video_watch.config.enabled else self.s.event_ttl_seconds
            )
            if self.video_watch.config.enabled:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE jobs SET expires=? WHERE id=? AND status='working'",
                        (deadline, job["id"]),
                    )

            async def video_guard():
                if not self.batch_authorized(message, members) or time.time() >= deadline:
                    raise Rejected("video_watch_invalidated")
                if (await self.sessions.snapshot(message))["epoch"] != session["epoch"]:
                    raise Rejected("video_watch_invalidated")

            await video_guard()
            video = await self.bili.video(bvid)
            material = await self.bili_watcher.public_material(video, guard=video_guard)
            if (
                not sandbox
                and not self.bot_guard.known_bot(message)
                and hasattr(self.bili_watcher, "lookup_watched")
            ):
                prior = await self.bili_watcher.lookup_watched(bvid)
                if prior:
                    material = {**material, "patrol_deja_vu": prior}
            if not aff_active or caps.allow_video_watch:
                analysis = await self.video_watch.analyze(
                    bvid,
                    material,
                    job["subject"],
                    "chat",
                    deep=deep,
                    message=message,
                    guard=video_guard,
                )
                text = filter_reply_links(analysis.text, public=public)
            else:
                mat_payload = self.video_watch.bounded(material, direct, patrol=False)
                await self.ai.screen(
                    mat_payload,
                    job["subject"],
                    purpose="source",
                    budget_lane="chat",
                    message=message,
                    before_call=video_guard,
                )
                await video_guard()
                summary_text = await self.ai.generate(
                    mat_payload,
                    job["subject"],
                    summary=True,
                    deep=deep,
                    budget_lane="chat",
                    message=message,
                    before_call=video_guard,
                )
                text = filter_reply_links(summary_text, public=public)
            result = self.output(text)
            # Sources and generated summaries are deliberately excluded from conversation memory.
        else:
            if (
                not sandbox
                and not self.bot_guard.known_bot(message)
                and (not aff_active or caps.allow_search)
                and (
                    explicit_search(members[-1].command_text or members[-1].text)
                    or getattr(intent, "need_external_lookup", False)
                )
            ):
                controller = self.store.runtime.web_search
                if not controller.config.enabled:
                    payload["web_search_status"] = "disabled"
                    self.record("web_search_disabled")
                else:
                    try:
                        query = search_query(
                            members[-1],
                            self.s,
                            batch_messages=members,
                            link_titles=[
                                p["title"]
                                for p in previews
                                if isinstance(p, dict) and p.get("title")
                            ],
                        )
                    except Rejected:
                        query = None
                        payload["web_search_status"] = "skipped_query"
                        self.record("web_search_skipped_query")
                    if query is not None:
                        self.store.groups.phase(message, "generating")
                        batch = self.store.groups.active.get(self.store.session_scope(message))
                        payload["web_search_status"] = "unavailable"
                        try:
                            findings = await self.ai.search(
                                query,
                                job["subject"],
                                before_call=message_guard,
                                cancel_event=batch.cancel if batch else None,
                            )
                            if findings and findings.strip() and controller.config.enabled:
                                payload["web_search_findings"] = findings
                                payload["web_search_status"] = "grounded"
                            else:
                                if not controller.config.enabled:
                                    payload["web_search_status"] = "disabled"
                                self.record("web_search_empty")
                        except Rejected as exc:
                            if str(exc) in {
                                "reply_invalidated",
                                "batch_interrupted",
                                "batch_recalled",
                                "budget_reservation_exceeded",
                            }:
                                raise
                            if str(exc) == "web_search_disabled":
                                payload["web_search_status"] = "disabled"
                            self.record("web_search_failed")
                        await message_guard()
            self.store.groups.phase(message, "generating")
            batch = self.store.groups.active.get(self.store.session_scope(message))
            chat_route = intent
            tier = getattr(intent, "tier", 2)
            if (
                payload.get("web_search_findings")
                or file_attachments
                or late_images
                or any(i.resolution == "high" for i in images)
            ) and tier < 2:
                tier = 2
            if aff_active:
                tier = min(tier, caps.max_tier)
                if not caps.allow_carbon and tier >= 4:
                    tier = 3
            if isinstance(intent, RouteDecision) and tier != getattr(intent, "tier", 2):
                chat_route = RouteDecision(
                    str(intent),
                    tier=tier,
                    sandbox_mode=intent.sandbox_mode,
                    need_memory_recall=intent.need_memory_recall,
                    need_external_lookup=intent.need_external_lookup,
                    reply_score=getattr(intent, "reply_score", None),
                    reply_threshold=getattr(intent, "reply_threshold", None),
                )
            if tier >= 3 and hasattr(self.sender, "quick_ack"):
                self.background(self.quick_ack(job, message, message_guard))
            if (
                not sandbox
                and message.platform == "onebot"
                and message.kind == "private"
                and hasattr(self.sender, "typing")
            ):
                await self.sender.typing(message)
            gen_trace = {}
            try:
                generated = await self.ai.generate(
                    payload,
                    job["subject"],
                    scenario=Scenario.OWNER_PRIVATE
                    if message.kind == "private" and is_persona_owner(message, self.s)
                    else Scenario.FAST_CHAT,
                    deep=deep,
                    route=chat_route,
                    message=message,
                    before_call=message_guard,
                    cancel_event=batch.cancel if batch else None,
                    generation_trace=gen_trace,
                    **({"images": images} if images else {}),
                )
            except BaseException:
                if gen_trace.get("chain_invalidate"):
                    await self.sessions.invalidate_chains(message)
                raise
            finally:
                if gen_trace.get("thinking_level"):
                    await self.store.runtime.filters.record_generation(
                        audit_id,
                        thinking_level=gen_trace.get("thinking_level"),
                        model_lane=gen_trace.get("model_lane"),
                    )
            await message_guard()
            allowed_reply = payload["allowed_reply_ids"]
            payload.clear()
            images.clear()
            if generated.lstrip().startswith("{"):
                allowed_poke = (
                    set()
                    if sandbox
                    else set(allowed_at)
                    if message.kind == "group"
                    else {message.user_id}
                )
                actions, folded = parse_actions(
                    generated,
                    public=public,
                    allowed_at=allowed_at,
                    allowed_reply=allowed_reply,
                    labels=self.memes.labels,
                    settings=self.s,
                    tier=tier,
                    allowed_poke=allowed_poke,
                    allow_longnick=(not sandbox and is_persona_owner(message, self.s)),
                    allow_sticker=(not aff_active or caps.allow_sticker),
                    allow_emoji_like=(not aff_active or caps.allow_emoji_like),
                    allow_poke_action=(
                        not aff_active or (caps.allow_poke and caps.allow_poke_inbound)
                    ),
                )
                aff_target, aff_delta, aff_reason = self._select_affection_target(
                    message, members, actions
                )
                update = (
                    await aff.apply_turn_delta(
                        aff_target, aff_delta, sandbox_mode=sandbox, reason=aff_reason
                    )
                    if aff_active
                    else None
                )
                farewell_bypass = bool(update and update.blacklisted_now)
                if farewell_bypass:
                    await self._alert_affection_blacklisted(aff_target, update)
                    if not actions.anchor.reply_id and aff_target.event_id in allowed_reply:
                        actions.anchor.reply_id = aff_target.event_id
                    if actions.abstain or not any(b.strip() for b in actions.bubbles):
                        actions.abstain = False
                        actions.bubbles = [update.farewell_message or aff.config.farewell_message]
                        actions.sticker.label = None
                        actions.emoji_like = None
                        actions.poke_qq = None
                        folded = None
                if not sandbox and not actions.abstain and not farewell_bypass:
                    if actions.save_meme_label and not self.bot_guard.known_bot(message):
                        await self._auto_save_meme(
                            message, [*neighbour_members, *members], actions.save_meme_label
                        )
                    if actions.set_longnick and is_persona_owner(message, self.s):
                        try:
                            await self.longnick.update(actions.set_longnick, automatic=False)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass
                allow_text_card = not aff_active or caps.allow_text_card
                if actions.abstain:
                    if (
                        not sandbox
                        and message.platform == "onebot"
                        and (actions.emoji_like or actions.poke_qq)
                    ):
                        await self.store.enqueue_reply(
                            job["id"],
                            message,
                            "",
                            session_epoch=session["epoch"],
                            participants=members,
                            allowed_at_qqs=allowed_at,
                            audit_id=audit_id,
                            actions={
                                **actions.model_dump(),
                                "bubbles": [],
                                "folded": None,
                                "watermark": watermark,
                                "source_ids": [m.event_id for m in members],
                                "public": public,
                                "allow_text_card": allow_text_card,
                                **({"farewell_bypass_mute": True} if farewell_bypass else {}),
                            },
                            consumed_image_events=neighbour_image_events,
                            consumed_file_events=consumed_file_events,
                        )
                        await self.store.runtime.filters.update_audit(audit_id, "queued")
                        return
                    if gen_trace.get("chain_invalidate"):
                        await self.sessions.invalidate_chains(message)
                    await self.store.runtime.filters.update_audit(
                        audit_id, "abstained", "model_abstain"
                    )
                    return
                if not actions.image_digest:
                    actions.image_digest = next(
                        (
                            v["local_sticker_label"]
                            for v in image_sources
                            if v.get("local_sticker_label")
                        ),
                        None,
                    )
                if actions.image_digest:
                    check_memory_text(actions.image_digest, self.s)
                chain_commit = gen_trace.get("chain_commit")
                await self.store.enqueue_reply(
                    job["id"],
                    message,
                    "\n".join(actions.bubbles),
                    session_epoch=session["epoch"],
                    participants=members,
                    allowed_at_qqs=allowed_at,
                    audit_id=audit_id,
                    actions={
                        **actions.model_dump(),
                        "folded": folded,
                        "watermark": watermark,
                        "source_ids": [m.event_id for m in members],
                        "public": public,
                        "allow_text_card": allow_text_card,
                        **({"farewell_bypass_mute": True} if farewell_bypass else {}),
                        **(
                            {
                                "chain_commit": {
                                    **chain_commit,
                                    "source_ids": [m.event_id for m in members],
                                }
                            }
                            if chain_commit
                            else {}
                        ),
                    },
                    consumed_image_events=neighbour_image_events,
                    consumed_file_events=consumed_file_events,
                )
                await self.store.runtime.filters.update_audit(audit_id, "queued")
                return
            # Read compatibility for integrations returning the old single-text contract.
            # The production Gemini gateway validates the new schema before reaching here.
            parsed = parse_reply(generated, message, self.s, allowed_at_qqs=allowed_at)
            generated, meme_tag = parsed.text, parsed.meme_tag
            result = self.output(generated) if generated else ""
        if (await self.sessions.snapshot(message))["epoch"] != session["epoch"]:
            await self.store.runtime.filters.update_audit(audit_id, "cancelled", "epoch_changed")
            return
        if parsed is None:
            parsed = parse_reply(result, message, self.s, allowed_at_qqs=allowed_at)
            result, meme_tag = parsed.text, parsed.meme_tag
        if not result and not (meme_tag and meme_tag in self.memes.labels):
            await self.store.runtime.filters.update_audit(audit_id, "cancelled", "empty_reply")
            return
        update = (
            await aff.apply_turn_delta(message, 0, sandbox_mode=sandbox) if aff_active else None
        )
        if update is not None and update.blacklisted_now:
            await self._enqueue_affection_farewell(
                job, message, members, session, watermark, audit_id, update
            )
            await self._alert_affection_blacklisted(message, update)
            return
        await self.store.enqueue_reply(
            job["id"],
            message,
            self.output(result) if result else "",
            session_epoch=session["epoch"],
            meme_tag=meme_tag,
            meme_eligible=public and message.platform == "onebot",
            participants=members if len(members) > 1 else None,
            segments=parsed.segments,
            allowed_at_qqs=allowed_at,
            audit_id=audit_id,
            consumed_image_events=neighbour_image_events,
            consumed_file_events=consumed_file_events,
        )
        await self.store.runtime.filters.update_audit(audit_id, "queued")

    async def handle_patrol(self, job: dict):
        sub_id = job["payload"]["subscription"]
        subs = await self.store.subscriptions(job["subject"])
        sub = next((s for s in subs if s["id"] == sub_id), None)
        if not sub:
            return
        target = Message.model_validate(sub["target"])
        if not authorize(target, self.s) or not owner(target, self.s) or target.kind != "private":
            return
        videos = [Video.model_validate(v) for v in job["payload"]["videos"]]
        result = await self.summarize(
            videos,
            job["subject"],
            "patrol",
            message=target if is_persona_owner(target, self.s) else None,
            patrol_notify=True,
        )
        # Recheck unsubscribe requests that arrived while the model was running.
        if not any(s["id"] == sub_id for s in await self.store.subscriptions(job["subject"])):
            return
        await self.store.save_report(job["subject"], result)
        if self.diary:
            await self.diary.patrol(job["id"], result)
        if is_persona_owner(target, self.s) and self.proactive.config.enabled:
            await self.proactive.enqueue("patrol", result, key=job["id"])

    async def process(self, job: dict):
        try:
            if job["kind"] == "message":
                await self.handle_message(job)
            else:
                await self.handle_patrol(job)
        except Rejected as exc:
            audit_id = job.get("_audit_id")
            if str(exc) == "batch_interrupted":
                await self.store.runtime.filters.update_audit(
                    audit_id, "interrupted", "batch_interrupted"
                )
                await self.store.groups.requeue_interrupted(job)
                return
            if str(exc) in {"batch_recalled", "reply_invalidated", "video_watch_invalidated"}:
                await self.store.runtime.filters.update_audit(audit_id, "cancelled", str(exc))
                await self.store.finish(job["id"], "cancelled")
                return
            self.record(str(exc), emit_audit=not exc.audited)
            await self.store.runtime.filters.update_audit(audit_id, "rejected", exc.audit_detail)
            if self.diary and str(exc) == "prompt_injection":
                await self.diary.injection(job["id"])
            if job["kind"] == "message":
                message = Message.model_validate(job["payload"])
                if message.kind == "private" and authorize(message, self.s):
                    notice = None
                    if str(exc) in {
                        "gemini_unavailable",
                        "gemini_circuit_open",
                        "generation_incomplete",
                        "invalid_action_output",
                    }:
                        notice = "唔，我这边刚刚卡了一下，等会儿再喊我试试~"
                    elif str(exc) in {
                        "budget_exhausted",
                        "budget_hard_stop",
                        "budget_circuit_open",
                        "budget_reservation_exceeded",
                        "daily_budget",
                        "monthly_budget",
                        "total_budget",
                        "user_budget",
                        "lane_budget",
                    }:
                        notice = "唔，今天的额度刚好用完啦，明天再接着聊吧~"
                    # Safety, permission and context rejections stay silent.
                    if notice:
                        await self.store.enqueue_reply(job["id"], message, notice)
            await self.store.finish(job["id"], "rejected")
        except Exception:
            self.record("job_failed")
            await self.store.runtime.filters.update_audit(
                job.get("_audit_id"), "failed", "job_failed"
            )
            await self.store.finish(job["id"], "failed")
        else:
            await self.store.finish(job["id"])
            self.counters["job_done"] += 1

    async def worker(self):
        while True:
            try:
                job = await self.store.claim()
                if job:
                    await self.process(job)
                else:
                    await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.record("worker_error")
                await asyncio.sleep(1)

    async def quick_ack(self, job, message, guard):
        try:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT at FROM local_quick_acks WHERE scope=?", (job["scope"],)
                )
                if row and time.time() - row[0] < 60:
                    return
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO local_quick_acks VALUES (?,?)",
                    (job["scope"], time.time()),
                )

            async def valid():
                try:
                    await guard()
                    active = self.store.groups.active.get(job["scope"])
                    return active is not None and not active.cancel.is_set()
                except Rejected:
                    return False

            phrases = (
                (
                    "等我一下下，我仔细看看~",
                    "稍等哦，我帮你理一下~",
                    "唔，这个问题我想仔细点，等我几秒~",
                )
                if message.kind == "private" and is_persona_owner(message, self.s)
                else ("稍等我捋一下", "等下，我看眼细节", "让我想想怎么讲", "稍等，我看下上下文")
            )
            receipt = await self.sender.quick_ack(
                message, random.choice(phrases), before_send=valid
            )
            if receipt:
                await self.store.runtime.attention.record_sent(message, receipt)
        except Exception:
            self.record("quick_ack_unavailable")

    async def _finish_delivery_audit(self, reply: dict, status: str, detail: str = ""):
        audit_id = reply.get("audit_id")
        if audit_id:
            await self.store.runtime.filters.update_audit(audit_id, status, detail)

    async def deliver_once(self) -> bool:
        reply = await self.store.claim_reply()
        if not reply:
            return False
        reply_actions = reply.get("actions") or {}
        message = Message.model_validate(reply["message"])
        farewell_bypass = bool(reply_actions.get("farewell_bypass_mute")) and (
            message.platform != "onebot"
            or await self.moderation.is_affection_muted(message.user_id)
        )
        if reply_actions and not (
            bool(reply_actions.get("farewell_bypass_mute"))
            and "bubbles" not in reply_actions
            and reply.get("text")
        ):
            from .outgoing import deliver_actions

            return await deliver_actions(self, reply)
        status = "sent"
        text_confirmed = False
        participants = [Message.model_validate(m) for m in reply.get("participants", [])]
        for original in participants or [message]:
            if self.bot_guard.known_bot(original):
                await self.bot_guard.prepare(original)
        delivery_epoch = (await self.sessions.snapshot(message))["epoch"]
        if "session_epoch" in reply and delivery_epoch != reply["session_epoch"]:
            await self.store.finish_reply(reply["id"], "cancelled")
            await self._finish_delivery_audit(reply, "cancelled", "epoch_changed")
            return True
        if not await self.bot_guard.may_process(message, reply["id"]):
            await self.store.finish_reply(reply["id"], "blocked")
            await self._finish_delivery_audit(reply, "blocked", "bot_cooldown")
            return True
        try:
            with farewell_send_bypass(message) if farewell_bypass else nullcontext():
                await self.bot_guard.extend_cooldown(message, reply["id"], before_send=True)

                async def valid_after_delay():
                    async with self.store.lock:
                        row = await self.store.one(
                            "SELECT status FROM outbox WHERE id=?", (reply["id"],)
                        )
                    if not row or row[0] != "sending":
                        return False
                    if (
                        not reply["proactive"]
                        and time.time() - message.created_at > self.s.event_ttl_seconds
                    ):
                        return False
                    if any(
                        self.hot.recalled(m)
                        or any(self.hot.recalled(m, ref) for ref in m.reply_ids)
                        for m in participants or [message]
                    ):
                        return False
                    if not await self.bot_guard.may_process(message, reply["id"]):
                        return False
                    bypass_uid = None
                    if farewell_bypass and (
                        message.platform != "onebot"
                        or await self.moderation.is_affection_muted(message.user_id)
                    ):
                        bypass_uid = message.user_id
                    return (
                        (await self.sessions.snapshot(message))["epoch"] == delivery_epoch
                    ) and self.batch_authorized(
                        message, participants or [message], bypass_mute_user_id=bypass_uid
                    )

                receipt = None
                image_confirmed = False

                async def current_at_targets():
                    targets = set(reply.get("allowed_at_qqs", []))
                    for target in list(targets & self.s.bot_qq_ids):
                        if not await self.bot_guard.allowed_reply_target(message, target):
                            targets.discard(target)
                    return targets - {
                        self.s.onebot_self_id,
                        "all",
                        "0",
                        *getattr(self.s, "muted_user_ids", set()),
                    }

                if (
                    reply.get("segments")
                    and any(s["type"] == "at" for s in reply["segments"])
                    and hasattr(self.sender, "send_segments")
                ):
                    receipt = await self.sender.send_segments(
                        message,
                        reply["segments"],
                        targets=current_at_targets,
                        before_send=valid_after_delay,
                    )
                elif reply["text"]:
                    receipt = await self.sender.send(
                        message,
                        self.output(reply["text"]),
                        reply["proactive"],
                        before_send=valid_after_delay,
                    )
            if message.platform == "onebot" and reply["text"]:
                from .outbound_actions import valid_receipt

                if not valid_receipt(receipt):
                    raise Rejected("onebot_send_unconfirmed")
            text_confirmed = (
                message.platform == "onebot"
                and isinstance(receipt, dict)
                and not isinstance(receipt.get("message_id"), bool)
                and bool(re.fullmatch(r"-?[0-9]{1,30}", str(receipt.get("message_id", ""))))
            )
            if text_confirmed:
                retained_text = reply["text"]
                await self.sessions.reply(
                    message, retained_text, reply["id"], expected_epoch=delivery_epoch
                )

                async def remember_sent():
                    if await self.store.runtime.attention.record_sent(message, receipt):
                        await self.store.runtime.filters.remember_group(
                            message.model_copy(
                                update={
                                    "event_id": "sent:" + str(receipt["message_id"]),
                                    "user_id": message.bot_id,
                                    "text": "[哈基米已回复] " + self.output(retained_text),
                                    "created_at": time.time(),
                                }
                            )
                        )

                retained = await self.sessions.receive_outgoing(
                    message,
                    receipt["message_id"],
                    retained_text,
                    expected_epoch=delivery_epoch,
                    on_confirmed=remember_sent,
                )
                if not retained:
                    await self.store.finish_reply(reply["id"], "cancelled")
                    await self._finish_delivery_audit(reply, "cancelled", "epoch_changed")
                    return True
            if reply.get("meme_eligible") and not reply["proactive"] and await valid_after_delay():
                allowed = await self.memes.reserve_reply(
                    self.store.session_scope(message), reply["id"], reply.get("meme_tag")
                )
                if allowed:
                    try:
                        await asyncio.sleep(0.6)
                        data = await self.memes.get_base64(reply["meme_tag"])

                        async def image_still_allowed():
                            return (
                                await valid_after_delay()
                                and await self.memes.get_base64(reply["meme_tag"]) == data
                            )

                        if data and await image_still_allowed():
                            image_receipt = await self.sender.send_meme(
                                message, data, before_send=image_still_allowed
                            )
                            image_confirmed = True  # Sender requires a valid OneBot message_id.
                            await self.sessions.receive_outgoing(
                                message,
                                image_receipt["message_id"],
                                "[本地表情：" + reply["meme_tag"] + "]",
                                expected_epoch=delivery_epoch,
                                on_confirmed=lambda: self.store.runtime.attention.record_sent(
                                    message, image_receipt
                                ),
                            )
                            if image_confirmed and not reply["text"]:
                                await self.sessions.reply(
                                    message,
                                    "[本地表情：" + reply["meme_tag"] + "]",
                                    reply["id"],
                                    expected_epoch=delivery_epoch,
                                )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.record("meme_send_failed")
            if not reply["text"] and not image_confirmed:
                status = "cancelled"
        except asyncio.CancelledError:
            # Leave 'sending': startup recovery will mark it uncertain without resending.
            raise
        except Exception:
            status = "uncertain"
            self.record("delivery_uncertain")
        await self.bot_guard.extend_cooldown(message, reply["id"])
        if not await self.store.finish_reply(reply["id"], status):
            await self._finish_delivery_audit(reply, "cancelled", "session_reset")
            return True  # A reset cancelled this delivery while the transport awaited its ACK.
        await self._finish_delivery_audit(
            reply, status, "delivery_uncertain" if status == "uncertain" else ""
        )
        if (
            status == "sent"
            and text_confirmed
            and not reply["proactive"]
            and message.kind == "group"
        ):
            await self.public_memory.record(
                reply["id"],
                message,
                participants or [message],
                reply["text"],
                expected_epoch=delivery_epoch,
            )
        if status == "sent" and text_confirmed and not reply["proactive"] and self.diary:
            await self.diary.bot_reply(reply["id"], message, reply["text"])
        if status == "sent" and not reply["proactive"] and "session_epoch" in reply:
            if reply.get("consumed_image_events"):
                await self.store.groups.mark_images_consumed(
                    message, reply["consumed_image_events"]
                )
            if reply.get("consumed_file_events"):
                await self.store.groups.mark_files_consumed(message, reply["consumed_file_events"])
            await self.proactive.note_activity(message)
            self.store.runtime.filters.spoke(message)
            self.compressor.schedule(message)
        return True

    async def delivery(self):
        while True:
            try:
                sent = await self.deliver_once()
                await asyncio.sleep(2 if sent else 0.25)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.record("delivery_worker_error")
                await asyncio.sleep(2)

    async def poll_once(self, *, force=False):
        if self.poll_lock.locked():
            return
        async with self.poll_lock:
            await self._poll_once(force=force)

    async def _poll_once(self, *, force=False):
        if not self.s.bilibili_enabled or (not force and self.store.runtime.is_resting()):
            return
        for sub in await self.store.subscriptions():
            if not force and sub["next_poll"] > time.time():
                continue
            target = Message.model_validate(sub["target"])
            if not authorize(target, self.s) or not owner(target, self.s):
                continue
            # Persist the next poll *before* network I/O, so restarts cannot create a tight loop.
            await self.store.poll_started(
                sub["id"], time.time() + self.s.patrol_interval_seconds + random.uniform(0, 60)
            )
            try:
                videos = await self.bili.updates(sub["mid"])
                await self.store.ingest(sub["id"], videos)
            except Exception:
                self.record("patrol_source_unavailable")
        async with self.store.lock:
            rooms = await self.store.all(
                "SELECT room_id,target,last_status,next_poll FROM live_watches"
            )
        for room in rooms:
            if not force and room["next_poll"] > time.time():
                continue
            target = Message.model_validate(
                self.store.vault.open("live:" + room["room_id"], room["target"])
            )
            if not authorize(target, self.s) or not owner(target, self.s):
                continue
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE live_watches SET next_poll=? WHERE room_id=?",
                    (time.time() + self.s.patrol_interval_seconds, room["room_id"]),
                )
            try:
                info = (
                    await self.bili.live_info(room["room_id"])
                    if hasattr(self.bili, "live_info")
                    else {
                        "room_id": room["room_id"],
                        "live_status": await self.bili.live_status(room["room_id"]),
                        "title": "",
                        "area": "",
                    }
                )
                status = info["live_status"]
                prev_status = room["last_status"]
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE live_watches SET last_status=? WHERE room_id=?",
                        (status, room["room_id"]),
                    )
                if prev_status is not None and prev_status != 1 and status == 1:
                    await self._notify_live_open(room["room_id"], target, info)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.record("patrol_source_unavailable")

    async def _notify_live_open(self, room_id: str, target: Message, info: dict) -> None:
        raw_title = str(info.get("title", "") or "").strip()
        raw_area = str(info.get("area", "") or "").strip()
        safe_title, safe_area = "", ""
        if raw_title or raw_area:
            try:
                await self.ai.screen(
                    {"room_id": room_id, "title": raw_title, "area": raw_area},
                    self.store.subject(target),
                    purpose="source",
                    budget_lane="patrol",
                )
                safe_title = redact_public_summary(safe_output(raw_title, self.secrets, limit=80))
                safe_area = redact_public_summary(safe_output(raw_area, self.secrets, limit=30))
            except asyncio.CancelledError:
                raise
            except Exception:
                safe_title, safe_area = "", ""
        detail = (
            f"今天播的是《{safe_title}》" + (f"（{safe_area}）" if safe_area else "") + "，"
            if safe_title
            else ""
        )
        live_text = self.output(
            f"诶！你蹲的直播间 {room_id} 刚开播啦~ {detail}快去瞄一眼喵！\n"
            f"https://live.bilibili.com/{room_id}"
        )
        await self.store.save_report(self.store.subject(target), live_text)
        if (
            target.kind == "private"
            and is_persona_owner(target, self.s)
            and self.proactive.config.enabled
        ):
            live_key = self.store.vault.digest("live-open", room_id, int(time.time() // 1800))
            try:
                await self.proactive.enqueue(
                    "patrol", live_text, key=live_key, expires=time.time() + 7200
                )
            except asyncio.CancelledError:
                raise
            except Rejected:
                pass

    async def gcp_billing_worker(self):
        while True:
            try:
                await self.store.runtime.gcp_billing.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(30)

    async def patrol(self):
        while True:
            try:
                if not self.world_state.proactive_suspended():
                    await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.record("patrol_worker_error")
            await asyncio.sleep(30)

    async def janitor(self):
        while True:
            await self.store.runtime.flush()
            await self.store.prune()
            await self.sessions.prune()
            await self.proactive.prune()
            if hasattr(self.ai, "cleanup_video_files"):
                try:
                    await self.ai.cleanup_video_files()
                except Exception:
                    self.record("video_file_cleanup_pending")
            if self.diary:
                await self.diary.prune()
            await asyncio.sleep(60)
