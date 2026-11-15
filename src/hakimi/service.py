import asyncio
import logging
import random
import re
import time
from collections import Counter

from .adapters import Sender, identifier, onebot_message
from .bili_watcher import BiliWatcher
from .bilibili import Bilibili, extract_bvid
from .bot_server import is_persona_owner, untrusted_input
from .config import Settings
from .gemini import GeminiGateway
from .generation_policy import Scenario
from .group_pipeline import other_mentions
from .hot_ring import HotRing
from .link_preview import preview_links
from .longnick import Longnick
from .media import ImageLoader
from .memes import MemeLibrary
from .memory_compressor import MemoryCompressor, check_memory_text
from .memory_mirror import MemoryMirror
from .mentions import parse_reply, valid_at_targets
from .message_context import MessageContextResolver
from .models import Message, Video
from .moderation import Moderation
from .outbound_actions import parse_actions
from .output_links import filter_reply_links
from .proactive_dm import OwnerProactive
from .public_memory import PublicMemory
from .rich_message_parser import parse_forward
from .roaming_logger import RoamingLogger
from .security import Rejected, authorize, configured_secrets, owner, safe_output
from .session_manager import SessionManager
from .session_scheduler import SessionScheduler
from .store import Store
from .streaming import cancellable
from .video_watch import VideoWatch
from .web_search import explicit_search, search_query

log = logging.getLogger("hakimi")
HELP = """哈基米在这里。直接提问，或发送 B 站 BV 号、视频链接或分享卡片。
/new-session：清空当前会话上下文，保留记忆开关
外部 Bot 默认忽略；仅主人在群内明确邀请后临时互动，同群 10 分钟最多两轮。
/哈基米 记忆 开：为当前私聊或本群启用短期记忆
/哈基米 记忆 关 或 /哈基米 清空：关闭并删除当前会话记忆
群聊记忆由配置中的主人管理；主人私聊可复盘近期公开群聊，私人记忆绝不进入群聊。
以下功能仅限配置中的主人私聊使用：
/mute <QQ> 或 /unmute <QQ>：应用层屏蔽或解除；不自动删好友
/mute-approve <编号> 或 /mute-reject <编号>：批准或驳回屏蔽申请
如同时移除好友，须在 /mute 或 /mute-approve 末尾明确加 --delete-friend
/set-sign <签名>：手动设置 50 字以内纯文本签名
/deep <问题或视频链接>：本次使用 high 档深度分析
/哈基米 订阅 <UP主UID>
/哈基米 退订 <UP主UID>
/哈基米 订阅列表
/哈基米 报告
/哈基米 状态
链接与小号巡逻优先阅读公开字幕资料；原片分析需后台开启，受时长、体积和次数限制。
UP订阅巡逻摘要公开标题和简介；首次巡逻建立基线，不推送历史内容。"""


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
    ):
        self.s, self.store, self.ai, self.bili, self.sender = settings, store, gateway, bili, sender
        self.sessions = SessionManager(settings, store)
        self.hot = HotRing()
        store.runtime.hot_ring = self.hot
        self.enrichments = {}
        self.preview_slots = asyncio.Semaphore(4)
        self.background_tasks = set()
        self.bot_guard = store.bot_guard
        self.images = image_loader or ImageLoader(
            settings, transport=getattr(sender, "onebot", None)
        )
        self.context_resolver = MessageContextResolver(
            settings, getattr(sender, "onebot", None), lookup=self.lookup_received
        )
        self.ingress_scheduler = SessionScheduler()
        self.dialogue_scheduler = SessionScheduler()
        self.public_memory = PublicMemory(store)
        self.diary = diary
        self.notebook = notebook
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
        self.longnick = Longnick(self)
        self.bili_watcher = BiliWatcher(self)
        self.video_watch = VideoWatch(self)
        self.memes = MemeLibrary(store)
        store.runtime.memes = self.memes

    def record(self, event: str):
        # Only internal constant labels. Never log exception repr, payloads, URLs or IDs.
        self.counters[event] += 1
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
                hydrate(), preview_links(message.command_text or message.text, self.preview_slots)
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
                if result == "accepted":
                    await self.store.runtime.filters.remember_group(message)
        if result in {"accepted", "ignored"}:
            self.hot.observe(message)
        if result == "accepted":
            self.store.groups.interrupt(message)
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
        ]
        if self.diary and self.s.roaming_enabled:
            self.tasks.append(asyncio.create_task(self.diary.run(), name="roaming-diary"))
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

    def output(self, text: str) -> str:
        return safe_output(text, self.secrets, limit=self.s.send_max_chars)

    def batch_authorized(self, message, members):
        # An accepted @Bot batch may include subsequent unaddressed follow-up text.
        # This relaxes attention only, never group admission, identity or blacklists.
        addressed_batch = (
            message.kind == "group" and len(members) > 1 and any(m.addressed for m in members)
        )
        return authorize(message, self.s) and all(
            authorize(m.model_copy(update={"addressed": True}) if addressed_batch else m, self.s)
            and not other_mentions(m)
            for m in members
        )

    def image_sender(self, message):
        nickname = message.sender_nickname.strip()[:40] or "群友"
        token = self.store.vault.digest("image-speaker", *message.subject_parts(self.s.namespace))[
            :6
        ]
        return f"{nickname}（成员标识 {token}）"

    async def message_images(self, message, members, nearby, *, limit=2, include_current=True):
        selected, seen = [], set()
        for origin, messages in (("本批消息", reversed(members)), ("紧邻群消息", reversed(nearby))):
            for item in messages:
                candidate = (
                    item.model_copy(update={"addressed": True}) if item.kind == "group" else item
                )
                if not authorize(candidate, self.s) or other_mentions(item):
                    continue
                for ref in item.images:
                    if ref.identity not in seen:
                        seen.add(ref.identity)
                        selected.append((ref, item, origin))
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
            else:
                images.append(loaded)
                data.update(image_number=len(images), status="available")
                semantic = self.memes.semantic_label(loaded.digest)
                if semantic:
                    data["local_sticker_label"] = semantic
            sources.append(data)
        if omitted:
            sources.append({"status": "omitted", "count": omitted})
        return images, sources

    async def expand_forwards(self, message: Message) -> Message:
        texts, images = [message.text], list(message.images)
        videos, shorts = list(message.video_ids), list(message.short_urls)
        for forward_id in message.forward_ids:
            rich = parse_forward(await self.sender.forward(forward_id), message.bot_id)
            texts.append(rich.text)
            images.extend(i for i in rich.images if i not in images)
            videos.extend(v for v in rich.video_ids if v not in videos)
            shorts.extend(u for u in rich.short_urls if u not in shorts)
            # No recursive get_forward_msg calls: a quoted reference cannot fan out requests.
        text = "\n".join(texts)
        if len(text) > 8000 or len(images) > 2:
            raise Rejected("forward_too_large")
        if any(secret and len(secret) >= 8 and secret in text for secret in self.secrets):
            raise Rejected("credential_in_input")
        return message.model_copy(
            update={
                "text": text,
                "images": images,
                "video_ids": videos[:3],
                "short_urls": shorts[:3],
                "forward_ids": [],
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
    ) -> str:
        # No chat memory is ever included in public-content summarization.
        payload = {
            "task": "summarize_public_metadata",
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

    async def _handle_message(self, job: dict):
        message = Message.model_validate(job["payload"])
        members = [Message.model_validate(m) for m in job.get("batch_messages", [])] or [message]
        for original in members:
            if self.bot_guard.known_bot(original):
                await self.bot_guard.prepare(original)
        neighbour_members = []
        if await self.recalled(members):
            return
        watermark = self.hot.watermark(message)
        if not self.batch_authorized(message, members) or time.time() > job["expires"]:
            return
        # Recheck persisted permits before image downloads, forward RPCs, memory or paid calls.
        if not await self.bot_guard.may_process(message, job["id"]):
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
        if not message.context_expanded:
            message = await self.expand_forwards(message)
        frozen_relevance = (
            (await self.store.runtime.filters.group_context(message))
            if message.kind == "group" and not message.addressed
            else []
        )
        images, image_sources = [], []
        images, image_sources = await self.message_images(message, members or [message], [])
        # Open exactly one physical database; never search other conversations.
        session = await self.sessions.snapshot(message)
        history = session["recent_messages"]
        payload = {
            "current_message": message.text,
            "current_sender": {
                "nickname": message.sender_nickname or "群友",
                "qq": message.user_id,
            },
            "surface": message.kind,
            "directly_addressed": message.addressed,
            "recent_messages": history,
            "rolling_summary": session["rolling_summary"],
            "session_epoch": session["epoch"],
            "allowed_reply_ids": [
                m.event_id for m in members if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)
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
        if not authorize(message, self.s):
            return

        async def message_guard():
            if await self.recalled(members):
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

        try:
            relevance = frozen_relevance
            intent = await self.ai.screen(
                current,
                job["subject"],
                purpose="message",
                group=message.kind == "group",
                message=message,
                before_call=message_guard,
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
            raise
        if not authorize(message, self.s):
            return
        if self.diary:
            await self.diary.group(job["id"], message)
        sandbox = getattr(intent, "sandbox_mode", False)
        if not sandbox:
            if not await self.sessions.admit_history(
                message, members, expected_epoch=session["epoch"]
            ):
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
            )
        else:
            payload.update(recent_messages=[], rolling_summary="")
            payload.pop("owner_notebook_snapshot", None)
            payload.pop("近期群聊巡逻记忆（只读背景）", None)
        if intent == "ignore":
            return
        if not sandbox:
            payload["dual_memory_data"] = await self.sessions.dual.prefetch(
                message, message.text, recall=getattr(intent, "need_memory_recall", False)
            )
        invited_bots = set()
        if not sandbox and len(members) == 1 and owner(message, self.s):
            invited_bots = await self.bot_guard.invite(message)
        if self.bot_guard.known_bot(message) and await self.bot_guard.allowed_reply_target(
            message, message.user_id
        ):
            invited_bots.add(message.user_id)
        if message.kind == "group" and not sandbox and len(images) < 2:
            nearby = await self.store.groups.previous_messages(message, members, limit=3)
            neighbour_members[:] = nearby
            neighbour_images, neighbour_sources = await self.message_images(
                message, [], nearby, limit=2 - len(images), include_current=False
            )
            nearby_context = [{"sender": self.image_sender(m), "text": m.text[:80]} for m in nearby]
            payload["nearby_group_context"] = nearby_context
            await message_guard()
            if neighbour_images:
                # Adjacent users' images remain an independent strict SOURCE gate.
                await self.ai.screen(
                    {"image_sources": neighbour_sources, "nearby_group_context": nearby_context},
                    job["subject"],
                    purpose="source",
                    images=[i.gate() for i in neighbour_images],
                    before_call=message_guard,
                )
            for source in neighbour_sources:
                if "image_number" in source:
                    source = {**source, "image_number": source["image_number"] + len(images)}
                image_sources.append(source)
            images.extend(neighbour_images)
            if neighbour_sources:
                payload["image_sources"] = image_sources
            payload["context_status"].extend(
                {"event_id": m.event_id, "states": m.context_status}
                for m in nearby
                if m.context_status
            )
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
        bvid = message.video_ids[0] if message.video_ids else extract_bvid(message.text)
        if sandbox:
            bvid = None
        if not sandbox and not bvid and message.short_urls:
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
            result = self.output(text)
            # Sources and generated summaries are deliberately excluded from conversation memory.
        else:
            if (
                not sandbox
                and not self.bot_guard.known_bot(message)
                and (message.kind == "private" or message.addressed)
                and (
                    explicit_search(members[-1].command_text or members[-1].text)
                    or getattr(intent, "need_external_lookup", False)
                )
            ):
                controller = self.store.runtime.web_search
                payload["web_search_status"] = "disabled"
                if controller.config.enabled:
                    self.store.groups.phase(message, "generating")
                    batch = self.store.groups.active.get(self.store.session_scope(message))
                    payload["web_search_status"] = "unavailable"
                    try:
                        findings = await self.ai.search(
                            search_query(members[-1], self.s),
                            job["subject"],
                            before_call=message_guard,
                            cancel_event=batch.cancel if batch else None,
                        )
                        if findings and findings.strip() and controller.config.enabled:
                            payload["web_search_findings"] = findings
                            payload.pop("web_search_status", None)
                    except Rejected as exc:
                        if str(exc) in {"reply_invalidated", "batch_interrupted", "batch_recalled"}:
                            raise
                        self.record("web_search_failed")
                    await message_guard()
            self.store.groups.phase(message, "generating")
            batch = self.store.groups.active.get(self.store.session_scope(message))
            tier = getattr(intent, "tier", 2)
            if tier >= 3 and hasattr(self.sender, "quick_ack"):
                self.background(self.quick_ack(job, message, message_guard))
            generated = await self.ai.generate(
                payload,
                job["subject"],
                scenario=Scenario.OWNER_PRIVATE
                if message.kind == "private" and is_persona_owner(message, self.s)
                else Scenario.FAST_CHAT,
                deep=deep,
                route=intent,
                message=message,
                before_call=message_guard,
                cancel_event=batch.cancel if batch else None,
                **({"images": images} if images else {}),
            )
            await message_guard()
            allowed_reply = payload["allowed_reply_ids"]
            payload.clear()
            images.clear()
            if generated.lstrip().startswith("{"):
                actions, folded = parse_actions(
                    generated,
                    public=public,
                    allowed_at=allowed_at,
                    allowed_reply=allowed_reply,
                    labels=self.memes.labels,
                    settings=self.s,
                    tier=tier,
                )
                if actions.abstain:
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
                await self.store.enqueue_reply(
                    job["id"],
                    message,
                    "\n".join(actions.bubbles),
                    session_epoch=session["epoch"],
                    participants=members,
                    allowed_at_qqs=allowed_at,
                    actions={
                        **actions.model_dump(),
                        "folded": folded,
                        "watermark": watermark,
                        "source_ids": [m.event_id for m in members],
                        "public": public,
                    },
                )
                return
            # Read compatibility for integrations returning the old single-text contract.
            # The production Gemini gateway validates the new schema before reaching here.
            parsed = parse_reply(generated, message, self.s, allowed_at_qqs=allowed_at)
            generated, meme_tag = parsed.text, parsed.meme_tag
            result = self.output(generated) if generated else ""
        if (await self.sessions.snapshot(message))["epoch"] != session["epoch"]:
            return
        if parsed is None:
            parsed = parse_reply(result, message, self.s, allowed_at_qqs=allowed_at)
            result, meme_tag = parsed.text, parsed.meme_tag
        if not result and not (meme_tag and meme_tag in self.memes.labels):
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
        )

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
        result = await self.summarize(videos, job["subject"], "patrol")
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
            if str(exc) == "batch_interrupted":
                await self.store.groups.requeue_interrupted(job)
                return
            if str(exc) in {"batch_recalled", "reply_invalidated", "video_watch_invalidated"}:
                await self.store.finish(job["id"], "cancelled")
                return
            self.record(str(exc))
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
                        notice = "今天的额度用完啦，明天再接着聊吧~"
                    # Safety, permission and context rejections stay silent.
                    if notice:
                        await self.store.enqueue_reply(job["id"], message, notice)
            await self.store.finish(job["id"], "rejected")
        except Exception:
            self.record("job_failed")
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
                ("等我一下下，我仔细看看~", "稍等哦，我帮你理一下")
                if message.kind == "private" and is_persona_owner(message, self.s)
                else ("稍等我捋一下", "等下，我看眼细节", "让我想想怎么讲")
            )
            receipt = await self.sender.quick_ack(
                message, random.choice(phrases), before_send=valid
            )
            if receipt:
                await self.store.runtime.attention.record_sent(message, receipt)
        except Exception:
            self.record("quick_ack_unavailable")

    async def deliver_once(self) -> bool:
        reply = await self.store.claim_reply()
        if not reply:
            return False
        if reply.get("actions"):
            from .outgoing import deliver_actions

            return await deliver_actions(self, reply)
        status = "sent"
        text_confirmed = False
        message = Message.model_validate(reply["message"])
        participants = [Message.model_validate(m) for m in reply.get("participants", [])]
        for original in participants or [message]:
            if self.bot_guard.known_bot(original):
                await self.bot_guard.prepare(original)
        delivery_epoch = (await self.sessions.snapshot(message))["epoch"]
        if "session_epoch" in reply and delivery_epoch != reply["session_epoch"]:
            await self.store.finish_reply(reply["id"], "cancelled")
            return True
        if not await self.bot_guard.may_process(message, reply["id"]):
            await self.store.finish_reply(reply["id"], "blocked")
            return True
        try:
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
                    self.hot.recalled(m) or any(self.hot.recalled(m, ref) for ref in m.reply_ids)
                    for m in participants or [message]
                ):
                    return False
                if not await self.bot_guard.may_process(message, reply["id"]):
                    return False
                return (
                    (await self.sessions.snapshot(message))["epoch"] == delivery_epoch
                ) and self.batch_authorized(message, participants or [message])

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
            return True  # A reset cancelled this delivery while the transport awaited its ACK.
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
        if status == "sent" and not reply["proactive"] and "session_epoch" in reply:
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
        if not self.s.bilibili_enabled:
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
            rooms = await self.store.all("SELECT room_id,target,next_poll FROM live_watches")
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
                status = await self.bili.live_status(room["room_id"])
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE live_watches SET last_status=? WHERE room_id=?",
                        (status, room["room_id"]),
                    )
            except Exception:
                self.record("patrol_source_unavailable")

    async def patrol(self):
        while True:
            try:
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
