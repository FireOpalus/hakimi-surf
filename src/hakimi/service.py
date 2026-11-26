"""Core service lifecycle, ingress admission, patrol workers, and subsystem composition."""

from __future__ import annotations

import asyncio
import inspect
import logging
import random
import re
import sys
import time
import weakref
from collections import Counter
from typing import Any

import httpx

from hakimi_plugin_sdk.bili_common import extract_bvid as extract_bvid

from . import text_card
from .audio_media import MAX_INLINE_AUDIO_BYTES, AudioLoader
from .bili_bridge import RemoteBiliClient, RemoteBiliWatcher
from .bot_server import is_persona_owner, untrusted_input
from .commands import (
    HELP as HELP,
)
from .commands import (
    build_default_command_registry,
    handle_affection_command,
    handle_bind_command,
    handle_learn_song_command,
    handle_save_meme_command,
)
from .config import Settings, is_external_plugin
from .drive_client import FolderDrive
from .file_reader import FileReader
from .gemini import GeminiGateway
from .group_pipeline import other_mentions
from .hot_ring import HotRing
from .life_engine import LifeEngine
from .link_preview import preview_links
from .media import ImageLoader
from .media_turn import (
    PRIOR_AUDIO_REF_RE as PRIOR_AUDIO_REF_RE,
)
from .media_turn import (
    PRIOR_FILE_REF_RE as PRIOR_FILE_REF_RE,
)
from .media_turn import (
    PRIOR_IMAGE_REF_RE as PRIOR_IMAGE_REF_RE,
)
from .media_turn import (
    auto_save_meme,
    expand_forwards,
    image_sender,
    is_sticker_candidate,
    load_message_audios,
    load_message_images,
    load_turn_files,
)
from .memory_compressor import check_memory_text
from .message_context import MessageContextResolver
from .models import Message, Video
from .moderation import Moderation
from .modules import ModuleRegistry, install_modules
from .modules.adapters import Sender, identifier, onebot_message
from .modules.bili_watcher import BiliWatcher
from .modules.bilibili import Bilibili
from .modules.longnick import Longnick
from .node_transport import NodeTransport
from .outbound_actions import normalize_emoji_like
from .output_links import filter_reply_links
from .plugin_kernel import PluginKernel
from .public_memory import PublicMemory
from .roaming_logger import PatrolDiary, redact_public_summary
from .security import Rejected, authorize, configured_secrets, owner, safe_output
from .service_delivery import _ServiceDeliveryMixin
from .service_owner_actions import (
    _LearnSongArgs as _LearnSongArgs,
)
from .service_owner_actions import (
    _ServiceOwnerActionsMixin,
)
from .service_owner_actions import (
    _SwitchSongVersionArgs as _SwitchSongVersionArgs,
)
from .service_turn import _ServiceTurnMixin
from .session_manager import SessionManager
from .session_scheduler import SessionScheduler
from .social import chat_module
from .store import Store
from .system_modules import SystemResources, install_system_modules
from .work_usage import model_work
from .world_experience import SOCIAL_LOW
from .world_state import WorldStateManager

log = logging.getLogger("hakimi")


class Service(_ServiceOwnerActionsMixin, _ServiceDeliveryMixin, _ServiceTurnMixin):
    def __init__(
        self,
        settings: Settings,
        store: Store,
        gateway: GeminiGateway,
        bili: Bilibili,
        sender: Sender,
        *,
        image_loader: ImageLoader | None = None,
        audio_loader: AudioLoader | None = None,
        diary: PatrolDiary | None = None,
        notebook=None,
        drive: FolderDrive | None = None,
        http: httpx.AsyncClient | None = None,
        node_transport: NodeTransport | None = None,
    ):
        self.s, self.store, self.ai, self.sender = settings, store, gateway, sender
        # Older composition roots supplied their shared HTTP client on the sender.
        self.http = http if http is not None else getattr(sender, "http", None)
        self.node_transport = (
            node_transport if node_transport is not None else NodeTransport(settings)
        )
        if is_external_plugin(settings, "bilibili_surf"):
            self.bili = RemoteBiliClient(self)
        else:
            self.bili = bili
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
        self.audios = audio_loader or AudioLoader(
            settings, transport=getattr(sender, "onebot", None)
        )
        self.files = FileReader(settings, store, transport=getattr(sender, "onebot", None))
        self.context_resolver = MessageContextResolver(
            settings, getattr(sender, "onebot", None), lookup=self.lookup_received
        )
        self.ingress_scheduler = SessionScheduler()
        self.dialogue_scheduler = SessionScheduler()
        self.public_memory = PublicMemory(store)
        self.recovery_task = None
        self.poll_lock = asyncio.Lock()
        self.owner_dialogue_lock = asyncio.Lock()
        self.started_at = time.monotonic()
        self.counters: Counter = Counter()
        self.tasks: list[asyncio.Task] = []
        self.secrets = configured_secrets(settings)
        self.moderation = Moderation(self)
        store.runtime.moderation = self.moderation
        self.longnick = Longnick(self)
        self.bili_watcher = (
            RemoteBiliWatcher(self)
            if is_external_plugin(settings, "bilibili_surf")
            else BiliWatcher(self)
        )
        self._lurk_react_at: dict[str, float] = {}
        self._last_aff_recovery_at: float = 0.0
        self.modules = ModuleRegistry()
        settings._chat_registry = weakref.ref(self.modules)
        self.kernel = PluginKernel()
        install_system_modules(
            self.modules,
            SystemResources(
                settings,
                store,
                gateway,
                self.http,
                self,
                notebook=notebook,
                diary=diary,
                drive=drive if drive is not None else getattr(diary, "drive", None),
            ),
        )
        # Existing chat and maintenance callers use these handles. Workers come from the registry.
        self.diary = self.modules.systems["patrol_diary"].diary
        self.drive_sync = self.modules.systems["drive_sync"]
        self.diary_sync = self.modules.systems["patrol_diary"].delivery
        self.db_backup = self.modules.systems["database_backup"].backup
        self.surf_mirror = self.modules.systems["surf_mirror"].mirror
        self.memory_mirror = self.modules.systems["group_memory_mirror"].mirror
        self.notebook = self.modules.systems["owner_notebook"].snapshot
        self.cloud_memory = self.modules.systems["owner_notebook"].cloud_memory
        self.drive = self.drive_sync.drive
        self.event_ledger = self.modules.systems["event_ledger"].ledger
        self.modules.event_ledger = self.event_ledger
        self.sessions.event_ledger = self.event_ledger
        self.proactive = self.modules.systems["proactive_delivery"].proactive
        self.modules.owner_delivery = self.proactive
        self.video_watch = self.modules.systems["video_watch"].watch
        self.screen_watch = self.modules.systems["screen_watch"].screen_watch
        self.voice_companion = self.modules.systems["voice_companion"].companion
        self.voice_reply = self.modules.systems["voice_gateway"].reply
        self.voice_gateway_admin = self.modules.systems["voice_gateway"].admin
        self.colab_compute = self.modules.systems["colab_compute"].compute
        self.songs = self.modules.systems["song_studio"].songs
        store.runtime.songs = self.songs
        self.gcp_billing = self.modules.systems["gcp_billing"].manager
        self.identity = self.modules.systems["identity"].identity
        self.affection = self.modules.systems["affection"].manager
        self.memory_hub = self.modules.systems["memory_hub"].hub
        self.compressor = self.modules.systems["memory_compressor"].compressor
        self.memes = self.modules.systems["meme_library"].library
        self.plugin_bus = self.modules.systems["plugin_bus"].plugin_bus
        self.images.plugin_bus = self.plugin_bus
        self.audios.plugin_bus = self.plugin_bus
        self.files.plugin_bus = self.plugin_bus
        self.songs.plugin_bus = self.plugin_bus
        if getattr(store.runtime, "web_search", None) is not None:
            store.runtime.web_search.plugin_bus = self.plugin_bus
        text_card.configure_remote_bridge(self.plugin_bus)
        store.runtime.identity = self.identity
        store.runtime.affection = self.affection
        store.runtime.memes = self.memes
        self.kernel.provide_core("identity", self.identity)
        self.kernel.provide_core("memory_hub", self.memory_hub)
        self.kernel.provide_core("event_ledger", self.event_ledger)
        self.kernel.provide_core("screen_watch", self.screen_watch)
        self.kernel.provide_core("voice_companion", self.voice_companion)
        self.kernel.provide_core("plugin_bus", self.plugin_bus)
        self.world_state = WorldStateManager(settings, store, self.modules)
        self.world_state.subscribe(self._broadcast_world_event)
        install_modules(self)
        if (
            is_external_plugin(self.s, "minecraft")
            or is_external_plugin(self.s, "bilibili_surf")
            or self.modules.get("minecraft") is None
            or self.modules.get("bilibili_surf") is None
        ):
            if getattr(self.node_transport, "on_event", None) is None:
                self.node_transport.on_event = self._relay_node_event
            if getattr(self.node_transport, "on_state", None) is None:
                self.node_transport.on_state = self._relay_node_state
        settings._thinking_scenes = self.modules.thinking_scenes
        self.commands = build_default_command_registry()
        self.life_engine = LifeEngine(self)

    async def _sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def _random(self) -> float:
        return random.random()

    @property
    def weather(self):
        """Compatibility accessor for existing weather integrations."""
        return self.modules.get("weather")

    async def _broadcast_world_event(self, event: Any) -> None:
        payload = (
            event.get("payload") if isinstance(event, dict) else getattr(event, "payload", None)
        )
        if isinstance(payload, dict) and (
            payload.get("private_first") or payload.get("owner_private")
        ):
            return
        if pb := getattr(self, "plugin_bus", None):
            await pb.broadcast_world_event(event)

    async def _relay_node_event(self, frame: dict[str, Any]) -> str:
        if not isinstance(frame, dict):
            return "ignored"
        event_name = frame.get("event")
        if not isinstance(event_name, str) or not event_name.startswith("mc."):
            return "ignored"
        pb = getattr(self, "plugin_bus", None)
        if pb is None:
            return "ignored"
        mc_mod = self.modules.get("minecraft")
        if mc_mod is None or not mc_mod.enabled:
            return "ignored"
        sessions = getattr(pb, "_sessions", None)
        if isinstance(sessions, dict):
            mc_sess = sessions.get("minecraft")
            if (
                mc_sess is None
                or not mc_sess.ready
                or mc_sess.closed
                or "node_bridge" not in mc_sess.scopes
            ):
                return "ignored"
        await pb.relay_node_event(frame)
        return "accepted"

    async def _relay_node_state(self, connected: bool) -> None:
        if pb := getattr(self, "plugin_bus", None):
            await pb.relay_node_state(connected)

    def refresh_secrets(self) -> None:
        refreshed = configured_secrets(self.s)
        if isinstance(getattr(self, "secrets", None), list):
            self.secrets[:] = refreshed
        else:
            self.secrets = refreshed

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
                if message.audios and isinstance(self.audios, AudioLoader):
                    await self.audios.load_results(
                        message.audios, scope=self.store.session_scope(message)
                    )

            _, links = await asyncio.gather(
                hydrate(),
                preview_links(
                    message.command_text or message.text,
                    self.preview_slots,
                    budget_seconds=1.8,
                    plugin_bus=self.plugin_bus,
                ),
            )
            return links
        except Exception:
            return []

    async def accept_event(self, payload):
        if self.modules.inactive_reason("qq_social"):
            return "ignored"
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
            from .modules.adapters import notice_message

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

    async def accept_from(self, module, message: Message):
        """Authenticated plugin entry point; a transport can only submit its declared platforms."""
        if module is None or self.modules.chat_platforms.get(message.platform) is not module:
            return "ignored"
        return await self.accept(message)

    async def accept(self, message: Message | None) -> str:
        if message is None or not chat_module(self.s, message.platform):
            return "ignored"
        if other_mentions(message):
            self.counters["addressed_to_others"] += 1
            return "ignored"
        message = await self.store.runtime.attention.resolve(message)
        if message is not None and self.bot_guard.known_bot(message):
            if not await self.bot_guard.prepare(message):
                return "ignored"
        if (
            message is not None
            and getattr(self, "identity", None) is not None
            and self.identity.is_muted(message.platform, message.user_id)
        ):
            self.counters["ignored"] += 1
            return "ignored"
        eligible_context = bool(
            message is not None
            and message.kind == "group"
            and authorize(message.model_copy(update={"addressed": True}), self.s)
        )
        if message is None or (not authorize(message, self.s) and not eligible_context):
            self.counters["ignored"] += 1
            return "ignored"
        if message.platform not in {"onebot", "qq"}:
            # Replies and media references from other transports may never invoke OneBot RPCs
            # or arbitrary outbound URLs; preserve only bus-namespaced plugin_media refs.
            plugin_images = [
                ref
                for ref in message.images
                if not ref.url and ref.file.startswith("plugin_media:")
            ]
            plugin_files = [
                ref
                for ref in message.files
                if not ref.url and ref.file_id.startswith("plugin_media:")
            ]
            plugin_audios = [
                ref
                for ref in message.audios
                if not ref.url and ref.file.startswith("plugin_media:")
            ]
            message = message.model_copy(
                update={
                    "images": plugin_images,
                    "files": plugin_files,
                    "audios": plugin_audios,
                    "forward_ids": [],
                    "context_expanded": True,
                }
            )
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
            if (message.images or message.audios or "http" in message.text) and len(
                self.background_tasks
            ) < 32:
                if len(self.enrichments) >= 256:
                    oldest = next(iter(self.enrichments))
                    self.enrichments.pop(oldest).cancel()
                self.enrichments[self.store.event_key(message)] = self.background(
                    self.enrich(message)
                )
            if not authorize(message, self.s) or (
                message.kind == "group"
                and not message.addressed
                and hasattr(self, "world_state")
                and (
                    self.world_state.proactive_suspended()
                    or await self.world_state.experience.battery(message, time.time()) < SOCIAL_LOW
                )
            ):
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
            and getattr(self.store.runtime.affection.config, "penalize_rate_limit", False) is True
            and not self.bot_guard.known_bot(message)
            and not self.store.runtime.affection.is_owner_id(message.platform, message.user_id)
            and (
                message.platform in {"onebot", "qq"} or getattr(self, "identity", None) is not None
            )
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
            asyncio.create_task(self.patrol(), name="patrol"),
            asyncio.create_task(self.janitor(), name="janitor"),
            asyncio.create_task(self.life_engine.run(), name="life-engine"),
        ]
        for module in self.modules.systems.values():
            for worker in module.workers():
                self.tasks.append(asyncio.create_task(worker(), name=module.module_id))

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
        if engine := getattr(self, "life_engine", None):
            await engine.close()
        await self.compressor.close()
        await self.bili_watcher.close()
        close_nt = getattr(self.node_transport, "close", None)
        if close_nt is not None:
            res = close_nt()
            if inspect.isawaitable(res):
                await res
        if kernel := getattr(self, "kernel", None):
            await kernel.dispose_all()
        if pb := getattr(self, "plugin_bus", None):
            if text_card._get_bridge_state()[0] is pb:
                text_card.configure_remote_bridge(None)
            await pb.close()
        if sw := getattr(self, "screen_watch", None):
            await sw.close()
        if vc := getattr(self, "voice_companion", None):
            await vc.close()
        if cc := getattr(self, "colab_compute", None):
            await cc.close()
        if songs := getattr(self, "songs", None):
            if (sd := getattr(songs, "drive", None)) is not None and hasattr(sd, "close"):
                await sd.close()

    async def command(self, message: Message) -> str | None:
        return await self.commands.dispatch(self, message)

    async def _bind_command(self, message: Message, raw_args: str) -> str:
        return await handle_bind_command(self, message, raw_args)

    async def _affection_command(self, message: Message, raw_args: str) -> str:
        return await handle_affection_command(self, message, raw_args)

    async def _save_meme_command(self, message: Message, raw_label: str) -> str:
        return await handle_save_meme_command(self, message, raw_label)

    async def _learn_song_command(self, message: Message, raw_args: str) -> str:
        return await handle_learn_song_command(self, message, raw_args)

    async def _auto_save_meme(
        self, message: Message, candidates: list[Message], label: str
    ) -> bool:
        return await auto_save_meme(self, message, candidates, label)

    def output(self, text: str) -> str:
        return safe_output(text, self.secrets, limit=self.s.send_max_chars)

    def platform_voice_capability(self, message: Message) -> tuple[bool, str]:
        if message.platform == "onebot":
            return (hasattr(self.sender, "send_voice"), "mp3")
        if message.platform == "qq":
            return (False, "")
        module = chat_module(self.s, message.platform)
        return (
            bool(module and getattr(module, "supports_voice", False)),
            str(getattr(module, "preferred_voice_format", "ogg_opus") or "ogg_opus"),
        )

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
        self,
        candidate: Message,
        *,
        bypass_mute_user_id: str | None = None,
        bypass_mute_user_ids: set[str] | frozenset[str] | None = None,
    ) -> bool:
        bypass_ids: set[str] = {
            str(u).strip() for u in (bypass_mute_user_ids or ()) if str(u).strip()
        }
        if bypass_mute_user_id and str(bypass_mute_user_id).strip():
            bypass_ids.add(str(bypass_mute_user_id).strip())
        can_bypass = candidate.user_id in bypass_ids and not self.bot_guard.known_bot(candidate)
        identity = getattr(self, "identity", None)
        if (
            identity is not None
            and identity.is_muted(candidate.platform, candidate.user_id)
            and not can_bypass
        ):
            return False
        if can_bypass and candidate.user_id in self.s.muted_user_ids:
            cfg = self.s.model_copy(update={"muted_user_ids": self.s.muted_user_ids - bypass_ids})
            return authorize(candidate, cfg)
        return authorize(candidate, self.s)

    def batch_authorized(
        self,
        message,
        members,
        *,
        bypass_mute_user_id: str | None = None,
        bypass_mute_user_ids: set[str] | frozenset[str] | None = None,
    ):
        if not chat_module(self.s, message.platform):
            return False
        invited_batch = (
            message.kind == "group"
            and len(members) > 1
            and any(m.invites_group_review for m in members)
        )
        return self._authorize_for_batch(
            message,
            bypass_mute_user_id=bypass_mute_user_id,
            bypass_mute_user_ids=bypass_mute_user_ids,
        ) and all(
            self._authorize_for_batch(
                m.model_copy(update={"addressed": True}) if invited_batch else m,
                bypass_mute_user_id=bypass_mute_user_id,
                bypass_mute_user_ids=bypass_mute_user_ids,
            )
            and not other_mentions(m)
            for m in members
        )

    def _is_explicit_song_request(self, *texts: str) -> bool:
        songs = getattr(self, "songs", None)
        if songs is None:
            return False
        checker = getattr(songs, "explicit_song_request", None)
        if callable(checker):
            return bool(checker(*texts))
        mod = sys.modules.get(type(songs).__module__)
        mod_checker = getattr(mod, "explicit_song_request", None) if mod is not None else None
        if callable(mod_checker):
            return bool(mod_checker(*texts))
        return False

    def image_sender(self, message):
        return image_sender(self, message)

    def _is_sticker_candidate(self, message, ref) -> bool:
        return is_sticker_candidate(self, message, ref)

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
        return await load_message_images(
            self,
            message,
            members,
            nearby,
            limit=limit,
            include_current=include_current,
            nearby_origin=nearby_origin,
            selected_members=selected_members,
        )

    async def message_audios(
        self,
        message,
        members,
        nearby,
        *,
        include_current=True,
        nearby_origin="紧邻群消息",
        selected_members=None,
        max_total_bytes=MAX_INLINE_AUDIO_BYTES,
    ):
        return await load_message_audios(
            self,
            message,
            members,
            nearby,
            include_current=include_current,
            nearby_origin=nearby_origin,
            selected_members=selected_members,
            max_total_bytes=max_total_bytes,
        )

    async def _load_turn_files(
        self,
        message: Message,
        members: list[Message],
        filtered_nearby: list[Message],
        history_events: set[str],
        current_events: set[str],
    ) -> tuple[list[dict], list[dict], list[str]]:
        return await load_turn_files(
            self, message, members, filtered_nearby, history_events, current_events
        )

    async def expand_forwards(self, message: Message) -> Message:
        return await expand_forwards(self, message)

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
        payload = {
            "task": (
                "notify_owner_subscribed_up_new_video"
                if patrol_notify
                else "summarize_public_metadata"
            ),
            "material_scope": "metadata_only",
            "untrusted_sources": [v.model_dump(exclude={"mid"}) for v in videos],
        }
        while len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            longest = max(payload["untrusted_sources"], key=lambda v: len(v["description"]))
            if not longest["description"]:
                raise Rejected("context_too_large")
            longest["description"] = longest["description"][: len(longest["description"]) // 2]
        await self.ai.screen(payload, subject, purpose="source", budget_lane=lane)
        text = await self.ai.generate(
            payload, subject, summary=True, deep=deep, budget_lane=lane, message=message
        )
        text = filter_reply_links(text, public=False)
        sources = "\n".join(v.url for v in videos)
        return self.output(f"{text}\n{sources}")

    async def handle_patrol(self, job: dict):
        if self.modules.inactive_reason("bilibili_surf"):
            return
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
        if not any(s["id"] == sub_id for s in await self.store.subscriptions(job["subject"])):
            return
        await self.store.save_report(job["subject"], result, public=True)
        if self.diary:
            await self.diary.patrol(job["id"], result)
        if self.proactive.matches(target) and self.proactive.delivery_enabled:
            await self.proactive.enqueue("patrol", result, key=job["id"])

    async def process(self, job: dict):
        try:
            work = "bili.subscription"
            if job["kind"] == "message":
                platform = job["payload"]["platform"]
                work = "qq.reply" if platform in {"qq", "onebot"} else platform + ".reply"
            with model_work(work, job["id"]):
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
            await self.store.runtime.filters.update_audit(
                audit_id, "rejected", exc.audit_detail, diagnostics=exc.provider_diagnostics
            )
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

    async def poll_once(self, *, force=False):
        if self.poll_lock.locked():
            return
        async with self.poll_lock:
            await self._poll_once(force=force)

    async def _poll_once(self, *, force=False):
        if (
            self.modules.inactive_reason("bilibili_surf")
            or not self.s.bilibili_enabled
            or (not force and self.store.runtime.rest_kind() != "awake")
        ):
            return
        for sub in await self.store.subscriptions():
            if not force and sub["next_poll"] > time.time():
                continue
            target = Message.model_validate(sub["target"])
            if not authorize(target, self.s) or not owner(target, self.s):
                continue
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
        live_key = self.store.vault.digest("live-open", room_id, int(time.time() // 1800))
        safe_title, safe_area = "", ""
        if raw_title or raw_area:
            try:
                with model_work("bili.live_open", live_key):
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
        await self.store.save_report(self.store.subject(target), live_text, public=True)
        if (
            target.kind == "private"
            and self.proactive.matches(target)
            and self.proactive.delivery_enabled
        ):
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
            await self.world_state.experience.prune(time.time())
            if hasattr(self.ai, "cleanup_video_files"):
                try:
                    await self.ai.cleanup_video_files()
                except Exception:
                    self.record("video_file_cleanup_pending")
            now = time.time()
            aff = getattr(self.store.runtime, "affection", None)
            if (
                aff is not None
                and hasattr(aff, "run_cooldown_recovery")
                and now - self._last_aff_recovery_at >= 600
            ):
                self._last_aff_recovery_at = now
                try:
                    await aff.run_cooldown_recovery()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.record("affection_recovery_error", emit_audit=False)
            await asyncio.sleep(60)
