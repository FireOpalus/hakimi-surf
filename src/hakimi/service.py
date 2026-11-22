import asyncio
import dataclasses
import inspect
import json
import logging
import random
import re
import sys
import time
import uuid
import weakref
from collections import Counter
from contextlib import AsyncExitStack, nullcontext
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hakimi_plugin_sdk.bili_common import extract_bvid

from . import text_card
from .attention import can_watch_video
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
from .gateway import redact_excerpt
from .gemini import GeminiGateway
from .generation_policy import Scenario
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
    recall_historical_images,
)
from .memes import explicit_meme_request
from .memory_compressor import check_memory_text
from .mentions import parse_reply, valid_at_targets
from .message_context import MessageContextResolver
from .models import Message, Video
from .moderation import Moderation
from .modules import ModuleRegistry, install_modules
from .modules.actions import ActionCall, reject_reason
from .modules.adapters import Sender, farewell_send_bypass, identifier, onebot_message
from .modules.bili_watcher import BiliWatcher
from .modules.bilibili import Bilibili
from .modules.longnick import Longnick
from .node_transport import NodeTransport
from .outbound_actions import ChatActionTrigger, normalize_emoji_like, parse_actions
from .outgoing import PRE_DISPATCH_REJECTED, is_affection_muted_only, safe_delivery_detail
from .output_links import filter_reply_links
from .plugin_kernel import PluginKernel
from .public_memory import PublicMemory
from .roaming_logger import PatrolDiary, redact_public_summary
from .routing import RouteDecision
from .security import Rejected, authorize, configured_secrets, owner, safe_output
from .session_manager import SessionManager, apply_audio_memory_tags
from .session_scheduler import SessionScheduler
from .social import chat_module, confirmed_message_id
from .store import Store
from .streaming import cancellable
from .system_modules import SystemResources, install_system_modules
from .voice_reply import explicit_voice_request
from .web_search import explicit_search, search_query
from .work_usage import model_work
from .world_experience import SOCIAL_LOW
from .world_state import WorldStateManager

log = logging.getLogger("hakimi")

_LEARN_SONG_ACTION_NAME = "song_studio.learn_song"
_SWITCH_SONG_VERSION_ACTION_NAME = "song_studio.switch_version"


class _LearnSongArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    title: str = Field(min_length=1, max_length=60, description="歌曲名称")
    artist: str = Field(default="", max_length=40, description="原唱歌手（可选）")
    tags: str = Field(default="", max_length=80, description="风格标签（可选）")
    engine: Literal["dual", "yingmusic_svc", "seed_vc", "rvc"] = Field(
        default="dual",
        description="翻唱引擎：dual（默认双引擎猫猫亲挑）/ yingmusic_svc / seed_vc / rvc",
    )
    ref_voice: str = Field(default="", max_length=160, description="参考干声文件名（可选）")
    pitch: int = Field(default=0, ge=-24, le=24, description="升降调半音数（-24..24，默认0）")
    auto_search: bool = Field(default=True, description="未附带音频时是否自动检索原版伴奏/原唱")


class _SwitchSongVersionArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    title: str = Field(min_length=1, max_length=60, description="歌单中的歌曲名称")
    take: str = Field(
        min_length=1,
        max_length=32,
        description="目标候选版本键或引擎名（如 yingmusic_svc / seed_vc / rvc）",
    )


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

    def _collect_owner_chat_actions(self) -> list[dict[str, Any]]:
        engine = getattr(self, "life_engine", None)
        catalog = getattr(engine, "catalog", None)
        cfg = getattr(engine, "config", None)
        if engine is None or catalog is None or cfg is None or not cfg.enabled:
            return []
        action_modes = getattr(cfg, "action_modes", {}) or {}
        items: list[dict[str, Any]] = []
        for spec in sorted(catalog.actions.values(), key=lambda s: s.name):
            if "owner_chat" not in spec.origins or spec.social:
                continue
            owner_mod = catalog.owners.get(spec.name, "")
            if not owner_mod or catalog.inactive_reason(owner_mod):
                continue
            if action_modes.get(spec.name, "active") == "off":
                continue
            if spec.unavailable():
                continue
            model_json_schema = getattr(spec.arguments, "model_json_schema", None)
            if callable(model_json_schema):
                schema = model_json_schema()
                args_props = schema.get("properties", {}) if isinstance(schema, dict) else {}
                required_args = schema.get("required", []) if isinstance(schema, dict) else []
            elif isinstance(spec.arguments, dict):
                args_props = dict(spec.arguments)
                required_args = []
            else:
                args_props = {}
                required_args = []
            entry: dict[str, Any] = {
                "action_name": spec.name,
                "description": spec.description,
                "arguments": args_props,
            }
            if required_args:
                entry["required"] = list(required_args)
            items.append(entry)
            if len(items) >= 12:
                break
        if (
            len(items) < 12
            and action_modes.get(_LEARN_SONG_ACTION_NAME, "active") != "off"
            and getattr(self, "songs", None) is not None
            and getattr(self, "colab_compute", None) is not None
            and self.colab_compute.configured()
            and not any(item["action_name"] == _LEARN_SONG_ACTION_NAME for item in items)
        ):
            schema = _LearnSongArgs.model_json_schema()
            items.append(
                {
                    "action_name": _LEARN_SONG_ACTION_NAME,
                    "description": (
                        "用 Colab 学唱一首新歌并加入歌单"
                        "（默认双引擎亲听自选，支持附带音频或自动检索原曲）"
                    ),
                    "arguments": schema.get("properties", {}),
                    "required": list(schema.get("required", ())),
                }
            )
            items.sort(key=lambda item: item["action_name"])
        songs_lib = getattr(self, "songs", None)
        has_switchable = (
            callable(getattr(songs_lib, "has_switchable_takes", None))
            and songs_lib.has_switchable_takes()
        )
        if (
            len(items) < 12
            and action_modes.get(_SWITCH_SONG_VERSION_ACTION_NAME, "active") != "off"
            and has_switchable
            and not any(item["action_name"] == _SWITCH_SONG_VERSION_ACTION_NAME for item in items)
        ):
            switch_schema = _SwitchSongVersionArgs.model_json_schema()
            items.append(
                {
                    "action_name": _SWITCH_SONG_VERSION_ACTION_NAME,
                    "description": "将歌单中某首多版本曲目的当前默认播放版本切换为指定候选版",
                    "arguments": switch_schema.get("properties", {}),
                    "required": list(switch_schema.get("required", ())),
                }
            )
            items.sort(key=lambda item: item["action_name"])
        return items

    @staticmethod
    def _safe_action_reason(raw: Any, *, fallback: str = "rejected") -> str:
        text = str(raw or "").strip()
        if re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", text):
            return text
        return fallback

    def _spawn_owner_chat_action(
        self, trigger: ChatActionTrigger, *, context_message: Message | None = None
    ) -> asyncio.Task[None]:
        return self.background(
            self._run_owner_chat_action(trigger, context_message=context_message)
        )

    async def _run_learn_song_owner_action(
        self,
        trigger: ChatActionTrigger,
        engine: Any,
        cfg: Any,
        *,
        context_message: Message | None = None,
    ) -> None:
        if context_message is not None and not owner(context_message, self.s):
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=owner_only",
                _LEARN_SONG_ACTION_NAME,
            )
            return
        if cfg.action_modes.get(_LEARN_SONG_ACTION_NAME, "active") == "off":
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=action_disabled",
                _LEARN_SONG_ACTION_NAME,
            )
            return
        colab = getattr(self, "colab_compute", None)
        songs = getattr(self, "songs", None)
        if colab is None or not colab.configured() or songs is None:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=colab_unconfigured",
                _LEARN_SONG_ACTION_NAME,
            )
            return
        parsed_args: dict[str, Any] = {}
        raw_args_json = trigger.arguments_json
        if isinstance(raw_args_json, str) and raw_args_json.strip():
            try:
                loaded = json.loads(raw_args_json)
            except (ValueError, TypeError):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    _LEARN_SONG_ACTION_NAME,
                )
                return
            if not isinstance(loaded, dict):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    _LEARN_SONG_ACTION_NAME,
                )
                return
            parsed_args = loaded
        try:
            args = _LearnSongArgs.model_validate(parsed_args)
        except ValidationError:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=invalid_action_arguments",
                _LEARN_SONG_ACTION_NAME,
            )
            return
        title = args.title.strip().strip("《》").strip()
        if not title:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=invalid_song_title",
                _LEARN_SONG_ACTION_NAME,
            )
            return

        audio_bytes: bytes | None = None
        audio_filename = ""
        if context_message is not None and context_message.files:
            for ref in context_message.files:
                try:
                    audio_bytes, audio_filename = await self.files.read_audio_attachment(
                        ref, context_message
                    )
                    break
                except Rejected as exc:
                    if str(exc) == "invalid_song_audio":
                        continue
                    log.info(
                        "event=owner_chat_action_done action=%s status=not_dispatched reason=%s",
                        _LEARN_SONG_ACTION_NAME,
                        self._safe_action_reason(exc, fallback="unavailable"),
                    )
                    return

        try:
            req_kwargs: dict[str, Any] = {
                "artist": args.artist.strip(),
                "tags": args.tags.strip(),
                "engine": args.engine,
                "pitch": args.pitch,
                "audio_bytes": audio_bytes,
                "audio_filename": audio_filename,
                "auto_search": args.auto_search,
                "requester": "owner_chat",
            }
            if args.ref_voice.strip():
                req_kwargs["ref_voice"] = args.ref_voice.strip()
            await songs.request_learn_song(title, **req_kwargs)
        except Rejected as exc:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                _LEARN_SONG_ACTION_NAME,
                self._safe_action_reason(exc),
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "event=owner_chat_action_done action=%s status=not_dispatched reason=%s",
                _LEARN_SONG_ACTION_NAME,
                type(exc).__name__,
            )
            return

        log.info(
            "event=owner_chat_action_done action=%s status=done reason=queued",
            _LEARN_SONG_ACTION_NAME,
        )
        planner = getattr(engine, "planner", None)
        replan = getattr(planner, "request_replan", None)
        if callable(replan):
            try:
                await replan(delay_seconds=35.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def _run_switch_song_version_owner_action(
        self,
        trigger: ChatActionTrigger,
        cfg: Any,
        *,
        context_message: Message | None = None,
    ) -> None:
        if context_message is not None and not owner(context_message, self.s):
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=owner_only",
                _SWITCH_SONG_VERSION_ACTION_NAME,
            )
            return
        if cfg.action_modes.get(_SWITCH_SONG_VERSION_ACTION_NAME, "active") == "off":
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=action_disabled",
                _SWITCH_SONG_VERSION_ACTION_NAME,
            )
            return
        songs = getattr(self, "songs", None)
        if songs is None or not hasattr(songs, "switch_take"):
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=song_studio_unavailable",
                _SWITCH_SONG_VERSION_ACTION_NAME,
            )
            return
        parsed_args: dict[str, Any] = {}
        raw_args_json = trigger.arguments_json
        if isinstance(raw_args_json, str) and raw_args_json.strip():
            try:
                loaded = json.loads(raw_args_json)
            except (ValueError, TypeError):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    _SWITCH_SONG_VERSION_ACTION_NAME,
                )
                return
            if not isinstance(loaded, dict):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    _SWITCH_SONG_VERSION_ACTION_NAME,
                )
                return
            parsed_args = loaded
        try:
            args = _SwitchSongVersionArgs.model_validate(parsed_args)
        except ValidationError:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=invalid_action_arguments",
                _SWITCH_SONG_VERSION_ACTION_NAME,
            )
            return
        title = args.title.strip().strip("《》").strip()
        if not title:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=invalid_song_title",
                _SWITCH_SONG_VERSION_ACTION_NAME,
            )
            return
        try:
            await songs.switch_take(title, args.take.strip(), by_title=True)
        except Rejected as exc:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                _SWITCH_SONG_VERSION_ACTION_NAME,
                self._safe_action_reason(exc),
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "event=owner_chat_action_done action=%s status=not_dispatched reason=%s",
                _SWITCH_SONG_VERSION_ACTION_NAME,
                type(exc).__name__,
            )
            return
        log.info(
            "event=owner_chat_action_done action=%s status=done reason=switched",
            _SWITCH_SONG_VERSION_ACTION_NAME,
        )

    async def _run_owner_chat_action(
        self, trigger: ChatActionTrigger, context_message: Message | None = None
    ) -> None:
        raw_name = (trigger.action_name or "").strip()
        engine = getattr(self, "life_engine", None)
        catalog = getattr(engine, "catalog", None)
        cfg = getattr(engine, "config", None)
        if engine is None or catalog is None or cfg is None or not cfg.enabled:
            log.info(
                "event=owner_chat_action_done action=unknown status=gated_skip "
                "reason=engine_disabled"
            )
            return
        if raw_name == _LEARN_SONG_ACTION_NAME:
            await self._run_learn_song_owner_action(
                trigger, engine, cfg, context_message=context_message
            )
            return
        if raw_name == _SWITCH_SONG_VERSION_ACTION_NAME:
            await self._run_switch_song_version_owner_action(
                trigger, cfg, context_message=context_message
            )
            return
        spec = catalog.actions.get(raw_name)
        if spec is None or "owner_chat" not in spec.origins:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip "
                "reason=action_origin_forbidden",
                spec.name if spec is not None else "unknown",
            )
            return
        owner_mod = catalog.owners.get(spec.name, "")
        if not owner_mod or (inactive := catalog.inactive_reason(owner_mod)):
            reason_code = (
                str(inactive)
                if isinstance(inactive, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", inactive)
                else "module_disabled"
            )
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                spec.name,
                reason_code,
            )
            return
        if cfg.action_modes.get(spec.name, "active") == "off":
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=action_disabled",
                spec.name,
            )
            return
        if unavail := spec.unavailable():
            reason_code = (
                str(unavail)
                if isinstance(unavail, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", unavail)
                else "action_unavailable"
            )
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                spec.name,
                reason_code,
            )
            return
        parsed_args: dict[str, Any] = {}
        raw_args_json = trigger.arguments_json
        if isinstance(raw_args_json, str) and raw_args_json.strip():
            try:
                loaded = json.loads(raw_args_json)
            except (ValueError, TypeError):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    spec.name,
                )
                return
            if not isinstance(loaded, dict):
                log.info(
                    "event=owner_chat_action_done action=%s status=not_dispatched "
                    "reason=invalid_action_arguments",
                    spec.name,
                )
                return
            parsed_args = loaded
        alias = next((a for a in spec.aliases if a[0] == owner_mod), None)
        ticket = {
            "action_name": spec.name,
            "target_module": owner_mod,
            "action_type": alias[1] if alias else spec.name,
            "origin": "owner_chat",
            "target_scope": "",
            "arguments": parsed_args,
        }
        try:
            resolved_spec, validated = engine.resolve(ticket)
        except ValueError as exc:
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                spec.name,
                reject_reason(exc),
            )
            return
        except Exception as exc:
            log.warning(
                "event=owner_chat_action_done action=%s status=not_dispatched reason=%s",
                spec.name,
                type(exc).__name__,
            )
            return
        try:
            call = ActionCall(
                ticket,
                validated,
                engine.clock(),
                still_allowed=lambda: (
                    bool(engine.config.enabled)
                    and not catalog.inactive_reason(owner_mod)
                    and engine.config.action_modes.get(resolved_spec.name, "active") != "off"
                    and not resolved_spec.unavailable()
                ),
            )
            outcome = await resolved_spec.run(call)
            raw_status = str(getattr(outcome, "status", "done") or "done")
            raw_reason = str(getattr(outcome, "reason", "ok") or "ok")
            status_code = (
                raw_status if re.fullmatch(r"[A-Za-z0-9_.:-]{1,32}", raw_status) else "done"
            )
            reason_code = raw_reason if re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", raw_reason) else "ok"
            log.info(
                "event=owner_chat_action_done action=%s status=%s reason=%s",
                resolved_spec.name,
                status_code,
                reason_code,
            )
            if status_code == "done":
                planner = getattr(engine, "planner", None)
                replan = getattr(planner, "request_replan", None)
                if callable(replan):
                    try:
                        await replan(delay_seconds=35.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "event=owner_chat_action_done action=%s status=not_dispatched reason=%s",
                resolved_spec.name,
                type(exc).__name__,
            )

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
            # Replies and media references from other transports may never invoke OneBot RPCs.
            message = message.model_copy(
                update={
                    "images": [],
                    "files": [],
                    "audios": [],
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
            # Start hydration before opening the debounce window. Context-only images
            # in an admitted group can support the next addressed message; no LLM call.
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
        # Followers inherit admission to the batch. The merged message retains the
        # original attention reason, so a possible name still needs a relevance decision.
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
                    async with AsyncExitStack() as turn:
                        if message.kind == "private" and is_persona_owner(message, self.s):
                            await turn.enter_async_context(self.owner_dialogue_lock)
                        return await self._handle_message(job, turn)

                return await cancellable(run, batch.cancel)
            except asyncio.CancelledError:
                if batch.cancel.is_set():
                    raise Rejected(
                        "batch_interrupted" if batch.reason == "followup" else "batch_recalled"
                    ) from None
                raise
            finally:
                self.store.groups.active.pop(job["scope"], None)

    _ALLOWED_AFFECTION_REASONS = frozenset(
        {
            "attack_owner",
            "severe_offense",
            "boundary_or_hostility",
            "chat_negative",
            "chat_positive",
            "chat",
        }
    )

    def _classify_affection_reason(
        self, target: Message, delta: int, model_reason: str | None = None
    ) -> str:
        tag = model_reason.strip() if isinstance(model_reason, str) else ""
        if tag in self._ALLOWED_AFFECTION_REASONS:
            if delta <= -10 and tag in {
                "attack_owner",
                "severe_offense",
                "boundary_or_hostility",
                "chat_negative",
            }:
                return tag
            if -9 <= delta < 0 and tag == "chat_negative":
                return tag
            if delta > 0 and tag in {"chat_positive", "chat"}:
                return tag
        target_text = target.text or ""
        lowered = target_text.lower()
        owner_qq = str(getattr(self.s, "owner_qq_id", "") or "").strip()
        mentions_owner = (
            ("boiser" in lowered)
            or ("主人" in target_text)
            or (bool(owner_qq) and owner_qq in target_text)
        )
        if delta <= -10 and mentions_owner:
            return "attack_owner"
        if delta <= -20:
            return "severe_offense"
        if delta <= -10:
            return "boundary_or_hostility"
        if delta < 0:
            return "chat_negative"
        if delta > 0:
            return "chat_positive"
        return "chat"

    def _select_affection_target(
        self, message: Message, members: list[Message], actions
    ) -> tuple[Message, int, str]:
        target = message
        raw_delta = getattr(actions, "affection_delta", 0)
        delta = 0 if isinstance(raw_delta, bool) else int(raw_delta or 0)
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
        reason = self._classify_affection_reason(target, delta)
        return target, delta, reason

    def _select_affection_targets(
        self, message: Message, members: list[Message], actions
    ) -> list[tuple[Message, int, str]]:
        raw_deltas = getattr(actions, "affection_deltas", None)
        if raw_deltas:
            member_by_uid: dict[str, Message] = {}
            for m in members:
                if not self.bot_guard.known_bot(m):
                    member_by_uid[m.user_id] = m
            if not self.bot_guard.known_bot(message) and message.user_id not in member_by_uid:
                member_by_uid[message.user_id] = message
            has_non_owner = any(
                not is_persona_owner(m, self.s)
                for m in (members or [message])
                if not self.bot_guard.known_bot(m)
            )
            results: list[tuple[Message, int, str]] = []
            for item in raw_deltas:
                if isinstance(item, dict):
                    tid = str(item.get("target_id") or "").strip()
                    raw_d = item.get("delta", 0)
                    raw_reason = item.get("reason")
                else:
                    tid = str(getattr(item, "target_id", "") or "").strip()
                    raw_d = getattr(item, "delta", 0)
                    raw_reason = getattr(item, "reason", None)
                t = member_by_uid.get(tid)
                if t is None:
                    continue
                d = 0 if isinstance(raw_d, bool) else int(raw_d or 0)
                if d == 0:
                    continue
                if is_persona_owner(t, self.s) and d < 0 and has_non_owner:
                    continue
                results.append((t, d, self._classify_affection_reason(t, d, raw_reason)))
            if results:
                return results
        non_owner_uids = {
            m.user_id
            for m in members
            if not is_persona_owner(m, self.s) and not self.bot_guard.known_bot(m)
        }
        raw_single_delta = getattr(actions, "affection_delta", 0)
        single_delta = 0 if isinstance(raw_single_delta, bool) else int(raw_single_delta or 0)
        if (
            len(members) > 1
            and len(non_owner_uids) >= 2
            and not actions.anchor.reply_id
            and not actions.anchor.mentions
            and single_delta <= -10
        ):
            self.counters["affection_ambiguous_penalty_skipped"] += 1
            return []
        target, delta, reason = self._select_affection_target(message, members, actions)
        return [(target, delta, reason)]

    async def _alert_affection_blacklisted(self, message: Message, update) -> None:
        self.record("affection_blacklisted")
        if self.s.owner_delivery_platform != "onebot":
            return
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

    async def _handle_message(self, job: dict, turn: AsyncExitStack):
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
        caps, caps_msg = (
            await aff.capabilities_for_batch(message, members)
            if hasattr(aff, "capabilities_for_batch")
            else (await aff.capabilities_for(message), message)
        )
        aff_active = (
            aff.config.enabled
            and not self.bot_guard.known_bot(caps_msg)
            and (
                caps_msg.platform in {"onebot", "qq"} or getattr(self, "identity", None) is not None
            )
        )
        if aff_active and caps.level == 0:
            return
        direct_chat = message.kind == "private" or message.addressed
        if direct_chat:
            await turn.enter_async_context(self.world_state.experience.chatting())
        has_direct_audio = any(a.origin in {"", "当前消息"} for a in message.audios) or any(
            a.origin in {"", "当前消息"} for m in members for a in m.audios
        )
        fixed = None if has_direct_audio else await self.command(message)
        if fixed is not None:
            await self.store.enqueue_reply(job["id"], message, self.output(fixed))
            return
        direct = message.command_text if message.command_text is not None else message.text
        deep_match = re.fullmatch(r"/deep(?:\s+(.*))?", direct, re.DOTALL)
        deep = (
            (not has_direct_audio)
            and deep_match is not None
            and not self.bot_guard.known_bot(message)
        )
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
            if getattr(aff.config, "penalize_poke_ignored", False) is True:
                update = await aff.record_annoyance(message, "poke_ignored")
                if update.blacklisted_now:
                    await self._enqueue_affection_farewell(
                        job, message, members, None, watermark, None, update
                    )
                    await self._alert_affection_blacklisted(message, update)
            return
        if (
            aff_active
            and not aff.is_owner_id(caps_msg.platform, caps_msg.user_id)
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
        audios, audio_sources, audio_durations = await self.message_audios(
            message, members or [message], []
        )
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
        if audio_sources:
            payload["audio_sources"] = audio_sources
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
            module_queries = self.life_engine.query_options()
            intent = await self.ai.screen(
                current,
                job["subject"],
                purpose="message",
                group=message.kind == "group",
                message=message,
                before_call=message_guard,
                audit_id=audit_id,
                audit_job_id=job["id"],
                **({"module_queries": module_queries} if module_queries else {}),
                **({"relevance_context": relevance} if relevance else {}),
                **({"images": [i.gate() for i in images]} if images else {}),
                **({"audios": [a.gate() for a in audios]} if audios else {}),
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
        if not sandbox and owner(message, self.s) and hasattr(self, "world_state"):
            await self.world_state.experience.touch_owner(self.world_state.clock())
        if self.diary and not sandbox:
            await self.diary.group(job["id"], message, members=members)
        if not sandbox:
            if not await self.sessions.admit_history(
                message,
                members,
                expected_epoch=session["epoch"],
                **({"audio_durations": audio_durations} if audio_durations else {}),
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
                and not message.invites_group_review
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
        # Ambient group traffic only becomes a conversation once the gate opts in.
        if not direct_chat:
            await turn.enter_async_context(self.world_state.experience.chatting())
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
        selected_neighbour_audio_members = []
        neighbour_audio_events = []
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
            if not audios and filtered_nearby:
                consumed_audio_events = await self.store.groups.consumed_audio_events(message)
                audio_nearby = [
                    m
                    for m in filtered_nearby
                    if m.audios
                    and m.event_id not in consumed_audio_events
                    and (m.user_id == message.user_id or PRIOR_AUDIO_REF_RE.search(message.text))
                ]
                if audio_nearby:
                    (
                        neighbour_audios,
                        neighbour_audio_sources,
                        neighbour_audio_durations,
                    ) = await self.message_audios(
                        message,
                        [],
                        audio_nearby,
                        include_current=False,
                        selected_members=selected_neighbour_audio_members,
                    )
                    recalled_members.extend(selected_neighbour_audio_members)
                    await message_guard()
                    if neighbour_audios:
                        await self.ai.screen(
                            {
                                "audio_sources": neighbour_audio_sources,
                                "nearby_group_context": nearby_context,
                            },
                            job["subject"],
                            purpose="source",
                            message=message,
                            audios=[a.gate() for a in neighbour_audios],
                            before_call=message_guard,
                        )
                        neighbour_audio_events = [
                            m.event_id
                            for m in audio_nearby
                            if m.event_id
                            and (
                                m in selected_neighbour_audio_members
                                or any(
                                    a.identity
                                    in {
                                        ref.identity
                                        for sm in selected_neighbour_audio_members
                                        for ref in sm.audios
                                    }
                                    for a in m.audios
                                )
                            )
                        ]
                        for source in neighbour_audio_sources:
                            if "audio_number" in source:
                                source = {
                                    **source,
                                    "audio_number": source["audio_number"] + len(audios),
                                }
                            audio_sources.append(source)
                        audios.extend(neighbour_audios)
                        for eid, durs in neighbour_audio_durations.items():
                            audio_durations.setdefault(eid, []).extend(durs)
                        if audio_sources:
                            payload["audio_sources"] = audio_sources
            payload["context_status"].extend(
                {"event_id": m.event_id, "states": m.context_status}
                for m in filtered_nearby
                if m.context_status
            )
        if img_limit > 0 and not sandbox and not images and PRIOR_IMAGE_REF_RE.search(message.text):
            recalled_images, recalled_sources = await recall_historical_images(
                self,
                message,
                members,
                intent=intent,
                img_limit=img_limit,
                digested_events=digested_events,
                current_events=current_events,
                image_nearby=image_nearby,
                aff_active=aff_active,
                caps=caps,
                recalled_members=recalled_members,
                neighbour_members=neighbour_members,
                job_subject=job["subject"],
                message_guard=message_guard,
            )
            if recalled_images:
                late_images = True
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
        owner_chat_actions: list[dict[str, Any]] = []
        allow_voice = False
        allow_sing_song = False
        explicit_voice = False
        explicit_voice_bypass = False
        verified_owner_turn = owner(message, self.s) and all(owner(m, self.s) for m in members)
        if not sandbox:
            related_user_ids = [
                *(m.user_id for m in members),
                *(uid for m in members for uid in m.mentioned_ids if uid != self.s.onebot_self_id),
                *(m.user_id for m in filtered_nearby),
                *(str(item.get("user_id", "")) for item in history[-8:]),
            ]
            recalled_ctx = await self.memory_hub.recall_context(
                message,
                message.text,
                mode="chat",
                recall=getattr(intent, "need_memory_recall", False),
                sandbox=sandbox,
                related_user_ids=related_user_ids,
                allow_dual_profile=(not aff_active or caps.allow_dual_profile),
            )
            payload.update(recalled_ctx)
            if aff_active:
                sender_prof = await aff.profile(
                    message.platform, message.user_id, message.sender_nickname
                )
                sender_aff_payload = {
                    "score": sender_prof["score"],
                    "lv": sender_prof["lv"],
                    "tier": sender_prof["tier"],
                    "attitude": sender_prof["attitude"],
                }
                if sender_prof.get("is_owner"):
                    sender_aff_payload["unique_owner"] = True
                payload["sender_affection"] = sender_aff_payload
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
            identity = getattr(self, "identity", None)
            if (
                identity is not None
                and not self.bot_guard.known_bot(message)
                and not identity.is_muted(message.platform, message.user_id)
            ):
                l2_tags = await identity.get_l2_tags(message.platform, message.user_id)
                if l2_tags:
                    sender_aff = dict(payload.get("sender_affection") or {})
                    sender_aff["known_preferences"] = l2_tags
                    payload["sender_affection"] = sender_aff
            if not self.bot_guard.known_bot(message):
                material = await self.modules.context_data(
                    limit=4, now=time.time(), message=message
                )
                payload.update({k: v for k, v in material.items() if v})
            if not self.bot_guard.known_bot(message) and hasattr(self, "world_state"):
                news = await self.world_state.experience.news(message, time.time(), limit=2)
                if news:
                    payload["unshared_world_news"] = news
                ws_summary = await self.world_state.chat_sensor_payload(message)
                if ws_summary:
                    payload["world_state_summary"] = ws_summary
            if verified_owner_turn:
                owner_chat_actions = self._collect_owner_chat_actions()
                if owner_chat_actions:
                    payload["owner_chat_actions"] = owner_chat_actions
            voice_supported, _ = self.platform_voice_capability(message)
            voice_aff_ok = (not aff_active) or caps.allow_sticker
            explicit_voice = explicit_voice_request(*(m.text for m in members), message.text)
            explicit_song = self._is_explicit_song_request(*(m.text for m in members), message.text)
            explicit_voice_bypass = (explicit_voice or explicit_song) and (
                message.kind == "private" or verified_owner_turn
            )
            is_group_msg = message.kind == "group"
            scope = self.store.session_scope(message)
            cooldown_ok = self.voice_reply.can_send_now(
                scope, explicit=explicit_voice_bypass, is_group=is_group_msg
            )[0]
            allow_voice = (
                voice_supported
                and not self.bot_guard.known_bot(message)
                and voice_aff_ok
                and self.voice_reply.allowed_for_message(message, sandbox=sandbox)
                and cooldown_ok
            )
            allow_sing_song = (
                voice_supported
                and not self.bot_guard.known_bot(message)
                and voice_aff_ok
                and bool(self.songs.titles)
                and self.voice_reply.allowed_for_song(message, sandbox=sandbox)
                and cooldown_ok
            )
            if allow_voice:
                payload["voice_reply_available"] = True
            if allow_sing_song:
                payload["song_catalog"] = self.songs.catalog_entries()
        context_members = list(members)
        if message.kind == "group":
            context_members.extend(neighbour_members if (images or audios) else [])
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
                                "audios": [],
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
            and not self.modules.inactive_reason("bilibili_surf")
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
                if self.modules.inactive_reason("bilibili_surf"):
                    raise Rejected("module_disabled")
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
                prior = await self.bili_watcher.lookup_watched(bvid, message=message)
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
            allow_lookup = not sandbox and not self.bot_guard.known_bot(message)
            explicit_web = explicit_search(members[-1].command_text or members[-1].text)
            query_name = getattr(intent, "module_query", None)
            if allow_lookup and query_name in module_queries and not explicit_web:
                await message_guard()
                outcome = await self.life_engine.query(query_name)
                await message_guard()
                payload["module_query_result"] = {
                    "name": query_name,
                    "description": module_queries[query_name],
                    "status": outcome.status,
                    "reason": outcome.reason,
                    **({"data": outcome.details} if outcome.status == "done" else {}),
                }
            elif (
                allow_lookup
                and (not aff_active or caps.allow_search)
                and (explicit_web or getattr(intent, "need_external_lookup", False))
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
                            findings = (
                                await controller.execute_search(
                                    query,
                                    job["subject"],
                                    ai=self.ai,
                                    before_call=message_guard,
                                    cancel_event=batch.cancel if batch else None,
                                    plugin_bus=self.plugin_bus,
                                )
                                if hasattr(controller, "execute_search")
                                else await self.ai.search(
                                    query,
                                    job["subject"],
                                    before_call=message_guard,
                                    cancel_event=batch.cancel if batch else None,
                                )
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
                            diagnostics = exc.provider_diagnostics
                            self.record("web_search_failed", emit_audit=not diagnostics)
                            if diagnostics:
                                await self.store.runtime.event(
                                    "web_search_failed", "production", diagnostics=diagnostics
                                )
                                await self.store.runtime.filters.record_generation(
                                    audit_id, diagnostics=diagnostics
                                )
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
                    module_query=intent.module_query,
                    reply_score=getattr(intent, "reply_score", None),
                    reply_threshold=getattr(intent, "reply_threshold", None),
                )
            if tier >= 3 and message.platform == "onebot" and hasattr(self.sender, "quick_ack"):
                self.background(self.quick_ack(job, message, message_guard))
            if (
                not sandbox
                and message.platform == "onebot"
                and message.kind == "private"
                and hasattr(self.sender, "typing")
            ):
                await self.sender.typing(message)
            audio_turn = bool(audios)
            audio_event_ids = list(
                dict.fromkeys(
                    [
                        *audio_durations.keys(),
                        *(m.event_id for m in members if m.audios and m.event_id),
                    ]
                )
            )
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
                    **({"audios": audios} if audios else {}),
                )
            except BaseException:
                if gen_trace.get("chain_invalidate"):
                    await self.sessions.invalidate_chains(message)
                raise
            finally:
                if gen_trace.get("thinking_level") or gen_trace.get("provider_diagnostics"):
                    await self.store.runtime.filters.record_generation(
                        audit_id,
                        thinking_level=gen_trace.get("thinking_level"),
                        model_lane=gen_trace.get("model_lane"),
                        provider_incident=gen_trace.get("provider_incident"),
                        diagnostics=gen_trace.get("provider_diagnostics"),
                    )
            await message_guard()
            allowed_reply = payload["allowed_reply_ids"]
            payload.clear()
            images.clear()
            audios.clear()
            if generated.lstrip().startswith("{"):
                allowed_poke = (
                    set()
                    if sandbox
                    else set(allowed_at)
                    if message.kind == "group"
                    else {message.user_id}
                )
                allowed_aff_targets = {
                    m.user_id for m in (members or [message]) if not self.bot_guard.known_bot(m)
                }
                allow_owner_actions = not sandbox and verified_owner_turn
                allowed_owner_action_names = (
                    {a["action_name"] for a in owner_chat_actions} if allow_owner_actions else ()
                )
                parse_params = inspect.signature(parse_actions).parameters
                has_varkw = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in parse_params.values()
                )
                extra_parse_kwargs: dict[str, Any] = {}
                if has_varkw or "allowed_affection_targets" in parse_params:
                    extra_parse_kwargs["allowed_affection_targets"] = allowed_aff_targets
                if has_varkw or "allow_owner_chat_actions" in parse_params:
                    extra_parse_kwargs["allow_owner_chat_actions"] = allow_owner_actions
                if has_varkw or "allowed_owner_chat_actions" in parse_params:
                    extra_parse_kwargs["allowed_owner_chat_actions"] = allowed_owner_action_names
                if has_varkw or "allow_voice" in parse_params:
                    extra_parse_kwargs["allow_voice"] = allow_voice
                if has_varkw or "allow_sing_song" in parse_params:
                    extra_parse_kwargs["allow_sing_song"] = allow_sing_song
                if has_varkw or "song_titles" in parse_params:
                    extra_parse_kwargs["song_titles"] = self.songs.titles
                if has_varkw or "allow_audio_gist" in parse_params:
                    extra_parse_kwargs["allow_audio_gist"] = audio_turn
                actions, folded = parse_actions(
                    generated,
                    public=public,
                    allowed_at=allowed_at,
                    allowed_reply=allowed_reply,
                    labels=self.memes.labels,
                    settings=self.s,
                    tier=tier,
                    allowed_poke=allowed_poke,
                    allow_longnick=(not sandbox and owner(message, self.s)),
                    allow_sticker=(not aff_active or caps.allow_sticker),
                    allow_emoji_like=(not aff_active or caps.allow_emoji_like),
                    allow_poke_action=(
                        not aff_active or (caps.allow_poke and caps.allow_poke_inbound)
                    ),
                    **extra_parse_kwargs,
                )
                aff_targets = self._select_affection_targets(message, members, actions)
                has_social_response = (not actions.abstain) or bool(
                    actions.emoji_like or actions.poke_qq
                )
                blacklisted_hits: list[tuple[Message, Any]] = []
                if aff_active:
                    for aff_target, aff_delta, aff_reason in aff_targets:
                        if has_social_response:
                            upd = await aff.apply_turn_delta(
                                aff_target,
                                aff_delta,
                                sandbox_mode=sandbox,
                                daily_first_bonus=True,
                                reason=aff_reason,
                            )
                        elif aff_delta < 0:
                            upd = await aff.apply_turn_delta(
                                aff_target,
                                aff_delta,
                                sandbox_mode=sandbox,
                                daily_first_bonus=False,
                                reason=aff_reason,
                            )
                        else:
                            upd = None
                        if upd and upd.blacklisted_now:
                            await self._alert_affection_blacklisted(aff_target, upd)
                            blacklisted_hits.append((aff_target, upd))
                farewell_bypass = any(
                    t.user_id == message.user_id and t.platform == message.platform
                    for t, _ in blacklisted_hits
                )
                farewell_bypass_user_ids = sorted({t.user_id for t, _ in blacklisted_hits})
                if blacklisted_hits:
                    first_bl_target, first_bl_upd = blacklisted_hits[0]
                    if not actions.anchor.reply_id and first_bl_target.event_id in allowed_reply:
                        actions.anchor.reply_id = first_bl_target.event_id
                    if actions.abstain or not any(b.strip() for b in actions.bubbles):
                        actions.abstain = False
                        actions.bubbles = [
                            first_bl_upd.farewell_message or aff.config.farewell_message
                        ]
                        actions.sticker.label = None
                        actions.emoji_like = None
                        actions.poke_qq = None
                        actions.voice_text = None
                        actions.sing_song = None
                        folded = None
                if not sandbox and not actions.abstain and not farewell_bypass:
                    if (
                        message.platform == "onebot"
                        and actions.save_meme_label
                        and not self.bot_guard.known_bot(message)
                    ):
                        await self._auto_save_meme(
                            message, [*neighbour_members, *members], actions.save_meme_label
                        )
                    if actions.set_longnick and owner(message, self.s):
                        try:
                            await self.longnick.update(actions.set_longnick, automatic=False)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass
                    if actions.trigger_action and verified_owner_turn:
                        self._spawn_owner_chat_action(
                            actions.trigger_action, context_message=message
                        )
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
                                **(
                                    {"farewell_bypass_user_ids": farewell_bypass_user_ids}
                                    if farewell_bypass_user_ids
                                    else {}
                                ),
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
                if actions.audio_gist:
                    try:
                        check_memory_text(actions.audio_gist, self.s)
                    except Rejected:
                        actions.audio_gist = None
                if (
                    not sandbox
                    and actions.audio_gist
                    and (audio_event_ids or (audio_turn and message.event_id))
                ):
                    await self.sessions.annotate_audio_gist(
                        message,
                        audio_event_ids,
                        actions.audio_gist,
                        fallback_event_id=message.event_id or None,
                        durations=audio_durations,
                        expected_epoch=session["epoch"],
                    )
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
                        **(
                            {"explicit_voice": explicit_voice_bypass}
                            if (actions.voice_text or actions.sing_song)
                            else {}
                        ),
                        **({"farewell_bypass_mute": True} if farewell_bypass else {}),
                        **(
                            {"farewell_bypass_user_ids": farewell_bypass_user_ids}
                            if farewell_bypass_user_ids
                            else {}
                        ),
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
                if neighbour_audio_events:
                    await self.store.groups.mark_audios_consumed(message, neighbour_audio_events)
                if not sandbox and not actions.abstain and not farewell_bypass:
                    recap_message = message
                    if audio_turn:
                        msg_durs = audio_durations.get(message.event_id) or [
                            d for ds in audio_durations.values() for d in ds
                        ]
                        recap_message = message.model_copy(
                            update={
                                "text": apply_audio_memory_tags(
                                    message.text, msg_durs, gist=actions.audio_gist
                                )
                            }
                        )
                    self._record_owner_turn_recap(
                        recap_message,
                        " ".join(b.strip() for b in actions.bubbles if b.strip())
                        or (folded or "")
                        or (actions.voice_text or "")
                        or (f"[唱歌] {actions.sing_song}" if actions.sing_song else ""),
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
        if not sandbox and result:
            self._record_owner_turn_recap(message, result)
        await self.store.runtime.filters.update_audit(audit_id, "queued")

    def _record_owner_turn_recap(self, message: Message, reply_text: str) -> None:
        memory_hub = getattr(self, "memory_hub", None)
        if memory_hub is not None and hasattr(memory_hub, "record_turn_recap"):
            memory_hub.record_turn_recap(message, reply_text)

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
        # Recheck unsubscribe requests that arrived while the model was running.
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
        async with (
            self.world_state.experience.chatting() if not reply["proactive"] else nullcontext()
        ):
            return await self._deliver_reply(reply)

    async def _deliver_reply(self, reply) -> bool:
        reply_actions = reply.get("actions") or {}
        message = Message.model_validate(reply["message"])
        farewell_bypass = bool(reply_actions.get("farewell_bypass_mute")) and (
            await is_affection_muted_only(self, message)
        )
        raw_bypass_ids = {
            str(u).strip()
            for u in (reply_actions.get("farewell_bypass_user_ids") or [])
            if isinstance(u, str) and str(u).strip()
        }
        if reply_actions.get("farewell_bypass_mute"):
            raw_bypass_ids.add(message.user_id)
        if reply_actions and not (
            bool(reply_actions.get("farewell_bypass_mute"))
            and "bubbles" not in reply_actions
            and reply.get("text")
        ):
            from .outgoing import deliver_actions

            return await deliver_actions(self, reply)
        status = "sent"
        detail = ""
        text_confirmed = False
        image_confirmed = False
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
                    if not chat_module(self.s, message.platform):
                        return False
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
                    bypass_uids: set[str] = set()
                    if raw_bypass_ids:
                        for candidate in [message, *(participants or [message])]:
                            if (
                                candidate.user_id in raw_bypass_ids
                                and candidate.user_id not in bypass_uids
                                and await is_affection_muted_only(self, candidate)
                            ):
                                bypass_uids.add(candidate.user_id)
                    return (
                        (await self.sessions.snapshot(message))["epoch"] == delivery_epoch
                    ) and self.batch_authorized(
                        message,
                        participants or [message],
                        bypass_mute_user_ids=bypass_uids,
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

                if message.platform not in {"onebot", "qq"}:
                    module = chat_module(self.s, message.platform)
                    if (
                        reply["proactive"]
                        or not module
                        or not reply["text"]
                        or not await valid_after_delay()
                    ):
                        await self.store.finish_reply(reply["id"], "cancelled")
                        await self._finish_delivery_audit(reply, "cancelled", "unauthorized")
                        return True
                    ident = await module.send_chat(
                        message, self.output(reply["text"]), before_send=valid_after_delay
                    )
                    receipt = {"message_id": confirmed_message_id(ident)}
                elif (
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
            if receipt is not None or reply["text"]:
                from .outbound_actions import valid_receipt

                if not valid_receipt(receipt, platform=message.platform):
                    raise Rejected("onebot_send_unconfirmed")
                text_confirmed = True
            if text_confirmed:
                retained_text = reply["text"]
                if not reply["proactive"] and self.world_state.experience.ready:
                    async with self.store.transaction():
                        await self.world_state.experience.confirmed_tx(
                            message, reply["id"], self.world_state.clock(), [retained_text]
                        )
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
                force_meme = explicit_meme_request(*(m.text for m in participants), message.text)
                allowed = await self.memes.reserve_reply(
                    self.store.session_scope(message),
                    reply["id"],
                    reply.get("meme_tag"),
                    force=force_meme,
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
                            if not text_confirmed and self.world_state.experience.ready:
                                async with self.store.transaction():
                                    await self.world_state.experience.spend_chat_tx(
                                        reply["id"], self.world_state.clock()
                                    )
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
            if not text_confirmed and not image_confirmed:
                status = "cancelled"
        except asyncio.CancelledError:
            # Leave 'sending': startup recovery will mark it uncertain without resending.
            raise
        except Rejected as exc:
            code = str(exc).strip()
            if code == "reply_invalidated":
                status, detail = "cancelled", "reply_invalidated"
            elif code in PRE_DISPATCH_REJECTED and not text_confirmed and not image_confirmed:
                status, detail = "failed", code
            else:
                status, detail = "uncertain", safe_delivery_detail(exc)
                self.record("delivery_uncertain")
        except Exception:
            status, detail = "uncertain", "delivery_uncertain"
            self.record("delivery_uncertain")
        await self.bot_guard.extend_cooldown(message, reply["id"])
        if not await self.store.finish_reply(reply["id"], status):
            await self._finish_delivery_audit(reply, "cancelled", "session_reset")
            return True  # A reset cancelled this delivery while the transport awaited its ACK.
        await self._finish_delivery_audit(reply, status, detail)
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
