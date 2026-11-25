"""Inbound chat turn pipeline mixin for `Service`."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import re
import time
import uuid
from contextlib import AsyncExitStack
from typing import Any

from hakimi_plugin_sdk.bili_common import extract_bvid

from .attention import can_watch_video
from .bot_server import is_persona_owner
from .gateway import redact_excerpt
from .generation_policy import Scenario
from .group_pipeline import other_mentions
from .media_turn import PRIOR_AUDIO_REF_RE, PRIOR_IMAGE_REF_RE, recall_historical_images
from .memory_compressor import check_memory_text
from .mentions import parse_reply, valid_at_targets
from .models import Message
from .outbound_actions import parse_actions
from .output_links import filter_reply_links
from .routing import RouteDecision
from .security import Rejected, authorize, owner
from .session_manager import apply_audio_memory_tags
from .streaming import cancellable
from .voice_reply import explicit_voice_request
from .web_search import (
    explicit_search,
    has_concrete_search_subject,
    natural_search_cue,
    sanitize_model_search_query,
    search_query,
)


class _ServiceTurnMixin:
    """Mixin implementing single-turn chat admission, context assembly, generation, and queuing."""

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
            if self._random() > caps.direct_reply_prob:
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
        current = {
            key: payload[key]
            for key in ("current_message", "current_sender", "surface", "directly_addressed")
        }
        if previews:
            current["web_preview_untrusted"] = previews[:2]
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
            await self._maybe_enqueue_lurk_emoji(
                job=job,
                message=message,
                members=members,
                session=session,
                watermark=watermark,
                audit_id=audit_id,
                intent=intent,
                sandbox=sandbox,
                aff_active=aff_active,
                caps=caps,
            )
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
            result = await self._run_bili_video_turn(
                bvid=bvid,
                job=job,
                message=message,
                members=members,
                session=session,
                direct=direct,
                deep=deep,
                public=public,
                sandbox=sandbox,
                aff_active=aff_active,
                caps=caps,
            )
        else:
            allow_lookup = not sandbox and not self.bot_guard.known_bot(message)
            raw_turn_text = members[-1].command_text or members[-1].text
            explicit_web = explicit_search(raw_turn_text)
            natural_web_cue = natural_search_cue(raw_turn_text)
            controller = self.store.runtime.web_search
            defer_to_model_search = False

            async def _execute_web_search(clean_query: str, *, trigger: str) -> None:
                self.store.groups.phase(message, "generating")
                active_batch = self.store.groups.active.get(self.store.session_scope(message))
                payload["web_search_status"] = "unavailable"
                search_trace: dict[str, Any] = {}
                try:
                    if hasattr(controller, "execute_search"):
                        findings = await controller.execute_search(
                            clean_query,
                            job["subject"],
                            ai=self.ai,
                            before_call=message_guard,
                            cancel_event=active_batch.cancel if active_batch else None,
                            plugin_bus=self.plugin_bus,
                            search_trace=search_trace,
                        )
                    else:
                        ai_search_kwargs: dict[str, Any] = {
                            "before_call": message_guard,
                            "cancel_event": active_batch.cancel if active_batch else None,
                        }
                        ai_search_sig = inspect.signature(self.ai.search)
                        if "search_trace" in ai_search_sig.parameters or any(
                            p.kind == inspect.Parameter.VAR_KEYWORD
                            for p in ai_search_sig.parameters.values()
                        ):
                            ai_search_kwargs["search_trace"] = search_trace
                        findings = await self.ai.search(
                            clean_query,
                            job["subject"],
                            **ai_search_kwargs,
                        )
                    if findings and findings.strip() and controller.config.enabled:
                        payload["web_search_findings"] = findings
                        payload["web_search_status"] = "grounded"
                        ws_audit: dict[str, Any] = {"status": "grounded", "trigger": trigger}
                        for key in ("provider", "fallback", "retried"):
                            if search_trace.get(key):
                                ws_audit[key] = search_trace[key]
                        await self.store.runtime.filters.record_generation(
                            audit_id,
                            diagnostics=search_trace.get("provider_diagnostics"),
                            web_search=ws_audit,
                        )
                    else:
                        if not controller.config.enabled:
                            payload["web_search_status"] = "disabled"
                        self.record("web_search_empty")
                        ws_audit = {
                            "status": payload["web_search_status"],
                            "trigger": trigger,
                            "reason": "web_search_empty",
                        }
                        for key in ("provider", "fallback", "retried"):
                            if search_trace.get(key):
                                ws_audit[key] = search_trace[key]
                        await self.store.runtime.filters.record_generation(
                            audit_id,
                            diagnostics=search_trace.get("provider_diagnostics"),
                            web_search=ws_audit,
                        )
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
                    diagnostics = exc.provider_diagnostics or search_trace.get(
                        "provider_diagnostics"
                    )
                    self.record("web_search_failed", emit_audit=not diagnostics)
                    if diagnostics:
                        await self.store.runtime.event(
                            "web_search_failed", "production", diagnostics=diagnostics
                        )
                    ws_audit = {
                        "status": payload["web_search_status"],
                        "trigger": trigger,
                        "reason": str(exc),
                    }
                    for key in ("provider", "fallback", "retried"):
                        if search_trace.get(key):
                            ws_audit[key] = search_trace[key]
                    await self.store.runtime.filters.record_generation(
                        audit_id,
                        diagnostics=diagnostics,
                        web_search=ws_audit,
                    )
                await message_guard()

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
                pre_trigger = "explicit" if explicit_web else "gate"
                if not controller.config.enabled:
                    payload["web_search_status"] = "disabled"
                    self.record("web_search_disabled")
                    await self.store.runtime.filters.record_generation(
                        audit_id,
                        web_search={"status": "disabled", "trigger": pre_trigger},
                    )
                elif (
                    not explicit_web
                    and (message.kind != "group" or bool(message.addressed))
                    and not has_concrete_search_subject(raw_turn_text)
                ):
                    defer_to_model_search = True
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
                    except Rejected as exc:
                        query = None
                        payload["web_search_status"] = "skipped_query"
                        self.record("web_search_skipped_query")
                        await self.store.runtime.filters.record_generation(
                            audit_id,
                            web_search={
                                "status": "skipped_query",
                                "trigger": pre_trigger,
                                "reason": str(exc),
                            },
                        )
                    if query is not None:
                        await _execute_web_search(query, trigger=pre_trigger)
            can_model_search = (
                allow_lookup
                and (not aff_active or caps.allow_search)
                and (message.kind != "group" or bool(message.addressed))
                and "web_search_status" not in payload
                and "module_query_result" not in payload
                and controller.config.enabled
            )
            if can_model_search:
                payload["web_search_available"] = True
                if defer_to_model_search or natural_web_cue:
                    payload["web_search_requested"] = True
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
            gen_scenario = (
                Scenario.OWNER_PRIVATE
                if message.kind == "private" and is_persona_owner(message, self.s)
                else Scenario.FAST_CHAT
            )

            gen_trace: dict[str, Any] = {}

            async def _generate_chat_once(active_route):
                gen_trace.clear()
                try:
                    return await self.ai.generate(
                        payload,
                        job["subject"],
                        scenario=gen_scenario,
                        deep=deep,
                        route=active_route,
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

            generated = await _generate_chat_once(chat_route)
            await message_guard()
            allowed_reply = payload["allowed_reply_ids"]
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
            has_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parse_params.values())

            def _parse_chat_actions(raw_output: str, *, allow_search_req: bool):
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
                if has_varkw or "allow_web_search" in parse_params:
                    extra_parse_kwargs["allow_web_search"] = allow_search_req
                return parse_actions(
                    raw_output,
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

            pre_parsed = None
            if can_model_search and generated.lstrip().startswith("{"):
                first_actions, first_folded = _parse_chat_actions(generated, allow_search_req=True)
                if first_actions.search_query:
                    payload.pop("web_search_available", None)
                    payload.pop("web_search_requested", None)
                    try:
                        clean_model_query = sanitize_model_search_query(
                            first_actions.search_query,
                            message,
                            self.s,
                            batch_messages=members,
                        )
                    except Rejected as exc:
                        clean_model_query = None
                        payload["web_search_status"] = "skipped_query"
                        self.record("web_search_skipped_query")
                        await self.store.runtime.filters.record_generation(
                            audit_id,
                            web_search={
                                "status": "skipped_query",
                                "trigger": "model",
                                "reason": str(exc),
                            },
                        )
                    if clean_model_query is not None:
                        await _execute_web_search(clean_model_query, trigger="model")
                    if payload.get("web_search_findings") and tier < 2:
                        tier = 2
                        if aff_active:
                            tier = min(tier, caps.max_tier)
                            if not caps.allow_carbon and tier >= 4:
                                tier = 3
                        if isinstance(chat_route, RouteDecision):
                            chat_route = RouteDecision(
                                str(chat_route),
                                tier=tier,
                                sandbox_mode=chat_route.sandbox_mode,
                                need_memory_recall=chat_route.need_memory_recall,
                                need_external_lookup=True,
                                module_query=chat_route.module_query,
                                reply_score=getattr(chat_route, "reply_score", None),
                                reply_threshold=getattr(chat_route, "reply_threshold", None),
                            )
                    await self.sessions.invalidate_chains(message)
                    generated = await _generate_chat_once(chat_route)
                    await message_guard()
                else:
                    pre_parsed = (first_actions, first_folded)
            payload.clear()
            images.clear()
            audios.clear()
            if generated.lstrip().startswith("{"):
                if pre_parsed is not None:
                    actions, folded = pre_parsed
                else:
                    actions, folded = _parse_chat_actions(generated, allow_search_req=False)
                await self._finalize_chat_actions_turn(
                    job=job,
                    message=message,
                    members=members,
                    neighbour_members=neighbour_members,
                    session=session,
                    watermark=watermark,
                    audit_id=audit_id,
                    actions=actions,
                    folded=folded,
                    allowed_at=allowed_at,
                    allowed_reply=allowed_reply,
                    public=public,
                    sandbox=sandbox,
                    aff=aff,
                    aff_active=aff_active,
                    caps=caps,
                    verified_owner_turn=verified_owner_turn,
                    explicit_voice_bypass=explicit_voice_bypass,
                    audio_turn=audio_turn,
                    audio_event_ids=audio_event_ids,
                    audio_durations=audio_durations,
                    image_sources=image_sources,
                    neighbour_image_events=neighbour_image_events,
                    neighbour_audio_events=neighbour_audio_events,
                    consumed_file_events=consumed_file_events,
                    gen_trace=gen_trace,
                )
                return
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

    async def _maybe_enqueue_lurk_emoji(
        self,
        *,
        job: dict,
        message: Message,
        members: list[Message],
        session: dict,
        watermark: str,
        audit_id: str,
        intent: Any,
        sandbox: bool,
        aff_active: bool,
        caps: Any,
    ) -> None:
        if (
            sandbox
            or message.platform != "onebot"
            or message.kind != "group"
            or message.invites_group_review
            or self.bot_guard.known_bot(message)
            or (aff_active and not caps.allow_emoji_like)
        ):
            return
        r_score = getattr(intent, "reply_score", None)
        r_thresh = getattr(intent, "reply_threshold", None)
        if not (
            isinstance(r_score, (int, float))
            and isinstance(r_thresh, (int, float))
            and max(0.20, r_thresh * 0.55) <= r_score < r_thresh
        ):
            return
        scope_key = self.store.session_scope(message)
        now_ts = time.time()
        if now_ts - self._lurk_react_at.get(scope_key, 0.0) < 180:
            return
        emoji_id = self._pick_lurk_emoji(members)
        target_id = next(
            (m.event_id for m in reversed(members) if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)),
            None,
        )
        if not (emoji_id and target_id):
            return
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

    async def _run_bili_video_turn(
        self,
        *,
        bvid: str,
        job: dict,
        message: Message,
        members: list[Message],
        session: dict,
        direct: str,
        deep: bool,
        public: bool,
        sandbox: bool,
        aff_active: bool,
        caps: Any,
    ) -> str:
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
        # Sources and generated summaries are deliberately excluded from conversation memory.
        return self.output(text)

    async def _finalize_chat_actions_turn(
        self,
        *,
        job: dict,
        message: Message,
        members: list[Message],
        neighbour_members: list[Message],
        session: dict,
        watermark: str,
        audit_id: str,
        actions: Any,
        folded: str | None,
        allowed_at: set[str],
        allowed_reply: list[str],
        public: bool,
        sandbox: bool,
        aff: Any,
        aff_active: bool,
        caps: Any,
        verified_owner_turn: bool,
        explicit_voice_bypass: bool,
        audio_turn: bool,
        audio_event_ids: list[str],
        audio_durations: dict[str, list[float]],
        image_sources: list[dict[str, Any]],
        neighbour_image_events: list[str],
        neighbour_audio_events: list[str],
        consumed_file_events: list[str],
        gen_trace: dict[str, Any],
    ) -> None:
        aff_targets = self._select_affection_targets(message, members, actions)
        has_social_response = (not actions.abstain) or bool(actions.emoji_like or actions.poke_qq)
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
                actions.bubbles = [first_bl_upd.farewell_message or aff.config.farewell_message]
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
                self._spawn_owner_chat_action(actions.trigger_action, context_message=message)
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
            await self.store.runtime.filters.update_audit(audit_id, "abstained", "model_abstain")
            return
        if not actions.image_digest:
            actions.image_digest = next(
                (v["local_sticker_label"] for v in image_sources if v.get("local_sticker_label")),
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

    def _record_owner_turn_recap(self, message: Message, reply_text: str) -> None:
        memory_hub = getattr(self, "memory_hub", None)
        if memory_hub is not None and hasattr(memory_hub, "record_turn_recap"):
            memory_hub.record_turn_recap(message, reply_text)
