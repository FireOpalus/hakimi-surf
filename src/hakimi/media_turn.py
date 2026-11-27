"""Per-turn rich media loading: sticker classification, images, files, and forward expansion."""

from __future__ import annotations

import asyncio
import dataclasses
import re
from collections.abc import Awaitable, Callable
from typing import Any

from .audio_media import MAX_INLINE_AUDIO_BYTES, AudioLoader, ModelAudio
from .group_pipeline import other_mentions
from .media import ImageLoader
from .memes import MAX_UPLOAD_BYTES
from .models import Message
from .rich_message_parser import parse_forward
from .security import Rejected, authorize

PRIOR_IMAGE_REF_RE = re.compile(
    r"(?:这|那|上|刚|前|原).{0,6}(?:张|幅|个)?(?:图|截图|照片|画面|报错图)"
    r"|(?:图|截图|照片)(?:里|中|上)"
    r"|(?:看|瞅|瞧|分析|解释|推导)(?:下|一下|看)?.{0,6}(?:图|截图|照片)"
)
PRIOR_AUDIO_REF_RE = re.compile(
    r"(?:这|那|上|刚|前|原).{0,6}(?:条|段|个)?(?:语音|声音|录音)"
    r"|(?:听|帮我听)(?:下|一下|听)?.{0,6}(?:语音|录音|刚才|说的)"
)
PRIOR_FILE_REF_RE = re.compile(
    r"(?:文件|日志|代码|脚本|配置|报错)"
    r"|\.(?:log|txt|py|json|ya?ml|toml|md|js|ts|sh|sql)\b",
    re.I,
)


def image_sender(service: Any, message: Message) -> str:
    nickname = message.sender_nickname.strip()[:40] or "群友"
    parts = message.subject_parts(service.s.namespace)
    token = service.store.vault.digest("image-speaker", *parts)[:6]
    return f"{nickname}（成员标识 {token}）"


def is_sticker_candidate(service: Any, message: Message, ref: Any) -> bool:
    if getattr(ref, "is_sticker", False):
        return True
    if isinstance(service.images, ImageLoader):
        cached = service.images.cache_get(service.store.session_scope(message), ref)
        if cached is not None and cached.digest and service.memes.semantic_label(cached.digest):
            return True
    return False


async def load_message_images(
    service: Any,
    message: Message,
    members: list[Message],
    nearby: list[Message],
    *,
    limit: int = 2,
    include_current: bool = True,
    nearby_origin: str = "紧邻群消息",
    selected_members: list[Message] | None = None,
) -> tuple[list[Any], list[dict[str, Any]]]:
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
            if not authorize(candidate, service.s) or other_mentions(item):
                continue
            for ref in item.images:
                if ref.identity not in seen:
                    seen.add(ref.identity)
                    entries.append((ref, item, origin))
        if origin == nearby_origin and entries:
            regular = [e for e in entries if not service._is_sticker_candidate(message, e[0])]
            stickers = [e for e in entries if service._is_sticker_candidate(message, e[0])]
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
    if hasattr(service.images, "load_results"):
        results = await service.images.load_results(
            refs,
            **(
                {"scope": service.store.session_scope(message)}
                if isinstance(service.images, ImageLoader)
                else {}
            ),
        )
    else:
        results = await service.images.load(refs)  # Compatible injected loaders in integrations.
    images, sources = [], []
    valid_loaded = sum(1 for item in results if item is not None)
    for (ref, member, origin), loaded in zip(selected, results, strict=True):
        sender_channel = "QQ" if member.platform in {"onebot", "qq"} else member.platform
        data: dict[str, Any] = {
            "sender": f"{ref.sender_nickname or '群友'}({sender_channel}:{ref.sender_id})"
            if ref.sender_id
            else service.image_sender(member),
            "origin": origin if ref.origin in {"", "当前消息"} else origin + " / " + ref.origin,
        }
        if loaded is None:
            data["status"] = (
                service.images.failure_status(ref)
                if hasattr(service.images, "failure_status")
                else "unavailable"
            )
        elif valid_loaded == 1 and len(loaded.tiles) == 2 and len(images) + 2 <= limit:
            split_imgs = loaded.tile_images()
            if selected_members is not None:
                selected_members.append(member)
            semantic = service.memes.semantic_label(loaded.digest)
            for part_idx, part_img in enumerate(split_imgs, 1):
                images.append(part_img)
                part_data: dict[str, Any] = {
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
            semantic = service.memes.semantic_label(loaded.digest)
            if semantic:
                data["local_sticker_label"] = semantic
        sources.append(data)
    if omitted:
        sources.append({"status": "omitted", "count": omitted})
    return images, sources


async def load_message_audios(
    service: Any,
    message: Message,
    members: list[Message],
    nearby: list[Message],
    *,
    include_current: bool = True,
    nearby_origin: str = "紧邻群消息",
    selected_members: list[Message] | None = None,
    max_total_bytes: int = MAX_INLINE_AUDIO_BYTES,
) -> tuple[list[ModelAudio], list[dict[str, Any]], dict[str, list[float]]]:
    """Load turn audios without business count/duration caps, bounded by inline byte cap."""
    selected: list[tuple[Any, Message, str]] = []
    seen: set[str] = set()
    for origin, messages in (
        ("本批消息", members),
        (nearby_origin, nearby),
    ):
        for item in messages:
            candidate = (
                item.model_copy(update={"addressed": True}) if item.kind == "group" else item
            )
            if not authorize(candidate, service.s) or other_mentions(item):
                continue
            for ref in item.audios:
                if ref.identity and ref.identity not in seen:
                    seen.add(ref.identity)
                    selected.append((ref, item, origin))
    for ref in message.audios if include_current else []:
        if ref.identity and ref.identity not in seen:
            selected.append((ref, message, "引用或转发材料"))
            seen.add(ref.identity)
    if not selected:
        return [], [], {}
    refs = [entry[0] for entry in selected]
    loader = getattr(service, "audios", None)
    if loader is None:
        return [], [], {}
    if hasattr(loader, "load_results"):
        results = await loader.load_results(
            refs,
            **(
                {
                    "scope": service.store.session_scope(message),
                    "max_total_bytes": max_total_bytes,
                }
                if isinstance(loader, AudioLoader)
                else {}
            ),
        )
    else:
        results = await loader.load(refs)
    audios: list[ModelAudio] = []
    sources: list[dict[str, Any]] = []
    durations_by_event: dict[str, list[float]] = {}
    for (ref, member, origin), loaded in zip(selected, results, strict=True):
        sender_channel = "QQ" if member.platform in {"onebot", "qq"} else member.platform
        data: dict[str, Any] = {
            "sender": f"{ref.sender_nickname or '群友'}({sender_channel}:{ref.sender_id})"
            if ref.sender_id
            else service.image_sender(member),
            "origin": origin if ref.origin in {"", "当前消息"} else origin + " / " + ref.origin,
        }
        if loaded is None:
            data["status"] = (
                loader.failure_status(ref) if hasattr(loader, "failure_status") else "unavailable"
            )
        else:
            audios.append(loaded)
            data.update(
                audio_number=len(audios),
                status="available",
                duration_sec=round(loaded.duration_sec, 1),
            )
            if selected_members is not None:
                selected_members.append(member)
            if member.event_id:
                durations_by_event.setdefault(member.event_id, []).append(loaded.duration_sec)
        sources.append(data)
    return audios, sources, durations_by_event


async def load_turn_files(
    service: Any,
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
        if not authorize(check, service.s) or other_mentions(item):
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
            await service.store.groups.consumed_file_events(message)
            if message.kind == "group"
            else set()
        )
        mention_any = bool(PRIOR_FILE_REF_RE.search(message.text))
        nearby_pool = [m for m in filtered_nearby if m.files and m.event_id not in consumed]
        hot_pool = [
            m
            for m in service.hot.recent_with_files(
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
            if await service.recalled([item]):
                continue
            check = item.model_copy(update={"addressed": True}) if item.kind == "group" else item
            if not authorize(check, service.s) or other_mentions(item):
                continue
            for ref in item.files:
                if ref.identity not in seen_ids:
                    seen_ids.add(ref.identity)
                    candidates.append((ref, item, "近期群聊文件"))
    if not candidates:
        return [], [], []
    selected: list[Message] = []
    attachments, sources = await service.files.load_for_turn(
        message, candidates, limit=1, selected_members=selected
    )
    consumed_events = [
        m.event_id for m in selected if m.event_id and m.event_id not in current_events
    ]
    return attachments, sources, consumed_events


async def expand_forwards(service: Any, message: Message) -> Message:
    texts, images = [message.text], list(message.images)
    seen_ids = {i.identity for i in images}
    files = list(message.files)
    seen_file_ids = {f.identity for f in files}
    audios = list(message.audios)
    seen_audio_ids = {a.identity for a in audios}
    videos, shorts = list(message.video_ids), list(message.short_urls)
    statuses = set(message.context_status)
    for forward_id in message.forward_ids:
        rich = parse_forward(await service.sender.forward(forward_id), message.bot_id)
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
        for aref in rich.audios:
            if aref.identity not in seen_audio_ids:
                audios.append(aref)
                seen_audio_ids.add(aref.identity)
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
    if len(audios) > 64:
        audios = audios[:64]
        statuses.add("truncated")
    if any(secret and len(secret) >= 8 and secret in text for secret in service.secrets):
        raise Rejected("credential_in_input")
    return message.model_copy(
        update={
            "text": text,
            "images": images,
            "files": files,
            "audios": audios,
            "video_ids": videos[:5],
            "short_urls": shorts[:5],
            "forward_ids": [],
            "context_expanded": True,
            "context_status": sorted(statuses),
        }
    )


async def auto_save_meme(
    service: Any, message: Message, candidates: list[Message], label: str
) -> bool:
    if not hasattr(service.images, "fetch_raw"):
        return False
    ref = next(
        (img for item in reversed(candidates) for img in reversed(item.images)),
        None,
    )
    try:
        if ref is None and message.reply_ids:
            for reply_id in reversed(message.reply_ids):
                try:
                    replied = await service.lookup_received(message, reply_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    continue
                if replied and replied.images:
                    ref = replied.images[-1]
                    break
        if ref is None:
            return False
        raw = await service.images.fetch_raw(ref, max_bytes=MAX_UPLOAD_BYTES)
        await service.memes.upload_bytes(label, raw)
        return True
    except asyncio.CancelledError:
        raise
    except Exception:
        return False


async def recall_historical_images(
    service: Any,
    message: Message,
    members: list[Message],
    *,
    intent: Any,
    img_limit: int,
    digested_events: set[str],
    current_events: set[str],
    image_nearby: list[Message],
    aff_active: bool,
    caps: Any,
    recalled_members: list[Message],
    neighbour_members: list[Message],
    job_subject: str,
    message_guard: Callable[[], Awaitable[None]],
) -> tuple[list[Any], list[dict[str, Any]]]:
    candidates = service.hot.recent_with_images(
        message,
        exclude_events={*current_events, *(m.event_id for m in image_nearby)},
    )
    valid_candidates = []
    for item in candidates:
        if not await service.recalled([item]):
            valid_candidates.append(item)
    if not valid_candidates:
        return [], []
    mentioned_peers = {
        uid for m in members for uid in m.mentioned_ids if uid != service.s.onebot_self_id
    }
    tier_hint = getattr(intent, "tier", 2)

    def _recall_rank(idx_item: tuple[int, Message]) -> tuple[int, int, int]:
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

    ordered_candidates = [item for _, item in sorted(enumerate(valid_candidates), key=_recall_rank)]
    recalled_members.clear()
    recalled_images, recalled_sources = await service.message_images(
        message,
        [],
        ordered_candidates,
        limit=img_limit,
        include_current=False,
        nearby_origin="历史消息附图",
        selected_members=recalled_members,
    )
    if aff_active and caps.low_res_images_only and recalled_images:
        recalled_images = [dataclasses.replace(i, resolution="low") for i in recalled_images]
    await message_guard()
    if not recalled_images:
        return [], []
    neighbour_members.extend(recalled_members)
    await service.ai.screen(
        {"image_sources": recalled_sources, "historical_image_recall": True},
        job_subject,
        purpose="source",
        message=message,
        images=[i.gate() for i in recalled_images],
        before_call=message_guard,
    )
    return recalled_images, recalled_sources
