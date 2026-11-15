"""Bounded OneBot v11 Array/CQ parsing. Parsed content is always untrusted data."""

import html
import json
import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .bilibili import extract_bvid
from .models import ImageRef
from .security import Rejected, clean_text

BV = re.compile(r"(?<![A-Za-z0-9])BV[1-9A-HJ-NP-Za-km-z]{10}(?![A-Za-z0-9])")
URL = re.compile(r"https?://[^\s\"'<>\\，。！？）]+")
CQ = re.compile(r"\[CQ:([a-z_]+)((?:,[^\]]*)?)\]")


@dataclass
class RichMessage:
    text: str = ""
    command_text: str = ""
    addressed: bool = False
    mentioned_ids: list[str] = field(default_factory=list)
    reply_ids: list[str] = field(default_factory=list)
    images: list[ImageRef] = field(default_factory=list)
    video_ids: list[str] = field(default_factory=list)
    short_urls: list[str] = field(default_factory=list)
    forward_ids: list[str] = field(default_factory=list)


def cq_segments(raw: str) -> list[dict]:
    """Recognize real CQ tags before decoding entities, so escaped tags cannot @ the bot."""
    segments, pos = [], 0
    for match in CQ.finditer(raw):
        if match.start() > pos:
            segments.append(
                {"type": "text", "data": {"text": html.unescape(raw[pos : match.start()])}}
            )
        data = {}
        for pair in match[2].lstrip(",").split(","):
            key, sep, value = pair.partition("=")
            if sep:
                data[key] = html.unescape(value)
        segments.append({"type": match[1], "data": data})
        pos = match.end()
    if pos < len(raw):
        segments.append({"type": "text", "data": {"text": html.unescape(raw[pos:])}})
    return segments


def parse_message(message: list | str, bot_id: str, *, max_chars: int = 8000) -> RichMessage:
    result = RichMessage()
    pieces, direct = [], []
    remaining = 300

    def links(value: str):
        candidates = BV.findall(URL.sub(" ", value))
        for raw in URL.findall(html.unescape(value)):
            bvid = extract_bvid(raw.rstrip("。，）)"))
            if bvid:
                candidates.append(bvid)
        for bvid in candidates:
            if bvid not in result.video_ids and len(result.video_ids) < 3:
                result.video_ids.append(bvid)
        for raw in URL.findall(html.unescape(value)):
            parsed = urlsplit(raw.rstrip("。，）)"))
            if (
                parsed.hostname == "b23.tv"
                and not parsed.username
                and not parsed.password
                and parsed.port in {None, 443}
                and re.fullmatch(r"/[A-Za-z0-9]{1,32}", parsed.path)
            ):
                url = "https://b23.tv" + parsed.path
                if url not in result.short_urls and len(result.short_urls) < 3:
                    result.short_urls.append(url)

    def add(value: str):
        if not isinstance(value, str):
            raise Rejected("invalid_segment")
        pieces.append(value)
        links(value)
        if sum(map(len, pieces)) > max_chars:
            raise Rejected("message_too_large")

    def card(value, depth=0):
        nonlocal remaining
        remaining -= 1
        if depth > 8 or remaining < 0:
            raise Rejected("message_too_complex")
        if isinstance(value, dict):
            for key, child in value.items():
                if isinstance(child, str):
                    links(child)
                    # Preserve instructions hidden in card titles/descriptions for the safety gate.
                    if key.lower() in {"title", "desc", "description", "prompt", "content", "text"}:
                        add(child)
                elif isinstance(child, (dict, list)):
                    card(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                card(child, depth + 1)
        elif isinstance(value, str):
            links(value)

    def walk(raw, depth=0):
        nonlocal remaining
        if depth > 4:
            raise Rejected("message_too_complex")
        if isinstance(raw, str):
            if len(raw) > 65536:
                raise Rejected("message_too_large")
            raw = cq_segments(raw)
        if not isinstance(raw, list):
            raise Rejected("invalid_message")
        for segment in raw:
            remaining -= 1
            if remaining < 0:
                raise Rejected("message_too_complex")
            if not isinstance(segment, dict) or not isinstance(segment.get("data"), dict):
                raise Rejected("invalid_segment")
            kind, data = segment.get("type"), segment["data"]
            if kind == "text":
                text = data.get("text", "")
                add(text)
                if depth == 0:
                    direct.append(text)
            elif kind == "at":
                target = str(data.get("qq", ""))
                if depth == 0 and (target == "all" or re.fullmatch(r"[0-9]{1,20}", target)):
                    if target not in result.mentioned_ids and len(result.mentioned_ids) < 32:
                        result.mentioned_ids.append(target)
                if depth == 0 and str(data.get("qq")) == bot_id:
                    result.addressed = True
                add("[提及机器人]" if str(data.get("qq")) == bot_id else "[提及群友]")
            elif kind in {"image", "mface"}:
                url = data.get("url", "")
                file = data.get("file", "")
                # A native opaque file id is useful over WS; paths/URLs in this field are not.
                file = (
                    file
                    if isinstance(file, str)
                    and re.fullmatch(r"[A-Za-z0-9_.=+\-]{1,1024}", file)
                    and file not in {".", ".."}
                    else ""
                )
                url = url if isinstance(url, str) and url.startswith("https://") else ""
                size = data.get("file_size")
                size = (
                    int(size)
                    if not isinstance(size, bool) and re.fullmatch(r"[0-9]{1,15}", str(size))
                    else None
                )
                ref = ImageRef(url=url, file=file, file_size=size)
                if (
                    ref.identity
                    and len(result.images) < 2
                    and ref.identity not in [i.identity for i in result.images]
                ):
                    result.images.append(ref)
                add("[商城贴纸]" if kind == "mface" else "[图片]")
            elif kind == "face":
                face = str(data.get("id", ""))
                label = {
                    "0": "惊讶",
                    "1": "撇嘴",
                    "2": "色",
                    "3": "发呆",
                    "4": "得意",
                    "5": "流泪",
                    "6": "害羞",
                    "9": "大哭",
                    "14": "微笑",
                    "21": "可爱",
                    "24": "饥饿",
                    "28": "憨笑",
                    "30": "奋斗",
                    "32": "疑问",
                    "38": "敲打",
                    "66": "爱心",
                    "76": "赞",
                    "97": "擦汗",
                    "178": "笑哭",
                }.get(face)
                add(
                    "[表情：" + label + "]"
                    if label
                    else "[表情" + (face if face.isdigit() and len(face) <= 6 else "") + "]"
                )
            elif kind == "json":
                content = data.get("data", {})
                if isinstance(content, str):
                    if len(content) > 32768:
                        raise Rejected("card_too_large")
                    try:
                        content = json.loads(content)
                    except (ValueError, RecursionError):
                        raise Rejected("invalid_card") from None
                add("[分享卡片]")
                card(content)
            elif kind == "forward":
                ref = str(data.get("id", ""))
                if re.fullmatch(r"[A-Za-z0-9_.=+\-]{1,256}", ref) and len(result.forward_ids) < 2:
                    result.forward_ids.append(ref)
                add("[合并转发]")
            elif kind == "node":
                add("[转发内容]")
                walk(data.get("content", []), depth + 1)
            elif kind == "reply":
                ref = str(data.get("id", ""))
                if depth == 0 and re.fullmatch(r"-?[0-9]{1,30}", ref):
                    if ref not in result.reply_ids and len(result.reply_ids) < 2:
                        result.reply_ids.append(ref)
                add("[引用消息]")
            else:
                # Never execute unknown segments (music, record, video, xml, location, poke, etc.).
                add("[其他消息段]")

    walk(message)
    result.text = clean_text("\n".join(pieces))
    result.command_text = clean_text("".join(direct))
    return result


def parse_forward(data: dict, bot_id: str, *, include_authors: bool = False) -> RichMessage:
    nodes = data.get("messages", data.get("message", []))
    if not isinstance(nodes, list):
        raise Rejected("forward_too_large")
    if include_authors:
        result = RichMessage()
        lines = []
        for node in nodes[:15]:
            if not isinstance(node, dict):
                raise Rejected("invalid_forward")
            sender = node.get("sender") or {}
            if not isinstance(sender, dict):
                sender = {}
            uid = str(sender.get("user_id", node.get("user_id", "")))
            uid = uid if re.fullmatch(r"[1-9][0-9]{0,19}", uid) else "未知"
            name = clean_text(str(sender.get("nickname", node.get("nickname", "群友"))))[:80]
            name = re.sub(r"[\r\n\[\]]", " ", name)
            if uid == bot_id:
                name = "吉米奈喵"
            author = "我自己(吉米奈喵)" if uid == bot_id else f"{name}(QQ:{uid})"
            rich = parse_message(node.get("content", node.get("message", [])), bot_id)
            lines.append(f"[转发记录 - {author}]: {rich.text}")
            for ref in rich.images:
                if len(result.images) < 2 and ref.identity not in [
                    i.identity for i in result.images
                ]:
                    result.images.append(
                        ref.model_copy(
                            update={
                                "sender_id": uid if uid != "未知" else "",
                                "sender_nickname": name,
                                "origin": "转发记录（作者声明）",
                            }
                        )
                    )
            for dest, values in (
                (result.video_ids, rich.video_ids),
                (result.short_urls, rich.short_urls),
            ):
                for value in values:
                    if len(dest) < 3 and value not in dest:
                        dest.append(value)
            if sum(map(len, lines)) > 7500:
                break
        result.text = "\n".join(lines)[:7900]
        if len(nodes) > 15:
            result.text += f"\n[转发共{len(nodes)}条，仅展示前15条]"
        return result
    segments = []
    for node in nodes[:15]:
        if not isinstance(node, dict):
            raise Rejected("invalid_forward")
        segments.append(
            {"type": "node", "data": {"content": node.get("content", node.get("message", []))}}
        )
    # All content remains nested: no quoted @ or slash command becomes an instruction.
    return parse_message(segments, bot_id)
