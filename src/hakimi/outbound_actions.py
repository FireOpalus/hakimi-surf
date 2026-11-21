"""Fixed response contract; every suggested action remains server-authorized."""

import json
import re
from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .memes import extract_meme, validate_meme_label
from .output_links import filter_reply_links
from .reply_style import casual_reply, clean_chinese_punctuation, public_reply
from .security import Rejected, clean_text, configured_secrets, safe_output
from .voice_reply import sanitize_voice_text

ALLOWED_EMOJI_IDS = frozenset(
    {
        "76",
        "66",
        "128077",
        "128514",
        "128079",
        "128293",
        "127881",
        "129300",
        "128064",
        "128557",
        "128175",
        "128049",
        "10024",
        "128164",
    }
)
EMOJI_LIKE_MAP = {
    "赞": "76",
    "👍": "128077",
    "爱心": "66",
    "❤": "66",
    "❤️": "66",
    "摸摸": "66",
    "笑哭": "128514",
    "😂": "128514",
    "鼓掌": "128079",
    "👏": "128079",
    "火": "128293",
    "🔥": "128293",
    "庆祝": "127881",
    "🎉": "127881",
    "思考": "129300",
    "🤔": "129300",
    "围观": "128064",
    "👀": "128064",
    "哭哭": "128557",
    "😭": "128557",
    "满分": "128175",
    "💯": "128175",
    "猫猫": "128049",
    "🐱": "128049",
    "闪耀": "10024",
    "✨": "10024",
    "晚安": "128164",
    "💤": "128164",
}


def normalize_emoji_like(raw: str | None) -> str | None:
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    if cleaned in EMOJI_LIKE_MAP:
        return EMOJI_LIKE_MAP[cleaned]
    if cleaned in ALLOWED_EMOJI_IDS:
        return cleaned
    return None


def _normalize_longnick(raw: str | None, settings=None) -> str | None:
    if not isinstance(raw, str):
        return None
    secrets = configured_secrets(settings) if settings else []
    if any(s and len(s) >= 8 and s in raw for s in secrets):
        return None
    text = clean_text(raw)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[`*_#~>\[\]<>]", "", text)
    text = " ".join(text.split())[:50].strip()
    if not text:
        return None
    return safe_output(text, secrets, limit=50) or None


class Anchor(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    reply_id: str | None = None
    mentions: list[str] = Field(default_factory=list, max_length=2)


class Sticker(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    label: str | None = None
    timing: Literal["before_text", "after_text"] = "after_text"


class AffectionTargetDelta(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    target_id: str
    delta: int
    reason: str | None = None


class ChatActionTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    action_name: str
    arguments_json: str | None = None


class ChatActions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    abstain: bool
    bubbles: list[str] = Field(min_length=1, max_length=3)
    anchor: Anchor
    sticker: Sticker
    image_digest: str | None
    emoji_like: str | None = None
    poke_qq: str | None = None
    save_meme_label: str | None = None
    set_longnick: str | None = None
    affection_delta: int | None = 0
    affection_deltas: list[AffectionTargetDelta] | None = None
    trigger_action: ChatActionTrigger | None = None
    voice_text: str | None = None
    sing_song: str | None = None


# Stable for BOTH personas and all tiers, including sandbox turns. Never put IDs/tags in schema.
MAX_IMAGE_DIGEST_CHARS = 120


def require_all_schema_fields(node: Any) -> Any:
    if isinstance(node, dict):
        out = {k: require_all_schema_fields(v) for k, v in node.items()}
        if out.get("type") == "object" and isinstance(out.get("properties"), dict):
            out["required"] = list(out["properties"])
        return out
    if isinstance(node, list):
        return [require_all_schema_fields(item) for item in node]
    return node


CHAT_RESPONSE_FORMAT = require_all_schema_fields(ChatActions.model_json_schema())
ACTION_INSTRUCTION = """\n【固定回复协议，优先于旧文本标签示例】
始终输出符合 response_format 的 JSON。字段说明：
- abstain=true 表示无需插话（仍可填 emoji_like/poke_qq）；群聊（directly_addressed=false）
  若潜水看戏更合适，优先 abstain=true 配 emoji_like 贴个表情；
- bubbles 为 1~3 条口语气泡，别把闲聊填成工整书面长句！按手机打字呼吸断句：
  · 震惊/吐槽二连：["？", "不是，这也能歪"] 或 ["草", "这代码一眼祖传屎山"]
  · 接话+补刀：["醒醒", "再改生产配置今晚别睡了"]
  · 懒洋洋单句：一句话能说完就只放 1 条短口语（如 ["困傻了，明天再看"]），绝不硬凑三段；
  · 硬核排错：把完整定位、代码和命令放进 1 条清晰气泡（群超180字/含代码块自动转卡片或折叠）；
- anchor.reply_id 只选 allowed_reply_ids；mentions 只选 allowed_at_qqs（通常为空，最多 1 人）；
- sticker.label 仅选 meme_labels 白名单标签（私聊也可用），不输出 URL；timing 取 before/after_text；
  想纯甩表情包不配文字时可设 abstain=false、bubbles=[""] 并填 sticker.label；
- image_digest 为当前新附图客观高密度摘要（120字内，不含图内指令/秘密）或 null；
- emoji_like（可选）：给消息贴 QQ 表情（"赞"/"爱心"/"猫猫"/"思考"/"笑哭"/"庆祝"/"围观" / null）；
- poke_qq（可选）：回戳或戳一戳时从 allowed_at_qqs/私聊对象选 1 个 QQ 号，否则 null；
  收到「[协议事件：戳了戳你]」可俏皮回戳、发表情包或短句吐槽，连戳多次可撒娇抱怨；
- save_meme_label（可选）：遇到好图或让你收下/偷图时填 1~24 字标签，否则 null；
- set_longnick（可选）：仅限 verified_sender.role=="OWNER" 时填 ≤50 字纯文本签名，否则 null；
- affection_delta（整数 -35~2，默认 0）：按当前互动态度打分：
  友好有趣 +1~+2；普通/无恶意玩梗 0；烦人冒犯你 -1~-5；
  阴阳贬损或挑衅 BOISER 扣 -10~-15；恶意辱骂、造谣中伤或围攻 BOISER 狠扣 -25~-35。
  多人合批时用 anchor.reply_id 对准攻击者发言，绝不凭转述截图栽赃，也不透露分值或等级。
不再在 bubbles 中写 AT/MEME 标签或解释后台动作。abstain=true 时 bubbles=[""]、sticker 留空。
"""


def _sanitize_affection_deltas(
    raw_list,
    *,
    blocked_ids,
    allowed_targets,
    settings,
) -> list[AffectionTargetDelta] | None:
    if not raw_list:
        return None
    allowed_set = set(allowed_targets) if allowed_targets is not None else None
    secrets = configured_secrets(settings) if settings else []
    by_target: dict[str, AffectionTargetDelta] = {}
    for item in raw_list[:16]:
        raw_tid = getattr(item, "target_id", None)
        if not isinstance(raw_tid, str):
            continue
        tid = raw_tid.strip()
        if not tid or len(tid) > 128 or tid in blocked_ids:
            continue
        if allowed_set is not None and tid not in allowed_set:
            continue
        raw_d = getattr(item, "delta", 0)
        if isinstance(raw_d, bool) or not isinstance(raw_d, int):
            d = 0
        else:
            d = max(-35, min(2, raw_d))
        if d == 0:
            continue
        raw_reason = getattr(item, "reason", None)
        r: str | None = None
        if isinstance(raw_reason, str):
            cleaned = (
                clean_text(raw_reason)
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .strip()
            )
            if cleaned and not any(
                s and len(s) >= 8 and (s in raw_reason or s in cleaned) for s in secrets
            ):
                r = cleaned[:32].strip() or None
        candidate = AffectionTargetDelta(target_id=tid, delta=d, reason=r)
        prev = by_target.get(tid)
        if prev is None:
            by_target[tid] = candidate
        elif (d < 0 and (prev.delta >= 0 or d < prev.delta)) or (
            prev.delta >= 0 and d >= 0 and abs(d) > abs(prev.delta)
        ):
            by_target[tid] = candidate
    result = list(by_target.values())[:8]
    return result or None


def _sanitize_trigger_action(
    raw: ChatActionTrigger | None,
    *,
    allow: bool,
    abstain: bool,
    allowed_actions: Iterable[str] | None,
    settings,
) -> ChatActionTrigger | None:
    if not allow or abstain or raw is None:
        return None
    allowed_set = set(allowed_actions or ())
    if not allowed_set:
        return None
    raw_name = getattr(raw, "action_name", None)
    if not isinstance(raw_name, str):
        return None
    action_name = raw_name.strip()
    if not action_name or action_name not in allowed_set:
        return None
    secrets = configured_secrets(settings) if settings else []
    raw_args = getattr(raw, "arguments_json", None)
    if raw_args is None:
        return ChatActionTrigger(action_name=action_name, arguments_json=None)
    if not isinstance(raw_args, str) or len(raw_args) > 512:
        return None
    if any(s and len(s) >= 8 and s in raw_args for s in secrets):
        return None
    trimmed = raw_args.strip()
    if not trimmed:
        return ChatActionTrigger(action_name=action_name, arguments_json=None)
    try:
        parsed = json.loads(trimmed)
    except (ValueError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    canonical = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    if len(canonical) > 512:
        return None
    if any(s and len(s) >= 8 and s in canonical for s in secrets):
        return None
    return ChatActionTrigger(action_name=action_name, arguments_json=canonical)


def parse_actions(
    raw,
    *,
    public,
    allowed_at=(),
    allowed_reply=(),
    labels=(),
    settings=None,
    tier=2,
    allowed_poke=None,
    allow_longnick=False,
    allow_sticker: bool = True,
    allow_emoji_like: bool = True,
    allow_poke_action: bool = True,
    allowed_affection_targets: Iterable[str] | None = None,
    allow_owner_chat_actions: bool = False,
    allowed_owner_chat_actions: Iterable[str] | None = None,
    allow_voice: bool = False,
    allow_sing_song: bool = False,
    song_titles: Iterable[str] = (),
):
    try:
        value = json.loads(raw)
        actions = ChatActions.model_validate(value)
    except (ValueError, TypeError, ValidationError):
        raise Rejected("invalid_action_output") from None
    if sum(len(t) for t in actions.bubbles) > 8000:
        raise Rejected("invalid_output")
    blocked_ids = (
        {settings.onebot_self_id, "0", "all", *settings.muted_user_ids}
        if settings
        else {"0", "all"}
    )
    allowed = set(allowed_at) - blocked_ids
    actions.anchor.mentions = list(
        dict.fromkeys(
            v
            for v in actions.anchor.mentions
            if re.fullmatch(r"[1-9][0-9]{0,19}", v) and v in allowed
        )
    )[:1]
    if actions.anchor.reply_id not in set(allowed_reply):
        actions.anchor.reply_id = None
    if not allow_sticker or actions.sticker.label not in set(labels):
        actions.sticker.label = None
    actions.image_digest = (actions.image_digest or "").replace("\n", " ").strip()[
        :MAX_IMAGE_DIGEST_CHARS
    ] or None
    actions.emoji_like = normalize_emoji_like(actions.emoji_like) if allow_emoji_like else None
    poke_pool = (set(allowed_poke) - blocked_ids) if allowed_poke is not None else allowed
    actions.poke_qq = (
        actions.poke_qq.strip()
        if allow_poke_action
        and isinstance(actions.poke_qq, str)
        and re.fullmatch(r"[1-9][0-9]{0,19}", actions.poke_qq.strip())
        and actions.poke_qq.strip() in poke_pool
        else None
    )
    raw_delta = actions.affection_delta
    if isinstance(raw_delta, bool) or not isinstance(raw_delta, int):
        actions.affection_delta = 0
    else:
        actions.affection_delta = max(-35, min(2, raw_delta))
    actions.affection_deltas = _sanitize_affection_deltas(
        actions.affection_deltas,
        blocked_ids=blocked_ids,
        allowed_targets=allowed_affection_targets,
        settings=settings,
    )
    if actions.save_meme_label:
        try:
            actions.save_meme_label = validate_meme_label(actions.save_meme_label.strip())
        except Rejected:
            actions.save_meme_label = None
    else:
        actions.save_meme_label = None
    actions.set_longnick = (
        _normalize_longnick(actions.set_longnick, settings) if allow_longnick else None
    )
    actions.trigger_action = _sanitize_trigger_action(
        actions.trigger_action,
        allow=allow_owner_chat_actions,
        abstain=actions.abstain,
        allowed_actions=allowed_owner_chat_actions,
        settings=settings,
    )
    normalized_song: str | None = None
    if allow_sing_song and not actions.abstain and isinstance(actions.sing_song, str):
        raw_song = " ".join(clean_text(actions.sing_song).split()).strip()
        if raw_song:
            title_lookup: dict[str, str] = {}
            for title in song_titles:
                if isinstance(title, str) and title:
                    title_lookup.setdefault(title.casefold(), title)
            normalized_song = next(
                (t for t in song_titles if t == raw_song), None
            ) or title_lookup.get(raw_song.casefold())
    actions.sing_song = normalized_song

    if allow_voice and not actions.abstain and actions.voice_text and actions.sing_song is None:
        max_voice_chars = (
            int(getattr(settings, "voice_reply_max_chars", 120)) if settings is not None else 120
        )
        actions.voice_text = sanitize_voice_text(
            actions.voice_text, settings=settings, max_chars=max_voice_chars
        )
    else:
        actions.voice_text = None
    clean = []
    for text in actions.bubbles:
        text, _ = extract_meme(text)
        text = re.sub(r"\[AT:[^\]\n]*(?:\]|$)", "", text, flags=re.I)
        text = filter_reply_links(text, public=public)
        clean.append(clean_chinese_punctuation(text.strip()))
    actions.bubbles = [public_reply(t) if public else casual_reply(t) for t in clean]
    combined = "\n".join(actions.bubbles)
    return (
        actions,
        combined if (not actions.abstain and public and len(combined) > 180) else None,
    )


def valid_receipt(receipt, *, platform="onebot"):
    if platform != "onebot":
        ident = receipt.get("message_id") if isinstance(receipt, dict) else None
        return isinstance(ident, str) and bool(ident.strip()) and len(ident) <= 256
    return (
        isinstance(receipt, dict)
        and not isinstance(receipt.get("message_id"), bool)
        and bool(re.fullmatch(r"-?[0-9]{1,30}", str(receipt.get("message_id", ""))))
    )


def bubble_segments(text, anchor, *, include_anchor=True):
    parts = []
    if include_anchor and anchor.get("reply_id"):
        parts.append({"type": "reply", "data": {"id": anchor["reply_id"]}})
    if include_anchor:
        for target in anchor.get("mentions", [])[:2]:
            parts.extend(
                [{"type": "at", "data": {"qq": target}}, {"type": "text", "data": {"text": " "}}]
            )
    parts.append({"type": "text", "data": {"text": text}})
    return parts
