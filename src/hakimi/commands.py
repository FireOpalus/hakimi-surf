"""Registry-driven chat command router and built-in command handlers.

Enforces protocol-layer Owner identity checks (verified OneBot OWNER_QQ_ID or bound
social owner via `is_persona_owner()`) before executing any privileged command or
reaching the model pipeline.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from .affection import _validate_platform_user
from .bot_server import is_persona_owner
from .memes import MAX_UPLOAD_BYTES, validate_meme_label
from .models import Message
from .security import Rejected
from .songs import DEFAULT_COVER_ENGINE, normalize_cover_engine, normalize_take_key

PermissionMode = Literal["public", "group_owner", "owner", "owner_private"]
CommandHandler = Callable[[Any, Message, str], Awaitable[str | None]]
DelegateResolver = Callable[[Any], Any]

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
/学歌 <歌名> [engine=dual|yingmusic_svc|seed_vc|rvc] [ref=干声] [pitch=-24..24]（或 /learn-song）：
  提交 Colab 学歌任务（可附带音频）
/换歌版本 <歌名> [版本名]（或 /switch-song、/切歌版本）：查看候选版本与点评，或切换默认版本
/哈基米 订阅 <UP主UID> / /哈基米 退订 <UP主UID>：UP 更新私聊提醒（直播开播监控可在 /admin 添加）
/哈基米 订阅列表 / /哈基米 报告 / /哈基米 状态：查看订阅、近 7 天巡逻报告与今日额度
链接与小号巡逻优先阅读公开字幕与热评资料；原片分析受后台开关、时长、体积和每日次数限制。"""


def _enforce_permission(service: Any, message: Message, permission: PermissionMode) -> None:
    if permission == "public":
        return
    if permission == "group_owner":
        if message.kind == "group" and not is_persona_owner(message, service.s):
            raise Rejected("group_owner_only")
        return
    if permission == "owner":
        if not is_persona_owner(message, service.s):
            raise Rejected("group_owner_only" if message.kind == "group" else "owner_private_only")
        return
    if permission == "owner_private":
        if message.kind != "private" or not is_persona_owner(message, service.s):
            raise Rejected("owner_private_only")
        return
    raise ValueError(f"Unknown command permission mode: {permission}")


def _normalize_slash_name(name: str) -> str:
    cleaned = str(name or "").strip()
    if not cleaned:
        raise ValueError("Command name must be non-empty")
    return cleaned if cleaned.startswith("/") else "/" + cleaned


@dataclass(frozen=True)
class CommandEntry:
    name: str
    handler: CommandHandler
    aliases: tuple[str, ...] = ()
    prefix_aliases: tuple[str, ...] = ()
    prefix_extra_prefixes: tuple[str, ...] = ()
    prefix_requires_space: bool = False
    exact: bool = False
    dotall: bool = False
    strip_input: bool = True
    permission: PermissionMode = "public"
    description: str = ""


@dataclass(frozen=True)
class PrefixCommandEntry:
    name: str
    handler: CommandHandler
    aliases: tuple[str, ...] = ()
    match_prefix: bool = False
    permission: PermissionMode = "public"
    description: str = ""


def _match_slash_entry(entry: CommandEntry, direct: str, prefix: str) -> tuple[bool, str]:
    candidate = direct.strip() if entry.strip_input else direct
    names = (entry.name, *entry.aliases)
    if entry.exact:
        return (candidate in names, "")
    alternatives = [re.escape(item) for item in names]
    if entry.prefix_aliases:
        prefixes = [prefix]
        for extra in entry.prefix_extra_prefixes:
            if extra not in prefixes:
                prefixes.append(extra)
        prefix_part = (
            re.escape(prefixes[0])
            if len(prefixes) == 1
            else "(?:" + "|".join(re.escape(p) for p in prefixes) + ")"
        )
        sep = r"\s+" if entry.prefix_requires_space else r"\s*"
        sub_part = "(?:" + "|".join(re.escape(a) for a in entry.prefix_aliases) + ")"
        alternatives.append(f"{prefix_part}{sep}{sub_part}")
    pattern = r"^(?:" + "|".join(alternatives) + r")(?:\s+(.*))?$"
    flags = re.DOTALL if entry.dotall else 0
    match = re.match(pattern, candidate, flags)
    if not match:
        return False, ""
    return True, match.group(1) or ""


class CommandRegistry:
    """Extensible registry for deterministic slash and prefix chat commands."""

    def __init__(self) -> None:
        self._delegates: list[DelegateResolver] = []
        self._commands: dict[str, CommandEntry] = {}
        self._prefix_commands: dict[str, PrefixCommandEntry] = {}

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._commands)

    @property
    def prefix_names(self) -> tuple[str, ...]:
        return tuple(self._prefix_commands)

    def register_delegate(self, resolver: DelegateResolver) -> None:
        if not callable(resolver):
            raise TypeError("delegate resolver must be callable")
        self._delegates.append(resolver)

    def register(
        self,
        name: str,
        handler: CommandHandler,
        *,
        aliases: tuple[str, ...] | list[str] = (),
        prefix_aliases: tuple[str, ...] | list[str] = (),
        prefix_extra_prefixes: tuple[str, ...] | list[str] = (),
        prefix_requires_space: bool = False,
        exact: bool = False,
        dotall: bool = False,
        strip_input: bool = True,
        permission: PermissionMode = "public",
        description: str = "",
    ) -> CommandEntry:
        if not callable(handler):
            raise TypeError("command handler must be callable")
        canonical = _normalize_slash_name(name)
        normalized_aliases = tuple(_normalize_slash_name(a) for a in aliases)
        entry = CommandEntry(
            name=canonical,
            handler=handler,
            aliases=normalized_aliases,
            prefix_aliases=tuple(str(a).strip() for a in prefix_aliases if str(a).strip()),
            prefix_extra_prefixes=tuple(
                str(p).strip() for p in prefix_extra_prefixes if str(p).strip()
            ),
            prefix_requires_space=prefix_requires_space,
            exact=exact,
            dotall=dotall,
            strip_input=strip_input,
            permission=permission,
            description=description,
        )
        self._commands[canonical] = entry
        return entry

    def unregister(self, name: str) -> None:
        canonical = _normalize_slash_name(name)
        self._commands.pop(canonical, None)

    def register_prefix(
        self,
        name: str,
        handler: CommandHandler,
        *,
        aliases: tuple[str, ...] | list[str] = (),
        match_prefix: bool = False,
        permission: PermissionMode = "public",
        description: str = "",
    ) -> PrefixCommandEntry:
        if not callable(handler):
            raise TypeError("prefix command handler must be callable")
        key = str(name)
        entry = PrefixCommandEntry(
            name=key,
            handler=handler,
            aliases=tuple(str(a) for a in aliases),
            match_prefix=match_prefix,
            permission=permission,
            description=description,
        )
        self._prefix_commands[key] = entry
        return entry

    async def dispatch(self, service: Any, message: Message) -> str | None:
        if service.bot_guard.known_bot(message):
            return None
        if any(a.origin in {"", "当前消息"} for a in getattr(message, "audios", ())):
            return None
        prefix = service.s.command_prefix
        direct = message.command_text if message.command_text is not None else message.text
        for resolver in self._delegates:
            delegate = resolver(service)
            if delegate is None:
                continue
            result = await delegate.command(message)
            if result is not None:
                return result
        for entry in self._commands.values():
            matched, raw_args = _match_slash_entry(entry, direct, prefix)
            if matched:
                _enforce_permission(service, message, entry.permission)
                return await entry.handler(service, message, raw_args)
        if not direct.startswith(prefix):
            return None
        command = direct[len(prefix) :].strip()
        for entry in self._prefix_commands.values():
            if not entry.match_prefix and command in (entry.name, *entry.aliases):
                _enforce_permission(service, message, entry.permission)
                return await entry.handler(service, message, command)
        for entry in self._prefix_commands.values():
            if entry.match_prefix and any(
                command.startswith(token) for token in (entry.name, *entry.aliases) if token
            ):
                _enforce_permission(service, message, entry.permission)
                return await entry.handler(service, message, command)
        return None


async def handle_bot_interact_command(service: Any, message: Message, _raw_args: str) -> str:
    return await service.bot_guard.grant(message)


async def handle_new_session_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "group_owner")
    await service.sessions.clear(message)
    return "已清空当前会话上下文，记忆开关保持原设置。"


async def handle_help_command(_service: Any, _message: Message, _raw_args: str) -> str:
    return HELP


async def handle_memory_clear_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "group_owner")
    await service.sessions.clear(message, enabled=False)
    return "已关闭并删除当前会话中的记忆。"


async def handle_memory_enable_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "group_owner")
    await service.sessions.enable(message)
    return "已启用当前会话的短期记忆；本群成员共用本群上下文，其他群和私聊均独立。"


async def handle_subscriptions_list_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    subject = service.store.subject(message)
    subscriptions = await service.store.subscriptions(subject)
    return "已订阅 UP：" + ("、".join(s["mid"] for s in subscriptions) or "暂无")


async def handle_reports_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    subject = service.store.subject(message)
    reports = await service.store.latest_reports(subject)
    return "\n\n".join(reports) if reports else "最近七天暂无巡逻摘要。"


async def handle_status_command(service: Any, message: Message, _raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    usage = await service.store.usage()
    amount = sum(x["amount"] for x in usage["lanes"]) / 1_000_000
    return (
        f"{service.s.bot_timezone} {usage['day_local']}：已计入费用（含预留）${amount:.4f}；"
        f"每日全局上限 ${service.s.daily_usd}。"
    )


async def handle_subscription_command(service: Any, message: Message, command: str) -> str:
    _enforce_permission(service, message, "owner_private")
    subject = service.store.subject(message)
    match = re.fullmatch(r"(订阅|退订) ([1-9][0-9]{0,19})", command)
    if not match:
        return "格式：/哈基米 订阅 <数字UID> 或 /哈基米 退订 <数字UID>"
    if match[1] == "退订":
        await service.store.remove_subscription(subject, match[2])
        return "已取消该 UP 主的巡逻订阅。"
    if service.modules.inactive_reason("bilibili_surf") or not service.s.bilibili_enabled:
        return "B 站读取功能尚未开启。"
    await service.store.add_subscription(message, match[2])
    tip = (
        "发现更新后会私聊汇报。"
        if service.proactive.matches(message) and service.proactive.delivery_enabled
        else "新摘要会保存在 /哈基米 报告 中。"
    )
    return "已订阅；首次巡逻仅建立基线。" + tip


async def handle_bind_command(service: Any, message: Message, raw_args: str) -> str:
    identity = getattr(service, "identity", None)
    if identity is None:
        return "跨平台账号绑定服务暂未就绪，请稍后再试。"
    arg = raw_args.strip()
    try:
        if not arg:
            code = await identity.create_bind_code(message)
            return (
                f"已生成一次性跨平台绑定码：{code}\n"
                f"请在 5 分钟内使用另一个平台的账号私聊发送：/bind {code}\n"
                "绑定完成后好感度与权限将自动合并同步（绑定码仅限单次使用）。"
            )
        await identity.redeem_bind_code(
            message,
            arg,
            affection=service.store.runtime.affection,
            moderation=service.moderation,
        )
        service.record("identity_bound")
        return "跨平台账号绑定成功！已合并同步好感度与账号状态~"
    except Rejected as exc:
        code = str(exc).strip()
        messages = {
            "bind_private_only": "为了安全，跨平台账号绑定仅支持在私聊中使用哦~",
            "bind_bot_forbidden": "Bot 账号不支持使用 /bind 绑定。",
            "protected_identity": "主人账号由系统配置受保护绑定，不支持通过 /bind 合并或修改。",
            "bind_muted_forbidden": "当前账号状态受限，暂无法使用 /bind 绑定。",
            "bind_rate_limited": "生成绑定码太频繁啦，请过几分钟再试~",
            "bind_code_generation_failed": "暂时未能生成绑定码，请稍后再试~",
            "bind_cooldown": "绑定码错误次数过多，已触发冷却保护，请 15 分钟后再试~",
            "invalid_bind_code": "绑定码无效或已过期，请核对后再试，或发送 /bind 重新生成。",
            "bind_same_account": "不能绑定当前同一个账号哦，请用另一个平台或账号私聊发送绑定码。",
            "bind_already_linked": "这两个账号已经绑定在同一个身份档案下啦，无需重复绑定~",
            "person_not_found": "未找到对应的身份档案，请重新发送 /bind 获取新绑定码。",
        }
        return messages.get(code, "绑定操作未完成，请稍后重试或联系管理员处理。")


async def handle_affection_command(service: Any, message: Message, raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    aff = service.store.runtime.affection
    args = raw_args.strip().split()
    if not args:
        st = await aff.status()
        owner_prof = await aff.profile(message.platform, message.user_id, message.sender_nickname)
        users = st["users"]
        blacklisted = [u for u in users if u["score"] == 0 and not u["is_owner"]]
        bl_text = "、".join(f"{u['nickname']}({u['user_id']})" for u in blacklisted[:10]) or "暂无"
        cap = st["config"]["daily_positive_cap"]
        init_score = st["config"]["initial_score"]
        return (
            f"好感度系统：{'已启用' if st['config']['enabled'] else '已停用'}"
            f"（初始 {init_score}，日正增长上限 +{cap}）。\n"
            f"BOISER 当前羁绊：{owner_prof['score']} 分"
            f"（{owner_prof['lv']} {owner_prof['tier_label']}）。\n"
            f"已建档用户：{len(users)} 人；拉黑(0分)：{len(blacklisted)} 人（{bl_text}）。\n"
            "用法：/affection <账号或平台:账号> 查看档案，"
            "或 /affection <账号或平台:账号> <0..120> 调整分数。"
        )
    if len(args) > 2:
        return "用法：/affection [平台:账号 或 账号] [0..120]"
    raw_target = args[0].strip()
    if ":" in raw_target:
        target_platform, target_uid = raw_target.split(":", 1)
    else:
        target_platform, target_uid = message.platform, raw_target
    try:
        target_uid = _validate_platform_user(target_platform, target_uid)
    except Rejected:
        return "用法：/affection [平台:账号 或 账号] [0..120]，目标账号格式不正确。"
    if len(args) == 1:
        prof = await aff.profile(target_platform, target_uid)
        safety_tag = ""
        if prof.get("is_owner") or prof.get("safety_status") == "owner_immune":
            safety_tag = "｜风控免疫"
        else:
            viol_30d = int(prof.get("violations_30d", 0) or 0)
            if viol_30d > 0:
                safety_tag = f"｜30天安全拦截 {viol_30d}次({prof.get('safety_status') or 'watch'})"
        reply = (
            f"{prof['nickname']}（{prof['platform']}:{prof['user_id']}）：{prof['score']} 分｜"
            f"{prof['lv']} {prof['tier_label']}｜"
            f"今日正向 +{prof['today_positive_gain']}/{prof['daily_positive_cap']}"
            f"{'｜已拉黑静音' if prof['muted'] else ''}"
            f"{safety_tag}"
        )
        if hasattr(aff, "user_history"):
            recent = await aff.user_history(target_platform, target_uid, limit=3)
            if recent:
                reply += "\n最近变动：" + "、".join(
                    f"{int(h['delta']):+d}({h['reason']})" for h in recent
                )
        return reply
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
            moderation=service.moderation,
        )
    except Rejected as exc:
        return f"设置失败：{exc}"
    return (
        f"已将 {updated['nickname']}（{updated['user_id']}）好感度设为 {updated['score']} 分"
        f"（{updated['lv']} {updated['tier_label']}）"
        + ("，并已同步应用层静音。" if updated["muted"] else "。")
    )


async def handle_save_meme_command(service: Any, message: Message, raw_label: str) -> str:
    _enforce_permission(service, message, "owner")
    raw_label = raw_label.strip()
    if not raw_label:
        return "用法：引用带图消息（或附图同发）并发送 /存表情 <1~24字标签>"
    try:
        label = validate_meme_label(raw_label)
    except Rejected:
        return "标签格式不对，请用 1～24 个汉字、字母、数字、下划线或短横线。"
    if (
        message.platform == "onebot"
        and not message.images
        and message.forward_ids
        and not message.context_expanded
    ):
        try:
            message = await service.expand_forwards(message)
        except Rejected:
            return "转发内容展开失败，无法获取其中的图片。"
    if not message.images:
        return "未找到图片，请引用包含表情包的消息或随指令附带图片。"
    if not hasattr(service.images, "fetch_raw"):
        return "当前图片加载器不支持原始下载，请在管理后台上传。"
    try:
        raw = await service.images.fetch_raw(message.images[0], max_bytes=MAX_UPLOAD_BYTES)
        saved = await service.memes.upload_bytes(label, raw)
    except Rejected as exc:
        code = str(exc)
        messages = {
            "image_too_large": "图片太大啦，单张表情不能超过 5 MiB。",
            "meme_upload_too_large": "图片太大啦，单张表情不能超过 5 MiB。",
            "meme_label_exists": f"标签「{label}」里已经有这张图啦，换个标签或直接用就行~",
            "meme_library_full": ("表情包库已经满啦（最多 100 张 / 50 MiB），去后台删几张再来吧。"),
            "invalid_meme_label": "标签格式不对，请用 1～24 个汉字、字母、数字、下划线或短横线。",
        }
        if code in messages:
            return messages[code]
        if code.startswith(("meme_image_", "invalid_meme_")):
            return "这张图格式或尺寸不支持呢（支持 jpg / png / gif / webp）。"
        return f"保存表情失败（{code}）。"
    extra = "（动画已取第一帧）" if saved.get("first_frame_only") else ""
    return f"表情已存好啦：标签「{saved['label']}」，随时可以用~{extra}"


_ENGINE_CMD_LABELS = {
    "dual": "双引擎猫猫亲挑",
    "yingmusic_svc": "YingMusic-SVC",
    "seed_vc": "Seed-VC",
    "rvc": "RVC",
}
_SWITCH_KNOWN_TAKES = frozenset(
    {
        "ying",
        "yingmusic",
        "yingmusic-svc",
        "yingmusic_svc",
        "seed",
        "seedvc",
        "seed-vc",
        "seed_vc",
        "rvc",
    }
)


async def handle_learn_song_command(service: Any, message: Message, raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    stripped = raw_args.strip()
    if not stripped:
        return (
            "用法：/学歌 <歌名> [engine=dual|yingmusic_svc|seed_vc|rvc] [ref=干声文件名] "
            "[pitch=-24..24] [artist=原唱] [tags=标签] [inbox=文件名] [gpu=L4|T4|A100|H100] "
            "[--no-search]；可附带音频文件（.mp3/.wav/.flac/.m4a/.ogg/.opus）。"
        )
    try:
        tokens = shlex.split(stripped)
    except ValueError:
        tokens = stripped.split()

    title_parts: list[str] = []
    artist = ""
    tags = ""
    inbox_filename = ""
    engine = DEFAULT_COVER_ENGINE
    ref_voice = ""
    model_name = "hakimi"
    gpu: str | None = None
    pitch = 0
    auto_search = True
    idx = 0
    while idx < len(tokens):
        tok = tokens[idx]
        low = tok.lower()
        if low in {"--no-search", "--nosearch"}:
            auto_search = False
        elif low.startswith("auto_search="):
            val = tok.split("=", 1)[1].strip().lower()
            if val in {"0", "false", "no", "off"}:
                auto_search = False
            elif val in {"1", "true", "yes", "on"}:
                auto_search = True
            else:
                return "auto_search 参数需为 true 或 false。"
        elif low in {"--engine", "-e"}:
            if idx + 1 >= len(tokens):
                return "用法：--engine <dual|yingmusic_svc|seed_vc|rvc>"
            idx += 1
            try:
                engine = normalize_cover_engine(tokens[idx], allow_aliases=True)
            except Rejected:
                return "engine 仅支持 dual（默认双引擎）、yingmusic_svc、seed_vc 或 rvc。"
        elif low.startswith(("engine=", "--engine=", "引擎=")):
            raw_eng = tok.split("=", 1)[1].strip()
            try:
                engine = normalize_cover_engine(raw_eng, allow_aliases=True)
            except Rejected:
                return "engine 仅支持 dual（默认双引擎）、yingmusic_svc、seed_vc 或 rvc。"
        elif low in {"--ref", "--ref-voice"}:
            if idx + 1 >= len(tokens):
                return "用法：--ref <云盘models目录参考干声文件名>"
            idx += 1
            ref_voice = tokens[idx].strip()
        elif low.startswith(("ref=", "--ref=", "ref_voice=", "--ref-voice=")):
            ref_voice = tok.split("=", 1)[1].strip()
        elif low in {"--model", "-m"}:
            if idx + 1 >= len(tokens):
                return "用法：--model <RVC模型名>"
            idx += 1
            model_name = tokens[idx].strip()
        elif low.startswith(("model=", "--model=", "model_name=")):
            model_name = tok.split("=", 1)[1].strip()
        elif low in {"--gpu", "-g"}:
            if idx + 1 >= len(tokens):
                return "用法：--gpu <L4|T4|A100|H100>"
            idx += 1
            gpu = tokens[idx].strip()
        elif low.startswith(("gpu=", "--gpu=")):
            gpu = tok.split("=", 1)[1].strip()
        elif low in {"--pitch", "-p"}:
            if idx + 1 >= len(tokens):
                return "用法：--pitch <整数 -24..24>"
            idx += 1
            raw_pitch = tokens[idx].strip()
            if not re.fullmatch(r"[+-]?[0-9]{1,3}", raw_pitch):
                return "pitch 必须是整数（范围 -24 到 24）。"
            pitch = int(raw_pitch)
        elif low.startswith(("pitch=", "--pitch=", "pitch:", "变调=")):
            sep = ":" if low.startswith("pitch:") else "="
            raw_pitch = tok.split(sep, 1)[1].strip()
            if not re.fullmatch(r"[+-]?[0-9]{1,3}", raw_pitch):
                return "pitch 必须是整数（范围 -24 到 24）。"
            pitch = int(raw_pitch)
        elif low in {"--artist", "-a"}:
            if idx + 1 >= len(tokens):
                return "用法：--artist <原唱歌手>"
            idx += 1
            artist = tokens[idx].strip()
        elif low.startswith(("artist=", "--artist=")):
            artist = tok.split("=", 1)[1].strip()
        elif low in {"--tags", "-t"}:
            if idx + 1 >= len(tokens):
                return "用法：--tags <标签>"
            idx += 1
            tags = tokens[idx].strip()
        elif low.startswith(("tags=", "--tags=")):
            tags = tok.split("=", 1)[1].strip()
        elif low in {"--inbox", "-i"}:
            if idx + 1 >= len(tokens):
                return "用法：--inbox <云盘inbox文件名>"
            idx += 1
            inbox_filename = tokens[idx].strip()
        elif low.startswith(("inbox=", "--inbox=")):
            inbox_filename = tok.split("=", 1)[1].strip()
        else:
            title_parts.append(tok)
        idx += 1

    if not -24 <= pitch <= 24:
        return "pitch 范围需在 -24 到 24 之间。"
    raw_title = " ".join(title_parts).strip().strip("《》\"'").strip()
    if not raw_title:
        return "请提供歌名，例如：/学歌 晴天 pitch=-2"

    if (
        message.platform == "onebot"
        and not message.files
        and message.forward_ids
        and not message.context_expanded
    ):
        try:
            message = await service.expand_forwards(message)
        except Rejected:
            return "转发内容展开失败，无法读取音频附件。"

    audio_bytes: bytes | None = None
    audio_filename = ""
    if message.files:
        try:
            audio_bytes, audio_filename = await service.files.read_audio_attachment(
                message.files[0], message
            )
        except Rejected as exc:
            code = str(exc)
            if code == "invalid_song_audio":
                return "音频附件格式不支持，仅支持 .mp3 / .wav / .flac / .m4a / .ogg / .opus。"
            if code == "oversized":
                return "音频附件太大啦（最大支持 100 MiB），请剪辑或压缩后再发。"
            if code == "unavailable":
                return "音频附件下载失败，请重新发送或上传到云盘 Hakimi_Songs/inbox/。"
            return f"读取音频附件失败（{code}）。"

    songs = getattr(service, "songs", None)
    if songs is None or not hasattr(songs, "request_learn_song"):
        return "歌单工坊模块尚未就绪，请稍后再试。"
    req_kwargs: dict[str, Any] = {
        "artist": artist,
        "tags": tags,
        "pitch": pitch,
        "engine": engine,
        "audio_bytes": audio_bytes,
        "audio_filename": audio_filename,
        "inbox_filename": inbox_filename,
        "auto_search": auto_search,
        "requester": "owner",
    }
    if ref_voice:
        req_kwargs["ref_voice"] = ref_voice
    if model_name and model_name != "hakimi":
        req_kwargs["model_name"] = model_name
    if gpu:
        req_kwargs["gpu"] = gpu
    try:
        job = await songs.request_learn_song(raw_title, **req_kwargs)
    except Rejected as exc:
        code = str(exc)
        err_map = {
            "colab_unconfigured": "Colab 云端算力未配置，请先在后台或环境变量配置 Worker。",
            "colab_disabled": "Colab 云端算力当前已停用，请先在后台启用。",
            "colab_worker_unavailable": "Colab Worker 暂时离线或不可达，请检查后台状态。",
            "colab_daily_job_limit": "今日 Colab 学歌作业次数已达上限啦。",
            "colab_daily_limit": "今日 Colab 学歌作业次数已达上限啦。",
            "colab_daily_cu_budget": "今日 Colab CU 预算已用完啦。",
            "colab_budget_exceeded": "今日 Colab CU 预算已用完啦。",
            "colab_job_limit_reached": "Colab 任务队列已满，请稍等当前任务跑完再试。",
            "colab_drive_not_configured": "Drive 歌单目录未配置，暂时无法上传/检索云盘。",
            "colab_drive_invalid_file": "指定的云盘 inbox 文件无效，请核对文件名。",
            "song_drive_not_configured": "歌单云盘 Drive 凭证未配置，暂时无法上传/检索云盘。",
            "song_drive_credentials_invalid": (
                "歌单云盘 Drive 凭证无效或权限不安全，请检查后台配置。"
            ),
            "song_drive_insufficient_scope": (
                "歌单云盘 Drive 凭证缺少 drive.file 权限，请重新授权。"
            ),
            "song_drive_reauth_required": "歌单云盘 Drive 授权已过期或被撤销，请重新授权。",
            "song_drive_invalid_file": "指定的云盘 inbox 文件无效，请核对文件名。",
            "song_drive_folder_unavailable": (
                "歌单云盘 Hakimi_Songs 目录不可用或无写入权限，请检查后台目录设置。"
            ),
            "song_drive_upload_failed": "上传原曲到云盘 inbox 失败，请稍后重试。",
            "song_inbox_upload_too_large": "音频超过 100 MiB 上限，请先压缩或截取后再试。",
            "colab_invalid_job": "提交学歌参数无效，请检查歌名或选项。",
            "colab_gpu_not_allowed": "当前任务不支持所选 GPU 型号，请更换可用显卡喵。",
            "invalid_song_engine": (
                "engine 仅支持 dual（默认双引擎）、yingmusic_svc、seed_vc 或 rvc。"
            ),
            "invalid_ref_voice": (
                "参考干声文件名无效，仅支持 models/ 目录下的 .wav/.flac/.mp3/.m4a/.ogg/.opus。"
            ),
            "ref_voice_not_found": (
                "未在云盘 Hakimi_Songs/models/ 中找到指定的参考干声文件（未创建 Colab 作业）。"
            ),
            "model_not_found": "未找到指定的 RVC 音色模型权重文件（未创建 Colab 作业）。",
            "source_not_found": (
                f"独立找歌未检索到《{raw_title}》的干净原版（未创建 Colab 作业），"
                "可手动上传原曲到 Drive inbox/ 或私聊附带音频重试。"
            ),
            "song_finder_remote_unavailable": (
                "独立音源检索插件（song_finder）当前离线或不可用（未创建 Colab 作业），"
                "请检查插件状态或直接附带音频/上传到 Drive inbox/。"
            ),
            "all_providers_in_cooldown": (
                "独立找歌源触发风控正在冷却中（未创建 Colab 作业），"
                "请稍后重试或手动上传原曲到 Drive inbox/。"
            ),
            "download_too_large": (
                "检索到的原曲文件超过体积上限（未创建 Colab 作业），请手动压缩后上传到 inbox/。"
            ),
            "song_drive_not_found": (
                "未在云盘 Hakimi_Songs/inbox/ 中找到指定的原曲文件（未创建 Colab 作业）。"
            ),
            "invalid_song_title": (
                "歌名格式不太对，支持 1～60 字多语种文字、数字与常见标点"
                '（不可含 / \\ < > " 等特殊符号）。'
            ),
            "invalid_song_meta": "歌手或标签格式过长或包含非法字符。",
            "invalid_song_pitch": "pitch 范围需在 -24 到 24 之间。",
            "invalid_song_audio": "音频格式不支持或文件已损坏。",
            "invalid_drive_file_id": "云盘文件标识无效，请重试。",
            "song_upload_too_large": "音频超过体积上限，请先压缩或截取后再试。",
            "song_source_required": "未附音频且已关闭自动搜歌，请附带音频或开启自动搜索。",
            "song_title_exists": f"歌单里已经有《{raw_title}》啦，换一首或先在后台删除旧版吧。",
            "song_library_full": "歌单已满（最多 300 首），去后台清理几首再来学吧。",
        }
        return err_map.get(code, f"提交学歌任务失败（{code}）。")

    job_id = str(job.get("id") or job.get("job_id") or "")
    pitch_note = f"，变调 {pitch:+d}" if pitch else ""
    engine_label = _ENGINE_CMD_LABELS.get(engine, engine)
    if audio_bytes is not None:
        source_note = "（已使用附带音频）"
    elif inbox_filename:
        source_note = "（云盘原曲）"
    else:
        source_note = "（自动检索原曲）"
    return (
        f"收到！已开始练习《{raw_title}》{source_note}"
        f"（{engine_label}，任务 {job_id}{pitch_note}），学好后会来叫你~"
    )


async def handle_switch_song_version_command(service: Any, message: Message, raw_args: str) -> str:
    _enforce_permission(service, message, "owner_private")
    stripped = raw_args.strip()
    if not stripped:
        return (
            "用法：/换歌版本 <歌名> [版本名]；不写版本名可查看该曲目所有已保留版本，"
            "版本名支持 yingmusic_svc（或 ying）、seed_vc（或 seed）、rvc。"
        )
    try:
        tokens = shlex.split(stripped)
    except ValueError:
        tokens = stripped.split()

    title_parts: list[str] = []
    take_key = ""
    idx = 0
    while idx < len(tokens):
        tok = tokens[idx]
        low = tok.lower()
        if low in {"--take", "--version", "-v"}:
            if idx + 1 >= len(tokens):
                return "用法：/换歌版本 <歌名> <版本名>"
            idx += 1
            take_key = tokens[idx].strip()
        elif low.startswith(("take=", "version=", "engine=", "版本=")):
            take_key = tok.split("=", 1)[1].strip()
        else:
            title_parts.append(tok)
        idx += 1

    if not take_key and len(title_parts) >= 2 and title_parts[-1].lower() in _SWITCH_KNOWN_TAKES:
        take_key = title_parts.pop()

    raw_title = " ".join(title_parts).strip().strip("《》\"'").strip()
    if not raw_title:
        return "请提供歌名，例如：/换歌版本 枝垂桜 seed_vc"

    songs = getattr(service, "songs", None)
    if songs is None or not hasattr(songs, "switch_take"):
        return "歌单工坊模块尚未就绪，请稍后再试。"

    if not take_key:
        entry = await songs.get_entry_by_title(raw_title)
        if entry is None:
            return f"歌单里还没找到《{raw_title}》呢~"
        takes = entry.get("takes") or []
        if not takes:
            eng = _ENGINE_CMD_LABELS.get(
                str(entry.get("active_engine") or ""), str(entry.get("active_engine") or "默认")
            )
            return f"《{entry['title']}》当前只有单版本（{eng}），暂无其他候选版本可切换。"
        lines = [f"《{entry['title']}》已保留版本："]
        for t in takes:
            t_label = _ENGINE_CMD_LABELS.get(str(t.get("engine") or ""), str(t.get("engine") or ""))
            if t.get("active"):
                mark = "【当前生效】"
            elif t.get("playable"):
                mark = "（可切换）"
            else:
                mark = "（不可用）"
            lines.append(f"- {t['key']}（{t_label}）{mark}")
        note = str(entry.get("review_note") or "").strip()
        if note:
            lines.append(f"猫猫试听点评：{note}")
        lines.append(f"发送 /换歌版本 {entry['title']} <版本名> 即可切换默认播放版本。")
        return "\n".join(lines)

    try:
        normalized_take = normalize_take_key(take_key, allow_aliases=True)
        updated = await songs.switch_take(raw_title, normalized_take, by_title=True)
    except Rejected as exc:
        code = str(exc)
        err_map = {
            "song_not_found": f"歌单里还没找到《{raw_title}》呢~",
            "invalid_song_take": (
                "版本名格式不对，支持 yingmusic_svc（ying）、seed_vc（seed）或 rvc。"
            ),
            "song_take_not_found": (
                f"《{raw_title}》没有保留「{take_key}」这个候选版本，"
                f"发送 /换歌版本 {raw_title} 可查看可用版本。"
            ),
            "song_take_unavailable": f"《{raw_title}》的「{take_key}」版本当前不可播放，无法切换。",
            "song_already_exists": "目标版本的音频指纹与歌单中其他曲目冲突，暂无法切换。",
        }
        return err_map.get(code, f"切换歌曲版本失败（{code}）。")

    eng_label = _ENGINE_CMD_LABELS.get(
        str(updated.get("active_engine") or ""), str(updated.get("active_engine") or "")
    )
    return (
        f"好呀！已将《{updated['title']}》的默认播放版本切换为 "
        f"{eng_label}（{updated.get('active_take') or normalized_take}），下次点唱就会唱这版喵~"
    )


def build_default_command_registry() -> CommandRegistry:
    """Create the default command registry with all built-in chat commands."""
    registry = CommandRegistry()
    registry.register_delegate(lambda svc: getattr(svc, "moderation", None))
    registry.register_delegate(lambda svc: getattr(svc, "longnick", None))

    registry.register(
        "/bot-interact",
        handle_bot_interact_command,
        dotall=True,
        strip_input=False,
        permission="public",
        description="授权外部 Bot 临时互动",
    )
    registry.register(
        "/new-session",
        handle_new_session_command,
        exact=True,
        strip_input=False,
        permission="group_owner",
        description="清空当前会话上下文",
    )
    registry.register(
        "/存表情",
        handle_save_meme_command,
        aliases=("/偷表情", "/save-meme"),
        prefix_aliases=("存表情", "偷表情"),
        dotall=True,
        permission="owner",
        description="保存图片到表情包库",
    )
    registry.register(
        "/affection",
        handle_affection_command,
        aliases=("/好感度",),
        prefix_aliases=("好感度",),
        prefix_extra_prefixes=("/哈基米",),
        prefix_requires_space=True,
        permission="owner_private",
        description="查看或调整用户好感度",
    )
    registry.register(
        "/bind",
        handle_bind_command,
        permission="public",
        description="跨平台账号绑定",
    )
    registry.register(
        "/学歌",
        handle_learn_song_command,
        aliases=("/learn-song",),
        prefix_aliases=("学歌",),
        dotall=True,
        permission="owner_private",
        description="提交 Colab 学歌任务",
    )
    registry.register(
        "/换歌版本",
        handle_switch_song_version_command,
        aliases=("/switch-song", "/切歌版本"),
        prefix_aliases=("换歌版本", "切歌版本"),
        dotall=False,
        permission="owner_private",
        description="查看或切换歌曲候选版本",
    )

    registry.register_prefix("", handle_help_command, aliases=("帮助",), permission="public")
    registry.register_prefix(
        "清空", handle_memory_clear_command, aliases=("记忆 关",), permission="group_owner"
    )
    registry.register_prefix("记忆 开", handle_memory_enable_command, permission="group_owner")
    registry.register_prefix(
        "订阅列表", handle_subscriptions_list_command, permission="owner_private"
    )
    registry.register_prefix("报告", handle_reports_command, permission="owner_private")
    registry.register_prefix("状态", handle_status_command, permission="owner_private")
    registry.register_prefix(
        "订阅",
        handle_subscription_command,
        aliases=("退订",),
        match_prefix=True,
        permission="owner_private",
    )
    return registry
