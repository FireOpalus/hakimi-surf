"""Owner chat action discovery, validation, and background dispatch mixin for `Service`."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .models import Message
from .modules.actions import ActionCall, reject_reason
from .outbound_actions import ChatActionTrigger
from .security import Rejected, owner

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


class _ServiceOwnerActionsMixin:
    """Mixin managing owner-private chat-triggered module and song-studio actions."""

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
            reason = self._safe_action_reason(exc)
            log.info(
                "event=owner_chat_action_done action=%s status=gated_skip reason=%s",
                _LEARN_SONG_ACTION_NAME,
                reason,
            )
            prep_notices = {
                "source_not_found": (
                    f"刚刚帮 BOISER 找《{title}》的原曲没找到干净原版（未创建 Colab 作业），"
                    "可以把原曲直接发给我或传到云盘 inbox/ 再开练喵~"
                ),
                "song_finder_remote_unavailable": (
                    f"独立找歌插件（song_finder）当前离线或不可用，暂时没法自动搜《{title}》"
                    "（未创建 Colab 作业），可以把原曲直接发给我或传到云盘 inbox/。"
                ),
                "all_providers_in_cooldown": (
                    f"独立找歌源当前触发风控冷却中，暂时没法自动搜《{title}》"
                    "（未创建 Colab 作业），请稍后再试或把原曲发给我。"
                ),
                "ref_voice_not_found": (
                    f"准备学《{title}》时没在云盘 models/ 里找到可用参考干声"
                    "（未创建 Colab 作业），请先在后台上传参考干声。"
                ),
                "song_drive_not_found": (
                    f"未在云盘 inbox/ 里找到《{title}》指定的原曲文件（未创建 Colab 作业）。"
                ),
                "download_too_large": (
                    f"检索到的《{title}》原曲超过体积上限（未创建 Colab 作业），"
                    "麻烦手动压缩后上传到 inbox/。"
                ),
            }
            notice_text = prep_notices.get(reason)
            proactive = getattr(self, "proactive", None)
            alert_fn = getattr(proactive, "alert", None) if proactive is not None else None
            if notice_text and callable(alert_fn):
                try:
                    await alert_fn(f"song_prep_{reason}", notice_text)
                except Exception:
                    pass
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
