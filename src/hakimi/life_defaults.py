"""Local offline routine used when no model plan is available."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from .gemini import WireLifePlan


def fallback_plan(
    settings,
    *,
    local_dt: datetime,
    current_energy: int,
    current_boredom: int,
    module_data: dict[str, Any],
    groups: list[str],
    owner_available: bool,
    desktop_recent: bool,
    patrol_without_paid_plan: bool = False,
) -> WireLifePlan:
    bili_surfed = module_data.get("recent_bili_surfed", [])
    hour = local_dt.hour
    if patrol_without_paid_plan or bili_surfed or hour in {13, 14, 20, 21}:
        module = "bilibili_surf"
        focus: Literal["LOW", "MEDIUM", "HIGH"] = "MEDIUM"
        mood = "curious"
    else:
        module = "idle"
        focus = "LOW"
        mood = "calm"
    energy = max(15, min(100, current_energy - 5))
    boredom = max(0, min(95, current_boredom + 8))
    intents: list[dict[str, Any]] = []
    if patrol_without_paid_plan:
        intents.append(
            {
                "action_name": "bili.patrol",
                "delay_minutes": 1,
                "ttl_minutes": 60,
                "intent_note": "用可用的巡逻额度去看看首页和关注动态",
            }
        )
    elif bili_surfed and groups:
        intents.append(
            {
                "target_module": "bilibili_surf",
                "target_scope": f"onebot:{groups[0]}",
                "action_type": "group_chat_share",
                "delay_minutes": 15,
                "ttl_minutes": 90,
                "intent_note": "刚才刷到一条挺有意思的公开视频，看看群里氛围合适就顺嘴聊两句",
                "allow_desktop_care": False,
            }
        )
    elif desktop_recent and owner_available:
        intents.append(
            {
                "target_module": "qq_social",
                "target_scope": f"owner:{settings.owner_qq_id}",
                "action_type": "owner_desktop_warm_care",
                "delay_minutes": 20,
                "ttl_minutes": 90,
                "intent_note": "留意到对方刚在电脑端忙完正事，合适时轻声关心一句注意休息喝水",
                "allow_desktop_care": True,
            }
        )
    return WireLifePlan.model_validate(
        {
            "current_module": module,
            "focus_level": focus,
            "mood": mood,
            "energy": energy,
            "boredom": boredom,
            "mood_longnick": "",
            "intents": intents,
        }
    )
