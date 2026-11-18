"""Base capability abstractions, registry, and mandatory anti-injection sensor firewall."""

from __future__ import annotations

import html
import re
from abc import ABC, abstractmethod
from typing import Any, Literal, final

from pydantic import BaseModel, ConfigDict, Field

from ..config import Settings
from ..context_budget import truncate_tokens
from ..models import Message
from ..security import clean_text, configured_secrets
from .actions import Action, ActionCatalog, DailyAction

AttentionLevel = Literal["LOW", "MEDIUM", "HIGH"]
ATTENTION_RANK: dict[str, int] = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
NodeRegion = Literal["KR", "CN"]

_AUTH_HEADER_RE = re.compile(r"(?i)\bauthorization\b\s*[:=]\s*Bearer\s+[A-Za-z0-9._~+/=-]{4,}")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{4,}")
_SK_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{6,}")
_AIZA_RE = re.compile(r"\bAIza[0-9A-Za-z_-]{8,}")
_CREDENTIAL_KEY_RE = re.compile(
    r"(?i)(?:[\"']?)"
    r"(?:api[_-]?key|apikey|password|passwd|pwd|secret|token|"
    r"sessdata|bili_jct|buvid3|buvid4|access[_-]?token|refresh[_-]?token|authorization)"
    r"(?:[\"']?)\s*[:=]\s*(?:[\"']?)[^\s,;}\"'\]]+(?:[\"']?)"
)
_ROLE_RE = re.compile(r"\[role=", re.IGNORECASE)
_CQ_RE = re.compile(r"\[cq:", re.IGNORECASE)
_AT_BROADCAST_RE = re.compile(r"@(everyone|all|全体成员)", re.IGNORECASE)
_COLLAPSE_WS_RE = re.compile(r"[\r\n\t ]+")


class GoalResult(BaseModel):
    """Structured result returned by a module after dispatching an intent goal."""

    model_config = ConfigDict(extra="forbid", strict=True)

    ok: bool
    module_id: str = Field(min_length=1, max_length=64)
    summary: str = ""
    artifacts: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None


def sanitize_sensor_text(
    text: str, settings: Settings | None = None, *, max_tokens: int = 90
) -> str:
    """Scrub secrets, neutralize prompt/protocol injections, and bound token length."""
    if max_tokens <= 0:
        return ""
    cleaned = clean_text(str(text or ""))
    if not cleaned:
        return ""
    if settings is not None:
        candidates = {
            val
            for raw in configured_secrets(settings)
            if raw
            for val in (raw, clean_text(raw))
            if len(val) >= 6
        }
        for secret in sorted(candidates, key=len, reverse=True):
            if secret in cleaned:
                cleaned = cleaned.replace(secret, "[REDACTED]")
    cleaned = _AUTH_HEADER_RE.sub("[REDACTED]", cleaned)
    cleaned = _BEARER_RE.sub("[REDACTED]", cleaned)
    cleaned = _SK_RE.sub("[REDACTED]", cleaned)
    cleaned = _AIZA_RE.sub("[REDACTED]", cleaned)
    cleaned = _CREDENTIAL_KEY_RE.sub("[REDACTED]", cleaned)
    cleaned = _ROLE_RE.sub("[untrusted_role=", cleaned)
    cleaned = _CQ_RE.sub("［CQ:", cleaned)
    cleaned = _AT_BROADCAST_RE.sub(r"＠\1", cleaned)
    cleaned = html.escape(cleaned, quote=False)
    cleaned = _COLLAPSE_WS_RE.sub(" ", cleaned).strip()
    return truncate_tokens(cleaned, max_tokens)


class BaseModule(ABC):
    """Abstract capability module with enforced sensor sanitization and proactive lock hooks."""

    module_id: str = ""
    node_region: NodeRegion = "KR"
    attention_level: AttentionLevel = "LOW"
    proactive_paused: bool = False
    settings: Settings | None = None

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings
        self.proactive_paused = False

    def actions(self) -> tuple[Action, ...]:
        return ()

    async def social_material(self, ticket: dict) -> str:
        return await self.get_sensor_summary()

    async def context_data(self, *, limit: int, now: float, message: Message | None = None) -> dict:
        return {}

    def schedules(self) -> tuple[DailyAction, ...]:
        return ()

    def event_rules(self) -> tuple:
        return ()

    @final
    async def get_sensor_summary(self) -> str:
        """Return a strictly sanitized, <=90-token summary of current module sensor state."""
        raw = await self._raw_sensor_summary()
        return sanitize_sensor_text(raw, getattr(self, "settings", None), max_tokens=90)

    @abstractmethod
    async def _raw_sensor_summary(self) -> str:
        """Produce raw module status string before mandatory sanitization."""

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        """Compatibility entry for 2.0 module callers."""
        return GoalResult(ok=False, module_id=self.module_id, reason="unsupported_goal", summary="")

    async def pause_proactive(self) -> None:
        """Suspend autonomous proactive behavior when another high-attention task locks focus."""
        self.proactive_paused = True

    async def resume_proactive(self) -> None:
        """Resume autonomous proactive behavior when focus drops below HIGH."""
        self.proactive_paused = False


class ModuleRegistry:
    """In-memory registry of capability modules for sensor collection and attention locking."""

    def __init__(self) -> None:
        self._modules: dict[str, BaseModule] = {}
        self.catalog = ActionCatalog()

    def register(self, module: BaseModule) -> None:
        """Register a capability module by its unique module_id."""
        if not module.module_id or module.module_id in self._modules:
            raise ValueError("module_id must be non-empty and unique")
        self.catalog.install(module.module_id, module.actions())
        self._modules[module.module_id] = module

    def get(self, module_id: str) -> BaseModule | None:
        """Return the registered module matching module_id, or None."""
        return self._modules.get(module_id)

    def all(self) -> list[BaseModule]:
        """Return all registered modules in registration order."""
        return list(self._modules.values())

    async def context_data(self, *, limit: int, now: float, message: Message | None = None) -> dict:
        data = {}
        for module in self.all():
            data.update(await module.context_data(limit=limit, now=now, message=message))
        return data

    async def collect_sensor_summaries(self) -> dict[str, str]:
        """Collect sanitized sensor summaries across all registered modules."""
        summaries: dict[str, str] = {}
        for module in self._modules.values():
            summaries[module.module_id] = await module.get_sensor_summary()
        return summaries
