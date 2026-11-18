"""Trusted, in-process action declarations. A model selects a name, never a callable."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class NoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


Origin = Literal["planner", "timer", "rule", "admin", "legacy"]
Mode = Literal["active", "shadow", "legacy", "off"]
AUTONOMOUS = frozenset({"planner", "rule", "timer", "legacy"})
NAME = re.compile(r"[a-z][a-z0-9_]{0,30}\.[a-z][a-z0-9_]{0,30}\Z")


@dataclass(frozen=True)
class ActionCall:
    ticket: dict
    args: BaseModel
    now: float
    evidence: dict = field(default_factory=dict)
    desktop_hint: str | None = None
    still_allowed: Callable[[], bool] = lambda: True


@dataclass(frozen=True)
class Outcome:
    status: Literal["done", "gated_skip", "not_dispatched", "uncertain"]
    reason: str
    details: dict = field(default_factory=dict)


@dataclass(frozen=True)
class GateDecision:
    reason: str = ""
    delay: int = 0
    desktop_hint: str | None = None


Handler = Callable[[ActionCall], Awaitable[Outcome]]
Alias = tuple[str, str, bool]  # exact 2.0 (target_module, action_type, has_recipient)


@dataclass(frozen=True, kw_only=True)
class Action:
    name: str
    description: str
    run: Handler
    arguments: type[BaseModel] = NoArgs
    social: bool = False
    owner_only: bool = False
    budgeted: bool = False
    origins: frozenset[str] = AUTONOMOUS
    unavailable: Callable[[], str | None] = lambda: None
    preflight: Callable[[dict, float, Any], Awaitable[GateDecision]] | None = None
    aliases: tuple[Alias, ...] = ()


class ActionCatalog:
    def __init__(self):
        self.actions: dict[str, Action] = {}
        self.owners: dict[str, str] = {}
        self.aliases: dict[Alias, str] = {}

    def install(self, module_id: str, actions: tuple[Action, ...]) -> None:
        names, aliases = set(self.actions), set(self.aliases)
        for spec in actions:
            if not NAME.fullmatch(spec.name) or spec.name in names:
                raise ValueError(f"invalid or duplicate action: {spec.name}")
            names.add(spec.name)
            for alias in spec.aliases:
                if alias in aliases or alias[2] != spec.social:
                    raise ValueError(f"invalid or duplicate action alias: {alias}")
                aliases.add(alias)
        for spec in actions:
            self.actions[spec.name] = spec
            self.owners[spec.name] = module_id
            self.aliases.update(dict.fromkeys(spec.aliases, spec.name))

    def old_name(self, ticket: dict) -> str | None:
        return self.aliases.get(
            (ticket["target_module"], ticket["action_type"], bool(ticket["target_scope"]))
        )

    def resolve(self, ticket: dict, *, compatibility: bool = False) -> Action:
        name = (
            self.old_name(ticket)
            if compatibility
            else ticket.get("action_name") or self.old_name(ticket)
        )
        spec = self.actions.get(name or "")
        if not spec:
            raise ValueError("unknown_action")
        if ticket.get("origin", "legacy") not in spec.origins:
            raise ValueError("action_origin_forbidden")
        if spec.social != bool(ticket.get("target_scope")):
            raise ValueError("invalid_action_audience")
        if compatibility and ticket.get("action_name") not in (None, "", name):
            raise ValueError("incompatible_action")
        return spec

    def view(
        self, modes: dict[str, Mode], *, planner: bool = False, module_id: str | None = None
    ) -> list[dict[str, Any]]:
        rows = []
        for name, spec in sorted(self.actions.items()):
            if module_id is not None and self.owners[name] != module_id:
                continue
            mode = modes.get(name, "active")
            reason = "action_disabled" if mode == "off" else spec.unavailable()
            if planner and (reason or "planner" not in spec.origins):
                continue
            schema = spec.arguments.model_json_schema()
            audience = "social" if spec.social else "internal"
            if spec.owner_only:
                audience = "owner"
            rows.append(
                {
                    "name": name,
                    "module": self.owners[name],
                    "description": spec.description,
                    "audience": audience,
                    "arguments": schema["properties"],
                    "required": schema.get("required", []),
                    "mode": mode,
                    "available": reason is None,
                    "reason": reason,
                    "origins": sorted(spec.origins),
                    "compatibility": bool(spec.aliases),
                }
            )
        return rows


@dataclass(frozen=True)
class DailyAction:
    """A module-provided local schedule; idempotency uses the local day and slot."""

    name: str
    at: str | None  # None closes the active window for the local day.
    ttl: int = 1800  # Fixed-time actions; a closing action expires at the end of its window.
    target_scope: str = ""


def reject_reason(exc: ValueError) -> str:
    # Pydantic diagnostics may include submitted values. Only send constant reason codes to admin.
    codes = {
        "unknown_action",
        "action_origin_forbidden",
        "invalid_action_audience",
        "incompatible_action",
        "action_disabled",
        "unauthorized_scope",
        "invalid_action_arguments",
    }
    return str(exc) if type(exc) is ValueError and str(exc) in codes else "invalid_action_arguments"
