"""Compose an offline plan from suggestions made by registered modules."""

from .gemini import WireLifePlan


async def fallback_plan(engine, context, local_dt, now, available, planner_funded):
    focus_module, focus, mood = "idle", "LOW", "calm"
    choices = []
    for module in engine.service.modules.all():
        if hint := module.local_focus(context, local_dt.hour):
            focus_module, (focus, mood) = module.module_id, hint
        choice = module.local_activity(context, available, not planner_funded)
        if choice is not None:
            choices.append((choice, module))

    intent = None
    for choice, module in sorted(choices, key=lambda item: item[0].priority, reverse=True):
        candidate = choice.intent
        name = candidate.get("action_name") or engine.catalog.old_name(
            {
                "target_module": candidate.get("target_module", module.module_id),
                "action_type": candidate.get("action_type", ""),
                "target_scope": candidate.get("target_scope", ""),
            }
        )
        if name not in available:
            continue
        if not await engine.gatekeeper.daily_limit_allows(
            name, candidate.get("target_scope", ""), now, engine.config
        ):
            continue
        if choice.one_outstanding:
            async with engine.store.lock:
                pending = await engine.store.one(
                    "SELECT 1 FROM scheduled_intents WHERE action_name=? "
                    "AND status IN ('pending','executing') AND expire_at>? LIMIT 1",
                    (name, now),
                )
            if pending:
                continue
        intent = candidate
        if choice.focus:
            focus_module, (focus, mood) = module.module_id, choice.focus
        break
    return WireLifePlan.model_validate(
        {
            "current_module": focus_module,
            "focus_level": focus,
            "mood": mood,
            "mood_longnick": "",
            "intents": [intent] if intent else [],
        }
    )
