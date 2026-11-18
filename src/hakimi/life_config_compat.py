"""Compatibility at the storage/API boundary for the original flat 2.1 settings."""

from .modules.bili_schedule import BiliScheduleOptions
from .modules.qq_schedule import QQScheduleOptions

OLD_MODULES = {
    "qq_social": (
        QQScheduleOptions,
        (
            "owner_delivery_enabled",
            "owner_dms_unlimited",
            "group_cooldown_seconds",
            "group_active_window_seconds",
            "group_min_recent_messages",
            "desktop_active_suppress_seconds",
        ),
    ),
    "bilibili_surf": (BiliScheduleOptions, ("bili_patrols_unlimited",)),
}


class LegacyScheduleAccess:
    """Read compatibility for integrations that accessed the old flat attributes."""

    def __getattr__(self, key):
        for module_id, (schema, fields) in OLD_MODULES.items():
            if key in fields:
                return getattr(schema.model_validate(self.module_settings.get(module_id, {})), key)
        return super().__getattr__(key)


def migrate_flat(raw):
    if not isinstance(raw, dict):
        return raw
    result = dict(raw)
    modules = result.get("module_settings", {})
    if not isinstance(modules, dict):
        return result
    modules = dict(modules)
    for name, (schema, fields) in OLD_MODULES.items():
        flat = {k: result.pop(k) for k in fields if k in result}
        if flat:
            existing = modules.get(name, {})
            if not isinstance(existing, dict):
                raise ValueError("invalid_module_settings")
            options = schema.model_validate({**existing, **flat})
            modules[name] = options.model_dump(exclude_unset=True)
    if modules:
        result["module_settings"] = modules
    return result


def legacy_view(config, registry=None):
    result = config.model_dump()
    for name, (schema, fields) in OLD_MODULES.items():
        module = registry.get(name) if registry else None
        options = (
            module.scheduling(config)
            if module
            else schema.model_validate(config.module_settings.get(name, {}))
        )
        result.update(options.model_dump(include=set(fields)))
    return result


def import_old_proactive(old, saved, merged):
    if "owner_delivery_enabled" not in saved and "owner_delivery_enabled" not in saved.get(
        "module_settings", {}
    ).get("qq_social", {}):
        if "enabled" in old:
            merged["owner_delivery_enabled"] = old["enabled"]
    if "quiet_hours" not in saved and "quiet_hours" in old:
        merged["quiet_hours"] = old["quiet_hours"]
    if not old.get("daily_digest_enabled", True):
        merged["action_modes"].setdefault("qq.owner_digest", "off")


async def next_owner_digest(engine, now):
    module = engine.service.modules.get("qq_social")
    if module:
        for slot in module.schedules():
            if slot.name == "qq.owner_digest":
                return await engine.next_slot_at(module, slot, now)
    return None
