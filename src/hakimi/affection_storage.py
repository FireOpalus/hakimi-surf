"""Persistence, identity migration, and history query helpers for AffectionManager."""

from typing import Any

from .affection_rules import (
    _sanitize_nickname,
    _sanitize_reason,
    _validate_platform_user,
    capabilities_for_score,
    clamp_score,
    tier_matrix_rows,
)


class _AffectionStorageMixin:
    async def _migrate_to_identity_unlocked(self, identity: Any) -> None:
        if identity is None or not getattr(identity, "initialized", False):
            return
        if self._migrated_to_person:
            return
        async with self.store.lock:
            already = await self.store.one(
                "SELECT 1 FROM meta WHERE key='affection_person_migrated_v1'"
            )
            user_rows = (
                []
                if already
                else await self.store.all(
                    "SELECT user_key, subject, payload, updated FROM user_affection WHERE scope=?",
                    (self.scope,),
                )
            )
            hist_rows = (
                []
                if already
                else await self.store.all(
                    "SELECT user_key, payload FROM affection_history WHERE scope=?",
                    (self.scope,),
                )
            )
        if already:
            await self._reload_caches_unlocked(identity)
            self._migrated_to_person = True
            return

        legacy_items: list[tuple[str, dict[str, Any], str]] = []
        for row in user_rows:
            data = self.store.vault.open("affection:" + row["user_key"], row["payload"])
            if not isinstance(data, dict):
                continue
            existing_pid = str(data.get("person_id", ""))
            if existing_pid and row["user_key"] == self._person_key(existing_pid):
                continue
            platform = str(data.get("platform", "onebot"))
            uid = str(data.get("user_id", ""))
            if not uid:
                continue
            nickname = str(data.get("nickname", ""))
            pid = await identity.resolve_person_id(
                platform, uid, nickname, create=True, source="auto"
            )
            if pid is not None:
                legacy_items.append((row["user_key"], data, pid))

        legacy_user_keys = {old_key for old_key, _, _ in legacy_items}
        extra_hist_seen: set[str] = set()
        extra_hist_migrations: list[tuple[str, str]] = []
        for hrow in hist_rows:
            hkey = str(hrow["user_key"])
            if hkey in legacy_user_keys or hkey in extra_hist_seen:
                continue
            hdata = self.store.vault.open("affection-hist:" + hkey, hrow["payload"])
            if not isinstance(hdata, dict):
                continue
            existing_pid = str(hdata.get("person_id") or "")
            if existing_pid and hkey == self._person_key(existing_pid):
                continue
            hplat = str(hdata.get("platform", "onebot"))
            huid = str(hdata.get("user_id", ""))
            if not huid:
                continue
            hpid = await identity.resolve_person_id(hplat, huid, create=False)
            if hpid is not None:
                extra_hist_seen.add(hkey)
                extra_hist_migrations.append((hkey, hpid))

        today = self._today_str()
        now = float(self.clock())
        zero_person_ids: list[str] = []

        async with self._lock:
            async with self.store.transaction():
                if await self.store.one(
                    "SELECT 1 FROM meta WHERE key='affection_person_migrated_v1'"
                ):
                    self._migrated_to_person = True
                    return

                grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {}
                for old_key, data, pid in legacy_items:
                    grouped.setdefault(pid, []).append((old_key, data))

                for pid, items in grouped.items():
                    person_key = self._person_key(pid)
                    existing_prow = await self.store.one(
                        "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                        (person_key, self.scope),
                    )
                    existing_pdata = (
                        self.store.vault.open("affection:" + person_key, existing_prow["payload"])
                        if existing_prow
                        else None
                    )
                    all_entries: list[dict[str, Any]] = [
                        *([existing_pdata] if isinstance(existing_pdata, dict) else []),
                        *(d for _, d in items),
                    ]
                    is_owner = (pid == "owner") or any(
                        self.is_owner_id(
                            str(e.get("platform", "onebot")), str(e.get("user_id", ""))
                        )
                        for e in all_entries
                    )
                    raw_scores = [
                        clamp_score(
                            int(e.get("score", 120 if is_owner else self.config.initial_score)),
                            is_owner=is_owner,
                        )
                        for e in all_entries
                    ]
                    if is_owner:
                        merged_score = clamp_score(max(raw_scores), is_owner=True)
                    elif 0 in raw_scores:
                        merged_score = 0
                        zero_person_ids.append(pid)
                    else:
                        merged_score = clamp_score(min(raw_scores), is_owner=False)

                    latest = max(all_entries, key=lambda e: float(e.get("updated_at", 0.0) or 0.0))
                    rep_platform = str(latest.get("platform", "onebot"))
                    rep_uid = str(latest.get("user_id", ""))
                    same_day_entries = [e for e in all_entries if e.get("daily_day") == today]
                    today_gain = max(
                        (max(0, int(e.get("today_positive_gain", 0))) for e in same_day_entries),
                        default=0,
                    )
                    first_day = next(
                        (
                            str(e.get("daily_first_day", ""))
                            for e in same_day_entries
                            if str(e.get("daily_first_day", ""))
                        ),
                        "",
                    )
                    last_annoyance = max(
                        (float(e.get("last_annoyance_at", 0.0) or 0.0) for e in all_entries),
                        default=0.0,
                    )
                    last_negative = max(
                        (float(e.get("last_negative_at", 0.0) or 0.0) for e in all_entries),
                        default=0.0,
                    )
                    last_recovery_day = max(
                        (str(e.get("last_recovery_day", "") or "") for e in all_entries),
                        default="",
                    )
                    safety_strikes_total = sum(
                        max(0, int(e.get("safety_strikes_total", 0) or 0)) for e in all_entries
                    )
                    last_violation_at = max(
                        (float(e.get("last_violation_at", 0.0) or 0.0) for e in all_entries),
                        default=0.0,
                    )
                    updated_at = max(
                        (float(e.get("updated_at", 0.0) or 0.0) for e in all_entries),
                        default=now,
                    )
                    merged_record = self._normalize_record(
                        {
                            **latest,
                            "score": merged_score,
                            "daily_day": today,
                            "today_positive_gain": today_gain,
                            "daily_first_day": first_day,
                            "last_annoyance_at": last_annoyance,
                            "last_negative_at": last_negative,
                            "last_recovery_day": last_recovery_day,
                            "safety_strikes_total": safety_strikes_total,
                            "last_violation_at": last_violation_at,
                            "updated_at": updated_at,
                        },
                        rep_platform,
                        rep_uid,
                        "",
                        today,
                        is_owner=is_owner,
                        person_id=pid,
                    )
                    sealed = self.store.vault.seal("affection:" + person_key, merged_record)
                    subject = self._subject(rep_platform, rep_uid)
                    await self.store.db.execute(
                        "INSERT INTO user_affection(user_key,scope,subject,payload,updated) "
                        "VALUES (?,?,?,?,?) "
                        "ON CONFLICT(user_key) DO UPDATE SET scope=excluded.scope,"
                        "subject=excluded.subject,payload=excluded.payload,updated=excluded.updated",
                        (
                            person_key,
                            self.scope,
                            subject,
                            sealed,
                            float(merged_record["updated_at"]),
                        ),
                    )
                    for old_key, _ in items:
                        if old_key != person_key:
                            await self.store.db.execute(
                                "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                                (old_key, self.scope),
                            )
                            await self.store.db.execute(
                                "UPDATE affection_events SET user_key=? "
                                "WHERE scope=? AND user_key=?",
                                (person_key, self.scope, old_key),
                            )
                            await self._migrate_history_key_in_tx(
                                old_key, person_key, person_id=pid
                            )

                for old_hist_key, pid in extra_hist_migrations:
                    await self._migrate_history_key_in_tx(
                        old_hist_key, self._person_key(pid), person_id=pid
                    )

                await self.store.db.execute(
                    "INSERT OR IGNORE INTO meta VALUES ('affection_person_migrated_v1', ?)",
                    (self.store.vault.seal("meta:affection_person_migrated_v1", {"v": 1}),),
                )

        for pid in zero_person_ids:
            if pid != "owner":
                await identity.set_person_muted(pid, True, source="ai_affection_zero")

        await self._reload_caches_unlocked(identity)
        self._migrated_to_person = True

    async def migrate_to_identity(self) -> None:
        identity = self._get_identity()
        if identity is None or not getattr(identity, "initialized", False):
            return
        async with self.init_lock:
            await self._migrate_to_identity_unlocked(identity)

    async def _ensure_identity_ready(self) -> Any:
        identity = self._get_identity()
        if identity is None:
            return None
        if not getattr(identity, "initialized", False):
            await identity.initialize()
        if not self._migrated_to_person:
            await self.migrate_to_identity()
        return identity

    def _default_record(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        is_owner: bool | None = None,
        person_id: str | None = None,
    ) -> dict[str, Any]:
        owner_flag = self.is_owner_id(platform, user_id) if is_owner is None else bool(is_owner)
        score = 120 if owner_flag else self.config.initial_score
        record: dict[str, Any] = {
            "user_id": user_id,
            "platform": platform,
            "nickname": _sanitize_nickname(nickname, self.s),
            "score": score,
            "daily_day": today,
            "today_positive_gain": 0,
            "daily_first_day": "",
            "last_annoyance_at": 0.0,
            "last_negative_at": 0.0,
            "last_recovery_day": "",
            "safety_strikes_total": 0,
            "last_violation_at": 0.0,
            "last_reason": "init",
            "last_delta": 0,
            "updated_at": 0.0,
        }
        if person_id:
            record["person_id"] = person_id
        return record

    def _normalize_record(
        self,
        data: dict[str, Any],
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        is_owner: bool | None = None,
        person_id: str | None = None,
    ) -> dict[str, Any]:
        owner_flag = self.is_owner_id(platform, user_id) if is_owner is None else bool(is_owner)
        default_score = 120 if owner_flag else self.config.initial_score
        score = clamp_score(int(data.get("score", default_score)), is_owner=owner_flag)
        same_day = data.get("daily_day") == today
        today_gain = max(0, int(data.get("today_positive_gain", 0))) if same_day else 0
        first_day = str(data.get("daily_first_day", "")) if same_day else ""
        clean_nick = (
            _sanitize_nickname(nickname, self.s)
            if nickname
            else _sanitize_nickname(data.get("nickname", ""), self.s)
        )
        resolved_pid = person_id or (str(data["person_id"]) if data.get("person_id") else None)
        record: dict[str, Any] = {
            "user_id": user_id,
            "platform": platform,
            "nickname": clean_nick,
            "score": score,
            "daily_day": today,
            "today_positive_gain": today_gain,
            "daily_first_day": first_day,
            "last_annoyance_at": float(data.get("last_annoyance_at", 0.0) or 0.0),
            "last_negative_at": float(data.get("last_negative_at", 0.0) or 0.0),
            "last_recovery_day": str(data.get("last_recovery_day", "") or ""),
            "safety_strikes_total": max(0, int(data.get("safety_strikes_total", 0) or 0)),
            "last_violation_at": max(0.0, float(data.get("last_violation_at", 0.0) or 0.0)),
            "last_reason": _sanitize_reason(data.get("last_reason", "chat"), self.s),
            "last_delta": int(data.get("last_delta", 0) or 0),
            "updated_at": float(data.get("updated_at", 0.0) or 0.0),
        }
        if resolved_pid:
            record["person_id"] = resolved_pid
        return record

    async def _load_user_unlocked(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        person_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        is_owner = (person_id == "owner") or self.is_owner_id(platform, user_id)
        user_key = (
            self._person_key(person_id)
            if person_id is not None
            else self._legacy_user_key(platform, user_id)
        )
        row = await self.store.one(
            "SELECT payload FROM user_affection WHERE user_key=?", (user_key,)
        )
        if not row:
            return (
                self._default_record(
                    platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
                ),
                False,
            )
        opened = self.store.vault.open("affection:" + user_key, row["payload"])
        if not isinstance(opened, dict):
            return (
                self._default_record(
                    platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
                ),
                False,
            )
        return (
            self._normalize_record(
                opened, platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
            ),
            True,
        )

    async def _save_user_in_tx(
        self,
        platform: str,
        user_id: str,
        record: dict[str, Any],
        *,
        bot_id: str | None = None,
        person_id: str | None = None,
    ) -> None:
        if person_id is not None:
            record = {**record, "person_id": person_id}
            user_key = self._person_key(person_id)
        else:
            user_key = self._legacy_user_key(platform, user_id)
        subject = self._subject(platform, user_id, bot_id)
        sealed = self.store.vault.seal("affection:" + user_key, record)
        await self.store.db.execute(
            "INSERT INTO user_affection(user_key,scope,subject,payload,updated) VALUES (?,?,?,?,?) "
            "ON CONFLICT(user_key) DO UPDATE SET scope=excluded.scope,subject=excluded.subject,"
            "payload=excluded.payload,updated=excluded.updated",
            (user_key, self.scope, subject, sealed, float(record["updated_at"])),
        )

    async def _append_history_in_tx(
        self,
        *,
        user_key: str,
        platform: str,
        user_id: str,
        person_id: str | None,
        nickname: str,
        prev_score: int,
        new_score: int,
        kind: str,
        reason: str,
        now: float,
        allow_zero: bool = False,
    ) -> None:
        delta = int(new_score) - int(prev_score)
        if delta == 0 and not allow_zero:
            return
        await self.store.db.execute(
            "DELETE FROM affection_history WHERE created<?",
            (now - 90 * 86400,),
        )
        payload_data = {
            "user_id": str(user_id),
            "platform": str(platform),
            "person_id": str(person_id) if person_id else None,
            "nickname": _sanitize_nickname(nickname, self.s),
            "prev_score": int(prev_score),
            "new_score": int(new_score),
            "delta": delta,
            "kind": str(kind),
            "reason": _sanitize_reason(reason, self.s, default=kind or "chat"),
        }
        sealed = self.store.vault.seal("affection-hist:" + user_key, payload_data)
        await self.store.db.execute(
            "INSERT INTO affection_history(scope,user_key,delta,payload,created) "
            "VALUES (?,?,?,?,?)",
            (self.scope, user_key, delta, sealed, float(now)),
        )

    async def _migrate_history_key_in_tx(
        self,
        old_key: str,
        new_key: str,
        *,
        person_id: str | None = None,
    ) -> None:
        if old_key == new_key:
            return
        rows = await self.store.all(
            "SELECT id, payload FROM affection_history WHERE scope=? AND user_key=?",
            (self.scope, old_key),
        )
        for row in rows:
            opened = self.store.vault.open("affection-hist:" + old_key, row["payload"])
            if isinstance(opened, dict):
                updated_payload = dict(opened)
                if person_id is not None:
                    updated_payload["person_id"] = person_id
                re_sealed = self.store.vault.seal("affection-hist:" + new_key, updated_payload)
                await self.store.db.execute(
                    "UPDATE affection_history SET user_key=?, payload=? WHERE id=?",
                    (new_key, re_sealed, int(row["id"])),
                )
            else:
                await self.store.db.execute(
                    "DELETE FROM affection_history WHERE id=?",
                    (int(row["id"]),),
                )

    @staticmethod
    def _safety_status(is_owner: bool, violations_30d: int) -> str:
        if is_owner:
            return "owner_immune"
        if violations_30d >= 3:
            return "risk"
        if violations_30d >= 1:
            return "watch"
        return "clean"

    def _build_profile_dict(
        self,
        record: dict[str, Any],
        *,
        person_id: str | None = None,
        violations_30d: int = 0,
    ) -> dict[str, Any]:
        platform = str(record["platform"])
        uid = str(record["user_id"])
        pid = person_id or (str(record["person_id"]) if record.get("person_id") else None)
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        score = clamp_score(int(record["score"]), is_owner=is_owner)
        caps = capabilities_for_score(score, is_owner=is_owner)
        identity = self._get_identity()
        muted = (not is_owner) and (
            (platform == "onebot" and uid in set(self.s.muted_user_ids))
            or (identity is not None and identity.is_muted(platform, uid))
        )
        clean_violations_30d = 0 if is_owner else max(0, int(violations_30d))
        out: dict[str, Any] = {
            "user_id": uid,
            "platform": platform,
            "nickname": record["nickname"],
            "score": score,
            "level": caps.level,
            "lv": caps.lv,
            "tier": caps.tier,
            "tier_label": caps.tier_label,
            "attitude": caps.attitude,
            "is_owner": is_owner,
            "owner_exclusive": caps.owner_exclusive,
            "muted": muted,
            "today_positive_gain": int(record["today_positive_gain"]),
            "daily_positive_cap": self.config.daily_positive_cap,
            "last_reason": record["last_reason"],
            "last_delta": int(record.get("last_delta", 0)),
            "last_negative_at": float(record.get("last_negative_at", 0.0) or 0.0),
            "violations_30d": clean_violations_30d,
            "safety_strikes_total": (
                0 if is_owner else max(0, int(record.get("safety_strikes_total", 0) or 0))
            ),
            "last_violation_at": (
                0.0 if is_owner else float(record.get("last_violation_at", 0.0) or 0.0)
            ),
            "safety_status": self._safety_status(is_owner, clean_violations_30d),
            "updated_at": float(record.get("updated_at", 0.0)),
            "capabilities": caps.to_dict(),
        }
        if pid is not None:
            out["person_id"] = pid
        return out

    async def merge_person_scores(
        self,
        source_person_id: str,
        target_person_id: str,
        *,
        force_zero: bool = False,
    ) -> int:
        await self.initialize()
        identity = self._get_identity()
        is_owner = target_person_id == "owner"
        src_key = self._person_key(source_person_id)
        dst_key = self._person_key(target_person_id)
        now = float(self.clock())
        today = self._today_str(now)
        merged_score: int | None = None

        async with self._lock:
            async with self.store.transaction():
                src_row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                    (src_key, self.scope),
                )
                dst_row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                    (dst_key, self.scope),
                )
                src_data = (
                    self.store.vault.open("affection:" + src_key, src_row["payload"])
                    if src_row
                    else None
                )
                dst_data = (
                    self.store.vault.open("affection:" + dst_key, dst_row["payload"])
                    if dst_row
                    else None
                )
                existing = [d for d in (dst_data, src_data) if isinstance(d, dict) and "score" in d]

                prev_for_merge = (
                    clamp_score(int(dst_data["score"]), is_owner=is_owner)
                    if isinstance(dst_data, dict) and "score" in dst_data
                    else (
                        clamp_score(int(src_data["score"]), is_owner=is_owner)
                        if isinstance(src_data, dict) and "score" in src_data
                        else (120 if is_owner else self.config.initial_score)
                    )
                )

                if is_owner:
                    raw_scores = [clamp_score(int(d["score"]), is_owner=True) for d in existing]
                    merged_score = clamp_score(
                        max(raw_scores) if raw_scores else 120, is_owner=True
                    )
                elif force_zero:
                    merged_score = 0
                elif existing:
                    raw_scores = [clamp_score(int(d["score"]), is_owner=False) for d in existing]
                    merged_score = clamp_score(min(raw_scores), is_owner=False)

                await self._migrate_history_key_in_tx(src_key, dst_key, person_id=target_person_id)

                if merged_score is not None:
                    base = (
                        dst_data
                        if isinstance(dst_data, dict)
                        else (src_data if isinstance(src_data, dict) else {})
                    )
                    rep_plat = str(base.get("platform", ""))
                    rep_uid = str(base.get("user_id", ""))
                    if (not rep_plat or not rep_uid) and identity is not None:
                        linked = identity.linked_accounts_cached(target_person_id)
                        if linked:
                            rep_plat, rep_uid = linked[0]
                    if not rep_plat:
                        rep_plat = "onebot"
                    if not rep_uid:
                        rep_uid = "0"
                    last_negative = max(
                        (float(d.get("last_negative_at", 0.0) or 0.0) for d in existing),
                        default=0.0,
                    )
                    last_recovery_day = max(
                        (str(d.get("last_recovery_day", "") or "") for d in existing),
                        default="",
                    )
                    safety_strikes_total = sum(
                        max(0, int(d.get("safety_strikes_total", 0) or 0)) for d in existing
                    )
                    last_violation_at = max(
                        (float(d.get("last_violation_at", 0.0) or 0.0) for d in existing),
                        default=0.0,
                    )
                    merge_reason = (
                        "merged_blacklist" if force_zero else str(base.get("last_reason", "merged"))
                    )
                    record = self._normalize_record(
                        {
                            **base,
                            "score": merged_score,
                            "last_negative_at": last_negative,
                            "last_recovery_day": last_recovery_day,
                            "safety_strikes_total": safety_strikes_total,
                            "last_violation_at": last_violation_at,
                            "last_reason": merge_reason,
                            "updated_at": now,
                        },
                        rep_plat,
                        rep_uid,
                        "",
                        today,
                        is_owner=is_owner,
                        person_id=target_person_id,
                    )
                    await self._save_user_in_tx(
                        rep_plat, rep_uid, record, person_id=target_person_id
                    )
                    if merged_score != prev_for_merge:
                        await self._append_history_in_tx(
                            user_key=dst_key,
                            platform=rep_plat,
                            user_id=rep_uid,
                            person_id=target_person_id,
                            nickname=record["nickname"],
                            prev_score=prev_for_merge,
                            new_score=merged_score,
                            kind="merge",
                            reason="merged_blacklist" if force_zero else "merged",
                            now=now,
                        )
                    self._update_caches(
                        rep_plat,
                        rep_uid,
                        merged_score,
                        is_owner=is_owner,
                        person_id=target_person_id,
                        identity=identity,
                    )

                if src_row:
                    await self.store.db.execute(
                        "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                        (src_key, self.scope),
                    )
                await self.store.db.execute(
                    "UPDATE affection_events SET user_key=? WHERE scope=? AND user_key=?",
                    (dst_key, self.scope, src_key),
                )

            self._user_scores.pop(src_key, None)
            self._person_scores.pop(source_person_id, None)
            self._annoyance_at.pop(src_key, None)

        return (
            merged_score
            if merged_score is not None
            else (120 if is_owner else self.config.initial_score)
        )

    @staticmethod
    def _clamp_history_limit(limit: int, *, default: int) -> int:
        if isinstance(limit, bool) or not isinstance(limit, int):
            return default
        return max(1, min(200, limit))

    def _open_history_row(self, row: Any) -> dict[str, Any] | None:
        user_key = str(row["user_key"])
        opened = self.store.vault.open("affection-hist:" + user_key, row["payload"])
        if not isinstance(opened, dict):
            return None
        return {
            "id": int(row["id"]),
            "user_id": str(opened.get("user_id", "")),
            "platform": str(opened.get("platform", "")),
            "person_id": str(opened["person_id"]) if opened.get("person_id") else None,
            "nickname": str(opened.get("nickname", "")),
            "prev_score": int(opened.get("prev_score", 0)),
            "new_score": int(opened.get("new_score", 0)),
            "delta": int(opened.get("delta", row["delta"])),
            "kind": str(opened.get("kind", "")),
            "reason": str(opened.get("reason", "")),
            "created_at": float(row["created"]),
        }

    async def user_history(
        self,
        platform: str,
        user_id: str,
        *,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        uid = _validate_platform_user(platform, user_id)
        await self.initialize()
        identity = await self._ensure_identity_ready()
        clamped_limit = self._clamp_history_limit(limit, default=30)
        pid = (
            await identity.resolve_person_id(platform, uid, create=False)
            if identity is not None
            else None
        )
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id, user_key, delta, payload, created FROM affection_history "
                "WHERE scope=? AND user_key=? ORDER BY created DESC, id DESC LIMIT ?",
                (self.scope, user_key, clamped_limit),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._open_history_row(row)
            if item is not None:
                items.append(item)
        return items

    async def recent_history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        await self.initialize()
        await self._ensure_identity_ready()
        clamped_limit = self._clamp_history_limit(limit, default=50)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id, user_key, delta, payload, created FROM affection_history "
                "WHERE scope=? ORDER BY created DESC, id DESC LIMIT ?",
                (self.scope, clamped_limit),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._open_history_row(row)
            if item is not None:
                items.append(item)
        return items

    async def list_users(self) -> list[dict[str, Any]]:
        await self.initialize()
        await self._ensure_identity_ready()
        now = float(self.clock())
        today = self._today_str(now)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT user_key, payload FROM user_affection WHERE scope=? "
                "ORDER BY updated DESC LIMIT 500",
                (self.scope,),
            )
            event_rows = await self.store.all(
                "SELECT user_key, COUNT(*) AS cnt FROM affection_events "
                "WHERE scope=? AND created>=? GROUP BY user_key",
                (self.scope, now - 30 * 86400),
            )
        violations_by_key = {str(er["user_key"]): int(er["cnt"]) for er in event_rows}
        items: list[dict[str, Any]] = []
        for row in rows:
            opened = self.store.vault.open("affection:" + row["user_key"], row["payload"])
            if not isinstance(opened, dict):
                continue
            platform = str(opened.get("platform", "onebot"))
            uid = str(opened.get("user_id", ""))
            pid = str(opened["person_id"]) if opened.get("person_id") else None
            if not uid:
                continue
            is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
            record = self._normalize_record(
                opened, platform, uid, "", today, is_owner=is_owner, person_id=pid
            )
            v30 = 0 if is_owner else violations_by_key.get(str(row["user_key"]), 0)
            items.append(self._build_profile_dict(record, person_id=pid, violations_30d=v30))
        items.sort(
            key=lambda item: (
                0 if item["is_owner"] else 1,
                -float(item.get("updated_at", 0.0)),
            )
        )
        return items

    def status_summary(self) -> dict[str, Any]:
        """Return a fast, non-blocking in-memory summary for system module status."""
        tracked_count = len(self._user_scores)
        blacklisted_count = sum(
            1 for score, is_owner in self._user_scores.values() if not is_owner and score <= 0
        )
        return {
            "initialized": self.initialized,
            "enabled": bool(self.config.enabled),
            "tracked_count": tracked_count,
            "persons_count": len(self._person_scores),
            "blacklisted_count": blacklisted_count,
            "identity_bound": self._identity is not None,
            "migrated_to_person": self._migrated_to_person,
            "source": "admin" if self.overridden else "env",
        }

    async def status(self) -> dict[str, Any]:
        await self.initialize()
        return {
            "config": self.config.model_dump(mode="json"),
            "environment": self.env_config.model_dump(mode="json"),
            "source": "admin" if self.overridden else "env",
            "tiers": tier_matrix_rows(),
            "users": await self.list_users(),
        }
