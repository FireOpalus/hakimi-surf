"""Daily generation, outbox and OneBot receipts. Every _tx method uses its caller's transaction."""

from .modules.actions import Outcome

# The encrypted body moves from the generation receipt to the outbox. The projection
# never selects either ciphertext, the OneBot receipt, or anything from private memory.
_HISTORY = """
SELECT s.id,s.day,s.status AS generation_state,s.reason AS generation_reason,s.expires_at,
       s.due_at AS generation_started_at,r.generated_at,r.submitted_at,
       CASE WHEN p.reason='expired_before_queue' THEN NULL ELSE p.created END AS queued_at,
       p.sent_at AS confirmed_at,
       p.status AS delivery_state,p.reason AS delivery_reason,
       (SELECT i.intent_id FROM owner_digest_intents i WHERE i.digest_id=s.id
        ORDER BY i.scheduled_for,i.rowid LIMIT 1) AS intent_id,
       (SELECT MIN(i.scheduled_for) FROM owner_digest_intents i
        WHERE i.digest_id=s.id) AS scheduled_for
FROM proactive_schedule s
LEFT JOIN owner_digest_receipts r ON r.id=s.id
LEFT JOIN pending_owner_dms p ON p.id=s.id AND p.scope=s.scope AND p.kind='digest'
WHERE s.kind='digest' AND s.scope=?
"""


def project(row, *, connected, qq=True):
    item = dict(row)
    generation = item.pop("generation_state")
    delivery = item.pop("delivery_state")
    generation_reason = item.pop("generation_reason")
    reason = item.pop("delivery_reason") or generation_reason
    if delivery in {"sent", "delivered"}:
        stage = "confirmed"
    elif delivery == "pending":
        stage = "queued" if connected else ("awaiting_qq" if qq else "awaiting_channel")
    elif delivery == "sending":
        stage = "submitting"
    elif delivery == "expired" or reason in {"outside_digest_window", "expired"}:
        stage = "expired"
    elif reason == "generation_interrupted":
        stage = "generation_uncertain"
    elif delivery == "uncertain":
        stage = "uncertain"
    elif delivery in {"cancelled", "skipped"} or generation == "cancelled":
        stage = "cancelled"
    elif generation == "sent":
        # Older databases may have already pruned the confirmed outbox row.
        stage = "confirmed"
    elif generation in {"generating", "generated", "failed"}:
        stage = generation
    else:
        stage = "uncertain"
    return {**item, "stage": stage, "reason": reason}


def world_outcome(stage, *, qq=True):
    if stage == "confirmed":
        return Outcome("done", "owner_digest_qq_confirmed" if qq else "owner_digest_confirmed")
    if stage == "expired":
        return Outcome("expired", "owner_digest_expired")
    if stage in {"failed", "cancelled"}:
        return Outcome("gated_skip", "owner_digest_" + stage)
    if stage in {"uncertain", "generation_uncertain"}:
        return Outcome("uncertain", "owner_digest_" + stage)
    if stage == "generating":
        return Outcome("executing", "owner_digest_generating")
    return Outcome("executing", "owner_digest_awaiting_delivery")


class DigestReceipts:
    def __init__(self, store, scope, owner, *, action_name="qq.owner_digest", target_scope=None):
        self.store, self.scope, self.target_scope = store, scope, target_scope or "owner:" + owner
        self.action_name = action_name

    async def history(self, *, connected):
        async with self.store.lock:
            rows = await self.store.all(_HISTORY + " ORDER BY s.day DESC LIMIT 8", (self.scope,))
        return [
            project(row, connected=connected, qq=self.action_name == "qq.owner_digest")
            for row in rows
        ]

    async def get_tx(self, key, *, connected=True):
        row = await self.store.one(_HISTORY + " AND s.id=?", (self.scope, key))
        return (
            project(row, connected=connected, qq=self.action_name == "qq.owner_digest")
            if row
            else None
        )

    async def link_tx(self, key, intent_id):
        if intent_id:
            await self.store.db.execute(
                "INSERT OR IGNORE INTO owner_digest_intents(intent_id,digest_id,scheduled_for) "
                "SELECT id,?,execute_after FROM scheduled_intents WHERE id=? AND target_scope=? "
                "AND action_name=? AND origin='timer'",
                (key, intent_id, self.target_scope, self.action_name),
            )

    async def sync_intents_tx(self, key, now):
        receipt = await self.get_tx(key)
        if not receipt or not receipt["intent_id"]:
            return
        result = world_outcome(receipt["stage"], qq=self.action_name == "qq.owner_digest")
        await self.store.db.execute(
            "UPDATE scheduled_intents SET status=?,result_reason=?,updated_at=? "
            "WHERE id IN (SELECT intent_id FROM owner_digest_intents WHERE digest_id=?) "
            "AND (status!=? OR result_reason!=?)",
            (result.status, result.reason, now, key, result.status, result.reason),
        )

    async def recover_links_tx(self, now):
        # Before this release, the world ticket became 'done' as soon as the
        # outbox accepted it. Only match a real same-day timer and its exact owner.
        if not await self.store.one(
            "SELECT 1 FROM sqlite_master WHERE name='scheduled_intents' AND type='table'"
        ):
            return
        await self.store.db.execute(
            "INSERT OR IGNORE INTO owner_digest_intents(intent_id,digest_id,scheduled_for) "
            "SELECT w.id,s.id,w.execute_after FROM scheduled_intents w JOIN proactive_schedule s "
            "ON w.dedupe_key LIKE 'timer:'||?||':'||s.day||':%' "
            "WHERE w.action_name=? AND w.origin='timer' AND w.target_scope=? "
            "AND s.kind='digest' AND s.scope=? AND (w.status='executing' OR "
            "(w.status='done' AND w.result_reason='owner_digest_queued'))",
            (self.action_name, self.action_name, self.target_scope, self.scope),
        )
        rows = await self.store.all(
            "SELECT DISTINCT s.id FROM proactive_schedule s JOIN owner_digest_intents i "
            "ON i.digest_id=s.id WHERE s.scope=?",
            (self.scope,),
        )
        for row in rows:
            await self.sync_intents_tx(row["id"], now)

    async def enqueue_generated_tx(self, now, key=None):
        """Replay a completed generation into its one outbox slot, including after restart."""
        rows = await self.store.all(
            "SELECT s.id,s.scope,s.day,s.expires_at,r.payload FROM proactive_schedule s "
            "JOIN owner_digest_receipts r ON r.id=s.id WHERE s.scope=? AND s.kind='digest' "
            "AND s.status='generated' AND r.payload IS NOT NULL" + (" AND s.id=?" if key else ""),
            (self.scope, key) if key else (self.scope,),
        )
        for row in rows:
            expired = row["expires_at"] <= now
            await self.store.db.execute(
                "INSERT OR IGNORE INTO pending_owner_dms "
                "(id,scope,day,kind,payload,created,expires_at,status,reason) "
                "VALUES (?,?,?,'digest',?,?,?,?,?)",
                (
                    row["id"],
                    row["scope"],
                    row["day"],
                    None if expired else row["payload"],
                    now,
                    row["expires_at"],
                    "expired" if expired else "pending",
                    "expired_before_queue" if expired else None,
                ),
            )
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='queued' WHERE id=?", (row["id"],)
            )
            await self.store.db.execute(
                "UPDATE owner_digest_receipts SET payload=NULL WHERE id=?", (row["id"],)
            )
            await self.sync_intents_tx(row["id"], now)
