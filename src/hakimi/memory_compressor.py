"""Background, bounded compression with persisted attempts and optimistic commits."""

import asyncio
import json
import re
import time
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .bot_server import is_persona_owner, untrusted_input
from .dual_memory import EXTRACTION_INSTRUCTION, parse_extraction
from .generation_policy import Scenario
from .security import Rejected, configured_secrets
from .work_usage import model_work

MAX_RATE_LIMIT_RETRIES = 2
RATE_LIMIT_BACKOFF_SECONDS = 60.0


def check_memory_text(text, settings):
    if any(
        secret and len(secret) >= 8 and secret in text for secret in configured_secrets(settings)
    ):
        raise Rejected("credential_in_memory")
    if re.search(
        r"(?:api[_ -]?key|password|passwd|token|secret|密钥|密码)\s*[:=：]\s*\S{6,}|"
        r"-----BEGIN .*PRIVATE KEY-----|\bsk-[A-Za-z0-9]{16,}",
        text,
        re.I,
    ):
        raise Rejected("credential_in_memory")


class MemoryCompressor:
    def __init__(
        self, settings, sessions, gateway, store, *, clock=time.monotonic, wall_clock=time.time
    ):
        self.s, self.sessions, self.ai, self.store = settings, sessions, gateway, store
        self.tasks = {}
        self.locks = [asyncio.Lock() for _ in range(32)]
        self.slots = asyncio.Semaphore(1)
        self.wakeup = asyncio.Event()
        self.clock = clock
        self.wall_clock = wall_clock
        self.slice_backoff: OrderedDict[str, tuple[int, float]] = OrderedDict()

    def schedule(self, message):
        if self.s.prefix_history_enabled:
            self.wakeup.set()
            return
        key = self.sessions.key(message)
        if not self.store.runtime.compression.enabled or key in self.tasks or len(self.tasks) >= 32:
            return
        task = asyncio.create_task(self.compress(message), name="memory-compression")
        self.tasks[key] = task

        def finished(done):
            self.tasks.pop(key, None)
            if not done.cancelled() and done.exception() is not None:
                self.store.runtime.sink.pending.append("compression_failed")

        task.add_done_callback(finished)

    async def close(self):
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def validate_summary(self, text, subject, *, lane="admin"):
        if len(text) > 800:
            raise Rejected("summary_too_long")
        check_memory_text(text, self.s)
        if text:
            with model_work("memory.summary_edit", independent=True):
                await self.ai.screen(
                    {"proposed_memory_summary": text},
                    subject,
                    purpose="source",
                    budget_lane=lane,
                    audit_source="memory",
                )
        return text

    async def compress(self, message, *, force=False):
        if self.s.prefix_history_enabled:
            if not force:
                self.wakeup.set()
                return {"status": "queued"}
            return await self.extract(message, force=True)
        lock = self.locks[int(self.sessions.key(message)[:8], 16) % len(self.locks)]
        if lock.locked():
            return {"status": "busy"}
        async with lock, self.slots:
            snapshot = await self.sessions.compression_snapshot(message, force=force)
            if snapshot is None:
                return {"status": "not_due_or_cooling_down"}
            payload = {
                "task": "compress_memory",
                "surface": message.kind,
                "owner_private": message.kind == "private" and is_persona_owner(message, self.s),
                "rolling_summary": snapshot["old_summary"],
                "recent_messages": snapshot["messages"],
            }
            # v0.9 folds contain up to half the 28k history. The single-message
            # ingress cap is not a cap on this already-screened background batch.
            limit = (
                self.s.history_max_bytes
                if self.s.prefix_history_enabled
                else self.s.max_context_bytes
            )
            if self.s.prefix_history_enabled:
                payload["recent_messages"] = [
                    {k: item[k] for k in ("role", "speaker", "text") if k in item}
                    for item in snapshot["messages"]
                ]
            else:
                for item in payload["recent_messages"]:
                    item["text"] = item["text"][:1000]
            while len(untrusted_input(payload).encode()) > limit:
                longest = max(payload["recent_messages"], key=lambda item: len(item["text"]))
                if len(longest["text"]) < 30:
                    return {"status": "context_too_large"}
                longest["text"] = longest["text"][: len(longest["text"]) // 2]
                payload["excerpts_truncated"] = True
            subject = self.store.vault.digest("compression-budget", self.sessions.key(message))
            try:
                check_memory_text(str(payload), self.s)
                with model_work("memory.compress", snapshot["attempt_id"], independent=True):
                    await self.ai.screen(
                        payload,
                        subject,
                        purpose="source",
                        budget_lane="memory",
                        audit_source="memory",
                    )
                    summary = await self.ai.generate(
                        payload,
                        subject,
                        scenario=Scenario.MEMORY_COMPRESSION,
                        budget_lane="memory",
                        audit_source="memory",
                    )
                summary = summary.strip()[:800]
                check_memory_text(summary, self.s)
                # generate already screens the output; no additional redundant paid call.
                committed = await self.sessions.commit_compression(message, snapshot, summary)
                if not committed:
                    await self.store.runtime.event("compression_stale", "memory")
                return {
                    "status": "compressed" if committed else "stale",
                    "compressed_messages": len(snapshot["ids"]) if committed else 0,
                }
            except asyncio.CancelledError:
                raise
            except Exception:
                await self.store.runtime.event("compression_failed", "memory")
                return {"status": "failed"}

    def _estimate_extract_reserve(self, payload_bytes: int) -> Decimal:
        decision_in = self.s.decision_input_usd_per_m
        flex_in = self.s.chat_input_usd_per_m / 2
        flex_out = self.s.chat_output_usd_per_m / 2
        system_bytes = len(EXTRACTION_INSTRUCTION.encode("utf-8"))
        source_reserve = ((payload_bytes + 4096) * decision_in) / 1_000_000
        flex_reserve = (
            (payload_bytes + system_bytes + 4096) * flex_in + 4096 * flex_out
        ) / 1_000_000
        output_reserve = (20098 * decision_in) / 1_000_000
        return source_reserve + flex_reserve + output_reserve

    async def extract(self, message, *, force=False, background=False):
        lock = self.locks[int(self.sessions.key(message)[:8], 16) % len(self.locks)]
        if lock.locked():
            return {"status": "busy"}
        async with lock, self.slots:
            if not force and not self.store.runtime.compression.enabled:
                return {"status": "disabled"}
            subject = self.store.vault.digest("compression-budget", self.sessions.key(message))
            # Do not snapshot/claim a slice or spend decision calls unless memory lane can pay.
            if (await self.store.budget_status())["hard_stop"] or not await self.store.can_reserve(
                subject, "memory", self._estimate_extract_reserve(0)
            ):
                return {"status": "budget_hard_stop"}
            if force:
                await self.sessions.dual.snapshot_cold(message, force=True)
            candidate = await self.sessions.dual.peek(
                message, background=background and not force, now=self.wall_clock()
            )
            if candidate is None:
                return {"status": "not_due_or_cooling_down"}
            if candidate.get("status") == "provider_backoff":
                return candidate
            backoff_entry = self.slice_backoff.get(candidate["id"])
            if background and not force and backoff_entry and self.clock() < backoff_entry[1]:
                return {"status": "provider_backoff"}
            known = await self.sessions.dual.view(message)
            payload = {
                "known_chronicles": [
                    {"topic": e.get("topic", ""), "text": e["text"][:250]}
                    for e in known["entries"]
                    if e["kind"] == "chronicle" and not e["deleted"]
                ][-24:],
                "task": "dual_memory",
                "surface": message.kind,
                "recent_messages": candidate["messages"],
                "rolling_summary": candidate["old_summary"],
            }
            payload_bytes = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            if not await self.store.can_reserve(
                subject, "memory", self._estimate_extract_reserve(payload_bytes)
            ):
                return {"status": "budget_hard_stop"}
            snapshot = await self.sessions.dual.claim(
                message,
                background=background and not force,
                expected_ids=candidate["ids"],
                now=self.wall_clock(),
            )
            if snapshot is None:
                return {"status": "not_due_or_cooling_down"}
            call_stage = 0

            async def guard():
                nonlocal call_stage
                async with self.sessions.database(message, preserve_history=True) as (db, _):
                    async with db.execute("SELECT epoch,version FROM rolling_state") as cur:
                        if await cur.fetchone() != (snapshot["epoch"], snapshot["version"]):
                            raise Rejected("compression_stale")
                    async with db.execute("SELECT enabled FROM session_meta") as cur:
                        if not (await cur.fetchone())[0]:
                            raise Rejected("compression_stale")
                if not force and not self.store.runtime.compression.enabled:
                    raise Rejected("compression_stale")
                call_stage += 1

            try:
                check_memory_text(str(payload), self.s)
                with model_work("memory.extract", snapshot["batch_id"], independent=True):
                    await self.ai.screen(
                        payload,
                        subject,
                        purpose="source",
                        budget_lane="memory",
                        audit_source="memory",
                    )
                    result = await self.ai.generate(
                        payload,
                        subject,
                        scenario=Scenario.MEMORY_COMPRESSION,
                        budget_lane="memory",
                        audit_source="memory",
                        memory_archive=True,
                        before_call=guard,
                    )
                check_memory_text(result, self.s)
                extraction = parse_extraction(result)
                committed = await self.sessions.dual.commit(message, snapshot, extraction)
            except asyncio.CancelledError:
                # A dispatched archive may have been billed. Keep its slices out of the retry queue.
                if await self.sessions.dual.finish(
                    message, snapshot, "unconfirmed", error="archive_interrupted"
                ):
                    await self.store.runtime.event("memory_archive_unconfirmed", "memory")
                raise
            except Exception as exc:
                if isinstance(exc, Rejected) and exc.detail == "flex_capacity_busy":
                    self.slice_backoff.pop(snapshot["id"], None)
                    result = await self.sessions.dual.defer_capacity(
                        message, snapshot, now=self.wall_clock()
                    )
                    code = {
                        "provider_backoff": "memory_flex_retry_scheduled",
                        "failed": "memory_flex_retry_exhausted",
                        "stale": "compression_stale",
                    }[result["status"]]
                    attempts = list(exc.provider_diagnostics)
                    if attempts and result["status"] == "provider_backoff":
                        attempts[-1] = {
                            **attempts[-1],
                            "retry_number": result["retry_number"],
                            "next_retry_at": result["next_retry_at"],
                        }
                    await self.store.runtime.event(code, "memory", diagnostics=attempts)
                    return result
                if isinstance(exc, Rejected) and str(exc) == "compression_stale":
                    await self.sessions.dual.finish(message, snapshot, "compression_stale")
                    await self.store.runtime.event("compression_stale", "memory")
                    return {"status": "stale"}
                if (
                    call_stage < 2
                    and isinstance(exc, Rejected)
                    and exc.args
                    and exc.args[0]
                    in {"budget_exhausted", "budget_circuit_open", "gemini_circuit_open"}
                ):
                    await self.sessions.dual.requeue(message, snapshot)
                    return {
                        "status": "provider_backoff"
                        if exc.args[0] == "gemini_circuit_open"
                        else "budget_hard_stop"
                    }
                if (
                    isinstance(exc, Rejected)
                    and str(exc) == "gemini_unavailable"
                    and exc.detail == "rate_limited"
                ):
                    prior = self.slice_backoff.get(snapshot["id"], (0, 0.0))[0]
                    if prior < MAX_RATE_LIMIT_RETRIES:
                        attempts = prior + 1
                        self.slice_backoff[snapshot["id"]] = (
                            attempts,
                            self.clock() + RATE_LIMIT_BACKOFF_SECONDS * attempts,
                        )
                        self.slice_backoff.move_to_end(snapshot["id"])
                        while len(self.slice_backoff) > 256:
                            self.slice_backoff.popitem(last=False)
                        await self.sessions.dual.requeue(message, snapshot)
                        return {"status": "provider_backoff"}
                self.slice_backoff.pop(snapshot["id"], None)
                err_code = (
                    str(exc.args[0])
                    if isinstance(exc, Rejected) and exc.args
                    else type(exc).__name__
                )
                await self.sessions.dual.finish(message, snapshot, "failed", error=err_code)
                await self.store.runtime.event("compression_failed", "memory")
                return {"status": "failed"}

            if committed:
                self.slice_backoff.pop(snapshot["id"], None)
                if snapshot["flex_failures"]:
                    await self.store.runtime.event("memory_flex_retry_succeeded", "memory")
            else:
                await self.sessions.dual.finish(message, snapshot, "compression_stale")
                await self.store.runtime.event("compression_stale", "memory")
            return {
                "status": "compressed" if committed else "stale",
                "compressed_messages": len(snapshot["messages"]) if committed else 0,
                "batch_id": snapshot["batch_id"],
            }

    async def nightly(self, now=None):
        now = now or datetime.now(ZoneInfo(self.s.bot_timezone))
        if now.strftime("%H:%M") < self.s.memory_archive_time:
            return
        day = now.date().isoformat()
        for _, message in await self.store.runtime.sessions():
            async with self.sessions.database(message, preserve_history=True) as (db, _):
                async with db.execute("SELECT last_nightly FROM memory_control") as cur:
                    done = (await cur.fetchone())[0] == day
            if done:
                continue
            await self.sessions.dual.snapshot_cold(message, before=now.timestamp() - 86400)
            async with self.sessions.database(message, preserve_history=True) as (db, _):
                await db.execute("UPDATE memory_control SET last_nightly=?", (day,))

    def status_summary(self) -> dict[str, object]:
        """Return a pure in-memory status summary without I/O or cloud access."""
        runtime = getattr(self.store, "runtime", None)
        comp_cfg = getattr(runtime, "compression", None) if runtime is not None else None
        compression_enabled = bool(getattr(comp_cfg, "enabled", True))
        now_mono = self.clock()
        backoff_slices = sum(1 for _, until in self.slice_backoff.values() if until > now_mono)
        return {
            "prefix_history_enabled": bool(self.s.prefix_history_enabled),
            "compression_enabled": compression_enabled,
            "active_tasks": len(self.tasks),
            "backoff_slices": backoff_slices,
            "archive_time": str(self.s.memory_archive_time),
        }

    async def run(self):
        # This worker polls local SQLite only. The LLM runs only for a durable unattempted slice.
        last_scan = float("-inf")
        import time

        while True:
            try:
                if self.s.prefix_history_enabled and self.store.runtime.compression.enabled:
                    if time.monotonic() - last_scan >= 3600:
                        await self.nightly()
                        last_scan = time.monotonic()
                    for _, message in await self.store.runtime.sessions():
                        await self.extract(message, background=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                await self.store.runtime.event("memory_worker_failed", "memory")
            self.wakeup.clear()
            try:
                await asyncio.wait_for(self.wakeup.wait(), timeout=60)
            except TimeoutError:
                pass
