"""Background, bounded compression with persisted attempts and optimistic commits."""

import asyncio
import re

from .bot_server import is_persona_owner, untrusted_input
from .generation_policy import Scenario
from .security import Rejected, configured_secrets


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
    def __init__(self, settings, sessions, gateway, store):
        self.s, self.sessions, self.ai, self.store = settings, sessions, gateway, store
        self.tasks = {}
        self.locks = [asyncio.Lock() for _ in range(32)]
        self.slots = asyncio.Semaphore(1)

    def schedule(self, message):
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
            await self.ai.screen(
                {"proposed_memory_summary": text},
                subject,
                purpose="source",
                budget_lane=lane,
                audit_source="memory",
            )
        return text

    async def compress(self, message, *, force=False):
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
            # Bound very long legacy turns, preserving chronology. Explicitly flag omitted text.
            for item in payload["recent_messages"]:
                item["text"] = item["text"][:1000]
            while len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
                longest = max(payload["recent_messages"], key=lambda item: len(item["text"]))
                if len(longest["text"]) < 30:
                    return {"status": "context_too_large"}
                longest["text"] = longest["text"][: len(longest["text"]) // 2]
                payload["excerpts_truncated"] = True
            subject = self.store.vault.digest("compression-budget", self.sessions.key(message))
            try:
                check_memory_text(str(payload), self.s)
                await self.ai.screen(
                    payload, subject, purpose="source", budget_lane="memory", audit_source="memory"
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
