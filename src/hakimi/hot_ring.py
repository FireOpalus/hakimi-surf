"""Ephemeral, scope-bound message/revocation ring. No user content is logged."""

import time
from collections import OrderedDict
from dataclasses import dataclass, field


@dataclass
class Ring:
    messages: OrderedDict = field(default_factory=OrderedDict)
    recalls: OrderedDict = field(default_factory=OrderedDict)
    sequence: int = 0
    size: int = 0


class HotRing:
    def __init__(self, *, groups=64, byte_limit=8 * 1024 * 1024):
        self.groups, self.byte_limit = groups, byte_limit
        self.rings = OrderedDict()

    @staticmethod
    def key(message):
        return (message.platform, message.bot_id, message.kind, message.conversation_id)

    def ring(self, message):
        key = self.key(message)
        if key not in self.rings:
            self.rings[key] = Ring()
        self.rings.move_to_end(key)
        while len(self.rings) > self.groups:
            self.rings.popitem(last=False)
        return self.rings[key]

    def observe(self, message):
        ring = self.ring(message)
        if message.event_id in ring.messages:
            return ring.sequence
        ring.sequence += 1
        size = len(message.model_dump_json().encode())
        ring.messages[message.event_id] = (ring.sequence, message, size)
        ring.size += size
        while len(ring.messages) > 200:
            _, (_, _, old_size) = ring.messages.popitem(last=False)
            ring.size -= old_size
        while sum(r.size for r in self.rings.values()) > self.byte_limit:
            oldest = next(iter(self.rings.values()))
            if oldest.messages:
                _, (_, _, old_size) = oldest.messages.popitem(last=False)
                oldest.size -= old_size
            else:
                self.rings.popitem(last=False)
        return ring.sequence

    def clear(self, message):
        self.rings.pop(self.key(message), None)

    def lookup(self, message, event_id):
        ring = self.rings.get(self.key(message))
        if not ring or self.recalled(message, event_id):
            return None
        row = ring.messages.get(str(event_id))
        return row[1] if row else None

    def recall(self, message, event_id):
        ring = self.ring(message)
        ring.recalls[str(event_id)] = time.monotonic()
        while len(ring.recalls) > 400:
            ring.recalls.popitem(last=False)

    def recalled(self, message, event_id=None):
        ring = self.rings.get(self.key(message))
        return bool(ring and str(event_id or message.event_id) in ring.recalls)

    def watermark(self, message):
        ring = self.rings.get(self.key(message))
        return ring.sequence if ring else 0

    def intervening(self, message, watermark, participants):
        ring = self.rings.get(self.key(message))
        return (
            sum(
                seq > watermark and item.user_id not in participants and item.user_id != item.bot_id
                for seq, item, _ in ring.messages.values()
            )
            if ring
            else 0
        )
