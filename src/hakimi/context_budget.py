"""Offline sizing, not a claim to reproduce Google's proprietary tokenizer.

Reservations still use the UTF-8 byte upper bound. Watermarks use a deterministic
text estimate plus an independent hard byte limit; provider usage is authoritative.
"""

import math
import re


def estimate_tokens(text: str) -> int:
    # CJK/punctuation are usually individual tokens; Latin runs average ~4 bytes.
    wide = sum(ord(c) > 127 for c in text)
    ascii_text = "".join(c if ord(c) < 128 else " " for c in text)
    return wide + sum(max(1, math.ceil(len(t) / 4)) for t in re.findall(r"\w+|[^\w\s]", ascii_text))


def truncate_tokens(text: str, limit: int) -> str:
    if estimate_tokens(text) <= limit:
        return text
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if estimate_tokens(text[:mid] + "…[节选]") <= limit:
            lo = mid
        else:
            hi = mid - 1
    return text[:lo] + "…[节选]"
