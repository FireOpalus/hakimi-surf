"""Chinese punctuation and short public replies, without exposing transport tags."""

import re
import unicodedata

_FULLWIDTH_CJK_PUNCT = frozenset("，。！？；：（）、")


def _normalize_prose(segment: str) -> str:
    segment = re.sub(r"[~～∼〜﹏]{3,}", "~", segment)
    segment = "".join(
        ch if ch in _FULLWIDTH_CJK_PUNCT else unicodedata.normalize("NFKC", ch) for ch in segment
    )
    segment = re.sub(r"[ \t\u3000]+", " ", segment)
    return re.sub(r"\n{3,}", "\n\n", segment)


def clean_chinese_punctuation(text: str, *, public=False) -> str:
    # Leave code, URLs and numeric/English comma usage intact.
    pieces = re.split(r"(```[\s\S]*?```|`[^`\n]*`|https?://\S+)", text)
    for index in range(0, len(pieces), 2):
        pieces[index] = re.sub(
            r"(?<=[\u3400-\u9fffA-Za-z0-9]),(?=\s*[\u3400-\u9fff])"
            r"|(?<=[\u3400-\u9fff]),(?=\s*[A-Za-z0-9])",
            "，",
            pieces[index],
        )
    text = "".join(pieces)
    visible = re.sub(r"[\ue000\ue001]", "", text)
    if (
        public
        and len(visible) <= 35
        and text.endswith("。")
        and len(re.findall(r"[。！？!?]", visible)) == 1
    ):
        text = text[:-1]
    return text


def public_reply(text: str) -> str:
    # Keep natural sentences, code and links intact. Only tame repeated prose tildes.
    pieces = re.split(r"(```[\s\S]*?```|`[^`\n]*`|https?://\S+)", text)
    for index in range(0, len(pieces), 2):
        pieces[index] = _normalize_prose(pieces[index])
    return clean_chinese_punctuation("".join(pieces).strip(), public=True)


def casual_reply(text: str) -> str:
    """Format short casual private replies without stiff trailing single-sentence period."""
    return public_reply(text)


def trim_complete_text(text: str, limit: int) -> str:
    """Trim overlong text cleanly at a sentence or clause boundary when over `limit`."""
    cleaned = str(text or "").strip()
    if limit <= 0:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    window = cleaned[:limit]
    min_sentence = max(1, int(limit * 0.4))
    best_sentence = max(
        (window.rfind(sep) for sep in ("。", "！", "？", "!", "?", "~", "\n")), default=-1
    )
    if best_sentence >= min_sentence:
        return window[: best_sentence + 1].rstrip()
    min_clause = max(1, int(limit * 0.45))
    best_clause = max((window.rfind(sep) for sep in ("；", ";", "，", ",", "、", " ")), default=-1)
    if best_clause >= min_clause:
        return window[:best_clause].rstrip() + "…"
    return window[: max(1, limit - 1)].rstrip() + "…"
