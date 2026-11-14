"""Chinese punctuation and short public replies, without exposing transport tags."""

import re
import unicodedata


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
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[~～∼〜﹏]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    sentences = re.findall(r"[^。！？!?]+[。！？!?]*", text)
    text = "".join(sentences[:2]).strip() if sentences else text
    if len(text) > 30:
        text = text[:29].rstrip("，、；：,;:") + "。"
    return clean_chinese_punctuation(text, public=True)
