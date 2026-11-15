"""Chinese punctuation and short public replies, without exposing transport tags."""

import re


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
        pieces[index] = re.sub(r"~{3,}", "~", pieces[index])
    return clean_chinese_punctuation("".join(pieces).strip(), public=True)
