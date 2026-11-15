"""In-memory long-text fallback; bounded canvas, no files or external rendering."""

import base64
import io
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


def render_card(text):
    paths = [
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    ]
    font_path = next((p for p in paths if Path(p).is_file()), None)
    font = ImageFont.truetype(font_path, 24) if font_path else ImageFont.load_default(size=24)
    lines = []
    for paragraph in text[:6000].splitlines():
        line = ""
        for char in paragraph:
            if font.getlength(line + char) > 820:
                lines.append(line)
                line = ""
            line += char
        lines.append(line)
    lines = lines[:180]
    image = Image.new("RGB", (900, 80 + 34 * len(lines)), "#f9f7fd")
    draw = ImageDraw.Draw(image)
    for index, line in enumerate(lines):
        draw.text((40, 40 + index * 34), line, font=font, fill="#282238")
    target = io.BytesIO()
    image.save(target, format="JPEG", quality=85)
    return "base64://" + base64.b64encode(target.getvalue()).decode("ascii")
