"""In-memory Markdown/code/table card renderer using Pillow; no browser or disk files."""

import base64
import io
import re
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

CARD_WIDTH = 960
MAX_CARD_HEIGHT = 4400
MAX_CARD_BYTES = 4_500_000
MAX_INPUT_CHARS = 8000
TABLE_SEP_RE = re.compile(r"^\s*\|?(?:\s*:?-{2,}:?\s*\|)+\s*:?-{2,}:?\s*\|?\s*$")
FENCE_START_RE = re.compile(r"^\s*```([A-Za-z0-9_+#.\-]*)\s*$")
FENCE_END_RE = re.compile(r"^\s*```\s*$")
CODE_TRIGGER_RE = re.compile(r"```[A-Za-z0-9_+#.\-]*\r?\n([\s\S]*?)```")

CJK_FONT_PATHS = (
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
)
MONO_FONT_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    *CJK_FONT_PATHS,
)


def should_render_rich_card(text: str, mode: str = "rich_auto") -> bool:
    """Return True when group reply contains rich code blocks or GFM tables under rich_auto."""
    if mode != "rich_auto" or not isinstance(text, str) or not text.strip():
        return False
    for match in CODE_TRIGGER_RE.finditer(text):
        body = match.group(1)
        if len(body.splitlines()) >= 4 or len(body.strip()) >= 100:
            return True
    lines = text.splitlines()
    for idx in range(len(lines) - 2):
        if "|" in lines[idx] and TABLE_SEP_RE.match(lines[idx + 1]) and "|" in lines[idx + 2]:
            headers = _split_table_row(lines[idx])
            if len(headers) >= 2:
                return True
    return False


def _load_font(paths: tuple[str, ...], size: int) -> ImageFont.ImageFont:
    chosen = next((p for p in paths if Path(p).is_file()), None)
    return ImageFont.truetype(chosen, size) if chosen else ImageFont.load_default(size=size)


def _split_runs(text: str, font_ascii, font_cjk) -> list[tuple[str, object]]:
    if font_ascii is font_cjk:
        return [(text, font_cjk)] if text else []
    return [
        (part, font_ascii if part.isascii() else font_cjk)
        for part in re.findall(r"[\x00-\x7f]+|[^\x00-\x7f]+", text)
    ]


def _mixed_width(text: str, font_ascii, font_cjk) -> float:
    return sum(font.getlength(part) for part, font in _split_runs(text, font_ascii, font_cjk))


def _draw_mixed(draw, xy: tuple[float, float], text: str, font_ascii, font_cjk, fill: str):
    x, y = xy
    if font_ascii is font_cjk:
        draw.text((x, y), text, font=font_cjk, fill=fill)
        return
    base_asc, _ = font_cjk.getmetrics()
    ascii_asc, _ = font_ascii.getmetrics()
    for part, font in _split_runs(text, font_ascii, font_cjk):
        offset_y = (base_asc - ascii_asc) if font is font_ascii else 0
        draw.text((x, y + offset_y), part, font=font, fill=fill)
        x += font.getlength(part)


def _wrap_line(text: str, max_width: float, font_ascii, font_cjk) -> list[str]:
    if not text:
        return [""]
    lines: list[str] = []
    cur = ""
    for ch in text:
        if cur and _mixed_width(cur + ch, font_ascii, font_cjk) > max_width:
            lines.append(cur)
            cur = ch
        else:
            cur += ch
    lines.append(cur)
    return lines


def _split_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|") and not stripped.endswith("\\|"):
        stripped = stripped[:-1]
    cells = re.split(r"(?<!\\)\|", stripped)
    return [c.replace("\\|", "|").strip() for c in cells]


def _tokenize_inline(text: str) -> list[tuple[str, str]]:
    """Return list of (kind: 'code'|'bold'|'text', content)."""
    tokens: list[tuple[str, str]] = []
    pattern = re.compile(r"`([^`\n]+)`|\*\*([^*\n]+)\*\*")
    pos = 0
    for match in pattern.finditer(text):
        if match.start() > pos:
            tokens.append(("text", text[pos : match.start()]))
        if match.group(1) is not None:
            tokens.append(("code", match.group(1)))
        elif match.group(2) is not None:
            tokens.append(("bold", match.group(2)))
        pos = match.end()
    if pos < len(text):
        tokens.append(("text", text[pos:]))
    return tokens or [("text", "")]


def _wrap_rich_lines(
    text: str, max_width: float, font_text, font_bold, font_code_ascii, font_code_cjk
) -> list[list[tuple[str, str, float]]]:
    """Wrap inline tokens into visual lines of (kind, segment_text, pixel_width)."""
    lines: list[list[tuple[str, str, float]]] = [[]]
    cur_width = 0.0

    def seg_width(kind: str, chunk: str) -> float:
        if kind == "code":
            return _mixed_width(chunk, font_code_ascii, font_code_cjk) + 12
        font = font_bold if kind == "bold" else font_text
        return font.getlength(chunk)

    for para_idx, para in enumerate(text.splitlines() or [""]):
        if para_idx > 0:
            lines.append([])
            cur_width = 0.0
        for kind, content in _tokenize_inline(para):
            if kind == "code":
                total_w = seg_width("code", content)
                if cur_width and cur_width + total_w > max_width:
                    lines.append([])
                    cur_width = 0.0
                if total_w <= max_width:
                    lines[-1].append(("code", content, total_w))
                    cur_width += total_w
                    continue
                for wrapped in _wrap_line(
                    content, max(80.0, max_width - 14), font_code_ascii, font_code_cjk
                ):
                    w = seg_width("code", wrapped)
                    if lines[-1]:
                        lines.append([])
                    lines[-1].append(("code", wrapped, w))
                    cur_width = w
                continue
            buf = ""
            font = font_bold if kind == "bold" else font_text
            for ch in content:
                cw = font.getlength(ch)
                if cur_width + font.getlength(buf) + cw > max_width and (buf or lines[-1]):
                    if buf:
                        lines[-1].append((kind, buf, font.getlength(buf)))
                    lines.append([])
                    cur_width = 0.0
                    buf = ch
                else:
                    buf += ch
            if buf:
                bw = font.getlength(buf)
                lines[-1].append((kind, buf, bw))
                cur_width += bw
    return lines


def _parse_blocks(text: str) -> list[tuple]:
    lines = text[:MAX_INPUT_CHARS].replace("\r\n", "\n").split("\n")
    blocks: list[tuple] = []
    para_buf: list[str] = []
    i, code_lines_used = 0, 0

    def flush_para():
        nonlocal para_buf
        if para_buf:
            blocks.append(("para", "\n".join(para_buf)))
            para_buf = []

    while i < len(lines):
        line = lines[i]
        fence = FENCE_START_RE.match(line)
        if fence:
            flush_para()
            lang = fence.group(1) or "code"
            i += 1
            code_lines: list[str] = []
            while i < len(lines) and not FENCE_END_RE.match(lines[i]):
                if code_lines_used < 120:
                    code_lines.append(lines[i])
                    code_lines_used += 1
                i += 1
            if i < len(lines) and FENCE_END_RE.match(lines[i]):
                i += 1
            blocks.append(("code", lang[:24], code_lines or [""]))
            continue

        if i + 2 < len(lines) and "|" in line and TABLE_SEP_RE.match(lines[i + 1]):
            headers = _split_table_row(line)[:8]
            if len(headers) >= 2:
                flush_para()
                ncols = len(headers)
                rows = [headers]
                i += 2
                while i < len(lines) and "|" in lines[i] and len(rows) < 41:
                    row = _split_table_row(lines[i])[:ncols]
                    while len(row) < ncols:
                        row.append("")
                    rows.append(row)
                    i += 1
                blocks.append(("table", rows))
                continue

        heading = re.match(r"^(#{1,3})\s+(.+)$", line)
        if heading:
            flush_para()
            blocks.append(("heading", len(heading.group(1)), heading.group(2).strip()))
            i += 1
            continue

        if line.lstrip().startswith(">"):
            flush_para()
            quotes: list[str] = []
            while i < len(lines) and lines[i].lstrip().startswith(">"):
                quotes.append(re.sub(r"^\s*>\s?", "", lines[i]))
                i += 1
            blocks.append(("quote", "\n".join(quotes)))
            continue

        lst = re.match(r"^\s*(?:[-*]|(\d+\.))\s+(.+)$", line)
        if lst:
            flush_para()
            marker = lst.group(1) or "•"
            blocks.append(("list", marker, lst.group(2).strip()))
            i += 1
            continue

        if not line.strip():
            flush_para()
            i += 1
            continue

        para_buf.append(line)
        i += 1
    flush_para()
    return blocks or [("para", "")]


def render_card(text: str) -> str:
    """Render plain text or Markdown (headings, code blocks, GFM tables) to JPEG base64."""
    text = str(text or "")[:MAX_INPUT_CHARS]
    font_body = _load_font(CJK_FONT_PATHS, 22)
    font_small = _load_font(CJK_FONT_PATHS, 17)
    font_h1 = _load_font(CJK_FONT_PATHS, 29)
    font_h2 = _load_font(CJK_FONT_PATHS, 25)
    font_h3 = _load_font(CJK_FONT_PATHS, 23)
    font_code_ascii = _load_font(MONO_FONT_PATHS, 19)
    font_code_cjk = _load_font(CJK_FONT_PATHS, 19)
    font_inline_ascii = _load_font(MONO_FONT_PATHS, 20)
    font_inline_cjk = _load_font(CJK_FONT_PATHS, 20)

    content_x = 56
    content_w = CARD_WIDTH - 112
    ops: list = []
    y = 92
    truncated = False

    def push_op(h_need: int, fn) -> bool:
        nonlocal y, truncated
        if y + h_need > MAX_CARD_HEIGHT - 84:
            truncated = True
            return False
        ops.append((y, fn))
        y += h_need
        return True

    for block in _parse_blocks(text):
        kind = block[0]
        if kind == "heading":
            _, level, heading_text = block
            h_font = font_h1 if level == 1 else font_h2 if level == 2 else font_h3
            line_h = 38 if level == 1 else 34
            wrapped = _wrap_line(heading_text, content_w - 18, h_font, h_font)
            total_h = len(wrapped) * line_h + 14

            def draw_heading(d, top, _w=wrapped, _f=h_font, _lh=line_h, _tot=total_h):
                d.rounded_rectangle(
                    (content_x, top + 4, content_x + 5, top + _tot - 14),
                    radius=3,
                    fill="#7355bc",
                )
                for idx, line in enumerate(_w):
                    d.text((content_x + 16, top + idx * _lh), line, font=_f, fill="#221b33")

            if not push_op(total_h, draw_heading):
                break

        elif kind == "code":
            _, lang, raw_lines = block
            inner_w = content_w - 36
            wrapped_code: list[str] = []
            for raw_line in raw_lines:
                wrapped_code.extend(_wrap_line(raw_line, inner_w, font_code_ascii, font_code_cjk))
            wrapped_code = wrapped_code[:120]
            line_h = 28
            header_h = 36
            box_h = header_h + 16 + len(wrapped_code) * line_h + 14

            def draw_code(
                d, top, _lang=lang, _lines=wrapped_code, _bh=box_h, _hh=header_h, _lh=line_h
            ):
                d.rounded_rectangle(
                    (content_x, top, content_x + content_w, top + _bh - 12),
                    radius=12,
                    fill="#1e1b29",
                    outline="#352f47",
                    width=1,
                )
                d.rounded_rectangle(
                    (content_x + 1, top + 1, content_x + content_w - 1, top + _hh),
                    radius=11,
                    fill="#2a2438",
                )
                for dot_i, color in enumerate(("#e06c75", "#e5c07b", "#98c379")):
                    cx = content_x + 18 + dot_i * 18
                    cy = top + 18
                    d.ellipse((cx - 4, cy - 4, cx + 4, cy + 4), fill=color)
                d.text((content_x + 76, top + 9), _lang, font=font_small, fill="#b7add2")
                text_y = top + _hh + 12
                for idx, cline in enumerate(_lines):
                    _draw_mixed(
                        d,
                        (content_x + 18, text_y + idx * _lh),
                        cline,
                        font_code_ascii,
                        font_code_cjk,
                        "#ebe6f7",
                    )

            if not push_op(box_h, draw_code):
                break

        elif kind == "table":
            _, rows = block
            ncols = len(rows[0])
            raw_widths = [0.0] * ncols
            for r in rows[:20]:
                for col_i, cell in enumerate(r):
                    raw_widths[col_i] = max(
                        raw_widths[col_i], font_small.getlength(cell) + 22, 64.0
                    )
            total_raw = sum(raw_widths) or 1.0
            col_widths = [max(56.0, content_w * (w / total_raw)) for w in raw_widths]
            scale = content_w / sum(col_widths)
            col_widths = [w * scale for w in col_widths]

            row_layouts = []
            cell_line_h = 24
            for r in rows:
                cell_wrapped = [
                    _wrap_line(cell, max(36.0, col_widths[ci] - 18), font_small, font_small)
                    for ci, cell in enumerate(r)
                ]
                rh = max(len(cw) for cw in cell_wrapped) * cell_line_h + 16
                row_layouts.append((cell_wrapped, rh))
            table_h = sum(rh for _, rh in row_layouts) + 16

            def draw_table(d, top, _rows=row_layouts, _cw=col_widths, _clh=cell_line_h):
                cur_y = top
                for r_idx, (cells, rh) in enumerate(_rows):
                    bg = "#ebe4f7" if r_idx == 0 else ("#ffffff" if r_idx % 2 else "#f8f6fc")
                    d.rectangle(
                        (content_x, cur_y, content_x + content_w, cur_y + rh),
                        fill=bg,
                        outline="#d8cfe8",
                    )
                    cur_x = float(content_x)
                    for c_idx, cell_lines in enumerate(cells):
                        w_col = _cw[c_idx]
                        if c_idx > 0:
                            d.line(
                                (cur_x, cur_y, cur_x, cur_y + rh),
                                fill="#d8cfe8",
                                width=1,
                            )
                        color = "#281e40" if r_idx == 0 else "#2e2840"
                        for li, cline in enumerate(cell_lines):
                            d.text(
                                (cur_x + 9, cur_y + 8 + li * _clh),
                                cline,
                                font=font_small,
                                fill=color,
                            )
                        cur_x += w_col
                    cur_y += rh

            if not push_op(table_h, draw_table):
                break

        elif kind == "quote":
            _, qtext = block
            wrapped_q = _wrap_rich_lines(
                qtext, content_w - 28, font_body, font_body, font_inline_ascii, font_inline_cjk
            )
            line_h = 31
            box_h = len(wrapped_q) * line_h + 22

            def draw_quote(d, top, _lines=wrapped_q, _bh=box_h, _lh=line_h):
                d.rounded_rectangle(
                    (content_x, top, content_x + content_w, top + _bh - 10),
                    radius=8,
                    fill="#f6f2fc",
                )
                d.rectangle(
                    (content_x, top, content_x + 4, top + _bh - 10),
                    fill="#7e61c4",
                )
                _draw_rich_lines(
                    d,
                    content_x + 18,
                    top + 7,
                    _lines,
                    _lh,
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    "#4b4360",
                )

            if not push_op(box_h, draw_quote):
                break

        elif kind == "list":
            _, marker, item_text = block
            indent = 30
            wrapped_l = _wrap_rich_lines(
                item_text,
                content_w - indent,
                font_body,
                font_body,
                font_inline_ascii,
                font_inline_cjk,
            )
            line_h = 32
            total_h = len(wrapped_l) * line_h + 6

            def draw_list(d, top, _marker=marker, _lines=wrapped_l, _ind=indent, _lh=line_h):
                d.text((content_x + 4, top), _marker, font=font_body, fill="#7355bc")
                _draw_rich_lines(
                    d,
                    content_x + _ind,
                    top,
                    _lines,
                    _lh,
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    "#282238",
                )

            if not push_op(total_h, draw_list):
                break

        else:
            _, ptext = block
            wrapped_p = _wrap_rich_lines(
                ptext, content_w, font_body, font_body, font_inline_ascii, font_inline_cjk
            )
            line_h = 32
            total_h = len(wrapped_p) * line_h + 12

            def draw_para(d, top, _lines=wrapped_p, _lh=line_h):
                _draw_rich_lines(
                    d,
                    content_x,
                    top,
                    _lines,
                    _lh,
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    "#282238",
                )

            if not push_op(total_h, draw_para):
                break

    footer_note_h = 34 if truncated else 0
    canvas_h = min(MAX_CARD_HEIGHT, max(180, y + footer_note_h + 52))
    image = Image.new("RGB", (CARD_WIDTH, canvas_h), "#f2eef9")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle(
        (24, 24, CARD_WIDTH - 24, canvas_h - 24),
        radius=16,
        fill="#ffffff",
        outline="#e3dce9",
        width=1,
    )
    draw.text((content_x, 44), "吉米奈喵 · 排版卡片", font=font_small, fill="#7355bc")
    draw.line((content_x, 76, CARD_WIDTH - content_x, 76), fill="#eee9f4", width=1)
    for top, fn in ops:
        fn(draw, top)
    if truncated:
        draw.text(
            (content_x, canvas_h - 64),
            "…… 内容较长，超出部分已折叠 ……",
            font=font_small,
            fill="#807994",
        )
    for quality in (88, 80, 72):
        target = io.BytesIO()
        image.save(target, format="JPEG", quality=quality, optimize=True)
        raw = target.getvalue()
        if len(raw) <= MAX_CARD_BYTES or quality == 72:
            return "base64://" + base64.b64encode(raw).decode("ascii")
    return "base64://"


def _draw_rich_lines(
    draw, x0: float, y0: float, lines, line_h: int, font_text, font_bold, font_ca, font_cc, fill
):
    for row_idx, segments in enumerate(lines):
        cur_x = float(x0)
        cur_y = y0 + row_idx * line_h
        for kind, seg, seg_w in segments:
            if kind == "code":
                draw.rounded_rectangle(
                    (cur_x + 2, cur_y + 2, cur_x + seg_w - 2, cur_y + line_h - 3),
                    radius=5,
                    fill="#efeaf8",
                )
                _draw_mixed(draw, (cur_x + 6, cur_y + 2), seg, font_ca, font_cc, "#543a99")
            elif kind == "bold":
                draw.text((cur_x, cur_y), seg, font=font_bold, fill="#1a1428")
            else:
                draw.text((cur_x, cur_y), seg, font=font_text, fill=fill)
            cur_x += seg_w
