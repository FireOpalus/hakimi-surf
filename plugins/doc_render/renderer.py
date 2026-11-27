"""Sandboxed in-memory Pillow Markdown/code/table card renderer.

Security invariants:
- Strictly enforces Pillow `Image.MAX_IMAGE_PIXELS` and explicit canvas pixel ceiling.
- Validates text length, dimensions, format, and theme before allocating any image buffer.
- Zero network I/O and zero user-controlled filesystem paths (reads only fixed system font paths).
"""

from __future__ import annotations

import functools
import io
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from PIL import Image, ImageDraw, ImageFont

CARD_WIDTH = 960
MIN_CARD_WIDTH = 320
MAX_CARD_WIDTH = 1440
MAX_CARD_HEIGHT = 4400
MIN_CARD_HEIGHT = 320
HARD_MAX_CARD_HEIGHT = 6000
MAX_RENDER_PIXELS = MAX_CARD_WIDTH * HARD_MAX_CARD_HEIGHT  # 8,640,000 px (~25 MiB RGB)
MAX_CARD_BYTES = 4_500_000
MAX_INPUT_CHARS = 8000


def apply_pillow_pixel_limit(limit: int = MAX_RENDER_PIXELS) -> int:
    """Apply process-wide Pillow pixel cap (intended for standalone `doc_render` process)."""
    bounded = min(max(1, int(limit)), MAX_RENDER_PIXELS)
    Image.MAX_IMAGE_PIXELS = bounded
    return bounded


TABLE_SEP_RE = re.compile(r"^\s*\|?(?:\s*:?-{2,}:?\s*\|)+\s*:?-{2,}:?\s*\|?\s*$")
FENCE_START_RE = re.compile(r"^\s*```([A-Za-z0-9_+#.\-]*)\s*$")
FENCE_END_RE = re.compile(r"^\s*```\s*$")

CJK_FONT_PATHS = (
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
)
MONO_FONT_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    *CJK_FONT_PATHS,
)


@dataclass(frozen=True)
class CardPalette:
    outer_bg: str
    card_bg: str
    card_border: str
    accent: str
    divider: str
    heading: str
    body: str
    bold: str
    muted: str
    code_bg: str
    code_border: str
    code_header_bg: str
    code_lang: str
    code_text: str
    inline_code_bg: str
    inline_code_text: str
    quote_bg: str
    quote_bar: str
    quote_text: str
    table_header_bg: str
    table_odd_bg: str
    table_even_bg: str
    table_border: str
    table_header_text: str
    table_text: str


CARD_THEMES: dict[str, CardPalette] = {
    "lavender": CardPalette(
        outer_bg="#f2eef9",
        card_bg="#ffffff",
        card_border="#e3dce9",
        accent="#7355bc",
        divider="#eee9f4",
        heading="#221b33",
        body="#282238",
        bold="#1a1428",
        muted="#807994",
        code_bg="#1e1b29",
        code_border="#352f47",
        code_header_bg="#2a2438",
        code_lang="#b7add2",
        code_text="#ebe6f7",
        inline_code_bg="#efeaf8",
        inline_code_text="#543a99",
        quote_bg="#f6f2fc",
        quote_bar="#7e61c4",
        quote_text="#4b4360",
        table_header_bg="#ebe4f7",
        table_odd_bg="#ffffff",
        table_even_bg="#f8f6fc",
        table_border="#d8cfe8",
        table_header_text="#281e40",
        table_text="#2e2840",
    ),
    "light": CardPalette(
        outer_bg="#f1f5f9",
        card_bg="#ffffff",
        card_border="#dbe3ec",
        accent="#2563eb",
        divider="#e2e8f0",
        heading="#0f172a",
        body="#1e293b",
        bold="#0f172a",
        muted="#64748b",
        code_bg="#0f172a",
        code_border="#334155",
        code_header_bg="#1e293b",
        code_lang="#cbd5e1",
        code_text="#f1f5f9",
        inline_code_bg="#e2e8f0",
        inline_code_text="#1d4ed8",
        quote_bg="#f8fafc",
        quote_bar="#3b82f6",
        quote_text="#475569",
        table_header_bg="#e2e8f0",
        table_odd_bg="#ffffff",
        table_even_bg="#f8fafc",
        table_border="#cbd5e1",
        table_header_text="#0f172a",
        table_text="#1e293b",
    ),
    "dark": CardPalette(
        outer_bg="#12111a",
        card_bg="#1b1926",
        card_border="#2f2b42",
        accent="#a78bfa",
        divider="#2c283e",
        heading="#f5f3ff",
        body="#e2ddf5",
        bold="#ffffff",
        muted="#9b92b8",
        code_bg="#110f19",
        code_border="#332e48",
        code_header_bg="#1d1a2c",
        code_lang="#c4b5fd",
        code_text="#ede9fe",
        inline_code_bg="#2b2540",
        inline_code_text="#c4b5fd",
        quote_bg="#242034",
        quote_bar="#8b5cf6",
        quote_text="#c7c0dd",
        table_header_bg="#2b2540",
        table_odd_bg="#1b1926",
        table_even_bg="#221e30",
        table_border="#3a3354",
        table_header_text="#f5f3ff",
        table_text="#e2ddf5",
    ),
}


class CardRenderError(Exception):
    """Constant-code card rendering validation or size failure."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.code)


@dataclass(frozen=True)
class RenderedCard:
    """Bounded rendered card image and metadata."""

    image_bytes: bytes
    width: int
    height: int
    format: Literal["png", "jpeg"]
    mime_type: str
    filename: str
    byte_size: int
    truncated: bool

    def to_metadata(self) -> dict[str, Any]:
        return {
            "width": self.width,
            "height": self.height,
            "format": self.format,
            "byte_size": self.byte_size,
            "truncated": self.truncated,
        }


@functools.lru_cache(maxsize=48)
def _load_font(paths: tuple[str, ...], size: int) -> ImageFont.ImageFont:
    chosen = next((p for p in paths if Path(p).is_file()), None)
    return ImageFont.truetype(chosen, size) if chosen else ImageFont.load_default(size=size)


def _split_runs(text: str, font_ascii: Any, font_cjk: Any) -> list[tuple[str, Any]]:
    if font_ascii is font_cjk:
        return [(text, font_cjk)] if text else []
    return [
        (part, font_ascii if part.isascii() else font_cjk)
        for part in re.findall(r"[\x00-\x7f]+|[^\x00-\x7f]+", text)
    ]


def _mixed_width(text: str, font_ascii: Any, font_cjk: Any) -> float:
    return sum(font.getlength(part) for part, font in _split_runs(text, font_ascii, font_cjk))


def _draw_mixed(
    draw: Any, xy: tuple[float, float], text: str, font_ascii: Any, font_cjk: Any, fill: str
) -> None:
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


def _check_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() > deadline:
        raise TimeoutError("card rendering timed out")


def _fit_prefix_end(
    text: str,
    start: int,
    max_width: float,
    measure: Callable[[str], float],
    *,
    hint: int = 0,
) -> int:
    """Return maximal end index in [start + 1, len(text)] whose width <= max_width."""
    n = len(text)
    if start + 1 >= n:
        return n

    lo = start + 1
    if hint > 1:
        probe = min(n, start + hint)
        if measure(text[start:probe]) <= max_width:
            lo = probe
            if lo >= n:
                return n
            step = 1
            hi = n
            while lo < n:
                nxt = min(n, lo + step)
                if measure(text[start:nxt]) <= max_width:
                    lo = nxt
                    step *= 2
                else:
                    hi = nxt
                    break
            else:
                return n
        else:
            hi = probe
            step = 1
            while hi > start + 1:
                prev = max(start + 1, hi - step)
                if measure(text[start:prev]) <= max_width:
                    lo = prev
                    break
                hi = prev
                step *= 2
            else:
                return start + 1
    else:
        step = 1
        hi = n
        while lo < n:
            nxt = min(n, lo + step)
            if measure(text[start:nxt]) <= max_width:
                lo = nxt
                step *= 2
            else:
                hi = nxt
                break
        else:
            return n

    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if measure(text[start:mid]) <= max_width:
            lo = mid
        else:
            hi = mid
    return lo


def _wrap_line(
    text: str,
    max_width: float,
    font_ascii: Any,
    font_cjk: Any,
    *,
    deadline: float | None = None,
) -> list[str]:
    if not text:
        return [""]
    measure: Callable[[str], float] = (
        font_cjk.getlength
        if font_ascii is font_cjk
        else (lambda s: _mixed_width(s, font_ascii, font_cjk))
    )
    lines: list[str] = []
    pos = 0
    n = len(text)
    hint = 0
    while pos < n:
        if deadline is not None and (len(lines) & 7) == 0:
            _check_deadline(deadline)
        end = _fit_prefix_end(text, pos, max_width, measure, hint=hint)
        lines.append(text[pos:end])
        hint = end - pos
        pos = end
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
    text: str,
    max_width: float,
    font_text: Any,
    font_bold: Any,
    font_code_ascii: Any,
    font_code_cjk: Any,
    *,
    deadline: float | None = None,
) -> list[list[tuple[str, str, float]]]:
    lines: list[list[tuple[str, str, float]]] = [[]]
    cur_width = 0.0

    def seg_width(kind: str, chunk: str) -> float:
        if kind == "code":
            return _mixed_width(chunk, font_code_ascii, font_code_cjk) + 12
        font = font_bold if kind == "bold" else font_text
        return font.getlength(chunk)

    for para_idx, para in enumerate(text.splitlines() or [""]):
        _check_deadline(deadline)
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
                    content,
                    max(80.0, max_width - 14),
                    font_code_ascii,
                    font_code_cjk,
                    deadline=deadline,
                ):
                    w = seg_width("code", wrapped)
                    if lines[-1]:
                        lines.append([])
                    lines[-1].append(("code", wrapped, w))
                    cur_width = w
                continue
            font = font_bold if kind == "bold" else font_text
            measure = font.getlength
            pos = 0
            n = len(content)
            hint = 0
            while pos < n:
                if deadline is not None and (len(lines) & 7) == 0:
                    _check_deadline(deadline)
                avail = max_width - cur_width
                if lines[-1] and measure(content[pos : pos + 1]) > avail:
                    lines.append([])
                    cur_width = 0.0
                    avail = max_width
                end = _fit_prefix_end(content, pos, avail, measure, hint=hint)
                seg = content[pos:end]
                seg_w = measure(seg)
                lines[-1].append((kind, seg, seg_w))
                if end < n:
                    hint = end - pos
                    lines.append([])
                    cur_width = 0.0
                else:
                    cur_width += seg_w
                pos = end
    return lines


def _parse_blocks(text: str) -> list[tuple[Any, ...]]:
    lines = text[:MAX_INPUT_CHARS].replace("\r\n", "\n").split("\n")
    blocks: list[tuple[Any, ...]] = []
    para_buf: list[str] = []
    i, code_lines_used = 0, 0

    def flush_para() -> None:
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


def _draw_rich_lines(
    draw: Any,
    x0: float,
    y0: float,
    lines: list[list[tuple[str, str, float]]],
    line_h: int,
    font_text: Any,
    font_bold: Any,
    font_ca: Any,
    font_cc: Any,
    fill: str,
    palette: CardPalette,
) -> None:
    for row_idx, segments in enumerate(lines):
        cur_x = float(x0)
        cur_y = y0 + row_idx * line_h
        for kind, seg, seg_w in segments:
            if kind == "code":
                draw.rounded_rectangle(
                    (cur_x + 2, cur_y + 2, cur_x + seg_w - 2, cur_y + line_h - 3),
                    radius=5,
                    fill=palette.inline_code_bg,
                )
                _draw_mixed(
                    draw, (cur_x + 6, cur_y + 2), seg, font_ca, font_cc, palette.inline_code_text
                )
            elif kind == "bold":
                draw.text((cur_x, cur_y), seg, font=font_bold, fill=palette.bold)
            else:
                draw.text((cur_x, cur_y), seg, font=font_text, fill=fill)
            cur_x += seg_w


def render_card_bytes(
    text: str,
    *,
    fmt: str = "png",
    width: int = CARD_WIDTH,
    max_height: int = MAX_CARD_HEIGHT,
    theme: str = "lavender",
    max_bytes: int = MAX_CARD_BYTES,
    deadline: float | None = None,
) -> RenderedCard:
    """Render plain text or Markdown card to bounded PNG or JPEG bytes."""
    _check_deadline(deadline)
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_INPUT_CHARS:
        raise CardRenderError("card_text_invalid")
    norm_fmt = str(fmt or "png").strip().lower()
    if norm_fmt not in {"png", "jpeg"}:
        raise CardRenderError("card_text_invalid")
    if isinstance(width, bool) or not isinstance(width, int):
        raise CardRenderError("card_text_invalid")
    if not (MIN_CARD_WIDTH <= width <= MAX_CARD_WIDTH):
        raise CardRenderError("card_text_invalid")
    if isinstance(max_height, bool) or not isinstance(max_height, int):
        raise CardRenderError("card_text_invalid")
    if not (MIN_CARD_HEIGHT <= max_height <= HARD_MAX_CARD_HEIGHT):
        raise CardRenderError("card_text_invalid")
    if width * max_height > MAX_RENDER_PIXELS:
        raise CardRenderError("card_too_large")
    palette = CARD_THEMES.get(str(theme or "lavender").strip().lower())
    if palette is None:
        raise CardRenderError("card_text_invalid")
    byte_cap = min(max(4096, int(max_bytes)), MAX_CARD_BYTES)

    try:
        font_body = _load_font(CJK_FONT_PATHS, 22)
        font_small = _load_font(CJK_FONT_PATHS, 17)
        font_h1 = _load_font(CJK_FONT_PATHS, 29)
        font_h2 = _load_font(CJK_FONT_PATHS, 25)
        font_h3 = _load_font(CJK_FONT_PATHS, 23)
        font_code_ascii = _load_font(MONO_FONT_PATHS, 19)
        font_code_cjk = _load_font(CJK_FONT_PATHS, 19)
        font_inline_ascii = _load_font(MONO_FONT_PATHS, 20)
        font_inline_cjk = _load_font(CJK_FONT_PATHS, 20)

        outer_pad = 24 if width >= 520 else 14
        content_x = 56 if width >= 520 else 28
        content_w = max(160, width - content_x * 2)
        ops: list[tuple[int, Any]] = []
        y = 92
        truncated = False

        def push_op(h_need: int, fn: Any) -> bool:
            nonlocal y, truncated
            if y + h_need > max_height - 84:
                truncated = True
                return False
            ops.append((y, fn))
            y += h_need
            return True

        for block in _parse_blocks(text):
            _check_deadline(deadline)
            kind = block[0]
            if kind == "heading":
                _, level, heading_text = block
                h_font = font_h1 if level == 1 else font_h2 if level == 2 else font_h3
                line_h = 38 if level == 1 else 34
                wrapped = _wrap_line(
                    heading_text,
                    max(80.0, content_w - 18),
                    h_font,
                    h_font,
                    deadline=deadline,
                )
                total_h = len(wrapped) * line_h + 14

                def draw_heading(
                    d: Any,
                    top: int,
                    _w: list[str] = wrapped,
                    _f: Any = h_font,
                    _lh: int = line_h,
                    _tot: int = total_h,
                ) -> None:
                    d.rounded_rectangle(
                        (content_x, top + 4, content_x + 5, top + _tot - 14),
                        radius=3,
                        fill=palette.accent,
                    )
                    for idx, line in enumerate(_w):
                        d.text(
                            (content_x + 16, top + idx * _lh),
                            line,
                            font=_f,
                            fill=palette.heading,
                        )

                if not push_op(total_h, draw_heading):
                    break

            elif kind == "code":
                _, lang, raw_lines = block
                inner_w = max(80.0, content_w - 36)
                wrapped_code: list[str] = []
                for raw_line in raw_lines:
                    _check_deadline(deadline)
                    wrapped_code.extend(
                        _wrap_line(
                            raw_line,
                            inner_w,
                            font_code_ascii,
                            font_code_cjk,
                            deadline=deadline,
                        )
                    )
                wrapped_code = wrapped_code[:120]
                line_h = 28
                header_h = 36
                box_h = header_h + 16 + len(wrapped_code) * line_h + 14

                def draw_code(
                    d: Any,
                    top: int,
                    _lang: str = lang,
                    _lines: list[str] = wrapped_code,
                    _bh: int = box_h,
                    _hh: int = header_h,
                    _lh: int = line_h,
                ) -> None:
                    d.rounded_rectangle(
                        (content_x, top, content_x + content_w, top + _bh - 12),
                        radius=12,
                        fill=palette.code_bg,
                        outline=palette.code_border,
                        width=1,
                    )
                    d.rounded_rectangle(
                        (content_x + 1, top + 1, content_x + content_w - 1, top + _hh),
                        radius=11,
                        fill=palette.code_header_bg,
                    )
                    for dot_i, color in enumerate(("#e06c75", "#e5c07b", "#98c379")):
                        cx = content_x + 18 + dot_i * 18
                        cy = top + 18
                        d.ellipse((cx - 4, cy - 4, cx + 4, cy + 4), fill=color)
                    d.text(
                        (content_x + 76, top + 9),
                        _lang,
                        font=font_small,
                        fill=palette.code_lang,
                    )
                    text_y = top + _hh + 12
                    for idx, cline in enumerate(_lines):
                        _draw_mixed(
                            d,
                            (content_x + 18, text_y + idx * _lh),
                            cline,
                            font_code_ascii,
                            font_code_cjk,
                            palette.code_text,
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
                    _check_deadline(deadline)
                    cell_wrapped = [
                        _wrap_line(
                            cell,
                            max(36.0, col_widths[ci] - 18),
                            font_small,
                            font_small,
                            deadline=deadline,
                        )
                        for ci, cell in enumerate(r)
                    ]
                    rh = max(len(cw) for cw in cell_wrapped) * cell_line_h + 16
                    row_layouts.append((cell_wrapped, rh))
                table_h = sum(rh for _, rh in row_layouts) + 16

                def draw_table(
                    d: Any,
                    top: int,
                    _rows: list[Any] = row_layouts,
                    _cw: list[float] = col_widths,
                    _clh: int = cell_line_h,
                ) -> None:
                    cur_y = top
                    for r_idx, (cells, rh) in enumerate(_rows):
                        bg = (
                            palette.table_header_bg
                            if r_idx == 0
                            else (palette.table_odd_bg if r_idx % 2 else palette.table_even_bg)
                        )
                        d.rectangle(
                            (content_x, cur_y, content_x + content_w, cur_y + rh),
                            fill=bg,
                            outline=palette.table_border,
                        )
                        cur_x = float(content_x)
                        for c_idx, cell_lines in enumerate(cells):
                            w_col = _cw[c_idx]
                            if c_idx > 0:
                                d.line(
                                    (cur_x, cur_y, cur_x, cur_y + rh),
                                    fill=palette.table_border,
                                    width=1,
                                )
                            color = palette.table_header_text if r_idx == 0 else palette.table_text
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
                    qtext,
                    max(80.0, content_w - 28),
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    deadline=deadline,
                )
                line_h = 31
                box_h = len(wrapped_q) * line_h + 22

                def draw_quote(
                    d: Any,
                    top: int,
                    _lines: list[Any] = wrapped_q,
                    _bh: int = box_h,
                    _lh: int = line_h,
                ) -> None:
                    d.rounded_rectangle(
                        (content_x, top, content_x + content_w, top + _bh - 10),
                        radius=8,
                        fill=palette.quote_bg,
                    )
                    d.rectangle(
                        (content_x, top, content_x + 4, top + _bh - 10),
                        fill=palette.quote_bar,
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
                        palette.quote_text,
                        palette,
                    )

                if not push_op(box_h, draw_quote):
                    break

            elif kind == "list":
                _, marker, item_text = block
                indent = 30
                wrapped_l = _wrap_rich_lines(
                    item_text,
                    max(80.0, content_w - indent),
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    deadline=deadline,
                )
                line_h = 32
                total_h = len(wrapped_l) * line_h + 6

                def draw_list(
                    d: Any,
                    top: int,
                    _marker: str = marker,
                    _lines: list[Any] = wrapped_l,
                    _ind: int = indent,
                    _lh: int = line_h,
                ) -> None:
                    d.text((content_x + 4, top), _marker, font=font_body, fill=palette.accent)
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
                        palette.body,
                        palette,
                    )

                if not push_op(total_h, draw_list):
                    break

            else:
                _, ptext = block
                wrapped_p = _wrap_rich_lines(
                    ptext,
                    content_w,
                    font_body,
                    font_body,
                    font_inline_ascii,
                    font_inline_cjk,
                    deadline=deadline,
                )
                line_h = 32
                total_h = len(wrapped_p) * line_h + 12

                def draw_para(
                    d: Any, top: int, _lines: list[Any] = wrapped_p, _lh: int = line_h
                ) -> None:
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
                        palette.body,
                        palette,
                    )

                if not push_op(total_h, draw_para):
                    break

        footer_note_h = 34 if truncated else 0
        canvas_h = min(max_height, max(180, y + footer_note_h + 52))
        if width * canvas_h > MAX_RENDER_PIXELS:
            raise CardRenderError("card_too_large")

        _check_deadline(deadline)
        image = Image.new("RGB", (width, canvas_h), palette.outer_bg)
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle(
            (outer_pad, outer_pad, width - outer_pad, canvas_h - outer_pad),
            radius=16,
            fill=palette.card_bg,
            outline=palette.card_border,
            width=1,
        )
        draw.text((content_x, 44), "吉米奈喵 · 排版卡片", font=font_small, fill=palette.accent)
        draw.line((content_x, 76, width - content_x, 76), fill=palette.divider, width=1)
        for top, fn in ops:
            _check_deadline(deadline)
            fn(draw, top)
        if truncated:
            draw.text(
                (content_x, canvas_h - 64),
                "…… 内容较长，超出部分已折叠 ……",
                font=font_small,
                fill=palette.muted,
            )

        raw = _encode_bounded_image(image, norm_fmt, byte_cap, deadline=deadline)
    except (CardRenderError, TimeoutError):
        raise
    except Exception:
        raise CardRenderError("render_failed") from None

    mime_type = "image/png" if norm_fmt == "png" else "image/jpeg"
    filename = "card.png" if norm_fmt == "png" else "card.jpg"
    typed_fmt: Literal["png", "jpeg"] = "png" if norm_fmt == "png" else "jpeg"
    return RenderedCard(
        image_bytes=raw,
        width=width,
        height=canvas_h,
        format=typed_fmt,
        mime_type=mime_type,
        filename=filename,
        byte_size=len(raw),
        truncated=truncated,
    )


def _encode_bounded_image(
    image: Image.Image,
    fmt: str,
    byte_cap: int,
    *,
    deadline: float | None = None,
) -> bytes:
    if fmt == "png":
        for compress_level in (6, 9):
            _check_deadline(deadline)
            target = io.BytesIO()
            image.save(target, format="PNG", optimize=True, compress_level=compress_level)
            raw = target.getvalue()
            if len(raw) <= byte_cap:
                return raw
        _check_deadline(deadline)
        quantized = image.quantize(colors=256)
        target = io.BytesIO()
        quantized.save(target, format="PNG", optimize=True, compress_level=9)
        raw = target.getvalue()
        if len(raw) <= byte_cap:
            return raw
        raise CardRenderError("card_too_large")

    for quality in (88, 80, 72):
        _check_deadline(deadline)
        target = io.BytesIO()
        image.save(target, format="JPEG", quality=quality, optimize=True)
        raw = target.getvalue()
        if len(raw) <= byte_cap:
            return raw
    raise CardRenderError("card_too_large")
