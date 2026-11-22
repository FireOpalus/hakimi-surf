"""Sandboxed in-memory document parser for plain text, PDF, DOCX, XLSX, and PPTX.

Security invariants:
- Strictly payload-only: never opens local filesystem paths or performs network I/O.
- Hard bounds on input size, ZIP entries/decompressed bytes/compression ratio, and path traversal.
- Custom Expat XML parser forbidding DOCTYPE/ENTITY/external entities and enforcing depth/node caps.
- Hard bounds on PDF page count, FlateDecode stream expansion, extracted characters, and runtime.
"""

from __future__ import annotations

import io
import re
import time
import xml.etree.ElementTree as ET
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from xml.parsers import expat

MAX_DOCUMENT_BYTES = 15 * 1024 * 1024
MAX_TEXT_BYTES = 2 * 1024 * 1024
MIN_MAX_CHARS = 256
MAX_MAX_CHARS = 16_000
DEFAULT_MAX_CHARS = 3_200

# ZIP bomb & OOXML container hardening limits
MAX_ZIP_ENTRIES = 512
MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES = 10 * 1024 * 1024
MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 32 * 1024 * 1024
MAX_ZIP_COMPRESSION_RATIO = 100.0
_ZIP_RATIO_MIN_BYTES = 512 * 1024

# Safe XML hardening limits
MAX_XML_BYTES = 8 * 1024 * 1024
MAX_XML_DEPTH = 64
MAX_XML_NODES = 120_000
MAX_XML_TEXT_CHARS = 2_000_000

# Office & PDF extraction bounds
MAX_PDF_PAGES = 50
MAX_PDF_EXTRACT_CHARS = 200_000
MAX_PDF_STREAM_BYTES = 8 * 1024 * 1024
MAX_PDF_TOTAL_DECOMPRESSED_BYTES = MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES
MAX_PDF_PREFLIGHT_STREAMS = 512
MAX_XLSX_SHEETS = 20
MAX_XLSX_ROWS_PER_SHEET = 500
MAX_XLSX_COLS = 50
MAX_XLSX_SHARED_STRINGS = 100_000
MAX_PPTX_SLIDES = 100

ALLOWED_TEXT_EXTENSIONS = frozenset(
    {
        ".txt",
        ".log",
        ".md",
        ".markdown",
        ".rst",
        ".csv",
        ".tsv",
        ".py",
        ".pyi",
        ".js",
        ".mjs",
        ".cjs",
        ".ts",
        ".tsx",
        ".jsx",
        ".json",
        ".jsonl",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
        ".conf",
        ".sh",
        ".bash",
        ".zsh",
        ".sql",
        ".html",
        ".htm",
        ".css",
        ".xml",
        ".c",
        ".h",
        ".cpp",
        ".hpp",
        ".rs",
        ".go",
        ".java",
        ".kt",
        ".swift",
        ".rb",
        ".php",
        ".lua",
    }
)
ALLOWED_SPECIAL_FILENAMES = frozenset({"dockerfile", ".gitignore", ".env.example"})
ALLOWED_RICH_DOC_EXTENSIONS = frozenset({".pdf", ".docx", ".xlsx", ".pptx"})

_CONTROL_RE = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u200b-\u200f\u202a-\u202e\u2066-\u2069\ufeff]"
)
_DRIVE_PREFIX_RE = re.compile(r"^[A-Za-z]:")
_XML_FORBIDDEN_DECL_RE = re.compile(
    rb"<!\s*(?:DOCTYPE|ENTITY|ELEMENT|ATTLIST|NOTATION)\b",
    re.IGNORECASE,
)
_SLIDE_XML_RE = re.compile(r"^ppt/slides/slide(\d+)\.xml$")
_WORKSHEET_XML_RE = re.compile(r"^xl/worksheets/sheet(\d+)\.xml$")
_CELL_REF_COL_RE = re.compile(r"^([A-Za-z]{1,3})\d+$")
_PDF_PAGE_RE = re.compile(rb"/Type\s*/Page(?![A-Za-z])")
_PDF_STREAM_RE = re.compile(rb"stream\r?\n(.*?)\r?\n?endstream", re.DOTALL)
_PDF_BT_ET_RE = re.compile(rb"\bBT\b(.*?)\bET\b", re.DOTALL)

_NS_W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_NS_A = "http://schemas.openxmlformats.org/drawingml/2006/main"
_NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_SS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


class DocumentParseError(Exception):
    """Constant-code document parsing or security validation failure."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.code)


@dataclass(frozen=True)
class ParsedDocument:
    """Normalized bounded document extraction output."""

    content: str
    char_count: int
    byte_size: int
    encoding: str
    format: str
    truncated: bool
    pages: int | None = None

    def to_result_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "content": self.content,
            "char_count": self.char_count,
            "byte_size": self.byte_size,
            "encoding": self.encoding,
            "format": self.format,
            "truncated": self.truncated,
        }
        if self.pages is not None:
            out["pages"] = self.pages
        return out


def _check_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() > deadline:
        raise DocumentParseError("parse_timeout")


def _clean_extracted_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    return _CONTROL_RE.sub("", normalized).strip()


def _excerpt_text(text: str, max_chars: int) -> tuple[str, bool]:
    cap = min(max(int(max_chars), MIN_MAX_CHARS), MAX_MAX_CHARS)
    total = len(text)
    if total <= cap:
        return text, False
    if cap == DEFAULT_MAX_CHARS:
        head_len = 1000
        tail_len = 2100
    else:
        usable = max(64, cap - 48)
        head_len = usable // 3
        tail_len = usable - head_len
    omitted = total - (head_len + tail_len)
    excerpt = f"{text[:head_len]}\n... [中间省略 {omitted} 字] ...\n{text[-tail_len:]}"
    return excerpt, True


def validate_document_filename(filename: str) -> tuple[str, str]:
    """Validate a caller-supplied filename (basename only) and return `(clean_name, doc_kind)`."""
    if not isinstance(filename, str):
        raise DocumentParseError("invalid_document", "filename must be a string")
    raw = filename.strip()
    if not raw or len(raw) > 255:
        raise DocumentParseError("invalid_document", "invalid filename length")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in raw):
        raise DocumentParseError("invalid_document", "control character in filename")
    # Reject arbitrary local paths, traversal segments, and Windows drive prefixes.
    if (
        "/" in raw
        or "\\" in raw
        or ":" in raw
        or raw in {".", ".."}
        or ".." in raw.split("/")
        or raw.startswith("..")
    ):
        raise DocumentParseError("invalid_document", "path traversal rejected")

    lower = raw.lower()
    if lower == ".env" or (lower.startswith(".env.") and lower != ".env.example"):
        raise DocumentParseError("unsupported_extension")
    if lower in ALLOWED_SPECIAL_FILENAMES:
        return raw, "text"
    suffix = PurePosixPath(lower).suffix
    if suffix in ALLOWED_TEXT_EXTENSIONS:
        return raw, "text"
    if suffix == ".pdf":
        return raw, "pdf"
    if suffix == ".docx":
        return raw, "docx"
    if suffix == ".xlsx":
        return raw, "xlsx"
    if suffix == ".pptx":
        return raw, "pptx"
    raise DocumentParseError("unsupported_extension")


def _validate_zip_member_name(name: str) -> str:
    if not isinstance(name, str) or not name or len(name) > 512:
        raise DocumentParseError("invalid_document", "invalid zip entry name")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        raise DocumentParseError("invalid_document", "control char in zip entry")
    if name.startswith(("/", "\\")) or _DRIVE_PREFIX_RE.match(name):
        raise DocumentParseError("invalid_document", "absolute zip entry path")
    normalized = name.replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    if any(part in {"..", ""} for part in parts):
        raise DocumentParseError("invalid_document", "zip path traversal")
    return normalized


class _SafeZipArchive:
    """Bounded in-memory ZIP reader defending against zip bombs, symlinks, and path traversal."""

    def __init__(self, raw: bytes, *, deadline: float | None = None) -> None:
        if not raw or not raw.startswith((b"PK\x03\x04", b"PK\x05\x06")):
            raise DocumentParseError("invalid_document", "not a zip archive")
        self._raw_size = len(raw)
        self._deadline = deadline
        self._actual_total_bytes = 0
        try:
            self._zf = zipfile.ZipFile(io.BytesIO(raw), "r")
        except zipfile.LargeZipFile:
            raise DocumentParseError("zip_bomb_detected") from None
        except (zipfile.BadZipFile, OSError, ValueError):
            raise DocumentParseError("invalid_document") from None

        self._entries: dict[str, zipfile.ZipInfo] = {}
        try:
            self._validate_directory()
        except BaseException:
            self._zf.close()
            raise

    def __enter__(self) -> _SafeZipArchive:
        return self

    def __exit__(self, *_: Any) -> None:
        self._zf.close()

    def _validate_directory(self) -> None:
        infos = self._zf.infolist()
        if not infos:
            raise DocumentParseError("invalid_document", "empty zip archive")
        if len(infos) > MAX_ZIP_ENTRIES:
            raise DocumentParseError("zip_bomb_detected", "too many zip entries")

        declared_total = 0
        for info in infos:
            _check_deadline(self._deadline)
            if info.flag_bits & 0x1:
                raise DocumentParseError("invalid_document", "encrypted zip unsupported")
            unix_mode = (info.external_attr >> 16) & 0o170000
            if unix_mode == 0o120000:
                raise DocumentParseError("invalid_document", "zip symlink rejected")
            norm_name = _validate_zip_member_name(info.filename)
            if info.file_size < 0 or info.compress_size < 0:
                raise DocumentParseError("zip_bomb_detected", "negative zip size")
            if info.file_size > MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES:
                raise DocumentParseError("zip_bomb_detected", "zip entry too large")
            declared_total += info.file_size
            if declared_total > MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES:
                raise DocumentParseError("zip_bomb_detected", "zip total size too large")
            if info.file_size > _ZIP_RATIO_MIN_BYTES:
                if info.compress_size == 0 or (
                    info.file_size / info.compress_size > MAX_ZIP_COMPRESSION_RATIO
                ):
                    raise DocumentParseError("zip_bomb_detected", "zip compression ratio exceeded")
            if not info.is_dir():
                self._entries[norm_name] = info

        if (
            declared_total > _ZIP_RATIO_MIN_BYTES * 4
            and (declared_total / max(self._raw_size, 1)) > MAX_ZIP_COMPRESSION_RATIO
        ):
            raise DocumentParseError("zip_bomb_detected", "zip overall ratio exceeded")

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._entries.keys())

    def has(self, name: str) -> bool:
        return name in self._entries

    def read_entry(self, name: str, *, max_bytes: int = MAX_XML_BYTES) -> bytes:
        info = self._entries.get(name)
        if info is None:
            raise DocumentParseError("invalid_document", f"missing entry {name}")
        entry_cap = min(max_bytes, MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES)
        buf = bytearray()
        try:
            with self._zf.open(info, "r") as stream:
                while True:
                    _check_deadline(self._deadline)
                    chunk = stream.read(65_536)
                    if not chunk:
                        break
                    buf.extend(chunk)
                    self._actual_total_bytes += len(chunk)
                    if len(buf) > entry_cap:
                        raise DocumentParseError("zip_bomb_detected", "entry actual size exceeded")
                    if self._actual_total_bytes > MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES:
                        raise DocumentParseError("zip_bomb_detected", "total actual size exceeded")
                    if len(buf) > _ZIP_RATIO_MIN_BYTES:
                        ratio = len(buf) / max(info.compress_size, 1)
                        if ratio > MAX_ZIP_COMPRESSION_RATIO:
                            raise DocumentParseError("zip_bomb_detected", "actual ratio exceeded")
        except DocumentParseError:
            raise
        except (zipfile.BadZipFile, zlib.error, EOFError, RuntimeError, OSError):
            raise DocumentParseError("invalid_document") from None
        return bytes(buf)


def _parse_xml_safe(raw_xml: bytes, *, deadline: float | None = None) -> ET.Element:
    """Parse XML strictly forbidding DOCTYPE/ENTITY/external refs and bounding depth/nodes."""
    if not raw_xml or len(raw_xml) > MAX_XML_BYTES:
        raise DocumentParseError("xml_unsafe")
    if _XML_FORBIDDEN_DECL_RE.search(raw_xml):
        raise DocumentParseError("xml_unsafe")

    parser = expat.ParserCreate(namespace_separator="}")
    parser.buffer_text = True
    builder = ET.TreeBuilder()
    depth = 0
    node_count = 0
    char_count = 0

    def _forbid(*_args: Any) -> None:
        raise DocumentParseError("xml_unsafe")

    def _forbid_external(*_args: Any) -> int:
        raise DocumentParseError("xml_unsafe")

    def _qualify(name: str) -> str:
        return f"{{{name}" if "}" in name else name

    def _start(name: str, attrs: dict[str, str]) -> None:
        nonlocal depth, node_count
        depth += 1
        node_count += 1
        if depth > MAX_XML_DEPTH or node_count > MAX_XML_NODES:
            raise DocumentParseError("xml_unsafe")
        if (node_count & 1023) == 0:
            _check_deadline(deadline)
        fixed_attrs = {_qualify(k): v for k, v in attrs.items()}
        builder.start(_qualify(name), fixed_attrs)

    def _end(name: str) -> None:
        nonlocal depth
        depth -= 1
        builder.end(_qualify(name))

    def _data(data: str) -> None:
        nonlocal char_count
        char_count += len(data)
        if char_count > MAX_XML_TEXT_CHARS:
            raise DocumentParseError("xml_unsafe")
        builder.data(data)

    parser.StartDoctypeDeclHandler = _forbid
    parser.EntityDeclHandler = _forbid
    parser.UnparsedEntityDeclHandler = _forbid
    parser.NotationDeclHandler = _forbid
    parser.ExternalEntityRefHandler = _forbid_external
    parser.StartElementHandler = _start
    parser.EndElementHandler = _end
    parser.CharacterDataHandler = _data

    try:
        step = 65_536
        total_len = len(raw_xml)
        for offset in range(0, total_len, step):
            _check_deadline(deadline)
            parser.Parse(raw_xml[offset : offset + step], offset + step >= total_len)
        root = builder.close()
    except DocumentParseError:
        raise
    except (expat.ExpatError, ValueError, TypeError):
        raise DocumentParseError("invalid_document") from None
    if root is None:
        raise DocumentParseError("invalid_document")
    return root


def _parse_plain_text(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    _check_deadline(deadline)
    if not raw or b"\x00" in raw[:8192]:
        raise DocumentParseError("binary_unsupported")
    text = ""
    encoding = ""
    for candidate in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            text = raw.decode(candidate, errors="strict")
            encoding = "utf-8" if candidate.startswith("utf-8") else candidate
            break
        except UnicodeDecodeError:
            continue
    if not encoding:
        raise DocumentParseError("encoding_unsupported")
    cleaned = _clean_extracted_text(text)
    excerpt, truncated = _excerpt_text(cleaned, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding=encoding,
        format="text",
        truncated=truncated,
        pages=None,
    )


def _extract_docx_paragraph(elem: ET.Element) -> str:
    parts: list[str] = []
    t_tag = f"{{{_NS_W}}}t"
    tab_tag = f"{{{_NS_W}}}tab"
    br_tag = f"{{{_NS_W}}}br"
    cr_tag = f"{{{_NS_W}}}cr"
    for node in elem.iter():
        if node.tag == t_tag and node.text:
            parts.append(node.text)
        elif node.tag == tab_tag:
            parts.append("\t")
        elif node.tag in {br_tag, cr_tag}:
            parts.append("\n")
    return "".join(parts).strip()


def _parse_docx(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    extract_cap = max(max_chars * 4, 64_000)
    lines: list[str] = []
    collected = 0
    truncated_early = False

    def _add_line(line: str) -> bool:
        nonlocal collected, truncated_early
        if not line:
            return True
        lines.append(line)
        collected += len(line) + 1
        if collected >= extract_cap:
            truncated_early = True
            return False
        return True

    with _SafeZipArchive(raw, deadline=deadline) as zf:
        if not zf.has("word/document.xml"):
            raise DocumentParseError("invalid_document", "missing word/document.xml")
        doc_root = _parse_xml_safe(zf.read_entry("word/document.xml"), deadline=deadline)
        body = doc_root.find(f"{{{_NS_W}}}body")
        container = body if body is not None else doc_root
        p_tag = f"{{{_NS_W}}}p"
        tbl_tag = f"{{{_NS_W}}}tbl"
        tr_tag = f"{{{_NS_W}}}tr"
        tc_tag = f"{{{_NS_W}}}tc"

        for child in container:
            _check_deadline(deadline)
            if child.tag == p_tag:
                if not _add_line(_extract_docx_paragraph(child)):
                    break
            elif child.tag == tbl_tag:
                for row in child.findall(tr_tag):
                    _check_deadline(deadline)
                    cells = []
                    for cell in row.findall(tc_tag):
                        cell_lines = [
                            text
                            for p in cell.findall(p_tag)
                            if (text := _extract_docx_paragraph(p))
                        ]
                        cells.append(" ".join(cell_lines))
                    if any(cells):
                        if not _add_line(" | ".join(cells)):
                            break
                if truncated_early:
                    break

        if not truncated_early:
            for extra_name in ("word/header1.xml", "word/footer1.xml", "word/footnotes.xml"):
                if zf.has(extra_name):
                    extra_root = _parse_xml_safe(zf.read_entry(extra_name), deadline=deadline)
                    for p in extra_root.iter(p_tag):
                        if not _add_line(_extract_docx_paragraph(p)):
                            break
                    if truncated_early:
                        break

    cleaned = _clean_extracted_text("\n".join(lines))
    excerpt, excerpt_truncated = _excerpt_text(cleaned, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding="utf-8",
        format="docx",
        truncated=truncated_early or excerpt_truncated,
        pages=None,
    )


def _col_letters_to_index(letters: str) -> int:
    idx = 0
    for ch in letters.upper():
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def _resolve_ooxml_target(base_dir: str, target: str) -> str:
    clean_target = target.replace("\\", "/").lstrip("/")
    if target.startswith("/"):
        parts = PurePosixPath(clean_target).parts
    else:
        parts = PurePosixPath(base_dir, clean_target).parts
    resolved: list[str] = []
    for part in parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if not resolved:
                raise DocumentParseError("invalid_document", "ooxml relationship traversal")
            resolved.pop()
        else:
            resolved.append(part)
    return "/".join(resolved)


def _parse_xlsx(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    extract_cap = max(max_chars * 4, 64_000)
    shared_strings: list[str] = []
    out_lines: list[str] = []
    collected = 0
    truncated_early = False

    def _add_line(line: str) -> bool:
        nonlocal collected, truncated_early
        out_lines.append(line)
        collected += len(line) + 1
        if collected >= extract_cap:
            truncated_early = True
            return False
        return True

    with _SafeZipArchive(raw, deadline=deadline) as zf:
        if zf.has("xl/sharedStrings.xml"):
            ss_root = _parse_xml_safe(zf.read_entry("xl/sharedStrings.xml"), deadline=deadline)
            si_tag = f"{{{_NS_SS}}}si"
            t_tag = f"{{{_NS_SS}}}t"
            for si in ss_root.iter(si_tag):
                if len(shared_strings) >= MAX_XLSX_SHARED_STRINGS:
                    break
                text_parts = [t.text for t in si.iter(t_tag) if t.text]
                shared_strings.append("".join(text_parts))

        rels_map: dict[str, str] = {}
        if zf.has("xl/_rels/workbook.xml.rels"):
            rels_root = _parse_xml_safe(
                zf.read_entry("xl/_rels/workbook.xml.rels"), deadline=deadline
            )
            for rel in rels_root:
                rid = rel.attrib.get("Id")
                target = rel.attrib.get("Target")
                if rid and target:
                    rels_map[rid] = _resolve_ooxml_target("xl", target)

        sheets: list[tuple[str, str]] = []
        if zf.has("xl/workbook.xml"):
            wb_root = _parse_xml_safe(zf.read_entry("xl/workbook.xml"), deadline=deadline)
            sheet_tag = f"{{{_NS_SS}}}sheet"
            rid_attr = f"{{{_NS_R}}}id"
            for sheet_el in wb_root.iter(sheet_tag):
                sname = (sheet_el.attrib.get("name") or "Sheet").strip()[:64]
                rid = sheet_el.attrib.get(rid_attr) or ""
                path = rels_map.get(rid, "")
                if path and zf.has(path):
                    sheets.append((sname, path))

        if not sheets:
            numbered: list[tuple[int, str]] = []
            for name in zf.names:
                match = _WORKSHEET_XML_RE.match(name)
                if match:
                    numbered.append((int(match.group(1)), name))
            numbered.sort(key=lambda item: item[0])
            sheets = [(f"Sheet{num}", path) for num, path in numbered]

        if not sheets:
            raise DocumentParseError("invalid_document", "no worksheets found")

        total_sheets = len(sheets)
        if total_sheets > MAX_XLSX_SHEETS:
            truncated_early = True

        row_tag = f"{{{_NS_SS}}}row"
        c_tag = f"{{{_NS_SS}}}c"
        v_tag = f"{{{_NS_SS}}}v"
        is_tag = f"{{{_NS_SS}}}is"
        t_tag = f"{{{_NS_SS}}}t"

        for sname, spath in sheets[:MAX_XLSX_SHEETS]:
            _check_deadline(deadline)
            ws_root = _parse_xml_safe(zf.read_entry(spath), deadline=deadline)
            if not _add_line(f"[Sheet: {sname}]"):
                break
            rows_seen = 0
            for row_el in ws_root.iter(row_tag):
                _check_deadline(deadline)
                if rows_seen >= MAX_XLSX_ROWS_PER_SHEET:
                    truncated_early = True
                    break
                row_cells: dict[int, str] = {}
                fallback_col = 0
                for c_el in row_el.findall(c_tag):
                    ref = c_el.attrib.get("r", "")
                    m_col = _CELL_REF_COL_RE.match(ref)
                    col_idx = _col_letters_to_index(m_col.group(1)) if m_col else fallback_col
                    fallback_col = col_idx + 1
                    if col_idx < 0 or col_idx >= MAX_XLSX_COLS:
                        continue
                    ctype = c_el.attrib.get("t", "")
                    val = ""
                    if ctype == "s":
                        v_el = c_el.find(v_tag)
                        if v_el is not None and v_el.text and v_el.text.strip().isdigit():
                            s_idx = int(v_el.text.strip())
                            if 0 <= s_idx < len(shared_strings):
                                val = shared_strings[s_idx]
                    elif ctype == "inlineStr":
                        is_el = c_el.find(is_tag)
                        if is_el is not None:
                            val = "".join(t.text for t in is_el.iter(t_tag) if t.text)
                    elif ctype == "b":
                        v_el = c_el.find(v_tag)
                        val = "TRUE" if (v_el is not None and v_el.text == "1") else "FALSE"
                    else:
                        v_el = c_el.find(v_tag)
                        if v_el is not None and v_el.text is not None:
                            val = v_el.text
                    val = val.replace("\r", " ").replace("\n", " ").strip()
                    if val:
                        row_cells[col_idx] = val
                if row_cells:
                    max_col = max(row_cells)
                    ordered = [row_cells.get(ci, "") for ci in range(max_col + 1)]
                    rows_seen += 1
                    if not _add_line(" | ".join(ordered)):
                        break
            if truncated_early:
                break

    cleaned = _clean_extracted_text("\n".join(out_lines))
    excerpt, excerpt_truncated = _excerpt_text(cleaned, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding="utf-8",
        format="xlsx",
        truncated=truncated_early or excerpt_truncated,
        pages=total_sheets,
    )


def _extract_drawingml_lines(root: ET.Element) -> list[str]:
    p_tag = f"{{{_NS_A}}}p"
    t_tag = f"{{{_NS_A}}}t"
    br_tag = f"{{{_NS_A}}}br"
    tab_tag = f"{{{_NS_A}}}tab"
    lines: list[str] = []
    for p in root.iter(p_tag):
        parts: list[str] = []
        for node in p.iter():
            if node.tag == t_tag and node.text:
                parts.append(node.text)
            elif node.tag == br_tag:
                parts.append("\n")
            elif node.tag == tab_tag:
                parts.append("\t")
        text = "".join(parts).strip()
        if text:
            lines.append(text)
    return lines


def _parse_pptx(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    extract_cap = max(max_chars * 4, 64_000)
    out_lines: list[str] = []
    collected = 0
    truncated_early = False

    def _add_line(line: str) -> bool:
        nonlocal collected, truncated_early
        out_lines.append(line)
        collected += len(line) + 1
        if collected >= extract_cap:
            truncated_early = True
            return False
        return True

    with _SafeZipArchive(raw, deadline=deadline) as zf:
        numbered: list[tuple[int, str]] = []
        for name in zf.names:
            match = _SLIDE_XML_RE.match(name)
            if match:
                numbered.append((int(match.group(1)), name))
        numbered.sort(key=lambda item: item[0])
        if not numbered:
            raise DocumentParseError("invalid_document", "no slides found")

        total_slides = len(numbered)
        if total_slides > MAX_PPTX_SLIDES:
            truncated_early = True

        for slide_num, slide_path in numbered[:MAX_PPTX_SLIDES]:
            _check_deadline(deadline)
            slide_root = _parse_xml_safe(zf.read_entry(slide_path), deadline=deadline)
            slide_lines = _extract_drawingml_lines(slide_root)
            notes_path = f"ppt/notesSlides/notesSlide{slide_num}.xml"
            note_lines: list[str] = []
            if zf.has(notes_path):
                notes_root = _parse_xml_safe(zf.read_entry(notes_path), deadline=deadline)
                note_lines = _extract_drawingml_lines(notes_root)
            if slide_lines or note_lines:
                if not _add_line(f"[Slide {slide_num}]"):
                    break
                for ln in slide_lines:
                    if not _add_line(ln):
                        break
                if truncated_early:
                    break
                if note_lines:
                    if not _add_line("Notes: " + " ".join(note_lines)):
                        break

    cleaned = _clean_extracted_text("\n".join(out_lines))
    excerpt, excerpt_truncated = _excerpt_text(cleaned, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding="utf-8",
        format="pptx",
        truncated=truncated_early or excerpt_truncated,
        pages=total_slides,
    )


def _decode_pdf_literal(data: bytes) -> str:
    out = bytearray()
    i = 0
    n = len(data)
    while i < n:
        b = data[i]
        if b == 0x5C and i + 1 < n:  # backslash escape
            nxt = data[i + 1]
            if nxt == ord("n"):
                out.append(0x0A)
                i += 2
            elif nxt == ord("r"):
                out.append(0x0D)
                i += 2
            elif nxt == ord("t"):
                out.append(0x09)
                i += 2
            elif nxt == ord("b"):
                out.append(0x08)
                i += 2
            elif nxt == ord("f"):
                out.append(0x0C)
                i += 2
            elif nxt in (ord("("), ord(")"), ord("\\")):
                out.append(nxt)
                i += 2
            elif nxt in (0x0A, 0x0D):
                i += 2
                if nxt == 0x0D and i < n and data[i] == 0x0A:
                    i += 1
            elif ord("0") <= nxt <= ord("7"):
                oct_digits = bytes([nxt])
                i += 2
                for _ in range(2):
                    if i < n and ord("0") <= data[i] <= ord("7"):
                        oct_digits += bytes([data[i]])
                        i += 1
                    else:
                        break
                out.append(int(oct_digits, 8) & 0xFF)
            else:
                out.append(nxt)
                i += 2
        else:
            out.append(b)
            i += 1
    raw_bytes = bytes(out)
    if raw_bytes.startswith(b"\xfe\xff"):
        return raw_bytes[2:].decode("utf-16-be", errors="replace")
    try:
        return raw_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1", errors="replace")


def _decode_pdf_hex(hex_bytes: bytes) -> str:
    cleaned = re.sub(rb"\s+", b"", hex_bytes)
    if not cleaned:
        return ""
    if len(cleaned) % 2 == 1:
        cleaned += b"0"
    try:
        raw = bytes.fromhex(cleaned.decode("ascii"))
    except (ValueError, UnicodeDecodeError):
        return ""
    if raw.startswith(b"\xfe\xff"):
        return raw[2:].decode("utf-16-be", errors="replace")
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="replace")


def _extract_pdf_text_operators(stream_data: bytes, *, deadline: float | None = None) -> list[str]:
    fragments: list[str] = []
    for bt_match in _PDF_BT_ET_RE.finditer(stream_data):
        _check_deadline(deadline)
        block = bt_match.group(1)
        i = 0
        n = len(block)
        line_parts: list[str] = []
        while i < n:
            ch = block[i]
            if ch == ord("("):
                depth = 1
                start = i + 1
                i += 1
                while i < n and depth > 0:
                    if block[i] == 0x5C:
                        i += 2
                        continue
                    if block[i] == ord("("):
                        depth += 1
                    elif block[i] == ord(")"):
                        depth -= 1
                    i += 1
                end = i - 1 if depth == 0 else n
                # Peek ahead to ensure a text-showing operator or array follows
                decoded = _decode_pdf_literal(block[start:end])
                if decoded:
                    line_parts.append(decoded)
            elif ch == ord("<") and (i + 1 >= n or block[i + 1] != ord("<")):
                end = block.find(b">", i + 1)
                if end == -1:
                    break
                decoded = _decode_pdf_hex(block[i + 1 : end])
                if decoded:
                    line_parts.append(decoded)
                i = end + 1
            else:
                i += 1
        joined = "".join(line_parts).strip()
        if joined:
            fragments.append(joined)
    return fragments


def _parse_pdf_stdlib_fallback(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    """Bounded pure-Python PDF text extractor used when optional `pypdf` is absent."""
    if b"/Encrypt" in raw:
        raise DocumentParseError("pdf_parse_failed", "encrypted pdf unsupported")
    has_structure = (
        b"endobj" in raw or b"%%EOF" in raw or bool(_PDF_PAGE_RE.search(raw)) or b"BT" in raw
    )
    if not has_structure:
        raise DocumentParseError("pdf_parse_failed", "malformed pdf structure")

    page_matches = _PDF_PAGE_RE.findall(raw)
    total_pages = len(page_matches) if page_matches else 1
    pages_truncated = total_pages > MAX_PDF_PAGES

    texts: list[str] = []
    extracted_chars = 0
    text_streams_seen = 0
    streams_scanned = 0
    max_scan_streams = MAX_PDF_PAGES * 4

    for match in _PDF_STREAM_RE.finditer(raw):
        _check_deadline(deadline)
        if streams_scanned >= max_scan_streams or text_streams_seen >= MAX_PDF_PAGES:
            pages_truncated = True
            break
        streams_scanned += 1
        stream_bytes = match.group(1)
        header_window = raw[max(0, match.start() - 512) : match.start()]
        if b"/FlateDecode" in header_window:
            try:
                decompressor = zlib.decompressobj()
                content = decompressor.decompress(stream_bytes, MAX_PDF_STREAM_BYTES + 1)
                if len(content) > MAX_PDF_STREAM_BYTES or decompressor.unconsumed_tail:
                    raise DocumentParseError("pdf_parse_failed", "pdf stream bomb rejected")
            except DocumentParseError:
                raise
            except zlib.error:
                continue
        else:
            content = stream_bytes[:MAX_PDF_STREAM_BYTES]

        stream_frags = _extract_pdf_text_operators(content, deadline=deadline)
        if stream_frags:
            text_streams_seen += 1
            for frag in stream_frags:
                texts.append(frag)
                extracted_chars += len(frag) + 1
                if extracted_chars >= MAX_PDF_EXTRACT_CHARS:
                    pages_truncated = True
                    break
        if extracted_chars >= MAX_PDF_EXTRACT_CHARS:
            break

    if not texts and b"BT" in raw and b"ET" in raw:
        for frag in _extract_pdf_text_operators(raw, deadline=deadline):
            texts.append(frag)
            extracted_chars += len(frag) + 1
            if extracted_chars >= MAX_PDF_EXTRACT_CHARS:
                pages_truncated = True
                break

    if not texts and not page_matches and b"endobj" not in raw:
        raise DocumentParseError("pdf_parse_failed", "unreadable pdf")

    combined = _clean_extracted_text("\n".join(texts))
    if not combined:
        combined = f"[PDF 文档共 {total_pages} 页，未提取到可复制文本（可能为扫描件或纯图片）]"
    excerpt, excerpt_truncated = _excerpt_text(combined, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding="utf-8",
        format="pdf",
        truncated=pages_truncated or excerpt_truncated,
        pages=total_pages,
    )


def _preflight_pdf_flate_streams(raw: bytes, *, deadline: float | None = None) -> None:
    """Bounded FlateDecode pre-scan defending both pypdf and fallback paths against stream bombs."""
    total_decompressed = 0
    flate_streams = 0
    for match in _PDF_STREAM_RE.finditer(raw):
        _check_deadline(deadline)
        header_window = raw[max(0, match.start() - 512) : match.start()]
        if b"/FlateDecode" not in header_window:
            continue
        flate_streams += 1
        if flate_streams > MAX_PDF_PREFLIGHT_STREAMS:
            raise DocumentParseError("pdf_parse_failed", "too many pdf compressed streams")
        stream_bytes = match.group(1)
        remaining = MAX_PDF_TOTAL_DECOMPRESSED_BYTES - total_decompressed
        stream_cap = min(MAX_PDF_STREAM_BYTES, remaining)
        if stream_cap < 0:
            raise DocumentParseError("pdf_parse_failed", "pdf total stream bomb rejected")
        try:
            decompressor = zlib.decompressobj()
            content = decompressor.decompress(stream_bytes, stream_cap + 1)
            if len(content) > stream_cap or decompressor.unconsumed_tail:
                raise DocumentParseError("pdf_parse_failed", "pdf stream bomb rejected")
            total_decompressed += len(content)
            if total_decompressed > MAX_PDF_TOTAL_DECOMPRESSED_BYTES:
                raise DocumentParseError("pdf_parse_failed", "pdf total stream bomb rejected")
        except DocumentParseError:
            raise
        except zlib.error:
            continue


def _tighten_pypdf_zlib_cap(pypdf_mod: Any) -> None:
    """Clamp `pypdf.filters.ZLIB_MAX_OUTPUT_LENGTH` (default 75 MiB) to MAX_PDF_STREAM_BYTES."""
    filters_mod = getattr(pypdf_mod, "filters", None)
    if filters_mod is None:
        try:
            import pypdf.filters as filters_mod  # type: ignore[import-not-found]
        except ImportError:
            filters_mod = None
    if filters_mod is not None and hasattr(filters_mod, "ZLIB_MAX_OUTPUT_LENGTH"):
        current = getattr(filters_mod, "ZLIB_MAX_OUTPUT_LENGTH", None)
        if not isinstance(current, int) or current <= 0 or current > MAX_PDF_STREAM_BYTES:
            filters_mod.ZLIB_MAX_OUTPUT_LENGTH = MAX_PDF_STREAM_BYTES


def _parse_pdf(
    raw: bytes,
    *,
    max_chars: int,
    deadline: float | None = None,
) -> ParsedDocument:
    if not raw or b"%PDF-" not in raw[:1024]:
        raise DocumentParseError("invalid_document", "missing pdf magic header")

    _preflight_pdf_flate_streams(raw, deadline=deadline)

    try:
        import pypdf  # type: ignore[import-not-found]
    except ImportError:
        return _parse_pdf_stdlib_fallback(raw, max_chars=max_chars, deadline=deadline)

    _tighten_pypdf_zlib_cap(pypdf)

    try:
        reader = pypdf.PdfReader(io.BytesIO(raw), strict=False)
        if reader.is_encrypted:
            try:
                decrypted = reader.decrypt("")
            except Exception:
                decrypted = 0
            if not decrypted:
                raise DocumentParseError("pdf_parse_failed", "encrypted pdf")
        total_pages = len(reader.pages)
        if total_pages <= 0:
            raise DocumentParseError("pdf_parse_failed", "empty pdf")

        pages_truncated = total_pages > MAX_PDF_PAGES
        parts: list[str] = []
        extracted_chars = 0
        for idx in range(min(total_pages, MAX_PDF_PAGES)):
            _check_deadline(deadline)
            page_text = (reader.pages[idx].extract_text() or "").strip()
            if page_text:
                parts.append(page_text)
                extracted_chars += len(page_text) + 1
                if extracted_chars >= MAX_PDF_EXTRACT_CHARS:
                    pages_truncated = True
                    break
    except DocumentParseError:
        raise
    except Exception:
        raise DocumentParseError("pdf_parse_failed") from None

    combined = _clean_extracted_text("\n\n".join(parts))
    if not combined:
        combined = f"[PDF 文档共 {total_pages} 页，未提取到可复制文本（可能为扫描件或纯图片）]"
    excerpt, excerpt_truncated = _excerpt_text(combined, max_chars)
    return ParsedDocument(
        content=excerpt,
        char_count=len(excerpt),
        byte_size=len(raw),
        encoding="utf-8",
        format="pdf",
        truncated=pages_truncated or excerpt_truncated,
        pages=total_pages,
    )


def parse_document_bytes(
    raw: bytes,
    filename: str,
    *,
    mime_type: str = "",
    max_chars: int = DEFAULT_MAX_CHARS,
    max_bytes: int = MAX_DOCUMENT_BYTES,
    timeout_seconds: float | None = None,
) -> ParsedDocument:
    """Parse bounded in-memory document bytes with strict format and security validation."""
    del mime_type  # Untrusted hint; trust validated extension + magic bytes only.
    if not isinstance(raw, (bytes, bytearray)):
        raise DocumentParseError("invalid_document", "raw payload must be bytes")
    raw_bytes = bytes(raw)
    if isinstance(max_chars, bool) or not isinstance(max_chars, int):
        raise DocumentParseError("invalid_document", "invalid max_chars")
    if not (MIN_MAX_CHARS <= max_chars <= MAX_MAX_CHARS):
        raise DocumentParseError("invalid_document", "max_chars out of range")

    _, doc_kind = validate_document_filename(filename)
    hard_cap = min(max(1, int(max_bytes)), MAX_DOCUMENT_BYTES)
    if doc_kind == "text":
        hard_cap = min(hard_cap, MAX_TEXT_BYTES)
    if len(raw_bytes) > hard_cap:
        raise DocumentParseError("oversized")

    deadline = (
        time.monotonic() + float(timeout_seconds)
        if timeout_seconds is not None and timeout_seconds > 0
        else None
    )

    if doc_kind == "text":
        return _parse_plain_text(raw_bytes, max_chars=max_chars, deadline=deadline)
    if doc_kind == "pdf":
        return _parse_pdf(raw_bytes, max_chars=max_chars, deadline=deadline)
    if doc_kind == "docx":
        return _parse_docx(raw_bytes, max_chars=max_chars, deadline=deadline)
    if doc_kind == "xlsx":
        return _parse_xlsx(raw_bytes, max_chars=max_chars, deadline=deadline)
    if doc_kind == "pptx":
        return _parse_pptx(raw_bytes, max_chars=max_chars, deadline=deadline)
    raise DocumentParseError("unsupported_extension")
