"""Tests for out-of-process `doc_render` plugin, security guards, and kernel bridges."""

from __future__ import annotations

import ast
import asyncio
import base64
import contextlib
import io
import random
import sys
import types
import zipfile
import zlib
from pathlib import Path
from typing import Any

import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.file_reader import FileReader
from hakimi.models import FileRef
from hakimi.plugin_bus import PluginBus
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected
from hakimi.text_card import (
    configure_remote_bridge,
    render_card,
    render_card_async,
)
from plugins.doc_render import (
    MAX_PDF_PAGES,
    MAX_PDF_STREAM_BYTES,
    MAX_RENDER_PIXELS,
    MAX_ZIP_ENTRIES,
    CardRenderError,
    DocumentParseError,
    apply_pillow_pixel_limit,
    parse_document_bytes,
    render_card_bytes,
)
from plugins.doc_render.plugin_main import DocRenderPlugin, build_doc_render_plugin_from_env
from tests.test_admin import TOKEN

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# In-memory test document builders (zero disk writes)
# ---------------------------------------------------------------------------


def _build_zip_bytes(
    entries: dict[str, bytes | str],
    *,
    compression: int = zipfile.ZIP_DEFLATED,
    symlink_entries: set[str] | None = None,
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compression) as zf:
        for name, data in entries.items():
            raw = data.encode("utf-8") if isinstance(data, str) else data
            info = zipfile.ZipInfo(name)
            info.compress_type = compression
            if symlink_entries and name in symlink_entries:
                info.external_attr = (0o120777 << 16) | 0o10
            zf.writestr(info, raw)
    return buf.getvalue()


def _build_docx_bytes(
    paragraphs: list[str],
    *,
    table_rows: list[list[str]] | None = None,
    header_text: str | None = None,
    extra_entries: dict[str, bytes | str] | None = None,
) -> bytes:
    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    body_parts = [f"<w:p><w:r><w:t>{p}</w:t></w:r></w:p>" for p in paragraphs]
    if table_rows:
        rows_xml = []
        for row in table_rows:
            cells = "".join(f"<w:tc><w:p><w:r><w:t>{c}</w:t></w:r></w:p></w:tc>" for c in row)
            rows_xml.append(f"<w:tr>{cells}</w:tr>")
        body_parts.append(f"<w:tbl>{''.join(rows_xml)}</w:tbl>")
    doc_xml = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<w:document xmlns:w="{ns}"><w:body>{"".join(body_parts)}</w:body></w:document>'
    )
    entries: dict[str, bytes | str] = {"word/document.xml": doc_xml}
    if header_text:
        entries["word/header1.xml"] = (
            f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<w:hdr xmlns:w="{ns}"><w:p><w:r><w:t>{header_text}</w:t></w:r></w:p></w:hdr>'
        )
    if extra_entries:
        entries.update(extra_entries)
    return _build_zip_bytes(entries)


def _build_xlsx_bytes() -> bytes:
    ns_ss = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    ns_r = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    workbook = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<workbook xmlns="{ns_ss}" xmlns:r="{ns_r}">'
        f"<sheets>"
        f'<sheet name="预算表" sheetId="1" r:id="rId1"/>'
        f'<sheet name="备注页" sheetId="2" r:id="rId2"/>'
        f"</sheets></workbook>"
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Target="worksheets/sheet2.xml"/>'
        "</Relationships>"
    )
    shared_strings = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<sst xmlns="{ns_ss}" count="3" uniqueCount="3">'
        f"<si><t>项目</t></si>"
        f"<si><t>金额</t></si>"
        f"<si><t>云服务器</t></si>"
        f"</sst>"
    )
    sheet1 = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<worksheet xmlns="{ns_ss}"><sheetData>'
        f'<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>'
        f'<row r="2"><c r="A2" t="s"><v>2</v></c>'
        f'<c r="B2"><v>128</v></c><c r="C2" t="b"><v>1</v></c></row>'
        f"</sheetData></worksheet>"
    )
    sheet2 = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<worksheet xmlns="{ns_ss}"><sheetData>'
        f'<row r="1"><c r="A1" t="inlineStr"><is><t>季度复盘</t></is></c></row>'
        f"</sheetData></worksheet>"
    )
    return _build_zip_bytes(
        {
            "xl/workbook.xml": workbook,
            "xl/_rels/workbook.xml.rels": rels,
            "xl/sharedStrings.xml": shared_strings,
            "xl/worksheets/sheet1.xml": sheet1,
            "xl/worksheets/sheet2.xml": sheet2,
        }
    )


def _build_pptx_bytes() -> bytes:
    ns_a = "http://schemas.openxmlformats.org/drawingml/2006/main"
    ns_p = "http://schemas.openxmlformats.org/presentationml/2006/main"
    slide1 = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<p:sld xmlns:p="{ns_p}" xmlns:a="{ns_a}">'
        f"<p:cSld><p:spTree><p:sp><p:txBody>"
        f"<a:p><a:r><a:t>Wave 3 架构演进</a:t></a:r></a:p>"
        f"<a:p><a:r><a:t>隔离文档解析与渲染</a:t></a:r></a:p>"
        f"</p:txBody></p:sp></p:spTree></p:cSld></p:sld>"
    )
    slide2 = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<p:sld xmlns:p="{ns_p}" xmlns:a="{ns_a}">'
        f"<p:cSld><p:spTree><p:sp><p:txBody>"
        f"<a:p><a:r><a:t>第二页总结</a:t></a:r></a:p>"
        f"</p:txBody></p:sp></p:spTree></p:cSld></p:sld>"
    )
    notes1 = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<p:notes xmlns:p="{ns_p}" xmlns:a="{ns_a}">'
        f"<p:cSld><p:spTree><p:sp><p:txBody>"
        f"<a:p><a:r><a:t>讲者备注提醒</a:t></a:r></a:p>"
        f"</p:txBody></p:sp></p:spTree></p:cSld></p:notes>"
    )
    return _build_zip_bytes(
        {
            "ppt/slides/slide1.xml": slide1,
            "ppt/slides/slide2.xml": slide2,
            "ppt/notesSlides/notesSlide1.xml": notes1,
        }
    )


def _build_minimal_pdf(pages_text: list[str], *, compress: bool = False) -> bytes:
    """Build a valid lightweight PDF with uncompressed or FlateDecode content streams."""
    objs: list[bytes] = []
    objs.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
    kid_refs = " ".join(f"{3 + i * 2} 0 R" for i in range(len(pages_text)))
    objs.append(
        f"2 0 obj\n<< /Type /Pages /Kids [{kid_refs}] /Count {len(pages_text)} >>\nendobj\n".encode(
            "ascii"
        )
    )
    for idx, text in enumerate(pages_text):
        page_obj_id = 3 + idx * 2
        content_obj_id = page_obj_id + 1
        objs.append(
            f"{page_obj_id} 0 obj\n"
            f"<< /Type /Page /Parent 2 0 R /Contents {content_obj_id} 0 R >>\n"
            f"endobj\n".encode("ascii")
        )
        if text.isascii():
            escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            stream_body = f"BT /F1 12 Tf ({escaped}) Tj ET".encode("ascii")
        else:
            hex_utf16 = (b"\xfe\xff" + text.encode("utf-16-be")).hex().upper()
            stream_body = f"BT /F1 12 Tf <{hex_utf16}> Tj ET".encode("ascii")
        if compress:
            payload = zlib.compress(stream_body)
            header = f"<< /Length {len(payload)} /Filter /FlateDecode >>".encode("ascii")
        else:
            payload = stream_body
            header = f"<< /Length {len(payload)} >>".encode("ascii")
        objs.append(
            f"{content_obj_id} 0 obj\n".encode("ascii")
            + header
            + b"\nstream\n"
            + payload
            + b"\nendstream\nendobj\n"
        )
    return b"%PDF-1.4\n" + b"".join(objs) + b"%%EOF\n"


# ---------------------------------------------------------------------------
# In-memory duplex WebSocket harness for real PluginBus <-> DocRenderPlugin
# ---------------------------------------------------------------------------


class _PipeClientSide:
    def __init__(self, pipe: _BiPipe) -> None:
        self._pipe = pipe
        self.close_code: int | None = None

    async def send(self, raw: str) -> None:
        await self._pipe.c2s.put(raw)

    async def recv(self) -> str:
        item = await self._pipe.s2c.get()
        if item.startswith("__EOF__:"):
            self.close_code = int(item.split(":", 1)[1])
            raise ConnectionError(item)
        return item

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        await self._pipe.close_from_client(code)


class _BiPipe:
    def __init__(self, bus: PluginBus, plugin_id: str, *, source_ip: str = "127.0.0.1") -> None:
        self.bus = bus
        self.plugin_id = plugin_id
        self.source_ip = source_ip
        self.c2s: asyncio.Queue[str] = asyncio.Queue()
        self.s2c: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.server_task: asyncio.Task[None] | None = None

    async def server_recv(self) -> str:
        item = await self.c2s.get()
        if item.startswith("__EOF__:"):
            raise ConnectionError(item)
        return item

    async def server_send(self, raw: str) -> None:
        if not self.closed:
            await self.s2c.put(raw)

    async def server_close(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.s2c.put(f"__EOF__:{code}")
        await self.c2s.put(f"__EOF__:{code}")

    async def close_from_client(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.c2s.put(f"__EOF__:{code}")
        await self.s2c.put(f"__EOF__:{code}")

    def connect_factory(self, _url: str, _headers: dict[str, str]) -> _PipeClientSide:
        self.server_task = asyncio.create_task(
            self.bus.serve(
                self.server_recv,
                self.server_send,
                self.server_close,
                source_ip=self.source_ip,
                plugin_id=self.plugin_id,
            )
        )
        return _PipeClientSide(self)

    async def shutdown(self) -> None:
        await self.close_from_client(1000)
        if self.server_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(self.server_task, timeout=2.0)


async def _wait_until(predicate: Any, *, max_wait: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + max_wait
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not met within max_wait")
        await asyncio.sleep(0.01)  # noqa: ASYNC110


@pytest.fixture
async def doc_render_bus_env(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
        }
    )
    app = create_app(s, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus: PluginBus = svc.plugin_bus
        await bus.initialize()
        token, _ = await bus.create_plugin(
            "doc_render",
            "文档解析与卡片渲染",
            ["actions"],
            node_region="KR",
        )
        pipe = _BiPipe(bus, "doc_render")
        plugin = DocRenderPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token=token,
            connect_factory=pipe.connect_factory,
        )
        await plugin.client.start()
        try:
            await _wait_until(lambda: bus.has_tool("doc_render", "doc_render.read_file"))
            await _wait_until(lambda: bus.has_tool("doc_render", "doc_render.render_card"))
            yield svc, bus, plugin, pipe
        finally:
            configure_remote_bridge(None)
            await plugin.close()
            await pipe.shutdown()


# ---------------------------------------------------------------------------
# 1. Document parser unit tests (Text / PDF / DOCX / XLSX / PPTX)
# ---------------------------------------------------------------------------


def test_parse_plain_text_encodings_special_names_and_truncation():
    utf8_doc = parse_document_bytes("你好世界\r\n第二行".encode(), "notes.md")
    assert utf8_doc.format == "text"
    assert utf8_doc.encoding == "utf-8"
    assert utf8_doc.content == "你好世界\n第二行"
    assert not utf8_doc.truncated

    gb_doc = parse_document_bytes("中文日志报错".encode("gb18030"), "error.log")
    assert gb_doc.encoding == "gb18030"
    assert gb_doc.content == "中文日志报错"

    special = parse_document_bytes(b"FROM python:3.12-slim\n", "Dockerfile")
    assert "FROM python:3.12-slim" in special.content

    long_text = ("HEAD-" + "A" * 1500) + "\n" + ("M" * 2000) + "\nTAIL-MARKER"
    truncated_doc = parse_document_bytes(long_text.encode("utf-8"), "crash.log")
    assert truncated_doc.truncated is True
    assert "HEAD-" in truncated_doc.content
    assert "中间省略" in truncated_doc.content
    assert "TAIL-MARKER" in truncated_doc.content


def test_parse_office_docx_xlsx_pptx():
    docx_raw = _build_docx_bytes(
        ["第一段：项目目标", "第二段：性能指标"],
        table_rows=[["模块", "状态"], ["doc_render", "就绪"]],
        header_text="内部报告页眉",
    )
    docx_res = parse_document_bytes(docx_raw, "report.docx")
    assert docx_res.format == "docx"
    assert "第一段：项目目标" in docx_res.content
    assert "doc_render | 就绪" in docx_res.content
    assert "内部报告页眉" in docx_res.content

    xlsx_raw = _build_xlsx_bytes()
    xlsx_res = parse_document_bytes(xlsx_raw, "budget.xlsx")
    assert xlsx_res.format == "xlsx"
    assert xlsx_res.pages == 2
    assert "[Sheet: 预算表]" in xlsx_res.content
    assert "云服务器 | 128 | TRUE" in xlsx_res.content
    assert "[Sheet: 备注页]" in xlsx_res.content
    assert "季度复盘" in xlsx_res.content

    pptx_raw = _build_pptx_bytes()
    pptx_res = parse_document_bytes(pptx_raw, "deck.pptx")
    assert pptx_res.format == "pptx"
    assert pptx_res.pages == 2
    assert "[Slide 1]" in pptx_res.content
    assert "Wave 3 架构演进" in pptx_res.content
    assert "讲者备注提醒" in pptx_res.content
    assert "[Slide 2]" in pptx_res.content
    assert "第二页总结" in pptx_res.content


def test_parse_pdf_uncompressed_flate_multipage_cap_and_pypdf_path(monkeypatch):
    pdf_plain = _build_minimal_pdf(["Hello PDF World", "中文第二页内容"], compress=False)
    res_plain = parse_document_bytes(pdf_plain, "spec.pdf")
    assert res_plain.format == "pdf"
    assert res_plain.pages == 2
    assert "Hello PDF World" in res_plain.content
    assert "中文第二页内容" in res_plain.content
    assert not res_plain.truncated

    pdf_flate = _build_minimal_pdf(["Compressed FlateDecode Stream OK"], compress=True)
    res_flate = parse_document_bytes(pdf_flate, "compressed.pdf")
    assert res_flate.format == "pdf"
    assert "Compressed FlateDecode Stream OK" in res_flate.content

    # Enforce MAX_PDF_PAGES cap (55 pages -> truncated, page 55 excluded)
    many_pages = [f"PageToken-{i:03d}" for i in range(1, MAX_PDF_PAGES + 6)]
    pdf_many = _build_minimal_pdf(many_pages, compress=False)
    res_many = parse_document_bytes(pdf_many, "big.pdf", max_chars=16_000)
    assert res_many.pages == MAX_PDF_PAGES + 5
    assert res_many.truncated is True
    assert "PageToken-001" in res_many.content
    assert f"PageToken-{MAX_PDF_PAGES + 5:03d}" not in res_many.content

    # Corrupt PDF rejected cleanly
    with pytest.raises(DocumentParseError) as exc_corrupt:
        parse_document_bytes(b"%PDF-1.4\nnot-a-valid-pdf", "broken.pdf")
    assert exc_corrupt.value.code == "pdf_parse_failed"

    with pytest.raises(DocumentParseError) as exc_magic:
        parse_document_bytes(b"NOTPDFDATA", "fake.pdf")
    assert exc_magic.value.code == "invalid_document"

    # FlateDecode stream bomb (> 8 MiB decompressed) rejected cleanly
    bomb_stream = zlib.compress(b"A" * (9 * 1024 * 1024))
    pdf_bomb = (
        b"%PDF-1.4\n1 0 obj\n<< /Type /Page >>\nendobj\n"
        b"2 0 obj\n<< /Filter /FlateDecode /Length "
        + str(len(bomb_stream)).encode("ascii")
        + b" >>\nstream\n"
        + bomb_stream
        + b"\nendstream\nendobj\n%%EOF\n"
    )
    with pytest.raises(DocumentParseError) as exc_pdf_bomb:
        parse_document_bytes(pdf_bomb, "stream_bomb.pdf")
    assert exc_pdf_bomb.value.code == "pdf_parse_failed"

    # Exercise primary `pypdf` codepath using a stub module + filters zlib cap + stream bomb guard
    reader_calls = 0

    class _FakePage:
        def __init__(self, txt: str) -> None:
            self._txt = txt

        def extract_text(self) -> str:
            return self._txt

    class _FakePdfReader:
        def __init__(self, _stream: Any, strict: bool = False) -> None:
            nonlocal reader_calls
            reader_calls += 1
            self.is_encrypted = False
            self.pages = [_FakePage("pypdf 第一页"), _FakePage("pypdf 第二页")]

    fake_filters = types.ModuleType("pypdf.filters")
    fake_filters.ZLIB_MAX_OUTPUT_LENGTH = 75_000_000  # type: ignore[attr-defined]
    fake_pypdf = types.ModuleType("pypdf")
    fake_pypdf.PdfReader = _FakePdfReader  # type: ignore[attr-defined]
    fake_pypdf.filters = fake_filters  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pypdf", fake_pypdf)
    monkeypatch.setitem(sys.modules, "pypdf.filters", fake_filters)

    # Single-stream bomb blocked BEFORE PdfReader is even instantiated
    with pytest.raises(DocumentParseError) as exc_pypdf_bomb:
        parse_document_bytes(pdf_bomb, "stream_bomb_pypdf.pdf")
    assert exc_pypdf_bomb.value.code == "pdf_parse_failed"
    assert reader_calls == 0

    # Multi-stream cumulative bomb (5 x 7 MiB = 35 MiB > 32 MiB) blocked BEFORE PdfReader
    chunk_7m = zlib.compress(b"B" * (7 * 1024 * 1024))
    multi_objs = b"".join(
        f"{i + 2} 0 obj\n<< /Filter /FlateDecode /Length {len(chunk_7m)} >>\nstream\n".encode(
            "ascii"
        )
        + chunk_7m
        + b"\nendstream\nendobj\n"
        for i in range(5)
    )
    pdf_multi_bomb = b"%PDF-1.4\n1 0 obj\n<< /Type /Page >>\nendobj\n" + multi_objs + b"%%EOF\n"
    with pytest.raises(DocumentParseError) as exc_multi_bomb:
        parse_document_bytes(pdf_multi_bomb, "multi_stream_bomb.pdf")
    assert exc_multi_bomb.value.code == "pdf_parse_failed"
    assert reader_calls == 0

    res_pypdf = parse_document_bytes(b"%PDF-1.7\n%stub\n", "via_pypdf.pdf")
    assert reader_calls == 1
    assert fake_filters.ZLIB_MAX_OUTPUT_LENGTH == MAX_PDF_STREAM_BYTES
    assert res_pypdf.pages == 2
    assert "pypdf 第一页" in res_pypdf.content
    assert "pypdf 第二页" in res_pypdf.content


# ---------------------------------------------------------------------------
# 2. Security defense tests: ZIP bomb, XML XXE/Billion Laughs, path traversal, timeout
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_name,expected_code",
    [
        ("../../etc/passwd", "invalid_document"),
        ("/etc/passwd.txt", "invalid_document"),
        ("C:\\Windows\\win.ini", "invalid_document"),
        ("sub/dir/notes.txt", "invalid_document"),
        ("bad\x00name.txt", "invalid_document"),
        ("app.exe", "unsupported_extension"),
        ("archive.zip", "unsupported_extension"),
        (".env", "unsupported_extension"),
        (".env.prod", "unsupported_extension"),
        ("legacy.doc", "unsupported_extension"),
    ],
)
def test_parser_blocks_malicious_paths_and_disallowed_extensions(bad_name, expected_code):
    with pytest.raises(DocumentParseError) as exc_info:
        parse_document_bytes(b"hello world", bad_name)
    assert exc_info.value.code == expected_code


def test_parser_blocks_oversized_and_binary_and_bad_encoding():
    with pytest.raises(DocumentParseError) as exc_big:
        parse_document_bytes(b"A" * 1024, "note.txt", max_bytes=512)
    assert exc_big.value.code == "oversized"

    with pytest.raises(DocumentParseError) as exc_bin:
        parse_document_bytes(b"\x00\x01\x02ELF", "note.txt")
    assert exc_bin.value.code == "binary_unsupported"

    with pytest.raises(DocumentParseError) as exc_enc:
        parse_document_bytes(b"\xff\xfe\xfd", "note.txt")
    assert exc_enc.value.code == "encoding_unsupported"


def test_parser_blocks_zip_bombs_entry_count_ratio_path_traversal_and_symlinks():
    # 1. Compression ratio bomb (12 MiB zeros compresses to ~12 KiB -> ratio ~1000x > 100x)
    bomb_payload = b"0" * (12 * 1024 * 1024)
    ratio_bomb = _build_zip_bytes(
        {
            "word/document.xml": (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                "<w:body><w:p><w:r><w:t>ok</w:t></w:r></w:p></w:body></w:document>"
            ),
            "word/bomb.bin": bomb_payload,
        }
    )
    with pytest.raises(DocumentParseError) as exc_ratio:
        parse_document_bytes(ratio_bomb, "bomb.docx")
    assert exc_ratio.value.code == "zip_bomb_detected"

    # 2. Entry count bomb (> MAX_ZIP_ENTRIES)
    many_entries = {f"word/f{i}.txt": b"x" for i in range(MAX_ZIP_ENTRIES + 5)}
    entry_bomb = _build_zip_bytes(many_entries)
    with pytest.raises(DocumentParseError) as exc_entries:
        parse_document_bytes(entry_bomb, "entries.docx")
    assert exc_entries.value.code == "zip_bomb_detected"

    # 3. ZIP entry path traversal
    traversal_zip = _build_zip_bytes(
        {
            "../etc/passwd": b"root:x:0:0",
            "word/document.xml": "<w:document/>",
        }
    )
    with pytest.raises(DocumentParseError) as exc_trav:
        parse_document_bytes(traversal_zip, "evil.docx")
    assert exc_trav.value.code == "invalid_document"

    # 4. ZIP symlink entry
    symlink_zip = _build_zip_bytes(
        {"word/document.xml": b"/etc/passwd"},
        symlink_entries={"word/document.xml"},
    )
    with pytest.raises(DocumentParseError) as exc_sym:
        parse_document_bytes(symlink_zip, "symlink.docx")
    assert exc_sym.value.code == "invalid_document"


def test_parser_blocks_xml_entity_expansion_xxe_and_excessive_depth():
    # 1. Billion Laughs internal entity expansion
    billion_laughs = (
        '<?xml version="1.0"?>'
        "<!DOCTYPE lolz ["
        '<!ENTITY lol "lol">'
        '<!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">'
        "]>"
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body><w:p><w:r><w:t>&lol2;</w:t></w:r></w:p></w:body></w:document>"
    )
    docx_lol = _build_zip_bytes({"word/document.xml": billion_laughs})
    with pytest.raises(DocumentParseError) as exc_lol:
        parse_document_bytes(docx_lol, "lol.docx")
    assert exc_lol.value.code == "xml_unsafe"

    # 2. XXE external entity attempt
    xxe_xml = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body><w:p><w:r><w:t>&xxe;</w:t></w:r></w:p></w:body></w:document>"
    )
    docx_xxe = _build_zip_bytes({"word/document.xml": xxe_xml})
    with pytest.raises(DocumentParseError) as exc_xxe:
        parse_document_bytes(docx_xxe, "xxe.docx")
    assert exc_xxe.value.code == "xml_unsafe"

    # 3. Excessive nesting depth (> MAX_XML_DEPTH)
    deep_open = "<a>" * 80
    deep_close = "</a>" * 80
    docx_deep = _build_zip_bytes({"word/document.xml": f"{deep_open}boom{deep_close}"})
    with pytest.raises(DocumentParseError) as exc_deep:
        parse_document_bytes(docx_deep, "deep.docx")
    assert exc_deep.value.code == "xml_unsafe"


# ---------------------------------------------------------------------------
# 3. Pillow card renderer tests (PNG/JPEG, themes, limits, MAX_IMAGE_PIXELS)
# ---------------------------------------------------------------------------


def test_render_card_bytes_png_jpeg_themes_and_limits(monkeypatch):
    markdown = (
        "# 排查报告\n\n"
        "请核对 `config.yaml` 中的**关键参数**：\n\n"
        "| 项 | 值 |\n"
        "| --- | --- |\n"
        "| port | 12141 |\n\n"
        "```python\n"
        "def hi():\n"
        "    return '你好'\n"
        "```\n"
        "> 提示信息\n"
        "- 检查状态\n"
    )
    original_pixel_cap = Image.MAX_IMAGE_PIXELS
    png_card = render_card_bytes(markdown, fmt="png", width=960, max_height=4400, theme="lavender")
    assert png_card.format == "png"
    assert png_card.mime_type == "image/png"
    assert png_card.image_bytes.startswith(b"\x89PNG\r\n\x1a\n")
    assert png_card.byte_size == len(png_card.image_bytes)
    # Rendering must NOT pollute process-wide PIL.Image.MAX_IMAGE_PIXELS in shared workers
    assert Image.MAX_IMAGE_PIXELS == original_pixel_cap

    # Explicit standalone-process helper clamps Pillow cap cleanly (isolated via monkeypatch)
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", original_pixel_cap)
    applied = apply_pillow_pixel_limit()
    assert applied == MAX_RENDER_PIXELS
    assert Image.MAX_IMAGE_PIXELS == MAX_RENDER_PIXELS

    with Image.open(io.BytesIO(png_card.image_bytes)) as img:
        assert img.width == 960
        assert 200 <= img.height <= 4400
        lavender_bg = img.getpixel((2, 2))

    dark_card = render_card_bytes(markdown, fmt="png", theme="dark")
    with Image.open(io.BytesIO(dark_card.image_bytes)) as img_dark:
        dark_bg = img_dark.getpixel((2, 2))
    assert dark_bg != lavender_bg

    jpg_card = render_card_bytes(markdown, fmt="jpeg", theme="light")
    assert jpg_card.image_bytes.startswith(b"\xff\xd8\xff")

    # Truncation when content exceeds max_height
    tall_text = "\n".join(f"第 {i} 行内容用于测试折叠截断行为" for i in range(120))
    tall_card = render_card_bytes(tall_text, fmt="png", max_height=480)
    assert tall_card.truncated is True
    assert tall_card.height <= 480

    # Validation & anti-abuse caps
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("   ")
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("A" * 8001)
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("ok", width=2000)
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("ok", max_height=9999)
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("ok", fmt="gif")
    with pytest.raises(CardRenderError, match="card_text_invalid"):
        render_card_bytes("ok", theme="neon")
    with pytest.raises(CardRenderError, match="card_too_large"):
        render_card_bytes(markdown, fmt="png", max_bytes=4096)


# ---------------------------------------------------------------------------
# 4. End-to-end PluginBus <-> DocRenderPlugin + FileReader & text_card routing
# ---------------------------------------------------------------------------


async def test_doc_render_plugin_end_to_end_chunked_blob_and_bridges(doc_render_bus_env, message):
    svc, bus, plugin, _pipe = doc_render_bus_env
    settings = svc.s
    store = svc.store

    # 4a. Direct PluginBus.invoke_tool with large (>128 KiB) chunked DOCX request blob
    incompressible_media = random.Random(42).randbytes(180_000)
    big_docx = _build_docx_bytes(
        ["大文档开头标记", "正文内容：" + ("测试句子" * 100)],
        extra_entries={"word/media/image1.png": incompressible_media},
    )
    assert len(big_docx) > 128 * 1024
    res_doc = await bus.invoke_tool(
        "doc_render",
        "doc_render.read_file",
        {"filename": "large_report.docx", "max_chars": 3200},
        blob=big_docx,
        blob_filename="large_report.docx",
        max_request_blob_bytes=15 * 1024 * 1024,
        max_response_blob_bytes=0,
    )
    assert res_doc.data["format"] == "docx"
    assert "大文档开头标记" in res_doc.data["content"]

    # 4b. Extra local path parameter is strictly forbidden (zero local path reads)
    with pytest.raises(Rejected, match="invalid_document"):
        await bus.invoke_tool(
            "doc_render",
            "doc_render.read_file",
            {"filename": "notes.txt", "path": "/etc/passwd"},
            blob=b"hello",
            max_request_blob_bytes=1024 * 1024,
        )

    # 4c. FileReader routes DOCX / PDF / TXT via online doc_render plugin
    class _BytesTransport:
        def __init__(self, mapping: dict[str, bytes]) -> None:
            self.mapping = mapping

        async def call(self, _action: str, params: dict[str, Any]) -> dict[str, Any]:
            raw = self.mapping[params["file_id"]]
            return {"base64": base64.b64encode(raw).decode("ascii")}

    secret_docx = _build_docx_bytes(["leak api_key=sk-1234567890abcdef1234567890"])
    good_pdf = _build_minimal_pdf(["PDF Remote Bridge Works"], compress=True)
    reader = FileReader(
        settings,
        store,
        transport=_BytesTransport(
            {
                "f-docx": big_docx,
                "f-pdf": good_pdf,
                "f-secret": secret_docx,
                "f-txt": b"plain text via remote",
            }
        ),
        plugin_bus=bus,
    )

    loaded_docx = await reader.read_one(
        FileRef(name="large_report.docx", file_id="f-docx", file_size=len(big_docx)),
        message,
        "u1",
    )
    assert loaded_docx.doc_format == "docx"
    assert "大文档开头标记" in loaded_docx.content

    loaded_pdf = await reader.read_one(
        FileRef(name="guide.pdf", file_id="f-pdf", file_size=len(good_pdf)),
        message,
        "u1",
    )
    assert loaded_pdf.doc_format == "pdf"
    assert loaded_pdf.pages == 1
    assert "PDF Remote Bridge Works" in loaded_pdf.content

    loaded_txt = await reader.read_one(
        FileRef(name="hello.txt", file_id="f-txt", file_size=21),
        message,
        "u1",
    )
    assert loaded_txt.content == "plain text via remote"

    # Kernel post-screening catches credential pattern extracted from DOCX!
    with pytest.raises(Rejected, match="credential_in_memory"):
        await reader.read_one(
            FileRef(name="secret.docx", file_id="f-secret", file_size=len(secret_docx)),
            message,
            "u1",
        )

    # 4d. text_card.render_card_async & sync render_card (via asyncio.to_thread) route to remote PNG
    code_lines = [
        f"line_{i} = '第 {i} 行代码，用来让 PNG 体积超过内联阈值测试分块传输'" for i in range(40)
    ]
    rich_md = (
        "# 远程渲染验收\n\n"
        "| 服务 | 状态 |\n| --- | --- |\n| doc_render | online |\n\n"
        "```python\n" + "\n".join(code_lines) + "\n```"
    )
    async_b64 = await render_card_async(rich_md, plugin_bus=bus, fmt="png")
    assert async_b64.startswith("base64://")
    async_raw = base64.b64decode(async_b64[9:])
    assert async_raw.startswith(b"\x89PNG\r\n\x1a\n")
    with Image.open(io.BytesIO(async_raw)) as img:
        assert img.width == 960

    # Long (>4096, <=8000 chars) card input works end-to-end across PluginBus.invoke_tool,
    # and explicitly exercises multi-chunk outbound `blob.get` when PNG exceeds inline cap.
    long_card_text = "# 长文卡片\n\n" + "".join(
        f"第{i:03d}段: key_{i}=val_{(i * 37) % 997}; " for i in range(210)
    )
    assert 4096 < len(long_card_text) <= 8000
    orig_client_cap = plugin.client.max_frame_bytes
    orig_bus_cap = settings.plugin_bus_ws_max_frame_bytes
    try:
        plugin.client.max_frame_bytes = 32_768
        settings.plugin_bus_ws_max_frame_bytes = 32_768
        long_b64 = await render_card_async(long_card_text, plugin_bus=bus, fmt="png")
    finally:
        plugin.client.max_frame_bytes = orig_client_cap
        settings.plugin_bus_ws_max_frame_bytes = orig_bus_cap
    long_raw = base64.b64decode(long_b64[9:])
    assert long_raw.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(long_raw) > 24 * 1024  # Exceeded 18 KiB inline cap -> chunked blob.get verified!

    configure_remote_bridge(bus)
    thread_b64 = await asyncio.to_thread(render_card, rich_md)
    assert thread_b64.startswith("base64://")
    thread_raw = base64.b64decode(thread_b64[9:])
    assert thread_raw.startswith(b"\x89PNG\r\n\x1a\n")

    # Sync render_card called directly on the event-loop thread must NOT deadlock (falls back)
    loop_thread_b64 = render_card("# 主循环线程防死锁测试")
    assert base64.b64decode(loop_thread_b64[9:]).startswith(b"\xff\xd8\xff")

    # Remote security rejection (e.g. XML bomb) must propagate cleanly, never swallowed
    xml_bomb_docx = _build_zip_bytes(
        {
            "word/document.xml": (
                '<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "boom">]><w:document/>'
            )
        }
    )
    bomb_reader = FileReader(
        settings,
        store,
        transport=_BytesTransport(
            {
                "f-xmlbomb": xml_bomb_docx,
                "f-botsecret": f"oops {settings.gemini_api_key.get_secret_value()}".encode(),
            }
        ),
        plugin_bus=bus,
    )
    with pytest.raises(Rejected, match="xml_unsafe"):
        await bomb_reader.read_one(
            FileRef(name="bomb.docx", file_id="f-xmlbomb", file_size=len(xml_bomb_docx)),
            message,
            "u-sec",
        )

    # Configured secret in plain-text file is blocked in-kernel BEFORE crossing WS
    with pytest.raises(Rejected, match="credential_in_input"):
        await bomb_reader.read_one(
            FileRef(name="leak.txt", file_id="f-botsecret", file_size=64),
            message,
            "u-sec",
        )

    # 4e. Disconnect doc_render -> verify smooth offline fallback and strict remote_only switch
    await plugin.close()
    await _wait_until(lambda: not bus.is_plugin_online("doc_render"))

    # Plain text smoothly falls back to local decode_text_bytes
    fallback_txt = await reader.read_one(
        FileRef(name="fallback.txt", file_id="f-txt", file_size=21),
        message,
        "u2",
    )
    assert fallback_txt.content == "plain text via remote"

    # Rich doc offline raises doc_render_remote_unavailable
    with pytest.raises(Rejected, match="doc_render_remote_unavailable"):
        await reader.read_one(
            FileRef(name="offline.pdf", file_id="f-pdf", file_size=len(good_pdf)),
            message,
            "u2",
        )

    # Strict remote_only blocks local fallback for plain text too
    strict_reader = FileReader(
        settings,
        store,
        transport=_BytesTransport({"f-txt": b"plain text"}),
        plugin_bus=bus,
        remote_only=True,
    )
    with pytest.raises(Rejected, match="doc_render_remote_unavailable"):
        await strict_reader.read_one(
            FileRef(name="strict.txt", file_id="f-txt", file_size=10),
            message,
            "u2",
        )

    # text_card falls back to local Pillow when offline (JPEG for sync render_card, PNG for async)
    fallback_sync_b64 = await asyncio.to_thread(render_card, "# 离线回退卡片")
    fallback_sync_raw = base64.b64decode(fallback_sync_b64[9:])
    assert fallback_sync_raw.startswith(b"\xff\xd8\xff")

    fallback_async_b64 = await render_card_async("# 离线回退 PNG", plugin_bus=bus, fmt="png")
    fallback_async_raw = base64.b64decode(fallback_async_b64[9:])
    assert fallback_async_raw.startswith(b"\x89PNG\r\n\x1a\n")

    with pytest.raises(Rejected, match="doc_render_remote_unavailable"):
        await render_card_async("# 严格模式", plugin_bus=bus, remote_only=True)


async def test_doc_render_parse_timeout_enforced(monkeypatch):
    import plugins.doc_render.plugin_main as pm

    def _slow_parse(*_args: Any, **_kwargs: Any) -> Any:
        import time

        time.sleep(0.4)
        raise AssertionError("should have timed out")

    monkeypatch.setattr(pm, "parse_document_bytes", _slow_parse)
    plugin = DocRenderPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok-timeout",
        parse_timeout_seconds=0.06,
    )
    try:
        from hakimi_plugin_sdk import PluginRpcError

        with pytest.raises(PluginRpcError) as exc_info:
            await plugin._handle_read_file(
                {"arguments": {"filename": "slow.txt"}, "blob": b"hello"}
            )
        assert exc_info.value.code == "parse_timeout"
    finally:
        await plugin.close()


def test_doc_render_env_builder_and_remote_only_env(monkeypatch, settings, store, message):
    monkeypatch.setenv("HAKIMI_PLUGIN_DOC_RENDER_TOKEN", "test-doc-token")
    monkeypatch.setenv("HAKIMI_PLUGIN_WS_URL", "ws://127.0.0.1:12141/ws/plugin")
    plugin = build_doc_render_plugin_from_env()
    assert plugin.client.config.plugin_id == "doc_render"
    assert plugin.client.config.scopes == ("actions",)
    assert set(plugin.client.actions) == {"doc_render.read_file", "doc_render.render_card"}

    monkeypatch.setenv("DOC_RENDER_REMOTE_ONLY", "true")
    configure_remote_bridge(None)
    with pytest.raises(Rejected, match="doc_render_remote_unavailable"):
        render_card("# blocked when offline")


# ---------------------------------------------------------------------------
# 5. Static security & Dockerfile audit tests
# ---------------------------------------------------------------------------


def test_doc_render_plugin_zero_network_and_zero_arbitrary_disk_open():
    forbidden_imports = {
        "httpx",
        "aiohttp",
        "requests",
        "urllib",
        "socket",
        "subprocess",
        "shutil",
    }
    plugin_dir = REPO_ROOT / "plugins" / "doc_render"
    py_files = sorted(plugin_dir.glob("*.py"))
    assert len(py_files) >= 4

    for path in py_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".", 1)[0]
                    assert top not in forbidden_imports, f"{path.name} imports forbidden {top}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".", 1)[0]
                assert top not in forbidden_imports, f"{path.name} imports forbidden {top}"
            if path.name == "parser.py" and isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "open":
                    raise AssertionError("parser.py must never call built-in open()")
                if isinstance(node.func, ast.Attribute) and node.func.attr in {
                    "open",
                    "read_bytes",
                    "read_text",
                }:
                    # Allow only ZipFile.open() on in-memory BytesIO archive
                    if not (
                        node.func.attr == "open"
                        and isinstance(node.func.value, ast.Attribute)
                        and node.func.value.attr == "_zf"
                    ):
                        raise AssertionError(
                            f"parser.py calls forbidden file method .{node.func.attr}()"
                        )


def test_doc_render_dockerfile_security_hardening():
    dockerfile = REPO_ROOT / "plugins" / "doc_render" / "Dockerfile"
    assert dockerfile.is_file()
    content = dockerfile.read_text(encoding="utf-8")
    assert "FROM python:3.12-slim@sha256:" in content
    assert "fonts-wqy-zenhei" in content
    assert "pillow" in content.lower()
    assert "pypdf" in content
    assert "python-docx" in content
    assert "openpyxl" in content
    assert "USER 10001:10001" in content
    assert "HOME=/tmp" in content
    assert "TMPDIR=/tmp" in content
    assert 'CMD ["python", "-m", "plugins.doc_render.plugin_main"]' in content
    assert "httpx" not in content
    assert "secrets" not in content
