"""Assemble and statically audit admin/admin_page.html from admin/src/."""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
from html.parser import HTMLParser
from pathlib import Path

ADMIN_DIR = Path(__file__).resolve().parent
REPO_ROOT = ADMIN_DIR.parent
SRC_DIR = ADMIN_DIR / "src"
CSS_DIR = SRC_DIR / "css"
VIEWS_DIR = SRC_DIR / "views"
JS_DIR = SRC_DIR / "js"
OUT_FILE = ADMIN_DIR / "admin_page.html"
INIT_FILE = REPO_ROOT / "src" / "hakimi" / "__init__.py"
CSP_FILE = ADMIN_DIR / "csp.py"

VIEW_ORDER = (
    "overview.html",
    "modules.html",
    "ledger.html",
    "routing.html",
    "memory.html",
    "community.html",
    "life.html",
    "media.html",
    "identity_plugins.html",
)

# Static IDs created dynamically at runtime by JS (currently none referenced by literal).
DYNAMIC_IDS: frozenset[str] = frozenset()

FORBIDDEN_SUBSTRINGS = (
    "innerHTML",
    "insertAdjacentHTML",
    "outerHTML",
    "document.write",
    "unpkg.com",
    "sessionStorage",
    'src="http',
    'href="http',
    "src='http",
    "href='http",
)

ALLOWED_LOCALSTORAGE_RE = re.compile(
    r"localStorage\.getItem\('hakimi-theme'\)"
    r"|localStorage\.setItem\('hakimi-theme',dark\?'dark':'light'\)"
)


class _IdCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.ids: list[str] = []
        self._in_raw_tag: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lower = tag.lower()
        if lower in ("script", "style"):
            self._in_raw_tag = lower
            return
        if self._in_raw_tag:
            return
        for name, value in attrs:
            if name.lower() == "id" and value is not None:
                self.ids.append(value)

    def handle_endtag(self, tag: str) -> None:
        if self._in_raw_tag and tag.lower() == self._in_raw_tag:
            self._in_raw_tag = None


def read_version(version_override: str | None = None) -> str:
    raw = (
        version_override if version_override is not None else os.environ.get("HAKIMI_VERSION", "")
    ).strip()
    if raw:
        if any(ch.isspace() or ch in "<>&\"'" for ch in raw):
            raise ValueError(f"Invalid version override: {raw!r}")
        return raw
    text = INIT_FILE.read_text(encoding="utf-8")
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', text, re.M)
    if not match:
        raise ValueError(f"Could not find __version__ in {INIT_FILE}")
    return match.group(1)


def _load_csp_module():
    spec = importlib.util.spec_from_file_location("admin_csp_audit", CSP_FILE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load CSP module from {CSP_FILE}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_html(version_override: str | None = None) -> str:
    version = read_version(version_override)

    css_files = sorted(CSS_DIR.glob("*.css"))
    if not css_files:
        raise FileNotFoundError(f"No CSS files found in {CSS_DIR}")
    css_bundle = "\n".join(f.read_text(encoding="utf-8").strip() for f in css_files)

    shell_path = VIEWS_DIR / "shell.html"
    if not shell_path.is_file():
        raise FileNotFoundError(f"Missing shell template: {shell_path}")
    shell = shell_path.read_text(encoding="utf-8")

    view_files_on_disk = {p.name for p in VIEWS_DIR.glob("*.html") if p.name != "shell.html"}
    expected_views = set(VIEW_ORDER)
    missing_views = expected_views - view_files_on_disk
    extra_views = view_files_on_disk - expected_views
    if missing_views:
        raise FileNotFoundError(f"Missing view partials: {sorted(missing_views)}")
    if extra_views:
        raise ValueError(f"Unlisted view partials in {VIEWS_DIR}: {sorted(extra_views)}")

    views_bundle = "\n\n".join(
        (VIEWS_DIR / name).read_text(encoding="utf-8").strip() for name in VIEW_ORDER
    )

    js_files = sorted(JS_DIR.glob("*.js"))
    if not js_files:
        raise FileNotFoundError(f"No JS files found in {JS_DIR}")
    js_inner = "\n\n".join(f.read_text(encoding="utf-8").strip() for f in js_files)
    js_bundle = "'use strict';\n(()=>{\n" + js_inner + "\n})();"

    html = (
        shell.replace("{{STYLE}}", css_bundle)
        .replace("{{VIEWS}}", views_bundle)
        .replace("{{SCRIPT}}", js_bundle)
        .replace("{{VERSION}}", version)
    )
    if not html.endswith("\n"):
        html += "\n"
    return html


def _extract_js_id_refs(script: str) -> list[str]:
    refs: list[str] = []
    # Direct literal calls: $('id'), text('id', ...), table('id', ...),
    # trackDirty('id', ...), markDirty('id'), clearDirty('id').
    direct_patterns = (
        r"(?<![A-Za-z0-9_$.])\$\(\s*(['\"])([^'\"\n]+)\1\s*\)",
        r"(?<![A-Za-z0-9_$.])text\(\s*(['\"])([^'\"\n]+)\1\s*,",
        r"(?<![A-Za-z0-9_$.])table\(\s*(['\"])([^'\"\n]+)\1\s*,",
        r"(?<![A-Za-z0-9_$.])(?:trackDirty|markDirty|clearDirty)\(\s*(['\"])([^'\"\n]+)\1",
    )
    for pat in direct_patterns:
        for match in re.finditer(pat, script):
            refs.append(match.group(2))

    # Static arrays of IDs passed directly or with a literal prefix into $(), text(), or table().
    direct_array_patterns = (
        r"for\s*\(\s*const\s+(\w+)\s+of\s*\[([^\]]+)\]\s*\)"
        r"\s*(?:\{\s*)?(?:\$|text|table)\(\s*\1\s*[,)]",
        r"\[([^\]]+)\]\.forEach\(\s*(\w+)\s*=>\s*(?:\{\s*)?(?:\$|text|table)\(\s*\2\s*[,)]",
    )
    for idx, pat in enumerate(direct_array_patterns):
        for match in re.finditer(pat, script):
            array_body = match.group(2) if idx == 0 else match.group(1)
            for item in re.finditer(r"(['\"])([^'\"\n]+)\1", array_body):
                refs.append(item.group(2))

    prefixed_array_patterns = (
        r"for\s*\(\s*const\s+(\w+)\s+of\s*\[([^\]]+)\]\s*\)"
        r"\s*(?:\{\s*)?(?:\$|text|table)\(\s*(['\"])([^'\"\n]+)\3\s*\+\s*\1\s*[,)]",
        r"\[([^\]]+)\]\.forEach\(\s*(\w+)\s*=>"
        r"\s*(?:\{\s*)?(?:\$|text|table)\(\s*(['\"])([^'\"\n]+)\3\s*\+\s*\2\s*[,)]",
    )
    for idx, pat in enumerate(prefixed_array_patterns):
        for match in re.finditer(pat, script):
            array_body = match.group(2) if idx == 0 else match.group(1)
            prefix = match.group(4)
            for item in re.finditer(r"(['\"])([^'\"\n]+)\1", array_body):
                refs.append(prefix + item.group(2))
    return refs


def audit_html(html: str, version_override: str | None = None) -> list[str]:
    errors: list[str] = []
    version = read_version(version_override)

    lines = html.splitlines()
    if not lines or lines[0].strip() != "<!doctype html>":
        errors.append("First line must be '<!doctype html>'")

    for placeholder in ("{{VERSION}}", "{{STYLE}}", "{{VIEWS}}", "{{SCRIPT}}"):
        if placeholder in html:
            errors.append(f"Unresolved template placeholder: {placeholder}")

    if html.count(f"v{version}") < 2:
        errors.append(f"Expected version string 'v{version}' to appear at least twice")

    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    if len(styles) != 1:
        errors.append(f"Expected exactly 1 <style> block, found {len(styles)}")
    if len(scripts) != 1:
        errors.append(f"Expected exactly 1 <script> block, found {len(scripts)}")

    for forbidden in FORBIDDEN_SUBSTRINGS:
        if forbidden in html:
            errors.append(f"Forbidden token found in assembled HTML: {forbidden!r}")

    if re.search(r"\sstyle\s*=", html):
        match = re.search(r".{0,40}\sstyle\s*=.{0,40}", html)
        snippet = match.group(0) if match else ""
        errors.append(f"Forbidden inline style= attribute/pattern found: {snippet!r}")

    if re.search(r"\son[a-z]+\s*=", html, re.I):
        match = re.search(r".{0,40}\son[a-z]+\s*=.{0,40}", html, re.I)
        snippet = match.group(0) if match else ""
        errors.append(f"Forbidden inline on*= handler/pattern found: {snippet!r}")

    without_theme_ls = ALLOWED_LOCALSTORAGE_RE.sub("", html)
    if "localStorage" in without_theme_ls:
        errors.append("Unexpected localStorage usage outside allowed theme preference literals")

    # Parse static HTML IDs & detect duplicates.
    collector = _IdCollector()
    collector.feed(html)
    seen: set[str] = set()
    dups: list[str] = []
    for elem_id in collector.ids:
        if elem_id in seen:
            dups.append(elem_id)
        seen.add(elem_id)
    if dups:
        errors.append(f"Duplicate HTML id attributes: {sorted(set(dups))}")

    # Audit JS references and invariants.
    if len(scripts) == 1:
        script = scripts[0]
        missing_ids = sorted(
            {
                ref
                for ref in _extract_js_id_refs(script)
                if ref not in seen and ref not in DYNAMIC_IDS
            }
        )
        if missing_ids:
            errors.append(f"JS references nonexistent DOM IDs: {missing_ids}")

        native_dlg = re.search(r"(?<![A-Za-z0-9_$.])(?:confirm|prompt|alert)\s*\(", script)
        if native_dlg:
            start = max(0, native_dlg.start() - 30)
            end = min(len(script), native_dlg.end() + 40)
            errors.append(f"Forbidden native dialog call in JS: {script[start:end]!r}")

        if re.search(r"['\"]/admin/[^'\"\n]*\?", script) or re.search(
            r"(?<![A-Za-z0-9_$.])api\(\s*['\"][^'\"\n]*\?", script
        ):
            errors.append("Forbidden query parameter in /admin/* or api() URL")

    # Verify CSP generation via admin/csp.py.
    try:
        csp_mod = _load_csp_module()
        policy = csp_mod.content_security_policy(html)
        if "script-src 'sha256-" not in policy or "style-src 'sha256-" not in policy:
            errors.append("Generated CSP is missing expected sha256 hashes")
    except Exception as exc:
        errors.append(f"admin/csp.py failed: {exc}")

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build and audit admin/admin_page.html")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Assemble and audit in memory without writing to disk",
    )
    parser.add_argument(
        "--verify-file",
        action="store_true",
        help="With --check, also verify that --output on disk matches the assembled HTML",
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Override version string instead of reading src/hakimi/__init__.py",
    )
    parser.add_argument(
        "--output",
        default=str(OUT_FILE),
        help="Target output path when writing assembled HTML",
    )
    args = parser.parse_args(argv)

    html = build_html(version_override=args.version)
    errors = audit_html(html, version_override=args.version)
    if errors:
        for err in errors:
            print(f"[build.py ERROR] {err}", file=sys.stderr)
        return 1

    target = Path(args.output)
    if args.check:
        if args.verify_file:
            if not target.is_file():
                print(f"[build.py ERROR] Missing output file: {target}", file=sys.stderr)
                return 1
            existing = target.read_text(encoding="utf-8")
            if existing != html:
                print(
                    f"[build.py ERROR] {target} is out of date; run 'python3 admin/build.py'",
                    file=sys.stderr,
                )
                return 1
        size = len(html.encode("utf-8"))
        print(f"[build.py OK] Assembled in memory ({size} bytes) and passed all audits.")
        return 0

    target.write_text(html, encoding="utf-8")
    size = len(html.encode("utf-8"))
    print(f"[build.py OK] Wrote {target} ({size} bytes), all audits passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
