"""Run inline Admin JS test harnesses via a temp file to avoid Linux MAX_ARG_STRLEN (128 KiB)."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

_MACOS_JSC = Path("/System/Library/Frameworks/JavaScriptCore.framework/Versions/A/Helpers/jsc")


def find_js_engine(skip_reason: str = "No local JS engine (node/jsc) available") -> str:
    """Return an executable JS runtime path (`node` or macOS `jsc`), or skip the test."""
    node = shutil.which("node")
    if node:
        return node
    if _MACOS_JSC.is_file():
        return str(_MACOS_JSC)
    pytest.skip(skip_reason)


def run_js_harness(
    script: str,
    *,
    cwd: Path,
    skip_reason: str = "No local JS engine (node/jsc) available",
) -> subprocess.CompletedProcess[str]:
    """Write a JS harness to a temporary `.js` file and execute it with `node` or `jsc`.

    Linux enforces a 128 KiB per-argument `MAX_ARG_STRLEN` limit on `execve()`, so passing
    assembled Admin JS bundles inline via `node -e <script>` fails with `Errno 7`
    (`Argument list too long`) once the bundle grows past 128 KiB.
    """
    engine = find_js_engine(skip_reason=skip_reason)
    fd, temp_path = tempfile.mkstemp(prefix="hakimi-js-test-", suffix=".js")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(script)
        return subprocess.run(
            [engine, temp_path],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
