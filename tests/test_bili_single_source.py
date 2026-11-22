"""Single-source-of-truth & drift-prevention tests for `hakimi_plugin_sdk.bili_common`."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from hakimi import (
    admin_session_routes,
    bili_account_admin,
    bili_bridge,
    gemini,
    rich_message_parser,
    video_downloader,
)
from hakimi.modules import bili_watcher as builtin_watcher
from hakimi.modules import bilibili as builtin_bili
from hakimi.security import Rejected
from hakimi_plugin_sdk import bili_common
from plugins.bilibili import client as plugin_client
from plugins.bilibili import patrol as plugin_patrol

ROOT = Path(__file__).resolve().parents[1]
BV1 = "BV1xx411c7mD"
BV2 = "BV1Q541167Qg"
NAV_OK = {
    "wbi_img": {
        "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
        "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
    }
}


def _imported_modules(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            prefix = "." * node.level
            found.append(prefix + (node.module or ""))
    return found


def test_bili_common_has_only_stdlib_and_pydantic_and_is_packaged_in_plugins_dockerfile() -> None:
    common_path = ROOT / "src" / "hakimi_plugin_sdk" / "bili_common.py"
    stdlib_names = getattr(sys, "stdlib_module_names", frozenset())
    tree = ast.parse(common_path.read_text(encoding="utf-8"), filename=str(common_path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".", 1)[0]
                assert top != "hakimi"
                assert top in stdlib_names or top == "pydantic", f"Unexpected import: {alias.name}"
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "bili_common.py should not use relative imports"
            top = (node.module or "").split(".", 1)[0]
            assert top != "hakimi"
            assert top in stdlib_names or top == "pydantic", f"Unexpected import: {node.module}"

    dockerfile = (ROOT / "plugins" / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY src/hakimi_plugin_sdk ./src/hakimi_plugin_sdk" in dockerfile
    assert "pydantic" in dockerfile


def test_client_and_builtin_bilibili_share_identical_single_source_objects() -> None:
    for name in (
        "BVID",
        "BVID_RE",
        "BV_RE",
        "MID_RE",
        "URL_RE",
        "API_ORIGIN",
        "LIVE_API_ORIGIN",
        "extract_bvid",
        "extract_video_ref",
    ):
        canonical = getattr(bili_common, name)
        assert getattr(plugin_client, name) is canonical, f"plugin_client.{name} drifted"
        assert getattr(builtin_bili, name) is canonical, f"builtin_bili.{name} drifted"

    assert plugin_client.CANONICAL_VIDEO_HOSTS is bili_common.CANONICAL_VIDEO_HOSTS
    assert plugin_client.SHORT_LINK_HOSTS is bili_common.SHORT_LINK_HOSTS
    assert plugin_client.clean_text is bili_common.clean_bili_text
    assert plugin_client._extract_url_video_ref is bili_common.extract_url_video_ref
    assert plugin_client._extract_url_bvid is bili_common.extract_url_bvid
    assert plugin_client._is_valid_short_link is bili_common.validate_short_link


def test_patrol_and_builtin_watcher_share_identical_models_helpers_and_constants() -> None:
    for name in (
        "BVID",
        "API_ORIGIN",
        "CREDENTIAL_KEYS",
        "COOKIE_NAMES",
        "COVER_URL_RE",
        "MAX_COVER_THUMB_BYTES",
        "GET_PATHS",
        "POST_PATHS",
        "PUBLIC_READ_PATHS",
        "SUBTITLE_HOSTS",
        "USER_AGENT",
        "SPI_PATH",
        "PRIVATE_SHARE_PRIORITY_SECONDS",
        "RISK_COOLDOWN_SECONDS",
        "RISK_CODES",
        "PUBLIC_DEGRADE_ERRORS",
        "BiliPolicy",
        "CredentialUpdate",
        "Pick",
        "Selection",
        "validate_cookie",
        "has_voucher",
        "video_duration",
        "subtitle_timestamp",
        "subtitle_text",
    ):
        canonical = getattr(bili_common, name)
        assert getattr(plugin_patrol, name) is canonical, f"plugin_patrol.{name} drifted"
        assert getattr(builtin_watcher, name) is canonical, f"builtin_watcher.{name} drifted"

    assert plugin_patrol.MIXIN is bili_common.MIXIN_KEY_ENC_TAB
    assert builtin_watcher.MIXIN is bili_common.MIXIN_KEY_ENC_TAB
    assert plugin_patrol._PUBLIC_DEGRADE_ERRORS is bili_common.PUBLIC_DEGRADE_ERRORS
    assert plugin_patrol.wbi_sign.__wrapped__ is bili_common.wbi_sign
    assert builtin_watcher.wbi_sign.__wrapped__ is bili_common.wbi_sign
    assert plugin_patrol.wbi_keys.__wrapped__ is bili_common.wbi_keys
    assert builtin_watcher.wbi_keys.__wrapped__ is bili_common.wbi_keys
    assert plugin_patrol._validate_subtitle_url.__wrapped__ is bili_common.validate_subtitle_url


def test_kernel_modules_use_bili_common_directly_with_zero_reverse_imports() -> None:
    assert bili_bridge.BVID is bili_common.BVID
    assert bili_bridge.BVID_RE is bili_common.BVID_RE
    assert bili_bridge.MID_RE is bili_common.MID_RE
    assert bili_bridge.COVER_URL_RE is bili_common.COVER_URL_RE
    assert bili_bridge.MAX_COVER_THUMB_BYTES is bili_common.MAX_COVER_THUMB_BYTES
    assert bili_bridge.CREDENTIAL_KEYS is bili_common.CREDENTIAL_KEYS
    assert bili_bridge.RISK_COOLDOWN_SECONDS is bili_common.RISK_COOLDOWN_SECONDS
    assert bili_bridge._RISK_CODES is bili_common.RISK_CODES
    assert bili_bridge._validate_cookie_str is bili_common.validate_cookie
    assert bili_common.PUBLIC_DEGRADE_ERRORS.issubset(bili_bridge._PUBLIC_DEGRADE_ERRORS)

    assert video_downloader.BVID is bili_common.BVID
    assert video_downloader.COOKIE_NAMES is bili_common.COOKIE_NAMES
    assert video_downloader.validate_cookie is bili_common.validate_cookie

    assert rich_message_parser.BV is bili_common.BV_RE
    assert rich_message_parser._extract_bili_url_bvid is bili_common.extract_url_bvid

    assert admin_session_routes.extract_bvid is bili_common.extract_bvid
    assert gemini.extract_bvid is bili_common.extract_bvid
    assert bili_account_admin.BiliPolicy is bili_common.BiliPolicy
    assert bili_account_admin.CredentialUpdate is bili_common.CredentialUpdate

    kernel_files = (
        ROOT / "src" / "hakimi" / "bili_bridge.py",
        ROOT / "src" / "hakimi" / "video_downloader.py",
        ROOT / "src" / "hakimi" / "rich_message_parser.py",
        ROOT / "src" / "hakimi" / "admin_session_routes.py",
        ROOT / "src" / "hakimi" / "gemini.py",
        ROOT / "src" / "hakimi" / "bili_account_admin.py",
    )
    for path in kernel_files:
        for mod in _imported_modules(path):
            assert "modules.bilibili" not in mod, f"{path.name} reverse-imports {mod}"
            assert "modules.bili_watcher" not in mod, f"{path.name} reverse-imports {mod}"


def test_no_duplicate_model_or_helper_definitions_in_consumers() -> None:
    forbidden_defs = {
        "BiliPolicy",
        "CredentialUpdate",
        "Pick",
        "Selection",
        "validate_cookie",
        "has_voucher",
        "video_duration",
        "subtitle_timestamp",
        "subtitle_text",
        "extract_bvid",
        "extract_video_ref",
    }
    consumer_files = (
        ROOT / "plugins" / "bilibili" / "client.py",
        ROOT / "plugins" / "bilibili" / "patrol.py",
        ROOT / "src" / "hakimi" / "modules" / "platforms" / "bilibili" / "bilibili.py",
        ROOT / "src" / "hakimi" / "modules" / "platforms" / "bilibili" / "bili_watcher.py",
        ROOT / "src" / "hakimi" / "bili_bridge.py",
        ROOT / "src" / "hakimi" / "rich_message_parser.py",
    )
    for path in consumer_files:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
        defined = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }
        overlap = forbidden_defs & defined
        assert not overlap, f"{path} redefines single-source symbols: {sorted(overlap)}"
        assert "hashlib.md5" not in src, f"{path} duplicates WBI md5 signing"


def test_wave1_url_port_page_festival_wbi_and_cookie_hardening_parity() -> None:
    # Wave 1 URL parsing: ?p=N, legacy http:// (port 80/None), festival?bvid=, invalid .port
    assert bili_common.extract_video_ref(f"http://www.bilibili.com/video/{BV1}?p=3") == (BV1, 3)
    assert bili_common.extract_video_ref(f"http://m.bilibili.com:80/video/{BV2}/?p=999") == (
        BV2,
        999,
    )
    assert bili_common.extract_video_ref(
        f"https://www.bilibili.com/festival/2026bnj?bvid={BV1}&p=4"
    ) == (BV1, 4)
    assert bili_common.extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=1000") == (BV1, 1)

    for bad in (
        f"http://www.bilibili.com:99999/video/{BV1}?p=2",
        f"https://www.bilibili.com:abc/festival/2026?bvid={BV1}&p=3",
        f"http://www.bilibili.com:443/video/{BV1}",
        f"https://www.bilibili.com:80/video/{BV1}",
        f"https://www.bilibili.com/festival/../admin?bvid={BV1}",
        f"https://evil.test/video/{BV1}",
    ):
        assert bili_common.extract_bvid(bad) is None
        assert builtin_bili.extract_bvid(bad) is None
        assert plugin_client.extract_bvid(bad) is None

    assert bili_common.validate_short_link("https://b23.tv:99999/AbCd12") is None
    assert bili_common.validate_short_link("https://b23.tv:abc/AbCd12") is None
    assert bili_common.validate_short_link("https://b23.tv:443/AbCd12") == "b23.tv"

    # WBI signing parity & domain-appropriate error types
    params = {"foo": "114", "bar": "514", "zab": 1919810}
    keys = ("7cd084941338484aae1ad9425b84077c", "4932caff0ff746eab6f01bf08b70ac45")
    expected = bili_common.wbi_sign(params, keys, 1702204169)
    assert expected["w_rid"] == "8f6f2b5b3d485fe1886cec6a0be8c5d4"
    assert plugin_patrol.wbi_sign(params, keys, 1702204169) == expected
    assert builtin_watcher.wbi_sign(params, keys, 1702204169) == expected
    assert bili_common.wbi_keys(NAV_OK) == ("a" * 32, "b" * 32)
    assert plugin_patrol.wbi_keys(NAV_OK) == ("a" * 32, "b" * 32)
    assert builtin_watcher.wbi_keys(NAV_OK) == ("a" * 32, "b" * 32)

    with pytest.raises(ValueError, match="bili_wbi_unavailable"):
        bili_common.wbi_sign({}, ("short", "key"), 1702204169)
    with pytest.raises(plugin_client.BiliClientError, match="bili_wbi_unavailable"):
        plugin_patrol.wbi_sign({}, ("short", "key"), 1702204169)
    with pytest.raises(Rejected, match="bili_wbi_unavailable"):
        builtin_watcher.wbi_sign({}, ("short", "key"), 1702204169)
    with pytest.raises(Rejected, match="bili_wbi_unavailable"):
        builtin_watcher.wbi_keys({})

    # Subtitle URL validation protects against invalid .port and traversal
    for bad_sub in (
        "https://aisubtitle.hdslb.com:99999/bfs/ai_subtitle/demo.json",
        "https://aisubtitle.hdslb.com/bfs/../secret.json",
        "http://aisubtitle.hdslb.com/bfs/ai_subtitle/demo.json",
    ):
        with pytest.raises(ValueError, match="bili_subtitle_host_forbidden"):
            bili_common.validate_subtitle_url(bad_sub)
        with pytest.raises(plugin_client.BiliClientError, match="bili_subtitle_host_forbidden"):
            plugin_patrol._validate_subtitle_url(bad_sub)

    # Cookie & model validation
    for bad_cookie in ("a; b=1", "a\nb", "a\rb", "a b", "中文", "x" * 4097):
        with pytest.raises(ValueError):
            bili_common.validate_cookie(bad_cookie)
        with pytest.raises(ValidationError):
            bili_common.CredentialUpdate(sessdata=bad_cookie)
