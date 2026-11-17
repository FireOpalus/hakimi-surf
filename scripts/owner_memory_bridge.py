#!/usr/bin/env python3
"""agy-free Owner Memory Bridge: sync Antigravity_Memory via Drive OAuth or safe tar staging."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
import stat
import sys
import tarfile
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from hakimi.drive_client import API, SCOPE, UPLOAD  # noqa: E402
from hakimi.drive_oauth import FOLDER_MARKER, OAuthDriveAuth  # noqa: E402
from hakimi.security import Rejected  # noqa: E402

BRIDGE_FILENAME = "owner-memory-bridge.json"
BRIDGE_APP_KEY = "hakimi_owner_bridge"
BRIDGE_APP_VALUE = "v1"
BRIDGE_FIELDS = "id,name,parents,mimeType,trashed,appProperties,md5Checksum,modifiedTime,size"
MAX_BRIDGE_BYTES = 16 * 1024 * 1024
MAX_TAR_BYTES = 32 * 1024 * 1024
MD_FILENAME_RE = re.compile(r"^[A-Za-z0-9._\u4e00-\u9fff-]+\.md$")
CONTAINER_GID = 10001


def is_valid_md_filename(name: Any) -> bool:
    return (
        isinstance(name, str)
        and bool(MD_FILENAME_RE.fullmatch(name))
        and not name.startswith(".")
        and ".." not in name
        and "/" not in name
        and "\\" not in name
    )


def load_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_path.is_file() or env_path.is_symlink():
        return values
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {"'", '"'}:
            val = val[1:-1]
        values[key] = val
    return values


def resolve_config(
    *,
    repo_root: Path | None = None,
    folder_id: str | None = None,
    credentials: Path | None = None,
    memory_dir: Path | None = None,
    state_file: Path | None = None,
    env_file: Path | None = None,
) -> dict[str, Any]:
    root = (repo_root or REPO_ROOT).resolve()
    env_map = load_env_file(env_file or (root / ".env"))

    resolved_folder = (
        folder_id
        or os.environ.get("GDRIVE_ROAMING_FOLDER_ID")
        or os.environ.get("HAKIMI_GDRIVE_ROAMING_FOLDER_ID")
        or env_map.get("GDRIVE_ROAMING_FOLDER_ID")
        or env_map.get("HAKIMI_GDRIVE_ROAMING_FOLDER_ID")
        or ""
    ).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", resolved_folder) or resolved_folder == "root":
        raise Rejected("bridge_folder_id_invalid")

    default_creds = root / "secrets" / "roaming-oauth.json"
    if credentials is not None:
        resolved_creds = credentials
    else:
        env_creds = (
            os.environ.get("GDRIVE_OAUTH_CREDENTIALS_FILE")
            or env_map.get("GDRIVE_OAUTH_CREDENTIALS_FILE")
            or ""
        ).strip()
        candidate = Path(env_creds) if env_creds else default_creds
        resolved_creds = candidate if candidate.exists() else default_creds

    resolved_memory_dir = memory_dir or (root / "secrets" / "antigravity-memory")
    resolved_state = state_file or (root / "secrets" / ".owner_bridge_state.json")
    return {
        "repo_root": root,
        "folder_id": resolved_folder,
        "credentials_path": resolved_creds,
        "memory_dir": resolved_memory_dir,
        "state_file": resolved_state,
    }


def _chown_container_ro(path: Path) -> None:
    try:
        os.chown(path, 0, CONTAINER_GID)
    except PermissionError:
        if os.geteuid() == 0:
            raise


def _ensure_dir_secure(path: Path, *, mode: int = 0o750, set_gid: bool = True) -> None:
    if path.is_symlink():
        raise Rejected("bridge_symlink_forbidden")
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    if path.is_symlink():
        raise Rejected("bridge_symlink_forbidden")
    if set_gid:
        _chown_container_ro(path)
    os.chmod(path, mode)


def _atomic_write_bytes(
    path: Path, data: bytes, *, mode: int = 0o640, set_gid: bool = True
) -> None:
    if path.is_symlink():
        raise Rejected("bridge_symlink_forbidden")
    fd, tmp_name = tempfile.mkstemp(prefix=".bridge-", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if set_gid:
            _chown_container_ro(tmp_path)
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def validate_bridge_payload(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise Rejected("bridge_schema_invalid")
    required = {"notebook_md", "short_term_json", "long_term_md", "notes_md"}
    if not required.issubset(data.keys()):
        raise Rejected("bridge_schema_missing_fields")

    notebook_md = data["notebook_md"]
    if not isinstance(notebook_md, str):
        raise Rejected("bridge_schema_invalid_notebook")

    short_term_raw = data["short_term_json"]
    if isinstance(short_term_raw, str):
        try:
            parsed_short = json.loads(short_term_raw)
        except ValueError:
            raise Rejected("bridge_schema_invalid_short_term") from None
        if not isinstance(parsed_short, list):
            raise Rejected("bridge_schema_invalid_short_term")
        short_term_str = short_term_raw
    elif isinstance(short_term_raw, list):
        short_term_str = json.dumps(short_term_raw, ensure_ascii=False, indent=2) + "\n"
    else:
        raise Rejected("bridge_schema_invalid_short_term")

    validated_dirs: dict[str, dict[str, str]] = {}
    for field_name in ("long_term_md", "notes_md"):
        mapping = data[field_name]
        if not isinstance(mapping, dict):
            raise Rejected("bridge_schema_invalid_markdown_map")
        clean_map: dict[str, str] = {}
        for name, content in mapping.items():
            if not is_valid_md_filename(name):
                raise Rejected("bridge_invalid_filename")
            if not isinstance(content, str):
                raise Rejected("bridge_schema_invalid_markdown_value")
            clean_map[name] = content
        validated_dirs[field_name] = clean_map

    return {
        "notebook_md": notebook_md,
        "short_term_json": short_term_str,
        "long_term_md": validated_dirs["long_term_md"],
        "notes_md": validated_dirs["notes_md"],
    }


def read_local_memory_seed(memory_dir: Path) -> dict[str, Any]:
    notebook_md = ""
    nb_path = memory_dir / "notebook.md"
    if nb_path.is_file() and not nb_path.is_symlink():
        notebook_md = nb_path.read_text(encoding="utf-8")

    short_term_json = "[]\n"
    st_path = memory_dir / "short_term_memory.json"
    if st_path.is_file() and not st_path.is_symlink():
        raw = st_path.read_text(encoding="utf-8")
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = None
        if isinstance(parsed, list):
            short_term_json = raw

    long_term_md: dict[str, str] = {}
    lt_dir = memory_dir / "long_term"
    if lt_dir.is_dir() and not lt_dir.is_symlink():
        for item in sorted(lt_dir.iterdir()):
            if item.is_file() and not item.is_symlink() and is_valid_md_filename(item.name):
                long_term_md[item.name] = item.read_text(encoding="utf-8")

    notes_md: dict[str, str] = {}
    notes_dir = memory_dir / "notes"
    if notes_dir.is_dir() and not notes_dir.is_symlink():
        for item in sorted(notes_dir.iterdir()):
            if item.is_file() and not item.is_symlink() and is_valid_md_filename(item.name):
                notes_md[item.name] = item.read_text(encoding="utf-8")

    return {
        "version": 1,
        "updated_at": datetime.now(UTC).isoformat(),
        "notebook_md": notebook_md,
        "short_term_json": short_term_json,
        "long_term_md": long_term_md,
        "notes_md": notes_md,
    }


def write_memory_dir_atomic(memory_dir: Path, payload: dict[str, Any]) -> int:
    _ensure_dir_secure(memory_dir, mode=0o750, set_gid=True)
    long_term_dir = memory_dir / "long_term"
    notes_dir = memory_dir / "notes"
    _ensure_dir_secure(long_term_dir, mode=0o750, set_gid=True)
    _ensure_dir_secure(notes_dir, mode=0o750, set_gid=True)

    written = 0
    _atomic_write_bytes(
        memory_dir / "notebook.md",
        payload["notebook_md"].encode("utf-8"),
        mode=0o640,
        set_gid=True,
    )
    written += 1
    _atomic_write_bytes(
        memory_dir / "short_term_memory.json",
        payload["short_term_json"].encode("utf-8"),
        mode=0o640,
        set_gid=True,
    )
    written += 1

    for name, content in payload["long_term_md"].items():
        if not is_valid_md_filename(name):
            raise Rejected("bridge_invalid_filename")
        _atomic_write_bytes(long_term_dir / name, content.encode("utf-8"), mode=0o640, set_gid=True)
        written += 1

    for name, content in payload["notes_md"].items():
        if not is_valid_md_filename(name):
            raise Rejected("bridge_invalid_filename")
        _atomic_write_bytes(notes_dir / name, content.encode("utf-8"), mode=0o640, set_gid=True)
        written += 1

    return written


def extract_safe_tar_stream(raw_bytes: bytes, staging_dir: Path) -> int:
    """Validate and extract only expected owner-memory paths into staging_dir."""
    if len(raw_bytes) > MAX_TAR_BYTES:
        raise Rejected("bridge_tar_too_large")
    if staging_dir.is_symlink():
        raise Rejected("bridge_symlink_forbidden")
    staging_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    (staging_dir / "long_term").mkdir(parents=True, exist_ok=True, mode=0o700)
    (staging_dir / "notes").mkdir(parents=True, exist_ok=True, mode=0o700)

    count = 0
    total_extracted = 0
    with tarfile.open(fileobj=io.BytesIO(raw_bytes), mode="r:*") as tf:
        for member in tf.getmembers():
            if member.issym() or member.islnk():
                raise Rejected("bridge_tar_symlink_forbidden")
            raw_name = member.name.replace("\\", "/")
            if raw_name.startswith("/"):
                raise Rejected("bridge_tar_absolute_path")
            parts = [p for p in raw_name.split("/") if p not in {"", "."}]
            if any(p == ".." for p in parts):
                raise Rejected("bridge_tar_traversal")
            if not parts:
                continue
            if member.isdir():
                if parts not in (["long_term"], ["notes"]):
                    raise Rejected("bridge_tar_unexpected_dir")
                continue
            if not member.isfile():
                raise Rejected("bridge_tar_special_file")
            if parts == ["notebook.md"] or parts == ["short_term_memory.json"]:
                rel_parts = parts
            elif (
                len(parts) == 2
                and parts[0] in {"long_term", "notes"}
                and is_valid_md_filename(parts[1])
            ):
                rel_parts = parts
            else:
                raise Rejected("bridge_tar_unexpected_file")

            total_extracted += member.size
            if total_extracted > MAX_BRIDGE_BYTES:
                raise Rejected("bridge_tar_too_large")
            extracted = tf.extractfile(member)
            if extracted is None:
                raise Rejected("bridge_tar_read_failed")
            data = extracted.read(MAX_BRIDGE_BYTES + 1)
            if len(data) > MAX_BRIDGE_BYTES:
                raise Rejected("bridge_tar_too_large")
            if rel_parts == ["short_term_memory.json"]:
                try:
                    parsed = json.loads(data.decode("utf-8"))
                except (UnicodeDecodeError, ValueError):
                    raise Rejected("bridge_tar_invalid_short_term") from None
                if not isinstance(parsed, list):
                    raise Rejected("bridge_tar_invalid_short_term")
            target = staging_dir.joinpath(*rel_parts)
            target.write_bytes(data)
            os.chmod(target, 0o600)
            count += 1
    return count


def _file_sha256_sync(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        return ""
    try:
        st = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(st.st_mode):
            return ""
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _core_hashes_sync(mem_dir: Path) -> dict[str, str]:
    return {
        "notebook_sha256": _file_sha256_sync(mem_dir / "notebook.md"),
        "short_term_sha256": _file_sha256_sync(mem_dir / "short_term_memory.json"),
    }


def _read_state_sync(state_file: Path) -> dict[str, Any]:
    if not state_file.is_file() or state_file.is_symlink():
        return {}
    try:
        st = state_file.stat(follow_symlinks=False)
        if not stat.S_ISREG(st.st_mode):
            return {}
        data = json.loads(state_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_state_sync(state_file: Path, data: dict[str, Any]) -> None:
    _ensure_dir_secure(state_file.parent, mode=0o700, set_gid=False)
    _atomic_write_bytes(
        state_file,
        (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        mode=0o600,
        set_gid=False,
    )


async def _drive_json(
    http: httpx.AsyncClient, method: str, url: str, token: str, **kwargs: Any
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}", **kwargs.pop("headers", {})}
    async with http.stream(method, url, headers=headers, follow_redirects=False, **kwargs) as resp:
        if resp.status_code not in {200, 201}:
            raise Rejected("drive_request_failed")
        body = bytearray()
        async for chunk in resp.aiter_bytes():
            body.extend(chunk)
            if len(body) > 65536:
                raise Rejected("drive_response_too_large")
        value = json.loads(body)
        if not isinstance(value, dict):
            raise Rejected("drive_invalid_response")
        return value


async def _validate_oauth_folder(
    http: httpx.AsyncClient, auth: OAuthDriveAuth, folder_id: str, *, require_write: bool
) -> str:
    token = await auth.access_token()
    folder = await _drive_json(
        http,
        "GET",
        f"{API}/{folder_id}",
        token,
        params={
            "fields": "id,mimeType,trashed,appProperties,capabilities(canAddChildren)",
            "supportsAllDrives": "true",
        },
    )
    if (
        folder.get("id") != folder_id
        or folder.get("mimeType") != "application/vnd.google-apps.folder"
        or folder.get("trashed", False)
    ):
        raise Rejected("drive_folder_unavailable")
    if require_write and not (folder.get("capabilities") or {}).get("canAddChildren"):
        raise Rejected("drive_folder_unavailable")
    if auth.info is None or (folder.get("appProperties") or {}).get(FOLDER_MARKER) != auth.info.get(
        "folder_marker"
    ):
        raise Rejected("drive_oauth_folder_mismatch")
    return token


def _validate_bridge_item(item: dict[str, Any], folder_id: str) -> str:
    file_id = str(item.get("id", ""))
    if (
        not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", file_id)
        or item.get("parents") != [folder_id]
        or item.get("trashed", False)
        or item.get("name") != BRIDGE_FILENAME
        or item.get("mimeType") != "application/json"
        or (item.get("appProperties") or {}).get(BRIDGE_APP_KEY) != BRIDGE_APP_VALUE
    ):
        raise Rejected("drive_file_outside_scope")
    return file_id


async def _find_bridge_file(
    http: httpx.AsyncClient, token: str, folder_id: str
) -> dict[str, Any] | None:
    listing = await _drive_json(
        http,
        "GET",
        API,
        token,
        params={
            "q": (
                f"'{folder_id}' in parents and trashed = false "
                f"and name = '{BRIDGE_FILENAME}' "
                f"and appProperties has {{ key='{BRIDGE_APP_KEY}' and value='{BRIDGE_APP_VALUE}' }}"
            ),
            "fields": f"nextPageToken,incompleteSearch,files({BRIDGE_FIELDS})",
            "pageSize": "10",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
            "corpora": "user",
        },
    )
    files = listing.get("files")
    if (
        not isinstance(files, list)
        or len(files) > 1
        or listing.get("nextPageToken")
        or listing.get("incompleteSearch")
    ):
        raise Rejected("drive_ambiguous_file")
    if not files:
        return None
    _validate_bridge_item(files[0], folder_id)
    return files[0]


async def run_init(
    cfg: dict[str, Any], *, client: httpx.AsyncClient | None = None
) -> dict[str, Any]:
    async def _execute(http: httpx.AsyncClient) -> dict[str, Any]:
        auth = OAuthDriveAuth(cfg["credentials_path"], cfg["folder_id"], http)
        token = await _validate_oauth_folder(http, auth, cfg["folder_id"], require_write=True)
        existing = await _find_bridge_file(http, token, cfg["folder_id"])
        if existing is not None:
            return {"status": "exists", "created": False, "file_id": existing["id"]}

        seed = await asyncio.to_thread(read_local_memory_seed, cfg["memory_dir"])
        seed_json = json.dumps(seed, ensure_ascii=False, indent=2) + "\n"
        if len(seed_json.encode("utf-8")) > MAX_BRIDGE_BYTES:
            raise Rejected("bridge_payload_too_large")
        metadata = {
            "name": BRIDGE_FILENAME,
            "mimeType": "application/json",
            "parents": [cfg["folder_id"]],
            "appProperties": {BRIDGE_APP_KEY: BRIDGE_APP_VALUE},
        }
        boundary = "hakimi_" + uuid.uuid4().hex
        body = (
            f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            + json.dumps(metadata)
            + f"\r\n--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            + seed_json
            + f"\r\n--{boundary}--\r\n"
        ).encode("utf-8")
        created = await _drive_json(
            http,
            "POST",
            UPLOAD,
            token,
            params={
                "uploadType": "multipart",
                "supportsAllDrives": "true",
                "fields": BRIDGE_FIELDS,
            },
            headers={"Content-Type": f"multipart/related; boundary={boundary}"},
            content=body,
        )
        file_id = _validate_bridge_item(created, cfg["folder_id"])
        return {"status": "created", "created": True, "file_id": file_id}

    if client is not None:
        return await _execute(client)
    async with httpx.AsyncClient(timeout=30, trust_env=False, follow_redirects=False) as http:
        return await _execute(http)


async def run_pull(
    cfg: dict[str, Any], *, client: httpx.AsyncClient | None = None, force: bool = False
) -> dict[str, Any]:
    async def _execute(http: httpx.AsyncClient) -> dict[str, Any]:
        auth = OAuthDriveAuth(cfg["credentials_path"], cfg["folder_id"], http)
        token = await _validate_oauth_folder(http, auth, cfg["folder_id"], require_write=False)
        item = await _find_bridge_file(http, token, cfg["folder_id"])
        if item is None:
            raise Rejected("bridge_not_initialized")
        file_id = _validate_bridge_item(item, cfg["folder_id"])
        remote_md5 = str(item.get("md5Checksum") or "").strip().lower()
        remote_mtime = str(item.get("modifiedTime") or "").strip()
        try:
            remote_size = int(item.get("size") or 0)
        except (TypeError, ValueError):
            raise Rejected("drive_invalid_response") from None
        if remote_size > MAX_BRIDGE_BYTES:
            raise Rejected("bridge_payload_too_large")

        prev_state = await asyncio.to_thread(_read_state_sync, cfg["state_file"])
        mem_dir: Path = cfg["memory_dir"]
        local_hashes = await asyncio.to_thread(_core_hashes_sync, mem_dir)
        has_core = bool(local_hashes["notebook_sha256"]) and bool(local_hashes["short_term_sha256"])
        local_matches = (
            prev_state.get("notebook_sha256") == local_hashes["notebook_sha256"]
            and prev_state.get("short_term_sha256") == local_hashes["short_term_sha256"]
        )
        if (
            not force
            and has_core
            and local_matches
            and prev_state.get("file_id") == file_id
            and (
                (remote_md5 and prev_state.get("md5Checksum") == remote_md5)
                or (remote_mtime and prev_state.get("modifiedTime") == remote_mtime)
            )
        ):
            return {"status": "unchanged", "file_id": file_id, "written_files": 0}

        headers = {"Authorization": f"Bearer {token}"}
        raw = bytearray()
        async with http.stream(
            "GET",
            f"{API}/{file_id}",
            headers=headers,
            params={"alt": "media", "supportsAllDrives": "true"},
            follow_redirects=False,
        ) as resp:
            if resp.status_code != 200:
                raise Rejected("bridge_download_failed")
            async for chunk in resp.aiter_bytes():
                raw.extend(chunk)
                if len(raw) > MAX_BRIDGE_BYTES:
                    raise Rejected("bridge_payload_too_large")

        if remote_md5:
            actual_md5 = hashlib.md5(raw, usedforsecurity=False).hexdigest().lower()
            if actual_md5 != remote_md5:
                raise Rejected("bridge_checksum_mismatch")

        try:
            parsed = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise Rejected("bridge_invalid_json") from None

        validated = validate_bridge_payload(parsed)
        written = await asyncio.to_thread(write_memory_dir_atomic, mem_dir, validated)
        saved_hashes = await asyncio.to_thread(_core_hashes_sync, mem_dir)
        new_state = {
            "file_id": file_id,
            "md5Checksum": remote_md5,
            "modifiedTime": remote_mtime,
            "notebook_sha256": saved_hashes["notebook_sha256"],
            "short_term_sha256": saved_hashes["short_term_sha256"],
            "updated_at": datetime.now(UTC).isoformat(),
            "scope": SCOPE,
        }
        await asyncio.to_thread(_write_state_sync, cfg["state_file"], new_state)
        return {"status": "pulled", "file_id": file_id, "written_files": written}

    if client is not None:
        return await _execute(client)
    async with httpx.AsyncClient(timeout=60, trust_env=False, follow_redirects=False) as http:
        return await _execute(http)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--init", action="store_true", help="Create or locate bridge file on Drive")
    mode.add_argument("--pull", action="store_true", help="Pull owner memory bridge from Drive")
    mode.add_argument("--extract-tar", type=Path, help="Extract validated stdin tar into dir")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--folder-id", help="Override GDRIVE_ROAMING_FOLDER_ID")
    parser.add_argument("--credentials", type=Path, help="Override roaming-oauth.json path")
    parser.add_argument("--memory-dir", type=Path, help="Override secrets/antigravity-memory")
    parser.add_argument("--state-file", type=Path, help="Override .owner_bridge_state.json")
    parser.add_argument("--env-file", type=Path, help="Override .env file path")
    parser.add_argument(
        "--force", action="store_true", help="Force re-pull even if remote/local hashes match"
    )
    args = parser.parse_args(argv)

    try:
        if args.extract_tar is not None:
            raw = sys.stdin.buffer.read(MAX_TAR_BYTES + 1)
            count = extract_safe_tar_stream(raw, args.extract_tar)
            print(json.dumps({"status": "staged", "files": count}, ensure_ascii=False))
            return 0

        cfg = resolve_config(
            repo_root=args.repo_root,
            folder_id=args.folder_id,
            credentials=args.credentials,
            memory_dir=args.memory_dir,
            state_file=args.state_file,
            env_file=args.env_file,
        )
        if args.init:
            result = asyncio.run(run_init(cfg))
        else:
            result = asyncio.run(run_pull(cfg, force=args.force))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Rejected as exc:
        print(
            json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False),
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            json.dumps({"status": "error", "error": "bridge_failed"}, ensure_ascii=False),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
