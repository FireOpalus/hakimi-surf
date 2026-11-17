"""Host/ops CLI for creating and verifying encrypted SQLite database backups.

Usage:
    .venv/bin/python scripts/backup_db.py [--verify-decrypt] [--upload-drive]
    .venv/bin/python scripts/backup_db.py --verify-decrypt --file path/to/backup.sqlite3.gz.enc
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx

from hakimi.config import Settings
from hakimi.db_backup import DatabaseBackup, verify_encrypted_backup
from hakimi.drive_client import FolderDrive
from hakimi.security import Rejected
from hakimi.store import Store


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create and verify encrypted hakimi SQLite backups."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional .env file path (defaults to Settings default).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Override HAKIMI_DATA_DIR (e.g. host volume data directory).",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Existing .sqlite3.gz.enc file to verify with --verify-decrypt.",
    )
    parser.add_argument(
        "--verify-decrypt",
        action="store_true",
        help="Decrypt, gunzip, and run PRAGMA integrity_check on the backup.",
    )
    parser.add_argument(
        "--upload-drive",
        action="store_true",
        help="Also upload the encrypted backup to Google Drive if roaming_enabled.",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=7,
        help="Number of daily backups to retain (default: 7).",
    )
    return parser


def _load_settings(env_file: Path | None, data_dir: Path | None) -> Settings:
    settings = Settings(_env_file=env_file) if env_file is not None else Settings()
    if data_dir is not None:
        settings = settings.model_copy(update={"data_dir": data_dir})
    return settings


async def _run_cli(args: argparse.Namespace) -> dict[str, Any]:
    settings = _load_settings(args.env_file, args.data_dir)
    key = settings.encryption_key.get_secret_value()

    if args.file is not None:
        if not args.verify_decrypt:
            raise ValueError("--file requires --verify-decrypt")
        verified = await asyncio.to_thread(verify_encrypted_backup, args.file, key)
        return {"status": "ok", "verified": verified}

    store = Store(settings)
    result: dict[str, Any]
    if args.upload_drive and settings.roaming_enabled:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10),
            trust_env=False,
            follow_redirects=False,
        ) as http:
            drive = FolderDrive(settings, http, store.vault.digest("db-backup", settings.namespace))
            backup = DatabaseBackup(settings, store, drive, keep_days=args.keep_days)
            result = await backup.snapshot_once(upload_drive=True)
    else:
        backup = DatabaseBackup(settings, store, None, keep_days=args.keep_days)
        result = await backup.snapshot_once(upload_drive=False)

    output: dict[str, Any] = {"status": "ok", "snapshot": result}
    if args.verify_decrypt:
        verified = await asyncio.to_thread(verify_encrypted_backup, Path(result["path"]), key)
        output["verified"] = verified
    return output


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        output = asyncio.run(_run_cli(args))
    except Rejected as exc:
        err = json.dumps({"status": "error", "code": str(exc)}, ensure_ascii=False)
        print(err, file=sys.stderr)
        return 1
    except Exception as exc:
        err = json.dumps({"status": "error", "error": type(exc).__name__}, ensure_ascii=False)
        print(err, file=sys.stderr)
        return 1
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
