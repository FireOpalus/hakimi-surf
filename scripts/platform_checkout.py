"""Materialize the catalog's pinned private repositories in CI, using one read key per job."""

import argparse
import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "src/hakimi/modules"
# Run before project dependencies or private platform sources have been installed.
sys.path.insert(0, str(CATALOG))
from manifest import check_entry, read_manifest  # noqa: E402


def pins(catalog):
    tree = subprocess.check_output(
        ["git", "ls-tree", "-rz", "HEAD", "--", "platforms"], cwd=catalog, text=True
    )
    result = {}
    for entry in tree.split("\0"):
        if entry.startswith("160000 commit "):
            header, path = entry.split("\t", 1)
            result[path] = header.split()[-1]
    if set(result) != {p.path for p in read_manifest(catalog)}:
        raise ValueError("The platform Git pins must match .gitmodules")
    return result


def github_repository(url, owner):
    name = r"[A-Za-z0-9_-][A-Za-z0-9_.-]*"
    url = url.removesuffix(".git")
    if re.fullmatch(rf"\.\./{name}", url):
        return f"{owner}/{url[3:]}"
    match = re.fullmatch(rf"(?:https://github\.com/|git@github\.com:)({name}/{name})", url)
    if not match:
        raise ValueError("CI needs a sibling or GitHub repository URL in .gitmodules")
    return match[1]


def checkout_matrix(catalog, owner):
    revisions = pins(catalog)
    return {
        "include": [
            {
                "name": p.name,
                "key_suffix": p.name.upper(),
                "repository": github_repository(p.url, owner),
                "sha": revisions[p.path],
            }
            for p in read_manifest(catalog)
        ]
    }


def restore(catalog, archives):
    revisions = pins(catalog)
    for platform in read_manifest(catalog):
        with (archives / f"{platform.name}.tar").open("rb") as stream:
            sha = subprocess.check_output(["git", "get-tar-commit-id"], stdin=stream, text=True)
            if sha.strip() != revisions[platform.path]:
                raise ValueError(f"Wrong archive revision for {platform.path}")
            stream.seek(0)
            with tarfile.open(fileobj=stream, mode="r:") as archive:
                archive.extractall(catalog / platform.path, filter="data")
        check_entry(catalog, platform)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("matrix")
    actions.add_parser("check")
    actions.add_parser("restore").add_argument("archives", type=Path)
    args = parser.parse_args()
    if args.action == "matrix":
        print(json.dumps(checkout_matrix(CATALOG, os.environ["GITHUB_REPOSITORY_OWNER"])))
    elif args.action == "restore":
        restore(CATALOG, args.archives)
    else:
        installed = read_manifest(CATALOG)
        for platform in installed:
            check_entry(CATALOG, platform)
        directories = {p.name for p in (CATALOG / "platforms").iterdir() if p.is_dir()}
        extra = directories - {p.name for p in installed} - {"__pycache__"}
        if extra:
            raise ValueError(f"Unlisted platforms in release: {', '.join(sorted(extra))}")


if __name__ == "__main__":
    main()
