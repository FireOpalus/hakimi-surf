"""Package an explicit source allowlist, never credentials, databases or local caches."""

import hashlib
import json
import tomllib
import zipfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
platforms = root / "src/hakimi/modules/platforms"
for entry in (
    "qq/qq_social.py",
    "bilibili/bilibili_surf.py",
    "minecraft/minecraft_slot.py",
    "weather/weather.py",
):
    if not (platforms / entry).is_file():
        raise SystemExit(f"Missing {entry}. Run git submodule update --init --recursive.")
output = root / "dist"
output.mkdir(exist_ok=True)
files = [
    root / name
    for name in (
        ".env.example",
        ".env.napcat.example",
        ".gitignore",
        ".dockerignore",
        "README.md",
        "architecture.html",
        "Dockerfile",
        "compose.yaml",
        "docker-compose.yml",
        "docker-compose.napcat.yml",
        "docker-compose.napcat.official-node.yml.example",
        "compose.roaming.yaml",
        "compose.roaming-oauth.yaml",
        "compose.napcat.yml",
        "pyproject.toml",
        "requirements.txt",
        "uv.lock",
    )
]
for directory, pattern in (
    ("src", "*.py"),
    ("src", "*.html"),
    ("tests", "*.py"),
    ("scripts", "*.py"),
    ("scripts", "*.sh"),
    ("deploy", "*.conf"),
    ("deploy", "*.json.example"),
    ("docs", "*.md"),
):
    files.extend(p for p in (root / directory).rglob(pattern) if ".git" not in p.parts)
files = sorted(set(files))
if any(p.is_symlink() or not p.is_file() for p in files):
    raise RuntimeError("Delivery source must be regular files")
manifest = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
manifest_path = output / "manifest.json"
manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
complete = output / "UPGRADE_FULL_CODE.txt"
with complete.open("w", encoding="utf-8") as stream:
    stream.write(f"哈基米跨平台冲浪助手 {version}：完整源码与配置\n")
    stream.write("含依赖锁定、部署配置、文档与测试；不含真实凭证或运行数据。\n")
    for path in files:
        stream.write(
            "\n" + "=" * 80 + "\nFILE: " + str(path.relative_to(root)) + "\n" + "=" * 80 + "\n"
        )
        stream.write(path.read_text(encoding="utf-8") + "\n")
archive = output / f"hakimi-surf-{version}.zip"
with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
    for path in files:
        bundle.write(path, "hakimi-surf/" + str(path.relative_to(root)))
    bundle.write(manifest_path, "hakimi-surf/dist/manifest.json")
    bundle.write(complete, "hakimi-surf/dist/UPGRADE_FULL_CODE.txt")
checksums = "".join(
    hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name + "\n"
    for path in (archive, complete, manifest_path)
)
(output / "SHA256SUMS").write_text(checksums)
print(f"Packaged {len(files)} source files; no runtime credentials or data included.")
