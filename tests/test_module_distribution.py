"""The platform checkouts share a stable public import namespace."""

import io
import subprocess
import tarfile
from importlib import import_module
from pathlib import Path

import hakimi.modules


def test_platform_sources_live_in_their_own_checkouts():
    platforms = Path(hakimi.modules.__file__).resolve().parent / "platforms"
    entries = {
        "qq": ("qq_social", "QQSocialModule"),
        "bilibili": ("bilibili_surf", "BilibiliSurfModule"),
        "minecraft": ("minecraft_slot", "MinecraftSlotModule"),
        "weather": ("weather", "WeatherModule"),
    }
    for checkout, (name, exported_class) in entries.items():
        module = import_module(f"hakimi.modules.{name}")
        assert Path(module.__file__).resolve() == platforms / checkout / f"{name}.py"
        assert issubclass(getattr(module, exported_class), hakimi.modules.BaseModule)


def test_release_archive_uses_nested_pins_and_ignores_working_changes(tmp_path):
    archive_script = Path(__file__).resolve().parents[1] / "scripts/archive_release.sh"
    host = tmp_path / "host"
    catalog = host / "catalog"
    platform = catalog / "platforms" / "weather"
    platform.mkdir(parents=True)

    def git(repo, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
            cwd=repo,
            text=True,
        ).strip()

    for repo in (host, catalog, platform):
        git(repo, "init", "-q", "-b", "main")
    entry = platform / "weather.py"
    entry.write_text("pinned\n")
    git(platform, "add", "weather.py")
    git(platform, "commit", "-qm", "pinned weather")
    for parent, child in ((catalog, platform), (host, catalog)):
        path = child.relative_to(parent).as_posix()
        git(
            parent,
            "update-index",
            "--add",
            "--cacheinfo",
            f"160000,{git(child, 'rev-parse', 'HEAD')},{path}",
        )
        git(parent, "commit", "-qm", "pin child")

    entry.write_text("newer\n")
    git(platform, "commit", "-qam", "next weather revision")
    entry.write_text("uncommitted\n")
    (platform / ".env").write_text("local only\n")
    data = subprocess.check_output([archive_script, "HEAD"], cwd=host)
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        assert archive.extractfile("./catalog/platforms/weather/weather.py").read() == b"pinned\n"
        assert all(
            ".git" not in Path(n).parts and not n.endswith(".env") for n in archive.getnames()
        )
    assert entry.read_text() == "uncommitted\n"
