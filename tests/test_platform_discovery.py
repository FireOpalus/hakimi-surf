"""Boot the host with a fifth module and exercise its real admin and timer paths."""

import asyncio
import os
import shutil
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.modules.manifest import read_manifest
from scripts.platform_checkout import checkout_matrix, restore
from tests.conftest import settings as settings_fixture
from tests.fakes import FakeGateway

ROOT = Path(__file__).resolve().parents[1]
ENTRY = (
    '[submodule "platforms/greenhouse"]\n path = platforms/greenhouse\n url = ../greenhouse.git\n'
)
PLUGIN = """
from contextlib import asynccontextmanager
from fastapi import APIRouter
from hakimi.modules import BaseModule
from hakimi.modules.actions import Action, HourlyAction, Outcome
from hakimi.modules.scheduling import ScheduleOptions

admin_router = APIRouter()

@admin_router.get("/probe")
async def probe():
    return {"installed": True}

class Options(ScheduleOptions):
    humidity: int = 55

class Greenhouse(BaseModule):
    module_id = "greenhouse"
    display_name = "温室"
    description = "读取温室湿度"
    enabled = False
    schedule_options = Options
    readings = 0

    @asynccontextmanager
    async def lifespan(self):
        self.ready = True
        try:
            yield
        finally:
            self.ready = False

    async def _raw_sensor_summary(self):
        return "湿度正常"

    def schedules(self):
        return (HourlyAction("greenhouse.read"),)

    def actions(self):
        return (Action(name="greenhouse.read", description="湿度采样", origins=frozenset({"timer"}),
                       run=self.read, background_sensor=True),)

    async def read(self, call):
        assert self.ready
        self.readings += 1
        return Outcome("done", "sampled")

def create_module(service):
    return Greenhouse(service.s)
"""


async def exercise_installed_module(data_dir):
    settings = settings_fixture.__wrapped__(data_dir)
    settings.admin_token = SecretStr("isolated-discovery-test")
    base = "/admin/api/life/modules"
    for first_boot in (True, False):
        app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
            ) as client,
        ):
            assert (await client.get(base + "/greenhouse/probe")).status_code == 401
            client.headers["Authorization"] = "Bearer isolated-discovery-test"
            assert (await client.get(base + "/greenhouse/probe")).json() == {"installed": True}
            rows = {row["id"]: row for row in (await client.get(base)).json()}
            assert (
                rows["greenhouse"]["title"] == "温室"
                and rows["greenhouse"]["description"] == "读取温室湿度"
            )
            assert rows["greenhouse"]["enabled"] is not first_boot
            module = app.state.service.modules.get("greenhouse")
            assert module.ready
            detail = (await client.get(base + "/greenhouse")).json()
            assert [a["name"] for a in detail["actions"]] == ["greenhouse.read"]
            if first_boot:
                for other in rows.keys() - {"greenhouse"}:
                    response = await client.put(base + f"/{other}/enabled", json={"enabled": False})
                    assert response.is_success
                assert (await client.get(base + "/greenhouse/sensor")).status_code == 409
                assert (
                    await client.put(base + "/greenhouse/enabled", json={"enabled": True})
                ).is_success
                assert (
                    await client.put(base + "/greenhouse/schedule", json={"humidity": 67})
                ).is_success
                assert (await client.get(base + "/greenhouse/sensor")).json() == {
                    "summary": "湿度正常"
                }
                engine = app.state.service.life_engine
                await engine.tick_gatekeeper_once(now=engine.clock())
                assert module.readings == 1
            else:
                assert detail["scheduling"]["values"]["humidity"] == 67
            assert not app.state.service.ai.calls
        assert not module.ready


def test_fifth_module_from_clean_process(tmp_path):
    source = tmp_path / "src"
    shutil.copytree(ROOT / "src", source, ignore=shutil.ignore_patterns(".git", "__pycache__"))
    catalog = source / "hakimi/modules"
    with (catalog / ".gitmodules").open("a") as manifest:
        manifest.write("\n" + ENTRY)
    plugin = catalog / "platforms/greenhouse"
    plugin.mkdir()
    (plugin / "plugin.py").write_text(PLUGIN)
    unrelated = catalog / "platforms/not_installed"
    unrelated.mkdir()
    (unrelated / "plugin.py").write_text("raise AssertionError('not declared in the manifest')")
    subprocess.run(
        [sys.executable, "-m", "tests.test_platform_discovery", str(tmp_path)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(map(str, [source, ROOT]))},
        check=True,
        timeout=60,
    )


@pytest.mark.parametrize(
    "path", ["../greenhouse", "platforms/a/b", "/tmp/module", "platforms/../core"]
)
def test_catalog_rejects_invalid_import_paths(tmp_path, path):
    (tmp_path / ".gitmodules").write_text(
        ENTRY.replace("path = platforms/greenhouse", "path = " + path)
    )
    with pytest.raises(ValueError, match="Invalid platform"):
        read_manifest(tmp_path)


def test_ci_fetches_and_restores_only_the_revision_in_the_manifest(tmp_path):
    def git(cwd, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", *args],
            cwd=cwd,
            text=True,
        ).strip()

    catalog = tmp_path / "catalog"
    upstream = tmp_path / "upstream"
    for repo in (catalog, upstream):
        repo.mkdir()
        git(repo, "init", "-q")
    (upstream / "plugin.py").write_text(PLUGIN)
    git(upstream, "add", "plugin.py")
    git(upstream, "commit", "-qm", "platform")
    sha = git(upstream, "rev-parse", "HEAD")
    (catalog / ".gitmodules").write_text(ENTRY)
    git(catalog, "add", ".gitmodules")
    git(catalog, "update-index", "--add", "--cacheinfo", f"160000,{sha},platforms/greenhouse")
    git(catalog, "commit", "-qm", "catalog")
    assert checkout_matrix(catalog, "example") == {
        "include": [
            {
                "name": "greenhouse",
                "key_suffix": "GREENHOUSE",
                "repository": "example/greenhouse",
                "sha": sha,
            }
        ]
    }
    tar = tmp_path / "greenhouse.tar"
    git(upstream, "archive", "--format=tar", f"--output={tar}", "HEAD")
    restore(catalog, tmp_path)
    assert (catalog / "platforms/greenhouse/plugin.py").read_text() == PLUGIN
    (upstream / "plugin.py").write_text("newer")
    git(upstream, "commit", "-qam", "newer")
    git(upstream, "archive", "--format=tar", f"--output={tar}", "HEAD")
    with pytest.raises(ValueError, match="Wrong archive revision"):
        restore(catalog, tmp_path)
    assert (catalog / "platforms/greenhouse/plugin.py").read_text() == PLUGIN


if __name__ == "__main__":
    asyncio.run(exercise_installed_module(Path(sys.argv[1])))
