"""Offline Docker smoke test. Creates and removes only its own temporary container.

Run with the project venv: python scripts/smoke_container.py hakimi-surf:upgrade-test
No real credentials, external network, QQ account or paid model calls are used.
"""

import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from cryptography.fernet import Fernet

PROBE = r"""
import asyncio, hashlib, hmac, io, json, os, sqlite3, time, urllib.request, urllib.error
import subprocess, importlib.metadata
from PIL import Image
from websockets.asyncio.client import connect

origin = "http://127.0.0.1:12141"
event = {"post_type":"message", "self_id":10000, "user_id":20000,
    "message_type":"private", "message_id":42, "time":int(time.time()),
    "message":[{"type":"text","data":{"text":"/new-session"}}]}

async def check():
    headers = {"Authorization":"Bearer " + os.environ["HAKIMI_ONEBOT_WS_TOKEN"],
        "X-Self-ID":"10000", "X-Client-Role":"Universal"}
    async with connect("ws://127.0.0.1:12141/ws/onebot",
            additional_headers=headers, proxy=None) as ws:
        request = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert request["action"] == "get_login_info"
        await ws.send(json.dumps({"status":"ok","retcode":0,
            "echo":request["echo"],"data":{"user_id":10000}}))
        await ws.send(json.dumps(event))
        request = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert request["action"] == "send_private_msg"
        assert request["params"]["user_id"] == 20000
        assert "已清空当前会话" in request["params"]["message"][0]["data"]["text"]
        await ws.send(json.dumps({"status":"ok","retcode":0,
            "echo":request["echo"],"data":{"message_id":99}}))
        await asyncio.sleep(0.2)

        # A known bot must be discarded before any paid call, despite smart_reply + allowlist.
        bot_event = {**event, "user_id":60000, "message_type":"group", "group_id":40000,
            "message_id":43, "message":[{"type":"text","data":{"text":"hello bot"}}]}
        await ws.send(json.dumps(bot_event))
        owner_invite = {**event, "message_type":"group", "group_id":40000,
            "message_id":44, "message":[
                {"type":"at","data":{"qq":"10000"}},
                {"type":"text","data":{"text":" /bot-interact "}},
                {"type":"at","data":{"qq":"60000"}},
                {"type":"text","data":{"text":" discuss astronomy"}}]}
        await ws.send(json.dumps(owner_invite))
        request = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert request["action"] == "send_group_msg"
        assert request["params"]["group_id"] == 40000
        assert "已授权" in request["params"]["message"][0]["data"]["text"]
        await ws.send(json.dumps({"status":"ok","retcode":0,
            "echo":request["echo"],"data":{"message_id":100}}))
        await asyncio.sleep(0.2)
        policy = urllib.request.Request(origin + "/admin/api/groups", method="PUT",
            data=json.dumps({"group_id":"40000","mode":"at_only"}).encode(),
            headers={"Authorization":"Bearer " + os.environ["ADMIN_TOKEN"],
                     "Content-Type":"application/json"})
        assert urllib.request.urlopen(policy, timeout=3).status == 200
        quoted = {**event, "message_type":"group", "group_id":40000,"message_id":45,
            "message":[{"type":"reply","data":{"id":"100"}},
                {"type":"text","data":{"text":"/new-session"}}]}
        await ws.send(json.dumps(quoted))
        request = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert request["action"] == "send_group_msg"
        assert "已清空当前会话" in request["params"]["message"][0]["data"]["text"]
        await ws.send(json.dumps({"status":"ok","retcode":0,
            "echo":request["echo"],"data":{"message_id":101}}))
        await asyncio.sleep(0.2)
        # A normal member absent from private allowed_users can use the allowlisted group.
        member = {**event,"message_type":"group","group_id":40000,"user_id":30000,
            "message_id":46,"message":[{"type":"at","data":{"qq":"10000"}},
                {"type":"text","data":{"text":"/哈基米 帮助"}}]}
        await ws.send(json.dumps(member))
        request = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert request["action"] == "send_group_msg"
        assert "哈基米在这里" in request["params"]["message"][0]["data"]["text"]
        await ws.send(json.dumps({"status":"ok","retcode":0,
            "echo":request["echo"],"data":{"message_id":102}}))
        # Group admission grants no private access to that same member.
        await ws.send(json.dumps({**event,"user_id":30000,"message_id":47,
            "message":[{"type":"text","data":{"text":"/哈基米 帮助"}}]}))
        # A real @other takes precedence over a verified quoted bot message.
        await ws.send(json.dumps({**member,"message_id":48,"message":[
            {"type":"at","data":{"qq":"31000"}},
            {"type":"reply","data":{"id":"102"}},
            {"type":"text","data":{"text":"你刚才说的是什么意思？"}}]}))
        await asyncio.sleep(0.2)

asyncio.run(check())
body = json.dumps(event).encode()
signature = "sha1=" + hmac.new(os.environ["HAKIMI_ONEBOT_SECRET"].encode(),
    body, hashlib.sha1).hexdigest()
request = urllib.request.Request(origin + "/webhooks/onebot", data=body,
    headers={"Content-Type":"application/json","X-Signature":signature})
assert urllib.request.urlopen(request, timeout=3).status == 204
request.headers["X-signature"] = "sha1=forged"
try:
    urllib.request.urlopen(request, timeout=3)
    raise AssertionError("forged webhook was accepted")
except urllib.error.HTTPError as error:
    assert error.code == 401
with sqlite3.connect("/app/data/hakimi.sqlite3") as db:
    assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 4
    assert db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 4
    assert db.execute("SELECT COUNT(*) FROM sent_bot_receipts").fetchone()[0] == 3
    assert db.execute("SELECT COUNT(*) FROM bot_interactions WHERE consumed=0").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM bot_turns").fetchone()[0] == 0
    assert db.execute("SELECT COALESCE(SUM(amount),0) FROM ledger").fetchone()[0] == 0
assert os.getuid() == 10001
page = urllib.request.urlopen(origin + "/admin", timeout=3)
assert page.status == 200 and b"<!doctype html>" in page.read()
try:
    urllib.request.urlopen(origin + "/admin/api/dashboard", timeout=3)
    raise AssertionError("admin API accepted no token")
except urllib.error.HTTPError as error:
    assert error.code == 401
admin = urllib.request.Request(origin + "/admin/api/dashboard",
    headers={"Authorization":"Bearer " + os.environ["ADMIN_TOKEN"]})
with urllib.request.urlopen(admin, timeout=3) as response:
    assert response.headers["Cache-Control"] == "no-store"
    dashboard = json.load(response)
    assert sum(row["calls"] for row in dashboard["usage"]["lanes"]) == 0
    assert dashboard["usage"]["token_usage"]["today"]["cached_tokens"] == 0
for path in ("admission", "moderation", "longnick", "bili-account", "bili-account/video-watch",
             "thinking", "filter", "decision-audit", "budget"):
    probe = urllib.request.Request(origin + "/admin/api/" + path,
        headers={"Authorization":"Bearer " + os.environ["ADMIN_TOKEN"]})
    with urllib.request.urlopen(probe, timeout=3) as response:
        assert response.status == 200
        data = json.load(response)
        if path == "admission":
            assert data["config"]["private_mode"] == "owner_only"
            assert data["allow_group_members"] is True
            assert data["group_membership_policy"] == "all_non_blocked_members"
        elif path == "bili-account":
            assert data["config"]["enabled"] is False
        elif path == "longnick":
            assert data["config"]["enabled"] is False
        elif path == "bili-account/video-watch":
            assert data["config"] == {"enabled":False,"max_seconds":480,"daily_limit":3}
            assert data["attempts_today"] == 0 and data["remote_cleanup_pending"] == 0
        elif path == "filter":
            assert data["config"]["injection_threshold"] == 0.78
            assert data["config"]["reply_threshold"] == 0.42
        elif path == "decision-audit":
            assert data == []
        elif path == "budget":
            assert float(data["config"]["monthly_usd"]) == 40
        elif path == "thinking":
            assert len(data["config"]) == 9
            assert set(data["config"].values()) == {"medium"}
            assert data["levels"] == ["low", "medium", "high", "xhigh"]
            for level in ("xhigh", "medium"):
                updated = {key:level for key in data["config"]}
                save = urllib.request.Request(origin + "/admin/api/thinking", method="PUT",
                    data=json.dumps(updated).encode(), headers={
                        "Authorization":"Bearer " + os.environ["ADMIN_TOKEN"],
                        "Content-Type":"application/json"})
                with urllib.request.urlopen(save, timeout=3) as saved:
                    result = json.load(saved)
                    assert result["config"] == updated and result["source"] == "sqlite"
raw = io.BytesIO()
Image.new("RGB", (24, 16), "purple").save(raw, format="PNG")
auth = {"Authorization":"Bearer " + os.environ["ADMIN_TOKEN"]}
request = urllib.request.Request(origin + "/admin/api/memes/upload/smoke.png",
    data=raw.getvalue(), headers={**auth,"Content-Type":"image/png"})
with urllib.request.urlopen(request, timeout=5) as response:
    entry = json.load(response)
    assert entry["label"] == "smoke"
request = urllib.request.Request(origin + "/admin/api/memes/" + entry["id"] + "/image",
    headers=auth)
with urllib.request.urlopen(request, timeout=3) as response:
    assert response.headers["Cache-Control"] == "no-store"
    with Image.open(io.BytesIO(response.read())) as image:
        assert image.format == "JPEG"
request = urllib.request.Request(origin + "/admin/api/memes/" + entry["id"],
    method="DELETE",headers=auth)
assert urllib.request.urlopen(request, timeout=3).status == 200
request = urllib.request.Request(origin + "/admin/api/memes",headers=auth)
with urllib.request.urlopen(request, timeout=3) as response:
    assert json.load(response)["items"] == []
assert importlib.metadata.version("yt-dlp") == "2026.10.26"
assert subprocess.run(["ffprobe", "-version"], capture_output=True).returncode == 0
assert os.stat("/tmp/bili_video_cache").st_mode & 0o777 == 0o700
assert not os.listdir("/tmp/bili_video_cache")
print("PASS: rootless container, reverse WS login/replies, strict bot ignore, "
    "independent group/private admission, verified quoted reply, "
    "Webhook dedup/auth, L0 other-mention ignore, admin meme upload/preview/delete, "
    "admin/filter/budget controls, "
    "ffprobe/yt-dlp, clean cache, zero model spend")
"""


def run(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True, **kwargs)


def main():
    image = sys.argv[1] if len(sys.argv) > 1 else "hakimi-surf:upgrade-test"
    name = "hakimi-upgrade-smoke-" + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix="hakimi-smoke-") as temp:
        env = Path(temp) / "test.env"
        env.write_text(
            "\n".join(
                [
                    "HAKIMI_GEMINI_API_KEY=offline-test-never-live-key",
                    "HAKIMI_ENCRYPTION_KEY=" + Fernet.generate_key().decode(),
                    "HAKIMI_DATA_DIR=/app/data",
                    "ADMIN_TOKEN=offline-admin-smoke-token",
                    "HAKIMI_ONEBOT_ENABLED=true",
                    "HAKIMI_ONEBOT_SELF_ID=10000",
                    'HAKIMI_ONEBOT_OWNER_IDS=["20000"]',
                    "OWNER_QQ_ID=20000",
                    "PROACTIVE_DM_ENABLED=false",
                    'BOT_QQ_IDS=["60000"]',
                    'HAKIMI_ONEBOT_ALLOWED_USERS=["60000"]',
                    'HAKIMI_ONEBOT_ALLOWED_GROUPS=["40000"]',
                    "HAKIMI_ALLOW_GROUP_MEMBERS=false",
                    'HAKIMI_GROUP_POLICIES={"onebot:40000":"smart_reply"}',
                    "HAKIMI_ONEBOT_SECRET=offline-webhook-test-secret",
                    "HAKIMI_ONEBOT_MODE=ws_reverse",
                    "HAKIMI_ONEBOT_WS_TOKEN=offline-ws-test-secret-longer-than-32-characters",
                    "HAKIMI_SEND_DELAY_MIN=0",
                    "HAKIMI_SEND_DELAY_MAX=0",
                ]
            )
        )
        os.chmod(env, 0o600)
        try:
            run(
                "run",
                "-d",
                "--name",
                name,
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--tmpfs",
                "/app/data:rw,uid=10001,gid=10001,mode=700,size=32m",
                "--tmpfs",
                "/tmp:rw,mode=1777,size=32m",
                "--env-file",
                str(env),
                image,
            )
            for _ in range(40):
                try:
                    run(
                        "exec",
                        name,
                        "python",
                        "-c",
                        "import urllib.request; urllib.request.urlopen('http://127.0.0.1:12141/healthz',timeout=1)",
                    )
                    break
                except subprocess.CalledProcessError:
                    time.sleep(0.25)
            else:
                raise RuntimeError("container did not become healthy")
            result = run("exec", name, "python", "-c", PROBE)
            print(result.stdout.strip())
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


if __name__ == "__main__":
    main()
