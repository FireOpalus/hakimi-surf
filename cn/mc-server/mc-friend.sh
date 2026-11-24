#!/usr/bin/env bash
set -euo pipefail

exec python3 - "$@" <<'PYEOF'
import argparse
import json
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

MC_CMD = "/opt/hakimi-mc-server/mc-cmd.sh"
WHITELIST_PATH = "/opt/hakimi-mc-server/data/whitelist.json"
LIBRELOGIN_DB = "/opt/hakimi-mc-server/data/plugins/LibreLogin/user-data.db"
ENV_MC_PATH = "/opt/hakimi-node-agent/.env.mc"
NODE_AGENT_DIR = "/opt/hakimi-node-agent"

SERVER_ADDR = os.environ.get("MC_SERVER_ADDR", "<YOUR_MC_SERVER_HOST>:25565")
SUPPORTED_VERSIONS = "Minecraft Java 26.1.2（兼容 26.1 / 26.1.1）"

MC_UID = 10001
MC_GID = 10001
USERNAME_RE = re.compile(r"\A[A-Za-z0-9_]{3,16}\Z")
PROTECTED_ACCOUNTS = {
    name.strip().lower()
    for name in os.environ.get("MC_PROTECTED_ACCOUNTS", "").split(",")
    if name.strip()
}


def die(msg: str, code: int = 1) -> int:
    print(f"Error: {msg}", file=sys.stderr)
    return code


def validate_username(name: str) -> str | None:
    if not USERNAME_RE.fullmatch(name):
        return (
            f"无效玩家名 '{name}'：必须匹配 ^[A-Za-z0-9_]{{3,16}}$（3-16位字母、数字或下划线）。"
        )
    return None


def is_protected(name: str) -> bool:
    return name.lower() in PROTECTED_ACCOUNTS


def normalize_uuid(raw: str) -> str:
    return str(uuid.UUID(raw.strip()))


def run_mc_cmd(cmd: str, redact: str | None = None) -> str:
    res = subprocess.run(
        [MC_CMD, cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    stdout = (res.stdout or "").strip()
    stderr = (res.stderr or "").strip()
    if res.returncode != 0:
        detail = stderr or stdout or f"exit code {res.returncode}"
        if redact:
            detail = detail.replace(redact, "***")
        safe_cmd = cmd.replace(redact, "***") if redact else cmd
        raise RuntimeError(f"mc-cmd.sh failed for '{safe_cmd}': {detail}")
    return stdout


def atomic_write_text(
    path: str,
    content: str,
    mode: int,
    uid: int | None = None,
    gid: int | None = None,
) -> None:
    parent = os.path.dirname(path) or "."
    base = os.path.basename(path)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=parent,
            prefix=f".{base}.tmp.",
            delete=False,
        ) as fp:
            tmp_path = fp.name
            fp.write(content)
            fp.flush()
            os.fsync(fp.fileno())
        os.chmod(tmp_path, mode)
        if uid is not None and gid is not None:
            os.chown(tmp_path, uid, gid)
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def load_whitelist() -> list[dict]:
    if not os.path.exists(WHITELIST_PATH):
        return []
    with open(WHITELIST_PATH, "r", encoding="utf-8") as fp:
        raw = fp.read().strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise RuntimeError("whitelist.json is not a JSON array")
    return [item for item in data if isinstance(item, dict)]


def save_whitelist(entries: list[dict]) -> None:
    content = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"
    atomic_write_text(WHITELIST_PATH, content, mode=0o644, uid=MC_UID, gid=MC_GID)


def upsert_whitelist(canonical_name: str, player_uuid: str) -> list[dict]:
    entries = load_whitelist()
    name_low = canonical_name.lower()
    uuid_low = player_uuid.lower()

    for entry in entries:
        e_name = str(entry.get("name", "")).lower()
        e_uuid = str(entry.get("uuid", "")).lower()
        if e_name in PROTECTED_ACCOUNTS or e_uuid == uuid_low and e_name in PROTECTED_ACCOUNTS:
            if e_name == name_low or e_uuid == uuid_low:
                raise RuntimeError(f"拒绝覆盖受保护账号：{entry.get('name')}")

    filtered = []
    inserted = False
    new_entry = {"uuid": player_uuid, "name": canonical_name}
    for entry in entries:
        e_name = str(entry.get("name", "")).lower()
        e_uuid = str(entry.get("uuid", "")).lower()
        if e_name == name_low or e_uuid == uuid_low:
            if not inserted:
                filtered.append(new_entry)
                inserted = True
            continue
        filtered.append(entry)
    if not inserted:
        filtered.append(new_entry)

    save_whitelist(filtered)
    run_mc_cmd("whitelist reload")
    return filtered


def remove_from_whitelist(name: str) -> tuple[bool, str | None]:
    entries = load_whitelist()
    name_low = name.lower()
    matched_name = None
    kept = []
    for entry in entries:
        e_name = str(entry.get("name", ""))
        if e_name.lower() == name_low:
            matched_name = e_name
            continue
        kept.append(entry)
    changed = len(kept) != len(entries)
    if changed:
        save_whitelist(kept)
    run_mc_cmd("whitelist reload")
    return changed, matched_name


def parse_allowed_players(raw_val: str) -> list[str]:
    result = []
    seen = set()
    for part in raw_val.split(","):
        p = part.strip()
        if not p:
            continue
        low = p.lower()
        if low in seen:
            continue
        seen.add(low)
        result.append(p)
    return result


def read_allowed_players() -> list[str]:
    if not os.path.exists(ENV_MC_PATH):
        return []
    with open(ENV_MC_PATH, "r", encoding="utf-8") as fp:
        for line in fp:
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            key, val = stripped.split("=", 1)
            if key.strip() == "MC_ALLOWED_PLAYERS":
                return parse_allowed_players(val)
    return []


def sync_allowed_players(action: str, player_name: str) -> tuple[bool, list[str]]:
    if not os.path.exists(ENV_MC_PATH):
        raise RuntimeError(f"Missing {ENV_MC_PATH}")
    with open(ENV_MC_PATH, "r", encoding="utf-8") as fp:
        lines = fp.readlines()

    found_idx = None
    current_players: list[str] = []
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key, val = stripped.split("=", 1)
        if key.strip() == "MC_ALLOWED_PLAYERS":
            found_idx = idx
            current_players = parse_allowed_players(val)
            break

    target_low = player_name.lower()
    if action == "add":
        new_players = []
        found_exact = False
        found_case_diff = False
        for p in current_players:
            if p.lower() == target_low:
                if p == player_name:
                    found_exact = True
                    new_players.append(p)
                else:
                    found_case_diff = True
                    new_players.append(player_name)
            else:
                new_players.append(p)
        if not found_exact and not found_case_diff:
            new_players.append(player_name)
        changed = new_players != current_players or found_idx is None
    elif action == "remove":
        new_players = [p for p in current_players if p.lower() != target_low]
        changed = new_players != current_players
    else:
        raise ValueError(f"Unsupported action: {action}")

    if not changed:
        return False, current_players

    new_line = f"MC_ALLOWED_PLAYERS={','.join(new_players)}\n"
    if found_idx is not None:
        lines[found_idx] = new_line
    else:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        lines.append(new_line)

    atomic_write_text(ENV_MC_PATH, "".join(lines), mode=0o600, uid=0, gid=0)
    subprocess.run(
        [
            "docker",
            "compose",
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "hakimi-mc-agent",
        ],
        cwd=NODE_AGENT_DIR,
        stdout=subprocess.DEVNULL,
        check=True,
    )
    return True, new_players


def lookup_mojang_profile(name: str) -> tuple[str, str]:
    quoted = urllib.parse.quote(name, safe="")
    url = f"https://api.mojang.com/users/profiles/minecraft/{quoted}"
    req = urllib.request.Request(url, headers={"User-Agent": "hakimi-mc-friend/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            if resp.status == 204:
                raise LookupError("not_found")
            body = resp.read().decode("utf-8").strip()
            if not body:
                raise LookupError("not_found")
            data = json.loads(body)
    except urllib.error.HTTPError as exc:
        if exc.code in (204, 404):
            raise LookupError("not_found") from exc
        raise RuntimeError(f"Mojang API HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"Mojang API 请求失败或超时: {exc}") from exc

    raw_id = data.get("id")
    canonical_name = data.get("name")
    if not raw_id or not canonical_name:
        raise RuntimeError("Mojang API 返回格式异常")
    return normalize_uuid(str(raw_id)), str(canonical_name)


def query_librelogin_user(name: str) -> tuple[str, str] | None:
    if not os.path.exists(LIBRELOGIN_DB):
        raise RuntimeError(f"LibreLogin database not found: {LIBRELOGIN_DB}")
    conn = sqlite3.connect(f"file:{LIBRELOGIN_DB}?mode=ro", uri=True, timeout=5)
    try:
        row = conn.execute(
            "SELECT uuid, last_nickname FROM librepremium_data WHERE lower(last_nickname)=lower(?)",
            (name,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    raw_uuid, nickname = row
    return normalize_uuid(str(raw_uuid)), str(nickname or name)


def generate_temp_password() -> str:
    for _ in range(32):
        pw = secrets.token_urlsafe(9)
        if not pw.startswith("-"):
            return pw
    return "H" + secrets.token_urlsafe(9)[1:]


def print_invite_card(
    canonical_name: str,
    player_uuid: str,
    offline: bool,
    temp_pw: str | None,
    already_registered: bool,
    follow_enabled: bool,
    follow_synced: bool,
) -> None:
    if offline:
        if temp_pw is not None:
            auth_mode = "离线/第三方启动器账号（已预注册 LibreLogin）"
            login_guide = (
                f"首次进服后请在游戏聊天栏输入：/login {temp_pw}\n"
                f"（登录后建议立即改密：/changepassword {temp_pw} <你的新密码>）"
            )
        elif already_registered:
            auth_mode = "离线/第三方启动器账号（LibreLogin 已有档案）"
            login_guide = (
                "该账号之前已注册过 LibreLogin，请进服后使用原有密码输入：/login <原密码>\n"
                "（如忘记密码请联系服主重置）"
            )
        else:
            auth_mode = "离线/第三方启动器账号"
            login_guide = "进服后请按聊天栏提示执行 /login <密码> 完成验证。"
    else:
        auth_mode = "Mojang 正版账号（已加入正版白名单）"
        login_guide = "使用正版启动器直连即可；如提示验证请按游戏内聊天栏指引操作。"

    if follow_enabled:
        sync_note = "已同步并重载 hakimi-mc-agent" if follow_synced else "已在跟随白名单内（无需重启）"
        follow_line = f"AI (Geminy) 跟随白名单：已开启（{sync_note}）"
    else:
        follow_line = "AI (Geminy) 跟随白名单：未开启（使用了 --no-follow）"

    card = f"""
==================== 哈基米 MC 联机邀请卡 ====================
服务器地址 : {SERVER_ADDR}
游戏版本   : {SUPPORTED_VERSIONS}
玩家 ID    : {canonical_name}
玩家 UUID  : {player_uuid}
验证方式   : {auth_mode}
{follow_line}

【进服指引】
1. 打开 {SUPPORTED_VERSIONS} 客户端，多人游戏 -> 添加服务器：
   地址填写：{SERVER_ADDR}
2. {login_guide}
==============================================================
""".strip("\n")
    print(card)


def cmd_add(args: argparse.Namespace) -> int:
    name = args.name
    err = validate_username(name)
    if err:
        return die(err)
    if is_protected(name):
        return die(f"受保护账号 '{name}' 不允许通过 add 覆盖或修改。")

    temp_pw: str | None = None
    already_registered = False

    if args.offline:
        existing = query_librelogin_user(name)
        if existing is not None:
            player_uuid, canonical_name = existing
            already_registered = True
        else:
            temp_pw = generate_temp_password()
            out = run_mc_cmd(f"librelogin user register {name} {temp_pw}", redact=temp_pw)
            if "too fast" in out.lower():
                return die("LibreLogin 命令触发频率限制，请等待 2 秒后重试。")
            time.sleep(1)
            registered = None
            for _ in range(8):
                registered = query_librelogin_user(name)
                if registered is not None:
                    break
                time.sleep(0.5)
            if registered is None:
                return die(
                    f"未在 LibreLogin 数据库中查到新注册账号 '{name}'（可能因服务器未就绪或密码/用户名被插件拒绝）。"
                )
            player_uuid, canonical_name = registered
        if is_protected(canonical_name):
            return die(f"受保护账号 '{canonical_name}' 不允许覆盖。")
    else:
        try:
            player_uuid, canonical_name = lookup_mojang_profile(name)
        except LookupError:
            return die(
                f"未在 Mojang 查到正版账号 {name}；如对方使用离线启动器，请改用：mc-friend add {name} --offline"
            )
        except RuntimeError as exc:
            return die(str(exc))
        if is_protected(canonical_name):
            return die(f"受保护账号 '{canonical_name}' 不允许覆盖。")

    upsert_whitelist(canonical_name, player_uuid)

    follow_enabled = not args.no_follow
    follow_synced = False
    if follow_enabled:
        follow_synced, _ = sync_allowed_players("add", canonical_name)

    print_invite_card(
        canonical_name=canonical_name,
        player_uuid=player_uuid,
        offline=args.offline,
        temp_pw=temp_pw,
        already_registered=already_registered,
        follow_enabled=follow_enabled,
        follow_synced=follow_synced,
    )
    return 0


def cmd_remove(args: argparse.Namespace) -> int:
    name = args.name
    err = validate_username(name)
    if err:
        return die(err)
    if is_protected(name):
        return die(f"受保护账号 '{name}' 禁止移除。")

    wl_changed, matched_name = remove_from_whitelist(name)
    target_name = matched_name or name

    try:
        subprocess.run(
            [MC_CMD, f"kick {target_name} Removed from whitelist"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        pass

    follow_changed, allowed = sync_allowed_players("remove", target_name)

    print(
        f"Removed '{target_name}': whitelist={'removed' if wl_changed else 'not_present'}, "
        f"follow_allowlist={'removed_and_recreated' if follow_changed else 'not_present'}, "
        f"MC_ALLOWED_PLAYERS={','.join(allowed) if allowed else '(empty)'}"
    )
    return 0


def cmd_list(_args: argparse.Namespace) -> int:
    entries = load_whitelist()
    print("=== Whitelist (whitelist.json) ===")
    if not entries:
        print("  (empty)")
    else:
        for entry in entries:
            print(f"  - {entry.get('name', '?')} ({entry.get('uuid', '?')})")

    print("=== Online Players (mc-cmd.sh list) ===")
    try:
        online_out = run_mc_cmd("list")
        print(f"  {online_out or '(no output)'}")
    except Exception as exc:
        print(f"  (unavailable: {exc})")

    print("=== Follow Allowlist (MC_ALLOWED_PLAYERS) ===")
    allowed = read_allowed_players()
    if not allowed:
        print("  (empty)")
    else:
        print(f"  {', '.join(allowed)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mc-friend",
        description="Manage Hakimi MC whitelist, LibreLogin offline registration, and Geminy follow allowlist.",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    p_add = sub.add_parser(
        "add",
        help="Add a Mojang premium (or --offline LibreLogin) friend to whitelist and follow allowlist",
    )
    p_add.add_argument("name", help="Minecraft username (3-16 chars: A-Za-z0-9_)")
    p_add.add_argument(
        "--offline",
        action="store_true",
        help="Register an offline/cracked player via LibreLogin with a 12-char temp password",
    )
    p_add.add_argument(
        "--no-follow",
        action="store_true",
        help="Do not add player to MC_ALLOWED_PLAYERS follow allowlist",
    )

    p_rm = sub.add_parser(
        "remove",
        help="Remove a friend from whitelist, kick if online, and remove from MC_ALLOWED_PLAYERS",
    )
    p_rm.add_argument("name", help="Minecraft username (3-16 chars: A-Za-z0-9_)")

    sub.add_parser(
        "list",
        help="Show whitelist.json entries, online players, and MC_ALLOWED_PLAYERS",
    )
    return parser


def main() -> int:
    parser = build_parser()
    if len(sys.argv) <= 1:
        parser.print_help(sys.stderr)
        return 1
    args = parser.parse_args()
    if args.command == "add":
        return cmd_add(args)
    if args.command == "remove":
        return cmd_remove(args)
    if args.command == "list":
        return cmd_list(args)
    parser.print_help(sys.stderr)
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
PYEOF
