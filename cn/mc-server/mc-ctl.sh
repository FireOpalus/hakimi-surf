#!/usr/bin/env bash
set -euo pipefail
umask 077

SERVER_DIR="/opt/hakimi-mc-server"
ENV_POWERD="$SERVER_DIR/.env.powerd"
MC_CMD="$SERVER_DIR/mc-cmd.sh"

usage() {
  cat <<'USAGE'
Usage: mc-ctl <start|stop|status>

Commands:
  start    Start hakimi-mc-server and wait for RCON ready (keeps MC_ENABLED=true intact)
  stop     Flush world save and gracefully stop hakimi-mc-server (keeps MC_ENABLED=true for on-demand wake)
  status   Show container states, hakimi-mc-powerd status, host memory, mc-agent /healthz, and online players
USAGE
}

mc_running() {
  docker ps --filter "name=^/hakimi-mc-server$" --filter "status=running" -q | grep -q .
}

query_powerd_status() {
  python3 - "$ENV_POWERD" <<'PYEOF'
import os
import sys
import urllib.error
import urllib.request

env_path = sys.argv[1]
cfg = {}
try:
    with open(env_path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
except OSError as exc:
    print(f"error=cannot_read_env ({type(exc).__name__})")
    raise SystemExit(0)

host = cfg.get("MC_POWERD_HOST", "172.22.0.1")
port = cfg.get("MC_POWERD_PORT", "8792")
token = cfg.get("MC_POWERD_TOKEN", "")
if not token:
    print("error=missing_token")
    raise SystemExit(0)

req = urllib.request.Request(
    f"http://{host}:{port}/status",
    headers={"Authorization": f"Bearer {token}"},
    method="GET",
)
try:
    with urllib.request.urlopen(req, timeout=5) as resp:
        print(resp.read().decode("utf-8", errors="replace"))
except urllib.error.HTTPError as exc:
    print(f"http_error={exc.code}")
except Exception as exc:  # noqa: BLE001
    print(f"error={type(exc).__name__}")
PYEOF
}

cmd="${1:-}"
case "$cmd" in
  start)
    echo "[mc-ctl] Starting hakimi-mc-server..."
    (cd "$SERVER_DIR" && docker compose up -d)
    ready=0
    for _ in $(seq 1 45); do
      if "$MC_CMD" "list" >/dev/null 2>&1; then
        ready=1
        break
      fi
      sleep 2
    done
    if [ "$ready" -ne 1 ]; then
      echo "[mc-ctl] Timed out waiting for MC RCON readiness after 90s" >&2
      exit 1
    fi
    # Refresh powerd readiness state / idle timer best-effort.
    query_powerd_status >/dev/null 2>&1 || true
    echo "[mc-ctl] Paper ready (MC_ENABLED kept true; mc-agent remains ready for on-demand connect)."
    ;;
  stop)
    if mc_running; then
      echo "[mc-ctl] Flushing world save..."
      "$MC_CMD" "save-all flush" || true
      sleep 2
      echo "[mc-ctl] Stopping hakimi-mc-server gracefully..."
      (cd "$SERVER_DIR" && docker compose stop)
    else
      echo "[mc-ctl] hakimi-mc-server is already stopped."
    fi
    echo "[mc-ctl] Minecraft server stopped cleanly (MC_ENABLED kept true for on-demand wake)."
    ;;
  status)
    echo "=== Containers ==="
    docker ps -a --filter "name=^/(hakimi-mc-server|hakimi-node-agent-hakimi-mc-agent-1)$" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo "=== hakimi-mc-powerd ==="
    printf "systemd: "
    systemctl is-active hakimi-mc-powerd 2>/dev/null || true
    printf "api: "
    query_powerd_status || true
    echo "=== Host Memory ==="
    free -h
    echo "=== mc-agent /healthz ==="
    docker exec hakimi-node-agent-hakimi-mc-agent-1 node -e 'fetch("http://127.0.0.1:8791/healthz").then(r=>r.text()).then(console.log)' || true
    if mc_running; then
      echo "=== Online Players ==="
      "$MC_CMD" "list" || true
    fi
    ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
