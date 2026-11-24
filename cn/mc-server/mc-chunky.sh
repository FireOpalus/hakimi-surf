#!/usr/bin/env bash
set -euo pipefail

MC_CMD="/opt/hakimi-mc-server/mc-cmd.sh"
WORLD_DIR="/opt/hakimi-mc-server/data/world"

usage() {
  cat <<'USAGE'
Usage: mc-chunky <command> [args]

Commands:
  start [radius]   Start square pre-generation centered at 0,0 (radius: 100..3000, default: 1000)
  status           Show recent Chunky logs, current progress, and region disk usage
  pause            Pause active Chunky task
  continue         Continue paused Chunky task
  cancel           Cancel and discard active/saved Chunky task
  -h, --help       Show this help message
USAGE
}

if [ "$#" -eq 0 ]; then
  usage >&2
  exit 1
fi

subcmd="$1"
case "$subcmd" in
  -h|--help|help)
    usage
    exit 0
    ;;
  start)
    if [ "$#" -gt 2 ]; then
      echo "Error: too many arguments for start" >&2
      usage >&2
      exit 1
    fi
    radius="${2:-1000}"
    if ! [[ "$radius" =~ ^[0-9]+$ ]] || [ "$radius" -lt 100 ] || [ "$radius" -gt 3000 ]; then
      echo "Error: radius must be an integer in 100..3000 (got: $radius)" >&2
      exit 1
    fi
    "$MC_CMD" "chunky world world"
    "$MC_CMD" "chunky shape square"
    "$MC_CMD" "chunky center 0 0"
    "$MC_CMD" "chunky radius $radius"
    "$MC_CMD" "chunky quiet 60"
    "$MC_CMD" "chunky start"
    ;;
  status)
    if [ "$#" -ne 1 ]; then
      echo "Error: status takes no arguments" >&2
      exit 1
    fi
    echo "=== Chunky Logs (last 10) ==="
    docker logs hakimi-mc-server 2>&1 | grep -i chunky | tail -n 10 || true
    echo "=== Chunky Live Progress ==="
    "$MC_CMD" "chunky progress" || true
    echo "=== World Region Disk Usage ==="
    if [ -d "$WORLD_DIR/region" ]; then
      du -sh "$WORLD_DIR/region"
    elif [ -d "$WORLD_DIR/dimensions/minecraft/overworld/region" ]; then
      du -sh "$WORLD_DIR/dimensions/minecraft/overworld/region"
    else
      du -sh "$WORLD_DIR"
    fi
    ;;
  pause|continue|cancel)
    if [ "$#" -ne 1 ]; then
      echo "Error: $subcmd takes no arguments" >&2
      exit 1
    fi
    "$MC_CMD" "chunky $subcmd"
    ;;
  *)
    echo "Error: unknown subcommand '$subcmd'" >&2
    usage >&2
    exit 1
    ;;
esac
