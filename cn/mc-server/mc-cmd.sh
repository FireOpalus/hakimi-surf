#!/usr/bin/env bash
set -euo pipefail
if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <command>" >&2
  exit 1
fi
exec docker exec hakimi-mc-server java -cp /opt/mc McRcon "$*"
