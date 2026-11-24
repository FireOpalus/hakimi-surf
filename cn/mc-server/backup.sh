#!/usr/bin/env bash
set -euo pipefail
umask 077

SERVER_DIR="/opt/hakimi-mc-server"
DATA_DIR="$SERVER_DIR/data"
BACKUP_DIR="$SERVER_DIR/backups"
MC_CMD="$SERVER_DIR/mc-cmd.sh"

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

SAVE_DISABLED=0
restore_save() {
  if [ "$SAVE_DISABLED" -eq 1 ]; then
    "$MC_CMD" "save-on" || true
    SAVE_DISABLED=0
  fi
}
trap restore_save EXIT

if docker ps --filter "name=^/hakimi-mc-server$" --filter "status=running" -q | grep -q .; then
  "$MC_CMD" "save-off"
  SAVE_DISABLED=1
  "$MC_CMD" "save-all flush"
  sleep 3
fi

TS="$(date +%Y%m%d-%H%M%S)"
ARCHIVE="$BACKUP_DIR/hakimi-mc-$TS.tar.gz"
TMP_ARCHIVE="$BACKUP_DIR/.hakimi-mc-$TS.tar.gz.tmp"

TARGETS=()
for item in world world_nether world_the_end plugins/LibreLogin whitelist.json ops.json server.properties; do
  if [ -e "$DATA_DIR/$item" ]; then
    TARGETS+=("$item")
  fi
done

if [ "${#TARGETS[@]}" -eq 0 ]; then
  echo "No backup targets found in $DATA_DIR" >&2
  exit 1
fi

tar -czf "$TMP_ARCHIVE" -C "$DATA_DIR" "${TARGETS[@]}"
chmod 600 "$TMP_ARCHIVE"
mv -f "$TMP_ARCHIVE" "$ARCHIVE"

restore_save

ls -1t "$BACKUP_DIR"/hakimi-mc-*.tar.gz 2>/dev/null | tail -n +8 | xargs -r rm -f
echo "Backup created: $ARCHIVE"
