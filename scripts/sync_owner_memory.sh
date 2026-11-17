#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC_DIR="${SRC_DIR:-/root/.gemini/Antigravity_Memory}"
DEST_DIR="${DEST_DIR:-${REPO_ROOT}/secrets/antigravity-memory}"

if [[ "${1:-}" == "--pull" ]]; then
  if command -v gdrive >/dev/null 2>&1 && [[ -f /tmp/ag_kit/gdrive_sync.py ]]; then
    python3 /tmp/ag_kit/gdrive_sync.py --data-dir /tmp/Antigravity_Memory cloud-boot >/dev/null
  else
    echo "error: gdrive CLI or /tmp/ag_kit/gdrive_sync.py not available for --pull" >&2
    exit 1
  fi
fi

if [[ ! -d "${SRC_DIR}" ]]; then
  echo "error: source memory directory not found: ${SRC_DIR}" >&2
  exit 1
fi

ensure_dir() {
  local dir="$1"
  mkdir -p "${dir}"
  chown root:10001 "${dir}"
  chmod 0750 "${dir}"
}

copy_atomic() {
  local src="$1"
  local dst="$2"
  [[ -f "${src}" && ! -L "${src}" ]] || return 0
  local tmp
  tmp="$(mktemp "${dst}.tmp.XXXXXX")"
  cp -f "${src}" "${tmp}"
  chown root:10001 "${tmp}"
  chmod 0640 "${tmp}"
  mv -f "${tmp}" "${dst}"
}

ensure_dir "${DEST_DIR}"
ensure_dir "${DEST_DIR}/long_term"
ensure_dir "${DEST_DIR}/notes"

copy_atomic "${SRC_DIR}/notebook.md" "${DEST_DIR}/notebook.md"
copy_atomic "${SRC_DIR}/short_term_memory.json" "${DEST_DIR}/short_term_memory.json"

shopt -s nullglob
for f in "${SRC_DIR}/long_term/"*.md; do
  copy_atomic "${f}" "${DEST_DIR}/long_term/$(basename "${f}")"
done
for f in "${SRC_DIR}/notes/"*.md; do
  copy_atomic "${f}" "${DEST_DIR}/notes/$(basename "${f}")"
done
shopt -u nullglob

echo "synced read-only owner memory to ${DEST_DIR}"
