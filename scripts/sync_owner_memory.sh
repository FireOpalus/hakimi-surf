#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEST_DIR="${DEST_DIR:-${REPO_ROOT}/secrets/antigravity-memory}"

resolve_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    printf '%s\n' "${PYTHON_BIN}"
    return 0
  fi
  if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    printf '%s\n' "${REPO_ROOT}/.venv/bin/python"
    return 0
  fi
  if [[ -x "/root/Document/hakimi-surf/.venv/bin/python" ]]; then
    printf '%s\n' "/root/Document/hakimi-surf/.venv/bin/python"
    return 0
  fi
  command -v python3
}

chown_container_ro() {
  local target="$1"
  if [[ "$(id -u)" -eq 0 ]]; then
    chown root:10001 "${target}"
  fi
}

ensure_dir() {
  local dir="$1"
  if [[ -L "${dir}" ]]; then
    echo "error: symlink directory forbidden: ${dir}" >&2
    exit 1
  fi
  mkdir -p "${dir}"
  chown_container_ro "${dir}"
  chmod 0750 "${dir}"
}

copy_atomic() {
  local src="$1"
  local dst="$2"
  [[ -f "${src}" && ! -L "${src}" ]] || return 0
  if [[ -L "${dst}" ]]; then
    echo "error: symlink destination forbidden: ${dst}" >&2
    exit 1
  fi
  local tmp
  tmp="$(mktemp "${dst}.tmp.XXXXXX")"
  cp -f "${src}" "${tmp}"
  chown_container_ro "${tmp}"
  chmod 0640 "${tmp}"
  mv -f "${tmp}" "${dst}"
}

sync_from_dir() {
  local src_root="$1"
  if [[ ! -d "${src_root}" || -L "${src_root}" ]]; then
    echo "error: source memory directory not found: ${src_root}" >&2
    exit 1
  fi
  ensure_dir "${DEST_DIR}"
  ensure_dir "${DEST_DIR}/long_term"
  ensure_dir "${DEST_DIR}/notes"

  copy_atomic "${src_root}/notebook.md" "${DEST_DIR}/notebook.md"
  copy_atomic "${src_root}/short_term_memory.json" "${DEST_DIR}/short_term_memory.json"

  shopt -s nullglob
  for f in "${src_root}/long_term/"*.md; do
    copy_atomic "${f}" "${DEST_DIR}/long_term/$(basename "${f}")"
  done
  for f in "${src_root}/notes/"*.md; do
    copy_atomic "${f}" "${DEST_DIR}/notes/$(basename "${f}")"
  done
  shopt -u nullglob
}

ACTION="${1:-}"

case "${ACTION}" in
  --init-bridge)
    PY="$(resolve_python)"
    exec "${PY}" "${SCRIPT_DIR}/owner_memory_bridge.py" --init --repo-root "${REPO_ROOT}" --memory-dir "${DEST_DIR}"
    ;;
  --pull|--pull-drive)
    FORCE_ARG=()
    if [[ "${2:-}" == "--force" ]]; then
      FORCE_ARG=("--force")
    elif [[ -n "${2:-}" ]]; then
      echo "usage: $0 [--pull|--pull-drive [--force]|--init-bridge|--stdin-tar]" >&2
      exit 2
    fi
    PY="$(resolve_python)"
    exec "${PY}" "${SCRIPT_DIR}/owner_memory_bridge.py" --pull --repo-root "${REPO_ROOT}" --memory-dir "${DEST_DIR}" "${FORCE_ARG[@]}"
    ;;
  --stdin-tar)
    PY="$(resolve_python)"
    STAGING_DIR="$(mktemp -d)"
    trap 'rm -rf "${STAGING_DIR}"' EXIT
    "${PY}" "${SCRIPT_DIR}/owner_memory_bridge.py" --extract-tar "${STAGING_DIR}" >/dev/null
    sync_from_dir "${STAGING_DIR}"
    echo "synced read-only owner memory to ${DEST_DIR}"
    ;;
  "")
    if [[ -n "${SRC_DIR:-}" ]]; then
      sync_from_dir "${SRC_DIR}"
      echo "synced read-only owner memory to ${DEST_DIR}"
    else
      PY="$(resolve_python)"
      exec "${PY}" "${SCRIPT_DIR}/owner_memory_bridge.py" --pull --repo-root "${REPO_ROOT}" --memory-dir "${DEST_DIR}"
    fi
    ;;
  *)
    echo "usage: $0 [--pull|--pull-drive|--init-bridge|--stdin-tar]" >&2
    exit 2
    ;;
esac
