#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

has_basetemp=0
has_numprocs=0
for arg in "$@"; do
    case "$arg" in
        --basetemp|--basetemp=*) has_basetemp=1 ;;
        -n|-n*|--numprocesses|--numprocesses=*) has_numprocs=1 ;;
    esac
done

extra_args=()
basetemp=""
cleanup() {
    if [[ -n "$basetemp" ]]; then
        rm -rf "$basetemp"
    fi
}
trap cleanup EXIT

if [[ $has_basetemp -eq 0 && -d /dev/shm && -w /dev/shm ]]; then
    basetemp="/dev/shm/hakimi-pytest-$$"
    mkdir -p "$basetemp"
    extra_args+=("--basetemp=$basetemp")
fi

if [[ $has_numprocs -eq 0 ]] && .venv/bin/python -c "import xdist" >/dev/null 2>&1; then
    extra_args+=("-n" "auto" "--dist=worksteal")
fi

.venv/bin/pytest "${extra_args[@]}" "$@"
