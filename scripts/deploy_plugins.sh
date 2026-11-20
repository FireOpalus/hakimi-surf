#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cmd=${1:-}
target=${2:-}
compose=(docker compose -f docker-compose.yml -f compose.roaming-oauth.yaml -f compose.plugins.yaml)

usage() {
    echo "Usage: $0 {up [weather|minecraft|bilibili]|restart [weather|minecraft|bilibili]|down [weather|minecraft|bilibili]|status [weather|minecraft|bilibili]}" >&2
    exit 2
}

resolve_services() {
    case "${1:-}" in
        "")
            echo "hakimi-plugin-weather hakimi-plugin-minecraft hakimi-plugin-bilibili"
            ;;
        weather|hakimi-plugin-weather)
            echo "hakimi-plugin-weather"
            ;;
        minecraft|hakimi-plugin-minecraft)
            echo "hakimi-plugin-minecraft"
            ;;
        bilibili|bilibili_surf|hakimi-plugin-bilibili)
            echo "hakimi-plugin-bilibili"
            ;;
        *)
            usage
            ;;
    esac
}

require_env_file() {
    if [[ ! -f secrets/plugins.env ]]; then
        echo "Missing secrets/plugins.env (create plugin tokens in /admin before starting plugins)" >&2
        exit 1
    fi
}

case "$cmd" in
    up)
        read -r -a services <<<"$(resolve_services "$target")"
        require_env_file
        "${compose[@]}" up -d --no-deps --build --wait --wait-timeout 60 "${services[@]}"
        ;;
    restart)
        read -r -a services <<<"$(resolve_services "$target")"
        require_env_file
        "${compose[@]}" up -d --no-deps --build --force-recreate --wait --wait-timeout 60 "${services[@]}"
        ;;
    down)
        read -r -a services <<<"$(resolve_services "$target")"
        "${compose[@]}" stop "${services[@]}"
        "${compose[@]}" rm -f "${services[@]}"
        ;;
    status)
        read -r -a services <<<"$(resolve_services "$target")"
        "${compose[@]}" ps "${services[@]}"
        ;;
    *)
        usage
        ;;
esac
