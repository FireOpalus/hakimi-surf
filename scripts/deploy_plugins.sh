#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cmd=${1:-}
if [[ $# -gt 0 ]]; then
    shift
fi
targets=("$@")
compose=(docker compose -f compose.yaml -f compose.roaming-oauth.yaml -f compose.plugins.yaml)

ALL_PLUGIN_SERVICES="hakimi-plugin-weather hakimi-plugin-minecraft hakimi-plugin-bilibili hakimi-plugin-web-tools hakimi-plugin-doc-render hakimi-plugin-song-finder hakimi-plugin-telegram"

usage() {
    echo "Usage: $0 {up|restart|down|status} [weather|minecraft|bilibili|web_tools|doc_render|song_finder|telegram|wave3|all ...]" >&2
    exit 2
}

resolve_single_target() {
    case "${1:-}" in
        ""|all)
            echo "$ALL_PLUGIN_SERVICES"
            ;;
        wave3)
            echo "hakimi-plugin-web-tools hakimi-plugin-doc-render hakimi-plugin-song-finder"
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
        web_tools|web-tools|hakimi-plugin-web-tools)
            echo "hakimi-plugin-web-tools"
            ;;
        doc_render|doc-render|hakimi-plugin-doc-render)
            echo "hakimi-plugin-doc-render"
            ;;
        song_finder|song-finder|hakimi-plugin-song-finder)
            echo "hakimi-plugin-song-finder"
            ;;
        telegram|tg|hakimi-plugin-telegram)
            echo "hakimi-plugin-telegram"
            ;;
        *)
            usage
            ;;
    esac
}

resolve_services() {
    local -a raw=()
    local -a unique=()
    local t svc existing dup
    if [[ $# -eq 0 ]]; then
        read -r -a raw <<<"$(resolve_single_target "")"
    else
        for t in "$@"; do
            local -a part=()
            read -r -a part <<<"$(resolve_single_target "$t")"
            raw+=("${part[@]}")
        done
    fi
    for svc in "${raw[@]}"; do
        dup=0
        for existing in "${unique[@]+"${unique[@]}"}"; do
            if [[ "$existing" == "$svc" ]]; then
                dup=1
                break
            fi
        done
        if [[ $dup -eq 0 ]]; then
            unique+=("$svc")
        fi
    done
    echo "${unique[*]}"
}

image_repo_for_service() {
    case "$1" in
        hakimi-plugin-doc-render)
            echo "hakimi-plugin-doc-render"
            ;;
        hakimi-plugin-song-finder)
            echo "hakimi-plugin-song-finder"
            ;;
        *)
            echo "hakimi-plugin-common"
            ;;
    esac
}

unique_build_services() {
    local -a out=()
    local -a seen_repos=()
    local svc repo r dup
    for svc in "$@"; do
        repo=$(image_repo_for_service "$svc")
        dup=0
        for r in "${seen_repos[@]+"${seen_repos[@]}"}"; do
            if [[ "$r" == "$repo" ]]; then
                dup=1
                break
            fi
        done
        if [[ $dup -eq 0 ]]; then
            seen_repos+=("$repo")
            out+=("$svc")
        fi
    done
    echo "${out[*]}"
}

tag_rollback_images() {
    local -a tagged_repos=()
    local svc cid prev_img repo r dup
    for svc in "$@"; do
        repo=$(image_repo_for_service "$svc")
        dup=0
        for r in "${tagged_repos[@]+"${tagged_repos[@]}"}"; do
            if [[ "$r" == "$repo" ]]; then
                dup=1
                break
            fi
        done
        if [[ $dup -eq 1 ]]; then
            continue
        fi
        cid=$("${compose[@]}" ps -aq "$svc" 2>/dev/null || true)
        if [[ -n "$cid" ]]; then
            prev_img=$(docker inspect --format '{{.Image}}' "$cid" 2>/dev/null || true)
            if [[ -n "$prev_img" ]]; then
                docker tag "$prev_img" "${repo}:rollback"
                tagged_repos+=("$repo")
            fi
        fi
    done
}

verify_running() {
    local svc cid running health failed=0
    for svc in "$@"; do
        cid=$("${compose[@]}" ps -aq "$svc" 2>/dev/null || true)
        if [[ -z "$cid" ]]; then
            echo "ERROR: $svc container not found after deploy" >&2
            failed=1
            continue
        fi
        running=$(docker inspect --format '{{.State.Running}}' "$cid" 2>/dev/null || echo false)
        health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo unknown)
        if [[ "$running" != "true" || "$health" == "unhealthy" ]]; then
            echo "ERROR: $svc is not healthy (running=$running health=$health)" >&2
            failed=1
        fi
    done
    return "$failed"
}

require_env_file() {
    if [[ ! -f secrets/plugins.env ]]; then
        echo "Missing secrets/plugins.env (create plugin tokens in /admin before starting plugins)" >&2
        exit 1
    fi
}

case "$cmd" in
    up)
        read -r -a services <<<"$(resolve_services "${targets[@]+"${targets[@]}"}")"
        require_env_file
        tag_rollback_images "${services[@]}"
        read -r -a build_targets <<<"$(unique_build_services "${services[@]}")"
        "${compose[@]}" build "${build_targets[@]}"
        "${compose[@]}" up -d --no-deps --no-build --wait --wait-timeout 60 "${services[@]}"
        verify_running "${services[@]}"
        ;;
    restart)
        read -r -a services <<<"$(resolve_services "${targets[@]+"${targets[@]}"}")"
        require_env_file
        tag_rollback_images "${services[@]}"
        read -r -a build_targets <<<"$(unique_build_services "${services[@]}")"
        "${compose[@]}" build "${build_targets[@]}"
        "${compose[@]}" up -d --no-deps --no-build --force-recreate --wait --wait-timeout 60 "${services[@]}"
        verify_running "${services[@]}"
        ;;
    down)
        read -r -a services <<<"$(resolve_services "${targets[@]+"${targets[@]}"}")"
        "${compose[@]}" stop "${services[@]}"
        "${compose[@]}" rm -f "${services[@]}"
        ;;
    status)
        read -r -a services <<<"$(resolve_services "${targets[@]+"${targets[@]}"}")"
        "${compose[@]}" ps "${services[@]}"
        ;;
    *)
        usage
        ;;
esac
