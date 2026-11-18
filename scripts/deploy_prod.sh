#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode=${1:-auto}
case "$mode" in auto|all|admin|engine) ;; *) echo "Usage: $0 [auto|admin|engine|all]" >&2; exit 2 ;; esac
compose=(docker compose -f docker-compose.yml -f compose.roaming-oauth.yaml)
revision=$(git rev-parse HEAD)
version=$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml","rb"))["project"]["version"])')

build_context() (
    local tree=$1 context=$2
    if [[ $context != . ]]; then
        git archive --format=tar "$tree:$context"
        return
    fi
    local modules=src/hakimi/modules pin staging
    pin=$(git rev-parse "$tree:$modules")
    if ! git -C "$modules" cat-file -e "$pin^{commit}" 2>/dev/null || [[ ! -f "$modules/.git" && ! -d "$modules/.git" ]]; then
        git submodule update --init -- "$modules" >&2
    fi
    staging=$(mktemp -d)
    trap 'rm -r -- "$staging"' EXIT
    git archive "$tree" | tar -x -C "$staging"
    mkdir -p "$staging/$modules"
    git -C "$modules" archive "$pin" | tar -x -C "$staging/$modules"
    tar -c -C "$staging" .
)

deploy() {
    local service=$1 context=$2 port=$3
    shift 3
    local image="hakimi-surf-$service" id previous="" running="" candidate
    id=$("${compose[@]}" ps -aq "$service")
    if [[ -n "$id" ]]; then
        previous=$(docker inspect --format '{{.Image}}' "$id")
        running=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$previous")
    fi
    if [[ $mode == auto && -n "$running" ]] && git cat-file -e "$running^{commit}" 2>/dev/null && git diff --quiet "$running" "$revision" -- "$@"; then
        if [[ $(docker inspect --format '{{.State.Running}}' "$id") == true ]] &&
            git diff --quiet "$running" "$revision" -- docker-compose.yml compose.roaming-oauth.yaml; then
            echo "$service already current"
            return
        fi
        # A Compose-only change can be applied without rebuilding the service image.
        "${compose[@]}" up -d --no-deps --no-build --wait --wait-timeout 90 "$service"
        return
    fi

    candidate="$image:rev-${revision:0:12}"
    build_context "$revision" "$context" | docker build --label "org.opencontainers.image.revision=$revision" \
        --label "org.opencontainers.image.version=$version" -t "$candidate" -
    if [[ $service == hakimi-admin ]]; then
        docker run --rm --network none --read-only --tmpfs /tmp --cap-drop ALL "$candidate" -t
    else
        timeout 150 .venv/bin/python scripts/smoke_container.py "$candidate"
    fi
    if [[ -n "$previous" ]]; then docker tag "$previous" "$image:rollback"; fi
    docker tag "$candidate" "$image:latest"
    if "${compose[@]}" up -d --no-deps --no-build --wait --wait-timeout 90 "$service" &&
        curl --noproxy '*' --max-time 5 -fsS "http://127.0.0.1:$port/healthz" >/dev/null; then
        echo "$service deployed at ${revision:0:12}"
    else
        if [[ -n "$previous" ]]; then
            docker tag "$previous" "$image:latest"
            "${compose[@]}" up -d --no-deps --no-build --wait --wait-timeout 90 "$service"
        fi
        return 1
    fi
}

if [[ $mode != engine ]]; then deploy hakimi-admin admin 12143 admin; fi
if [[ $mode != admin ]]; then deploy hakimi-bot . 12141 src Dockerfile pyproject.toml uv.lock .dockerignore; fi
