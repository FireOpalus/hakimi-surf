#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode=${1:-auto}
case "$mode" in auto|all|admin|engine) ;; *) echo "Usage: $0 [auto|admin|engine|all]" >&2; exit 2 ;; esac
compose=(docker compose -f docker-compose.yml -f compose.roaming-oauth.yaml)
revision=$(git rev-parse HEAD)
version=$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml","rb"))["project"]["version"])')

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
    scripts/archive_release.sh "$revision" "$context" | docker build \
        --build-arg "HAKIMI_VERSION=$version" \
        --label "org.opencontainers.image.revision=$revision" \
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

if [[ $mode != admin ]]; then deploy hakimi-bot . 12141 src Dockerfile pyproject.toml uv.lock .dockerignore; fi
if [[ $mode != engine ]]; then deploy hakimi-admin admin 12143 admin pyproject.toml src/hakimi/__init__.py; fi
