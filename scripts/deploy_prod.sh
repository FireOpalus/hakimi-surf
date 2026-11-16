#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE_ARGS=(-f docker-compose.yml -f compose.roaming-oauth.yaml)
IMAGE="hakimi-surf-hakimi-bot"
HAS_ROLLBACK=0

if docker image inspect "${IMAGE}:latest" >/dev/null 2>&1; then
  docker tag "${IMAGE}:latest" "${IMAGE}:rollback"
  HAS_ROLLBACK=1
fi

OLD_ROLLBACKS="$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E "^${IMAGE}:rollback-pre-" || true)"
if [[ -n "${OLD_ROLLBACKS}" ]]; then
  echo "${OLD_ROLLBACKS}" | xargs -r docker rmi || true
fi

docker compose "${COMPOSE_ARGS[@]}" build hakimi-bot
timeout 120 ./.venv/bin/python scripts/smoke_container.py "${IMAGE}:latest"

rollback() {
  if [[ "${HAS_ROLLBACK}" -eq 1 ]] && docker image inspect "${IMAGE}:rollback" >/dev/null 2>&1; then
    echo "Deploy check failed; rolling back to ${IMAGE}:rollback..." >&2
    docker tag "${IMAGE}:rollback" "${IMAGE}:latest"
    docker compose "${COMPOSE_ARGS[@]}" up -d --wait --no-deps hakimi-bot || true
  fi
}

if ! docker compose "${COMPOSE_ARGS[@]}" up -d --wait --no-deps hakimi-bot \
  || ! curl --noproxy '*' -fsS http://127.0.0.1:12141/healthz >/dev/null; then
  rollback
  exit 1
fi

docker image prune -f
docker builder prune -f --filter until=24h
echo "deploy_prod ok"
