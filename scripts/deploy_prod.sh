#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mode=${1:-auto}
case "$mode" in auto|all|admin|engine) ;; *) echo "Usage: $0 [auto|admin|engine|all]" >&2; exit 2 ;; esac
compose=(docker compose -f compose.yaml -f compose.roaming-oauth.yaml)
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
            git diff --quiet "$running" "$revision" -- compose.yaml compose.roaming-oauth.yaml; then
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

prune_old_images() {
    python3 - <<'PY' || true
from collections import defaultdict
from datetime import datetime, timezone
import re
import subprocess

KEEP_REV_TAGS = 3


def run_docker(*args: str) -> list[str]:
    proc = subprocess.run(["docker", *args], capture_output=True, text=True, check=False)
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def parse_created(raw: str) -> float:
    text = raw.strip()
    if not text:
        return 0.0
    iso = re.sub(r"(\.\d{6})\d+", r"\1", text)
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    elif re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4}(?: \S+)?$", iso):
        parts = iso.split()
        iso = f"{parts[0]}T{parts[1]}{parts[2][:3]}:{parts[2][3:]}"
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return 0.0


in_use_ids: set[str] = set()
container_ids = run_docker("ps", "-aq")
if container_ids:
    in_use_ids.update(run_docker("inspect", "--format", "{{.Image}}", *container_ids))

rev_entries: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
unique_image_ids: set[str] = set()
for line in run_docker("image", "ls", "--no-trunc", "--format", "{{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}"):
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    repo, tag, img_id = parts[0], parts[1], parts[2]
    created_fallback = parts[3] if len(parts) >= 4 else ""
    if not repo.startswith("hakimi-surf-"):
        continue
    if tag in {"latest", "rollback"} or not tag.startswith("rev-"):
        continue
    rev_entries[repo].append((tag, img_id, created_fallback))
    unique_image_ids.add(img_id)

created_by_id: dict[str, float] = {}
if unique_image_ids:
    for line in run_docker(
        "image", "inspect", "--format", "{{.Id}}\t{{.Created}}", *sorted(unique_image_ids)
    ):
        if "\t" not in line:
            continue
        img_id, created_raw = line.split("\t", 1)
        created_by_id[img_id.strip()] = parse_created(created_raw)

for repo, entries in rev_entries.items():
    entries.sort(
        key=lambda item: (
            created_by_id.get(item[1], parse_created(item[2])),
            item[0],
        ),
        reverse=True,
    )
    for tag, img_id, _ in entries[KEEP_REV_TAGS:]:
        if img_id in in_use_ids:
            continue
        subprocess.run(
            ["docker", "image", "rm", f"{repo}:{tag}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
PY
    docker image prune -f >/dev/null 2>&1 || true
    docker builder prune -f --keep-storage=2GB >/dev/null 2>&1 || true
}

if [[ $mode != admin ]]; then deploy hakimi-bot . 12141 src Dockerfile pyproject.toml uv.lock .dockerignore; fi
if [[ $mode != engine ]]; then deploy hakimi-admin admin 12143 admin pyproject.toml src/hakimi/__init__.py; fi
prune_old_images || true
