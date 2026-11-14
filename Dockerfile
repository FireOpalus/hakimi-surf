FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
COPY --from=uv /uv /usr/local/bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH" TZ=Asia/Shanghai
WORKDIR /app
# Latest stable yt-dlp is pinned in uv.lock; ffmpeg/ffprobe validate and merge bounded media.
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev && \
    groupadd --gid 10001 hakimi && useradd --uid 10001 --gid 10001 --no-create-home hakimi && \
    mkdir -p /app/data /tmp/bili_video_cache && \
    chown 10001:10001 /app/data /tmp/bili_video_cache && \
    chmod 700 /app/data /tmp/bili_video_cache
USER 10001:10001
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:12141/healthz', timeout=2)"]
CMD ["uvicorn", "hakimi.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "12141", "--workers", "1", "--no-access-log", "--no-proxy-headers", "--ws", "websockets-sansio", "--ws-max-size", "16777216", "--ws-max-queue", "2", "--ws-ping-interval", "25", "--ws-ping-timeout", "30", "--ws-per-message-deflate", "false"]
