FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
COPY --from=uv /uv /usr/local/bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH" TZ=Asia/Shanghai
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY node_agent ./node_agent
RUN groupadd --gid 10001 hakimi && useradd --uid 10001 --gid 10001 --no-create-home hakimi && \
    mkdir -p /tmp/node_bili_jobs && \
    chown 10001:10001 /tmp/node_bili_jobs && \
    chmod 700 /tmp/node_bili_jobs
USER 10001:10001
CMD ["python", "-m", "node_agent.daemon"]
