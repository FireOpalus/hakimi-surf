FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "fastapi>=0.115,<1" \
    "uvicorn[standard]>=0.34,<1" \
    "httpx>=0.28,<1" \
    "websockets>=15,<18"

COPY voice_client ./voice_client
COPY voice_gateway ./voice_gateway

RUN python -c "from voice_gateway.app import create_gateway_app" && \
    groupadd --gid 10001 hakimi && \
    useradd --uid 10001 --gid 10001 --no-create-home hakimi && \
    chown -R 10001:10001 /app

USER 10001:10001

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:12145/healthz', timeout=2)"]

CMD ["python", "-m", "voice_gateway"]
