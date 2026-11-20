ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

WORKDIR /app
RUN python3 -c "import websockets" 2>/dev/null || pip install --no-cache-dir "websockets>=14,<18"
COPY --chmod=0644 relay.py /app/relay.py

USER 10001:10001
ENTRYPOINT ["python3", "-u", "/app/relay.py"]
