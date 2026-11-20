FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app \
    TZ=Asia/Shanghai
WORKDIR /app
RUN pip install --no-cache-dir "websockets>=15,<18" "httpx>=0.28,<1" "pydantic>=2.7,<3" && \
    groupadd --gid 10001 hakimi && \
    useradd --uid 10001 --gid 10001 --no-create-home hakimi
COPY src/hakimi_plugin_sdk ./src/hakimi_plugin_sdk
COPY plugins ./plugins
USER 10001:10001
