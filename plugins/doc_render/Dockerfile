FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src:/app \
    HOME=/tmp \
    TMPDIR=/tmp \
    TZ=Asia/Shanghai
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends fonts-wqy-zenhei fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir \
        "websockets>=15,<18" \
        "pydantic>=2.7,<3" \
        "pillow>=12,<13" \
        "pypdf>=5,<7" \
        "python-docx>=1.1,<2" \
        "openpyxl>=3.1,<4" && \
    groupadd --gid 10001 hakimi && \
    useradd --uid 10001 --gid 10001 --no-create-home hakimi
COPY src/hakimi_plugin_sdk ./src/hakimi_plugin_sdk
COPY plugins/__init__.py ./plugins/__init__.py
COPY plugins/doc_render ./plugins/doc_render
USER 10001:10001
CMD ["python", "-m", "plugins.doc_render.plugin_main"]
