ARG BASE_IMAGE=node:22-slim@sha256:e9f565d32859f0cbfb52125eafe8be39390f24dac302dc3cdf566e8ba3ab21ee
FROM ${BASE_IMAGE}
ARG NPM_REGISTRY=https://registry.npmjs.org/
ENV NODE_ENV=production NODE_OPTIONS="--max-old-space-size=512" HOME=/tmp TZ=Asia/Shanghai
WORKDIR /app
COPY package.json package-lock.json ./
RUN REG="${NPM_REGISTRY%/}/" && \
    if [ "${REG}" != "https://registry.npmjs.org/" ]; then \
      sed -i "s|https://registry.npmjs.org/|${REG}|g" package-lock.json; \
    fi && \
    npm ci --omit=dev --ignore-scripts --no-audit --no-fund --registry="${REG}" && \
    npm cache clean --force
COPY src ./src
RUN groupadd --gid 10001 hakimi && \
    useradd --uid 10001 --gid 10001 --no-create-home hakimi && \
    mkdir -p /tmp/mc_cache && \
    chown -R 10001:10001 /app /tmp/mc_cache && \
    chmod 700 /tmp/mc_cache
USER 10001:10001
EXPOSE 8791
CMD ["node", "src/index.mjs"]
