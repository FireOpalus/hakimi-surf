FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS page
ARG HAKIMI_VERSION=2.1.0
WORKDIR /build
COPY build.py csp.py ./
COPY src ./src
RUN python build.py --version "${HAKIMI_VERSION}" --output admin_page.html && python csp.py > admin-csp.conf

FROM nginx:stable-alpine@sha256:901e593c95dc85f5a729f5765516321366ef4ac401821431d408897e95287d0a
COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=page /build/admin-csp.conf /etc/nginx/admin-csp.conf
COPY --from=page /build/admin_page.html /srv/admin/admin_page.html
USER nginx
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
