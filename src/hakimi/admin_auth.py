"""Bearer-only admin authentication. No URL tokens, cookies, or browser persistence."""

import hmac
import time
from collections import deque
from urllib.parse import urlsplit

from fastapi import HTTPException


class AdminAuth:
    def __init__(self, settings):
        self.s = settings
        self.failures = deque(maxlen=60)
        self.actions = deque(maxlen=10)

    def check(self, request):
        token = self.s.admin_token.get_secret_value()
        if not token.strip():
            raise HTTPException(403, "admin_disabled")
        host = request.url.hostname
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise HTTPException(403, "admin_loopback_host_required")
        if request.headers.get("sec-fetch-site") in {"cross-site", "same-site"}:
            raise HTTPException(403, "cross_origin_forbidden")
        origin = request.headers.get("origin")
        if origin:
            parsed = urlsplit(origin)
            if parsed.scheme != request.url.scheme or parsed.netloc != request.url.netloc:
                raise HTTPException(403, "cross_origin_forbidden")
        if request.query_params:
            raise HTTPException(400, "admin_query_forbidden")
        if request.url.path in {"/admin", "/admin/"} and request.method in {"HEAD", "GET"}:
            return
        expected = ("Bearer " + token).encode()
        if not hmac.compare_digest(request.headers.get("authorization", "").encode(), expected):
            now = time.monotonic()
            self.failures.append(now)
            if len(self.failures) >= 10 and self.failures[-10] > now - 60:
                raise HTTPException(429, "login_rate_limited")
            raise HTTPException(
                401, "authentication_required", headers={"WWW-Authenticate": "Bearer"}
            )

    def costly(self):
        now = time.monotonic()
        if len(self.actions) >= 5 and self.actions[-5] > now - 60:
            raise HTTPException(429, "admin_action_rate_limited")
        self.actions.append(now)
