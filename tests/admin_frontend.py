"""Local double for the standalone UI in engine tests. Docker smoke exercises the real proxy."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from admin.csp import content_security_policy
from hakimi import admin_routes


@pytest.fixture(scope="session")
def console_endpoint():
    data = (Path(__file__).resolve().parents[1] / "admin/admin_page.html").read_bytes()
    policy = content_security_policy(data.decode())

    class Document(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/admin" or self.headers["Host"] != "127.0.0.1":
                self.send_error(404)
                return
            assert self.headers["Authorization"] is None
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Security-Policy", policy)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Document) as server:
        thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/admin"
        finally:
            server.shutdown()
            thread.join()


@pytest.fixture
def admin_frontend(console_endpoint, monkeypatch):
    monkeypatch.setattr(admin_routes, "ADMIN_PAGE_URL", console_endpoint)
