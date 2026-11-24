#!/usr/bin/env python3
"""Restricted host power daemon for hakimi-mc-server (zero external deps)."""

from __future__ import annotations

import hmac
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

SERVER_DIR = "/opt/hakimi-mc-server"
CONTAINER_NAME = "hakimi-mc-server"
MC_CMD_PATH = "/opt/hakimi-mc-server/mc-cmd.sh"
DOCKER_BIN = "/usr/bin/docker"

MAX_BODY_BYTES = 4096
START_POLL_INTERVAL_S = 2.0
START_READY_TIMEOUT_S = 90.0
WATCHDOG_INTERVAL_S = 60.0

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_MC_FMT_RE = re.compile(r"§.")
_LIST_PRIMARY_RE = re.compile(r"There\s+are\s+(\d+)\s+of\s+a\s+max\b", re.IGNORECASE)
_LIST_FALLBACK_RE = re.compile(r"\b(\d+)\s+of\s+(?:a\s+max\s+of\s+)?\d+\b", re.IGNORECASE)


_LOG_LOCK = threading.Lock()


def log_event(event: str, **fields: Any) -> None:
    parts = [f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    line = " ".join(parts) + "\n"
    with _LOG_LOCK:
        sys.stdout.write(line)
        sys.stdout.flush()


def get_container_state() -> bool:
    try:
        res = subprocess.run(
            [DOCKER_BIN, "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log_event("mc_inspect_error", error=type(exc).__name__)
        return False
    return res.returncode == 0 and res.stdout.strip().lower() == "true"


def get_avail_mem_mib() -> int:
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) // 1024
    except (OSError, ValueError) as exc:
        log_event("mc_meminfo_error", error=type(exc).__name__)
    return 0


def parse_online_players(output: str) -> int | None:
    cleaned = _MC_FMT_RE.sub("", _ANSI_RE.sub("", output or "")).strip()
    if not cleaned:
        return None
    match = _LIST_PRIMARY_RE.search(cleaned)
    if match is None:
        match = _LIST_FALLBACK_RE.search(cleaned)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def query_online_players() -> int | None:
    try:
        res = subprocess.run(
            [MC_CMD_PATH, "list"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if res.returncode != 0:
        return None
    return parse_online_players(res.stdout)


class PowerController:
    def __init__(self, idle_sleep_minutes: int, min_avail_mem_mib: int) -> None:
        self.idle_sleep_minutes = idle_sleep_minutes
        self.min_avail_mem_mib = min_avail_mem_mib
        self._lock = threading.Lock()
        self._compose_lock = threading.Lock()
        self._shutdown_event = threading.Event()

        self._starting = False
        self._stopping = False
        self._ready = False
        self._last_non_empty_at = time.monotonic()

    def stop_watchdog(self) -> None:
        self._shutdown_event.set()

    def get_status(self) -> dict[str, Any]:
        avail_mem_mib = get_avail_mem_mib()
        running = get_container_state()
        now = time.monotonic()

        with self._lock:
            if self._stopping:
                return {
                    "ok": True,
                    "state": "stopping",
                    "ready": False,
                    "online_players": None,
                    "idle_seconds": 0,
                    "idle_sleep_minutes": self.idle_sleep_minutes,
                    "avail_mem_mib": avail_mem_mib,
                }
            if self._starting:
                return {
                    "ok": True,
                    "state": "starting",
                    "ready": False,
                    "online_players": None,
                    "idle_seconds": 0,
                    "idle_sleep_minutes": self.idle_sleep_minutes,
                    "avail_mem_mib": avail_mem_mib,
                }
            if not running:
                self._ready = False
                return {
                    "ok": True,
                    "state": "stopped",
                    "ready": False,
                    "online_players": None,
                    "idle_seconds": 0,
                    "idle_sleep_minutes": self.idle_sleep_minutes,
                    "avail_mem_mib": avail_mem_mib,
                }

        # Container is running and not in an active start/stop transition.
        online = query_online_players()
        now = time.monotonic()
        with self._lock:
            if self._stopping:
                state = "stopping"
                ready = False
                online_out = None
                idle_seconds = 0
            elif self._starting:
                state = "starting"
                ready = False
                online_out = None
                idle_seconds = 0
            else:
                if online is not None:
                    if not self._ready:
                        self._last_non_empty_at = now
                    self._ready = True
                    if online > 0:
                        self._last_non_empty_at = now
                    state = "running"
                    ready = True
                    online_out = online
                    idle_seconds = 0 if online > 0 else max(0, int(now - self._last_non_empty_at))
                else:
                    self._ready = False
                    state = "running"
                    ready = False
                    online_out = None
                    idle_seconds = 0

        return {
            "ok": True,
            "state": state,
            "ready": ready,
            "online_players": online_out,
            "idle_seconds": idle_seconds,
            "idle_sleep_minutes": self.idle_sleep_minutes,
            "avail_mem_mib": avail_mem_mib,
        }

    def request_start(self) -> tuple[int, dict[str, Any]]:
        with self._lock:
            if self._stopping:
                return 409, {"ok": False, "reason": "stopping", "state": "stopping"}
            if self._starting:
                return 200, {"ok": True, "state": "starting", "ready": False}

        running = get_container_state()
        if running:
            with self._lock:
                if self._stopping:
                    return 409, {"ok": False, "reason": "stopping", "state": "stopping"}
                if self._starting:
                    return 200, {"ok": True, "state": "starting", "ready": False}
                if self._ready:
                    self._last_non_empty_at = time.monotonic()
                    return 200, {"ok": True, "state": "running", "ready": True}

            # Externally started or powerd restarted while container already up: check RCON once.
            online = query_online_players()
            now = time.monotonic()
            with self._lock:
                if self._stopping:
                    return 409, {"ok": False, "reason": "stopping", "state": "stopping"}
                if online is not None:
                    self._ready = True
                    self._starting = False
                    self._last_non_empty_at = now
                    return 200, {"ok": True, "state": "running", "ready": True}
                if self._starting:
                    return 200, {"ok": True, "state": "starting", "ready": False}
                self._starting = True
                self._ready = False
                self._last_non_empty_at = now
            threading.Thread(
                target=self._run_start_worker,
                args=(False,),
                name="mc-start-poll",
                daemon=True,
            ).start()
            return 200, {"ok": True, "state": "starting", "ready": False}

        avail_mem_mib = get_avail_mem_mib()
        if avail_mem_mib < self.min_avail_mem_mib:
            log_event(
                "mc_start_rejected_low_memory",
                avail_mem_mib=avail_mem_mib,
                min_avail_mem_mib=self.min_avail_mem_mib,
            )
            return 503, {
                "ok": False,
                "reason": "low_memory",
                "avail_mem_mib": avail_mem_mib,
            }

        with self._lock:
            if self._stopping:
                return 409, {"ok": False, "reason": "stopping", "state": "stopping"}
            if self._starting:
                return 200, {"ok": True, "state": "starting", "ready": False}
            self._starting = True
            self._ready = False
            self._last_non_empty_at = time.monotonic()

        threading.Thread(
            target=self._run_start_worker,
            args=(True,),
            name="mc-start-worker",
            daemon=True,
        ).start()
        return 200, {"ok": True, "state": "starting", "ready": False}

    def _run_start_worker(self, run_compose_up: bool) -> None:
        started_at = time.monotonic()
        log_event("mc_start_begin", run_compose_up=run_compose_up)
        if run_compose_up:
            with self._compose_lock:
                with self._lock:
                    if self._stopping or not self._starting:
                        log_event("mc_start_cancelled_before_compose")
                        return
                try:
                    res = subprocess.run(
                        [DOCKER_BIN, "compose", "up", "-d"],
                        cwd=SERVER_DIR,
                        capture_output=True,
                        text=True,
                        timeout=60,
                        check=False,
                    )
                except (OSError, subprocess.SubprocessError) as exc:
                    log_event("mc_start_compose_error", error=type(exc).__name__)
                    with self._lock:
                        self._starting = False
                        self._ready = False
                    return
                if res.returncode != 0:
                    log_event("mc_start_compose_failed", rc=res.returncode)
                    with self._lock:
                        self._starting = False
                        self._ready = False
                    return

        deadline = time.monotonic() + START_READY_TIMEOUT_S
        while time.monotonic() < deadline and not self._shutdown_event.is_set():
            with self._lock:
                if self._stopping or not self._starting:
                    log_event("mc_start_cancelled")
                    return
            online = query_online_players()
            if online is not None:
                now = time.monotonic()
                with self._lock:
                    if not self._stopping:
                        self._starting = False
                        self._ready = True
                        self._last_non_empty_at = now
                log_event(
                    "mc_start_ready",
                    online_players=online,
                    elapsed_s=round(now - started_at, 1),
                )
                return
            if time.monotonic() - started_at > 10.0 and not get_container_state():
                log_event(
                    "mc_start_container_exited", elapsed_s=round(time.monotonic() - started_at, 1)
                )
                with self._lock:
                    self._starting = False
                    self._ready = False
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._shutdown_event.wait(min(START_POLL_INTERVAL_S, remaining))

        log_event("mc_start_timeout", timeout_s=int(START_READY_TIMEOUT_S))
        with self._lock:
            self._starting = False
            self._ready = False

    def request_stop(self, force: bool = False) -> tuple[int, dict[str, Any]]:
        with self._lock:
            if self._stopping:
                return 200, {"ok": True, "state": "stopping"}

        running = get_container_state()
        with self._lock:
            if self._stopping:
                return 200, {"ok": True, "state": "stopping"}
            if not running and not self._starting:
                self._ready = False
                return 200, {"ok": True, "state": "stopped"}

        if not force and running:
            online = query_online_players()
            if online is not None and online > 0:
                with self._lock:
                    self._last_non_empty_at = time.monotonic()
                log_event("mc_stop_blocked_players_online", online_players=online)
                return 409, {
                    "ok": False,
                    "reason": "players_online",
                    "online_players": online,
                }

        with self._lock:
            if self._stopping:
                return 200, {"ok": True, "state": "stopping"}
            self._stopping = True
            self._starting = False
            self._ready = False

        threading.Thread(
            target=self._run_stop_worker,
            args=("http_stop", force),
            name="mc-stop-worker",
            daemon=True,
        ).start()
        return 200, {"ok": True, "state": "stopping"}

    def _graceful_stop_sync(self) -> int:
        with self._compose_lock:
            try:
                subprocess.run(
                    [MC_CMD_PATH, "save-all flush"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError):
                pass
            time.sleep(2.0)
            try:
                res = subprocess.run(
                    [DOCKER_BIN, "compose", "stop"],
                    cwd=SERVER_DIR,
                    capture_output=True,
                    text=True,
                    timeout=90,
                    check=False,
                )
                return res.returncode
            except (OSError, subprocess.SubprocessError) as exc:
                log_event("mc_compose_stop_error", error=type(exc).__name__)
                return -1

    def _run_stop_worker(self, reason: str, force: bool) -> None:
        log_event("mc_stop_begin", reason=reason, force=str(force).lower())
        rc = self._graceful_stop_sync()
        with self._lock:
            self._stopping = False
            self._starting = False
            self._ready = False
        log_event("mc_stop_done", reason=reason, rc=rc)

    def run_watchdog_loop(self) -> None:
        log_event(
            "mc_watchdog_started",
            interval_s=int(WATCHDOG_INTERVAL_S),
            idle_sleep_minutes=self.idle_sleep_minutes,
        )
        while not self._shutdown_event.wait(WATCHDOG_INTERVAL_S):
            try:
                self._watchdog_tick()
            except Exception as exc:  # noqa: BLE001
                log_event("mc_watchdog_error", error=type(exc).__name__)

    def _watchdog_tick(self) -> None:
        with self._lock:
            if self._starting or self._stopping:
                return

        if not get_container_state():
            with self._lock:
                if not self._starting and not self._stopping:
                    self._ready = False
            return

        online = query_online_players()
        if online is None:
            return

        now = time.monotonic()
        should_sleep = False
        idle_seconds = 0
        with self._lock:
            if self._starting or self._stopping:
                return
            if not self._ready:
                self._ready = True
                self._last_non_empty_at = now
            if online > 0:
                self._last_non_empty_at = now
            else:
                idle_seconds = max(0, int(now - self._last_non_empty_at))
                if idle_seconds >= self.idle_sleep_minutes * 60:
                    self._stopping = True
                    self._starting = False
                    self._ready = False
                    should_sleep = True

        if should_sleep:
            log_event(
                "mc_idle_sleep_stopping",
                idle_seconds=idle_seconds,
                idle_sleep_minutes=self.idle_sleep_minutes,
            )
            rc = self._graceful_stop_sync()
            with self._lock:
                self._stopping = False
                self._starting = False
                self._ready = False
            log_event("mc_idle_sleep_stopped", rc=rc)


class PowerdHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 16


def make_handler(controller: PowerController, token_bytes: bytes) -> type[BaseHTTPRequestHandler]:
    class PowerdHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            # Intentionally avoid logging headers or raw request details that could carry tokens.
            return

        def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _check_auth(self) -> bool:
            header = self.headers.get("Authorization", "")
            prefix = "Bearer "
            if not header.startswith(prefix):
                return False
            provided = header[len(prefix) :].encode("utf-8")
            return hmac.compare_digest(provided, token_bytes)

        def _read_optional_json_body(
            self,
        ) -> tuple[dict[str, Any] | None, tuple[int, dict[str, Any]] | None]:
            raw_len = self.headers.get("Content-Length")
            if raw_len is None or raw_len.strip() == "":
                return {}, None
            try:
                content_len = int(raw_len.strip())
            except ValueError:
                return None, (400, {"ok": False, "error": "invalid_content_length"})
            if content_len < 0:
                return None, (400, {"ok": False, "error": "invalid_content_length"})
            if content_len > MAX_BODY_BYTES:
                return None, (413, {"ok": False, "error": "payload_too_large"})
            if content_len == 0:
                return {}, None
            raw_body = self.rfile.read(content_len)
            if not raw_body.strip():
                return {}, None
            try:
                parsed = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None, (400, {"ok": False, "error": "invalid_json"})
            if not isinstance(parsed, dict):
                return None, (400, {"ok": False, "error": "invalid_json"})
            return parsed, None

        def _reject_unauth(self) -> None:
            self._send_json(401, {"ok": False, "error": "unauthorized"})

        def do_GET(self) -> None:
            if not self._check_auth():
                self._reject_unauth()
                return
            path = urlsplit(self.path).path
            if path == "/status":
                self._send_json(200, controller.get_status())
                return
            self._send_json(404, {"ok": False, "error": "not_found"})

        def do_POST(self) -> None:
            if not self._check_auth():
                self._reject_unauth()
                return
            path = urlsplit(self.path).path
            if path == "/start":
                _, err = self._read_optional_json_body()
                if err is not None:
                    self._send_json(err[0], err[1])
                    return
                code, payload = controller.request_start()
                self._send_json(code, payload)
                return
            if path == "/stop":
                body, err = self._read_optional_json_body()
                if err is not None:
                    self._send_json(err[0], err[1])
                    return
                raw_force = (body or {}).get("force", False)
                if not isinstance(raw_force, bool):
                    self._send_json(400, {"ok": False, "error": "invalid_force_flag"})
                    return
                code, payload = controller.request_stop(force=raw_force)
                self._send_json(code, payload)
                return
            self._send_json(404, {"ok": False, "error": "not_found"})

        def _handle_other_method(self) -> None:
            if not self._check_auth():
                self._reject_unauth()
                return
            path = urlsplit(self.path).path
            if path in ("/status", "/start", "/stop"):
                self._send_json(405, {"ok": False, "error": "method_not_allowed"})
                return
            self._send_json(404, {"ok": False, "error": "not_found"})

        do_PUT = _handle_other_method
        do_DELETE = _handle_other_method
        do_PATCH = _handle_other_method
        do_HEAD = _handle_other_method
        do_OPTIONS = _handle_other_method

    return PowerdHandler


def main() -> int:
    host = os.environ.get("MC_POWERD_HOST", "172.22.0.1").strip()
    if host in ("", "0.0.0.0", "::", "*"):
        print("ERROR: Refusing to bind mc-powerd to wildcard address", file=sys.stderr, flush=True)
        return 2

    port = int(os.environ.get("MC_POWERD_PORT", "8792").strip())
    token = os.environ.get("MC_POWERD_TOKEN", "").strip()
    if len(token) < 16:
        print("ERROR: MC_POWERD_TOKEN is missing or too short", file=sys.stderr, flush=True)
        return 2

    idle_sleep_minutes = max(1, int(os.environ.get("MC_IDLE_SLEEP_MINUTES", "30").strip()))
    min_avail_mem_mib = max(0, int(os.environ.get("MC_MIN_AVAIL_MEM_MIB", "1300").strip()))

    controller = PowerController(
        idle_sleep_minutes=idle_sleep_minutes,
        min_avail_mem_mib=min_avail_mem_mib,
    )
    handler_cls = make_handler(controller, token.encode("utf-8"))
    server = PowerdHTTPServer((host, port), handler_cls)

    watchdog_thread = threading.Thread(
        target=controller.run_watchdog_loop,
        name="mc-idle-watchdog",
        daemon=True,
    )
    watchdog_thread.start()

    def _on_signal(signum: int, _frame: Any) -> None:
        log_event("mc_powerd_signal", signum=signum)
        controller.stop_watchdog()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log_event(
        "mc_powerd_listening",
        host=host,
        port=port,
        idle_sleep_minutes=idle_sleep_minutes,
        min_avail_mem_mib=min_avail_mem_mib,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        controller.stop_watchdog()
        server.server_close()
        log_event("mc_powerd_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
