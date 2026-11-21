"""Module entry point for `python -m voice_gateway`."""

from __future__ import annotations

import uvicorn

from .app import create_gateway_app
from .config import load_gateway_config


def main() -> None:
    """Load config, validate gateway auth, and serve hakimi-voice-gateway via uvicorn."""
    config = load_gateway_config()
    config.require_gateway_auth()
    app = create_gateway_app(config)
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        workers=1,
        access_log=False,
        proxy_headers=False,
        ws="websockets-sansio",
        ws_max_size=config.max_ws_frame_bytes,
        ws_ping_interval=25.0,
        ws_ping_timeout=30.0,
        ws_per_message_deflate=False,
    )


if __name__ == "__main__":
    main()
