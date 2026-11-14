"""Create .env without printing secrets or overwriting an existing configuration."""

import base64
import os
import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
text = (root / ".env.example").read_text()
replacements = {
    "HAKIMI_ENCRYPTION_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
    "HAKIMI_ONEBOT_SECRET": secrets.token_urlsafe(32),
    "HAKIMI_ONEBOT_ACCESS_TOKEN": secrets.token_urlsafe(32),
    "HAKIMI_ONEBOT_WS_TOKEN": secrets.token_urlsafe(32),
}
for name, value in replacements.items():
    text = text.replace(f"{name}=GENERATE_WITH_INIT_ENV", f"{name}={value}")
fd = os.open(root / ".env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(text)
print("Created private .env. Set Gemini credentials and QQ IDs before starting the service.")
