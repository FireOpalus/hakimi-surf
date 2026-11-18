"""Generate the Nginx CSP include from the exact UTF-8 page shipped in this image."""

import base64
import hashlib
import re
from pathlib import Path


def content_security_policy(html: str) -> str:
    directives = ["default-src 'none'"]
    for tag in ("script", "style"):
        blocks = re.findall(rf"<{tag}>(.*?)</{tag}>", html, re.S)
        if not blocks:
            raise ValueError(f"Expected inline {tag} in admin page")
        digests = [base64.b64encode(hashlib.sha256(b.encode()).digest()).decode() for b in blocks]
        directives.append(f"{tag}-src " + " ".join(f"'sha256-{digest}'" for digest in digests))
    directives += [
        "connect-src 'self'",
        "img-src blob:",
        "base-uri 'none'",
        "frame-ancestors 'none'",
        "form-action 'none'",
    ]
    return "; ".join(directives)


if __name__ == "__main__":
    policy = content_security_policy(Path(__file__).with_name("admin_page.html").read_text("utf-8"))
    print(f'add_header Content-Security-Policy "{policy}" always;')
