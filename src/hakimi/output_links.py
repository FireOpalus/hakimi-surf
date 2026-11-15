"""Outbound link presentation only; this module never fetches a URL."""

import re
from urllib.parse import urlsplit

# Exact domains or their real subdomains, never a substring of an arbitrary host.
PUBLIC_DOMAINS = frozenset(
    {
        "bilibili.com",
        "b23.tv",
        "github.com",
        "arxiv.org",
        "python.org",
        "pypi.org",
        "mozilla.org",
    }
)
URL = re.compile(r"https?://[^\s<>\[\]`\"'，。！？；、（）]+", re.I)
MARKDOWN_LINK = re.compile(r"\[([^\]\n]*)\]\((https?://[^\s<>]+)\)", re.I)


def allowed_url(value: str, *, public: bool) -> bool:
    try:
        url = urlsplit(value)
        host = (url.hostname or "").lower().rstrip(".")
        if url.scheme not in {"https", "http"} or not host or url.username or url.password:
            return False
        # Grounding destinations are tracking redirects, never user-facing evidence.
        if host == "vertexaisearch.cloud.google.com" or "/grounding-api-redirect/" in url.path:
            return False
        if not public:
            return url.scheme == "https" or host in {"localhost", "127.0.0.1", "::1"}
        return url.port in (None, 80, 443) and any(
            host == domain or host.endswith("." + domain) for domain in PUBLIC_DOMAINS
        )
    except ValueError:
        return False


def filter_reply_links(text: str, *, public: bool) -> str:
    # Preserve readable labels instead of leaving broken Markdown or a refusal placeholder.
    text = MARKDOWN_LINK.sub(lambda m: m[0] if allowed_url(m[2], public=public) else m[1], text)
    return URL.sub(lambda m: m[0] if allowed_url(m[0], public=public) else "", text)


def fact_text(text: str) -> str:
    """Search is evidence for the main brain, not a second outbound link renderer."""
    text = MARKDOWN_LINK.sub(lambda m: m[1], text)
    return URL.sub("", text).strip()
