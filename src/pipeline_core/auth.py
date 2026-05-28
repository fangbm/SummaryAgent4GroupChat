from __future__ import annotations

import hmac
import time
from hashlib import sha256
from urllib.parse import urlencode


def authorization_header(token: str) -> str:
    return f"Bearer {token}"


def verify_bearer(header: str | None, expected_token: str) -> bool:
    if not expected_token:
        return False
    if not header or not header.startswith("Bearer "):
        return False
    supplied = header.removeprefix("Bearer ").strip()
    return hmac.compare_digest(supplied, expected_token)


def _signature(filename: str, expires: int, secret: str) -> str:
    canonical = f"{filename}:{expires}".encode()
    return hmac.new(secret.encode("utf-8"), canonical, sha256).hexdigest()


def sign_image_url(base_url: str, filename: str, secret: str, ttl_seconds: int) -> str:
    expires = int(time.time()) + ttl_seconds
    sig = _signature(filename, expires, secret)
    query = urlencode({"expires": expires, "sig": sig})
    return f"{base_url.rstrip('/')}/{filename}?{query}"


def verify_image_signature(
    filename: str,
    expires: int,
    sig: str,
    secret: str,
    *,
    now: int | None = None,
) -> bool:
    current = int(time.time()) if now is None else now
    if expires < current:
        return False
    expected = _signature(filename, expires, secret)
    return hmac.compare_digest(expected, sig)
