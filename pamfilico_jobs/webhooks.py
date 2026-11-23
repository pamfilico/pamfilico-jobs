"""Verify a webhook from jobs.

    from pamfilico_jobs.webhooks import verify
    event = verify(request.get_data(), request.headers, secret=os.environ["JOBS_WEBHOOK_SECRET"])
    # event["type"] == "run.completed"; event["data"] is the run; event["sequence"] orders them

Rules for the receiver: a message can arrive twice (dedupe on event["id"] / X-Jobs-Delivery),
messages are not ordered (keep the highest `sequence` per run), answer 2xx fast.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Mapping

from pamfilico_jobs.errors import InvalidSignature

DEFAULT_TOLERANCE_S = 300


def sign(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else int(timestamp)
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def _header(headers: Mapping, name: str) -> str | None:
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if getter is not None:
        value = getter(name)
        if value is not None:
            return value
    lower = name.lower()
    for k, v in dict(headers).items():
        if str(k).lower() == lower:
            return v
    return None


def verify(body, headers: Mapping, secret: str, tolerance: int = DEFAULT_TOLERANCE_S) -> dict:
    """Return the event dict, or raise InvalidSignature."""
    if not secret:
        raise InvalidSignature("no webhook secret configured")
    raw = body.encode() if isinstance(body, str) else bytes(body or b"")
    sig = _header(headers, "X-Jobs-Signature")
    if not sig:
        raise InvalidSignature("missing X-Jobs-Signature")
    parts = dict(p.split("=", 1) for p in sig.split(",") if "=" in p)
    try:
        ts = int(parts.get("t", ""))
    except ValueError as exc:
        raise InvalidSignature("bad timestamp") from exc
    if tolerance and abs(time.time() - ts) > tolerance:
        raise InvalidSignature("timestamp outside the tolerance window (replayed or clock skew)")
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, parts.get("v1", "")):
        raise InvalidSignature("signature mismatch")
    try:
        return json.loads(raw)
    except ValueError as exc:
        raise InvalidSignature("body is not JSON") from exc
