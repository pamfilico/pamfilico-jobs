from __future__ import annotations

import re

_PART = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)")
_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def seconds(value) -> float | None:
    """'90s', '10m', '1h30m', '2d', 45, 45.0 → seconds. None → None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if not text:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", text):
        return float(text)
    total, pos = 0.0, 0
    for match in _PART.finditer(text):
        if text[pos:match.start()].strip():
            break
        total += float(match.group(1)) * _UNITS[match.group(2)]
        pos = match.end()
    if pos != len(text) or pos == 0:
        raise ValueError(f"not a duration: {value!r} (use e.g. '30s', '10m', '1h30m')")
    return total
