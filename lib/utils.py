from __future__ import annotations

import uuid
from datetime import datetime, timezone


def uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def now_iso() -> str:
    # ISO without microseconds, in UTC
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_int(x, default=None):
    try:
        if x is None or x == "" or str(x).lower() == "nan":
            return default
        return int(float(x))
    except Exception:
        return default


def safe_float(x, default=None):
    try:
        if x is None or x == "" or str(x).lower() == "nan":
            return default
        return float(x)
    except Exception:
        return default
