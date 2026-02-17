from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from typing import Any, Dict, Optional

class CollectorError(RuntimeError):
    """Base error for collector operations."""
    pass

def utc_now() -> datetime:
    """Return current aware UTC datetime."""
    return datetime.now(timezone.utc)

def iso_utc_now() -> str:
    """Return current UTC time as ISO 8601 string."""
    return utc_now().isoformat()

def get_collector_host() -> str:
    """Return hostname for lineage."""
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"

def build_wrapper(
    *,
    source: str,
    entity: str,
    schema_version: int,
    host: str,
    payload: Any,
    meta_tags: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Wrap raw API payload with standard metadata (Bronze layer pattern).
    """
    return {
        "meta": {
            "source": source,
            "entity": entity,
            "schema_version": schema_version,
            "host": host,
            "collected_at_utc": iso_utc_now(),
            "collector_host": get_collector_host(),
            "environment": os.getenv("ENVIRONMENT", "homelab"),
            **(meta_tags or {})
        },
        "payload": payload,
    }