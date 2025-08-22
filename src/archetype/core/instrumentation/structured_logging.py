from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# ----------------------------
# Minimal structured logging sink
# ----------------------------

# Enabled via environment variable to avoid any overhead by default
_ENABLED: bool = os.getenv("ARCT_STRUCTURED_LOG", "0") == "1"

# Sink configuration
# - jsonl: append JSON Lines per-world under ARCT_LOG_DIR (default .archetype_logs)
_SINK: str = os.getenv("ARCT_LOG_SINK", "jsonl")  # future: parquet, iceberg
_LOG_DIR: str = os.getenv("ARCT_LOG_DIR", ".archetype_logs")


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _world_events_path(world_id: Optional[str | int]) -> str:
    wid = str(world_id or "unknown")
    dir_path = os.path.join(_LOG_DIR, "events")
    _ensure_dir(dir_path)
    return os.path.join(dir_path, f"world_{wid}.events.jsonl")


def enable(enabled: bool = True) -> None:
    global _ENABLED
    _ENABLED = enabled


def configure(*, sink: Optional[str] = None, log_dir: Optional[str] = None) -> None:
    global _SINK, _LOG_DIR
    if sink is not None:
        _SINK = sink
    if log_dir is not None:
        _LOG_DIR = log_dir


def emit_event(level: int, event: str, payload: Dict[str, Any]) -> None:
    """Best-effort structured emission.

    - Always non-throwing; falls back silently on errors
    - Currently supports jsonl sink only; future sinks can be added in-place
    - Writes per-world file to avoid interleaving across concurrent runs
    """
    if not _ENABLED:
        return
    try:
        # Split standard fields from extras to keep a stable base schema over time
        standard_keys = {
            "world_id",
            "run_id",
            "tick",
            "archetype",
            "processor",
            "duration_ms",
            "rows",
        }
        record: Dict[str, Any] = {
            "ts": time.time_ns(),
            "ts_iso": datetime.now(timezone.utc).isoformat(),
            "level": int(level),
            "event": event,
            "world_id": payload.get("world_id"),
            "run_id": payload.get("run_id"),
            "tick": payload.get("tick"),
            "archetype": payload.get("archetype"),
            "processor": payload.get("processor"),
            "duration_ms": payload.get("duration_ms"),
            "rows": payload.get("rows"),
            # keep full arbitrary payload for richness; callers can parse as needed
            "extra": {k: v for k, v in payload.items() if k not in standard_keys},
        }

        if _SINK == "jsonl":
            path = _world_events_path(payload.get("world_id"))
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
            # Minimal contention approach: open/append/close
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
        # Placeholders for future sinks to avoid accidental exceptions
        elif _SINK in {"parquet", "iceberg"}:
            # Not implemented yet; no-op to maintain safety
            return
        else:
            return
    except Exception:
        # Never let structured logging break the run
        return


