from __future__ import annotations

import os
from contextlib import contextmanager


# Enable profiling via environment variable
TRACY_ENABLED: bool = os.getenv("ARCT_TRACY", "0") == "1"

_tracy = None
if TRACY_ENABLED:
    try:
        # Replace with actual tracy python binding import when available
        # e.g., import tracy; here we attempt dynamic import to avoid hard dep
        _tracy = __import__("tracy")
    except Exception:
        _tracy = None


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def zone(name: str):
    """Return a context manager for a Tracy zone, or a no-op if disabled."""
    if _tracy is not None:
        try:
            return _tracy.Zone(name)
        except Exception:
            return _NullCtx()
    return _NullCtx()


def frame_mark(name: str | None = None) -> None:
    if _tracy is not None:
        try:
            if name:
                _tracy.FrameMarkNamed(name)
            else:
                _tracy.FrameMark()
        except Exception:
            return


def message(text: str) -> None:
    if _tracy is not None:
        try:
            _tracy.Message(text)
        except Exception:
            return


def plot(name: str, value: float) -> None:
    if _tracy is not None:
        try:
            _tracy.Plot(name, float(value))
        except Exception:
            return


def set_thread_name(name: str) -> None:
    if _tracy is not None:
        try:
            _tracy.SetThreadName(name)
        except Exception:
            return


