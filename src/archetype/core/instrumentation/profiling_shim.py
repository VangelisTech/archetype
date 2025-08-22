from __future__ import annotations

import os

# Enable profiling via environment variable
# Set ARCT_VIZTRACER=1 to enable instrumentation
PROFILING_ENABLED: bool = os.getenv("ARCT_VIZTRACER", "0") == "1"


_viz = None
if PROFILING_ENABLED:
    try:
        from viztracer import get_tracer  # type: ignore

        _viz = get_tracer()
    except Exception:
        _viz = None


class _NullCtx:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _VizZone:
    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        try:
            if _viz is not None:
                _viz.add_instant(self.name + ":begin", scope="t")
        except Exception:
            pass
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if _viz is not None:
                _viz.add_instant(self.name + ":end", scope="t")
        except Exception:
            pass
        return False


def zone(name: str):
    """Return a context manager for a VizTracer pseudo-zone, or a no-op if disabled."""
    if _viz is not None:
        return _VizZone(name)
    return _NullCtx()


def frame_mark(name: str | None = None) -> None:
    try:
        if _viz is not None:
            _viz.add_instant((name or "frame"), scope="t")
    except Exception:
        return


def message(text: str) -> None:
    try:
        if _viz is not None:
            _viz.add_instant(text, scope="p")
    except Exception:
        return


def plot(name: str, value: float) -> None:
    # Best-effort: represent as an instant with value; VizTracer has limited counter support
    try:
        if _viz is not None:
            _viz.add_instant(f"plot:{name}={float(value)}", scope="p")
    except Exception:
        return


def set_thread_name(name: str) -> None:
    # VizTracer does not expose a stable public API for thread names; no-op
    return None

