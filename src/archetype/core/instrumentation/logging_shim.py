from __future__ import annotations

import logging
import os
from typing import Any, Dict

from archetype.core import Archetype, ArchetypeSignature
from archetype.core.config import RunConfig

try:
    from rich.console import Console
    from rich.text import Text
    from rich.rule import Rule
    from rich.panel import Panel
    from rich.columns import Columns
    from rich.live import Live
    from rich.console import Group
    RICH_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    Console = None
    Text = None
    Rule = None
    Panel = None
    Columns = None
    Live = None
    Group = None
    RICH_AVAILABLE = False


_console = None  # type: ignore[assignment]
_live_dashboards = {}
_world_log_paths = {}
_world_recent_logs = {}
_console_handler: logging.Handler | None = None


def _get_console():
    global _console
    if _console is None:
        if RICH_AVAILABLE:
            _console = Console(highlight=False)
        else:
            # Fallback console using logging
            _console = None
    return _console


def _ensure_logger() -> logging.Logger:
    logger = logging.getLogger("archetype")
    if not logger.handlers:
        global _console_handler
        _console_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s")
        _console_handler.setFormatter(formatter)
        _console_handler.setLevel(logging.INFO)
        logger.addHandler(_console_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def _normalize(value: Any) -> Any:
    try:
        return str(value)
    except Exception:
        return value


def _kv(**fields: Any) -> str:
    parts = []
    for key, value in fields.items():
        val = _normalize(value)
        if isinstance(val, str) and (" " in val or "=" in val):
            parts.append(f"{key}='{val}'")
        else:
            parts.append(f"{key}={val}")
    return " ".join(parts)


_EVENT_STYLE = {
    "tick_start": "bold magenta",
    "tick_complete": "green",
    "archetype_start": "cyan",
    "proc_start": "cyan",
}

_LEVEL_STYLE = {
    logging.DEBUG: "dim",
    logging.INFO: "white",
    logging.WARNING: "yellow",
    logging.ERROR: "red",
    logging.CRITICAL: "bold red",
}


def log_event(level: int, event: str, base: Dict[str, Any] | None = None, **fields: Any) -> None:
    payload = dict(base or {})
    payload.update(fields)

    # If a live dashboard is active, avoid printing individual rich lines; capture recent logs for panel
    # and still route logs to file via the logger below.
    live_active = bool(_live_dashboards)
    if live_active:
        try:
            wid = str(payload.get("world_id", ""))
            if wid:
                from collections import deque
                dq = _world_recent_logs.get(wid)
                if dq is None:
                    dq = deque(maxlen=5)
                    _world_recent_logs[wid] = dq
                dq.append(_kv(event=event, **{k: v for k, v in payload.items() if k != "event"}))
        except Exception:
            pass
    if not live_active and RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
        style = _EVENT_STYLE.get(event, _LEVEL_STYLE.get(level, "white"))
        header = Text(f"[{event}] ", style=style)
        body = Text(_kv(**payload), style=_LEVEL_STYLE.get(level, "white"))

        # Section dividers for ticks
        if event == "tick_start":
            _get_console().print(Rule(title=f"tick {payload.get('tick')} start", style=style))
        _get_console().print(Text.assemble(header, body))
        if event == "tick_complete":
            _get_console().print(Rule(title=f"tick {payload.get('tick')} complete", style=style))
    else:
        logger = _ensure_logger()
        logger.log(level, _kv(event=event, **payload))


def is_rich_tty() -> bool:
    try:
        return bool(RICH_AVAILABLE and _get_console() is not None and os.isatty(1))
    except Exception:
        return False


# ----------------------------
# World file logging helpers
# ----------------------------

class _WorldFilter(logging.Filter):
    def __init__(self, world_id_str: str):
        super().__init__()
        self.world_id_str = world_id_str

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        try:
            msg = str(record.getMessage())
            return self.world_id_str in msg
        except Exception:
            return True


def _get_log_dir() -> str:
    log_dir = os.getenv("ARCT_LOG_DIR", ".archetype_logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception:
        pass
    return log_dir


def setup_world_file_logging(world) -> str:
    world_id_str = str(getattr(world, "world_id", "unknown"))
    if world_id_str in _world_log_paths:
        return _world_log_paths[world_id_str]

    log_dir = _get_log_dir()
    log_path = os.path.join(log_dir, f"world_{world_id_str}.log")
    try:
        logger = _ensure_logger()
        fh = logging.FileHandler(log_path)
        fh.setFormatter(logging.Formatter("%(message)s"))
        fh.addFilter(_WorldFilter(world_id_str))
        logger.addHandler(fh)
        _world_log_paths[world_id_str] = log_path
    except Exception:
        _world_log_paths[world_id_str] = log_path
    return log_path


def get_world_log_path(world) -> str:
    return _world_log_paths.get(str(getattr(world, "world_id", "unknown")), "")

def dbg(world, event: str, level: int = logging.INFO, **fields: Any) -> None:
    base = {
        "world_id": getattr(world, "world_id", None),
        "run_id": getattr(world, "run_id", None),
        "tick": getattr(world, "tick", None),
    }
    log_event(level, event, base, **fields)


def dbg_sig(world, sig: ArchetypeSignature, event: str, level: int = logging.INFO, **fields: Any) -> None:
    fields = {"sig": Archetype.get_name(sig), **fields}
    dbg(world, event, level, **fields)


# ----------------------------
# Pretty helpers for rich view
# ----------------------------

def short_id(value: Any, n: int = 12) -> str:
    try:
        s = str(value)
        return s[:n]
    except Exception:
        return str(value)


def tick_header(world, num_sigs: int) -> None:
    if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
        dbg(world, "tick_start", num_sigs=num_sigs)
        return
    title = f"tick {getattr(world, 'tick', None)} start"
    _get_console().print(Rule(title=title, style=_EVENT_STYLE["tick_start"]))
    header = Text()
    header.append("world ", style="dim")
    header.append(short_id(getattr(world, "world_id", "")), style="magenta")
    header.append("  run ", style="dim")
    header.append(short_id(getattr(world, "run_id", "")), style="magenta")
    header.append("  archetypes ", style="dim")
    header.append(str(num_sigs), style="white")
    _get_console().print(header)


def tick_footer(world, duration_ms: float) -> None:
    if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
        dbg(world, "tick_complete", duration_ms=duration_ms)
        return
    footer = Text()
    footer.append("duration ", style="dim")
    footer.append(f"{duration_ms:.3f} ms", style="green")
    _get_console().print(footer)
    title = f"tick {getattr(world, 'tick', None)} complete"
    _get_console().print(Rule(title=title, style=_EVENT_STYLE["tick_complete"]))


def archetype_panel(world, sig: ArchetypeSignature, df=None, run_config: RunConfig | None = None) -> None:
    name = Archetype.get_name(sig)
    if RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
        header = Text()
        header.append("archetype ", style="dim")
        header.append(name, style="cyan")
        _get_console().print(Panel(header, border_style="cyan", title="archetype", title_align="left"))
    else:
        dbg_sig(world, sig, "archetype_start")

    if df is not None and run_config and run_config.show_rows > 0:
        try:
            df.limit(run_config.show_rows).show()
        except Exception:
            pass

    if df is not None and run_config and run_config.explain:
        try:
            if RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
                from io import StringIO
                buf = StringIO()
                # Show a concise plan to reduce noise; users can toggle more later if desired
                df.explain(show_all=False, simple=True, file=buf)
                plan_text = buf.getvalue()
                _get_console().print(Panel(plan_text, border_style="dim", title="plan", title_align="left"))
            else:
                df.explain(show_all=False, simple=True)
        except Exception:
            pass


def proc_line(world, sig: ArchetypeSignature, *, processor: str, duration_ms: float | None = None, status: str = "ok") -> None:
    if RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
        line = Text()
        line.append("proc ", style="dim")
        line.append(processor, style="white")
        if duration_ms is not None:
            line.append("  ", style="dim")
            line.append(f"{duration_ms:.2f} ms", style="yellow")
        if status != "ok":
            line.append("  ", style="dim")
            line.append(status, style="red")
        _get_console().print(line)
    else:
        dbg_sig(world, sig, "proc_start", processor=processor, duration_ms=duration_ms, status=status)


# ----------------------------
# DataFrame debug renderers
# ----------------------------

def show_df(world, title: str, df, run_config: RunConfig) -> None:
    """
    Render a small DataFrame snapshot if enabled via run_config.show_rows.
    """
    rows = getattr(run_config, "show_rows", 0)
    if not getattr(run_config, "debug", False) or rows <= 0:
        return
    try:
        if RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
            _get_console().print(Panel(f"{title}", border_style="white", title="data", title_align="left"))
        df.limit(rows).show()
    except Exception:
        pass


def show_plan(world, title: str, df, run_config: RunConfig) -> None:
    """
    Render a concise logical plan if enabled via run_config.explain.
    """
    if not getattr(run_config, "debug", False) or not getattr(run_config, "explain", False):
        return
    try:
        if RICH_AVAILABLE and _get_console() is not None and os.isatty(1):
            from io import StringIO
            buf = StringIO()
            df.explain(show_all=False, simple=True, file=buf)
            plan_text = buf.getvalue()
            _get_console().print(Panel(plan_text, border_style="dim", title=f"plan: {title}", title_align="left"))
        else:
            df.explain(show_all=False, simple=True)
    except Exception:
        pass



# ----------------------------
# Live runtime dashboard
# ----------------------------

def _build_runtime_panel(world, tick: int, archetype_counts: dict[str, int], world_stats: dict | None = None, archetype_stats: dict | None = None):
    # Panel title shows world name and short id
    try:
        name = getattr(world, "name", "world")
        wid = str(getattr(world, "world_id", ""))
        short_wid = wid[:12] if wid else ""
        title = f"{name} ({short_wid})"
    except Exception:
        title = "world"
    header_group_items = []
    link = _build_links_row(world)
    if link:
        header_group_items.append(link)
    if world_stats:
        header_group_items.append(_build_world_stats_line(world_stats))
        io_line = _build_io_stats_line(world_stats)
        if io_line:
            header_group_items.append(io_line)
    header_group = Group(*header_group_items) if header_group_items else None

    # Build one small panel per archetype and show them as columns
    panels = []
    keys = sorted(archetype_stats.keys()) if archetype_stats else sorted(archetype_counts.keys())
    for name in keys:
        if archetype_stats and name in archetype_stats:
            panels.append(_build_archetype_panel(name, archetype_stats[name]))
        else:
            count = archetype_counts.get(name, 0)
            text = Text()
            text.append("entities ", style="dim")
            text.append(str(count), style="yellow")
            panels.append(Panel(text, border_style="cyan", title=name, title_align="left"))

    cols = Columns(panels, expand=True, equal=True)
    if header_group:
        content = Group(header_group, cols)
    else:
        content = cols
    return Panel(content, border_style="white", title=title, title_align="left")


def _build_links_row(world):
    try:
        if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
            return None
        path = get_world_log_path(world)
        if not path:
            return None
        text = Text()
        text.append("log ", style="dim")
        url = f"file://{os.path.abspath(path)}"
        text.append("open", style=f"link {url}")
        return text
    except Exception:
        return None


def _build_world_stats_line(world_stats: dict):
    try:
        t = Text()
        t.append("tick ", style="dim")
        t.append(str(world_stats.get("tick", "-")), style="white")
        t.append("  ")
        t.append("archetypes ", style="dim")
        t.append(str(world_stats.get("active_archetypes", "-")), style="white")
        t.append("  ")
        t.append("entities ", style="dim")
        t.append(str(world_stats.get("entities", "-")), style="yellow")
        t.append("  ")
        t.append("spawns ", style="dim")
        t.append(str(world_stats.get("spawns", 0)), style="green")
        t.append("  ")
        t.append("despawns ", style="dim")
        t.append(str(world_stats.get("despawns", 0)), style="red")
        t.append("  ")
        t.append("step ", style="dim")
        t.append(f"{world_stats.get('step_ms', 0):.1f} ms", style="cyan")
        return t
    except Exception:
        return None


def _build_io_stats_line(world_stats: dict):
    try:
        if not world_stats:
            return None
        t = Text()
        t.append("IO ", style="dim")
        t.append("rows ", style="dim")
        t.append(str(world_stats.get("appended_rows", 0)), style="white")
        t.append("  ")
        p50 = world_stats.get("append_p50_ms")
        p95 = world_stats.get("append_p95_ms")
        if p50 is not None:
            t.append("p50 ", style="dim")
            t.append(f"{p50:.1f} ms", style="white")
            t.append("  ")
        if p95 is not None:
            t.append("p95 ", style="dim")
            t.append(f"{p95:.1f} ms", style="white")
        return t
    except Exception:
        return None


def _build_archetype_panel(name: str, stats: dict):
    try:
        text = Text()
        text.append("entities ", style="dim")
        text.append(str(stats.get("entities", "-")), style="yellow")
        text.append("\n")
        text.append("spawns ", style="dim")
        text.append(str(stats.get("spawns", 0)), style="green")
        text.append("  ")
        text.append("desp ", style="dim")
        text.append(str(stats.get("despawns", 0)), style="red")
        text.append("\n")
        text.append("procs ", style="dim")
        text.append(str(stats.get("procs", "-")), style="white")
        text.append("  ")
        text.append("step ", style="dim")
        text.append(f"{stats.get('step_ms', 0):.1f} ms", style="cyan")
        return Panel(text, border_style="cyan", title=name, title_align="left")
    except Exception:
        return Panel(Text("-"), border_style="cyan", title=name, title_align="left")


def start_live_dashboard(world) -> None:
    if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
        return
    try:
        if str(world.world_id) in _live_dashboards:
            return
        renderable = _build_runtime_panel(world, getattr(world, "tick", 0), {})
        live = Live(renderable, console=_get_console(), refresh_per_second=10)
        live.start()
        _live_dashboards[str(world.world_id)] = live
        # Quiet console INFO logs while live dashboard runs (file handlers still receive logs)
        try:
            if _console_handler is not None:
                _console_handler.setLevel(logging.WARNING)
        except Exception:
            pass
    except Exception:
        return


def update_live_dashboard(world, tick: int, archetype_counts: dict[str, int]) -> None:
    if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
        return
    try:
        live = _live_dashboards.get(str(world.world_id))
        if live is None:
            return
        # Optional richer stats if provided via temporary stash
        wid = str(getattr(world, "world_id", ""))
        stats = getattr(world, "_dash_world_stats", None)
        astats = getattr(world, "_dash_archetype_stats", None)
        # Attach recent logs
        logs = _world_recent_logs.get(wid)
        if logs:
            try:
                log_text = Text("\n").join([Text(line, style="dim") for line in list(logs)])
                world._dash_logs = log_text  # type: ignore[attr-defined]
            except Exception:
                pass
        live.update(_build_runtime_panel(world, tick, archetype_counts, stats, astats))
    except Exception:
        return


def stop_live_dashboard(world) -> None:
    if not (RICH_AVAILABLE and _get_console() is not None and os.isatty(1)):
        return
    try:
        live = _live_dashboards.pop(str(world.world_id), None)
        if live is not None:
            live.stop()
        # Restore console verbosity when no dashboards remain
        if not _live_dashboards:
            try:
                if _console_handler is not None:
                    _console_handler.setLevel(logging.INFO)
            except Exception:
                pass
    except Exception:
        return

