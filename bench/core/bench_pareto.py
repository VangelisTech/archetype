#!/usr/bin/env python3
"""Pareto-frontier sweep over Archetype's storage knobs.

Sweeps the cache / batch / flush configuration space against the same
workload (append, updater, query) used by ``bench_param_sweep`` and
emits a JSON file plus a self-contained HTML/SVG visualization that
highlights the Pareto-optimal configurations along
``(latency_p95_ms, throughput_rows_s)``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

import daft
from daft import DataType, col

from archetype.core.aio.async_cached_store import AsyncCachedStore
from archetype.core.aio.async_querier import AsyncQueryManager
from archetype.core.aio.async_store import AsyncStore
from archetype.core.aio.async_updater import AsyncUpdateManager
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig
from archetype.runtime.session import configure_session


class Position(Component):
    x: int
    y: int


@dataclass
class Knob:
    """A single point in the configuration space."""

    name: str
    cache: bool
    batch_size: int
    flush_rows: int | None = None
    flush_mb: int | None = None
    global_mb: int | None = None
    idle_sec: float | None = None

    def cache_config(self) -> CacheConfig | None:
        if not self.cache:
            return None
        return CacheConfig(
            flush_rows=self.flush_rows or 500_000,
            flush_mb=self.flush_mb or 256,
            global_mb=self.global_mb or 2048,
            idle_sec=self.idle_sec or 30.0,
        )

    def short_label(self) -> str:
        if not self.cache:
            return f"plain·bs={self.batch_size}"
        return (
            f"cached·bs={self.batch_size}·"
            f"fr={self.flush_rows}·gm={self.global_mb}MB"
        )


@dataclass
class Sample:
    knob: Knob
    mode: str
    throughput_rows_s: float
    latency_p50_ms: float
    latency_p95_ms: float
    duration_s: float
    extras: dict[str, Any] = field(default_factory=dict)


def _build_rows(start_id: int, count: int, tick: int, world_id: str, run_id: str):
    rows = [
        {
            "entity_id": start_id + i,
            "position__x": start_id + i,
            "position__y": -(start_id + i),
            "is_active": True,
            "tick": tick,
            "world_id": world_id,
            "run_id": run_id,
        }
        for i in range(count)
    ]
    df = daft.from_pylist(rows).collect()
    try:
        df = df.with_columns(
            {
                "entity_id": col("entity_id").cast(DataType.uint32()),
                "tick": col("tick").cast(DataType.uint32()),
            }
        )
    except Exception:
        pass
    return df


async def _make_store(uri: str, namespace: str, cache_cfg: CacheConfig | None):
    storage = StorageConfig(uri=uri, namespace=namespace)
    session = configure_session(storage)
    base = AsyncStore(session, io_config=storage.io_config)
    if cache_cfg is None:
        return base, AsyncQueryManager(base), AsyncUpdateManager(base)
    cached = AsyncCachedStore(async_store=base, cache_config=cache_cfg)
    return cached, AsyncQueryManager(cached), AsyncUpdateManager(cached)


def _stats(latencies: list[float], total_rows: int, dur: float) -> dict[str, float]:
    if not latencies:
        return {
            "throughput_rows_s": 0.0,
            "latency_p50_ms": 0.0,
            "latency_p95_ms": 0.0,
            "duration_s": dur,
        }
    sl = sorted(latencies)
    p50 = median(sl)
    p95_idx = max(0, int(math.ceil(0.95 * len(sl))) - 1)
    p95 = sl[p95_idx]
    return {
        "throughput_rows_s": total_rows / max(dur, 1e-9),
        "latency_p50_ms": p50 * 1000.0,
        "latency_p95_ms": p95 * 1000.0,
        "duration_s": dur,
    }


async def _bench_append(store, knob: Knob, num_entities: int, steps: int, world_id: str, run_id: str):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    t0 = time.perf_counter()
    latencies: list[float] = []
    appended = 0
    for step in range(steps):
        remaining = num_entities
        start_id = step * num_entities
        while remaining > 0:
            n = min(knob.batch_size, remaining)
            df = _build_rows(start_id, n, tick=step, world_id=world_id, run_id=run_id)
            s = time.perf_counter()
            await store.append(sig, df)
            latencies.append(time.perf_counter() - s)
            appended += n
            start_id += n
            remaining -= n
    # Always include flush so cached variants aren't unfairly favored.
    s2 = time.perf_counter()
    await store.shutdown()
    dur = time.perf_counter() - t0
    flush_dur = time.perf_counter() - s2
    stats = _stats(latencies, appended, dur)
    stats["flush_s"] = flush_dur
    return stats, appended


async def _bench_updater(updater, store, knob: Knob, num_entities: int, steps: int, world_id: str, run_id: str):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    _ = await store.get_archetype_df(sig, world_id=world_id, run_id=run_id)
    t0 = time.perf_counter()
    latencies: list[float] = []
    appended = 0
    for step in range(steps):
        remaining = num_entities
        start_id = step * num_entities
        while remaining > 0:
            n = min(knob.batch_size, remaining)
            rows = [
                {
                    "entity_id": start_id + i,
                    "position__x": start_id + i,
                    "position__y": -(start_id + i),
                    "is_active": True,
                }
                for i in range(n)
            ]
            df = daft.from_pylist(rows)
            s = time.perf_counter()
            await updater.update(df, sig, tick=step, world_id=world_id, run_id=run_id)
            latencies.append(time.perf_counter() - s)
            appended += n
            start_id += n
            remaining -= n
    s2 = time.perf_counter()
    await store.shutdown()
    dur = time.perf_counter() - t0
    flush_dur = time.perf_counter() - s2
    stats = _stats(latencies, appended, dur)
    stats["flush_s"] = flush_dur
    return stats, appended


async def _bench_query(querier, world_id: str, run_id: str, ticks: list[int], iters: int):
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    latencies: list[float] = []
    scanned = 0
    for _ in range(iters):
        s = time.perf_counter()
        df = await querier.query_archetype(
            sig,
            world_id=world_id,
            ticks=ticks,
            entity_ids=None,
            components=[Position(x=0, y=0)],
            run_id=run_id,
        )
        scanned += df.collect().count_rows()
        latencies.append(time.perf_counter() - s)
    dur = sum(latencies)
    return _stats(latencies, scanned, dur), scanned


async def run_knob(knob: Knob, num_entities: int, steps: int, q_iters: int, root: str, namespace: str):
    base = os.path.join(root, knob.name)
    os.makedirs(base, exist_ok=True)

    cache_cfg = knob.cache_config()
    world_id = "w"
    run_id = "r"

    out: dict[str, Any] = {"knob": knob.__dict__.copy(), "label": knob.short_label()}

    # APPEND phase
    a_uri = os.path.join(base, "append")
    os.makedirs(a_uri, exist_ok=True)
    store, _, _ = await _make_store(a_uri, namespace, cache_cfg)
    a_stats, _ = await _bench_append(store, knob, num_entities, steps, world_id, run_id)
    out["append"] = a_stats

    # UPDATER phase
    u_uri = os.path.join(base, "updater")
    os.makedirs(u_uri, exist_ok=True)
    store, _, updater = await _make_store(u_uri, namespace, cache_cfg)
    u_stats, _ = await _bench_updater(updater, store, knob, num_entities, steps, world_id, run_id)
    out["updater"] = u_stats

    # QUERY phase: seed first, then read
    q_uri = os.path.join(base, "query")
    os.makedirs(q_uri, exist_ok=True)
    store, _, _ = await _make_store(q_uri, namespace, cache_cfg)
    await _bench_append(store, knob, num_entities, steps, world_id, run_id)
    store, querier, _ = await _make_store(q_uri, namespace, cache_cfg)
    q_stats, _ = await _bench_query(querier, world_id, run_id, list(range(steps)), q_iters)
    await store.shutdown()
    out["query"] = q_stats

    return out


def default_grid() -> list[Knob]:
    """A tractable but information-rich slice of config space."""
    knobs: list[Knob] = []
    # Plain (no cache): batch sizes only
    for bs in (100, 500, 2000, 10000):
        knobs.append(Knob(name=f"plain_bs{bs}", cache=False, batch_size=bs))
    # Cached: batch x flush_rows x global_mb
    for bs in (100, 500, 2000, 10000):
        for fr, gm in [
            (10_000, 64),
            (100_000, 256),
            (500_000, 1024),
            (2_000_000, 4096),
        ]:
            knobs.append(
                Knob(
                    name=f"cached_bs{bs}_fr{fr}_gm{gm}",
                    cache=True,
                    batch_size=bs,
                    flush_rows=fr,
                    flush_mb=min(gm, 256),
                    global_mb=gm,
                    idle_sec=30.0,
                )
            )
    return knobs


def pareto_front(points: list[tuple[float, float]]) -> list[int]:
    """Return indices on the 2D Pareto frontier (min x, max y)."""
    front: list[int] = []
    for i, (xi, yi) in enumerate(points):
        dominated = False
        for j, (xj, yj) in enumerate(points):
            if i == j:
                continue
            if xj <= xi and yj >= yi and (xj < xi or yj > yi):
                dominated = True
                break
        if not dominated:
            front.append(i)
    return front


def pareto_front_3d(points: list[tuple[float, float, float]]) -> list[int]:
    """Return indices on the 3D Pareto frontier.

    Convention: minimize x (latency), maximize y (throughput), minimize z (memory).
    """
    front: list[int] = []
    for i, (xi, yi, zi) in enumerate(points):
        dominated = False
        for j, (xj, yj, zj) in enumerate(points):
            if i == j:
                continue
            if (
                xj <= xi
                and yj >= yi
                and zj <= zi
                and (xj < xi or yj > yi or zj < zi)
            ):
                dominated = True
                break
        if not dominated:
            front.append(i)
    return front


def memory_cost_mb(r: dict) -> float:
    """Configured cache memory budget in MB. Plain stores cost 0."""
    knob = r["knob"]
    if not knob.get("cache"):
        return 0.0
    return float(knob.get("global_mb") or 0.0)


def render_html(results: list[dict], out_html: Path) -> None:
    """Render an interactive multi-panel SVG visualization, no JS deps.

    Each panel plots one workload phase along (p95 latency, throughput).
    Bubble area encodes the configured cache memory budget. Points on the
    full 3D frontier (min latency, max throughput, min memory) are ringed
    in orange; the dashed line connects the 2D-projection frontier so the
    classic shape stays visible.
    """
    modes = ["append", "updater", "query"]
    width, height = 1380, 520
    pad_l, pad_r, pad_t, pad_b = 80, 40, 70, 80
    panel_w = (width - pad_l - pad_r) // len(modes)
    plot_h = height - pad_t - pad_b

    panels_svg: list[str] = []
    summary_rows: list[str] = []

    # Memory bubble scaling, shared across panels for visual consistency.
    all_mem = [memory_cost_mb(r) for r in results]
    max_mem = max(all_mem) if all_mem else 1.0

    def bubble_radius(mem_mb: float, on_front: bool) -> float:
        if max_mem <= 0:
            base = 5.0
        else:
            base = 4.0 + 9.0 * math.sqrt(mem_mb / max_mem)
        return base + (2.0 if on_front else 0.0)

    for mi, mode in enumerate(modes):
        pts: list[tuple[float, float, float, dict]] = []  # (x, y, mem, r)
        for r in results:
            stats = r.get(mode) or {}
            x = stats.get("latency_p95_ms", 0.0)
            y = stats.get("throughput_rows_s", 0.0)
            if x <= 0 or y <= 0:
                continue
            pts.append((x, y, memory_cost_mb(r), r))
        if not pts:
            continue

        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        x_min = max(min(xs) * 0.80, 1e-3)
        x_max = max(xs) * 1.20
        y_min = max(min(ys) * 0.80, 1.0)
        y_max = max(ys) * 1.20
        lx_min, lx_max = math.log10(x_min), math.log10(x_max)
        ly_min, ly_max = math.log10(y_min), math.log10(y_max)

        ox = pad_l + mi * panel_w
        oy = pad_t
        plot_w = panel_w - 60

        def sx(x: float) -> float:
            return ox + (math.log10(x) - lx_min) / (lx_max - lx_min) * plot_w

        def sy(y: float) -> float:
            return oy + plot_h - (math.log10(y) - ly_min) / (ly_max - ly_min) * plot_h

        # Frontiers
        front_3d = set(pareto_front_3d([(p[0], p[1], p[2]) for p in pts]))
        front_2d = set(pareto_front([(p[0], p[1]) for p in pts]))
        front_2d_pts = sorted([pts[i] for i in front_2d], key=lambda p: p[0])

        parts: list[str] = []
        parts.append(
            f'<rect x="{ox}" y="{oy}" width="{plot_w}" height="{plot_h}" '
            f'fill="#0e1117" stroke="#2a3340" stroke-width="1" />'
        )
        parts.append(
            f'<text x="{ox + plot_w / 2}" y="{oy - 22}" fill="#e6edf3" '
            f'font-size="16" font-weight="600" text-anchor="middle">{mode.upper()}</text>'
        )
        parts.append(
            f'<text x="{ox + plot_w / 2}" y="{oy - 6}" fill="#8b949e" '
            f'font-size="11" text-anchor="middle">{len(pts)} configs · '
            f'{len(front_3d)} on 3D frontier</text>'
        )

        # Log gridlines
        for tick in range(int(math.floor(lx_min)), int(math.ceil(lx_max)) + 1):
            tx = sx(10 ** tick)
            if tx < ox or tx > ox + plot_w:
                continue
            parts.append(
                f'<line x1="{tx}" y1="{oy}" x2="{tx}" y2="{oy + plot_h}" '
                f'stroke="#1c232c" stroke-width="1" />'
            )
            parts.append(
                f'<text x="{tx}" y="{oy + plot_h + 16}" fill="#8b949e" '
                f'font-size="10" text-anchor="middle">10^{tick}</text>'
            )
        for tick in range(int(math.floor(ly_min)), int(math.ceil(ly_max)) + 1):
            ty = sy(10 ** tick)
            if ty < oy or ty > oy + plot_h:
                continue
            parts.append(
                f'<line x1="{ox}" y1="{ty}" x2="{ox + plot_w}" y2="{ty}" '
                f'stroke="#1c232c" stroke-width="1" />'
            )
            parts.append(
                f'<text x="{ox - 6}" y="{ty + 4}" fill="#8b949e" font-size="10" '
                f'text-anchor="end">10^{tick}</text>'
            )

        # Axis labels
        parts.append(
            f'<text x="{ox + plot_w / 2}" y="{oy + plot_h + 38}" '
            f'fill="#c9d1d9" font-size="11" text-anchor="middle">p95 latency (ms, log)</text>'
        )
        if mi == 0:
            parts.append(
                f'<text transform="translate({ox - 56}, {oy + plot_h / 2}) rotate(-90)" '
                f'fill="#c9d1d9" font-size="11" text-anchor="middle">throughput rows/s (log)</text>'
            )

        # 2D projection frontier as dashed polyline (visual aid).
        if len(front_2d_pts) >= 2:
            poly = " ".join(f"{sx(p[0]):.1f},{sy(p[1]):.1f}" for p in front_2d_pts)
            parts.append(
                f'<polyline points="{poly}" fill="none" stroke="#f39c12" '
                f'stroke-width="1.5" stroke-dasharray="4,3" opacity="0.7" />'
            )

        # Points
        for i, (x, y, mem, r) in enumerate(pts):
            cx = sx(x)
            cy = sy(y)
            cached = r["knob"]["cache"]
            color = "#58a6ff" if cached else "#d2a8ff"
            on_front_3d = i in front_3d
            on_front_2d = i in front_2d
            radius = bubble_radius(mem, on_front_3d)
            stroke = "#f39c12" if on_front_3d else "#0e1117"
            stroke_w = 2.5 if on_front_3d else 0.8
            opacity = "0.9" if on_front_3d else ("0.65" if on_front_2d else "0.45")
            mem_str = f"{mem:.0f}MB" if cached else "0MB (no cache)"
            tag = " · PARETO" if on_front_3d else ""
            tip = (
                f"{r['label']}{tag}\n"
                f"  p95: {x:.2f} ms\n"
                f"  throughput: {y:,.0f} rows/s\n"
                f"  cache budget: {mem_str}"
            )
            parts.append(
                f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{radius:.1f}" fill="{color}" '
                f'fill-opacity="{opacity}" stroke="{stroke}" stroke-width="{stroke_w}">'
                f'<title>{tip}</title></circle>'
            )

        panels_svg.append("\n".join(parts))

        # Add 3D frontier members to the summary table
        for i in sorted(front_3d, key=lambda i: pts[i][0]):
            x, y, mem, r = pts[i]
            mem_str = f"{mem:.0f}" if r["knob"]["cache"] else "0"
            summary_rows.append(
                f"<tr><td>{mode}</td><td>{r['label']}</td>"
                f"<td>{x:.2f}</td><td>{y:,.0f}</td><td>{mem_str}</td></tr>"
            )

    svg = "\n".join(panels_svg)
    rows_html = summary_rows
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"/>
<title>Archetype — Pareto Frontier</title>
<style>
  body {{ background:#0d1117; color:#e6edf3;
         font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Helvetica Neue', sans-serif;
         margin: 24px 32px; }}
  h1 {{ margin: 0 0 4px 0; font-weight: 600; letter-spacing: -0.02em; }}
  p.sub {{ color:#8b949e; margin:0 0 18px 0; max-width: 920px; line-height: 1.5; }}
  svg {{ background:#0d1117; }}
  .legend {{ display:flex; gap:24px; margin: 14px 0 6px 0; font-size: 12px; color:#c9d1d9; flex-wrap: wrap; }}
  .swatch {{ display:inline-block; width:10px; height:10px; border-radius: 50%; vertical-align:middle; margin-right:6px; }}
  .ring {{ display:inline-block; width:10px; height:10px; border-radius: 50%;
          border: 2px solid #f39c12; vertical-align:middle; margin-right:6px; }}
  .bubbles {{ display:inline-flex; align-items: center; gap:6px; }}
  .bubbles span.b {{ display:inline-block; border-radius:50%; background:#58a6ff; opacity:0.7; }}
  table {{ width:100%; border-collapse: collapse; font-size: 12px; margin-top:6px; }}
  th, td {{ text-align:left; padding: 6px 10px; border-bottom: 1px solid #1c232c; }}
  th {{ color:#8b949e; font-weight:500; }}
  td:nth-child(3), td:nth-child(4), td:nth-child(5) {{ font-variant-numeric: tabular-nums; }}
  code {{ background:#161b22; padding: 1px 6px; border-radius: 4px; }}
</style>
</head>
<body>
  <h1>Archetype — Pareto frontier of storage configurations</h1>
  <p class="sub">
    Each point is one storage configuration evaluated against the same workload.
    Axes are log-scale. Bubble area encodes the configured cache memory budget
    (<code>global_mb</code>; plain configs cost 0). Orange-ringed points are on the
    full <strong>3D Pareto frontier</strong> — no other config beats them on all of
    p95 latency, throughput, <em>and</em> memory. The dashed orange curve is the 2D
    (latency, throughput) projection for visual orientation.
    Hover for full configuration details.
  </p>
  <div class="legend">
    <span><span class="swatch" style="background:#d2a8ff"></span>plain (no cache)</span>
    <span><span class="swatch" style="background:#58a6ff"></span>cached</span>
    <span><span class="ring"></span>3D Pareto-optimal</span>
    <span class="bubbles">cache budget:
      <span class="b" style="width:6px; height:6px;"></span>64MB
      <span class="b" style="width:10px; height:10px;"></span>1GB
      <span class="b" style="width:16px; height:16px;"></span>4GB
    </span>
  </div>
  <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    {svg}
  </svg>
  <h3 style="margin-top:28px;">3D Pareto-optimal configurations</h3>
  <table>
    <thead><tr>
      <th>Phase</th><th>Configuration</th>
      <th>p95 latency (ms)</th><th>Throughput (rows/s)</th><th>Cache budget (MB)</th>
    </tr></thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</body></html>
"""
    out_html.write_text(html)


async def main_async(args: argparse.Namespace) -> None:
    root = os.path.abspath(args.tmp_dir)
    os.makedirs(root, exist_ok=True)
    knobs = default_grid()
    if args.limit:
        knobs = knobs[: args.limit]

    results: list[dict] = []
    for i, knob in enumerate(knobs, 1):
        print(f"[{i}/{len(knobs)}] {knob.short_label()}", file=sys.stderr, flush=True)
        try:
            res = await run_knob(
                knob,
                num_entities=args.num_entities,
                steps=args.steps,
                q_iters=args.q_iters,
                root=root,
                namespace=args.namespace,
            )
            results.append(res)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! failed: {exc}", file=sys.stderr, flush=True)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(results, indent=2))
    print(f"wrote {out_json}", file=sys.stderr)

    out_html = Path(args.out_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    render_html(results, out_html)
    print(f"wrote {out_html}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pareto-frontier sweep for Archetype storage knobs")
    p.add_argument("--num-entities", type=int, default=2_000)
    p.add_argument("--steps", type=int, default=3)
    p.add_argument("--q-iters", type=int, default=3)
    p.add_argument("--namespace", type=str, default="bench_pareto")
    p.add_argument("--tmp-dir", type=str, default=".archetype_pareto")
    p.add_argument("--out-json", type=str, default="docs/reports/pareto_frontier.json")
    p.add_argument("--out-html", type=str, default="docs/reports/pareto_frontier.html")
    p.add_argument("--limit", type=int, default=0, help="Only run the first N knobs (for smoke testing)")
    return p.parse_args()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
