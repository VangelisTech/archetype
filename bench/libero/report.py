# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO eval report generator.

Reads the lab world written by ``eval_driver.py`` (or any parquet/Lance
store carrying ``EvalTrialResult`` rows) and emits a markdown report with:

- Per-suite and per-task success-rate table
- Mean episode length per task
- Wall-clock summary per task
- Provenance-tax headline: ledger overhead per tick as % of mean step
  latency (uses the measured 7 ms figure with citation; re-wire this when
  ``bench/mujoco/rollout_overhead.py`` is plumbed in live)

Usage:
    uv run python bench/libero/report.py --lab-dir /tmp/libero_eval
    uv run python bench/libero/report.py --lab-dir /tmp/libero_eval --out report.md

The report is also returned as a string by ``generate_report()`` so eval
pipelines can embed it in CI artifacts or Slack posts without writing a file.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

sys.path.insert(0, str(Path(__file__).parent))

# ---------------------------------------------------------------------------
# Provenance-tax constant (cite to measurement)
# ---------------------------------------------------------------------------

# 7 ms per-tick ledger overhead measured in bench/mujoco/rollout_overhead.py.
# This figure is reused until a fresh measurement is wired from that script.
# Once wired, replace this constant with a direct read from the bench output.
_LEDGER_OVERHEAD_MS: float = 7.0
_LEDGER_OVERHEAD_CITATION = "bench/mujoco/rollout_overhead.py (measured 2026-06)"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class TaskStats:
    """Aggregated statistics for one (suite, task_id) pair."""

    suite: str
    task_id: int
    trials: list[dict[str, Any]] = field(default_factory=list)

    @property
    def n(self) -> int:
        return len(self.trials)

    @property
    def successes(self) -> int:
        return sum(1 for t in self.trials if t.get("evaltrialresult__success", False))

    @property
    def success_rate(self) -> float:
        return self.successes / self.n if self.n else 0.0

    @property
    def mean_episode_length(self) -> float:
        lengths = [t.get("evaltrialresult__episode_length", 0) for t in self.trials]
        return sum(lengths) / len(lengths) if lengths else 0.0

    @property
    def mean_wall_s(self) -> float:
        walls = [t.get("evaltrialresult__wall_s", 0.0) for t in self.trials]
        return sum(walls) / len(walls) if walls else 0.0

    @property
    def mean_step_latency_ms(self) -> float:
        """Mean wall-clock ms per env step (across all trials)."""
        if not self.trials:
            return 0.0
        total_steps = sum(t.get("evaltrialresult__episode_length", 1) for t in self.trials)
        total_wall_ms = (
            sum(t.get("evaltrialresult__wall_s", 0.0) for t in self.trials) * 1000.0
        )
        return total_wall_ms / total_steps if total_steps else 0.0


@dataclass
class SuiteStats:
    """Aggregated statistics for one suite."""

    suite: str
    tasks: list[TaskStats] = field(default_factory=list)

    @property
    def n(self) -> int:
        return sum(t.n for t in self.tasks)

    @property
    def success_rate(self) -> float:
        total = sum(t.n for t in self.tasks)
        wins = sum(t.successes for t in self.tasks)
        return wins / total if total else 0.0

    @property
    def mean_episode_length(self) -> float:
        lengths = [t.mean_episode_length for t in self.tasks if t.n > 0]
        return sum(lengths) / len(lengths) if lengths else 0.0


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_trial_rows(lab_dir: str) -> list[dict[str, Any]]:
    """Load EvalTrialResult rows from the lab world parquet.

    Tries Lance/LanceDB store layout first (the Archetype default), then
    falls back to scanning ``*.parquet`` files in the directory tree so
    the function works against both the full store and exported snapshots.

    Archetype stores data in ``{lab_dir}/lab/{namespace}/lance/`` directories.
    Each namespace directory is a LanceDB database; we scan all of them.

    Returns a list of raw row dicts with ``evaltrialresult__*`` column keys.
    """
    lab_path = Path(lab_dir) / "lab"
    if not lab_path.exists():
        lab_path = Path(lab_dir)  # Allow passing the lab subdir directly
    rows: list[dict[str, Any]] = []

    # Strategy 1: Lance table (Archetype default store layout).
    # Layout: lab/{namespace}/lance/*.lance/
    # Each namespace dir may be a LanceDB database; try connecting to
    # both ``lab/`` directly and any ``*/lance/`` subdirectory.
    try:
        import lancedb  # type: ignore[import-untyped]
        import warnings

        # Collect candidate LanceDB database paths: the immediate lab path
        # and any {namespace}/lance subdirectory found under it.
        candidate_paths: list[Path] = [lab_path]
        if lab_path.is_dir():
            for child in lab_path.iterdir():
                if child.is_dir():
                    # Archetype uses {namespace}/lance/ as the Lance root.
                    lance_subdir = child / "lance"
                    if lance_subdir.is_dir():
                        candidate_paths.append(lance_subdir)

        for db_path in candidate_paths:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    db = lancedb.connect(str(db_path))
                    table_names = db.table_names()
                for table_name in table_names:
                    try:
                        tbl = db.open_table(table_name)
                        arrow_tbl = tbl.to_lance().scanner().to_table()
                        col_names = arrow_tbl.schema.names
                        # Check if this table has evaltrialresult__ columns.
                        if any(c.startswith("evaltrialresult__") for c in col_names):
                            tbl_dict = arrow_tbl.to_pydict()
                            n = len(next(iter(tbl_dict.values())))
                            for i in range(n):
                                rows.append({k: v[i] for k, v in tbl_dict.items()})
                    except Exception:
                        pass
            except Exception:
                pass

        if rows:
            return rows
    except Exception:
        pass

    # Strategy 2: Parquet files (exported snapshots or alternative stores).
    try:
        import daft  # type: ignore[import-untyped]

        parquets = list(lab_path.rglob("*.parquet"))
        if not parquets and Path(lab_dir).exists():
            parquets = list(Path(lab_dir).rglob("*.parquet"))
        for pq in parquets:
            try:
                df = daft.read_parquet(str(pq))
                if "evaltrialresult__suite" in df.schema().column_names():
                    rows.extend(df.to_pylist())
            except Exception:
                pass
        if rows:
            return rows
    except Exception:
        pass

    return rows


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------


def generate_report(
    rows: list[dict[str, Any]],
    *,
    title: str = "LIBERO Eval Report",
    run_id: str = "",
    generated_at: str = "",
) -> str:
    """Generate a markdown report from a list of EvalTrialResult row dicts.

    Args:
        rows: EvalTrialResult row dicts (keys prefixed with evaltrialresult__).
        title: Report title.
        run_id: Optional run identifier to include in the header.
        generated_at: ISO timestamp string; defaults to now.

    Returns the report as a markdown string.
    """
    generated_at = generated_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Group by suite then task_id.
    by_suite: dict[str, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        suite = str(row.get("evaltrialresult__suite", "unknown"))
        task_id = int(row.get("evaltrialresult__task_id", 0))
        by_suite[suite][task_id].append(row)

    suite_stats_list: list[SuiteStats] = []
    for suite_name in sorted(by_suite):
        task_dict = by_suite[suite_name]
        task_stats_list: list[TaskStats] = []
        for tid in sorted(task_dict):
            ts = TaskStats(suite=suite_name, task_id=tid, trials=task_dict[tid])
            task_stats_list.append(ts)
        suite_stats_list.append(SuiteStats(suite=suite_name, tasks=task_stats_list))

    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    if run_id:
        lines.append(f"**Run:** `{run_id}`  ")
    lines.append(f"**Generated:** {generated_at}  ")
    lines.append(f"**Total trials:** {len(rows)}  ")
    lines.append("")

    # --- Per-suite summary ---
    lines.append("## Suite Summary")
    lines.append("")
    lines.append("| Suite | Tasks | Trials | Success Rate | Mean Ep. Length |")
    lines.append("|-------|-------|--------|--------------|-----------------|")
    for ss in suite_stats_list:
        lines.append(
            f"| {ss.suite} | {len(ss.tasks)} | {ss.n} "
            f"| {ss.success_rate:.1%} | {ss.mean_episode_length:.1f} |"
        )
    lines.append("")

    # --- Per-task detail ---
    lines.append("## Per-Task Results")
    lines.append("")
    for ss in suite_stats_list:
        lines.append(f"### {ss.suite}")
        lines.append("")
        lines.append(
            "| Task | Trials | Successes | Success Rate | "
            "Mean Steps | Mean Wall (s) |"
        )
        lines.append(
            "|------|--------|-----------|--------------|"
            "------------|---------------|"
        )
        for ts in ss.tasks:
            lines.append(
                f"| {ts.task_id} | {ts.n} | {ts.successes} "
                f"| {ts.success_rate:.1%} "
                f"| {ts.mean_episode_length:.1f} "
                f"| {ts.mean_wall_s:.2f} |"
            )
        lines.append("")

    # --- Provenance-tax headline ---
    # Mean step latency averaged across all tasks that have timing data.
    all_step_latencies = []
    for ss in suite_stats_list:
        for ts in ss.tasks:
            lat = ts.mean_step_latency_ms
            if lat > 0:
                all_step_latencies.append(lat)

    lines.append("## Provenance Tax")
    lines.append("")
    if all_step_latencies:
        mean_step_ms = sum(all_step_latencies) / len(all_step_latencies)
        tax_pct = (_LEDGER_OVERHEAD_MS / mean_step_ms) * 100.0 if mean_step_ms > 0 else 0.0
        lines.append(
            f"**Ledger overhead:** {_LEDGER_OVERHEAD_MS:.0f} ms/tick  "
        )
        lines.append(
            f"**Mean step latency:** {mean_step_ms:.1f} ms/tick  "
        )
        lines.append(
            f"**Overhead as % of step:** {tax_pct:.1f}%  "
        )
    else:
        lines.append(
            f"**Ledger overhead (fixed citation):** "
            f"{_LEDGER_OVERHEAD_MS:.0f} ms/tick  "
        )
        lines.append("*(No timing data available; step latency unknown.)*  ")
    lines.append("")
    lines.append(
        f"> Ledger overhead is the time to write one tick's append-only "
        f"snapshot to the Lance store, measured independently in "
        f"`{_LEDGER_OVERHEAD_CITATION}`. This cost is paid once per tick "
        f"regardless of entity count (dominated by metadata, not payload size). "
        f"The provenance-tax figure above is the ratio of this fixed overhead "
        f"to the mean control-loop step latency for this eval run."
    )
    lines.append("")

    # --- Seed table ---
    lines.append("## Trial Seed Table")
    lines.append("")
    lines.append("| Suite | Task | Trial | Seed | Env Key | Success | Steps |")
    lines.append("|-------|------|-------|------|---------|---------|-------|")
    for row in sorted(
        rows,
        key=lambda r: (
            r.get("evaltrialresult__suite", ""),
            r.get("evaltrialresult__task_id", 0),
            r.get("evaltrialresult__trial_idx", 0),
        ),
    ):
        lines.append(
            f"| {row.get('evaltrialresult__suite', '')} "
            f"| {row.get('evaltrialresult__task_id', '')} "
            f"| {row.get('evaltrialresult__trial_idx', '')} "
            f"| {row.get('evaltrialresult__seed', '')} "
            f"| {row.get('evaltrialresult__env_key', '')} "
            f"| {row.get('evaltrialresult__success', '')} "
            f"| {row.get('evaltrialresult__episode_length', '')} |"
        )
    lines.append("")

    return "\n".join(lines)


def report_from_components(
    results: list[Any],
    *,
    title: str = "LIBERO Eval Report",
    run_id: str = "",
) -> str:
    """Generate a report from a list of ``EvalTrialResult`` component instances.

    Convenience wrapper for the eval driver's in-memory return value —
    converts component instances to the row-dict format expected by
    ``generate_report``.
    """
    rows = [r.to_row_dict() if hasattr(r, "to_row_dict") else vars(r) for r in results]
    return generate_report(rows, title=title, run_id=run_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate LIBERO eval report")
    parser.add_argument(
        "--lab-dir",
        required=True,
        help="Lab world directory written by eval_driver.py",
    )
    parser.add_argument("--out", default="", help="Write markdown report to this file path")
    parser.add_argument("--title", default="LIBERO Eval Report", help="Report title")
    parser.add_argument("--run-id", default="", help="Run identifier label")
    args = parser.parse_args()

    rows = load_trial_rows(args.lab_dir)
    if not rows:
        print(
            f"No EvalTrialResult rows found in {args.lab_dir!r}. "
            "Run eval_driver.py first or check --lab-dir.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Loaded {len(rows)} trial rows from {args.lab_dir!r}")
    report = generate_report(rows, title=args.title, run_id=args.run_id)
    print(report)

    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"\nReport written to {args.out!r}")


if __name__ == "__main__":
    main()
