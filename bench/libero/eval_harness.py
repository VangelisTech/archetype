# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: E402

"""DataFrame-based LIBERO eval harness.

This module is the LIBERO-specific template for composing evals over persisted
Archetype rows:

1. Select a target set of rows from the ledger or an exported lab directory.
2. Apply DataFrame filters for suite/task/trial/run sampling.
3. Execute graders over the resulting DataFrame.
4. Return the existing eval framework's ``TaskResult`` objects.

The harness does not define a new durable result schema. If a score needs to
persist, callers should write it as a component after grading.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from inspect import isawaitable
from pathlib import Path
from typing import Any

_ROOT = str(Path(__file__).parents[2])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import daft
from daft import DataFrame, col
from uuid_utils import UUID

from archetype.app.eval_service import EvalService
from archetype.core.config import StorageConfig
from archetype.experiments.components import EvalTrialResult
from evals.graders import state_check, threshold
from evals.types import GraderResult, TaskResult, TrialResult

try:
    from bench.libero.report import load_trial_rows
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    sys.path.insert(0, str(Path(__file__).parent))
    from report import load_trial_rows  # type: ignore[no-redef]

EvalDataFrameGrader = Callable[[DataFrame], GraderResult | Sequence[GraderResult]]
AsyncEvalDataFrameGrader = Callable[
    [DataFrame],
    GraderResult | Sequence[GraderResult] | Awaitable[GraderResult | Sequence[GraderResult]],
]

_EVAL_TRIAL_COLUMNS = (
    "evaltrialresult__suite",
    "evaltrialresult__task_id",
    "evaltrialresult__trial_idx",
    "evaltrialresult__seed",
    "evaltrialresult__env_key",
    "evaltrialresult__success",
    "evaltrialresult__episode_length",
    "evaltrialresult__wall_s",
    "evaltrialresult__run_id",
    "evaltrialresult__recorded_at_ms",
)


@dataclass(frozen=True)
class LiberoEvalTarget:
    """Where and how to sample LIBERO eval rows."""

    world_id: str | UUID | None = None
    run_id: str | UUID | None = None
    storage_config: StorageConfig | None = None
    lab_dir: str | None = None
    rows: Sequence[dict[str, Any]] | None = None
    ticks: list[int] | None = None
    entity_ids: list[int] | None = None
    lineage: list[tuple[str, str, int]] | None = None
    suites: Sequence[str] | None = None
    task_ids: Sequence[int] | None = None
    trial_idxs: Sequence[int] | None = None
    run_labels: Sequence[str] | None = None

    def overlay(self, other: LiberoEvalTarget | None) -> LiberoEvalTarget:
        """Return ``self`` with non-empty fields from ``other`` applied."""
        if other is None:
            return self
        return LiberoEvalTarget(
            world_id=other.world_id if other.world_id is not None else self.world_id,
            run_id=other.run_id if other.run_id is not None else self.run_id,
            storage_config=(
                other.storage_config if other.storage_config is not None else self.storage_config
            ),
            lab_dir=other.lab_dir if other.lab_dir is not None else self.lab_dir,
            rows=other.rows if other.rows is not None else self.rows,
            ticks=other.ticks if other.ticks is not None else self.ticks,
            entity_ids=other.entity_ids if other.entity_ids is not None else self.entity_ids,
            lineage=other.lineage if other.lineage is not None else self.lineage,
            suites=other.suites if other.suites is not None else self.suites,
            task_ids=other.task_ids if other.task_ids is not None else self.task_ids,
            trial_idxs=other.trial_idxs if other.trial_idxs is not None else self.trial_idxs,
            run_labels=other.run_labels if other.run_labels is not None else self.run_labels,
        )


@dataclass(frozen=True)
class LiberoDataFrameEval:
    """One dataframe-backed eval task."""

    task_id: str
    graders: Sequence[AsyncEvalDataFrameGrader]
    suite: str = "capability"
    desc: str = ""
    target: LiberoEvalTarget | None = None


@dataclass
class LiberoDataFrameEvalHarness:
    """Compose dataframe-backed LIBERO eval tasks."""

    eval_service: EvalService | None = None
    tasks: list[LiberoDataFrameEval] = field(default_factory=list)

    def add(
        self,
        task_id: str,
        *,
        graders: Sequence[AsyncEvalDataFrameGrader],
        suite: str = "capability",
        desc: str = "",
        target: LiberoEvalTarget | None = None,
    ) -> None:
        self.tasks.append(
            LiberoDataFrameEval(
                task_id=task_id,
                graders=graders,
                suite=suite,
                desc=desc,
                target=target,
            )
        )

    async def query_trials(self, target: LiberoEvalTarget) -> DataFrame:
        """Load and filter ``EvalTrialResult`` rows for a target."""
        if target.rows is not None:
            df = trial_rows_to_dataframe(target.rows)
        elif target.lab_dir is not None:
            df = trial_rows_to_dataframe(load_trial_rows(target.lab_dir))
        elif self.eval_service is not None and target.world_id is not None and target.run_id:
            df = await self.eval_service.query_components(
                [EvalTrialResult],
                world_id=target.world_id,
                run_id=target.run_id,
                storage_config=target.storage_config,
                ticks=target.ticks,
                entity_ids=target.entity_ids,
                lineage=target.lineage,
            )
        else:
            raise ValueError(
                "LiberoEvalTarget requires rows, lab_dir, or eval_service with world_id/run_id"
            )
        return filter_trial_rows(df, target)

    async def run(self, target: LiberoEvalTarget) -> list[TaskResult]:
        """Run all registered dataframe eval tasks."""
        results: list[TaskResult] = []
        for task in self.tasks:
            task_target = target.overlay(task.target)
            df = await self.query_trials(task_target)
            t0 = time.perf_counter()
            try:
                grader_results = await run_dataframe_graders(df, task.graders)
                elapsed_s = time.perf_counter() - t0
                passed = all(result.passed for result in grader_results)
                score = _mean_score(grader_results)
                trial = TrialResult(
                    trial_idx=0,
                    passed=passed,
                    score=score,
                    elapsed_s=elapsed_s,
                    grader_results=grader_results,
                )
            except Exception as exc:
                elapsed_s = time.perf_counter() - t0
                trial = TrialResult(
                    trial_idx=0,
                    passed=False,
                    score=0.0,
                    elapsed_s=elapsed_s,
                    error=str(exc),
                )
            results.append(
                TaskResult(
                    task_id=task.task_id,
                    suite=task.suite,
                    desc=task.desc,
                    trials=[trial],
                )
            )
        return results


def build_default_libero_harness(
    *,
    min_trials: int = 1,
    min_success_rate: float = 0.0,
    max_mean_episode_length: float | None = None,
) -> LiberoDataFrameEvalHarness:
    """Build a default LIBERO dataframe eval suite."""
    harness = LiberoDataFrameEvalHarness()
    harness.add(
        "libero_trial_success",
        suite="capability",
        desc="LIBERO trial count and success-rate gates over selected rows",
        graders=[
            trial_count_at_least(min_trials),
            success_rate_at_least(min_success_rate),
        ],
    )
    harness.add(
        "libero_trial_routing",
        suite="regression",
        desc="LIBERO seed/env routing invariants for selected rows",
        graders=[seeds_match_task_trial_rule(), env_key_matches_trial_idx()],
    )
    if max_mean_episode_length is not None:
        harness.add(
            "libero_episode_length",
            suite="capability",
            desc="LIBERO mean episode length gate over selected rows",
            graders=[mean_episode_length_at_most(max_mean_episode_length)],
        )
    return harness


def trial_rows_to_dataframe(rows: Sequence[dict[str, Any]]) -> DataFrame:
    """Convert loaded row dicts into a Daft DataFrame."""
    columns = set(_EVAL_TRIAL_COLUMNS)
    for row in rows:
        columns.update(row.keys())
    ordered_columns = sorted(columns)
    return daft.from_pydict({name: [row.get(name) for row in rows] for name in ordered_columns})


def filter_trial_rows(df: DataFrame, target: LiberoEvalTarget) -> DataFrame:
    """Apply LIBERO suite/task/trial/run-label filters."""
    filters: tuple[tuple[str, Sequence[Any] | None], ...] = (
        ("evaltrialresult__suite", target.suites),
        ("evaltrialresult__task_id", target.task_ids),
        ("evaltrialresult__trial_idx", target.trial_idxs),
        ("evaltrialresult__run_id", target.run_labels),
    )
    for column_name, values in filters:
        if values is None:
            continue
        df = df.where(col(column_name).is_in(list(values)))
    return df


async def run_dataframe_graders(
    df: DataFrame,
    graders: Sequence[AsyncEvalDataFrameGrader],
) -> list[GraderResult]:
    """Execute dataframe graders and flatten their results."""
    results: list[GraderResult] = []
    for grader in graders:
        raw = grader(df)
        output = await raw if isawaitable(raw) else raw
        if isinstance(output, GraderResult):
            results.append(output)
        else:
            results.extend(output)
    return results


def trial_count_at_least(min_trials: int, *, name: str = "trial_count") -> EvalDataFrameGrader:
    """Grade that at least ``min_trials`` rows were selected."""

    def grade(df: DataFrame) -> GraderResult:
        return threshold(float(df.count_rows()), min_val=float(min_trials), name=name)

    return grade


def success_rate_at_least(
    min_rate: float,
    *,
    name: str = "success_rate",
) -> EvalDataFrameGrader:
    """Grade selected rows by mean ``EvalTrialResult.success``."""

    def grade(df: DataFrame) -> GraderResult:
        if df.count_rows() == 0:
            return GraderResult(
                grader_name=name,
                passed=False,
                score=0.0,
                details="no selected trial rows",
            )
        rate = _scalar(
            df.select(col("evaltrialresult__success").cast("int64").mean().alias("success_rate")),
            "success_rate",
        )
        return threshold(float(rate or 0.0), min_val=min_rate, name=name)

    return grade


def mean_episode_length_at_most(
    max_steps: float,
    *,
    name: str = "mean_episode_length",
) -> EvalDataFrameGrader:
    """Grade selected rows by mean episode length."""

    def grade(df: DataFrame) -> GraderResult:
        if df.count_rows() == 0:
            return GraderResult(
                grader_name=name,
                passed=False,
                score=0.0,
                details="no selected trial rows",
            )
        mean_steps = _scalar(
            df.select(
                col("evaltrialresult__episode_length")
                .cast("float64")
                .mean()
                .alias("mean_episode_length")
            ),
            "mean_episode_length",
        )
        return threshold(float(mean_steps or 0.0), max_val=max_steps, name=name)

    return grade


def seeds_match_task_trial_rule(
    *,
    name: str = "seed_routing",
) -> EvalDataFrameGrader:
    """Grade ``seed == task_id * 1000 + trial_idx`` for each selected row."""

    def grade(df: DataFrame) -> GraderResult:
        rows = _selected_routing_rows(df)
        if not rows:
            return GraderResult(
                grader_name=name,
                passed=False,
                score=0.0,
                details="no selected trial rows",
            )
        checks = {}
        for row in rows:
            task_id = int(row["evaltrialresult__task_id"])
            trial_idx = int(row["evaltrialresult__trial_idx"])
            expected = task_id * 1000 + trial_idx
            seed = int(row["evaltrialresult__seed"])
            checks[f"task{task_id}:trial{trial_idx}:seed"] = seed == expected
        return state_check(checks, name=name)

    return grade


def env_key_matches_trial_idx(
    *,
    name: str = "env_key_routing",
) -> EvalDataFrameGrader:
    """Grade ``env_key == trial_idx`` for each selected row."""

    def grade(df: DataFrame) -> GraderResult:
        rows = _selected_routing_rows(df)
        if not rows:
            return GraderResult(
                grader_name=name,
                passed=False,
                score=0.0,
                details="no selected trial rows",
            )
        checks = {}
        for row in rows:
            task_id = int(row["evaltrialresult__task_id"])
            trial_idx = int(row["evaltrialresult__trial_idx"])
            env_key = int(row["evaltrialresult__env_key"])
            checks[f"task{task_id}:trial{trial_idx}:env_key"] = env_key == trial_idx
        return state_check(checks, name=name)

    return grade


def _selected_routing_rows(df: DataFrame) -> list[dict[str, Any]]:
    return (
        df.select(
            col("evaltrialresult__task_id"),
            col("evaltrialresult__trial_idx"),
            col("evaltrialresult__seed"),
            col("evaltrialresult__env_key"),
        )
        .collect()
        .to_pylist()
    )


def _scalar(df: DataFrame, column_name: str) -> Any:
    rows = df.collect().to_pylist()
    if not rows:
        return None
    return rows[0][column_name]


def _mean_score(results: Sequence[GraderResult]) -> float:
    if not results:
        return 0.0
    return sum(result.score for result in results) / len(results)


async def _run_cli(args: Any) -> int:
    target = LiberoEvalTarget(
        lab_dir=args.lab_dir,
        suites=[args.suite] if args.suite else None,
        task_ids=args.task_ids,
        run_labels=[args.run_id] if args.run_id else None,
    )
    harness = build_default_libero_harness(
        min_trials=args.min_trials,
        min_success_rate=args.min_success_rate,
        max_mean_episode_length=args.max_mean_episode_length,
    )
    results = await harness.run(target)
    print(json.dumps([result.to_dict() for result in results], indent=2))
    return 0 if all(result.all_passed for result in results) else 1


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run dataframe-based LIBERO evals")
    parser.add_argument("--lab-dir", required=True, help="Lab directory written by eval_driver.py")
    parser.add_argument("--suite", default="", help="Optional LIBERO suite filter")
    parser.add_argument(
        "--task-ids",
        type=lambda s: [int(x) for x in s.split(",")],
        default=None,
        help="Optional comma-separated task IDs",
    )
    parser.add_argument("--run-id", default="", help="Optional EvalTrialResult run_id filter")
    parser.add_argument("--min-trials", type=int, default=1)
    parser.add_argument("--min-success-rate", type=float, default=0.0)
    parser.add_argument("--max-mean-episode-length", type=float, default=None)
    return asyncio.run(_run_cli(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
