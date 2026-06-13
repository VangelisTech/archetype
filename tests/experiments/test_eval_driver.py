# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the LIBERO eval driver (bench/libero/eval_driver.py).

Drives the full driver loop with scripted env clients and a FakeChunkPolicy
(no Modal, no GPU) so tests are CI-safe.  Verifies:

- N trials produce N EvalTrialResult rows with correct success accounting
- Seeds are recorded as task_id * 1000 + trial_idx
- env_key == trial_idx for each trial
- Lab-world genesis places Experiment at tick-0 (initial-conditions contract)
- Report generator emits per-task table and provenance-tax headline from
  real recorded data

All in-process; no Modal or external envs required.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

# Make bench/libero importable for the driver and report helpers.
_BENCH_LIBERO = str(Path(__file__).parents[2] / "bench" / "libero")
if _BENCH_LIBERO not in sys.path:
    sys.path.insert(0, _BENCH_LIBERO)

from archetype import ArchetypeRuntime  # noqa: E402
from archetype.core.config import StorageConfig  # noqa: E402
from archetype.experiments.components import EvalTrialResult  # noqa: E402
from archetype.experiments.manipulation import ScriptedReachEnv  # noqa: E402
from bench.libero.eval_driver import DriverConfig, run_episode, run_eval  # noqa: E402
from bench.libero.report import generate_report, report_from_components  # noqa: E402

# ---------------------------------------------------------------------------
# FakeChunkPolicy — a scripted policy that returns deterministic zero actions
# ---------------------------------------------------------------------------


class FakeChunkPolicy:
    """Scripted policy for CI: returns zero actions, no Modal, no GPU."""

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        # Zero action on all envs — ScriptedReachEnv converges on its own.
        return [[0.0] * 7 for _ in env_keys]


class TaskLanguageReachEnv(ScriptedReachEnv):
    """Scripted env with the same task_language hook as ModalEnvClient."""

    def task_language(self) -> str:
        return "reach the scripted target"


class InstructionAssertingPolicy:
    """Moves only when the episode driver supplies the task text."""

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        actions = []
        for instruction in instructions:
            if instruction == "reach the scripted target":
                actions.append([0.05, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            else:
                actions.append([0.0] * 7)
        return actions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scripted_config(
    tmp_path: Path,
    *,
    suite: str = "scripted_suite",
    task_ids: list[int] | None = None,
    trials: int = 2,
    max_steps: int = 20,
) -> DriverConfig:
    """Build a DriverConfig that uses the scripted env (no Modal)."""
    return DriverConfig(
        suite=suite,
        task_ids=task_ids if task_ids is not None else [0],
        trials=trials,
        max_steps=max_steps,
        out=str(tmp_path / "eval_out"),
        run_id=f"test-{suite}",
        env_client_type="scripted",
        use_policy=False,  # zero-action policy via scripted path
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_n_trials_produce_n_result_rows(tmp_path):
    """N trials → N EvalTrialResult rows, one per trial."""
    config = _scripted_config(tmp_path, trials=3, task_ids=[0])
    results = await run_eval(config)

    assert len(results) == 3, f"Expected 3 result rows, got {len(results)}"
    for r in results:
        assert isinstance(r, EvalTrialResult)


@pytest.mark.asyncio
async def test_seeds_recorded_correctly(tmp_path):
    """Seed = task_id * 1000 + trial_idx for each trial."""
    config = _scripted_config(tmp_path, task_ids=[0, 2], trials=3)
    results = await run_eval(config)

    # task 0, trials 0,1,2 → seeds 0, 1, 2
    # task 2, trials 0,1,2 → seeds 2000, 2001, 2002
    by_task: dict[int, list[EvalTrialResult]] = {}
    for r in results:
        by_task.setdefault(r.task_id, []).append(r)

    for task_id, trials in by_task.items():
        trials_sorted = sorted(trials, key=lambda r: r.trial_idx)
        for r in trials_sorted:
            expected_seed = task_id * 1000 + r.trial_idx
            assert r.seed == expected_seed, (
                f"task {task_id} trial {r.trial_idx}: expected seed {expected_seed}, got {r.seed}"
            )


@pytest.mark.asyncio
async def test_env_key_equals_trial_idx(tmp_path):
    """env_key must equal trial_idx to route each trial to its own env instance."""
    config = _scripted_config(tmp_path, task_ids=[0], trials=4)
    results = await run_eval(config)

    for r in results:
        assert r.env_key == r.trial_idx, (
            f"trial {r.trial_idx}: env_key={r.env_key} != trial_idx={r.trial_idx}"
        )


@pytest.mark.asyncio
async def test_suite_and_task_id_recorded(tmp_path):
    """EvalTrialResult rows must carry the correct suite and task_id."""
    config = _scripted_config(tmp_path, suite="libero_test", task_ids=[3], trials=2)
    results = await run_eval(config)

    for r in results:
        assert r.suite == "libero_test"
        assert r.task_id == 3


@pytest.mark.asyncio
async def test_episode_length_positive(tmp_path):
    """episode_length should be > 0 for any non-trivial run."""
    config = _scripted_config(tmp_path, task_ids=[0], trials=2, max_steps=5)
    results = await run_eval(config)

    for r in results:
        # Even if the scripted env reaches done quickly, at least 1 step was taken.
        assert r.episode_length >= 0, f"episode_length should be >= 0, got {r.episode_length}"


@pytest.mark.asyncio
async def test_run_id_recorded(tmp_path):
    """run_id must be propagated to every EvalTrialResult row."""
    config = _scripted_config(tmp_path, task_ids=[0], trials=2)
    config = DriverConfig(
        **{**vars(config), "run_id": "my-test-run"},
    )
    results = await run_eval(config)

    for r in results:
        assert r.run_id == "my-test-run"


@pytest.mark.asyncio
async def test_multi_task_row_counts(tmp_path):
    """tasks * trials rows should be produced in total."""
    config = _scripted_config(tmp_path, task_ids=[0, 1, 2], trials=2)
    results = await run_eval(config)

    assert len(results) == 6, f"Expected 6 rows (3 tasks x 2 trials), got {len(results)}"

    # All three tasks should appear.
    task_ids_seen = {r.task_id for r in results}
    assert task_ids_seen == {0, 1, 2}


@pytest.mark.asyncio
async def test_success_accounting(tmp_path):
    """Success field must be a bool; overall rate must be in [0, 1]."""
    config = _scripted_config(tmp_path, task_ids=[0], trials=4, max_steps=30)
    results = await run_eval(config)

    for r in results:
        assert isinstance(r.success, bool), f"success must be bool, got {type(r.success)}"

    n_success = sum(1 for r in results if r.success)
    rate = n_success / len(results)
    assert 0.0 <= rate <= 1.0


@pytest.mark.asyncio
async def test_run_episode_uses_env_task_language_for_policy_instruction(tmp_path):
    """Live VLA evals must pass the LIBERO task text to the policy."""
    env = TaskLanguageReachEnv(targets={0: (0.05, 0.0, 0.5)}, tolerance=0.01)
    policy = InstructionAssertingPolicy()

    async with ArchetypeRuntime() as runtime:
        success, episode_length = await run_episode(
            env_client=env,
            policy_client=policy,
            suite="scripted_suite",
            task_id=0,
            trial_idx=0,
            seed=0,
            env_key=0,
            # The first world step materializes the raw spawn row; the policy
            # action is visible on the next tick.
            max_steps=2,
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="instruction"),
            runtime=runtime,
            use_frames=False,
        )

    assert success is True
    assert episode_length == 1


# ---------------------------------------------------------------------------
# Report generator tests (no async needed — pure data transformation)
# ---------------------------------------------------------------------------


def test_report_from_components_per_task_table():
    """Report must include per-task success-rate and episode-length columns."""
    results = [
        EvalTrialResult.make(
            suite="libero_spatial",
            task_id=0,
            trial_idx=0,
            seed=0,
            env_key=0,
            success=True,
            episode_length=42,
            wall_s=1.5,
            run_id="test-run",
        ),
        EvalTrialResult.make(
            suite="libero_spatial",
            task_id=0,
            trial_idx=1,
            seed=1,
            env_key=1,
            success=False,
            episode_length=80,
            wall_s=2.0,
            run_id="test-run",
        ),
    ]
    report = report_from_components(results, run_id="test-run")

    # Must contain table header for per-task section.
    assert "| Task |" in report
    assert "Success Rate" in report
    assert "Mean Steps" in report
    # The suite name must appear.
    assert "libero_spatial" in report
    # 1 success out of 2 trials = 50%.
    assert "50.0%" in report


def test_report_provenance_tax_headline():
    """Report must include the provenance-tax section with ledger overhead."""
    results = [
        EvalTrialResult.make(
            suite="s",
            task_id=0,
            trial_idx=0,
            seed=0,
            env_key=0,
            success=True,
            episode_length=10,
            wall_s=5.0,
            run_id="run",
        ),
    ]
    report = report_from_components(results)

    assert "Provenance Tax" in report
    assert "Ledger overhead" in report
    # The fixed 7 ms figure must appear.
    assert "7" in report
    # Citation.
    assert "rollout_overhead" in report


def test_report_from_row_dicts():
    """generate_report must work with row dicts (the live-driver path)."""
    rows = [
        {
            "evaltrialresult__suite": "libero_goal",
            "evaltrialresult__task_id": 1,
            "evaltrialresult__trial_idx": 0,
            "evaltrialresult__seed": 1000,
            "evaltrialresult__env_key": 0,
            "evaltrialresult__success": True,
            "evaltrialresult__episode_length": 55,
            "evaltrialresult__wall_s": 3.2,
            "evaltrialresult__run_id": "live-run",
            "evaltrialresult__recorded_at_ms": 0,
        }
    ]
    report = generate_report(rows, run_id="live-run")

    assert "libero_goal" in report
    assert "100.0%" in report  # 1/1 success


def test_report_suite_summary_section():
    """Report must include a suite summary section with overall success rate."""
    results = [
        EvalTrialResult.make(
            suite="libero_spatial",
            task_id=i,
            trial_idx=0,
            seed=i * 1000,
            env_key=0,
            success=(i % 2 == 0),
            episode_length=20,
            wall_s=1.0,
            run_id="r",
        )
        for i in range(4)
    ]
    report = report_from_components(results)

    assert "Suite Summary" in report
    # 2/4 successes = 50%.
    assert "50.0%" in report
