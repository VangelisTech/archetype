# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for dataframe-based LIBERO eval composition."""

from __future__ import annotations

import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.experiments.components import EvalTrialResult
from bench.libero.eval_harness import (
    LiberoDataFrameEvalHarness,
    LiberoEvalTarget,
    build_default_libero_harness,
    trial_count_at_least,
)


def _trial_rows(run_id: str = "run-a") -> list[dict]:
    return [
        EvalTrialResult.make(
            suite="libero_spatial",
            task_id=0,
            trial_idx=0,
            seed=0,
            env_key=0,
            success=True,
            episode_length=12,
            wall_s=1.2,
            run_id=run_id,
        ).to_row_dict(),
        EvalTrialResult.make(
            suite="libero_spatial",
            task_id=0,
            trial_idx=1,
            seed=1,
            env_key=1,
            success=False,
            episode_length=18,
            wall_s=1.8,
            run_id=run_id,
        ).to_row_dict(),
        EvalTrialResult.make(
            suite="libero_object",
            task_id=2,
            trial_idx=0,
            seed=2000,
            env_key=0,
            success=True,
            episode_length=20,
            wall_s=2.0,
            run_id="run-b",
        ).to_row_dict(),
    ]


@pytest.mark.asyncio
async def test_default_libero_dataframe_harness_grades_selected_rows():
    harness = build_default_libero_harness(
        min_trials=2,
        min_success_rate=0.5,
        max_mean_episode_length=20,
    )

    results = await harness.run(
        LiberoEvalTarget(
            rows=_trial_rows(),
            suites=["libero_spatial"],
            task_ids=[0],
            run_labels=["run-a"],
        )
    )

    assert [result.task_id for result in results] == [
        "libero_trial_success",
        "libero_trial_routing",
        "libero_episode_length",
    ]
    assert all(result.all_passed for result in results)


@pytest.mark.asyncio
async def test_libero_dataframe_harness_task_target_overrides_sampling():
    harness = LiberoDataFrameEvalHarness()
    harness.add(
        "object-run-count",
        graders=[trial_count_at_least(1)],
        target=LiberoEvalTarget(suites=["libero_object"], task_ids=[2], run_labels=["run-b"]),
    )

    results = await harness.run(LiberoEvalTarget(rows=_trial_rows(), suites=["libero_spatial"]))

    assert len(results) == 1
    assert results[0].all_passed is True
    assert results[0].trials[0].grader_results[0].grader_name == "trial_count"


@pytest.mark.asyncio
async def test_libero_dataframe_harness_queries_trials_with_eval_service(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="libero_eval")
        world = await container.world_service.create_world(WorldConfig(name="libero-lab"), storage)
        for row in _trial_rows("ledger-run")[:2]:
            await world.create_entity(
                [
                    EvalTrialResult(
                        suite=row["evaltrialresult__suite"],
                        task_id=row["evaltrialresult__task_id"],
                        trial_idx=row["evaltrialresult__trial_idx"],
                        seed=row["evaltrialresult__seed"],
                        env_key=row["evaltrialresult__env_key"],
                        success=row["evaltrialresult__success"],
                        episode_length=row["evaltrialresult__episode_length"],
                        wall_s=row["evaltrialresult__wall_s"],
                        run_id=row["evaltrialresult__run_id"],
                        recorded_at_ms=row["evaltrialresult__recorded_at_ms"],
                    )
                ]
            )

        run = await container.simulation_service.run(world.world_id, RunConfig(num_steps=1))
        harness = build_default_libero_harness(min_trials=2, min_success_rate=0.5)
        harness.eval_service = container.eval_service

        results = await harness.run(
            LiberoEvalTarget(
                world_id=world.world_id,
                run_id=run.run_id,
                storage_config=storage,
                suites=["libero_spatial"],
                run_labels=["ledger-run"],
            )
        )

        assert len(results) == 2
        assert all(result.all_passed for result in results)
    finally:
        await container.shutdown()
