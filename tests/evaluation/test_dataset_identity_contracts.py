# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for evaluation-owned dataset evidence identity."""

from __future__ import annotations

import dataclasses

import pytest

from archetype.evaluation import (
    EpisodeRef,
    Eval,
    Grader,
    GraderKind,
    Rubric,
    RuntimeSlice,
    TaskRef,
    Trial,
)


def _task() -> TaskRef:
    return TaskRef(benchmark="libero", suite="libero_spatial", task_key="3")


def _rubric() -> Rubric:
    return Rubric(
        graders=(
            Grader(name="success_flag", kind=GraderKind.CHECK),
            Grader(name="goal_within_horizon", kind=GraderKind.TEST),
        )
    )


def test_eval_binds_one_task_to_a_non_empty_rubric() -> None:
    evaluation = Eval(task=_task(), rubric=_rubric())

    assert evaluation.task.key == ("libero", "libero_spatial", "3")
    assert [grader.kind for grader in evaluation.rubric.graders] == [
        GraderKind.CHECK,
        GraderKind.TEST,
    ]


def test_trial_keeps_dataset_identity_and_runtime_provenance_separate() -> None:
    runtime = RuntimeSlice(
        world_id="world-7",
        run_id="run-9",
        entity_id=12,
        start_tick=0,
        final_tick=41,
    )
    trial = Trial(
        task=_task(),
        seed=5,
        episode=EpisodeRef(benchmark="libero", episode_id=17),
        runtime=runtime,
    )

    assert trial.dataset_coordinates == ("libero", "libero_spatial", "3", 17)
    assert trial.runtime == runtime
    assert trial.episode.key == ("libero", 17)


def test_reader_trial_may_have_no_runtime_provenance() -> None:
    trial = Trial(
        task=_task(),
        seed=0,
        episode=EpisodeRef(benchmark="libero", episode_id=0),
    )

    assert trial.runtime is None


@pytest.mark.parametrize("episode_id", [-1, True, 1.5, "1"])
def test_episode_id_is_a_non_negative_integer(episode_id: object) -> None:
    error = ValueError if episode_id == -1 else TypeError
    with pytest.raises(error):
        EpisodeRef(benchmark="libero", episode_id=episode_id)  # type: ignore[arg-type]


def test_trial_rejects_cross_benchmark_episode() -> None:
    with pytest.raises(ValueError, match="same benchmark"):
        Trial(
            task=_task(),
            seed=0,
            episode=EpisodeRef(benchmark="droid", episode_id=0),
        )


def test_runtime_slice_rejects_reversed_tick_bounds() -> None:
    with pytest.raises(ValueError, match="final_tick"):
        RuntimeSlice(
            world_id="world-7",
            run_id="run-9",
            entity_id=12,
            start_tick=5,
            final_tick=4,
        )


def test_rubric_requires_unique_graders() -> None:
    grader = Grader(name="success", kind=GraderKind.CHECK)

    with pytest.raises(ValueError, match="unique"):
        Rubric(graders=(grader, grader))


def test_vocabulary_is_immutable_and_evaluation_owned() -> None:
    task = _task()

    assert TaskRef.__module__ == "archetype.evaluation.contracts"
    with pytest.raises(dataclasses.FrozenInstanceError):
        task.suite = "changed"  # type: ignore[misc]
