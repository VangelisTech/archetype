# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Vocabulary invariants from docs/guide/dataset-eval-ontology.md.

The spec eval suite (evals/suites/spec_contracts.py) is the merge gate for
these laws; this module keeps them visible in the fast pytest loop too.
"""

from __future__ import annotations

import dataclasses

import pytest

from archetype.datasets import (
    EpisodeRef,
    Eval,
    Grader,
    GraderKind,
    Rubric,
    TaskRef,
    Trial,
)


def _mug_eval() -> Eval:
    task = TaskRef(benchmark="libero", suite="libero_spatial", task_key="3")
    rubric = Rubric(
        graders=(
            Grader(name="success_flag", kind=GraderKind.CHECK),
            Grader(name="goal_within_horizon", kind=GraderKind.TEST),
        )
    )
    return Eval(task=task, rubric=rubric)


def test_eval_binds_exactly_one_task():
    ev = _mug_eval()
    assert isinstance(ev.task, TaskRef)


def test_trial_produces_exactly_one_episode():
    ev = _mug_eval()
    trial = Trial(task=ev.task, seed=0, episode=EpisodeRef(benchmark="libero", episode_id=0))
    assert isinstance(trial.episode, EpisodeRef)
    assert trial.episode.episode_id == 0  # zero-based, dataset-scoped


def test_grader_kinds_are_exactly_check_test_judge():
    assert {kind.value for kind in GraderKind} == {"check", "test", "judge"}


def test_vocabulary_is_frozen():
    ev = _mug_eval()
    with pytest.raises(dataclasses.FrozenInstanceError):
        ev.task = TaskRef(benchmark="droid", suite="default", task_key="0")  # type: ignore[misc]
