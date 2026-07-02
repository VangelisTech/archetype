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

"""Vocabulary for the physical-AI dataset & eval domain.

Normative source: ``docs/guide/dataset-eval-ontology.md``. This module is the
executable mirror of that page's Definitions section: identity and cardinality
only. Full row schemas (episode contents, sampling metadata) land with the
dataset readers.

The ``spec`` eval suite asserts this module's shape against the ontology; the
suite runs inside the required ``evals`` status check, so changes here that
break the ontology's laws cannot merge.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class GraderKind(StrEnum):
    """The three kinds of grader a rubric may compose."""

    CHECK = "check"
    """Mechanical lint: a deterministic predicate over stored state."""

    TEST = "test"
    """Deterministic behavioral assertion over episode dynamics."""

    JUDGE = "judge"
    """Model-graded, qualitative scoring. The least reliable kind."""


@dataclass(frozen=True)
class TaskRef:
    """Identity of a task: a label + instruction inside a benchmark suite."""

    benchmark: str
    suite: str
    task_key: str


@dataclass(frozen=True)
class EpisodeRef:
    """Identity of an episode: dataset coordinates, never runtime ones.

    ``episode_id`` is a zero-based, dataset-scoped integer. Bare
    ``episode_id`` is not globally unique; the composite key is.
    """

    benchmark: str
    episode_id: int


@dataclass(frozen=True)
class Grader:
    """One scorer of an episode against a task."""

    name: str
    kind: GraderKind


@dataclass(frozen=True)
class Rubric:
    """The skeleton of graders that composes an eval."""

    graders: tuple[Grader, ...]


@dataclass(frozen=True)
class Eval:
    """The grading of exactly one task: the task plus its rubric."""

    task: TaskRef
    rubric: Rubric


@dataclass(frozen=True)
class Trial:
    """One seeded assignment of a task to an eval's execution.

    Running a trial produces exactly one episode: the trial is the act, the
    episode is the evidence. Distinct from the meta-eval harness's internal
    ``evals.types.TrialResult``.
    """

    task: TaskRef
    seed: int
    episode: EpisodeRef
