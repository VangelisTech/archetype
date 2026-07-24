# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation value contracts: evidence identity, grading, and receipts.

The dataset vocabulary describes immutable evidence identity and cardinality.
It is not an ECS schema or a trial-execution state machine. Dataset coordinates
and runtime provenance remain distinct and travel side by side when both exist.

Identity is three digests:

- subject: the immutable snapshot reference (manifest head + tokens) plus
  the canonical selector. Never row-content hashing — materializing a
  10k-entity x 600-tick trajectory to hash it would break the lazy contract.
- contract: the versioned grader descriptor. Bare callables get no digest;
  persisted receipts REQUIRE a contract.
- evaluation_id: caller-supplied trial identity, deliberately DISTINCT from
  subject+contract — repeated trials of nondeterministic graders are a
  feature, not a replay bug.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic_core import to_jsonable_python

if TYPE_CHECKING:
    from daft import DataFrame
else:
    # Callback contracts stay importable without loading the Daft/Arrow stack.
    DataFrame = Any

_RECEIPT_DIGEST_DOMAIN = "archetype.receipt.v1"

OUTCOME_STATUSES = frozenset({"pass", "fail", "invalid", "inconclusive"})

# PR-5 repoints application consumers to these family-owned callback values and
# deletes the narrow compatibility exports in app.evaluation.interfaces.
GraderOutput = object
GraderReturn = GraderOutput | Sequence[GraderOutput]
TrajectoryGrader = Callable[[DataFrame], GraderReturn | Awaitable[GraderReturn]]


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")


def _require_non_negative_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


class GraderKind(StrEnum):
    """Supported grader semantics, ordered from mechanical to qualitative."""

    CHECK = "check"
    TEST = "test"
    JUDGE = "judge"


@dataclass(frozen=True, slots=True)
class TaskRef:
    """Natural identity of one task inside a benchmark suite."""

    benchmark: str
    suite: str
    task_key: str

    def __post_init__(self) -> None:
        _require_text(self.benchmark, "benchmark")
        _require_text(self.suite, "suite")
        _require_text(self.task_key, "task_key")

    @property
    def key(self) -> tuple[str, str, str]:
        """Return the canonical task natural key."""

        return self.benchmark, self.suite, self.task_key


@dataclass(frozen=True, slots=True)
class EpisodeRef:
    """Natural identity of one frozen dataset episode.

    ``episode_id`` is a zero-based integer allocated by the dataset curator or
    exporter. It is not ``EpisodeResult.episode_id``, which is a runtime UUID.
    """

    benchmark: str
    episode_id: int

    def __post_init__(self) -> None:
        _require_text(self.benchmark, "benchmark")
        _require_non_negative_int(self.episode_id, "episode_id")

    @property
    def key(self) -> tuple[str, int]:
        """Return the canonical episode natural key."""

        return self.benchmark, self.episode_id


@dataclass(frozen=True, slots=True)
class RuntimeSlice:
    """Optional provenance locating one trial in an Archetype ledger.

    A runtime episode can batch several trial entities. The entity and tick
    bounds therefore belong in the provenance; ``world_id`` and ``run_id``
    alone do not identify one trial.
    """

    world_id: str
    run_id: str
    entity_id: int
    start_tick: int
    final_tick: int

    def __post_init__(self) -> None:
        _require_text(self.world_id, "world_id")
        _require_text(self.run_id, "run_id")
        _require_non_negative_int(self.entity_id, "entity_id")
        _require_non_negative_int(self.start_tick, "start_tick")
        _require_non_negative_int(self.final_tick, "final_tick")
        if self.final_tick < self.start_tick:
            raise ValueError("final_tick must be greater than or equal to start_tick")


@dataclass(frozen=True, slots=True)
class Grader:
    """One named scorer used by a rubric."""

    name: str
    kind: GraderKind

    def __post_init__(self) -> None:
        _require_text(self.name, "grader name")
        if not isinstance(self.kind, GraderKind):
            raise TypeError("grader kind must be a GraderKind")


@dataclass(frozen=True, slots=True)
class Rubric:
    """A non-empty, named-by-member composition of graders."""

    graders: tuple[Grader, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.graders, tuple):
            raise TypeError("rubric graders must be a tuple")
        if not self.graders:
            raise ValueError("a rubric must contain at least one grader")
        if not all(isinstance(grader, Grader) for grader in self.graders):
            raise TypeError("rubric graders must all be Grader instances")
        names = [grader.name for grader in self.graders]
        if len(names) != len(set(names)):
            raise ValueError("grader names must be unique within a rubric")


@dataclass(frozen=True, slots=True)
class Eval:
    """The rubric bound to exactly one task."""

    task: TaskRef
    rubric: Rubric

    def __post_init__(self) -> None:
        if not isinstance(self.task, TaskRef):
            raise TypeError("eval task must be a TaskRef")
        if not isinstance(self.rubric, Rubric):
            raise TypeError("eval rubric must be a Rubric")


@dataclass(frozen=True, slots=True)
class Trial:
    """A recorded seeded execution that produced one dataset episode.

    This is the immutable evidence-side record. Pending/submitted/running
    lifecycle state belongs to application orchestration.
    """

    task: TaskRef
    seed: int
    episode: EpisodeRef
    runtime: RuntimeSlice | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task, TaskRef):
            raise TypeError("trial task must be a TaskRef")
        _require_non_negative_int(self.seed, "seed")
        if not isinstance(self.episode, EpisodeRef):
            raise TypeError("trial episode must be an EpisodeRef")
        if self.runtime is not None and not isinstance(self.runtime, RuntimeSlice):
            raise TypeError("trial runtime must be a RuntimeSlice or None")
        if self.task.benchmark != self.episode.benchmark:
            raise ValueError("trial task and episode must belong to the same benchmark")

    @property
    def dataset_coordinates(self) -> tuple[str, str, str, int]:
        """Return the complete dataset coordinate tuple for this trial."""

        return (*self.task.key, self.episode.episode_id)


@dataclass(frozen=True)
class Outcome:
    """Represent a validated grading conclusion.

    `status` must be `pass`, `fail`, `invalid`, or `inconclusive`. A supplied
    score must be finite.
    """

    status: str
    score: float | None = None
    evidence: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in OUTCOME_STATUSES:
            raise ValueError(
                f"outcome status {self.status!r} is not one of {sorted(OUTCOME_STATUSES)}"
            )
        if self.score is not None and not math.isfinite(self.score):
            raise ValueError("outcome score must be finite when present")


@dataclass(frozen=True)
class GraderContract:
    """Identify the grader configuration used for a durable receipt.

    Two receipts are directly comparable only when their contract digests
    match. Change `implementation_version`, configuration, thresholds, or
    seed whenever that comparison should no longer be valid.
    """

    grader_id: str
    implementation_version: str
    config: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=dict)
    seed: int | None = None

    def __post_init__(self) -> None:
        if not self.grader_id.strip():
            raise ValueError("grader_id must be a non-empty stable identity")
        if not self.implementation_version.strip():
            raise ValueError(
                "implementation_version must name the grader implementation "
                "(code version, prompt hash, or model id)"
            )

    def digest(self) -> str:
        payload = json.dumps(
            {
                "domain": _RECEIPT_DIGEST_DOMAIN,
                "kind": "grader-contract",
                "grader_id": self.grader_id,
                "implementation_version": self.implementation_version,
                "config": to_jsonable_python(self.config),
                "thresholds": to_jsonable_python(self.thresholds),
                "seed": self.seed,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def subject_digest(
    world_id: str,
    run_id: str,
    *,
    snapshot_tick: int,
    snapshot_tokens: list[str],
    component_names: list[str],
    ticks: list[int] | None = None,
    entity_ids: list[int] | None = None,
) -> str:
    """The pinned-subject identity: snapshot reference + canonical selector.

    The snapshot reference is the manifest head (tick + its commit tokens)
    from the control catalog — immutable by the atomic-visibility contract.
    The selector is what was asked of that snapshot. Together they make a
    receipt recomputable and attributable without hashing row content.
    """
    payload = json.dumps(
        {
            "domain": _RECEIPT_DIGEST_DOMAIN,
            "kind": "subject",
            "world_id": str(world_id),
            "run_id": str(run_id),
            "snapshot": {"tick": snapshot_tick, "tokens": sorted(snapshot_tokens)},
            "selector": {
                "components": sorted(component_names),
                "ticks": sorted(int(t) for t in ticks) if ticks is not None else None,
                "entity_ids": sorted(int(e) for e in entity_ids)
                if entity_ids is not None
                else None,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evaluation_identity_digest(subject: str, contract: str) -> str:
    """The claim's payload digest: what this evaluation is OF.

    Deliberately excludes the graded outcome — two trials of a
    nondeterministic grader share subject+contract while concluding
    differently. Same evaluation_id + different identity digest is a loud
    conflict; same identity under a new evaluation_id is a fresh trial.
    """
    payload = json.dumps(
        {
            "domain": _RECEIPT_DIGEST_DOMAIN,
            "kind": "evaluation-identity",
            "subject": subject,
            "contract": contract,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
