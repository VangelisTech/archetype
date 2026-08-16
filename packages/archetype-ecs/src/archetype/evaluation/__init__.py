# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation family: import-light values and snapshot-pinned receipt workflow.

This package owns ``EvalReceipt`` (persistent ECS schema), immutable
dataset/evidence identity, grader callbacks, pure grading, pinned views, and
the storage-only durable evaluation workflow.

A top-level path does not make a symbol public: the supported surface is
exactly the names re-exported here, which back the ``Outcome``,
``GraderContract``, and ``EvalReceipt`` root exports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from archetype.evaluation.contracts import (
    EpisodeRef,
    Eval,
    FrameGrader,
    Grader,
    GraderContract,
    GraderKind,
    Outcome,
    Rubric,
    RuntimeSlice,
    TaskRef,
    Trial,
)

if TYPE_CHECKING:
    from archetype.evaluation.components import EvalReceipt

__all__ = [
    "EpisodeRef",
    "Eval",
    "EvalReceipt",
    "FrameGrader",
    "Grader",
    "GraderContract",
    "GraderKind",
    "Outcome",
    "Rubric",
    "RuntimeSlice",
    "TaskRef",
    "Trial",
]


def __getattr__(name: str) -> Any:
    # The value contracts stay importable without the Arrow-backed component
    # stack: loading ``EvalReceipt`` here eagerly would drag
    # ``archetype.core.component`` (pyarrow, lancedb) into digest-only and
    # contract-only consumers of this package.
    if name == "EvalReceipt":
        from archetype.evaluation.components import EvalReceipt

        globals()[name] = EvalReceipt
        return EvalReceipt
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + __all__))
