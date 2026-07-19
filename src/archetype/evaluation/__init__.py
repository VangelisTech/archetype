# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation domain family: receipt schema and grading value contracts.

This package owns the reusable, deterministic evaluation definitions:
``EvalReceipt`` (persistent ECS schema) and the grading value contracts
(``Outcome``, ``GraderContract``, and the identity digest helpers in
``archetype.evaluation.contracts``). Grading orchestration, snapshot
pinning, storage, and receipt writes remain internal application authority
under ``archetype.app.evaluation``.

A top-level path does not make a symbol public: the supported surface is
exactly the names re-exported here, which back the ``Outcome``,
``GraderContract``, and ``EvalReceipt`` root exports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from archetype.evaluation.contracts import GraderContract, Outcome

if TYPE_CHECKING:
    from archetype.evaluation.components import EvalReceipt

__all__ = [
    "EvalReceipt",
    "GraderContract",
    "Outcome",
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
