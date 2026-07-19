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

from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import GraderContract, Outcome

__all__ = [
    "EvalReceipt",
    "GraderContract",
    "Outcome",
]
