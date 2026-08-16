# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation receipt schema (issue #275): the durable receipt row.

A receipt records that ONE grader ran under ONE pinned contract against ONE
pinned subject snapshot, and what it concluded. Receipts carry no authority:
no accepted/promote/approved/allowed_next_action fields, ever — a PASS means
one grader passed under one contract, nothing more. The layer above decides
what that means.

The identity digests a receipt stores are defined in
``archetype.evaluation.contracts``.
"""

from __future__ import annotations

from archetype.core.component import Component


class EvalReceipt(Component):
    """Persist the evidence produced by one evaluation.

    Receipts are historical evidence rather than active simulation entities.
    They record what a grader concluded under a specific contract; callers
    decide what that conclusion means for policy or promotion.
    """

    evaluation_id: str = ""
    subject_digest: str = ""
    contract_digest: str = ""
    grader_id: str = ""
    outcome: str = ""
    score: float | None = None
    graded_at_ms: int = 0
    evidence_json: str = "{}"
