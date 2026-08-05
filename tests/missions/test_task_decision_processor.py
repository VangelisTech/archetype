# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Review-budget exhaustion leaves the candidate pending and is reported.

Normative contract (docs/guide/agent-missions.md, runtime.md,
service-protocols.md): reviewer infrastructure failure consumes only the
bounded review budget — it never becomes a task failure or an implicit
approval. The decision processor leaves the candidate in ``CANDIDATE``; the
run boundary projects the exhaustion and raises
``CriticReviewBudgetExhaustedError`` instead of waiting out its tick budget.
"""

from __future__ import annotations

from typing import Any

import daft
import pytest

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.core.resources import Resources
from archetype.graph import GraphView
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    CriticExecution,
    CriticReceipt,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
)
from archetype.missions.processors import TaskDecisionProcessor
from archetype.missions.projections import (
    CriticReviewBudgetExhausted,
    CriticReviewBudgetExhaustedError,
    project_pending_review_exhaustion,
)
from archetype.missions.relations import Guards
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticConclusion,
    CriticExecutionStatus,
    TaskStatus,
)

_MISSION_ID = 1
_TASK_ID = 11
_CANDIDATE_ENTITY_ID = 31


def _signature_frame(entities: list[tuple[int, tuple[Component, ...]]]) -> daft.DataFrame:
    columns: dict[str, list[Any]] = {}
    for entity_id, components in entities:
        row: dict[str, Any] = {"entity_id": entity_id, "tick": 1, "is_active": True}
        for component in components:
            prefix = type(component).get_prefix()
            for field, value in component.model_dump().items():
                row[f"{prefix}{field}"] = value
        for key, value in row.items():
            columns.setdefault(key, []).append(value)
    return daft.from_pydict(columns)


def _candidate() -> Candidate:
    return Candidate(
        candidate_id="candidate-1",
        mission_id=_MISSION_ID,
        task_id=_TASK_ID,
        dispatch_id="dispatch-1",
        dispatch_sequence=1,
        author_execution_id=21,
        author_sandbox_id="sandbox-author",
        repository="https://example.invalid/repo.git",
        branch="feature",
        base_ref="main",
        base_revision="base-sha",
        head_revision="head-sha",
        diff_digest="diff-digest",
        validator_bundle_digest="validator-digest",
        policy_digest="policy-digest",
        candidate_digest="candidate-digest",
        created_at_ms=1,
    )


def _critic_policy(max_reviews: int) -> TaskCriticPolicy:
    return TaskCriticPolicy(
        policy_id="critic-policy",
        version="1",
        digest="policy-digest",
        perspective="independent-reviewer",
        information_view="task-diff-validators",
        driver="codex",
        model="model-a",
        sampling="provider-default",
        max_reviews=max_reviews,
    )


def _critic_execution(attempt: int) -> CriticExecution:
    return CriticExecution(
        candidate_entity_id=_CANDIDATE_ENTITY_ID,
        candidate_id="candidate-1",
        review_id=f"review-{attempt}",
        attempt=attempt,
        status=CriticExecutionStatus.ERRORED.value,
        sandbox_id=f"sandbox-critic-{attempt}",
        error="critic run failed before any receipt",
    )


def _receipt() -> CriticReceipt:
    return CriticReceipt(
        candidate_entity_id=_CANDIDATE_ENTITY_ID,
        critic_execution_id=41,
        critic_sandbox_id="sandbox-critic-1",
        review_id="review-1",
        conclusion=CriticConclusion.APPROVED.value,
        candidate_digest="candidate-digest",
        policy_digest="policy-digest",
        evidence_digest="evidence-digest",
        reviewed_base_revision="base-sha",
        reviewed_head_revision="head-sha",
        reviewed_diff_digest="diff-digest",
        validator_bundle_digest="validator-digest",
    )


def _candidate_task_components(max_reviews: int) -> tuple[Component, ...]:
    return (
        Task(name="task", prompt="do the work"),
        TaskState(status=TaskStatus.CANDIDATE.value),
        TaskDispatch(dispatch_id="dispatch-1", sequence=1),
        TaskPolicy(),
        _critic_policy(max_reviews),
    )


def _candidate_task_frame(max_reviews: int) -> daft.DataFrame:
    return _signature_frame([(_TASK_ID, _candidate_task_components(max_reviews))])


def _view(
    *,
    max_reviews: int,
    attempts: int,
    receipts: tuple[CriticReceipt, ...] = (),
) -> GraphView:
    results: dict[tuple[type[Component], ...], daft.DataFrame] = {
        # Unrelated task keeps the author-evidence frames non-empty without
        # matching this task's dispatch join keys.
        (AgentExecution,): _signature_frame(
            [
                (
                    91,
                    (
                        AgentExecution(
                            task_id=999,
                            dispatch_id="other-dispatch",
                            dispatch_sequence=1,
                            status=AgentExecutionStatus.EXITED.value,
                            sandbox_id="sandbox-other",
                        ),
                    ),
                )
            ]
        ),
        (Guards,): _signature_frame([(92, (Guards(source=93, target=999),))]),
        (Task, TaskState, TaskDispatch, TaskPolicy, TaskCriticPolicy): _signature_frame(
            [(_TASK_ID, _candidate_task_components(max_reviews))]
        ),
        (Candidate,): _signature_frame([(_CANDIDATE_ENTITY_ID, (_candidate(),))]),
    }
    if attempts:
        results[(CriticExecution,)] = _signature_frame(
            [(40 + attempt, (_critic_execution(attempt),)) for attempt in range(1, attempts + 1)]
        )
    if receipts:
        results[(CriticReceipt,)] = _signature_frame(
            [(80 + index, (receipt,)) for index, receipt in enumerate(receipts)]
        )
    view = GraphView()
    view.on_post_tick_sync(PostTick(world_id="mission-world", tick=2, results=results))
    return view


def _resources(view: GraphView) -> Resources:
    resources = Resources()
    resources.insert(view)
    return resources


async def _decide(
    *,
    max_reviews: int,
    attempts: int,
    receipts: tuple[CriticReceipt, ...] = (),
) -> dict[str, Any]:
    processor = TaskDecisionProcessor()
    result = await processor.process(
        _candidate_task_frame(max_reviews),
        resources=_resources(_view(max_reviews=max_reviews, attempts=attempts, receipts=receipts)),
    )
    rows = result.to_pylist()
    assert len(rows) == 1
    return rows[0]


@pytest.mark.asyncio
async def test_exhausted_review_budget_leaves_candidate_pending() -> None:
    """Reviewer failure never becomes a task decision; the candidate waits."""

    row = await _decide(max_reviews=2, attempts=2)

    state = TaskState.get_prefix()
    assert row[f"{state}status"] == TaskStatus.CANDIDATE.value
    assert row[f"{state}reason"] == ""


@pytest.mark.asyncio
async def test_candidate_within_review_budget_keeps_waiting() -> None:
    row = await _decide(max_reviews=2, attempts=1)

    state = TaskState.get_prefix()
    assert row[f"{state}status"] == TaskStatus.CANDIDATE.value
    assert row[f"{state}reason"] == ""


@pytest.mark.asyncio
async def test_matching_receipt_decides_even_at_the_review_budget() -> None:
    row = await _decide(max_reviews=2, attempts=2, receipts=(_receipt(),))

    state = TaskState.get_prefix()
    assert row[f"{state}status"] == TaskStatus.ACCEPTED.value


def test_exhausted_review_budget_is_projected_for_the_run_boundary() -> None:
    """The whole budget spent with no receipt is reported, not a hang."""

    pending = project_pending_review_exhaustion(
        _view(max_reviews=2, attempts=2),
        _MISSION_ID,
    )

    assert pending == (
        CriticReviewBudgetExhausted(
            mission_id=_MISSION_ID,
            task_id=_TASK_ID,
            candidate_id="candidate-1",
            attempts=2,
            max_reviews=2,
        ),
    )
    error = CriticReviewBudgetExhaustedError(_MISSION_ID, pending)
    assert "pending independent review" in str(error)


def test_candidate_within_budget_is_not_projected_as_exhausted() -> None:
    assert project_pending_review_exhaustion(_view(max_reviews=2, attempts=1), _MISSION_ID) == ()


def test_receipt_bearing_candidate_is_not_projected_as_exhausted() -> None:
    view = _view(max_reviews=2, attempts=2, receipts=(_receipt(),))
    assert project_pending_review_exhaustion(view, _MISSION_ID) == ()


def test_other_missions_exhaustion_is_not_reported() -> None:
    assert project_pending_review_exhaustion(_view(max_reviews=2, attempts=2), 999) == ()
