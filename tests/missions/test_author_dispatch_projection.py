# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Committed repository-lineage contracts for Mission author dispatches."""

from __future__ import annotations

from typing import Any

import daft
import pytest

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.graph import Relation
from archetype.missions.components import (
    Candidate,
    Task,
    TaskCriticPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
)
from archetype.missions.contracts import CriticPolicy
from archetype.missions.projections import project_task_dispatch_requests
from archetype.missions.relations import DependsOn, Guards, PartOfMission
from archetype.missions.transitions import TaskStatus


def _frame(entity_id: int, tick: int, *components: Component | Relation):
    values: dict[str, list[Any]] = {"entity_id": [entity_id], "tick": [tick]}
    for component in components:
        prefix = type(component).get_prefix()
        for field, value in component.model_dump().items():
            values[f"{prefix}{field}"] = [value]
    return daft.from_pydict(values)


def _critic_policy() -> TaskCriticPolicy:
    policy = CriticPolicy()
    return TaskCriticPolicy(
        policy_id=policy.policy_id,
        version=policy.version,
        digest=policy.digest,
        perspective=policy.perspective,
        information_view=policy.information_view,
        driver=policy.driver,
        model=policy.model,
        sampling=policy.sampling,
        max_reviews=policy.max_reviews,
        timeout_seconds=policy.timeout_seconds,
        output_schema_version=policy.output_schema_version,
        max_output_chars=policy.max_output_chars,
    )


def _task(
    *,
    name: str,
    status: TaskStatus,
    dispatch_id: str,
) -> tuple[Component, ...]:
    return (
        Task(name=name, prompt=f"Implement {name}."),
        TaskWorkspace(
            repository="owner/repository",
            branch="agent/mission",
            base_ref="main",
        ),
        TaskPolicy(),
        _critic_policy(),
        TaskState(status=status.value),
        TaskDispatch(dispatch_id=dispatch_id, sequence=1),
    )


def _event(*, dependent: bool) -> PostTick:
    tick = 7
    mission_id = 1
    first_task_id = 2
    second_task_id = 3
    validator_id = 4
    first = _task(
        name="first",
        status=TaskStatus.ACCEPTED,
        dispatch_id="dispatch-first",
    )
    second = _task(
        name="second",
        status=TaskStatus.DISPATCHED,
        dispatch_id="dispatch-second",
    )
    task_frame = _frame(first_task_id, tick, *first).concat(_frame(second_task_id, tick, *second))
    membership = _frame(
        10,
        tick,
        PartOfMission(source=first_task_id, target=mission_id),
    ).concat(
        _frame(
            11,
            tick,
            PartOfMission(source=second_task_id, target=mission_id),
        )
    )
    validator = TaskValidator(name="focused", command=["true"])
    results = {
        tuple(type(item) for item in first): task_frame,
        (PartOfMission,): membership,
        (TaskValidator,): _frame(validator_id, tick, validator),
        (Guards,): _frame(
            12,
            tick,
            Guards(source=validator_id, target=second_task_id),
        ),
        (Candidate,): _frame(
            14,
            tick,
            Candidate(
                candidate_id="a" * 64,
                mission_id=mission_id,
                task_id=first_task_id,
                dispatch_id="dispatch-first",
                dispatch_sequence=1,
                author_execution_id=20,
                author_sandbox_id="sandbox-first",
                repository="owner/repository",
                branch="agent/mission",
                base_ref="main",
                base_revision="1" * 40,
                head_revision="2" * 40,
                diff_digest="3" * 64,
                validator_bundle_digest="4" * 64,
                policy_digest=CriticPolicy().digest,
                candidate_digest="5" * 64,
                created_at_ms=100,
            ),
        ),
    }
    if dependent:
        results[(DependsOn,)] = _frame(
            13,
            tick,
            DependsOn(source=second_task_id, target=first_task_id),
        )
    return PostTick(world_id="world", tick=tick + 1, results=results)


def _fan_in_event() -> PostTick:
    tick = 9
    mission_id = 1
    first_task_id, second_task_id, fan_in_task_id = 2, 3, 4
    validator_id = 5
    task_frame = _frame(
        first_task_id,
        tick,
        *_task(
            name="first-root",
            status=TaskStatus.ACCEPTED,
            dispatch_id="dispatch-first",
        ),
    )
    task_frame = task_frame.concat(
        _frame(
            second_task_id,
            tick,
            *_task(
                name="second-root",
                status=TaskStatus.ACCEPTED,
                dispatch_id="dispatch-second",
            ),
        )
    )
    task_frame = task_frame.concat(
        _frame(
            fan_in_task_id,
            tick,
            *_task(
                name="fan-in",
                status=TaskStatus.DISPATCHED,
                dispatch_id="dispatch-fan-in",
            ),
        )
    )
    memberships = _frame(
        10,
        tick,
        PartOfMission(source=first_task_id, target=mission_id),
    )
    memberships = memberships.concat(
        _frame(11, tick, PartOfMission(source=second_task_id, target=mission_id))
    )
    memberships = memberships.concat(
        _frame(12, tick, PartOfMission(source=fan_in_task_id, target=mission_id))
    )
    dependencies = _frame(
        20,
        tick,
        DependsOn(source=fan_in_task_id, target=first_task_id),
    ).concat(
        _frame(
            21,
            tick,
            DependsOn(source=fan_in_task_id, target=second_task_id),
        )
    )
    policy = CriticPolicy()

    def candidate(
        *,
        task_id: int,
        dispatch_id: str,
        base: str,
        head: str,
        created_at_ms: int,
    ) -> Candidate:
        return Candidate(
            candidate_id=head[0] * 64,
            mission_id=mission_id,
            task_id=task_id,
            dispatch_id=dispatch_id,
            dispatch_sequence=1,
            author_execution_id=30 + task_id,
            author_sandbox_id=f"sandbox-{task_id}",
            repository="owner/repository",
            branch="agent/mission",
            base_ref="main",
            base_revision=base,
            head_revision=head,
            diff_digest=head[0] * 64,
            validator_bundle_digest="4" * 64,
            policy_digest=policy.digest,
            candidate_digest="5" * 64,
            created_at_ms=created_at_ms,
        )

    candidates = _frame(
        30,
        tick,
        candidate(
            task_id=first_task_id,
            dispatch_id="dispatch-first",
            base="1" * 40,
            head="2" * 40,
            created_at_ms=100,
        ),
    ).concat(
        _frame(
            31,
            tick,
            candidate(
                task_id=second_task_id,
                dispatch_id="dispatch-second",
                base="2" * 40,
                head="6" * 40,
                created_at_ms=200,
            ),
        )
    )
    validator = TaskValidator(name="focused", command=["true"])
    return PostTick(
        world_id="world",
        tick=tick + 1,
        results={
            tuple(
                type(item)
                for item in _task(
                    name="signature",
                    status=TaskStatus.ACCEPTED,
                    dispatch_id="signature",
                )
            ): task_frame,
            (PartOfMission,): memberships,
            (DependsOn,): dependencies,
            (TaskValidator,): _frame(validator_id, tick, validator),
            (Guards,): _frame(
                22,
                tick,
                Guards(source=validator_id, target=fan_in_task_id),
            ),
            (Candidate,): candidates,
        },
    )


@pytest.mark.asyncio
async def test_dispatch_projects_latest_mission_head_for_independent_and_dependent_tasks() -> None:
    independent_root = await project_task_dispatch_requests(_event(dependent=False))
    dependent_task = await project_task_dispatch_requests(_event(dependent=True))

    assert len(independent_root) == len(dependent_task) == 1
    assert independent_root[0].checkout_revision == "2" * 40
    assert dependent_task[0].checkout_revision == "2" * 40
    assert dependent_task[0].base_ref == "main"


@pytest.mark.asyncio
async def test_fan_in_dispatch_uses_latest_serialized_mission_head() -> None:
    requests = await project_task_dispatch_requests(_fan_in_event())

    assert len(requests) == 1
    assert requests[0].task_name == "fan-in"
    assert requests[0].checkout_revision == "6" * 40


@pytest.mark.asyncio
async def test_mission_head_selection_ignores_accepted_tasks_in_other_missions() -> None:
    event = _event(dependent=False)
    task_signature = next(signature for signature in event.results if Task in signature)
    event.results[task_signature] = event.results[task_signature].concat(
        _frame(
            90,
            7,
            *_task(
                name="other-mission",
                status=TaskStatus.ACCEPTED,
                dispatch_id="dispatch-other",
            ),
        )
    )
    event.results[(PartOfMission,)] = event.results[(PartOfMission,)].concat(
        _frame(91, 7, PartOfMission(source=90, target=89))
    )
    event.results[(PartOfMission,)] = event.results[(PartOfMission,)].concat(
        _frame(92, 7, PartOfMission(source=93, target=1))
    )

    requests = await project_task_dispatch_requests(event)

    assert len(requests) == 1
    assert requests[0].checkout_revision == "2" * 40
