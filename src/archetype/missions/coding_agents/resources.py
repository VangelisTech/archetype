# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World resources used by the built-in coding-agent mission feature."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.hooks import PostTick
from archetype.missions.coding_agents.components import (
    AgentTaskAttempt,
    AgentTaskEvidence,
    AgentTaskRecord,
    AgentTaskState,
    AgentTaskValidators,
    AgentTaskWorkspace,
)
from archetype.missions.coding_agents.transitions import (
    AgentAttemptStatus,
    AgentTaskStatus,
)
from archetype.missions.contracts import AgentMissionSandbox, TaskExecutionRequest
from archetype.missions.relationships import PartOfMission


@dataclass
class AgentMissionSandboxResource:
    """Inject the provider-facing sandbox service into one mission world."""

    sandbox: AgentMissionSandbox


def _live_frame(event: PostTick, *components: type[Component]) -> DataFrame | None:
    columns = ["entity_id", "tick"]
    for component in components:
        prefix = component.get_prefix()
        columns.extend(f"{prefix}{field}" for field in component.model_fields)

    matches: list[DataFrame] = []
    for signature, frame in event.results.items():
        if all(component in signature for component in components):
            selected = frame
            if "is_active" in selected.column_names:
                selected = selected.where(col("is_active"))
            matches.append(selected.select(*columns))
    if not matches:
        return None
    result = matches[0]
    for match in matches[1:]:
        result = result.concat(match)
    return result


class TaskExecutionOutbox:
    """Project newly committed DISPATCHED task rows into execution requests.

    The hook runs after persistence, so no sandbox side effect can escape a
    failed tick. Materialization is intentionally limited to the committed
    execution intents and task-to-mission edges crossing into the provider.
    """

    _TASK_COMPONENTS = (
        AgentTaskRecord,
        AgentTaskWorkspace,
        AgentTaskValidators,
        AgentTaskState,
        AgentTaskAttempt,
        AgentTaskEvidence,
    )

    def __init__(self) -> None:
        self._queued: list[TaskExecutionRequest] = []
        self._seen_attempts: set[str] = set()

    async def on_post_tick(self, event: PostTick) -> None:
        task_frame = _live_frame(event, *self._TASK_COMPONENTS)
        relation_frame = _live_frame(event, PartOfMission)
        if task_frame is None or relation_frame is None:
            return

        state = AgentTaskState.get_prefix()
        attempt = AgentTaskAttempt.get_prefix()
        task_dispatched = cast(
            Expression,
            col(f"{state}status") == AgentTaskStatus.DISPATCHED.value,
        )
        attempt_pending = cast(
            Expression,
            col(f"{attempt}status") == AgentAttemptStatus.PENDING.value,
        )
        dispatched = task_frame.where(task_dispatched & attempt_pending & ~col(f"{attempt}settled"))
        relation = PartOfMission.get_prefix()
        dispatched = dispatched.join(
            relation_frame.select(f"{relation}source", f"{relation}target"),
            left_on="entity_id",
            right_on=f"{relation}source",
        )
        rows = dispatched.to_pylist()
        if not rows:
            return
        record = AgentTaskRecord.get_prefix()
        workspace = AgentTaskWorkspace.get_prefix()
        validators = AgentTaskValidators.get_prefix()
        evidence = AgentTaskEvidence.get_prefix()
        for row in rows:
            attempt_id = str(row[f"{attempt}attempt_id"])
            if attempt_id in self._seen_attempts:
                continue
            task_id = int(row["entity_id"])
            mission_id = int(row[f"{relation}target"])
            validator_set = AgentTaskValidators(
                specs_json=str(row[f"{validators}specs_json"])
            ).specs()
            prior_evidence = AgentTaskEvidence(
                validator_results_json=str(row[f"{evidence}validator_results_json"]),
                artifacts_json=str(row[f"{evidence}artifacts_json"]),
                friction_json=str(row[f"{evidence}friction_json"]),
            )
            self._queued.append(
                TaskExecutionRequest(
                    mission_id=mission_id,
                    task_id=task_id,
                    task_name=str(row[f"{record}name"]),
                    repository=str(row[f"{workspace}repository"]),
                    branch=str(row[f"{workspace}branch"]),
                    base_ref=str(row[f"{workspace}base_ref"]),
                    prompt=str(row[f"{record}prompt"]),
                    validators=validator_set,
                    attempt_id=attempt_id,
                    attempt_index=int(row[f"{attempt}attempt_index"]),
                    previous_session_id=str(row[f"{attempt}agent_session_id"]),
                    previous_validator_results=prior_evidence.validator_results(),
                )
            )
            self._seen_attempts.add(attempt_id)

    def drain(self) -> tuple[TaskExecutionRequest, ...]:
        requests = tuple(self._queued)
        self._queued.clear()
        return requests
