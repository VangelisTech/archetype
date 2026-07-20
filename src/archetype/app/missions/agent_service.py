# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application composition for the batteries-included Agent Missions family."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.coding_agents import (
    AgentMissionSandboxResource,
    TaskExecutionOutbox,
    agent_mission_processors,
)
from archetype.missions.coding_agents.components import (
    AgentMissionRecord,
    AgentMissionState,
    AgentTaskAttempt,
    AgentTaskEvidence,
    AgentTaskPolicy,
    AgentTaskRecord,
    AgentTaskState,
    AgentTaskValidators,
    AgentTaskWorkspace,
)
from archetype.missions.coding_agents.transitions import AgentMissionStatus
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    ExecutionOutcome,
    MissionResult,
    MissionSubmission,
    RepositoryPublicationPolicy,
    SubmittedMission,
    TaskExecutionReceipt,
    TaskResult,
)
from archetype.missions.relationships import DependsOn, PartOfMission


class MissionWorld(Protocol):
    """Structural runtime-world surface required by the application service."""

    async def reserve_ids(self, n: int) -> list[int]: ...

    async def spawn_reserved(self, entity_id: int, *components: Component) -> None: ...

    async def spawn(self, *components: Component) -> int: ...

    async def update(self, entity_id: int, *components: Component) -> None: ...

    async def step(self, **kwargs: object) -> None: ...

    async def query(self, *components: type[Component]) -> DataFrame: ...

    async def info(self) -> MissionWorldInfo: ...

    async def shutdown(self) -> None: ...

    @property
    def world_id(self) -> object: ...


class MissionWorldInfo(Protocol):
    tick: int


class AgentMissionService:
    """Compose and drive the batteries-included coding-agent mission workflow.

    Transition policy remains in the installed family processors. This service
    owns bundle and world lifecycle, graph materialization, post-commit I/O, and
    typed result projection.
    """

    def __init__(
        self,
        *,
        world_factory: Callable[..., MissionWorld],
        name: str,
        config: AgentMissionConfig,
        storage: str | Path | StorageConfig | None = None,
    ) -> None:
        view = GraphView()
        outbox = TaskExecutionOutbox()
        world = world_factory(
            name,
            storage=storage,
            processors=list(agent_mission_processors()),
            resources=[
                view,
                outbox,
                AgentMissionSandboxResource(config.sandbox),
            ],
            hooks=[
                (PostTick, view.on_post_tick),
                (PostTick, outbox.on_post_tick),
            ],
        )
        self._world = world
        self._view = view
        self._outbox = outbox
        self._sandbox = config.sandbox
        self._max_ticks = config.max_ticks

    async def submit(
        self,
        *,
        repository: str,
        branch: str,
        tasks: Sequence[AgentTask],
        name: str = "agent-mission",
        base_ref: str = "main",
    ) -> SubmittedMission:
        """Materialize one explicit task DAG into mission, task, and edge entities."""

        submission = MissionSubmission(
            repository=repository,
            branch=branch,
            tasks=tuple(tasks),
            name=name,
            base_ref=base_ref,
        )
        identities = await self._world.reserve_ids(len(submission.tasks) + 1)
        mission_id, *task_entity_ids = identities
        task_ids = {
            task.name: entity_id
            for task, entity_id in zip(submission.tasks, task_entity_ids, strict=True)
        }

        await self._world.spawn_reserved(
            mission_id,
            AgentMissionRecord(
                name=submission.name,
                repository=submission.repository,
                branch=submission.branch,
                base_ref=submission.base_ref,
            ),
            AgentMissionState(),
        )
        for task, task_id in zip(submission.tasks, task_entity_ids, strict=True):
            await self._world.spawn_reserved(
                task_id,
                AgentTaskRecord(name=task.name, prompt=task.prompt),
                AgentTaskWorkspace(
                    repository=submission.repository,
                    branch=submission.branch,
                    base_ref=submission.base_ref,
                ),
                AgentTaskPolicy(
                    max_attempts=task.max_attempts,
                    publication_policy=task.publication_policy.value,
                ),
                AgentTaskValidators.from_specs(task.validators),
                AgentTaskState(),
                AgentTaskAttempt(),
                AgentTaskEvidence(),
            )
            await self._world.spawn(PartOfMission(source=task_id, target=mission_id))
        for task in submission.tasks:
            for dependency in task.depends_on:
                await self._world.spawn(
                    DependsOn(source=task_ids[task.name], target=task_ids[dependency])
                )
        return SubmittedMission(
            mission_id=mission_id,
            task_ids=tuple((task.name, task_ids[task.name]) for task in submission.tasks),
        )

    async def run(
        self,
        mission: SubmittedMission,
        *,
        max_ticks: int | None = None,
    ) -> MissionResult:
        """Drive ticks and sandbox receipts until one submitted mission is terminal."""

        limit = max_ticks if max_ticks is not None else self._max_ticks
        if limit < 1:
            raise ValueError("max_ticks must be positive")

        for _ in range(limit):
            await self._world.step()
            requests = self._outbox.drain()
            if requests:
                receipts = await self._execute(requests)
                for receipt in receipts:
                    await self._world.update(
                        receipt.task_id,
                        AgentTaskAttempt.from_receipt(receipt),
                        AgentTaskEvidence.from_receipt(receipt),
                    )

            status = self._mission_status(mission.mission_id)
            if status in {AgentMissionStatus.SUCCEEDED, AgentMissionStatus.FAILED}:
                result = await self._result(mission)
                await self._sandbox.close_mission(mission.mission_id)
                return result

        status = self._mission_status(mission.mission_id)
        raise RuntimeError(
            f"mission {mission.mission_id} did not terminate after {limit} ticks "
            f"(status={status.value if status else 'not-visible'})"
        )

    async def close(self) -> None:
        failures: list[BaseException] = []
        for close in (self._sandbox.close, self._world.shutdown):
            try:
                await close()
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup(
                f"Agent Missions shutdown failed for {len(failures)} operation(s)",
                failures,
            )

    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the mission-world read path."""

        return await self._world.query(*components)

    @property
    def world_id(self) -> object:
        """Return the mission world's durable identity."""

        return self._world.world_id

    async def _execute(self, requests):
        try:
            receipts = tuple(await self._sandbox.run_many(requests))
        except Exception as exc:
            receipts = tuple(
                TaskExecutionReceipt(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    attempt_id=request.attempt_id,
                    attempt_index=request.attempt_index,
                    outcome=ExecutionOutcome.FAILED,
                    validator_results=(),
                    error=f"{type(exc).__name__}: {exc}",
                )
                for request in requests
            )

        request_by_attempt = {request.attempt_id: request for request in requests}
        if len(receipts) != len(request_by_attempt):
            raise ValueError("sandbox must return exactly one receipt per execution request")
        seen: set[str] = set()
        for receipt in receipts:
            if receipt.attempt_id in seen:
                raise ValueError(f"sandbox returned duplicate receipt {receipt.attempt_id}")
            seen.add(receipt.attempt_id)
            try:
                request = request_by_attempt[receipt.attempt_id]
            except KeyError as exc:
                raise ValueError(
                    f"sandbox returned an unknown attempt {receipt.attempt_id}"
                ) from exc
            self._validate_receipt(request, receipt)
        return receipts

    @staticmethod
    def _validate_receipt(request, receipt: TaskExecutionReceipt) -> None:
        if (
            receipt.mission_id,
            receipt.task_id,
            receipt.attempt_index,
        ) != (request.mission_id, request.task_id, request.attempt_index):
            raise ValueError("sandbox receipt identity does not match its request")

        requested = {validator.name: validator for validator in request.validators}
        observed = {result.name: result for result in receipt.validator_results}
        if len(observed) != len(receipt.validator_results):
            raise ValueError("sandbox receipt validator names must be unique")
        if receipt.outcome is ExecutionOutcome.ACCEPTED:
            if set(observed) != set(requested):
                raise ValueError("accepted receipt must contain every requested validator")
            for name, validator in requested.items():
                result = observed[name]
                if result.command != validator.command or not result.passed:
                    raise ValueError("accepted receipt contains invalid validator evidence")
            if not receipt.commit_sha:
                raise ValueError("accepted coding task receipt requires a commit SHA")
            if (
                request.publication_policy is RepositoryPublicationPolicy.COMMIT_AND_PUSH
                and not receipt.pushed
            ):
                raise ValueError("accepted coding task receipt must satisfy commit-and-push policy")

    def _mission_status(self, mission_id: int) -> AgentMissionStatus | None:
        frame = self._view.frame(AgentMissionState)
        if frame is None:
            return None
        rows = (
            frame.where(cast(Expression, col("entity_id") == mission_id))
            .select(f"{AgentMissionState.get_prefix()}status")
            .to_pylist()
        )
        if not rows:
            return None
        return AgentMissionStatus(rows[0][f"{AgentMissionState.get_prefix()}status"])

    async def _result(self, mission: SubmittedMission) -> MissionResult:
        mission_frame = self._view.frame(AgentMissionRecord, AgentMissionState)
        task_frame = self._view.frame(
            AgentTaskRecord,
            AgentTaskState,
            AgentTaskAttempt,
        )
        if mission_frame is None or task_frame is None:
            raise RuntimeError("terminal mission state is not queryable")
        mission_rows = mission_frame.where(
            cast(Expression, col("entity_id") == mission.mission_id)
        ).to_pylist()
        if len(mission_rows) != 1:
            raise RuntimeError("terminal mission projection is not unique")
        mission_row = mission_rows[0]

        task_ids = dict(mission.task_ids)
        task_rows = {
            int(row["entity_id"]): row
            for row in task_frame.where(col("entity_id").is_in(list(task_ids.values()))).to_pylist()
        }
        record = AgentTaskRecord.get_prefix()
        state = AgentTaskState.get_prefix()
        attempt = AgentTaskAttempt.get_prefix()
        tasks = tuple(
            TaskResult(
                task_id=task_id,
                name=str(task_rows[task_id][f"{record}name"]),
                status=str(task_rows[task_id][f"{state}status"]),
                attempts=int(task_rows[task_id][f"{attempt}attempt_index"]),
                commit_sha=str(task_rows[task_id][f"{attempt}commit_sha"]),
                reason=str(task_rows[task_id][f"{state}reason"]),
            )
            for _, task_id in mission.task_ids
        )
        info = await self._world.info()
        mission_record = AgentMissionRecord.get_prefix()
        mission_state = AgentMissionState.get_prefix()
        return MissionResult(
            mission_id=mission.mission_id,
            status=str(mission_row[f"{mission_state}status"]),
            repository=str(mission_row[f"{mission_record}repository"]),
            branch=str(mission_row[f"{mission_record}branch"]),
            ticks_completed=int(info.tick),
            tasks=tasks,
            reason=str(mission_row[f"{mission_state}reason"]),
        )


__all__ = ["AgentMissionService", "MissionWorld"]
