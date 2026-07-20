# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application composition for the batteries-included Agent Missions family."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

from daft import DataFrame

from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
)
from archetype.missions.coding_agents.harness import (
    CodexDriver,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.components import (
    AgentExecution,
    Commit,
    FrictionLog,
    Mission,
    MissionState,
    Sandbox,
    Task,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
    ValidationResult,
)
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    MissionResult,
    MissionSubmission,
    SubmittedMission,
)
from archetype.missions.processors import mission_processors
from archetype.missions.projections import (
    TaskDispatchOutbox,
    current_mission_status,
    project_mission_result,
)
from archetype.missions.relations import (
    DependsOn,
    Executes,
    Guards,
    PartOfMission,
    ProducedBy,
    RunsIn,
)
from archetype.missions.sandboxes import (
    SandboxIdentity,
    SandboxKey,
    SandboxServiceProtocol,
    SandboxSpec,
    SandboxStatus,
)
from archetype.missions.transitions import AgentExecutionStatus, MissionStatus


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


class MissionService:
    """Materialize task graphs and compose committed ticks with external I/O."""

    def __init__(
        self,
        *,
        world_factory: Callable[..., MissionWorld],
        name: str,
        config: AgentMissionConfig,
        sandbox_service: SandboxServiceProtocol,
        storage: str | Path | StorageConfig | None = None,
    ) -> None:
        view = GraphView()
        outbox = TaskDispatchOutbox()
        world = world_factory(
            name,
            storage=storage,
            processors=list(mission_processors()),
            resources=[view, outbox],
            hooks=[
                (PostTick, view.on_post_tick),
                (PostTick, outbox.on_post_tick),
            ],
        )
        driver = config.driver or CodexDriver(
            model=config.model,
            workspace=config.workspace,
        )
        self._world = world
        self._view = view
        self._outbox = outbox
        self._sandboxes = sandbox_service
        self._sandbox_provider = config.sandbox_backend.name
        self._sandbox_environment = config.sandbox_environment
        self._workspace = config.workspace
        self._harness = CodingAgentHarness(
            driver,
            CodingAgentHarnessConfig(workspace=config.workspace),
        )
        self._max_ticks = config.max_ticks
        self._sandbox_entities: dict[str, tuple[int, Sandbox]] = {}
        self._mission_sandboxes: dict[int, str] = {}

    async def submit(
        self,
        *,
        repository: str,
        branch: str,
        tasks: Sequence[AgentTask],
        name: str = "agent-mission",
        base_ref: str = "main",
    ) -> SubmittedMission:
        """Materialize one explicit task and validator graph."""

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
            Mission(
                name=submission.name,
                repository=submission.repository,
                branch=submission.branch,
                base_ref=submission.base_ref,
            ),
            MissionState(),
        )
        for task, task_id in zip(submission.tasks, task_entity_ids, strict=True):
            await self._world.spawn_reserved(
                task_id,
                Task(name=task.name, prompt=task.prompt),
                TaskWorkspace(
                    repository=submission.repository,
                    branch=submission.branch,
                    base_ref=submission.base_ref,
                ),
                TaskPolicy(
                    max_dispatches=task.max_dispatches,
                    publication_policy=task.publication_policy.value,
                ),
                TaskState(),
                TaskDispatch(),
            )
            await self._world.spawn(PartOfMission(source=task_id, target=mission_id))
            for validator in task.validators:
                validator_id = await self._world.spawn(
                    TaskValidator(
                        name=validator.name,
                        command=list(validator.command),
                        expected_returncode=validator.expected_returncode,
                        timeout_seconds=validator.timeout_seconds,
                    )
                )
                await self._world.spawn(Guards(source=validator_id, target=task_id))
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
        """Drive ticks and stage observations until one mission is terminal."""

        limit = max_ticks if max_ticks is not None else self._max_ticks
        if limit < 1:
            raise ValueError("max_ticks must be positive")

        for _ in range(limit):
            await self._world.step()
            requests = self._outbox.drain()
            for result, sandbox_status in await self._execute(requests):
                await self._stage_result(result, sandbox_status)

            status = current_mission_status(self._view, mission.mission_id)
            if status in {MissionStatus.SUCCEEDED, MissionStatus.FAILED}:
                await self._close_mission_sandbox(mission.mission_id)
                await self._world.step()
                info = await self._world.info()
                return project_mission_result(
                    self._view,
                    mission,
                    ticks_completed=int(info.tick),
                )

        status = current_mission_status(self._view, mission.mission_id)
        raise RuntimeError(
            f"mission {mission.mission_id} did not terminate after {limit} ticks "
            f"(status={status.value if status else 'not-visible'})"
        )

    async def close(self) -> None:
        failures: list[BaseException] = []
        for close in (self._sandboxes.shutdown, self._world.shutdown):
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
        results: list[tuple[AgentExecutionResult, SandboxStatus]] = []
        for request in requests:
            key = SandboxKey(f"mission:{request.mission_id}")
            spec = SandboxSpec(
                provider=self._sandbox_provider,
                environment=self._sandbox_environment,
                workdir=self._workspace,
                metadata=(
                    ("mission", str(request.mission_id)),
                    ("branch", request.branch),
                ),
            )
            try:
                session = await self._sandboxes.acquire(key, spec)
                result = await self._harness.execute(session, request)
                sandbox_status = await session.status()
            except Exception as exc:
                sandbox_status = SandboxStatus.ERRORED
                result = AgentExecutionResult(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    dispatch_id=request.dispatch_id,
                    dispatch_sequence=request.dispatch_sequence,
                    status=AgentExecutionStatus.ERRORED,
                    sandbox=SandboxIdentity(
                        self._sandbox_provider,
                        f"unavailable-{request.dispatch_id}",
                        self._sandbox_environment,
                    ),
                    worktree=self._workspace,
                    agent_session_id="",
                    agent_returncode=-1,
                    starting_revision=request.task_base_revision,
                    final_revision="",
                    error=f"{type(exc).__name__}: {exc}",
                )
            results.append((result, sandbox_status))
        return tuple(results)

    async def _stage_result(
        self,
        result: AgentExecutionResult,
        sandbox_status: SandboxStatus,
    ) -> None:
        retained_sandbox = self._sandbox_entities.get(result.sandbox.sandbox_id)
        if retained_sandbox is None:
            sandbox_state = Sandbox(
                provider=result.sandbox.provider,
                sandbox_id=result.sandbox.sandbox_id,
                environment=result.sandbox.environment,
                worktree=result.worktree,
                status=sandbox_status.value,
                error=result.error if sandbox_status is SandboxStatus.ERRORED else "",
            )
            sandbox_entity = await self._world.spawn(sandbox_state)
            self._sandbox_entities[result.sandbox.sandbox_id] = (
                sandbox_entity,
                sandbox_state,
            )
            self._mission_sandboxes[result.mission_id] = result.sandbox.sandbox_id
        else:
            sandbox_entity, _ = retained_sandbox

        execution_id = await self._world.spawn(
            AgentExecution(
                task_id=result.task_id,
                dispatch_id=result.dispatch_id,
                dispatch_sequence=result.dispatch_sequence,
                status=result.status.value,
                sandbox_id=result.sandbox.sandbox_id,
                agent_session_id=result.agent_session_id,
                agent_returncode=result.agent_returncode,
                starting_revision=result.starting_revision,
                final_revision=result.final_revision,
                error=result.error,
            )
        )
        await self._world.spawn(Executes(source=execution_id, target=result.task_id))
        await self._world.spawn(RunsIn(source=execution_id, target=sandbox_entity))

        for observed in result.validation:
            output_id = await self._world.spawn(
                ValidationResult(
                    task_id=result.task_id,
                    validator_id=observed.validator_id,
                    execution_id=execution_id,
                    dispatch_id=result.dispatch_id,
                    dispatch_sequence=result.dispatch_sequence,
                    revision=observed.revision,
                    expected_returncode=observed.expected_returncode,
                    actual_returncode=observed.actual_returncode,
                    stdout=observed.stdout,
                    stderr=observed.stderr,
                )
            )
            await self._world.spawn(ProducedBy(source=output_id, target=execution_id))
        for observed in result.commits:
            output_id = await self._world.spawn(
                Commit(
                    task_id=result.task_id,
                    execution_id=execution_id,
                    dispatch_id=result.dispatch_id,
                    sha=observed.sha,
                    message=observed.message,
                    branch=observed.branch,
                    pushed=observed.pushed,
                    final_revision=observed.final_revision,
                )
            )
            await self._world.spawn(ProducedBy(source=output_id, target=execution_id))
        for observed in result.friction:
            output_id = await self._world.spawn(
                FrictionLog(
                    task_id=result.task_id,
                    execution_id=execution_id,
                    dispatch_id=result.dispatch_id,
                    kind=observed.kind,
                    message=observed.message,
                )
            )
            await self._world.spawn(ProducedBy(source=output_id, target=execution_id))

    async def _close_mission_sandbox(self, mission_id: int) -> None:
        await self._sandboxes.close(SandboxKey(f"mission:{mission_id}"))
        sandbox_id = self._mission_sandboxes.get(mission_id)
        if sandbox_id is None:
            return
        entity_id, sandbox_state = self._sandbox_entities[sandbox_id]
        await self._world.update(
            entity_id,
            sandbox_state.model_copy(update={"status": SandboxStatus.CLOSED.value}),
        )


__all__ = ["MissionService", "MissionWorld"]
