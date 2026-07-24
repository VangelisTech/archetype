# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Application composition for the batteries-included Agent Missions family."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol

from daft import DataFrame

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.coding_agents.contracts import (
    AgentExecutionResult,
    FrictionObservation,
    TaskDispatchRequest,
)
from archetype.missions.coding_agents.harness import (
    CodexDriver,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.components import (
    AgentExecution,
    Candidate,
    Checkpoint,
    Commit,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    FrictionLog,
    Mission,
    MissionState,
    Sandbox,
    Task,
    TaskCriticPolicy,
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
from archetype.missions.critics import (
    CandidateReviewRequest,
    CodexCriticDriver,
    CriticExecutionResult,
    CriticHarness,
    CriticHarnessConfig,
    CriticPrewarmRequest,
)
from archetype.missions.critics.contracts import (
    candidate_subject_digest,
    canonical_digest,
)
from archetype.missions.processors import mission_processors
from archetype.missions.projections import (
    CriticReviewOutbox,
    TaskDispatchOutbox,
    current_mission_status,
    project_mission_result,
)
from archetype.missions.relations import (
    AuthoredBy,
    CandidateFor,
    DependsOn,
    Executes,
    Guards,
    PartOfMission,
    ProducedBy,
    Reviews,
    RunsIn,
    Supersedes,
)
from archetype.missions.sandboxes import (
    CheckpointRef,
    SandboxEvent,
    SandboxEventType,
    SandboxIdentity,
    SandboxKey,
    SandboxServiceProtocol,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
    SandboxTeardownError,
)
from archetype.missions.transitions import (
    AgentExecutionStatus,
    CriticExecutionStatus,
    MissionStatus,
)
from archetype.redaction import RedactedText, RedactionReceipt


@dataclass(frozen=True)
class _ExecutionEnvelope:
    request: TaskDispatchRequest
    result: AgentExecutionResult
    sandbox_status: SandboxStatus
    session: SandboxSession | None
    bind_mission: bool = True


@dataclass(frozen=True)
class _CheckpointCandidate:
    result: AgentExecutionResult
    execution_id: int
    session: SandboxSession


@dataclass(frozen=True)
class _CriticWarmSession:
    session: SandboxSession
    provision_started_at_ms: int
    sandbox_ready_at_ms: int
    base_hydrated_at_ms: int


@dataclass(frozen=True)
class _PendingCriticClosure:
    request: TaskDispatchRequest | CandidateReviewRequest
    sandbox_id: str = ""
    sandbox_acquired: bool = True
    friction_kind: str = "critic_sandbox_teardown"


class MissionWorld(Protocol):
    """Structural runtime-world surface required by the application service."""

    @property
    def active_world_id(self) -> object | None:
        """Return the durable identity without activating a lazy world."""

        ...

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


class MissionTaskOwner(Protocol):
    """Minimal supervised-task capability supplied by process composition."""

    def spawn[T](
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
        *,
        label: str,
    ) -> asyncio.Task[T]: ...


class MissionRedactor(Protocol):
    """Exact stateless-redaction capability consumed by mission execution."""

    @property
    def policy_id(self) -> str: ...

    def redact_text(self, value: str, *, scope: str) -> RedactedText: ...

    def assert_safe_metadata(self, value: str, *, field: str) -> RedactionReceipt: ...


class MissionCleanup(Protocol):
    """Exact-world mutation authority available only after close starts."""

    @property
    def world_id(self) -> str: ...

    async def stage_teardown(self, components: list[Component]) -> int: ...

    async def update_retained(
        self,
        entity_id: int,
        components: list[Component],
    ) -> None: ...

    async def commit(
        self,
        run_config: RunConfig,
        **input_kwargs: Any,
    ) -> int: ...

    async def finish(self) -> None: ...


type MissionCleanupFactory = Callable[[object], Awaitable[MissionCleanup]]


class MissionService:
    """Materialize task graphs and compose committed ticks with external I/O."""

    def __init__(
        self,
        *,
        world_factory: Callable[..., MissionWorld],
        name: str,
        config: AgentMissionConfig,
        sandbox_service: SandboxServiceProtocol,
        redaction_service: MissionRedactor,
        task_owner: MissionTaskOwner,
        cleanup_factory: MissionCleanupFactory,
        storage: str | Path | StorageConfig | None = None,
    ) -> None:
        view = GraphView()
        outbox = TaskDispatchOutbox()
        critic_outbox = CriticReviewOutbox()
        world = world_factory(
            name,
            storage=storage,
            processors=list(mission_processors()),
            resources=[view, outbox],
            hooks=[
                (PostTick, view.on_post_tick),
                (PostTick, outbox.on_post_tick),
                (PostTick, critic_outbox.on_post_tick),
            ],
        )
        driver = config.driver or CodexDriver(
            model=config.model,
            workspace=config.workspace,
        )
        self._world = world
        self._view = view
        self._outbox = outbox
        self._critic_outbox = critic_outbox
        self._sandboxes = sandbox_service
        self._redaction_service = redaction_service
        self._task_owner = task_owner
        self._cleanup_factory = cleanup_factory
        self._cleanup: MissionCleanup | None = None
        self._mission_cleanup_complete = False
        self._closed = False
        self._sandbox_provider = config.sandbox_backend.name
        self._sandbox_environment = config.sandbox_environment
        self._workspace = config.workspace
        self._critic_workspace = config.critic_workspace
        self._harness = CodingAgentHarness(
            driver,
            CodingAgentHarnessConfig(workspace=config.workspace),
        )
        critic_driver = config.critic_driver or CodexCriticDriver(
            workspace=config.critic_workspace,
        )
        critic_driver_id = str(getattr(critic_driver, "driver_id", "")).strip()
        if not critic_driver_id:
            raise ValueError("configured critic driver must declare a non-empty driver_id")
        self._critic_driver_id = critic_driver_id
        self._critic_harness = CriticHarness(
            critic_driver,
            CriticHarnessConfig(workspace=config.critic_workspace),
        )
        self._max_ticks = config.max_ticks
        self._sandbox_entities: dict[str, tuple[int, Sandbox]] = {}
        self._mission_sandboxes: dict[int, str] = {}
        self._checkpoint_after_dispatch = config.checkpoint_after_dispatch
        self._on_sandbox_event = config.on_sandbox_event
        self._observed_sandbox_ids: set[str] = set()
        self._critic_prewarms: dict[str, asyncio.Task[_CriticWarmSession]] = {}
        self._pending_critic_closures: dict[str, _PendingCriticClosure] = {}
        self._critic_sandbox_context: dict[
            str,
            tuple[SandboxKey, int, int, str],
        ] = {}
        self._task_candidates: dict[int, int] = {}
        self._candidate_dispatches: set[str] = set()

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
        mismatched_drivers = sorted(
            {
                task.critic_policy.driver
                for task in submission.tasks
                if task.critic_policy.driver != self._critic_driver_id
            }
        )
        if mismatched_drivers:
            raise ValueError(
                "task critic policy driver must match the configured critic driver "
                f"{self._critic_driver_id!r}; got {', '.join(mismatched_drivers)}"
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
                TaskCriticPolicy(
                    policy_id=task.critic_policy.policy_id,
                    version=task.critic_policy.version,
                    digest=task.critic_policy.digest,
                    perspective=task.critic_policy.perspective,
                    information_view=task.critic_policy.information_view,
                    driver=task.critic_policy.driver,
                    model=task.critic_policy.model,
                    sampling=task.critic_policy.sampling,
                    max_reviews=task.critic_policy.max_reviews,
                    timeout_seconds=task.critic_policy.timeout_seconds,
                    output_schema_version=task.critic_policy.output_schema_version,
                    max_output_chars=task.critic_policy.max_output_chars,
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
            repository=submission.repository,
            branch=submission.branch,
            base_ref=submission.base_ref,
        )

    async def restore_sandbox(
        self,
        mission: SubmittedMission,
        checkpoint: CheckpointRef,
    ) -> SandboxIdentity:
        """Restore one explicit checkpoint without automatically submitting work."""

        if not mission.branch:
            raise ValueError("restoring a submitted mission requires its branch identity")
        key = SandboxKey(f"mission:{mission.mission_id}")
        spec = self._sandbox_spec(mission.mission_id, mission.branch)
        previous_sandbox_id = self._mission_sandboxes.get(mission.mission_id)
        try:
            session = await self._sandboxes.restore(key, spec, checkpoint)
        except SandboxTeardownError as exc:
            await self._mark_sandbox_errored(exc.identity.sandbox_id, exc)
            await self._world.spawn(
                FrictionLog(
                    kind="sandbox_restore",
                    message=self._redact_and_tail(
                        f"{type(exc).__name__}: {exc}",
                        limit=4_000,
                        scope=f"mission:{mission.mission_id}:sandbox-restore",
                    ),
                )
            )
            await self._world.step()
            raise
        except Exception:
            retained = self._sandboxes.session(key)
            replaced_is_gone = retained is None or (
                previous_sandbox_id is not None
                and retained.identity.sandbox_id != previous_sandbox_id
            )
            if previous_sandbox_id is not None and replaced_is_gone:
                await self._mark_sandbox_closed(previous_sandbox_id)
                self._mission_sandboxes.pop(mission.mission_id, None)
                await self._world.step()
            raise
        identity = session.identity
        if previous_sandbox_id is not None and previous_sandbox_id != identity.sandbox_id:
            await self._mark_sandbox_closed(previous_sandbox_id)
        self._mission_sandboxes[mission.mission_id] = identity.sandbox_id
        await self._ensure_sandbox_entity(
            mission.mission_id,
            identity,
            status=SandboxStatus.READY,
        )
        self._emit_sandbox_event(SandboxEventType.READY, identity)
        await self._world.step()
        return identity

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

        pending_checkpoints: list[tuple[int, _CheckpointCandidate]] = []
        checkpoint_commit_pending = False
        for _ in range(limit):
            await self._world.step()
            if checkpoint_commit_pending:
                checkpoint_commit_pending = False

            await self._close_pending_critic_sandboxes()

            waiting: list[tuple[int, _CheckpointCandidate]] = []
            for remaining_commits, candidate in pending_checkpoints:
                remaining_commits -= 1
                if remaining_commits == 0:
                    await self._stage_checkpoint(candidate)
                    checkpoint_commit_pending = True
                else:
                    waiting.append((remaining_commits, candidate))
            pending_checkpoints = waiting

            requests = self._outbox.drain()
            for request in requests:
                self._start_critic_prewarm(request)
            for envelope in await self._execute(requests):
                execution_id = await self._stage_result(
                    envelope.result,
                    envelope.sandbox_status,
                    bind_mission=envelope.bind_mission,
                )
                candidate_id = await self._stage_candidate(
                    envelope.request,
                    envelope.result,
                    execution_id,
                )
                if candidate_id is None:
                    await self._close_unused_critic(envelope.request)
                if (
                    self._checkpoint_after_dispatch
                    and envelope.session is not None
                    and envelope.session.capabilities.checkpoints
                ):
                    # GraphView is previous-tick: one commit makes execution
                    # evidence visible and the next commits the task decision.
                    pending_checkpoints.append(
                        (
                            2,
                            _CheckpointCandidate(
                                envelope.result,
                                execution_id,
                                envelope.session,
                            ),
                        )
                    )

            for review in self._critic_outbox.drain():
                result = await self._execute_review(review)
                self._pending_critic_closures[review.dispatch_id] = _PendingCriticClosure(
                    review,
                    result.sandbox.sandbox_id,
                    result.sandbox_acquired,
                )
                await self._stage_critic_result(result)
            exhausted_reviews = self._critic_outbox.drain_exhausted()
            if exhausted_reviews:
                exhausted = exhausted_reviews[0]
                raise RuntimeError(
                    "critic review budget exhausted for "
                    f"candidate {exhausted.candidate_id!r} "
                    f"after {exhausted.attempts}/{exhausted.max_reviews} attempts; "
                    "the task remains pending review"
                )

            status = current_mission_status(self._view, mission.mission_id)
            if status in {MissionStatus.SUCCEEDED, MissionStatus.FAILED}:
                # A terminal task decision is authoritative. Any checkpoint
                # still waiting on that decision can now be attempted without
                # consuming more of the caller's simulation-tick budget.
                for _remaining_commits, candidate in pending_checkpoints:
                    await self._stage_checkpoint(candidate)
                    checkpoint_commit_pending = True
                pending_checkpoints.clear()
                if checkpoint_commit_pending:
                    await self._world.step()
                await self._close_mission_sandbox(mission.mission_id)
                await self._world.step()
                info = await self._world.info()
                return project_mission_result(
                    self._view,
                    mission,
                    ticks_completed=int(info.tick),
                )

        # Checkpoint evidence is best effort and does not consume the caller's
        # mission-tick budget. Do not silently discard a post-dispatch
        # checkpoint just because the task has not reached a terminal state.
        for _remaining_commits, candidate in pending_checkpoints:
            await self._stage_checkpoint(candidate)
            checkpoint_commit_pending = True
        if checkpoint_commit_pending:
            await self._world.step()
        if self._pending_critic_closures:
            await self._world.step()
            await self._close_pending_critic_sandboxes()
            await self._world.step()

        status = current_mission_status(self._view, mission.mission_id)
        raise RuntimeError(
            f"mission {mission.mission_id} did not terminate after {limit} ticks "
            f"(status={status.value if status else 'not-visible'})"
        )

    async def close(self) -> None:
        if self._closed:
            return
        cleanup = await self._exact_cleanup()
        if not self._mission_cleanup_complete:
            failures: list[BaseException] = []
            sandbox_failure: BaseException | None = None
            if self._critic_prewarms:
                await asyncio.gather(
                    *tuple(self._critic_prewarms.values()),
                    return_exceptions=True,
                )
            try:
                await self._sandboxes.shutdown()
            except BaseException as exc:
                sandbox_failure = exc
                failures.append(exc)

            if cleanup is not None:
                try:
                    await self._reconcile_sandboxes_after_shutdown(
                        sandbox_failure,
                        cleanup=cleanup,
                    )
                except BaseException as exc:
                    failures.append(exc)

            if failures:
                raise BaseExceptionGroup(
                    f"Agent Missions shutdown failed for {len(failures)} operation(s)",
                    failures,
                )

            if cleanup is not None:
                try:
                    await cleanup.finish()
                except BaseException as exc:
                    raise BaseExceptionGroup(
                        "Agent Missions shutdown failed for 1 operation(s)",
                        [exc],
                    ) from exc
            self._mission_cleanup_complete = True
        try:
            await self._world.shutdown()
        except BaseException as exc:
            raise BaseExceptionGroup(
                "Agent Missions shutdown failed for 1 operation(s)",
                [exc],
            ) from exc
        self._closed = True

    async def query(self, *components: type[Component]) -> DataFrame:
        """Query persisted mission state through the mission-world read path."""

        return await self._world.query(*components)

    @property
    def world_id(self) -> object:
        """Return the mission world's durable identity."""

        return self._world.world_id

    async def _exact_cleanup(self) -> MissionCleanup | None:
        cleanup = self._cleanup
        if cleanup is None:
            world_id = self._world.active_world_id
            if world_id is None:
                return None
            cleanup = await self._cleanup_factory(world_id)
            if str(cleanup.world_id) != str(world_id):
                raise ValueError("mission cleanup capability is bound to another world")
            self._cleanup = cleanup
        return cleanup

    async def _execute(
        self,
        requests: Sequence[TaskDispatchRequest],
    ) -> tuple[_ExecutionEnvelope, ...]:
        results: list[_ExecutionEnvelope] = []
        for request in requests:
            key = SandboxKey(f"mission:{request.mission_id}")
            spec = self._sandbox_spec(request.mission_id, request.branch)
            session: SandboxSession | None = None
            previous_sandbox_id = self._mission_sandboxes.get(request.mission_id)
            bind_mission = True
            try:
                session = await self._sandboxes.acquire(key, spec)
                if (
                    previous_sandbox_id is not None
                    and previous_sandbox_id != session.identity.sandbox_id
                    and previous_sandbox_id in self._sandbox_entities
                ):
                    await self._mark_sandbox_closed(previous_sandbox_id)
                await self._ensure_sandbox_entity(
                    request.mission_id,
                    session.identity,
                    status=SandboxStatus.READY,
                )
                self._emit_sandbox_event(SandboxEventType.READY, session.identity)
                self._emit_sandbox_event(
                    SandboxEventType.PROCESS_STARTED,
                    session.identity,
                    operation="coding-agent",
                )
                result = await self._harness.execute(session, request)
                sandbox_status = await session.status()
                self._emit_sandbox_event(
                    SandboxEventType.PROCESS_FINISHED,
                    session.identity,
                    operation="coding-agent",
                    returncode=result.agent_returncode,
                )
            except SandboxTeardownError as exc:
                sandbox_status = SandboxStatus.ERRORED
                bind_mission = False
                if exc.identity.sandbox_id in self._sandbox_entities:
                    await self._mark_sandbox_errored(exc.identity.sandbox_id, exc)
                error = f"{type(exc).__name__}: {exc}"
                result = AgentExecutionResult(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    dispatch_id=request.dispatch_id,
                    dispatch_sequence=request.dispatch_sequence,
                    status=AgentExecutionStatus.ERRORED,
                    sandbox=exc.identity,
                    worktree=self._workspace,
                    agent_session_id="",
                    agent_returncode=-1,
                    starting_revision=request.task_base_revision,
                    final_revision="",
                    friction=(
                        FrictionObservation(
                            kind="sandbox_teardown",
                            message=error,
                        ),
                    ),
                    error=error,
                )
            except Exception as exc:
                sandbox_status = SandboxStatus.ERRORED
                retained = self._sandboxes.session(key)
                if session is None and previous_sandbox_id is not None:
                    replaced_is_gone = retained is None or (
                        retained.identity.sandbox_id != previous_sandbox_id
                    )
                    if replaced_is_gone:
                        if previous_sandbox_id in self._sandbox_entities:
                            await self._mark_sandbox_closed(previous_sandbox_id)
                        if self._mission_sandboxes.get(request.mission_id) == previous_sandbox_id:
                            self._mission_sandboxes.pop(request.mission_id, None)
                failed_identity = (
                    session.identity
                    if session is not None
                    else retained.identity
                    if retained is not None
                    else SandboxIdentity(
                        self._sandbox_provider,
                        f"unavailable-{request.dispatch_id}",
                        self._sandbox_environment,
                    )
                )
                bind_mission = session is not None or retained is not None
                result = AgentExecutionResult(
                    mission_id=request.mission_id,
                    task_id=request.task_id,
                    dispatch_id=request.dispatch_id,
                    dispatch_sequence=request.dispatch_sequence,
                    status=AgentExecutionStatus.ERRORED,
                    sandbox=failed_identity,
                    worktree=self._workspace,
                    agent_session_id="",
                    agent_returncode=-1,
                    starting_revision=request.task_base_revision,
                    final_revision="",
                    error=f"{type(exc).__name__}: {exc}",
                )
            results.append(
                _ExecutionEnvelope(
                    request=request,
                    result=result,
                    sandbox_status=sandbox_status,
                    session=session,
                    bind_mission=bind_mission,
                )
            )
        return tuple(results)

    async def _stage_result(
        self,
        result: AgentExecutionResult,
        sandbox_status: SandboxStatus,
        *,
        bind_mission: bool,
    ) -> int:
        retained_sandbox = self._sandbox_entities.get(result.sandbox.sandbox_id)
        if retained_sandbox is None:
            sandbox_entity = await self._ensure_sandbox_entity(
                result.mission_id,
                result.sandbox,
                status=sandbox_status,
                error=result.error if sandbox_status is SandboxStatus.ERRORED else "",
                bind_mission=bind_mission,
            )
        else:
            sandbox_entity, sandbox_state = retained_sandbox
            observed_error = self._redact_and_tail(
                result.error if sandbox_status is SandboxStatus.ERRORED else "",
                limit=4_000,
                scope=f"mission:{result.mission_id}:sandbox-error",
            )
            if sandbox_state.status == SandboxStatus.CLOSED.value:
                # A later request in the same execution batch may already have
                # replaced and closed this session. Staging the earlier envelope
                # must not move durable lifecycle evidence backwards.
                updated_status = SandboxStatus.CLOSED.value
                updated_error = sandbox_state.error or observed_error
            else:
                updated_status = sandbox_status.value
                updated_error = observed_error
            updated = sandbox_state.model_copy(
                update={
                    "status": updated_status,
                    "error": updated_error,
                }
            )
            if updated != sandbox_state:
                await self._world.update(sandbox_entity, updated)
                self._sandbox_entities[result.sandbox.sandbox_id] = (
                    sandbox_entity,
                    updated,
                )

        execution_id = await self._world.spawn(
            AgentExecution(
                task_id=result.task_id,
                dispatch_id=result.dispatch_id,
                dispatch_sequence=result.dispatch_sequence,
                status=result.status.value,
                sandbox_id=result.sandbox.sandbox_id,
                agent_session_id=result.agent_session_id,
                agent_returncode=result.agent_returncode,
                agent_stdout=self._redact_and_tail(
                    result.agent_stdout,
                    limit=16_000,
                    scope=f"mission:{result.mission_id}:agent-stdout",
                ),
                agent_stderr=self._redact_and_tail(
                    result.agent_stderr,
                    limit=16_000,
                    scope=f"mission:{result.mission_id}:agent-stderr",
                ),
                trace_uri=self._safe_metadata(
                    result.trace_uri,
                    field="AgentExecution.trace_uri",
                ),
                redaction_policy_id=self._redaction_service.policy_id,
                starting_revision=result.starting_revision,
                final_revision=result.final_revision,
                error=self._redact_and_tail(
                    result.error,
                    limit=4_000,
                    scope=f"mission:{result.mission_id}:execution-error",
                ),
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
                    stdout=self._redact_and_tail(
                        observed.stdout,
                        limit=4_000,
                        scope=f"mission:{result.mission_id}:validator-stdout",
                    ),
                    stderr=self._redact_and_tail(
                        observed.stderr,
                        limit=4_000,
                        scope=f"mission:{result.mission_id}:validator-stderr",
                    ),
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
                    message=self._redact_and_tail(
                        observed.message,
                        limit=4_000,
                        scope=f"mission:{result.mission_id}:friction",
                    ),
                )
            )
            await self._world.spawn(ProducedBy(source=output_id, target=execution_id))
        return execution_id

    async def _stage_candidate(
        self,
        request: TaskDispatchRequest,
        result: AgentExecutionResult,
        execution_id: int,
    ) -> int | None:
        """Stage one immutable candidate only for complete authored-green evidence."""

        if request.dispatch_id in self._candidate_dispatches:
            return None
        expected_validator_ids = {item.validator_id for item in request.validators}
        observed_validator_ids = {item.validator_id for item in result.validation}
        exact_validation = (
            expected_validator_ids == observed_validator_ids
            and len(result.validation) == len(request.validators)
            and all(
                item.passed and item.revision == result.final_revision for item in result.validation
            )
        )
        final_publications = [
            item
            for item in result.commits
            if item.sha == result.final_revision and item.pushed and item.final_revision
        ]
        if not (
            result.status is AgentExecutionStatus.EXITED
            and exact_validation
            and len(final_publications) == 1
            and result.final_revision
            and result.starting_revision
            and result.diff_digest
            and result.validator_bundle_digest
        ):
            return None

        candidate_id = canonical_digest(
            {
                "mission_id": request.mission_id,
                "task_id": request.task_id,
                "dispatch_id": request.dispatch_id,
                "author_execution_id": execution_id,
                "head_revision": result.final_revision,
                "policy_digest": request.critic_policy.digest,
            }
        )
        subject_digest = candidate_subject_digest(
            candidate_id=candidate_id,
            mission_id=request.mission_id,
            task_id=request.task_id,
            dispatch_id=request.dispatch_id,
            author_execution_id=execution_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
            base_revision=result.starting_revision,
            head_revision=result.final_revision,
            diff_digest=result.diff_digest,
            validator_bundle_digest=result.validator_bundle_digest,
            policy_digest=request.critic_policy.digest,
        )
        candidate_entity_id = await self._world.spawn(
            Candidate(
                candidate_id=candidate_id,
                mission_id=request.mission_id,
                task_id=request.task_id,
                dispatch_id=request.dispatch_id,
                dispatch_sequence=request.dispatch_sequence,
                author_execution_id=execution_id,
                author_sandbox_id=result.sandbox.sandbox_id,
                repository=request.repository,
                branch=request.branch,
                base_ref=request.base_ref,
                base_revision=result.starting_revision,
                head_revision=result.final_revision,
                diff_digest=result.diff_digest,
                validator_bundle_digest=result.validator_bundle_digest,
                policy_digest=request.critic_policy.digest,
                candidate_digest=subject_digest,
                created_at_ms=int(time.time() * 1000),
            )
        )
        await self._world.spawn(CandidateFor(source=candidate_entity_id, target=request.task_id))
        await self._world.spawn(AuthoredBy(source=candidate_entity_id, target=execution_id))
        prior_candidate = self._task_candidates.get(request.task_id)
        if prior_candidate is not None:
            await self._world.spawn(Supersedes(source=candidate_entity_id, target=prior_candidate))
        self._task_candidates[request.task_id] = candidate_entity_id
        self._candidate_dispatches.add(request.dispatch_id)
        return candidate_entity_id

    def _start_critic_prewarm(self, request: TaskDispatchRequest) -> None:
        if request.dispatch_id in self._critic_prewarms:
            return
        prewarm = CriticPrewarmRequest(
            mission_id=request.mission_id,
            task_id=request.task_id,
            dispatch_id=request.dispatch_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
        )
        self._critic_prewarms[request.dispatch_id] = self._task_owner.spawn(
            lambda: self._prewarm_critic(prewarm),
            label="critic-prewarm",
        )

    def _start_review_prewarm(self, request: CandidateReviewRequest) -> None:
        if request.dispatch_id in self._critic_prewarms:
            return
        prewarm = CriticPrewarmRequest(
            mission_id=request.mission_id,
            task_id=request.task_id,
            dispatch_id=request.dispatch_id,
            repository=request.repository,
            branch=request.branch,
            base_ref=request.base_ref,
        )
        self._critic_prewarms[request.dispatch_id] = self._task_owner.spawn(
            lambda: self._prewarm_critic(prewarm),
            label="critic-prewarm",
        )

    async def _prewarm_critic(
        self,
        request: CriticPrewarmRequest,
    ) -> _CriticWarmSession:
        provision_started_at_ms = int(time.time() * 1000)
        session = await self._sandboxes.acquire(
            self._critic_sandbox_key(request.dispatch_id),
            self._critic_sandbox_spec(
                mission_id=request.mission_id,
                dispatch_id=request.dispatch_id,
                branch=request.branch,
            ),
        )
        sandbox_ready_at_ms = int(time.time() * 1000)
        await self._critic_harness.prewarm(session, request)
        return _CriticWarmSession(
            session=session,
            provision_started_at_ms=provision_started_at_ms,
            sandbox_ready_at_ms=sandbox_ready_at_ms,
            base_hydrated_at_ms=int(time.time() * 1000),
        )

    @staticmethod
    async def _observe_sandbox_status(session: SandboxSession) -> SandboxStatus:
        try:
            return await session.status()
        except asyncio.CancelledError:
            raise
        except Exception:
            return SandboxStatus.ERRORED

    async def _execute_review(
        self,
        request: CandidateReviewRequest,
    ) -> CriticExecutionResult:
        self._start_review_prewarm(request)
        task = self._critic_prewarms[request.dispatch_id]
        try:
            warm = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            retained = self._sandboxes.session(self._critic_sandbox_key(request.dispatch_id))
            sandbox_acquired = retained is not None
            identity = (
                retained.identity
                if retained is not None
                else SandboxIdentity(
                    self._sandbox_provider,
                    f"critic-unavailable-{request.review_id}",
                    self._sandbox_environment,
                )
            )
            sandbox_status = (
                await self._observe_sandbox_status(retained)
                if retained is not None
                else SandboxStatus.ERRORED
            )
            now_ms = int(time.time() * 1000)
            return CriticExecutionResult(
                request=request,
                status=CriticExecutionStatus.ERRORED,
                sandbox=identity,
                sandbox_status=sandbox_status,
                sandbox_acquired=sandbox_acquired,
                started_at_ms=now_ms,
                ended_at_ms=now_ms,
                error=f"{type(exc).__name__}: {exc}",
            )
        if warm.session.identity.sandbox_id == request.author_sandbox_id:
            now_ms = int(time.time() * 1000)
            return CriticExecutionResult(
                request=request,
                status=CriticExecutionStatus.UNVERIFIABLE,
                sandbox=warm.session.identity,
                sandbox_status=await self._observe_sandbox_status(warm.session),
                sandbox_acquired=True,
                started_at_ms=now_ms,
                ended_at_ms=now_ms,
                provision_started_at_ms=warm.provision_started_at_ms,
                sandbox_ready_at_ms=warm.sandbox_ready_at_ms,
                base_hydrated_at_ms=warm.base_hydrated_at_ms,
                error="critic reused the author sandbox identity",
            )
        await self._ensure_sandbox_entity(
            request.mission_id,
            warm.session.identity,
            status=SandboxStatus.READY,
            bind_mission=False,
        )
        self._emit_sandbox_event(SandboxEventType.READY, warm.session.identity)
        self._emit_sandbox_event(
            SandboxEventType.PROCESS_STARTED,
            warm.session.identity,
            operation="critic",
        )
        result = await self._critic_harness.execute(
            warm.session,
            request,
            provision_started_at_ms=warm.provision_started_at_ms,
            sandbox_ready_at_ms=warm.sandbox_ready_at_ms,
            base_hydrated_at_ms=warm.base_hydrated_at_ms,
        )
        result = replace(
            result,
            sandbox_status=await self._observe_sandbox_status(warm.session),
        )
        self._emit_sandbox_event(
            SandboxEventType.PROCESS_FINISHED,
            warm.session.identity,
            operation="critic",
            returncode=0 if result.status is CriticExecutionStatus.EXITED else 1,
        )
        if result.receipt is not None and (
            result.receipt.candidate_digest != request.candidate_digest
            or result.receipt.policy_digest != request.policy.digest
        ):
            return replace(
                result,
                status=CriticExecutionStatus.UNVERIFIABLE,
                findings=(),
                receipt=None,
                error="critic receipt identity does not match its request",
            )
        return result

    async def _close_unused_critic(self, request: TaskDispatchRequest) -> None:
        if request.dispatch_id not in self._critic_prewarms:
            return
        self._pending_critic_closures[request.dispatch_id] = _PendingCriticClosure(
            request,
            friction_kind="critic_prewarm_teardown",
        )
        await self._close_pending_critic_sandbox(request.dispatch_id)

    async def _stage_critic_result(self, result: CriticExecutionResult) -> int:
        request = result.request
        if result.sandbox_acquired:
            self._critic_sandbox_context[result.sandbox.sandbox_id] = (
                self._critic_sandbox_key(request.dispatch_id),
                request.mission_id,
                request.task_id,
                request.dispatch_id,
            )
        sandbox_entity = await self._ensure_sandbox_entity(
            request.mission_id,
            result.sandbox,
            status=result.sandbox_status,
            error=result.error if result.sandbox_status is SandboxStatus.ERRORED else "",
            bind_mission=False,
        )
        receipt_staged_at_ms = int(time.time() * 1000) if result.receipt else 0
        execution_id = await self._world.spawn(
            CriticExecution(
                candidate_entity_id=request.candidate_entity_id,
                candidate_id=request.candidate_id,
                review_id=request.review_id,
                attempt=request.attempt,
                status=result.status.value,
                sandbox_id=result.sandbox.sandbox_id,
                driver=self._critic_driver_id,
                model=request.policy.model,
                started_at_ms=result.started_at_ms,
                ended_at_ms=result.ended_at_ms,
                provision_started_at_ms=result.provision_started_at_ms,
                sandbox_ready_at_ms=result.sandbox_ready_at_ms,
                base_hydrated_at_ms=result.base_hydrated_at_ms,
                candidate_published_at_ms=request.candidate_published_at_ms,
                head_ready_at_ms=result.head_ready_at_ms,
                critic_started_at_ms=result.critic_started_at_ms,
                receipt_staged_at_ms=receipt_staged_at_ms,
                raw_output=self._redact_and_tail(
                    result.raw_output,
                    limit=request.policy.max_output_chars,
                    scope=f"mission:{request.mission_id}:critic-output",
                ),
                trace_uri=self._safe_metadata(
                    result.trace_uri,
                    field="CriticExecution.trace_uri",
                ),
                redaction_policy_id=self._redaction_service.policy_id,
                error=self._redact_and_tail(
                    result.error,
                    limit=4_000,
                    scope=f"mission:{request.mission_id}:critic-error",
                ),
            )
        )
        await self._world.spawn(Reviews(source=execution_id, target=request.candidate_entity_id))
        await self._world.spawn(RunsIn(source=execution_id, target=sandbox_entity))
        for finding in result.findings:
            finding_id = await self._world.spawn(
                CriticFinding(
                    candidate_entity_id=request.candidate_entity_id,
                    critic_execution_id=execution_id,
                    finding_id=self._redact_and_tail(
                        finding.finding_id,
                        limit=256,
                        scope=f"mission:{request.mission_id}:critic-finding-id",
                    ),
                    severity=finding.severity,
                    category=self._redact_and_tail(
                        finding.category,
                        limit=256,
                        scope=f"mission:{request.mission_id}:critic-category",
                    ),
                    confidence=finding.confidence,
                    title=self._redact_and_tail(
                        finding.title,
                        limit=1_000,
                        scope=f"mission:{request.mission_id}:critic-title",
                    ),
                    detail=self._redact_and_tail(
                        finding.detail,
                        limit=4_000,
                        scope=f"mission:{request.mission_id}:critic-detail",
                    ),
                    evidence_location=self._redact_and_tail(
                        finding.evidence_location,
                        limit=1_000,
                        scope=f"mission:{request.mission_id}:critic-location",
                    ),
                    reproduction=self._redact_and_tail(
                        finding.reproduction,
                        limit=4_000,
                        scope=f"mission:{request.mission_id}:critic-reproduction",
                    ),
                )
            )
            await self._world.spawn(ProducedBy(source=finding_id, target=execution_id))
        if result.receipt is not None:
            receipt = result.receipt
            receipt_id = await self._world.spawn(
                CriticReceipt(
                    candidate_entity_id=request.candidate_entity_id,
                    critic_execution_id=execution_id,
                    critic_sandbox_id=result.sandbox.sandbox_id,
                    review_id=receipt.review_id,
                    conclusion=receipt.conclusion.value,
                    candidate_digest=receipt.candidate_digest,
                    policy_digest=receipt.policy_digest,
                    evidence_digest=receipt.evidence_digest,
                    reviewed_base_revision=request.base_revision,
                    reviewed_head_revision=request.head_revision,
                    reviewed_diff_digest=request.diff_digest,
                    validator_bundle_digest=request.validator_bundle_digest,
                    reviewed_scope=self._redact_and_tail(
                        receipt.reviewed_scope,
                        limit=1_000,
                        scope=f"mission:{request.mission_id}:critic-scope",
                    ),
                    finding_count=receipt.finding_count,
                    blocking_count=receipt.blocking_count,
                    output_schema_version=receipt.output_schema_version,
                    completed_at_ms=receipt.completed_at_ms,
                )
            )
            await self._world.spawn(ProducedBy(source=receipt_id, target=execution_id))
        return execution_id

    async def _close_pending_critic_sandboxes(self) -> None:
        for dispatch_id in tuple(self._pending_critic_closures):
            await self._close_pending_critic_sandbox(dispatch_id)

    async def _close_pending_critic_sandbox(self, dispatch_id: str) -> None:
        pending = self._pending_critic_closures.get(dispatch_id)
        if pending is None:
            return
        request = pending.request
        key = self._critic_sandbox_key(dispatch_id)
        sandbox_id = pending.sandbox_id
        if not sandbox_id:
            task = self._critic_prewarms.get(dispatch_id)
            if task is None:
                self._pending_critic_closures.pop(dispatch_id, None)
                return
            try:
                warm = await asyncio.shield(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                retained = self._sandboxes.session(key)
                if retained is None:
                    self._critic_prewarms.pop(dispatch_id, None)
                    self._pending_critic_closures.pop(dispatch_id, None)
                    return
                sandbox_id = retained.identity.sandbox_id
            else:
                sandbox_id = warm.session.identity.sandbox_id
            pending = replace(pending, sandbox_id=sandbox_id)
            self._pending_critic_closures[dispatch_id] = pending
        try:
            await self._sandboxes.close(key)
        except asyncio.CancelledError:
            # SandboxService.close() shields provider teardown. Preserve this
            # pending closure so a later run can join the same single-flight
            # close without turning caller cancellation into error evidence.
            raise
        except BaseException as exc:
            if sandbox_id in self._sandbox_entities:
                await self._mark_sandbox_failed(
                    sandbox_id,
                    SandboxStatus.ERRORED,
                    exc,
                )
            await self._world.spawn(
                FrictionLog(
                    task_id=request.task_id,
                    dispatch_id=request.dispatch_id,
                    kind=pending.friction_kind,
                    message=self._redact_and_tail(
                        self._format_exception(exc),
                        limit=4_000,
                        scope=f"mission:{request.mission_id}:critic-teardown",
                    ),
                )
            )
            await self._world.step()
            raise
        else:
            if pending.sandbox_acquired and sandbox_id in self._sandbox_entities:
                await self._mark_sandbox_closed(sandbox_id)
            self._critic_prewarms.pop(dispatch_id, None)
            self._pending_critic_closures.pop(dispatch_id, None)

    async def _stage_checkpoint(self, candidate: _CheckpointCandidate) -> None:
        result = candidate.result
        identity = candidate.session.identity
        self._emit_sandbox_event(SandboxEventType.CHECKPOINT_STARTED, identity)
        checkpoint: CheckpointRef | None = None
        checkpoint_error = ""
        try:
            checkpoint = await candidate.session.checkpoint()
        except Exception as exc:
            checkpoint_error = self._redact_and_tail(
                f"{type(exc).__name__}: {exc}",
                limit=4_000,
                scope=f"mission:{result.mission_id}:checkpoint-error",
            )
            self._emit_sandbox_event(
                SandboxEventType.CHECKPOINT_FAILED,
                identity,
                message=checkpoint_error,
            )
            friction_id = await self._world.spawn(
                FrictionLog(
                    task_id=result.task_id,
                    execution_id=candidate.execution_id,
                    dispatch_id=result.dispatch_id,
                    kind="checkpoint",
                    message=checkpoint_error,
                )
            )
            await self._world.spawn(ProducedBy(source=friction_id, target=candidate.execution_id))
            try:
                sandbox_status = await candidate.session.status()
            except Exception:
                sandbox_status = SandboxStatus.ERRORED
            if sandbox_status is not SandboxStatus.READY:
                await self._ensure_sandbox_entity(
                    result.mission_id,
                    identity,
                    status=sandbox_status,
                    error=checkpoint_error,
                )
        else:
            self._emit_sandbox_event(
                SandboxEventType.CHECKPOINT_FINISHED,
                identity,
                checkpoint_uri=checkpoint.uri,
            )
        output_id = await self._world.spawn(
            Checkpoint(
                task_id=result.task_id,
                execution_id=candidate.execution_id,
                dispatch_id=result.dispatch_id,
                provider=checkpoint.provider if checkpoint is not None else identity.provider,
                checkpoint_id=checkpoint.checkpoint_id if checkpoint is not None else "",
                uri=checkpoint.uri if checkpoint is not None else "",
                created_at_ms=checkpoint.created_at_ms if checkpoint is not None else 0,
                environment=checkpoint.environment if checkpoint is not None else "",
                source_sandbox_id=(
                    checkpoint.source_sandbox_id if checkpoint is not None else identity.sandbox_id
                ),
                owner_id=checkpoint.owner_id if checkpoint is not None else "",
                locality=checkpoint.locality.value if checkpoint is not None else "",
                expires_at_ms=(
                    checkpoint.expires_at_ms
                    if checkpoint is not None and checkpoint.expires_at_ms is not None
                    else 0
                ),
                integrity=checkpoint.integrity if checkpoint is not None else "",
                restorable=checkpoint.restorable if checkpoint is not None else False,
                error=checkpoint_error,
            )
        )
        await self._world.spawn(ProducedBy(source=output_id, target=candidate.execution_id))

    async def _ensure_sandbox_entity(
        self,
        mission_id: int,
        identity: SandboxIdentity,
        *,
        status: SandboxStatus,
        error: str = "",
        bind_mission: bool = True,
    ) -> int:
        retained = self._sandbox_entities.get(identity.sandbox_id)
        if retained is not None:
            entity_id, sandbox_state = retained
            if sandbox_state.status == SandboxStatus.CLOSED.value:
                # Deferred observations may arrive after another request has
                # replaced and closed this session. Preserve terminal evidence;
                # only fill a previously empty error with the late observation.
                if not sandbox_state.error and error:
                    updated = sandbox_state.model_copy(
                        update={
                            "error": self._redact_and_tail(
                                error,
                                limit=4_000,
                                scope=f"mission:{mission_id}:sandbox-error",
                            )
                        }
                    )
                    await self._world.update(entity_id, updated)
                    self._sandbox_entities[identity.sandbox_id] = (entity_id, updated)
                return entity_id
            if sandbox_state.status != status.value or sandbox_state.error:
                updated = sandbox_state.model_copy(
                    update={
                        "status": status.value,
                        "error": self._redact_and_tail(
                            error,
                            limit=4_000,
                            scope=f"mission:{mission_id}:sandbox-error",
                        ),
                    }
                )
                await self._world.update(entity_id, updated)
                self._sandbox_entities[identity.sandbox_id] = (entity_id, updated)
            return entity_id
        sandbox_state = Sandbox(
            provider=identity.provider,
            sandbox_id=identity.sandbox_id,
            environment=identity.environment,
            worktree=self._workspace,
            status=status.value,
            error=self._redact_and_tail(
                error,
                limit=4_000,
                scope=f"mission:{mission_id}:sandbox-error",
            ),
        )
        entity_id = await self._world.spawn(sandbox_state)
        self._sandbox_entities[identity.sandbox_id] = (entity_id, sandbox_state)
        if bind_mission:
            self._mission_sandboxes[mission_id] = identity.sandbox_id
        return entity_id

    async def _close_mission_sandbox(self, mission_id: int) -> None:
        key = SandboxKey(f"mission:{mission_id}")
        sandbox_id = self._mission_sandboxes.get(mission_id)
        try:
            await self._sandboxes.close(key)
        except asyncio.CancelledError:
            # SandboxService.close() shields provider teardown. Caller
            # cancellation is not evidence that the provider cleanup failed;
            # a later run or service shutdown reconciles the retained entity
            # with the single-flight close result.
            raise
        except BaseException as exc:
            if sandbox_id is not None:
                await self._record_sandbox_teardown_failure(
                    mission_id,
                    sandbox_id,
                    self._sandboxes.session(key),
                    exc,
                )
                await self._world.step()
            raise
        if sandbox_id is None:
            return
        await self._mark_sandbox_closed(sandbox_id)

    async def _reconcile_sandboxes_after_shutdown(
        self,
        shutdown_failure: BaseException | None,
        *,
        cleanup: MissionCleanup,
    ) -> None:
        if not self._mission_sandboxes and not self._critic_sandbox_context:
            return
        changed = False
        for mission_id, sandbox_id in tuple(self._mission_sandboxes.items()):
            key = SandboxKey(f"mission:{mission_id}")
            retained = self._sandboxes.session(key)
            if retained is None:
                changed = (
                    await self._mark_sandbox_closed(
                        sandbox_id,
                        cleanup=cleanup,
                    )
                    or changed
                )
                continue
            failure = shutdown_failure or RuntimeError("sandbox teardown remained incomplete")
            await self._record_sandbox_teardown_failure(
                mission_id,
                sandbox_id,
                retained,
                failure,
                cleanup=cleanup,
            )
            changed = True
        for sandbox_id, (
            key,
            mission_id,
            task_id,
            dispatch_id,
        ) in tuple(self._critic_sandbox_context.items()):
            retained_state = self._sandbox_entities.get(sandbox_id)
            if retained_state is None or retained_state[1].status == SandboxStatus.CLOSED.value:
                continue
            retained = self._sandboxes.session(key)
            if retained is None:
                changed = (
                    await self._mark_sandbox_closed(
                        sandbox_id,
                        cleanup=cleanup,
                    )
                    or changed
                )
                continue
            failure = shutdown_failure or RuntimeError(
                "critic sandbox teardown remained incomplete"
            )
            await self._mark_sandbox_failed(
                sandbox_id,
                SandboxStatus.ERRORED,
                failure,
                cleanup=cleanup,
            )
            await cleanup.stage_teardown(
                [
                    FrictionLog(
                        task_id=task_id,
                        dispatch_id=dispatch_id,
                        kind="critic_sandbox_teardown",
                        message=self._redact_and_tail(
                            self._format_exception(failure),
                            limit=4_000,
                            scope=f"mission:{mission_id}:critic-teardown",
                        ),
                    )
                ]
            )
            changed = True
        if changed:
            await cleanup.commit(RunConfig())

    async def _record_sandbox_teardown_failure(
        self,
        mission_id: int,
        sandbox_id: str,
        session: SandboxSession | None,
        exc: BaseException,
        *,
        cleanup: MissionCleanup | None = None,
    ) -> None:
        status = SandboxStatus.ERRORED
        if session is not None:
            try:
                observed_status = await session.status()
            except BaseException:
                pass
            else:
                if observed_status is not SandboxStatus.READY:
                    status = observed_status
        await self._mark_sandbox_failed(
            sandbox_id,
            status,
            exc,
            cleanup=cleanup,
        )
        friction = FrictionLog(
            kind="sandbox_teardown",
            message=self._redact_and_tail(
                self._format_exception(exc),
                limit=4_000,
                scope=f"mission:{mission_id}:sandbox-teardown",
            ),
        )
        if cleanup is None:
            await self._world.spawn(friction)
        else:
            await cleanup.stage_teardown([friction])

    async def _mark_sandbox_closed(
        self,
        sandbox_id: str,
        *,
        cleanup: MissionCleanup | None = None,
    ) -> bool:
        entity_id, sandbox_state = self._sandbox_entities[sandbox_id]
        if sandbox_state.status == SandboxStatus.CLOSED.value:
            return False
        closed = sandbox_state.model_copy(update={"status": SandboxStatus.CLOSED.value})
        if cleanup is None:
            await self._world.update(entity_id, closed)
        else:
            await cleanup.update_retained(entity_id, [closed])
        self._sandbox_entities[sandbox_id] = (entity_id, closed)
        return True

    async def _mark_sandbox_errored(self, sandbox_id: str, exc: BaseException) -> None:
        await self._mark_sandbox_failed(sandbox_id, SandboxStatus.ERRORED, exc)

    async def _mark_sandbox_failed(
        self,
        sandbox_id: str,
        status: SandboxStatus,
        exc: BaseException,
        *,
        cleanup: MissionCleanup | None = None,
    ) -> None:
        entity_id, sandbox_state = self._sandbox_entities[sandbox_id]
        errored = sandbox_state.model_copy(
            update={
                "status": status.value,
                "error": self._redact_and_tail(
                    self._format_exception(exc),
                    limit=4_000,
                    scope=f"sandbox:{sandbox_id}:close-error",
                ),
            }
        )
        if cleanup is None:
            await self._world.update(entity_id, errored)
        else:
            await cleanup.update_retained(entity_id, [errored])
        self._sandbox_entities[sandbox_id] = (entity_id, errored)

    @classmethod
    def _format_exception(cls, exc: BaseException) -> str:
        if isinstance(exc, BaseExceptionGroup):
            nested = "; ".join(cls._format_exception(child) for child in exc.exceptions)
            return f"{type(exc).__name__}: {exc.message}: {nested}"
        return f"{type(exc).__name__}: {exc}"

    def _redact(self, value: str, *, scope: str) -> str:
        if not value:
            return ""
        return self._redaction_service.redact_text(value, scope=scope).text

    def _redact_and_tail(self, value: str, *, limit: int, scope: str) -> str:
        """Redact a complete observation before applying its storage bound."""

        return self._redact(value, scope=scope)[-limit:]

    def _safe_metadata(self, value: str, *, field: str) -> str:
        if value:
            self._redaction_service.assert_safe_metadata(value, field=field)
        return value

    def _sandbox_spec(self, mission_id: int, branch: str) -> SandboxSpec:
        return SandboxSpec(
            provider=self._sandbox_provider,
            environment=self._sandbox_environment,
            workdir=self._workspace,
            metadata=(
                ("mission", str(mission_id)),
                ("branch", branch),
            ),
        )

    @staticmethod
    def _critic_sandbox_key(dispatch_id: str) -> SandboxKey:
        return SandboxKey(f"critic:{dispatch_id}")

    def _critic_sandbox_spec(
        self,
        *,
        mission_id: int,
        dispatch_id: str,
        branch: str,
    ) -> SandboxSpec:
        return SandboxSpec(
            provider=self._sandbox_provider,
            environment=self._sandbox_environment,
            workdir=self._critic_workspace,
            metadata=(
                ("mission", str(mission_id)),
                ("branch", branch),
                ("role", "critic"),
                ("dispatch", dispatch_id),
            ),
        )

    def _emit_sandbox_event(
        self,
        kind: SandboxEventType,
        identity: SandboxIdentity,
        *,
        operation: str = "",
        returncode: int | None = None,
        checkpoint_uri: str = "",
        message: str = "",
    ) -> None:
        if kind is SandboxEventType.READY and identity.sandbox_id in self._observed_sandbox_ids:
            return
        if kind is SandboxEventType.READY:
            self._observed_sandbox_ids.add(identity.sandbox_id)
        if self._on_sandbox_event is None:
            return
        try:
            self._on_sandbox_event(
                SandboxEvent(
                    kind=kind,
                    sandbox=identity,
                    timestamp_ms=int(time.time() * 1000),
                    operation=operation,
                    returncode=returncode,
                    checkpoint_uri=checkpoint_uri,
                    message=message,
                )
            )
        except Exception:
            # Observers are UI/operations consumers, never task authority.
            return


__all__ = ["MissionService", "MissionWorld"]
