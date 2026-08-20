# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Family workflow for batteries-included Agent Missions."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, Protocol, cast

from daft import DataFrame, Expression, col

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView
from archetype.missions.components import (
    FrictionLog,
    Mission,
    MissionState,
    Sandbox,
    Task,
    TaskCriticPolicy,
    TaskCriticSubjectPolicy,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    TaskWorkspace,
)
from archetype.missions.contracts import (
    AgentMissionConfig,
    AgentTask,
    CriticPolicy,
    MissionResult,
    MissionSubmission,
    SubmittedMission,
    mission_episode_id,
)
from archetype.missions.processors import mission_processors
from archetype.missions.projections import (
    CriticReviewBudgetExhaustedError,
    current_mission_status,
    project_mission_result,
    project_pending_review_exhaustion,
)
from archetype.missions.relations import (
    DependsOn,
    Guards,
    PartOfMission,
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
)
from archetype.missions.transitions import MissionStatus
from archetype.redaction import RedactedText, RedactionReceipt


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


class MissionActivityWorker(Protocol):
    async def run_once(self) -> bool: ...

    async def run_until_idle(self) -> bool: ...


class MissionActivityRuntime(Protocol):
    world_id: str

    @property
    def worker(self) -> MissionActivityWorker: ...

    async def aclose(self) -> None: ...


type MissionCleanupFactory = Callable[[object], Awaitable[MissionCleanup]]
type MissionActivityFactory = Callable[
    [str],
    Awaitable[MissionActivityRuntime],
]


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
        cleanup_factory: MissionCleanupFactory,
        activity_factory: MissionActivityFactory,
        storage: str | Path | StorageConfig | None = None,
    ) -> None:
        view = GraphView()
        world = world_factory(
            name,
            storage=storage,
            processors=list(mission_processors()),
            resources=[view],
            hooks=[(PostTick, view.on_post_tick)],
        )
        self._world = world
        self._view = view
        self._sandboxes = sandbox_service
        self._redaction_service = redaction_service
        self._cleanup_factory = cleanup_factory
        self._activity_factory = activity_factory
        self._activity: MissionActivityRuntime | None = None
        self._cleanup: MissionCleanup | None = None
        self._mission_cleanup_complete = False
        self._closed = False
        self._sandbox_provider = config.sandbox_backend.name
        self._sandbox_environment = config.sandbox_environment
        self._workspace = config.workspace
        critic_driver_id = (
            str(getattr(config.critic_driver, "driver_id", "")).strip()
            if config.critic_driver is not None
            else CriticPolicy().driver
        )
        if not critic_driver_id:
            raise ValueError("configured critic driver must declare a non-empty driver_id")
        self._critic_driver_id = critic_driver_id
        self._max_ticks = config.max_ticks
        self._sandbox_entities: dict[str, tuple[int, Sandbox]] = {}
        self._mission_sandboxes: dict[int, str] = {}
        self._on_sandbox_event = config.on_sandbox_event
        self._observed_sandbox_ids: set[str] = set()

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
        await self._ensure_activity()
        mission_id, *task_entity_ids = identities
        episode_id = mission_episode_id(self._world.world_id, mission_id)
        task_ids = {
            task.name: entity_id
            for task, entity_id in zip(submission.tasks, task_entity_ids, strict=True)
        }

        await self._world.spawn_reserved(
            mission_id,
            Mission(
                name=submission.name,
                episode_id=episode_id,
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
                TaskCriticSubjectPolicy(
                    max_subject_bytes=task.critic_policy.max_subject_bytes,
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
            episode_id=episode_id,
            world_id=str(self._world.world_id),
            repository=submission.repository,
            branch=submission.branch,
            base_ref=submission.base_ref,
        )

    async def recover_submitted(self) -> SubmittedMission | None:
        """Return the world's existing Mission identity after crash recovery."""

        await self._world.info()
        mission_prefix = Mission.get_prefix()
        task_prefix = Task.get_prefix()
        part_prefix = PartOfMission.get_prefix()
        mission_rows = (
            (await self._world.query(Mission))
            .select(
                "entity_id",
                f"{mission_prefix}episode_id",
                f"{mission_prefix}repository",
                f"{mission_prefix}branch",
                f"{mission_prefix}base_ref",
            )
            .limit(1)
            .to_pylist()
        )
        if not mission_rows:
            return None
        row = mission_rows[0]
        mission_id = int(row["entity_id"])
        membership = (
            (await self._world.query(PartOfMission))
            .where(cast(Expression, col(f"{part_prefix}target") == mission_id))
            .select(col(f"{part_prefix}source").alias("task_entity_id"))
        )
        names = (await self._world.query(Task)).select(
            "entity_id",
            col(f"{task_prefix}name").alias("task_name"),
        )
        task_rows = (
            membership.join(
                names,
                left_on="task_entity_id",
                right_on="entity_id",
            )
            .select("task_name", "task_entity_id")
            .to_pylist()
        )
        return SubmittedMission(
            mission_id=mission_id,
            task_ids=tuple(
                sorted(
                    ((str(task["task_name"]), int(task["task_entity_id"])) for task in task_rows),
                    key=lambda item: item[1],
                )
            ),
            episode_id=str(row[f"{mission_prefix}episode_id"]),
            world_id=str(self._world.world_id),
            repository=str(row[f"{mission_prefix}repository"]),
            branch=str(row[f"{mission_prefix}branch"]),
            base_ref=str(row.get(f"{mission_prefix}base_ref") or "main"),
        )

    async def restore_sandbox(
        self,
        mission: SubmittedMission,
        checkpoint: CheckpointRef,
    ) -> SandboxIdentity:
        """Reject restore until Activity admission can bind its checkpoint."""

        del mission, checkpoint
        raise NotImplementedError(
            "Mission checkpoint restore is unavailable on the v0.6 Modal Activity path; "
            "run() would otherwise ignore the restored filesystem"
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
        # A replacement process has a lazy RuntimeWorld with no active identity
        # until its durable catalog entry is resolved.  Resolve it before the
        # exact-world Activity binding so run(SubmittedMission) can recover
        # without an artificial second submit().
        await self._world.info()
        await self._ensure_activity()

        for _ in range(limit):
            await self._world.step()
            assert self._activity is not None
            await self._activity.worker.run_until_idle()

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
            # Reviewer infrastructure failure never decides a task: once a
            # current candidate's whole committed review budget is consumed
            # with no matching independent receipt, no further review can be
            # admitted, so report the still-pending candidate now instead of
            # waiting out the remaining tick budget (docs/guide/
            # agent-missions.md, runtime.md).
            pending = project_pending_review_exhaustion(self._view, mission.mission_id)
            if pending:
                raise CriticReviewBudgetExhaustedError(mission.mission_id, pending)

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
        if self._activity is not None:
            await self._activity.aclose()
        self._closed = True

    async def bind_activity(self) -> None:
        """Bind exact-world recovery before a cold writer is reconstructed."""

        await self._ensure_activity()

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

    async def _ensure_activity(self) -> None:
        if self._activity is not None:
            return
        world_id = self._world.active_world_id
        if world_id is None:
            raise RuntimeError("Mission Activity binding requires an activated world")
        binding = await self._activity_factory(str(world_id))
        if binding.world_id != str(world_id):
            await binding.aclose()
            raise ValueError("Mission Activity factory returned another world")
        self._activity = binding

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
        if not self._mission_sandboxes:
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
