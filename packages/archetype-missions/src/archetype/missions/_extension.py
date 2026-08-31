# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted process-extension composition for the Missions world library."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel

from archetype.activities import ActivityCoordinator
from archetype.commands.registry import OperationSpec
from archetype.core.config import StorageConfig
from archetype.errors import ConflictError, WorldNotFoundError
from archetype.missions.activity_world import (
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.missions.api import create_router
from archetype.missions.config import (
    MissionsExtensionConfig,
    installed_execution_profiles,
)
from archetype.missions.contracts import MissionSubmission, SubmittedMission
from archetype.missions.critic_activity_world import (
    WorldMissionCriticObservationStager,
)
from archetype.missions.models import (
    AcceptMissionRun,
    CancelMissionRun,
    GetMissionRun,
    GetMissionRunEvents,
    ListMissionRuns,
    RestoreMissionSandbox,
    RunMission,
    SubmitMission,
    summarize_mission_operation,
)
from archetype.missions.run_contracts import (
    ExecutionProfileIdentity,
    MissionRun,
    MissionRunCleanupState,
    MissionRunEvent,
    MissionRunNotFoundError,
    MissionRunStatus,
    execution_profile_identity,
    mission_result_from_json,
    submission_from_json,
)
from archetype.missions.runtime import Missions, MissionWorld
from archetype.missions.sandboxes.modal import ModalSandboxBackend
from archetype.missions.sandboxes.service import SandboxService
from archetype.missions.service import MissionService
from archetype.missions.temporal.activity_runtime import (
    MissionTemporalActivityBinding,
    MissionTemporalAuthorActivityCatalog,
    MissionTemporalCriticActivityCatalog,
)
from archetype.missions.temporal.client import MissionTemporalClient
from archetype.missions.temporal.contracts import MissionWorkflowEvent, MissionWorkflowState
from archetype.missions.temporal.modal_job_client import MissionModalJobWorkflowLauncher
from archetype.missions.temporal.worker import create_mission_worker
from archetype.missions.trajectories.models import (
    GradeTrajectory,
    IngestClaudeTranscript,
    QueryTrajectory,
    QueryTranscriptRows,
    summarize_trajectory_operation,
)
from archetype.missions.trajectory_service import TrajectoryService
from archetype.missions.transcript_service import TranscriptIngestionService
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    activity_catalog_path_for,
)
from archetype.world import query
from archetype.world.cleanup import WorldCleanup
from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryContext,
    WorldLibraryManifest,
)

MISSION_OPERATION_MODELS: tuple[type[BaseModel], ...] = (
    IngestClaudeTranscript,
    QueryTranscriptRows,
    QueryTrajectory,
    GradeTrajectory,
    SubmitMission,
    RunMission,
    RestoreMissionSandbox,
    AcceptMissionRun,
    GetMissionRun,
    CancelMissionRun,
    GetMissionRunEvents,
    ListMissionRuns,
)

_OPERATION_SCOPES: dict[type[BaseModel], Any] = {
    IngestClaudeTranscript: "live_world",
    QueryTranscriptRows: "durable_world",
    QueryTrajectory: "durable_world",
    GradeTrajectory: "durable_world",
    SubmitMission: "application",
    RunMission: "application",
    RestoreMissionSandbox: "application",
    AcceptMissionRun: "application",
    GetMissionRun: "application",
    CancelMissionRun: "application",
    GetMissionRunEvents: "application",
    ListMissionRuns: "application",
}


def _operation_name(model: type[BaseModel]) -> str:
    value = model.model_fields["operation"].default
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{model.__name__} has no fixed operation discriminator")
    return value


def _world_key(operation: BaseModel) -> object:
    return cast(Any, operation).world_id


async def _handle_ingest_claude_transcript(
    service: TranscriptIngestionService,
    operation: IngestClaudeTranscript,
) -> Any:
    return await service.ingest(
        str(operation.world_id),
        operation.source,
        storage_config=operation.storage_config,
    )


async def _handle_query_transcript_rows(
    service: TranscriptIngestionService,
    operation: QueryTranscriptRows,
) -> Any:
    return await service.read(
        str(operation.world_id),
        storage_config=operation.storage_config,
    )


async def _resolve_storage(
    context: WorldLibraryContext,
    world_id: object,
    storage_config: StorageConfig | None,
) -> StorageConfig | None:
    if storage_config is not None:
        return storage_config
    record = await context.worlds.storage_record(str(world_id))
    return record[0] if record is not None else None


async def _resolve_lineage(
    context: WorldLibraryContext,
    world_id: object,
    run_id: object,
    storage_config: StorageConfig | None,
) -> list[tuple[str, str, int]] | None:
    try:
        async with context.worlds.operation(str(world_id)) as world:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
    except KeyError:
        return await query.get_lineage(
            context.storage,
            str(world_id),
            str(run_id),
            storage_config,
        )


async def _handle_query_trajectory(
    context: WorldLibraryContext,
    service: TrajectoryService,
    operation: QueryTrajectory,
) -> Any:
    storage_config = await _resolve_storage(
        context,
        operation.world_id,
        operation.storage_config,
    )
    return await service.query(
        operation.component,
        world_id=str(operation.world_id),
        run_id=str(operation.run_id),
        storage_config=storage_config,
        lineage=await _resolve_lineage(
            context,
            operation.world_id,
            operation.run_id,
            storage_config,
        ),
        selection=operation.selection,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
    )


async def _handle_grade_trajectory(
    context: WorldLibraryContext,
    service: TrajectoryService,
    operation: GradeTrajectory,
) -> Any:
    storage_config = await _resolve_storage(
        context,
        operation.world_id,
        operation.storage_config,
    )
    return await service.grade(
        operation.component,
        world_id=str(operation.world_id),
        run_id=str(operation.run_id),
        graders=operation.graders,
        storage_config=storage_config,
        lineage=await _resolve_lineage(
            context,
            operation.world_id,
            operation.run_id,
            storage_config,
        ),
        selection=operation.selection,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
    )


def _mission_world_factory(
    context: WorldLibraryContext,
    *,
    world_id: str | None = None,
    install_initializers: bool = False,
) -> Callable[..., Any]:
    def create(*args: Any, **kwargs: Any) -> Any:
        return context.runtime_world_factory(
            *args,
            world_id=world_id,
            install_initializers=install_initializers,
            **kwargs,
        )

    return create


def _coerce_storage(storage: str | Path | StorageConfig | None) -> StorageConfig:
    if isinstance(storage, StorageConfig):
        return storage
    if storage is not None:
        return StorageConfig(uri=str(storage))
    return StorageConfig()


async def _world_recorded(
    context: WorldLibraryContext,
    world_id: str,
    storage_config: StorageConfig,
) -> bool:
    catalog = context.storage.get_control_catalog(storage_config)
    return await catalog.get_world(world_id) is not None


async def _create_run_world(
    context: WorldLibraryContext,
    *,
    world_id: str,
    name: str,
    storage_config: StorageConfig,
) -> None:
    from archetype.core.config import WorldConfig

    await context.lifecycle.create_world(
        WorldConfig(world_id=world_id, name=name),
        storage_config,
    )


async def _handle_mission(
    context: WorldLibraryContext,
    extension_config: MissionsExtensionConfig,
    operation: SubmitMission | RunMission,
) -> Any:
    if not isinstance(operation.config.sandbox_backend, ModalSandboxBackend):
        raise ValueError("Agent Mission admission requires the Modal sandbox backend in v0.6.0")
    temporal = extension_config.temporal_activities
    if temporal is None:
        raise RuntimeError(
            "Missions requires Temporal activity routing; legacy activity execution is removed"
        )

    reservation = context.resources.owner(operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        cold_constructed = False
        created_predetermined = False
        storage_config = _coerce_storage(operation.storage)
        intended_world_id = ""
        if isinstance(operation, RunMission):
            intended_world_id = operation.mission.world_id.strip()
            if not intended_world_id:
                raise ValueError("cold Mission run requires SubmittedMission.world_id")
        elif operation.predetermined_world_id.strip():
            intended_world_id = operation.predetermined_world_id.strip()
            if not await _world_recorded(context, intended_world_id, storage_config):
                await _create_run_world(
                    context,
                    world_id=intended_world_id,
                    name=operation.name,
                    storage_config=storage_config,
                )
                created_predetermined = True

        async def construct() -> MissionService:
            nonlocal cold_constructed
            cold_world_id: str | None = None
            if intended_world_id:
                cold_world_id = intended_world_id
                cold_constructed = not created_predetermined

            sandbox = SandboxService((operation.config.sandbox_backend,))
            reservation.bind(sandbox, close=sandbox.shutdown)

            async def bind_mission_activity(
                world_id: str,
            ) -> MissionTemporalActivityBinding:
                storage_record = await context.worlds.storage_record(world_id)
                if storage_record is None:
                    raise WorldNotFoundError(world_id)
                storage_config = storage_record[0]
                catalog_path = activity_catalog_path_for(
                    storage_config,
                    context.control_catalog_config,
                )
                physical = SqliteActivityCatalog(catalog_path)
                coordinator = ActivityCoordinator(physical)
                reader = StorageMissionCommittedIntentReader(
                    context.storage,
                    storage_config,
                )
                author_stager = WorldMissionAuthorObservationStager(
                    storage=context.storage,
                    registry=context.worlds,
                )
                critic_stager = WorldMissionCriticObservationStager(
                    storage=context.storage,
                    registry=context.worlds,
                )
                binding: MissionTemporalActivityBinding

                async def close_binding() -> None:
                    await physical.close()
                    await context.required_projectors.unbind(world_id, binding)

                values = temporal.values
                workflows = cast(MissionModalJobWorkflowLauncher, temporal.workflows)
                temporal_author = MissionTemporalAuthorActivityCatalog(
                    index=coordinator,
                    workflows=workflows,
                    values=values,
                    namespace_digest=temporal.namespace_digest,
                )
                temporal_critic = MissionTemporalCriticActivityCatalog(
                    index=coordinator,
                    workflows=workflows,
                    values=values,
                    namespace_digest=temporal.namespace_digest,
                )
                binding = MissionTemporalActivityBinding(
                    world_id=world_id,
                    reader=reader,
                    author=temporal_author,
                    critic=temporal_critic,
                    author_values=values.author,
                    critic_values=values.critic,
                    author_stager=author_stager,
                    critic_stager=critic_stager,
                    close=close_binding,
                )
                reservation.retain_anchor(binding)
                await context.required_projectors.bind(world_id, binding)
                try:
                    routed = context.required_projectors.required_projector_for(world_id)
                    if (
                        await context.worlds.live_world(world_id) is not None
                        and context.worlds.required_projector(world_id) is not routed
                    ):
                        await context.worlds.bind_required_projector(world_id, routed)
                except BaseException:
                    await context.required_projectors.unbind(world_id, binding)
                    await physical.close()
                    raise
                return binding

            async def cleanup_factory(world_id: object) -> WorldCleanup:
                lease = await context.lifecycle.begin_close(str(world_id))
                return WorldCleanup(
                    registry=context.worlds,
                    lifecycle=context.lifecycle,
                    world_id=str(world_id),
                    lease=lease,
                    cancel_unsettled=context.scheduler.cancel_world,
                )

            return MissionService(
                world_factory=_mission_world_factory(
                    context,
                    world_id=cold_world_id,
                    install_initializers=cold_world_id is not None,
                ),
                name=operation.name,
                config=operation.config,
                sandbox_service=sandbox,
                redaction_service=context.redaction,
                cleanup_factory=cleanup_factory,
                activity_factory=bind_mission_activity,
                storage=operation.storage,
            )

        service = await reservation.construct(construct)
        if intended_world_id:
            await context.worlds.remember_storage_identity(intended_world_id, storage_config)
            await service.bind_activity()
            if cold_constructed:
                await context.lifecycle.open_world_mutable(storage_config, intended_world_id)

        if isinstance(operation, SubmitMission):
            if intended_world_id:
                recovered = await service.recover_submitted()
                if recovered is not None:
                    return _require_recovered_submission_matches(
                        recovered,
                        operation.submission,
                    )
            submission = operation.submission
            return await service.submit(
                repository=submission.repository,
                branch=submission.branch,
                tasks=submission.tasks,
                name=submission.name,
                base_ref=submission.base_ref,
            )
        return await service.run(operation.mission, max_ticks=operation.max_ticks)


def _require_recovered_submission_matches(
    recovered: SubmittedMission,
    submission: MissionSubmission,
) -> SubmittedMission:
    """Refuse recovery evidence that does not correspond to the request.

    ``recover_submitted`` trusts the predetermined world alone; comparing the
    recovered repository coordinates and task-name inventory against the
    requested submission keeps a reused world id from silently binding a
    different mission.
    """

    expected = (
        submission.repository,
        submission.branch,
        submission.base_ref,
        tuple(sorted(task.name for task in submission.tasks)),
    )
    actual = (
        recovered.repository,
        recovered.branch,
        recovered.base_ref,
        tuple(sorted(name for name, _task_id in recovered.task_ids)),
    )
    if expected != actual:
        raise ConflictError(
            "recovered Mission evidence does not correspond to the requested submission"
        )
    return recovered


class _TemporalMissionExecutor:
    """Effect adapter retained by Temporal, with no local lifecycle authority."""

    def __init__(self, context: WorldLibraryContext) -> None:
        self._context = context

    @staticmethod
    def _owner_id(run: MissionRun) -> str:
        return f"mission-temporal:{run.run_id}"

    def prepare(self, run: MissionRun) -> None:
        owner_id = self._owner_id(run)
        try:
            self._context.resources.owner(owner_id)
        except KeyError:
            self._context.resources.reserve_owner(
                owner_id,
                phase="workflow-handles",
                closed_message=f"Temporal Mission {run.run_id!r} is closed",
            )

    def _config(self, run: MissionRun) -> Any:
        installed = self._context.resources.world_library("missions")
        return (
            installed_execution_profiles(installed)
            .resolve(
                run.profile.profile_id,
                version=run.profile.version,
                digest=run.profile.digest,
            )
            .build_config()
        )

    async def load_existing(self, run: MissionRun) -> SubmittedMission | None:
        try:
            reservation = self._context.resources.owner(self._owner_id(run))
        except KeyError:
            return None
        service = reservation.require_bound()
        recovered = await service.recover_submitted()
        return (
            None
            if recovered is None
            else _require_recovered_submission_matches(recovered, run.submission)
        )

    async def submit(self, run: MissionRun) -> SubmittedMission:
        self.prepare(run)
        return await self._context.resources.dispatcher.apply(
            SubmitMission(
                owner_id=self._owner_id(run),
                name=f"mission:{run.run_id}",
                config=self._config(run),
                submission=run.submission,
                predetermined_world_id=run.world_id,
            )
        )

    async def run(self, run: MissionRun, mission: SubmittedMission) -> Any:
        self.prepare(run)
        return await self._context.resources.dispatcher.apply(
            RunMission(
                owner_id=self._owner_id(run),
                name=f"mission:{run.run_id}",
                config=self._config(run),
                mission=mission,
            )
        )


def _accepted_profile_identity(operation: AcceptMissionRun) -> Any:
    if operation.profile_id or operation.profile_version or operation.profile_digest:
        return ExecutionProfileIdentity(
            profile_id=operation.profile_id,
            version=operation.profile_version,
            digest=operation.profile_digest,
        )
    if operation.config is None:
        raise ValueError("AcceptMissionRun requires a pinned execution profile or a mission config")
    return execution_profile_identity(operation.config)


async def _handle_accept_mission_run(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    operation: AcceptMissionRun,
) -> Any:
    _ensure_temporal_mission_workers(context, config)
    client = _require_temporal_run_client(config)
    handle = await client.start(operation.request, _accepted_profile_identity(operation))
    return await _temporal_run(handle, operation.request.submission)


async def _handle_get_mission_run(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    operation: GetMissionRun,
) -> Any:
    _ensure_temporal_mission_workers(context, config)
    client = _require_temporal_run_client(config)
    handle = client.get(operation.run_id)
    state = await handle.query("state")
    if state is None:
        raise MissionRunNotFoundError(operation.run_id)
    return await _temporal_run(handle, submission_from_json(state.submission_json))


async def _handle_cancel_mission_run(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    operation: CancelMissionRun,
) -> Any:
    _ensure_temporal_mission_workers(context, config)
    client = _require_temporal_run_client(config)
    handle = client.get(operation.run_id)
    reason = (
        context.redaction.redact_text(operation.reason, scope="mission-run-control").text
        if operation.reason
        else ""
    )
    await handle.signal("request_cancel", reason)
    state = await handle.query("state")
    if state is None:
        raise MissionRunNotFoundError(operation.run_id)
    return await _temporal_run(handle, submission_from_json(state.submission_json))


async def _handle_get_mission_run_events(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    operation: GetMissionRunEvents,
) -> Any:
    _ensure_temporal_mission_workers(context, config)
    client = _require_temporal_run_client(config)
    events = await client.get(operation.run_id).query("events")
    return tuple(
        MissionRunEvent(
            run_id=operation.run_id,
            cursor=event.cursor,
            event_type=event.event_type,
            phase=event.phase,
            payload_json="{}",
            created_at_ms=event.created_at_ms,
        )
        for event in events
        if event.cursor > operation.after
    )[: operation.limit]


async def _handle_list_mission_runs(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    operation: ListMissionRuns,
) -> Any:
    _ensure_temporal_mission_workers(context, config)
    client = _require_temporal_run_client(config)
    handles = await client.list_for_principal(
        operation.owner_principal,
        limit=operation.limit,
    )
    runs: list[MissionRun] = []
    for handle in handles:
        state = await handle.query("state")
        if state is not None:
            runs.append(await _temporal_run(handle, submission_from_json(state.submission_json)))
    return tuple(runs)


def _require_temporal_run_client(config: MissionsExtensionConfig) -> MissionTemporalClient:
    client = config.temporal_runs
    if client is None:
        raise RuntimeError("Missions requires a Temporal run client; legacy MissionRun is removed")
    return client


async def _temporal_run(handle: Any, submission: MissionSubmission) -> MissionRun:
    state = cast(MissionWorkflowState | None, await handle.query("state"))
    if state is None:
        raise MissionRunNotFoundError(str(getattr(handle, "id", "unknown")))
    events = cast(tuple[MissionWorkflowEvent, ...], await handle.query("events"))
    terminal = state.status in {"succeeded", "failed", "cancelled"}
    timestamps = {event.event_type: event.created_at_ms for event in events}
    result = mission_result_from_json(state.result_json) if state.result_json else None
    return MissionRun(
        run_id=state.run_id,
        principal=state.principal,
        idempotency_key=state.idempotency_key,
        request_digest=state.request_digest,
        profile=ExecutionProfileIdentity(
            profile_id=state.profile_id,
            version=state.profile_version,
            digest=state.profile_digest,
        ),
        status=MissionRunStatus(state.status),
        submission=submission,
        world_id=state.world_id,
        active_operation=state.active_operation,
        cancellation_intent=state.cancellation_requested,
        cancellation_reason=state.cancellation_reason,
        result=result,
        cleanup_state=MissionRunCleanupState.NONE,
        accepted_at_ms=timestamps.get("accepted", 0),
        running_at_ms=timestamps.get("running"),
        terminal_at_ms=(timestamps.get(state.status) if terminal else None),
        updated_at_ms=(events[-1].created_at_ms if events else 0),
        interrupted_reason=state.failure_reason,
    )


async def _handle_restore_mission_sandbox(
    context: WorldLibraryContext,
    operation: RestoreMissionSandbox,
) -> Any:
    reservation = context.resources.owner(operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        service = cast(MissionService, reservation.require_bound())
        return await service.restore_sandbox(operation.mission, operation.checkpoint)


def _operation_handlers(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
    transcripts: TranscriptIngestionService,
    trajectories: TrajectoryService,
) -> dict[type[BaseModel], Callable[[BaseModel], Awaitable[Any]]]:
    return {
        IngestClaudeTranscript: cast(
            Any,
            partial(_handle_ingest_claude_transcript, transcripts),
        ),
        QueryTranscriptRows: cast(
            Any,
            partial(_handle_query_transcript_rows, transcripts),
        ),
        QueryTrajectory: cast(
            Any,
            partial(_handle_query_trajectory, context, trajectories),
        ),
        GradeTrajectory: cast(
            Any,
            partial(_handle_grade_trajectory, context, trajectories),
        ),
        SubmitMission: cast(Any, partial(_handle_mission, context, config)),
        RunMission: cast(Any, partial(_handle_mission, context, config)),
        RestoreMissionSandbox: cast(
            Any,
            partial(_handle_restore_mission_sandbox, context),
        ),
        AcceptMissionRun: cast(Any, partial(_handle_accept_mission_run, context, config)),
        GetMissionRun: cast(Any, partial(_handle_get_mission_run, context, config)),
        CancelMissionRun: cast(Any, partial(_handle_cancel_mission_run, context, config)),
        GetMissionRunEvents: cast(
            Any,
            partial(_handle_get_mission_run_events, context, config),
        ),
        ListMissionRuns: cast(Any, partial(_handle_list_mission_runs, context, config)),
    }


def _ensure_temporal_mission_workers(
    context: WorldLibraryContext,
    config: MissionsExtensionConfig,
) -> None:
    """Lazily start host-owned Workers from admitted async Mission ingress."""

    client = config.temporal_runs
    if client is None:
        return
    owner_id = "missions-temporal-worker"
    try:
        reservation = context.resources.owner(owner_id)
    except KeyError:
        reservation = context.resources.reserve_owner(
            owner_id,
            phase="workflow-handles",
            closed_message="Temporal Mission Worker is closed",
        )
    if getattr(reservation, "_mission_temporal_worker", None) is not None:
        return
    worker = create_mission_worker(
        client.client,
        _TemporalMissionExecutor(context),
        task_queue=client.task_queue,
    )
    workers = (worker, *config.temporal_workers)
    for index, owned_worker in enumerate(workers):
        reservation.retain_anchor(owned_worker)
        reservation.spawn(
            cast(Any, owned_worker).run,
            label=f"mission-temporal-worker-{index}",
        )
    object.__setattr__(reservation, "_mission_temporal_worker", worker)


def install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    """Compose Missions internals and register its exact operations."""

    if not isinstance(context, WorldLibraryContext):
        raise TypeError("context must be a WorldLibraryContext")
    config = context.config
    if config is not None and not isinstance(config, MissionsExtensionConfig):
        raise TypeError("missions config must be a MissionsExtensionConfig")
    if config is None:
        config = MissionsExtensionConfig()

    transcripts = TranscriptIngestionService(
        context.redaction,
        context.storage,
        context.artifact_store_config,
    )
    trajectories = TrajectoryService(context.storage)
    handlers = _operation_handlers(context, config, transcripts, trajectories)
    if set(handlers) != set(MISSION_OPERATION_MODELS):
        raise RuntimeError("Missions operation composition is incomplete")
    for model in MISSION_OPERATION_MODELS:
        name = _operation_name(model)
        scope = _OPERATION_SCOPES[model]
        context.registry.register(
            OperationSpec(
                name=name,
                model=model,
                handler=handlers[model],
                permission=name,
                summarize=(
                    cast(Any, summarize_trajectory_operation)
                    if model
                    in {
                        IngestClaudeTranscript,
                        QueryTranscriptRows,
                        QueryTrajectory,
                        GradeTrajectory,
                    }
                    else cast(Any, summarize_mission_operation)
                ),
                quota_scope=scope,
                world_key=None if scope == "application" else _world_key,
                durable=None,
                trusted=True,
                untrusted=False,
                token_cost=0,
            )
        )

    return InstalledWorldLibrary(
        name="missions",
        runtime_adapter=Missions,
        world_adapter=MissionWorld,
        config=config,
    )


MANIFEST = WorldLibraryManifest(
    name="missions",
    distribution="archetype-missions",
    version="0.6.3",
    requires_framework=">=0.6,<0.7",
    operation_models=MISSION_OPERATION_MODELS,
    install=install,
    api_router_factories=(create_router,),
)


def get_manifest() -> WorldLibraryManifest:
    """Return the immutable Missions extension declaration."""

    return MANIFEST


__all__ = ["MANIFEST", "MISSION_OPERATION_MODELS", "get_manifest", "install"]
