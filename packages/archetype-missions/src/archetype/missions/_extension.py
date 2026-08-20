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
from archetype.errors import WorldNotFoundError
from archetype.missions.activity_binding import MissionActivityBinding
from archetype.missions.activity_coordinator import MissionAuthorActivityCoordinator
from archetype.missions.activity_world import (
    MissionAuthorActivityBinding,
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.missions.api import create_router
from archetype.missions.coding_agents.app_server import CodexAppServerDriver
from archetype.missions.coding_agents.harness import (
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.config import MissionsExtensionConfig
from archetype.missions.critic_activity_coordinator import MissionCriticActivityCoordinator
from archetype.missions.critic_activity_world import (
    MissionCriticActivityBinding,
    WorldMissionCriticObservationStager,
)
from archetype.missions.critics import (
    CodexAppServerCriticDriver,
    CriticActivityCodec,
    CriticHarness,
    CriticHarnessConfig,
)
from archetype.missions.local_activity_values import LocalMissionAuthorValueStore
from archetype.missions.local_critic_activity_values import LocalMissionCriticValueStore
from archetype.missions.modal_author import (
    ModalMissionAuthorExecutor,
    ModalMissionAuthorExecutorConfig,
)
from archetype.missions.modal_critic import (
    ModalMissionCriticExecutor,
    ModalMissionCriticExecutorConfig,
)
from archetype.missions.models import (
    AcceptMissionRun,
    CancelMissionRun,
    GetMissionRun,
    GetMissionRunEvents,
    RestoreMissionSandbox,
    RunMission,
    SubmitMission,
    summarize_mission_operation,
)
from archetype.missions.run_catalog import (
    SqliteMissionRunCatalog,
    mission_run_catalog_path_for,
)
from archetype.missions.run_contracts import (
    ExecutionProfileIdentity,
    MissionRun,
    execution_profile_identity,
)
from archetype.missions.run_lifecycle import MissionRunLifecycle
from archetype.missions.run_supervisor import MissionRunSupervisor
from archetype.missions.runtime import Missions, MissionWorld
from archetype.missions.sandboxes.modal import (
    ModalCodexAppServerConnector,
    ModalSandboxBackend,
    ModalSandboxOperationCapability,
)
from archetype.missions.sandboxes.modal_barrier import ModalProviderStartBarrier
from archetype.missions.sandboxes.service import SandboxService
from archetype.missions.service import MissionService
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
    operation: SubmitMission | RunMission,
) -> Any:
    backend = operation.config.sandbox_backend
    if not isinstance(backend, ModalSandboxBackend):
        raise ValueError("Agent Mission admission requires the Modal sandbox backend in v0.6.0")

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

            sandbox = SandboxService((backend,))
            reservation.bind(sandbox, close=sandbox.shutdown)
            capability = ModalSandboxOperationCapability(backend)
            backend_config = backend.config
            assert backend_config.workspace_name is not None
            assert backend_config.environment_name is not None
            assert backend_config.operation_protocol_epoch is not None
            barrier = ModalProviderStartBarrier(
                workspace_name=backend_config.workspace_name,
                environment_name=backend_config.environment_name,
                app_name=backend_config.app_name,
                protocol_epoch=backend_config.operation_protocol_epoch,
            )
            author_driver = operation.config.driver or CodexAppServerDriver(
                connector=ModalCodexAppServerConnector(),
                model=operation.config.model,
                workspace=operation.config.workspace,
            )
            author_executor = ModalMissionAuthorExecutor(
                capability=capability,
                barrier=barrier,
                harness=CodingAgentHarness(
                    author_driver,
                    CodingAgentHarnessConfig(workspace=operation.config.workspace),
                ),
                redactor=context.redaction,
                config=ModalMissionAuthorExecutorConfig(
                    sandbox_environment=operation.config.sandbox_environment,
                    workspace=operation.config.workspace,
                    checkpoint_after_dispatch=operation.config.checkpoint_after_dispatch,
                ),
                observer=operation.config.on_sandbox_event,
            )
            critic_driver = operation.config.critic_driver or CodexAppServerCriticDriver(
                connector=ModalCodexAppServerConnector(),
                workspace=operation.config.critic_workspace,
            )
            critic_executor = ModalMissionCriticExecutor(
                capability=capability,
                barrier=barrier,
                harness=CriticHarness(
                    critic_driver,
                    CriticHarnessConfig(workspace=operation.config.critic_workspace),
                ),
                redactor=context.redaction,
                config=ModalMissionCriticExecutorConfig(
                    sandbox_environment=operation.config.sandbox_environment,
                    workspace=operation.config.critic_workspace,
                ),
            )

            async def bind_mission_activity(world_id: str) -> MissionActivityBinding:
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
                author = MissionAuthorActivityBinding(
                    world_id=world_id,
                    owner=f"mission-author:{reservation.owner}",
                    reader=reader,
                    catalog=MissionAuthorActivityCoordinator(coordinator),
                    values=LocalMissionAuthorValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-author-values"),
                        redactor=context.redaction,
                    ),
                    executor=author_executor,
                    stager=WorldMissionAuthorObservationStager(
                        storage=context.storage,
                        registry=context.worlds,
                    ),
                )
                critic = MissionCriticActivityBinding(
                    world_id=world_id,
                    owner=f"mission-critic:{reservation.owner}",
                    reader=reader,
                    catalog=MissionCriticActivityCoordinator(coordinator),
                    values=LocalMissionCriticValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-critic-values"),
                        codec=CriticActivityCodec(context.redaction),
                    ),
                    executor=critic_executor,
                    stager=WorldMissionCriticObservationStager(
                        storage=context.storage,
                        registry=context.worlds,
                    ),
                )
                binding: MissionActivityBinding

                async def close_binding() -> None:
                    await physical.close()
                    await context.required_projectors.unbind(world_id, binding)

                binding = MissionActivityBinding(
                    world_id=world_id,
                    author=author,
                    critic=critic,
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
                    return recovered
            submission = operation.submission
            return await service.submit(
                repository=submission.repository,
                branch=submission.branch,
                tasks=submission.tasks,
                name=submission.name,
                base_ref=submission.base_ref,
            )
        return await service.run(operation.mission, max_ticks=operation.max_ticks)


class _DispatchedMissionRunExecutor:
    """Invoke governed SubmitMission/RunMission without a second scheduler."""

    def __init__(
        self,
        context: WorldLibraryContext,
        *,
        owner_id: str,
        name: str,
        config: Any,
        storage: str | Path | StorageConfig | None,
    ) -> None:
        self._context = context
        self._owner_id = owner_id
        self._name = name
        self._config = config
        self._storage = storage

    def _mission_config(self, run: MissionRun) -> Any:
        """Return the process-bound config or materialize the pinned profile.

        REST-accepted runs carry no caller config; the host-owned execution
        profile bound through ``world_library_configs`` composes the live
        ``AgentMissionConfig`` for the exact pinned identity. An unbound host
        fails here and supervision records an honest failed run instead of
        fabricating provider work.
        """

        if self._config is not None:
            return self._config
        config = self._context.config
        if not isinstance(config, MissionsExtensionConfig):
            raise RuntimeError(
                "mission run execution requires host-bound execution profiles"
            )
        binding = config.execution_profiles.resolve(
            run.profile.profile_id,
            version=run.profile.version,
            digest=run.profile.digest,
        )
        return binding.build_config()

    async def submit(self, run: MissionRun) -> Any:
        return await self._context.resources.dispatcher.apply(
            SubmitMission(
                owner_id=self._owner_id,
                name=f"mission-run:{run.run_id}",
                config=self._mission_config(run),
                storage=self._storage,
                submission=run.submission,
                predetermined_world_id=run.world_id,
            )
        )

    async def load_existing(self, run: MissionRun) -> Any:
        del run
        reservation = self._context.resources.owner(self._owner_id)
        try:
            service = reservation.require_bound()
        except RuntimeError:
            return None
        recover = getattr(service, "recover_submitted", None)
        if recover is None:
            return None
        return await recover()

    async def run(self, run: MissionRun, mission: Any) -> Any:
        return await self._context.resources.dispatcher.apply(
            RunMission(
                owner_id=self._owner_id,
                name=self._name,
                config=self._mission_config(run),
                storage=self._storage,
                mission=mission,
            )
        )

    async def reconcile(self, run: MissionRun) -> Any:
        del run
        return "retry"

    async def active_activity(self, run: MissionRun) -> tuple[str, str] | None:
        if run.active_activity_kind and run.active_activity_id:
            return (run.active_activity_kind, run.active_activity_id)
        return None


type _RunControlOperation = (
    AcceptMissionRun | GetMissionRun | CancelMissionRun | GetMissionRunEvents
)


def _run_owner_reservation(context: WorldLibraryContext, owner_id: str) -> Any:
    """Resolve or lazily reserve the run-control owner for this process.

    Trusted ``Missions`` handles reserve their owner at construction. The
    REST control surface dispatches under one stable host owner id with no
    adapter; reservation happens synchronously on first use so restarts
    reuse the same durable catalog under the same process owner.
    """

    try:
        return context.resources.owner(owner_id)
    except KeyError:
        return context.resources.reserve_owner(
            owner_id,
            phase="workflow-handles",
            closed_message="mission-run control owner is closed",
        )


def _run_control(
    context: WorldLibraryContext,
    reservation: Any,
    operation: _RunControlOperation,
) -> tuple[MissionRunLifecycle, MissionRunSupervisor, SqliteMissionRunCatalog]:
    existing = getattr(reservation, "_mission_run_control", None)
    if existing is not None:
        return existing
    catalog = SqliteMissionRunCatalog(
        mission_run_catalog_path_for(
            _coerce_storage(operation.storage),
            context.control_catalog_config,
        )
    )
    lifecycle = MissionRunLifecycle(catalog)
    supervisor = MissionRunSupervisor(
        lifecycle,
        _DispatchedMissionRunExecutor(
            context,
            owner_id=operation.owner_id,
            name=operation.name,
            config=operation.config,
            storage=operation.storage,
        ),
        spawn=lambda factory, label: reservation.spawn(factory, label=label),
    )
    control = (lifecycle, supervisor, catalog)
    reservation.retain_anchor(control)
    object.__setattr__(reservation, "_mission_run_control", control)
    return control


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
    operation: AcceptMissionRun,
) -> Any:
    reservation = _run_owner_reservation(context, operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        lifecycle, supervisor, _catalog = _run_control(context, reservation, operation)
        run = await lifecycle.accept(
            operation.request,
            _accepted_profile_identity(operation),
        )
        supervisor.ensure(run)
        return run


async def _handle_get_mission_run(
    context: WorldLibraryContext,
    operation: GetMissionRun,
) -> Any:
    reservation = _run_owner_reservation(context, operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        lifecycle, supervisor, _catalog = _run_control(context, reservation, operation)
        run = await lifecycle.get(operation.run_id)
        supervisor.ensure(run)
        return await lifecycle.get(operation.run_id)


async def _handle_cancel_mission_run(
    context: WorldLibraryContext,
    operation: CancelMissionRun,
) -> Any:
    reservation = _run_owner_reservation(context, operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        lifecycle, supervisor, _catalog = _run_control(context, reservation, operation)
        run = await lifecycle.get(operation.run_id)
        run = await lifecycle.record_cancellation_intent(run, reason=operation.reason)
        supervisor.ensure(run)
        return await lifecycle.get(operation.run_id)


async def _handle_get_mission_run_events(
    context: WorldLibraryContext,
    operation: GetMissionRunEvents,
) -> Any:
    reservation = _run_owner_reservation(context, operation.owner_id)
    async with context.resources.admit_owner_operation(reservation):
        lifecycle, _supervisor, _catalog = _run_control(context, reservation, operation)
        return await lifecycle.events(
            operation.run_id,
            after=operation.after,
            limit=operation.limit,
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
        SubmitMission: cast(Any, partial(_handle_mission, context)),
        RunMission: cast(Any, partial(_handle_mission, context)),
        RestoreMissionSandbox: cast(
            Any,
            partial(_handle_restore_mission_sandbox, context),
        ),
        AcceptMissionRun: cast(Any, partial(_handle_accept_mission_run, context)),
        GetMissionRun: cast(Any, partial(_handle_get_mission_run, context)),
        CancelMissionRun: cast(Any, partial(_handle_cancel_mission_run, context)),
        GetMissionRunEvents: cast(Any, partial(_handle_get_mission_run_events, context)),
    }


def install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    """Compose Missions internals and register its exact operations."""

    if not isinstance(context, WorldLibraryContext):
        raise TypeError("context must be a WorldLibraryContext")
    config = context.config
    if config is not None and not isinstance(config, MissionsExtensionConfig):
        raise TypeError("missions config must be a MissionsExtensionConfig")

    transcripts = TranscriptIngestionService(
        context.redaction,
        context.storage,
        context.artifact_store_config,
    )
    trajectories = TrajectoryService(context.storage)
    handlers = _operation_handlers(context, transcripts, trajectories)

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
    )


MANIFEST = WorldLibraryManifest(
    name="missions",
    distribution="archetype-missions",
    version="0.6.1",
    requires_framework=">=0.6,<0.7",
    operation_models=MISSION_OPERATION_MODELS,
    install=install,
    api_router_factories=(create_router,),
)


def get_manifest() -> WorldLibraryManifest:
    """Return the immutable Missions extension declaration."""

    return MANIFEST


__all__ = ["MANIFEST", "MISSION_OPERATION_MODELS", "get_manifest", "install"]
