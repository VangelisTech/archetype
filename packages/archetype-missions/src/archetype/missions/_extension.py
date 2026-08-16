# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted process-extension composition for the Missions world library."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial
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
    RestoreMissionSandbox,
    RunMission,
    SubmitMission,
    summarize_mission_operation,
)
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
)

_OPERATION_SCOPES: dict[type[BaseModel], Any] = {
    IngestClaudeTranscript: "live_world",
    QueryTranscriptRows: "durable_world",
    QueryTrajectory: "durable_world",
    GradeTrajectory: "durable_world",
    SubmitMission: "application",
    RunMission: "application",
    RestoreMissionSandbox: "application",
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

        async def construct() -> MissionService:
            nonlocal cold_constructed
            cold_world_id: str | None = None
            if isinstance(operation, RunMission):
                cold_world_id = operation.mission.world_id.strip()
                if not cold_world_id:
                    raise ValueError("cold Mission run requires SubmittedMission.world_id")
                cold_constructed = True

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
        if cold_constructed:
            assert isinstance(operation, RunMission)
            world_id = operation.mission.world_id
            storage_config = (
                operation.storage
                if isinstance(operation.storage, StorageConfig)
                else StorageConfig(uri=str(operation.storage))
                if operation.storage is not None
                else StorageConfig()
            )
            await context.worlds.remember_storage_identity(world_id, storage_config)
            # Bind the exact projector before reconstructing the mutable world.
            await service.bind_activity()
            await context.lifecycle.open_world_mutable(storage_config, world_id)

        if isinstance(operation, SubmitMission):
            submission = operation.submission
            return await service.submit(
                repository=submission.repository,
                branch=submission.branch,
                tasks=submission.tasks,
                name=submission.name,
                base_ref=submission.base_ref,
            )
        return await service.run(operation.mission, max_ticks=operation.max_ticks)


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
    }


def install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    """Compose Missions internals and register its seven exact operations."""

    if not isinstance(context, WorldLibraryContext):
        raise TypeError("context must be a WorldLibraryContext")
    if context.config is not None:
        raise TypeError("missions does not accept world-library configuration")

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
