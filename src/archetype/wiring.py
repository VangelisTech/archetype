# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single explicit process composition transaction."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, cast

from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.activities import ActivityCoordinator
from archetype.artifacts import handlers as artifact_handlers
from archetype.artifacts.models import (
    ArtifactStoreConfig,
    IngestArtifacts,
    QueryArtifacts,
    summarize_artifact_operation,
)
from archetype.commands.audit import AuditLog
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import GetAuditHistory
from archetype.commands.policy import Policy
from archetype.commands.registry import (
    DurableOperation,
    OperationRegistry,
    OperationSpec,
)
from archetype.commands.scheduler import CommandScheduler
from archetype.core.config import StorageConfig
from archetype.episodes.models import (
    GradeTrajectory,
    IngestClaudeTranscript,
    QueryTrajectory,
    QueryTranscriptRows,
    summarize_episode_operation,
)
from archetype.errors import WorldNotFoundError
from archetype.evaluation import handlers as evaluation_handlers
from archetype.evaluation.models import (
    Evaluate,
    RunGraders,
    summarize_evaluation_operation,
)
from archetype.missions.activity_binding import MissionActivityBinding
from archetype.missions.activity_coordinator import (
    MissionAuthorActivityCoordinator,
)
from archetype.missions.activity_world import (
    MissionAuthorActivityBinding,
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.missions.coding_agents.harness import (
    CodexDriver,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.critic_activity_coordinator import (
    MissionCriticActivityCoordinator,
)
from archetype.missions.critic_activity_world import (
    MissionCriticActivityBinding,
    WorldMissionCriticObservationStager,
)
from archetype.missions.critics import (
    CodexCriticDriver,
    CriticActivityCodec,
    CriticHarness,
    CriticHarnessConfig,
)
from archetype.missions.local_activity_values import (
    LocalMissionAuthorValueStore,
)
from archetype.missions.local_critic_activity_values import (
    LocalMissionCriticValueStore,
)
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
from archetype.missions.sandboxes.modal import (
    ModalSandboxBackend,
    ModalSandboxOperationCapability,
)
from archetype.missions.sandboxes.modal_barrier import ModalProviderStartBarrier
from archetype.missions.sandboxes.service import SandboxService
from archetype.missions.service import MissionService
from archetype.missions.trajectory_service import TrajectoryService
from archetype.missions.transcript_service import TranscriptIngestionService
from archetype.physical_ai import hosted_workflow as physical_ai_handlers
from archetype.physical_ai.hosted_activities import PhysicalHostedActivityCoordinator
from archetype.physical_ai.hosted_activity_contracts import HostedEpisodeProvider
from archetype.physical_ai.hosted_activity_values import LocalHostedEpisodeValueStore
from archetype.physical_ai.hosted_activity_world import (
    PhysicalHostedActivityBinding,
    StoragePhysicalCommittedIntentReader,
    WorldHostedEpisodeObservationStager,
)
from archetype.physical_ai.hosted_modal import (
    ModalHostedEpisodeConfig,
    ModalHostedEpisodeProvider,
)
from archetype.physical_ai.models import (
    RunHostedEpisode,
    summarize_physical_ai_operation,
)
from archetype.redaction.service import RedactionService
from archetype.research import handlers as research_handlers
from archetype.research.models import AutoResearch, summarize_research_operation
from archetype.runtime_resources import OwnerReservation, RuntimeResources
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    activity_catalog_path_for,
)
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world import mutation, query, simulation
from archetype.world.cleanup import WorldCleanup
from archetype.world.handlers import WORLD_OPERATION_HANDLERS, materialize_locked
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
)
from archetype.world.projectors import RequiredProjectorFanout
from archetype.world.registry import WorldRegistry

_APPLICATION_SCOPED_OPERATIONS = frozenset(
    {
        "create_world",
        "discover_worlds",
        "list_signatures",
        "list_worlds",
    }
)
_DURABLE_WORLD_SCOPED_OPERATIONS = frozenset(
    {
        "destroy_world",
        "list_world_signatures",
        "open_world_readonly",
        "query_archetype",
        "query_components",
        "resume_world",
    }
)
_PERMISSION_OVERRIDES = {
    "list_world_signatures": "list_signatures",
    "reserve_entity_ids": "spawn",
    "spawn_reserved": "spawn",
}
_INTERNAL_OPERATIONS = frozenset({"reserve_entity_ids", "spawn_reserved"})
_WORLD_TOKEN_COSTS = {
    "add_components": 8,
    "add_hook": 10,
    "add_processor": 15,
    "add_resource": 10,
    "create_entities": 10,
    "create_world": 50,
    "despawn": 5,
    "destroy_world": 10,
    "discover_worlds": 2,
    "fork_world": 100,
    "get_world_info": 2,
    "list_hooks": 2,
    "list_processors": 2,
    "list_resources": 2,
    "list_signatures": 2,
    "list_world_signatures": 2,
    "list_worlds": 2,
    "open_world_readonly": 2,
    "query_archetype": 5,
    "query_components": 5,
    "remove_components": 5,
    "remove_hook": 5,
    "remove_processor": 5,
    "reserve_entity_ids": 10,
    "resume_world": 50,
    "run": 50,
    "run_episode": 500,
    "run_rollout": 200,
    "spawn": 10,
    "spawn_reserved": 10,
    "step": 10,
    "update": 8,
}
_PULL_FORWARD_MODELS: tuple[type[BaseModel], ...] = (
    IngestArtifacts,
    QueryArtifacts,
    RunGraders,
    Evaluate,
    AutoResearch,
    RunHostedEpisode,
    IngestClaudeTranscript,
    QueryTranscriptRows,
    QueryTrajectory,
    GradeTrajectory,
    SubmitMission,
    RunMission,
    RestoreMissionSandbox,
)
_ACTOR_AWARE_PULL_FORWARD = frozenset(
    {
        "autoresearch",
        "evaluate",
        "ingest_artifacts",
        "query_artifacts",
    }
)
_PULL_FORWARD_SCOPES: dict[str, Literal["application", "live_world", "durable_world"]] = {
    "autoresearch": "live_world",
    "evaluate": "durable_world",
    "grade_trajectory": "durable_world",
    "ingest_artifacts": "durable_world",
    "ingest_claude_transcript": "live_world",
    "query_artifacts": "durable_world",
    "query_trajectory": "durable_world",
    "query_transcript_rows": "durable_world",
    "restore_mission_sandbox": "application",
    "run_graders": "application",
    "run_mission": "application",
    "submit_mission": "application",
    "run_hosted_episode": "live_world",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class RuntimeBootstrapConfig:
    """Fully resolved inputs for one process composition transaction."""

    control_catalog_config: ControlCatalogConfig
    storage_service: StorageService | None = None
    world_registry: WorldRegistry | None = None
    audit_storage_config: StorageConfig | None = None
    artifact_store_config: ArtifactStoreConfig | None = None
    redaction_service: RedactionService | None = None
    required_projector_factory: Callable[[str], Any | None] | None = None
    unsettled_world_oracle: Callable[[str], Awaitable[bool]] | None = None
    hosted_episode_provider_factory: (
        Callable[[ModalHostedEpisodeConfig], HostedEpisodeProvider] | None
    ) = None
    hosted_activity_lease_seconds: float = 300.0

    @classmethod
    def from_env(
        cls,
        *,
        storage_service: StorageService | None = None,
        world_registry: WorldRegistry | None = None,
        audit_storage_config: StorageConfig | None = None,
        artifact_store_config: ArtifactStoreConfig | None = None,
        redaction_service: RedactionService | None = None,
        required_projector_factory: Callable[[str], Any | None] | None = None,
        unsettled_world_oracle: Callable[[str], Awaitable[bool]] | None = None,
        hosted_episode_provider_factory: (
            Callable[[ModalHostedEpisodeConfig], HostedEpisodeProvider] | None
        ) = None,
        hosted_activity_lease_seconds: float = 300.0,
        environ: Mapping[str, str] | None = None,
    ) -> RuntimeBootstrapConfig:
        """Resolve environment-backed configuration once at the host boundary."""

        return cls(
            control_catalog_config=ControlCatalogConfig.from_env(environ),
            storage_service=storage_service,
            world_registry=world_registry,
            audit_storage_config=audit_storage_config,
            artifact_store_config=artifact_store_config,
            redaction_service=redaction_service,
            required_projector_factory=required_projector_factory,
            unsettled_world_oracle=unsettled_world_oracle,
            hosted_episode_provider_factory=hosted_episode_provider_factory,
            hosted_activity_lease_seconds=hosted_activity_lease_seconds,
        )


class _WorldCleanupLifetimes:
    """Retain retryable exact-world cleanup in the process owner."""

    def __init__(
        self,
        resources: RuntimeResources,
        worlds: WorldRegistry,
        lifecycle: WorldLifecycle,
        scheduler: CommandScheduler,
    ) -> None:
        self._resources = resources
        self._worlds = worlds
        self._lifecycle = lifecycle
        self._scheduler = scheduler
        self._entries: dict[object, OwnerReservation] = {}

    async def close_current(self, world_id: object) -> None:
        """Join cleanup for the current exact world selected by public destroy."""

        try:
            lease = await self._lifecycle.begin_close(str(world_id))
        except KeyError:
            return
        reservation = self._entries.get(lease)
        if reservation is None:
            cleanup = WorldCleanup(
                registry=self._worlds,
                lifecycle=self._lifecycle,
                world_id=str(world_id),
                lease=lease,
                cancel_unsettled=self._scheduler.cancel_world,
            )
            reservation = self._resources.reserve_owner(
                f"world-cleanup:{uuid7()}",
                phase="workflow-handles",
                closed_message="world cleanup owner is closed",
            )
            reservation.bind(cleanup, close=cleanup.finish)
            self._entries[lease] = reservation
        try:
            await reservation.aclose()
        finally:
            if reservation.released and self._entries.get(lease) is reservation:
                self._entries.pop(lease)


class _AdmissionGuardedCatalog:
    """Keep durable admission ordered with the exact world's close barrier."""

    def __init__(
        self,
        worlds: WorldRegistry,
        world_id: str,
        delegate: object,
    ) -> None:
        self._worlds = worlds
        self._world_id = world_id
        self._delegate = cast(Any, delegate)

    async def admit_commands(self, world_id: str, admissions: object) -> object:
        if str(world_id) != self._world_id:
            raise ValueError("catalog admission target differs from its bound world")
        try:
            async with self._worlds.operation(self._world_id):
                return await self._delegate.admit_commands(world_id, admissions)
        except KeyError:
            raise WorldNotFoundError(self._world_id) from None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class _AuditRuntimeResource:
    """Close audit projection only after warming every known world catalog."""

    def __init__(self, audit: AuditLog, worlds: WorldRegistry) -> None:
        self._audit = audit
        self._worlds = worlds

    def __getattr__(self, name: str) -> Any:
        return getattr(self._audit, name)

    async def shutdown(self) -> None:
        for world_id in await self._worlds.catalog_world_ids():
            await self._audit.project_outbox(world_id=world_id)
        await self._audit.shutdown()


def _operation_name(model: type[BaseModel]) -> str:
    value = model.model_fields["operation"].default
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{model.__name__} has no fixed operation discriminator")
    return value


def _world_quota_scope(
    operation_name: str,
) -> Literal["application", "live_world", "durable_world"]:
    if operation_name in _APPLICATION_SCOPED_OPERATIONS:
        return "application"
    if operation_name in _DURABLE_WORLD_SCOPED_OPERATIONS:
        return "durable_world"
    return "live_world"


def _world_key(operation: BaseModel) -> object:
    return cast(Any, operation).world_id


def _source_world_key(operation: BaseModel) -> object:
    return cast(Any, operation).source_world_id


def _summarize_world(operation: BaseModel) -> Mapping[str, Any]:
    summary: dict[str, Any] = {"operation": cast(Any, operation).operation}
    for field in ("world_id", "source_world_id"):
        value = getattr(operation, field, None)
        if value is not None:
            summary[field] = str(value)
    return summary


async def _query_audit(audit: AuditLog, operation: GetAuditHistory) -> Any:
    return await audit.query(
        operation.world_id,
        tick_range=operation.tick_range,
        actor_id=operation.actor_id,
        idempotency_key=operation.idempotency_key,
        status=operation.status,
        limit=operation.limit,
    )


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
    worlds: WorldRegistry,
    world_id: object,
    storage_config: StorageConfig | None,
) -> StorageConfig | None:
    if storage_config is not None:
        return storage_config
    record = await worlds.storage_record(str(world_id))
    return record[0] if record is not None else None


async def _resolve_lineage(
    worlds: WorldRegistry,
    storage: StorageService,
    world_id: object,
    run_id: object,
    storage_config: StorageConfig | None,
) -> list[tuple[str, str, int]] | None:
    try:
        async with worlds.operation(str(world_id)) as world:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
    except KeyError:
        return await query.get_lineage(
            storage,
            str(world_id),
            str(run_id),
            storage_config,
        )


async def _handle_query_trajectory(
    service: TrajectoryService,
    worlds: WorldRegistry,
    storage: StorageService,
    operation: QueryTrajectory,
) -> Any:
    storage_config = await _resolve_storage(
        worlds,
        operation.world_id,
        operation.storage_config,
    )
    return await service.query(
        operation.component,
        world_id=str(operation.world_id),
        run_id=str(operation.run_id),
        storage_config=storage_config,
        lineage=await _resolve_lineage(
            worlds,
            storage,
            operation.world_id,
            operation.run_id,
            storage_config,
        ),
        selection=operation.selection,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
    )


async def _handle_grade_trajectory(
    service: TrajectoryService,
    worlds: WorldRegistry,
    storage: StorageService,
    operation: GradeTrajectory,
) -> Any:
    storage_config = await _resolve_storage(
        worlds,
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
            worlds,
            storage,
            operation.world_id,
            operation.run_id,
            storage_config,
        ),
        selection=operation.selection,
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
    )


def _runtime_world_factory(resources: RuntimeResources) -> Callable[..., Any]:
    """Resolve the runtime-owned mission-world constructor without import cycles."""

    def create(*args: Any, **kwargs: Any) -> Any:
        from archetype.runtime.runtime import _runtime_world_for_resources

        return _runtime_world_for_resources(resources, *args, **kwargs)

    return create


async def _handle_submit_mission(
    resources: RuntimeResources,
    worlds: WorldRegistry,
    lifecycle: WorldLifecycle,
    scheduler: CommandScheduler,
    storage: StorageService,
    redaction: RedactionService,
    control_catalog_config: ControlCatalogConfig,
    unsettled_worlds: RequiredProjectorFanout,
    operation: SubmitMission,
) -> Any:
    backend = operation.config.sandbox_backend
    if not isinstance(backend, ModalSandboxBackend):
        raise ValueError("Agent Mission admission requires the Modal sandbox backend in v0.5.0")
    reservation = resources.owner(operation.owner_id)
    async with resources.admit_owner_operation(reservation):

        async def construct() -> MissionService:
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
            author_driver = operation.config.driver or CodexDriver(
                model=operation.config.model,
                workspace=operation.config.workspace,
            )
            author_executor = ModalMissionAuthorExecutor(
                capability=capability,
                barrier=barrier,
                harness=CodingAgentHarness(
                    author_driver,
                    CodingAgentHarnessConfig(
                        workspace=operation.config.workspace,
                    ),
                ),
                redactor=redaction,
                config=ModalMissionAuthorExecutorConfig(
                    sandbox_environment=operation.config.sandbox_environment,
                    workspace=operation.config.workspace,
                    checkpoint_after_dispatch=operation.config.checkpoint_after_dispatch,
                ),
                observer=operation.config.on_sandbox_event,
            )
            critic_driver = operation.config.critic_driver or CodexCriticDriver(
                workspace=operation.config.critic_workspace,
            )
            critic_executor = ModalMissionCriticExecutor(
                capability=capability,
                barrier=barrier,
                harness=CriticHarness(
                    critic_driver,
                    CriticHarnessConfig(
                        workspace=operation.config.critic_workspace,
                    ),
                ),
                redactor=redaction,
                config=ModalMissionCriticExecutorConfig(
                    sandbox_environment=operation.config.sandbox_environment,
                    workspace=operation.config.critic_workspace,
                ),
            )

            async def bind_mission_activity(
                world_id: str,
            ) -> MissionActivityBinding:
                storage_record = await worlds.storage_record(world_id)
                if storage_record is None:
                    raise WorldNotFoundError(world_id)
                storage_config = storage_record[0]
                catalog_path = activity_catalog_path_for(
                    storage_config,
                    control_catalog_config,
                )
                physical = SqliteActivityCatalog(catalog_path)
                coordinator = ActivityCoordinator(physical)
                reader = StorageMissionCommittedIntentReader(
                    storage,
                    storage_config,
                )
                author = MissionAuthorActivityBinding(
                    world_id=world_id,
                    owner=f"mission-author:{reservation.owner}",
                    reader=reader,
                    catalog=MissionAuthorActivityCoordinator(coordinator),
                    values=LocalMissionAuthorValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-author-values"),
                        redactor=redaction,
                    ),
                    executor=author_executor,
                    stager=WorldMissionAuthorObservationStager(
                        storage=storage,
                        registry=worlds,
                    ),
                )
                critic = MissionCriticActivityBinding(
                    world_id=world_id,
                    owner=f"mission-critic:{reservation.owner}",
                    reader=reader,
                    catalog=MissionCriticActivityCoordinator(coordinator),
                    values=LocalMissionCriticValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-critic-values"),
                        codec=CriticActivityCodec(redaction),
                    ),
                    executor=critic_executor,
                    stager=WorldMissionCriticObservationStager(
                        storage=storage,
                        registry=worlds,
                    ),
                )
                binding: MissionActivityBinding

                async def close_binding() -> None:
                    await physical.close()
                    await unsettled_worlds.unbind(world_id, binding)

                binding = MissionActivityBinding(
                    world_id=world_id,
                    author=author,
                    critic=critic,
                    close=close_binding,
                )
                reservation.retain_anchor(binding)
                await unsettled_worlds.bind(world_id, binding)
                try:
                    routed = unsettled_worlds.required_projector_for(world_id)
                    if worlds.required_projector(world_id) is not routed:
                        await worlds.bind_required_projector(
                            world_id,
                            routed,
                        )
                except BaseException:
                    await unsettled_worlds.unbind(world_id, binding)
                    await physical.close()
                    raise
                return binding

            async def cleanup_factory(world_id: object) -> WorldCleanup:
                lease = await lifecycle.begin_close(str(world_id))
                return WorldCleanup(
                    registry=worlds,
                    lifecycle=lifecycle,
                    world_id=str(world_id),
                    lease=lease,
                    cancel_unsettled=scheduler.cancel_world,
                )

            return MissionService(
                world_factory=_runtime_world_factory(resources),
                name=operation.name,
                config=operation.config,
                sandbox_service=sandbox,
                redaction_service=redaction,
                cleanup_factory=cleanup_factory,
                activity_factory=bind_mission_activity,
                storage=operation.storage,
            )

        service = await reservation.construct(construct)
        submission = operation.submission
        return await service.submit(
            repository=submission.repository,
            branch=submission.branch,
            tasks=submission.tasks,
            name=submission.name,
            base_ref=submission.base_ref,
        )


async def _handle_run_mission(
    resources: RuntimeResources,
    operation: RunMission,
) -> Any:
    reservation = resources.owner(operation.owner_id)
    async with resources.admit_owner_operation(reservation):
        service = cast(MissionService, reservation.require_bound())
        return await service.run(operation.mission, max_ticks=operation.max_ticks)


async def _handle_restore_mission_sandbox(
    resources: RuntimeResources,
    operation: RestoreMissionSandbox,
) -> Any:
    reservation = resources.owner(operation.owner_id)
    async with resources.admit_owner_operation(reservation):
        service = cast(MissionService, reservation.require_bound())
        return await service.restore_sandbox(operation.mission, operation.checkpoint)


async def _handle_run_hosted_episode(
    worlds: WorldRegistry,
    hosted_activity_for: Callable[[RunHostedEpisode], Awaitable[PhysicalHostedActivityBinding]],
    operation: RunHostedEpisode,
) -> Any:
    binding = await hosted_activity_for(operation)
    return await physical_ai_handlers.run_hosted_episode(
        worlds,
        binding,
        operation,
    )


def _register_world_operations(
    registry: OperationRegistry,
    *,
    worlds: WorldRegistry,
    lifecycle: WorldLifecycle,
    storage: StorageService,
    audit: AuditLog,
    fork_world: Callable[..., Awaitable[Any]],
    destroy_world: Callable[[object], Awaitable[None]],
) -> None:
    models = tuple(cast(type[BaseModel], model) for model in WORLD_OPERATION_TYPES)
    actual_names = {_operation_name(model) for model in models}
    if actual_names != set(_WORLD_TOKEN_COSTS):
        raise RuntimeError("world operation composition is incomplete")

    dependencies: dict[str, tuple[object, ...]] = {name: (worlds,) for name in actual_names}
    dependencies.update(
        {
            "create_world": (lifecycle.create_world,),
            "destroy_world": (destroy_world,),
            "discover_worlds": (lifecycle.discover_worlds,),
            "fork_world": (fork_world,),
            "list_signatures": (storage,),
            "list_world_signatures": (worlds, storage),
            "open_world_readonly": (lifecycle.open_world_readonly,),
            "query_archetype": (worlds, storage),
            "query_components": (worlds, storage),
            "resume_world": (lifecycle.open_world_mutable,),
            "run_episode": (worlds, storage),
            "run_rollout": (worlds, storage, fork_world, destroy_world),
        }
    )

    for model in models:
        name = _operation_name(model)
        scope = _world_quota_scope(name)
        durable = None
        if model in PORTABLE_TICK_OPERATION_TYPES:
            durable = DurableOperation(
                decode=model.model_validate_json,
                materialize=cast(Any, materialize_locked),
            )
        registry.register(
            OperationSpec(
                name=name,
                model=model,
                handler=partial(
                    cast(Any, WORLD_OPERATION_HANDLERS)[model],
                    *dependencies[name],
                ),
                permission=_PERMISSION_OVERRIDES.get(name, name),
                summarize=_summarize_world,
                quota_scope=scope,
                world_key=(
                    None
                    if scope == "application"
                    else (_source_world_key if name == "fork_world" else _world_key)
                ),
                durable=durable,
                trusted=True,
                untrusted=name not in _INTERNAL_OPERATIONS,
                token_cost=_WORLD_TOKEN_COSTS[name],
            )
        )

    registry.register(
        OperationSpec(
            name="get_audit_history",
            model=GetAuditHistory,
            handler=cast(Any, partial(_query_audit, audit)),
            permission="get_audit_history",
            summarize=_summarize_world,
            quota_scope="durable_world",
            world_key=_world_key,
            durable=None,
            trusted=True,
            untrusted=True,
            token_cost=5,
        )
    )


def _pull_forward_handler(
    model: type[BaseModel],
    *,
    resources: RuntimeResources,
    worlds: WorldRegistry,
    lifecycle: WorldLifecycle,
    scheduler: CommandScheduler,
    storage: StorageService,
    redaction: RedactionService,
    control_catalog_config: ControlCatalogConfig,
    unsettled_worlds: RequiredProjectorFanout,
    artifact_store_config: ArtifactStoreConfig | None,
    research_admissions: research_handlers.AutoResearchAdmissions,
    destroy_world: simulation.DestroyWorldCallable,
    hosted_activity_for: Callable[[RunHostedEpisode], Awaitable[PhysicalHostedActivityBinding]],
    transcripts: TranscriptIngestionService,
    trajectories: TrajectoryService,
) -> Callable[[BaseModel], Awaitable[Any]]:
    handlers: dict[type[BaseModel], Callable[[BaseModel], Awaitable[Any]]] = {
        IngestArtifacts: cast(
            Any,
            partial(
                artifact_handlers.ingest_artifacts,
                storage,
                store_config=artifact_store_config,
            ),
        ),
        QueryArtifacts: cast(Any, partial(artifact_handlers.query_artifacts, storage)),
        RunGraders: cast(Any, evaluation_handlers.run_graders),
        Evaluate: cast(Any, partial(evaluation_handlers.evaluate, storage)),
        AutoResearch: cast(
            Any,
            partial(
                research_handlers.handle_autoresearch,
                research_admissions,
                worlds,
                lifecycle,
                storage,
                destroy_world,
            ),
        ),
        RunHostedEpisode: cast(
            Any,
            partial(
                _handle_run_hosted_episode,
                worlds,
                hosted_activity_for,
            ),
        ),
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
            partial(_handle_query_trajectory, trajectories, worlds, storage),
        ),
        GradeTrajectory: cast(
            Any,
            partial(_handle_grade_trajectory, trajectories, worlds, storage),
        ),
        SubmitMission: cast(
            Any,
            partial(
                _handle_submit_mission,
                resources,
                worlds,
                lifecycle,
                scheduler,
                storage,
                redaction,
                control_catalog_config,
                unsettled_worlds,
            ),
        ),
        RunMission: cast(Any, partial(_handle_run_mission, resources)),
        RestoreMissionSandbox: cast(
            Any,
            partial(_handle_restore_mission_sandbox, resources),
        ),
    }
    return handlers[model]


def _pull_forward_summarizer(
    model: type[BaseModel],
) -> Callable[[BaseModel], Mapping[str, Any]]:
    if model in {IngestArtifacts, QueryArtifacts}:
        return cast(Any, summarize_artifact_operation)
    if model in {Evaluate, RunGraders}:
        return cast(Any, summarize_evaluation_operation)
    if model is AutoResearch:
        return cast(Any, summarize_research_operation)
    if model is RunHostedEpisode:
        return cast(Any, summarize_physical_ai_operation)
    if model in {
        IngestClaudeTranscript,
        QueryTranscriptRows,
        QueryTrajectory,
        GradeTrajectory,
    }:
        return cast(Any, summarize_episode_operation)
    return cast(Any, summarize_mission_operation)


def _pull_forward_token_cost(name: str) -> int | Callable[[BaseModel], int]:
    if name == "autoresearch":
        return lambda operation: (
            200
            * max(
                int(cast(AutoResearch, operation).config.max_iterations),
                1,
            )
        )
    return {
        "evaluate": 10,
        "ingest_artifacts": 10,
        "query_artifacts": 5,
    }.get(name, 0)


def _register_pull_forward_operations(
    registry: OperationRegistry,
    *,
    resources: RuntimeResources,
    worlds: WorldRegistry,
    lifecycle: WorldLifecycle,
    scheduler: CommandScheduler,
    storage: StorageService,
    redaction: RedactionService,
    control_catalog_config: ControlCatalogConfig,
    unsettled_worlds: RequiredProjectorFanout,
    artifact_store_config: ArtifactStoreConfig | None,
    research_admissions: research_handlers.AutoResearchAdmissions,
    destroy_world: simulation.DestroyWorldCallable,
    hosted_activity_for: Callable[[RunHostedEpisode], Awaitable[PhysicalHostedActivityBinding]],
    transcripts: TranscriptIngestionService,
    trajectories: TrajectoryService,
) -> None:
    for model in _PULL_FORWARD_MODELS:
        name = _operation_name(model)
        scope = _PULL_FORWARD_SCOPES[name]
        registry.register(
            OperationSpec(
                name=name,
                model=model,
                handler=_pull_forward_handler(
                    model,
                    resources=resources,
                    worlds=worlds,
                    lifecycle=lifecycle,
                    scheduler=scheduler,
                    storage=storage,
                    redaction=redaction,
                    control_catalog_config=control_catalog_config,
                    unsettled_worlds=unsettled_worlds,
                    artifact_store_config=artifact_store_config,
                    research_admissions=research_admissions,
                    destroy_world=destroy_world,
                    hosted_activity_for=hosted_activity_for,
                    transcripts=transcripts,
                    trajectories=trajectories,
                ),
                permission=name,
                summarize=_pull_forward_summarizer(model),
                quota_scope=scope,
                world_key=None if scope == "application" else _world_key,
                durable=None,
                trusted=True,
                untrusted=name in _ACTOR_AWARE_PULL_FORWARD,
                token_cost=_pull_forward_token_cost(name),
            )
        )


def build_runtime_resources(config: RuntimeBootstrapConfig) -> RuntimeResources:
    """Construct the complete process graph in explicit dependency order."""

    if not isinstance(config, RuntimeBootstrapConfig):
        raise TypeError("config must be a RuntimeBootstrapConfig")
    injected_storage = config.storage_service
    if injected_storage is not None and injected_storage.has_injected_session:
        if config.audit_storage_config is None:
            raise ValueError("audit_storage_config is required with an injected Daft Session")
        injected_storage.require_iceberg_identity(config.audit_storage_config)

    storage = injected_storage or StorageService(
        control_catalog_config=config.control_catalog_config,
    )
    redaction = config.redaction_service or RedactionService()
    worlds = config.world_registry or WorldRegistry()
    registry = OperationRegistry()

    async def durable_activity_unsettled(world_id: str) -> bool:
        configured = config.unsettled_world_oracle
        if configured is not None and await configured(world_id):
            return True
        record = await worlds.storage_record(world_id)
        if record is None:
            return False
        catalog_path = activity_catalog_path_for(
            record[0],
            config.control_catalog_config,
        )
        if not catalog_path.exists():
            return False
        physical = SqliteActivityCatalog(catalog_path)
        try:
            return await ActivityCoordinator(physical).has_unsettled(world_id)
        finally:
            await physical.close()

    unsettled_worlds = RequiredProjectorFanout(
        fallback_unsettled=durable_activity_unsettled,
        static_projector_factory=config.required_projector_factory,
    )

    async def resolve_control_catalog(world_id: str) -> Any:
        record = await worlds.storage_record(str(world_id))
        if record is None:
            raise WorldNotFoundError(str(world_id))
        return _AdmissionGuardedCatalog(
            worlds,
            str(world_id),
            storage.get_control_catalog(record[0]),
        )

    async def reserve_entity_ids(world_id: object, count: int) -> list[int]:
        try:
            return await mutation.reserve_entity_ids(
                worlds,
                cast(Any, world_id),
                count,
            )
        except KeyError:
            raise WorldNotFoundError(str(world_id)) from None

    scheduler = CommandScheduler(
        registry=registry,
        catalog_for_world=resolve_control_catalog,
        reserve_entity_ids=reserve_entity_ids,
    )

    lifecycle = WorldLifecycle(
        storage,
        worlds,
        materialize_commands=scheduler.materialize,
        required_projector_factory=unsettled_worlds.required_projector_for,
        unsettled_world_oracle=unsettled_worlds.has_unsettled,
    )
    audit = AuditLog(
        storage,
        config.audit_storage_config,
        read_outbox=scheduler.read_outbox,
        acknowledge_outbox=scheduler.acknowledge_outbox,
    )

    cleanup_lifetimes: _WorldCleanupLifetimes | None = None

    async def destroy_owned_world(world_id: object) -> None:
        lifetimes = cleanup_lifetimes
        if lifetimes is None:
            raise RuntimeError("world cleanup lifetime owner is not composed")
        await lifetimes.close_current(world_id)

    async def fork_owned_world(*args: Any, **kwargs: Any) -> Any:
        return await lifecycle.fork_world(*args, **kwargs)

    _register_world_operations(
        registry,
        worlds=worlds,
        lifecycle=lifecycle,
        storage=storage,
        audit=audit,
        fork_world=fork_owned_world,
        destroy_world=destroy_owned_world,
    )
    policy = Policy()
    dispatcher = CommandDispatcher(
        registry=registry,
        policy=policy,
        scheduler=scheduler,
        record_access=audit.record_access,
        target_tick_for_world=lambda world_id: worlds.target_tick(str(world_id)),
    )
    resources = RuntimeResources(
        dispatcher=dispatcher,
        audit=_AuditRuntimeResource(audit, worlds),
        storage=storage,
        owns_storage=injected_storage is None,
    )
    cleanup_lifetimes = _WorldCleanupLifetimes(
        resources,
        worlds,
        lifecycle,
        scheduler,
    )

    transcripts = TranscriptIngestionService(
        redaction,
        storage,
        config.artifact_store_config,
    )
    trajectories = TrajectoryService(storage)
    hosted_bindings: dict[str, tuple[ModalHostedEpisodeConfig, PhysicalHostedActivityBinding]] = {}
    hosted_bindings_lock = asyncio.Lock()

    async def hosted_activity_for(
        operation: RunHostedEpisode,
    ) -> PhysicalHostedActivityBinding:
        world_id = str(operation.world_id)
        async with hosted_bindings_lock:
            retained = hosted_bindings.get(world_id)
            if retained is not None:
                retained_config, binding = retained
                if retained_config != operation.provider:
                    raise ValueError("one world cannot change its hosted Modal provider namespace")
                return binding

            storage_record = await worlds.storage_record(world_id)
            if storage_record is None:
                raise WorldNotFoundError(world_id)
            storage_config = storage_record[0]
            if storage_config != operation.storage_config:
                raise ValueError("hosted operation storage does not match the live world")
            catalog_path = activity_catalog_path_for(
                storage_config,
                config.control_catalog_config,
            )
            physical = SqliteActivityCatalog(catalog_path)
            reservation = resources.reserve_owner(
                f"physical-ai:hosted:{world_id}",
                phase="workflow-handles",
                closed_message="hosted Physical-AI worker is closed",
            )

            async def construct() -> PhysicalHostedActivityBinding:
                coordinator = PhysicalHostedActivityCoordinator(
                    ActivityCoordinator(physical),
                    lease_seconds=config.hosted_activity_lease_seconds,
                )
                provider_factory = config.hosted_episode_provider_factory
                provider = (
                    provider_factory(operation.provider)
                    if provider_factory is not None
                    else ModalHostedEpisodeProvider(operation.provider)
                )
                if provider.provider != operation.provider.provider_identity:
                    raise ValueError(
                        "hosted provider does not implement the requested Modal namespace"
                    )
                binding: PhysicalHostedActivityBinding

                async def close_binding() -> None:
                    await unsettled_worlds.unbind(world_id, binding)
                    await physical.close()

                binding = PhysicalHostedActivityBinding(
                    world_id=world_id,
                    owner=f"physical-hosted:{reservation.owner}",
                    reader=StoragePhysicalCommittedIntentReader(
                        storage,
                        storage_config,
                    ),
                    catalog=coordinator,
                    values=LocalHostedEpisodeValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-physical-values")
                    ),
                    provider=provider,
                    stager=WorldHostedEpisodeObservationStager(
                        storage=storage,
                        registry=worlds,
                    ),
                    close=close_binding,
                )
                await unsettled_worlds.bind(world_id, binding)
                routed = unsettled_worlds.required_projector_for(world_id)
                if worlds.required_projector(world_id) is not routed:
                    await worlds.bind_required_projector(world_id, routed)
                return binding

            try:
                binding = await reservation.construct(construct)
            except BaseException:
                await physical.close()
                raise
            hosted_bindings[world_id] = (operation.provider, binding)
            return binding

    research_admissions = research_handlers.AutoResearchAdmissions()
    _register_pull_forward_operations(
        registry,
        resources=resources,
        worlds=worlds,
        lifecycle=lifecycle,
        scheduler=scheduler,
        storage=storage,
        redaction=redaction,
        control_catalog_config=config.control_catalog_config,
        unsettled_worlds=unsettled_worlds,
        artifact_store_config=config.artifact_store_config,
        research_admissions=research_admissions,
        destroy_world=destroy_owned_world,
        hosted_activity_for=hosted_activity_for,
        transcripts=transcripts,
        trajectories=trajectories,
    )
    if len(registry.specs) != 46:
        raise RuntimeError("runtime composition did not register exactly 46 operations")
    return resources


__all__ = [
    "RuntimeBootstrapConfig",
    "build_runtime_resources",
]
