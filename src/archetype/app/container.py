# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal composition root for the application service graph."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from typing import Any, Literal, cast

from pydantic import BaseModel

from archetype.app.application.service import RuntimeApplication
from archetype.app.artifacts.service import ArtifactService
from archetype.app.evaluation.service import EvaluationService
from archetype.app.gateway.service import CommandGateway
from archetype.app.ingestion.service import IngestionService
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.missions.transcript_service import TranscriptIngestionService
from archetype.app.physical_ai.service import PhysicalAIService
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.service import RedactionService
from archetype.app.research.service import AutoResearchService
from archetype.artifacts.contracts import ArtifactStoreConfig
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
from archetype.errors import WorldNotFoundError
from archetype.missions.contracts import AgentMissionConfig
from archetype.missions.sandboxes.service import SandboxService
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world import mutation
from archetype.world.handlers import WORLD_OPERATION_HANDLERS, materialize_locked
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
)
from archetype.world.registry import WorldRegistry

_APPLICATION_SCOPED_OPERATIONS = frozenset(
    {
        "create_world",
        "list_worlds",
        "discover_worlds",
        "list_signatures",
    }
)
_DURABLE_WORLD_SCOPED_OPERATIONS = frozenset(
    {
        "destroy_world",
        "open_world_readonly",
        "resume_world",
        "query_components",
        "query_archetype",
        "list_world_signatures",
    }
)
_PERMISSION_OVERRIDES = {
    "reserve_entity_ids": "spawn",
    "spawn_reserved": "spawn",
    "list_world_signatures": "list_signatures",
}
_INTERNAL_OPERATIONS = frozenset({"reserve_entity_ids", "spawn_reserved"})
_WORLD_TOKEN_COSTS = {
    "spawn": 10,
    "create_entities": 10,
    "reserve_entity_ids": 10,
    "spawn_reserved": 10,
    "despawn": 5,
    "update": 8,
    "add_components": 8,
    "remove_components": 5,
    "add_processor": 15,
    "remove_processor": 5,
    "create_world": 50,
    "fork_world": 100,
    "destroy_world": 10,
    "get_world_info": 2,
    "list_worlds": 2,
    "discover_worlds": 2,
    "open_world_readonly": 2,
    "resume_world": 50,
    "step": 10,
    "run": 50,
    "run_episode": 500,
    "run_rollout": 200,
    "query_components": 5,
    "query_archetype": 5,
    "list_signatures": 2,
    "list_world_signatures": 2,
    "add_resource": 10,
    "add_hook": 10,
    "remove_hook": 5,
    "list_processors": 2,
    "list_hooks": 2,
    "list_resources": 2,
}


def _operation_name(model: type[BaseModel]) -> str:
    value = model.model_fields["operation"].default
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{model.__name__} has no fixed operation discriminator")
    return value


def _quota_scope(
    operation_name: str,
) -> Literal["application", "live_world", "durable_world"]:
    if operation_name in _APPLICATION_SCOPED_OPERATIONS:
        return "application"
    if operation_name in _DURABLE_WORLD_SCOPED_OPERATIONS:
        return "durable_world"
    return "live_world"


def _world_key(operation: BaseModel) -> object:
    return cast("Any", operation).world_id


def _source_world_key(operation: BaseModel) -> object:
    return cast("Any", operation).source_world_id


def _summarize(operation: BaseModel) -> Mapping[str, Any]:
    """Expose only stable routing identity, never arbitrary operation payloads."""
    summary: dict[str, Any] = {"operation": cast("Any", operation).operation}
    for field in ("world_id", "source_world_id"):
        value = getattr(operation, field, None)
        if value is not None:
            summary[field] = str(value)
    return summary


async def _query_audit(audit: AuditLog, operation: GetAuditHistory):
    return await audit.query(
        operation.world_id,
        tick_range=operation.tick_range,
        actor_id=operation.actor_id,
        idempotency_key=operation.idempotency_key,
        status=operation.status,
        limit=operation.limit,
    )


def _register_operations(
    registry: OperationRegistry,
    *,
    worlds: WorldRegistry,
    lifecycle: WorldLifecycle,
    storage: StorageService,
    audit: AuditLog,
    fork_world: Callable[..., Awaitable[Any]],
    destroy_world: Callable[[object], Awaitable[None]],
) -> None:
    """Bind the complete world surface once at the sole composition root."""
    models = tuple(cast("type[BaseModel]", model) for model in WORLD_OPERATION_TYPES)
    actual_names = {_operation_name(model) for model in models}
    expected_names = set(_WORLD_TOKEN_COSTS)
    if actual_names != expected_names:
        raise RuntimeError(
            "world operation composition is incomplete: "
            f"missing={sorted(expected_names - actual_names)}, "
            f"extra={sorted(actual_names - expected_names)}"
        )

    dependencies: dict[str, tuple[object, ...]] = {name: (worlds,) for name in actual_names}
    dependencies.update(
        {
            "create_world": (lifecycle.create_world,),
            "fork_world": (fork_world,),
            "destroy_world": (destroy_world,),
            "discover_worlds": (lifecycle.discover_worlds,),
            "open_world_readonly": (lifecycle.open_world_readonly,),
            "resume_world": (lifecycle.open_world_mutable,),
            "run_episode": (worlds, storage),
            "run_rollout": (
                worlds,
                storage,
                fork_world,
                destroy_world,
            ),
            "query_components": (worlds, storage),
            "query_archetype": (worlds, storage),
            "list_signatures": (storage,),
            "list_world_signatures": (worlds, storage),
        }
    )

    for model in models:
        name = _operation_name(model)
        scope = _quota_scope(name)
        world_key = None
        if scope != "application":
            world_key = _source_world_key if name == "fork_world" else _world_key
        durable = None
        if model in PORTABLE_TICK_OPERATION_TYPES:
            durable = DurableOperation(
                decode=model.model_validate_json,
                materialize=cast("Any", materialize_locked),
            )
        registry.register(
            OperationSpec(
                name=name,
                model=model,
                handler=partial(
                    cast("Any", WORLD_OPERATION_HANDLERS)[model],
                    *dependencies[name],
                ),
                permission=_PERMISSION_OVERRIDES.get(name, name),
                summarize=_summarize,
                quota_scope=scope,
                world_key=world_key,
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
            handler=cast("Any", partial(_query_audit, audit)),
            permission="get_audit_history",
            summarize=_summarize,
            quota_scope="durable_world",
            world_key=_world_key,
            durable=None,
            trusted=True,
            untrusted=True,
            token_cost=5,
        )
    )


class _AdmissionGuardedCatalog:
    """Keep durable admission ordered with the world close barrier."""

    def __init__(
        self,
        worlds: WorldRegistry,
        world_id: str,
        delegate: object,
    ) -> None:
        self._worlds = worlds
        self._world_id = world_id
        self._delegate = cast("Any", delegate)

    async def admit_commands(self, world_id: str, admissions):
        if str(world_id) != self._world_id:
            raise ValueError("catalog admission target differs from its bound world")
        try:
            async with self._worlds.operation(self._world_id):
                return await self._delegate.admit_commands(world_id, admissions)
        except KeyError:
            raise WorldNotFoundError(self._world_id) from None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class ServiceContainer:
    """Construct and own the internal service graph.

    Each service owns its internal composition.
    The container handles concrete construction and callback wiring. Application
    code uses ``ArchetypeRuntime`` or the REST/CLI adapters rather than calling
    these delegates directly.
    """

    def __init__(
        self,
        storage_service: StorageService | None = None,
        audit_storage_config: StorageConfig | None = None,
        artifact_store_config: ArtifactStoreConfig | None = None,
        redaction_service: iRedactionService | None = None,
    ):
        if storage_service is not None and storage_service.has_injected_session:
            if audit_storage_config is None:
                raise ValueError(
                    "audit_storage_config is required when ServiceContainer uses an "
                    "injected Daft Session"
                )
            storage_service.require_iceberg_identity(audit_storage_config)

        # Leaf services
        self._owns_storage_service = storage_service is None
        self.storage_service = (
            storage_service
            if storage_service is not None
            else StorageService(
                control_catalog_config=ControlCatalogConfig.from_env(),
            )
        )
        self.redaction_service = (
            redaction_service if redaction_service is not None else RedactionService()
        )

        # Canonical world ownership, exact registration, and tick materialization.
        self.world_registry = WorldRegistry()
        self.operation_registry = OperationRegistry()

        async def resolve_control_catalog(world_id: str):
            record = await self.world_registry.storage_record(str(world_id))
            if record is None:
                raise WorldNotFoundError(str(world_id))
            return cast(
                "Any",
                _AdmissionGuardedCatalog(
                    self.world_registry,
                    str(world_id),
                    self.storage_service.get_control_catalog(record[0]),
                ),
            )

        async def reserve_entity_ids(world_id: object, count: int) -> list[int]:
            try:
                return await mutation.reserve_entity_ids(
                    self.world_registry,
                    cast("Any", world_id),
                    count,
                )
            except KeyError:
                raise WorldNotFoundError(str(world_id)) from None

        self.command_scheduler = CommandScheduler(
            registry=self.operation_registry,
            catalog_for_world=resolve_control_catalog,
            reserve_entity_ids=reserve_entity_ids,
        )
        self.world_lifecycle = WorldLifecycle(
            self.storage_service,
            self.world_registry,
            materialize_commands=self.command_scheduler.materialize,
        )

        self.audit_log = AuditLog(
            self.storage_service,
            audit_storage_config,
            read_outbox=self.command_scheduler.read_outbox,
            acknowledge_outbox=self.command_scheduler.acknowledge_outbox,
        )

        async def destroy_owned_world(world_id: object) -> None:
            # Late binding breaks the construction cycle without recursively
            # entering the public dispatcher from compound cleanup.
            await self.application._destroy_world_owned(world_id)

        async def fork_owned_world(*args, **kwargs):
            # Resolve at call time so compound handlers and focused injection
            # tests share the same lifecycle capability.
            return await self.world_lifecycle.fork_world(*args, **kwargs)

        _register_operations(
            self.operation_registry,
            worlds=self.world_registry,
            lifecycle=self.world_lifecycle,
            storage=self.storage_service,
            audit=self.audit_log,
            fork_world=fork_owned_world,
            destroy_world=destroy_owned_world,
        )
        self.policy = Policy()
        self.command_dispatcher = CommandDispatcher(
            registry=self.operation_registry,
            policy=self.policy,
            scheduler=self.command_scheduler,
            record_access=self.audit_log.record_access,
            target_tick_for_world=lambda world_id: self.world_registry.target_tick(str(world_id)),
        )

        # Storage-backed application workflows.
        self.ingestion_service = IngestionService(
            self.storage_service,
            self.world_registry,
        )
        self.artifact_service = ArtifactService(
            self.storage_service,
            self.world_registry,
            self.ingestion_service,
            artifact_store_config,
        )
        self.transcript_ingestion_service = TranscriptIngestionService(
            self.artifact_service,
            self.ingestion_service,
            self.redaction_service,
            self.storage_service,
            self.world_registry,
        )
        self.evaluation_service = EvaluationService(
            self.ingestion_service,
            self.storage_service,
            self.world_registry,
        )
        self.trajectory_service = TrajectoryService(
            self.storage_service,
            self.evaluation_service,
        )
        self.physical_ai_service = PhysicalAIService(
            self.world_registry,
            self.world_lifecycle,
            self.evaluation_service,
            self.storage_service,
        )

        self.autoresearch_service = AutoResearchService(
            self.world_registry,
            self.world_lifecycle,
            self.storage_service,
            destroy_world=destroy_owned_world,
        )

        self.application = RuntimeApplication(
            registry=self.world_registry,
            lifecycle=self.world_lifecycle,
            storage=self.storage_service,
            dispatcher=self.command_dispatcher,
            scheduler=self.command_scheduler,
            artifacts=self.artifact_service,
            transcripts=self.transcript_ingestion_service,
            evaluations=self.evaluation_service,
            trajectories=self.trajectory_service,
            research=self.autoresearch_service,
            physical_ai=self.physical_ai_service,
            agent_missions=self._agent_mission_service,
        )
        self.command_gateway = CommandGateway(
            self.application,
            self.command_dispatcher,
            self.policy,
            self.audit_log.record_access,
            target_tick_for_world=lambda world_id: self.world_registry.target_tick(str(world_id)),
        )

    def _agent_mission_service(self, *, config: AgentMissionConfig, **kwargs) -> MissionService:
        """Compose one mission-owned sandbox lifetime beneath the app workflow."""

        return MissionService(
            config=config,
            sandbox_service=SandboxService((config.sandbox_backend,)),
            redaction_service=self.redaction_service,
            **kwargs,
        )

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        steps: list[tuple[str, Callable[[], Awaitable[None]]]] = [
            ("command admission", self._stop_command_admission),
            ("audit log", self._shutdown_audit_log),
        ]
        if self._owns_storage_service:
            steps.append(("storage service", self.storage_service.shutdown))

        failures: list[BaseException] = []
        for label, shutdown in steps:
            try:
                await shutdown()
            except BaseException as exc:
                exc.add_note(f"ServiceContainer shutdown step failed: {label}")
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup(
                f"ServiceContainer shutdown failed for {len(failures)} step(s)",
                failures,
            )

    async def _stop_command_admission(self) -> None:
        await self.command_dispatcher.stop_admission()
        await self.command_dispatcher.wait_drained()

    async def _shutdown_audit_log(self) -> None:
        # A restarted process may know durable world coordinates without having
        # opened a scheduler catalog yet. Warm each known coordinate so global
        # shutdown cannot strand command outbox evidence.
        for world_id in await self.world_registry.catalog_world_ids():
            await self.audit_log.project_outbox(world_id=world_id)
        await self.audit_log.shutdown()
