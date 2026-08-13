# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single explicit process composition transaction.

The framework graph is complete without any world libraries. Separately
installed libraries receive a bounded context only after their manifests have
been resolved and checked against the framework operation inventory.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from types import MappingProxyType
from typing import Any, Literal, cast

from pydantic import BaseModel
from uuid_utils import uuid7

from archetype import __version__
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
from archetype.commands.registry import DurableOperation, OperationRegistry, OperationSpec
from archetype.commands.scheduler import CommandScheduler
from archetype.core.config import StorageConfig
from archetype.errors import WorldNotFoundError
from archetype.evaluation import handlers as evaluation_handlers
from archetype.evaluation.models import Evaluate, RunGraders, summarize_evaluation_operation
from archetype.migration.contracts import MigrationEndpoint
from archetype.migration.interfaces import ColdMigrationVerifier
from archetype.redaction.service import RedactionService
from archetype.runtime_resources import OwnerReservation, RuntimeResources
from archetype.storage.activity_catalog import SqliteActivityCatalog, activity_catalog_path_for
from archetype.storage.catalog import SqliteControlCatalog
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world import mutation
from archetype.world.cleanup import WorldCleanup
from archetype.world.handlers import WORLD_OPERATION_HANDLERS, materialize_locked
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.models import PORTABLE_TICK_OPERATION_TYPES, WORLD_OPERATION_TYPES
from archetype.world.projectors import RequiredProjectorFanout
from archetype.world.registry import WorldRegistry
from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryContext,
    WorldLibraryManifest,
    resolve_world_libraries,
)

_APPLICATION_SCOPED_OPERATIONS = frozenset(
    {"create_world", "discover_worlds", "list_signatures", "list_worlds"}
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
_FRAMEWORK_PULL_FORWARD_MODELS: tuple[type[BaseModel], ...] = (
    IngestArtifacts,
    QueryArtifacts,
    RunGraders,
    Evaluate,
)
_FRAMEWORK_OPERATION_COUNT = 37


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
    world_libraries: tuple[WorldLibraryManifest, ...] | None = None
    world_library_configs: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.world_libraries is not None and not isinstance(self.world_libraries, tuple):
            raise TypeError("world_libraries must be a tuple or None")
        if not isinstance(self.world_library_configs, Mapping):
            raise TypeError("world_library_configs must be a mapping")
        configs = dict(self.world_library_configs)
        if any(not isinstance(name, str) or not name for name in configs):
            raise ValueError("world-library config names must be non-empty strings")
        object.__setattr__(self, "world_library_configs", MappingProxyType(configs))

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
        world_libraries: tuple[WorldLibraryManifest, ...] | None = None,
        world_library_configs: Mapping[str, object] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> RuntimeBootstrapConfig:
        """Resolve environment-backed framework configuration once."""

        return cls(
            control_catalog_config=ControlCatalogConfig.from_env(environ),
            storage_service=storage_service,
            world_registry=world_registry,
            audit_storage_config=audit_storage_config,
            artifact_store_config=artifact_store_config,
            redaction_service=redaction_service,
            required_projector_factory=required_projector_factory,
            unsettled_world_oracle=unsettled_world_oracle,
            world_libraries=world_libraries,
            world_library_configs=world_library_configs or {},
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

    def __init__(self, worlds: WorldRegistry, world_id: str, delegate: object) -> None:
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
    for field_name in ("world_id", "source_world_id"):
        value = getattr(operation, field_name, None)
        if value is not None:
            summary[field_name] = str(value)
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


def _runtime_world_factory(
    runtime_resources: RuntimeResources,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Create a world handle for a library without a runtime import cycle."""

    from archetype.runtime.runtime import _runtime_world_for_resources

    return _runtime_world_for_resources(runtime_resources, *args, **kwargs)


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
                handler=partial(cast(Any, WORLD_OPERATION_HANDLERS)[model], *dependencies[name]),
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
            trusted=True,
            untrusted=True,
            token_cost=5,
        )
    )


def _register_framework_pull_forward_operations(
    registry: OperationRegistry,
    *,
    storage: StorageService,
    artifact_store_config: ArtifactStoreConfig | None,
) -> None:
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
    }
    for model in _FRAMEWORK_PULL_FORWARD_MODELS:
        name = _operation_name(model)
        artifact = model in {IngestArtifacts, QueryArtifacts}
        scope: Literal["application", "live_world", "durable_world"] = (
            "application" if model is RunGraders else "durable_world"
        )
        registry.register(
            OperationSpec(
                name=name,
                model=model,
                handler=handlers[model],
                permission=name,
                summarize=cast(
                    Any,
                    summarize_artifact_operation if artifact else summarize_evaluation_operation,
                ),
                quota_scope=scope,
                world_key=None if scope == "application" else _world_key,
                trusted=True,
                untrusted=model is not RunGraders,
                token_cost={
                    "evaluate": 10,
                    "ingest_artifacts": 10,
                    "query_artifacts": 5,
                }.get(name, 0),
            )
        )


def _preflight_library_operations(
    registry: OperationRegistry,
    manifests: tuple[WorldLibraryManifest, ...],
) -> None:
    base_names = {spec.name for spec in registry.specs}
    base_models = {spec.model for spec in registry.specs}
    for manifest in manifests:
        for name, model in zip(
            manifest.operation_names,
            manifest.operation_models,
            strict=True,
        ):
            if name in base_names:
                raise ValueError(
                    f"world library {manifest.name!r} operation name {name!r} "
                    "collides with the framework"
                )
            if model in base_models:
                raise ValueError(
                    f"world library {manifest.name!r} operation model "
                    f"{model.__name__} collides with the framework"
                )


def _install_world_library(
    manifest: WorldLibraryManifest,
    context: WorldLibraryContext,
) -> InstalledWorldLibrary:
    registry = cast(OperationRegistry, context.registry)
    before = registry.specs
    installed = manifest.install(context)
    if inspect.isawaitable(installed):
        if inspect.iscoroutine(installed):
            installed.close()
        raise TypeError(f"world library {manifest.name!r} installer must be synchronous")
    if not isinstance(installed, InstalledWorldLibrary):
        raise TypeError(
            f"world library {manifest.name!r} installer did not return InstalledWorldLibrary"
        )
    if installed.name != manifest.name:
        raise ValueError(f"world library {manifest.name!r} installer returned {installed.name!r}")
    after = registry.specs
    if after[: len(before)] != before:
        raise RuntimeError(
            f"world library {manifest.name!r} mutated existing operation registrations"
        )
    added = after[len(before) :]
    actual = tuple((spec.name, spec.model) for spec in added)
    declared = tuple(zip(manifest.operation_names, manifest.operation_models, strict=True))
    if actual != declared:
        raise RuntimeError(
            f"world library {manifest.name!r} registered {actual!r}; declared {declared!r}"
        )
    return installed


def build_runtime_resources(config: RuntimeBootstrapConfig) -> RuntimeResources:
    """Construct the framework graph and install the resolved library set."""

    if not isinstance(config, RuntimeBootstrapConfig):
        raise TypeError("config must be a RuntimeBootstrapConfig")
    manifests = resolve_world_libraries(
        config.world_libraries,
        framework_version=__version__,
    )
    unknown_configs = set(config.world_library_configs) - {manifest.name for manifest in manifests}
    if unknown_configs:
        raise ValueError(
            "world-library configs have no resolved manifest: " + ", ".join(sorted(unknown_configs))
        )

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
        catalog_path = activity_catalog_path_for(record[0], config.control_catalog_config)
        if not catalog_path.exists():
            return False
        physical = SqliteActivityCatalog(catalog_path)
        try:
            return await ActivityCoordinator(physical).has_unsettled(world_id)
        finally:
            await physical.close()

    required_projectors = RequiredProjectorFanout(
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
            return await mutation.reserve_entity_ids(worlds, cast(Any, world_id), count)
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
        required_projector_factory=required_projectors.required_projector_for,
        unsettled_world_oracle=required_projectors.has_unsettled,
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
    _register_framework_pull_forward_operations(
        registry,
        storage=storage,
        artifact_store_config=config.artifact_store_config,
    )
    if len(registry.specs) != _FRAMEWORK_OPERATION_COUNT:
        raise RuntimeError(
            "framework composition did not register exactly "
            f"{_FRAMEWORK_OPERATION_COUNT} operations"
        )
    _preflight_library_operations(registry, manifests)
    dispatcher = CommandDispatcher(
        registry=registry,
        policy=Policy(),
        scheduler=scheduler,
        record_access=audit.record_access,
        target_tick_for_world=lambda world_id: worlds.target_tick(str(world_id)),
    )
    resources = RuntimeResources(
        dispatcher=dispatcher,
        audit=_AuditRuntimeResource(audit, worlds),
        storage=storage,
        owns_storage=injected_storage is None,
        world_library_manifests=manifests,
    )
    cleanup_lifetimes = _WorldCleanupLifetimes(resources, worlds, lifecycle, scheduler)

    installed: list[InstalledWorldLibrary] = []
    for manifest in manifests:
        context = WorldLibraryContext(
            registry=registry,
            resources=resources,
            worlds=worlds,
            lifecycle=lifecycle,
            scheduler=scheduler,
            storage=storage,
            redaction=redaction,
            required_projectors=required_projectors,
            control_catalog_config=config.control_catalog_config,
            artifact_store_config=config.artifact_store_config,
            destroy_world=destroy_owned_world,
            runtime_world_factory=partial(_runtime_world_factory, resources),
            config=config.world_library_configs.get(manifest.name),
        )
        installed.append(_install_world_library(manifest, context))
    resources.retain_world_libraries(tuple(installed))
    return resources


def build_local_migration_endpoint(
    storage_config: StorageConfig,
    storage_service: StorageService,
    *,
    audit_storage_config: StorageConfig,
    artifact_store_config: ArtifactStoreConfig | None = None,
    cold_verifier: ColdMigrationVerifier | None = None,
) -> MigrationEndpoint:
    """Bind migration capabilities to one caller-owned local storage service.

    The caller owns the service lifetime. The explicit audit configuration
    proves that audit rows share this same storage identity; omission cannot
    silently stand for Archetype's separate default audit lakehouse. Local v1
    deliberately refuses a remote control catalog; the remote administrative
    protocol remains a separate implementation slice.
    """

    storage_service.require_iceberg_identity(storage_config)
    control = storage_service.get_control_catalog(storage_config)
    if not isinstance(control, SqliteControlCatalog):
        raise TypeError("local migration v1 requires a SQLite control catalog")
    activity_path = control.path.with_name(
        f"{control.path.stem}-activities{control.path.suffix}"
    ).resolve()
    return MigrationEndpoint(
        storage_config=storage_config,
        storage_service=storage_service,
        control_catalog=control,
        artifact_store_config=artifact_store_config or ArtifactStoreConfig(),
        activity_catalog_path=activity_path,
        audit_storage_config=audit_storage_config,
        cold_verifier=cold_verifier,
    )


__all__ = [
    "RuntimeBootstrapConfig",
    "build_local_migration_endpoint",
    "build_runtime_resources",
]
