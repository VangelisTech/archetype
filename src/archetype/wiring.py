# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The single explicit process composition transaction."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, cast

from daft.pickle import dumps as daft_dumps
from pydantic import BaseModel
from uuid_utils import uuid7

from archetype.activities import ActivityCoordinator
from archetype.app.missions.activity_coordinator import (
    MissionAuthorActivityCoordinator,
)
from archetype.app.missions.activity_world import (
    MissionAuthorActivityBinding,
    StorageMissionCommittedIntentReader,
    WorldMissionAuthorObservationStager,
)
from archetype.app.missions.local_activity_values import (
    LocalMissionAuthorValueStore,
)
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.missions.transcript_service import TranscriptIngestionService
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
from archetype.missions.coding_agents.harness import (
    CodexDriver,
    CodingAgentHarness,
    CodingAgentHarnessConfig,
)
from archetype.missions.modal_author import (
    ModalMissionAuthorExecutor,
    ModalMissionAuthorExecutorConfig,
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
from archetype.physical_ai import handlers as physical_ai_handlers
from archetype.physical_ai.interfaces import (
    EnvClient,
    PhysicalClientLifetimeRegistrar,
    PhysicalEvidenceWorldRetirement,
    PhysicalWorkflowLifetime,
    PolicyClient,
)
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    SweepPhysicalInstructions,
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
from archetype.storage.catalog import ControlCatalog, WorldRecord
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
from archetype.world.registry import WorldCleanupLease, WorldRegistry

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
    EvaluatePhysicalTask,
    SweepPhysicalInstructions,
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
    "evaluate_physical_task": "application",
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
    "sweep_physical_instructions": "application",
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
        )


class _UnsettledWorldRouter:
    """Compose static host policy with dynamically bound Activity families."""

    def __init__(
        self,
        fallback: Callable[[str], Awaitable[bool]] | None,
    ) -> None:
        self._fallback = fallback
        self._bindings: dict[str, object] = {}
        self._lock = asyncio.Lock()

    async def bind(self, world_id: str, binding: object) -> None:
        async with self._lock:
            existing = self._bindings.get(world_id)
            if existing is not None and existing is not binding:
                raise ValueError(f"world {world_id!r} already has an Activity binding")
            self._bindings[world_id] = binding

    async def unbind(self, world_id: str, binding: object) -> None:
        async with self._lock:
            if self._bindings.get(world_id) is binding:
                self._bindings.pop(world_id)

    async def has_unsettled(self, world_id: str) -> bool:
        fallback = self._fallback
        if fallback is not None and await fallback(world_id):
            return True
        async with self._lock:
            binding = self._bindings.get(world_id)
        if binding is None:
            return False
        oracle = getattr(binding, "has_unsettled_work", None)
        if not callable(oracle):
            raise TypeError("Activity binding must expose has_unsettled_work")
        return bool(await oracle(world_id))


@dataclass(frozen=True, slots=True)
class _PhysicalAIClientEntry:
    """One identity-keyed provider owner and its operation lock."""

    identity: int
    client: object
    reservation: OwnerReservation
    lock: asyncio.Lock


@dataclass(slots=True)
class _WorldCleanupReservation:
    """One pre-effect process-owner slot for an eventual exact-world cleanup."""

    reservation: OwnerReservation
    deferred: _DeferredWorldCleanup
    bound: bool = False
    entry: _WorldCleanupEntry | None = None


@dataclass(frozen=True, slots=True)
class _LazyExactWorldCleanup:
    """Canonical exact cleanup selected now and constructed only when joined."""

    registry: WorldRegistry
    lifecycle: WorldLifecycle
    scheduler: CommandScheduler
    world_id: str
    lease: WorldCleanupLease

    async def finish(self) -> None:
        """Revalidate and run the canonical cleanup inside its retained owner."""

        cleanup = WorldCleanup(
            registry=self.registry,
            lifecycle=self.lifecycle,
            world_id=self.world_id,
            lease=self.lease,
            cancel_unsettled=self.scheduler.cancel_world,
        )
        await cleanup.finish()


@dataclass(frozen=True, slots=True)
class _RegistrationWorldCleanup:
    """Exact durable registration retirement owned before its first write."""

    catalog: ControlCatalog
    record: WorldRecord

    async def finish(self) -> None:
        """Destroy or tombstone only the attempted immutable registration."""

        await self.catalog.retire_world_registration(self.record)


@dataclass(slots=True)
class _DeferredWorldCleanup:
    """A process-owned cleanup target promoted without changing its owner."""

    cleanup: _RegistrationWorldCleanup | _LazyExactWorldCleanup | None = None
    on_success: Callable[[], None] | None = None

    def bind_registration(
        self,
        cleanup: _RegistrationWorldCleanup,
        *,
        on_success: Callable[[], None],
    ) -> None:
        """Select exact durable cleanup before registration may execute."""

        if self.cleanup is not None:
            if self.cleanup is cleanup and self.on_success is on_success:
                return
            raise RuntimeError("deferred world cleanup is already bound")
        self.cleanup = cleanup
        self.on_success = on_success

    def bind_exact(
        self,
        cleanup: _LazyExactWorldCleanup,
        *,
        on_success: Callable[[], None],
    ) -> None:
        """Select canonical cleanup directly for non-provisional callers."""

        if self.cleanup is not None:
            if self.cleanup is cleanup and self.on_success is on_success:
                return
            raise RuntimeError("deferred world cleanup is already bound")
        self.cleanup = cleanup
        self.on_success = on_success

    def promote(self, cleanup: _LazyExactWorldCleanup) -> None:
        """Replace provisional retirement with canonical exact-world cleanup."""

        if not isinstance(self.cleanup, _RegistrationWorldCleanup):
            if self.cleanup is cleanup:
                return
            raise RuntimeError("deferred world cleanup has no provisional registration")
        self.cleanup = cleanup

    async def finish(self) -> None:
        """Finish the current exact target, releasing only after success."""

        cleanup = self.cleanup
        if cleanup is None:
            return
        await cleanup.finish()
        on_success = self.on_success
        if on_success is not None:
            on_success()


class _PhysicalAIClientLifetimes:
    """Own providers and serialize operations that share their identities."""

    def __init__(
        self,
        resources: RuntimeResources,
        cleanup_lifetimes: _WorldCleanupLifetimes,
    ) -> None:
        self._resources = resources
        self._cleanup_lifetimes = cleanup_lifetimes
        self._entries: dict[int, _PhysicalAIClientEntry] = {}

    def lease(
        self,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> AbstractAsyncContextManager[PhysicalWorkflowLifetime]:
        """Synchronously own clients, then return their ordered async lease."""

        self._require_role(
            env_client,
            role="environment",
            methods=("reset", "step", "aclose"),
        )
        if policy_client is not None:
            self._require_role(
                policy_client,
                role="policy",
                methods=("act", "aclose"),
            )
        entries = self._entries_for((env_client, policy_client))
        return self._hold(entries)

    @staticmethod
    def _require_role(
        client: object,
        *,
        role: str,
        methods: tuple[str, ...],
    ) -> None:
        for method in methods:
            member = inspect.getattr_static(client, method, None)
            target = member.__func__ if isinstance(member, (classmethod, staticmethod)) else member
            if target is None or not callable(target):
                expected = "async" if method == "aclose" else "synchronous"
                raise TypeError(f"physical-AI {role} providers must define {expected} {method}()")
            is_async = inspect.iscoroutinefunction(target)
            if method == "aclose" and not is_async:
                raise TypeError(f"physical-AI {role} providers must define async aclose()")
            if method != "aclose" and is_async:
                raise TypeError(f"physical-AI {role} providers must define synchronous {method}()")
        try:
            daft_dumps(client)
        except Exception as exc:
            raise TypeError(
                f"physical-AI {role} provider must be serializable by Daft as a non-owning handle"
            ) from exc

    def _entries_for(
        self,
        clients: tuple[Any | None, ...],
    ) -> tuple[_PhysicalAIClientEntry, ...]:
        unique: dict[int, object] = {}
        for client in clients:
            if client is None:
                continue
            identity = id(client)
            selected = unique.get(identity)
            if selected is not None:
                if selected is not client:
                    raise RuntimeError("physical-AI provider identity collision")
                continue
            retained = self._entries.get(identity)
            if retained is not None and retained.client is not client:
                raise RuntimeError("physical-AI provider identity collision")
            unique[identity] = client

        new_clients = tuple(
            (identity, client)
            for identity, client in unique.items()
            if identity not in self._entries
        )
        for identity, client in new_clients:

            async def close_client(
                client: object = client,
                identity: int = identity,
            ) -> None:
                await self._cleanup_lifetimes.close_for_provider(identity)
                result = cast(Any, client).aclose()
                if not inspect.isawaitable(result):
                    raise TypeError("physical-AI provider aclose() must return an awaitable")
                await result
                retained = self._entries.get(identity)
                if retained is not None and retained.client is client:
                    self._entries.pop(identity)

            reservation = self._resources.reserve_owner(
                f"physical-ai:client:{uuid7()}",
                phase="workflow-handles",
                closed_message="physical-AI provider owner is closed",
            )
            reservation.bind(client, close=close_client)
            self._entries[identity] = _PhysicalAIClientEntry(
                identity=identity,
                client=client,
                reservation=reservation,
                lock=asyncio.Lock(),
            )

        return tuple(self._entries[identity] for identity in sorted(unique))

    @asynccontextmanager
    async def _hold(
        self,
        entries: tuple[_PhysicalAIClientEntry, ...],
    ) -> AsyncIterator[PhysicalWorkflowLifetime]:
        acquired: list[_PhysicalAIClientEntry] = []
        cleanup_reservation: _WorldCleanupReservation | None = None
        async with AsyncExitStack() as admissions:
            for entry in entries:
                await admissions.enter_async_context(
                    self._resources.admit_owner_operation(entry.reservation)
                )
            try:
                for entry in entries:
                    await entry.lock.acquire()
                    acquired.append(entry)
                cleanup_reservation = self._cleanup_lifetimes.reserve()
                yield _PhysicalWorkflowLifetime(
                    cleanup_lifetimes=self._cleanup_lifetimes,
                    cleanup_reservation=cleanup_reservation,
                    provider_ids=frozenset(entry.identity for entry in entries),
                )
            finally:
                try:
                    if cleanup_reservation is not None and not cleanup_reservation.bound:
                        await cleanup_reservation.reservation.aclose()
                finally:
                    for entry in reversed(acquired):
                        entry.lock.release()


@dataclass(eq=False, slots=True)
class _WorldCleanupEntry:
    """One provisional or canonical cleanup retained by the process owner."""

    world_id: str
    lease: WorldCleanupLease | None
    reservation: OwnerReservation
    provider_ids: set[int]


@dataclass(frozen=True, slots=True)
class _WorldCleanupHandle:
    """A stale-safe join handle bound directly to one cleanup reservation."""

    reservation: OwnerReservation

    async def aclose(self) -> None:
        """Join the reservation's shielded, retryable exact cleanup."""

        await self.reservation.aclose()


class _WorldCleanupLifetimes:
    """Coalesce and retain complete world-close transactions by exact lease."""

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
        self._entries: dict[WorldCleanupLease, _WorldCleanupEntry] = {}
        self._by_provider: dict[int, set[_WorldCleanupEntry]] = {}

    def reserve(self) -> _WorldCleanupReservation:
        """Reserve cleanup ownership before a workflow may create its world."""

        reservation = self._resources.reserve_owner(
            f"world-cleanup:{uuid7()}",
            phase="workflow-handles",
            closed_message="world cleanup owner is closed",
        )
        deferred = _DeferredWorldCleanup()
        reservation.bind(deferred, close=deferred.finish)
        return _WorldCleanupReservation(
            reservation=reservation,
            deferred=deferred,
        )

    def retain(
        self,
        world_id: object,
        lease: WorldCleanupLease,
        *,
        provider_ids: frozenset[int] = frozenset(),
        reservation: _WorldCleanupReservation | None = None,
    ) -> _WorldCleanupHandle:
        """Synchronously retain one exact cleanup transaction before effects."""

        return self._retain_exact(
            world_id,
            lease,
            provider_ids=provider_ids,
            reservation=reservation,
        )

    def retain_registration(
        self,
        catalog: ControlCatalog,
        record: WorldRecord,
        *,
        provider_ids: frozenset[int],
        reservation: _WorldCleanupReservation,
    ) -> None:
        """Bind exact durable retirement before registration can execute."""

        if record.writer_mode != "cleanup_only":
            raise ValueError("provisional cleanup requires writer_mode='cleanup_only'")
        exact_world_id = str(record.world_id)
        if reservation.bound:
            entry = reservation.entry
            if entry is None:
                raise RuntimeError("bound world cleanup reservation has no exact entry")
            if entry.world_id != exact_world_id:
                raise RuntimeError("world cleanup reservation is bound to another world")
            cleanup = reservation.deferred.cleanup
            if (
                not isinstance(cleanup, _RegistrationWorldCleanup)
                or cleanup.catalog is not catalog
                or cleanup.record != record
            ):
                raise RuntimeError("world cleanup reservation is bound to another registration")
            self._associate_providers(entry, provider_ids)
            return
        if reservation.reservation.released:
            raise RuntimeError("world cleanup reservation is already released")
        entry = _WorldCleanupEntry(
            world_id=exact_world_id,
            lease=None,
            reservation=reservation.reservation,
            provider_ids=set(),
        )
        cleanup = _RegistrationWorldCleanup(catalog=catalog, record=record)
        reservation.deferred.bind_registration(
            cleanup,
            on_success=partial(self._release_entry, entry),
        )
        reservation.entry = entry
        reservation.bound = True
        self._associate_providers(entry, provider_ids)

    def promote_registration(
        self,
        world_id: object,
        lease: WorldCleanupLease,
        *,
        provider_ids: frozenset[int],
        reservation: _WorldCleanupReservation,
    ) -> _WorldCleanupHandle:
        """Promote the same provisional owner to canonical registry cleanup."""

        exact_world_id = str(world_id)
        entry = reservation.entry
        if not reservation.bound or entry is None:
            raise RuntimeError("world cleanup reservation has no provisional registration")
        if entry.world_id != exact_world_id:
            raise RuntimeError("world cleanup reservation is bound to another world")
        if entry.lease is not None:
            if entry.lease != lease:
                raise RuntimeError("world cleanup reservation is bound to another lease")
            retained = self._entries.get(lease)
            if retained is not None and retained is not entry:
                raise RuntimeError("world cleanup lease has another retained owner")
            self._entries[lease] = entry
            self._associate_providers(entry, provider_ids)
            return _WorldCleanupHandle(entry.reservation)
        retained = self._entries.get(lease)
        if retained is not None and retained is not entry:
            raise RuntimeError("world cleanup lease has another retained owner")
        cleanup = _LazyExactWorldCleanup(
            registry=self._worlds,
            lifecycle=self._lifecycle,
            scheduler=self._scheduler,
            world_id=exact_world_id,
            lease=lease,
        )
        reservation.deferred.promote(cleanup)
        entry.lease = lease
        self._entries[lease] = entry
        self._associate_providers(entry, provider_ids)
        return _WorldCleanupHandle(entry.reservation)

    def retain_reserved(
        self,
        world_id: object,
        lease: WorldCleanupLease,
        *,
        provider_ids: frozenset[int],
        reservation: _WorldCleanupReservation,
    ) -> _WorldCleanupHandle:
        """Restore the same pre-owned slot without repeating normal validation."""

        return self._bind_or_restore_exact(
            str(world_id),
            lease,
            provider_ids=provider_ids,
            reservation=reservation,
        )

    def _retain_exact(
        self,
        world_id: object,
        lease: WorldCleanupLease,
        *,
        provider_ids: frozenset[int],
        reservation: _WorldCleanupReservation | None,
    ) -> _WorldCleanupHandle:
        exact_world_id = str(world_id)
        retained = self._bind_or_restore_exact(
            exact_world_id,
            lease,
            provider_ids=provider_ids,
            reservation=reservation,
        )
        self._worlds.validate_cleanup_lease(lease, world_id=exact_world_id)
        return retained

    def _bind_or_restore_exact(
        self,
        exact_world_id: str,
        lease: WorldCleanupLease,
        *,
        provider_ids: frozenset[int],
        reservation: _WorldCleanupReservation | None,
    ) -> _WorldCleanupHandle:
        """Bind lazy canonical cleanup before any fallible lease validation."""

        if reservation is not None and reservation.bound:
            entry = reservation.entry
            if entry is None:
                raise RuntimeError("bound world cleanup reservation has no exact entry")
            if entry.world_id != exact_world_id:
                raise RuntimeError("world cleanup reservation is bound to another world")
            if entry.lease is None:
                return self.promote_registration(
                    exact_world_id,
                    lease,
                    provider_ids=provider_ids,
                    reservation=reservation,
                )
            if entry.lease != lease:
                raise RuntimeError("world cleanup reservation is bound to another world")
            retained = self._entries.get(lease)
            if retained is not None and retained is not entry:
                raise RuntimeError("world cleanup lease has another retained owner")
            self._entries[lease] = entry
            self._associate_providers(entry, provider_ids)
            return _WorldCleanupHandle(entry.reservation)

        retained = self._entries.get(lease)
        if retained is not None:
            if retained.world_id != exact_world_id:
                raise RuntimeError("world cleanup entry is bound to another world")
            self._associate_providers(retained, provider_ids)
            return _WorldCleanupHandle(retained.reservation)

        selected = reservation or self.reserve()
        if selected.bound:
            raise RuntimeError("world cleanup reservation is already bound")
        if selected.reservation.released:
            raise RuntimeError("world cleanup reservation is already released")
        cleanup = _LazyExactWorldCleanup(
            registry=self._worlds,
            lifecycle=self._lifecycle,
            scheduler=self._scheduler,
            world_id=exact_world_id,
            lease=lease,
        )
        entry = _WorldCleanupEntry(
            world_id=exact_world_id,
            lease=lease,
            reservation=selected.reservation,
            provider_ids=set(),
        )
        selected.deferred.bind_exact(
            cleanup,
            on_success=partial(self._release_entry, entry),
        )
        selected.entry = entry
        selected.bound = True
        self._entries[lease] = entry
        self._associate_providers(entry, provider_ids)
        return _WorldCleanupHandle(selected.reservation)

    async def close_current(self, world_id: object) -> None:
        """Join cleanup for the current exact world selected by public destroy."""

        try:
            lease = await self._lifecycle.begin_close(str(world_id))
        except KeyError:
            return
        await self.retain(world_id, lease).aclose()

    async def close_for_provider(self, provider_id: int) -> None:
        """Finish every exact evidence world before closing its provider."""

        entries = tuple(
            sorted(
                self._by_provider.get(provider_id, ()),
                key=lambda entry: entry.world_id,
            )
        )
        if not entries:
            return
        results = await asyncio.gather(
            *(entry.reservation.aclose() for entry in entries),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise BaseExceptionGroup(
                f"physical provider retains {len(failures)} failed evidence cleanup(s)",
                failures,
            )

    def _associate_providers(
        self,
        entry: _WorldCleanupEntry,
        provider_ids: frozenset[int],
    ) -> None:
        for provider_id in provider_ids:
            if provider_id in entry.provider_ids:
                continue
            entry.provider_ids.add(provider_id)
            self._by_provider.setdefault(provider_id, set()).add(entry)

    def _release_entry(self, entry: _WorldCleanupEntry) -> None:
        lease = entry.lease
        if lease is not None and self._entries.get(lease) is entry:
            self._entries.pop(lease)
        for provider_id in tuple(entry.provider_ids):
            entries = self._by_provider.get(provider_id)
            if entries is None:
                continue
            entries.discard(entry)
            if not entries:
                self._by_provider.pop(provider_id)
        entry.provider_ids.clear()


@dataclass(frozen=True, slots=True)
class _PhysicalWorkflowLifetime:
    """Concrete provider lease token injected into one physical workflow."""

    cleanup_lifetimes: _WorldCleanupLifetimes
    cleanup_reservation: _WorldCleanupReservation
    provider_ids: frozenset[int]

    def bind_registration(
        self,
        catalog: ControlCatalog,
        record: WorldRecord,
    ) -> None:
        """Own exact durable retirement before registration may execute."""

        self.cleanup_lifetimes.retain_registration(
            catalog,
            record,
            provider_ids=self.provider_ids,
            reservation=self.cleanup_reservation,
        )

    def promote(
        self,
        world_id: object,
        lease: WorldCleanupLease,
    ) -> None:
        """Promote provisional retirement to canonical exact-world cleanup."""

        self.cleanup_lifetimes.promote_registration(
            world_id,
            lease,
            provider_ids=self.provider_ids,
            reservation=self.cleanup_reservation,
        )

    async def aclose(self) -> None:
        """Join or retry the currently bound cleanup transaction."""

        await self.cleanup_reservation.reservation.aclose()

    def retain_evidence_world(
        self,
        world_id: object,
        lease: WorldCleanupLease,
    ) -> PhysicalEvidenceWorldRetirement:
        """Bind exact cleanup to every provider before the next await."""

        return self.cleanup_lifetimes.retain(
            world_id,
            lease,
            provider_ids=self.provider_ids,
            reservation=self.cleanup_reservation,
        )

    def retain_evidence_world_for_compensation(
        self,
        world_id: object,
        lease: WorldCleanupLease,
    ) -> PhysicalEvidenceWorldRetirement:
        """Recover the pre-owned exact cleanup without normal admission."""

        return self.cleanup_lifetimes.retain_reserved(
            world_id,
            lease,
            provider_ids=self.provider_ids,
            reservation=self.cleanup_reservation,
        )


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
    unsettled_worlds: _UnsettledWorldRouter,
    operation: SubmitMission,
) -> Any:
    reservation = resources.owner(operation.owner_id)
    async with resources.admit_owner_operation(reservation):

        async def construct() -> MissionService:
            sandbox = SandboxService((operation.config.sandbox_backend,))
            reservation.bind(sandbox, close=sandbox.shutdown)
            author_activity_factory = None
            backend = operation.config.sandbox_backend
            if isinstance(backend, ModalSandboxBackend):
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
                driver = operation.config.driver or CodexDriver(
                    model=operation.config.model,
                    workspace=operation.config.workspace,
                )
                executor = ModalMissionAuthorExecutor(
                    capability=capability,
                    barrier=barrier,
                    harness=CodingAgentHarness(
                        driver,
                        CodingAgentHarnessConfig(
                            workspace=operation.config.workspace,
                        ),
                    ),
                    redactor=redaction,
                    config=ModalMissionAuthorExecutorConfig(
                        sandbox_environment=operation.config.sandbox_environment,
                        workspace=operation.config.workspace,
                    ),
                )

                async def bind_author_activity(
                    world_id: str,
                ) -> MissionAuthorActivityBinding:
                    storage_record = await worlds.storage_record(world_id)
                    if storage_record is None:
                        raise WorldNotFoundError(world_id)
                    storage_config = storage_record[0]
                    catalog_path = activity_catalog_path_for(
                        storage_config,
                        control_catalog_config,
                    )
                    physical = SqliteActivityCatalog(catalog_path)
                    values = LocalMissionAuthorValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-author-values"),
                        redactor=redaction,
                    )
                    binding: MissionAuthorActivityBinding

                    async def close_binding() -> None:
                        await physical.close()
                        await unsettled_worlds.unbind(world_id, binding)

                    binding = MissionAuthorActivityBinding(
                        world_id=world_id,
                        owner=f"mission-author:{reservation.owner}",
                        reader=StorageMissionCommittedIntentReader(
                            storage,
                            storage_config,
                        ),
                        catalog=MissionAuthorActivityCoordinator(
                            ActivityCoordinator(physical),
                        ),
                        values=values,
                        executor=executor,
                        stager=WorldMissionAuthorObservationStager(
                            storage=storage,
                            registry=worlds,
                        ),
                        close=close_binding,
                    )
                    await unsettled_worlds.bind(world_id, binding)
                    try:
                        await worlds.bind_required_projector(
                            world_id,
                            binding.required_projector,
                        )
                    except BaseException:
                        await unsettled_worlds.unbind(world_id, binding)
                        await physical.close()
                        raise
                    return binding

                author_activity_factory = bind_author_activity

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
                task_owner=reservation,
                cleanup_factory=cleanup_factory,
                author_activity_factory=author_activity_factory,
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
    unsettled_worlds: _UnsettledWorldRouter,
    artifact_store_config: ArtifactStoreConfig | None,
    research_admissions: research_handlers.AutoResearchAdmissions,
    destroy_world: simulation.DestroyWorldCallable,
    physical_ai_lifetimes: PhysicalClientLifetimeRegistrar,
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
        EvaluatePhysicalTask: cast(
            Any,
            partial(
                physical_ai_handlers.evaluate_physical_task,
                physical_ai_lifetimes,
                worlds,
                lifecycle,
                storage,
            ),
        ),
        SweepPhysicalInstructions: cast(
            Any,
            partial(
                physical_ai_handlers.sweep_physical_instructions,
                physical_ai_lifetimes,
                worlds,
                lifecycle,
                storage,
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
    if model in {EvaluatePhysicalTask, SweepPhysicalInstructions}:
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
    unsettled_worlds: _UnsettledWorldRouter,
    artifact_store_config: ArtifactStoreConfig | None,
    research_admissions: research_handlers.AutoResearchAdmissions,
    destroy_world: simulation.DestroyWorldCallable,
    physical_ai_lifetimes: PhysicalClientLifetimeRegistrar,
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
                    physical_ai_lifetimes=physical_ai_lifetimes,
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
    unsettled_worlds = _UnsettledWorldRouter(config.unsettled_world_oracle)

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
        required_projector_factory=config.required_projector_factory,
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
    physical_ai_lifetimes = _PhysicalAIClientLifetimes(
        resources,
        cleanup_lifetimes,
    )
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
        physical_ai_lifetimes=physical_ai_lifetimes,
        transcripts=transcripts,
        trajectories=trajectories,
    )
    if len(registry.specs) != 47:
        raise RuntimeError("runtime composition did not register exactly 47 operations")
    return resources


__all__ = [
    "RuntimeBootstrapConfig",
    "build_runtime_resources",
]
