# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Actor-free facade over the application service graph.

``RuntimeApplication`` is the canonical application boundary. Trusted hosts
call it directly; untrusted adapters reach it only after ``CommandGateway``
has authenticated and authorized the caller. It owns operation admission and
same-world serialization, but no storage or domain state of its own.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from uuid_utils import UUID

from archetype.app.models import (
    Command,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    WorldInfo,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from archetype.app.artifacts.interfaces import (
        iArtifactBundleService,
        iArtifactService,
        iArtifactTableService,
    )
    from archetype.app.audit.interfaces import iAuditLog
    from archetype.app.commands.interfaces import iCommandScheduler
    from archetype.app.evaluation.interfaces import iEvaluationService
    from archetype.app.missions.interfaces import iAgentMissionService, iTrajectoryService
    from archetype.app.query.interfaces import iQueryService
    from archetype.app.research.interfaces import iResearchService
    from archetype.app.world.interfaces import iMutationService, iSimulationService, iWorldService


_ACTIVE_APPLICATION: ContextVar[RuntimeApplication | None] = ContextVar(
    "archetype_active_application", default=None
)
_HELD_WORLD_LANES: ContextVar[frozenset[str]] = ContextVar(
    "archetype_held_world_lanes", default=frozenset()
)


@dataclass
class _WorldLane:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _world_info(world) -> WorldInfo:
    return WorldInfo(
        world_id=world.world_id,
        name=world.name,
        tick=getattr(world, "tick", 0),
        run_id=getattr(world, "run_id", None),
    )


class RuntimeApplication:
    """Coordinate actor-free use cases across application families."""

    def __init__(
        self,
        *,
        mutations: iMutationService,
        worlds: iWorldService,
        simulation: iSimulationService,
        queries: iQueryService,
        commands: iCommandScheduler,
        audit: iAuditLog | None = None,
        research: iResearchService | None = None,
        artifact_tables: iArtifactTableService | None = None,
        artifacts: iArtifactService | None = None,
        artifact_bundles: iArtifactBundleService | None = None,
        evaluations: iEvaluationService | None = None,
        trajectories: iTrajectoryService | None = None,
        agent_missions: Callable[..., iAgentMissionService] | None = None,
    ) -> None:
        self._mutations = mutations
        self._worlds = worlds
        self._simulation = simulation
        self._queries = queries
        self._commands = commands
        self._audit = audit
        self._research = research
        self._artifact_tables = artifact_tables
        self._artifacts = artifacts
        self._artifact_bundles = artifact_bundles
        self._evaluations = evaluations
        self._trajectories = trajectories
        self._agent_missions = agent_missions

        self._state_lock = asyncio.Lock()
        self._drained = asyncio.Event()
        self._drained.set()
        self._active_operations = 0
        self._accepting = True
        self._lanes: dict[str, _WorldLane] = {}
        self._create_lock = asyncio.Lock()

    def agent_mission_service(self, **kwargs) -> iAgentMissionService:
        """Compose the internal mission workflow port for a trusted runtime handle."""

        if not self._accepting:
            raise RuntimeError("RuntimeApplication is shutting down")
        if self._agent_missions is None:
            raise RuntimeError("agent mission service is not wired")
        return self._agent_missions(**kwargs)

    @asynccontextmanager
    async def _admit(self):
        if _ACTIVE_APPLICATION.get() is self:
            yield
            return

        async with self._state_lock:
            if not self._accepting:
                raise RuntimeError("RuntimeApplication is shutting down")
            self._active_operations += 1
            self._drained.clear()

        token = _ACTIVE_APPLICATION.set(self)
        try:
            yield
        finally:
            _ACTIVE_APPLICATION.reset(token)
            async with self._state_lock:
                self._active_operations -= 1
                if self._active_operations == 0:
                    self._drained.set()

    @asynccontextmanager
    async def _world_operation(self, world_id: str | UUID):
        """Serialize operations for one world while allowing worlds to parallelize."""
        key = str(world_id)
        held = _HELD_WORLD_LANES.get()
        if key in held:
            yield
            return

        async with self._state_lock:
            lane = self._lanes.setdefault(key, _WorldLane())
        async with lane.lock:
            token = _HELD_WORLD_LANES.set(held | {key})
            try:
                yield
            finally:
                _HELD_WORLD_LANES.reset(token)

    async def stop_admission(self) -> None:
        """Reject new top-level calls and wait for every admitted call."""
        async with self._state_lock:
            self._accepting = False
        await self._drained.wait()

    # World mutations -------------------------------------------------

    async def create_entity(self, world_id, components):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.create_entity(world_id, components)

    async def create_entities(self, world_id, entities):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.create_entities(world_id, entities)

    def reserve_entity_ids(self, world_id, n: int) -> list[int]:
        if not self._accepting:
            raise RuntimeError("RuntimeApplication is shutting down")
        return self._mutations.reserve_entity_ids(world_id, n)

    async def spawn_with_reserved_id(self, world_id, entity_id, components):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.spawn_with_reserved_id(world_id, entity_id, components)

    async def remove_entity(self, world_id, entity_id):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.remove_entity(world_id, entity_id)

    async def update_entity(self, world_id, entity_id, components):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.update_entity(world_id, entity_id, components)

    async def add_components(self, world_id, entity_id, components):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.add_components(world_id, entity_id, components)

    async def remove_components(self, world_id, entity_id, component_types):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.remove_components(world_id, entity_id, component_types)

    async def add_processor(self, world_id, processor):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.add_processor(world_id, processor)

    async def remove_processor(self, world_id, proc_type):
        async with self._admit(), self._world_operation(world_id):
            return await self._mutations.remove_processor(world_id, proc_type)

    # World lifecycle -------------------------------------------------

    async def create_world(self, config, storage_config=None, cache_config=None) -> WorldInfo:
        async with self._admit(), self._create_lock:
            return _world_info(
                await self._worlds.create_world(config, storage_config, cache_config)
            )

    async def fork_world(
        self,
        source_world_id,
        name=None,
        *,
        storage_config=None,
        cache_config=None,
    ) -> WorldInfo:
        async with self._admit(), self._world_operation(source_world_id):
            return _world_info(
                await self._worlds.fork_world(source_world_id, name, storage_config, cache_config)
            )

    async def destroy_world(self, world_id) -> None:
        async with self._admit(), self._world_operation(world_id):
            if not self._worlds.has_world(world_id):
                # Destroy is an idempotent lifecycle operation. There is no
                # live world's catalog to cancel against in this case.
                return
            await self._commands.cancel_world(world_id)
            await self._worlds.destroy_world(world_id)

    async def get_world_info(self, world_id) -> WorldInfo:
        async with self._admit(), self._world_operation(world_id):
            return _world_info(self._worlds.get_world(UUID(str(world_id))))

    async def list_worlds(self) -> list[WorldInfo]:
        async with self._admit():
            return [_world_info(world) for world in self._worlds.list_worlds()]

    async def discover_worlds(self, storage_config) -> list[WorldInfo]:
        async with self._admit():
            return await self._worlds.discover_worlds(storage_config)

    async def open_world_readonly(self, storage_config, world_id) -> WorldInfo:
        async with self._admit():
            return await self._worlds.open_world_readonly(storage_config, world_id)

    async def resume_world(self, storage_config, world_id) -> WorldInfo:
        async with self._admit(), self._world_operation(world_id):
            return _world_info(await self._worlds.open_world_mutable(storage_config, world_id))

    # Simulation and long workflows ----------------------------------

    async def step(self, world_id, run_config, **input_kwargs) -> int:
        async with self._admit(), self._world_operation(world_id):
            return await self._simulation.step(world_id, run_config, **input_kwargs)

    async def run(self, world_id, run_config, **input_kwargs):
        async with self._admit(), self._world_operation(world_id):
            return await self._simulation.run(world_id, run_config, **input_kwargs)

    async def run_episode(self, world_id, config, **input_kwargs):
        async with self._admit(), self._world_operation(world_id):
            return await self._simulation.run_episode(world_id, config, **input_kwargs)

    async def run_rollout(self, world_id, config, **input_kwargs):
        async with self._admit():
            return await self._simulation.run_rollout(world_id, config, **input_kwargs)

    async def autoresearch(
        self,
        world_id,
        config,
        evaluator,
        *,
        prepare_candidate=None,
        lab_world_id=None,
        on_iteration=None,
    ):
        if self._research is None:
            raise RuntimeError("research service is not wired")
        async with self._admit():
            return await self._research.run(
                world_id,
                config,
                evaluator,
                prepare_candidate=prepare_candidate,
                lab_world_id=lab_world_id,
                on_iteration=on_iteration,
            )

    # Query and introspection ----------------------------------------

    def _resolve_storage(self, world_id, storage_config):
        if storage_config is not None:
            return storage_config
        record = self._worlds.storage_record(world_id)
        return record[0] if record is not None else None

    async def _resolve_lineage(self, world_id, run_id, storage_config):
        try:
            world = self._worlds.get_world(UUID(str(world_id)))
        except Exception:
            world = None
        if world is not None:
            lineage = getattr(world, "lineage", None)
            return list(lineage) if lineage else None
        return await self._queries.get_lineage(str(world_id), str(run_id), storage_config)

    async def query_components(
        self,
        components,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
    ):
        async with self._admit():
            storage_config = self._resolve_storage(world_id, storage_config)
            return await self._queries.query_components(
                components=components,
                world_id=str(world_id),
                run_id=str(run_id),
                storage_config=storage_config,
                ticks=ticks,
                entity_ids=entity_ids,
                lineage=await self._resolve_lineage(world_id, run_id, storage_config),
            )

    async def query_archetype(
        self,
        sig,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
        components=None,
    ):
        async with self._admit():
            storage_config = self._resolve_storage(world_id, storage_config)
            return await self._queries.query_archetype(
                sig,
                str(world_id),
                str(run_id),
                storage_config,
                ticks=ticks,
                entity_ids=entity_ids,
                components=components,
                lineage=await self._resolve_lineage(world_id, run_id, storage_config),
            )

    async def list_signatures(self, storage_config=None, *, world_id=None):
        async with self._admit():
            if world_id is not None:
                storage_config = self._resolve_storage(world_id, storage_config)
            return await self._queries.list_signatures(storage_config)

    async def add_resource(self, world_id, resource):
        async with self._admit(), self._world_operation(world_id):
            return await self._worlds.add_resource(world_id, resource)

    async def add_hook(self, world_id, event_type, fn, *, mode="blocking"):
        async with self._admit(), self._world_operation(world_id):
            return self._worlds.add_hook(world_id, event_type, fn, mode=mode)

    async def remove_hook(self, world_id, handle):
        async with self._admit(), self._world_operation(world_id):
            return self._worlds.remove_hook(world_id, handle)

    async def list_processors(self, world_id) -> list[ProcessorInfo]:
        async with self._admit():
            return [
                ProcessorInfo(
                    qualname=f"{type(proc).__module__}.{type(proc).__qualname__}",
                    priority=getattr(proc, "priority", 0),
                    components=tuple(
                        f"{component.__module__}.{component.__qualname__}"
                        for component in getattr(proc, "components", ())
                    ),
                )
                for proc in self._worlds.list_processors(world_id)
            ]

    async def list_hooks(self, world_id) -> list[HookInfo]:
        async with self._admit():
            return [
                HookInfo(
                    event_type=event_type.__name__,
                    handler_qualname=getattr(fn, "__qualname__", str(fn)),
                    mode=mode,
                    handle_id=handle.id,
                )
                for event_type, handle, fn, mode in self._worlds.list_hooks(world_id)
            ]

    async def list_resources(self, world_id) -> list[ResourceInfo]:
        async with self._admit():
            return [
                ResourceInfo(qualname=f"{resource_type.__module__}.{resource_type.__qualname__}")
                for resource_type, _resource in self._worlds.list_resources(world_id)
            ]

    async def get_audit_history(self, world_id=None, **filters):
        async with self._admit():
            if self._audit is None:
                return []
            return await self._audit.query(world_id=world_id, **filters)

    # Artifacts and evaluation --------------------------------------

    async def ingest_artifact(
        self,
        world_id,
        components,
        *,
        external_id,
        producer="default",
        storage_config=None,
    ):
        if self._artifacts is None:
            raise RuntimeError("artifact service is not wired")
        async with self._admit():
            return await self._artifacts.publish(
                str(world_id),
                components,
                external_id=external_id,
                producer=producer,
                storage_config=storage_config,
            )

    async def ingest_files(self, world_id, paths, processor, *, storage_config=None):
        if self._artifact_tables is None:
            raise RuntimeError("artifact table service is not wired")
        async with self._admit():
            return await self._artifact_tables.ingest_files(
                str(world_id), paths, processor, storage_config=storage_config
            )

    async def write_artifacts(self, world_id, table_name, artifacts, *, storage_config=None):
        if self._artifact_tables is None:
            raise RuntimeError("artifact table service is not wired")
        async with self._admit():
            return await self._artifact_tables.write_artifacts(
                str(world_id), table_name, artifacts, storage_config=storage_config
            )

    async def query_artifacts(self, world_id, table_name, *, storage_config=None):
        if self._artifact_tables is None:
            raise RuntimeError("artifact table service is not wired")
        async with self._admit():
            return await self._artifact_tables.read_artifacts(
                str(world_id), table_name, storage_config=storage_config
            )

    async def publish_artifact_bundle(self, request, *, storage_config=None):
        if self._artifact_bundles is None:
            raise RuntimeError("artifact bundle service is not wired")
        async with self._admit(), self._world_operation(request.world_id):
            return await self._artifact_bundles.publish(request, storage_config=storage_config)

    async def query_artifact_bundles(self, world_id, run_id, *, attempt_id=None, kinds=None):
        if self._artifact_bundles is None:
            raise RuntimeError("artifact bundle service is not wired")
        async with self._admit():
            return await self._artifact_bundles.query(
                str(world_id), str(run_id), attempt_id=attempt_id, kinds=kinds
            )

    async def reconcile_artifact_bundles(self, world_id, *, storage_config=None, limit=100):
        if self._artifact_bundles is None:
            raise RuntimeError("artifact bundle service is not wired")
        async with self._admit(), self._world_operation(world_id):
            return await self._artifact_bundles.reconcile(
                str(world_id), storage_config=storage_config, limit=limit
            )

    async def run_graders(self, df, graders):
        if self._evaluations is None:
            raise RuntimeError("evaluation service is not wired")
        async with self._admit():
            return await self._evaluations.run_graders(df, graders)

    async def evaluate(self, world_id, components, **kwargs):
        if self._evaluations is None:
            raise RuntimeError("evaluation service is not wired")
        async with self._admit():
            return await self._evaluations.evaluate(str(world_id), components, **kwargs)

    async def query_trajectory(
        self,
        component,
        world_id,
        run_id,
        storage_config=None,
        **kwargs,
    ):
        if self._trajectories is None:
            raise RuntimeError("trajectory service is not wired")
        async with self._admit():
            storage_config = self._resolve_storage(world_id, storage_config)
            return await self._trajectories.query(
                component,
                world_id=str(world_id),
                run_id=str(run_id),
                storage_config=storage_config,
                lineage=await self._resolve_lineage(world_id, run_id, storage_config),
                **kwargs,
            )

    async def grade_trajectory(
        self,
        component,
        world_id,
        run_id,
        *,
        graders,
        storage_config=None,
        **kwargs,
    ):
        if self._trajectories is None:
            raise RuntimeError("trajectory service is not wired")
        async with self._admit():
            storage_config = self._resolve_storage(world_id, storage_config)
            return await self._trajectories.grade(
                component,
                world_id=str(world_id),
                run_id=str(run_id),
                graders=graders,
                storage_config=storage_config,
                lineage=await self._resolve_lineage(world_id, run_id, storage_config),
                **kwargs,
            )

    # Deferred commands ---------------------------------------------

    def require_world(self, world_id) -> None:
        self._commands.require_world(world_id)

    def validate_deferred_command(self, command: Command) -> None:
        self._commands.validate_deferred(command)

    async def submit(
        self,
        world_id,
        command: Command,
        *,
        principal_id=None,
        origin: str = "local",
    ):
        async with self._admit(), self._world_operation(world_id):
            return await self._commands.admit(
                world_id,
                command,
                principal_id=principal_id,
                origin=origin,
            )

    async def submit_batch(
        self,
        world_id,
        commands: list[Command],
        *,
        principal_id=None,
        origin: str = "local",
    ):
        async with self._admit(), self._world_operation(world_id):
            return await self._commands.admit_batch(
                world_id,
                commands,
                principal_id=principal_id,
                origin=origin,
            )

    async def submit_spawn(
        self,
        world_id,
        components,
        *,
        tick=0,
        priority=0,
        principal_id=None,
        origin: str = "local",
    ):
        async with self._admit(), self._world_operation(world_id):
            return await self._commands.admit_spawn(
                world_id,
                components,
                tick=tick,
                priority=priority,
                principal_id=principal_id,
                origin=origin,
            )

    async def drain_and_apply(self, world_id, tick: int) -> list[Command]:
        return await self._commands.drain_and_apply(world_id, tick)
