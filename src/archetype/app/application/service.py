# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Actor-free facade over the application service graph.

``RuntimeApplication`` is the canonical application boundary. Trusted hosts
call it directly; untrusted adapters reach it only after ``CommandGateway``
has authenticated and authorized the caller. It owns process-lifetime
admission, while ``WorldRegistry`` owns exact-world synchronization and cleanup
authority.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

from archetype.app.models import Command
from archetype.world import handlers, mutation, query, simulation
from archetype.world.models import ListHooks, ListProcessors, ListResources, WorldInfo

if TYPE_CHECKING:
    from collections.abc import Callable

    from archetype.app.artifacts.interfaces import iArtifactService
    from archetype.app.audit.interfaces import iAuditLog
    from archetype.app.commands.interfaces import iCommandScheduler
    from archetype.app.evaluation.interfaces import iEvaluationService
    from archetype.app.missions.interfaces import (
        iMissionService,
        iTrajectoryService,
        iTranscriptIngestionService,
    )
    from archetype.app.physical_ai.interfaces import iPhysicalAIService
    from archetype.app.research.interfaces import iResearchService
    from archetype.physical_ai.contracts import (
        InstructionSweepConfig,
        InstructionSweepReport,
        PhysicalTaskEvalConfig,
        PhysicalTaskEvalReport,
    )
    from archetype.physical_ai.manipulation import EnvClient
    from archetype.physical_ai.policy import PolicyClient
    from archetype.storage.interfaces import iStorageService
    from archetype.world.interfaces import iWorldLifecycle, iWorldRegistry


_ACTIVE_APPLICATION: ContextVar[RuntimeApplication | None] = ContextVar(
    "archetype_active_application", default=None
)


def _world_info(world) -> WorldInfo:
    return WorldInfo(
        world_id=world.world_id,
        name=world.name,
        tick=getattr(world, "tick", 0),
        run_id=world.run_id,
    )


class RuntimeApplication:
    """Coordinate actor-free use cases across application families."""

    def __init__(
        self,
        *,
        registry: iWorldRegistry,
        lifecycle: iWorldLifecycle,
        storage: iStorageService,
        commands: iCommandScheduler,
        audit: iAuditLog | None = None,
        research: iResearchService | None = None,
        artifacts: iArtifactService | None = None,
        transcripts: iTranscriptIngestionService | None = None,
        evaluations: iEvaluationService | None = None,
        trajectories: iTrajectoryService | None = None,
        physical_ai: iPhysicalAIService | None = None,
        agent_missions: Callable[..., iMissionService] | None = None,
    ) -> None:
        self._registry = registry
        self._lifecycle = lifecycle
        self._storage = storage
        self._commands = commands
        self._audit = audit
        self._research = research
        self._artifacts = artifacts
        self._transcripts = transcripts
        self._evaluations = evaluations
        self._trajectories = trajectories
        self._physical_ai = physical_ai
        self._agent_missions = agent_missions

        self._state_lock = asyncio.Lock()
        self._drained = asyncio.Event()
        self._drained.set()
        self._active_operations = 0
        self._accepting = True
        self._create_lock = asyncio.Lock()

    def agent_mission_service(self, **kwargs) -> iMissionService:
        """Compose the internal mission workflow port for a trusted runtime handle."""

        if not self._accepting:
            raise RuntimeError("RuntimeApplication is shutting down")
        if self._agent_missions is None:
            raise RuntimeError("agent mission service is not wired")
        return self._agent_missions(**kwargs)

    @asynccontextmanager
    async def _admit(self):
        """Track top-level process work without granting world authority.

        ``_ACTIVE_APPLICATION`` is a temporary PR-4 nesting marker only.
        Inherited child tasks must still acquire and validate the registry's
        exact-world operation or cleanup lease.
        """
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

    async def stop_admission(self) -> None:
        """Reject new top-level calls and wait for every admitted call."""
        async with self._state_lock:
            self._accepting = False
        await self._drained.wait()

    # World mutations -------------------------------------------------

    async def create_entity(self, world_id, components):
        async with self._admit():
            return await mutation.create_entity(self._registry, world_id, components)

    async def create_entities(self, world_id, entities):
        async with self._admit():
            return await mutation.create_entities(self._registry, world_id, entities)

    async def reserve_entity_ids(self, world_id, n: int) -> list[int]:
        async with self._admit():
            return await mutation.reserve_entity_ids(self._registry, world_id, n)

    async def spawn_with_reserved_id(self, world_id, entity_id, components):
        async with self._admit():
            return await mutation.spawn_with_reserved_id(
                self._registry,
                world_id,
                entity_id,
                components,
            )

    async def remove_entity(self, world_id, entity_id):
        async with self._admit():
            return await mutation.remove_entity(self._registry, world_id, entity_id)

    async def update_entity(self, world_id, entity_id, components):
        async with self._admit():
            return await mutation.update_entity(
                self._registry,
                world_id,
                entity_id,
                components,
            )

    async def add_components(self, world_id, entity_id, components):
        async with self._admit():
            return await mutation.add_components(
                self._registry,
                world_id,
                entity_id,
                components,
            )

    async def remove_components(self, world_id, entity_id, component_types):
        async with self._admit():
            return await mutation.remove_components(
                self._registry,
                world_id,
                entity_id,
                component_types,
            )

    async def add_processor(self, world_id, processor):
        async with self._admit():
            return await mutation.add_processor(self._registry, world_id, processor)

    async def remove_processor(self, world_id, proc_type):
        async with self._admit():
            return await mutation.remove_processor(self._registry, world_id, proc_type)

    # World lifecycle -------------------------------------------------

    async def create_world(self, config, storage_config=None, cache_config=None) -> WorldInfo:
        async with self._admit(), self._create_lock:
            return _world_info(
                await self._lifecycle.create_world(config, storage_config, cache_config)
            )

    async def fork_world(
        self,
        source_world_id,
        name=None,
        *,
        storage_config=None,
        cache_config=None,
    ) -> WorldInfo:
        async with self._admit():
            return _world_info(
                await self._lifecycle.fork_world(
                    source_world_id,
                    name,
                    storage_config,
                    cache_config,
                )
            )

    async def destroy_world(self, world_id) -> None:
        async with self._admit():
            try:
                lease = await self._registry.begin_close(str(world_id))
            except KeyError:
                # Destroy remains idempotent when no live world is bound.
                return
            async with self._registry.cleanup_operation(lease) as world:
                await simulation.reconcile_committed_work_locked(
                    self._registry,
                    str(world_id),
                    world,
                )
                await self._commands.cancel_world(world_id)
            await self._lifecycle.destroy_world(world_id, lease=lease)

    async def get_world_info(self, world_id) -> WorldInfo:
        async with self._admit(), self._registry.operation(str(world_id)) as world:
            await simulation.reconcile_committed_work_locked(
                self._registry,
                str(world_id),
                world,
            )
            return _world_info(world)

    async def list_worlds(self) -> list[WorldInfo]:
        async with self._admit():
            snapshot = await self._registry.list_worlds()
            world_ids = [str(world.world_id) for world in snapshot]
            infos: list[WorldInfo] = []
            # Reconcile each snapshotted world under its own lock. Recovery
            # runs user hooks and required projectors, so holding sibling
            # locks here could deadlock a callback that targets another world.
            # A close racing the snapshot fails the whole call closed.
            for world_id in world_ids:
                async with self._registry.operation(world_id) as world:
                    await simulation.reconcile_committed_work_locked(
                        self._registry,
                        world_id,
                        world,
                    )
                    infos.append(_world_info(world))
            return infos

    async def discover_worlds(self, storage_config) -> list[WorldInfo]:
        async with self._admit():
            return await self._lifecycle.discover_worlds(storage_config)

    async def open_world_readonly(self, storage_config, world_id) -> WorldInfo:
        async with self._admit():
            return await self._lifecycle.open_world_readonly(storage_config, world_id)

    async def resume_world(self, storage_config, world_id) -> WorldInfo:
        async with self._admit():
            return _world_info(await self._lifecycle.open_world_mutable(storage_config, world_id))

    # Simulation and long workflows ----------------------------------

    async def step(self, world_id, run_config, **input_kwargs) -> int:
        async with self._admit():
            return await simulation.step(
                self._registry,
                world_id,
                run_config,
                **input_kwargs,
            )

    async def run(self, world_id, run_config, **input_kwargs):
        async with self._admit():
            return await simulation.run(
                self._registry,
                world_id,
                run_config,
                **input_kwargs,
            )

    async def run_episode(self, world_id, config, **input_kwargs):
        async with self._admit():
            return await simulation.run_episode(
                self._registry,
                self._storage,
                world_id,
                config,
                **input_kwargs,
            )

    async def run_rollout(self, world_id, config, **input_kwargs):
        async with self._admit():
            return await simulation.run_rollout(
                self._registry,
                self._storage,
                self._lifecycle.fork_world,
                self._lifecycle.destroy_world,
                world_id,
                config,
                **input_kwargs,
            )

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

    async def evaluate_physical_task(
        self,
        config: PhysicalTaskEvalConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient | None = None,
    ) -> PhysicalTaskEvalReport:
        """Run one ledger-backed physical evaluation through its owning service."""

        if self._physical_ai is None:
            raise RuntimeError("physical AI service is not wired")
        async with self._admit():
            return await self._physical_ai.evaluate_task(
                config,
                env_client=env_client,
                policy_client=policy_client,
            )

    async def sweep_physical_instructions(
        self,
        config: InstructionSweepConfig,
        *,
        env_client: EnvClient,
        policy_client: PolicyClient,
    ) -> InstructionSweepReport:
        """Run one paired-seed instruction sweep through its owning service."""

        if self._physical_ai is None:
            raise RuntimeError("physical AI service is not wired")
        async with self._admit():
            return await self._physical_ai.sweep_instructions(
                config,
                env_client=env_client,
                policy_client=policy_client,
            )

    # Query and introspection ----------------------------------------

    async def _resolve_storage(self, world_id, storage_config):
        if storage_config is not None:
            return storage_config
        record = await self._registry.storage_record(str(world_id))
        return record[0] if record is not None else None

    async def _resolve_lineage(self, world_id, run_id, storage_config):
        # Catch only a missing binding. A live-but-closing world raises
        # RuntimeError and must not be reclassified as a cold durable read.
        try:
            async with self._registry.operation(str(world_id)) as world:
                lineage = getattr(world, "lineage", None)
                return list(lineage) if lineage else None
        except KeyError:
            return await query.get_lineage(
                self._storage,
                str(world_id),
                str(run_id),
                storage_config,
            )

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
            storage_config = await self._resolve_storage(world_id, storage_config)
            return await query.query_components(
                self._storage,
                components,
                str(world_id),
                str(run_id),
                storage_config,
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
            storage_config = await self._resolve_storage(world_id, storage_config)
            return await query.query_archetype(
                self._storage,
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
                storage_config = await self._resolve_storage(world_id, storage_config)
            return await query.list_signatures(self._storage, storage_config)

    async def add_resource(self, world_id, resource):
        async with self._admit():
            return await mutation.add_resource(self._registry, world_id, resource)

    async def add_hook(self, world_id, event_type, fn, *, mode="blocking"):
        async with self._admit():
            return await mutation.add_hook(
                self._registry,
                world_id,
                event_type,
                fn,
                mode=mode,
            )

    async def remove_hook(self, world_id, handle):
        async with self._admit():
            return await mutation.remove_hook(self._registry, world_id, handle)

    async def list_processors(self, world_id):
        async with self._admit():
            return await handlers.list_processors(
                self._registry,
                ListProcessors(world_id=world_id),
            )

    async def list_hooks(self, world_id):
        async with self._admit():
            return await handlers.list_hooks(
                self._registry,
                ListHooks(world_id=world_id),
            )

    async def list_resources(self, world_id):
        async with self._admit():
            return await handlers.list_resources(
                self._registry,
                ListResources(world_id=world_id),
            )

    async def get_audit_history(self, world_id=None, **filters):
        async with self._admit():
            if self._audit is None:
                return []
            return await self._audit.query(world_id=world_id, **filters)

    # Artifacts and evaluation --------------------------------------

    async def ingest_artifacts(self, world_id, sources, *, storage_config=None):
        if self._artifacts is None:
            raise RuntimeError("artifact service is not wired")
        async with self._admit():
            return await self._artifacts.ingest(
                str(world_id),
                sources,
                storage_config=storage_config,
            )

    async def ingest_claude_transcript(self, world_id, source, *, storage_config=None):
        if self._transcripts is None:
            raise RuntimeError("transcript ingestion service is not wired")
        async with self._admit():
            return await self._transcripts.ingest(
                str(world_id), source, storage_config=storage_config
            )

    async def query_transcript_rows(self, world_id, *, storage_config=None):
        if self._transcripts is None:
            raise RuntimeError("transcript ingestion service is not wired")
        async with self._admit():
            return await self._transcripts.read(str(world_id), storage_config=storage_config)

    async def query_artifacts(self, world_id, *, storage_config=None):
        if self._artifacts is None:
            raise RuntimeError("artifact service is not wired")
        async with self._admit():
            return await self._artifacts.index(str(world_id), storage_config=storage_config)

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
            storage_config = await self._resolve_storage(world_id, storage_config)
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
            storage_config = await self._resolve_storage(world_id, storage_config)
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

    async def require_world(self, world_id) -> None:
        async with self._admit():
            await self._commands.require_world(world_id)

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
        async with self._admit():
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
        async with self._admit():
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
        async with self._admit():
            return await self._commands.admit_spawn(
                world_id,
                components,
                tick=tick,
                priority=priority,
                principal_id=principal_id,
                origin=origin,
            )
