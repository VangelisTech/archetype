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

import json
from typing import TYPE_CHECKING

from archetype.app.models import Command, deferred_operation
from archetype.commands.models import DeferredItem, DurableOptions, GetAuditHistory
from archetype.world import query, simulation
from archetype.world.models import (
    AddComponents,
    AddHook,
    AddProcessor,
    AddResource,
    ComponentTypeRef,
    ComponentValue,
    CreateEntities,
    CreateWorld,
    Despawn,
    DestroyWorld,
    DiscoverWorlds,
    ForkWorld,
    GetWorldInfo,
    ListHooks,
    ListProcessors,
    ListResources,
    ListSignatures,
    ListWorlds,
    ListWorldSignatures,
    OpenWorldReadonly,
    QueryArchetype,
    QueryComponents,
    RemoveComponents,
    RemoveHook,
    RemoveProcessor,
    ReserveEntityIds,
    ResumeWorld,
    Run,
    RunEpisode,
    RunRollout,
    Spawn,
    SpawnReserved,
    Step,
    Update,
    WorldInfo,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from archetype.app.artifacts.interfaces import iArtifactService
    from archetype.app.evaluation.interfaces import iEvaluationService
    from archetype.app.missions.interfaces import (
        iMissionService,
        iTrajectoryService,
        iTranscriptIngestionService,
    )
    from archetype.app.physical_ai.interfaces import iPhysicalAIService
    from archetype.app.research.interfaces import iResearchService
    from archetype.commands.dispatch import CommandDispatcher
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


def _component_values(components) -> tuple[ComponentValue, ...]:
    return tuple(ComponentValue.from_component(component) for component in components)


def _component_types(component_types) -> tuple[ComponentTypeRef, ...]:
    return tuple(ComponentTypeRef.from_type(component_type) for component_type in component_types)


def _input_kwargs_json(input_kwargs: dict) -> str:
    return json.dumps(
        input_kwargs,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


class RuntimeApplication:
    """Coordinate actor-free use cases across application families."""

    def __init__(
        self,
        *,
        registry: iWorldRegistry,
        lifecycle: iWorldLifecycle,
        storage: iStorageService,
        dispatcher: CommandDispatcher,
        cancel_world_commands: Callable[[object], Awaitable[int]],
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
        self._dispatcher = dispatcher
        self._cancel_world_commands = cancel_world_commands
        self._research = research
        self._artifacts = artifacts
        self._transcripts = transcripts
        self._evaluations = evaluations
        self._trajectories = trajectories
        self._physical_ai = physical_ai
        self._agent_missions = agent_missions

    def agent_mission_service(self, **kwargs) -> iMissionService:
        """Compose the internal mission workflow port for a trusted runtime handle."""

        if self._agent_missions is None:
            raise RuntimeError("agent mission service is not wired")
        return self._agent_missions(**kwargs)

    async def stop_admission(self) -> None:
        """Reject new top-level calls and wait for every admitted call."""
        await self._dispatcher.stop_admission()
        await self._dispatcher.wait_drained()

    # World mutations -------------------------------------------------

    async def create_entity(self, world_id, components):
        return await self._dispatcher.apply(
            Spawn.from_components(world_id=world_id, components=components)
        )

    async def create_entities(self, world_id, entities):
        return await self._dispatcher.apply(
            CreateEntities.from_entities(world_id=world_id, entities=entities)
        )

    async def reserve_entity_ids(self, world_id, n: int) -> list[int]:
        return await self._dispatcher.apply(ReserveEntityIds(world_id=world_id, count=n))

    async def spawn_with_reserved_id(self, world_id, entity_id, components):
        return await self._dispatcher.apply(
            SpawnReserved(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            )
        )

    async def remove_entity(self, world_id, entity_id):
        return await self._dispatcher.apply(Despawn(world_id=world_id, entity_id=entity_id))

    async def update_entity(self, world_id, entity_id, components):
        return await self._dispatcher.apply(
            Update(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            )
        )

    async def add_components(self, world_id, entity_id, components):
        return await self._dispatcher.apply(
            AddComponents(
                world_id=world_id,
                entity_id=entity_id,
                components=_component_values(components),
            )
        )

    async def remove_components(self, world_id, entity_id, component_types):
        return await self._dispatcher.apply(
            RemoveComponents(
                world_id=world_id,
                entity_id=entity_id,
                component_types=_component_types(component_types),
            )
        )

    async def add_processor(self, world_id, processor):
        return await self._dispatcher.apply(AddProcessor(world_id=world_id, processor=processor))

    async def remove_processor(self, world_id, proc_type):
        return await self._dispatcher.apply(
            RemoveProcessor(world_id=world_id, processor_type=proc_type)
        )

    # World lifecycle -------------------------------------------------

    async def create_world(self, config, storage_config=None, cache_config=None) -> WorldInfo:
        return await self._dispatcher.apply(
            CreateWorld(
                config=config,
                storage_config=storage_config,
                cache_config=cache_config,
            )
        )

    async def fork_world(
        self,
        source_world_id,
        name=None,
        *,
        storage_config=None,
        cache_config=None,
    ) -> WorldInfo:
        return await self._dispatcher.apply(
            ForkWorld(
                source_world_id=source_world_id,
                name=name,
                storage_config=storage_config,
                cache_config=cache_config,
            )
        )

    async def destroy_world(self, world_id) -> None:
        await self._dispatcher.apply(DestroyWorld(world_id=world_id))

    async def _destroy_world_owned(self, world_id) -> None:
        """Finish one already-owned teardown without recursive admission."""
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
            await self._cancel_world_commands(world_id)
        await self._lifecycle.destroy_world(world_id, lease=lease)

    async def get_world_info(self, world_id) -> WorldInfo:
        return await self._dispatcher.apply(GetWorldInfo(world_id=world_id))

    async def list_worlds(self) -> list[WorldInfo]:
        return await self._dispatcher.apply(ListWorlds())

    async def discover_worlds(self, storage_config) -> list[WorldInfo]:
        return await self._dispatcher.apply(DiscoverWorlds(storage_config=storage_config))

    async def open_world_readonly(self, storage_config, world_id) -> WorldInfo:
        return await self._dispatcher.apply(
            OpenWorldReadonly(storage_config=storage_config, world_id=world_id)
        )

    async def resume_world(self, storage_config, world_id) -> WorldInfo:
        return await self._dispatcher.apply(
            ResumeWorld(storage_config=storage_config, world_id=world_id)
        )

    # Simulation and long workflows ----------------------------------

    async def step(self, world_id, run_config, **input_kwargs) -> int:
        return await self._dispatcher.apply(
            Step(
                world_id=world_id,
                run_config=run_config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            )
        )

    async def run(self, world_id, run_config, **input_kwargs):
        return await self._dispatcher.apply(
            Run(
                world_id=world_id,
                run_config=run_config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            )
        )

    async def run_episode(self, world_id, config, **input_kwargs):
        return await self._dispatcher.apply(
            RunEpisode(
                world_id=world_id,
                config=config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            )
        )

    async def run_rollout(self, world_id, config, **input_kwargs):
        return await self._dispatcher.apply(
            RunRollout(
                world_id=world_id,
                config=config,
                input_kwargs_json=_input_kwargs_json(input_kwargs),
            )
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
        return await self._dispatcher.apply(
            QueryComponents(
                components=_component_types(components),
                world_id=world_id,
                run_id=run_id,
                storage_config=storage_config,
                ticks=tuple(ticks) if ticks is not None else None,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            )
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
        return await self._dispatcher.apply(
            QueryArchetype(
                signature=_component_types(sig),
                world_id=world_id,
                run_id=run_id,
                storage_config=storage_config,
                ticks=tuple(ticks) if ticks is not None else None,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
                components=(_component_types(components) if components is not None else None),
            )
        )

    async def list_signatures(self, storage_config=None, *, world_id=None):
        if world_id is None:
            return await self._dispatcher.apply(ListSignatures(storage_config=storage_config))
        return await self._dispatcher.apply(
            ListWorldSignatures(
                world_id=world_id,
                storage_config=storage_config,
            )
        )

    async def add_resource(self, world_id, resource):
        return await self._dispatcher.apply(AddResource(world_id=world_id, resource=resource))

    async def add_hook(self, world_id, event_type, fn, *, mode="blocking"):
        return await self._dispatcher.apply(
            AddHook(
                world_id=world_id,
                event_type=event_type,
                handler=fn,
                mode=mode,
            )
        )

    async def remove_hook(self, world_id, handle):
        return await self._dispatcher.apply(RemoveHook(world_id=world_id, handle=handle))

    async def list_processors(self, world_id):
        return await self._dispatcher.apply(ListProcessors(world_id=world_id))

    async def list_hooks(self, world_id):
        return await self._dispatcher.apply(ListHooks(world_id=world_id))

    async def list_resources(self, world_id):
        return await self._dispatcher.apply(ListResources(world_id=world_id))

    async def get_audit_history(self, world_id=None, **filters):
        if world_id is None:
            raise ValueError("world_id is required for command audit history")
        return await self._dispatcher.apply(GetAuditHistory(world_id=world_id, **filters))

    # Artifacts and evaluation --------------------------------------

    async def ingest_artifacts(self, world_id, sources, *, storage_config=None):
        if self._artifacts is None:
            raise RuntimeError("artifact service is not wired")
        return await self._artifacts.ingest(
            str(world_id),
            sources,
            storage_config=storage_config,
        )

    async def ingest_claude_transcript(self, world_id, source, *, storage_config=None):
        if self._transcripts is None:
            raise RuntimeError("transcript ingestion service is not wired")
        return await self._transcripts.ingest(str(world_id), source, storage_config=storage_config)

    async def query_transcript_rows(self, world_id, *, storage_config=None):
        if self._transcripts is None:
            raise RuntimeError("transcript ingestion service is not wired")
        return await self._transcripts.read(str(world_id), storage_config=storage_config)

    async def query_artifacts(self, world_id, *, storage_config=None):
        if self._artifacts is None:
            raise RuntimeError("artifact service is not wired")
        return await self._artifacts.index(str(world_id), storage_config=storage_config)

    async def run_graders(self, df, graders):
        if self._evaluations is None:
            raise RuntimeError("evaluation service is not wired")
        return await self._evaluations.run_graders(df, graders)

    async def evaluate(self, world_id, components, **kwargs):
        if self._evaluations is None:
            raise RuntimeError("evaluation service is not wired")
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
        await self._dispatcher.apply(GetWorldInfo(world_id=world_id))

    def validate_deferred_command(self, command: Command) -> None:
        deferred_operation("__validation__", command)

    async def submit(
        self,
        world_id,
        command: Command,
        *,
        principal_id=None,
        origin: str = "local",
    ):
        if principal_id is not None or origin != "local":
            raise ValueError("actor-aware deferred admission must use CommandGateway")
        operation, options = deferred_operation(world_id, command)
        return await self._dispatcher.defer(
            operation,
            options,
            command_id=command.id,
            version=command.version,
        )

    async def submit_batch(
        self,
        world_id,
        commands: list[Command],
        *,
        principal_id=None,
        origin: str = "local",
    ):
        if principal_id is not None or origin != "local":
            raise ValueError("actor-aware deferred admission must use CommandGateway")
        items = tuple(
            DeferredItem(
                operation=operation,
                options=options,
                command_id=command.id,
                version=command.version,
            )
            for command in commands
            for operation, options in (deferred_operation(world_id, command),)
        )
        return await self._dispatcher.defer_batch(items)

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
        if principal_id is not None or origin != "local":
            raise ValueError("actor-aware deferred admission must use CommandGateway")
        return await self._dispatcher.defer_spawn(
            Spawn.from_components(world_id=world_id, components=components),
            DurableOptions(target_tick=tick, priority=priority),
        )
