# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Authorization gateway for untrusted adapters.

The gateway translates authenticated ``ActorCtx`` calls into actor-free
``RuntimeApplication`` use cases. It owns RBAC, quota debit, safe access
audit metadata, and no domain workflow or durable command state.
"""

from __future__ import annotations

import json
import logging
import math
from typing import TYPE_CHECKING

from archetype._obs import instrument
from archetype.app.application.interfaces import iRuntimeApplication
from archetype.app.audit.interfaces import iAuditLog
from archetype.app.audit.models import make_audit_row
from archetype.app.gateway.auth.guard import guardrail_allow, guardrail_check, guardrail_commit
from archetype.app.models import Command, CommandType

if TYPE_CHECKING:
    from archetype.app.gateway.auth.models import ActorCtx

logger = logging.getLogger(__name__)


class CommandGateway:
    """Authenticate/authorize one untrusted call, then delegate it."""

    def __init__(
        self,
        application: iRuntimeApplication,
        audit: iAuditLog | None = None,
    ) -> None:
        self._application = application
        self._audit = audit

    @staticmethod
    def _gate(command: Command, ctx: ActorCtx) -> None:
        guardrail_allow(command, ctx)

    @staticmethod
    def _gate_batch(commands: list[Command], ctx: ActorCtx) -> None:
        projected_tokens = 0
        for index, command in enumerate(commands):
            projected_tokens += guardrail_check(
                command,
                ctx,
                projected_count=index,
                projected_tokens=projected_tokens,
            )
        guardrail_commit(ctx, count=len(commands), tokens=projected_tokens)

    async def _emit(
        self,
        ctx: ActorCtx,
        command_type: str,
        world_id=None,
        *,
        command_id=None,
        status: str = "applied",
        payload_json: str = "{}",
    ) -> None:
        if self._audit is None:
            return
        try:
            await self._audit.record(
                make_audit_row(
                    ctx,
                    command_type,
                    world_id,
                    command_id=command_id,
                    status=status,
                    payload_json=payload_json,
                )
            )
        except Exception:
            # This is access-decision evidence, not the authority for domain
            # completion. Durable workflow transitions use family-owned
            # transactional outboxes and must never depend on this projection.
            logger.warning("audit emission failed", exc_info=True)

    # Mutations ------------------------------------------------------

    @instrument("gateway.create_entity")
    async def create_entity(self, ctx, world_id, components):
        self._gate(Command(type=CommandType.SPAWN), ctx)
        result = await self._application.create_entity(world_id, components)
        await self._emit(ctx, "spawn", world_id)
        return result

    async def create_entities(self, ctx, world_id, entities):
        self._gate(Command(type=CommandType.SPAWN), ctx)
        result = await self._application.create_entities(world_id, entities)
        await self._emit(
            ctx, "spawn_batch", world_id, payload_json=json.dumps({"count": len(entities)})
        )
        return result

    def reserve_entity_ids(self, ctx, world_id, n):
        self._gate(Command(type=CommandType.SPAWN), ctx)
        return self._application.reserve_entity_ids(world_id, n)

    async def spawn_with_reserved_id(self, ctx, world_id, entity_id, components):
        self._gate(Command(type=CommandType.SPAWN), ctx)
        await self._application.spawn_with_reserved_id(world_id, entity_id, components)
        await self._emit(
            ctx,
            "spawn_reserved",
            world_id,
            payload_json=json.dumps({"entity_id": entity_id}),
        )

    async def remove_entity(self, ctx, world_id, entity_id):
        self._gate(Command(type=CommandType.DESPAWN), ctx)
        await self._application.remove_entity(world_id, entity_id)
        await self._emit(ctx, "despawn", world_id)

    async def update_entity(self, ctx, world_id, entity_id, components):
        self._gate(Command(type=CommandType.UPDATE), ctx)
        await self._application.update_entity(world_id, entity_id, components)
        await self._emit(ctx, "update", world_id)

    async def add_components(self, ctx, world_id, entity_id, components):
        self._gate(Command(type=CommandType.ADD_COMPONENT), ctx)
        await self._application.add_components(world_id, entity_id, components)
        await self._emit(ctx, "add_component", world_id)

    async def remove_components(self, ctx, world_id, entity_id, component_types):
        self._gate(Command(type=CommandType.REMOVE_COMPONENT), ctx)
        await self._application.remove_components(world_id, entity_id, component_types)
        await self._emit(ctx, "remove_component", world_id)

    async def add_processor(self, ctx, world_id, processor):
        self._gate(Command(type=CommandType.ADD_PROCESSOR), ctx)
        await self._application.add_processor(world_id, processor)
        await self._emit(ctx, "add_processor", world_id)

    async def remove_processor(self, ctx, world_id, proc_type):
        self._gate(Command(type=CommandType.REMOVE_PROCESSOR), ctx)
        await self._application.remove_processor(world_id, proc_type)
        await self._emit(ctx, "remove_processor", world_id)

    # Lifecycle ------------------------------------------------------

    @instrument("gateway.create_world")
    async def create_world(self, ctx, config, storage_config=None, cache_config=None):
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        info = await self._application.create_world(config, storage_config, cache_config)
        await self._emit(ctx, "create_world", info.world_id)
        return info

    async def fork_world(
        self,
        ctx,
        source_world_id,
        name=None,
        *,
        storage_config=None,
        cache_config=None,
    ):
        self._gate(Command(type=CommandType.FORK_WORLD), ctx)
        info = await self._application.fork_world(
            source_world_id,
            name,
            storage_config=storage_config,
            cache_config=cache_config,
        )
        await self._emit(ctx, "fork_world", info.world_id)
        return info

    async def destroy_world(self, ctx, world_id):
        self._gate(Command(type=CommandType.DESTROY_WORLD), ctx)
        await self._application.destroy_world(world_id)
        await self._emit(ctx, "destroy_world", world_id)

    @instrument("gateway.get_world_info")
    async def get_world_info(self, ctx, world_id):
        self._gate(Command(type=CommandType.GET_WORLD_INFO), ctx)
        info = await self._application.get_world_info(world_id)
        await self._emit(ctx, "get_world_info", world_id)
        return info

    async def list_worlds(self, ctx):
        self._gate(Command(type=CommandType.LIST_WORLDS), ctx)
        infos = await self._application.list_worlds()
        await self._emit(ctx, "list_worlds")
        return infos

    async def discover_worlds(self, ctx, storage_config):
        self._gate(Command(type=CommandType.LIST_WORLDS), ctx)
        infos = await self._application.discover_worlds(storage_config)
        await self._emit(ctx, "discover_worlds")
        return infos

    async def open_world_readonly(self, ctx, storage_config, world_id):
        self._gate(Command(type=CommandType.GET_WORLD_INFO), ctx)
        info = await self._application.open_world_readonly(storage_config, world_id)
        await self._emit(ctx, "open_world_readonly", world_id)
        return info

    async def resume_world(self, ctx, storage_config, world_id):
        self._gate(Command(type=CommandType.CREATE_WORLD), ctx)
        info = await self._application.resume_world(storage_config, world_id)
        await self._emit(ctx, "resume_world", world_id)
        return info

    # Simulation and workflows --------------------------------------

    async def step(self, ctx, world_id, run_config, **input_kwargs):
        self._gate(Command(type=CommandType.STEP), ctx)
        result = await self._application.step(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "step", world_id)
        return result

    async def run(self, ctx, world_id, run_config, **input_kwargs):
        self._gate(Command(type=CommandType.RUN), ctx)
        result = await self._application.run(world_id, run_config, **input_kwargs)
        await self._emit(ctx, "run", world_id)
        return result

    async def run_episode(self, ctx, world_id, config, **input_kwargs):
        self._gate(Command(type=CommandType.RUN_EPISODE), ctx)
        result = await self._application.run_episode(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_episode",
            world_id,
            payload_json=json.dumps(
                {
                    "episode_id": str(result.episode_id),
                    "run_id": str(result.run_id) if result.run_id is not None else None,
                    "start_tick": result.start_tick,
                    "final_tick": result.final_tick,
                    "terminated": result.terminated,
                    "duration_steps": result.duration_steps,
                }
            ),
        )
        return result

    async def run_rollout(self, ctx, world_id, config, **input_kwargs):
        self._gate(Command(type=CommandType.RUN_ROLLOUT), ctx)
        result = await self._application.run_rollout(world_id, config, **input_kwargs)
        await self._emit(
            ctx,
            "run_rollout",
            world_id,
            payload_json=json.dumps(
                {
                    "num_episodes": result.num_episodes,
                    "total_duration_steps": result.total_duration_steps,
                    "episode_world_ids": [str(episode.world_id) for episode in result.episodes],
                }
            ),
        )
        return result

    async def autoresearch(
        self,
        ctx,
        world_id,
        config,
        evaluator,
        *,
        prepare_candidate=None,
        lab_world_id=None,
        on_iteration=None,
    ):
        self._gate(
            Command(
                type=CommandType.AUTORESEARCH,
                payload={
                    "max_iterations": config.max_iterations,
                    "num_episodes": config.num_episodes,
                },
            ),
            ctx,
        )
        result = await self._application.autoresearch(
            world_id,
            config,
            evaluator,
            prepare_candidate=prepare_candidate,
            lab_world_id=lab_world_id,
            on_iteration=on_iteration,
        )
        await self._emit(
            ctx,
            "autoresearch",
            world_id,
            payload_json=json.dumps(
                {
                    "experiment_id": config.experiment_id,
                    "iterations_completed": result.iterations_completed,
                    "initial_score": (
                        result.initial_score if math.isfinite(result.initial_score) else None
                    ),
                    "final_score": (
                        result.final_score if math.isfinite(result.final_score) else None
                    ),
                    "improved": result.improved,
                    "lab_world_id": result.lab_world_id,
                }
            ),
        )
        return result

    # Queries --------------------------------------------------------

    async def query_components(
        self,
        ctx,
        components,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
    ):
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        result = await self._application.query_components(
            components,
            world_id,
            run_id,
            storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    async def query_archetype(
        self,
        ctx,
        sig,
        world_id,
        run_id,
        storage_config=None,
        *,
        ticks=None,
        entity_ids=None,
        components=None,
    ):
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        result = await self._application.query_archetype(
            sig,
            world_id,
            run_id,
            storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            components=components,
        )
        await self._emit(ctx, "query_world", world_id)
        return result

    async def list_signatures(self, ctx, storage_config=None, *, world_id=None):
        self._gate(Command(type=CommandType.LIST_SIGNATURES), ctx)
        result = await self._application.list_signatures(storage_config, world_id=world_id)
        await self._emit(ctx, "list_signatures", world_id)
        return result

    async def get_audit_history(self, ctx, world_id=None, **filters):
        self._gate(Command(type=CommandType.GET_AUDIT_HISTORY), ctx)
        result = await self._application.get_audit_history(world_id, **filters)
        await self._emit(ctx, "get_audit_history", world_id)
        return result

    # World wiring/introspection ------------------------------------

    async def add_resource(self, ctx, world_id, resource):
        self._gate(Command(type=CommandType.ADD_RESOURCE), ctx)
        result = await self._application.add_resource(world_id, resource)
        await self._emit(ctx, "add_resource", world_id)
        return result

    async def add_hook(self, ctx, world_id, event_type, fn, *, mode="blocking"):
        self._gate(Command(type=CommandType.ADD_HOOK), ctx)
        result = await self._application.add_hook(world_id, event_type, fn, mode=mode)
        await self._emit(ctx, "add_hook", world_id)
        return result

    async def remove_hook(self, ctx, world_id, handle):
        self._gate(Command(type=CommandType.REMOVE_HOOK), ctx)
        await self._application.remove_hook(world_id, handle)
        await self._emit(ctx, "remove_hook", world_id)

    async def list_processors(self, ctx, world_id):
        self._gate(Command(type=CommandType.LIST_PROCESSORS), ctx)
        result = await self._application.list_processors(world_id)
        await self._emit(ctx, "list_processors", world_id)
        return result

    async def list_hooks(self, ctx, world_id):
        self._gate(Command(type=CommandType.LIST_HOOKS), ctx)
        result = await self._application.list_hooks(world_id)
        await self._emit(ctx, "list_hooks", world_id)
        return result

    async def list_resources(self, ctx, world_id):
        self._gate(Command(type=CommandType.LIST_RESOURCES), ctx)
        result = await self._application.list_resources(world_id)
        await self._emit(ctx, "list_resources", world_id)
        return result

    # Artifacts and evaluation --------------------------------------

    async def ingest_artifact(
        self,
        ctx,
        world_id,
        components,
        *,
        external_id,
        producer="default",
        storage_config=None,
    ):
        self._gate(Command(type=CommandType.PUBLISH_ARTIFACT), ctx)
        result = await self._application.ingest_artifact(
            world_id,
            components,
            external_id=external_id,
            producer=producer,
            storage_config=storage_config,
        )
        await self._emit(ctx, "ingest_artifact", world_id)
        return result

    async def ingest_files(self, ctx, world_id, paths, processor, *, storage_config=None):
        self._gate(Command(type=CommandType.PUBLISH_ARTIFACT), ctx)
        result = await self._application.ingest_files(
            world_id, paths, processor, storage_config=storage_config
        )
        await self._emit(ctx, "ingest_files", world_id)
        return result

    async def write_artifacts(self, ctx, world_id, table_name, artifacts, *, storage_config=None):
        self._gate(Command(type=CommandType.PUBLISH_ARTIFACT), ctx)
        result = await self._application.write_artifacts(
            world_id, table_name, artifacts, storage_config=storage_config
        )
        await self._emit(ctx, "write_artifacts", world_id)
        return result

    async def query_artifacts(self, ctx, world_id, table_name, *, storage_config=None):
        self._gate(Command(type=CommandType.QUERY_WORLD), ctx)
        result = await self._application.query_artifacts(
            world_id, table_name, storage_config=storage_config
        )
        await self._emit(ctx, "query_artifacts", world_id)
        return result

    async def evaluate(self, ctx, world_id, components, **kwargs):
        self._gate(Command(type=CommandType.EVALUATE), ctx)
        result = await self._application.evaluate(world_id, components, **kwargs)
        await self._emit(
            ctx,
            "evaluate",
            world_id,
            payload_json=json.dumps(
                {
                    "evaluation_id": kwargs.get("evaluation_id"),
                    "duplicate": result.duplicate,
                    "grader_id": kwargs["contract"].grader_id,
                }
            ),
        )
        return result

    # Deferred command acceptance ----------------------------------

    async def submit(self, ctx, world_id, command):
        self._application.require_world(world_id)
        self._application.validate_deferred_command(command)
        self._gate(command, ctx)
        command_id = await self._application.submit(
            world_id,
            command,
            principal_id=ctx.id,
            origin="gateway",
        )
        return command_id

    async def submit_batch(self, ctx, world_id, commands):
        self._application.require_world(world_id)
        for command in commands:
            self._application.validate_deferred_command(command)
        self._gate_batch(commands, ctx)
        return await self._application.submit_batch(
            world_id,
            commands,
            principal_id=ctx.id,
            origin="gateway",
        )

    async def submit_spawn(self, ctx, world_id, components, *, tick=0, priority=0):
        self._application.require_world(world_id)
        self._gate(Command(type=CommandType.SPAWN), ctx)
        entity_id, command = await self._application.submit_spawn(
            world_id,
            components,
            tick=tick,
            priority=priority,
            principal_id=ctx.id,
            origin="gateway",
        )
        del command  # durable admission/outbox is the queued-event authority
        return entity_id
