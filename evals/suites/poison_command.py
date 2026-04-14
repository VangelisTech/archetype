# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Poison command regression suite: adversarial command handling.

Verifies that malformed, invalid, or unhandled commands are swallowed
gracefully by ``drain_and_apply`` without corrupting world state or
blocking valid commands in the same tick.  These are regression
guardrails — they must never fail.
"""

from __future__ import annotations

import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig
from evals.graders import exact_match, state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "regression"


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------


class _PoisonPos(Component):
    x: int = 0
    y: int = 0


class _PoisonTag(Component):
    label: str = ""


# ---------------------------------------------------------------------------
# Task 1: poison command in a batch doesn't block valid commands
# ---------------------------------------------------------------------------


def task_poison_in_batch() -> list[GraderResult]:
    """A malformed command must not prevent valid commands in the same tick."""
    return asyncio.run(_task_poison_in_batch())


async def _task_poison_in_batch() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await container.world_service.create_world(
                WorldConfig(name="poison-batch"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            # Valid SPAWN
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=1, y=1).to_payload()]},
                ),
                ctx,
            )

            # Poison SPAWN — bare model_dump() without "type" key
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=2, y=2).model_dump()]},
                ),
                ctx,
            )

            # Valid SPAWN
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=3, y=3).to_payload()]},
                ),
                ctx,
            )

            await container.simulation_service.step(world.world_id)

            entity_count = len(world._entity2sig)
            signatures = {frozenset(sig) for sig in world._live}

            return [
                exact_match(entity_count, 2, name="valid_commands_applied"),
                state_check(
                    {
                        "has_typed_archetype": frozenset({_PoisonPos}) in signatures,
                        "no_base_component_archetype": frozenset({Component}) not in signatures,
                    },
                    name="world_not_corrupted",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Task 2: missing payload keys fail gracefully
# ---------------------------------------------------------------------------


def task_missing_payload_keys() -> list[GraderResult]:
    """Commands with missing required payload keys must fail without corruption."""
    return asyncio.run(_task_missing_payload_keys())


async def _task_missing_payload_keys() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await container.world_service.create_world(
                WorldConfig(name="missing-keys"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            # DESPAWN with no entity_id
            await container.command_service.submit(
                wid, Command(type=CommandType.DESPAWN, tick=0, payload={}), ctx
            )
            # REMOVE_COMPONENT with no entity_id
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.REMOVE_COMPONENT,
                    tick=0,
                    payload={"component_types": ["_PoisonPos"]},
                ),
                ctx,
            )
            # UPDATE with no entity_id
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.UPDATE,
                    tick=0,
                    payload={"components": [_PoisonPos(x=9, y=9).to_payload()]},
                ),
                ctx,
            )

            await container.simulation_service.step(world.world_id)

            return [
                state_check(
                    {
                        "no_entities_created": len(world._entity2sig) == 0,
                        "no_archetypes": len(world._live) == 0,
                    },
                    name="world_unchanged",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Task 3: unknown component type names fail gracefully
# ---------------------------------------------------------------------------


def task_unknown_component_type() -> list[GraderResult]:
    """REMOVE_COMPONENT with bogus type name must not strip real components."""
    return asyncio.run(_task_unknown_component_type())


async def _task_unknown_component_type() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await container.world_service.create_world(
                WorldConfig(name="unknown-type"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            # Spawn an entity with two components
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={
                        "components": [
                            _PoisonPos(x=1, y=2).to_payload(),
                            _PoisonTag(label="keep").to_payload(),
                        ]
                    },
                ),
                ctx,
            )
            await container.simulation_service.step(world.world_id)

            entity_id = next(iter(world._entity2sig))
            original_sig = frozenset(world._entity2sig[entity_id])

            # Try to remove a nonexistent component type
            reset_tick_counters()
            reset_daily_tokens()
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.REMOVE_COMPONENT,
                    tick=1,
                    payload={
                        "entity_id": entity_id,
                        "component_types": ["TotallyFakeComponent"],
                    },
                ),
                ctx,
            )
            await container.simulation_service.step(world.world_id)

            current_sig = frozenset(world._entity2sig[entity_id])

            return [
                state_check(
                    {
                        "signature_preserved": current_sig == original_sig,
                        "entity_still_exists": entity_id in world._entity2sig,
                    },
                    name="entity_intact",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Task 4: despawn nonexistent entity doesn't corrupt
# ---------------------------------------------------------------------------


def task_despawn_nonexistent_entity() -> list[GraderResult]:
    """DESPAWN for a missing entity must not corrupt existing entities."""
    return asyncio.run(_task_despawn_nonexistent_entity())


async def _task_despawn_nonexistent_entity() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await container.world_service.create_world(
                WorldConfig(name="despawn-missing"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            # Spawn a real entity
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=1, y=1).to_payload()]},
                ),
                ctx,
            )
            await container.simulation_service.step(world.world_id)

            entity_count_before = len(world._entity2sig)

            # Despawn a nonexistent entity
            reset_tick_counters()
            reset_daily_tokens()
            await container.command_service.submit(
                wid,
                Command(
                    type=CommandType.DESPAWN,
                    tick=1,
                    payload={"entity_id": 99999},
                ),
                ctx,
            )
            await container.simulation_service.step(world.world_id)

            return [
                state_check(
                    {
                        "entity_count_unchanged": len(world._entity2sig) == entity_count_before,
                        "real_entity_intact": any(
                            eid in world._entity2sig for eid in world._entity2sig
                        ),
                    },
                    name="world_intact",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Task 5: unhandled command types are consumed as no-ops
# ---------------------------------------------------------------------------


def task_unhandled_command_noop() -> list[GraderResult]:
    """MESSAGE, QUERY_WORLD, and CUSTOM must be dequeued but produce no mutations."""
    return asyncio.run(_task_unhandled_command_noop())


async def _task_unhandled_command_noop() -> list[GraderResult]:
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await container.world_service.create_world(
                WorldConfig(name="noop-cmds"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            for cmd_type in (CommandType.MESSAGE, CommandType.QUERY_WORLD, CommandType.CUSTOM):
                await container.command_service.submit(
                    wid,
                    Command(type=cmd_type, tick=0, payload={"data": "test"}),
                    ctx,
                )

            await container.simulation_service.step(world.world_id)

            pending = await container.broker.get_pending_count(wid)

            return [
                state_check(
                    {
                        "all_dequeued": pending == 0,
                        "no_entities_created": len(world._entity2sig) == 0,
                        "no_archetypes": len(world._live) == 0,
                    },
                    name="clean_noop",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------


def register(harness: EvalHarness) -> None:
    """Register all poison-command regression tasks."""
    harness.add(
        "poison_in_batch",
        suite=SUITE,
        fn=task_poison_in_batch,
        desc="Malformed command in a batch must not block valid commands",
    )
    harness.add(
        "missing_payload_keys",
        suite=SUITE,
        fn=task_missing_payload_keys,
        desc="Commands with missing required keys fail without world corruption",
    )
    harness.add(
        "unknown_component_type",
        suite=SUITE,
        fn=task_unknown_component_type,
        desc="REMOVE_COMPONENT with bogus type name preserves entity",
    )
    harness.add(
        "despawn_nonexistent",
        suite=SUITE,
        fn=task_despawn_nonexistent_entity,
        desc="DESPAWN for missing entity does not corrupt existing entities",
    )
    harness.add(
        "unhandled_command_noop",
        suite=SUITE,
        fn=task_unhandled_command_noop,
        desc="MESSAGE/QUERY_WORLD/CUSTOM are dequeued as no-ops",
    )
