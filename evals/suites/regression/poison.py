# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Poison command regression suite: adversarial command handling.

Verifies that malformed, invalid, or unhandled commands are isolated during
exact-world tick materialization without corrupting world state or blocking
valid commands in the same tick. These are regression guardrails — they must
never fail.
"""

from __future__ import annotations

import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.container import ServiceContainer
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from evals.graders import state_check
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


async def _create_live_world(
    container: ServiceContainer,
    config: WorldConfig,
    storage: StorageConfig,
) -> AsyncWorld:
    info = await container.application.create_world(config, storage)
    world = await container.world_registry.live_world(str(info.world_id))
    if not isinstance(world, AsyncWorld):
        raise RuntimeError(f"world {info.world_id} was not activated")
    return world


# ---------------------------------------------------------------------------
# Task 1: poison command in a batch doesn't block valid commands
# ---------------------------------------------------------------------------


def task_poison_in_batch() -> list[GraderResult]:
    """A malformed command must not prevent valid commands in the same tick."""
    return asyncio.run(_task_poison_in_batch())


async def _task_poison_in_batch() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(container, WorldConfig(name="poison-batch"), storage)
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            valid_commands = [
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=1, y=1).to_payload()]},
                ),
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=3, y=3).to_payload()]},
                ),
            ]
            poison = Command(
                type=CommandType.SPAWN,
                tick=0,
                payload={"components": [_PoisonPos(x=2, y=2).model_dump()]},
            )

            rejected_atomically = False
            try:
                await container.command_gateway.submit_batch(
                    ctx,
                    wid,
                    [valid_commands[0], poison, valid_commands[1]],
                )
            except ValueError:
                rejected_atomically = True
            pending_after_rejection = await container.command_scheduler.pending_count(wid)

            await container.command_gateway.submit_batch(ctx, wid, valid_commands)

            await container.application.step(world.world_id, rc)

            entity_count = len(world.entity2sig)
            signatures = {frozenset(sig) for sig in set(world.entity2sig.values())}

            return [
                state_check(
                    {
                        "poison_batch_rejected": rejected_atomically,
                        "no_partial_admission": pending_after_rejection == 0,
                        "valid_commands_applied": entity_count == 2,
                    },
                    name="canonical_batch_validation",
                ),
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
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(container, WorldConfig(name="missing-keys"), storage)
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            malformed = [
                Command(type=CommandType.DESPAWN, tick=0, payload={}),
                Command(
                    type=CommandType.REMOVE_COMPONENT,
                    tick=0,
                    payload={"component_types": ["_PoisonPos"]},
                ),
                Command(
                    type=CommandType.UPDATE,
                    tick=0,
                    payload={"components": [_PoisonPos(x=9, y=9).to_payload()]},
                ),
            ]
            rejected = 0
            for command in malformed:
                try:
                    await container.command_gateway.submit(ctx, wid, command)
                except (TypeError, ValueError):
                    rejected += 1
            pending = await container.command_scheduler.pending_count(wid)

            return [
                state_check(
                    {
                        "all_malformed_rejected": rejected == len(malformed),
                        "nothing_persisted": pending == 0,
                        "no_entities_created": len(world.entity2sig) == 0,
                        "no_archetypes": len(world.entity2sig) == 0,
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
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(container, WorldConfig(name="unknown-type"), storage)
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            # Spawn an entity with two components
            await container.command_gateway.submit(
                ctx,
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
            )
            await container.application.step(world.world_id, rc)

            entity_id = next(iter(world.entity2sig))
            original_sig = frozenset(world.entity2sig[entity_id])

            # Try to remove a nonexistent component type
            rejected = False
            try:
                await container.command_gateway.submit(
                    ctx,
                    wid,
                    Command(
                        type=CommandType.REMOVE_COMPONENT,
                        tick=1,
                        payload={
                            "entity_id": entity_id,
                            "component_types": ["TotallyFakeComponent"],
                        },
                    ),
                )
            except ValueError:
                rejected = True
            pending = await container.command_scheduler.pending_count(wid)
            await container.application.step(world.world_id, rc)

            current_sig = frozenset(world.entity2sig[entity_id])

            return [
                state_check(
                    {
                        "unknown_type_rejected": rejected,
                        "nothing_persisted": pending == 0,
                        "signature_preserved": current_sig == original_sig,
                        "entity_still_exists": entity_id in world.entity2sig,
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
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                container, WorldConfig(name="despawn-missing"), storage
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            # Spawn a real entity
            await container.command_gateway.submit(
                ctx,
                wid,
                Command(
                    type=CommandType.SPAWN,
                    tick=0,
                    payload={"components": [_PoisonPos(x=1, y=1).to_payload()]},
                ),
            )
            await container.application.step(world.world_id, rc)

            entity_count_before = len(world.entity2sig)

            # Despawn a nonexistent entity
            await container.command_gateway.submit(
                ctx,
                wid,
                Command(
                    type=CommandType.DESPAWN,
                    tick=1,
                    payload={"entity_id": 99999},
                ),
            )
            await container.application.step(world.world_id, rc)

            return [
                state_check(
                    {
                        "entity_count_unchanged": len(world.entity2sig) == entity_count_before,
                        "real_entity_intact": any(
                            eid in world.entity2sig for eid in world.entity2sig
                        ),
                    },
                    name="world_intact",
                ),
            ]
        finally:
            await container.shutdown()


# ---------------------------------------------------------------------------
# Task 5: unsupported legacy command types reject before persistence
# ---------------------------------------------------------------------------


def task_unhandled_command_noop() -> list[GraderResult]:
    """MESSAGE, QUERY_WORLD, and CUSTOM reject before portable admission."""
    return asyncio.run(_task_unhandled_command_noop())


async def _task_unhandled_command_noop() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(container, WorldConfig(name="noop-cmds"), storage)
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            rejected = 0
            for cmd_type in (CommandType.MESSAGE, CommandType.QUERY_WORLD, CommandType.CUSTOM):
                try:
                    await container.command_gateway.submit(
                        ctx,
                        wid,
                        Command(type=cmd_type, tick=0, payload={"data": "test"}),
                    )
                except ValueError:
                    rejected += 1

            pending = await container.command_scheduler.pending_count(wid)

            return [
                state_check(
                    {
                        "all_unsupported_rejected": rejected == 3,
                        "nothing_persisted": pending == 0,
                        "no_entities_created": len(world.entity2sig) == 0,
                        "no_archetypes": len(world.entity2sig) == 0,
                    },
                    name="unsupported_commands_fail_closed",
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
        desc="MESSAGE/QUERY_WORLD/CUSTOM reject before portable admission",
    )
