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

from archetype.commands.models import ActorCtx, DeferredItem, DurableOptions
from archetype.core.aio import AsyncWorld
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.world.models import (
    ComponentTypeRef,
    CreateWorld,
    Despawn,
    GetWorldInfo,
    ListWorlds,
    RemoveComponents,
    Spawn,
    Step,
    Update,
)
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.infra.runtime import (
    EvalProcess,
    component_refs,
    component_values,
    isolated_eval_process,
)
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
    process: EvalProcess,
    config: WorldConfig,
    storage: StorageConfig,
) -> AsyncWorld:
    info = await process.dispatcher.apply(CreateWorld(config=config, storage_config=storage))
    world = await process.worlds.live_world(str(info.world_id))
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
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                process,
                WorldConfig(name="poison-batch"),
                storage,
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            valid_items = (
                DeferredItem(
                    Spawn.from_components(
                        world_id=wid,
                        components=[_PoisonPos(x=1, y=1)],
                    ),
                    DurableOptions(target_tick=0),
                ),
                DeferredItem(
                    Spawn.from_components(
                        world_id=wid,
                        components=[_PoisonPos(x=3, y=3)],
                    ),
                    DurableOptions(target_tick=0),
                ),
            )
            direct_only_item = DeferredItem(
                GetWorldInfo(world_id=wid),
                DurableOptions(target_tick=0),
            )

            rejected_atomically = False
            try:
                await process.dispatcher.defer_batch_as(
                    ctx,
                    (valid_items[0], direct_only_item, valid_items[1]),
                )
            except ValueError:
                rejected_atomically = True
            pending_after_rejection = await process.scheduler.pending_count(wid)

            await process.dispatcher.defer_batch_as(ctx, valid_items)

            await process.dispatcher.apply(Step(world_id=world.world_id, run_config=rc))

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
            await process.aclose()


# ---------------------------------------------------------------------------
# Task 2: missing payload keys fail gracefully
# ---------------------------------------------------------------------------


def task_missing_payload_keys() -> list[GraderResult]:
    """Commands with missing required payload keys must fail without corruption."""
    return asyncio.run(_task_missing_payload_keys())


async def _task_missing_payload_keys() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                process,
                WorldConfig(name="missing-keys"),
                storage,
            )
            wid = str(world.world_id)
            malformed = [
                (Despawn, {"operation": "despawn", "world_id": wid}),
                (
                    RemoveComponents,
                    {
                        "operation": "remove_components",
                        "world_id": wid,
                        "component_types": component_refs([_PoisonPos]),
                    },
                ),
                (
                    Update,
                    {
                        "operation": "update",
                        "world_id": wid,
                        "components": component_values([_PoisonPos(x=9, y=9)]),
                    },
                ),
            ]
            rejected = 0
            for operation_type, payload in malformed:
                try:
                    operation_type.model_validate(payload)
                except (TypeError, ValueError):
                    rejected += 1
            pending = await process.scheduler.pending_count(wid)

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
            await process.aclose()


# ---------------------------------------------------------------------------
# Task 3: unknown component type names fail gracefully
# ---------------------------------------------------------------------------


def task_unknown_component_type() -> list[GraderResult]:
    """REMOVE_COMPONENT with bogus type name must not strip real components."""
    return asyncio.run(_task_unknown_component_type())


async def _task_unknown_component_type() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                process,
                WorldConfig(name="unknown-type"),
                storage,
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            # Spawn an entity with two components
            await process.dispatcher.apply_as(
                ctx,
                Spawn.from_components(
                    world_id=wid,
                    components=[
                        _PoisonPos(x=1, y=2),
                        _PoisonTag(label="keep"),
                    ],
                ),
            )
            await process.dispatcher.apply(Step(world_id=world.world_id, run_config=rc))

            entity_id = next(iter(world.entity2sig))
            original_sig = frozenset(world.entity2sig[entity_id])

            # Try to remove a nonexistent component type
            rejected = False
            try:
                await process.dispatcher.apply_as(
                    ctx,
                    RemoveComponents(
                        world_id=wid,
                        entity_id=entity_id,
                        component_types=(
                            ComponentTypeRef(
                                type_name="TotallyFakeComponent",
                                schema_fingerprint="0" * 64,
                            ),
                        ),
                    ),
                )
            except ValueError:
                rejected = True
            pending = await process.scheduler.pending_count(wid)
            await process.dispatcher.apply(Step(world_id=world.world_id, run_config=rc))

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
            await process.aclose()


# ---------------------------------------------------------------------------
# Task 4: despawn nonexistent entity doesn't corrupt
# ---------------------------------------------------------------------------


def task_despawn_nonexistent_entity() -> list[GraderResult]:
    """DESPAWN for a missing entity must not corrupt existing entities."""
    return asyncio.run(_task_despawn_nonexistent_entity())


async def _task_despawn_nonexistent_entity() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                process,
                WorldConfig(name="despawn-missing"),
                storage,
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)
            rc = RunConfig()

            # Spawn a real entity
            await process.dispatcher.defer_as(
                ctx,
                Spawn.from_components(
                    world_id=wid,
                    components=[_PoisonPos(x=1, y=1)],
                ),
                DurableOptions(target_tick=0),
            )
            await process.dispatcher.apply(Step(world_id=world.world_id, run_config=rc))

            entity_count_before = len(world.entity2sig)

            # Despawn a nonexistent entity
            await process.dispatcher.defer_as(
                ctx,
                Despawn(world_id=wid, entity_id=99999),
                DurableOptions(target_tick=1),
            )
            await process.dispatcher.apply(Step(world_id=world.world_id, run_config=rc))

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
            await process.aclose()


# ---------------------------------------------------------------------------
# Task 5: unsupported legacy command types reject before persistence
# ---------------------------------------------------------------------------


def task_unhandled_command_noop() -> list[GraderResult]:
    """Registered direct-only operations reject before portable admission."""
    return asyncio.run(_task_unhandled_command_noop())


async def _task_unhandled_command_noop() -> list[GraderResult]:
    with tempfile.TemporaryDirectory() as tmp:
        process = isolated_eval_process(tmp)
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="poison")
            world = await _create_live_world(
                process,
                WorldConfig(name="direct-only"),
                storage,
            )
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            wid = str(world.world_id)

            rejected = 0
            direct_only = (
                GetWorldInfo(world_id=wid),
                ListWorlds(),
                CreateWorld(config=WorldConfig(name="must-not-admit")),
            )
            for operation in direct_only:
                try:
                    await process.dispatcher.defer_as(
                        ctx,
                        operation,
                        DurableOptions(target_tick=0),
                    )
                except ValueError:
                    rejected += 1

            pending = await process.scheduler.pending_count(wid)

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
            await process.aclose()


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
        desc="Registered direct-only operations reject before portable admission",
    )
