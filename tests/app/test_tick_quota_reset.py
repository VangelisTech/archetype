# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Per-tick RBAC quota reset contract (bug B1).

The gate debits a per-actor, per-tick command counter (``_tick_counters``)
at submit time and rejects an actor once it exceeds ``MAX_CMDS_PER_TICK``
*in a single tick*. Nothing reset that counter between ticks, so it
accumulated across the whole process: a long-running driver eventually hit
the per-tick ceiling even though no single tick was anywhere near it. The
LIBERO eval driver papered over this by calling ``reset_tick_counters()``
by hand before every step (eval_driver.py:179/237). That hand-roll is the
symptom; the contract belongs in the framework.

Contract: advancing a world by one tick (``SimulationService.step``) resets
the per-tick command quota, so the budget is *per tick*, not *per process*.

Given / When / Then:
- GIVEN an actor that has issued commands on previous ticks
- WHEN the world is stepped (a new tick begins)
- THEN the actor's per-tick budget is fresh again.
"""

from __future__ import annotations

import pytest
from uuid_utils import UUID, uuid7

import archetype.app.auth.guard as guard
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Marker(Component):
    tag: str = ""


@pytest.fixture(autouse=True)
def _reset_quotas():
    # ``_tick_counters`` / ``_daily_tokens`` are module globals; isolate tests.
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_step_clears_per_actor_tick_counter(tmp_path):
    """TDD (mechanism): a step empties the per-tick counter.

    Pre-seed the counter to the ceiling for one actor, step the world, and
    assert the counter is cleared — proving the tick boundary resets quota.
    """
    container = ServiceContainer()
    actor = uuid7()
    try:
        world = await container.world_service.create_world(
            WorldConfig(name="quota"), StorageConfig(uri=str(tmp_path / "store"))
        )

        # Simulate an actor that has already saturated this tick's budget.
        guard._tick_counters[UUID(str(actor))] = guard.MAX_CMDS_PER_TICK
        assert guard._tick_counters[UUID(str(actor))] == guard.MAX_CMDS_PER_TICK

        await container.simulation_service.step(world.world_id, RunConfig())

        # The per-tick counter is cleared at the tick boundary.
        assert guard._tick_counters.get(UUID(str(actor)), 0) == 0
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_actor_not_blocked_across_many_ticks(tmp_path, monkeypatch):
    """BDD (regression): the per-tick quota does not accumulate across ticks.

    Lower the ceiling so a handful of ticks would blow a *process-wide*
    counter, then drive (spawn + step) through the gate for more ticks than
    the ceiling allows. Without the reset the actor is rejected partway
    through; with it, every tick starts fresh and all ticks succeed.
    """
    from archetype.app.auth.errors import GuardrailError

    monkeypatch.setattr(guard, "MAX_CMDS_PER_TICK", 4)

    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    try:
        info = await container.command_service.create_world(
            ctx, WorldConfig(name="long-run"), StorageConfig(uri=str(tmp_path / "store"))
        )

        # 8 ticks × (1 spawn + 1 step) = 16 gated commands, four-fold over the
        # ceiling of 4. Each individual tick issues only 2 commands (< 4), so a
        # correct per-tick quota never trips.
        for _ in range(8):
            await container.command_service.create_entity(ctx, info.world_id, [Marker(tag="x")])
            await container.command_service.step(ctx, info.world_id, RunConfig())
    except GuardrailError as exc:  # pragma: no cover - the bug path
        pytest.fail(f"per-tick quota accumulated across ticks: {exc}")
    finally:
        await container.shutdown()
