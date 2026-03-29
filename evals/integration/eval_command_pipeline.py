# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration eval: Command pipeline correctness.

Multi-step eval of the command lifecycle:
submit → broker → step → history — verifying RBAC, ordering, and state
transitions across the CommandService ↔ Broker ↔ SimulationService chain.
"""

from __future__ import annotations

import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import StorageConfig, WorldConfig
from evals.types import EvalResult, Tier, binary, rubric


async def _run() -> list[EvalResult]:
    results = []
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_cmd")
            world = await container.world_service.create_world(
                WorldConfig(name="cmd-eval"), storage
            )
            wid = str(world.world_id)

            admin = ActorCtx(id=uuid7(), roles={"admin"})
            viewer = ActorCtx(id=uuid7(), roles={"viewer"})

            # --- eval: submit enqueues correctly ---
            cmd = Command(type=CommandType.SPAWN, tick=0, payload={"components": []})
            await container.command_service.submit(wid, cmd, admin)
            pending = await container.broker.get_pending_count(wid)
            results.append(
                binary(
                    pending,
                    1,
                    case_name="pipeline_submit_enqueues",
                    tier=Tier.INTEGRATION,
                )
            )

            # --- eval: step drains and applies ---
            applied = await container.simulation_service.step(world.world_id)
            pending_after = await container.broker.get_pending_count(wid)
            results.append(
                binary(
                    (applied, pending_after),
                    (1, 0),
                    case_name="pipeline_step_drains",
                    tier=Tier.INTEGRATION,
                )
            )

            # --- eval: history records applied command ---
            history = await container.query_service.get_command_history(world.world_id)
            results.append(
                binary(
                    len(history),
                    1,
                    case_name="pipeline_history_recorded",
                    tier=Tier.INTEGRATION,
                )
            )
            results.append(
                binary(
                    history[0].type,
                    CommandType.SPAWN,
                    case_name="pipeline_history_type_correct",
                    tier=Tier.INTEGRATION,
                )
            )

            # --- eval: RBAC blocks viewer at service boundary ---
            viewer_denied = False
            try:
                await container.command_service.submit(
                    wid,
                    Command(type=CommandType.SPAWN, payload={}),
                    viewer,
                )
            except PermissionError:
                viewer_denied = True
            results.append(
                binary(
                    viewer_denied,
                    True,
                    case_name="pipeline_rbac_viewer_blocked",
                    tier=Tier.INTEGRATION,
                )
            )

            # --- eval: bulk submit + drain ---
            reset_tick_counters()
            reset_daily_tokens()
            for i in range(5):
                await container.command_service.submit(
                    wid,
                    Command(type=CommandType.SPAWN, tick=i, payload={"idx": i}),
                    admin,
                )
            pending_bulk = await container.broker.get_pending_count(wid)
            applied_bulk = await container.simulation_service.step(world.world_id)
            results.append(
                rubric(
                    {
                        "5_enqueued": pending_bulk == 5,
                        "5_applied": applied_bulk == 5,
                    },
                    case_name="pipeline_bulk_submit_drain",
                    tier=Tier.INTEGRATION,
                )
            )

        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()

    return results


def run() -> list[EvalResult]:
    return asyncio.run(_run())
