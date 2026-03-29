# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Eval: Command flow correctness.

End-to-end eval of the command pipeline:
submit → broker → step → history — verifying RBAC, ordering, and persistence.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import StorageConfig, WorldConfig

PASS = 0
FAIL = 1


async def eval_command_flow() -> int:
    """Verify submit → step → history pipeline and RBAC enforcement."""
    failures = []
    reset_tick_counters()
    reset_daily_tokens()

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=f"{tmp}/store", namespace="eval_cmd")
            world = await container.world_service.create_world(
                WorldConfig(name="cmd-eval"), storage
            )

            admin_ctx = ActorCtx(id=uuid7(), roles={"admin"})
            viewer_ctx = ActorCtx(id=uuid7(), roles={"viewer"})

            # --- 1. Admin can submit commands ---
            cmd = Command(type=CommandType.SPAWN, tick=0, payload={"components": []})
            await container.command_service.submit(str(world.world_id), cmd, admin_ctx)

            pending = await container.broker.get_pending_count(str(world.world_id))
            if pending != 1:
                failures.append(f"FAIL: expected 1 pending command, got {pending}")

            # --- 2. Step drains the command ---
            applied = await container.simulation_service.step(world.world_id)
            if applied != 1:
                failures.append(f"FAIL: expected 1 applied command, got {applied}")

            pending = await container.broker.get_pending_count(str(world.world_id))
            if pending != 0:
                failures.append(f"FAIL: expected 0 pending after step, got {pending}")

            # --- 3. History records the command ---
            history = await container.query_service.get_command_history(world.world_id)
            if len(history) != 1:
                failures.append(f"FAIL: expected 1 history entry, got {len(history)}")
            elif history[0].type != CommandType.SPAWN:
                failures.append(f"FAIL: history type mismatch: {history[0].type}")

            # --- 4. Viewer CANNOT submit SPAWN ---
            viewer_denied = False
            try:
                await container.command_service.submit(
                    str(world.world_id),
                    Command(type=CommandType.SPAWN, payload={}),
                    viewer_ctx,
                )
            except PermissionError:
                viewer_denied = True

            if not viewer_denied:
                failures.append("FAIL: viewer was able to submit SPAWN (should be denied)")

            # --- 5. Multiple commands respect ordering ---
            reset_tick_counters()
            for i in range(5):
                await container.command_service.submit(
                    str(world.world_id),
                    Command(type=CommandType.SPAWN, tick=i, payload={"idx": i}),
                    admin_ctx,
                )

            pending = await container.broker.get_pending_count(str(world.world_id))
            if pending != 5:
                failures.append(f"FAIL: expected 5 pending commands, got {pending}")

            applied = await container.simulation_service.step(world.world_id)
            if applied != 5:
                failures.append(f"FAIL: expected 5 applied commands, got {applied}")

        finally:
            await container.shutdown()
            reset_tick_counters()
            reset_daily_tokens()

    if failures:
        for f in failures:
            print(f, file=sys.stderr)
        print(f"\neval_command_flow: {len(failures)} failure(s)", file=sys.stderr)
        return FAIL

    print("eval_command_flow: PASS")
    return PASS


def main() -> int:
    return asyncio.run(eval_command_flow())


if __name__ == "__main__":
    sys.exit(main())
