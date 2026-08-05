# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""MuJoCo rollout parity contract.

The ledger must contain the exact physics trajectory: the spawn-tick row
is the raw initial conditions (x_0 is given), and the row at tick t is
bit-for-bit equal to the state after t * substeps MuJoCo steps. If these
hold, recording a rollout through Archetype provably does not perturb or
lose dynamics.
"""

import pytest

from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.physical_ai.mujoco_cartpole import (
    CartpoleState,
    CartpoleStepProcessor,
    raw_rollout,
)
from tests.conftest import make_world_harness

# mujoco_cartpole imports mujoco lazily, so the module imports above are safe.
pytest.importorskip("mujoco")

TICKS = 4
SUBSTEPS = 5

INITIAL_STATES = [
    (0.0, 0.10, 0.0, 0.0),
    (-0.2, 0.35, 0.4, -0.1),
    (0.15, -0.25, -0.3, 0.2),
]


def _row_state(row) -> tuple[float, float, float, float]:
    return (
        row["cartpolestate__cart_pos"],
        row["cartpolestate__pole_angle"],
        row["cartpolestate__cart_vel"],
        row["cartpolestate__pole_vel"],
    )


@pytest.mark.asyncio
async def test_ledger_matches_raw_mujoco_rollout_bit_for_bit(tmp_path):
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="mujoco")
        system = AsyncSystem()
        await system.add_processor(CartpoleStepProcessor(substeps=SUBSTEPS))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="cartpole"), storage_config=storage, system=system
        )

        entity_ids = []
        for cart_pos, pole_angle, cart_vel, pole_vel in INITIAL_STATES:
            entity_ids.append(
                await world.create_entity(
                    [
                        CartpoleState(
                            cart_pos=cart_pos,
                            pole_angle=pole_angle,
                            cart_vel=cart_vel,
                            pole_vel=pole_vel,
                        )
                    ]
                )
            )

        await world.run(RunConfig(num_steps=TICKS))

        reference = raw_rollout(INITIAL_STATES, ticks=TICKS, substeps=SUBSTEPS)
        expected = dict(zip(entity_ids, reference, strict=True))

        sig = (CartpoleState,)
        for tick in range(TICKS):
            rows = (await world.query_archetype(sig=sig, ticks=[tick])).to_pylist()
            assert len(rows) == len(INITIAL_STATES), (
                f"tick {tick}: expected one row per env, got {len(rows)}"
            )
            for row in rows:
                got = _row_state(row)
                want = expected[row["entity_id"]][tick]
                assert got == want, (
                    f"entity {row['entity_id']} tick {tick}: ledger {got} != raw MuJoCo {want}"
                )
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_spawn_tick_row_is_raw_initial_conditions(tmp_path):
    """Tick 0 is x_0 exactly — the physics processor must not touch it."""
    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="mujoco")
        system = AsyncSystem()
        await system.add_processor(CartpoleStepProcessor(substeps=SUBSTEPS))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="cartpole-ic"), storage_config=storage, system=system
        )
        await world.create_entity(
            [CartpoleState(cart_pos=0.5, pole_angle=0.25, cart_vel=-1.0, pole_vel=2.0)]
        )

        await world.run(RunConfig(num_steps=1))

        rows = (await world.query_archetype(sig=(CartpoleState,), ticks=[0])).to_pylist()
        assert len(rows) == 1
        assert _row_state(rows[0]) == (0.5, 0.25, -1.0, 2.0)
    finally:
        await ws.close()
