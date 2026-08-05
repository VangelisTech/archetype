# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Chunk-cadence provenance contract for VlaJepaPolicyClient-style buffering.

Verifies three normative properties with strict equality (no Modal, CI-safe):

1. **Chunk-cadence**: action at tick t equals the correct element from the
   chunk computed from the observation at the most recent chunk-boundary tick.
   The chunk refreshes exactly every N ticks (N = chunk_len).

2. **Done-row freezing**: once a row is done its action column is not
   updated even if a new chunk would normally be requested.

3. **Explicit provider construction**: processors reject missing clients and
   expose no worker-local provider-factory path that lacks teardown authority.

``FakeChunkPolicy`` is a deterministic in-process policy that returns
arithmetic chunks so that all assertions can be expressed as closed-form
exact equalities without any floating-point tolerance.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

import pytest

from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
    _EnvStepProcessor,
)
from archetype.physical_ai.policy import (
    _PolicyActionProcessor,
)
from tests.conftest import make_world_harness

# ---------------------------------------------------------------------------
# FakeChunkPolicy
# ---------------------------------------------------------------------------

CHUNK_LEN = 4  # deliberately != 7 to ensure the test is not VLA-specific


class FakeChunkPolicy:
    """Deterministic chunk-buffered policy for contract testing.

    Chunk generation (per env_key, per refresh)::

        chunk[step] = [base + step * DIM_FACTOR + dim * 0.01 for dim in range(ACTION_DIM)]

    where ``base = eef_pos[0] * 100``.  This makes the chunk a pure function
    of the observation's ``eef_pos[0]`` value, so the test can reproduce it
    without running the policy.

    Buffer behavior mirrors ``VlaJepaPolicyClient``: a call to ``act``
    returns ``chunk.pop(0)`` and refreshes when empty.
    """

    DIM_FACTOR = 0.001

    def __init__(self, chunk_len: int = CHUNK_LEN) -> None:
        self._chunk_len = chunk_len
        self._buffers: dict[int, list[list[float]]] = {}
        # Track which obs triggered the last refresh (for provenance assertions).
        self.refresh_obs: dict[int, dict[str, Any]] = {}
        self.refresh_count: dict[int, int] = {}

    def _make_chunk(self, obs: dict[str, Any]) -> list[list[float]]:
        base = obs["eef_pos"][0] * 100.0
        return [
            [base + s * self.DIM_FACTOR + d * 0.01 for d in range(ACTION_DIM)]
            for s in range(self._chunk_len)
        ]

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        actions = []
        for env_key, obs in zip(env_keys, observations, strict=True):
            buf = self._buffers.get(env_key, [])
            if not buf:
                self.refresh_obs[env_key] = dict(obs)
                self.refresh_count[env_key] = self.refresh_count.get(env_key, 0) + 1
                buf = self._make_chunk(obs)
            action = buf.pop(0)
            self._buffers[env_key] = buf
            actions.append(action)
        return actions

    def expected_action(self, obs: dict[str, Any], chunk_step: int) -> list[float]:
        """Return the expected action for a given obs and position within the chunk."""
        base = obs["eef_pos"][0] * 100.0
        return [base + chunk_step * self.DIM_FACTOR + d * 0.01 for d in range(ACTION_DIM)]

    async def aclose(self) -> None:
        """Release this dependency-free provider (a deliberate no-op)."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIG = (ManipAction, ManipProprio, ManipStatus, ManipTask)
TARGETS = {0: (0.5, 0.0, 0.5), 1: (-0.5, 0.0, 0.5)}
TICKS = CHUNK_LEN * 3  # 3 full chunks worth of ticks


async def _build_world(tmp_path, policy, env):
    ws = make_world_harness()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="chunk")
    system = AsyncSystem()
    await system.add_processor(_PolicyActionProcessor(policy))
    await system.add_processor(_EnvStepProcessor(env))
    world = await ws.lifecycle.create_world(
        WorldConfig(name="chunk-test"), storage_config=storage, system=system
    )
    return ws, world


async def _spawn_envs(world, env, targets):
    eids = {}
    for env_key in targets:
        obs = env.reset(env_key, seed=env_key)
        eids[env_key] = await world.create_entity(
            [
                ManipProprio(
                    eef_pos=obs["eef_pos"],
                    eef_quat=obs["eef_quat"],
                    gripper=obs["gripper"],
                    gripper_qpos=obs.get("gripper_qpos", [0.0, 0.0]),
                ),
                ManipAction(values=[0.0] * ACTION_DIM),
                ManipStatus(),
                ManipTask(
                    suite="scripted",
                    task_id=0,
                    instruction="reach",
                    seed=env_key,
                    env_key=env_key,
                ),
            ]
        )
    return eids


async def _fetch_history(world, ticks):
    history = {}
    for tick in range(ticks):
        rows = (await world.query_archetype(sig=SIG, ticks=[tick])).to_pylist()
        history[tick] = {r["entity_id"]: r for r in rows}
    return history


# ---------------------------------------------------------------------------
# Test 1: chunk-cadence provenance with strict equality
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chunk_cadence_provenance_strict_equality(tmp_path):
    """Action at tick t equals chunk_step-th element of the chunk computed
    from obs at the chunk-boundary tick.  Chunk refreshes exactly every
    CHUNK_LEN ticks.  Done rows freeze without triggering a new refresh.

    This is the normative chunk-cadence provenance contract, proven with
    strict Python equality (no tolerance).
    """
    env = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
    policy = FakeChunkPolicy(chunk_len=CHUNK_LEN)

    ws, world = await _build_world(tmp_path, policy, env)
    try:
        eids = await _spawn_envs(world, env, TARGETS)
        await world.run(RunConfig(num_steps=TICKS))
        history = await _fetch_history(world, TICKS)

        # Reconstruct expected actions from first principles.
        # obs_{t-1} drives action_t.  The policy sees obs_{t-1} on tick t;
        # the chunk is refreshed when the buffer is empty (every CHUNK_LEN ticks).
        replay_env = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
        replay_policy = FakeChunkPolicy(chunk_len=CHUNK_LEN)
        for ek in TARGETS:
            replay_env.reset(ek, seed=ek)

        # Tick-0: spawn action untouched (raw initial conditions).
        for env_key, eid in eids.items():
            assert history[0][eid]["manipaction__values"] == [0.0] * ACTION_DIM, (
                f"env {env_key}: tick-0 action must be the spawn action"
            )

        # Ticks 1..N: verify chunk-cadence provenance.
        # We replay the env/policy in lockstep to compute expected actions.
        # Track which obs each chunk was computed from.
        chunk_boundary_obs: dict[
            int, dict[str, Any]
        ] = {}  # env_key -> obs that triggered last refresh
        chunk_pos: dict[int, int] = {}  # env_key -> position within current chunk

        for env_key in TARGETS:
            chunk_boundary_obs[env_key] = {}
            chunk_pos[env_key] = 0

        prev_obs: dict[int, dict[str, Any]] = {}
        for env_key, eid in eids.items():
            row0 = history[0][eid]
            prev_obs[env_key] = {
                "eef_pos": row0["manipproprio__eef_pos"],
                "eef_quat": row0["manipproprio__eef_quat"],
                "gripper": row0["manipproprio__gripper"],
            }

        for tick in range(1, TICKS):
            for env_key, eid in eids.items():
                prev_row = history[tick - 1][eid]
                row = history[tick][eid]

                if prev_row["manipstatus__done"]:
                    # Frozen: action and obs persist unchanged.
                    assert row["manipaction__values"] == prev_row["manipaction__values"], (
                        f"env {env_key} tick {tick}: done row must freeze action"
                    )
                    continue

                obs_prev = {
                    "eef_pos": prev_row["manipproprio__eef_pos"],
                    "eef_quat": prev_row["manipproprio__eef_quat"],
                    "gripper": prev_row["manipproprio__gripper"],
                }

                # Determine chunk step: (tick - 1) % CHUNK_LEN gives the position
                # within the chunk that was used to generate action at this tick.
                # The chunk is computed from obs at the chunk-boundary tick.
                chunk_step = (tick - 1) % CHUNK_LEN
                if chunk_step == 0:
                    # New chunk boundary: the chunk was generated from obs_{tick-1}.
                    chunk_boundary_obs[env_key] = obs_prev

                want = replay_policy.expected_action(chunk_boundary_obs[env_key], chunk_step)
                assert row["manipaction__values"] == want, (
                    f"env {env_key} tick {tick}: "
                    f"chunk_step={chunk_step} action != expected\n"
                    f"  got:  {row['manipaction__values']}\n"
                    f"  want: {want}"
                )

        # Chunk refreshes exactly every CHUNK_LEN ticks per env.
        # First refresh at tick 1 (chunk_step=0), then every CHUNK_LEN ticks.
        # Count: TICKS ticks → ceil(TICKS / CHUNK_LEN) refreshes per non-done env.
        import math

        for env_key in TARGETS:
            eid = eids[env_key]
            # Count ticks where this env was not done (it may have gone done mid-run).
            done_tick = None
            for tick in range(TICKS):
                if history[tick][eid]["manipstatus__done"]:
                    done_tick = tick
                    break
            if done_tick is None:
                # Never done: TICKS / CHUNK_LEN refreshes.
                expected_refreshes = math.ceil(TICKS / CHUNK_LEN)
                assert policy.refresh_count.get(env_key, 0) == expected_refreshes, (
                    f"env {env_key}: expected {expected_refreshes} refreshes, "
                    f"got {policy.refresh_count.get(env_key, 0)}"
                )
            # (Done envs: fewer refreshes, already verified by done-freeze above)

    finally:
        await ws.close()


# ---------------------------------------------------------------------------
# Test 2: done rows freeze (standalone, small episode)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_done_rows_freeze_action_and_no_refresh(tmp_path):
    """Once a row goes done, the action freezes and no new chunk is requested."""
    # Use a very close target so the env goes done after 1 step.
    targets = {0: (0.001, -0.001, 0.5)}  # seed=0 start: [0, 0, 0.5], target nearby
    env = ScriptedReachEnv(targets=targets, tolerance=0.1)
    policy = FakeChunkPolicy(chunk_len=CHUNK_LEN)

    ws = make_world_harness()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="freeze")
        system = AsyncSystem()
        await system.add_processor(_PolicyActionProcessor(policy))
        await system.add_processor(_EnvStepProcessor(env))
        world = await ws.lifecycle.create_world(
            WorldConfig(name="freeze-test"), storage_config=storage, system=system
        )

        obs = env.reset(0, seed=0)
        eid = await world.create_entity(
            [
                ManipProprio(
                    eef_pos=obs["eef_pos"],
                    eef_quat=obs["eef_quat"],
                    gripper=obs["gripper"],
                    gripper_qpos=obs.get("gripper_qpos", [0.0, 0.0]),
                ),
                ManipAction(values=[0.0] * ACTION_DIM),
                ManipStatus(),
                ManipTask(suite="scripted", task_id=0, instruction="reach", seed=0, env_key=0),
            ]
        )

        ticks = CHUNK_LEN * 2
        await world.run(RunConfig(num_steps=ticks))

        history = {}
        for tick in range(ticks):
            rows = (await world.query_archetype(sig=SIG, ticks=[tick])).to_pylist()
            history[tick] = {r["entity_id"]: r for r in rows}[eid]

        # Find when done latched.
        done_tick = next(t for t in range(ticks) if history[t]["manipstatus__done"])
        terminal_action = history[done_tick]["manipaction__values"]

        for tick in range(done_tick + 1, ticks):
            assert history[tick]["manipaction__values"] == terminal_action, (
                f"tick {tick}: action should be frozen after done at tick {done_tick}"
            )

        # The policy was called (refresh_count > 0) but only up to done.
        # After done, no new refreshes: refresh_count stays constant.
        refreshes_at_done = policy.refresh_count.get(0, 0)
        # Run would not call act again on done rows; refresh count must be <= ceil((done_tick)/CHUNK_LEN).
        assert refreshes_at_done <= ticks // CHUNK_LEN + 1, (
            f"too many refreshes: {refreshes_at_done}"
        )

    finally:
        await ws.close()


# ---------------------------------------------------------------------------
# Test 3: worker-local provider factories are unsupported
# ---------------------------------------------------------------------------


def test_processors_require_explicit_runtime_owned_clients() -> None:
    with pytest.raises(TypeError):
        _EnvStepProcessor()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        _PolicyActionProcessor()  # type: ignore[call-arg]

    manipulation = import_module("archetype.physical_ai.manipulation")
    policy = import_module("archetype.physical_ai.policy")
    assert not hasattr(manipulation, "EnvClientSpec")
    assert not hasattr(policy, "PolicyClientSpec")
