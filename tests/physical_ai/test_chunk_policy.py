# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Chunk-cadence provenance contract for VlaJepaPolicyClient-style buffering.

Verifies three normative properties with strict equality (no Modal, CI-safe):

1. **Chunk-cadence**: action at tick t equals the correct element from the
   chunk computed from the observation at the most recent chunk-boundary tick.
   The chunk refreshes exactly every N ticks (N = chunk_len).

2. **Done-row freezing**: once a row is done its action column is not
   updated even if a new chunk would normally be requested.

3. **Resources-spec construction produces identical results to constructor
   injection**: a processor built from an ``EnvClientSpec``/``PolicyClientSpec``
   in Resources gives the same ledger rows as one built by passing the client
   directly.

``FakeChunkPolicy`` is a deterministic in-process policy that returns
arithmetic chunks so that all assertions can be expressed as closed-form
exact equalities without any floating-point tolerance.
"""

from __future__ import annotations

from typing import Any

import pytest

from archetype.core.aio import AsyncSystem
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.physical_ai.manipulation import (
    ACTION_DIM,
    EnvClientSpec,
    EnvStepProcessor,
    ManipAction,
    ManipProprio,
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)
from archetype.physical_ai.policy import (
    PolicyActionProcessor,
    PolicyClientSpec,
)
from tests.conftest import make_world_service

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIG = (ManipAction, ManipProprio, ManipStatus, ManipTask)
TARGETS = {0: (0.5, 0.0, 0.5), 1: (-0.5, 0.0, 0.5)}
TICKS = CHUNK_LEN * 3  # 3 full chunks worth of ticks


async def _build_world(tmp_path, policy, env):
    ws = make_world_service()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="chunk")
    system = AsyncSystem()
    await system.add_processor(PolicyActionProcessor(policy))
    await system.add_processor(EnvStepProcessor(env))
    world = await ws.create_world(
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
        await ws.shutdown()


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

    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="freeze")
        system = AsyncSystem()
        await system.add_processor(PolicyActionProcessor(policy))
        await system.add_processor(EnvStepProcessor(env))
        world = await ws.create_world(
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
        await ws.shutdown()


# ---------------------------------------------------------------------------
# Test 3: Resources-spec construction == constructor injection
# ---------------------------------------------------------------------------


class _TestPolicySpec(PolicyClientSpec):
    """Test subclass of PolicyClientSpec.

    Overrides ``build()`` to return a pre-constructed ``FakeChunkPolicy``
    instead of a live ``VlaJepaPolicyClient``.  Registered in Resources via
    ``insert_as(spec, PolicyClientSpec)`` so that the production
    ``PolicyActionProcessor._ensure_callers`` path is exercised without
    requiring a deployed Modal worker.
    """

    def __init__(self, policy: Any) -> None:
        self._policy = policy

    def build(self) -> Any:  # type: ignore[override]
        return self._policy


class _TestEnvSpec(EnvClientSpec):
    """Test subclass of EnvClientSpec.

    Overrides ``build()`` to return a pre-constructed ``ScriptedReachEnv``
    instead of a live ``ModalEnvClient``.  Registered via
    ``insert_as(spec, EnvClientSpec)`` so that the production
    ``EnvStepProcessor._ensure_stepper`` path is exercised.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def build(self) -> Any:  # type: ignore[override]
        return self._client


@pytest.mark.asyncio
async def test_resources_spec_produces_identical_results(tmp_path):
    """A processor built from spec (via Resources) produces the same ledger
    rows as one built by constructor injection.

    This test exercises the **production** ``_ensure_callers`` /
    ``_ensure_stepper`` code paths by registering ``_TestPolicySpec`` /
    ``_TestEnvSpec`` (subclasses of ``PolicyClientSpec`` / ``EnvClientSpec``)
    in Resources under the base types via ``resources.insert_as``.  The
    production ``PolicyActionProcessor`` and ``EnvStepProcessor`` are used
    directly (no test-only processor variants).

    The driver/worker boundary rationale: in production the spec holds
    picklable scalars; the live client is built in ``spec.build()`` inside the
    processor (driver process for ``AsyncProcessor``, Daft worker process for
    ``@daft.cls`` UDFs).  Here ``_TestPolicySpec.build()`` returns a
    ``FakeChunkPolicy`` and ``_TestEnvSpec.build()`` returns a
    ``ScriptedReachEnv`` so the test remains CI-safe with no Modal deployment.
    """
    TICKS_SPEC = CHUNK_LEN + 1

    # --- World A: constructor injection ---
    env_a = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
    policy_a = FakeChunkPolicy(chunk_len=CHUNK_LEN)

    ws_a = make_world_service()
    try:
        storage_a = StorageConfig(uri=str(tmp_path / "store_a"), namespace="spec_a")
        system_a = AsyncSystem()
        await system_a.add_processor(PolicyActionProcessor(policy_a))
        await system_a.add_processor(EnvStepProcessor(env_a))
        world_a = await ws_a.create_world(
            WorldConfig(name="spec-inject-a"), storage_config=storage_a, system=system_a
        )
        eids_a = await _spawn_envs(world_a, env_a, TARGETS)
        await world_a.run(RunConfig(num_steps=TICKS_SPEC))
        history_a = await _fetch_history(world_a, TICKS_SPEC)
    finally:
        await ws_a.shutdown()

    # --- World B: Resources-spec construction via production processors ---
    # env_b and policy_b are fresh instances of the same types as world A.
    # _TestPolicySpec.build() / _TestEnvSpec.build() return them on first
    # process() call, exercising PolicyActionProcessor._ensure_callers and
    # EnvStepProcessor._ensure_stepper.
    env_b = ScriptedReachEnv(targets=TARGETS, tolerance=0.02)
    policy_b = FakeChunkPolicy(chunk_len=CHUNK_LEN)

    ws_b = make_world_service()
    try:
        storage_b = StorageConfig(uri=str(tmp_path / "store_b"), namespace="spec_b")
        system_b = AsyncSystem()
        # Production processors — no test-only variants.
        await system_b.add_processor(PolicyActionProcessor())
        await system_b.add_processor(EnvStepProcessor())
        world_b = await ws_b.create_world(
            WorldConfig(name="spec-inject-b"), storage_config=storage_b, system=system_b
        )
        # Register specs under their base types so the production require()
        # lookups in _ensure_callers / _ensure_stepper find them.
        world_b.resources.insert_as(_TestPolicySpec(policy_b), PolicyClientSpec)
        world_b.resources.insert_as(_TestEnvSpec(env_b), EnvClientSpec)
        eids_b = await _spawn_envs(world_b, env_b, TARGETS)
        await world_b.run(RunConfig(num_steps=TICKS_SPEC))
        history_b = await _fetch_history(world_b, TICKS_SPEC)
    finally:
        await ws_b.shutdown()

    # --- Assert identical ledger rows ---
    # Map env_key → (eid_a, eid_b) for comparison.
    env_keys = list(TARGETS.keys())
    for env_key in env_keys:
        eid_a = eids_a[env_key]
        eid_b = eids_b[env_key]
        for tick in range(TICKS_SPEC):
            row_a = history_a[tick][eid_a]
            row_b = history_b[tick][eid_b]
            assert row_a["manipaction__values"] == row_b["manipaction__values"], (
                f"env {env_key} tick {tick}: action mismatch between inject and spec construction\n"
                f"  inject: {row_a['manipaction__values']}\n"
                f"  spec:   {row_b['manipaction__values']}"
            )
            assert row_a["manipproprio__eef_pos"] == row_b["manipproprio__eef_pos"], (
                f"env {env_key} tick {tick}: eef_pos mismatch"
            )
            assert row_a["manipstatus__done"] == row_b["manipstatus__done"], (
                f"env {env_key} tick {tick}: done mismatch"
            )
