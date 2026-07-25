# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Batched physical-task evaluation through the registered runtime boundary.

Proves the redesign that replaces the old per-trial-world driver, using the
in-process scripted env+policy (no Modal, no LIBERO) — the exact same
orchestration that the in-process LIBERO client will run unchanged:

1. **One control-plane world, N trial entities, batch-stepped.** All trials
   advance together through ``run_episode``; finished trials freeze on the
   ledger while the rest keep going.
2. **Addressable trajectories (the A2 fix).** Every trial's rows are reachable
   by the single ``(world_id, run_id)`` — nothing is orphaned in a destroyed
   per-trial world.
3. **Graded from raw ``ManipStatus`` by the eval service**, and the numbers
   match an independent in-Python sim of the same policy+env (so there is no
   ``EvalTrialResult`` summary to drift — E1).
4. **B1+B2 exercised end-to-end**: ``run_episode`` advances target-tick quota
   scope and terminates on the value-based "all done" contract.
"""

from __future__ import annotations

from dataclasses import fields

import pytest

from archetype import ArchetypeRuntime, PhysicalTaskEvalConfig
from archetype.core.config import StorageConfig
from archetype.physical_ai.manipulation import ManipStatus, ManipTask, ScriptedReachEnv
from archetype.physical_ai.models import (
    EvaluatePhysicalTask,
    InstructionSweepReport,
    PhysicalTaskEvalReport,
    TrialOutcome,
    VariantOutcome,
)
from archetype.physical_ai.policy import ScriptedReachPolicy
from archetype.world.models import ComponentTypeRef, QueryComponents
from tests._runtime import build_test_runtime

# Proportional-controller params shared by the policy, the env, and the replay.
GAIN = 0.5
MAX_STEP = 0.05
TOL = 0.02
MAX_STEPS = 40
# env_key 0/1/2 reachable at increasing distances; env_key 3 unreachable in 40 steps.
TARGETS = {
    0: (0.10, 0.0, 0.5),
    1: (0.20, 0.0, 0.5),
    2: (0.30, 0.0, 0.5),
    3: (5.00, 0.0, 0.5),
}


def _start(seed: int) -> list[float]:
    # Mirrors ScriptedReachEnv.reset's deterministic seed-derived start.
    return [0.001 * seed, -0.001 * seed, 0.5]


def _simulate(target: tuple[float, float, float], seed: int) -> tuple[bool, int]:
    """Independent ground truth: ScriptedReachPolicy + ScriptedReachEnv in pure
    Python. Returns (success, env_steps_to_success). Never touches the ledger."""
    pos = _start(seed)
    for step in range(1, MAX_STEPS + 1):
        for axis in range(3):
            delta = GAIN * (target[axis] - pos[axis])
            pos[axis] += max(-MAX_STEP, min(MAX_STEP, delta))
        dist = sum((pos[i] - target[i]) ** 2 for i in range(3)) ** 0.5
        if dist < TOL:
            return True, step
    return False, MAX_STEPS


def test_physical_report_shapes_and_derived_lengths_are_exact() -> None:
    assert tuple(field.name for field in fields(TrialOutcome)) == (
        "trial_idx",
        "env_key",
        "seed",
        "success",
        "episode_length",
    )
    assert tuple(field.name for field in fields(PhysicalTaskEvalReport)) == (
        "suite",
        "task_id",
        "instruction",
        "world_id",
        "run_id",
        "trials",
    )
    assert tuple(field.name for field in fields(VariantOutcome)) == (
        "instruction",
        "n_trials",
        "n_success",
        "success_rate",
        "mean_length",
    )
    assert tuple(field.name for field in fields(InstructionSweepReport)) == (
        "suite",
        "task_id",
        "world_id",
        "run_id",
        "variants",
    )

    report = PhysicalTaskEvalReport(
        suite="shape",
        task_id=1,
        instruction="reach",
        world_id="world",
        run_id="run",
        trials=(
            TrialOutcome(0, 0, 1000, True, 2),
            TrialOutcome(1, 1, 1001, False, 6),
        ),
    )
    assert report.success_rate == 0.5
    assert report.mean_length == 4.0


@pytest.mark.asyncio
async def test_batched_control_plane_eval_is_addressable_and_graded(tmp_path):
    resources = build_test_runtime(tmp_path)
    dispatcher = resources.dispatcher
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="evalrun")
    try:
        env = ScriptedReachEnv(targets=TARGETS, tolerance=TOL)
        policy = ScriptedReachPolicy(targets=TARGETS, gain=GAIN, max_step=MAX_STEP)

        report = await dispatcher.apply(
            EvaluatePhysicalTask(
                config=PhysicalTaskEvalConfig(
                    suite="scripted",
                    task_id=0,
                    trials=4,
                    max_steps=MAX_STEPS,
                    storage=storage,
                ),
                env_client=env,
                policy_client=policy,
            )
        )

        assert report.suite == "scripted"
        assert report.task_id == 0
        assert report.instruction == "reach"
        assert report.trials == (
            TrialOutcome(0, 0, 0, True, 3),
            TrialOutcome(1, 1, 1, True, 5),
            TrialOutcome(2, 2, 2, True, 7),
            TrialOutcome(3, 3, 3, False, 39),
        )
        assert report.mean_length == 13.5

        # Independent replay (seed = task_id*1000 + env_key = env_key for task 0).
        expected = {ek: _simulate(TARGETS[ek], ek) for ek in TARGETS}
        got = {t.env_key: (t.success, t.episode_length) for t in report.trials}

        for ek in TARGETS:
            exp_success, exp_len = expected[ek]
            got_success, got_len = got[ek]
            assert got_success == exp_success, f"trial {ek}: success mismatch"
            if exp_success:
                # length-to-success is well-defined; assert it exactly.
                assert got_len == exp_len, f"trial {ek}: length {got_len} != replay {exp_len}"

        assert report.success_rate == 3 / 4
        assert report.world_id and report.run_id

        # A2: every trial's trajectory is addressable by the one (world_id, run_id).
        df = await dispatcher.apply(
            QueryComponents(
                components=tuple(
                    ComponentTypeRef.from_type(component) for component in (ManipStatus, ManipTask)
                ),
                world_id=report.world_id,
                run_id=report.run_id,
                storage_config=storage,
            )
        )
        rows = df.collect().to_pylist()
        assert len({r["entity_id"] for r in rows}) == 4, "all 4 trials must persist, none orphaned"
    finally:
        await resources.aclose()


@pytest.mark.asyncio
async def test_runtime_path_matches_replay_without_gateway_access_audit(tmp_path):
    """Trusted runtime evaluation matches replay without fabricating an actor."""
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="evalrt")
    async with ArchetypeRuntime() as runtime:
        env = ScriptedReachEnv(targets=TARGETS, tolerance=TOL)
        policy = ScriptedReachPolicy(targets=TARGETS, gain=GAIN, max_step=MAX_STEP)

        report = await runtime.evaluate_physical_task(
            PhysicalTaskEvalConfig(
                suite="scripted",
                task_id=0,
                trials=4,
                max_steps=MAX_STEPS,
                storage=storage,
            ),
            env_client=env,
            policy_client=policy,
        )

        expected = {ek: _simulate(TARGETS[ek], ek) for ek in TARGETS}
        got = {t.env_key: (t.success, t.episode_length) for t in report.trials}
        for ek in TARGETS:
            exp_success, exp_len = expected[ek]
            assert got[ek][0] == exp_success
            if exp_success:
                assert got[ek][1] == exp_len
        assert report.success_rate == 3 / 4

        # Trusted scripting never fabricates gateway authorization/access
        # events. Product evaluation evidence is carried by the report and
        # evaluation receipts, not an actor-shaped access audit.
        audit = runtime.attach(report.world_id, storage=storage)
        rows = (await audit.history(limit=200)).collect().to_pylist()
        assert rows == []


def test_sync_runtime_exposes_the_same_typed_workflow(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="evalsync")
    env = ScriptedReachEnv(targets={0: TARGETS[0]}, tolerance=TOL)
    policy = ScriptedReachPolicy(targets={0: TARGETS[0]}, gain=GAIN, max_step=MAX_STEP)

    with ArchetypeRuntime.sync() as runtime:
        report = runtime.evaluate_physical_task(
            PhysicalTaskEvalConfig(
                suite="scripted",
                task_id=0,
                trials=1,
                max_steps=MAX_STEPS,
                storage=storage,
            ),
            env_client=env,
            policy_client=policy,
        )

    assert report.world_id and report.run_id
    assert len(report.trials) == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"suite": "", "trials": 1, "max_steps": 1}, "suite"),
        ({"suite": "s", "trials": 0, "max_steps": 1}, "trials"),
        ({"suite": "s", "trials": 1, "max_steps": 0}, "max_steps"),
    ],
)
def test_task_eval_config_rejects_invalid_work(kwargs, message):
    with pytest.raises(ValueError, match=message):
        PhysicalTaskEvalConfig(task_id=0, **kwargs)
