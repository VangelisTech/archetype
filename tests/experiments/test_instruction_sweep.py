# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Instruction-perturbation eval + self-improving optimizer (archetype.experiments.instruction_sweep).

SCOPE — this is a MECHANISM CHECK, not evidence for the scientific claim.
``InstructionConditionedReachPolicy`` defines its effective gain *as*
``instruction_quality(instruction)``, and the replay oracle here recomputes that
same function, so success-rate is monotone in the optimized metric BY
CONSTRUCTION. These tests therefore prove only that the machinery is correct —
the sweep grades per-variant from the ledger, the seeds are paired, the
optimizer hill-climbs and never regresses, no per-env state leaks. They do NOT
demonstrate that prompt optimization lifts a *real* VLA's success rate; that
requires running ``optimize_task`` on real LIBERO against an objective the
optimizer does not define (roadmap A3/H5, and the Limitations section).

The plumbing under test is real: the instruction is a per-entity ``ManipTask``
field that flows to the policy (``PolicyClient.act`` receives it; the real
``InProcessVlaJepaPolicy`` forwards it into the VLA). Swap ``ScriptedReachEnv`` ->
``InProcessLiberoEnvClient`` and the stand-in policy -> ``InProcessVlaJepaPolicy``
and the same sweep + optimizer run on real LIBERO on a Modal GPU container.
"""

from __future__ import annotations

import pytest

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig
from archetype.experiments.instruction_sweep import (
    TemplatePerturbation,
    optimize_instruction,
    run_instruction_sweep,
)
from archetype.experiments.manipulation import ManipStatus, ManipTask, ScriptedReachEnv
from archetype.experiments.policy import (
    InstructionConditionedReachPolicy,
    instruction_quality,
)

# Proportional-controller params shared by the policy and the replay oracle.
GAIN = 0.6
MAX_STEP = 0.05
TOL = 0.02
# ``run_episode(max_steps=N)`` persists tick 0 (the reset observation) plus
# N-1 control-step ticks — the reset occupies one slot of the budget — so a
# trial gets CONTROL_STEPS env steps. The replay oracle below uses exactly that.
MAX_STEPS = 7
CONTROL_STEPS = MAX_STEPS - 1
SEEDS = 5
TASK_ID = 0  # run_instruction_sweep derives seed = TASK_ID * 1000 + env_key
# Per-seed target distance (along +x). The spread makes the success threshold
# cross gradually as the instruction's effective gain rises, so success-rate is
# graded in instruction quality (0 -> 0.2 -> 0.8 -> 1.0), not a binary cliff.
DISTS = [0.05, 0.10, 0.15, 0.20, 0.25]

# The "true" task keywords; quality = fraction named. Vocabulary = required
# keywords plus neutral distractors the optimizer must learn to ignore.
REQUIRED = ("reach", "red", "block")
VOCAB = ["reach", "red", "block", "blue", "slowly", "gripper"]


def _targets(n_env_keys: int = 64) -> dict[int, tuple[float, float, float]]:
    """Target per env_key such that the *distance* depends only on the seed
    slot (``env_key % SEEDS``), never on which variant a trial belongs to.

    ``ScriptedReachEnv`` derives the start from the seed
    (``seed = TASK_ID * 1000 + env_key``, the sweep's own formula); placing the
    target at ``start + DISTS[slot]`` along +x makes ``target - start`` exactly
    ``(DISTS[slot], 0, 0)`` for every variant, so a variant's success-rate is a
    pure function of its instruction quality.
    """
    targets: dict[int, tuple[float, float, float]] = {}
    for env_key in range(n_env_keys):
        seed = TASK_ID * 1000 + (env_key % SEEDS)  # paired seed-slot, not position
        start_x = 0.001 * seed
        start_y = -0.001 * seed
        dist = DISTS[env_key % SEEDS]
        targets[env_key] = (start_x + dist, start_y, 0.5)
    return targets


def _simulate(
    env_key: int, eff_gain: float, targets: dict[int, tuple[float, float, float]]
) -> bool:
    """Independent ground truth: replay the proportional controller + env in
    pure Python at a given effective gain. Returns whether the trial reaches the
    target within ``MAX_STEPS``. Never touches the ledger."""
    seed = TASK_ID * 1000 + (env_key % SEEDS)  # paired seed-slot, not position
    pos = [0.001 * seed, -0.001 * seed, 0.5]
    target = targets[env_key]
    for _step in range(1, CONTROL_STEPS + 1):
        for axis in range(3):
            delta = eff_gain * (target[axis] - pos[axis])
            pos[axis] += max(-MAX_STEP, min(MAX_STEP, delta))
        dist = sum((pos[i] - target[i]) ** 2 for i in range(3)) ** 0.5
        if dist < TOL:
            return True
    return False


def _expected_success_rate(
    instruction: str, variant_index: int, targets: dict[int, tuple[float, float, float]]
) -> float:
    """Replay all SEEDS trials of one variant and return its success-rate."""
    eff_gain = GAIN * instruction_quality(instruction, REQUIRED)
    base = variant_index * SEEDS
    successes = sum(_simulate(base + j, eff_gain, targets) for j in range(SEEDS))
    return successes / SEEDS


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


@pytest.mark.asyncio
async def test_instruction_sweep_grades_success_rate_per_variant(tmp_path):
    """V variants in ONE control-plane world, graded per variant from the
    persisted ledger, matching an independent replay exactly — and every
    trajectory addressable by the single ``(world_id, run_id)``."""
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="isweep")
    targets = _targets()
    try:
        env = ScriptedReachEnv(targets=targets, tolerance=TOL)
        policy = InstructionConditionedReachPolicy(
            targets=targets, required_keywords=REQUIRED, gain=GAIN, max_step=MAX_STEP
        )
        # qualities: 0, 1/3, 2/3, 1  ->  graded success-rates.
        variants = ["", "reach", "reach red", "reach red block"]

        report = await run_instruction_sweep(
            world_service=container.world_service,
            simulation_service=container.simulation_service,
            eval_service=container.eval_service,
            env_client=env,
            policy_client=policy,
            suite="scripted",
            task_id=TASK_ID,
            variants=variants,
            seeds_per_variant=SEEDS,
            max_steps=MAX_STEPS,
            storage=storage,
        )

        assert [o.instruction for o in report.variants] == variants
        for vidx, variant in enumerate(variants):
            outcome = report.variants[vidx]
            expected = _expected_success_rate(variant, vidx, targets)
            assert outcome.success_rate == expected, (
                f"variant {variant!r}: ledger {outcome.success_rate} != replay {expected}"
            )
            assert outcome.n_trials == SEEDS

        rates = [o.success_rate for o in report.variants]
        assert rates == sorted(rates), "success-rate must be monotonic in instruction quality"
        assert report.variants[0].success_rate == 0.0, "vague instruction -> no success"
        assert report.variants[-1].success_rate == 1.0, "precise instruction -> full success"
        assert report.best is not None and report.best.instruction == "reach red block"

        # A2 addressability: every trial persists under one (world_id, run_id).
        df = await container.eval_service.query_components(
            [ManipStatus, ManipTask],
            world_id=report.world_id,
            run_id=report.run_id,
            storage_config=storage,
        )
        rows = df.collect().to_pylist()
        assert len({r["entity_id"] for r in rows}) == len(variants) * SEEDS
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_optimize_instruction_climbs_from_vague_to_precise(tmp_path):
    """The self-improving loop: start from a useless instruction (0% success),
    mutate-and-evaluate on the real batched eval each round, and climb to full
    success — gradually, against the success-rate metric itself."""
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="iopt")
    targets = _targets()
    try:
        env = ScriptedReachEnv(targets=targets, tolerance=TOL)
        policy = InstructionConditionedReachPolicy(
            targets=targets, required_keywords=REQUIRED, gain=GAIN, max_step=MAX_STEP
        )

        async def evaluate(instructions: list[str]) -> dict[str, float]:
            report = await run_instruction_sweep(
                world_service=container.world_service,
                simulation_service=container.simulation_service,
                eval_service=container.eval_service,
                env_client=env,
                policy_client=policy,
                suite="scripted",
                task_id=TASK_ID,
                variants=instructions,
                seeds_per_variant=SEEDS,
                max_steps=MAX_STEPS,
                storage=storage,
            )
            return report.scores

        result = await optimize_instruction(
            evaluate=evaluate,
            base="",
            strategy=TemplatePerturbation(VOCAB),
            rounds=6,
            neighbors=len(VOCAB),
            patience=3,
        )

        assert result.trace[0].best_success_rate == 0.0, "the vague base must fail"
        assert result.best_success_rate == 1.0, "optimization must reach full success"
        assert result.best_success_rate > result.trace[0].best_success_rate, "must improve"

        bests = [r.best_success_rate for r in result.trace]
        assert bests == sorted(bests), "best-so-far must never regress"
        assert any(0.0 < r.best_success_rate < 1.0 for r in result.trace), (
            "the climb must be gradual (a partial-credit round), not a single jump"
        )

        # It found the task's real keywords (>= 2 of 3), not just distractors.
        assert instruction_quality(result.best_instruction, REQUIRED) >= 2 / 3
        assert result.best_instruction.split(), "the optimized instruction is non-empty"
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_sweep_is_paired_and_position_invariant(tmp_path):
    """An instruction's success-rate must be identical regardless of where it
    sits in the candidate list — variants are graded on the SAME paired
    init-states (slot-keyed seeds), not on disjoint position-keyed ones. This is
    the A/B-pairing the thesis depends on; locks seed = task*1000 + seed_slot."""
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ipair")
    targets = _targets()
    try:
        env = ScriptedReachEnv(targets=targets, tolerance=TOL)
        policy = InstructionConditionedReachPolicy(
            targets=targets, required_keywords=REQUIRED, gain=GAIN, max_step=MAX_STEP
        )

        async def sweep(variants):
            report = await run_instruction_sweep(
                world_service=container.world_service,
                simulation_service=container.simulation_service,
                eval_service=container.eval_service,
                env_client=env,
                policy_client=policy,
                suite="scripted",
                task_id=TASK_ID,
                variants=variants,
                seeds_per_variant=SEEDS,
                max_steps=MAX_STEPS,
                storage=storage,
            )
            return report.scores

        forward = await sweep(["reach", "reach red", "reach red block"])
        reversed_ = await sweep(["reach red block", "reach red", "reach"])
        for instruction in forward:
            assert forward[instruction] == reversed_[instruction], (
                f"{instruction!r} graded differently by position: "
                f"{forward[instruction]} vs {reversed_[instruction]}"
            )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_sweep_resets_policy_state_between_runs(tmp_path):
    """``run_instruction_sweep`` must drop a stateful policy's per-env buffers at
    each sweep boundary, so a reused env_key never replays a prior sweep's
    recurrent state (the VlaJepaPolicyClient chunk-leak). Locks the reset() call."""
    container = ServiceContainer()
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ireset")
    targets = _targets()

    class _SpyPolicy(InstructionConditionedReachPolicy):
        resets = 0

        def reset(self):
            type(self).resets += 1

    try:
        env = ScriptedReachEnv(targets=targets, tolerance=TOL)
        policy = _SpyPolicy(
            targets=targets, required_keywords=REQUIRED, gain=GAIN, max_step=MAX_STEP
        )
        for _ in range(2):
            await run_instruction_sweep(
                world_service=container.world_service,
                simulation_service=container.simulation_service,
                eval_service=container.eval_service,
                env_client=env,
                policy_client=policy,
                suite="scripted",
                task_id=TASK_ID,
                variants=["reach red"],
                seeds_per_variant=SEEDS,
                max_steps=MAX_STEPS,
                storage=storage,
            )
        assert _SpyPolicy.resets == 2, "each sweep must reset the policy's per-env state"
    finally:
        await container.shutdown()
