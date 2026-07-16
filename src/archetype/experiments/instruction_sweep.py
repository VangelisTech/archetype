# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Instruction-perturbation eval + a GEPA-style optimizer over it.

The research boundary, stated precisely: the natural-language instruction is a
per-entity ``ManipTask`` field that flows, unchanged, to the policy.
``PolicyActionProcessor`` hands each entity's ``maniptask__instruction`` to
``PolicyClient.act(env_keys, instructions, observations)``; the real
``InProcessVlaJepaPolicy`` forwards it straight into the VLA
(``predict_action(instructions=...)``). So "optimize the prompt for a VLA" is
not a new mechanism — it is:

    spawn V instruction variants x S seeds as trial entities in ONE
    control-plane world, batch-step them, grade success-rate *per variant*
    from the persisted ledger, then mutate the winner and repeat.

That is the same orchestration as :func:`bench.libero.eval_run.run_task_eval`
— one world, N entities, batch-stepped, graded by the eval service — with the
single difference that the per-entity instruction varies. The scripted
``InstructionConditionedReachPolicy`` proves the loop end-to-end in CI (no GPU,
no model, fully replayable); swapping in ``InProcessLiberoEnvClient`` +
``InProcessVlaJepaPolicy`` runs the identical loop on real LIBERO on a Modal GPU
container, the orchestration untouched.

The optimizer's ``evaluate`` callback is injectable. The default closes over a
real batched sweep (ground truth: actually run the policy). A future fast inner
loop can pass a VLA-JEPA *latent* scorer instead — score variants by imagined
rollout in the world model, no env step — without changing the search.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import uuid_utils as uuid

from archetype.app.models import EpisodeConfig
from archetype.core.config import StorageConfig, WorldConfig
from archetype.experiments.eval_rollouts import _final_row_per_entity
from archetype.experiments.manipulation import (
    ACTION_DIM,
    EnvStepProcessor,
    FramedEnvStepProcessor,
    ManipAction,
    ManipFrameRef,
    ManipProprio,
    ManipStatus,
    ManipTask,
)
from archetype.experiments.policy import PolicyActionProcessor

# ---------------------------------------------------------------------------
# Per-variant graded outcome
# ---------------------------------------------------------------------------


@dataclass
class VariantOutcome:
    """Graded result for one instruction variant, from the persisted ledger."""

    instruction: str
    n_trials: int
    n_success: int
    success_rate: float
    mean_length: float


@dataclass
class SweepReport:
    """All variants of one task, graded from one ``(world_id, run_id)``."""

    suite: str
    task_id: int
    world_id: str
    run_id: str
    variants: list[VariantOutcome] = field(default_factory=list)

    @property
    def scores(self) -> dict[str, float]:
        """instruction -> success_rate, the optimizer's objective surface."""
        return {v.instruction: v.success_rate for v in self.variants}

    @property
    def best(self) -> VariantOutcome | None:
        """Highest success-rate variant; ties break to the shorter instruction
        then lexicographically, so the winner is deterministic."""
        if not self.variants:
            return None
        return max(
            self.variants,
            key=lambda v: (v.success_rate, -len(v.instruction), _neg_lex(v.instruction)),
        )


def _neg_lex(s: str) -> tuple[int, ...]:
    """Lexicographic key negated so ``max`` prefers the earlier string."""
    return tuple(-ord(c) for c in s)


def _dedup(items: Sequence[str]) -> list[str]:
    """Order-preserving de-duplication."""
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


# ---------------------------------------------------------------------------
# The sweep — V variants x S seeds, one world, graded per variant
# ---------------------------------------------------------------------------


async def run_instruction_sweep(
    *,
    world_service: Any,
    simulation_service: Any,
    eval_service: Any,
    env_client: Any,
    policy_client: Any,
    suite: str,
    task_id: int,
    variants: Sequence[str],
    seeds_per_variant: int,
    max_steps: int,
    storage: StorageConfig,
    with_frames: bool = False,
) -> SweepReport:
    """Run every instruction variant of one task in a single batched world.

    Each ``(variant, seed)`` pair is one trial entity carrying that variant's
    instruction in ``ManipTask``. The policy (priority 1) conditions its action
    on the entity's instruction; ``EnvStepProcessor`` (priority 10) consumes
    it. ``run_episode`` batch-steps every trial until each one's
    ``ManipStatus.done`` latches (B2), then the eval service grades the
    persisted rows, grouped by instruction. Duplicate variants are collapsed
    (the same instruction is the same condition) — add seeds, not duplicates,
    for more trials.

    ``max_steps`` bounds the persisted episode length *including the reset tick*
    (tick 0 is the reset observation), so each trial receives ``max_steps - 1``
    control steps. Size it as ``desired_control_steps + 1``.
    """
    unique_variants = _dedup(variants)
    if not unique_variants:
        raise ValueError("run_instruction_sweep needs at least one instruction variant")

    # Unique per sweep: the optimizer runs many sweeps with the same
    # suite/task_id, and the world registry enforces name uniqueness.
    world = await world_service.create_world(
        WorldConfig(name=f"isweep:{suite}:t{task_id}:{uuid.uuid7()}"), storage
    )
    await world.add_processor(PolicyActionProcessor(policy_client))
    processor = FramedEnvStepProcessor(env_client) if with_frames else EnvStepProcessor(env_client)
    await world.add_processor(processor)

    # Fresh measurement: drop any per-env recurrent state (e.g. a VLA's action
    # chunk buffer) carried over from a previous sweep that reused these env_keys.
    policy_reset = getattr(policy_client, "reset", None)
    if callable(policy_reset):
        policy_reset()

    # Reset-then-spawn: each trial's reset obs lands as its raw tick-0 row.
    #
    # Seeding is PAIRED and position-invariant: the trial's init-state is keyed by
    # its per-variant ``seed_slot`` (0..S-1), NOT by the global ``env_key``. So
    # every variant's slot-j trial starts from the *same* init-state — the only
    # difference between variants is the instruction (a clean A/B) — and the same
    # instruction draws the same seeds regardless of where it sits in the
    # candidate list across optimizer rounds (reproducible). ``env_key`` stays
    # globally unique only because it routes rows to env instances.
    env_key = 0
    for variant in unique_variants:
        for seed_slot in range(seeds_per_variant):
            seed = task_id * 1000 + seed_slot
            obs = env_client.reset(env_key, seed)
            components: list[Any] = [
                ManipProprio(
                    eef_pos=list(obs.get("eef_pos", [0.0, 0.0, 0.0])),
                    eef_quat=list(obs.get("eef_quat", [1.0, 0.0, 0.0, 0.0])),
                    gripper=float(obs.get("gripper", 0.0)),
                    gripper_qpos=list(obs.get("gripper_qpos", [0.0, 0.0])),
                ),
                ManipAction(values=[0.0] * ACTION_DIM),
                ManipStatus(),
                ManipTask(
                    suite=suite,
                    task_id=task_id,
                    instruction=variant,
                    seed=seed,
                    env_key=env_key,
                ),
            ]
            if with_frames:
                components.append(
                    ManipFrameRef(
                        agentview_ref=str(obs.get("agentview_ref", "")),
                        wrist_ref=str(obs.get("wrist_ref", "")),
                    )
                )
            await world.create_entity(components)
            env_key += 1

    episode = await simulation_service.run_episode(
        world.world_id,
        EpisodeConfig(
            max_steps=max_steps,
            terminal_component=ManipStatus,
            terminal_field="done",
            terminal_all=True,
        ),
    )

    df = await eval_service.query_components(
        [ManipStatus, ManipTask],
        world_id=world.world_id,
        run_id=episode.run_id,
        storage_config=storage,
    )
    final = _final_row_per_entity(df)

    # Group the latched final rows by instruction, preserving variant order.
    # Every persisted instruction was spawned from ``unique_variants``; a row
    # carrying anything else means entity/ledger corruption — fail loudly rather
    # than let it silently escape the per-variant trial count.
    rows_by_instruction: dict[str, list[dict]] = {v: [] for v in unique_variants}
    for row in final.values():
        instr = str(row.get("maniptask__instruction", ""))
        if instr not in rows_by_instruction:
            raise ValueError(
                f"sweep graded a row with an unspawned instruction {instr!r}; "
                f"expected one of {unique_variants}"
            )
        rows_by_instruction[instr].append(row)

    report = SweepReport(
        suite=suite,
        task_id=task_id,
        world_id=str(world.world_id),
        run_id=str(episode.run_id),
    )
    for instruction in unique_variants:
        rows = rows_by_instruction.get(instruction, [])
        n = len(rows)
        n_success = sum(1 for r in rows if bool(r.get("manipstatus__success", False)))
        lengths = [int(r.get("manipstatus__env_step", 0)) for r in rows]
        report.variants.append(
            VariantOutcome(
                instruction=instruction,
                n_trials=n,
                n_success=n_success,
                success_rate=(n_success / n) if n else 0.0,
                mean_length=(sum(lengths) / n) if n else 0.0,
            )
        )

    # Loud failure on a shrunk denominator: a missing final row would silently
    # drop a trial (graded 0/0), deflating success-rate. Every spawned trial must
    # be graded.
    graded = sum(v.n_trials for v in report.variants)
    expected = len(unique_variants) * seeds_per_variant
    if graded != expected:
        raise ValueError(
            f"sweep graded {graded} trials but spawned {expected}; "
            f"a trajectory is missing from the ledger (corruption or a dropped run)"
        )
    return report


# ---------------------------------------------------------------------------
# Perturbation strategy — how variants are proposed
# ---------------------------------------------------------------------------


class PerturbationStrategy(Protocol):
    """Proposes instruction variants near a base instruction.

    The CI strategy (:class:`TemplatePerturbation`) is deterministic and
    dependency-free. A real strategy can be an LLM doing GEPA-style reflective
    mutation; it only has to return candidate strings.
    """

    def propose(self, base: str, n: int) -> list[str]: ...


@dataclass
class TemplatePerturbation:
    """Deterministic single-occurrence (Hamming-1) toggle over a fixed vocabulary.

    ``propose(base, n)`` returns ``n`` variants, the i-th formed by toggling
    ``vocabulary[i % len]`` in ``base``: remove *one* occurrence if present, else
    append. Single-occurrence (not remove-all) so a repeated-word instruction
    isn't collapsed and no duplicate candidates are emitted. With
    ``n == len(vocabulary)`` this is the full 1-edit neighborhood, so hill-climbing
    from the incumbent reaches the optimum reachable by edits *within the
    vocabulary*, deterministically and reproducibly.

    NOTE on reachability: the search can only reach instructions expressible by
    these toggles. If ``vocabulary`` is exactly ``base.split()`` the optimum is
    ``base`` itself (deletions only) — to let an optimized instruction *beat* the
    base, the vocabulary must include candidate words not already in ``base``.
    For a real VLA experiment, supply a paraphrase/keyword-injection or LLM
    strategy (roadmap H5) rather than this token-toggle stand-in.
    """

    vocabulary: Sequence[str]

    def propose(self, base: str, n: int) -> list[str]:
        base_tokens = base.split()
        out: list[str] = []
        for i in range(n):
            word = self.vocabulary[i % len(self.vocabulary)]
            new_tokens = list(base_tokens)
            if word in new_tokens:
                new_tokens.remove(word)  # one occurrence (Hamming-1), not all
            else:
                new_tokens.append(word)
            out.append(" ".join(new_tokens))
        return out


# ---------------------------------------------------------------------------
# The self-improving loop
# ---------------------------------------------------------------------------


@dataclass
class RoundRecord:
    round: int
    best_instruction: str
    best_success_rate: float
    evaluated: int


@dataclass
class OptimizationResult:
    best_instruction: str
    best_success_rate: float
    trace: list[RoundRecord] = field(default_factory=list)


Evaluator = Callable[[list[str]], Awaitable[dict[str, float]]]


async def optimize_instruction(
    *,
    evaluate: Evaluator,
    base: str,
    strategy: PerturbationStrategy,
    rounds: int,
    neighbors: int,
    patience: int = 2,
) -> OptimizationResult:
    """Hill-climb the instruction to maximize graded success-rate.

    Each round proposes ``neighbors`` variants of the incumbent, evaluates them
    (plus the incumbent, so the search never regresses) in one batched call,
    and adopts the best. Stops at a perfect score, after ``rounds`` rounds, or
    after ``patience`` rounds with no improvement.

    ``evaluate`` maps a list of instructions to ``{instruction: success_rate}``.
    The default caller closes it over :func:`run_instruction_sweep` (ground
    truth). It is the single seam where a VLA-JEPA latent scorer would slot in
    for a cheap inner loop.
    """
    scores = await evaluate([base])
    best_instruction = base
    best_success_rate = scores.get(base, 0.0)
    trace = [RoundRecord(0, best_instruction, best_success_rate, 1)]
    stale = 0

    for r in range(1, rounds + 1):
        if best_success_rate >= 1.0:
            break
        candidates = _dedup([best_instruction, *strategy.propose(best_instruction, neighbors)])
        scores = await evaluate(candidates)
        ranked = sorted(
            candidates,
            key=lambda c: (-scores.get(c, 0.0), len(c), c),
        )
        top = ranked[0]
        top_sr = scores.get(top, 0.0)
        improved = top_sr > best_success_rate or (
            top_sr == best_success_rate and len(top) < len(best_instruction)
        )
        if improved:
            best_instruction, best_success_rate = top, top_sr
            stale = 0
        else:
            stale += 1
        trace.append(RoundRecord(r, best_instruction, best_success_rate, len(candidates)))
        if stale >= patience:
            break

    return OptimizationResult(
        best_instruction=best_instruction,
        best_success_rate=best_success_rate,
        trace=trace,
    )
