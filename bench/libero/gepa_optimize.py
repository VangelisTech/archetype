# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Genuine GEPA optimization of a frozen VLA's instruction — sim-truth reflective.

We evolve a SINGLE global instruction-rewrite strategy (a natural-language prompt)
that maps each task's default/perturbed instruction to a better-grounded one, using
the OFFICIAL GEPA optimizer (Agrawal et al. 2025, ``pip install gepa``) wired to our
batched LIBERO rollout harness as the evaluation function. No policy weights change:
GEPA only edits the instruction text fed to the frozen VLA-JEPA policy.

Architecture (the contribution: a reproducible, sim-grounded GEPA loop)::

    gepa.optimize(seed, trainset, valset, adapter=SimRolloutAdapter, reflection_lm=...)
        │  for each candidate strategy:
        ▼
    SimRolloutAdapter.evaluate(tasks, candidate)
        │  rewrite_lm applies the evolved strategy → grounded instruction
        ▼
    eval_instruction.remote(suite, task_id, instruction, seeds)   ← Modal, batched
        │  run_episode_batched: N seeds = N entities, one env.step/tick
        ▼  sim-truth success (env.check_success) + per-rollout failure traces
    EvaluationBatch(scores=success_rate, trajectories=feedback)
        │  make_reflective_dataset → {"Feedback": ledger failure text}
        ▼
    reflection_lm reflects on the failures → mutates the strategy text

``pass`` is ALWAYS the simulator's ``check_success`` — the LLM is used only to apply
the strategy (rewrite_lm) and to reflect/mutate (GEPA's reflection_lm), never to judge.

The A/B/C/D arms (run via ``--arm``):
    A = default instruction (baseline)           D = neutral length-matched paraphrase (floor)
    B = GEPA-evolved answer-blind rewrite        C = answer-smuggling oracle (ceiling)
Legitimate gap requires A < D < B < C on a question-disjoint split.

Run (after the env worker + ANTHROPIC secret are up)::

    modal run bench/libero/gepa_optimize.py --suite libero_plus_language \\
        --n-tasks 8 --n-seeds 5 --max-steps 300 --budget 120
"""

from __future__ import annotations

import os
from typing import Any

import modal

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

from colocated_runner import image as _base_image  # noqa: E402

# GEPA + its reflection LM client (litellm) live in the loop container.
image = _base_image.pip_install("gepa>=0.1.1", "litellm>=1.64.0")

app = modal.App("archetype-gepa-optimize", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)
RESULTS_DIR = "/results"
CANONICAL_NS = "gepa_optimize"

# The reflection/rewrite LM credential. Verify the secret exists:
#   modal secret list | grep anthropic
ANTHROPIC_SECRET = modal.Secret.from_name("anthropic-api-key")

REWRITE_LM = "anthropic/claude-sonnet-4-6"
REFLECTION_LM = "anthropic/claude-opus-4-8"

COMPONENT = "instruction_rewrite_strategy"

SEED_STRATEGY = (
    "You rewrite a robot-manipulation instruction into a clearer, better-grounded "
    "instruction for a frozen vision-language-action policy. Identify the target "
    "object by its visual appearance and spatial relations to nearby objects; state "
    "the goal location explicitly and unambiguously; keep the result a single "
    "imperative sentence. Do NOT add information that solves the task for the policy "
    "(no coordinates, no step counts) — only re-ground what the original already says."
)


# ---------------------------------------------------------------------------
# Sim evaluation — one (task, instruction) -> sim-truth success + failure traces
# ---------------------------------------------------------------------------
@app.function(
    volumes={RESULTS_DIR: results_volume}, timeout=3600, secrets=[ANTHROPIC_SECRET]
)
def eval_instruction(
    suite: str,
    task_id: int,
    instruction: str,
    seeds: list[int],
    max_steps: int,
    run_name: str,
) -> dict[str, Any]:
    """Run one batched rollout for a given instruction; return success + feedback.

    The instruction is whatever GEPA's current strategy produced (or a fixed-arm
    string). Success is the simulator's own ``check_success`` read off the ledger;
    the per-rollout failure traces are the reflective signal GEPA learns from.
    """
    import asyncio

    async def _run() -> dict[str, Any]:
        import sys

        sys.path.insert(0, "/repo/bench/libero")  # so eval_driver is importable remotely

        from archetype import ArchetypeRuntime
        from archetype.core.config import StorageConfig
        from eval_driver import (  # proven batched primitive + client factories
            _make_libero_plus_env_client,
            _make_vla_policy_client,
            run_episode_batched,
        )

        # LIBERO-Plus LANGUAGE env (perturbed instruction on the same scene) +
        # the frozen VLA-JEPA policy — same factories the proven sweep uses, so
        # GEPA's eval is identical to the baseline path except for the instruction.
        env = _make_libero_plus_env_client(suite, task_id, with_frames=True)
        pol = _make_vla_policy_client(suite, task_id)
        store = StorageConfig(uri=f"{RESULTS_DIR}/{CANONICAL_NS}", namespace=CANONICAL_NS)
        n = len(seeds)
        env_keys = list(range(n))

        async with ArchetypeRuntime() as rt:
            per_trial = await run_episode_batched(
                env_client=env,
                policy_client=pol,
                suite=suite,
                task_id=task_id,
                seeds=seeds,
                env_keys=env_keys,
                max_steps=max_steps,
                storage=store,
                runtime=rt,
                use_frames=True,
                instruction=instruction,
            )
        return _summarize(per_trial, max_steps)

    results_volume.reload()
    out = asyncio.run(_run())
    results_volume.commit()
    out.update({"suite": suite, "task_id": task_id, "run_name": run_name})
    return out


def _summarize(per_trial: list[tuple[bool, int]], max_steps: int) -> dict[str, Any]:
    """Aggregate (success, length) per seed into success_rate + textual feedback.

    Feedback quality is the whole game for GEPA: we distinguish *timeout* (never
    finished — target likely never grasped/placed; instruction may be ambiguous
    about the object or the order) from *wrong-outcome* (motion completed but the
    goal predicate failed — likely the wrong object or wrong destination)."""
    n = len(per_trial) or 1
    successes = sum(1 for s, _ in per_trial if s)
    timeouts = sum(1 for s, ln in per_trial if not s and ln >= max_steps - 1)
    wrong = sum(1 for s, ln in per_trial if not s and ln < max_steps - 1)
    lines = [f"{successes}/{len(per_trial)} rollouts succeeded."]
    if timeouts:
        lines.append(
            f"{timeouts} timed out at step {max_steps} without completing the task "
            "(the policy likely never grasped/placed the target — the instruction may "
            "be ambiguous about which object or in what order)."
        )
    if wrong:
        lines.append(
            f"{wrong} finished the motion but failed the goal check "
            "(likely the wrong object or the wrong destination — the instruction may "
            "not disambiguate the target clearly enough)."
        )
    if successes == len(per_trial):
        lines.append("All rollouts succeeded; preserve what worked in the rewrite.")
    return {
        "success_rate": successes / n,
        "successes": successes,
        "n": len(per_trial),
        "feedback": " ".join(lines),
    }


# ---------------------------------------------------------------------------
# GEPA adapter — our sim rollout IS the evaluator
# ---------------------------------------------------------------------------
def _build_adapter() -> Any:
    from dataclasses import dataclass
    from typing import Mapping, Sequence

    import litellm
    from gepa.core.adapter import EvaluationBatch, GEPAAdapter

    @dataclass
    class LiberoTask:
        suite: str
        task_id: int
        default_instruction: str
        seeds: list[int]
        max_steps: int

    def rewrite(strategy: str, task_instruction: str) -> str:
        """Apply the (GEPA-evolved) strategy to one task's instruction."""
        resp = litellm.completion(
            model=REWRITE_LM,
            messages=[
                {"role": "system", "content": strategy},
                {"role": "user", "content": f"Original instruction:\n{task_instruction}\n\nRewritten instruction:"},
            ],
            max_tokens=120,
            temperature=0.0,
        )
        return resp["choices"][0]["message"]["content"].strip()

    class SimRolloutAdapter(GEPAAdapter):
        """Evolve ONE global instruction-rewrite prompt against VLA-JEPA sim success."""

        def evaluate(self, batch, candidate, capture_traces=False):  # noqa: ANN001
            strategy = candidate[COMPONENT]
            outputs: list[str] = []
            scores: list[float] = []
            trajs: list[dict[str, Any]] = []
            for task in batch:
                try:
                    grounded = rewrite(strategy, task.default_instruction)
                    res = eval_instruction.remote(
                        task.suite, task.task_id, grounded, task.seeds, task.max_steps, "gepa"
                    )
                    sr, fb = float(res["success_rate"]), str(res["feedback"])
                except Exception as exc:  # never raise per example
                    grounded, sr = task.default_instruction, 0.0
                    fb = f"harness error: {exc}"
                outputs.append(grounded)
                scores.append(sr)
                if capture_traces:
                    trajs.append(
                        {
                            "Inputs": task.default_instruction,
                            "Generated Outputs": grounded,
                            "Feedback": f"Sim success {sr:.2f}. {fb}",
                        }
                    )
            return EvaluationBatch(
                outputs=outputs, scores=scores, trajectories=trajs if capture_traces else None
            )

        def make_reflective_dataset(
            self, candidate, eval_batch, components_to_update
        ) -> Mapping[str, Sequence[Mapping[str, Any]]]:
            return {COMPONENT: list(eval_batch.trajectories or [])}

    return SimRolloutAdapter(), LiberoTask


# ---------------------------------------------------------------------------
# Driver — run the real GEPA loop in-region
# ---------------------------------------------------------------------------
@app.function(volumes={RESULTS_DIR: results_volume}, timeout=21600, secrets=[ANTHROPIC_SECRET])
def optimize_remote(
    suite: str,
    task_ids: list[int],
    instruction_for: dict[int, str],
    n_seeds: int,
    max_steps: int,
    budget: int,
    val_frac: float,
) -> dict[str, Any]:
    """Run gepa.optimize with the sim-rollout adapter; return the evolved strategy."""
    import gepa

    adapter, LiberoTask = _build_adapter()
    tasks = [
        LiberoTask(
            suite=suite,
            task_id=t,
            default_instruction=instruction_for[t],
            seeds=list(range(t * 1000, t * 1000 + n_seeds)),
            max_steps=max_steps,
        )
        for t in task_ids
    ]
    # Question-disjoint split: hold out the back of the task list for val/test.
    cut = max(1, int(len(tasks) * (1.0 - val_frac)))
    trainset, valset = tasks[:cut], tasks[cut:] or tasks[:1]

    result = gepa.optimize(
        seed_candidate={COMPONENT: SEED_STRATEGY},
        trainset=trainset,
        valset=valset,
        adapter=adapter,
        reflection_lm=REFLECTION_LM,
        max_metric_calls=budget,
        reflection_minibatch_size=3,
        candidate_selection_strategy="pareto",
        display_progress_bar=False,
    )
    return {
        "best_strategy": result.best_candidate[COMPONENT],
        "seed_strategy": SEED_STRATEGY,
        "n_train": len(trainset),
        "n_val": len(valset),
        "budget": budget,
    }


@app.local_entrypoint()
def main(
    suite: str = "libero_spatial",  # base suite; LIBERO-Plus LANGUAGE variants via the libero_plus env
    n_tasks: int = 8,
    n_seeds: int = 5,
    max_steps: int = 300,
    budget: int = 120,
    val_frac: float = 0.3,
):
    """Optimize the instruction-rewrite strategy on the headroom suite.

    Pulls each task's default (perturbed) instruction from the env worker, runs the
    real GEPA loop, and prints the evolved strategy. Arm A/B/C/D evaluation of the
    frozen strategy is a follow-up pass (eval_instruction with fixed arm strings).
    """
    import json

    # Pull each language ordinal's PERTURBED instruction from the LIBERO-Plus env
    # (task_language() returns the perturbed string — the honest baseline GEPA repairs).
    task_ids = list(range(n_tasks))
    env_cls = modal.Cls.from_name("archetype-libero-plus-env", "LiberoPlusEnvBatch")
    instruction_for = {
        t: env_cls(suite=suite, task_id=t).task_language.remote() for t in task_ids
    }
    out = optimize_remote.remote(
        suite, task_ids, instruction_for, n_seeds, max_steps, budget, val_frac
    )
    print(json.dumps(out, indent=2))
