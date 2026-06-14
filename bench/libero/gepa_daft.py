# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""GEPA as a Daft pipeline — reflective instruction evolution over the ledger.

No gepa package, no nested Modal functions, no LiteLLM. The whole optimization is
DataFrame ops:

    rewrite   = daft.functions.prompt(...)        # LLM-in-expression (Claude via OAI-compat)
    evaluate  = RolloutEvaluator @daft.cls        # async, use_process → isolated VLA-JEPA rollout
    reflect   = daft.functions.prompt(..., return_format=Strategy)   # LLM-in-expression
    select    = plain DataFrame / Python at the generation boundary

The ONLY .remote() calls are to the *deployed* env/policy workers
(archetype-libero-plus-env, archetype-vla-jepa) — the proven baseline_sweep path.
The whole thing runs inside ONE Modal function (run, --detach friendly); the Daft
executor provides intra-container parallelism. Every (strategy, task) cell is
written to the canonical ledger as Lance, so the optimization is queryable +
reproducible.

Arms A/D/B-seed/C are gen-0 with a fixed strategy (A = identity, no rewrite); GEPA
is the same `evaluate()` in a reflect→select loop. `pass` = sim ground truth.

Run (after archetype-libero-plus-env + archetype-vla-jepa are deployed):
    modal run --detach bench/libero/gepa_daft.py --suite libero_goal \\
        --n-tasks 8 --n-seeds 5 --max-steps 300 --generations 3 --beam-k 3
"""

import os
from typing import Any

import modal

os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")

ROOT = "/repo"
RESULTS_DIR = "/results"
LEDGER = f"{RESULTS_DIR}/gepa_daft"

# Claude via Anthropic's OpenAI-compatible endpoint (we already have the key as a
# Modal secret). Set as the Daft session provider inside `run`.
ANTHROPIC_OAI_BASE = "https://api.anthropic.com/v1/"
REWRITE_MODEL = "claude-sonnet-4-6"   # apply a strategy to one instruction (cheap)
REFLECT_MODEL = "claude-opus-4-8"     # propose improved strategies (reasoning-heavy)

# The seed rewrite strategy GEPA evolves; also arm "B-seed" (one-shot, un-evolved).
SEED_STRATEGY = (
    "You rewrite a robot-manipulation instruction into a clearer, better-grounded "
    "instruction for a frozen vision-language-action policy. Name the target object "
    "by its visual appearance and spatial relation to nearby objects; state the goal "
    "location explicitly. Keep it ONE imperative sentence. Do NOT add information that "
    "solves the task (no coordinates, no answer) — only re-ground what is already there."
)
# Fixed-arm strategies. None = identity (use the perturbed instruction verbatim).
NEUTRAL_PARAPHRASE = (
    "Paraphrase the instruction in different words WITHOUT adding any grounding, object "
    "descriptions, or spatial detail. Keep the same information content. One sentence."
)
ORACLE_SMUGGLE = (
    "Rewrite the instruction and explicitly NAME the exact correct target object and "
    "destination so the policy cannot pick the wrong one. One imperative sentence."
)
ARMS: dict[str, str | None] = {
    "A-default": None,            # perturbed instruction as-is (the ~85% baseline)
    "D-paraphrase": NEUTRAL_PARAPHRASE,
    "B-seed": SEED_STRATEGY,
    "C-oracle": ORACLE_SMUGGLE,
}

# Lean image: archetype + daft[openai] (the prompt() provider) + pydantic.
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("uv")
    .add_local_dir(
        ".",
        ROOT,
        copy=True,
        ignore=[".git", "**/__pycache__", ".claude", "target", "*.mp4", ".venv",
                "**/*.parquet", "**/*.log", "**/out/**"],
    )
    .run_commands(
        # archetype already pulls daft[openai,lance,iceberg]>=0.7.4 (0.7.9 has
        # prompt() + the OpenAI provider). Do NOT force a daft upgrade — it would
        # drift from archetype's pinned daft. Just add the LLM-output validator.
        f"cd {ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy pydantic"
    )
)

app = modal.App("archetype-gepa-daft", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)
ANTHROPIC_SECRET = modal.Secret.from_name("anthropic-api-key")


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=21600, secrets=[ANTHROPIC_SECRET])
def run(
    suite: str = "libero_goal",
    n_tasks: int = 8,
    n_seeds: int = 5,
    max_steps: int = 300,
    budget: int = 60,        # GEPA rollout budget, counted in (strategy×task) metric calls
    minibatch: int = 3,      # GEPA minibatch size b for the mutate→accept gate
    do_gepa: bool = True,
) -> dict[str, Any]:
    """Run the fixed-arm ablation (A/D/B-seed/C) + the GEPA loop, all as Daft."""
    import sys

    sys.path.insert(0, f"{ROOT}/bench/libero")

    import daft
    from daft import col, lit
    from daft.functions import format as dfmt
    from daft.functions import prompt, unnest, when
    from pydantic import BaseModel, Field

    # ---- Claude as the Daft prompt() provider (OpenAI-compatible) -------------
    daft.set_provider(
        "openai", api_key=os.environ["ANTHROPIC_API_KEY"], base_url=ANTHROPIC_OAI_BASE
    )

    RES_DTYPE = daft.DataType.struct(
        {
            "success_rate": daft.DataType.float64(),
            "successes": daft.DataType.int64(),
            "n": daft.DataType.int64(),
            "feedback": daft.DataType.string(),
        }
    )

    # ---- The evaluator: one isolated process per concurrent rollout ----------
    # use_process=True → each instance has its OWN global RBAC tick-counter, so
    # concurrent rollouts can't race on it. async → Daft runs up to
    # max_concurrency rollouts at once; each calls the DEPLOYED workers.
    @daft.cls(max_concurrency=min(n_tasks, 8), use_process=True)
    class RolloutEvaluator:
        def __init__(self, max_steps: int, n_seeds: int):
            self.max_steps = max_steps
            self.n_seeds = n_seeds
            self._envs: dict[int, Any] = {}
            self._pols: dict[int, Any] = {}

        @daft.method(return_dtype=RES_DTYPE)
        async def eval(self, suite: str, task_id: int, instruction: str) -> dict[str, Any]:
            import sys as _sys
            import tempfile

            _sys.path.insert(0, f"{ROOT}/bench/libero")
            from archetype import ArchetypeRuntime
            from archetype.core.config import StorageConfig
            from eval_driver import (
                _make_libero_plus_env_client,
                _make_vla_policy_client,
                run_episode_batched,
            )

            env = self._envs.get(task_id)
            if env is None:
                env = _make_libero_plus_env_client(suite, task_id, with_frames=True)
                self._envs[task_id] = env
            pol = self._pols.get(task_id)
            if pol is None:
                pol = _make_vla_policy_client(suite, task_id)
                self._pols[task_id] = pol

            seeds = [task_id * 1000 + i for i in range(self.n_seeds)]
            env_keys = list(range(self.n_seeds))
            store = StorageConfig(uri=tempfile.mkdtemp(prefix="gepa-"), namespace="gepa_eval")
            try:
                async with ArchetypeRuntime() as rt:
                    per_trial = await run_episode_batched(
                        env_client=env,
                        policy_client=pol,
                        suite=suite,
                        task_id=task_id,
                        seeds=seeds,
                        env_keys=env_keys,
                        max_steps=self.max_steps,
                        storage=store,
                        runtime=rt,
                        use_frames=True,
                        instruction=instruction,
                    )
            except Exception as exc:  # noqa: BLE001 — one cell failing must not kill the plan
                return {"success_rate": 0.0, "successes": 0, "n": 0, "feedback": f"harness error: {exc}"}
            return _summarize(per_trial, self.max_steps)

    # ---- helpers --------------------------------------------------------------
    def evaluate(strategies: "daft.DataFrame", tasks: "daft.DataFrame" = None) -> "daft.DataFrame":
        """strategies: [strategy_id, strategy_text(nullable)] × tasks → per-cell scores+feedback."""
        cells = strategies.join(tasks if tasks is not None else tasks_df, how="cross")
        # rewrite: identity when strategy_text is null, else apply the strategy via Claude.
        cells = cells.with_column(
            "grounded",
            when(col("strategy_text").is_null(), col("perturbed_instr")).otherwise(
                prompt(
                    messages=dfmt(
                        "{}\n\nOriginal instruction:\n{}\n\n"
                        "Rewritten instruction (one imperative sentence):",
                        col("strategy_text"),
                        col("perturbed_instr"),
                    ),
                    model=REWRITE_MODEL,
                    use_chat_completions=True,
                    max_tokens=200,
                )
            ),
        )
        cells = cells.with_column("res", evaluator.eval(col("suite"), col("task_id"), col("grounded")))
        return cells.select(
            "strategy_id",
            "task_id",
            "grounded",
            col("res")["success_rate"].alias("success"),
            col("res")["feedback"].alias("feedback"),
        )

    # ---- tasks: pull each task's PERTURBED instruction from the deployed env --
    env_cls = modal.Cls.from_name("archetype-libero-plus-env", "LiberoPlusEnvBatch").with_options(
        max_containers=1
    )
    task_rows = [
        {
            "suite": suite,
            "task_id": t,
            "perturbed_instr": str(env_cls(suite=suite, task_id=t).task_language.remote()),
        }
        for t in range(n_tasks)
    ]
    tasks_df = daft.from_pylist(task_rows)
    evaluator = RolloutEvaluator(max_steps, n_seeds)

    results_volume.reload()

    # ---- Tier 1: fixed-arm ablation (A < D < B-seed < C is the legitimacy test) ----
    arms_df = daft.from_pylist([{"strategy_id": k, "strategy_text": v} for k, v in ARMS.items()])
    arm_cells = evaluate(arms_df).collect()
    arm_cells.write_lance(f"{LEDGER}/arms", mode="append")
    arm_summary = (
        arm_cells.groupby("strategy_id")
        .agg(col("success").mean().alias("mean_success"), col("success").count().alias("n"))
        .sort("strategy_id")
        .to_pylist()
    )

    # ---- Tier 2: GEPA — algorithm in archetype.optimize.gepa; effects injected here ----
    # The lib owns faithful Algorithm 1 + Pareto Algorithm 2 (no Daft/Modal dep,
    # unit-tested in tests/experiments/test_gepa_daft.py). run() only supplies the two
    # EFFECTS: eval_fn (prompt-rewrite + sim rollout) and reflect_fn (Claude UpdatePrompt).
    best: dict[str, Any] | None = None
    if do_gepa:
        import random

        from archetype.optimize.gepa import gepa_search

        class Strategy(BaseModel):
            strategy_text: str = Field(description="an improved instruction-rewrite strategy")

        def eval_fn(sid: str, text: str, ids: list[int]) -> dict[int, dict[str, Any]]:
            """Effect: rewrite (prompt) + sim rollout for one strategy on a task subset; ledgered."""
            sub = daft.from_pylist([r for r in task_rows if r["task_id"] in ids])
            cells = evaluate(daft.from_pylist([{"strategy_id": sid, "strategy_text": text}]), sub).collect()
            cells.write_lance(f"{LEDGER}/gepa", mode="append")
            return {r["task_id"]: {"s": r["success"], "fb": r["feedback"]} for r in cells.to_pylist()}

        def reflect_fn(text: str, feedback_block: str) -> str:
            """Effect: GEPA UpdatePrompt via Claude (prompt, structured)."""
            rdf = daft.from_pylist([{"p": (
                f"Current rewrite strategy:\n{text}\n\nPer-instance sim outcomes:\n{feedback_block}\n\n"
                "Propose an improved strategy that fixes the failure modes above. "
                "Stay answer-blind (never name the correct target/answer)."
            )}])
            out = (
                rdf.with_column("out", prompt(
                    messages=col("p"),
                    system_message="You improve instruction-rewrite strategies for a frozen VLA. Output only the improved strategy.",
                    model=REFLECT_MODEL, use_chat_completions=True, return_format=Strategy))
                .select(unnest(col("out"))).to_pylist()
            )
            return out[0]["strategy_text"]

        best = gepa_search(
            seed_text=SEED_STRATEGY,
            instance_ids=[r["task_id"] for r in task_rows],
            eval_fn=eval_fn,
            reflect_fn=reflect_fn,
            budget=budget,
            minibatch=minibatch,
            rng=random.Random(0),
        )

    results_volume.commit()
    return {"arms": arm_summary, "best": best}


def _summarize(per_trial: list[tuple[bool, int]], max_steps: int) -> dict[str, Any]:
    """(success, length) per seed → success_rate + reflective feedback text."""
    n = len(per_trial)
    successes = sum(1 for s, _ in per_trial if s)
    timeouts = sum(1 for s, ln in per_trial if not s and ln >= max_steps - 1)
    wrong = sum(1 for s, ln in per_trial if not s and ln < max_steps - 1)
    parts = [f"{successes}/{n} succeeded."]
    if timeouts:
        parts.append(f"{timeouts} timed out (target likely never grasped — instruction ambiguous about which object/order).")
    if wrong:
        parts.append(f"{wrong} finished but failed the goal (likely wrong object/destination — instruction underspecifies the target).")
    if successes == n and n:
        parts.append("All succeeded; preserve what worked.")
    return {
        "success_rate": (successes / n) if n else 0.0,
        "successes": successes,
        "n": n,
        "feedback": " ".join(parts),
    }


@app.local_entrypoint()
def main(
    suite: str = "libero_goal",
    n_tasks: int = 8,
    n_seeds: int = 5,
    max_steps: int = 300,
    budget: int = 60,
    minibatch: int = 3,
    do_gepa: bool = True,
):
    import json

    out = run.remote(
        suite=suite,
        n_tasks=n_tasks,
        n_seeds=n_seeds,
        max_steps=max_steps,
        budget=budget,
        minibatch=minibatch,
        do_gepa=do_gepa,
    )
    print(json.dumps(out, indent=2))
