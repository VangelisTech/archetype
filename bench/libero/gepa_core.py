# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The GEPA algorithm — pure, effect-injected, testable in isolation.

This is a faithful, single-module implementation of GEPA (Agrawal et al. 2025,
arXiv:2507.19457): Algorithm 1 (reflective evolution loop) + Algorithm 2
(Pareto-based candidate selection). It has NO Daft/Modal/LLM/sim dependency — the
two effects are injected:

    eval_fn(sid, text, instance_ids)  -> {instance_id: {"s": float, "fb": str}}
        run the candidate strategy on those instances; return per-instance score + feedback.
    reflect_fn(text, feedback_block)  -> str
        GEPA's UpdatePrompt: reflect on minibatch feedback → an improved strategy.

`gepa_daft.py` injects Daft-backed effects (prompt() rewrite + sim rollout);
`tests/experiments/test_gepa_daft.py` injects deterministic mocks to prove the
control logic matches GEPA. Single MODULE: we evolve one prompt component, so
GEPA's per-module round-robin and system-aware merge are no-ops; the parts that
*define* GEPA — per-instance score vectors, the Pareto frontier, the
minibatch→accept gate, and the rollout budget — are all here.

GEPA Alg-line references are in the code.
"""

from __future__ import annotations

from random import Random
from typing import Any, Callable, Protocol


class _Rng(Protocol):
    def sample(self, population: list[int], k: int) -> list[int]: ...
    def choices(self, population: list[Any], weights: list[float], k: int) -> list[Any]: ...


EvalFn = Callable[[str, str, list[int]], dict[int, dict[str, Any]]]
ReflectFn = Callable[[str, str], str]


def pareto_select(pool: list[dict[str, Any]], instance_ids: list[int], rng: _Rng) -> dict[str, Any]:
    """GEPA Algorithm 2 — instance-wise Pareto candidate selection.

    NOT top-k-by-mean (beam). Steps, verbatim from the paper:
      1. per-instance best:   s*[i] = max_k S[k][i]
      2. per-instance winners: P*[i] = {k : S[k][i] == s*[i]}
      3. prune candidates strictly dominated across instances
      4. sample ∝ f[Φ] = #instances for which Φ ∈ P*[i]

    A specialist that wins one instance survives even if its mean is low — which is
    exactly the diversity beam search throws away.
    """
    star = {i: max(c["scores"][i] for c in pool) for i in instance_ids}
    wins = {c["sid"]: sum(1 for i in instance_ids if c["scores"][i] == star[i]) for c in pool}
    frontier = [c for c in pool if wins[c["sid"]] > 0]
    nondominated = [
        c
        for c in frontier
        if not any(
            d is not c
            and all(d["scores"][i] >= c["scores"][i] for i in instance_ids)
            and any(d["scores"][i] > c["scores"][i] for i in instance_ids)
            for d in frontier
        )
    ]
    return rng.choices(nondominated, weights=[wins[c["sid"]] for c in nondominated], k=1)[0]


def _candidate(sid: str, text: str, res: dict[int, dict[str, Any]], instance_ids: list[int]) -> dict[str, Any]:
    return {
        "sid": sid,
        "text": text,
        "scores": {i: res[i]["s"] for i in instance_ids},  # per-instance score VECTOR (Pareto needs this)
        "fbk": {i: res[i]["fb"] for i in instance_ids},
    }


def gepa_search(
    *,
    seed_text: str,
    instance_ids: list[int],
    eval_fn: EvalFn,
    reflect_fn: ReflectFn,
    budget: int,
    minibatch: int,
    rng: _Rng | None = None,
) -> dict[str, Any]:
    """GEPA Algorithm 1 — reflective evolution with Pareto selection.

    Budget is counted in metric calls (one candidate × one instance = one call).
    Returns the best candidate on the full instance set + the search trace.
    """
    rng = rng or Random(0)

    # Alg 1 line 7 + 18-20: P ← [base]; full-eval the seed on all instances (D_pareto).
    pool = [_candidate("gepa-seed", seed_text, eval_fn("gepa-seed", seed_text, instance_ids), instance_ids)]
    spent = len(instance_ids)
    trace: list[dict[str, Any]] = []
    gen = 0

    while spent + minibatch <= budget:  # Alg 1 line 8: while budget B not exhausted
        gen += 1
        parent = pareto_select(pool, instance_ids, rng)                      # Alg 2
        mb = rng.sample(instance_ids, min(minibatch, len(instance_ids)))     # minibatch from D_feedback
        sigma = sum(parent["scores"][i] for i in mb) / len(mb)              # σ (cached parent vector/feedback)
        fb = "\n".join(f"- instance {i}: {parent['fbk'][i]}" for i in mb)
        child_text = reflect_fn(parent["text"], fb)                         # Alg 1 line 13: UpdatePrompt
        csid = f"gepa-g{gen}"
        c_res = eval_fn(f"{csid}-mb", child_text, mb)                       # evaluate child on minibatch
        spent += len(mb)
        sigma_prime = sum(c_res[i]["s"] for i in mb) / len(mb)            # σ'
        accepted = sigma_prime > sigma                                      # Alg 1 line 16: acceptance gate
        trace.append(
            {"gen": gen, "parent": parent["sid"], "sigma": round(sigma, 4),
             "sigma_prime": round(sigma_prime, 4), "accepted": accepted, "spent": spent}
        )
        if accepted and spent + len(instance_ids) <= budget:
            full = eval_fn(csid, child_text, instance_ids)                  # Alg 1 line 18-20: full-eval D_pareto
            spent += len(instance_ids)
            pool.append(_candidate(csid, child_text, full, instance_ids))

    best = max(pool, key=lambda c: sum(c["scores"].values()) / len(c["scores"]))  # best on D_pareto
    return {
        "strategy_id": best["sid"],
        "strategy_text": best["text"],
        "mean_success": sum(best["scores"].values()) / len(best["scores"]),
        "pool_size": len(pool),
        "metric_calls": spent,
        "trace": trace,
    }
