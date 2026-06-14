# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Demonstrates that bench/libero/gepa_core.py IS the GEPA algorithm.

These tests inject deterministic mocks for the two effects (eval_fn, reflect_fn)
so they exercise the pure control logic — no Daft, Modal, LLM, or sim. They assert
the behaviors that *define* GEPA (Agrawal et al. 2025) and distinguish it from
naive beam / top-k-by-mean prompt evolution:

  - Pareto frontier keeps a SPECIALIST a top-k-by-mean beam would cull (Alg 2).
  - Strictly-dominated candidates are pruned (Alg 2).
  - Candidates are sampled ∝ number of instances won (Alg 2).
  - The minibatch acceptance gate (σ'>σ) admits only real improvements (Alg 1).
  - The rollout budget is respected (Alg 1, line 8).
"""

import sys
from collections import Counter
from pathlib import Path
from random import Random

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bench" / "libero"))

from gepa_core import gepa_search, pareto_select  # noqa: E402


def test_pareto_keeps_specialist_a_beam_would_cull():
    # generalist mean 0.60; specialist mean 0.33 — top-1-by-mean would drop the
    # specialist. GEPA keeps it because it wins instance 0. THIS is the distinction.
    pool = [
        {"sid": "generalist", "scores": {0: 0.6, 1: 0.6, 2: 0.6}},
        {"sid": "specialist", "scores": {0: 1.0, 1: 0.0, 2: 0.0}},
    ]
    rng = Random(0)
    picks = {pareto_select(pool, [0, 1, 2], rng)["sid"] for _ in range(200)}
    assert picks == {"generalist", "specialist"}  # both on the frontier; specialist survives


def test_pareto_prunes_strictly_dominated():
    pool = [
        {"sid": "strong", "scores": {0: 0.9, 1: 0.9}},
        {"sid": "weak", "scores": {0: 0.5, 1: 0.5}},  # dominated by strong on every instance
        {"sid": "spec", "scores": {0: 1.0, 1: 0.0}},  # wins instance 0
    ]
    rng = Random(1)
    picks = {pareto_select(pool, [0, 1], rng)["sid"] for _ in range(200)}
    assert "weak" not in picks
    assert picks == {"strong", "spec"}


def test_pareto_samples_proportional_to_instances_won():
    pool = [
        {"sid": "A", "scores": {0: 1.0, 1: 1.0, 2: 0.0}},  # wins 2 instances
        {"sid": "B", "scores": {0: 0.0, 1: 0.0, 2: 1.0}},  # wins 1 instance
    ]
    rng = Random(2)
    c = Counter(pareto_select(pool, [0, 1, 2], rng)["sid"] for _ in range(4000))
    assert 1.6 < c["A"] / c["B"] < 2.5  # ∝ #wins (2:1)


# A mock "world": a strategy's per-instance success = fraction of target keywords it
# contains. reflect_fn adds one missing keyword per round → improvable children.
_KEYWORDS = ["ground", "spatial", "explicit"]


def _mock_eval(sid, text, ids):
    score = sum(k in text for k in _KEYWORDS) / len(_KEYWORDS)
    miss = [k for k in _KEYWORDS if k not in text]
    return {i: {"s": score, "fb": f"missing: {miss}"} for i in ids}


def _mock_reflect(text, _fb):
    miss = [k for k in _KEYWORDS if k not in text]
    return text + ((" " + miss[0]) if miss else "")


def test_gepa_search_improves_over_seed_and_respects_budget():
    out = gepa_search(
        seed_text="rewrite", instance_ids=[0, 1, 2, 3],
        eval_fn=_mock_eval, reflect_fn=_mock_reflect, budget=40, minibatch=2, rng=Random(0),
    )
    assert out["mean_success"] >= 0.66          # climbed well past the 0.0 seed
    assert out["metric_calls"] <= 40            # budget respected
    assert out["pool_size"] > 1                 # accepted real improvements
    assert any(t["accepted"] for t in out["trace"])


def test_acceptance_gate_rejects_non_improving_mutations():
    # A no-op reflect never improves σ → the gate admits nothing; pool stays the seed.
    out = gepa_search(
        seed_text="rewrite", instance_ids=[0, 1, 2, 3],
        eval_fn=_mock_eval, reflect_fn=lambda t, _fb: t, budget=40, minibatch=2, rng=Random(0),
    )
    assert out["pool_size"] == 1
    assert all(not t["accepted"] for t in out["trace"])
