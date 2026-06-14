# Handoff: couple the daft-GEPA demo with GEPA's candidate-tree visualizer

**Goal.** Add a `--visualize` flag to the daft-GEPA demo (`prompt_gepa.py`) that, after a
run, renders GEPA's official **candidate-tree HTML** for *our* run and opens it in a
browser. Works for both the live and the offline-mock run (the mock produces a real
candidate tree). It's a fun, legible payoff: you watch the seed prompt fan out into a
Pareto frontier of children, best in cyan, frontier in orange, full prompt text on hover.

**Why this is an offload (not a one-liner).** It needs four things: (1) surface the
candidate-tree data the optimizer already computes internally, (2) a render + browser-launch
path, (3) a vendor-vs-depend decision with attribution, (4) verification the HTML actually
renders from a run. ~1–2 hours, self-contained.

---

## What exists today

| Thing | Path |
|---|---|
| daft-examples PR (OpenRouter) | `Eventual-Inc/daft-examples` **PR #42**, `examples/prompt/prompt_gepa.py` |
| archetype demo (Anthropic, imports the lib) | `examples/gepa_daft_demo.py` |
| archetype lib (the algorithm) | `src/archetype/optimize/gepa.py` — `gepa_search`, `pareto_select` |
| parity eval (proves it == GEPA) | `tests/experiments/test_gepa_daft.py` |

`gepa_search(...)` today returns `{text, mean, pool, calls, trace}` where
`trace[g] = {gen, parent(sid), sigma, sigma_prime, accepted}`. It does **not** yet expose
the full candidate pool with lineage + per-instance Pareto sets — that's task 1.

## The target: GEPA's visualizer

- **File:** `gepa/visualization.py` in the official repo (`github.com/gepa-ai/gepa`).
  Dependency-free: it emits a self-contained HTML page that renders the graph client-side
  via `@viz-js/viz` from a CDN. © 2025 Lakshya A Agrawal and the GEPA contributors.
- **Entry point we want** (raw data — no `GEPAState` needed):

  ```python
  candidate_tree_html_from_data(candidates, parents, val_scores, pareto_front_programs) -> str
  candidate_tree_dot_from_data(...)  -> str   # Graphviz DOT, if you want a static render
  ```

- **Arg shapes** (this is the whole contract):
  - `candidates: Sequence[dict[str,str]]` — per candidate, `{component_name: prompt_text}`.
    For us: `[{"instruction": cand_text}, ...]`, **index 0 must be the seed** (the viz treats
    idx 0 as `Seed`).
  - `parents: Sequence[Sequence[int|None]]` — `parents[i]` = list of parent candidate indices,
    `[None]` for the seed. (List-of-parents because GEPA merge can have two; ours is single → `[idx]`.)
  - `val_scores: Sequence[float]` — per-candidate aggregate score (our `mean` over instances).
  - `pareto_front_programs: Mapping[Any, set[int]]` — **per validation instance**, the set of
    candidate indices achieving the max score on that instance. The viz derives the orange
    "Pareto front" nodes from this via `find_dominator_programs(pareto_front_programs, list(val_scores))`
    (`gepa.gepa_utils`).

Colors: best=cyan, Pareto-dominators=orange, others=gray; hover tooltip = full prompt text + score + parent.

---

## Task 1 — surface the candidate-tree data from `gepa_search`

Everything needed is already computed; just record lineage and emit the four arrays.

1. In `_candidate(...)`, also store `parent_sid` (None for the seed).
2. In `gepa_search`, when building the accepted child, pass `parent_sid=parent["sid"]`.
3. After the loop, the pool is already in insertion order (seed first → index 0). Build:

```python
sid_to_idx = {c["sid"]: i for i, c in enumerate(pool)}
candidates = [{"instruction": c["text"]} for c in pool]
parents = [
    [None] if c["parent_sid"] is None else [sid_to_idx[c["parent_sid"]]]
    for c in pool
]
val_scores = [sum(c["scores"].values()) / len(c["scores"]) for c in pool]
pareto_front_programs = {
    i: {k for k, c in enumerate(pool) if c["scores"][i] == max(d["scores"][i] for d in pool)}
    for i in instance_ids
}
return {..., "tree": {
    "candidates": candidates, "parents": parents,
    "val_scores": val_scores, "pareto_front_programs": pareto_front_programs,
}}
```

> Note this `pareto_front_programs` is the same `star`/`wins` idea as `pareto_select`, but
> computed once over the **final** pool. Keep `pareto_select` unchanged — the parity test
> depends on it.

The archetype lib change should land with a small unit test (mock eval/reflect) asserting
`tree.candidates[0]` is the seed, `tree.parents[0] == [None]`, and every non-seed parent
index resolves into range.

## Task 2 — render + launch

**Sourcing the visualizer — two options:**

- **(a) Depend on `gepa`:** `dependencies += ["gepa"]`; `from gepa.visualization import candidate_tree_html_from_data`.
  Cleanest attribution. Caveat: the `*_from_data` functions import `gepa.gepa_utils.find_dominator_programs`,
  so the **package** must be installed, not just the one file.
- **(b) Vendor:** copy `candidate_tree_*_from_data` + `find_dominator_programs` into a local
  `gepa_viz.py` with the GEPA copyright/attribution header preserved + a link. Keeps the
  daft-example's "**deps = `daft[openai]` only**" property.

**Recommendation:** vendor for the **daft-examples PR** (preserve the deps property; keep the
attribution header); depend on `gepa` for the **archetype** demo (it already has heavier deps).

**Launch:**

```python
import os, pathlib, webbrowser
html = candidate_tree_html_from_data(**result["tree"])
out = pathlib.Path("gepa_tree.html"); out.write_text(html)
if not os.environ.get("CI"):
    webbrowser.open(out.resolve().as_uri())
print(f"wrote {out}")
```

Gate the whole thing behind `--visualize` (argparse) so the base demo stays zero-UI and
CI-safe. Only call `webbrowser.open` outside CI (the HTML still gets written for inspection).

## Task 3 — verify

- `uv run prompt_gepa.py --visualize` (mock is fine) writes `gepa_tree.html`, opens it, and the
  tree shows the real lineage: seed at idx 0 (gray/SEED), accepted children as descendants,
  one cyan BEST, orange PARETO nodes; hovering a node shows the evolved instruction text.
- Confirm `find_dominator_programs` resolves (option a) or is vendored (option b).
- No regression to the no-flag run or to CI (no browser in CI — guard with the `CI` env check).

## Gotchas

- **idx 0 = seed**, always. The viz labels idx 0 as `Seed`.
- `pareto_front_programs` keys are **per instance**, values are **sets of candidate indices**.
- The HTML pulls `@viz-js/viz` from a CDN → needs browser network to render (GEPA's design).
- Keep `parents[i]` a **list** even though ours is single-parent.
- Don't touch `pareto_select` / the parity test — only add the post-loop `tree` builder.

---

## Cross-check: this *is* the same algorithm GEPA ships

The official repo's toy (`examples/langchain_adapter/pair_sum_product.py`) calls
`gepa.optimize(seed_candidate=..., adapter=..., candidate_selection_strategy="pareto",
use_merge=True, max_metric_calls=..., reflection_minibatch_size=...)` where the adapter's
`eval_fn` returns `(score, feedback)`. That maps 1:1 onto our `gepa_search`
(`eval_fn -> {id:{s,fb}}`, Pareto selection, reflection minibatch, metric-call budget). Ours
is single-module (no `use_merge`) and batch-native via Daft; theirs is LangChain + threads.

---

## Full current script (`examples/prompt/prompt_gepa.py`, the daft-examples / OpenRouter variant)

```python
# /// script
# description = "GEPA: reflective prompt optimization as a Daft pipeline (Pareto selection + reflect-and-mutate)"
# requires-python = ">=3.12"
# dependencies = ["daft[openai]>=0.7.10"]
# ///
"""GEPA, as a Daft pipeline — reflective prompt optimization in one file.

GEPA (Agrawal et al. 2025, arXiv:2507.19457) evolves a prompt by *reflecting* on
its own failures and keeping a Pareto frontier of candidates — not a single
best-by-mean. The algorithm below is ~70 lines of plain Python; its two effects
are Daft expressions:

    eval    = daft.functions.prompt(...)  -> classify each example, score vs gold
    reflect = daft.functions.prompt(...)  -> rewrite the instruction from failures

No agent framework, no orchestration glue: the optimizer is a loop, the LLM is a
column, and Daft runs the batch.

    export OPENROUTER_API_KEY=sk-or-...
    uv run prompt_gepa.py
    # No key? It runs a deterministic mock so the loop is still visible.
"""

from __future__ import annotations

import os
from random import Random

# ===========================================================================
# GEPA — the algorithm (pure; the two effects are injected by the caller).
# Agrawal et al. 2025, arXiv:2507.19457 — Algorithm 1 (reflective evolution) +
# Algorithm 2 (Pareto candidate selection). Alg-line refs inline.
# ===========================================================================


def pareto_select(pool, instance_ids, rng):
    """GEPA Algorithm 2 — instance-wise Pareto selection (NOT top-k-by-mean).

    1. per-instance best:    s*[i] = max_k S[k][i]
    2. per-instance winners: P*[i] = {k : S[k][i] == s*[i]}
    3. prune candidates strictly dominated across instances
    4. sample ∝ #instances each candidate wins

    A specialist that wins one instance survives even if its mean is low — exactly
    the diversity beam search throws away.
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


def _candidate(sid, text, res, instance_ids):
    return {
        "sid": sid,
        "text": text,
        "scores": {i: res[i]["s"] for i in instance_ids},  # per-instance VECTOR (Pareto needs it)
        "fbk": {i: res[i]["fb"] for i in instance_ids},
    }


def gepa_search(*, seed_text, instance_ids, eval_fn, reflect_fn, budget, minibatch, rng):
    """GEPA Algorithm 1 — reflective evolution with Pareto selection.

    eval_fn(sid, text, ids) -> {id: {"s": float, "fb": str}}   # score + feedback
    reflect_fn(text, feedback_block) -> str                    # GEPA's UpdatePrompt
    Budget is counted in metric calls (one candidate x one instance = one call).
    """
    seed_res = eval_fn("gepa-seed", seed_text, instance_ids)
    pool = [_candidate("gepa-seed", seed_text, seed_res, instance_ids)]
    spent = len(instance_ids)  # Alg 1 L7 + L18-20: seed full-eval on D_pareto
    trace, gen = [], 0

    while spent + minibatch <= budget:  # Alg 1 L8: while budget not exhausted
        gen += 1
        parent = pareto_select(pool, instance_ids, rng)                   # Alg 2
        mb = rng.sample(instance_ids, min(minibatch, len(instance_ids)))  # minibatch
        sigma = sum(parent["scores"][i] for i in mb) / len(mb)          # sigma (cached)
        fb = "\n".join(f"- instance {i}: {parent['fbk'][i]}" for i in mb)
        child_text = reflect_fn(parent["text"], fb)                      # Alg 1 L13: UpdatePrompt
        csid = f"gepa-g{gen}"
        c_res = eval_fn(f"{csid}-mb", child_text, mb)                    # eval child on minibatch
        spent += len(mb)
        sigma_prime = sum(c_res[i]["s"] for i in mb) / len(mb)        # sigma'
        accepted = sigma_prime > sigma                                   # Alg 1 L16: accept gate
        trace.append(
            {"gen": gen, "parent": parent["sid"], "sigma": round(sigma, 3),
             "sigma_prime": round(sigma_prime, 3), "accepted": accepted}
        )
        if accepted and spent + len(instance_ids) <= budget:
            full = eval_fn(csid, child_text, instance_ids)              # Alg 1 L18-20: full-eval
            spent += len(instance_ids)
            pool.append(_candidate(csid, child_text, full, instance_ids))

    best = max(pool, key=lambda c: sum(c["scores"].values()) / len(c["scores"]))
    return {
        "text": best["text"],
        "mean": sum(best["scores"].values()) / len(best["scores"]),
        "pool": len(pool),
        "calls": spent,
        "trace": trace,
    }


# ===========================================================================
# The task: a tiny sentiment set. Gold labels => deterministic scoring (no LLM
# judge). The last three are the hard cases the seed instruction trips on; GEPA
# has to *discover* them from feedback and write guidance for them.
# ===========================================================================
DATA = [
    ("Absolutely loved every minute of it.", "positive"),
    ("A complete waste of my evening.", "negative"),
    ("Best purchase I've made all year.", "positive"),
    ("It broke on the second day.", "negative"),
    ("Oh great, another Monday. Just what I needed.", "negative"),  # sarcasm
    ("This is not bad at all, honestly.", "positive"),  # negation
    ("Well, it certainly exists, I'll give it that.", "negative"),  # faint praise
]
IDS = list(range(len(DATA)))
SEED = "Classify the sentiment of the text as 'positive' or 'negative'."

# Any OpenRouter model id works. A small task model leaves more headroom for GEPA
# to demonstrate a lift; a strong reflection model writes better instructions.
TASK_MODEL = "meta-llama/llama-3.1-8b-instruct"  # runs per example
REFLECT_MODEL = "openai/gpt-4o"  # runs once per generation
FORMAT = "\nRespond with exactly one word: positive or negative."  # fixed scaffold; GEPA evolves only SEED


# ---------------------------------------------------------------------------
# Effects, backed by Daft. Each eval/reflect is one prompt() over a batch.
# ---------------------------------------------------------------------------
def daft_effects():
    import daft
    from daft.functions import prompt

    daft.set_provider(
        "openai",
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
        # Anthropic swap: base_url="https://api.anthropic.com/v1/", api_key=os.environ["ANTHROPIC_API_KEY"]
    )

    def eval_fn(sid, instruction, ids):
        df = (
            daft.from_pydict(
                {"id": ids, "text": [DATA[i][0] for i in ids], "gold": [DATA[i][1] for i in ids]}
            )
            .with_column(
                "resp",
                prompt(
                    daft.col("text"),
                    system_message=instruction + FORMAT,
                    model=TASK_MODEL,
                    use_chat_completions=True,
                ),
            )
            .with_column("pred_pos", daft.col("resp").lower().contains("positive"))
            .with_column(
                "ok",
                (daft.col("pred_pos") == (daft.col("gold") == "positive")).cast(daft.DataType.float32()),
            )
            .select("id", "text", "gold", "pred_pos", "ok")
            .collect()  # one materialization per candidate eval — this IS the metric call
        )
        out = {}
        for r in df.to_pylist():
            pred = "positive" if r["pred_pos"] else "negative"
            out[r["id"]] = {
                "s": float(r["ok"]),
                "fb": "correct" if r["ok"] else f"said {pred}, gold {r['gold']} for: {r['text']!r}",
            }
        return out

    def reflect_fn(instruction, feedback_block):
        msg = (
            "Improve this sentiment-classification instruction given how it did on a minibatch.\n\n"
            f"INSTRUCTION:\n{instruction}\n\nFEEDBACK:\n{feedback_block}\n\n"
            "Write one improved instruction that fixes the mistakes. Output only the instruction."
        )
        df = (
            daft.from_pydict({"x": [msg]})
            .with_column("out", prompt(daft.col("x"), model=REFLECT_MODEL, use_chat_completions=True))
            .collect()
        )
        return df.to_pylist()[0]["out"].strip()

    return eval_fn, reflect_fn


# ---------------------------------------------------------------------------
# Offline mock — same loop shape, no key, no daft import. A prompt "scores" by
# how many failure modes it names; reflect adds the next missing one.
# ---------------------------------------------------------------------------
def mock_effects():
    cues = {4: "sarcasm", 5: "negation", 6: "faint praise"}

    def eval_fn(sid, instruction, ids):
        out = {}
        for i in ids:
            cue = cues.get(i)
            ok = cue is None or cue in instruction.lower()
            out[i] = {"s": 1.0 if ok else 0.0, "fb": "correct" if ok else f"missed the {cue} case"}
        return out

    def reflect_fn(instruction, feedback_block):
        for cue in ("sarcasm", "negation", "faint praise"):
            if f"the {cue} case" in feedback_block and cue not in instruction.lower():
                return f"{instruction} Also handle {cue}."
        return instruction

    return eval_fn, reflect_fn


def main():
    live = bool(os.environ.get("OPENROUTER_API_KEY"))
    eval_fn, reflect_fn = daft_effects() if live else mock_effects()
    mode = "LIVE (prompt() via OpenRouter)" if live else "offline mock (set OPENROUTER_API_KEY for live)"
    print(f"GEPA as a Daft pipeline  |  {mode}\n")

    out = gepa_search(
        seed_text=SEED, instance_ids=IDS, eval_fn=eval_fn, reflect_fn=reflect_fn,
        budget=60, minibatch=3, rng=Random(0),
    )

    print("trace — Pareto-selected parent -> reflect -> minibatch gate:")
    for t in out["trace"]:
        verdict = "accept" if t["accepted"] else "reject"
        print(f"  gen {t['gen']:>2}  parent={t['parent']:<10}  sigma={t['sigma']:.2f} -> sigma'={t['sigma_prime']:.2f}  {verdict}")
    print(f"\nseed    : {SEED}")
    print(f"best    : mean={out['mean']:.2f}  (pool={out['pool']}, llm_calls={out['calls']})")
    print(f"evolved : {out['text']}")


if __name__ == "__main__":
    main()
```

> The archetype variant (`examples/gepa_daft_demo.py`) is identical in structure but imports
> `from archetype.optimize.gepa import gepa_search` instead of inlining it, and defaults to
> Anthropic. For the visualizer, do task 1 in `src/archetype/optimize/gepa.py` (so both
> variants benefit) and vendor/depend per task 2.
