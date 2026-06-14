# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
# /// script
# requires-python = ">=3.11"
# dependencies = ["daft[openai]>=0.7.9", "archetype-ecs"]
# ///
"""GEPA, as a Daft pipeline — reflective prompt optimization in ~one screen.

GEPA (Agrawal et al. 2025, arXiv:2507.19457) evolves a prompt by *reflecting* on
its own failures, keeping a Pareto frontier of candidates instead of a single
best-by-mean. The algorithm lives in `archetype.optimize.gepa` (pure, ~120 lines,
parity-tested against the paper). This script supplies its two effects with Daft:

    eval_fn    = daft.functions.prompt(...)  -> classify each example, score vs gold
    reflect_fn = daft.functions.prompt(...)  -> rewrite the prompt given failures

No LiteLLM, no agent framework, no orchestration glue — the optimizer is a loop,
the LLM is a column expression, and Daft runs the batch.

Run it:
    export ANTHROPIC_API_KEY=sk-...            # live: Claude via the OpenAI-compatible API
    uv run examples/gepa_daft_demo.py
    # no key? it runs a deterministic mock so the loop is still visible (and CI-green).
"""

from __future__ import annotations

import os
from random import Random

from archetype.optimize.gepa import gepa_search

# A tiny sentiment task. Gold labels => deterministic scoring, no LLM judge. The
# last three are the hard cases (sarcasm, negation, faint praise) the seed prompt
# trips on — GEPA has to *discover* them from feedback and write guidance for them.
DATA: list[tuple[str, str]] = [
    ("Absolutely loved every minute of it.", "positive"),
    ("A complete waste of my evening.", "negative"),
    ("Best purchase I've made all year.", "positive"),
    ("It broke on the second day.", "negative"),
    ("Oh great, another Monday. Just what I needed.", "negative"),  # sarcasm
    ("This is not bad at all, honestly.", "positive"),  # negation
    ("Well, it certainly exists, I'll give it that.", "negative"),  # faint praise
]
INSTANCE_IDS = list(range(len(DATA)))

SEED_PROMPT = "Classify the sentiment of the text as 'positive' or 'negative'."

REWRITE_MODEL = "claude-sonnet-4-6"  # the task model (cheap, runs per example)
REFLECT_MODEL = "claude-opus-4-8"  # the reflector (smart, runs once per generation)


# --------------------------------------------------------------------------- #
# Effects, backed by Daft. Each call is one prompt() expression over a batch.
# --------------------------------------------------------------------------- #
def make_daft_effects():
    import daft
    from daft import col
    from daft.functions import prompt

    daft.set_provider(
        "openai",
        api_key=os.environ["ANTHROPIC_API_KEY"],
        base_url="https://api.anthropic.com/v1/",
    )

    # Fixed output scaffold so responses parse cleanly; GEPA evolves only the strategy.
    fmt = "\nRespond with exactly one word: positive or negative."

    def eval_fn(sid: str, prompt_text: str, ids: list[int]) -> dict[int, dict]:
        rows = {"id": ids, "text": [DATA[i][0] for i in ids], "gold": [DATA[i][1] for i in ids]}
        df = (
            daft.from_pydict(rows)
            .with_column(
                "resp",
                prompt(
                    col("text"),
                    system_message=prompt_text + fmt,
                    model=REWRITE_MODEL,
                    use_chat_completions=True,  # Anthropic's OAI-compat endpoint has no Responses API
                ),
            )
            .with_column("pred_pos", col("resp").lower().contains("positive"))
            .with_column(
                "ok",
                (col("pred_pos") == (col("gold") == "positive")).cast(daft.DataType.float32()),
            )
            .select("id", "text", "gold", "pred_pos", "ok")
            .collect()  # one materialization per candidate eval — this IS the metric call
        )
        out: dict[int, dict] = {}
        for r in df.to_pylist():
            pred = "positive" if r["pred_pos"] else "negative"
            fb = (
                "correct"
                if r["ok"]
                else f"said '{pred}', expected '{r['gold']}' for: {r['text']!r}"
            )
            out[r["id"]] = {"s": float(r["ok"]), "fb": fb}
        return out

    def reflect_fn(prompt_text: str, feedback_block: str) -> str:
        instruction = (
            "You improve a sentiment-classification instruction. Here is the current instruction "
            f"and how it did on a minibatch:\n\nINSTRUCTION:\n{prompt_text}\n\nFEEDBACK:\n{feedback_block}\n\n"
            "Write a single improved instruction that fixes the observed mistakes. Output only the instruction."
        )
        df = (
            daft.from_pydict({"x": [instruction]})
            .with_column("out", prompt(col("x"), model=REFLECT_MODEL, use_chat_completions=True))
            .collect()
        )
        return df.to_pylist()[0]["out"].strip()

    return eval_fn, reflect_fn


# --------------------------------------------------------------------------- #
# Deterministic mock — same loop shape, no key needed (keeps CI green and the
# demo legible offline). A prompt "scores" by how many failure modes it names.
# --------------------------------------------------------------------------- #
def make_mock_effects():
    cues = {
        4: "sarcasm",
        5: "negation",
        6: "faint praise",
    }  # hard cases -> the cue a good prompt must mention

    def eval_fn(sid: str, prompt_text: str, ids: list[int]) -> dict[int, dict]:
        out = {}
        for i in ids:
            cue = cues.get(i)
            ok = (
                cue is None or cue in prompt_text.lower()
            )  # easy cases always pass; hard cases need the cue named
            out[i] = {"s": 1.0 if ok else 0.0, "fb": "correct" if ok else f"missed the {cue} case"}
        return out

    def reflect_fn(prompt_text: str, feedback_block: str) -> str:
        for cue in ("sarcasm", "negation", "faint praise"):
            if f"the {cue} case" in feedback_block and cue not in prompt_text.lower():
                return f"{prompt_text} Also handle {cue}."
        return prompt_text

    return eval_fn, reflect_fn


def main() -> None:
    live = bool(os.environ.get("ANTHROPIC_API_KEY"))
    eval_fn, reflect_fn = make_daft_effects() if live else make_mock_effects()
    print(
        f"GEPA-as-Daft  |  mode={'LIVE (Claude via Daft prompt())' if live else 'mock (no API key)'}\n"
    )

    result = gepa_search(
        seed_text=SEED_PROMPT,
        instance_ids=INSTANCE_IDS,
        eval_fn=eval_fn,
        reflect_fn=reflect_fn,
        budget=60,
        minibatch=3,
        rng=Random(0),
    )

    print("trace (Pareto-selected parent → reflect → minibatch gate):")
    for t in result["trace"]:
        mark = "✓ accept" if t["accepted"] else "· reject"
        print(
            f"  gen {t['gen']:>2}  parent={t['parent']:<10}  σ={t['sigma']:.2f} → σ'={t['sigma_prime']:.2f}  {mark}"
        )

    print(f"\nseed accuracy : evaluate it yourself — seed='{SEED_PROMPT}'")
    print(
        f"best accuracy : {result['mean_success']:.2f}  (pool={result['pool_size']}, metric_calls={result['metric_calls']})"
    )
    print(f"evolved prompt:\n  {result['strategy_text']}")


if __name__ == "__main__":
    main()
