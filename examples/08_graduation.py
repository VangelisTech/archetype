# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Graduation: L0 -> L1 in one runnable script.
============================================

Minimal demonstration of the graduation thesis from
``docs/guide/graduation.md``:

    L0  frontier LLM via daft.functions.prompt (pinned model)
    L1  Python primitive

The task: "is this string a valid Python identifier?"

L0 asks gpt-5-mini through prompt() with a Pydantic verdict shape, in
one columnar pass over the fixture. L1 calls str.isidentifier(). Both
run against the same fixture; a gate compares pass rate, latency, and
a cost proxy and prints a Promote / Hold verdict.

Requirements:
    export OPENAI_API_KEY=sk-...           # required for L0
    # without the key the script still runs but skips L0 and the gate

Usage:
    uv run python examples/08_graduation.py
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass

import daft
from daft import col
from daft.functions import prompt as daft_prompt
from pydantic import BaseModel, Field

# ── Pinned baseline (Inv R6 in graduation.md) ───────────────────────────

L0_MODEL = "gpt-5-mini"
L0_PROVIDER: str | None = None  # auto-detect from env

# ── Fixture (immutable per Inv R3 once cited by a gate) ─────────────────

FIXTURE_ID = "py_identifier_v1"

# (subject, expected) — ground truth comes from str.isidentifier itself,
# which is the spec we're claiming L0 should match.
FIXTURE: list[tuple[str, bool]] = [
    ("hello", True),
    ("hello_world", True),
    ("Hello123", True),
    ("_underscore", True),
    ("CamelCase", True),
    ("snake_case_123", True),
    ("class", True),         # keyword — str.isidentifier returns True
    ("if", True),            # keyword — same
    ("def", True),           # keyword — same
    ("こんにちは", True),       # Unicode-letter identifier
    ("123abc", False),       # leading digit
    ("hello world", False),  # space
    ("hello-world", False),  # hyphen
    ("hello.world", False),  # dot
    ("hello!", False),       # punctuation
    ("", False),             # empty
]


class IdVerdict(BaseModel):
    """Structured verdict the L0 model returns per case."""

    is_identifier: bool
    reasoning: str = Field(default="")


# ── Implementations ─────────────────────────────────────────────────────


def l1_isidentifier(strings: list[str]) -> list[bool]:
    """L1 — Python primitive."""
    return [s.isidentifier() for s in strings]


_L0_INSTRUCTION = (
    "Decide whether the given string is a valid Python identifier per "
    "str.isidentifier() semantics. Identifiers start with a letter or "
    "underscore (any Unicode letter is allowed) and contain only "
    "letters, digits, or underscores. Python keywords like 'class', "
    "'if', or 'def' ARE identifiers under this rule. The empty string "
    "is NOT an identifier."
)


def l0_prompt_isidentifier(
    strings: list[str],
    *,
    prompt_fn: Callable[..., daft.Expression] | None = None,
) -> list[bool]:
    """L0 — frontier LLM, single columnar pass via daft.functions.prompt.

    Pass ``prompt_fn`` to inject a fake (e.g. for tests).
    """
    fn = prompt_fn or daft_prompt
    df = daft.from_pylist([{"s": s} for s in strings])
    message = daft.lit(f"{_L0_INSTRUCTION}\n\nString:\n") + col("s")
    rows = (
        df.with_column(
            "verdict",
            fn(
                message,
                return_format=IdVerdict,
                model=L0_MODEL,
                provider=L0_PROVIDER,
            ),
        )
        .select("verdict")
        .collect()
        .to_pylist()
    )
    return [row["verdict"]["is_identifier"] for row in rows]


# ── Eval ────────────────────────────────────────────────────────────────


@dataclass
class EvalResult:
    impl: str
    level: str
    pass_rate: float
    n_cases: int
    n_correct: int
    latency_ms: float
    cost_proxy: int      # 0 for L1; sum of input chars for L0 (token-ish)
    fixture_id: str

    def line(self) -> str:
        return (
            f"{self.level} {self.impl:32s}  "
            f"pass={self.pass_rate:6.1%} ({self.n_correct}/{self.n_cases})  "
            f"latency={self.latency_ms:8.1f}ms  cost_proxy={self.cost_proxy}"
        )


def run_eval(
    impl_name: str,
    level: str,
    fn: Callable[[list[str]], list[bool]],
    cost_proxy: int,
) -> EvalResult:
    inputs = [s for s, _ in FIXTURE]
    expected = [exp for _, exp in FIXTURE]
    t0 = time.perf_counter()
    actual = fn(inputs)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    if len(actual) != len(expected):
        raise RuntimeError(
            f"{impl_name} returned {len(actual)} verdicts for {len(expected)} cases"
        )
    correct = sum(1 for a, e in zip(actual, expected) if a == e)
    return EvalResult(
        impl=impl_name,
        level=level,
        pass_rate=correct / len(expected),
        n_cases=len(expected),
        n_correct=correct,
        latency_ms=elapsed_ms,
        cost_proxy=cost_proxy,
        fixture_id=FIXTURE_ID,
    )


# ── Gate (L0 -> L1 promotion predicate) ─────────────────────────────────


@dataclass
class Verdict:
    decision: str    # "Promote" | "Hold"
    reason: str


def gate_l0_to_l1(below: EvalResult, candidate: EvalResult) -> Verdict:
    """L1 promotes iff: pass_rate matches L0 AND wins on cost or latency."""
    if candidate.pass_rate < below.pass_rate:
        return Verdict(
            "Hold",
            f"L1 pass_rate {candidate.pass_rate:.1%} < L0 {below.pass_rate:.1%}",
        )
    cost_win = candidate.cost_proxy < below.cost_proxy
    latency_win = candidate.latency_ms < below.latency_ms
    if not (cost_win or latency_win):
        return Verdict("Hold", "L1 didn't win on cost or latency")
    wins = []
    if cost_win:
        wins.append(f"cost {candidate.cost_proxy} < {below.cost_proxy}")
    if latency_win:
        wins.append(
            f"latency {candidate.latency_ms:.1f}ms < {below.latency_ms:.1f}ms"
        )
    return Verdict(
        "Promote",
        f"matches L0 pass_rate ({candidate.pass_rate:.1%}); wins on {', '.join(wins)}",
    )


# ── Main ────────────────────────────────────────────────────────────────


def main() -> None:
    has_key = bool(os.getenv("OPENAI_API_KEY"))
    print(f"Fixture: {FIXTURE_ID}  ({len(FIXTURE)} cases)")
    print(f"Mode:    {'LIVE — calling ' + L0_MODEL if has_key else 'STUB — no OPENAI_API_KEY, L0 skipped'}")
    print()

    # L1 — always run.
    l1 = run_eval("isidentifier (Python builtin)", "L1", l1_isidentifier, cost_proxy=0)
    print(l1.line())

    if not has_key:
        print()
        print("Set OPENAI_API_KEY to run the L0 baseline and the gate.")
        return

    # L0 — only run with a key.
    cost = sum(len(s) for s, _ in FIXTURE)  # naive char-count proxy
    l0 = run_eval(
        f"prompt({L0_MODEL})",
        "L0",
        l0_prompt_isidentifier,
        cost_proxy=cost,
    )
    print(l0.line())
    print()

    verdict = gate_l0_to_l1(below=l0, candidate=l1)
    print(f"Gate L0 -> L1: {verdict.decision}")
    print(f"  reason: {verdict.reason}")


if __name__ == "__main__":
    main()
