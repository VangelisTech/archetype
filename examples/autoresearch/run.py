#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
AutoResearch — Karpathy-style Autonomous Optimization on Archetype
==================================================================

Runs an experiment loop that evolves scoring strategies:
    frontier → propose → execute → evaluate → keep|discard → advance

Usage:
    cd <repo-root>
    source .env
    .venv/bin/python -m examples.autoresearch.run
"""

import ast
import asyncio
import re

from archetype.app.autoresearch import AutoResearchLoop

from .processors import EvaluateProcessor, ProposeProcessor

# ── Test corpus ──

TEST_TEXTS = [
    "The quick brown fox jumps over the lazy dog.",
    "Why do we build systems that understand themselves? Because recursion is the point.",
    "SELECT * FROM users WHERE active = true AND last_login > NOW() - INTERVAL '30 days';",
    "I've never published to PyPI before.",
    "The ECS decouples compute from data — processors are pure DataFrame transforms that parallelize on Ray.",
    "",
    "ok",
    "Archetype is a lazily-evaluated Virtual Data Architecture at your agent's commands.",
]


_ALLOWED_NAMES: dict[str, object] = {
    "len": len,
    "min": min,
    "max": max,
    "abs": abs,
    "float": float,
    "int": int,
    "str": str,
    "sum": sum,
    "round": round,
    "sorted": sorted,
    "enumerate": enumerate,
    "range": range,
    "zip": zip,
    "map": map,
    "filter": filter,
    "True": True,
    "False": False,
    "None": None,
}

_ALLOWED_NODE_TYPES = (
    ast.Expression,
    ast.Lambda,
    ast.BinOp,
    ast.UnaryOp,
    ast.BoolOp,
    ast.Compare,
    ast.IfExp,
    ast.Call,
    ast.Constant,
    ast.Name,
    ast.Tuple,
    ast.List,
    ast.Subscript,
    ast.Slice,
    ast.Index,  # Python 3.8 compat, harmless on 3.12
    ast.Load,
    ast.Store,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.Not,
    ast.And,
    ast.Or,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
    ast.arguments,
    ast.arg,
    ast.ListComp,
    ast.GeneratorExp,
    ast.comprehension,
    ast.Starred,
    ast.JoinedStr,
    ast.FormattedValue,
)


def _validate_lambda_ast(code: str) -> ast.Expression | None:
    """Parse and whitelist-validate a lambda expression. Returns AST or None."""
    try:
        tree = ast.parse(code, mode="eval")
    except SyntaxError:
        return None
    if not isinstance(tree.body, ast.Lambda):
        return None
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODE_TYPES):
            return None
        # Only allow names from the allowlist + the lambda parameter
        if isinstance(node, ast.Name) and node.id not in _ALLOWED_NAMES:
            # Check if it's the lambda param (e.g., "text" or "t")
            params = {a.arg for a in tree.body.args.args}
            if node.id not in params:
                return None
        # Block all attribute access (no obj.method calls)
        if isinstance(node, ast.Attribute):
            return None
    return tree


def evaluate_strategy(strategy_text: str) -> float:
    """Evaluate a lambda strategy against test texts with safety checks."""
    match = re.search(r"(lambda\s+text?\s*:.+?)(?:\n|$)", strategy_text)
    if not match:
        return 0.0
    lambda_str = match.group(1).strip()
    tree = _validate_lambda_ast(lambda_str)
    if tree is None:
        return 0.0
    try:
        code = compile(tree, filename="<strategy>", mode="eval")
        namespace = {**_ALLOWED_NAMES, "__builtins__": {}}
        func = eval(code, namespace)  # noqa: S307
    except Exception:
        return 0.0
    scores = []
    for text in TEST_TEXTS:
        try:
            scores.append(max(0.0, min(10.0, float(func(text)))))
        except Exception:
            scores.append(0.0)
    return sum(scores) / len(scores) if scores else 0.0


def parse_judge_score(reasoning: str) -> float:
    """Extract SCORE: X.X from judge output."""
    match = re.search(r"SCORE:\s*([\d.]+)", reasoning or "")
    try:
        return float(match.group(1)) if match else 0.0
    except ValueError:
        return 0.0


async def main():
    initial = "lambda text: len(text) / 10.0"
    initial_score = evaluate_strategy(initial)

    print(f"Initial frontier: score={initial_score:.2f}")
    print(f"  strategy: {initial}\n")

    loop = AutoResearchLoop(
        processors=[ProposeProcessor(), EvaluateProcessor()],
        evaluate_fn=evaluate_strategy,
        parse_judge_fn=parse_judge_score,
    )

    results = await loop.run(
        initial_strategy=initial,
        initial_score=initial_score,
        num_generations=3,
        experiments_per_gen=3,
    )

    # ── Print results ──
    for gen_result in results:
        status = "ADVANCED" if gen_result.frontier_advanced else "HELD"
        print(f"\n{'='*60}")
        print(f"GENERATION {gen_result.generation + 1} — {status} at {gen_result.frontier_score:.2f}")
        print(f"{'='*60}")

        for exp in gen_result.experiments:
            tag = "KEPT" if exp.kept else "DISC"
            strat = exp.strategy.replace("\n", " ")[:100]
            print(f"  [{tag}] {exp.experiment_id}: "
                  f"combined={exp.combined_score:.2f} "
                  f"(exec={exp.exec_score:.2f}, judge={exp.judge_score:.1f})")
            print(f"    {strat}")

    # ── Final report ──
    final = results[-1] if results else None
    print(f"\n{'='*60}")
    print("FINAL FRONTIER")
    print(f"{'='*60}")
    if final:
        print(f"Score: {final.frontier_score:.2f}")
        print(f"Strategy: {final.frontier_strategy[:200]}")
    print(f"Generations: {len(results)}")
    print(f"Total experiments: {sum(len(g.experiments) for g in results)}")


if __name__ == "__main__":
    asyncio.run(main())
