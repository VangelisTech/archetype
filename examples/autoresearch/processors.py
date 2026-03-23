# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Processors for the AutoResearch experiment loop.

Each processor does ONE thing: call the LLM. Status transitions
happen in the runner after collecting results.
"""

from daft import DataFrame, col
from daft.functions import prompt

from archetype.core.aio.async_processor import AsyncProcessor

from .components import BranchHead, Experiment, Result, Run


PROPOSE_SYSTEM = """You are an AI research scientist optimizing a scoring function.

The current task: write a Python one-liner lambda that scores a string for "interestingness".
The function signature is: score = lambda text: <your expression>
The score should be a float between 0.0 and 10.0.

You are given the current best strategy and its score. Propose a DIFFERENT, MORE CREATIVE strategy.

Think about what makes text interesting:
- Information density (unique words, vocabulary richness)
- Structural complexity (sentence variety, punctuation)
- Semantic signals (questions, technical terms, emotional words)
- Length-normalized metrics (don't just reward longer text)

Respond with ONLY the lambda expression on a single line. No explanation. Example:
lambda text: min(10, len(set(text.split())) / max(1, len(text.split())) * 10)"""

EVALUATE_SYSTEM = """You are a judge evaluating how well a scoring strategy captures "interestingness" of text.

Given a scoring strategy (a Python lambda), evaluate it on these criteria:
1. Does it capture vocabulary richness? (0-3 points)
2. Does it reward structural complexity? (0-3 points)
3. Is it robust to edge cases (empty strings, very short/long)? (0-2 points)
4. Is it creative/novel compared to simple length-based metrics? (0-2 points)

Respond in exactly this format:
SCORE: <float 0.0-10.0>
REASONING: <one sentence>"""


class ProposeProcessor(AsyncProcessor):
    """Generate a new experiment strategy based on the current frontier."""

    components = (BranchHead, Experiment)
    priority = 1

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({
            "experiment__hypothesis": prompt(
                "Current best strategy (score="
                + col("branchhead__frontier_metric").cast(str)
                + "): "
                + col("experiment__strategy")
                + "\n\nPropose a better one. Reply with ONLY the lambda.",
                system_message=PROPOSE_SYSTEM,
                model="gpt-5-mini",
            ),
        })


class EvaluateProcessor(AsyncProcessor):
    """Judge evaluates the proposed strategy."""

    components = (Experiment, Result)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns({
            "result__reasoning": prompt(
                "Evaluate this scoring strategy:\n" + col("experiment__hypothesis"),
                system_message=EVALUATE_SYSTEM,
                model="gpt-5-mini",
            ),
        })
