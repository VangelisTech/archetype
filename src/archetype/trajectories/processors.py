# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Processors
=====================

Three pipeline stages, priority-ordered:

    SamplingProcessor  (priority=10)  — selects which sessions to evaluate
    LabelingProcessor  (priority=20)  — applies a labeling technique via LLM
    ScoringProcessor   (priority=30)  — normalizes and aggregates scores

Each is a standard AsyncProcessor. They compose in a single tick:
one tick = sample → label → score. Fork the world to try different configs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import daft
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources
from archetype.trajectories.components import Label, Session

# ── Resources (injected into world.resources) ──


@dataclass
class SamplingConfig:
    """Configures which sessions get evaluated.

    Attributes:
        max_sessions:  Maximum number of sessions to sample (0 = all)
        min_turns:     Minimum turn count to include
        max_turns:     Maximum turn count to include (0 = no limit)
        require_tags:  Only include sessions with ALL of these tags
        exclude_tags:  Exclude sessions with ANY of these tags
        outcome_filter: If set, only include sessions matching this outcome substring
    """

    max_sessions: int = 0
    min_turns: int = 0
    max_turns: int = 0
    require_tags: list[str] | None = None
    exclude_tags: list[str] | None = None
    outcome_filter: str | None = None


@dataclass
class LabelingConfig:
    """Configures the LLM-based labeling.

    Attributes:
        model:             LLM model to use for labeling
        max_output_tokens: Max tokens for LLM response
        temperature:       Sampling temperature
    """

    model: str = "gpt-5-mini"
    max_output_tokens: int = 512
    temperature: float = 0.0


# ── Processors ──


class SamplingProcessor(AsyncProcessor):
    """Selects which sessions to evaluate based on SamplingConfig.

    Sets label__sampled = True/False. Downstream processors skip
    unsampled sessions.
    """

    components = (Session, Label)
    priority = 10

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        resources: Resources = kwargs.get("resources", Resources())
        config = resources.get(SamplingConfig) or SamplingConfig()

        sampled = col("label__sampled")

        if config.min_turns > 0:
            sampled = sampled & (col("session__total_turns") >= config.min_turns)

        if config.max_turns > 0:
            sampled = sampled & (col("session__total_turns") <= config.max_turns)

        if config.outcome_filter:
            sampled = sampled & col("session__outcome").str.contains(config.outcome_filter)

        if config.require_tags:
            for tag in config.require_tags:
                sampled = sampled & col("session__tags").str.contains(f'"{tag}"')

        if config.exclude_tags:
            for tag in config.exclude_tags:
                sampled = sampled & ~col("session__tags").str.contains(f'"{tag}"')

        df = df.with_columns({"label__sampled": sampled})

        if config.max_sessions > 0:
            df = df.limit(config.max_sessions)

        return df


class LabelingProcessor(AsyncProcessor):
    """Applies a labeling technique to sampled sessions via LLM.

    Reads label__description (the natural language labeling instruction)
    and session__turns (the full trajectory), then calls an LLM to produce:
        - label__value:     categorical or freeform label
        - label__score:     numeric 0.0-1.0
        - label__rationale: explanation

    Only processes sessions where label__sampled is True.
    """

    components = (Session, Label)
    priority = 20

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        resources: Resources = kwargs.get("resources", Resources())
        config = resources.get(LabelingConfig) or LabelingConfig()

        from daft.functions import prompt

        eval_prompt = (
            "You are an expert evaluator of AI agent sessions.\n\n"
            "## Evaluation Technique\n"
            + col("label__technique")
            + ": "
            + col("label__description")
            + "\n\n## Session Trajectory\n"
            "Source: "
            + col("session__source")
            + "\nOutcome: "
            + col("session__outcome")
            + "\nTotal turns: "
            + col("session__total_turns").cast(daft.DataType.string())
            + "\nDuration: "
            + col("session__duration_seconds").cast(daft.DataType.string())
            + "s\n\nTurns:\n"
            + col("session__turns")
            + "\n\n## Instructions\n"
            "Evaluate this session according to the technique above.\n"
            "Respond in EXACTLY this format (no other text):\n"
            "VALUE: <a short categorical label>\n"
            "SCORE: <float 0.0 to 1.0>\n"
            "RATIONALE: <1-2 sentence explanation>"
        )

        llm_output = prompt(
            eval_prompt,
            model=config.model,
            max_output_tokens=config.max_output_tokens,
        )

        # Row-wise extractors — @daft.func with auto type inference (LEARNINGS.md §4)
        # No batching benefit here, just parsing strings.

        @daft.func
        def extract_value(response: str, sampled: bool) -> str:
            if not sampled or not response:
                return ""
            for line in response.split("\n"):
                if line.startswith("VALUE:"):
                    return line[6:].strip()
            return response[:100]

        @daft.func
        def extract_score(response: str, sampled: bool) -> float:
            if not sampled or not response:
                return 0.0
            for line in response.split("\n"):
                if line.startswith("SCORE:"):
                    try:
                        return float(line[6:].strip())
                    except ValueError:
                        return 0.0
            return 0.0

        @daft.func
        def extract_rationale(response: str, sampled: bool) -> str:
            if not sampled or not response:
                return ""
            for line in response.split("\n"):
                if line.startswith("RATIONALE:"):
                    return line[10:].strip()
            return ""

        llm_col = llm_output
        sampled_col = col("label__sampled")

        return df.with_columns(
            {
                "label__value": extract_value(llm_col, sampled_col),
                "label__score": extract_score(llm_col, sampled_col),
                "label__rationale": extract_rationale(llm_col, sampled_col),
            }
        )


class ScoringProcessor(AsyncProcessor):
    """Post-processes scores for normalization and summary stats.

    Runs after labeling. Clamps scores to [0, 1] and could be extended
    for cross-session normalization, percentile ranking, etc.
    """

    components = (Session, Label)
    priority = 30

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        clamped = col("label__score").if_else(
            col("label__score") > 1.0,
            1.0,
            col("label__score"),
        )
        clamped = clamped.if_else(clamped < 0.0, 0.0, clamped)
        return df.with_columns({"label__score": clamped})
