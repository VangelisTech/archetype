# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Processors
=====================

Three pipeline stages, priority-ordered:

    SamplingProcessor  (priority=10)  — selects which trajectories to evaluate
    LabelingProcessor  (priority=20)  — applies a labeling technique via LLM
    ScoringProcessor   (priority=30)  — normalizes and aggregates scores

Each is a standard AsyncProcessor. They compose in a single tick:
one tick = sample → label → score. Fork the world to try different configs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import daft
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources
from archetype.trajectories.components import Label, Trajectory

# ── Resources (injected into world.resources) ──


@dataclass
class SamplingConfig:
    """Configures which trajectories get evaluated.

    Attributes:
        max_trajectories: Maximum number of trajectories to sample (0 = all)
        min_turns:        Minimum turn count to include
        max_turns:        Maximum turn count to include (0 = no limit)
        require_tags:     Only include trajectories with ALL of these tags
        exclude_tags:     Exclude trajectories with ANY of these tags
        outcome_filter:   If set, only include trajectories matching this outcome substring
    """

    max_trajectories: int = 0
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
    """

    model: str = "gpt-5-mini"
    max_output_tokens: int = 512


# ── Tag matching UDF ──


@daft.func
def _tags_contain(tags_json: str, tag: str) -> bool:
    """Exact membership check against JSON-encoded tag list."""
    try:
        return tag in json.loads(tags_json)
    except (json.JSONDecodeError, TypeError):
        return False


# ── Processors ──


class SamplingProcessor(AsyncProcessor):
    """Selects which trajectories to evaluate based on SamplingConfig.

    Sets label__sampled = True/False. Downstream processors skip
    unsampled trajectories. Never drops rows — all entities are preserved.
    """

    components = (Trajectory, Label)
    priority = 10

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        resources: Resources = kwargs.get("resources", Resources())
        config = resources.get(SamplingConfig) or SamplingConfig()

        # Start fresh from True so sampling is recomputed each tick
        sampled = daft.lit(True)

        if config.min_turns > 0:
            sampled = sampled & (col("trajectory__total_turns") >= config.min_turns)

        if config.max_turns > 0:
            sampled = sampled & (col("trajectory__total_turns") <= config.max_turns)

        if config.outcome_filter:
            sampled = sampled & col("trajectory__outcome").str.contains(config.outcome_filter)

        if config.require_tags:
            for tag in config.require_tags:
                sampled = sampled & _tags_contain(
                    col("trajectory__tags_json"), daft.lit(tag)
                )

        if config.exclude_tags:
            for tag in config.exclude_tags:
                sampled = sampled & ~_tags_contain(
                    col("trajectory__tags_json"), daft.lit(tag)
                )

        df = df.with_columns({"label__sampled": sampled})

        # Enforce max_trajectories without dropping rows: mark excess as unsampled
        if config.max_trajectories > 0:
            df = df._add_monotonically_increasing_id("_sample_idx")
            df = df.with_columns(
                {
                    "label__sampled": col("label__sampled")
                    & (col("_sample_idx") < config.max_trajectories),
                }
            ).exclude("_sample_idx")

        return df


class LabelingProcessor(AsyncProcessor):
    """Applies a labeling technique to sampled trajectories via LLM.

    Reads label__description (the natural language labeling instruction)
    and trajectory__turns_json (the full trajectory), then calls an LLM to produce:
        - label__value:     categorical or freeform label
        - label__score:     numeric 0.0-1.0
        - label__rationale: explanation

    Only invokes the LLM for trajectories where label__sampled is True.
    """

    components = (Trajectory, Label)
    priority = 20

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        resources: Resources = kwargs.get("resources", Resources())
        config = resources.get(LabelingConfig) or LabelingConfig()

        from daft.functions import prompt

        # Split sampled vs unsampled to avoid LLM calls on unsampled rows
        sampled_df = df.where(col("label__sampled"))
        unsampled_df = df.where(~col("label__sampled"))

        eval_prompt = (
            "You are an expert evaluator of AI agent trajectories.\n\n"
            "## Evaluation Technique\n"
            + col("label__technique")
            + ": "
            + col("label__description")
            + "\n\n## Trajectory\n"
            "Source: "
            + col("trajectory__source")
            + "\nOutcome: "
            + col("trajectory__outcome")
            + "\nTotal turns: "
            + col("trajectory__total_turns").cast(daft.DataType.string())
            + "\nDuration: "
            + col("trajectory__duration_seconds").cast(daft.DataType.string())
            + "s\n\nTurns:\n"
            + col("trajectory__turns_json")
            + "\n\n## Instructions\n"
            "Evaluate this trajectory according to the technique above.\n"
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

        @daft.func
        def extract_value(response: str) -> str:
            if not response:
                return ""
            for line in response.split("\n"):
                if line.startswith("VALUE:"):
                    return line[6:].strip()
            return response[:100]

        @daft.func
        def extract_score(response: str) -> float:
            if not response:
                return 0.0
            for line in response.split("\n"):
                if line.startswith("SCORE:"):
                    try:
                        return float(line[6:].strip())
                    except ValueError:
                        return 0.0
            return 0.0

        @daft.func
        def extract_rationale(response: str) -> str:
            if not response:
                return ""
            for line in response.split("\n"):
                if line.startswith("RATIONALE:"):
                    return line[10:].strip()
            return ""

        llm_col = llm_output

        sampled_df = sampled_df.with_columns(
            {
                "label__value": extract_value(llm_col),
                "label__score": extract_score(llm_col),
                "label__rationale": extract_rationale(llm_col),
            }
        )

        # Rejoin: unsampled rows keep their existing label values
        return sampled_df.concat(unsampled_df)


class ScoringProcessor(AsyncProcessor):
    """Post-processes scores for normalization and summary stats.

    Runs after labeling. Clamps scores to [0, 1] and could be extended
    for cross-trajectory normalization, percentile ranking, etc.
    """

    components = (Trajectory, Label)
    priority = 30

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        @daft.func
        def clamp_score(score: float) -> float:
            return max(0.0, min(1.0, score))

        return df.with_columns({"label__score": clamp_score(col("label__score"))})
