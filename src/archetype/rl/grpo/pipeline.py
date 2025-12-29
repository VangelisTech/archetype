"""
Daft-native GRPO pipeline glue (rollouts → rewards → advantages).

This module is intentionally lightweight: it constructs a Daft DataFrame that
contains the *artifacts* a PyTorch GRPO step needs:
- prompt_token_ids
- completion_token_ids
- old_token_logprobs (behavior policy)
- reward
- advantage (group-relative)

It does *not* do training; see `train_udf.py` / `pytorch_grpo.py`.
"""

from __future__ import annotations

from dataclasses import dataclass

import daft  # type: ignore[import-not-found]
from daft import Window, col  # type: ignore[import-not-found]
from daft.functions import format, monotonically_increasing_id  # type: ignore[import-not-found]

from archetype.rl.grpo.rollout_transformers import TransformersTextRollout


@dataclass(frozen=True)
class RolloutConfig:
    """
    Rollout config for GRPO.

    `num_samples_per_prompt` corresponds to GRPO group size K.
    """

    model: str
    num_samples_per_prompt: int = 8

    # Sampling
    temperature: float = 0.7
    max_tokens: int = 32


def _repeat_rows(df: daft.DataFrame, *, k: int) -> daft.DataFrame:
    """
    Repeat each row `k` times to get multiple samples per prompt.

    Uses explode over a literal list so it stays lazy.
    """
    if k <= 0:
        raise ValueError("num_samples_per_prompt must be >= 1")
    df = df.with_column("_sample_id", daft.lit(list(range(k))))
    return df.explode("_sample_id")


def _extract_first_choice(text_col: daft.Expression) -> daft.Expression:
    """
    Extract a single A/B/C/D letter from the model output.

    This is intentionally simple: it grabs the first occurrence of A-D.
    """
    # Match first letter A-D (case-insensitive), then uppercase it.
    return text_col.regexp_extract(r"(?i)\b([A-D])\b", 1).upper()


def build_grpo_rollouts_from_mc_dataset(
    df: daft.DataFrame,
    *,
    cfg: RolloutConfig,
    question_col: str = "question",
    answer_choice_col: str = "answer_choice",
    prefix: str | None = None,
) -> daft.DataFrame:
    """
    Build GRPO rollouts for a multiple-choice dataset (text-only).

    Expected input schema:
    - question_col: string (contains question + options)
    - answer_choice_col: string (A/B/C/D)
    """
    prompt_prefix = prefix or "Answer with a single letter A, B, C, or D."
    df = df.with_column("_prompt_id", monotonically_increasing_id())

    # Build the prompt (keep this as a Daft expression so prefix caching can work later)
    df = df.with_column(
        "prompt",
        format(
            "{}\n\n<question>\n{}\n</question>\n\nAnswer:",
            daft.lit(prompt_prefix),
            col(question_col),
        ),
    )

    # Repeat for K samples per prompt (GRPO group)
    df = _repeat_rows(df, k=cfg.num_samples_per_prompt)

    # Rollout engine (CPU-friendly): Transformers.
    rollout = TransformersTextRollout(
        model=cfg.model,
        temperature=cfg.temperature,
        max_tokens=cfg.max_tokens,
    )
    df = df.with_column("rollout", rollout.generate(col("prompt")))

    # Extract artifacts
    df = df.with_columns(
        {
            "completion_text": col("rollout")["text"],
            "completion_token_ids": col("rollout")["token_ids"],
            "old_token_logprobs": col("rollout")["token_logprobs"],
        }
    )

    # Reward: correctness of the sampled completion (1.0 or 0.0)
    df = df.with_column("pred_choice", _extract_first_choice(col("completion_text")))
    df = df.with_column(
        "reward",
        (col("pred_choice") == col(answer_choice_col)).cast(daft.DataType.float64()),
    )

    # Advantage: group-relative normalization over prompt_id (GRPO core)
    w = Window().partition_by("_prompt_id")
    mean_r = col("reward").mean().over(w).fill_null(daft.lit(0.0))
    std_r = col("reward").stddev().over(w).fill_null(daft.lit(0.0))

    df = df.with_column(
        "advantage",
        ((col("reward") - mean_r) / (std_r + daft.lit(1e-8))).fill_null(daft.lit(0.0)),
    )

    # Minimal output for training UDFs (keep question/answer for debugging)
    return df.select(
        "_prompt_id",
        "_sample_id",
        col(question_col).alias("question"),
        col(answer_choice_col).alias("answer_choice"),
        "prompt",
        "completion_text",
        "completion_token_ids",
        "old_token_logprobs",
        "reward",
        "advantage",
    )
