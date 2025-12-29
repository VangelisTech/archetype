# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Collection and Curation
==================================

Collect agent trajectories, filter the good ones, and prepare for training.
Works with any agent type (bandits, OODA, custom).
"""

import time

import daft
from daft import DataType, col, lit
from daft.functions import monotonically_increasing_id

from archetype.rl.agents.rubrics import RubricWeights, compute_rubric_reward


def collect_trajectories(
    df: daft.DataFrame,
    weights: RubricWeights = None,
) -> daft.DataFrame:
    """
    Add trajectory metadata and compute rewards.

    Args:
        df: DataFrame after running agent loop
        weights: Rubric weights for reward computation

    Returns:
        DataFrame with trajectory_id, timestamp, and reward columns
    """
    # Add trajectory metadata (per-row, not a single shared literal)
    df = df.with_column("_row_id", monotonically_increasing_id())
    df = df.with_column("trajectory_id", col("_row_id").cast(DataType.string()))
    df = df.with_column("timestamp", lit(time.time()))
    df = df.exclude("_row_id")

    # Compute rubric-based rewards if applicable
    df = compute_rubric_reward(df, weights)

    return df


def filter_good_trajectories(
    df: daft.DataFrame,
    reward_threshold: float = 0.7,
    reward_col: str = "reward",
) -> daft.DataFrame:
    """
    Filter trajectories by reward threshold.

    Args:
        df: DataFrame with reward column
        reward_threshold: Minimum reward to keep (0-1)
        reward_col: Name of reward column

    Returns:
        DataFrame with only good trajectories
    """
    return df.where(col(reward_col) >= reward_threshold)


def filter_by_percentile(
    df: daft.DataFrame,
    percentile: float = 0.8,
    reward_col: str = "reward",
) -> daft.DataFrame:
    """
    Keep top percentile of trajectories by reward.

    Args:
        df: DataFrame with reward column
        percentile: Fraction to keep (e.g., 0.8 = top 80%)
        reward_col: Name of reward column

    Returns:
        DataFrame with top trajectories
    """
    threshold_df = df.agg(col(reward_col).quantile(1 - percentile).alias("_threshold"))
    return (
        df.join(threshold_df, how="cross")
        .where(col(reward_col) >= col("_threshold"))
        .exclude("_threshold")
    )


def label_trajectory_quality(
    df: daft.DataFrame,
    good_threshold: float = 0.7,
    bad_threshold: float = 0.3,
    reward_col: str = "reward",
) -> daft.DataFrame:
    """
    Label trajectories as good, neutral, or bad.
    """
    df = df.with_column(
        "trajectory_quality",
        col(reward_col).if_else(
            col(reward_col) >= good_threshold,
            lit("good"),
            col(reward_col).if_else(col(reward_col) <= bad_threshold, lit("bad"), lit("neutral")),
        ),
    )
    return df


def trajectories_to_training_data(
    df: daft.DataFrame,
    include_bad: bool = False,
) -> daft.DataFrame:
    """
    Convert trajectories to training sequences.

    Creates a "sequence" column with the full trajectory as a training example.
    """
    if not include_bad:
        df = df.where(col("trajectory_quality") != lit("bad"))

    # Label for training (1 = good, 0 = bad)
    df = df.with_column(
        "label",
        col("trajectory_quality").if_else(col("trajectory_quality") == lit("good"), lit(1), lit(0)),
    )

    return df
