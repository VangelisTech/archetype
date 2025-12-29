# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import daft
from daft import Window, col


def test_extract_first_choice() -> None:
    from archetype.rl.grpo.pipeline import _extract_first_choice

    df = daft.from_pydict(
        {
            "out": [
                "Answer: B",
                "I think it's (c).",
                "Final: d",
                "no choice here",
            ]
        }
    )
    df = df.with_column("choice", _extract_first_choice(col("out")))
    rows = df.collect().to_pylist()

    assert rows[0]["choice"] == "B"
    assert rows[1]["choice"] == "C"
    assert rows[2]["choice"] == "D"
    # regexp_extract returns null for no match; Daft materializes it as None
    assert rows[3]["choice"] is None


def test_repeat_rows_k() -> None:
    from archetype.rl.grpo.pipeline import _repeat_rows

    df = daft.from_pydict({"x": [1, 2]})
    out = _repeat_rows(df, k=3).select("x", "_sample_id")
    rows = out.collect().to_pylist()

    assert len(rows) == 6
    assert {r["_sample_id"] for r in rows} == {0, 1, 2}


def test_group_relative_advantage_matches_definition() -> None:
    # This mirrors the advantage computation contract in pipeline.py:
    # advantage = (r - mean(r | prompt_id)) / (std(r | prompt_id) + eps)
    df = daft.from_pydict(
        {
            "_prompt_id": [0, 0, 0, 1, 1],
            "reward": [0.0, 1.0, 1.0, 2.0, 0.0],
        }
    )
    w = Window().partition_by("_prompt_id")
    mean_r = col("reward").mean().over(w).fill_null(daft.lit(0.0))
    std_r = col("reward").stddev().over(w).fill_null(daft.lit(0.0))

    out = df.with_column(
        "advantage",
        ((col("reward") - mean_r) / (std_r + daft.lit(1e-8))).fill_null(daft.lit(0.0)),
    )
    rows = out.collect().to_pylist()

    # Prompt 0 has rewards [0,1,1] => mean=2/3. Stddev is >0. Advantages should be ordered.
    p0 = [r for r in rows if r["_prompt_id"] == 0]
    assert len(p0) == 3
    # reward 0 should have the lowest advantage within its group
    assert min(r["advantage"] for r in p0) == p0[0]["advantage"]

    # Prompt 1 has rewards [2,0] => mean=1, symmetric advantages
    p1 = [r for r in rows if r["_prompt_id"] == 1]
    assert len(p1) == 2
    # Sum of standardized z-scores should be ~0 (numerical noise tolerated)
    assert abs(sum(r["advantage"] for r in p1)) < 1e-6
