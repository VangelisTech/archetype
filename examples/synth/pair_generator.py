# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generate contrastive triplets from labeled segments."""

from __future__ import annotations

from collections.abc import Iterator

import daft
from daft import DataType, col, lit

_EMPTY_TRIPLETS = {
    "anchor_text": [],
    "positive_text": [],
    "negative_text": [],
    "label_source": [],
}


@daft.func(
    return_dtype=DataType.struct({
        "positive_text": DataType.string(),
        "negative_text": DataType.string(),
    }),
    unnest=True,
)
def _sample_pairs(
    anchor: str,
    positives: list[str],
    negatives: list[str],
    max_per: int,
    seed: int,
) -> Iterator[dict]:
    """Generator func: 1 anchor → N triplet rows (capped by max_per)."""
    import random

    rng = random.Random(seed + hash(anchor))
    pos = [p for p in positives if p != anchor]
    if not pos or not negatives:
        return
    for _ in range(min(max_per, len(pos))):
        yield {"positive_text": rng.choice(pos), "negative_text": rng.choice(negatives)}


def generate_triplets(
    df: daft.DataFrame,
    label_col: str,
    text_col: str = "segment__content",
    min_per_group: int = 2,
    max_triplets_per_anchor: int = 3,
    seed: int = 42,
) -> daft.DataFrame:
    # Step 1: Group by label — one row per label with aggregated text list
    valid = df.where(col(text_col) != "").where(col(label_col) != "")
    grouped = valid.groupby(label_col).agg(
        col(text_col).list_agg().alias("__group_texts"),
        col(text_col).count().alias("__cnt"),
    )
    grouped = grouped.where(col("__cnt") >= min_per_group)

    # Step 2: Build negative pool per label (one small collect — label-level, not row-level)
    label_rows = grouped.select(label_col, "__group_texts").collect().to_pylist()
    if len(label_rows) < 2:
        return daft.from_pydict(_EMPTY_TRIPLETS)

    neg_pool: dict[str, list[str]] = {}
    for row in label_rows:
        negs: list[str] = []
        for other in label_rows:
            if other[label_col] != row[label_col]:
                negs.extend(other["__group_texts"])
        neg_pool[row[label_col]] = negs

    neg_df = daft.from_pydict({
        "__neg_label": list(neg_pool.keys()),
        "__neg_texts": list(neg_pool.values()),
    })

    # Step 3: Join anchors with their group's positives + cross-label negatives
    anchor_df = (
        grouped.select(col(label_col), col("__group_texts"))
        .join(valid, on=label_col, how="inner")
        .join(neg_df, left_on=label_col, right_on="__neg_label", how="inner")
    )

    # Step 4: Generator func emits sampled triplets per anchor (no cross-join explosion)
    return anchor_df.select(
        col(text_col).alias("anchor_text"),
        _sample_pairs(
            col(text_col), col("__group_texts"), col("__neg_texts"),
            max_triplets_per_anchor, seed,
        ),
        lit(label_col).alias("label_source"),
    )
