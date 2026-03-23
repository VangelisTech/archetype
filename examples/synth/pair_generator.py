# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Generate contrastive triplets from labeled segments."""

import random

import daft


def generate_triplets(
    df: daft.DataFrame,
    label_col: str,
    text_col: str = "segment__content",
    min_per_group: int = 2,
    max_triplets_per_anchor: int = 3,
    seed: int = 42,
) -> daft.DataFrame:
    rng = random.Random(seed)
    rows = df.select(text_col, label_col).collect().to_pylist()

    groups: dict[str, list[str]] = {}
    for row in rows:
        label = row[label_col]
        text = row[text_col]
        if label and text:
            groups.setdefault(label, []).append(text)

    groups = {k: v for k, v in groups.items() if len(v) >= min_per_group}
    if len(groups) < 2:
        return daft.from_pydict(
            {"anchor_text": [], "positive_text": [], "negative_text": [], "label_source": []}
        )

    all_labels = list(groups.keys())
    triplets = []

    for label, texts in groups.items():
        other_labels = [la for la in all_labels if la != label]
        negatives = [t for la in other_labels for t in groups[la]]

        for anchor in texts:
            positives = [t for t in texts if t != anchor]
            if not positives or not negatives:
                continue
            for _ in range(min(max_triplets_per_anchor, len(positives))):
                triplets.append({
                    "anchor_text": anchor,
                    "positive_text": rng.choice(positives),
                    "negative_text": rng.choice(negatives),
                    "label_source": label_col,
                })

    if not triplets:
        return daft.from_pydict(
            {"anchor_text": [], "positive_text": [], "negative_text": [], "label_source": []}
        )

    return daft.from_pydict({k: [t[k] for t in triplets] for k in triplets[0]})
