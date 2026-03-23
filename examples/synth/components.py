# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS Components for the synthetic data engine."""

from lancedb.pydantic import Vector

from archetype.core.component import Component


class Triplet(Component):
    anchor_text: str = ""
    positive_text: str = ""
    negative_text: str = ""
    label_source: str = ""


class Embedding(Component):
    vector: Vector(128) = [0.0] * 128  # type: ignore[valid-type]
    model_version: str = ""


class Cluster(Component):
    cluster_id: int = -1
    centroid_distance: float = 0.0


class Neighbors(Component):
    neighbor_ids: list[str] = []
    distances: list[float] = []


class Anomaly(Component):
    outlier_score: float = 0.0


class Drift(Component):
    divergence: float = 0.0
    window: str = ""


class TrainingMetric(Component):
    loss: float = 0.0
    epoch: int = 0
    num_triplets: int = 0
    model_version: str = ""
