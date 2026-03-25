# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS Components for the recursive embeddings improvement loop."""

from archetype.core.component import Component


class Document(Component):
    """A text chunk to be embedded."""

    doc_id: str = ""
    content: str = ""
    chunk_index: int = 0


class Embedding(Component):
    """The current embedding vector and quality metadata for a document."""

    doc_id: str = ""
    model_name: str = ""
    # JSON-encoded float list — Arrow cannot store list[float] directly
    vector_json: str = "[]"
    generation: int = 0
    quality_score: float = 0.0


class EmbeddingFrontier(Component):
    """
    Tracks the best-known embedding configuration.

    One entity per world carries this component. Each generation either
    advances the frontier (higher avg_quality) or is discarded.
    """

    model_name: str = ""
    avg_quality: float = 0.0
    generation: int = 0
    # JSON-encoded list[float] — centroid of all embeddings this generation
    centroid_json: str = "[]"
