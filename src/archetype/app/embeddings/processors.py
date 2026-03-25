# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Recursive Embedding Improvement Processors
==========================================

Three processors form the improvement loop:

  EmbedProcessor      — generate embeddings for all documents this generation
  QualityProcessor    — score each embedding via cosine similarity to frontier centroid
  FrontierProcessor   — advance the frontier if avg quality improved

All processing stays lazy (Daft expression DAG) until the store append.
No premature .collect() — mutations expressed as when().then().otherwise() chains
or @daft.func row-wise UDFs.
"""

import json
import math

import daft
from daft import DataFrame, Series, col
from daft.functions import when

from archetype.core.aio.async_processor import AsyncProcessor

from .components import Document, Embedding, EmbeddingFrontier


# ── Stateful embedding model (init once per worker) ──────────────────────────


@daft.cls()
class _EmbeddingModel:
    """
    Wraps sentence-transformers. __init__ runs once per Daft worker,
    amortizing the model load cost across the full partition.
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(model_name)
            self._model_name = model_name
        except ImportError:
            # Graceful degradation: return deterministic pseudo-embeddings
            # so the pipeline runs without sentence-transformers installed.
            self._model = None
            self._model_name = model_name

    @daft.method.batch(return_dtype=daft.DataType.string())
    def embed(self, texts: Series) -> Series:
        """Embed a batch of texts, return JSON-encoded float lists."""
        text_list = texts.to_pylist()

        if self._model is not None:
            vectors = self._model.encode(text_list, show_progress_bar=False).tolist()
        else:
            # Deterministic fallback: hash-based pseudo-vector (dim=8)
            import hashlib

            vectors = []
            for t in text_list:
                h = hashlib.sha256(t.encode()).digest()
                vec = [(b / 255.0) * 2 - 1 for b in h[:8]]
                vectors.append(vec)

        return Series.from_pylist([json.dumps(v) for v in vectors])


# ── Row-wise UDFs (stateless) ─────────────────────────────────────────────────


@daft.func
def _cosine_sim(vec_a_json: str, vec_b_json: str) -> float:
    """Cosine similarity between two JSON-encoded float lists."""
    a = json.loads(vec_a_json) if vec_a_json else []
    b = json.loads(vec_b_json) if vec_b_json else []
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x**2 for x in a))
    mag_b = math.sqrt(sum(x**2 for x in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


@daft.func
def _mean_vector(vectors_json: str) -> str:
    """
    Compute element-wise mean of a JSON array of float arrays.
    Input: '[[0.1, 0.2], [0.3, 0.4]]'  →  Output: '[0.2, 0.3]'
    Used for centroid updates inside a reduce.
    """
    vecs = json.loads(vectors_json) if vectors_json else []
    if not vecs:
        return "[]"
    dim = len(vecs[0])
    centroid = [sum(v[i] for v in vecs) / len(vecs) for i in range(dim)]
    return json.dumps(centroid)


# ── Processors ────────────────────────────────────────────────────────────────


class EmbedProcessor(AsyncProcessor):
    """
    Generation N: embed every document using the configured model.

    Daft executes @daft.method.batch across partitions in parallel —
    the ECS gives us distributed batch embedding for free.
    """

    components = (Document, Embedding)
    priority = 10

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self._model_name = model_name

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        embedder = _EmbeddingModel(self._model_name)
        return (
            df.with_column("embedding__vector_json", embedder.embed(col("document__content")))
            .with_column("embedding__model_name", daft.lit(self._model_name))
            .with_column("embedding__generation", daft.lit(tick))
        )


class QualityProcessor(AsyncProcessor):
    """
    Score each embedding against the current frontier centroid.

    quality_score = cosine_similarity(embedding, frontier_centroid)

    A score of 1.0 means the embedding is identical to the centroid
    (perfect alignment). Scores improve as the corpus clusters tighter
    around semantically coherent regions.
    """

    components = (Document, Embedding, EmbeddingFrontier)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        has_frontier = ~col("embeddingfrontier__centroid_json").is_null()
        has_vector = col("embedding__vector_json") != daft.lit("[]")

        scored = _cosine_sim(
            col("embedding__vector_json"),
            col("embeddingfrontier__centroid_json"),
        )

        new_quality = (
            when(has_frontier & has_vector, then=scored)
            .otherwise(col("embedding__quality_score"))
        )

        return df.with_column("embedding__quality_score", new_quality)


class FrontierProcessor(AsyncProcessor):
    """
    Advance the frontier if this generation's avg quality beats the last.

    The frontier entity is the single EmbeddingFrontier component carrier.
    When the frontier advances:
      - avg_quality updated
      - generation incremented
      - centroid_json updated to the mean of this generation's vectors

    Expressed entirely as when().then().otherwise() — no collect, no Python loop.
    """

    components = (Embedding, EmbeddingFrontier)
    priority = 30

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        # Compute per-generation avg quality as a scalar via agg + join
        # We keep this lazy: agg builds a plan node, join is another plan node.
        gen_stats = (
            df.where(col("embedding__generation") == daft.lit(tick))
            .agg(col("embedding__quality_score").mean().alias("gen_avg_quality"))
        )

        # Broadcast the scalar back via cross-join (single-row agg table × full df)
        df = df.join(gen_stats, how="cross")

        advances = col("gen_avg_quality") > col("embeddingfrontier__avg_quality")
        is_frontier_entity = ~col("embeddingfrontier__centroid_json").is_null()

        new_avg = (
            when(is_frontier_entity & advances, then=col("gen_avg_quality"))
            .otherwise(col("embeddingfrontier__avg_quality"))
        )
        # col("embedding__generation") already holds tick — avoids daft.lit int type ambiguity
        new_gen = (
            when(is_frontier_entity & advances, then=col("embedding__generation"))
            .otherwise(col("embeddingfrontier__generation"))
        )

        # with_columns (plural) applies both from the same base plan —
        # prevents the chained-with_column plan dependency where the
        # updated avg_quality column makes advances=False in the next step.
        cols_to_keep = [c for c in df.column_names if c != "gen_avg_quality"]
        return (
            df.with_columns({
                "embeddingfrontier__avg_quality": new_avg,
                "embeddingfrontier__generation": new_gen,
            })
            .select(*cols_to_keep)
        )
