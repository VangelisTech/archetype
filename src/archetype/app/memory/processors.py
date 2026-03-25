# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Memory processors — MinHash signature + deduplication.

Pipeline (in priority order):
  MinHashProcessor  (10) — compute MinHash sig for every chunk
  DedupeProcessor   (20) — self-join on band keys, mark duplicates
"""

import json

import daft
from daft import DataFrame, Series, col
from daft.functions import minhash, when

from archetype.core.aio.async_processor import AsyncProcessor

from .components import MemoryChunk

# ── Config ────────────────────────────────────────────────────────────────────

NUM_HASHES = 128
NGRAM_SIZE = 3
BANDS = 16          # NUM_HASHES / BANDS = rows per band (8)
ROWS_PER_BAND = NUM_HASHES // BANDS
JACCARD_THRESHOLD = 0.8


# ── Row-wise UDFs ─────────────────────────────────────────────────────────────


@daft.func
def _list_to_json(sig_list) -> str:
    """Serialize a List[uint32] signature to a JSON string for storage."""
    if sig_list is None:
        return "[]"
    return json.dumps(list(sig_list))


@daft.func
def _jaccard_from_json(sig_a_json: str, sig_b_json: str) -> float:
    """Estimate Jaccard similarity from two JSON-encoded MinHash signatures."""
    a = json.loads(sig_a_json) if sig_a_json and sig_a_json != "[]" else []
    b = json.loads(sig_b_json) if sig_b_json and sig_b_json != "[]" else []
    if not a or not b or len(a) != len(b):
        return 0.0
    matches = sum(1 for x, y in zip(a, b) if x == y)
    return matches / len(a)


@daft.func.batch(return_dtype=daft.DataType.string())
def _band_keys(sig_json_col: Series) -> Series:
    """
    Compute LSH band keys for each signature.

    Returns JSON string: list of (band_idx, hash_tuple) pairs.
    Rows with matching ANY band key are candidate duplicates (prob ~ 1 at threshold).
    """
    results = []
    for sig_json in sig_json_col.to_pylist():
        sig = json.loads(sig_json) if sig_json and sig_json != "[]" else []
        if not sig or len(sig) < NUM_HASHES:
            results.append("[]")
            continue
        bands = []
        for b in range(BANDS):
            start = b * ROWS_PER_BAND
            band_hash = tuple(sig[start : start + ROWS_PER_BAND])
            bands.append((b, band_hash))
        results.append(json.dumps(bands))
    return Series.from_pylist(results)


# ── Processors ────────────────────────────────────────────────────────────────


class MinHashProcessor(AsyncProcessor):
    """
    Compute MinHash signatures for all MemoryChunk entities.

    Writes the signature as a JSON string to sig_json.
    Only processes chunks that don't have a signature yet (sig_json == "[]").
    """

    components = (MemoryChunk,)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        needs_sig = col("memorychunk__sig_json") == daft.lit("[]")

        raw_sig = minhash(
            col("memorychunk__content"),
            num_hashes=NUM_HASHES,
            ngram_size=NGRAM_SIZE,
            seed=42,
        )
        sig_json = _list_to_json(raw_sig)

        new_sig = when(needs_sig, then=sig_json).otherwise(col("memorychunk__sig_json"))
        return df.with_column("memorychunk__sig_json", new_sig)


class DedupeProcessor(AsyncProcessor):
    """
    Mark duplicate MemoryChunk entities using MinHash band-LSH.

    Strategy:
    1. Compute band keys for all chunks
    2. Self-join on (session_id, band_key) to find candidate pairs
    3. Compute Jaccard similarity for candidate pairs
    4. Mark the later chunk (higher entity_id) as duplicate, point to the earlier one

    Chunks with no signature or already marked duplicate are skipped.
    """

    components = (MemoryChunk,)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Skip already-marked duplicates and unsigned chunks
        active = df.where(
            ~col("memorychunk__is_duplicate")
            & (col("memorychunk__sig_json") != daft.lit("[]"))
        )

        if active.count_rows() < 2:
            return df

        # Collect signatures — pairwise O(n²) within session
        # Acceptable: agent session memory is small by design
        dup_map = self._find_duplicates(active)

        if not dup_map:
            return df

        # Mark duplicates via join + when/then/otherwise (proven pattern)
        # Cast entity_id to int32 to match Archetype's BASE_SCHEMA (int32),
        # which from_pydict would otherwise infer as int64 — causing join mismatches.
        dup_df = daft.from_pydict({
            "entity_id": list(dup_map.keys()),
            "new_canonical_id": [v[0] for v in dup_map.values()],
            "new_similarity_score": [float(v[1]) for v in dup_map.values()],
        }).with_column("entity_id", daft.col("entity_id").cast(daft.DataType.int32()))

        original_cols = df.column_names
        return (
            df.join(dup_df, on="entity_id", how="left", suffix="_dup")
            .with_columns({
                "memorychunk__is_duplicate": when(
                    ~col("new_canonical_id").is_null(), then=daft.lit(True)
                ).otherwise(col("memorychunk__is_duplicate")),
                "memorychunk__canonical_id": when(
                    ~col("new_canonical_id").is_null(), then=col("new_canonical_id")
                ).otherwise(col("memorychunk__canonical_id")),
                "memorychunk__similarity_score": when(
                    ~col("new_similarity_score").is_null(), then=col("new_similarity_score")
                ).otherwise(col("memorychunk__similarity_score")),
            })
            .select(*original_cols)
        )

    def _find_duplicates(self, df: DataFrame) -> dict[int, tuple[str, float]]:
        """
        Return {entity_id: (canonical_chunk_id, jaccard_score)} for duplicates.

        Collects to Python — dataset is agent session memory (small by design).
        """
        rows = df.select(
            "entity_id",
            "memorychunk__chunk_id",
            "memorychunk__session_id",
            "memorychunk__sig_json",
        ).collect().to_pylist()

        # Group by session_id
        by_session: dict[str, list[dict]] = {}
        for row in rows:
            sid = row["memorychunk__session_id"]
            by_session.setdefault(sid, []).append(row)

        duplicates: dict[int, tuple[str, float]] = {}

        for session_rows in by_session.values():
            # O(n²) pairwise within session — fine for memory scale
            for i, a in enumerate(session_rows):
                if a["entity_id"] in duplicates:
                    continue
                sig_a = json.loads(a["memorychunk__sig_json"])
                for b in session_rows[i + 1 :]:
                    if b["entity_id"] in duplicates:
                        continue
                    sig_b = json.loads(b["memorychunk__sig_json"])
                    if not sig_a or not sig_b:
                        continue
                    matches = sum(1 for x, y in zip(sig_a, sig_b) if x == y)
                    jaccard = matches / len(sig_a)
                    if jaccard >= JACCARD_THRESHOLD:
                        # Mark b as duplicate of a (a has lower entity_id → was first)
                        duplicates[b["entity_id"]] = (a["memorychunk__chunk_id"], jaccard)

        return duplicates
