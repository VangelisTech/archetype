# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
MemoryService — persistent, deduplicated agent memory.

One world per MemoryService instance. All sessions share it;
session_id is a field on MemoryChunk for filtering.

Usage:
    svc = MemoryService(container)
    await svc.setup()

    chunk_id = await svc.ingest(session_id="s1", text="...", source="conversation")
    results  = await svc.query(session_id="s1", q="...", top_k=5)
    chunks   = await svc.list_chunks(session_id="s1")
"""

from __future__ import annotations

import json
import logging
from typing import Any

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .components import MemoryChunk
from .processors import JACCARD_THRESHOLD, NUM_HASHES, NGRAM_SIZE, DedupeProcessor, MinHashProcessor

logger = logging.getLogger(__name__)


class MemoryService:
    """
    Manages agent memory: ingest → MinHash dedupe → similarity retrieval.

    Backed by a dedicated Archetype world whose state persists in LanceDB.
    """

    def __init__(self, container, storage_uri: str = "./memory_data"):
        self._container = container
        self._storage_uri = storage_uri
        self._world = None
        self._world_id = None
        self._ctx = ActorCtx(id=uuid7(), roles={"admin"})

    async def setup(self) -> None:
        """Initialize the memory world with dedupe processors."""
        if self._world is not None:
            return

        world = await self._container.world_service.create_world(
            WorldConfig(name="memory"),
            StorageConfig(uri=self._storage_uri, namespace="memory"),
        )
        await world.system.add_processor(MinHashProcessor())
        await world.system.add_processor(DedupeProcessor())

        self._world = world
        self._world_id = world.world_id

    async def ingest(
        self,
        session_id: str,
        text: str,
        source: str = "unknown",
    ) -> dict[str, Any]:
        """
        Ingest a text chunk into memory.

        Runs MinHash + dedupe on the next tick.
        Returns the chunk metadata including whether it was marked duplicate.
        """
        await self.setup()

        chunk_id = str(uuid7())
        chunk = MemoryChunk(
            chunk_id=chunk_id,
            session_id=session_id,
            content=text,
            source=source,
        )

        # Include "type" key so Component.from_dict() hydrates the correct subclass.
        # Without it, from_dict() returns a base Component and processors can't match.
        chunk_dict = {"type": "MemoryChunk", **chunk.model_dump()}
        await self._container.command_service.submit(
            self._world_id,
            Command(
                type=CommandType.SPAWN,
                payload={"components": [chunk_dict]},
            ),
            self._ctx,
        )

        # prefer_live_reads=True: use in-memory _live snapshot instead of querying
        # LanceDB by run_id. Without this, each ingest sees only its own tick's
        # entities because each call creates a fresh run_id.
        await self._container.simulation_service.run(
            self._world_id, RunConfig(num_steps=1, prefer_live_reads=True)
        )

        # Read back the entity we just created
        row = self._find_chunk(chunk_id)
        return row or {"chunk_id": chunk_id, "session_id": session_id, "is_duplicate": False}

    async def query(
        self,
        session_id: str,
        q: str,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Retrieve the top-K most similar non-duplicate chunks for a query.

        Similarity is estimated via MinHash Jaccard.
        Returns chunks sorted by descending similarity.
        """
        await self.setup()

        # Compute query signature in Python (single string, no Daft overhead)
        import daft
        from daft.functions import minhash as _minhash

        q_df = daft.from_pydict({"text": [q]})
        q_sig_row = q_df.with_column(
            "sig", _minhash(daft.col("text"), num_hashes=NUM_HASHES, ngram_size=NGRAM_SIZE, seed=42)
        ).collect().to_pylist()[0]
        q_sig = list(q_sig_row["sig"]) if q_sig_row.get("sig") is not None else []

        if not q_sig:
            return []

        # Collect all non-duplicate chunks for this session
        chunks = self._list_active_chunks(session_id)
        if not chunks:
            return []

        # Score each chunk
        scored = []
        for chunk in chunks:
            sig_json = chunk.get("memorychunk__sig_json", "[]")
            sig = json.loads(sig_json) if sig_json and sig_json != "[]" else []
            if not sig or len(sig) != len(q_sig):
                continue
            matches = sum(1 for a, b in zip(q_sig, sig) if a == b)
            jaccard = matches / len(q_sig)
            scored.append({**self._flatten(chunk), "query_similarity": jaccard})

        scored.sort(key=lambda x: x["query_similarity"], reverse=True)
        return scored[:top_k]

    async def list_chunks(self, session_id: str | None = None) -> list[dict[str, Any]]:
        """List all non-duplicate chunks, optionally filtered by session."""
        await self.setup()
        chunks = self._list_active_chunks(session_id)
        return [self._flatten(c) for c in chunks]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _all_chunk_rows(self) -> list[dict]:
        """Collect all MemoryChunk rows from the live snapshot."""
        if self._world is None:
            return []
        rows = []
        for sig, df in self._world._live.items():
            if not any(c.__name__ == "MemoryChunk" for c in sig):
                continue
            rows.extend(df.collect().to_pylist())
        return rows

    def _find_chunk(self, chunk_id: str) -> dict | None:
        for row in self._all_chunk_rows():
            if row.get("memorychunk__chunk_id") == chunk_id:
                return self._flatten(row)
        return None

    def _list_active_chunks(self, session_id: str | None) -> list[dict]:
        rows = self._all_chunk_rows()
        return [
            r for r in rows
            if not r.get("memorychunk__is_duplicate", False)
            and (session_id is None or r.get("memorychunk__session_id") == session_id)
        ]

    @staticmethod
    def _flatten(row: dict) -> dict:
        """Strip component prefix, return clean dict."""
        prefix = "memorychunk__"
        return {
            k[len(prefix):] if k.startswith(prefix) else k: v
            for k, v in row.items()
            if not k.startswith(("world_id", "run_id", "tick", "is_active", "entity_id"))
        }
