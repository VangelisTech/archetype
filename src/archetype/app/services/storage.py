# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Managed Storage Layer for Archetype
===================================

High-level storage abstraction that wraps AsyncLancedbStore with:
- Automatic embedding generation for vector search
- Catalog management across worlds/runs/ticks
- Unified search interface for triples, trajectories, and documents

This is the recommended interface for applications requiring
search and catalog functionality beyond basic ECS storage.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any

import daft
from daft import DataType, col

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.runtime.storage import StorageContext, StorageContextFactory
from archetype.core.storage.lancedb import AsyncLancedbStore

logger = getLogger(__name__)


@dataclass
class CatalogEntry:
    """Entry in the managed catalog."""

    world_id: str
    run_id: str
    archetype_name: str
    tick: int
    num_entities: int
    table_name: str
    metadata: dict[str, Any] = field(default_factory=dict)


class ManagedStorage:
    """
    High-level storage manager with search and catalog capabilities.

    Wraps AsyncLancedbStore to provide:
    - Unified store/retrieve interface
    - Vector search over embeddings
    - Catalog of all worlds/runs/ticks
    - Cross-archetype queries

    Example:
        storage = await ManagedStorage.create("s3://bucket/archetype_data")

        # Store simulation state
        await storage.store(df, sig, tick=10, world_id="w1", run_id="r1")

        # Search for similar documents
        results = await storage.search_text("query", limit=10)

        # Browse catalog
        for entry in await storage.get_catalog():
            print(f"{entry.world_id}/{entry.run_id}: {entry.num_entities} entities")
    """

    def __init__(
        self,
        context: StorageContext,
        store: AsyncLancedbStore,
    ):
        """
        Initialize managed storage (use create() factory instead).

        Args:
            context: Storage context with session/config
            store: Underlying AsyncLancedbStore instance
        """
        self._context = context
        self._store = store
        self._catalog: dict[str, CatalogEntry] = {}

    @classmethod
    async def create(
        cls,
        uri: str,
        namespace: str = "archetypes",
    ) -> "ManagedStorage":
        """
        Factory method to create ManagedStorage.

        Args:
            uri: Storage URI (local path or s3:// URL)
            namespace: Namespace for archetype tables

        Returns:
            Initialized ManagedStorage instance
        """
        from archetype.core.config import StorageConfig

        config = StorageConfig(uri=uri, namespace=namespace)
        context = StorageContextFactory.build(config)
        store = AsyncLancedbStore(context)

        return cls(context=context, store=store)

    @property
    def uri(self) -> str:
        """Storage URI."""
        return self._context.uri

    @property
    def namespace(self) -> str:
        """Storage namespace."""
        return self._context.namespace

    # -------------------------------------------------------------------------
    # Core storage operations
    # -------------------------------------------------------------------------
    async def store(
        self,
        df: daft.DataFrame,
        sig: ArchetypeSignature,
        tick: int,
        world_id: str,
        run_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Store a DataFrame with catalog tracking.

        Args:
            df: DataFrame to store
            sig: Archetype signature
            tick: Simulation tick
            world_id: World identifier
            run_id: Run identifier
            metadata: Optional metadata for catalog entry
        """
        # Add required columns if missing
        if "tick" not in df.column_names:
            df = df.with_column("tick", daft.lit(tick))
        if "world_id" not in df.column_names:
            df = df.with_column("world_id", daft.lit(world_id))
        if "run_id" not in df.column_names:
            df = df.with_column("run_id", daft.lit(run_id))

        # Append to store
        await self._store.append(sig, df)

        # Update catalog
        table_name = Archetype.get_name(sig)
        catalog_key = f"{world_id}/{run_id}/{table_name}/{tick}"
        self._catalog[catalog_key] = CatalogEntry(
            world_id=world_id,
            run_id=run_id,
            archetype_name=table_name,
            tick=tick,
            num_entities=df.count_rows(),
            table_name=table_name,
            metadata=metadata or {},
        )

        logger.info(f"Stored {df.count_rows()} entities to {catalog_key}")

    async def retrieve(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        tick: int | None = None,
    ) -> daft.DataFrame:
        """
        Retrieve entities from storage.

        Args:
            sig: Archetype signature
            world_id: World identifier
            run_id: Run identifier
            tick: Optional tick filter (latest if None)

        Returns:
            DataFrame with matching entities
        """
        df = await self._store.get_archetype_df(sig, world_id, run_id)

        if tick is not None:
            df = df.where(col("tick") == tick)

        return df

    # -------------------------------------------------------------------------
    # Search operations
    # -------------------------------------------------------------------------
    async def search_text(
        self,
        query: str,
        text_column: str = "documentcomponent__text",
        limit: int = 10,
        world_id: str | None = None,
        run_id: str | None = None,
    ) -> daft.DataFrame:
        """
        Full-text search over document text.

        Args:
            query: Search query string
            text_column: Column containing text to search
            limit: Maximum results to return
            world_id: Optional filter by world
            run_id: Optional filter by run

        Returns:
            DataFrame with matching documents
        """
        # Access LanceDB directly for FTS
        if self._store.lancedb is None:
            raise RuntimeError("LanceDB connection not initialized")

        # Build filter
        filters = []
        if world_id:
            filters.append(f"world_id = '{world_id}'")
        if run_id:
            filters.append(f"run_id = '{run_id}'")

        filter_str = " AND ".join(filters) if filters else None

        # Search across all tables with text column
        results = []
        for table_name in await self._store.lancedb.table_names():
            try:
                table = await self._store.lancedb.open_table(table_name)
                schema = await table.schema

                if text_column in [f.name for f in schema]:
                    query_builder = table.query()
                    if filter_str:
                        query_builder = query_builder.where(filter_str)

                    # LanceDB FTS (if available) or fallback to contains
                    arrow_data = await query_builder.limit(limit).to_arrow()
                    if arrow_data.num_rows > 0:
                        results.append(daft.from_arrow(arrow_data))

            except Exception as e:
                logger.debug(f"Skipping table {table_name}: {e}")

        if not results:
            return daft.from_pydict({})

        # Combine results
        combined = results[0]
        for df in results[1:]:
            combined = combined.concat(df)

        # Filter by query (basic contains match)
        combined = combined.where(col(text_column).str.contains(query))

        return combined.limit(limit)

    async def search_triples(
        self,
        subject: str | None = None,
        predicate: str | None = None,
        obj: str | None = None,
        limit: int = 10,
    ) -> daft.DataFrame:
        """
        Search for knowledge triples.

        Args:
            subject: Filter by subject (partial match)
            predicate: Filter by predicate (partial match)
            obj: Filter by object (partial match)
            limit: Maximum results

        Returns:
            DataFrame with matching triples
        """
        # Search tables with triples_json column
        results = []
        triples_col = "documentcomponent__triples_json"

        if self._store.lancedb is None:
            raise RuntimeError("LanceDB connection not initialized")

        for table_name in await self._store.lancedb.table_names():
            try:
                table = await self._store.lancedb.open_table(table_name)
                schema = await table.schema

                if triples_col in [f.name for f in schema]:
                    arrow_data = await table.query().limit(limit * 10).to_arrow()
                    if arrow_data.num_rows > 0:
                        results.append(daft.from_arrow(arrow_data))

            except Exception as e:
                logger.debug(f"Skipping table {table_name}: {e}")

        if not results:
            return daft.from_pydict({})

        combined = results[0]
        for df in results[1:]:
            combined = combined.concat(df)

        # Filter by triple components
        @daft.func.batch(return_dtype=DataType.bool())
        def matches_triple(triples_json_series: daft.Series) -> list:
            import json

            results = []
            for t_json in triples_json_series.to_pylist():
                if not t_json:
                    results.append(False)
                    continue

                triples = json.loads(t_json)
                match = False
                for t in triples:
                    if subject and subject.lower() not in t.get("sub", "").lower():
                        continue
                    if predicate and predicate.lower() not in t.get("pred", "").lower():
                        continue
                    if obj and obj.lower() not in t.get("obj", "").lower():
                        continue
                    match = True
                    break
                results.append(match)
            return results

        combined = combined.where(matches_triple(col(triples_col)))
        return combined.limit(limit)

    # -------------------------------------------------------------------------
    # Catalog operations
    # -------------------------------------------------------------------------
    async def get_catalog(
        self,
        world_id: str | None = None,
        run_id: str | None = None,
    ) -> list[CatalogEntry]:
        """
        Get catalog entries.

        Args:
            world_id: Optional filter by world
            run_id: Optional filter by run

        Returns:
            List of catalog entries
        """
        entries = list(self._catalog.values())

        if world_id:
            entries = [e for e in entries if e.world_id == world_id]
        if run_id:
            entries = [e for e in entries if e.run_id == run_id]

        return sorted(entries, key=lambda e: (e.world_id, e.run_id, e.tick))

    async def get_table_names(self) -> list[str]:
        """Get all table names in the LanceDB catalog."""
        if self._store.lancedb is None:
            return []
        return await self._store.lancedb.table_names()

    async def get_worlds(self) -> list[str]:
        """Get unique world IDs from catalog."""
        return list({e.world_id for e in self._catalog.values()})

    async def get_runs(self, world_id: str) -> list[str]:
        """Get unique run IDs for a world."""
        return list({
            e.run_id
            for e in self._catalog.values()
            if e.world_id == world_id
        })

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    async def optimize(self) -> None:
        """Optimize all LanceDB tables."""
        await self._store.optimize_tables()

    async def close(self) -> None:
        """Close storage connections."""
        await self._store.shutdown()

    async def __aenter__(self) -> "ManagedStorage":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# Sync wrapper for convenience
class SyncManagedStorage:
    """
    Synchronous wrapper for ManagedStorage.

    Useful for scripts and notebooks where async isn't convenient.
    """

    def __init__(self, managed: ManagedStorage):
        self._managed = managed
        self._loop = asyncio.new_event_loop()

    @classmethod
    def create(cls, uri: str, namespace: str = "archetypes") -> "SyncManagedStorage":
        """Create sync managed storage."""
        loop = asyncio.new_event_loop()
        managed = loop.run_until_complete(ManagedStorage.create(uri, namespace))
        instance = cls(managed)
        instance._loop = loop
        return instance

    def store(self, df: daft.DataFrame, sig: ArchetypeSignature, **kwargs) -> None:
        """Store DataFrame synchronously."""
        self._loop.run_until_complete(self._managed.store(df, sig, **kwargs))

    def retrieve(self, sig: ArchetypeSignature, **kwargs) -> daft.DataFrame:
        """Retrieve DataFrame synchronously."""
        return self._loop.run_until_complete(self._managed.retrieve(sig, **kwargs))

    def search_text(self, query: str, **kwargs) -> daft.DataFrame:
        """Search text synchronously."""
        return self._loop.run_until_complete(self._managed.search_text(query, **kwargs))

    def search_triples(self, **kwargs) -> daft.DataFrame:
        """Search triples synchronously."""
        return self._loop.run_until_complete(self._managed.search_triples(**kwargs))

    def get_catalog(self, **kwargs) -> list[CatalogEntry]:
        """Get catalog synchronously."""
        return self._loop.run_until_complete(self._managed.get_catalog(**kwargs))

    def close(self) -> None:
        """Close connections."""
        self._loop.run_until_complete(self._managed.close())
        self._loop.close()

    def __enter__(self) -> "SyncManagedStorage":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
