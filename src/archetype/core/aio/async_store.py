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

from typing import Optional, List, Dict, Tuple, Any, Set

import time
import os
import asyncio

import daft
from daft import DataFrame, col, lit
from daft.catalog import Catalog
from daft.io import IOConfig
from daft.io.object_store_options import io_config_to_storage_options

import lancedb
from lancedb.index import Bitmap, BTree

from archetype.core.aio.async_interfaces import iAsyncStore
from archetype.core.interfaces import Archetype, ArchetypeSignature

from logging import getLogger

logger = getLogger(__name__)


class AsyncStore(iAsyncStore):
    """
    The ArchetypeStore is a component that manages the storage and retrieval of archetype tables.

    Since our Schema supports multiple simulations and runs, our namespace is simply "archetypes".
    This allows us to have multiple simulations and runs in the same catalog since archetypes, by definition,
    the exact set of components attached to an entity. So we don't really which simulation or run we're using,
    so long as we differentiate between the simulation and run.

    Using Daft Sessions/Catalogs enables us to reference arbitrary
    numbers of archetype tables without having to hold them in memory, provided we

    """
    def __init__(self,
        uri: str,
        namespace: Optional[str] = None,
        catalog: Optional[Catalog] = None,
        io_config: Optional[IOConfig] = None,
        debug: bool = False,
    ):

        self.namespace = namespace or "archetypes"
        self.debug = debug
        self.uri = uri
        self.catalog = catalog
        self.io_config = io_config
        self.storage_options = io_config_to_storage_options(self.io_config) if self.io_config else None
        self.lancedb = None  # Initialize lancedb connection




    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    async def _ensure_table(self, sig: ArchetypeSignature) -> lancedb.AsyncTable:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        table_name = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)

        if self.lancedb is None:
            self.lancedb = await lancedb.connect_async(os.path.join(self.uri, self.namespace + "lance"))

        if table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
            except Exception as e:
                raise Exception(f"Error opening LanceDB table {table_name}: {e}")

            return async_table

        else:
            try:
                async_table = await self.lancedb.create_table(
                    name=table_name,
                    schema=pyarrow_schema,
                    storage_options= io_config_to_storage_options(self.io_config) if self.io_config else None,
                    exist_ok=True
                )
                await async_table.create_index(column="entity_id", config= BTree(), replace=True)
                await async_table.create_index(column="world_id", config= Bitmap(), replace=True)
                await async_table.create_index(column="run_id", config= Bitmap(), replace=True)
                await async_table.create_index(column="step", config= BTree(), replace=True)
            except Exception as e:
                logger.error(f"Error creating LanceDB table {table_name}: {e}")
                raise Exception(f"Error creating LanceDB table {table_name}: {e}")

        return async_table



    # ---------------------------------------------------------------------
    # Querying
    # ---------------------------------------------------------------------
    async def get_archetype_df(self, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame:

        table_name = Archetype.get_name(sig)
        async_table = await self._ensure_table(sig)

        try:
            # Use LanceDB query instead of daft.read_lance to avoid duplicate column errors
            lance_uri = os.path.join(self.uri, self.namespace + "lance", table_name + ".lance")
            if os.path.exists(lance_uri):
                # Query with LanceDB directly
                filtered_arrow = await async_table.query().where(
                    f"world_id = '{world_id}' AND run_id = '{run_id}' AND step = {step} AND is_active = true"
                ).to_arrow()

                # Convert to Daft DataFrame
                df = daft.from_arrow(filtered_arrow)
            else:
                # Return empty dataframe with correct schema if table doesn't exist yet
                schema = Archetype.get_archetype_schema(sig)
                df = daft.from_arrow(schema.empty_table())

        except Exception as e:
            logger.error(f"Error reading archetype table {table_name}: {e}")
            raise e

        return df

    #--------------------------------------------------------------------------
    # Updating
    #--------------------------------------------------------------------------

    async def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        """
        Materialize the spawn cache into the tables.
        """
        for sig, rows in spawn_cache.items():
            # Coerce List of PyDicts to PyArrow table
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            empty_table = pyarrow_schema.empty_table()
            arrow_table = empty_table.from_pylist(rows)
            df = daft.from_arrow(arrow_table)

            # Write to the table
            async_table = await self._ensure_table(sig)
            try:
                materialized_arrow_table = df.to_arrow()
                await async_table.add(materialized_arrow_table, mode="append")
                logger.debug(f"Appended {len(rows)} rows to table {async_table.name}")
            except Exception as e:
                logger.error(f"Error appending {len(rows)} rows to table {async_table.name}: {e}")
                raise

    async def remove_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        async_table = await self._ensure_table(sig) # Ensure table exists

        arrow_table = await async_table.to_arrow()
        df = daft.from_arrow(arrow_table)

        entity_df = df.where(
            (col("entity_id") == lit(entity_id)) &
            (col("step") == lit(step)) &
            (col("simulation") == lit(world_id)) &
            (col("run") == lit(run_id))
        )
        entity_df = entity_df.with_column(
            "is_active",
            lit(False)
        )
        try:
            await async_table.search(f"""
                UPDATE {async_table.name}
                SET is_active = FALSE
                WHERE entity_id = {entity_id}
                AND step = {step}
                AND world_id = '{world_id}'
                AND run_id = '{run_id}'
            """)
            logger.debug(f"Marked entity {entity_id} as inactive in archetype table {async_table.name} for step {step}.")
        except Exception as e:
            logger.error(f"Error marking entity {entity_id} as inactive in archetype table {async_table.name} for step {step}: {e}")
            raise

    async def append(self, sig: ArchetypeSignature, df: DataFrame, step: int, world_id: str, run_id: str) -> None:
        """
        Append a table with a new dataframe.
        """

        async_table = await self._ensure_table(sig)
        table_name = async_table.name
        try:
            start_time = time.time()

            # Update the step column to the new step value
            df_with_step = df.with_column("step", lit(step))

            # Convert to arrow and add to the table
            await async_table.add(df_with_step.to_arrow(), mode="append")
            end_time = time.time()
            logger.info(f"Appended dataframe to table {table_name} for step {step} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name} for step {step}: {e}")
            raise

        if self.debug:
            logger.info(f"Appending dataframe to table {table_name}")
            df_with_step.show()
            print(f"Appending {df_with_step.count_rows()} rows to table {table_name} for step {step}")

    async def optimize_tables(self) -> None:
        """
        Optimize all tables in the LanceDB catalog.
        """
        for table_name in await self.lancedb.table_names():
            try:
                async_table = await self.lancedb.open_table(table_name)
                async_table.optimize(retrain=False)
            except Exception as e:
                raise Exception(f"Error optimizing table {table_name}: {e}")




class AsyncEpisodeStore:
    """
    Episode-aware store that caches writes and periodically flushes to the underlying store.
    
    This implementation centralizes episode logic at the storage layer, making it transparent
    to worlds and systems. All archetype operations are cached in memory until flush.
    """
    
    def __init__(self, base_store: iAsyncStore, max_cache_size_mb: int = 500):
        self.base_store = base_store
        self.max_cache_size_mb = max_cache_size_mb
        
        # Episode cache: ArchetypeSignature -> accumulated DataFrame
        self.episode_cache: Dict[ArchetypeSignature, DataFrame] = {}
        
        # Track which signatures have been modified for efficient flushing
        self.dirty_signatures: Set[ArchetypeSignature] = set()
        
        # Episode statistics
        self.episode_count = 0
        self.total_cached_operations = 0
        
    async def get_archetype_df(self, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get archetype data, combining base store data with cached episode data.
        """
        # Check if we have cached updates for this signature
        cached_df = self.episode_cache.get(sig)
        
        if cached_df is not None:
            # Filter cached data to only include relevant world_id/run_id/step
            df = cached_df.where(
                (cached_df["world_id"] == world_id) & 
                (cached_df["run_id"] == run_id) &
                (cached_df["step"] == step)
            )
        else: 
            # No cached data found, query from persistent store
            df = await self.base_store.get_archetype_df(sig, step, world_id, run_id)


        return df
    
    async def append(self, sig: ArchetypeSignature, df: DataFrame, step: int, world_id: str, run_id: str) -> None:
        """
        Append data to the episode cache instead of directly to the store.
        """
        # Add metadata columns if they don't exist
        if "step" not in df.column_names:
            df = df.with_column("step", lit(step))
        if "world_id" not in df.column_names:
            df = df.with_column("world_id", lit(world_id))
        if "run_id" not in df.column_names:
            df = df.with_column("run_id", lit(run_id))
        
        # Accumulate in cache
        if sig in self.episode_cache:
            # Concat and collect to materialize the DataFrame and avoid lazy evaluation chains
            df_cached = self.episode_cache[sig]
            self.episode_cache[sig] = df_cached.concat(df).collect()
        else:
            # Collect the initial DataFrame to ensure it's materialized
            self.episode_cache[sig] = df.collect()
            
        # Track dirty signatures for efficient flushing
        self.dirty_signatures.add(sig)
        self.total_cached_operations += 1
        
        # Check if we should auto-flush based on cache size
        await self._check_auto_flush()
    
    async def remove_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
        """
        Mark an entity as inactive in the episode cache.
        """
        # Create a "tombstone" record to mark entity as inactive
        tombstone_df = daft.from_pydict({
            "entity_id": [entity_id],
            "step": [step],
            "world_id": [world_id],
            "run_id": [run_id],
            "is_active": [False]
        })
        
        await self.append(sig, tombstone_df, step, world_id, run_id)
    
    async def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        """
        Convert spawn cache to DataFrames and add to episode cache.
        """
        for sig, entity_dicts in spawn_cache.items():
            if entity_dicts:
                spawn_df = daft.from_pylist(entity_dicts)
                await self.append(sig, spawn_df, entity_dicts[0]["step"], world_id, run_id)
    
    async def flush_episodes(self, signatures: Optional[Set[ArchetypeSignature]] = None) -> Dict[str, Any]:
        """
        Flush episode cache to the underlying store.
        
        Args:
            signatures: Optional set of signatures to flush. If None, flushes all dirty signatures.
            
        Returns:
            Dictionary with flush statistics
        """
        flush_signatures = signatures or self.dirty_signatures.copy()
        
        if not flush_signatures:
            return {"flushed_signatures": 0, "total_records": 0}
        
        # Create flush tasks for parallel execution
        flush_tasks = []
        total_records = 0
        
        for sig in flush_signatures:
            if sig in self.episode_cache:
                cached_df = self.episode_cache[sig]
                total_records += len(cached_df)
                
                # Create a flush task for this signature
                flush_tasks.append(self._flush_signature(sig, cached_df))
        
        # Execute all flushes in parallel
        if flush_tasks:
            await asyncio.gather(*flush_tasks)
        
        # Clear flushed data from cache
        for sig in flush_signatures:
            self.episode_cache.pop(sig, None)
            self.dirty_signatures.discard(sig)
        
        self.episode_count += 1
        
        return {
            "flushed_signatures": len(flush_signatures),
            "total_records": total_records,
            "episode_count": self.episode_count
        }
    
    async def _flush_signature(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Flush a single signature's data to the base store.
        This can be overridden in subclasses to implement custom flush strategies.
        """
        # Group by world_id, run_id, step for efficient batch writes
        grouped = df.groupby(["world_id", "run_id", "step"]).agg([])
        
        # Process each group as a separate write operation
        for group in grouped.to_pylist():
            world_id = group["world_id"]
            run_id = group["run_id"] 
            step = group["step"]
            
            # Filter the dataframe to this specific group
            group_df = df.where(
                (df["world_id"] == world_id) & 
                (df["run_id"] == run_id) & 
                (df["step"] == step)
            )
            
            # Delegate to base store
            await self.base_store.append(sig, group_df, step, world_id, run_id)
    
    async def _check_auto_flush(self) -> None:
        """
        Check if we should automatically flush based on cache size or other criteria.
        """
        # Simple heuristic: flush if we have more than 1000 cached operations
        # In production, this could be based on actual memory usage
        if self.total_cached_operations > 1000:
            await self.flush_episodes()
            self.total_cached_operations = 0
    
    async def optimize_tables(self) -> None:
        """
        Optimize the underlying store tables.
        """
        await self.base_store.optimize_tables()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the current episode cache.
        """
        cache_signatures = len(self.episode_cache)
        total_cached_records = sum(len(df) for df in self.episode_cache.values())
        
        return {
            "cached_signatures": cache_signatures,
            "cached_records": total_cached_records,
            "dirty_signatures": len(self.dirty_signatures),
            "episode_count": self.episode_count,
            "total_operations": self.total_cached_operations
        }
