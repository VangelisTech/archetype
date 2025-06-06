from typing import Optional, List, Dict, Set, Tuple, Type, Any
import asyncio
from functools import lru_cache
import ulid 
from datetime import datetime, timezone

import time
import os

import daft
from daft import Schema, DataFrame, col, lit
from daft.catalog import Catalog, Table
from daft.session import Session
from daft.io import IOConfig
from daft.io.object_store_options import io_config_to_storage_options

import pyarrow as pa
import lancedb
from lancedb.index import Bitmap, BTree

from archetype.core.interfaces import ArchetypeSignature
from archetype.core.aio.async_interfaces import iAsyncStore
from archetype.core.base import Component
from archetype.core.store import (
    get_component_prefix, 
    sig_from_components, 
    sig2hash, 
    BaseArchetypeTableSchema, 
    get_datetime_str,
    convert_component_to_pyarrow_schema
)


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
        simulation: Optional[str] = None, 
        run: Optional[str] = None,
        namespace: Optional[str] = None,
        catalog: Optional[Catalog] = None,
        io_config: Optional[IOConfig] = None,
        debug: bool = False,
    ):
        
        self.simulation = simulation or f"sim_{get_datetime_str()}"
        self.run = run or f"run_{str(ulid.ULID())}"
        self.namespace = namespace or "archetypes"
        self.debug = debug
        self.uri = uri
        self.catalog = catalog
        self.io_config = io_config
        self.storage_options = io_config_to_storage_options(self.io_config) if self.io_config else None
        self.lancedb = None  # Initialize lancedb connection
        
        # Initialize internal properties
        from itertools import count
        self._entity2sig: Dict[int, ArchetypeSignature] = {} # Necessary mapping for entity_id -> archetype signature
        self._entity_counter = count(start=1)

        
    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    
    @lru_cache(maxsize=None, typed=True)
    def get_active_signatures(self) -> Set[ArchetypeSignature]:
        """
        Get all active signatures.
        """
        return set(self._entity2sig.values())
    
    
    
    @lru_cache(maxsize=None, typed=True)
    def _get_component_schema(self, component_type: Type[Component]) -> pa.Schema:
        component_schema = convert_component_to_pyarrow_schema(component_type)
        prefix = get_component_prefix(component_type)

        # Rename the fields of the component schema with the prefix
        for i, field_name in enumerate(component_schema.names):
            field = component_schema.field(field_name)
            renamed_field = field.with_name(prefix + field_name)
            component_schema = component_schema.set(i, renamed_field)

        return component_schema

    @lru_cache(maxsize=None, typed=True)
    def _get_archetype_schema(self, sig: ArchetypeSignature) -> pa.Schema:
        """
        Get the schema for an archetype from a list of components.
        """
    
        archetype_schema = BaseArchetypeTableSchema
        for component_type in sig:
            component_schema = self._get_component_schema(component_type)
            archetype_schema = pa.unify_schemas([archetype_schema, component_schema])
        
        return archetype_schema
    
    async def _ensure_lancedb(self) -> None:
        if self.lancedb is None:
            self.lancedb = await lancedb.connect_async(os.path.join(self.uri, self.namespace + "lance"))

    async def _ensure_table(self, sig: ArchetypeSignature) -> lancedb.AsyncTable:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        table_name = sig2hash(sig)
        pyarrow_schema = self._get_archetype_schema(sig)
        await self._ensure_lancedb()

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

    #--------------------------------------------------------------------------
    # iStore methods
    #--------------------------------------------------------------------------
    
    def add_entity(self, components: List[Component], step: int, world_id: str, run_id: str) -> int:
        """
        Add an entity to the store.
        """
        assert len(components) != 0, "Cannot create an entity with no components"
        
        # Get the entity id and signature
        entity_id = next(self._entity_counter)
        sig = sig_from_components(components)
        
        # Store entity mapping
        self._entity2sig[entity_id] = sig
        
        return entity_id

    async def remove_entity(self, entity_id: int, step: int, world_id: str, run_id: str) -> None:
        if entity_id not in self._entity2sig:
            logger.warn(f"Entity {entity_id} not found in _entity2sig. Cannot remove.")
            return
        
        sig = self._entity2sig[entity_id]
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

    async def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        """
        Materialize the spawn cache into the tables.
        """
        for sig, rows in spawn_cache.items():
            # Coerce List of PyDicts to PyArrow table 
            pyarrow_schema = self._get_archetype_schema(sig)
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
    # ---------------------------------------------------------------------
    # iQuerier methods 
    # ---------------------------------------------------------------------
    async def get_archetype_for_entity(self, entity_id: int, *component_types: Type[Component], world_id: str, run_id: str, step: int) -> DataFrame:  
        """
        Get all archetypes.
        """
        sig = self._entity2sig[entity_id]
        table_name = sig2hash(sig)
        async_table = await self.lancedb.open_table(table_name)
        
        # Use LanceDB query instead of daft.read_lance to avoid duplicate column errors
        lance_uri = os.path.join(self.uri, self.namespace + "lance", sig2hash(sig) + ".lance")
        if os.path.exists(lance_uri):
            # Query with LanceDB directly
            filtered_arrow = await async_table.query().where(
                f"world_id = '{world_id}' AND run_id = '{run_id}' AND step = {step} AND is_active = true AND entity_id = {entity_id}"
            ).to_arrow()
            
            # Convert to Daft DataFrame
            df = daft.from_arrow(filtered_arrow)
        else:
            # Return empty dataframe with correct schema if table doesn't exist yet
            schema = self._get_archetype_schema(sig)
            df = daft.from_arrow(schema.empty_table())
        
        return df
    
    async def get_archetype(self, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> Tuple[ArchetypeSignature, DataFrame]:
        """
        Get  archetypes using the entity2sig mapping for efficiency.
        Returns dict mapping archetype_hash -> (DataFrame, component_signature)
        This avoids expensive schema comparisons by using tracked signatures.
        """
        try:
            async_table = await self._ensure_table(sig)

            # Use LanceDB query instead of daft.read_lance to avoid duplicate column errors
            lance_uri = os.path.join(self.uri, self.namespace + "lance", sig2hash(sig) + ".lance")
            if os.path.exists(lance_uri):
                # Query with LanceDB directly
                filtered_arrow = await async_table.query().where(
                    f"world_id = '{world_id}' AND run_id = '{run_id}' AND step = {step} AND is_active = true"
                ).to_arrow()
                
                # Convert to Daft DataFrame
                df = daft.from_arrow(filtered_arrow)
            else:
                # Return empty dataframe with correct schema if table doesn't exist yet
                schema = self._get_archetype_schema(sig)
                df = daft.from_arrow(schema.empty_table())
            
        except Exception as e:
            logger.error(f"Error reading archetype table {sig2hash(sig)}: {e}")
            raise e
        
        return (sig, df)
    
    #--------------------------------------------------------------------------
    # iUpdater methods
    #--------------------------------------------------------------------------

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