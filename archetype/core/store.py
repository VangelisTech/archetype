from itertools import count as _count
from typing import Dict, Tuple, List, Type, Optional
from logging import getLogger
from hashlib import blake2b
import ulid 
from datetime import datetime, timezone
import os
import pyarrow as pa
import daft
from daft import col, DataFrame
from functools import lru_cache
import lancedb
from lancedb.table import AsyncTable, DATA
from lancedb.index import BTree
from .interfaces import Component, iStore

logger = getLogger(__name__)

# Partition keys are the keys that are used to partition the data in the table.
# We only partition by step because we want to be able to query the latest data for an archetype.
PARTITION_KEYS = ["simulation", "run", "step"]

# Data is the type of data that can be added to the table.

class BaseArchetypeTable(Component):
    simulation: str 
    run: str 
    entity_id: int 
    step: int
    is_active: bool
    
    
def _get_datetime_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # ISO 8601

class ArchetypeStore(iStore):
    
    async def __init__(self, 
        uri: str, 
        simulation: Optional[str] = None, # 
        run: Optional[str] = None,
        
    ):
        self.async_db = await lancedb.connect_async(uri, api_key=os.environ.get("LANCEDB_API_KEY"))
        self.simulation = simulation or f"sim_{_get_datetime_str()}"
        self.run = run or f"sim_{str(ulid.ULID())}"
        
        # Initialize internal properties
        self._latest_cache: Dict[str, DataFrame] = {} # Cache of the latest data for each archetype
        self._entity2sig: Dict[int, Tuple[Component, ...]] = {} # Necessary mapping for entity_id -> signature
        self._hash2sig: Dict[str, Tuple[Component, ...]] = {} # Convenience mapping for hash -> signature
        self._entity_counter = _count(start=1)

    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    
    @staticmethod
    def _sig_from_components(components: List[Component]) -> Tuple[Type[Component], ...]:
        # Get the signature of the components
        sig = tuple(sorted(type(c) for c in components))
        return sig
    
    @staticmethod
    def _create_archetype_hash(sig: Tuple[Type[Component], ...]) -> str:
        # Create a hash of the signature
        h = blake2b(digest_size=10)
        for comp in sig:
            h.update(comp.__name__.encode())
        hash = h.hexdigest()
        return f"archetype_{hash}"
    
    @staticmethod
    def _get_component_prefix(component_type: Type[Component]) -> str:
        """Generate a standardized prefix for a component type's fields."""
        return component_type.__name__.lower() + "__"
    
    
    def _build_archetype_schema(self, sig: Tuple[Type[Component], ...]) -> pa.Schema:
        """
        Get the schema for an archetype from a list of components.
        """
        archetype_schema = BaseArchetypeTable.to_arrow_schema()
        for component_type in sig:
            component_schema = component_type.to_arrow_schema()
            prefix = self._get_component_prefix(component_type)

            # Rename the fields of the component schema with the prefix
            for field_name in component_schema.names:
                field = component_schema.field_by_name(field_name)
                renamed_field = field.with_name(prefix + field_name)
                archetype_schema = archetype_schema.append(renamed_field)
            
        return archetype_schema
    
    async def _ensure_table(self, sig: Tuple[Component, ...]) -> AsyncTable:
        """
        Ensure that the table for the given archetype signature exists.
        """
        hash = self._create_archetype_hash(sig)
        schema = self._build_archetype_schema(sig)

        # Create the table if it doesn't exist
        if hash not in self.async_db.table_names():
            table = await self.async_db.create_table(
                hash, schema=schema, exist_ok=False
            )
            # Index the table
            table.create_index("entity_id", config=BTree(unique=False))
            table.create_index("step", config=BTree(unique=False))
        else:
            table = await self.async_db.open_table(hash)

        return table, hash

    #--------------------------------------------------------------------------
    # ComponentStoreInterface methods
    #--------------------------------------------------------------------------

    @lru_cache(maxsize=1000)
    def get_sig_from_entity(self, entity_id: int) -> Tuple[Component, ...]:
        return self._entity2sig[entity_id]

    async def add_entity(self, components: List[Component], step: int = 0) -> int:
        """
        Add an entity to the store.
        """
        if len(components) == 0:
            raise ValueError("Cannot create an entity with no components")
            
        # Get the next entity id
        entity_id = next(self._entity_counter)

        # Create the base archetype row
        base_archetype_dict = BaseArchetypeTable(
            simulation=self.simulation,
            run=self.run,
            entity_id=entity_id,
            step=step,
            is_active=True
        ).model_dump()

        # Create the entity data dict
        entity_data_dict = base_archetype_dict.copy()
        for component_instance in components:
            prefix = self._get_component_prefix(component_instance.__class__)
            component_dump = component_instance.model_dump()
            for key, value in component_dump.items():
                entity_data_dict[prefix + key] = value
        
        # Get the table and hash for the archetype
        sig = self._sig_from_components(components)
        table, hash = await self._ensure_table(sig)

        # Add the entity to the table
        await table.add(entity_data_dict)

        # Update the cache
        self._latest_cache[hash] = daft.from_arrow(table)


        return entity_id
    
    def remove_entity(self, entity_id: int, step: int = None) -> None:
        sig = self._entity2sig[entity_id]

        # Query the entity staet at the lastest step
        df = self.tables[sig] \
            .where(col("entity_id") == entity_id) \
            .where(col("step") == step) \
            .limit(1)

        # Set is_active to False at the given step
        df = df.with_column("is_active", col("is_active").lit(False))

        # Add the row to the df
        self.tables[sig] = self.tables[sig].concat(df)
        
        # Update the entity2sig mapping to pop the entity_id, keeping in mind its an lru_cache
        return self._entity2sig.pop(entity_id) #returns key error if not found

    # Wont support adding or removing components in-situ, only with entity creation and deletion. 
    # User's can work around this by creating a new entity with the desired components and deleting the old one in the same step.

    # ---------------------------------------------------------------------
    # Query helpers used by Processor._fetch_state and QueryManager
    # ---------------------------------------------------------------------

    def get_archetypes(self, *component_types: Type[Component]) -> Dict[str, DataFrame]:       
        if not component_types:
            raise ValueError("Must request at least one component type")

        # Get archetype dataframes that contain all of the requested component types (definition of an archetype is that all components must be present)
        hashes = [hash for hash in self._latest_cache.keys() if all(C in self._hash2sig[hash] for C in component_types)]
        
        # We need to return a dictionary of archetypes, keyed by the signature hash, in order for us to be able to return the transformations upon update 
        # Since signatures can become quite large,and we are passing this around a lot, we use the hash as the key
        archetypes = self._latest_cache[hashes] 

        # When we return the processed archetypes we can use the hash2sig to get the signature.
        # This allows us to keep the signatures small and manageable.
        return archetypes

    # ---------------------------------------------------------------------
    # Update Manager Interface
    # ---------------------------------------------------------------------

    async def upsert(self, sig: Tuple[Component, ...], data: DATA):
        table = await self._ensure_table(sig)
        # Upsert the data into the table
        await table.merge_insert(on=PARTITION_KEYS) \
            .when_not_matched_insert_all() \
            .execute(data)
        
        # Update the latest cache
        self._latest_cache[hash] = daft.from_arrow(table)

    async def update(self, sig: Tuple[Component, ...], data: DATA):
        table = await self._ensure_table(sig)
        # Update the data in the table
        await table.add(data, mode='append')

        self._latest_cache[hash] = daft.from_arrow(table)
