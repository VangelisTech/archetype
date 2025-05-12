from itertools import count as _count
from typing import Dict, Tuple, List, Type, Optional
from logging import getLogger
from hashlib import blake2b
import ulid 
from datetime import datetime, timezone
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
    
    def __init__(self, 
        async_db_client: lancedb.AsyncConnection, # Changed from uri to async_db_client
        simulation: Optional[str] = None, 
        run: Optional[str] = None,
    ):
        self.async_db = async_db_client # Use the passed-in client
        self.simulation = simulation or f"sim_{_get_datetime_str()}"
        self.run = run or f"sim_{str(ulid.ULID())}"
        
        # Initialize internal properties
        self._entity2sig: Dict[int, Tuple[Type[Component], ...]] = {} # Necessary mapping for entity_id -> signature
        self._hash2sig: Dict[str, Tuple[Type[Component], ...]] = {} # Convenience mapping for hash -> signature
        self._entity_counter = _count(start=1)

    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    
    @staticmethod
    def _sig_from_components(components: List[Component]) -> Tuple[Type[Component], ...]:
        # Get the signature of the components by sorting their types by name
        component_types = [type(c) for c in components]
        sig = tuple(sorted(component_types, key=lambda t: t.__name__))
        return sig
    
    @staticmethod
    def _create_archetype_hash(sig: Tuple[Type[Component], ...]) -> str:
        # Create a hash of the signature
        h = blake2b(digest_size=10)
        for comp_type in sig:
            h.update(comp_type.__name__.encode())
        hash_val = h.hexdigest()
        return f"archetype_{hash_val}"
    
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
                field = component_schema.field(field_name)
                renamed_field = field.with_name(prefix + field_name)
                archetype_schema = archetype_schema.append(renamed_field)
            
        return archetype_schema
    
    async def _ensure_table(self, sig: Tuple[Type[Component], ...]) -> Tuple[AsyncTable, str]:
        """
        Ensure that the table for the given archetype signature exists.
        """
        hash_val = self._create_archetype_hash(sig)
        schema = self._build_archetype_schema(sig)

        # Create the table if it doesn't exist
        table_names = await self.async_db.table_names() # Await here
        if hash_val not in table_names:
            table = await self.async_db.create_table(
                hash_val, schema=schema, exist_ok=True # Failsafe to not overwrite existing table, that would be bad
            )
            await table.create_index("entity_id", config=BTree(), replace=True) 
            await table.create_index("step", config=BTree(), replace=True)

            self._hash2sig[hash_val] = sig # Set Once
        else:
            table = await self.async_db.open_table(hash_val)

        
        return table, hash_val

    #--------------------------------------------------------------------------
    # ComponentStoreInterface methods
    #--------------------------------------------------------------------------

    @lru_cache(maxsize=1000)
    def get_sig_from_entity(self, entity_id: int) -> Tuple[Type[Component], ...]:
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
        table, hash_val = await self._ensure_table(sig) # Use hash_val

        # Add the entity to the table
        # Assuming entity_data_dict is now correctly formatted for LanceDB
        # (e.g., a list of dicts, or a PyArrow table)
        await table.add([entity_data_dict]) # Ensure data is in a list or correct format

        self._entity2sig[entity_id] = sig # Store mapping
        return entity_id
    
    async def remove_entity(self, entity_id: int, step: int) -> None:
        if entity_id not in self._entity2sig:
            logger.error(f"Entity {entity_id} not found in _entity2sig. Cannot remove.")
            return 
        sig = self._entity2sig[entity_id]
        table, hash_val = await self._ensure_table(sig)
        await table.update(
            where=f"simulation == '{self.simulation}' AND run == '{self.run}' AND entity_id == {entity_id} AND step == {step}",
            updates={"is_active": False}
        )
        self._entity2sig.pop(entity_id, None)

    # Wont support adding or removing components in-situ, only with entity creation and deletion. 
    # User's can work around this by creating a new entity with the desired components and deleting the old one in the same step.

    # ---------------------------------------------------------------------
    # Query helpers used by Processor._fetch_state and QueryManager
    # ---------------------------------------------------------------------

    def _get_matching_signatures(self, component_types: Tuple[Type[Component], ...]) -> List[Tuple[Type[Component], ...]]:
        if not component_types:
            raise ValueError("Must request at least one component type")
        target_component_types = set(component_types) # Create a set for issubset
        matching_sigs = [
            sig for sig in self._hash2sig.values() 
            if target_component_types.issubset(set(sig))
        ]
        return matching_sigs

    async def get_archetypes(self, *component_types: Type[Component]) -> Dict[str, DataFrame]:  
        """
        Returns a dictionary of all archetypes whose component signatures include all of the specified component types.

        Args:
            *component_types: Type[Component]   The component types to get the archetypes for.

        Returns:
            archetypes: Dict[str, DataFrame]    A dictionary of archetype hashes to their latest data.
        """
        if not component_types:
            raise ValueError("Must request at least one component type")
        
        matching_sigs = self._get_matching_signatures(component_types)

        archetypes = {}
        for sig in matching_sigs:
            table, hash_val = await self._ensure_table(sig)
            arrow_table = await table.query(
                where=f"simulation == '{self.simulation}' AND run == '{self.run}'"
            ).to_arrow()
            try:
                # Filter and group by entity_id to get the latest step for each entity
                latest_df = daft.from_arrow(arrow_table)
            
            except Exception as e: # Broad exception for now, Lance/Daft might raise specific errors
                logger.error(f"Error reading lance table {table.name} for cache update: {e}")

            archetypes[hash_val] = latest_df
        
        return archetypes

    async def get_history(self, *component_types: Type[Component], include_all_runs: bool = False) -> DataFrame:
        """
        Get the full history of the given component types.
        """
        matching_sigs = self._get_matching_signatures(component_types)
        history = {}
        for sig in matching_sigs:
            table, hash_val = await self._ensure_table(sig)
            
            try: 
                await table.optimize()
            except Exception as e:
                logger.error(f"Error optimizing lance table {table.name}: {e}")


            try: 
                if include_all_runs:
                    arrow_table = await table.query(where=f"simulation == '{self.simulation}'").to_arrow()
                else:
                    arrow_table = await table.query(where=f"simulation == '{self.simulation}' AND run == '{self.run}'").to_arrow()

                df = daft.from_arrow(arrow_table)
            except Exception as e:
                logger.error(f"Error reading lance table {table.name} for cache update: {e}")

            history[hash_val] = df

        return history

    # ---------------------------------------------------------------------
    # Update Manager Interface
    # ---------------------------------------------------------------------

    async def upsert(self, sig: Tuple[Type[Component], ...], data: DATA, step: int):
        table, hash_val = await self._ensure_table(sig) # Use hash_val
        # Upsert the data into the table
        await table.merge_insert(on=["entity_id", "step", "run", "simulation"]) \
            .when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute(data)

    async def update(self, sig: Tuple[Type[Component], ...], data: DATA, step: int):
        table, hash_val = await self._ensure_table(sig) # Use hash_val
        # Update the data in the table
        await table.add(data, mode='append')


