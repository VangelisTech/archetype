from itertools import count as _count
from typing import Dict, Tuple, List, Type, Optional, Union
from logging import getLogger
from hashlib import blake2b
import os
import ulid 
from datetime import datetime, timezone, timedelta

# Technologies
import pyarrow as pa
import daft
from daft import col, DataFrame
from daft.expressions import lit
from daft.session import Session
from daft import Catalog, Table
from .interfaces import Component, iStore

logger = getLogger(__name__)

# Partition keys are the keys that are used to partition the data in the table.
# We only partition by step because we want to be able to query the latest data for an archetype.



BaseArchetypeTableSchema = pa.schema([
    pa.field("simulation", pa.string(), nullable=False),
    pa.field("run", pa.string(), nullable=False),
    pa.field("entity_id", pa.uint64(), nullable=False),
    pa.field("step", pa.uint64(), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
])
PARTITION_KEYS = ["simulation", "run", "step"]
    
    
def _get_datetime_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # ISO 8601

class ArchetypeStore(iStore):
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
        simulation: Optional[str] = None, 
        run: Optional[str] = None,
        catalog: Optional[Catalog] = None,
    ):
        
        self.simulation = simulation or f"sim_{_get_datetime_str()}"
        self.run = run or f"run_{str(ulid.ULID())}"
        self.namespace = "archetype"

        # Initialize the catalog
        self.catalog = catalog or Catalog.from_pydict({
            self.namespace:{}
        })  

        # Initialize the session
        self.sess = Session.attach(self.catalog) 
        self.sess.create_namespace_if_not_exists(self.namespace)
        self.sess.set_namespace(self.namespace)

        # Initialize internal properties
        self._entity2sig: Dict[int, Tuple[Type[Component], ...]] = {} # Necessary mapping for entity_id -> signature
        self._hash2sig: Dict[str, Tuple[Type[Component], ...]] = {} # Convenience mapping for hash -> signature
        self._sig2schema: Dict[Tuple[Type[Component], ...], pa.Schema] = {} # Convenience mapping for signature -> schema
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
    
    
    def _get_archetype_schema(self, sig: Tuple[Type[Component], ...]) -> pa.Schema:
        """
        Get the schema for an archetype from a list of components.
        """
        if sig in self._sig2schema:
            return self._sig2schema[sig] # Return the cached schema
        
        archetype_schema = BaseArchetypeTableSchema
        for component_type in sig:
            component_schema = component_type.to_arrow_schema()
            prefix = self._get_component_prefix(component_type)

            # Rename the fields of the component schema with the prefix
            for field_name in component_schema.names:
                field = component_schema.field(field_name)
                renamed_field = field.with_name(prefix + field_name)
                archetype_schema = archetype_schema.append(renamed_field)
        
        self._sig2schema[sig] = archetype_schema
        return archetype_schema
    
    def _ensure_table(self, sig: Tuple[Type[Component], ...]) -> Table:
        """
        Ensure that the table for the given archetype signature exists.
        """
        hash_val = self._create_archetype_hash(sig)
        schema = self._get_archetype_schema(sig)

        
        table = self.sess.create_table_if_not_exists(hash_val, source=schema)

        # Store hash mapping
        if table not in self._hash2sig:
            self._hash2sig[hash_val] = sig 

        return table
        



    def _new_archetype_row(self, entity_id: int, step: int, components: List[Component], schema: pa.Schema) -> DataFrame:
        """
        Convert the single entity dictionary to a columnar dict for PyArrow
        Ensure the order of keys matches the schema for from_pydict if schema wasn't passed
        BUT since we pass the schema explicitly, the order in columnar_data doesn't strictly matter,
        although maintaining it is good practice.
        We need values to be lists.
        """
        # Create the base archetype from archetype arrow schema
        df = daft.from_arrow(schema.empty_table())
        df = df.with_columns({
            "simulation": lit(self.simulation),
            "run": lit(self.run),
            "entity_id": lit(entity_id),
            "step": lit(step),
            "is_active": lit(True)
        })

        for component_instance in components:
            prefix = self._get_component_prefix(component_instance.__class__)
            df = df.with_columns({prefix + key: lit(value) for key, value in component_instance.model_dump().items()})

        # Store entity mapping
        self._entity2sig[entity_id] = sig 
        
        return df

    #--------------------------------------------------------------------------
    # ComponentStoreInterface methods
    #--------------------------------------------------------------------------

    async def add_entity(self, components: List[Component], step: int = 0) -> int:
        """
        Add an entity to the store.
        """

        if len(components) == 0:
            raise ValueError("Cannot create an entity with no components")
        
        # Get the next entity id
        entity_id = next(self._entity_counter)
        sig = self._sig_from_components(components)
        schema = self._get_archetype_schema(sig)

        # Create the archetype row
        df = self._new_archetype_row(entity_id, step, components, schema)

        # Append the archetype row to the table
        self.append_table(df)

        return entity_id
    
    async def remove_entity(self, entity_id: int, step: int) -> None:
        if entity_id not in self._entity2sig:
            logger.error(f"Entity {entity_id} not found in _entity2sig. Cannot remove.")
            return 
        sig = self._entity2sig[entity_id]
        table = await self._ensure_table(sig)
        await table.update(
            where=f"simulation == '{self.simulation}' AND run == '{self.run}' AND entity_id == {entity_id} AND step == {step}",
            updates={"is_active": False}
        )
        self._entity2sig.pop(entity_id, None)

    # NOTE: ---------------------------------------------------------------
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
            table = await self._ensure_table(sig)
            try:
                table_uri = os.path.join(self.async_db.uri, table.name + ".lance")
                latest_df = daft.read_lance(table_uri) \
                    .where(col("simulation") == self.simulation) \
                    .where(col("run") == self.run) \
                    .repartition(None, *PARTITION_KEYS)
            
            except Exception as e: # Broad exception for now, Lance/Daft might raise specific errors
                logger.error(f"Error reading lance table {table.name} for cache update: {e}")

            archetypes[table.name] = latest_df
        
        return archetypes

    async def optimize(self,
        *component_types: Type[Component],
        cleanup_older_than: Optional[timedelta] = None,
        delete_unverified: Optional[bool] = None,
        retrain: Optional[bool] = None
        ) -> None:
        """
        Optimize the LanceDB tables for the given component types.
        """
        matching_sigs = self._get_matching_signatures(component_types)
        
        for sig in matching_sigs:
            table = await self._ensure_table(sig)
            
            try: 
                await table.optimize(
                cleanup_older_than=cleanup_older_than, 
                delete_unverified=delete_unverified, 
                retrain=retrain
                )
            except Exception as e:
                logger.error(f"Error optimizing lance table {table.name}: {e}")


    # ---------------------------------------------------------------------
    # Update Manager Interface
    # ---------------------------------------------------------------------

    def append_table(self, tablename: str, data: daft.DataFrame, step: int):
        table = self.sess.open_table(tablename)
        # Update the data in the table
        table.add(data, mode='append')
