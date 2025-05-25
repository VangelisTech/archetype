# Standard Python Libraries
from itertools import count as _count
from typing import Dict, Tuple, List, Type, Optional
from logging import getLogger
from hashlib import blake2b
import ulid 
from datetime import datetime, timezone
from functools import lru_cache
# Technologies
import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Catalog, Table
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa    
from lancedb.pydantic import LanceModel
import time
# Internals
from .interfaces import Component, iStore

logger = getLogger(__name__)





BaseArchetypeTableSchema = pa.schema([
    pa.field("simulation", pa.string(), nullable=False),
    pa.field("run", pa.string(), nullable=False),
    pa.field("entity_id", pa.uint64(), nullable=False),
    pa.field("step", pa.uint64(), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
])
PARTITION_KEYS = ["simulation", "run", "step"]
    
    
def get_datetime_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # ISO 8601

def sig_from_components(components: List[Component]) -> Tuple[Type[Component], ...]:
    # Get the signature of the components by sorting their types by name
    component_types = [type(c) for c in components]
    sig = tuple(sorted(component_types, key=lambda t: t.__name__))
    return sig


def get_component_prefix(component_type: Type[Component]) -> str:
    """Generate a standardized prefix for a component type's fields."""
    return component_type.__name__.lower() + "__"

@lru_cache(maxsize=None, typed=True)
def sig2hash(sig: Tuple[Type[Component], ...]) -> str:
    hash_val = ""
    for comp_type in sig:
        hash_val += comp_type.__name__[0:3]
    return "archetype_" + hash_val


def convert_component_to_pyarrow_schema(component_type: Type[Component]) -> pa.Schema:
    if issubclass(component_type, LanceModel):
        return component_type.to_arrow_schema()
    else:
        # TODO: Implement conversion of non-LanceModel components to PyArrow schema
        # Currently this is unreachable. 
        raise ValueError(f"Component {component_type} is not a subclass of LanceModel")
    
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
        uri: str,
        simulation: Optional[str] = None, 
        run: Optional[str] = None,
        namespace: Optional[str] = None,
        catalog: Optional[Catalog] = None,
        debug: bool = False,
    ):
        self.debug = debug
        self.simulation = simulation or f"sim_{get_datetime_str()}"
        self.run = run or f"run_{str(ulid.ULID())}"
        self.namespace = namespace or "archetypes"

        # Initialize the catalog
        self.catalog = catalog or Catalog.from_iceberg(
            SqlCatalog(
                "default",
                **{
                    "uri": f"sqlite:///{uri}/catalog.db",
                    "warehouse": f"file://{uri}",
                },
            )
        )


        # Initialize the session
        self.sess = Session()
        self.sess.attach(object=self.catalog) 
        self.sess.create_namespace_if_not_exists(self.namespace)
        self.sess.set_namespace(self.namespace)

        # Initialize internal properties
        self._entity2sig: Dict[int, Tuple[Type[Component], ...]] = {} # Necessary mapping for entity_id -> archetype signature
        self._entity_counter = _count(start=1)

    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------
    

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
    def _get_archetype_schema(self, sig: Tuple[Type[Component], ...]) -> pa.Schema:
        """
        Get the schema for an archetype from a list of components.
        """
    
        archetype_schema = BaseArchetypeTableSchema
        for component_type in sig:
            component_schema = self._get_component_schema(component_type)
            archetype_schema = pa.unify_schemas([archetype_schema, component_schema])
        
        return archetype_schema
    
    def _ensure_table(self, sig: Tuple[Type[Component], ...]) -> Table:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        hash_val = sig2hash(sig)
        pyarrow_schema = self._get_archetype_schema(sig)
        daft_schema = Schema.from_pyarrow_schema(pyarrow_schema)
        try:
            table = self.sess.create_table_if_not_exists(hash_val, source=daft_schema)
            logger.info(f"Created Daft table {hash_val} with schema: {daft_schema}")
        except Exception as e:
            logger.error(f"Error creating Daft table {hash_val}: {e}")
            raise
        
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
        #df = daft.from_arrow(schema.empty_table())
        row_dict = {
            "simulation": self.simulation,
            "run": self.run,
            "entity_id": entity_id,
            "step": step,
            "is_active": True
        }

        for c in components:
            prefix = get_component_prefix(type(c))
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})


        return row_dict

    #--------------------------------------------------------------------------
    # ComponentStoreInterface methods
    #--------------------------------------------------------------------------

    def add_entity(self, components: List[Component], step: int = 0) -> int:
        """
        Add an entity to the store.
        """
        if len(components) == 0:
            raise ValueError("Cannot create an entity with no components")
        
        entity_id = next(self._entity_counter)
        sig = sig_from_components(components)
        
        pyarrow_schema = self._get_archetype_schema(sig)

        row_dict = self._new_archetype_row(entity_id, step, components, pyarrow_schema)

        table = self._ensure_table(sig)
        try:
            df_row = daft.from_pylist([row_dict])
            table.append(df_row)
            logger.debug(f"Appended entity {entity_id} to table {table.name}")
        except Exception as e:
            logger.error(f"Error appending entity {entity_id} to table {table.name}: {e}")
            raise

        # Store entity mapping
        self._entity2sig[entity_id] = sig

        return entity_id
    
    def remove_entity(self, entity_id: int, step: int) -> None:
        if entity_id not in self._entity2sig:
            logger.warn(f"Entity {entity_id} not found in _entity2sig. Cannot remove.")
            return
        
        sig = self._entity2sig[entity_id]
        table = self._ensure_table(sig) # Ensure table exists

        entity_df = table.to_dataframe().where(
            (col("entity_id") == lit(entity_id)) & 
            (col("step") == lit(step)) & 
            (col("simulation") == lit(self.simulation)) & 
            (col("run") == lit(self.run))
        ) 
        entity_df = entity_df.with_column(
            "is_active", 
            lit(False)
        )
        try: 
            self.sess.sql(f"""
                UPDATE {table.name} 
                SET is_active = FALSE 
                WHERE entity_id = {entity_id} 
                AND step = {step} 
                AND simulation = '{self.simulation}' 
                AND run = '{self.run}'
            """)
            logger.debug(f"Marked entity {entity_id} as inactive in archetype table {table.name} for step {step}.")
        except Exception as e:
            logger.error(f"Error marking entity {entity_id} as inactive in archetype table {table.name} for step {step}: {e}")
            raise

    # ---------------------------------------------------------------------
    # Query helpers used by Processor._fetch_state and QueryManager
    # ---------------------------------------------------------------------
    def _does_table_contain_components(self, *component_types: Type[Component], table: Table) -> bool:
        """
        Given a table, returns a list of tables that contain columns from the specified component types.
        """
        required_columns = set()
        for ct in component_types:
            prefix = get_component_prefix(ct)
            # Assuming components have fields, otherwise this needs adjustment
            # This also assumes component_type.model_fields is a valid way to get field names
            if hasattr(ct, 'model_fields'):
                for field_name in ct.model_fields.keys():
                    required_columns.add(prefix + field_name)
            else:
                # Fallback or error if model_fields is not present
                logger.warning(f"Component type {ct.__name__} does not have 'model_fields'. Cannot determine required columns for table check.")

     
        table_columns = set(table.to_dataframe().column_names)
        return required_columns.issubset(table_columns)


    def get_archetypes(self, *component_types: Type[Component]) -> Dict[str, DataFrame]:  
        """
        Given a list of component types, returns a dictionary of dataframes queried from tables whose column signatures include ALL of the specified component types.
        Data is read from Daft-managed tables.

        Args:
            *component_types: Type[Component]   The component types to get the archetypes for.

        Returns:
            archetypes: Dict[str, DataFrame]    A dictionary of archetype hashes to their latest data as Daft DataFrames.
        """
        if len(component_types) == 0:
            raise ValueError("Must request at least one component type")

        # Find all tables that contain columns from target_component_types
        archetypes_dfs: Dict[str, DataFrame] = {}
        for tablename in self.catalog.list_tables(self.namespace):
            table = self.catalog.get_table(tablename)
            if self._does_table_contain_components(*component_types, table=table):
                try:
                    archetypes_dfs[table.name] = table.to_dataframe() \
                        .where(col("simulation") == self.simulation) \
                        .where(col("run") == self.run) \
                        .repartition(None, *PARTITION_KEYS)
                    
                    if self.debug: 
                        logger.info(f"Showing table {table.name}")
                        archetypes_dfs[table.name].show()
                except Exception as e:
                    logger.error(f"Error reading or processing Daft table {table.name}: {e}")
                    continue # Skip this table on error
        
        return archetypes_dfs

    def append(self, table_name: str, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """
        table = self.sess.get_table(table_name)
        try: 
            start_time = time.time()
            df.collect()
            df.show()
            table.append(df)
            end_time = time.time()
            logger.info(f"Appended dataframe to table {table_name} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name}: {e}")
            raise


