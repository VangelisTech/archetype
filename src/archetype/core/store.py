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

# Standard Python Libraries
from itertools import count
from typing import Dict, Tuple, List, Type, Optional, Any, Set
from logging import getLogger
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
from .interfaces import Component, iStore, ArchetypeSignature

logger = getLogger(__name__)

BaseArchetypeTableSchema = pa.schema([
    pa.field("world_id", pa.string(), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("entity_id", pa.uint64(), nullable=False),
    pa.field("step", pa.uint64(), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
])
PARTITION_KEYS = ["world_id", "run_id", "step"]


# Utility functions
def get_datetime_str() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # ISO 8601

def get_component_prefix(component_type: Type[Component]) -> str:
    """Generate a standardized prefix for a component type's fields."""
    return component_type.__name__.lower() + "__"


def sig_from_components(components: List[Component]) -> ArchetypeSignature:
    # Get the signature of the components by sorting their types by name
    component_types = [type(c) for c in components]
    sig = tuple(sorted(component_types, key=lambda t: t.__name__))
    return sig

@lru_cache(maxsize=None, typed=True)
def sig2hash(sig: ArchetypeSignature) -> str:
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

# Store classes
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
        namespace: Optional[str] = None,
        catalog: Optional[Catalog] = None,
        debug: bool = False,
    ):

        self.namespace = namespace or "archetypes"
        self.debug = debug



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
        self.sess.attach_table()

        # Initialize internal properties
        self._entity2sig: Dict[int, ArchetypeSignature] = {} # Necessary mapping for entity_id -> archetype signature
        self._entity_counter = count(start=1)

        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}

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

    def _ensure_table(self, sig: ArchetypeSignature) -> Table:
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

    def _new_archetype_row(self, entity_id: int, step: int, components: List[Component], world_id: str, run_id: str) -> DataFrame:
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
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "step": step,
            "is_active": True
        }

        for c in components:
            prefix = get_component_prefix(type(c))
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})


        return row_dict

    #--------------------------------------------------------------------------
    # iStore methods
    #--------------------------------------------------------------------------
    def add_entity(self, components: List[Component], step: int, world_id: str, run_id: str) -> int:
        """
        Add an entity to the store.
        """
        assert len(components) != 0, "Cannot create an entity with no components"
        assert step == 0, "ArchetypeStore does not currently support adding entities in-situ"

        # Get the entity id and signature
        entity_id = next(self._entity_counter)
        sig = sig_from_components(components)

        # Create the row dict
        row_dict = self._new_archetype_row(entity_id, step, components, world_id, run_id)

        # Add row to the spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)

        # Store entity mapping
        self._entity2sig[entity_id] = sig

        return entity_id


    def materialize_spawns(self) -> None:
        """
        Write the entity spawn cache to the tables for simulation initialization.
        """
        for sig, rows in self._spawn_cache.items():
            # Coerce List of PyDicts to PyArrow table
            pyarrow_schema = self._get_archetype_schema(sig)

            # Create empty arrow table from schema
            empty_table = pyarrow_schema.empty_table()

            # populate table with rows from pylist (list of dicts)
            arrow_table = empty_table.from_pylist(rows)
            df = daft.from_arrow(arrow_table)

            # Write to the table
            table = self._ensure_table(sig)
            try:
                table.append(df)
                logger.debug(f"Appended {len(rows)} rows to table {table.name}")
            except Exception as e:
                logger.error(f"Error appending {len(rows)} rows to table {table.name}: {e}")
                raise

        # Clear the cache for this signature
        self._spawn_cache.clear()


    def remove_entity(self, entity_id: int, step: int, world_id: str, run_id: str) -> None:
        if entity_id not in self._entity2sig:
            logger.warn(f"Entity {entity_id} not found in _entity2sig. Cannot remove.")
            return

        sig = self._entity2sig[entity_id]
        table = self._ensure_table(sig) # Ensure table exists

        entity_df = table.to_dataframe().where(
            (col("entity_id") == lit(entity_id)) &
            (col("step") == lit(step)) &
            (col("world_id") == lit(world_id)) &
            (col("run_id") == lit(run_id))
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
                AND world_id = '{world_id}'
                AND run_id = '{run_id}'
            """)
            logger.debug(f"Marked entity {entity_id} as inactive in archetype table {table.name} for step {step}.")
        except Exception as e:
            logger.error(f"Error marking entity {entity_id} as inactive in archetype table {table.name} for step {step}: {e}")
            raise


    # ---------------------------------------------------------------------
    # iQuerier methods
    # ---------------------------------------------------------------------
    def get_archetype_for_entity(self, entity_id: int, *component_types: Type[Component], world_id: str, run_id: str) -> Dict[str, DataFrame]:
        """
        Get all archetypes.
        """
        sig = self._entity2sig[entity_id]
        table_name = sig2hash(sig)
        table = self.catalog.get_table(table_name)

        df = table.to_dataframe() \
            .where(col("entity_id") == entity_id) \
            .where(col("world_id") == world_id) \
            .where(col("run_id") == run_id)

        return df

    def get_archetypes(self, world_id: str, run_id: str) -> List[Tuple[ArchetypeSignature, DataFrame]]:
        """
        Get all active archetypes using the entity2sig mapping for efficiency.
        Returns dict mapping archetype_hash -> (DataFrame, component_signature)
        This avoids expensive schema comparisons by using tracked signatures.
        """
        active_signatures = self.get_active_signatures()
        archetypes_with_sigs: List[Tuple[ArchetypeSignature, DataFrame]] = []

        for sig in active_signatures:
            table_name = sig2hash(sig)
            try:
                table = self.catalog.get_table(self.namespace + "." + table_name)
                df = table.to_dataframe() \
                    .where(col("world_id") == world_id) \
                    .where(col("run_id") == run_id)

                archetypes_with_sigs.append((sig, df))
            except Exception as e:
                logger.error(f"Error reading archetype table {table_name}: {e}")
                continue

        return archetypes_with_sigs

    #--------------------------------------------------------------------------
    # iUpdater methods
    #--------------------------------------------------------------------------

    def append(self, sig: ArchetypeSignature, df: DataFrame, step: int, world_id: str, run_id: str) -> None:
        """
        Append a table with a new dataframe.
        """
        table_name = sig2hash(sig)
        table = self.sess.get_table(table_name)
        try:
            start_time = time.time()
            if self.debug:
                logger.info(f"Appending dataframe to table {table_name}")
                df.collect()
                df.show()
                print(f"Appending {df.count_rows()} rows to table {table_name} for step {step}")
            table.append(df)
            end_time = time.time()
            logger.info(f"Appended dataframe to table {table_name} for step {step} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name} for step {step}: {e}")
            raise
