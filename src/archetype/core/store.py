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
from typing import Dict, Tuple, List, Optional, Any
from logging import getLogger

# Technologies
import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Catalog, Table
from daft.io import IOConfig
from pyiceberg.catalog.sql import SqlCatalog
import time

# Internals
from archetype.core.interfaces import Archetype, ArchetypeSignature, iStore

logger = getLogger(__name__)

# Store classes
class SyncStore(iStore):
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

    #--------------------------------------------------------------------------
    # Helper methods
    #--------------------------------------------------------------------------

    def _ensure_table(self, sig: ArchetypeSignature) -> Table:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        hash_val = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        daft_schema = Schema.from_pyarrow_schema(pyarrow_schema)
        try:
            table = self.sess.create_table_if_not_exists(hash_val, source=daft_schema)
            logger.info(f"Created Daft table {hash_val} with schema: {daft_schema}")
        except Exception as e:
            logger.error(f"Error creating Daft table {hash_val}: {e}")
            raise

        return table

    # ---------------------------------------------------------------------
    # Querying
    # ---------------------------------------------------------------------

    def get_archetype_df(self, sig: ArchetypeSignature) -> DataFrame:
        """
        Get all archetypes.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = table.read()
        if self.debug:
            df.show()
        return df

    #--------------------------------------------------------------------------
    # Updating
    #--------------------------------------------------------------------------

    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None:
        """
        Materialize the spawn cache into the tables.
        """
        for sig, rows in spawn_cache.items():
            # Coerce List of PyDicts to PyArrow table
            pyarrow_schema = Archetype.get_archetype_schema(sig)
            empty_table = pyarrow_schema.empty_table()
            arrow_table = empty_table.from_pylist(rows)
            df = daft.from_arrow(arrow_table)

            if self.debug:
                print(f"SyncStore: Getting archetype for {sig}")
                df.show()

            # Write to the table
            table = self._ensure_table(sig)
            try:
                table.append(df)
                logger.debug(f"Appended {len(rows)} rows to table {table.name}")
            except Exception as e:
                logger.error(f"Error appending {len(rows)} rows to table {table.name}: {e}")
                raise


    def remove_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None:
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

    def append(self, sig: ArchetypeSignature, df: DataFrame, step: int, world_id: str, run_id: str) -> None:
        """
        Append a table with a new dataframe.
        """
        table_name = Archetype.get_name(sig)
        table = self.sess.get_table(table_name)
        try:
            start_time = time.time()
            if self.debug:
                logger.info(f"SyncStore: Appending dataframe to table {table_name}")
                df.collect()
                df.show()
                print(f"Appending {df.count_rows()} rows to table {table_name} for step {step}")
            df.show()
            table.append(df)
            end_time = time.time()
            logger.info(f"Appended dataframe to table {table_name} for step {step} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Error appending dataframe to table {table_name} for step {step}: {e}")
            raise
