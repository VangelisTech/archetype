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
from logging import getLogger

# Technologies
from daft import DataFrame, Schema, read_iceberg
from daft.catalog import Table
from daft.io import IOConfig
from daft.session import Session
from pyiceberg.exceptions import TableAlreadyExistsError

from archetype.core.archetype import Archetype

# Internals
from archetype.core.interfaces import ArchetypeSignature, iStore

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

    def __init__(
        self,
        uri: str,
        session: Session,
        debug: bool = False,
        io_config: IOConfig | None = None,
    ):
        self.uri = uri
        self.debug = debug
        self.sess = session
        self.io_config = io_config
        self.flush_interval = None
        self._known_sigs: dict[str, ArchetypeSignature] = {}

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
        except TableAlreadyExistsError:
            # Another catalog client won first registration after both
            # create-if-absent checks. The catalog winner is authoritative.
            table = self.sess.get_table(hash_val)
        except Exception as e:
            raise RuntimeError(f"Error creating table {hash_val}: {e}") from e

        self._known_sigs[hash_val] = sig
        return table

    def list_signatures(self) -> list[ArchetypeSignature]:
        """List archetype signatures registered via _ensure_table."""
        return list(self._known_sigs.values())

    def _read_table(self, table: Table) -> DataFrame:
        if self.io_config is None:
            return table.read()

        # Daft's Iceberg Table wrapper does not currently expose io_config
        # through Table.read(...), so use the native Iceberg API when needed.
        inner_table = getattr(table, "_inner", None)
        if inner_table is None:
            return table.read()

        return read_iceberg(inner_table, io_config=self.io_config)

    def _append_table(self, table: Table, df: DataFrame) -> None:
        if self.io_config is None:
            table.append(df)
            return

        # See _read_table: explicit io_config requires the native Iceberg path.
        inner_table = getattr(table, "_inner", None)
        if inner_table is None:
            table.append(df)
            return

        df.write_iceberg(inner_table, mode="append", io_config=self.io_config)

    def get_archetype_df(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes for a given world_id and run_id.
        Filtering by tick/is_active is done by the QueryManager.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = self._read_table(table)  # Cheap, Lazy

        if self.debug:
            logger.debug("Reading table %s", table.name)

        # stored as strings; ensure filter values are strings
        # (Daft stubs type Expression.__eq__ as bool; these are Expressions.)
        df = df.where(df["world_id"] == str(world_id))  # ty: ignore[invalid-argument-type]
        return df.where(df["run_id"] == str(run_id))  # ty: ignore[invalid-argument-type]

    def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        Tick, world_id, run_id are stamped by the UpdateManager before calling this.
        """
        table = self._ensure_table(sig)
        table_name = table.name

        if self.debug:
            logger.debug("Appending to table %s", table_name)

        self._append_table(table, df)

    def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        pass  # Daft handles this automatically
