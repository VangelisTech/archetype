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
from daft import DataFrame, Schema
from daft.catalog import Table
from daft.session import Session

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
    ):
        self.uri = uri
        self.debug = debug
        self.sess = session
        self.flush_interval = None

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
        except Exception as e:
            raise RuntimeError(f"Error creating table {hash_val}: {e}") from e

        return table

    def get_archetype_df(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes for a given world_id and run_id.
        Filtering by tick/is_active is done by the QueryManager.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = table.read()  # Cheap, Lazy

        if self.debug:
            logger.debug("Reading table %s", table.name)
            df.limit(5).show()

        # stored as strings; ensure filter values are strings
        return df.where(df["world_id"] == str(world_id)).where(df["run_id"] == str(run_id))

    def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        Tick, world_id, run_id are stamped by the UpdateManager before calling this.
        """
        table = self._ensure_table(sig)
        table_name = table.name

        if self.debug:
            df.collect()
            logger.debug("Appending %s rows to table %s", df.count_rows(), table_name)
            df.show()

        table.append(df)

    def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        pass  # Daft handles this automatically
