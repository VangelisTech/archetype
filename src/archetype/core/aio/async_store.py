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
from typing import cast

# Technologies
from daft import DataFrame, Schema, read_iceberg
from daft.catalog import Table
from daft.io import IOConfig
from daft.session import Session

# Internals
from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore

# Logger
logger = getLogger(__name__)


class AsyncStore(iAsyncStore):
    """
    The ArchetypeStore is a component that manages the storage and retrieval of archetype tables.

    Since our Schema supports multiple simulations and runs, our namespace is simply "archetypes".
    This allows us to run multiple simulations across many worlds using the same catalog.archetypes, by definition,
    the exact set of components attached to an entity. So we don't really which simulation or run we're using,
    so long as we differentiate between the simulation and run.

    Using Daft Sessions/Catalogs enables us to reference archetype tables without having to hold them in memory.

    """

    def __init__(self, session: Session | object, io_config: IOConfig | None = None):
        # Accepts a Session, a wrapper carrying `.session`, or any Session-like
        # duck type (tests pass fakes); the cast records the resolved contract.
        self.session: Session = cast("Session", getattr(session, "session", session))
        self.io_config: IOConfig | None = (
            io_config if io_config is not None else getattr(session, "io_config", None)
        )
        self._known_sigs: dict[str, ArchetypeSignature] = {}
        # Tracks only signatures that have been durably committed via append().
        # get_archetype_df may create session tables on demand (create-on-read),
        # but those are NOT included here — only append() populates this set.
        # This lets query_archetype distinguish an unknown sig (never committed)
        # from an empty-but-known sig (committed with zero active rows).
        self._committed_sigs: set[str] = set()

    def _ensure_table(self, sig: ArchetypeSignature) -> Table:
        """
        Ensure that the table for the given archetype signature exists in the Daft session.
        Returns the table name (hash_val).
        """
        hash_val = Archetype.get_name(sig)
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        daft_schema = Schema.from_pyarrow_schema(pyarrow_schema)
        try:
            table = self.session.create_table_if_not_exists(hash_val, source=daft_schema)
        except Exception as e:
            raise RuntimeError(f"Error creating table {hash_val}: {e}") from e

        self._known_sigs[hash_val] = sig
        return table

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

    async def get_archetype_df(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = self._read_table(table)  # Cheap, Lazy

        # stored as strings; ensure filter values are strings
        # (Daft stubs type Expression.__eq__ as bool; these are Expressions.)
        df = df.where(df["world_id"] == str(world_id))  # ty: ignore[invalid-argument-type]
        df = df.where(df["run_id"] == str(run_id))  # ty: ignore[invalid-argument-type]

        if active_only:
            df = df.where(df["is_active"])

        if ticks is not None:
            df = df.where(df["tick"].is_in(ticks))

        if entity_ids is not None:
            df = df.where(df["entity_id"].is_in(entity_ids))

        return df

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """
        List all archetype signatures that have been registered via _ensure_table.
        """
        return list(self._known_sigs.values())

    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """List only signatures that have been durably committed via append().

        Unlike list_signatures(), this excludes signatures that were only
        auto-created by get_archetype_df (create-on-read).  Use this when you
        need to distinguish 'sig was never written' from 'sig exists but has
        no rows at the queried tick'.
        """
        return [sig for key, sig in self._known_sigs.items() if key in self._committed_sigs]

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Append a table with a new dataframe.
        """
        # Defensive: skip zero-row or empty-schema appends to protect backends
        try:
            df.collect()
            if df.count_rows() == 0 or not df.column_names:
                logger.info(
                    f"Append skipped (store): archetype={Archetype.get_name(sig)} rows=0 or empty schema"
                )
                return
        except Exception as e:
            # A frame that cannot materialize cannot be persisted; the caller
            # must see that, not a silent no-op.
            logger.error(f"Append collect failed for {Archetype.get_name(sig)}: {e}")
            raise

        table = self._ensure_table(sig)
        # Record that this sig has been durably committed (not just auto-created
        # by a read).  list_committed_signatures() uses this to give callers a
        # reliable "was this sig ever written?" answer.
        self._committed_sigs.add(Archetype.get_name(sig))

        # Daft's Table.append is synchronous; do not await
        self._append_table(table, df)

    async def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        pass  # Daft handles this automatically
