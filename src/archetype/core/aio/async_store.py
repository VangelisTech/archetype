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
import daft
from daft import DataFrame, Schema, lit, read_iceberg
from daft.catalog import Table
from daft.io import IOConfig
from daft.session import Session

# Internals
from archetype.core.archetype import Archetype
from archetype.core.interfaces import AppendReceipt, ArchetypeSignature, iAsyncStore

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
        # Memoized presence of v0.2 legacy tables per legacy table id — frozen
        # history, checked once (see _legacy_df).
        self._legacy_present: dict[str, bool] = {}

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
        commit_tokens: list[str] | None = None,
    ) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.

        ``commit_tokens`` is the reader-side visibility allowlist (issue #273):
        current-generation rows must match a published manifest token. Legacy
        (v0.2) rows carry no commit identity and are implicitly epoch-0
        visible — the allowlist never applies to them.
        """
        table: Table = self._ensure_table(sig)
        df: DataFrame = self._read_table(table)  # Cheap, Lazy
        df = self._filter(
            df, world_id, run_id, ticks=ticks, entity_ids=entity_ids, active_only=active_only
        )
        if commit_tokens is not None:
            # (Daft stubs Expression.__and__ etc. as bool; these are Expressions.)
            visible = df["commit_token"].is_in(commit_tokens) if commit_tokens else lit(False)
            df = df.where(visible)  # ty: ignore[invalid-argument-type]

        legacy_df = self._legacy_df(
            sig, world_id, run_id, ticks=ticks, entity_ids=entity_ids, active_only=active_only
        )
        if legacy_df is not None:
            df = df.concat(legacy_df)

        return df

    def _filter(
        self,
        df: DataFrame,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None,
        entity_ids: list[int] | None,
        active_only: bool,
    ) -> DataFrame:
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

    def _legacy_df(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None,
        entity_ids: list[int] | None,
        active_only: bool,
    ) -> DataFrame | None:
        """Rows persisted by v0.2 under the legacy table id, if any.

        Presence is memoized per signature: legacy tables are frozen history
        (nothing writes them after #273), so one existence check suffices.
        Rows are stamped with the epoch-0 identity and aligned to the current
        schema so callers see one uniform shape.
        """
        legacy_name = Archetype.get_legacy_name(sig)
        known = self._legacy_present.get(legacy_name)
        if known is None:
            known = self.session.has_table(legacy_name)
            self._legacy_present[legacy_name] = known
        if not known:
            return None

        table = self.session.get_table(legacy_name)
        df = self._filter(
            self._read_table(table),
            world_id,
            run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=active_only,
        )
        df = df.with_columns(
            {
                "commit_token": lit(""),
                "writer_epoch": lit(0).cast(daft.DataType.int64()),
            }
        )
        return df.select(*Archetype.get_archetype_schema(sig).names)

    # ── Durable-discovery seam (issue #272) ─────────────────────────────────
    # Open-never-create reads by persisted physical table identity, so a cold
    # process holding a catalog descriptor can read without Python component
    # classes. Signature caches are untouched: they remain caches, not
    # discovery authority.

    def _get_existing_table(self, table_id: str) -> Table:
        if not self.session.has_table(table_id):
            raise KeyError(f"Iceberg table {table_id!r} does not exist")
        try:
            return self.session.get_table(table_id)
        except Exception as exc:
            raise RuntimeError(f"Error opening Iceberg table {table_id}: {exc}") from exc

    async def get_existing_table_schema(self, table_id: str):
        """Return an existing table's Arrow schema without creating it."""
        table = self._get_existing_table(table_id)
        return self._read_table(table).schema().to_pyarrow_schema()

    async def get_existing_table_df(
        self,
        table_id: str,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame:
        """Read an existing table by durable physical identity (never creates)."""
        table = self._get_existing_table(table_id)
        df = self._read_table(table)
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

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt:
        """
        Append a table with a new dataframe. Returns the durable batch receipt.
        """
        table_id = Archetype.get_name(sig)
        # Defensive: skip zero-row or empty-schema appends to protect backends
        try:
            df.collect()
            rows = df.count_rows()
            if rows == 0 or not df.column_names:
                logger.info(
                    f"Append skipped (store): archetype={table_id} rows=0 or empty schema"
                )
                return AppendReceipt(table_id=table_id, rows=0, durable=True)
        except Exception as e:
            # A frame that cannot materialize cannot be persisted; the caller
            # must see that, not a silent no-op.
            logger.error(f"Append collect failed for {table_id}: {e}")
            raise

        table = self._ensure_table(sig)
        # Record that this sig has been durably committed (not just auto-created
        # by a read).  list_committed_signatures() uses this to give callers a
        # reliable "was this sig ever written?" answer.
        self._committed_sigs.add(table_id)

        # Daft's Table.append is synchronous; do not await
        self._append_table(table, df)
        return AppendReceipt(
            table_id=table_id, rows=rows, durable=True, backend_ref=self._snapshot_ref(table)
        )

    @staticmethod
    def _snapshot_ref(table: Table) -> str | None:
        """Auxiliary diagnostic only — never the visibility mechanism."""
        inner = getattr(table, "_inner", None)
        current = getattr(inner, "current_snapshot", None)
        try:
            snapshot = current() if callable(current) else None
        except Exception:
            return None
        return str(snapshot.snapshot_id) if snapshot is not None else None

    async def flush(self) -> None:
        """No-op: appends write through to Iceberg."""

    async def shutdown(self) -> None:
        """
        Shutdown the store.
        """
        pass  # Daft handles this automatically
