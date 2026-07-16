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

"""Configured Daft Iceberg operations shared by app-layer table services."""

import asyncio
from dataclasses import dataclass

from daft import DataFrame, Schema, read_iceberg
from daft.catalog import Table
from daft.io import IOConfig
from daft.session import Session
from pyiceberg.exceptions import CommitFailedException

_MAX_COMMIT_ATTEMPTS = 16


@dataclass(frozen=True)
class IcebergCatalogContext:
    """An authoritative Daft catalog plus explicit data-plane credentials."""

    session: Session
    io_config: IOConfig | None

    def has_table(self, table_name: str) -> bool:
        return self.session.has_table(table_name)

    def get_table(self, table_name: str) -> Table:
        return self.session.get_table(table_name)

    def create_table_if_not_exists(self, table_name: str, schema: Schema) -> Table:
        return self.session.create_table_if_not_exists(table_name, source=schema)

    def read(self, table: Table) -> DataFrame:
        if self.io_config is None:
            return table.read()
        return read_iceberg(self._native_table(table), io_config=self.io_config)

    async def append(self, table: Table, frame: DataFrame) -> None:
        """Append with bounded optimistic-conflict retries across processes."""
        for attempt in range(_MAX_COMMIT_ATTEMPTS):
            try:
                self._append_once(table, frame)
                return
            except CommitFailedException:
                if attempt + 1 == _MAX_COMMIT_ATTEMPTS:
                    raise
                await asyncio.sleep(min(0.005 * (2**attempt), 0.1))
                self._native_table(table).refresh()

    async def append_counted(self, table: Table, frame: DataFrame) -> int:
        """Execute one arbitrary fact pipeline and return its committed row count."""
        written = frame.write_iceberg(
            self._native_table(table),
            mode="append",
            io_config=self.io_config,
        )
        files = written.collect(num_preview_rows=0).to_pydict()
        return sum(files["rows"])

    def current_snapshot_id(self, table: Table) -> int | None:
        native = self._native_table(table)
        native.refresh()
        snapshot = native.current_snapshot()
        return snapshot.snapshot_id if snapshot is not None else None

    def _append_once(self, table: Table, frame: DataFrame) -> None:
        if self.io_config is None:
            table.append(frame)
        else:
            frame.write_iceberg(
                self._native_table(table),
                mode="append",
                io_config=self.io_config,
            )

    @staticmethod
    def _native_table(table: Table):
        """Resolve Daft's native Iceberg handle for explicit ``IOConfig`` calls."""
        native = getattr(table, "_inner", None)
        if native is None:
            raise RuntimeError("Daft table does not expose an Iceberg handle for explicit IOConfig")
        return native
