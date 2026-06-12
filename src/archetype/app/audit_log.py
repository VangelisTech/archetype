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

"""
Audit Log

Append-only record of accepted-and-applied commands.
In-memory buffer with flush-to-storage capability.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import daft
import lancedb
import pyarrow as pa
from uuid_utils import UUID

from archetype.app.models import AuditRow
from archetype.app.storage_service import _resolve_uri
from archetype.core.config import StorageConfig

if TYPE_CHECKING:
    from archetype.app.storage_service import StorageService

logger = logging.getLogger(__name__)

_AUDIT_TABLE = "audit_rows"
_AUDIT_COLUMNS = (
    "audit_id",
    "command_id",
    "world_id",
    "actor_id",
    "command_type",
    "status",
    "payload_json",
    "accepted_at",
    "applied_at",
    "idempotency_key",
)


def _audit_schema() -> pa.Schema:
    return pa.schema(
        [
            ("audit_id", pa.string()),
            ("command_id", pa.string()),
            ("world_id", pa.string()),
            ("actor_id", pa.string()),
            ("command_type", pa.string()),
            ("status", pa.string()),
            ("payload_json", pa.string()),
            ("accepted_at", pa.string()),
            ("applied_at", pa.string()),
            ("idempotency_key", pa.string()),
        ]
    )


def _row_to_dict(row: AuditRow) -> dict:
    return {
        "audit_id": str(row.audit_id),
        "command_id": str(row.command_id) if row.command_id else None,
        "world_id": str(row.world_id) if row.world_id else None,
        "actor_id": str(row.actor_id) if row.actor_id else None,
        "command_type": row.command_type,
        "status": row.status,
        "payload_json": row.payload_json,
        "accepted_at": row.accepted_at,
        "applied_at": row.applied_at,
        "idempotency_key": row.idempotency_key,
    }


def _rows_to_frame(rows: list[dict]) -> daft.DataFrame:
    if not rows:
        return daft.from_pydict({column: [] for column in _AUDIT_COLUMNS})
    return daft.from_pydict(
        {column: [row.get(column) for row in rows] for column in _AUDIT_COLUMNS}
    )


class AuditLog:
    """Append-only audit log backed by dedicated storage.

    Rows are kept in memory for current-process observability and persisted to
    a dedicated LanceDB table under the configured audit namespace.
    """

    def __init__(
        self,
        storage_service: StorageService | None = None,
        storage_config: StorageConfig | None = None,
    ) -> None:
        self._storage_service = storage_service
        self._storage_config = storage_config or StorageConfig(
            uri="./archetype_data", namespace="audit"
        )
        self._rows: list[AuditRow] = []
        self._pending: list[AuditRow] = []
        self._db = None
        self._table = None
        self._lock = asyncio.Lock()

    async def _connect(self):
        if self._db is not None:
            return self._db

        uri = _resolve_uri(str(self._storage_config.uri))
        subdir = os.environ.get("ARCT_LANCEDB_SUBDIR", "lance")
        path = Path(uri) / self._storage_config.namespace / subdir
        path.mkdir(parents=True, exist_ok=True)
        self._db = await lancedb.connect_async(str(path))
        return self._db

    async def _list_table_names(self) -> list[str]:
        db = await self._connect()
        list_tables = getattr(db, "list_tables", None)
        if list_tables is not None:
            response = await list_tables()
            if hasattr(response, "tables"):
                return list(response.tables)
            return list(response)
        return list(await db.table_names())

    async def _ensure_table(self):
        if self._table is not None:
            return self._table

        db = await self._connect()
        if _AUDIT_TABLE in await self._list_table_names():
            self._table = await db.open_table(_AUDIT_TABLE)
            return self._table

        self._table = await db.create_table(
            name=_AUDIT_TABLE,
            schema=_audit_schema(),
            exist_ok=True,
        )
        return self._table

    async def record(self, row: AuditRow) -> None:
        """Append one audit row to memory and durable audit storage."""
        self._rows.append(row)
        self._pending.append(row)
        await self.flush()

    async def flush(self) -> None:
        """Flush buffered rows to the dedicated audit table."""
        if not self._pending:
            return

        async with self._lock:
            if not self._pending:
                return
            pending = list(self._pending)
            table = await self._ensure_table()
            arrow_table = pa.Table.from_pylist(
                [_row_to_dict(row) for row in pending],
                schema=_audit_schema(),
            )
            await table.add(arrow_table, mode="append")
            del self._pending[: len(pending)]

    async def _read_persisted_rows(self) -> list[dict]:
        if _AUDIT_TABLE not in await self._list_table_names():
            return []
        table = await self._ensure_table()
        arrow_table = await table.query().to_arrow()
        columns = arrow_table.to_pydict()
        if not columns:
            return []
        row_count = len(next(iter(columns.values())))
        return [
            {column: values[index] for column, values in columns.items()}
            for index in range(row_count)
        ]

    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ) -> daft.DataFrame:
        """Query the audit log with optional filters. Returns a Daft DataFrame."""
        try:
            await self.flush()
            rows = await self._read_persisted_rows()
        except Exception:
            logger.debug(
                "audit storage query failed; falling back to in-memory rows", exc_info=True
            )
            rows = [_row_to_dict(row) for row in self._rows]

        if self._pending:
            seen = {row["audit_id"] for row in rows}
            rows.extend(_row_to_dict(row) for row in self._pending if str(row.audit_id) not in seen)

        predicates = []

        if world_id is not None:
            wid = str(world_id)
            predicates.append(lambda row, wid=wid: row["world_id"] == wid)

        if actor_id is not None:
            aid = str(actor_id)
            predicates.append(lambda row, aid=aid: row["actor_id"] == aid)

        if idempotency_key is not None:
            predicates.append(lambda row: row["idempotency_key"] == idempotency_key)

        del tick_range  # Audit rows do not yet carry tick; accepted for API contract compatibility.

        filtered = [row for row in rows if all(predicate(row) for predicate in predicates)]

        if limit is not None:
            filtered = filtered[-limit:]

        return _rows_to_frame(filtered)

    async def shutdown(self) -> None:
        """Flush and close. Idempotent."""
        await self.flush()
        if self._db is not None:
            close = getattr(self._db, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result
            self._db = None
            self._table = None


def make_audit_row(
    ctx,
    command_type: str,
    world_id: str | UUID | None = None,
    *,
    command_id: UUID | None = None,
    status: str = "applied",
    payload_json: str = "{}",
) -> AuditRow:
    """Helper to build an AuditRow from a gate operation."""
    now = datetime.now(UTC).isoformat()
    return AuditRow(
        command_id=command_id,
        world_id=world_id,
        actor_id=ctx.id,
        command_type=command_type,
        status=status,
        payload_json=payload_json,
        accepted_at=now,
        applied_at=now,
    )
