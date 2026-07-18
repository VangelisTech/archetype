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

"""Batched, append-only audit rows in a dedicated Iceberg table."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence

import daft
import pyarrow as pa
from daft import Expression, Schema
from daft.catalog import Table
from uuid_utils import UUID

from archetype.app.errors import AvailabilityError
from archetype.app.models import AuditRow
from archetype.app.storage.catalog import OutboxRecord
from archetype.app.storage.iceberg import IcebergCatalogContext
from archetype.app.storage.interfaces import iStorageService
from archetype.core.config import StorageBackend, StorageConfig

_AUDIT_TABLE = "audit_rows"
DEFAULT_AUDIT_FLUSH_ROWS = 128


class AuditBackpressureError(AvailabilityError):
    """The bounded audit buffer rejected a row while storage was unavailable."""

    public_detail = "Audit log is temporarily unavailable"


def default_audit_storage() -> StorageConfig:
    """Return Archetype's concrete local audit lakehouse configuration."""
    return StorageConfig(
        uri="./archetype_data",
        namespace="audit",
        backend=StorageBackend.ICEBERG,
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


def _row_to_dict(row: AuditRow) -> dict[str, str | None]:
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


def _rows_to_frame(rows: Sequence[AuditRow]) -> daft.DataFrame:
    arrow = pa.Table.from_pylist([_row_to_dict(row) for row in rows], schema=_audit_schema())
    return daft.from_arrow(arrow)


class AuditLog:
    """Append audit telemetry to one dedicated Iceberg table in bounded batches."""

    def __init__(
        self,
        storage_service: iStorageService,
        storage_config: StorageConfig | None = None,
        *,
        flush_rows: int = DEFAULT_AUDIT_FLUSH_ROWS,
    ) -> None:
        if flush_rows < 1:
            raise ValueError("flush_rows must be at least 1")
        effective_config = storage_config or default_audit_storage()
        if effective_config.backend != StorageBackend.ICEBERG:
            raise ValueError("audit storage requires backend=iceberg")
        self._storage_service = storage_service
        self._storage_config = effective_config
        self._flush_rows = flush_rows
        self._pending: list[AuditRow] = []
        self._context: IcebergCatalogContext | None = None
        self._table: Table | None = None
        self._lock = asyncio.Lock()
        self._projection_lock = asyncio.Lock()
        self._rejected_rows = 0
        self._outbox_source: Callable[..., Awaitable[list[OutboxRecord]]] | None = None
        self._outbox_ack: Callable[[list[OutboxRecord]], Awaitable[None]] | None = None

    def set_outbox_source(
        self,
        source: Callable[..., Awaitable[list[OutboxRecord]]],
        acknowledge: Callable[[list[OutboxRecord]], Awaitable[None]],
    ) -> None:
        """Attach a transactional event source for eventual projection."""
        self._outbox_source = source
        self._outbox_ack = acknowledge

    @property
    def rejected_rows(self) -> int:
        """Rows rejected before admission because the bounded batch was full."""
        return self._rejected_rows

    async def _get_context(self) -> IcebergCatalogContext:
        if self._context is None:
            self._context = await self._storage_service.get_iceberg_context(self._storage_config)
        return self._context

    async def _ensure_table(self) -> Table:
        if self._table is None:
            context = await self._get_context()
            schema = Schema.from_pyarrow_schema(_audit_schema())
            self._table = context.create_table_if_not_exists(_AUDIT_TABLE, schema)
        return self._table

    async def _existing_table(self) -> Table | None:
        if self._table is not None:
            return self._table
        context = await self._get_context()
        if not context.has_table(_AUDIT_TABLE):
            return None
        self._table = context.get_table(_AUDIT_TABLE)
        return self._table

    async def record(self, row: AuditRow) -> None:
        """Buffer one row, flushing at the configured batch boundary."""
        async with self._lock:
            # A failed threshold flush leaves exactly one bounded batch. Retry
            # it before accepting another row so a broken backend cannot turn
            # advisory telemetry into an unbounded memory sink.
            if len(self._pending) >= self._flush_rows:
                try:
                    await self._flush_locked()
                except Exception as exc:
                    self._rejected_rows += 1
                    raise AuditBackpressureError(
                        "audit row rejected: the bounded pending batch could not flush"
                    ) from exc
            self._pending.append(row)
            if len(self._pending) >= self._flush_rows:
                await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._pending:
            return
        pending = tuple(self._pending)
        context = await self._get_context()
        table = await self._ensure_table()
        await context.append(table, _rows_to_frame(pending))
        del self._pending[: len(pending)]

    async def flush(self) -> None:
        """Persist the current batch as one Iceberg append/snapshot."""
        async with self._lock:
            await self._flush_locked()

    async def project_outbox(self, *, limit: int = 1000) -> int:
        """Project authoritative events, acknowledging only after durable append.

        A crash after the Iceberg append but before acknowledgement may replay
        an event. ``audit_id`` is the outbox event identity and query-time
        deduplication keeps the analytical view exactly once.
        """
        if self._outbox_source is None or self._outbox_ack is None:
            return 0
        async with self._projection_lock:
            events = await self._outbox_source(limit=limit)
            if not events:
                return 0
            rows = [
                AuditRow(
                    audit_id=UUID(event.event_id),
                    command_id=UUID(event.aggregate_id),
                    world_id=event.world_id,
                    actor_id=event.actor_id,
                    command_type=event.command_type,
                    status=event.status,
                    payload_json=event.payload_json,
                    accepted_at=event.occurred_at,
                    applied_at=event.occurred_at,
                    idempotency_key=event.event_id,
                )
                for event in events
            ]
            async with self._lock:
                self._pending.extend(rows)
                await self._flush_locked()
            await self._outbox_ack(events)
            return len(events)

    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> daft.DataFrame:
        """Return a lazy, deterministically ordered query over persisted rows."""
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")

        await self.project_outbox()
        await self.flush()
        table = await self._existing_table()
        if table is None:
            frame = _rows_to_frame(())
        else:
            context = await self._get_context()
            frame = context.read(table).distinct("audit_id")

        if world_id is not None:
            frame = frame.where(
                frame["world_id"] == str(world_id)  # ty: ignore[invalid-argument-type]
            )
        if actor_id is not None:
            frame = frame.where(
                frame["actor_id"] == str(actor_id)  # ty: ignore[invalid-argument-type]
            )
        if idempotency_key is not None:
            frame = frame.where(
                frame["idempotency_key"] == idempotency_key  # ty: ignore[invalid-argument-type]
            )
        if status is not None:
            frame = frame.where(
                frame["status"] == status  # ty: ignore[invalid-argument-type]
            )

        del tick_range  # Accepted compatibility parameter; AuditRow has no tick field.

        order: list[Expression | str] = ["accepted_at", "audit_id"]
        if limit is not None:
            if limit == 0:
                return frame.limit(0)
            frame = frame.sort(order, desc=[True, True]).limit(limit)
        return frame.sort(order)

    async def shutdown(self) -> None:
        """Flush pending rows and release standalone storage ownership."""
        await self.project_outbox()
        await self.flush()
        self._table = None
        self._context = None
