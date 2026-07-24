# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Bounded access evidence and replay-safe command-outbox projection."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence

import daft
import pyarrow as pa
from daft import Expression
from uuid_utils import UUID

from archetype.commands.models import AuditRow
from archetype.core.config import StorageBackend, StorageConfig
from archetype.errors import AvailabilityError
from archetype.storage.catalog import OutboxRecord
from archetype.storage.interfaces import iStorageService

_AUDIT_TABLE = "audit_rows"
DEFAULT_AUDIT_FLUSH_ROWS = 128

ReadOutbox = Callable[..., Awaitable[list[OutboxRecord]]]
AcknowledgeOutbox = Callable[[list[OutboxRecord]], Awaitable[None]]


class AuditBackpressureError(AvailabilityError):
    """A bounded access batch rejected a row while storage was unavailable."""

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
    table = pa.Table.from_pylist(
        [_row_to_dict(row) for row in rows],
        schema=_audit_schema(),
    )
    return daft.from_arrow(table)


class AuditLog:
    """Append access rows and project authoritative command events."""

    def __init__(
        self,
        storage_service: iStorageService,
        storage_config: StorageConfig | None = None,
        *,
        read_outbox: ReadOutbox,
        acknowledge_outbox: AcknowledgeOutbox,
        flush_rows: int = DEFAULT_AUDIT_FLUSH_ROWS,
    ) -> None:
        if flush_rows < 1:
            raise ValueError("flush_rows must be at least 1")
        effective_config = storage_config or default_audit_storage()
        if effective_config.backend != StorageBackend.ICEBERG:
            raise ValueError("audit storage requires backend=iceberg")
        self._storage_service = storage_service
        self._storage_config = effective_config
        self._read_outbox = read_outbox
        self._acknowledge_outbox = acknowledge_outbox
        try:
            self._read_outbox_accepts_world = (
                "world_id" in inspect.signature(read_outbox).parameters
            )
        except (TypeError, ValueError):
            self._read_outbox_accepts_world = False
        self._flush_rows = flush_rows
        self._pending: list[AuditRow] = []
        self._lock = asyncio.Lock()
        self._projection_lock = asyncio.Lock()
        self._rejected_rows = 0

    @property
    def rejected_rows(self) -> int:
        """Return rows rejected while one failed bounded batch was retained."""
        return self._rejected_rows

    async def record(self, row: AuditRow) -> None:
        """Buffer one advisory access row without allowing unbounded growth."""
        async with self._lock:
            if len(self._pending) >= self._flush_rows:
                try:
                    await self._flush_locked()
                except Exception as error:
                    self._rejected_rows += 1
                    raise AuditBackpressureError(
                        "audit row rejected: the bounded pending batch could not flush"
                    ) from error
            self._pending.append(row)
            if len(self._pending) >= self._flush_rows:
                await self._flush_locked()

    async def _append_rows(self, rows: Sequence[AuditRow]) -> None:
        if not rows:
            return
        await self._storage_service.append_table(
            self._storage_config,
            _AUDIT_TABLE,
            _rows_to_frame(rows),
        )

    async def _flush_locked(self) -> None:
        if not self._pending:
            return
        pending = tuple(self._pending)
        await self._append_rows(pending)
        del self._pending[: len(pending)]

    async def flush(self) -> None:
        """Persist the current access batch as one append."""
        async with self._lock:
            await self._flush_locked()

    @staticmethod
    def _outbox_row(event: OutboxRecord) -> AuditRow:
        return AuditRow(
            audit_id=UUID(str(event.event_id)),
            command_id=UUID(str(event.aggregate_id)),
            world_id=event.world_id,
            actor_id=event.actor_id,
            command_type=event.command_type,
            status=event.status,
            payload_json=event.payload_json,
            accepted_at=event.occurred_at,
            applied_at=event.occurred_at,
            idempotency_key=event.event_id,
        )

    async def project_outbox(
        self,
        *,
        world_id: str | UUID | None = None,
        limit: int = 1000,
    ) -> int:
        """Append authoritative events before acknowledging their watermark."""
        if limit < 1:
            raise ValueError("limit must be positive")
        async with self._projection_lock:
            if world_id is not None and self._read_outbox_accepts_world:
                events = await self._read_outbox(
                    world_id=world_id,
                    limit=limit,
                )
            else:
                events = await self._read_outbox(limit=limit)
            if not events:
                return 0
            rows = tuple(self._outbox_row(event) for event in events)
            async with self._lock:
                # Preserve accepted access rows first, but never copy durable
                # outbox events into the bounded process-memory buffer. A
                # failed event append remains recoverable at the source.
                await self._flush_locked()
                await self._append_rows(rows)
            await self._acknowledge_outbox(events)
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
        """Return a lazy, deterministically ordered, replay-deduped view."""
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")

        await self.project_outbox(world_id=world_id)
        await self.flush()
        try:
            frame = await self._storage_service.read_table(
                self._storage_config,
                _AUDIT_TABLE,
            )
        except KeyError:
            frame = _rows_to_frame(())
        else:
            frame = frame.distinct("audit_id")

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
                frame["idempotency_key"]  # ty: ignore[invalid-argument-type]
                == idempotency_key
            )
        if status is not None:
            frame = frame.where(
                frame["status"] == status  # ty: ignore[invalid-argument-type]
            )

        del tick_range
        order: list[Expression | str] = ["accepted_at", "audit_id"]
        if limit is not None:
            if limit == 0:
                return frame.limit(0)
            frame = frame.sort(order, desc=[True, True]).limit(limit)
        return frame.sort(order)

    async def shutdown(self) -> None:
        """Project available command evidence and flush accepted access rows."""
        await self.project_outbox()
        await self.flush()


__all__ = [
    "AuditBackpressureError",
    "AuditLog",
    "DEFAULT_AUDIT_FLUSH_ROWS",
    "default_audit_storage",
]
