# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Audit Log

Append-only record of accepted-and-applied commands.
In-memory buffer with flush-to-storage capability.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import daft
import pyarrow as pa
from uuid_utils import UUID

from archetype.app.models import AuditRow

if TYPE_CHECKING:
    from archetype.app.storage_service import StorageService

logger = logging.getLogger(__name__)


class AuditLog:
    """Append-only audit log. Buffers rows in memory, flushable to storage.

    For v1, rows are kept in memory and queryable via DataFrame.
    Storage-backed persistence (Iceberg/LanceDB) is a future extension.
    """

    def __init__(self, storage_service: StorageService | None = None) -> None:
        self._storage_service = storage_service
        self._rows: list[AuditRow] = []

    async def record(self, row: AuditRow) -> None:
        """Buffer one audit row."""
        self._rows.append(row)

    async def flush(self) -> None:
        """Flush buffered rows. Currently a no-op (in-memory only for v1)."""
        # Future: write to storage via self._storage_service
        pass

    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        signer_address: str | None = None,
        idempotency_key: str | None = None,
        limit: int | None = None,
    ) -> daft.DataFrame:
        """Query the audit log with optional filters. Returns a Daft DataFrame."""
        filtered = self._rows

        if world_id is not None:
            wid = str(world_id)
            filtered = [r for r in filtered if str(r.world_id) == wid]

        if actor_id is not None:
            aid = str(actor_id)
            filtered = [r for r in filtered if str(r.actor_id) == aid]

        if signer_address is not None:
            filtered = [r for r in filtered if r.signer_address == signer_address]

        if idempotency_key is not None:
            filtered = [r for r in filtered if r.idempotency_key == idempotency_key]

        if limit is not None:
            filtered = filtered[-limit:]

        # Convert to DataFrame
        if not filtered:
            return daft.from_pydict({
                "audit_id": [],
                "command_id": [],
                "world_id": [],
                "actor_id": [],
                "command_type": [],
                "status": [],
                "payload_json": [],
                "accepted_at": [],
                "applied_at": [],
            })

        return daft.from_pydict({
            "audit_id": [str(r.audit_id) for r in filtered],
            "command_id": [str(r.command_id) if r.command_id else None for r in filtered],
            "world_id": [str(r.world_id) if r.world_id else None for r in filtered],
            "actor_id": [str(r.actor_id) if r.actor_id else None for r in filtered],
            "command_type": [r.command_type for r in filtered],
            "status": [r.status for r in filtered],
            "payload_json": [r.payload_json for r in filtered],
            "accepted_at": [r.accepted_at for r in filtered],
            "applied_at": [r.applied_at for r in filtered],
        })

    async def shutdown(self) -> None:
        """Flush and close. Idempotent."""
        await self.flush()


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
