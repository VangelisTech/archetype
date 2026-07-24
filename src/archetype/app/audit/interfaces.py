# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the audit family."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from daft import DataFrame
from uuid_utils import UUID

from archetype.app.models import AuditRow, Command
from archetype.storage.catalog import OutboxRecord


@runtime_checkable
class iAuditLog(Protocol):
    """Project and query append-only workflow and access evidence."""

    def set_outbox_source(
        self,
        source: Callable[..., Awaitable[list[OutboxRecord]]],
        acknowledge: Callable[[list[OutboxRecord]], Awaitable[None]],
    ) -> None: ...
    async def record(self, row: AuditRow) -> None: ...
    async def flush(self) -> None: ...
    async def project_outbox(self, *, limit: int = 1000) -> int: ...
    async def query(
        self,
        world_id: str | UUID | None = None,
        *,
        tick_range: tuple[int, int] | None = None,
        actor_id: str | UUID | None = None,
        idempotency_key: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> DataFrame: ...
    async def get_command_history(
        self,
        world_id: str | UUID,
        limit: int = 100,
    ) -> list[Command]: ...
    async def shutdown(self) -> None: ...
