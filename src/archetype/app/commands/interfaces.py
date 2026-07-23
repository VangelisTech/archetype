# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ports owned by the durable command family."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from uuid_utils import UUID

from archetype.app.models import Command
from archetype.storage.catalog import CommandRecord, OutboxRecord


@runtime_checkable
class iCommandScheduler(Protocol):
    """Admit, lease, dispatch, settle, and inspect durable commands."""

    @staticmethod
    def validate_deferred(command: Command) -> None: ...
    def require_world(self, world_id: Any) -> None: ...
    async def admit(
        self,
        world_id: Any,
        command: Command,
        *,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> UUID: ...
    async def admit_batch(
        self,
        world_id: Any,
        commands: list[Command],
        *,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> list[UUID]: ...
    async def admit_spawn(
        self,
        world_id: Any,
        components: list[Any],
        *,
        tick: int = 0,
        priority: int = 0,
        principal_id: str | UUID | None = None,
        origin: str = "local",
    ) -> tuple[int, Command]: ...
    async def drain_and_apply(self, world_id: Any, tick: int) -> list[Command]: ...
    async def pending_count(self, world_id: Any) -> int: ...
    async def records(
        self, world_id: Any, *, status: str | None = None, limit: int = 100
    ) -> list[CommandRecord]: ...
    async def history(self, world_id: Any, *, limit: int = 100) -> list[Command]: ...
    async def cancel_world(self, world_id: Any, *, reason: str = "world destroyed") -> int: ...
    async def read_outbox(self, *, limit: int = 1000) -> list[OutboxRecord]: ...
    async def mark_outbox_projected(self, events: list[OutboxRecord]) -> None: ...
    async def outbox_progress(self) -> dict[str, tuple[int, int]]: ...
