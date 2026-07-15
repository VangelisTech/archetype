# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog-backed commit coordinator (issue #273).

Implements the core ``iCommitCoordinator`` protocol over the SQLite control
catalog: one fenced writer epoch per world, one commit token per tick
attempt, manifest published LAST. Core never imports this module — the
coordinator is injected into worlds by WorldService, the same way stores are.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from uuid_utils import uuid7

from archetype.core.interfaces import CommitContext

if TYPE_CHECKING:
    from archetype.app._catalog import SqliteControlCatalog


class CatalogCommitCoordinator:
    """One coordinator per (world, fence acquisition).

    The epoch is acquired once — at world creation, fork, or (later) fenced
    resume — and every tick this writer commits carries it. begin_tick is
    pure token minting (no catalog I/O); publish_tick is the single catalog
    transaction per tick and fails closed on a lost fence.
    """

    def __init__(self, catalog: SqliteControlCatalog, *, epoch: int) -> None:
        self._catalog = catalog
        self._epoch = epoch

    @property
    def epoch(self) -> int:
        return self._epoch

    async def begin_tick(self, world_id: str, run_id: str, tick: int) -> CommitContext:
        return CommitContext(commit_token=uuid7().hex, writer_epoch=self._epoch)

    async def publish_tick(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        ctx: CommitContext,
        table_ids: list[str],
    ) -> None:
        await self._catalog.publish_manifest(
            world_id,
            run_id,
            tick,
            commit_token=ctx.commit_token,
            writer_epoch=ctx.writer_epoch,
            table_ids=table_ids,
        )

    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, str] | None:
        return await self._catalog.visible_tokens(world_id, run_id, ticks)
