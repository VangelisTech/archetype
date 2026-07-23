# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog-backed commit coordinator (issue #273).

Implements the core ``iCommitCoordinator`` protocol over the selected control
catalog: one fenced writer epoch per world, one commit token per tick
attempt, manifest published LAST. Core never imports this module — the
coordinator is injected into worlds by WorldService, the same way stores are.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from uuid_utils import uuid7

from archetype.core.archetype import Archetype
from archetype.core.interfaces import ArchetypeSignature, CommitContext

if TYPE_CHECKING:
    from archetype.storage.catalog import ControlCatalog


@dataclass(frozen=True, slots=True)
class CommitCoordinatorIdentity:
    """Durable identity one coordinator is authorized to publish for."""

    world_id: str
    run_id: str
    writer_epoch: int


class CatalogCommitCoordinator:
    """One coordinator per (world, fence acquisition).

    The epoch is acquired once — at world creation, fork, or fenced resume —
    and every tick this writer commits carries it. begin_tick is pure token
    minting (no catalog I/O); publish_tick is the per-tick catalog write and
    fails closed on a lost fence.

    Signature registration rides publication: the first commit that touches
    a table registers its durable descriptor, so discovery and resume work
    for every coordinated world regardless of which layer drives its steps
    (the AutoResearch lab, for one, steps its world directly). Memoized —
    the steady-state cost stays one manifest transaction per tick.
    """

    def __init__(
        self,
        catalog: ControlCatalog,
        *,
        identity: CommitCoordinatorIdentity,
    ) -> None:
        self._catalog = catalog
        self._identity = identity
        self._registered: set[str] = set()
        self._staged_commands: dict[int, tuple[str, list[str]]] = {}

    @classmethod
    def bound(
        cls,
        catalog: ControlCatalog,
        world_id: str,
        run_id: str,
        writer_epoch: int,
    ) -> Self:
        """Construct a coordinator pinned to one world, run, and writer epoch."""
        return cls(
            catalog,
            identity=CommitCoordinatorIdentity(
                world_id=world_id,
                run_id=run_id,
                writer_epoch=writer_epoch,
            ),
        )

    @property
    def epoch(self) -> int:
        return self._identity.writer_epoch

    @property
    def writer_epoch(self) -> int:
        """Return the fenced writer epoch used for every begun tick."""
        return self._identity.writer_epoch

    @property
    def identity(self) -> CommitCoordinatorIdentity:
        """Return the immutable world, run, and writer identity."""
        return self._identity

    async def begin_tick(self, tick: int) -> CommitContext:
        """Mint a commit context for this coordinator's bound identity."""
        del tick
        return CommitContext(
            commit_token=uuid7().hex,
            writer_epoch=self._identity.writer_epoch,
        )

    def stage_command(self, tick: int, owner: str, command_id: str) -> None:
        """Attach an in-memory staged mutation to its future manifest commit."""
        current_owner, command_ids = self._staged_commands.setdefault(tick, (owner, []))
        if current_owner != owner:
            raise RuntimeError(
                f"tick {tick} already has staged commands leased by {current_owner}, not {owner}"
            )
        if command_id not in command_ids:
            command_ids.append(command_id)

    def is_command_staged(self, tick: int, command_id: str) -> bool:
        staged = self._staged_commands.get(tick)
        return staged is not None and command_id in staged[1]

    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[ArchetypeSignature],
    ) -> None:
        identity = self._identity
        if ctx.writer_epoch != identity.writer_epoch:
            raise ValueError(
                "commit coordinator writer epoch mismatch: "
                f"bound to writer_epoch={identity.writer_epoch}; "
                f"received writer_epoch={ctx.writer_epoch}"
            )

        from archetype.storage.catalog import (
            SignatureRecord,
            arrow_schema_descriptor,
            schema_fingerprint,
        )

        table_ids = []
        for sig in sigs:
            table_id = Archetype.get_name(sig)
            table_ids.append(table_id)
            if table_id in self._registered:
                continue
            schema = Archetype.get_archetype_schema(sig)
            await self._catalog.register_signature(
                SignatureRecord(
                    table_id=table_id,
                    component_names=tuple(c.__name__ for c in sig),
                    schema_json=json.dumps(arrow_schema_descriptor(schema)),
                    fingerprint=schema_fingerprint(schema),
                )
            )
            self._registered.add(table_id)

        staged = self._staged_commands.get(tick)
        owner, command_ids = staged if staged is not None else (None, [])
        await self._catalog.publish_manifest(
            identity.world_id,
            identity.run_id,
            tick,
            commit_token=ctx.commit_token,
            writer_epoch=ctx.writer_epoch,
            table_ids=table_ids,
            command_ids=command_ids,
            lease_owner=owner,
        )
        self._staged_commands.pop(tick, None)

    def acknowledge_published_tick(self, tick: int, ctx: CommitContext) -> None:
        """Release local staging after an authoritative exact-token read.

        The manifest transaction already settled the command IDs. This method
        performs no catalog write, so a later fence holder cannot prevent the
        old world from finalizing its already-committed receipt.
        """
        if ctx.writer_epoch != self._identity.writer_epoch:
            raise ValueError(
                "commit coordinator writer epoch mismatch: "
                f"bound to writer_epoch={self._identity.writer_epoch}; "
                f"received writer_epoch={ctx.writer_epoch}"
            )
        self._staged_commands.pop(tick, None)

    async def visible_tokens(
        self, world_id: str, run_id: str, ticks: list[int] | None = None
    ) -> dict[int, list[str]] | None:
        # Binding fences writes, not reads. A fork's coordinator must be able
        # to inspect an ancestor segment while reconstructing lineage.
        return await self._catalog.visible_tokens(world_id, run_id, ticks)
