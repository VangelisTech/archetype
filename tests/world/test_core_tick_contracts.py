# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Red contracts for the managed command phase and committed tick receipt.

These tests deliberately install the planned constructor state through a test
subclass while the production constructor seam is still absent.  That lets the
red oracle reach tick ordering instead of stopping at an unrelated fixture
``TypeError``.  The public constructor signature is asserted independently
after the behavioral order has been proved.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass, replace
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.core.aio import (
    AsyncLancedbStore,
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig
from archetype.core.hooks import HookRegistry, PreTick
from archetype.core.interfaces import CommitContext
from archetype.core.resources import Resources

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.contract("world.tick.atomic_visibility"),
    pytest.mark.contract("commands.settlement.atomic"),
]


class DueCommandMarker(Component):
    value: int = 0


@dataclass(frozen=True, slots=True)
class _PublishedManifest:
    world_id: str
    run_id: str
    tick: int
    commit_token: str
    writer_epoch: int
    table_ids: tuple[str, ...]
    command_ids: tuple[str, ...]


class _RecordingCoordinator:
    """A protocol-compatible bound coordinator with observable settlement.

    ``*args`` accepts both the current identity-resupplying protocol and PR-2's
    frozen bound protocol.  The assertions below concern world behavior, not
    which side of the coordinator migration happens to land first.
    """

    def __init__(self) -> None:
        self.world_id = ""
        self.run_id = ""
        self.writer_epoch = 17
        self.identity: SimpleNamespace | None = None
        self.begin_ticks: list[int] = []
        self.manifests: list[_PublishedManifest] = []
        self.settled_command_ids: list[str] = []
        self._staged: dict[int, tuple[str, list[str]]] = {}

    def bind(self, world: AsyncWorld) -> None:
        self.world_id = str(world.world_id)
        self.run_id = str(world.run_id)
        self.identity = SimpleNamespace(
            world_id=self.world_id,
            run_id=self.run_id,
            writer_epoch=self.writer_epoch,
        )

    async def begin_tick(self, *args: object) -> CommitContext:
        tick = int(args[-1])
        self.begin_ticks.append(tick)
        return CommitContext(
            commit_token=f"visibility-{tick}-{len(self.begin_ticks)}",
            writer_epoch=self.writer_epoch,
        )

    def stage_command(self, tick: int, owner: str, command_id: str) -> None:
        current_owner, command_ids = self._staged.setdefault(tick, (owner, []))
        if current_owner != owner:
            raise RuntimeError("one tick cannot mix command lease owners")
        if command_id not in command_ids:
            command_ids.append(command_id)

    def is_command_staged(self, tick: int, command_id: str) -> bool:
        staged = self._staged.get(tick)
        return staged is not None and command_id in staged[1]

    async def publish_tick(self, *args: object) -> str:
        if len(args) == 5:
            world_id, run_id, tick, ctx, signatures = args
        elif len(args) == 3:
            tick, ctx, signatures = args
            world_id, run_id = self.world_id, self.run_id
        else:  # pragma: no cover - a protocol mismatch must be loud
            raise AssertionError(f"unexpected publish_tick call: {args!r}")

        assert isinstance(ctx, CommitContext)
        signature_list = list(signatures)  # type: ignore[arg-type]
        staged = self._staged.pop(int(tick), ("", []))
        command_ids = tuple(staged[1])
        self.settled_command_ids.extend(command_ids)
        self.manifests.append(
            _PublishedManifest(
                world_id=str(world_id),
                run_id=str(run_id),
                tick=int(tick),
                commit_token=ctx.commit_token,
                writer_epoch=ctx.writer_epoch,
                table_ids=tuple(Archetype.get_name(sig) for sig in signature_list),
                command_ids=command_ids,
            )
        )
        return ctx.commit_token

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]] | None:
        del world_id, run_id, ticks
        return None


Materializer = Callable[[AsyncWorld, int], Awaitable[int]]


class _MaterializerInjectedWorld(AsyncWorld):
    """Reach the missing step behavior without hiding it behind constructor red."""

    def install_materializer(self, materializer: Materializer) -> None:
        self._materialize_commands = materializer


@dataclass(slots=True)
class _WorldHarness:
    world: _MaterializerInjectedWorld
    store: AsyncLancedbStore
    coordinator: _RecordingCoordinator


@asynccontextmanager
async def _tick_world(
    tmp_path: Any,
    materializer: Materializer,
) -> AsyncIterator[_WorldHarness]:
    store = AsyncLancedbStore(str(tmp_path / "store"), namespace="tick-contract")
    coordinator = _RecordingCoordinator()
    world = _MaterializerInjectedWorld(
        world_id="00000000-0000-7000-8000-000000000001",
        name="tick-contract",
        querier=AsyncQueryManager(store=store),
        updater=AsyncUpdateManager(store=store),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
        commit_coordinator=coordinator,
    )
    world.install_materializer(materializer)
    coordinator.bind(world)
    try:
        yield _WorldHarness(world=world, store=store, coordinator=coordinator)
    finally:
        await store.shutdown()


async def test_due_command_materializes_before_pre_tick_and_signature_capture(tmp_path) -> None:
    events: list[tuple[str, int, tuple[str, ...]]] = []

    async def materialize(world: AsyncWorld, target_tick: int) -> int:
        await world.spawn_with_reserved_id(41, [DueCommandMarker(value=7)])
        events.append(
            (
                "materialize",
                target_tick,
                tuple(sorted(Archetype.get_name(sig) for sig in world.active_signatures)),
            )
        )
        return 1

    async with _tick_world(tmp_path, materialize) as harness:

        async def observe_pre_tick(event: PreTick) -> None:
            events.append(
                (
                    "pre_tick",
                    event.tick,
                    tuple(
                        sorted(Archetype.get_name(sig) for sig in harness.world.active_signatures)
                    ),
                )
            )

        harness.world.hooks.add(PreTick, observe_pre_tick)
        receipt = await harness.world.step(RunConfig())

        signature = Archetype.sig_from_components([DueCommandMarker()])
        table_id = Archetype.get_name(signature)
        assert events == [
            ("materialize", 0, (table_id,)),
            ("pre_tick", 0, (table_id,)),
        ], "the due spawn must exist before PreTick and active-signature capture"
        assert harness.coordinator.manifests[0].table_ids == (table_id,)
        rows = (
            await harness.store.get_archetype_df(
                signature,
                str(harness.world.world_id),
                str(harness.world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert [(row["entity_id"], row["duecommandmarker__value"]) for row in rows] == [(41, 7)]
        assert receipt.commands_applied == 1

        parameters = inspect.signature(AsyncWorld.__init__).parameters
        assert "materialize_commands" in parameters, (
            "CommandMaterializer must be construction state, not a hook or setter"
        )


async def test_materializer_infrastructure_failure_advances_and_settles_nothing(
    tmp_path,
) -> None:
    attempts = 0

    async def fail_once(world: AsyncWorld, target_tick: int) -> int:
        nonlocal attempts
        attempts += 1
        assert target_tick == 0
        coordinator = world.commit_coordinator
        assert coordinator is not None
        if not coordinator.is_command_staged(target_tick, "command-1"):
            coordinator.stage_command(target_tick, "worker-1", "command-1")
        if attempts == 1:
            raise RuntimeError("control catalog lease interrupted")
        return 1

    async with _tick_world(tmp_path, fail_once) as harness:
        await harness.world.create_entity([DueCommandMarker(value=3)])
        signature = next(iter(harness.world.spawn_cache))
        cached_before = [dict(row) for row in harness.world.spawn_cache[signature]]
        next_id_before = harness.world.next_entity_id

        with pytest.raises(RuntimeError, match="control catalog lease interrupted"):
            await harness.world.step(RunConfig())

        assert harness.world.tick == 0
        assert harness.world.next_entity_id == next_id_before
        assert harness.world.spawn_cache[signature] == cached_before
        assert harness.coordinator.begin_ticks == []
        assert harness.coordinator.manifests == []
        assert harness.coordinator.settled_command_ids == []
        assert harness.coordinator.is_command_staged(0, "command-1")

        receipt = await harness.world.step(RunConfig())

        assert attempts == 2
        assert harness.world.tick == 1
        assert receipt.committed_tick == 0
        assert receipt.commands_applied == 1
        assert harness.coordinator.settled_command_ids == ["command-1"]
        assert [manifest.tick for manifest in harness.coordinator.manifests] == [0]
        assert signature not in harness.world.spawn_cache


async def test_committed_receipt_is_frozen_and_manifest_bound(tmp_path) -> None:
    async def materialize(_world: AsyncWorld, target_tick: int) -> int:
        assert target_tick == 0
        return 2

    async with _tick_world(tmp_path, materialize) as harness:
        await harness.world.create_entity([DueCommandMarker(value=9)])
        receipt = await harness.world.step(RunConfig())
        (manifest,) = harness.coordinator.manifests

        assert type(receipt).__name__ == "CommittedTickReceipt"
        assert (
            receipt.world_id,
            receipt.run_id,
            receipt.committed_tick,
            receipt.visibility_token,
        ) == (
            manifest.world_id,
            manifest.run_id,
            manifest.tick,
            manifest.commit_token,
        )
        assert receipt.identity == (
            manifest.world_id,
            manifest.run_id,
            manifest.tick,
            manifest.commit_token,
        )
        assert receipt.commands_applied == 2

        same_commit_different_diagnostic = replace(receipt, commands_applied=999)
        assert same_commit_different_diagnostic.identity == receipt.identity
        assert same_commit_different_diagnostic != receipt
        with pytest.raises(FrozenInstanceError):
            receipt.committed_tick = 99
