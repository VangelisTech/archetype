# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the managed command phase and committed tick receipt."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass, replace
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError
from uuid_utils import UUID, uuid7

from archetype.core.aio import (
    AsyncLancedbStore,
    AsyncQueryManager,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.errors import AmbiguousTickCommitError
from archetype.core.hooks import HookRegistry, PostTick, PreTick
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
    """A protocol-compatible bound coordinator with observable settlement."""

    def __init__(self) -> None:
        self.world_id = ""
        self.run_id = ""
        self.writer_epoch = 17
        self.identity: SimpleNamespace | None = None
        self.begin_ticks: list[int] = []
        self.manifests: list[_PublishedManifest] = []
        self.settled_command_ids: list[str] = []
        self.acknowledged_ticks: list[tuple[int, str]] = []
        self._staged: dict[int, tuple[str, list[str]]] = {}

    def bind(self, world: AsyncWorld) -> None:
        self.world_id = str(world.world_id)
        self.run_id = str(world.run_id)
        self.identity = SimpleNamespace(
            world_id=self.world_id,
            run_id=self.run_id,
            writer_epoch=self.writer_epoch,
        )

    async def begin_tick(self, tick: int) -> CommitContext:
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

    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[tuple[type[Component], ...]],
    ) -> None:
        signature_list = list(sigs)
        staged = self._staged.pop(tick, ("", []))
        command_ids = tuple(staged[1])
        self.settled_command_ids.extend(command_ids)
        self.manifests.append(
            _PublishedManifest(
                world_id=self.world_id,
                run_id=self.run_id,
                tick=tick,
                commit_token=ctx.commit_token,
                writer_epoch=ctx.writer_epoch,
                table_ids=tuple(Archetype.get_name(sig) for sig in signature_list),
                command_ids=command_ids,
            )
        )

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]] | None:
        del world_id, run_id, ticks
        return None

    def acknowledge_published_tick(self, tick: int, ctx: CommitContext) -> None:
        self.acknowledged_ticks.append((tick, ctx.commit_token))
        self._staged.pop(tick, None)


class _CommittedResponseLossCoordinator(_RecordingCoordinator):
    """Commit the first manifest, then lose only its caller response."""

    def __init__(self) -> None:
        super().__init__()
        self.publish_tokens: list[str] = []
        self.settlement_effects = 0
        self._lost_response = False

    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[tuple[type[Component], ...]],
    ) -> None:
        self.publish_tokens.append(ctx.commit_token)
        existing = next((manifest for manifest in self.manifests if manifest.tick == tick), None)
        if existing is not None:
            if existing.commit_token != ctx.commit_token:
                raise RuntimeError("published tick retried with a different commit identity")
            self._staged.pop(tick, None)
            return

        signature_list = list(sigs)
        staged = self._staged.get(tick, ("", []))
        self.manifests.append(
            _PublishedManifest(
                world_id=self.world_id,
                run_id=self.run_id,
                tick=tick,
                commit_token=ctx.commit_token,
                writer_epoch=ctx.writer_epoch,
                table_ids=tuple(Archetype.get_name(sig) for sig in signature_list),
                command_ids=tuple(staged[1]),
            )
        )
        self.settled_command_ids.extend(staged[1])
        self.settlement_effects += 1
        if not self._lost_response:
            self._lost_response = True
            raise RuntimeError("manifest committed but response was lost")

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]]:
        del world_id, run_id
        selected = set(ticks) if ticks is not None else None
        return {
            manifest.tick: [manifest.commit_token]
            for manifest in self.manifests
            if selected is None or manifest.tick in selected
        }


class _UnreadableResponseLossCoordinator(_CommittedResponseLossCoordinator):
    def __init__(self) -> None:
        super().__init__()
        self._lose_visibility_response = True

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]]:
        if self._lose_visibility_response and self.manifests:
            self._lose_visibility_response = False
            raise RuntimeError("visibility authority unavailable")
        return await super().visible_tokens(world_id, run_id, ticks)


class _CompetingHeadCoordinator(_RecordingCoordinator):
    def __init__(self, *, legacy_none: bool = False) -> None:
        super().__init__()
        self.legacy_none = legacy_none
        self.publish_tokens: list[str] = []
        self.publish_attempted = False

    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[tuple[type[Component], ...]],
    ) -> None:
        del tick, sigs
        self.publish_attempted = True
        self.publish_tokens.append(ctx.commit_token)
        raise RuntimeError("manifest publish response failed")

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]] | None:
        del world_id, run_id, ticks
        if not self.publish_attempted:
            return {}
        if self.legacy_none:
            return None
        return {0: ["competing-token"]}


class _CancelledPublishCoordinator(_RecordingCoordinator):
    def __init__(self) -> None:
        super().__init__()
        self.publish_started = asyncio.Event()
        self.never_released = asyncio.Event()
        self.publish_tokens: list[str] = []

    async def publish_tick(
        self,
        tick: int,
        ctx: CommitContext,
        sigs: list[tuple[type[Component], ...]],
    ) -> None:
        self.publish_tokens.append(ctx.commit_token)
        if len(self.publish_tokens) == 1:
            self.publish_started.set()
            await self.never_released.wait()
        await super().publish_tick(tick, ctx, sigs)


Materializer = Callable[[AsyncWorld, int], Awaitable[int]]


@dataclass(slots=True)
class _WorldHarness:
    world: AsyncWorld
    store: AsyncLancedbStore
    coordinator: _RecordingCoordinator


@asynccontextmanager
async def _tick_world(
    tmp_path: Any,
    materializer: Materializer,
    *,
    coordinator: _RecordingCoordinator | None = None,
) -> AsyncIterator[_WorldHarness]:
    store = AsyncLancedbStore(str(tmp_path / "store"), namespace="tick-contract")
    coordinator = coordinator or _RecordingCoordinator()
    world = AsyncWorld(
        world_id="00000000-0000-7000-8000-000000000001",
        name="tick-contract",
        querier=AsyncQueryManager(store=store),
        updater=AsyncUpdateManager(store=store),
        system=AsyncSystem(),
        resources=Resources(),
        hooks=HookRegistry(),
        commit_coordinator=coordinator,
        materialize_commands=materializer,
    )
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


async def test_committed_response_loss_retries_exact_prepared_tick_without_recompute(
    tmp_path,
) -> None:
    materializations = 0
    coordinator = _CommittedResponseLossCoordinator()

    async def materialize(world: AsyncWorld, target_tick: int) -> int:
        nonlocal materializations
        materializations += 1
        assert target_tick == 0
        if not world.commit_coordinator.is_command_staged(target_tick, "command-1"):
            world.commit_coordinator.stage_command(target_tick, "worker-1", "command-1")
        return 1

    async with _tick_world(
        tmp_path,
        materialize,
        coordinator=coordinator,
    ) as harness:
        await harness.world.create_entity([DueCommandMarker(value=12)])
        signature = next(iter(harness.world.spawn_cache))

        receipt = await harness.world.step(RunConfig())

        assert materializations == 1, "a prepared tick cannot materialize commands twice"
        assert coordinator.begin_ticks == [0], "one tick owns one stable commit identity"
        assert coordinator.publish_tokens == [receipt.visibility_token]
        assert coordinator.acknowledged_ticks == [(0, receipt.visibility_token)]
        assert coordinator.settled_command_ids == ["command-1"]
        assert coordinator.settlement_effects == 1
        assert signature not in harness.world.spawn_cache
        rows = (
            await harness.store.get_archetype_df(
                signature,
                str(harness.world.world_id),
                str(harness.world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(rows) == 1, "publication retry must not append the prepared frame twice"


async def test_unreadable_publish_outcome_blocks_mutation_until_exact_retry(
    tmp_path,
) -> None:
    from archetype.world.mutation import _add_resource_locked

    coordinator = _UnreadableResponseLossCoordinator()
    post_tick_events: list[str] = []

    async def no_commands(_world: AsyncWorld, _target_tick: int) -> int:
        return 0

    async def existing_post_tick(_event: PostTick) -> None:
        post_tick_events.append("existing")

    async def late_post_tick(_event: PostTick) -> None:
        post_tick_events.append("late")

    async with _tick_world(
        tmp_path,
        no_commands,
        coordinator=coordinator,
    ) as harness:
        await harness.world.create_entity([DueCommandMarker(value=21)])
        existing_handle = harness.world.add_hook(PostTick, existing_post_tick)
        next_entity_id = harness.world.next_entity_id

        with pytest.raises(AmbiguousTickCommitError) as raised:
            await harness.world.step(RunConfig())

        assert raised.value.tick == 0
        assert harness.world.tick == 0
        assert harness.world.has_prepared_tick_commit
        with pytest.raises(RuntimeError, match="prepared tick"):
            await harness.world.create_entity([DueCommandMarker(value=22)])
        with pytest.raises(RuntimeError, match="prepared tick"):
            harness.world.add_hook(PostTick, late_post_tick)
        with pytest.raises(RuntimeError, match="prepared tick"):
            harness.world.remove_hook(existing_handle)
        with pytest.raises(RuntimeError, match="prepared tick"):
            _add_resource_locked(harness.world, object())
        assert harness.world.next_entity_id == next_entity_id

        receipt = await harness.world.step(RunConfig())

        assert receipt.committed_tick == 0
        assert harness.world.tick == 1
        assert not harness.world.has_prepared_tick_commit
        assert coordinator.begin_ticks == [0]
        assert coordinator.publish_tokens == [
            receipt.visibility_token,
            receipt.visibility_token,
        ]
        assert coordinator.settlement_effects == 1
        assert post_tick_events == ["existing"]


@pytest.mark.parametrize("legacy_none", [False, True])
async def test_non_exact_manifest_state_stays_prepared_and_never_replays(
    tmp_path,
    legacy_none: bool,
) -> None:
    coordinator = _CompetingHeadCoordinator(legacy_none=legacy_none)
    materializations = 0

    async def no_commands(_world: AsyncWorld, _target_tick: int) -> int:
        nonlocal materializations
        materializations += 1
        return 0

    async with _tick_world(
        tmp_path,
        no_commands,
        coordinator=coordinator,
    ) as harness:
        await harness.world.create_entity([DueCommandMarker(value=23)])
        signature = next(iter(harness.world.spawn_cache))
        expected_error = AmbiguousTickCommitError if legacy_none else RuntimeError

        with pytest.raises(expected_error):
            await harness.world.step(RunConfig())
        with pytest.raises(expected_error):
            await harness.world.step(RunConfig())

        assert harness.world.tick == 0
        assert harness.world.last_committed_receipt is None
        assert harness.world.has_prepared_tick_commit
        assert signature in harness.world.spawn_cache
        assert materializations == 1
        assert coordinator.begin_ticks == [0]
        assert len(coordinator.publish_tokens) == 2
        assert len(set(coordinator.publish_tokens)) == 1
        with pytest.raises(RuntimeError, match="prepared tick"):
            await harness.world.create_entity([DueCommandMarker(value=24)])
        physical_rows = (
            await harness.store.get_archetype_df(
                signature,
                str(harness.world.world_id),
                str(harness.world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 1


async def test_cancellation_during_manifest_publish_retains_prepared_identity_only(
    tmp_path,
) -> None:
    coordinator = _CancelledPublishCoordinator()
    materializations = 0

    async def materialize(world: AsyncWorld, target_tick: int) -> int:
        nonlocal materializations
        materializations += 1
        if not world.commit_coordinator.is_command_staged(target_tick, "command-1"):
            world.commit_coordinator.stage_command(target_tick, "worker-1", "command-1")
        return 1

    async with _tick_world(
        tmp_path,
        materialize,
        coordinator=coordinator,
    ) as harness:
        await harness.world.create_entity([DueCommandMarker(value=25)])
        signature = next(iter(harness.world.spawn_cache))
        task = asyncio.create_task(harness.world.step(RunConfig()))
        await coordinator.publish_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert harness.world.tick == 0
        assert harness.world.last_committed_receipt is None
        assert harness.world.has_prepared_tick_commit
        assert signature in harness.world.spawn_cache
        assert coordinator.is_command_staged(0, "command-1")

        receipt = await harness.world.step(RunConfig())

        assert receipt.committed_tick == 0
        assert receipt.visibility_token == coordinator.publish_tokens[0]
        assert materializations == 1
        assert coordinator.begin_ticks == [0]
        assert coordinator.publish_tokens == [
            receipt.visibility_token,
            receipt.visibility_token,
        ]
        assert coordinator.settled_command_ids == ["command-1"]
        assert signature not in harness.world.spawn_cache
        physical_rows = (
            await harness.store.get_archetype_df(
                signature,
                str(harness.world.world_id),
                str(harness.world.run_id),
                ticks=[0],
            )
        ).to_pylist()
        assert len(physical_rows) == 1


async def test_post_tick_cancellation_retains_committed_receipt_for_required_projection(
    tmp_path,
) -> None:
    from archetype.world.registry import WorldRegistry
    from archetype.world.simulation import RequiredProjector, step

    hook_entered = asyncio.Event()
    never_released = asyncio.Event()
    projected = []
    events: list[str] = []
    materialized_ticks: list[int] = []

    async def no_commands(_world: AsyncWorld, target_tick: int) -> int:
        materialized_ticks.append(target_tick)
        events.append(f"materialize:{target_tick}")
        return 0

    async def block_post_tick(_event: PostTick) -> None:
        hook_entered.set()
        await never_released.wait()

    async def project(receipt) -> None:
        projected.append(receipt)
        events.append(f"project:{receipt.committed_tick}")

    async with _tick_world(tmp_path, no_commands) as harness:
        await harness.world.create_entity([DueCommandMarker(value=13)])
        hook_handle = harness.world.hooks.add(PostTick, block_post_tick)
        registry = WorldRegistry()
        await registry.insert(
            harness.world,
            required_projector=RequiredProjector(
                consumer_name="test.required-index",
                project=project,
            ),
        )

        task = asyncio.create_task(step(registry, harness.world.world_id, RunConfig()))
        await hook_entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert harness.world.tick == 1
        pending = registry.pending_receipt(harness.world.world_id)
        assert pending is not None
        assert pending is harness.world.last_committed_receipt
        assert pending.committed_tick == 0
        assert pending.visibility_token == harness.coordinator.manifests[0].commit_token
        assert projected == []

        harness.world.hooks.remove(hook_handle)
        await step(registry, harness.world.world_id, RunConfig())

        assert [receipt.committed_tick for receipt in projected] == [0, 1]
        assert materialized_ticks == [0, 1]
        assert events == [
            "materialize:0",
            "project:0",
            "materialize:1",
            "project:1",
        ]
        assert registry.pending_receipt(harness.world.world_id) is None


async def test_post_tick_handler_self_cancellation_remains_advisory(tmp_path) -> None:
    events: list[str] = []

    async def no_commands(_world: AsyncWorld, _target_tick: int) -> int:
        return 0

    async def cancel_handler(_event: PostTick) -> None:
        events.append("cancel")
        raise asyncio.CancelledError("handler stopped itself")

    async def later_handler(_event: PostTick) -> None:
        events.append("later")

    async with _tick_world(tmp_path, no_commands) as harness:
        await harness.world.create_entity([DueCommandMarker(value=14)])
        harness.world.hooks.add(PostTick, cancel_handler)
        harness.world.hooks.add(PostTick, later_handler)

        receipt = await harness.world.step(RunConfig())

        assert receipt.committed_tick == 0
        assert events == ["cancel", "later"]


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


async def test_run_identity_is_immutable_uuid7_construction_state(tmp_path) -> None:
    async def no_commands(_world: AsyncWorld, _target_tick: int) -> int:
        return 0

    assert "run_id" not in RunConfig.model_fields
    assert "run_id" not in WorldConfig.model_fields
    with pytest.raises(ValidationError):
        RunConfig.model_validate({"run_id": str(uuid7())})
    with pytest.raises(ValidationError):
        WorldConfig.model_validate({"run_id": str(uuid7())})

    async with _tick_world(tmp_path, no_commands) as harness:
        run_id = harness.world.run_id
        assert isinstance(run_id, UUID)
        assert run_id.version == 7

        with pytest.raises(AttributeError):
            harness.world.run_id = uuid7()
        with pytest.raises(AttributeError):
            harness.world.commit_coordinator = None

        restored_run_id = uuid7()
        restored = AsyncWorld(
            world_id="00000000-0000-7000-8000-000000000002",
            name="restored",
            querier=harness.world.querier,
            updater=harness.world.updater,
            system=AsyncSystem(),
            resources=Resources(),
            hooks=HookRegistry(),
            run_id=restored_run_id,
        )
        assert restored.run_id == restored_run_id
        assert restored.run_id.version == 7

        with pytest.raises(ValueError, match="UUIDv7"):
            AsyncWorld(
                world_id="00000000-0000-7000-8000-000000000003",
                name="invalid-restored",
                querier=harness.world.querier,
                updater=harness.world.updater,
                system=AsyncSystem(),
                resources=Resources(),
                hooks=HookRegistry(),
                run_id="00000000-0000-4000-8000-000000000003",
            )
