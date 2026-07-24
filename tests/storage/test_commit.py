# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for the storage-family commit coordinator move."""

from dataclasses import FrozenInstanceError

import pytest

from archetype.app.storage.commit import (
    CatalogCommitCoordinator as CompatibilityCoordinator,
)
from archetype.app.storage.commit import (
    CommitCoordinatorIdentity as CompatibilityIdentity,
)
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import CommitContext
from archetype.storage.catalog import SignatureRecord
from archetype.storage.commit import CatalogCommitCoordinator, CommitCoordinatorIdentity


class CommitMoveProbeComponent(Component):
    value: int = 0


class RecordingCatalog:
    def __init__(self, *, fail_publish: bool = False) -> None:
        self.fail_publish = fail_publish
        self.registered: list[SignatureRecord] = []
        self.published: list[tuple[str, str, int, dict[str, object]]] = []
        self.visibility_requests: list[tuple[str, str, list[int] | None]] = []

    async def register_signature(self, record: SignatureRecord) -> None:
        self.registered.append(record)

    async def publish_manifest(
        self,
        world_id: str,
        run_id: str,
        tick: int,
        **kwargs: object,
    ) -> None:
        if self.fail_publish:
            raise RuntimeError("injected publish failure")
        self.published.append((world_id, run_id, tick, kwargs))

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ) -> dict[int, list[str]]:
        self.visibility_requests.append((world_id, run_id, ticks))
        return {4: ["visible-token"]}


def test_compatibility_module_reexports_canonical_coordinator() -> None:
    assert CompatibilityCoordinator is CatalogCommitCoordinator
    assert CompatibilityIdentity is CommitCoordinatorIdentity


@pytest.mark.asyncio
async def test_publish_registers_once_settles_staged_commands_and_delegates_visibility() -> None:
    catalog = RecordingCatalog()
    coordinator = CatalogCommitCoordinator.bound(catalog, "world", "run", writer_epoch=7)
    signature = (CommitMoveProbeComponent,)

    coordinator.stage_command(4, "worker-a", "command-1")
    coordinator.stage_command(4, "worker-a", "command-1")
    context = await coordinator.begin_tick(4)

    assert coordinator.epoch == 7
    assert coordinator.writer_epoch == 7
    assert coordinator.identity == CommitCoordinatorIdentity(
        world_id="world",
        run_id="run",
        writer_epoch=7,
    )
    assert context.writer_epoch == 7
    assert context.commit_token
    assert coordinator.is_command_staged(4, "command-1")

    await coordinator.publish_tick(4, context, [signature])

    assert len(catalog.registered) == 1
    record = catalog.registered[0]
    assert record.table_id == Archetype.get_name(signature)
    assert record.component_names == ("CommitMoveProbeComponent",)
    assert record.matches(Archetype.get_archetype_schema(signature))
    assert catalog.published == [
        (
            "world",
            "run",
            4,
            {
                "commit_token": context.commit_token,
                "writer_epoch": 7,
                "table_ids": [record.table_id],
                "command_ids": ["command-1"],
                "lease_owner": "worker-a",
            },
        )
    ]
    assert not coordinator.is_command_staged(4, "command-1")

    next_context = await coordinator.begin_tick(5)
    await coordinator.publish_tick(5, next_context, [signature])
    assert len(catalog.registered) == 1
    assert await coordinator.visible_tokens("world", "run", [4]) == {4: ["visible-token"]}
    assert catalog.visibility_requests == [("world", "run", [4])]


def test_staging_rejects_multiple_lease_owners_for_one_tick() -> None:
    coordinator = CatalogCommitCoordinator.bound(
        RecordingCatalog(),
        "world",
        "run",
        writer_epoch=1,
    )
    coordinator.stage_command(2, "worker-a", "command-1")

    with pytest.raises(RuntimeError, match="leased by worker-a, not worker-b"):
        coordinator.stage_command(2, "worker-b", "command-2")


@pytest.mark.asyncio
async def test_failed_publish_keeps_staged_command_retryable() -> None:
    catalog = RecordingCatalog(fail_publish=True)
    coordinator = CatalogCommitCoordinator.bound(catalog, "world", "run", writer_epoch=3)
    coordinator.stage_command(6, "worker-a", "command-1")
    context = await coordinator.begin_tick(6)

    with pytest.raises(RuntimeError, match="injected publish failure"):
        await coordinator.publish_tick(6, context, [])

    assert coordinator.is_command_staged(6, "command-1")


def test_bound_coordinator_exposes_frozen_identity_and_writer_epoch() -> None:
    coordinator = CatalogCommitCoordinator.bound(
        RecordingCatalog(),
        "world",
        "run",
        writer_epoch=11,
    )

    assert coordinator.epoch == 11
    assert coordinator.writer_epoch == 11
    identity = coordinator.identity
    assert identity == CommitCoordinatorIdentity(
        world_id="world",
        run_id="run",
        writer_epoch=11,
    )
    with pytest.raises(FrozenInstanceError):
        identity.world_id = "other"


@pytest.mark.asyncio
async def test_bound_coordinator_publishes_only_to_its_bound_identity() -> None:
    catalog = RecordingCatalog()
    coordinator = CatalogCommitCoordinator.bound(
        catalog,
        "world",
        "run",
        writer_epoch=5,
    )
    context = await coordinator.begin_tick(0)
    await coordinator.publish_tick(0, context, [(CommitMoveProbeComponent,)])

    assert len(catalog.published) == 1
    assert catalog.published[0][:3] == ("world", "run", 0)


@pytest.mark.asyncio
async def test_bound_coordinator_rejects_context_from_another_writer_epoch() -> None:
    catalog = RecordingCatalog()
    coordinator = CatalogCommitCoordinator.bound(
        catalog,
        "world",
        "run",
        writer_epoch=5,
    )

    with pytest.raises(ValueError, match="commit coordinator writer epoch mismatch"):
        await coordinator.publish_tick(
            0,
            CommitContext(commit_token="foreign", writer_epoch=6),
            [(CommitMoveProbeComponent,)],
        )

    assert catalog.registered == []
    assert catalog.published == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("world_id", "run_id"),
    [("other-world", "run"), ("world", "other-run")],
)
async def test_bound_coordinator_allows_visibility_reads_for_lineage_segments(
    world_id: str,
    run_id: str,
) -> None:
    catalog = RecordingCatalog()
    coordinator = CatalogCommitCoordinator.bound(
        catalog,
        "world",
        "run",
        writer_epoch=5,
    )

    assert await coordinator.visible_tokens(world_id, run_id, [0]) == {4: ["visible-token"]}
    assert catalog.visibility_requests == [(world_id, run_id, [0])]
