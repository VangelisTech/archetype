# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Subprocess fixture that hard-exits while owning a committed world fork."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig
from archetype.core.hooks import OnDestroy
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources
from archetype.world.models import (
    AddHook,
    ComponentValue,
    CreateWorld,
    ForkWorld,
    Spawn,
    Step,
    Update,
)
from archetype.world.registry import WorldRegistry


class TemporalCounter(Component):
    value: int = 0


async def main() -> None:
    control_dir = Path(sys.argv[1])
    storage_uri = sys.argv[2]
    checkpoint_path = Path(sys.argv[3])
    destroyed_marker = Path(sys.argv[4])
    workflow_id = sys.argv[5]
    destination_world_id = sys.argv[6]

    control = ControlCatalogConfig(catalog_dir=control_dir)
    storage_service = StorageService(control_catalog_config=control)
    worlds = WorldRegistry()
    resources = build_runtime_resources(
        RuntimeBootstrapConfig(
            control_catalog_config=control,
            storage_service=storage_service,
            world_registry=worlds,
        )
    )
    storage = StorageConfig(uri=storage_uri, namespace="fork-proof")
    parent = await resources.dispatcher.apply(
        CreateWorld(
            config=WorldConfig(name="temporal-fork-parent"),
            storage_config=storage,
        )
    )
    entity_id = await resources.dispatcher.apply(
        Spawn.from_components(
            world_id=parent.world_id,
            components=[TemporalCounter(value=21)],
        )
    )
    await resources.dispatcher.apply(Step(world_id=parent.world_id))
    fork = await resources.dispatcher.apply(
        ForkWorld(
            source_world_id=parent.world_id,
            destination_world_id=destination_world_id,
            name="temporal-fork-child",
        )
    )

    async def on_destroy(_event: OnDestroy) -> None:
        destroyed_marker.write_text("destroyed", encoding="utf-8")

    await resources.dispatcher.apply(
        AddHook(
            world_id=fork.world_id,
            event_type=OnDestroy,
            handler=on_destroy,
        )
    )
    await resources.dispatcher.apply(
        Update(
            world_id=parent.world_id,
            entity_id=entity_id,
            components=(ComponentValue.from_component(TemporalCounter(value=22)),),
        )
    )
    await resources.dispatcher.apply(Step(world_id=parent.world_id))
    await resources.dispatcher.apply(
        Update(
            world_id=fork.world_id,
            entity_id=entity_id,
            components=(ComponentValue.from_component(TemporalCounter(value=100)),),
        )
    )
    await resources.dispatcher.apply(Step(world_id=fork.world_id))

    catalog = storage_service.get_control_catalog(storage)
    parent_record = await catalog.get_world(str(parent.world_id))
    fork_record = await catalog.get_world(str(fork.world_id))
    parent_manifests = await catalog.list_manifests(str(parent.world_id), str(parent.run_id))
    manifests = await catalog.list_manifests(str(fork.world_id), str(fork.run_id))
    if parent_record is None or fork_record is None:
        raise RuntimeError("committed parent and fork must be catalog-visible before crash")
    async with worlds.operation(fork.world_id) as live_fork:
        coordinator = live_fork.commit_coordinator
        if coordinator is None:
            raise RuntimeError("fork recovery proof requires a fenced writer")
        fork_writer_epoch = coordinator.writer_epoch
        lineage = list(live_fork.lineage)
    checkpoint_path.write_text(
        json.dumps(
            {
                "workflow_id": workflow_id,
                "parent_world_id": str(parent.world_id),
                "parent_run_id": str(parent.run_id),
                "fork_world_id": str(fork.world_id),
                "fork_run_id": str(fork.run_id),
                "entity_id": entity_id,
                "parent_tick": parent_record.tick_head + 1,
                "fork_tick": fork_record.tick_head + 1,
                "fork_status": fork_record.status,
                "fork_writer_epoch": fork_writer_epoch,
                "lineage_json": json.dumps(lineage, separators=(",", ":")),
                "parent_manifest_tokens_json": json.dumps(
                    {str(item.tick): item.commit_token for item in parent_manifests},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "manifest_tokens_json": json.dumps(
                    {str(item.tick): item.commit_token for item in manifests},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    # Deliberately bypass every cleanup hook and context manager. Exit 17 is
    # the fixture's explicit crash sentinel, not a successful process close.
    os._exit(17)


if __name__ == "__main__":
    asyncio.run(main())
