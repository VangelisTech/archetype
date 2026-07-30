# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Deterministic red oracle for GitHub issue #655."""

from __future__ import annotations

import asyncio
from importlib import import_module

import pytest

from archetype.core.config import StorageConfig


class _World:
    def __init__(self) -> None:
        self.world_id = "00000000-0000-7000-8000-000000000655"
        self.name = "duplicate-destroy"


class _BlockingCatalog:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def set_world_status(self, _world_id: str, _status: str) -> None:
        self.entered.set()
        await self.release.wait()


class _Storage:
    def __init__(self, catalog: _BlockingCatalog) -> None:
        self.catalog = catalog

    def get_control_catalog(self, _storage_config: StorageConfig) -> _BlockingCatalog:
        return self.catalog


@pytest.mark.asyncio
async def test_concurrent_duplicate_destroy_is_an_idempotent_noop() -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    world = _World()
    registry = registry_module.WorldRegistry()
    await registry.insert(world, storage_config=StorageConfig())
    catalog = _BlockingCatalog()
    lifecycle = lifecycle_module.WorldLifecycle(_Storage(catalog), registry)
    lease = await lifecycle.begin_close(world.world_id)

    first = asyncio.create_task(lifecycle.destroy_world(world.world_id, lease=lease))
    await asyncio.wait_for(catalog.entered.wait(), timeout=5)
    second = asyncio.create_task(lifecycle.destroy_world(world.world_id, lease=lease))
    await asyncio.sleep(0)
    catalog.release.set()

    await asyncio.wait_for(asyncio.gather(first, second), timeout=5)
    assert not await registry.contains(world.world_id)


@pytest.mark.asyncio
async def test_stale_lease_cannot_destroy_a_replacement_world() -> None:
    lifecycle_module = import_module("archetype.world.lifecycle")
    registry_module = import_module("archetype.world.registry")
    original = _World()
    registry = registry_module.WorldRegistry()
    await registry.insert(original, storage_config=StorageConfig())
    stale_lease = await registry.begin_close(original.world_id)
    await registry.finish_close(stale_lease)

    replacement = _World()
    await registry.insert(replacement, storage_config=StorageConfig())
    lifecycle = lifecycle_module.WorldLifecycle(
        _Storage(_BlockingCatalog()),
        registry,
    )

    with pytest.raises(ValueError, match="no longer owns"):
        await lifecycle.destroy_world(replacement.world_id, lease=stale_lease)
    assert await registry.live_world(replacement.world_id) is replacement
