# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for issue #39.

``ServiceContainer.world_service.create_world`` must default to a durable
LanceDB storage backend so that state persists across process restarts
even when the caller does not pass an explicit ``StorageConfig``.
"""

from __future__ import annotations

import os
import stat

import pytest

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig


class _Tag(Component):
    label: str = ""


class TestDefaultStorageDurable:
    @pytest.mark.asyncio
    async def test_default_create_world_uses_lancedb(self, tmp_path, monkeypatch):
        """Without an explicit StorageConfig, the factory wires LanceDB."""
        from archetype.app.storage_service import AsyncLancedbStore

        monkeypatch.chdir(tmp_path)

        container = ServiceContainer()
        try:
            world = await container.world_service.create_world(WorldConfig(name="default"))
            # The querier/updater must be backed by the real LanceDB store,
            # not an in-memory stub.
            store = container.storage_service._instances
            assert store, "StorageService should have at least one backend instance"
            any_store = next(iter(store.values()))[0]
            assert isinstance(any_store, AsyncLancedbStore)
            assert world.world_id is not None
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_entity_survives_container_restart_with_defaults(self, tmp_path, monkeypatch):
        """Spawn an entity in one ServiceContainer, reopen it, verify persistence."""
        from archetype.core.archetype import Archetype
        from archetype.core.config import RunConfig

        monkeypatch.chdir(tmp_path)
        registry_path = tmp_path / "registry.json"

        # First container: create a world with default storage, spawn an entity, step.
        c1 = ServiceContainer(registry_path=registry_path)
        try:
            world1 = await c1.world_service.create_world(WorldConfig(name="persist"))
            wid = world1.world_id
            await world1.create_entity([_Tag(label="survivor")])
            # Step once so the spawn cache materializes and writes through updater.
            run_config = RunConfig(num_steps=1)
            saved_run_id = str(run_config.run_id)
            await c1.simulation_service.step(wid, run_config=run_config)
        finally:
            await c1.shutdown()

        # Default path should have been created under cwd.
        assert (tmp_path / "archetype_data").exists()

        # Second container: rediscover via the persistent registry, query state.
        c2 = ServiceContainer(registry_path=registry_path)
        try:
            loaded = await c2.world_service.discover_worlds()
            assert wid in loaded

            rehydrated = c2.world_service.get_world(wid)
            sig = Archetype.sig_from_components([_Tag()])
            df = await rehydrated.querier.query_archetype(
                sig=sig,
                world_id=wid,
                run_id=saved_run_id,
            )
            rows = df.to_pylist()
            labels = [row.get("_tag__label") for row in rows]
            assert "survivor" in labels, f"Spawned entity did not survive restart; got rows: {rows}"
        finally:
            await c2.shutdown()

    @pytest.mark.asyncio
    async def test_explicit_storage_config_is_honored(self, tmp_path):
        """Passing a StorageConfig explicitly must not be overridden by defaults."""
        explicit = StorageConfig(uri=str(tmp_path / "custom"), namespace="explicit_ns")

        container = ServiceContainer()
        try:
            await container.world_service.create_world(WorldConfig(name="explicit"), explicit)
            # The multiton key on StorageService encodes uri+namespace.
            keys = list(container.storage_service._instances.keys())
            assert any("custom" in k and "explicit_ns" in k for k in keys), keys
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_unwritable_uri_raises_clear_error(self, tmp_path):
        """Unwritable URIs must raise PermissionError with a helpful message."""
        if os.geteuid() == 0:
            pytest.skip("cannot reliably test write perms as root")

        locked = tmp_path / "locked"
        locked.mkdir()
        # Make directory read+execute only; creating children must fail.
        locked.chmod(stat.S_IRUSR | stat.S_IXUSR)
        target = locked / "archetype_data"

        container = ServiceContainer()
        try:
            with pytest.raises(PermissionError, match="not writable"):
                await container.world_service.create_world(
                    WorldConfig(name="nope"),
                    StorageConfig(uri=str(target)),
                )
        finally:
            # Restore perms so pytest can clean up.
            locked.chmod(stat.S_IRWXU)
            await container.shutdown()
