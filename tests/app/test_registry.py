# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for persistent WorldRegistry and cross-container discovery."""

from __future__ import annotations

import json

import pytest

from archetype.app.container import ServiceContainer
from archetype.app.registry import (
    REGISTRY_ENV_VAR,
    WorldRegistry,
    default_registry_path,
)
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class TestWorldRegistry:
    def test_empty_when_missing(self, tmp_path):
        reg = WorldRegistry(tmp_path / "reg.json")
        assert reg.load() == {}
        assert reg.list_entries() == []
        assert reg.get("anything") is None

    def test_upsert_and_delete_roundtrip(self, tmp_path):
        reg = WorldRegistry(tmp_path / "reg.json")
        reg.upsert(
            "00000000-0000-0000-0000-000000000001",
            {"world_id": "00000000-0000-0000-0000-000000000001", "name": "a", "tick": 0},
        )
        assert reg.get("00000000-0000-0000-0000-000000000001") == {
            "world_id": "00000000-0000-0000-0000-000000000001",
            "name": "a",
            "tick": 0,
        }
        reg.delete("00000000-0000-0000-0000-000000000001")
        assert reg.get("00000000-0000-0000-0000-000000000001") is None

    def test_corrupt_file_treated_as_empty(self, tmp_path):
        path = tmp_path / "reg.json"
        path.write_text("not-json")
        reg = WorldRegistry(path)
        assert reg.load() == {}

    def test_default_path_env_override(self, tmp_path, monkeypatch):
        target = tmp_path / "custom.json"
        monkeypatch.setenv(REGISTRY_ENV_VAR, str(target))
        assert default_registry_path() == target


class TestCrossContainerDiscovery:
    @pytest.mark.asyncio
    async def test_world_survives_container_restart(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        # First container: create a world
        c1 = ServiceContainer(registry_path=registry_path)
        try:
            world = await c1.world_service.create_world(WorldConfig(name="persisted"), storage)
            wid = world.world_id
        finally:
            await c1.shutdown()

        # Registry file should exist with one entry
        assert registry_path.exists()
        data = json.loads(registry_path.read_text())
        assert str(wid) in data
        assert data[str(wid)]["name"] == "persisted"
        assert data[str(wid)]["storage_uri"] == str(tmp_path / "store")

        # Second container: rediscover the world
        c2 = ServiceContainer(registry_path=registry_path)
        try:
            loaded = await c2.world_service.discover_worlds()
            assert wid in loaded

            worlds = c2.world_service.list_worlds()
            assert len(worlds) == 1
            assert worlds[0].world_id == wid
            assert worlds[0].name == "persisted"

            # get_world should now work across "restart"
            rehydrated = c2.world_service.get_world(wid)
            assert rehydrated.world_id == wid
            assert c2.world_service.get_world_by_name("persisted").world_id == wid
        finally:
            await c2.shutdown()

    @pytest.mark.asyncio
    async def test_tick_persisted_across_restart(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        c1 = ServiceContainer(registry_path=registry_path)
        try:
            world = await c1.world_service.create_world(WorldConfig(name="ticks"), storage)
            await c1.simulation_service.run(world.world_id, RunConfig(num_steps=3))
            wid = world.world_id
        finally:
            await c1.shutdown()

        data = json.loads(registry_path.read_text())
        assert data[str(wid)]["tick"] == 3

        c2 = ServiceContainer(registry_path=registry_path)
        try:
            await c2.world_service.discover_worlds()
            rehydrated = c2.world_service.get_world(wid)
            assert rehydrated.tick == 3
        finally:
            await c2.shutdown()

    @pytest.mark.asyncio
    async def test_remove_world_clears_registry(self, tmp_path):
        registry_path = tmp_path / "registry.json"
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        c1 = ServiceContainer(registry_path=registry_path)
        try:
            world = await c1.world_service.create_world(WorldConfig(name="temp"), storage)
            wid = world.world_id
            c1.world_service.remove_world(wid)
        finally:
            await c1.shutdown()

        data = json.loads(registry_path.read_text())
        assert str(wid) not in data

        c2 = ServiceContainer(registry_path=registry_path)
        try:
            loaded = await c2.world_service.discover_worlds()
            assert loaded == []
            assert c2.world_service.list_worlds() == []
        finally:
            await c2.shutdown()

    @pytest.mark.asyncio
    async def test_no_registry_means_no_persistence(self, tmp_path):
        """Default behavior without registry path is unchanged."""
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        c1 = ServiceContainer()
        try:
            await c1.world_service.create_world(WorldConfig(name="ephemeral"), storage)
            assert len(c1.world_service.list_worlds()) == 1
        finally:
            await c1.shutdown()

        c2 = ServiceContainer()
        try:
            loaded = await c2.world_service.discover_worlds()
            assert loaded == []
            assert c2.world_service.list_worlds() == []
        finally:
            await c2.shutdown()
