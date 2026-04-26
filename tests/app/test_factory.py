# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for WorldFactory assembly logic."""

import pytest

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldFactory
from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig


class TestWorldFactory:
    @pytest.mark.asyncio
    async def test_creates_async_world(self, tmp_path):
        ss = StorageService()
        factory = WorldFactory(ss)
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f1"),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_world_has_querier_and_updater(self, tmp_path):
        ss = StorageService()
        factory = WorldFactory(ss)
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f2"),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
            assert world.querier is not None
            assert world.updater is not None
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_default_system_is_async(self, tmp_path):
        ss = StorageService()
        factory = WorldFactory(ss)
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f3"),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
            assert isinstance(world.system, AsyncSystem)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_custom_system_is_used(self, tmp_path):
        ss = StorageService()
        factory = WorldFactory(ss)
        custom = AsyncSystem()
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f4"),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
                system=custom,
            )
            assert isinstance(world, AsyncWorld)
            assert world.system is custom
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_world_id_from_config(self, tmp_path):
        from uuid_utils import uuid7

        ss = StorageService()
        factory = WorldFactory(ss)
        wid = uuid7()
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f5", world_id=wid),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert world.world_id == wid
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_cache_config_wraps_store(self, tmp_path):
        from archetype.core.aio import AsyncCachedStore

        ss = StorageService()
        factory = WorldFactory(ss)
        try:
            world = await factory.create_world(
                world_config=WorldConfig(name="f6"),
                storage_config=StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
                cache_config=CacheConfig(),
            )
            assert isinstance(world, AsyncWorld)
            # Verify the store is wrapped with caching
            assert isinstance(world.querier._store, AsyncCachedStore)
        finally:
            await ss.shutdown()
