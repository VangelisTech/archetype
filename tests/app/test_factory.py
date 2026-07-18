# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for WorldFactory assembly logic via WorldService."""

import pytest

from archetype.app.storage.service import StorageService, _resolve_uri
from archetype.app.world.service import WorldService
from archetype.core.aio import AsyncSystem, AsyncWorld
from archetype.core.config import CacheConfig, StorageConfig, WorldConfig


def _make_orchestrator(tmp_path):
    ss = StorageService()
    orch = WorldService(ss)
    return orch, ss


def test_resolve_file_uri_uses_filesystem_path(tmp_path):
    target = tmp_path / "file store"
    assert _resolve_uri(target.as_uri()) == str(target)
    assert target.is_dir()


def test_resolve_localhost_file_uri_ignores_netloc(tmp_path):
    target = tmp_path / "localhost-store"
    assert _resolve_uri(f"file://localhost{target}") == str(target)
    assert target.is_dir()


def test_resolve_remote_uri_passes_through():
    assert _resolve_uri("s3://bucket/prefix") == "s3://bucket/prefix"


@pytest.mark.asyncio
async def test_equivalent_file_uri_shares_store_and_catalog(tmp_path):
    service = StorageService()
    target = tmp_path / "file store"
    path_config = StorageConfig(uri=str(target), namespace="ns")
    uri_config = StorageConfig(uri=target.as_uri(), namespace="ns")
    try:
        path_store = await service.get_or_create_store(path_config)
        uri_store = await service.get_or_create_store(uri_config)

        assert uri_store is path_store
        assert service.get_control_catalog(uri_config) is service.get_control_catalog(path_config)
    finally:
        await service.shutdown()


class TestWorldFactory:
    @pytest.mark.asyncio
    async def test_creates_async_world(self, tmp_path):
        orch, ss = _make_orchestrator(tmp_path)
        try:
            world = await orch.create_world(
                WorldConfig(name="f1"),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_world_has_querier_and_updater(self, tmp_path):
        orch, ss = _make_orchestrator(tmp_path)
        try:
            world = await orch.create_world(
                WorldConfig(name="f2"),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
            assert world.querier is not None
            assert world.updater is not None
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_default_system_is_async(self, tmp_path):
        orch, ss = _make_orchestrator(tmp_path)
        try:
            world = await orch.create_world(
                WorldConfig(name="f3"),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert isinstance(world, AsyncWorld)
            assert isinstance(world.system, AsyncSystem)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_custom_system_is_used(self, tmp_path):
        orch, ss = _make_orchestrator(tmp_path)
        custom = AsyncSystem()
        try:
            world = await orch.create_world(
                WorldConfig(name="f4"),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
                system=custom,
            )
            assert isinstance(world, AsyncWorld)
            assert world.system is custom
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_world_id_from_config(self, tmp_path):
        from uuid_utils import uuid7

        orch, ss = _make_orchestrator(tmp_path)
        wid = uuid7()
        try:
            world = await orch.create_world(
                WorldConfig(name="f5", world_id=wid),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
            )
            assert world.world_id == str(wid)
        finally:
            await ss.shutdown()

    @pytest.mark.asyncio
    async def test_cache_config_wraps_store(self, tmp_path):
        from archetype.core.aio import AsyncCachedStore

        orch, ss = _make_orchestrator(tmp_path)
        try:
            world = await orch.create_world(
                WorldConfig(name="f6"),
                StorageConfig(uri=str(tmp_path / "s"), namespace="ns"),
                CacheConfig(),
            )
            assert isinstance(world, AsyncWorld)
            assert isinstance(world.querier._store, AsyncCachedStore)
        finally:
            await ss.shutdown()
