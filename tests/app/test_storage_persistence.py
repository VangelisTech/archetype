# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for StorageConfig defaults and persistence across process restart."""

import os
import tempfile
from pathlib import Path
from uuid_utils import uuid7
import pytest

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, WorldConfig


class TestStorageDefaults:
    @pytest.mark.asyncio
    async def test_create_world_without_storage_config(self, tmp_path):
        """Test that create_world works with default StorageConfig when none provided."""
        # Change to tmp directory for this test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            container = ServiceContainer()
            try:
                # Create world without explicit storage config
                config = WorldConfig(name="default_world")
                world = await container.world_service.create_world(config)
                
                assert world is not None
                assert world.world_id is not None
                
                # Verify default storage path was created
                default_path = Path("./archetype_data")
                assert default_path.exists()
                
            finally:
                await container.shutdown()
        finally:
            os.chdir(old_cwd)

    @pytest.mark.asyncio
    async def test_explicit_storage_config_still_works(self, tmp_path):
        """Test backward compatibility - explicit StorageConfig should still work."""
        container = ServiceContainer()
        try:
            storage = StorageConfig(uri=str(tmp_path / "explicit_store"), namespace="test")
            config = WorldConfig(name="explicit_world")
            world = await container.world_service.create_world(config, storage)
            
            assert world is not None
            assert world.world_id is not None
            
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_unwritable_path_raises_error(self):
        """Test that unwritable storage paths raise clear errors."""
        container = ServiceContainer()
        try:
            # Use a clearly unwritable path
            storage = StorageConfig(uri="/root/unwritable", namespace="test")
            config = WorldConfig(name="bad_world")
            
            with pytest.raises(ValueError, match="not writable"):
                await container.world_service.create_world(config, storage)
                
        finally:
            await container.shutdown()

    @pytest.mark.asyncio
    async def test_data_survives_container_restart(self, tmp_path):
        """Test that data persists when ServiceContainer is recreated (simulating process restart)."""
        # Change to tmp directory for this test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            world_id = None
            world_name = "persistent_world"
            
            # First container - create and store data
            container1 = ServiceContainer()
            try:
                config = WorldConfig(name=world_name)
                world = await container1.world_service.create_world(config)
                world_id = world.world_id
                
                # Verify the world exists in the first container
                worlds = container1.world_service.list_worlds()
                assert len(worlds) == 1
                assert worlds[0].name == world_name
                
            finally:
                await container1.shutdown()
            
            # Simulate process restart by creating new container
            container2 = ServiceContainer()
            try:
                # Try to recreate world with same config (should use same storage)
                config = WorldConfig(world_id=world_id, name=world_name)  
                world2 = await container2.world_service.create_world(config)
                
                # Should get the same world_id back (idempotent)
                assert world2.world_id == world_id
                
                # Verify storage path still exists
                default_path = Path("./archetype_data")
                assert default_path.exists()
                
            finally:
                await container2.shutdown()
                
        finally:
            os.chdir(old_cwd)


class TestServiceContainerMethod:
    @pytest.mark.asyncio 
    async def test_service_container_create_world_no_explicit_storage(self, tmp_path):
        """Test the specific use case mentioned in the issue."""
        # Change to tmp directory for this test
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            container = ServiceContainer()
            try:
                # This is what the user wants to work:
                # container.create_world(config) without explicit StorageConfig
                config = WorldConfig(name="issue_39_test")
                world = await container.world_service.create_world(config)  # No storage_config param
                
                assert world is not None
                worlds = container.world_service.list_worlds()
                assert len(worlds) == 1
                assert worlds[0].name == "issue_39_test"
                
                # Data should be in ./archetype_data
                default_path = Path("./archetype_data")
                assert default_path.exists()
                
            finally:
                await container.shutdown()
        finally:
            os.chdir(old_cwd)