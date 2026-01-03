# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Application Layer
===========================

Infrastructure for multi-world simulation orchestration.

Components:
- CommandBroker: Priority queue for world commands (MESSAGE, SPAWN, etc.)
- WorldOrchestrator: Manages multiple worlds, coordinates execution
- WorldFactory: Assembles world instances with storage wiring
- StorageBackendManager: Shared storage backend pooling

For agent-centric programming, use archetype.dsl:
    from archetype.dsl import World, behavior, spawn_world

Usage:
    from archetype.app import CommandBroker, WorldOrchestrator
    
    broker = CommandBroker()
    orchestrator = WorldOrchestrator()
    
    world = await orchestrator.create_world(config, system, storage_config)
    await orchestrator.run_world(world.world_id, run_config)
"""

from archetype.app.broker import CommandBroker
from archetype.app.factory import WorldFactory
from archetype.app.models import Command, CommandType
from archetype.app.orchestrator import WorldOrchestrator
from archetype.app.storage_manager import StorageBackendManager

# Core config (re-export for convenience)
from archetype.core import CacheConfig, RunConfig, StorageConfig, WorldConfig

__all__ = [
    # Infrastructure
    "CommandBroker",
    "WorldOrchestrator",
    "WorldFactory",
    "StorageBackendManager",
    # Models
    "Command",
    "CommandType",
    # Config (re-exports)
    "CacheConfig",
    "RunConfig",
    "StorageConfig",
    "WorldConfig",
]
