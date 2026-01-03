# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Services Layer: API Surface for External Consumers
===================================================

Services are the stable API contract for external consumers:
- FastAPI routes
- MCP tools
- CLI commands
- Agents (recursive simulation)

All services delegate to the runtime and episodes layers.

Available Services:
- SimulationService: Multi-world execution, parameter sweeps, Monte Carlo
- CommandService: Command queue management (universal simulation interface)
- WorldService: World-level command execution
- ManagedStorage: High-level storage with catalog, search, embeddings

Example:
    from archetype.app.services import SimulationService, CommandService

    # Via container (recommended)
    container = ServiceContainer(storage_config)
    sim = container.simulation_service
    cmd = container.command_service

    # Run simulations
    world_ids = await sim.run_parallel_worlds(10, run_config)

    # Submit commands
    cmd_id = await cmd.create_entity_command(world_id, components)
"""

from archetype.app.services.command import CommandService
from archetype.app.services.simulation import SimulationService
from archetype.app.services.storage import CatalogEntry, ManagedStorage, SyncManagedStorage
from archetype.app.services.world import WorldService

__all__ = [
    "SimulationService",
    "CommandService",
    "WorldService",
    "ManagedStorage",
    "SyncManagedStorage",
    "CatalogEntry",
]
