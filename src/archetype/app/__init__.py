# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Archetype Application Layer
===========================

High-level services and application logic for running multi-world simulations.

Architecture:
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     SERVICES LAYER (API Surface)                        │
    │  SimulationService │ CommandService │ WorldService                      │
    └─────────────────────────────────────────────────────────────────────────┘
                                        │
                        ┌───────────────┼───────────────┐
                        ▼               ▼               ▼
    ┌──────────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
    │   CommandBroker      │  │  Episodes       │  │  Runtime             │
    │   (Universal Sim IF) │  │  - Sampler      │  │  - WorldOrchestrator │
    │   - Priority Queues  │  │  - Episode      │  │  - WorldFactory      │
    │   - Per-World        │  │  - Rollout      │  │  - Resources         │
    └──────────────────────┘  └─────────────────┘  └──────────────────────┘
                                        │
                                        ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                            CORE ECS                                     │
    │  AsyncWorld  →  AsyncSystem  →  AsyncProcessor  →  DataFrame            │
    └─────────────────────────────────────────────────────────────────────────┘

Usage:
    # Simple single world
    app = await ArchetypeApp.create()
    world = await app.create_world("my_world")
    await app.run_world(world.world_id, steps=100)

    # Parallel worlds
    app = await ArchetypeApp.create()
    world_ids = await app.run_parallel_worlds(num_worlds=10, steps=1000)

    # Via container
    container = ServiceContainer(storage_config)
    sim = container.simulation_service
    cmd = container.command_service
"""

# Broker (universal simulation interface)
from archetype.app.broker import CommandBroker

# Container and Services
from archetype.app.container import ServiceContainer

# Episodes layer
from archetype.app.episodes import (
    CompositeSampler,
    Episode,
    EpisodeResult,
    GridSampler,
    InitialConditionSampler,
    LatinHypercubeSampler,
    TrimPointSampler,
    UniformSampler,
)

# Models
from archetype.app.models import Command, CommandType

# Runtime layer
from archetype.app.runtime import (
    StorageResourceManager,
    WorldFactory,
    WorldOrchestrator,
)
from archetype.app.services import CommandService, SimulationService, WorldService

# Core config (re-export for convenience)
from archetype.core import CacheConfig, RunConfig, StorageConfig, WorldConfig
from archetype.core.interfaces import iWorld


class ArchetypeApp:
    """
    Main application entry point for Archetype.

    Provides a high-level API for creating and running simulations.
    For more control, use ServiceContainer directly.

    Example:
        # Simple usage
        app = await ArchetypeApp.create()
        world = await app.create_world("my_world")
        await app.run_world(world.world_id, steps=100)

        # Parallel worlds
        world_ids = await app.run_parallel_worlds(num_worlds=10, steps=1000)

        # Cleanup
        await app.shutdown()
    """

    def __init__(self, container: ServiceContainer):
        self.container = container
        self.orchestrator = container.orchestrator
        self.simulation = container.simulation_service
        self.world_service = container.world_service
        self.command_service = container.command_service
        self.broker = container.broker

    @classmethod
    async def create(
        cls,
        storage_uri: str = ".archetype_data",
        cache_config: CacheConfig | None = None,
    ) -> "ArchetypeApp":
        """
        Create and initialize an Archetype application.

        Args:
            storage_uri: Path or URI for storage backend
            cache_config: Optional cache configuration

        Returns:
            Initialized Archetype application
        """
        storage_config = StorageConfig(uri=storage_uri, namespace="simulations")

        container = ServiceContainer(
            storage_config=storage_config,
            cache_config=cache_config,
        )

        return cls(container)

    async def create_world(self, name: str, system=None) -> iWorld:
        """
        Create a single world with the given name.

        Args:
            name: Human-readable name for the world
            system: Optional system configuration (uses default if not provided)

        Returns:
            The created world instance
        """
        from archetype.core.aio import AsyncSystem

        config = WorldConfig(name=name)
        system = system or AsyncSystem()

        return await self.orchestrator.create_world(
            config=config,
            system=system,
            storage_config=self.container.storage_config,
            cache_config=self.container.cache_config,
        )

    async def run_world(self, world_id, steps: int = 1, debug: bool = False) -> None:
        """
        Run a single world for the specified number of steps.

        Args:
            world_id: World ID or name
            steps: Number of simulation steps to run
            debug: Enable debug mode
        """
        run_config = RunConfig(num_steps=steps, debug=debug)

        if isinstance(world_id, str):
            await self.orchestrator.run_world_by_name(world_id, run_config)
        else:
            await self.orchestrator.run_world(world_id, run_config)

    async def run_parallel_worlds(
        self,
        num_worlds: int,
        steps: int = 1,
        system_factory=None,
    ) -> list:
        """
        Create and run multiple worlds in parallel.

        Args:
            num_worlds: Number of worlds to create
            steps: Number of steps to run each world
            system_factory: Optional callable to create systems

        Returns:
            List of created world IDs
        """
        run_config = RunConfig(num_steps=steps)

        return await self.simulation.run_parallel_worlds(
            num_worlds=num_worlds,
            run_config=run_config,
            system_factory=system_factory,
        )

    async def shutdown(self):
        """Gracefully shutdown the application."""
        await self.container.shutdown()


# Convenience function for quick starts
async def quick_start(storage_uri: str = ".archetype_data") -> ArchetypeApp:
    """
    Quick start helper for getting an Archetype instance.

    Example:
        app = await archetype.quick_start()
        world = await app.create_world("test")
        await app.run_world("test", steps=10)
    """
    return await ArchetypeApp.create(storage_uri=storage_uri)


__all__ = [
    # Application
    "ArchetypeApp",
    "ServiceContainer",
    "quick_start",
    # Services (API surface)
    "SimulationService",
    "CommandService",
    "WorldService",
    # Runtime
    "WorldOrchestrator",
    "WorldFactory",
    "StorageResourceManager",
    # Broker (universal simulation interface)
    "CommandBroker",
    "Command",
    "CommandType",
    # Episodes
    "InitialConditionSampler",
    "UniformSampler",
    "LatinHypercubeSampler",
    "GridSampler",
    "TrimPointSampler",
    "CompositeSampler",
    "Episode",
    "EpisodeResult",
]
