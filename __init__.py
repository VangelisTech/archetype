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
Archetype - Lazy Distributed Dataframe-Based ECS Simulation Engine

This module provides the public API for creating and managing simulation universes,
worlds, and their components. It serves as the main entry point for simulation scripting
and is designed for future CLI integration.

Example usage:
    ```python
    import archetype as arch
    
    # Create a universe with episode caching
    universe = arch.create_universe("lance://my_simulation.db")
    
    # Create a world with processors
    world_id = await universe.create_world([
        MovementProcessor(),
        CollisionProcessor()
    ])
    
    # Run simulation
    await universe.run_simulation(world_id, steps=1000)
    ```
"""

from typing import List, Optional, Any, Dict, Union
import asyncio
import ulid

# Core interfaces and base classes
from archetype.core.interfaces import Component, Archetype, ArchetypeSignature
from archetype.core.base import BaseProcessor, BaseWorld, BaseSystem

# Async implementations
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_store import AsyncStore, AsyncEpisodeStore
from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater, iAsyncSystem

# Store implementations
from archetype.core.store import ArchetypeStore
from archetype.core.querier import ArchetypeQuerier
from archetype.core.updater import ArchetypeUpdater

# Version info
__version__ = "0.1.0"
__author__ = "Vangelis Technologies Inc."

# Public API exports
__all__ = [
    # Core concepts
    "Component",
    "Archetype", 
    "ArchetypeSignature",
    "BaseProcessor",
    
    # Universe management
    "Universe",
    "create_universe",
    "create_ray_universe",
    
    # World configuration
    "WorldConfig",
    "RayWorldConfig",
    
    # Store management
    "create_store",
    "create_episode_store",
    
    # Utilities
    "init_ray",
    "shutdown_ray",
]


# Base Universe Implementation
class _BaseUniverse:
    """
    Base universe implementation managing multiple worlds with shared infrastructure.
    
    This provides the core functionality for universe management, coordinating
    multiple simulation worlds with shared store, querier, and updater instances.
    """
    
    def __init__(
        self,
        store: AsyncEpisodeStore,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        system: iAsyncSystem
    ):
        """
        Initialize the universe with shared infrastructure.
        
        Args:
            store: Episode-aware store for data persistence
            querier: Query interface for reading data
            updater: Update interface for writing data
            system: System for processing archetypes
        """
        self.store = store
        self.querier = querier
        self.updater = updater
        self.system = system
        
        # World registry - maps world_id to world instance
        self.worlds: Dict[str, AsyncWorld] = {}
        
        # Universe statistics
        self.total_worlds_created = 0
        self.total_steps_executed = 0
    
    async def create_world(
        self,
        processors: List[BaseProcessor],
        world_id: Optional[str] = None,
        run_id: Optional[str] = None,
        debug: bool = False,
        **kwargs
    ) -> str:
        """
        Create a new world with the given processors.
        
        Args:
            processors: List of processors to add to the world
            world_id: Optional world identifier
            run_id: Optional run identifier
            debug: Enable debug mode
            **kwargs: Additional world configuration
            
        Returns:
            The world_id of the created world
        """
        world_id = world_id or f"world_{str(ulid.ULID())}"
        
        if world_id in self.worlds:
            raise ValueError(f"World {world_id} already exists")
        
        # Add processors to the shared system
        for processor in processors:
            await self.system.add_processor(processor)
        
        # Create the world with shared infrastructure
        world = AsyncWorld(
            querier=self.querier,
            updater=self.updater,
            async_system=self.system,
            world_id=world_id,
            run_id=run_id,
            debug=debug,
            **kwargs
        )
        
        # Register the world
        self.worlds[world_id] = world
        self.total_worlds_created += 1
        
        return world_id
    
    async def step_world(self, world_id: str, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for a specific world."""
        if world_id not in self.worlds:
            raise ValueError(f"World {world_id} not found")
        
        world = self.worlds[world_id]
        await world.step(*args, **kwargs)
        
        self.total_steps_executed += 1
        
        return {
            "world_id": world_id,
            "step": world.current_step,
            "run_id": world.run_id
        }
    
    async def step_all_worlds(self, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for all worlds concurrently."""
        if not self.worlds:
            return {"worlds_stepped": 0, "total_time": 0}
        
        import time
        start_time = time.time()
        
        # Create step tasks for all worlds
        step_tasks = [
            world.step(*args, **kwargs) 
            for world in self.worlds.values()
        ]
        
        # Execute all steps concurrently
        await asyncio.gather(*step_tasks)
        
        end_time = time.time()
        self.total_steps_executed += len(self.worlds)
        
        return {
            "worlds_stepped": len(self.worlds),
            "total_time": end_time - start_time,
            "world_results": [
                {
                    "world_id": world.world_id,
                    "step": world.current_step,
                    "run_id": world.run_id
                }
                for world in self.worlds.values()
            ]
        }
    
    async def flush_episodes(self, world_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Flush episode cache to persistent storage.
        
        Args:
            world_ids: Optional list of world IDs to flush. If None, flushes all.
            
        Returns:
            Flush statistics
        """
        # Store-level flushing - episode cache is at the store level
        flush_stats = await self.store.flush_episodes()
        
        return {
            "store_flush_stats": flush_stats,
            "worlds_affected": len(world_ids) if world_ids else len(self.worlds),
            "timestamp": asyncio.get_event_loop().time()
        }
    
    async def get_world_stats(self, world_id: str) -> Dict[str, Any]:
        """Get statistics for a specific world."""
        if world_id not in self.worlds:
            raise ValueError(f"World {world_id} not found")
        
        world = self.worlds[world_id]
        return {
            "world_id": world.world_id,
            "run_id": world.run_id,
            "current_step": world.current_step,
            "active_entities": len(world._entity2sig),
            "cached_spawns": sum(len(entities) for entities in world._spawn_cache.values())
        }
    
    async def remove_world(self, world_id: str) -> None:
        """Remove a world from the universe."""
        if world_id not in self.worlds:
            raise ValueError(f"World {world_id} not found")
        
        del self.worlds[world_id]
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the universe."""
        # Flush any remaining episode data
        await self.flush_episodes()
        
        # Clear world registry
        self.worlds.clear()
        
        # Optimize the store
        await self.store.optimize_tables()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get universe-level statistics."""
        return {
            "active_worlds": len(self.worlds),
            "total_worlds_created": self.total_worlds_created,
            "total_steps_executed": self.total_steps_executed,
            "store_cache_stats": self.store.get_cache_stats(),
            "world_ids": list(self.worlds.keys())
        }
    
    # Convenience methods
    
    async def create_simple_world(
        self,
        processors: List[BaseProcessor],
        world_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Convenience method for creating a world with processors."""
        return await self.create_world(processors, world_id, **kwargs)
    
    async def run_simulation(
        self,
        world_id: str,
        steps: int,
        flush_every: int = 10,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run a simulation for a specified number of steps with periodic flushing.
        
        Args:
            world_id: ID of the world to run
            steps: Number of steps to execute
            flush_every: Flush episodes every N steps
            
        Returns:
            Simulation statistics
        """
        import time
        start_time = time.time()
        
        step_results = []
        for step in range(steps):
            result = await self.step_world(world_id, *args, **kwargs)
            step_results.append(result)
            
            # Periodic flush
            if (step + 1) % flush_every == 0:
                await self.flush_episodes()
        
        # Final flush
        await self.flush_episodes()
        
        end_time = time.time()
        
        return {
            "world_id": world_id,
            "steps_executed": steps,
            "total_time": end_time - start_time,
            "avg_time_per_step": (end_time - start_time) / steps,
            "step_results": step_results
        }


class Universe:
    """
    Public API wrapper for universe management.
    
    This provides a simplified, declarative interface for managing simulation
    universes while hiding implementation complexity.
    """
    
    def __init__(self, impl):
        """Initialize with a universe implementation (base or ray)."""
        self._impl = impl
    
    async def create_world(
        self, 
        processors: List[BaseProcessor], 
        world_id: Optional[str] = None,
        **config
    ) -> str:
        """
        Create a new world with the given processors.
        
        Args:
            processors: List of processors to add to the world
            world_id: Optional world identifier
            **config: Additional configuration options
            
        Returns:
            The created world's ID
        """
        return await self._impl.create_simple_world(processors, world_id, **config)
    
    async def step_world(self, world_id: str, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for a specific world."""
        return await self._impl.step_world(world_id, *args, **kwargs)
    
    async def step_all_worlds(self, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for all worlds concurrently."""
        return await self._impl.step_all_worlds(*args, **kwargs)
    
    async def run_simulation(
        self, 
        world_id: str, 
        steps: int, 
        flush_every: int = 10,
        *args, 
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run a simulation for a specified number of steps.
        
        Args:
            world_id: ID of the world to run
            steps: Number of steps to execute
            flush_every: Flush episodes every N steps
            
        Returns:
            Simulation statistics
        """
        return await self._impl.run_simulation(world_id, steps, flush_every, *args, **kwargs)
    
    async def flush_episodes(self, world_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Flush episode cache to persistent storage."""
        return await self._impl.flush_episodes(world_ids)
    
    async def get_world_stats(self, world_id: str) -> Dict[str, Any]:
        """Get statistics for a specific world."""
        if hasattr(self._impl, 'get_world_stats'):
            return await self._impl.get_world_stats(world_id)
        else:
            world = await self._impl.get_world(world_id)
            return {
                "world_id": world.world_id,
                "run_id": world.run_id,
                "current_step": world.current_step
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get universe-level statistics."""
        return self._impl.get_stats()
    
    async def remove_world(self, world_id: str) -> None:
        """Remove a world from the universe."""
        return await self._impl.remove_world(world_id)
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the universe."""
        return await self._impl.shutdown()


# Configuration classes for public API
class WorldConfig:
    """Configuration for creating standard worlds."""
    
    def __init__(
        self,
        processors: Optional[List[BaseProcessor]] = None,
        world_id: Optional[str] = None,
        run_id: Optional[str] = None,
        debug: bool = False,
        max_concurrency: int = 10
    ):
        self.processors = processors or []
        self.world_id = world_id
        self.run_id = run_id
        self.debug = debug
        self.max_concurrency = max_concurrency


class RayWorldConfig(WorldConfig):
    """Configuration for creating Ray world actors."""
    
    def __init__(
        self,
        num_cpus: float = 1.0,
        num_gpus: float = 0.0,
        memory: Optional[int] = None,  # MB
        **kwargs
    ):
        super().__init__(**kwargs)
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory


# Factory functions for the public API

async def create_universe(
    store_uri: str,
    universe_type: str = "episode",
    **config
) -> Universe:
    """
    Create a new universe with the specified store.
    
    Args:
        store_uri: URI for the data store (e.g., "lance://path/to/db")
        universe_type: Type of universe ("async", "episode", "ray")
        **config: Additional configuration options
        
    Returns:
        A Universe instance
    """
    # Create store components
    store = await create_store(store_uri)
    
    if universe_type == "episode":
        episode_store = create_episode_store(store)
        querier = ArchetypeQuerier(episode_store)
        updater = ArchetypeUpdater(episode_store)
        system = AsyncSystem()
        
        # Use the base universe implementation
        impl = _BaseUniverse(episode_store, querier, updater, system)
        
    elif universe_type == "async":
        querier = ArchetypeQuerier(store)
        updater = ArchetypeUpdater(store)
        system = AsyncSystem()
        
        impl = _BaseUniverse(store, querier, updater, system)
        
    else:
        raise ValueError(f"Unknown universe type: {universe_type}")
    
    return Universe(impl)


async def create_ray_universe(
    store_uri: str,
    system_pool_size: int = 4,
    **config
) -> Universe:
    """
    Create a Ray-enhanced universe.
    
    Args:
        store_uri: URI for the data store
        system_pool_size: Number of RaySystem actors in the pool
        **config: Additional configuration options
        
    Returns:
        A Universe instance backed by Ray actors
    """
    try:
        # Import Ray components
        from archetype.ray.ray_universe import RayUniverse
        
        # Create store components
        store = await create_store(store_uri)
        episode_store = create_episode_store(store)
        querier = ArchetypeQuerier(episode_store)
        updater = ArchetypeUpdater(episode_store)
        
        # Create Ray universe
        impl = RayUniverse(episode_store, querier, updater, system_pool_size)
        
        return Universe(impl)
        
    except ImportError as e:
        raise ImportError(
            "Ray is required for Ray universe. Install with: pip install 'archetype[ray]'"
        ) from e


async def create_store(store_uri: str) -> AsyncStore:
    """
    Create a store from a URI.
    
    Args:
        store_uri: Store URI (e.g., "lance://path/to/db", "memory://")
        
    Returns:
        An AsyncStore instance
    """
    if store_uri.startswith("lance://"):
        path = store_uri[8:]  # Remove "lance://" prefix
        return AsyncStore(path)
    elif store_uri == "memory://":
        # For testing - create in-memory store
        import tempfile
        temp_dir = tempfile.mkdtemp()
        return AsyncStore(temp_dir)
    else:
        raise ValueError(f"Unsupported store URI: {store_uri}")


def create_episode_store(base_store: AsyncStore, max_cache_size_mb: int = 500) -> AsyncEpisodeStore:
    """
    Wrap a store with episode caching capabilities.
    
    Args:
        base_store: The underlying store
        max_cache_size_mb: Maximum cache size in MB
        
    Returns:
        An AsyncEpisodeStore instance
    """
    return AsyncEpisodeStore(base_store, max_cache_size_mb)


# Ray utilities

def init_ray(**kwargs) -> None:
    """
    Initialize Ray with sensible defaults.
    
    Args:
        **kwargs: Additional arguments passed to ray.init()
    """
    try:
        import ray
        if not ray.is_initialized():
            ray.init(**kwargs)
    except ImportError:
        raise ImportError("Ray is required. Install with: pip install 'archetype[ray]'")


def shutdown_ray() -> None:
    """Shutdown Ray if it's running."""
    try:
        import ray
        if ray.is_initialized():
            ray.shutdown()
    except ImportError:
        pass  # Ray not available, nothing to shutdown


# Convenience functions for common patterns

async def quick_simulation(
    processors: List[BaseProcessor],
    steps: int = 100,
    store_uri: str = "memory://",
    universe_type: str = "episode"
) -> Dict[str, Any]:
    """
    Run a quick simulation with minimal setup.
    
    Args:
        processors: List of processors to use
        steps: Number of steps to run
        store_uri: Store URI
        universe_type: Type of universe to create
        
    Returns:
        Simulation results
    """
    universe = await create_universe(store_uri, universe_type)
    
    try:
        world_id = await universe.create_world(processors)
        results = await universe.run_simulation(world_id, steps)
        return results
    finally:
        await universe.shutdown()


# Module-level convenience for common operations
_default_universe: Optional[Universe] = None


async def set_default_universe(store_uri: str, universe_type: str = "episode") -> None:
    """Set a default universe for module-level operations."""
    global _default_universe
    _default_universe = await create_universe(store_uri, universe_type)


async def create_world(processors: List[BaseProcessor], **config) -> str:
    """Create a world using the default universe."""
    if _default_universe is None:
        raise RuntimeError("No default universe set. Call set_default_universe() first.")
    return await _default_universe.create_world(processors, **config)


async def step_world(world_id: str, *args, **kwargs) -> Dict[str, Any]:
    """Step a world using the default universe."""
    if _default_universe is None:
        raise RuntimeError("No default universe set. Call set_default_universe() first.")
    return await _default_universe.step_world(world_id, *args, **kwargs)


async def shutdown_default_universe() -> None:
    """Shutdown the default universe."""
    global _default_universe
    if _default_universe is not None:
        await _default_universe.shutdown()
        _default_universe = None 