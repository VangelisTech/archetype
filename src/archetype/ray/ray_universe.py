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

import asyncio
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
import ulid

import ray
from archetype.core.aio.async_interfaces import iAsyncQuerier, iAsyncUpdater
from archetype.core.aio.async_store import AsyncEpisodeStore
from archetype.core.base import BaseProcessor
from archetype.core.interfaces import ArchetypeSignature


@dataclass
class RayWorldConfig:
    """Configuration for creating a Ray world actor."""
    world_type: str = "ray"
    processors: Optional[List[BaseProcessor]] = None
    world_id: Optional[str] = None
    run_id: Optional[str] = None
    debug: bool = False
    max_concurrency: int = 10
    
    # Ray-specific configuration
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    memory: Optional[int] = None  # MB
    
    def __post_init__(self):
        if self.processors is None:
            self.processors = []


@ray.remote
class RayWorld:
    """
    Stateful Ray actor representing a single simulation world.
    
    This actor maintains its own state (world_id, run_id, entity cache) and delegates
    processing to the shared RaySystem actor pool managed by RayUniverse.
    """
    
    def __init__(
        self,
        world_id: str,
        run_id: str,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        debug: bool = False
    ):
        self.world_id = world_id
        self.run_id = run_id
        self.querier = querier
        self.updater = updater
        self.debug = debug
        self.current_step = 0
        
        # Entity management state
        from itertools import count
        self._entity_counter = count(start=1)
        self._entity2sig: Dict[int, ArchetypeSignature] = {}
        self._spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]] = {}
        
    async def step(self, system_actors: List[ray.ObjectRef], *args, **kwargs) -> Dict[str, Any]:
        """
        Execute one simulation step using the provided system actors.
        
        Args:
            system_actors: List of RaySystem actor references to use for processing
            
        Returns:
            Step execution statistics
        """
        import time
        start_time = time.time()
        
        # Materialize any pending spawns
        await self._materialize_spawns()
        self.current_step += 1
        
        # Get active archetype signatures
        active_signatures = self._get_active_signatures()
        
        if not active_signatures:
            return {"step": self.current_step, "processed_signatures": 0, "time": 0}
        
        # Distribute processing across available system actors
        processing_tasks = []
        for i, sig in enumerate(active_signatures):
            # Round-robin assignment to system actors
            system_actor = system_actors[i % len(system_actors)]
            task = system_actor.process_archetype.remote(
                sig, self.current_step, self.world_id, self.run_id, *args, **kwargs
            )
            processing_tasks.append(task)
        
        # Wait for all processing to complete
        if processing_tasks:
            await asyncio.gather(*[ray.get(task) for task in processing_tasks])
        
        end_time = time.time()
        
        return {
            "step": self.current_step,
            "processed_signatures": len(active_signatures),
            "time": end_time - start_time
        }
    
    def spawn(self, *components, step: Optional[int] = None) -> int:
        """Create a new entity with the given components."""
        from archetype.core.interfaces import Archetype
        
        assert len(components) != 0, "Cannot create an entity with no components"
        
        # Get entity ID and signature
        entity_id = next(self._entity_counter)
        sig = Archetype.sig_from_components(list(components))
        self._entity2sig[entity_id] = sig
        
        # Create row dict for spawn cache
        row_dict = {
            "world_id": self.world_id,
            "run_id": self.run_id,
            "entity_id": entity_id,
            "step": step or self.current_step,
            "is_active": True
        }
        
        # Add component data
        for component in components:
            prefix = component.get_prefix()
            row_dict.update({prefix + key: value for key, value in component.model_dump().items()})
        
        # Add to spawn cache
        if sig not in self._spawn_cache:
            self._spawn_cache[sig] = []
        self._spawn_cache[sig].append(row_dict)
        
        return entity_id
    
    async def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity as inactive."""
        if entity_id in self._entity2sig:
            sig = self._entity2sig[entity_id]
            await self.updater.remove_entity(entity_id, step or self.current_step, self.world_id, self.run_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get world statistics."""
        return {
            "world_id": self.world_id,
            "run_id": self.run_id,
            "current_step": self.current_step,
            "active_entities": len(self._entity2sig),
            "cached_spawns": sum(len(entities) for entities in self._spawn_cache.values())
        }
    
    def _get_active_signatures(self) -> Set[ArchetypeSignature]:
        """Get all active archetype signatures."""
        return set(self._entity2sig.values())
    
    async def _materialize_spawns(self) -> None:
        """Write spawn cache to the store."""
        if self._spawn_cache:
            await self.updater.materialize_spawns(self._spawn_cache, self.world_id, self.run_id)
            self._spawn_cache.clear()


class RayUniverse:
    """
    Ray-enhanced Universe that manages distributed world actors and system actor pools.
    
    This provides the same API as the base Universe but leverages Ray for:
    - Distributed world actors across the cluster
    - Shared system actor pools for efficient resource utilization
    - Automatic scaling based on workload
    """
    
    def __init__(
        self,
        store: AsyncEpisodeStore,
        querier: iAsyncQuerier,
        updater: iAsyncUpdater,
        system_pool_size: int = 4
    ):
        """
        Initialize the Ray Universe.
        
        Args:
            store: Episode-aware store for data persistence
            querier: Query interface for reading data
            updater: Update interface for writing data
            system_pool_size: Number of RaySystem actors to maintain in the pool
        """
        self.store = store
        self.querier = querier
        self.updater = updater
        self.system_pool_size = system_pool_size
        
        # World registry - maps world_id to RayWorld actor reference
        self.world_actors: Dict[str, ray.ObjectRef] = {}
        
        # Shared system actor pool - reused across all worlds
        self.system_actor_pool: List[ray.ObjectRef] = []
        
        # Universe statistics
        self.total_worlds_created = 0
        self.total_steps_executed = 0
        
        # Initialize Ray if not already done
        if not ray.is_initialized():
            ray.init()
        
        # Create the initial system actor pool
        self._initialize_system_pool()
    
    def _initialize_system_pool(self) -> None:
        """Initialize the shared system actor pool."""
        from archetype.ray.ray_system import RaySystem
        
        for _ in range(self.system_pool_size):
            system_actor = RaySystem.remote(
                querier=self.querier,
                updater=self.updater,
                processors=[]  # Processors will be added dynamically
            )
            self.system_actor_pool.append(system_actor)
    
    async def create_world(self, config: RayWorldConfig) -> str:
        """
        Create a new Ray world actor.
        
        Args:
            config: Ray world configuration
            
        Returns:
            The world_id of the created world
        """
        world_id = config.world_id or f"world_{str(ulid.ULID())}"
        run_id = config.run_id or f"run_{str(ulid.ULID())}"
        
        if world_id in self.world_actors:
            raise ValueError(f"World {world_id} already exists")
        
        # Configure Ray actor resources
        ray_options = {
            "num_cpus": config.num_cpus,
            "num_gpus": config.num_gpus,
        }
        if config.memory:
            ray_options["memory"] = config.memory * 1024 * 1024  # Convert MB to bytes
        
        # Create the world actor
        world_actor = RayWorld.options(**ray_options).remote(
            world_id=world_id,
            run_id=run_id,
            querier=self.querier,
            updater=self.updater,
            debug=config.debug
        )
        
        # Add processors to the system pool
        if config.processors:
            await self._add_processors_to_pool(config.processors)
        
        # Register the world
        self.world_actors[world_id] = world_actor
        self.total_worlds_created += 1
        
        return world_id
    
    async def step_world(self, world_id: str, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for a specific world."""
        if world_id not in self.world_actors:
            raise ValueError(f"World {world_id} not found")
        
        world_actor = self.world_actors[world_id]
        
        # Execute step with the shared system pool
        step_result = await world_actor.step.remote(self.system_actor_pool, *args, **kwargs)
        result = ray.get(step_result)
        
        self.total_steps_executed += 1
        return result
    
    async def step_all_worlds(self, *args, **kwargs) -> Dict[str, Any]:
        """Execute one step for all worlds concurrently."""
        if not self.world_actors:
            return {"worlds_stepped": 0, "total_time": 0}
        
        import time
        start_time = time.time()
        
        # Create step tasks for all worlds
        step_tasks = []
        for world_actor in self.world_actors.values():
            task = world_actor.step.remote(self.system_actor_pool, *args, **kwargs)
            step_tasks.append(task)
        
        # Execute all steps concurrently
        results = ray.get(step_tasks)
        
        end_time = time.time()
        self.total_steps_executed += len(self.world_actors)
        
        return {
            "worlds_stepped": len(self.world_actors),
            "total_time": end_time - start_time,
            "world_results": results
        }
    
    async def flush_episodes(self, world_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Flush episode cache to persistent storage."""
        # Store-level flushing - same as base Universe
        flush_stats = await self.store.flush_episodes()
        
        return {
            "store_flush_stats": flush_stats,
            "worlds_affected": len(world_ids) if world_ids else len(self.world_actors),
            "timestamp": asyncio.get_event_loop().time()
        }
    
    async def get_world_stats(self, world_id: str) -> Dict[str, Any]:
        """Get statistics for a specific world."""
        if world_id not in self.world_actors:
            raise ValueError(f"World {world_id} not found")
        
        world_actor = self.world_actors[world_id]
        stats = ray.get(world_actor.get_stats.remote())
        return stats
    
    async def remove_world(self, world_id: str) -> None:
        """Remove a world from the universe."""
        if world_id not in self.world_actors:
            raise ValueError(f"World {world_id} not found")
        
        # Kill the actor
        world_actor = self.world_actors[world_id]
        ray.kill(world_actor)
        
        del self.world_actors[world_id]
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the Ray universe."""
        # Flush any remaining episode data
        await self.flush_episodes()
        
        # Kill all world actors
        for world_actor in self.world_actors.values():
            ray.kill(world_actor)
        
        # Kill system actors
        for system_actor in self.system_actor_pool:
            ray.kill(system_actor)
        
        # Clear registries
        self.world_actors.clear()
        self.system_actor_pool.clear()
        
        # Optimize the store
        await self.store.optimize_tables()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get universe-level statistics."""
        return {
            "active_worlds": len(self.world_actors),
            "system_pool_size": len(self.system_actor_pool),
            "total_worlds_created": self.total_worlds_created,
            "total_steps_executed": self.total_steps_executed,
            "store_cache_stats": self.store.get_cache_stats(),
            "world_ids": list(self.world_actors.keys()),
            "ray_cluster_resources": ray.cluster_resources() if ray.is_initialized() else {}
        }
    
    async def _add_processors_to_pool(self, processors: List[BaseProcessor]) -> None:
        """Add processors to all system actors in the pool."""
        tasks = []
        for system_actor in self.system_actor_pool:
            for processor in processors:
                task = system_actor.add_processor.remote(processor)
                tasks.append(task)
        
        if tasks:
            ray.get(tasks)
    
    # Convenience methods
    
    async def create_simple_world(
        self,
        processors: List[BaseProcessor],
        world_id: Optional[str] = None,
        **ray_options
    ) -> str:
        """Convenience method for creating a world with processors."""
        config = RayWorldConfig(
            processors=processors,
            world_id=world_id,
            **ray_options
        )
        return await self.create_world(config)
    
    async def run_simulation(
        self,
        world_id: str,
        steps: int,
        flush_every: int = 10,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """Run a simulation for a specified number of steps with periodic flushing."""
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