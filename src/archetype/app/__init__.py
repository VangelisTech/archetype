"""
Archetype Application Layer

This module provides the high-level services and application logic
for running multi-world simulations at scale.
"""

from typing import Optional
import asyncio
from uuid_utils import uuid7

from .container import ServiceContainer
from .config import StorageConfig, CacheConfig, RunConfig, WorldConfig
from .auth.models import ActorCtx


class ArchetypeApp:
    """
    Main application entry point for the Archetype framework.
    
    Example usage:
        # Simple single world
        app = await Archetype.create()
        world = await app.create_world("my_world")
        await app.run_world(world.world_id, steps=100)
        
        # Parallel worlds
        app = await Archetype.create()
        world_ids = await app.run_parallel_worlds(
            num_worlds=10,
            steps=1000
        )
    """
    
    def __init__(self, container: ServiceContainer):
        self.container = container
        self.orchestrator = container.orchestrator
        self.simulation = container.simulation_service
        self.world_service = container.world_service
        self.command_service = container.command_service
    
    @classmethod
    async def create(cls,
                    storage_uri: str = ".archetype_data",
                    cache_config: Optional[CacheConfig] = None,
                    actor_ctx: Optional[ActorCtx] = None) -> "Archetype":
        """
        Create and initialize an Archetype application.
        
        Args:
            storage_uri: Path or URI for storage backend
            cache_config: Optional cache configuration
            actor_ctx: Actor context for commands (creates default if not provided)
        
        Returns:
            Initialized Archetype application
        """
        storage_config = StorageConfig(uri=storage_uri)
        cache_config = cache_config or CacheConfig()
        actor_ctx = actor_ctx or ActorCtx(id=uuid7())
        
        container = ServiceContainer(
            actor_ctx=actor_ctx,
            storage_config=storage_config,
            cache_config=cache_config
        )
        
        return cls(container)
    
    async def create_world(self, 
                          name: str,
                          system=None) -> 'World':
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
            cache_config=self.container.cache_config
        )
    
    async def run_world(self, 
                       world_id,
                       steps: int = 1,
                       debug: bool = False) -> None:
        """
        Run a single world for the specified number of steps.
        
        Args:
            world_id: World ID or name
            steps: Number of simulation steps to run
            debug: Enable debug mode
        """
        run_config = RunConfig(
            num_steps=steps,
            debug=debug
        )
        
        if isinstance(world_id, str):
            await self.orchestrator.run_world_by_name(world_id, run_config)
        else:
            await self.orchestrator.run_world(world_id, run_config)
    
    async def run_parallel_worlds(self,
                                 num_worlds: int,
                                 steps: int = 1,
                                 system_factory=None) -> list:
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
            system_factory=system_factory
        )
    
    async def shutdown(self):
        """Gracefully shutdown the application."""
        await self.container.shutdown()


# Convenience function for quick starts
async def quick_start(storage_uri: str = ".archetype_data") -> Archetype:
    """
    Quick start helper for getting an Archetype instance.
    
    Example:
        app = await archetype.quick_start()
        world = await app.create_world("test")
        await app.run_world("test", steps=10)
    """
    return await Archetype.create(storage_uri=storage_uri)
