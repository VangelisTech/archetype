import asyncio
import time
from typing import List, Type, Optional

from ..base import Component
from ..world import SimpleWorld
from .async_system import AsyncSystem, AsyncProcessor


class AsyncWorld:
    """
    Async version of SimpleWorld that demonstrates concurrent archetype processing.
    
    Uses the same store/querier/updater infrastructure but with AsyncSystem
    for concurrent execution.
    """
    
    def __init__(self, sync_world: SimpleWorld, max_concurrent_archetypes: int = 10):
        """
        Initialize async world by wrapping an existing sync world.
        This lets us reuse all the existing infrastructure.
        """
        # Reuse sync world's infrastructure
        self.store = sync_world.store
        self.querier = sync_world.querier 
        self.updater = sync_world.updater
        self.current_step = sync_world.current_step
        self.id = sync_world.id
        
        # Replace system with async version
        self.async_system = AsyncSystem(max_concurrent_archetypes)
        
    def add_processor(self, proc: AsyncProcessor) -> None:
        """Add an async processor to the system."""
        self.async_system.add_processor(proc)
        
    def remove_processor(self, proc: AsyncProcessor) -> None:
        """Remove an async processor from the system."""
        self.async_system.remove_processor(type(proc))
    
    # Delegate sync methods to underlying world
    def spawn(self, *components: Component, step: Optional[int] = None) -> int:
        """Create a new entity with these components."""
        return self.store.add_entity(list(components), step or self.current_step)
    
    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        self.store.remove_entity(entity_id, step or self.current_step)
    
    def materialize_spawns(self) -> None:
        """Materialize any pending spawns before the first step."""
        self.store.materialize_spawns()
        
    async def async_step(self, dt: float):
        """
        Async version of step() with concurrent archetype processing.
        """
        # Materialize any pending spawns before the first step
        self.materialize_spawns()
            
        start = time.time()
        
        # 1) Run all processors concurrently (per archetype)
        updated_archetypes = await self.async_system.execute(self.querier, self.current_step, dt)

        # 2) Materialize changes into the ArchetypeStore (sync - no changes needed)
        self.updater(updated_archetypes)

        self.current_step += 1
        end = time.time()
        print(f"Async Step {self.current_step} done in {end-start:.3f}s")
        
    def sync_step(self, dt: float):
        """
        Sync step method for comparison - runs the same async processors sequentially.
        This gives us a fair comparison of sequential vs concurrent execution.
        """
        self.materialize_spawns()
            
        start = time.time()
        
        # Use a minimal sync system for comparison
        modified_archetypes = {}
        active_archetypes = self.store.get_active_archetypes_with_signatures()
        
        for archetype_hash, (df, archetype_signature) in active_archetypes.items():
            current_df = df
            archetype_modified = False
            
            # Apply async processors synchronously for fair comparison
            for proc_instance in sorted(self.async_system.processors, key=lambda x: x.priority):
                if proc_instance.can_process_archetype(archetype_signature):
                    # Call async process method synchronously - this blocks on I/O
                    processed_df = asyncio.run(proc_instance.process(current_df, dt))
                    processed_df = proc_instance.postprocess(processed_df, self.current_step)
                    current_df = processed_df
                    archetype_modified = True
            
            if archetype_modified:
                modified_archetypes[archetype_hash] = current_df

        self.updater(modified_archetypes)
        self.current_step += 1
        end = time.time()
        print(f"Sync Step {self.current_step} done in {end-start:.3f}s")