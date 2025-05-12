import time
from typing import List, Type, Optional, Union, Dict
from daft import DataFrame
from .processor import Processor
from .base import Component
import ulid

from .interfaces import iSystem, iStore, iQuerier, iUpdater, iWorld

class World(iWorld):
    def __init__(self, store: iStore, querier: iQuerier, updater: iUpdater, system: iSystem):
        # Inject dependencies
        self.store      = store
        self.querier    = querier
        self.updater    = updater
        self.system     = system

        # Initialize the world state
        self.id: str = str(ulid.ULID())
        self.current_step = 0
        

    async def step(self, dt: float):
        start = time.time()
        # 1) run all processors in sequence
        updated_archetypes = await self.execute(self.current_step, dt)

        # 2) Materialize changes into the ArchetypeStore
        await self.update(updated_archetypes, self.current_step)

        self.current_step += 1
        end = time.time()
        print(f"Step {self.current_step} done in {end-start:.3f}s")

    # ---------------------------------------------------------------------
    # Entity Management (Store Facade Methods)
    # ---------------------------------------------------------------------

    async def spawn(self,
              *components: Component,
              step: Optional[int] = None
             ) -> int:
        """Create a new entity with these components."""
        return await self.store.add_entity(components, step or self.current_step)

    async def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        await self.store.remove_entity(entity_id, step or self.current_step)

    # ---------------------------------------------------------------------
    # QueryManager Facade
    # ---------------------------------------------------------------------

    def query(self,
              *components: Type[Component],
              step: Optional[int] = None,
             ) -> DataFrame:
        """Fetch the latest live state of these components at the given step."""
        return self.querier(
            list(components),
            step = step if step is not None else self.current_step
        )

    async def get_history(self, *components: Type[Component]) -> Dict[str, DataFrame]:
        """Fetch the history of these components at the given step."""
        return await self.querier.get_history(*components)
    
    # ---------------------------------------------------------------------
    # UpdateManager Facade
    # ---------------------------------------------------------------------

    async def update(self, archetypes: Dict[str, DataFrame], step: int):
        await self.updater(archetypes, step)

    # ---------------------------------------------------------------------
    # System Facade
    # ---------------------------------------------------------------------

    def add_processor(self, proc: Processor) -> None:
        """Install a Processor into the sequential system."""
        self.system.add_processor(proc)

    def remove_processor(self, proc: Processor) -> None:
        """Remove a Processor from the sequential system."""
        self.system.remove_processor(proc)

    def execute(self, step: int, dt: float) -> Dict[str, DataFrame]:
        """Execute the system for a single step."""
        return self.system.execute(step, dt)