import time
from typing import List, Type, Optional, Dict
from daft import DataFrame
from daft.session import Session
from .processor import Processor
from .base import Component
import ulid

from .interfaces import iSystem, iStore, iQuerier, iUpdater, iWorld

class SimpleWorld(iWorld):
    def __init__(self, store: iStore, querier: iQuerier, updater: iUpdater, system: iSystem):
        # Inject dependencies
        self.store      = store
        self.querier    = querier
        self.updater    = updater
        self.system     = system

        # Initialize the world state
        self.id: str = str(ulid.ULID())
        self.current_step = 0
        

    def step(self, dt: float):
        # Materialize any pending spawns before the first step
        self.materialize_spawns()
            
        start = time.time()
        # 1) run all processors in sequence
        updated_archetypes = self.execute(self.querier, self.current_step, dt)

        # 2) Materialize changes into the ArchetypeStore
        self.updater(updated_archetypes)

        self.current_step += 1
        end = time.time()
        print(f"Step {self.current_step} done in {end-start:.3f}s")

    # ---------------------------------------------------------------------
    # Entity Management (Store Facade Methods)
    # ---------------------------------------------------------------------

    def spawn(self,
              *components: Component,
              step: Optional[int] = None
             ) -> int:
        """Create a new entity with these components."""
        return self.store.add_entity(list(components), step or self.current_step)

    def despawn(self, entity_id: int, step: Optional[int] = None) -> None:
        """Mark an entity dead (is_active=False)."""
        self.store.remove_entity(entity_id, step or self.current_step)

    def get_session(self) -> Session:
        """Get the Daft Session for this world."""
        return self.store.sess
    
    def materialize_spawns(self) -> None:
        """Materialize any pending spawns before the first step."""
        self.store.materialize_spawns()

    # ---------------------------------------------------------------------
    # QueryManager Facade
    # ---------------------------------------------------------------------

    def query(self, *components: Type[Component], step: Optional[int] = None) -> DataFrame:
        """Fetch the latest live state of these components at the given step."""
        return self.querier(
            list(components),
            step = step or self.current_step
        )
    
    def get_history(self, *components: Type[Component], step: List[int] = None) -> DataFrame:
        """Fetch the latest live state of these components at the given step."""
        return self.querier.query(
            *components,
            step = step
        )
    
    def archetype_for_entity(self, entity_id: int, *components: Type[Component], step: int = None) -> DataFrame:
        """Get a component for an entity."""
        return self.querier.archetype_for_entity(entity_id, components, step)
    # ---------------------------------------------------------------------
    # UpdateManager Facade
    # ---------------------------------------------------------------------

    def update(self, archetypes: Dict[str, DataFrame]):
        self.updater(archetypes)

    # ---------------------------------------------------------------------
    # System Facade
    # ---------------------------------------------------------------------

    def add_processor(self, proc: Processor) -> None:
        """Install a Processor into the sequential system."""
        self.system.add_processor(proc)

    def remove_processor(self, proc: Processor) -> None:
        """Remove a Processor from the sequential system."""
        self.system.remove_processor(proc)

    def execute(self, querier: iQuerier, step: int, dt: float) -> Dict[str, DataFrame]:
        """Execute the system for a single step."""
        return self.system.execute(querier, step, dt)