# -*- coding: utf-8 -*-
"""
The main EcsWorld class, orchestrating the ECS lifecycle, managers, and systems.

Key features of EcsWorld:
Composition: Holds instances of ComponentStore, EcsQueryInterface, EcsUpdateManager, and the crucial System.
System Dependency: Requires a System instance upon initialization, making the execution strategy explicit.
process Loop: Clearly defines the 5 phases (Init, Cleanup, Execute, Commit, Collect) and delegates calls to the appropriate managers or the system.
Facade Methods: Provides a clean public API for common ECS operations (entity creation/deletion, component addition/removal/querying, processor management), hiding the internal manager interactions.
Delegation: Most facade methods delegate directly to the corresponding manager (_store, _querier, _updater) or the configured _system.
Helper Method: Includes _create_component_update_df to simplify adding components.
Deferred Component Removal (Basic): Implemented remove_component(immediate=False) by queueing a None update, though this requires the store/join logic to correctly interpret None structs as removals (which Daft's anti-join/concat should handle naturally if the None row makes it to apply_updates).
"""

from typing import Any, Dict, List, Set, Type, Optional, TYPE_CHECKING
from itertools import count as _count
from itertools import count as _step_counter

import daft
from daft import col, lit

# Import base types needed at runtime
from .base import Component, Processor
from .store import ComponentStore
from .managers import QueryInterface, UpdateManager
from .systems import SequentialSystem
import time # Import the time module

# Conditionally import for type checking only
if TYPE_CHECKING:
    from .store import ComponentStore
    from .managers import QueryInterface, UpdateManager
    from .systems import SequentialSystem


# --- EcsWorld Implementation ---

class World:
    """
    Orchestrates the ECS simulation loop, manages entities, components,
    and processor execution via a configured System.
    Provides the main user-facing API for interacting with the ECS.
    """
    def __init__(self):
        # Import locally within __init__ to avoid top-level circular dependency
        # This is another common pattern to resolve circular imports needed for instantiation
        from .store import ComponentStore
        from .managers import QueryInterface, UpdateManager
        from .systems import SequentialSystem

        self._store: 'ComponentStore' = ComponentStore(self)
        self._querier: 'QueryInterface' = QueryInterface(self)
        self._updater: 'UpdateManager' = UpdateManager(self)
        self._system: 'SequentialSystem' = SequentialSystem(self)

        # Flags
        self._verbose = True

        # Entity Management 
        self.entities: Dict[int, List[Type[Component]]] = {}
        self._entity_count = _count(start=1)
        self._dead_entities = set()

        # Step counter
        self._step_counter = _step_counter(start=0)
        self._current_step: int = -1 # Will be 0 on the first process call

    # --- Simulation Loop ---
    def step(self, dt: float, *inputs: Any):
        """
        Orchestrates a single simulation time step through all phases.

        Args:
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments to pass down to processors.
        """
        start_time = time.time() # Define start_time
        # Increment step counter at the beginning of the process
        self._current_step = next(self._step_counter)

        # Execute the system
        self._system.execute(self, dt, *inputs)

        self._updater.clear_pending_updates()
        self._querier.clear_caches()
        

        end_time = time.time()
        print(f"--- World: Step Complete (Total Time: {(end_time - start_time):.4f}s) ---")

    # --- Public API Facade ---

    def add_entity(self, *components: Component, step: Optional[int] = None) -> int:
        """
        Adds a new entity to the store.
        """
        entity = next(self._entity_count)

        if step is None:
            step = self._current_step # If no step is provided, use the current step, init yields -1

        # Add entity to components
        for component in components:
            self._store.add_component(entity, component, step)

        # Add entity to entities
        self.entities[entity] = [type(component) for component in components]

        return entity
    
    def remove_entity(self, entity_id: int, step: int, immediate: bool = False) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        if entity_id in self._dead_entities:
            return

        self._dead_entities.add(entity_id)

        # If immediate, remove the entity from all components otherwise, QueryInterface will handle it 
        if immediate:
            for component_type in self.entities[entity_id]:
                df = self.get_component(component_type)

                # Find the row for the entity at the given step and set is_active to False
                df = df.where(col("entity_id") == entity_id) \
                    .where(col("step") == step) \
                    .with_column("is_active", lit(False))
                
                # Replace original composite with updated one
                self.components[component_type] = df 

    # Component Management
    def add_component(self, entity_id: int, component_instance: Component):
        """
        Adds or replaces a component for an entity directly in the store.
        The change is recorded with the current step number.
        """
        self._store.add_component(entity_id, component_instance, step=self._current_step)

    def remove_component(self, entity_id: int, component_type: Type[Component], immediate: bool = False):
        """
        Removes a component from an entity.

        Args:
            entity_id: The ID of the entity.
            component_type: The type of component to remove.
        """
        
        self._store.remove_entity_from_component(entity_id, component_type, self._current_step)
        self._querier.clear_caches() # State changed immediately

    # Querying Facade (delegates to Querier

    def get_components(self, *component_types: Type[Component]) -> daft.DataFrame:
        """Facade for QueryInterface.get_latest_active_state_from_step."""
        df = self._querier.get_latest_active_state_from_step(*component_types, step=self._current_step)

        if df is None or df.is_empty():
            print(f"No entities found for components {component_types}.")
            return None
        
        return df

    def component_for_entity(self, entity_id: int, component_type: Type[Component]) -> Optional[Component]:
        """
        Facade for EcsQueryInterface.component_for_entity.
        Retrieves a Python component instance from committed state. Returns None if
        entity is marked dead, doesn't have the component, or other errors occur.
        """
        # Explicitly check if marked dead *now* before querying
        if entity_id in self._dead_entities:
            return None
        return self._querier.component_for_entity(entity_id, component_type)

    # Processor/System Management Facade (delegates to System)
    def add_processor(self, processor_instance: Processor, priority: Optional[int] = None):
        """Adds a processor to the underlying System, injecting the world reference."""
        processor_instance.world = self # Inject world reference
        self._system.add_processor(processor_instance)
        if self._verbose:
            print(f"World: Added processor {processor_instance.__class__.__name__} to {self._system.__class__.__name__}.")

    def remove_processor(self, processor_type: Type[Processor]):
        """Removes processors of a given type from the underlying System."""
        self._system.remove_processor(processor_type)
        print(f"World: Removed processor type {processor_type.__name__} from {self._system.__class__.__name__}.")

    def get_processor(self, processor_type: Type[Processor]) -> Optional[Processor]:
        """Gets a processor instance from the underlying System."""
        return self._system.get_processor(processor_type)

    # Component Type Registration (Optional - often handled implicitly)
    def register_component(self, component_type: Type[Component]) -> None:
         """
         Explicitly registers a component type with the underlying store,
         primarily to pre-calculate its schema. Often called implicitly.
         """
         self._store.register_component(component_type)


class NetworkWorld(World):
    pass