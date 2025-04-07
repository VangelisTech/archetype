# -*- coding: utf-8 -*-
"""
The main EcsWorld class, orchestrating the ECS lifecycle, managers, and systems.

Key features of EcsWorld:
Composition: Holds instances of ComponentStore, EcsQueryManager, EcsUpdateManager, and the crucial System.
System Dependency: Requires a System instance upon initialization, making the execution strategy explicit.
process Loop: Clearly defines the 5 phases (Init, Cleanup, Execute, Commit, Collect) and delegates calls to the appropriate managers or the system.
Facade Methods: Provides a clean public API for common ECS operations (entity creation/deletion, component addition/removal/querying, processor management), hiding the internal manager interactions.
Delegation: Most facade methods delegate directly to the corresponding manager (_store, _querier, _updater) or the configured _system.
Helper Method: Includes _create_component_update_df to simplify adding components.
Deferred Component Removal (Basic): Implemented remove_component(immediate=False) by queueing a None update, though this requires the store/join logic to correctly interpret None structs as removals (which Daft's anti-join/concat should handle naturally if the None row makes it to apply_updates).
"""
# Python
from typing import Any, Dict, List, Set, Type, Optional, TYPE_CHECKING
from itertools import count as _step_counter
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide
import time

# Technologies
import daft

# Internal
from .base import Component
# Import interfaces
from .interfaces import (
    WorldInterface, 
    ComponentStoreInterface, 
    QueryManagerInterface, 
    UpdateManagerInterface, 
    SystemInterface,
    ProcessorInterface
)

# Import the CONTAINER INTERFACE
if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface
    # Import concrete types for internal use or type checking if needed
    from .store import ComponentStore
    from .managers import QueryManager, UpdateManager
    from .systems import SequentialSystem
    from .processors import Processor


# --- EcsWorld Implementation ---

class World(WorldInterface): # Implement interface
    """
    Orchestrates the ECS simulation loop, manages entities, components,
    and processor execution via a configured System.
    Provides the main user-facing API for interacting with the ECS.
    """
    # Declare attributes with interface types
    _store: ComponentStoreInterface
    _querier: QueryManagerInterface
    _updater: UpdateManagerInterface
    _system: SystemInterface
    _verbose: bool
    _step_counter: Any # Iterator type hint
    _current_step: int

    @inject
    def __init__(self,
            # Inject using INTERFACE container providers
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store],
            query_interface: QueryManagerInterface = Provide[CoreContainerInterface.query_interface],
            update_manager: UpdateManagerInterface = Provide[CoreContainerInterface.update_manager],
            system: SystemInterface = Provide[CoreContainerInterface.system]
            # config: providers.Configuration = Provide[CoreContainerInterface.config] # Example if config needed
        ):
        self.store = store
        self.querier = query_interface
        self.updater = update_manager
        self.system = system

        # Flags
        self.verbose = True # Or get from config: config.get('verbose', True)

        # Step counter
        self.step_counter = _step_counter(start=0)
        self.current_step: int = -1 # Will be 0 on the first process call

    # --- Simulation Loop ---
    def step(self, dt: float): # Signature matches interface
        """
        Orchestrates a single simulation time step through all phases.

        Args:
            dt: Time delta for the current step.
        """
        start_time = time.time() # Define start_time
        # Increment step counter at the beginning of the process
        self.current_step = next(self.step_counter)
        
        # Execute system(s) - Assuming execute returns Dict[ProcessorInterface, daft.DataFrame]
        results = self.system.execute(dt) # results might be None if system doesn't return anything

        # Commit the updates - Assuming UpdateManager handles potentially empty/None results
        if results:
            merged_df = None
            all_components = set()
            
            for processor, df in results.items():
                # How to know which components this df affects? 
                # Need ProcessorInterface to expose affected components or pass explicitly.
                # Placeholder: Assume processor has a method `get_updated_components()`
                # components_updated = processor.get_updated_components() 
                # all_components.update(components_updated)
                
                if merged_df is None:
                    merged_df = df
                else:
                    # Daft doesn't have a direct equivalent to pandas' flexible update.
                    # A full outer join might be needed, carefully handling conflicts.
                    # This is complex and might be a bottleneck.
                    # Consider revising how updates are passed/merged.
                    # merged_df = merged_df.join(df, on='entity_id', how='outer', ...) # Complex conflict handling needed
                    pass # Skip merging for now
            
            # If merging was done and successful:
            # if merged_df is not None:
            #    self.updater.commit(merged_df, list(all_components))
            
            # TEMPORARY WORKAROUND: Commit last result? Very incorrect.
            if results:
               last_processor, last_df = list(results.items())[-1]
               # Need components for last_df!
               # components = last_processor.get_updated_components() 
               # self.updater.commit(last_df, components)
               print("WARN: Update commit logic needs revision based on System output and UpdateManager input.")

        # Collect and push the updates
        self.updater.collect(self.current_step)
        # self.updater.push(self.current_step) # push seems removed from UpdateManager

        # self.updater.clear_pending_updates() # clear_pending_updates seems removed
        self.updater.clear_caches() # clear_caches exists
        # self.querier.clear_caches() # Querier doesn't have clear_caches
        

        end_time = time.time()
        if self.verbose:
            print(f"--- World: Step {self.current_step} Complete (Total Time: {(end_time - start_time):.4f}s) ---")

    # --- Public API Facade (Signatures match interface) ---

    def add_entity(self, components: Optional[List[Component]] = None, step: Optional[int] = None) -> int:
        """
        Adds a new entity to the store.
        """
        # Use current step if not provided
        effective_step = step if step is not None else self.current_step 
        return self.store.add_entity(components=components, step=effective_step)
    
    def remove_entity(self, entity_id: int) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        self.store.remove_entity(entity_id)

    # Component Management
    def add_component(self, entity_id: int, component_instance: Component) -> None:
        """
        Adds or replaces a component for an entity directly in the store.
        The change is recorded with the current step number.
        """
        self.store.add_component(entity_id, component_instance, step=self.current_step)

    def remove_component(self, entity_id: int, component_type: Type[Component], immediate: bool = False) -> None:
        """
        Removes a component from an entity.
        Uses store's method, handles step.
        Immediate flag seems unused by store method.
        """
        self.store.remove_entity_from_component(entity_id, component_type, self.current_step)

    def remove_component_from_entity(self, entity_id: int, component_type: Component) -> None:
        """
        Removes a component from an entity.
        """
        self.store.remove_component_from_entity(entity_id, component_type)

    # Querying Facade (delegates to Querier)
    def get_components(self, *components: Type[Component]) -> daft.DataFrame:
        """Facade for QueryManager.get_latest_active_state_from_step."""
        return self.querier.get_latest_active_state_from_step(*components, step=self.current_step)
    
    def get_components_from_step(self, *components: Type[Component], step: int) -> daft.DataFrame:
        """Facade for QueryManager.get_latest_active_state_from_step."""
        return self.querier.get_latest_active_state_from_step(*components, step=step)

    def component_for_entity(self, entity_id: int, component: Type[Component]) -> Optional[Component]:
        """
        Facade for QueryManager.component_for_entity.
        """
        return self.querier.component_for_entity(entity_id, component)

    # Processor/System Management Facade (delegates to System)
    def add_processor(self, processor: ProcessorInterface, priority: Optional[int] = None) -> None: # Use interface type
        """Adds a processor to the underlying System."""
        # Priority seems unused by SequentialSystem
        self.system.add_processor(processor)

    def remove_processor(self, processor_type: Type[ProcessorInterface]) -> None: # Use interface type
        """Removes processors of a given type from the underlying System."""
        # SequentialSystem expects instance, not type? Needs check.
        # Find processor instance of that type first? 
        processor_instance = self.get_processor(processor_type)
        if processor_instance:
            self.system.remove_processor(processor_instance)
        # else: log warning?

    def get_processor(self, processor_type: Type[ProcessorInterface]) -> Optional[ProcessorInterface]: # Use interface type
        """Gets a processor instance from the underlying System."""
        # SequentialSystem expects instance? Needs check.
        # Ask system for processor by type
        # This assumes SystemInterface has a method like get_processor_by_type
        # return self.system.get_processor_by_type(processor_type) 
        
        # Workaround: If system only stores instances, find by type
        # This is inefficient if many processors
        for proc in getattr(self.system, 'processors', []): # Access underlying list (unsafe)
            if isinstance(proc, processor_type):
                return proc
        return None


