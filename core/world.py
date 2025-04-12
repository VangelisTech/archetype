# -*- coding: utf-8 -*-
"""
The main EcsWorld class, orchestrating the ECS lifecycle, managers, and systems.
Implements the preprocess -> process -> merge -> commit -> collect pattern.

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
from typing import Any, Dict, List, Set, Type, Optional, TYPE_CHECKING, Union, Tuple
from itertools import count as _step_counter
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide
import time
import logging # Import logging

# Technologies
import daft
from daft import col # Import col

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

# Setup logger
logger = logging.getLogger(__name__)

# --- EcsWorld Implementation ---

class World(WorldInterface): # Implement interface
    """
    Orchestrates the ECS simulation loop using a configured System.
    Manages entity/component lifecycle via Store, Querier, Updater.
    Implements the step phases: Execute (Preprocess/Process), Merge, Commit, Collect.
    """
    # Declare attributes with interface types
    store: ComponentStoreInterface 
    querier: QueryManagerInterface 
    updater: UpdateManagerInterface 
    system: SystemInterface 
    step_counter: Any 
    current_step: int 

    @inject
    def __init__(self,
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store],
            query_interface: QueryManagerInterface = Provide[CoreContainerInterface.query_interface],
            update_manager: UpdateManagerInterface = Provide[CoreContainerInterface.update_manager],
            system: SystemInterface = Provide[CoreContainerInterface.system],
            config: Optional[providers.Configuration] = None 
        ):
        self.store = store
        self.querier = query_interface
        self.updater = update_manager
        self.system = system

        # Step counter
        self.step_counter = _step_counter(start=0)
        self.current_step: int = -1 # Will be 0 on the first step call

    # --- Simulation Loop ---
    def step(self, dt: float): # Signature matches interface
        """
        Orchestrates a single simulation time step through all phases:
        1. Execute: Run system's preprocess & process steps.
        2. Merge: Combine results from processors into a single DataFrame.
        3. Commit: Pass the merged DataFrame to the UpdateManager.
        4. Collect: Trigger UpdateManager to split and store updates.
        """
        start_time = time.time()
        self.current_step = next(self.step_counter)
        step_start_msg = f"--- World: Starting Step {self.current_step} (dt={dt:.4f}) ---"
        logger.info(step_start_msg)

        # 1. Execute system(s) and get merged results
        merged_df = self.system.execute(dt=dt)

        

        # 2. Commit the merged DataFrame 
        logger.debug("Triggering updater commit.")
        self.updater.commit(merged_df) 
        
        # 3. Collect and push the updates
        logger.debug("Triggering updater collect.")
        self.updater.collect(self.current_step)

        # Clear caches for the next step
        logger.debug("Clearing caches.")
        self.updater.clear_caches()

        end_time = time.time()
        step_end_msg = f"--- World: Step {self.current_step} Complete (Total Time: {(end_time - start_time):.4f}s) ---"
        logger.info(step_end_msg)


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
    def add_component(self, entity_id: int, component: Component) -> None:
        """
        Adds or replaces a component for an entity directly in the store.
        The change is recorded with the current step number.
        """
        self.store.add_component(entity_id, component, step=self.current_step)

    def remove_entity_from_component(self, entity_id: int, component: Component) -> None:
        """
        Removes a component from an entity.
        Uses store's method, handles step.
        """
        self.store.remove_entity_from_component(entity_id, component)

    def remove_component_from_entity(self, entity_id: int, component: Component) -> None:
        """
        Removes a component from an entity.
        """
        self.store.remove_component_from_entity(entity_id, component)

    # Querying Facade (delegates to Querier)
    def get_components(self, *components: Component) -> daft.DataFrame:
        """Facade for QueryManager.get_components."""
        return self.querier.get_components(*components, steps=self.current_step)
    
    def get_components_from_steps(self, *components: Component, steps: Union[int, List[int]]) -> daft.DataFrame:
        """Facade for QueryManager.get_components."""
        return self.querier.get_components(*components, steps=steps)

    def get_component_for_entities(self, entity_ids: Union[int, List[int]], component: Component) -> Optional[Component]:
        """
        Facade for QueryManager.component_for_entity.
        """
        return self.querier.get_component_for_entities(entity_ids, component)

    # Processor/System Management Facade (delegates to System)
    def add_processor(self, processor: ProcessorInterface) -> None: 
        """Adds a processor to the underlying System."""
        self.system.add_processor(processor)

    # Remove processor by Type - needs adjustment to match SequentialSystem's remove_processor (which takes instance)
    def remove_processor(self, processor_type: Type[ProcessorInterface]) -> None: # Changed arg type
        """Removes the first processor instance of a given type from the underlying System."""
        processor_instance = self.system.get_processor(processor_type) # Find instance by type
        if processor_instance:
            self.system.remove_processor(processor_instance) # Remove the specific instance
        else:
             logger.warning(f"World: Attempted to remove processor of type {processor_type.__name__}, but no instance was found.")

    # Get processor by Type - needs adjustment to match SequentialSystem's get_processor (which takes type)
    def get_processor(self, processor_type: Type[ProcessorInterface]) -> Optional[ProcessorInterface]: # Changed arg type
        """Gets the first processor instance of a given type from the underlying System."""
        # SequentialSystem.get_processor already takes a type and returns an Optional instance
        return self.system.get_processor(processor_type)


