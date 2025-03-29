# -*- coding: utf-8 -*-
"""
The main EcsWorld class, orchestrating the ECS lifecycle, managers, and systems.

Key features of EcsWorld:
Composition: Holds instances of EcsComponentStore, EcsQueryInterface, EcsUpdateManager, and the crucial System.
System Dependency: Requires a System instance upon initialization, making the execution strategy explicit.
process Loop: Clearly defines the 5 phases (Init, Cleanup, Execute, Commit, Collect) and delegates calls to the appropriate managers or the system.
Facade Methods: Provides a clean public API for common ECS operations (entity creation/deletion, component addition/removal/querying, processor management), hiding the internal manager interactions.
Delegation: Most facade methods delegate directly to the corresponding manager (_store, _querier, _updater) or the configured _system.
Helper Method: Includes _create_component_update_df to simplify adding components.
Deferred Component Removal (Basic): Implemented remove_component(immediate=False) by queueing a None update, though this requires the store/join logic to correctly interpret None structs as removals (which Daft's anti-join/concat should handle naturally if the None row makes it to apply_updates).
"""

from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from itertools import count as _count
from dataclasses import dataclass, fields, is_dataclass

import daft
from daft import col, lit, DataType, Schema
import daft.expressions as F

# Import from our new structure
from .base import Component, EntityType, Processor, System, _C
from .store import EcsComponentStore
from .managers import EcsQueryInterface, EcsUpdateManager

# --- EcsWorld Implementation ---

class EcsWorld:
    """
    Orchestrates the ECS simulation loop, manages entities, components,
    and processor execution via a configured System.
    Provides the main user-facing API for interacting with the ECS.
    """
    def __init__(self, system: System):
        """
        Initializes the EcsWorld.

        Args:
            system: The System instance responsible for processor execution
                    (e.g., SequentialSystem, RayDagSystem).
        """
        if not isinstance(system, System):
             raise TypeError("EcsWorld must be initialized with an instance of a System subclass.")

        # Core Managers
        self._store = EcsComponentStore()
        self._querier = EcsQueryInterface(self._store)
        self._updater = EcsUpdateManager(self._store)
        self._system = system # System for processor execution

        # Entity Management
        self._entity_count = _count(start=1) # Simple incrementing ID generator
        self._dead_entities: Set[int] = set() # Entities marked for deletion next step
        self._entity_types: Dict[str, EntityType] = {} # Definitions of entity types

        print(f"EcsWorld initialized with System: {system.__class__.__name__}")

    # --- Simulation Loop ---
    def process(self, dt: float, *args: Any, **kwargs: Any):
        """
        Orchestrates a single simulation time step through all phases.

        Args:
            dt: Time delta for the current step.
            *args, **kwargs: Additional arguments to pass down to processors.
        """
        print(f"\n--- World: Processing Step (dt={dt:.4f}) ---")
        start_time = time.time()

        # 1. Initialization Phase
        # Clear pending updates from the previous step and query caches
        self._updater.clear_pending_updates()
        self._querier.clear_caches()
        print("  Phase 1: Init Complete.")

        # 2. Entity Cleanup Phase
        # Remove entities marked dead in the previous step from the store
        if self._dead_entities:
            self._store.clear_dead_entities(self._dead_entities)
            self._dead_entities.clear() # Clear the marking set for the current step
            print(f"  Phase 2: Cleanup Complete (Cleared dead entities).")
        else:
            print(f"  Phase 2: Cleanup Complete (No dead entities).")


        # 3. Processor Execution Phase
        # Delegate execution to the configured System strategy
        print(f"  Phase 3: Executing Processors via {self._system.__class__.__name__}...")
        exec_start = time.time()
        try:
            # The system interacts with the querier and updater
            self._system.execute(self._querier, self._updater, dt, *args, **kwargs)
            print(f"  Phase 3: Processor Execution Complete ({(time.time() - exec_start):.4f}s).")
        except Exception as e:
             print(f"!!! ERROR during System execution: {e}")
             # Depending on severity, might want to re-raise or halt
             import traceback
             traceback.print_exc()
             print(f"  Phase 3: Processor Execution FAILED.")
             # Decide whether to proceed with commit or skip? Skipping commit for safety.
             print("--- World: Step Processing Aborted Due to Execution Error ---")
             return # Exit processing early


        # 4. Update Commit Phase
        # Aggregate and apply all updates queued by processors during execution
        print("  Phase 4: Committing Updates...")
        commit_start = time.time()
        self._updater.commit_updates()
        print(f"  Phase 4: Update Commit Complete ({(time.time() - commit_start):.4f}s).")

        # 5. Collection Phase (for Observers)
        # Update the store's cache of collected data for external readers
        print("  Phase 5: Collecting Data...")
        collect_start = time.time()
        self._store.update_collected_data_cache()
        print(f"  Phase 5: Data Collection Complete ({(time.time() - collect_start):.4f}s).")

        end_time = time.time()
        print(f"--- World: Step Complete (Total Time: {(end_time - start_time):.4f}s) ---")

    # --- Public API Facade ---

    # Entity Management
    def create_entity(self) -> int:
        """Creates a new, unique entity ID."""
        entity_id = next(self._entity_count)
        print(f"World: Created entity {entity_id}")
        return entity_id

    def delete_entity(self, entity_id: int, immediate: bool = False):
        """
        Marks an entity for deletion at the start of the next step,
        or attempts to delete it immediately.

        Args:
            entity_id: The ID of the entity to delete.
            immediate: If True, attempt to remove immediately. Use with caution,
                       especially during processor execution. Best used between steps.
        """
        if immediate:
            print(f"World: Immediately deleting entity {entity_id}")
            # Remove from all component stores directly
            # We need to know which components *might* have this entity. Iterate all known types.
            # This could be slow if many component types exist.
            # An alternative: have the store maintain an entity->components mapping?
            for comp_type in self._store._component_data.keys():
                 self._store.remove_entity_from_component(entity_id, comp_type)
            # Remove entity type mapping
            self._store.remove_entity_type_mapping(entity_id)
            # Ensure it's not also marked for deferred deletion
            self._dead_entities.discard(entity_id)
            # Clear query cache as state changed instantly
            self._querier.clear_caches()
            print(f"World: Immediate deletion of entity {entity_id} complete.")
        else:
            # Check if entity logically exists before marking (might already be dead)
            if self.entity_exists(entity_id):
                 print(f"World: Marking entity {entity_id} for deletion at start of next step.")
                 self._dead_entities.add(entity_id)
            else:
                 print(f"World: Entity {entity_id} already deleted or never existed. Cannot mark for deletion.")


    def entity_exists(self, entity_id: int) -> bool:
        """
        Checks if an entity ID currently exists in the committed state
        and is not marked for deletion in the current step.
        Note: This check might involve computation if data is not collected.
        """
        if entity_id in self._dead_entities:
            return False
        # Check if the entity has an entry in the type map (fastest check)
        if self._store.get_entity_type_for_entity(entity_id) is not None:
             return True
        # Fallback: Check if it exists in *any* component DataFrame. This is slower.
        # Consider optimizing if this becomes a bottleneck.
        for comp_type in self._store._component_data.keys():
             df = self._store.get_component_df(comp_type)
             if df is not None:
                  # Need to collect to check existence accurately
                  if len(df.where(col("entity_id") == entity_id).limit(1).collect()) > 0:
                       return True
        return False

    # Component Management
    def _create_component_update_df(self, entity_id: int, component_instance: Component) -> Optional[daft.DataFrame]:
        """Internal helper to create a single-row DataFrame plan for an update."""
        component_type = type(component_instance)
        # Ensure component type is known to the store
        comp_data = self._store.get_component_data_container(component_type)
        if not comp_data: # Should have been registered by now
            print(f"ERROR: Cannot create update DF. Component type {component_type.__name__} not registered.")
            return None

        # Construct the struct data from the instance
        if not is_dataclass(component_instance):
             print(f"ERROR: Component instance {component_instance} is not a dataclass.")
             return None

        struct_data = {}
        valid_field_names = {f.name for f in fields(component_type) if f.init}
        for field_name in valid_field_names:
            if hasattr(component_instance, field_name):
                struct_data[field_name] = getattr(component_instance, field_name)
            else:
                # Handle cases where field exists but isn't set? Or rely on dataclass defaults.
                # For safety, maybe skip if not present, or raise error? Let's skip.
                print(f"Warning: Field '{field_name}' not found on component instance for update DF.")


        # Create the dict for Daft conversion
        struct_name = comp_data.struct_name
        update_dict = {
            "entity_id": [entity_id],
             struct_name: [struct_data] # Store struct dict inside a list
        }

        # Create DataFrame plan
        try:
             # Let Daft infer the schema initially
             update_df = daft.from_pydict(update_dict)
             # We rely on add_update in UpdateManager to cast to the final schema
             return update_df
        except Exception as e:
             print(f"ERROR: Failed creating Daft DataFrame for component update: {e}")
             return None


    def add_component(self, entity_id: int, component_instance: Component):
        """
        Queues the addition or replacement of a component for an entity.
        The change will be applied during the commit phase of the current step.
        """
        # Type Check: Ensure the entity's type allows this component
        entity_type = self.get_entity_type_for_entity(entity_id)
        component_type = type(component_instance)
        if entity_type and not entity_type.allows_component(component_type):
             raise TypeError(f"World: Component type {component_type.__name__} not allowed for entity {entity_id}'s type '{entity_type.name}'")

        # Create the single-row update DataFrame plan
        update_df = self._create_component_update_df(entity_id, component_instance)
        if update_df is None:
            print(f"World: Failed to create update DF for {component_type.__name__} on entity {entity_id}. Update skipped.")
            return

        # Queue the update via the UpdateManager
        self._updater.add_update(component_type, update_df)
        # print(f"World: Queued add/replace {component_type.__name__} for entity {entity_id}") # Noisy

        # Invalidate relevant part of query cache immediately?
        # For simplicity, clear_caches() at start of step handles this broadly.
        # Finer-grained invalidation is complex.

    def remove_component(self, entity_id: int, component_type: Type[Component], immediate: bool = False):
        """
        Removes a component from an entity.

        Args:
            entity_id: The ID of the entity.
            component_type: The type of component to remove.
            immediate: If True, attempts immediate removal (use with caution).
                       If False (default), queues a removal (TODO: Not Implemented yet).
        """
        if not immediate:
            # TODO: Implement deferred component removal.
            # This would likely involve queueing a special "null" update or a separate removal list.
            # Option A: Queue an update with the struct column set to None.
            # Option B: Add a separate mechanism to UpdateManager/Store for removals.
            # Option A seems more aligned with the update flow.
            print(f"World: Queuing removal of {component_type.__name__} for entity {entity_id}...")
            update_dict = {"entity_id": [entity_id], self._store._get_component_struct_name(component_type): [None]}
            try:
                 update_df = daft.from_pydict(update_dict)
                 self._updater.add_update(component_type, update_df) # Queue the 'None' update
                 print(f"World: Queued null update for removing {component_type.__name__} from entity {entity_id}.")
            except Exception as e:
                 print(f"ERROR: Failed creating Daft DataFrame for component removal: {e}")

            # raise NotImplementedError("Deferred component removal via queueing is not fully implemented yet.")
        else:
            print(f"World: Immediately removing {component_type.__name__} from entity {entity_id}")
            self._store.remove_entity_from_component(entity_id, component_type)
            self._querier.clear_caches() # State changed immediately

    # Querying Facade (delegates to Querier)
    def get_component(self, component_type: Type[_C]) -> daft.DataFrame:
        """Facade for EcsQueryInterface.get_component."""
        return self._querier.get_component(component_type)

    def get_components(self, *component_types: Type[Component]) -> daft.DataFrame:
        """Facade for EcsQueryInterface.get_components."""
        return self._querier.get_components(*component_types)

    def component_for_entity(self, entity_id: int, component_type: Type[_C]) -> Optional[_C]:
        """
        Facade for EcsQueryInterface.component_for_entity.
        Retrieves a Python component instance from committed state. Returns None if
        entity is marked dead, doesn't have the component, or other errors occur.
        """
        # Explicitly check if marked dead *now* before querying
        if entity_id in self._dead_entities:
            return None
        return self._querier.component_for_entity(entity_id, component_type)

    def get_collected_component_data(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
         """
         Facade for accessing the final, collected data copy from the last step,
         via EcsQueryInterface. Useful for observers.
         """
         return self._querier.get_collected_component_data(component_type)

    # Processor/System Management Facade (delegates to System)
    def add_processor(self, processor_instance: Processor, priority: Optional[int] = None):
        """Adds a processor to the underlying System."""
        self._system.add_processor(processor_instance, priority)
        print(f"World: Added processor {processor_instance.__class__.__name__} to {self._system.__class__.__name__}.")

    def remove_processor(self, processor_type: Type[Processor]):
        """Removes processors of a given type from the underlying System."""
        self._system.remove_processor(processor_type)
        print(f"World: Removed processor type {processor_type.__name__} from {self._system.__class__.__name__}.")

    def get_processor(self, processor_type: Type[Processor]) -> Optional[Processor]:
        """Gets a processor instance from the underlying System."""
        return self._system.get_processor(processor_type)

    # Entity Type Management
    def register_entity_type(self, entity_type: EntityType) -> None:
        """Registers a predefined EntityType."""
        if not isinstance(entity_type, EntityType):
             raise TypeError("Can only register EntityType instances.")
        if entity_type.name in self._entity_types:
             # Allow re-registration if identical? Or enforce unique names? Enforce unique.
             raise ValueError(f"EntityType '{entity_type.name}' already registered.")
        self._entity_types[entity_type.name] = entity_type
        print(f"World: Registered EntityType '{entity_type.name}'.")

    def create_entity_type(self, name: str, allowed_components: Optional[Set[Type[Component]]] = None) -> EntityType:
        """Creates and registers a new EntityType."""
        if name in self._entity_types:
            raise ValueError(f"EntityType '{name}' already exists.")
        entity_type = EntityType(name, allowed_components)
        self.register_entity_type(entity_type) # Use the registration method
        return entity_type

    def get_entity_type(self, name: str) -> Optional[EntityType]:
        """Gets a registered EntityType by name."""
        return self._entity_types.get(name)

    def create_typed_entity(self, entity_type_name: str, *components: Component) -> int:
         """
         Creates a new entity ID, assigns it an EntityType, and queues
         initial components for addition.

         Args:
             entity_type_name: The name of a registered EntityType.
             *components: Initial components to add to the entity.

         Returns:
             The newly created entity ID.

         Raises:
             ValueError: If the entity_type_name is not registered.
             TypeError: If any initial components are not allowed by the EntityType.
         """
         entity_type = self.get_entity_type(entity_type_name)
         if not entity_type:
             raise ValueError(f"EntityType '{entity_type_name}' not registered.")

         # Perform pre-checks on components *before* creating entity ID
         for comp in components:
             comp_type = type(comp)
             if not entity_type.allows_component(comp_type):
                  raise TypeError(f"[Pre-check failed] Component {comp_type.__name__} not allowed for EntityType '{entity_type.name}'")
             if not is_dataclass(comp):
                  raise TypeError(f"Component {comp} must be a dataclass instance.")


         # Create entity and assign type in the store
         entity_id = self.create_entity()
         self._store.set_entity_type_for_entity(entity_id, entity_type)
         print(f"World: Creating typed entity {entity_id} ('{entity_type.name}'). Queuing components...")

         # Queue initial components using the standard add_component method
         for comp in components:
             self.add_component(entity_id, comp)

         return entity_id

    def get_entity_type_for_entity(self, entity_id: int) -> Optional[EntityType]:
        """Gets the EntityType assigned to a specific entity ID."""
        return self._store.get_entity_type_for_entity(entity_id) # Delegate to store

    # Component Type Registration (Optional - often handled implicitly)
    def register_component(self, component_type: Type[Component]) -> None:
         """
         Explicitly registers a component type with the underlying store,
         primarily to pre-calculate its schema. Often called implicitly.
         """
         self._store.register_component(component_type)

