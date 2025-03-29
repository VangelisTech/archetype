# -*- coding: utf-8 -*-
"""
Manages the storage and state of components using Daft DataFrames.

Key aspects of this implementation:
_ComponentData: This internal dataclass bundles the component_type, its Daft schema, the actual dataframe, and the struct_name.
_component_data Dictionary: The store uses this dictionary (Type[Component] -> _ComponentData) as its main internal state.
Schema Caching: The schema is generated once (in _create_daft_schema) and stored within the _ComponentData object, effectively caching it.
Lazy DataFrame Initialization: DataFrames within _ComponentData start as None. They are created/updated only when apply_updates is called with relevant data.
apply_updates: Implements the anti-join/concat pattern, modifying the dataframe attribute of the correct _ComponentData object. It collects the result to materialize the state.
clear_dead_entities: Iterates through the _component_data values and filters the DataFrames within them.
Collected Data Cache: _collected_component_data holds copies of the committed state after collection, intended for observers who need a static view from the end of the last step. update_collected_data_cache populates this.
Entity Type Mapping: Methods for managing the entity_id -> EntityType mapping are included here.
"""

from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from dataclasses import dataclass, fields, is_dataclass
import daft
from daft import col, lit, DataType, Schema
import daft.expressions as F
import datetime
import time
import inspect # Keep for potential future use with non-dataclasses

# Import base types from our new structure
from .base import Component, EntityType, _C

# --- Internal Data Structure ---
@dataclass
class _ComponentData:
    """Internal structure to hold schema and DataFrame for a component type."""
    component_type: Type[Component]
    schema: Schema
    dataframe: Optional[daft.DataFrame] = None # Lazily initialized or loaded
    struct_name: str = ""

    def __post_init__(self):
        if not self.struct_name:
             self.struct_name = self.component_type.__name__.lower()


# --- Component Store Implementation ---
class EcsComponentStore:
    """
    Manages the committed state of components, stored internally using
    a _ComponentData structure for each component type.
    """
    def __init__(self):
        # Stores _ComponentData objects, keyed by component type
        self._component_data: Dict[Type[Component], _ComponentData] = {}
        # Maps entity IDs to their defined EntityType
        self._entity_type_map: Dict[int, EntityType] = {}
        # Cache for collected data (copies of committed state for observers)
        self._collected_component_data: Dict[Type[Component], Optional[daft.DataFrame]] = {}

    def _get_component_struct_name(self, component_type: Type[Component]) -> str:
        """Helper to get the conventional struct column name."""
        return component_type.__name__.lower()

    def _create_daft_schema(self, component_type: Type[Component]) -> Schema:
        """Creates the Daft Schema for a component type's DataFrame."""
        if not is_dataclass(component_type):
            raise TypeError(f"Component type {component_type.__name__} must be a dataclass.")

        struct_name = self._get_component_struct_name(component_type)
        daft_fields = {}
        for f in fields(component_type):
            # Skip fields that shouldn't be part of the Daft schema (e.g., non-init fields)
            if not f.init:
                 continue
            py_type = f.type
            try:
                # Attempt direct conversion
                daft_fields[f.name] = DataType.from_py_type(py_type)
            except TypeError:
                # Fallback to Python object type for complex/unsupported types
                # Consider logging a warning here
                print(f"Warning: Using DataType.python() for field '{f.name}' in component '{component_type.__name__}'. Serialization may be limited.")
                daft_fields[f.name] = DataType.python()

        # Base schema includes entity_id and the component struct
        schema_dict = {
            "entity_id": DataType.int64(),
            struct_name: DataType.struct(daft_fields)
        }
        return Schema.from_py_dict(schema_dict)

    def register_component(self, component_type: Type[Component]) -> _ComponentData:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist. Returns the _ComponentData.
        """
        if component_type not in self._component_data:
            print(f"Store: Registering component type {component_type.__name__}")
            schema = self._create_daft_schema(component_type)
            self._component_data[component_type] = _ComponentData(
                component_type=component_type,
                schema=schema,
                dataframe=None # Initialize with no DataFrame
            )
            self._collected_component_data[component_type] = None
        return self._component_data[component_type]

    def get_component_data_container(self, component_type: Type[Component]) -> Optional[_ComponentData]:
         """Gets the internal _ComponentData container, registering if necessary."""
         if component_type not in self._component_data:
             # Auto-register if accessed but not explicitly registered?
             # Or should registration be mandatory via World? Let's auto-register for now.
             print(f"Store: Auto-registering {component_type.__name__} on access.")
             return self.register_component(component_type)
         return self._component_data.get(component_type)

    def get_component_schema(self, component_type: Type[Component]) -> Optional[Schema]:
        """Gets the Daft Schema for a component type."""
        container = self.get_component_data_container(component_type)
        return container.schema if container else None

    def get_component_df(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
        """Gets the current committed Daft DataFrame for a component type."""
        container = self.get_component_data_container(component_type)
        return container.dataframe if container else None

    def get_collected_component_data(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
        """Gets the final, collected DataFrame from the last completed step."""
        # Ensure type is known, even if no data exists yet
        if component_type not in self._collected_component_data:
             self.register_component(component_type)
        df = self._collected_component_data.get(component_type)
        # Perform a quick check if it's already collected and not empty
        # Note: len() might trigger collection if df is a plan. Be cautious.
        # Relying on the fact that _collected_component_data holds *collected* DFs.
        if df is not None and len(df) > 0:
             return df
        return None

    def update_collected_data_cache(self):
        """Copies the current committed state to the collected cache for observers."""
        print("Store: Updating collected data cache for observers...")
        for comp_type, data_container in self._component_data.items():
            if data_container.dataframe is not None:
                # Ensure we store a *collected* copy if it's currently a plan
                # Note: This incurs cost but ensures observers get static data.
                # Optimization: only collect if the df reference changed since last cache update?
                collected_df = data_container.dataframe.collect()
                if len(collected_df) > 0:
                     self._collected_component_data[comp_type] = collected_df
                else:
                     # Store None if empty after collection
                     self._collected_component_data[comp_type] = None
            else:
                self._collected_component_data[comp_type] = None
        print("Store: Collected data cache updated.")


    def apply_updates(self, component_type: Type[Component], aggregated_updates_df: daft.DataFrame) -> None:
        """
        Applies aggregated updates to a component type's DataFrame using
        anti-join + concat + collect pattern. Modifies the DataFrame within
        the internal _ComponentData structure.
        """
        data_container = self.get_component_data_container(component_type)
        if not data_container:
             print(f"ERROR: Store cannot apply updates. Component type {component_type.__name__} not registered.")
             return # Or raise?

        # Collect updates to get count and ensure it's materialized for joins
        # This is a potential performance bottleneck if updates are huge, but simplifies logic.
        # Alternative: Keep updates as plans for anti-join? Daft's optimizer might handle it.
        # Let's try keeping it as a plan first.
        # update_count = len(aggregated_updates_df.collect()) # Potential cost
        # print(f"Store: Applying aggregated update plan for {component_type.__name__}...")

        # Ensure aggregated updates have the correct schema (casting)
        expected_schema = data_container.schema
        try:
             # Important: Ensure the update plan has the correct schema before joining/concatenating
             updates_casted = aggregated_updates_df.cast_to_schema(expected_schema)
        except Exception as e:
             print(f"ERROR: Store failed to cast aggregated updates for {component_type.__name__}. Update aborted. Error: {e}")
             # Consider logging the problematic dataframe's schema here
             # print(f"Update schema: {aggregated_updates_df.schema()}")
             # print(f"Expected schema: {expected_schema}")
             return

        original_df = data_container.dataframe # This might be None or an empty DF

        if original_df is None or len(original_df.collect()) == 0: # Check length after collect
            # Component type didn't exist or was empty, updates are the new full set
            print(f"Store: Creating new committed DF for {component_type.__name__} from updates.")
            # Collect here to materialize the final state for this component
            new_committed_df = updates_casted.collect()
            if len(new_committed_df) > 0:
                 data_container.dataframe = new_committed_df
            else:
                 # Ensure dataframe is None if updates resulted in empty
                 data_container.dataframe = None
        else:
            # --- Anti-join + Concat Strategy ---
            # Keep rows from the original DF that are *not* in the updates (based on entity_id)
            original_unupdated_part = original_df.anti_join(updates_casted, on="entity_id")

            # Combine the unupdated part with the new/updated rows
            # Ensure both parts are DataFrames, not None
            if original_unupdated_part is not None:
                 new_committed_df_plan = daft.concat(original_unupdated_part, updates_casted)
            else: # Should not happen if original_df wasn't empty, but safety check
                 new_committed_df_plan = updates_casted

            # Collect here to materialize the final state for this component
            new_committed_df = new_committed_df_plan.collect()
            print(f"Store: Updated committed DF for {component_type.__name__}. New total: {len(new_committed_df)}")

            if len(new_committed_df) > 0:
                data_container.dataframe = new_committed_df
            else:
                 # Set to None if the update resulted in an empty state
                 data_container.dataframe = None

    def clear_dead_entities(self, dead_entity_ids: Set[int]):
        """Removes entities from all component DataFrames."""
        if not dead_entity_ids:
            return
        print(f"Store: Clearing {len(dead_entity_ids)} dead entities from committed state...")
        dead_list = list(dead_entity_ids) # Daft's is_in works well with lists

        types_affected: List[str] = []
        for component_type, data_container in self._component_data.items():
            df = data_container.dataframe
            if df is None or len(df.collect()) == 0: # Check after collect
                continue

            original_len = len(df.collect()) # Collect to get accurate length before filtering
            # Filter out dead entities. Use ~ for negation.
            filtered_df_plan = df.where(~col("entity_id").is_in(dead_list))
            filtered_df = filtered_df_plan.collect() # Collect to apply filter

            if len(filtered_df) < original_len:
                 types_affected.append(component_type.__name__)
                 if len(filtered_df) > 0:
                      data_container.dataframe = filtered_df
                 else:
                      # Set to None if component data becomes empty
                      data_container.dataframe = None

        if types_affected:
             print(f"  - Removed dead entities from: {', '.join(types_affected)}")
        else:
             print("  - No component data matched the dead entity IDs.")

        # Remove entity type mappings for dead entities
        for entity_id in dead_list:
            self._entity_type_map.pop(entity_id, None)

    def remove_entity_from_component(self, entity: int, component_type: Type[Component]):
        """Implements immediate removal of a component from a specific entity."""
        data_container = self._component_data.get(component_type)
        if not data_container or data_container.dataframe is None:
            print(f"Store Immediate Remove: Component {component_type.__name__} not found or empty.")
            return

        df = data_container.dataframe
        original_len = len(df.collect())
        filtered_df_plan = df.where(col("entity_id") != entity)
        filtered_df = filtered_df_plan.collect()

        if len(filtered_df) < original_len:
             print(f"Store: Immediately removing {component_type.__name__} from entity {entity}")
             if len(filtered_df) > 0:
                 data_container.dataframe = filtered_df
             else:
                 # Set to None if component data becomes empty
                 data_container.dataframe = None
        else:
             print(f"Store Immediate Remove: Entity {entity} not found in {component_type.__name__}.")


    # --- Entity Type Mapping ---
    def get_entity_type_for_entity(self, entity_id: int) -> Optional[EntityType]:
        """Gets the EntityType associated with an entity ID."""
        return self._entity_type_map.get(entity_id)

    def set_entity_type_for_entity(self, entity_id: int, entity_type: EntityType):
        """Sets the EntityType for an entity ID."""
        if entity_id in self._entity_type_map and self._entity_type_map[entity_id] != entity_type:
             # Handle changing type? For now, maybe warn or error.
             print(f"Warning: Changing EntityType for entity {entity_id} from {self._entity_type_map[entity_id]} to {entity_type}")
        self._entity_type_map[entity_id] = entity_type

    def remove_entity_type_mapping(self, entity_id: int):
        """Removes the EntityType mapping for an entity ID."""
        self._entity_type_map.pop(entity_id, None)
