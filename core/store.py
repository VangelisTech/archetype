# -*- coding: utf-8 -*-
"""
Manages the storage and state of components using Daft DataFrames.

Key aspects of this implementation:
- Component data is stored historically, with each row representing the state
  of a component for an entity at a specific step.
- Includes an `entity_id`, a `step` column, an `is_active` flag, and component fields.
- Updates append new rows with the current step number.
- Component removal appends a row with `is_active=False`.
- Provides methods to query the latest state or the state at a specific step.

_ComponentData: Internal dataclass bundling component_type, Daft schema, and the historical dataframe.
_component_data Dictionary: Main internal state (Type[Component] -> _ComponentData).
Schema Generation: Schema includes `entity_id`, `step`, `is_active`, and component fields.
apply_updates: Appends new state rows with the current step.
clear_dead_entities: Currently only removes EntityType mapping. Historical data is kept.
remove_entity_from_component: Appends a record marking the component as inactive for the entity at the current step.
get_latest_component_df: Retrieves the most recent active state for each entity.
get_component_df_at_step: Retrieves the active state for each entity as it was at a specific step.
"""

from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from dataclasses import dataclass, fields, is_dataclass
import daft
from daft import col, lit, DataType, Schema
import daft.expressions as F
import pyarrow as pa # Need pyarrow for schema manipulation
import datetime
import time
import inspect # Keep for potential future use with non-dataclasses

# Import base types from our new structure
from .base import Component, EntityType, _C

# --- Internal Data Structure ---
@dataclass
class _ComponentData:
    """Internal structure to hold schema and historical DataFrame for a component type."""
    component_type: Type[Component]
    schema: daft.Schema
    dataframe: Optional[daft.DataFrame] = None # Stores historical data
    # struct_name: str = "" # Removed struct_name as it wasn't clearly used

    # def __post_init__(self):
    #     if not self.struct_name:
    #          self.struct_name = self.component_type.__name__.lower()


# --- Component Store Implementation ---
class EcsComponentStore:
    """
    Manages the historical state of components, stored internally using
    a _ComponentData structure for each component type. Each row includes
    entity_id, step, is_active, and component data.
    """
    def __init__(self):
        # Stores _ComponentData objects, keyed by component type
        self._component_data: Dict[Type[Component], _ComponentData] = {}
        # Maps entity IDs to their defined EntityType
        self._entity_type_map: Dict[int, EntityType] = {}

    def _create_daft_schema(self, component_type: Type[Component]) -> Schema:
        """
        Creates the Daft Schema for a component type's historical DataFrame.
        Includes entity_id, step, is_active, and component fields.
        """
        component_arrow_schema: pa.Schema = component_type.to_arrow_schema()
        # Insert core fields: entity_id, step, is_active
        composite_arrow_schema = component_arrow_schema.insert(0, pa.field("entity_id", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(1, pa.field("step", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(2, pa.field("is_active", pa.bool_()))

        final_daft_schema = Schema.from_pyarrow_schema(composite_arrow_schema)
        return final_daft_schema

    def register_component(self, component_type: Type[Component]) -> _ComponentData:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist. Returns the _ComponentData.
        """
        if component_type not in self._component_data:
            # print(f"Store: Registering component type {component_type.__name__}")
            schema = self._create_daft_schema(component_type)
            self._component_data[component_type] = _ComponentData(
                component_type=component_type,
                schema=schema,
                dataframe=None # Initialize with no DataFrame
            )
        return self._component_data[component_type]

    def get_component_data_container(self, component_type: Type[Component]) -> Optional[_ComponentData]:
         """Gets the internal _ComponentData container, registering if necessary."""
         # Auto-registering logic remains the same
         if component_type not in self._component_data:
             return self.register_component(component_type)
         return self._component_data.get(component_type)

    def get_component_schema(self, component_type: Type[Component]) -> Optional[Schema]:
        """Gets the Daft Schema for a component type (includes history columns)."""
        container = self.get_component_data_container(component_type)
        return container.schema if container else None

    def get_component_df(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
        """
        Gets the full historical Daft DataFrame for a component type,
        including all steps. Returns None if the component is not registered
        or has no data.
        """
        container = self.get_component_data_container(component_type)
        return container.dataframe if container else None

    # --- New Methods for Querying History ---

    def get_latest_component_df(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
        """
        Gets a DataFrame representing the latest active state for each entity
        for the given component type. Returns a DataFrame plan or None.
        """
        container = self.get_component_data_container(component_type)
        if container is None or container.dataframe is None:
            # print(f"Store GetLatest: No data for {component_type.__name__}")
            return None

        df = container.dataframe
        # print(f"Store GetLatest [{component_type.__name__}]: Input DF len (plan): {len(df)}") # Might trigger compute

        # Find the latest step for each entity
        # Ensure step column is used correctly
        latest_steps = df.groupby("entity_id").agg(F.max(col("step")).alias("latest_step"))
        # print(f"Store GetLatest [{component_type.__name__}]: Found latest steps.")

        # Join back to get the full row for the latest step
        # Use the correct column names from the original df and the aggregation result
        latest_df = df.join(latest_steps, left_on=["entity_id", "step"], right_on=["entity_id", "latest_step"])
        # print(f"Store GetLatest [{component_type.__name__}]: Joined back.")

        # Filter out inactive Entities
        active_latest_df = latest_df.where(col("is_active"))

        # Select only original columns defined in the schema (excluding 'latest_step')
        final_df = active_latest_df.select(*[col(name) for name in container.schema.column_names()])
        # print(f"Store GetLatest [{component_type.__name__}]: Selected final columns. Returning plan.")

        # Return the plan for flexibility, let caller collect if needed
        return final_df


    def get_component_df_at_step(self, component_type: Type[Component], step: int) -> Optional[daft.DataFrame]:
        """
        Gets a DataFrame representing the active state for each entity
        for the given component type as it existed exactly at the specified step.
        If an entity had no update at that specific step, but was active before,
        it finds the most recent state up to that step. Returns a DataFrame plan or None.
        """
        container = self.get_component_data_container(component_type)
        if container is None or container.dataframe is None:
            return None

        df = container.dataframe

        # 1. Filter history to include only steps up to the target step
        relevant_history = df.where(col("step") <= step)

        # 2. Find the maximum step for each entity within that relevant history
        latest_relevant_steps = relevant_history.groupby("entity_id").agg(
            F.max(col("step")).alias("latest_relevant_step")
        )

        # 3. Join back to the relevant history to get the full row corresponding to that latest relevant step
        df_at_step_candidates = relevant_history.join(
            latest_relevant_steps,
            left_on=["entity_id", "step"],
            right_on=["entity_id", "latest_relevant_step"]
        )

        # 4. Filter out entities where the state at that latest relevant step was inactive
        active_df_at_step = df_at_step_candidates.where(col("is_active"))

        # 5. Select only the original schema columns to present a clean state view
        final_df = active_df_at_step.select(*[col(name) for name in container.schema.column_names()])

        # Return the plan
        return final_df

    # --- Modified State Update Methods ---

    def apply_updates(self, component_type: Type[Component], aggregated_updates_df: daft.DataFrame, current_step: int) -> None:
        """
        Appends aggregated updates to a component type's historical DataFrame.
        Adds the current_step and sets is_active=True for the new rows.
        """
        data_container = self.get_component_data_container(component_type)
        if not data_container:
             print(f"ERROR: Store cannot apply updates. Component type {component_type.__name__} not registered.")
             return # Or raise?

        if len(aggregated_updates_df.collect()) == 0: # Check if there are any updates
            # print(f"Store ApplyUpdates [{component_type.__name__}]: No updates to apply for step {current_step}.")
            return

        # Add step and is_active columns to the updates
        # Ensure casting to handle potential type inference issues
        updates_with_meta = aggregated_updates_df.with_column(
            "step", lit(current_step).cast(DataType.int64())
        ).with_column(
            "is_active", lit(True).cast(DataType.bool())
        )

        # Ensure schema alignment before concat
        try:
            # Ensure the *incoming* df matches the store's schema
            target_schema = data_container.schema
            print(f"Store ApplyUpdates [{component_type.__name__}] Step {current_step}: Schema BEFORE select/cast:")
            updates_with_meta.print_schema()
            print(f"Store ApplyUpdates [{component_type.__name__}] Step {current_step}: Target Schema:")
            target_schema.print_schema()

            # --- Added Step: Explicitly select columns in target order ---
            ordered_cols = [col(name) for name in target_schema.column_names()]
            updates_reordered = updates_with_meta.select(*ordered_cols)
            print(f"Store ApplyUpdates [{component_type.__name__}] Step {current_step}: Schema AFTER select/reorder:")
            updates_reordered.print_schema()
            # --- End Added Step ---

            updates_to_apply = updates_reordered.cast_schema(target_schema) # Use reordered DF
            print(f"Store ApplyUpdates [{component_type.__name__}] Step {current_step}: Cast successful.")
        except Exception as e:
            print(f"ERROR: Store ApplyUpdates [{component_type.__name__}] schema alignment/casting failed for step {current_step}. Error: {e}")
            print("Update Schema (before select/cast):")
            updates_with_meta.print_schema() # Log the schema that failed
            print("Target Schema:")
            target_schema.print_schema()
            return # Abort update if schema mismatch

        original_df = data_container.dataframe
        # print(f"Store ApplyUpdates [{component_type.__name__}]: Appending updates for step {current_step}.")

        # Use the casted DataFrame 'updates_to_apply'
        if original_df is None:
            data_container.dataframe = updates_to_apply
            print(f"Store ApplyUpdates [{component_type.__name__}]: Assigned initial DataFrame plan for step {current_step}. Is None: {data_container.dataframe is None}")
        else:
            # Append new updates to existing historical data
            # Concat assumes schemas are compatible (hence the cast above)
            # Use the DataFrame instance method concat
            data_container.dataframe = original_df.concat(other=updates_to_apply)
            print(f"Store ApplyUpdates [{component_type.__name__}]: Concatenated DataFrame plan for step {current_step}. Is None: {data_container.dataframe is None}")

        # print(f"Store ApplyUpdates [{component_type.__name__}]: Completed append for step {current_step}.")
        # We keep the dataframe as a plan, collection happens on read if needed


    def clear_dead_entities(self, dead_entity_ids: Set[int], current_step: int):
        """
        Removes EntityType mapping for dead entities.
        NOTE: This method NO LONGER removes data from component DataFrames to preserve history.
              Downstream systems should handle not generating updates for dead entities.
              Alternatively, tombstones could be added here in a future iteration.
        """
        if not dead_entity_ids:
            return
        # print(f"Store: Clearing type mapping for {len(dead_entity_ids)} dead entities at step {current_step}.")
        dead_list = list(dead_entity_ids) # Keep for map clearing

        # --- Data Removal Logic Removed ---
        # The historical data remains untouched.

        # Remove entity type mappings for dead entities
        cleared_count = 0
        for entity_id in dead_list:
            if self._entity_type_map.pop(entity_id, None):
                 cleared_count += 1
        # if cleared_count > 0:
        #      print(f"  - Cleared {cleared_count} entity type mappings.")


    def remove_entity_from_component(self, entity: int, component_type: Type[Component], current_step: int):
        """
        Appends a record indicating a component is no longer active for a specific entity
        at the given step. Preserves the history of when it was active.
        """
        data_container = self._component_data.get(component_type)
        # Ensure component is registered and has a schema
        if not data_container:
            print(f"Store Immediate Remove: Component {component_type.__name__} not registered.")
            return
        if data_container.dataframe is None:
             print(f"Store Immediate Remove: No history for {component_type.__name__} to append removal record to.")
             # Optionally, we could create the dataframe here with just the removal record.
             # For now, let's assume removal only makes sense if there was prior state.
             return

        schema = data_container.schema
        # print(f"Store: Appending inactive record for entity {entity}, component {component_type.__name__} at step {current_step}")

        # Create data for the removal record (inactive state)
        removal_data = {
            "entity_id": [entity],
            "step": [current_step],
            "is_active": [False]
        }
        # Add None for all other component-specific fields
        for field_name in schema.column_names():
            if field_name not in removal_data:
                # TODO: Determine correct null value based on field_type if needed?
                # For now, None should work for Daft's from_pydict -> cast_schema
                removal_data[field_name] = [None]

        try:
            # Create a Daft DataFrame for the single removal record
            removal_df = daft.from_pydict(removal_data)
            # Cast to the full component schema to ensure compatibility for concat
            removal_df = removal_df.cast_schema(schema)

            # Append the removal record to the historical DataFrame using instance method
            if data_container.dataframe is not None:
                data_container.dataframe = data_container.dataframe.concat(other=removal_df)
            else:
                # If history was empty, the removal record becomes the history
                data_container.dataframe = removal_df
            # print(f"Store: Appended inactive record successfully.")
        except Exception as e:
            print(f"ERROR: Store failed to append inactive record for entity {entity}, comp {component_type.__name__} at step {current_step}. Error: {e}")
            print("Removal Data Dict:", removal_data)
            print("Target Schema:")
            schema.print_schema()


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
