# -*- coding: utf-8 -*-
"""
Defines the QueryInterface for reading ECS state and the UpdateManager
for queueing and committing state changes.

EcsQueryInterface:
Takes the EcsComponentStore on initialization.
get_component and get_components delegate to the store but ensure appropriate empty DataFrames with schemas are returned if no data exists. get_components focuses on creating the join plan.
component_for_entity handles the logic of filtering the DataFrame, collecting the single row, parsing the struct using dataclasses.fields, and caching the resulting Python object (or None).
get_collected_component_data provides access to the observer cache in the store.
EcsUpdateManager:
Also takes the EcsComponentStore.
add_update appends incoming update DataFrame plans to a list associated with the component_type. It performs essential schema casting.
commit_updates iterates through the pending updates, concats plans for each type, uses drop_duplicates to handle multiple updates to the same entity within the step (keeping the last one), and then passes the final aggregated plan to store.apply_updates.
"""

from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from collections import defaultdict
from dataclasses import fields, is_dataclass
import daft
from daft import col, lit, DataType, Schema
import daft.expressions as F

# Import from our new structure
from .base import Component, EntityType, _C
from .store import EcsComponentStore # Needs the store to interact with

# --- Query Interface ---
class EcsQueryInterface:
    """Provides read-only access to the committed ECS state."""
    def __init__(self, component_store: EcsComponentStore):
        self._store = component_store
        # Cache for reconstructed Python component instances (entity_id, type) -> Component
        self._component_instance_cache: Dict[Tuple[int, Type[Component]], Optional[Component]] = {}

    def clear_caches(self):
        """Clears internal caches, e.g., for component_for_entity."""
        self._component_instance_cache.clear()
        print("QueryInterface: Caches cleared.")

    def get_component(self, component_type: Type[_C]) -> daft.DataFrame:
        """
        Gets the committed DataFrame for a component type.
        Returns an empty DataFrame with the correct schema if the component
        has no entities or hasn't been registered yet.
        """
        df = self._store.get_component_df(component_type)
        if df is not None:
            # Return the actual DataFrame (might be empty, but has schema)
            # Important: This returns the *committed* DataFrame, might be a plan or collected.
            return df
        else:
            # Component type might not be registered or has no data yet.
            # Get schema (this will auto-register if needed) and return empty DF.
            schema = self._store.get_component_schema(component_type)
            if schema:
                 return daft.DataFrame.empty(schema=schema)
            else:
                 # Should not happen if store registration works, but safeguard
                 raise ValueError(f"QueryInterface: Could not get schema for unregistered component type {component_type.__name__}")

    def get_components(self, *component_types: Type[Component]) -> daft.DataFrame:
        """
        Gets a Daft DataFrame *plan* for entities that have ALL specified component types.
        The plan includes columns for 'entity_id' and struct columns for each component type.
        Returns an empty DataFrame with just 'entity_id' if no component types are specified
        or if no entities possess all components.
        """
        if not component_types:
            # Return an empty DataFrame with just entity_id
            return daft.DataFrame.from_pydict({"entity_id": []}).cast_to_schema(
                Schema.from_py_dict({"entity_id": DataType.int64()})
            )

        # Start with the first component's DataFrame
        base_df = self.get_component(component_types[0])
        # If the first DF is already empty (after collection check), no entities can match.
        # Collect here to check emptiness efficiently. This might be a performance hit if DFs are large plans.
        # Alternative: rely on joins naturally producing empty results. Let's try that first.
        # if len(base_df.collect()) == 0:
        #     return base_df.select(col("entity_id")) # Return empty with just entity_id

        joined_df = base_df # Start the join chain

        # Iteratively join with the rest of the component DataFrames
        for i in range(1, len(component_types)):
            next_comp_type = component_types[i]
            next_df = self.get_component(next_comp_type)

            # If any subsequent DF is definitely empty, the inner join will be empty.
            # Again, avoid collecting if possible.
            # if len(next_df.collect()) == 0:
            #     return joined_df.select(col("entity_id")).limit(0) # Return empty structure

            # Perform an inner join on entity_id
            joined_df = joined_df.join(next_df, on="entity_id", how="inner")

            # Optimization: If join result is provably empty, stop early?
            # Daft's optimizer might handle this.

        # Select only the entity_id and the requested component struct columns
        select_cols = ["entity_id"]
        select_cols.extend([self._store._get_component_struct_name(ct) for ct in component_types])

        # Ensure all columns exist before selecting (joins might rename) - Daft handles this.
        return joined_df.select(*(col(c) for c in select_cols))


    def component_for_entity(self, entity_id: int, component_type: Type[_C]) -> Optional[_C]:
        """
        Retrieves a Python instance of a component for a specific entity from the
        committed state. Uses caching. Returns None if the entity doesn't exist,
        doesn't have the component, or the component is not a dataclass.
        """
        cache_key = (entity_id, component_type)
        if cache_key in self._component_instance_cache:
            # Return cached result (could be None if previously determined not to exist)
            return self._component_instance_cache[cache_key]

        # Get the committed DataFrame for this component type
        df = self._store.get_component_df(component_type)
        if df is None:
             # Component type has no data
             self._component_instance_cache[cache_key] = None
             return None

        # Filter the DataFrame for the specific entity
        # Limit(1) is crucial for performance if entity_id is unique (which it should be)
        result_df = df.where(col("entity_id") == entity_id).limit(1)

        # Collect the result (this triggers computation)
        collected = result_df.collect()

        instance: Optional[_C] = None
        if len(collected) == 0:
            # Entity doesn't have this component
            instance = None
        elif not is_dataclass(component_type):
             print(f"Warning: Cannot reconstruct non-dataclass component {component_type.__name__} for entity {entity_id}")
             instance = None
        else:
            # Reconstruct the dataclass instance from the struct
            try:
                row_dict = collected.to_pydict() # Convert single row DF to dict
                struct_name = self._store._get_component_struct_name(component_type)
                struct_contents_list = row_dict.get(struct_name)

                if not struct_contents_list: # Check if struct column exists
                     instance = None
                else:
                     struct_data = struct_contents_list[0] # Get the struct dict from the list
                     if struct_data is None: # Check if the struct itself is null
                          instance = None
                     else:
                          # Get valid field names for the dataclass constructor
                          valid_field_names = {f.name for f in fields(component_type) if f.init}
                          # Filter the struct data to only include valid fields
                          kwargs = {k: v for k, v in struct_data.items() if k in valid_field_names}
                          instance = component_type(**kwargs) # Instantiate!
            except Exception as e:
                 print(f"ERROR: Failed reconstructing {component_type.__name__} for entity {entity_id}: {e}")
                 # Potentially log traceback here
                 instance = None

        # Store the result (including None) in the cache
        self._component_instance_cache[cache_key] = instance
        return instance

    def get_entity_type_for_entity(self, entity_id: int) -> Optional[EntityType]:
        """Gets the EntityType associated with an entity ID from the store."""
        return self._store.get_entity_type_for_entity(entity_id)

    def get_collected_component_data(self, component_type: Type[Component]) -> Optional[daft.DataFrame]:
         """
         Gets the final, collected DataFrame copy from the last completed step,
         intended for observers. Returns None if no data exists.
         """
         return self._store.get_collected_component_data(component_type)


# --- Update Manager ---
class EcsUpdateManager:
    """
    Manages pending component updates generated by processors during a step.
    Updates are queued as Daft DataFrame plans and committed via the ComponentStore.
    """
    def __init__(self, component_store: EcsComponentStore):
        self._store = component_store
        # ComponentType -> List of DataFrame plans representing updates for that type
        self._pending_updates: Dict[Type[Component], List[daft.DataFrame]] = defaultdict(list)

    def clear_pending_updates(self):
        """Clears all queued updates. Called at the start of each step."""
        self._pending_updates.clear()
        # print("UpdateManager: Cleared pending updates.") # Can be noisy

    def add_update(self, component_type: Type[Component], update_df: daft.DataFrame):
        """
        Queues a DataFrame plan representing component updates (additions or modifications).
        Performs basic schema validation and casting.
        """
        # Ensure the component type is known to the store and get its schema
        expected_schema = self._store.get_component_schema(component_type)
        if not expected_schema:
            # This implicitly registers the component type in the store if needed
            print(f"UpdateManager: Registering component {component_type.__name__} via add_update.")
            expected_schema = self._store.get_component_schema(component_type)
            if not expected_schema: # Should not happen after registration attempt
                 print(f"ERROR: UpdateManager failed to get schema for {component_type.__name__} even after registration attempt. Update ignored.")
                 return

        # Validate essential columns exist in the update_df plan
        struct_name = self._store._get_component_struct_name(component_type)
        required_cols = {"entity_id", struct_name}
        if not required_cols.issubset(update_df.column_names()):
            print(f"ERROR: UpdateManager update_df for {component_type.__name__} missing required columns. Has: {update_df.column_names()}, Requires: {required_cols}. Update ignored.")
            return

        # Attempt to cast the update plan to the expected schema
        try:
            # Select only the necessary columns before casting
            update_df_selected = update_df.select(col("entity_id"), col(struct_name))
            update_df_casted = update_df_selected.cast_to_schema(expected_schema)
            # Queue the casted plan
            self._pending_updates[component_type].append(update_df_casted)
            # print(f"UpdateManager: Queued update plan for {component_type.__name__}") # Can be noisy
        except Exception as e:
            print(f"ERROR: UpdateManager failed to cast pending update for {component_type.__name__}. Update ignored. Error: {e}")
            # print(f"Update schema was: {update_df.schema()}")
            # print(f"Expected schema: {expected_schema}")


    def commit_updates(self):
        """
        Aggregates all pending update plans for each component type and
        instructs the ComponentStore to apply them to the committed state.
        """
        print("UpdateManager: Committing updates...")
        if not self._pending_updates:
            print("  No pending updates to commit.")
            return

        num_committed = 0
        for component_type, update_list in self._pending_updates.items():
            if not update_list: continue

            # Aggregate all update plans for this component type
            if len(update_list) == 1:
                aggregated_updates_plan = update_list[0]
            else:
                # Concatenate multiple update plans into one
                aggregated_updates_plan = daft.concat(*update_list)

            # Handle duplicate entity updates within this step: keep the last update queued.
            # This assumes the order in update_list corresponds to the order updates were added.
            # Daft's drop_duplicates with keep='last' relies on row order.
            # Ensure the Daft version supports keep='last'. If not, this might keep the first.
            try:
                # Important: Apply drop_duplicates *after* concat
                final_update_plan = aggregated_updates_plan.drop_duplicates("entity_id", keep="last")
            except Exception as e:
                 # Fallback or warning if drop_duplicates fails or 'last' isn't supported
                 print(f"Warning: Failed to drop duplicates with keep='last' for {component_type.__name__} (may not be supported or other error: {e}). Proceeding without explicit step-level de-duplication.")
                 final_update_plan = aggregated_updates_plan # Proceed without drop_duplicates

            # --- Trigger the actual application in the component store ---
            # The store handles the anti-join/concat logic against the current committed state.
            print(f"  - Submitting aggregated update plan for {component_type.__name__} to store.")
            self._store.apply_updates(component_type, final_update_plan)
            num_committed += 1

        print(f"UpdateManager: Submitted updates for {num_committed} component types to the store.")
