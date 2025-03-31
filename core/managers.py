# -*- coding: utf-8 -*-
"""
Defines the QueryInterface for reading ECS state and the UpdateManager
for queueing and committing state changes based on a historical store.

EcsQueryInterface:
Takes the EcsComponentStore on initialization.
Queries operate on the *latest active state* derived from the historical store.
get_component and get_components return the latest active DataFrames.
component_for_entity reconstructs Python objects from the latest active state, caching results.

EcsUpdateManager:
Takes the EcsComponentStore.
add_update queues DataFrame plans containing entity_id and component fields.
commit_updates aggregates updates, adds the current_step and is_active=True,
ensures schema alignment, and calls store.apply_updates to append to history.
"""

from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from collections import defaultdict
# from dataclasses import fields, is_dataclass # No longer needed as we've moved to Pydantic
import daft
from daft import col, lit, DataType, Schema, DataFrame
from daft.expressions import Expression
import daft.expressions as F
import pyarrow as pa # Import pyarrow

# Import from our new structure
from .base import Component, EntityType, _C
from .store import EcsComponentStore # Needs the store to interact with

# --- Query Interface ---
class EcsQueryInterface:
    """
    Provides read-only access to the *latest active* ECS state,
    derived from the historical ComponentStore.
    """
    def __init__(self, component_store: EcsComponentStore):
        self._store = component_store
        # Cache for reconstructed Python component instances (entity_id, type) -> Component
        # This cache represents the latest known state from previous queries.
        self._component_instance_cache: Dict[Tuple[int, Type[Component]], Optional[Component]] = {}

    def clear_caches(self):
        """Clears internal caches, e.g., for component_for_entity."""
        self._component_instance_cache.clear()
        # print("QueryInterface: Caches cleared.")

    def _get_latest_or_empty(self, component_type: Type[Component]) -> daft.DataFrame:
        """
        Internal helper to get the latest active DataFrame for a type,
        or an empty DataFrame with the correct schema if none exists.
        """
        df_plan = self._store.get_latest_component_df(component_type)
        if df_plan is not None:
             # Return the plan for the latest active state
             return df_plan
        else:
            # No active data. Get schema (this will auto-register) and return empty DF.
            schema = self._store.get_component_schema(component_type)
            if schema:
                try:
                    arrow_schema = schema.to_pyarrow_schema()
                    empty_arrow_table = pa.Table.from_batches([], schema=arrow_schema)
                    return daft.from_arrow(empty_arrow_table)
                except Exception as e:
                    print(f"ERROR: Failed to create empty Daft DataFrame for {component_type.__name__}: {e}")
                    raise e
            else:
                # Should not happen if store registration works
                raise ValueError(f"QueryInterface: Could not get schema for unregistered component type {component_type.__name__}")

    def get_component(self, component_type: Type[_C]) -> daft.DataFrame:
        """
        Gets a DataFrame plan representing the latest active state for a component type.
        Returns an empty DataFrame with the correct schema if the component
        has no active entities.
        """
        return self._get_latest_or_empty(component_type)

    def get_components(self, *component_types: Type[Component]) -> daft.DataFrame:
        """
        Gets a Daft DataFrame *plan* for entities that have ALL specified
        component types in their latest active state.
        The plan includes columns for 'entity_id' and the component fields.
        Returns an empty DataFrame with just 'entity_id' if no types are specified
        or if no entities possess all components in their latest active state.
        """
        if not component_types:
            # Return an empty DataFrame with just entity_id and history cols?
            # Let's just return entity_id for simplicity.
            return daft.DataFrame.from_pydict({"entity_id": []}).cast_to_schema(
                Schema.from_py_dict({"entity_id": DataType.int64()})
            )

        # Start with the latest active state of the first component's DataFrame
        base_df = self._get_latest_or_empty(component_types[0])

        joined_df = base_df # Start the join chain

        # Iteratively join with the latest active state of the rest of the component DataFrames
        for i in range(1, len(component_types)):
            next_comp_type = component_types[i]
            next_df = self._get_latest_or_empty(next_comp_type)

            # Perform an inner join on entity_id
            # Joining latest active state DFs naturally filters for entities having all components actively.
            joined_df = joined_df.join(next_df, on="entity_id", how="inner")

        # Select entity_id and all unique component fields from the latest active schemas
        # Note: The schema from _get_latest_or_empty includes step/is_active.
        # We should select based on the component store's full schema.
        select_cols_set = {"entity_id"}
        all_required_columns = [col("entity_id")] # Track columns needed for the final select

        for comp_type in component_types:
            schema = self._store.get_component_schema(comp_type) # Get the full historical schema
            if schema:
                 # Get all field names from the component's full schema
                 comp_cols = set(schema.column_names())
                 # Track required columns, avoiding duplicates (like entity_id, step, is_active)
                 for col_name in comp_cols:
                     if col_name not in select_cols_set:
                          select_cols_set.add(col_name)
                          all_required_columns.append(col(col_name))
            else:
                 # Should not happen if _get_latest_or_empty worked
                 print(f"Warning: Could not get schema for {comp_type.__name__} during get_components select.")
        # entity_id is guaranteed to be in all_required_columns due to initialization

        # Select the columns. Daft handles potential name conflicts from joins if any.
        # The join result should contain all necessary columns from the involved latest-state DFs.
        try:
            return joined_df.select(*all_required_columns)
        except Exception as e:
            print(f"Error during final select in get_components: {e}")
            print("Attempting selection on joined schema:")
            joined_df.print_schema()
            print("Columns attempted:", [str(c) for c in all_required_columns])
            # Return empty on error? Or re-raise? Let's return empty with entity_id.
            return daft.DataFrame.from_pydict({"entity_id": []}).cast_to_schema(
                Schema.from_py_dict({"entity_id": DataType.int64()})
            )


    def component_for_entity(self, entity_id: int, component_type: Type[_C]) -> Optional[_C]:
        """
        Retrieves a Python instance of a component for a specific entity based on its
        *latest active state* from the historical store. Uses caching.
        Returns None if the entity doesn't exist, doesn't have the component active,
        or the component cannot be reconstructed.
        """
        cache_key = (entity_id, component_type)
        if cache_key in self._component_instance_cache:
            return self._component_instance_cache[cache_key]

        # Get the latest active state DataFrame plan for this component type
        df_plan = self._store.get_latest_component_df(component_type)
        if df_plan is None:
             self._component_instance_cache[cache_key] = None
             return None

        # Filter the latest active plan for the specific entity
        # Since it's latest active, we don't need to check is_active here
        result_df = df_plan.where(col("entity_id") == entity_id).limit(1)

        # Collect the result (triggers computation)
        collected = result_df.collect()

        instance: Optional[_C] = None
        if len(collected) == 0:
            # Entity doesn't have this component active in the latest state
            instance = None
        else:
            # Reconstruct the Pydantic model (LanceModel subclass) from the row dictionary
            try:
                row_dict_list = collected.to_pydict()
                # Extract the single row's data into a flat dict
                # Exclude entity_id, step, is_active as they are not part of the component model
                component_data = {
                    k: v[0] for k, v in row_dict_list.items()
                    if k not in ("entity_id", "step", "is_active")
                }

                if not component_data:
                    instance = None
                else:
                    # Instantiate the Pydantic model
                    instance = component_type.model_validate(component_data)
            except Exception as e:
                print(f"ERROR: Failed reconstructing latest {component_type.__name__} for entity {entity_id}: {e}")
                instance = None

        # Store the result (including None) in the cache
        self._component_instance_cache[cache_key] = instance
        return instance

    def get_entity_type_for_entity(self, entity_id: int) -> Optional[EntityType]:
        """Gets the EntityType associated with an entity ID from the store."""
        return self._store.get_entity_type_for_entity(entity_id)

    # get_collected_component_data removed as the cache mechanism is gone


# --- Update Manager ---
class EcsUpdateManager:
    """
    Manages pending component updates generated by processors during a step.
    Updates are queued as Daft DataFrame plans (containing entity_id + component fields)
    and committed by adding step and activation info before appending to the ComponentStore's history.
    """
    def __init__(self, component_store: EcsComponentStore):
        self._store = component_store
        # ComponentType -> List of DataFrame plans representing updates for that type
        self._pending_updates: Dict[Type[Component], List[daft.DataFrame]] = defaultdict(list)

    def clear_pending_updates(self):
        """Clears all queued updates. Called at the start of each step."""
        self._pending_updates.clear()
        # print("UpdateManager: Cleared pending updates.")

    def add_update(self, component_type: Type[Component], update_df: daft.DataFrame):
        """
        Queues a DataFrame plan representing component updates (additions or modifications).
        The input `update_df` should contain 'entity_id' and the component-specific fields.
        It does NOT need 'step' or 'is_active' at this stage.
        """
        # Ensure the component type is known to the store (registers if needed)
        store_schema = self._store.get_component_schema(component_type)
        if not store_schema:
             print(f"ERROR: UpdateManager failed to ensure registration for {component_type.__name__}. Update ignored.")
             return

        # Basic check: Ensure entity_id exists in the update
        if "entity_id" not in update_df.column_names:
            print(f"ERROR: UpdateManager update_df for {component_type.__name__} missing required 'entity_id' column. Update ignored.")
            return

        # No strict schema validation here. We rely on the commit phase to cast correctly.
        # Just queue the provided DataFrame plan.
        self._pending_updates[component_type].append(update_df)
        print(f"UpdateManager: Queued update for {component_type.__name__}. Current keys: {list(self._pending_updates.keys())}")
        # print(f"UpdateManager: Queued raw update plan for {component_type.__name__}")


    def commit_updates(self, current_step: int):
        """
        Aggregates pending updates, adds step and activation info, ensures schema
        alignment, and instructs the ComponentStore to append them to the historical state.

        Args:
            current_step: The current simulation step number.
        """
        print(f"UpdateManager: Committing updates for step {current_step}...")
        if not self._pending_updates:
            print("  No pending updates to commit.")
            return

        num_committed_types = 0
        print(f"  UpdateManager: Pending update keys: {list(self._pending_updates.keys())}")
        for component_type, update_list in self._pending_updates.items():
            print(f"  UpdateManager: Processing updates for {component_type.__name__}...")
            if not update_list:
                print(f"    Skipping {component_type.__name__}: No updates in list.")
                continue

            # 1. Aggregate all update plans for this component type
            aggregated_updates_df: Optional[daft.DataFrame] = None
            if len(update_list) == 1:
                aggregated_updates_df = update_list[0]
            else:
                try:
                    # Use the top-level daft.concat function with a list
                    aggregated_updates_df = daft.concat(dataframes=update_list)
                except Exception as e:
                     print(f"ERROR: UpdateManager failed to concat updates for {component_type.__name__} at step {current_step}. Skipping type. Error: {e}")
                     # Potentially inspect individual update_list schemas here
                     continue # Skip to next component type

            # 2. --- Handle Duplicates (Keep Last) ---
            # If multiple updates for the same entity exist in this step, keep only the last one.
            # Sort by an implicit order (if concat preserves it) or add a temporary index?
            # Daft's concat doesn't guarantee order preservation across partitions.
            # A common strategy is drop_duplicates on entity_id, keeping the 'last' row.
            # However, defining 'last' without explicit ordering is ambiguous.
            # For simplicity now, let's assume the store handles appending, and queries
            # (`get_latest_...`) correctly find the highest step number.
            # If strict "last update within a step" is needed, more complex logic is required here
            # (e.g., adding row numbers before concat, then grouping/filtering).
            final_update_plan = aggregated_updates_df # Keep as is for now

            # 3. Add step and is_active columns
            try:
                updates_with_meta = final_update_plan.with_column(
                    "step", lit(current_step).cast(DataType.int64())
                ).with_column(
                    "is_active", lit(True).cast(DataType.bool())
                )
            except Exception as e:
                print(f"ERROR: UpdateManager failed adding step/is_active for {component_type.__name__} at step {current_step}. Skipping type. Error: {e}")
                continue

            # 4. --- Trigger the actual application in the component store ---
            # The store's apply_updates now expects the step number as an argument
            # and handles schema casting internally before concat.
            try:
                # print(f"  - Submitting processed update plan for {component_type.__name__} to store for step {current_step}.")
                self._store.apply_updates(component_type, updates_with_meta, current_step)
                num_committed_types += 1
            except Exception as e:
                # Catch errors during the store's apply_updates if needed
                print(f"ERROR: Store failed applying updates for {component_type.__name__} at step {current_step}. Error: {e}")
                # Potentially log schemas involved:
                # print("Schema being sent to store:")
                # updates_with_meta.print_schema()
                # store_schema = self._store.get_component_schema(component_type)
                # if store_schema: store_schema.print_schema()

        print(f"UpdateManager: Submitted updates for {num_committed_types} component types to the store for step {current_step}.")
