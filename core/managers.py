# Python
from typing import List, Optional, Set, TYPE_CHECKING, Type, Union
from dependency_injector.wiring import inject, Provide
import logging

# Technologies
from daft import col, lit, DataFrame

# Internal
from .base import Component
from .interfaces import QueryManagerInterface, UpdateManagerInterface, ComponentStoreInterface 

if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface

# Setup logger
logger = logging.getLogger(__name__)


class QueryManager(QueryManagerInterface): 
    """
    Provides read-only access to the ECS Component Store.
    """

    _store: ComponentStoreInterface 

    @inject
    def __init__(self,
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store]
        ):
        self._store = store # Dependency Injection Ensures Singleton Instance of ComponentStore
    
    def get_components(self, *component_types: Type[Component], step: Union[int, List[int]]) -> List[DataFrame]:
        """
        Fetches combined DataFrames for all active entities at the specified step(s).
        """
        
        # Determine which transform function to use based on step type
        if isinstance(step, int):
            transform_func = self.get_latest_active_state_from_step
        elif isinstance(step, list):
            transform_func = self.get_state_at_steps
        else:
            raise ValueError(f"Invalid step type: {type(step)}")
        
        # Build list of DataFrames to join
        join_df = None # Initialize before loop
        for component_type in component_types:
            original_df = self._store.get_component(component_type)

            # Apply the transform function to the original DataFrame
            df = original_df.if_else(step, )
            
            # Filter out inactive entities and drop the is_active column
            df = df.where(col("is_active")).exclude("is_active")
            
            # Sort by entity_id and step for final join
            final_df = df.sort("entity_id", "step")

            if join_df is None:
                join_df = final_df
            else:
                # Accumulate the join
                join_df = join_df.join(
                    final_df, # Join the newly processed component df
                    on=["entity_id", "step"],
                    # Use appropriate prefix for the *newly added* columns from final_df
                    # This might require getting column names specific to the component
                    # prefix=f"{component_type.__name__.lower()}." # Prefix might apply incorrectly here
                    # Need to be careful about column naming with repeated joins.
                    # Consider selecting only component-specific columns + keys before join.
                    how="outer",
                    strategy="sort_merge"
                )

        return join_df


    def get_latest_active_state_from_step(self, df: DataFrame, step: int) -> DataFrame:
        """
        Gets the latest active state for all specified component types from a specific step.
        """
        # Filter out inactive entities up to the specified step
        new_df = df.where(col("step") <= step) 
            
        # Get the latest step for each entity 
        # (Some Components may not have been updated in last step for some entities)
        latest_steps = new_df.groupby("entity_id") \
                         .max(col("step") \
                         .alias("latest_step"))
        
        # If the smallest latest step is the step we're looking for
        # then all components had a state at that step, skip the join
        if latest_steps.min(col("latest_step")) == step:
            final_df = df.where(col("step") == step)
            
        else:
            # Join back to get the full row for the latest step
            new_df = new_df.join(
                latest_steps,
                left_on=["entity_id", "step"],
                right_on=["entity_id", "latest_step"],
                how="inner",
                strategy="sort_merge"
            )

            # Select only original columns defined in the schema (excluding 'latest_step' and 'step')
            final_df = new_df.select(*df.column_names)
        
        return final_df

    def get_state_at_steps(self, df: DataFrame, steps: List[int]) -> DataFrame:
        """
        Gets the state of all specified component types at the specified steps.
        """
        return df.where(col("step").is_in(steps))



class UpdateManager(UpdateManagerInterface):
    """
    Receives merged update DataFrames for a step (`commit`)
    and applies them to the ComponentStore during `collect` by splitting
    the merged frame into individual component updates.
    Also handles updating the set of dead entities based on 'is_active' flag.
    """
    _store: ComponentStoreInterface 
    _components_to_update: Set[Type[Component]] 
    _merged_df: Optional[DataFrame] 

    @inject 
    def __init__(self,
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store]
        ):
        self._store = store
        self._components_to_update = set()
        self._merged_df = None

    def commit(self, merged_update_df: DataFrame, components_updated_in_step: List[Type[Component]]):
        """
        Stores the single, merged DataFrame containing all updates for the current step,
        along with the set of component types potentially affected.
        """
        # Previous logic for merging multiple commits is removed.
        # World now performs the merge before calling commit.
        if self._merged_df is not None:
             logger.warning("UpdateManager commit called multiple times within a step. Overwriting previous commit.")

        logger.debug(f"Committing merged DataFrame with columns: {merged_update_df.column_names}")
        self._merged_df = merged_update_df
        self._components_to_update = set(components_updated_in_step)
        logger.debug(f"Components to potentially update: {[c.__name__ for c in self._components_to_update]}")

    def collect(self, step: int):
        """Materializes updates from the committed merged DataFrame.
        
        Splits the merged DataFrame based on component schemas and sends
        individual updates to the ComponentStore.
        Also updates the store's list of dead entities.
        """
        if self._merged_df is None:
            logger.debug("Collect called with no committed DataFrame. Skipping.")
            self.clear_caches() # Ensure caches are cleared even if no work done
            return
        
        if not self._components_to_update:
            logger.debug("Collect called with no components marked for update. Skipping store updates.")
            # Still might need to update dead entities if the df exists
            # Let's proceed to dead entity logic regardless
            # self.clear_caches()
            # return
        
        # --- Dead Entity Handling (Moved before potential materialization) ---
        # Update the store's set of dead entities based on the 'is_active' column
        # in the merged DataFrame *before* potentially filtering rows.
        self.update_dead_entities(self._merged_df)

        # Remove dead entities from the DataFrame we will use for component updates
        active_update_df = self.remove_dead_entities(self._merged_df)

        # Optimization: If active_update_df becomes empty after removing dead entities, maybe skip store updates?
        # Requires checking length, which triggers compute. Let Daft handle empty selects for now.
        # if len(active_update_df) == 0:
        #    logger.debug("No active entities remain in the update DataFrame after removing dead ones.")
        #    self.clear_caches()
        #    return
        
        # --- Update Component Store ---
        logger.debug(f"Collecting updates for step {step} for components: {[c.__name__ for c in self._components_to_update]}")
        update_triggered = False
        for component in self._components_to_update:
            try:
                # Get expected columns from the store for this component type
                store_columns = self._store.get_column_names(component)
                if not store_columns:
                    logger.warning(f"Component type {component.__name__} not registered or has no columns in store. Skipping update.")
                    continue

                # Select only the columns relevant to this component from the *active* merged df
                # Ensure essential keys like entity_id, step, is_active are included if expected by store
                cols_to_select = [c for c in store_columns if c in active_update_df.column_names]
                
                # Check if essential key 'entity_id' is available for selection
                if "entity_id" not in cols_to_select:
                     if "entity_id" in active_update_df.column_names:
                          cols_to_select.insert(0, "entity_id") # Add if missing but available
                     else:
                          logger.error(f"Cannot select for component {component.__name__}: 'entity_id' missing in active_update_df. Skipping.")
                          continue
                
                # Perform the selection
                update_select_df = active_update_df.select(*[col(c) for c in cols_to_select])

                # Optimization: Check if selection is empty? Avoids compute but adds check.
                # if len(update_select_df) == 0:
                #    logger.debug(f"Selection for {component.__name__} resulted in empty DataFrame. Skipping store update.")
                #    continue

                # Add the current step literal column
                # Ensure 'step' isn't already selected if it happens to be a component field
                if "step" in update_select_df.column_names:
                     # If 'step' is a data field, maybe rename the literal or handle differently?
                     # Assume for now we overwrite or store expects it.
                     logger.debug(f"Column 'step' already exists in selection for {component.__name__}. Overwriting with step literal {step}.")
                     update_final_df = update_select_df.with_column("step", lit(step))
                else:
                    update_final_df = update_select_df.with_column("step", lit(step))

                # Send the specific component update DataFrame to the store
                # This is where the computation for this component's update happens.
                logger.debug(f"Updating store for component {component.__name__} with {len(update_final_df)} rows (requires compute). Columns: {update_final_df.column_names}")
                self._store.update_component(update_final_df, component)
                update_triggered = True

            except Exception as e:
                # Catch errors during individual component updates
                logger.error(f"Error collecting update for component {component.__name__}: {e}", exc_info=True)
                # Continue to next component

        if update_triggered:
             logger.debug(f"Finished component store updates for step {step}.")
        else:
             logger.debug(f"No component store updates were triggered for step {step}.")

        # Clear caches after processing the step
        self.clear_caches()

    def remove_dead_entities(self, df: DataFrame) -> DataFrame:
        """Filters a DataFrame to exclude rows matching dead entities in the store."""
        if df is None:
            return None
        dead_entities_set = self._store._dead_entities # Access store's set
        if not dead_entities_set:
            return df # No dead entities to remove
        
        # Convert set to list or use Daft's is_in if available on sets directly
        # Assuming is_in works with lists/tuples
        logger.debug(f"Removing entities {list(dead_entities_set)} from update DataFrame.")
        # Ensure 'entity_id' exists
        if "entity_id" in df.column_names:
             return df.where(~col("entity_id").is_in(list(dead_entities_set)))
        else:
             logger.warning("Cannot remove dead entities: 'entity_id' column missing.")
             return df

    def update_dead_entities(self, df: DataFrame):
        """Updates the store's set of dead entities based on the 'is_active' flag
           in the provided DataFrame.
        """
        if df is None or "is_active" not in df.column_names or "entity_id" not in df.column_names:
            logger.debug("Skipping dead entity update: DataFrame missing 'is_active' or 'entity_id'.")
            return

        # Find entities marked as inactive in the current update
        newly_inactive_df = df.where(~col("is_active")).select("entity_id").distinct()
        
        # Materialize only the entity IDs
        if len(newly_inactive_df) > 0:
             try:
                 # Collect might be expensive if many entities become inactive.
                 # Consider alternatives if this is a bottleneck.
                 new_dead_entities_dict = newly_inactive_df.to_pydict() # Returns dict[str, List[Any]]
                 new_dead_entities_set = set(new_dead_entities_dict.get("entity_id", []))
                 
                 if new_dead_entities_set:
                      logger.debug(f"Updating store with newly dead entities: {new_dead_entities_set}")
                      # Update the store's persistent set
                      self._store._dead_entities.update(new_dead_entities_set)
                 else:
                      logger.debug("No new dead entities found in this update.")
             except Exception as e:
                  logger.error(f"Error collecting newly dead entities: {e}", exc_info=True)
        else:
             logger.debug("No potentially dead entities found via is_active=False flag.")

    def clear_caches(self):
        """Clears the cached merged DataFrame and component list for the next step."""
        logger.debug("Clearing UpdateManager caches.")
        self._merged_df = None
        self._components_to_update = set()
