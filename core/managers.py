from typing import List, Optional, Set, TYPE_CHECKING
from dependency_injector.wiring import inject, Provide

# Technologies
from daft import col, lit, DataFrame

# Internal
from .base import Component
# Import interfaces
from .interfaces import QueryManagerInterface, UpdateManagerInterface, ComponentStoreInterface 

# Import the CONTAINER INTERFACE
if TYPE_CHECKING:
    from .container_interface import CoreContainerInterface
    # Keep ComponentStore concrete type for Provide marker resolution if needed by DI?
    # Let's try without first, relying on the interface injection.
    # from .store import ComponentStore


# --- Query Interface ---
class QueryManager(QueryManagerInterface): # Implement interface
    """
    Provides read-only access to the *latest active* ECS state,
    derived from the historical ComponentStore.
    """
    @inject
    def __init__(self,
            # Inject using INTERFACE container and INTERFACE type hint
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store]
        ):
        self._store = store

    def get_component(self, component: Component) -> DataFrame:
        """
        Gets the latest active state for a specific component type.
        """
        return self._store.get_component(component)
    
    def get_combined_state_history(self, *components: Component) -> List[DataFrame]:
        """
        Gets the latest active state for all specified component types.

        Joins component DataFrames on entity_id, step, and is_active to maintain proper historical state
        and ensure we only join active records from the same step together.
        """
        if not components:
            components = self._store.components.keys()

        df = self.get_component(components[0])
        for component in components[1:]:
            new_df = self.get_component(component)
            df = df.join(
                new_df, 
                on=["entity_id", "step", "is_active"],
                prefix=f"{component.__name__.lower()}.",
                how="outer",
                strategy="hash" # "sort_merge" Not Supported Yet
            ).collect()

        return df


    def get_latest_active_state_from_step(self, *components: Component, step: Optional[int] = None) -> DataFrame:
        """
        Gets the latest active state for all specified component types from a specific step.
        """
        df = self.get_combined_state_history(*components)
        
        # Filter to only include steps up to and including the specified step
        if step is not None:
            df = df.where(col("step") <= step)
        
        # Calculate the latest step for each entity (Potentially non-uniform latest step)
        latest_steps = df.groupby("entity_id").max(col("step").alias("latest_step"))

        # Join back to get the full row for the latest step
        latest_df = df.join(
            latest_steps,
            left_on=["entity_id", "step"],
            right_on=["entity_id", "latest_step"],
            how="inner",
            strategy="hash" # "sort_merge" Not Supported Yet
        )

        # Select only original columns defined in the schema (excluding 'latest_step')
        final_df = latest_df.select(*[col(name) for name in df.column_names])
        
        return final_df

# --- Update Manager ---

class UpdateManager(UpdateManagerInterface): # Implement interface
    """
    Responsible for applying materialized state updates from processors
    to components. 
    
    Splits state dataframes and applying updates.  


    """
    @inject # Keep inject decorator
    def __init__(self,
            # Inject using INTERFACE container and INTERFACE type hint
            store: ComponentStoreInterface = Provide[CoreContainerInterface.store]
        ):
        self._store = store
        self.components_to_update: Set[Component] = set()
        self.df = None

    def commit(self, update_df: DataFrame, components: List[Component]):
        """
        Commits a new update to the component store.
        """
        if self.df is None:
            self.df = update_df
        else:
            # Sort and repartition by entity_id
            self.df = self.df.sort(col("entity_id")) \
                        .join(update_df,
                            on=["entity_id", "is_active"], 
                            how="outer", 
                            strategy="hash"
                        )
        
        # Track which components need to be updated
        self.components_to_update.update(components)

    def collect(self, step: int):
        """Materialized updates."""
        # Remove dead entities
        self.remove_dead_entities()
        
        try:
            self.df.collect()
        except Exception as e:
            print(e)

        # Update dead entities
        self.update_dead_entities()
        self.remove_dead_entities()

        # Update Store
        for component in self.components_to_update:
            columns = self._store.get_column_names(component)
            update_df = self.df.select(*columns) \
                        .with_column("step", lit(step))
            self._store.update_component(update_df, component)

        self.clear_caches()

    def remove_dead_entities(self):
        """Remove dead entities from the dataframe."""
        self.df = self.df.where(col("entity_id") != self._store._dead_entities)

    def update_dead_entities(self):
        """Update the world's dead entities."""
        # Look for any new dead entities
        new_dead_entities = self.df.filter(~col("is_active")) \
                                    .select("entity_id") \
                                    .to_pydict() # Returns dict[str, List[Any]]
        
        # Convert List[dict[str,Any]] to Set[int]
        new_dead_entities = set(new_dead_entities["entity_id"])

        # Update the world's dead entities
        self._store._dead_entities.update(new_dead_entities)

    def clear_caches(self):
        self.components_to_update = set()
        self.df = None
