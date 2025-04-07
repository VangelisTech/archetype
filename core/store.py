# -*- coding: utf-8 -*-
"""
Manages the storage and state of components using LanceDB tables.

Key aspects of this implementation:
- Component data is stored historically, with each row representing the state
  of a component for an entity at a specific step.
- Includes an `entity_id`, a `step` column, an `is_active` flag, and component fields.
- Updates append new rows with the current step number.
- Component removal appends a row with `is_active=False`.
- Provides methods to query the latest state or the state at a specific step.

Uses a Daft Catalog and Session to manage persistent storage:
- Each component type gets its own table in the catalog
- Tables are created with a schema derived from the component's Pydantic model
- Schema includes `entity_id`, `step`, `is_active`, and component fields
- Updates are processed through Daft DataFrames before being committed to tables
- Queries use Daft's DataFrame API to filter and transform data efficiently
- Component instances are reconstructed from table data using Pydantic validation
"""

from typing import Dict, List, Optional 
from itertools import count as _count
import daft
from daft import col, DataFrame
import pyarrow as pa

# Import base types from our new structure
from .base import Component
# Import interface
from .interfaces import ComponentStoreInterface

class ComponentStore(ComponentStoreInterface): # Implement interface
    """
    Manages the historical state of components, stored internally using
    a _ComponentData structure for each component type. Each row includes
    entity_id, step, is_active, and component data.
    """

    def __init__(self):
        self.components: Dict[Component, DataFrame] = {}
        self.entities: Dict[int, List[Component]] = {}

        # Entity Management 
        self._entity_count = _count(start=1)
        self._dead_entities = set()

    def register_component(self, component: Component) -> None:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist setting composite to 
        a blank daft.DataFrame with the correct schema
        """
        # Get the arrow schema for the component type
        component_arrow_schema: pa.Schema = component.to_arrow_schema()

        # Insert core fields: entity_id, step, is_active
        composite_arrow_schema = component_arrow_schema.insert(0, pa.field("entity_id", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(1, pa.field("step", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(2, pa.field("is_active", pa.bool_()))

        df = daft.from_arrow(composite_arrow_schema.empty_table())
        df = df.repartition(None,"step")
        
        self.components[component] = df


    def get_component(self, component: Component) -> daft.DataFrame:
        """
        Gets the latest active state for a specific component type.
        """
        if self.components.get(component) is None:
            self.register_component(component)
        elif len(self.components.get(component)) == 0:
            return None

        return self.components.get(component)
    
    # Update Methods
    def update_component(self, update_df: daft.DataFrame, component: Component) -> None:
        """Once a processor has applied its changes to the joined dataframe, we need to then update the individual component instances"""
        
        original_df = self.get_component(component)
        new_df = original_df.concat(update_df)

        self.components[component] = new_df # Formal Storage assignment, Must Collect to update component state
    
    def add_component(self, entity_id: int, component: Component, step: int) -> None:  
        """
        A convenience method for updating composites from component instances directly. 
        """
        
        df = self.get_component(component)

        # Create a dictionary from the component's model fields
        component_dict = component.model_dump()
        component_dict["entity_id"] = entity_id
        component_dict["step"] = step
        component_dict["is_active"] = True

        # Prep Concat
        component_df = daft.from_pylist([component_dict])

        # adding the component record to the dataframe
        self.components[component] = df.concat(component_df[df.column_names])

    def add_entity(self, components: Optional[List[Component]] = None, step: Optional[int] = -1) -> int:
        """
        Adds a new entity to the store.
        """
        entity = next(self._entity_count)

        # Add entity to components
        for component in components:
            self.add_component(entity, component, step)

        # Add entity to entities
        self.entities[entity] = [component for component in components]

        return entity
    
    def remove_entity(self, entity_id: int) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        if entity_id in self._dead_entities:
            return

        self._dead_entities.update(entity_id)
    
    def remove_component_from_entity(self, entity_id: int, component: Component) -> None:
        """
        Removes a component from an entity.
        """
        self.entities[entity_id].pop(component)
        self.components[component] = self.components[component].where(col("entity_id") != entity_id)

    def remove_component(self, component: Component) -> None:
        """
        Removes a 
        """
        # Get active entities for the component type
        df = self.get_component(component)

        active_entities = df.select("entity_id")\
                            .where(col("is_active"))

        # Remove the component from the active entities
        for entity in active_entities["entity_id"]:
            self.remove_component_from_entity(entity, component)
        
        # Finally, remove the component from the store
        del self.components[component]
    
    def get_column_names(self, component: Component) -> List[str]:
        """
        Returns the column names for a component type.
        """
        return self.components[component].column_names