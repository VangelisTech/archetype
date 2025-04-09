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

from typing import Dict, List, Optional, Type, Union
from itertools import count as _count
from logging import getLogger
import daft
from daft import col, DataFrame
import pyarrow as pa

# Import base types from our new structure
from .base import Component
# Import interface
from .interfaces import ComponentStoreInterface

logger = getLogger(__name__)

class ComponentStore(ComponentStoreInterface): # Implement interface
    """
    Manages the historical state of components, stored internally using
    a _ComponentData structure for each component type. Each row includes
    entity_id, step, is_active, and component data.
    """

    def __init__(self):
        self.components: Dict[Type[Component], DataFrame] = {}
        self.entities: Dict[int, List[Component]] = {}

        # Entity Management 
        self._entity_count = _count(start=1)
        self._dead_entities = set()

    def register_component(self, component_type: Type[Component]) -> None:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist setting composite to 
        a blank daft.DataFrame with the correct schema
        """
        
        component_arrow_schema: pa.Schema = component_type.to_arrow_schema()

        # Insert core fields
        composite_arrow_schema = component_arrow_schema.insert(0, pa.field("entity_id", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(1, pa.field("step", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(2, pa.field("is_active", pa.bool_()))

        # Create empty DataFrame
        df = daft.from_arrow(composite_arrow_schema.empty_table())
        df = df.repartition(
            num=None,
            partition_by="step"
        )
        
        self.components[component_type] = df
        logger.debug(f"Registered component type: {component_type.__name__}")


    def get_component(self, component_type: Type[Component]) -> DataFrame:
        """
        Gets the DataFrame for a component type, registering it if needed.
        Raises exception if registration fails critically.
        Eagerly materializes the DataFrame to fetch latest state.
        """
        if component_type not in self.components:
            try:
                self.register_component(component_type)
            except Exception as e:
                 raise RuntimeError(f"Failed to register component {component_type.__name__}") from e

        df = self.components.get(component_type)

        # Materialize upon retrieval for latest state 
        return df.collect() # possibly empty df, but that's ok
    
    # Update Methods
    def update_component(self, update_df: DataFrame, component_type: Type[Component]) -> None:
        """
        Concatenates new update data to the existing component DataFrame.
        Eagerly materializes the DataFrame to ensure it's in memory.
        Important for entity initialization. 
        
        """
        original_df = self.get_component(component_type)

        # Schema alignment and concat logic (user's version with try/except around concat)
        update_df_aligned = update_df.select(*original_df.column_names) # Align columns first
        try:
            # IMPORTANT: Processor MUST return full schema for any component type,
            # otherwise, we'll have issues with column alignment.
            # Responsibility lies with Processor developer to ensure this
            new_df = original_df.concat(update_df_aligned) # FROM DAFT DOC: DataFrames being concatenated must have exactly the same schema.
        except Exception as e:
            logger.error(f"Error concatenating DataFrames for {component_type.__name__}: {e}", exc_info=True)
            raise ValueError(f"Schema mismatch or concat error for {component_type.__name__}") from e

        self.components[component_type] = new_df
    
    def add_component(self, entity_id: int, component_instance: Component, step: int) -> None:  
        """
        Adds/updates a component from an instance.
        Relies on get_component to auto-register.
        """
        component_type = type(component_instance)

        # Prepare data row
        component_dict = component_instance.model_dump()
        component_dict["entity_id"] = entity_id
        component_dict["step"] = step
        component_dict["is_active"] = True

        # Create single-row DataFrame
        component_df = daft.from_pylist([component_dict]) # Expects List[Dict[str, Any]]

        self.update_component(component_df, component_type)

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
    
    def remove_component_from_entity(self, 
            entity_id: int, 
            component_type: Type[Component], 
            steps: Optional[Union[int, List[int]]] = None
        ) -> None:
        """
        Removes a component from an entity.
        """
        # Remove Entity from Entity Dictionary
        if entity_id in self.entities.keys():
            self.entities[entity_id].pop(component_type)

        # Remove Entity from Component DataFrame
        df = self.get_component(component_type)
        df = df.where(col("entity_id") != entity_id)
        self.components[component_type] = df

    def remove_entity_from_component(self, entity_id: int, component_type: Type[Component], steps: Optional[Union[int, List[int]]] = None) -> None:
        """
        Removes a entity from a component. 
        Alias for remove_component_from_entity which does the same thing.
        (remove entity/component relationship)
        """
        self.remove_component_from_entity(entity_id, component_type, steps)

    
    def get_column_names(self, component_type: Type[Component]) -> List[str]:
        """
        Returns a list of column names for a component type.
        """
        return self.components[component_type].column_names
