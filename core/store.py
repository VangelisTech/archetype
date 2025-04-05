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

from typing import Dict, List, Type, Optional
from itertools import count as _count
from itertools import count as _step_counter

import daft
from daft import col, lit, DataType as DT, Catalog, Session
import daft.expressions as F
import pyarrow as pa # Need pyarrow for schema manipulation

# Import base types from our new structure
from core.base import Component


class EcsComponentStore:
    """
    Manages the historical state of components, stored internally using
    a _ComponentData structure for each component type. Each row includes
    entity_id, step, is_active, and component data.
    """
    def __init__(self):
        self.components: Dict[Type[Component], daft.DataFrame] = {}
        self.entities: Dict[int, List[Type[Component]]] = {}
        self._entity_count = _count(start=1)
        self._step_counter = _step_counter(start=0)
        self._dead_entities = set()
    
    def register_component(self, component_type: Type[Component]) -> None:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist setting composite to 
        a blank daft.DataFrame with the correct schema
        """
        # Get the arrow schema for the component type
        component_arrow_schema: pa.Schema = component_type.to_arrow_schema()

        # Insert core fields: entity_id, step, is_active
        composite_arrow_schema = component_arrow_schema.insert(0, pa.field("entity_id", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(1, pa.field("step", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(2, pa.field("is_active", pa.bool_()))

        df = daft.from_arrow(composite_arrow_schema.empty_table())
        
        self.components[component_type] = df.collect()


    def get_component(self, component_type: Type[Component]) -> daft.DataFrame:
        """
        Gets the latest active state for a specific component type.
        """
        if not self.components.get(component_type):
            self.components[component_type] = self._create_df_for_composite(component_type)

        return self.components.get(component_type)
    
    # Update Methods
    def update_component(self, update_df: daft.DataFrame, component: Component) -> None:
        """Once a processor has applied its changes to the joined dataframe, we need to then update the individual component instances"""
        
        original_df = self.get_component(type(component))
        updated_df = original_df.concat(update_df)

        # Set dead entities to inactive
        updated_df = updated_df.where(col("entity_id") == self._dead_entities) \
            .with_column("is_active", lit(False))

        self.components[type(component)] = updated_df
    
    def add_component(self, entity_id: int, component: Component, step: Optional[int] = -1, is_active: Optional[bool] = True) -> None:  
        """
        A convenience method for updating composites from component instances directly. 
        """
        
        df = self.get_component(type(component))

        # Create a dictionary from the component's model fields
        component_dict = component.model_dump()
        component_dict["entity_id"] = entity_id
        component_dict["step"] = step
        component_dict["is_active"] = is_active

        #adding the component to the dataframe
        self.components[type(component)] = df.concat(daft.from_pydict(component_dict))
 
    def create_entity(self, *components: Component) -> int:
        """
        Creates a new entity in the store.
        """
        entity = next(self._entity_count)

        # Add entity to components
        for component in components:
            self.add_component(entity, component)

        # Add entity to entities
        self.entities[entity] = [type(component) for component in components]

        return entity
    
    def remove_entity(self, entity_id: int, step: int, immediate: bool = False) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        if entity_id in self._dead_entities:
            return

        self._dead_entities.add(entity_id)

        # If immediate, remove the entity from all components otherwise, QueryInterface will handle it 
        if immediate:
            for component_type in self.entities[entity_id]:
                df = self.get_component(component_type)

                # Find the row for the entity at the given step and set is_active to False
                df = df.where(col("entity_id") == entity_id) \
                    .where(col("step") == step) \
                    .with_column("is_active", lit(False))
                
                # Replace original composite with updated one
                self.components[component_type] = df 
            
    
    def remove_component(self, component_type: Type[Component]) -> None:
        """
        Removes a row from the  from the store.
        """
        self.components[component_type] = None
       


class EcsComponentSessionStore:
    """
    Manages the historical state of components, stored internally using
    a _ComponentData structure for each component type. Each row includes
    entity_id, step, is_active, and component data.
    """
    def __init__(self, catalog: Catalog):
        self._session = Session()
        self._session.attach(catalog, alias="ecs_catalog")
        self._namespace = "components"

        self._session.set_namespace(self._namespace)

    def _create_composite_arrow_schema(self, component: Type[Component]) -> pa.Schema:
        """
        Creates the Daft Schema for a component type's historical DataFrame.
        Includes entity_id, step, is_active, and component fields.
        """
        component_arrow_schema: pa.Schema = component.to_arrow_schema()
        # Insert core fields: entity_id, step, is_active
        composite_arrow_schema = component_arrow_schema.insert(0, pa.field("entity_id", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(1, pa.field("step", pa.int64()))
        composite_arrow_schema = composite_arrow_schema.insert(2, pa.field("is_active", pa.bool_()))

        return composite_arrow_schema


    def get_table_name(self, component_type: Type[Component]) -> str:
        """
        Returns the table name for a component type.
        """
        return f"{self._namespace}.{component_type.__name__}"

    def create_table_for_composite(self, component_type: Type[Component]) -> pa.Table:
        """
        Creates a table for a component type's historical DataFrame.
        """
        table_name = self.get_table_name(component_type)
        table_schema = self._create_composite_arrow_schema(component_type)
        table = self._session.create_table(
            table_name,
            daft.from_arrow(table_schema.empty_table())
        )
        return table

    
    def register_component(self, component_type: Type[Component]) -> daft.Table:
        """
        Ensures a component type is known to the store, creating its schema
        and internal data structure if it doesn't exist. Returns the _ComponentData.
        """
        table_name = self.get_table_name(component_type)
        
        if not self._session.has_table(table_name):
            self.create_table_for_composite(component_type)

        table = self._session.get_table(table_name)
        return table

    def add_component(self, entity_id: int, component: Component, step: Optional[int] = -1) -> None:  
        """
        Adds a component to the store for an entity.
        """
        table = self.register_component(type(component))
        df = daft.from_pydict(component.model_dump())
        df = daft.with_columns({
            "entity_id": lit(entity_id),
            "step": lit(step),
            "is_active": lit(True),
        })
        table.append(df)

    def read_table(self, component_type: Type[Component]) -> daft.DataFrame:
        """
        Reads the table for a component type.
        """
        table = self._session.get_table(self.get_table_name(component_type))
        return table.read()

    def get_latest_state(self, df: daft.DataFrame) -> Optional[daft.DataFrame]:
        """
        Gets a DataFrame representing the latest active state for each entity
        for the given component type. Returns a DataFrame plan or None.
        """
        # Get the latest step for each entity
        latest_steps = df.groupby("entity_id").agg(F.max(col("step")).alias("latest_step"))

        # Join back to get the full row for the latest step
        latest_df = df.join(latest_steps, left_on=["entity_id", "step"], right_on=["entity_id", "latest_step"])

        # Filter out inactive Entities
        active_latest_df = latest_df.where(col("is_active"))

        # Select only original columns defined in the schema (excluding 'latest_step')
        final_df = active_latest_df.select(*[col(name) for name in df.column_names()])
        
        return final_df

    def get_state_at_step(self, df: daft.DataFrame, step: int) -> Optional[daft.DataFrame]:
        """
        Gets a DataFrame representing the active state for each entity
        for the given component type as it existed exactly at the specified step.
        If an entity had no update at that specific step, but was active before,
        it finds the most recent state up to that step. Returns a DataFrame plan or None.
        """
        # Filter to the specific step
        step_df = df.where(col("step") <= step)

        # Get the latest step for each entity
        final_df = self.get_latest_state(step_df)

        return final_df

    def get_components(self, component_types: List[Type[Component]]) -> Dict[Type[Component], daft.DataFrame]:
        shared_df = None
        # Get tables for each component type
        for component in component_types:
            df = self.read_table(component)
            latest_df = self.get_latest_state(df)
            if shared_df is None:
                shared_df = latest_df
            else:
                shared_df = shared_df.join(latest_df, on=["entity_id", "step", "is_active"])

        return shared_df
    
    def remove_entity(self, entity_id: int) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        for component in self._component_types:
            self.remove_component(entity_id, component)
    
    def remove_component(self, entity_id: int, component_type: Type[Component]) -> None:
        """
        Sets the is_active flag to False for an entity.
        """
        table = self.register_component(component_type)
        table.update(daft.DataFrame({"is_active": False}))



    

if __name__ == "__main__":
    from core.base import Component
    from daft.catalog import Catalog
    class Position(Component):
        x: float = 0.0
        y: float = 0.0
        z: float = 0.0

    class Velocity(Component):
        vx: float = 0.0
        vy: float = 0.0
        vz: float = 0.0

    class Acceleration(Component):
        ax: float = 0.0
        ay: float = 0.0
        az: float = 0.0

    catalog = Catalog.from_pydict({})
    store = EcsComponentStore(catalog)

    for i in range(10):
        store.add_component(i, Position(x=3, y=2, z=3))
        store.add_component(i, Velocity(vx=1, vy=2, vz=3))
        store.add_component(i, Acceleration(ax=1, ay=2, az=3))
  

    components = store.get_components([Position, Velocity, Acceleration])

    components.collect() # OMG I LOVE THIS (Thanks Daft for the collect method)
    components.show()

    def yulk(df: daft.DataFrame) -> daft.DataFrame:
        return print(df.filter(col("is_active")).show())


