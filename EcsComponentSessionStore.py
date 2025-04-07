
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

        raise NotImplementedError("This is not implemented yet")

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
    from .base import Component

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

    store = ComponentStore()

    for i in range(10):
        store.add_component(i, Position(x=3, y=2, z=3))
        store.add_component(i, Velocity(vx=1, vy=2, vz=3))
        store.add_component(i, Acceleration(ax=1, ay=2, az=3))
  

    components = store.get_components([Position, Velocity, Acceleration])

    components.collect() 
    components.show()

    def yulk(df: daft.DataFrame) -> daft.DataFrame:
        print(df.show())


