from typing import Type, Dict, Any 
from lancedb.pydantic import LanceModel
import pyarrow as pa


class Component(LanceModel):
    """
    Base class for all archetype components, extending LanceModel with additional utilities.
    """

    @classmethod
    def get_type_by_name(cls, name: str) -> Type["Component"]:
        """Finds a Component subclass by its name."""
        # This could be optimized with a cache if needed
        for subclass in cls.__subclasses__():
            if subclass.__name__ == name:
                return subclass
        raise ValueError(f"Component type '{name}' not found.")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Component":
        """Create a component instance from a dictionary."""
        component_type_name = data.pop("type", None)
        if component_type_name:
            ComponentType = cls.get_type_by_name(component_type_name)
            return ComponentType(**data)
        return cls(**data)

    @classmethod
    def get_prefix(cls) -> str:
        """Generate a standardized prefix for this component type's fields."""
        return cls.__name__.lower() + "__"

    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema:
        """
        Convert this component type to a PyArrow schema.
        Uses LanceModel's built-in to_arrow_schema method.
        """
        if issubclass(cls, LanceModel):
            return cls.to_arrow_schema()
        else:
            raise ValueError(f"Component {cls} is not a subclass of LanceModel")

    @classmethod
    def get_prefixed_schema(cls) -> pa.Schema:
        """
        Get this component's PyArrow schema with prefixed field names.
        This is used when combining multiple components into an archetype schema.
        """
        component_schema = cls.to_pyarrow_schema()
        prefix = cls.get_prefix()

        # Rename the fields of the component schema with the prefix
        for i, field_name in enumerate(component_schema.names):
            field = component_schema.field(field_name)
            renamed_field = field.with_name(prefix + field_name)
            component_schema = component_schema.set(i, renamed_field)

        return component_schema
    
    def to_row_dict(self):
        prefix = self.get_prefix()
        row_dict = {prefix + key: value for key, value in self.model_dump().items()}
        return row_dict

