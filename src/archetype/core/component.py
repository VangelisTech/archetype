# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any

import pyarrow as pa
from lancedb.pydantic import LanceModel


class Component(LanceModel):
    """
    Base class for all archetype components, extending LanceModel with additional utilities.
    """

    @classmethod
    def get_type_by_name(cls, name: str) -> type["Component"]:
        """Finds a Component subclass by its name (searches all descendants)."""
        for subclass in cls.__subclasses__():
            if subclass.__name__ == name:
                return subclass
            try:
                found = subclass.get_type_by_name(name)
            except ValueError:
                continue
            else:
                return found
        raise ValueError(f"Component type '{name}' not found.")

    def model_dump(self, **kwargs) -> dict[str, Any]:
        """Serialize this component to a dict, including the ``type`` key.

        The ``type`` key holds the class name and is used by :meth:`from_dict`
        to reconstruct the correct subclass.  It is intentionally excluded from
        :meth:`to_row_dict` so it is never written to the storage schema.
        """
        data = super().model_dump(**kwargs)
        data["type"] = self.__class__.__name__
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Component":
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
        row_dict = {
            prefix + key: value for key, value in self.model_dump().items() if key != "type"
        }
        return row_dict
