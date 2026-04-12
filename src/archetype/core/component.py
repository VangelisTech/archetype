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
        """Find a Component subclass (at any depth) by its class name.

        Walks the full subclass tree — ``__subclasses__()`` only returns
        direct subclasses, so a naive lookup misses components defined as
        subclasses of intermediate base classes.
        """
        stack: list[type[Component]] = [cls]
        seen: set[type[Component]] = set()
        while stack:
            current = stack.pop()
            for subclass in current.__subclasses__():
                if subclass in seen:
                    continue
                seen.add(subclass)
                if subclass.__name__ == name:
                    return subclass
                stack.append(subclass)
        raise ValueError(f"Component type '{name}' not found.")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Component":
        """Create a component instance from a dictionary.

        The dict must either include a ``"type"`` key naming a concrete
        Component subclass, or this method must be called on a concrete
        subclass directly. Calling ``Component.from_dict({...})`` without
        a ``"type"`` key raises ``ValueError`` rather than silently
        constructing a base Component with lost type information.
        """
        component_type_name = data.pop("type", None)
        if component_type_name:
            ComponentType = cls.get_type_by_name(component_type_name)
            return ComponentType(**data)
        if cls is Component:
            raise ValueError(
                "Component.from_dict requires a 'type' key in the payload to "
                "identify the concrete subclass. Use Component.to_payload() "
                "to serialize, or include 'type' explicitly. Got keys: "
                + ", ".join(sorted(data.keys()))
            )
        return cls(**data)

    def to_payload(self) -> dict[str, Any]:
        """Serialize to a dict suitable for ``Command`` payloads.

        Unlike ``model_dump()``, this includes a ``"type"`` key so that
        ``Component.from_dict()`` can reconstruct the concrete subclass
        on the other side of a round-trip through ``CommandService``.
        """
        return {"type": type(self).__name__, **self.model_dump()}

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
