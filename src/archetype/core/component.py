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
    """Base class for typed entity data.

    Define a component by subclassing `Component` and declaring Pydantic fields.
    Keep components small and focused on one concern.

    Examples:
        >>> class Position(Component):
        ...     x: float = 0.0
        ...     y: float = 0.0
    """

    @classmethod
    def get_type_by_name(cls, name: str) -> type["Component"]:
        """Find a Component subclass (at any depth) by its class name.

        Raises:
            ValueError: If no matching subclass exists or more than one
                subclass has the requested name.
        """
        matches: list[type[Component]] = []
        stack: list[type[Component]] = [cls]
        seen: set[type[Component]] = set()
        while stack:
            current = stack.pop()
            for subclass in current.__subclasses__():
                if subclass in seen:
                    continue
                seen.add(subclass)
                if subclass.__name__ == name:
                    matches.append(subclass)
                stack.append(subclass)
        if not matches:
            raise ValueError(f"Component type '{name}' not found.")
        if len(matches) > 1:
            modules = ", ".join(sorted(f"{m.__module__}.{m.__name__}" for m in matches))
            raise ValueError(
                f"Component type name '{name}' is ambiguous ({modules}); "
                "name-addressed payloads require unique component class names."
            )
        return matches[0]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Component":
        """Create a component instance from a dictionary.

        Call this method on a concrete subclass, or include a `"type"` key
        naming a registered subclass when calling `Component.from_dict()`.

        Raises:
            ValueError: If the concrete component type cannot be determined.
        """
        component_type_name = data.get("type")
        # Build kwargs without mutating the caller's dict. ``dict.pop`` would
        # strip ``"type"`` from the input in-place, so a second call on the
        # same dict silently falls through to the base ``Component`` branch
        # and returns the wrong type.
        fields = {k: v for k, v in data.items() if k != "type"}
        if component_type_name:
            ComponentType = cls.get_type_by_name(component_type_name)
            return ComponentType(**fields)
        if cls is Component:
            raise ValueError(
                "Component.from_dict requires a 'type' key in the payload to "
                "identify the concrete subclass. Use Component.to_payload() "
                "to serialize, or include 'type' explicitly. Got keys: "
                + ", ".join(sorted(fields.keys()))
            )
        return cls(**fields)

    def to_payload(self) -> dict[str, Any]:
        """Serialize this component for command and API payloads.

        Unlike `model_dump()`, the result includes the concrete component
        type so `Component.from_dict()` can reconstruct it.
        """
        return {"type": type(self).__name__, **self.model_dump()}

    @classmethod
    def get_prefix(cls) -> str:
        """Return the column prefix for this component type."""
        return cls.__name__.lower() + "__"

    @classmethod
    def to_pyarrow_schema(cls) -> pa.Schema:
        """Return the unprefixed PyArrow schema for this component type."""
        if issubclass(cls, LanceModel):
            return cls.to_arrow_schema()
        else:
            raise ValueError(f"Component {cls} is not a subclass of LanceModel")

    @classmethod
    def get_prefixed_schema(cls) -> pa.Schema:
        """Return a PyArrow schema whose fields use the component prefix."""
        component_schema = cls.to_pyarrow_schema()
        prefix = cls.get_prefix()

        # Rename the fields of the component schema with the prefix
        for i, field_name in enumerate(component_schema.names):
            field = component_schema.field(field_name)
            renamed_field = field.with_name(prefix + field_name)
            component_schema = component_schema.set(i, renamed_field)

        return component_schema

    def to_row_dict(self) -> dict[str, Any]:
        """Return component values keyed by their prefixed column names."""
        prefix = self.get_prefix()
        row_dict = {prefix + key: value for key, value in self.model_dump().items()}
        return row_dict
