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

import hashlib
from typing import Any

import pyarrow as pa

from archetype.core.component import Component
from archetype.core.interfaces import ArchetypeSignature


class Archetype:
    """Convenience handler for archetype operations"""

    BASE_SCHEMA = pa.schema(
        [
            pa.field("world_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("entity_id", pa.int32(), nullable=False),
            pa.field("tick", pa.int32(), nullable=False),
            pa.field("is_active", pa.bool_(), nullable=False),
        ]
    )
    PARTITION_KEYS = ["world_id", "run_id", "tick"]

    def __init__(self, components: list["Component"]):
        self.components = components
        self.sig: ArchetypeSignature = self.sig_from_components(components)
        self.name = self.get_name(self.sig)
        self.schema = self.get_archetype_schema(self.sig)

    @staticmethod
    def sig_from_components(components: list["Component"]) -> ArchetypeSignature:
        """
        Generate an archetype signature from a list of component instances.
        Returns a tuple of component types sorted by name for consistent signatures.
        """
        component_types = [type(c) for c in components]
        sig = tuple(sorted(component_types, key=lambda t: t.__name__))
        return sig

    @staticmethod
    def add_components(
        sig: ArchetypeSignature, component_types: list[type[Component]]
    ) -> ArchetypeSignature:
        """
        Generate a new archetype signature by adding components to an existing signature.
        """
        return tuple(sorted(set(sig).union(component_types), key=lambda t: t.__name__))

    @staticmethod
    def remove_components(
        sig: ArchetypeSignature, component_types: list[type[Component]]
    ) -> ArchetypeSignature:
        """
        Generate a new archetype signature by removing components from an existing signature.
        """

        return tuple(sorted(list(set(sig) - set(component_types)), key=lambda t: t.__name__))

    @staticmethod
    def get_name(sig: ArchetypeSignature) -> str:
        """
        Generate a compact, filesystem-safe name for an archetype.
        We avoid extremely long identifiers by using only a short descriptor and
        a schema hash. This ensures stable uniqueness without exceeding path limits.
        """
        # Schema hash part: hash of the combined PyArrow schema
        combined_schema = Archetype.get_archetype_schema(sig)
        # Convert PyArrow schema to a JSON string for consistent hashing
        schema_json = str(combined_schema)
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:16]  # 16-char suffix

        # Compact descriptor: number of components
        num_components = len(sig)
        return f"a_{num_components}c_s{schema_hash}"

    @staticmethod
    def get_archetype_schema(sig: ArchetypeSignature) -> pa.Schema:
        """
        Get the schema for an archetype from a list of component types.
        Combines the base archetype schema with prefixed component schemas.
        """
        archetype_schema = Archetype.BASE_SCHEMA
        for component_type in sig:
            component_schema = component_type.get_prefixed_schema()
            archetype_schema = pa.unify_schemas([archetype_schema, component_schema])

        return archetype_schema

    @staticmethod
    def to_row_dict(
        entity_id: int, tick: int, components: list[Component], world_id: str, run_id: str
    ) -> dict[str, Any]:
        """
        Convert entity components to a row dictionary for archetype storage.

        Args:
            entity_id: The unique entity identifier
            tick: The simulation tick
            components: List of component instances
            world_id: The world identifier
            run_id: The run identifier

        Returns:
            Dict containing the row data for this entity
        """
        row_dict = {
            "world_id": str(world_id),
            "run_id": str(run_id),
            "entity_id": entity_id,
            "tick": tick,
            "is_active": True,
        }

        for c in components:
            prefix = c.get_prefix()
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

        return row_dict
