from typing import List, Type, Dict, Any
import pyarrow as pa
import hashlib

from archetype.core.interfaces import ArchetypeSignature
from archetype.core import Component

class Archetype:
    """Convenience handler for archetype operations"""
    BASE_SCHEMA = pa.schema([
        pa.field("world_id", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("entity_id", pa.uint32(), nullable=False),
        pa.field("tick", pa.uint32(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
    ])
    PARTITION_KEYS = ["world_id", "run_id", "tick"]

    def __init__(self, components: List['Component']):
        self.components = components
        self.sig: ArchetypeSignature = self.sig_from_components(components)
        self.name = self.get_name(self.sig)
        self.schema = self.get_archetype_schema(self.sig)

    
    @staticmethod
    def sig_from_components(components: List['Component']) -> ArchetypeSignature:
        """
        Generate an archetype signature from a list of component instances.
        Returns a tuple of component types sorted by name for consistent signatures.
        """
        component_types = [type(c) for c in components]
        sig = tuple(sorted(component_types, key=lambda t: t.__name__))
        return sig

    @staticmethod
    def add_components(sig: ArchetypeSignature, component_types: List[Type[Component]]) -> ArchetypeSignature:
        """
        Generate a new archetype signature by adding components to an existing signature.
        """
        return tuple(sorted(set(sig).union(component_types), key=lambda t: t.__name__))

    @staticmethod
    def remove_components(sig: ArchetypeSignature, component_types: List[Type[Component]]) -> ArchetypeSignature:
        """
        Generate a new archetype signature by removing components from an existing signature.
        """

        return tuple(sorted(list(set(sig) - set(component_types)), key=lambda t: t.__name__))

    @staticmethod
    def get_name(sig: ArchetypeSignature) -> str:
        """
        Generate a human-readable name for an archetype, including a schema hash.
        The name combines sorted component names and an 8-character SHA256 hash
        of the archetype's combined PyArrow schema, ensuring uniqueness and
        indicating schema changes.
        """
        # Human-readable part: sorted component names
        component_names = sorted([comp_type.__name__ for comp_type in sig])
        readable_name = "_".join(component_names)

        # Schema hash part: hash of the combined PyArrow schema
        combined_schema = Archetype.get_archetype_schema(sig)
        # Convert PyArrow schema to a JSON string for consistent hashing
        schema_json = str(combined_schema)
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:8] # Take first 8 chars for brevity

        return f"{readable_name}_s{schema_hash}"

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
        entity_id: int,
        tick: int,
        components: List[Component],
        world_id: str,
        run_id: str
    ) -> Dict[str, Any]:
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
            "is_active": True
        }

        for c in components:
            prefix = c.get_prefix()
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

        return row_dict