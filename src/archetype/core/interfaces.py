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

from typing import Protocol, List, Type, Tuple, Dict, Optional, Any
from daft import DataFrame
import pyarrow as pa
from lancedb.pydantic import LanceModel
import hashlib
from uuid import UUID
from .command import Command


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


ArchetypeSignature = Tuple[Type[Component], ...]

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

    def __init__(self, components: List[Component]):
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
    def add_components(sig: ArchetypeSignature, components: List[Component]) -> ArchetypeSignature:
        """
        Generate a new archetype signature by adding components to an existing signature.
        """
        component_types = set(sig)
        for c in components:
            component_types.add(type(c))
        return tuple(sorted(list(component_types), key=lambda t: t.__name__))

    @staticmethod
    def remove_components(sig: ArchetypeSignature, component_types: List[Type[Component]]) -> ArchetypeSignature:
        """
        Generate a new archetype signature by removing components from an existing signature.
        """
        existing_component_types = set(sig)
        component_types_to_remove = set(component_types)
        new_component_types = existing_component_types - component_types_to_remove
        return tuple(sorted(list(new_component_types), key=lambda t: t.__name__))

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
            "world_id": world_id,
            "run_id": run_id,
            "entity_id": entity_id,
            "tick": tick,
            "is_active": True
        }

        for c in components:
            prefix = c.get_prefix()
            row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

        return row_dict


class iProcessor(Protocol):
    components: Tuple[Type[Component], ...] = None
    priority: int = 0
    def process(self, df: DataFrame, *args, **kwargs) -> DataFrame: ...

class iStore(Protocol):
    def add_entity(self, components: List[Component], tick: int, world_id: str, run_id: str ) -> int: ...
    def remove_entity(self, entity_id: int, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> None: ...
    def get_archetype_df(self, sig: ArchetypeSignature) -> DataFrame: ...
    def append(self, sig: ArchetypeSignature, df: DataFrame, tick: int, world_id: str, run_id: str) -> None: ...
    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None: ...
    def transition_entity(self, entity_id: int, old_sig: ArchetypeSignature, new_sig: ArchetypeSignature, new_data: Dict[str, Any], tick: int, world_id: str, run_id: str) -> None: ...

class iQueryManager(Protocol):
    def get_archetype(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> Tuple[ArchetypeSignature, DataFrame]: ...
    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame: ...

class iUpdateManager(Protocol):
    def update(self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> None: ...
    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None: ...
    def remove_entity(self, entity_id: int, tick: int, world_id: str, run_id: str) -> None: ...
class iSystem(Protocol):
    def add_processor(self, processor: iProcessor) -> None: ...
    def remove_processor(self, processor: iProcessor) -> None: ...
    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame: ...



class iWorld(Protocol):
    def __init__(self, store: iStore, querier: iQueryManager, updater: iUpdateManager, system: iSystem): ...
    def step(self, dt: float): ...
    def spawn(self, components: List[Component], tick: Optional[int] = None) -> int: ...
    def despawn(self, entity_id: int, tick: Optional[int] = None) -> None: ...
    def remove(self, entity_id: int, comp_type: Type[Component]) -> None: ...


class iCommandBroker(Protocol):
    def enqueue(self, cmd: "Command") -> None: ...
    def enqueue_bulk(self, cmds: List["Command"]) -> None: ...
    def dequeue_due(self, *, tick: int, limit: int = 1_000) -> List[Command]: ...
    def ack(self, cmd_ids: List[UUID]) -> None: ...






