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

class Component(LanceModel):
    """
    Base class for all archetype components, extending LanceModel with additional utilities.
    """

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
        pa.field("entity_id", pa.uint64(), nullable=False),
        pa.field("step", pa.uint64(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
    ])
    PARTITION_KEYS = ["world_id", "run_id", "step"]

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
    def get_name(sig: ArchetypeSignature) -> str:
        """Generate a unique hash string for an archetype signature."""
        hash_val = ""
        for comp_type in sig:
            hash_val += comp_type.__name__[0:3]
        return "arch_" + hash_val

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



class iProcessor(Protocol):
    components: Tuple[Type[Component], ...] = None
    priority: int = 0
    def process(self, df: DataFrame, *args, **kwargs) -> DataFrame: ...

class iStore(Protocol):
    def add_entity(self, components: List[Component], step: int, world_id: str, run_id: str ) -> int: ...
    def remove_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None: ...
    def get_archetype_df(self, sig: ArchetypeSignature) -> DataFrame: ...
    def append(self, sig: ArchetypeSignature, df: DataFrame, step: int, world_id: str, run_id: str) -> None: ...
    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None: ...

class iQuerier(Protocol):
    def get_archetype(self, sig: ArchetypeSignature, current_step: int, world_id: str, run_id: str) -> Tuple[ArchetypeSignature, DataFrame]: ...
    def get_archetype_for_entity(self, entity_id: int, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> DataFrame: ...

class iUpdater(Protocol):
    def update(self, df: DataFrame, sig: ArchetypeSignature, step: int, world_id: str, run_id: str) -> None: ...
    def materialize_spawns(self, spawn_cache: Dict[ArchetypeSignature, List[Dict[str, Any]]], world_id: str, run_id: str) -> None: ...
    def remove_entity(self, entity_id: int, step: int, world_id: str, run_id: str) -> None: ...
class iSystem(Protocol):
    def add_processor(self, processor: iProcessor) -> None: ...
    def remove_processor(self, processor: iProcessor) -> None: ...
    def execute(self, df: DataFrame, sig: ArchetypeSignature, *args, **kwargs) -> DataFrame: ...

class iWorld(Protocol):
    def __init__(self, store: iStore, querier: iQuerier, updater: iUpdater, system: iSystem): ...
    def step(self, dt: float): ...
    def spawn(self, components: List[Component], step: Optional[int] = None) -> int: ...
    def despawn(self, entity_id: int, step: Optional[int] = None) -> None: ...
    def remove(self, entity_id: int, comp_type: Type[Component]) -> None: ...
