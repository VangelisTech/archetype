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

from .store import SyncStore
from .querier import QueryManager
from .updater import UpdateManager
from .system import SyncSystem
from .world import SyncWorld
from .processor import Processor, processor

from daft.catalog import Catalog
import daft
from daft import DataFrame
import pyarrow as pa
from lancedb.pydantic import LanceModel
from typing import List, Type, Tuple

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




def make_simple_world(
        uri: str,
        world_id: str | None = None,
        run_id: str | None = None,
        namespace: str | None = None,
        catalog: Catalog | None = None,
        debug: bool = False
    ) -> SyncWorld:

    store = SyncStore(
        uri = uri,
        namespace = namespace,
        catalog = catalog,
        debug = debug
    )
    querier = QueryManager(store=store)
    updater = UpdateManager(store=store)
    system  = SyncSystem()

    world = SyncWorld(
        querier=querier,
        updater=updater,
        system=system,
        world_id=world_id,
        run_id=run_id,
        debug=debug,
    )
    return world

__all__ = [
    "SimpleWorld",
    "Processor",
    "processor",
    "Component",
    "make_simple_world",
    "InputProcessor"
]
