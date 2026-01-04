# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype DSL v2 — DataFrame-First Design
==========================================

A better DSL that honors the core engine by:
1. Preserving DataFrame batch operations (no collect-and-loop)
2. Compiling behaviors to pure DataFrame transforms
3. Making archetypes explicit and first-class
4. Separating query API from behavior definition

Design Principles:
- Behaviors generate Daft expressions, not Python loops
- Component access compiles to col("component__field")
- Filters compile to df.where() clauses
- AgentProxy only for query results, not in behavior hot path
"""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import daft
from daft import DataFrame, col

from archetype.app.broker import CommandBroker
from archetype.app.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncProcessor, AsyncSystem, AsyncWorld
from archetype.core.component import Component
from archetype.core.config import (
    CacheConfig,
    RunConfig,
    StorageBackend,
    StorageConfig,
    WorldConfig,
)
from archetype.core.interfaces import ArchetypeSignature
from archetype.core.resources import Resources


# =============================================================================
# Component Field Expression Builder
# =============================================================================


class Field:
    """
    Represents a component field that compiles to a Daft expression.
    
    Usage in behavior:
        @processor
        class Move:
            requires = [Position, Velocity]
            
            def transform(self, arch):
                # arch.position.x returns Field("position__x")
                # which compiles to col("position__x")
                return {
                    "position__x": arch.position.x + arch.velocity.vx,
                    "position__y": arch.position.y + arch.velocity.vy,
                }
    """
    
    def __init__(self, column_name: str):
        self._column_name = column_name
    
    def to_expr(self) -> daft.Expression:
        """Compile to Daft column expression."""
        return col(self._column_name)
    
    # Arithmetic operations
    def __add__(self, other):
        return BinaryOp(self, other, "+")
    
    def __sub__(self, other):
        return BinaryOp(self, other, "-")
    
    def __mul__(self, other):
        return BinaryOp(self, other, "*")
    
    def __truediv__(self, other):
        return BinaryOp(self, other, "/")
    
    def __radd__(self, other):
        return BinaryOp(other, self, "+")
    
    def __rsub__(self, other):
        return BinaryOp(other, self, "-")
    
    def __rmul__(self, other):
        return BinaryOp(other, self, "*")
    
    def __rtruediv__(self, other):
        return BinaryOp(other, self, "/")
    
    # Comparison operations
    def __gt__(self, other):
        return BinaryOp(self, other, ">")
    
    def __lt__(self, other):
        return BinaryOp(self, other, "<")
    
    def __ge__(self, other):
        return BinaryOp(self, other, ">=")
    
    def __le__(self, other):
        return BinaryOp(self, other, "<=")
    
    def __eq__(self, other):
        return BinaryOp(self, other, "==")
    
    def __ne__(self, other):
        return BinaryOp(self, other, "!=")


class BinaryOp:
    """Binary operation between Fields/literals."""
    
    def __init__(self, left, right, op: str):
        self.left = left
        self.right = right
        self.op = op
    
    def to_expr(self) -> daft.Expression:
        """Compile to Daft expression."""
        left_expr = self.left.to_expr() if isinstance(self.left, (Field, BinaryOp)) else daft.lit(self.left)
        right_expr = self.right.to_expr() if isinstance(self.right, (Field, BinaryOp)) else daft.lit(self.right)
        
        if self.op == "+":
            return left_expr + right_expr
        elif self.op == "-":
            return left_expr - right_expr
        elif self.op == "*":
            return left_expr * right_expr
        elif self.op == "/":
            return left_expr / right_expr
        elif self.op == ">":
            return left_expr > right_expr
        elif self.op == "<":
            return left_expr < right_expr
        elif self.op == ">=":
            return left_expr >= right_expr
        elif self.op == "<=":
            return left_expr <= right_expr
        elif self.op == "==":
            return left_expr == right_expr
        elif self.op == "!=":
            return left_expr != right_expr
        else:
            raise ValueError(f"Unknown operation: {self.op}")
    
    # Allow chaining operations
    def __add__(self, other):
        return BinaryOp(self, other, "+")
    
    def __sub__(self, other):
        return BinaryOp(self, other, "-")
    
    def __mul__(self, other):
        return BinaryOp(self, other, "*")
    
    def __truediv__(self, other):
        return BinaryOp(self, other, "/")


class ComponentFieldAccessor:
    """Provides field access for a component type."""
    
    def __init__(self, component_cls: type[Component]):
        self._component_cls = component_cls
        self._prefix = component_cls.get_prefix()
    
    def __getattr__(self, name: str) -> Field:
        """Return Field for component.field access."""
        return Field(self._prefix + name)


class ArchetypeAccessor:
    """Provides component access for an archetype."""
    
    def __init__(self, components: tuple[type[Component], ...]):
        self._components = components
        self._accessors = {}
        
        for cls in components:
            # Convert ClassName to class_name for attribute access
            attr_name = self._to_snake_case(cls.__name__)
            self._accessors[attr_name] = ComponentFieldAccessor(cls)
    
    @staticmethod
    def _to_snake_case(name: str) -> str:
        import re
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    
    def __getattr__(self, name: str) -> ComponentFieldAccessor:
        if name in self._accessors:
            return self._accessors[name]
        raise AttributeError(f"Archetype has no component '{name}'")


# =============================================================================
# Processor Decorator - DataFrame-First Behaviors
# =============================================================================


@dataclass
class ProcessorSpec:
    """Specification for a DataFrame processor."""
    
    name: str
    processor_class: type
    requires: tuple[type[Component], ...]
    priority: int = 10
    filter_expr: Callable[[ArchetypeAccessor], Any] | None = None


def processor(cls: type) -> ProcessorSpec:
    """
    Decorator for DataFrame-first behaviors.
    
    Usage:
        @processor
        class Move:
            requires = [Position, Velocity]
            priority = 10
            
            # Optional: filter entities (compiles to df.where())
            @staticmethod
            def filter(arch):
                return arch.velocity.vx != 0
            
            # Required: return dict of column updates
            def transform(self, arch, tick: int, dt: float):
                return {
                    "position__x": arch.position.x + arch.velocity.vx * dt,
                    "position__y": arch.position.y + arch.velocity.vy * dt,
                }
    """
    return ProcessorSpec(
        name=cls.__name__,
        processor_class=cls,
        requires=tuple(getattr(cls, "requires", [])),
        priority=getattr(cls, "priority", 10),
        filter_expr=getattr(cls, "filter", None) if hasattr(cls, "filter") else None,
    )


class DataFrameProcessor(AsyncProcessor):
    """Compiles ProcessorSpec to pure DataFrame transforms."""
    
    def __init__(self, spec: ProcessorSpec):
        self._spec = spec
        self.components = spec.requires
        self.priority = spec.priority
        self._instance = spec.processor_class()
    
    async def process(
        self,
        df: DataFrame,
        resources: Resources = None,
        tick: int = 0,
        **kwargs,
    ) -> DataFrame:
        """Execute as pure DataFrame transform."""
        
        # Apply filter if present
        if self._spec.filter_expr:
            arch = ArchetypeAccessor(self.components)
            filter_field = self._spec.filter_expr(arch)
            if filter_field:
                filter_expr = filter_field.to_expr() if hasattr(filter_field, 'to_expr') else filter_field
                df = df.where(filter_expr)
        
        # Get transform from instance
        arch = ArchetypeAccessor(self.components)
        
        # Call transform method
        if hasattr(self._instance, "transform"):
            dt = kwargs.get("dt", 1.0)
            updates = self._instance.transform(arch, tick=tick, dt=dt)
            
            if updates:
                # Apply updates as with_columns
                for col_name, expr in updates.items():
                    if hasattr(expr, 'to_expr'):
                        df = df.with_column(col_name, expr.to_expr())
                    else:
                        df = df.with_column(col_name, expr)
        
        return df


# =============================================================================
# Query API - Agent-Centric Reads
# =============================================================================


class AgentView:
    """
    Read-only view of an agent for queries.
    Does NOT support mutations - use processors for writes.
    """
    
    def __init__(self, entity_id: int, row_data: dict[str, Any], components: list[type[Component]]):
        self.entity_id = entity_id
        self._row_data = row_data
        self._components = components
        self._component_views = {}
        
        for cls in components:
            attr_name = self._to_snake_case(cls.__name__)
            self._component_views[attr_name] = ComponentView(cls, row_data)
    
    @staticmethod
    def _to_snake_case(name: str) -> str:
        import re
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    
    def __getattr__(self, name: str):
        if name in self._component_views:
            return self._component_views[name]
        raise AttributeError(f"Agent has no component '{name}'")


class ComponentView:
    """Read-only view of a component."""
    
    def __init__(self, component_cls: type[Component], row_data: dict[str, Any]):
        self._component_cls = component_cls
        self._row_data = row_data
        self._prefix = component_cls.get_prefix()
    
    def __getattr__(self, name: str) -> Any:
        col_name = self._prefix + name
        if col_name in self._row_data:
            value = self._row_data[col_name]
            # Auto-deserialize JSON
            if isinstance(value, str) and value.startswith(("[", "{")):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    pass
            return value
        raise AttributeError(f"{self._component_cls.__name__} has no field '{name}'")


# =============================================================================
# World V2 - DataFrame-First Simulation
# =============================================================================


class WorldV2:
    """
    DataFrame-first world that honors the core engine.
    
    Key differences from v1:
    - Behaviors compile to pure DataFrame transforms
    - Query API separate from behavior API
    - Archetypes are explicit
    - No collect-and-loop in hot path
    """
    
    def __init__(
        self,
        name: str,
        storage: str | Path = "./archetype_data_v2",
        cache: bool = True,
    ):
        self.name = name
        self._storage_path = Path(storage)
        self._use_cache = cache
        
        self._orchestrator: WorldOrchestrator | None = None
        self._inner_world: AsyncWorld | None = None
        self._system: AsyncSystem | None = None
        self._broker: CommandBroker | None = None
        self._processor_specs: list[ProcessorSpec] = []
    
    async def __aenter__(self) -> WorldV2:
        import shutil
        
        # Clean previous run
        if self._storage_path.exists():
            shutil.rmtree(self._storage_path)
        
        self._orchestrator = WorldOrchestrator()
        self._broker = CommandBroker()
        self._system = AsyncSystem()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._orchestrator:
            await self._orchestrator.shutdown()
    
    def register(self, spec: ProcessorSpec) -> None:
        """Register a processor spec."""
        self._processor_specs.append(spec)
    
    async def spawn(self, *components: Component) -> int:
        """Spawn entity with components."""
        if self._inner_world is None:
            await self._ensure_world_created()
        
        entity_id = await self._inner_world.create_entity(list(components))
        return entity_id
    
    async def run(self, ticks: int = 1, dt: float = 1.0) -> None:
        """Run simulation."""
        if self._inner_world is None:
            await self._ensure_world_created()
        
        # Register compiled processors
        for spec in self._processor_specs:
            processor = DataFrameProcessor(spec)
            await self._system.add_processor(processor)
        
        run_config = RunConfig.dev(steps=ticks, prefer_live_reads=True)
        await self._inner_world.run(run_config, dt=dt)
    
    def query(self, *component_types: type[Component]) -> list[AgentView]:
        """
        Query agents with given components.
        Returns read-only views - use processors to modify state.
        """
        if self._inner_world is None:
            return []
        
        # Find matching archetype
        sig = tuple(sorted(component_types, key=lambda c: c.__name__))
        
        agents = []
        for archetype_sig, df in self._inner_world._live.items():
            # Check if this archetype contains all required components
            if all(comp in archetype_sig for comp in component_types):
                rows = df.collect().to_pylist()
                for row in rows:
                    agent = AgentView(
                        entity_id=row.get("entity_id"),
                        row_data=row,
                        components=list(archetype_sig),
                    )
                    agents.append(agent)
        
        return agents
    
    async def _ensure_world_created(self) -> None:
        if self._inner_world is not None:
            return
        
        storage_config = StorageConfig(
            uri=self._storage_path,
            namespace=f"{self.name}_archetypes",
            backend=StorageBackend.ICEBERG,
        )
        
        cache_config = CacheConfig() if self._use_cache else None
        
        self._inner_world = await self._orchestrator.create_world(
            config=WorldConfig(name=self.name),
            system=self._system,
            storage_config=storage_config,
            cache_config=cache_config,
        )
        
        self._inner_world.resources.insert(self._broker)
    
    @property
    def resources(self) -> Resources:
        """Access world resources."""
        return self._inner_world.resources if self._inner_world else Resources()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "WorldV2",
    "processor",
    "ProcessorSpec",
    "Field",
    "ArchetypeAccessor",
    "AgentView",
    "ComponentView",
]
