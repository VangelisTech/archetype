"""Archetype Graph Module.

This module provides stores, systems, and components for graph-based 
simulations and knowledge representation within the Archetype framework.
"""

from .components import (
    NodeID,
    GraphNodeComponent,
    GraphEdgeComponent,
    KnowledgeGraphComponent,
    SpacyDocComponent
)
from .graph_store import GraphStore
from .async_processor import AsyncGraphProcessor, async_graph_processor
from .graph_system import AsyncGraphSystem
# from .graph_world import GraphWorld # To be created when GraphWorld is defined

__all__ = [
    # Components
    "NodeID",
    "GraphNodeComponent",
    "GraphEdgeComponent",
    "KnowledgeGraphComponent",
    "SpacyDocComponent",
    
    # Store
    "GraphStore",
    
    # Processors
    "AsyncGraphProcessor",
    "async_graph_processor",
    
    # System
    "AsyncGraphSystem",
    
    # World (to be added)
    # "GraphWorld",
]

class GraphStore(iStore, Protocol):
    "GraphStore is a physical topology of the entities and relations between them"
    def add_entity(self, components: List[Component], step: int = 0) -> int:...
    def add_relation(self, entity_id1: int, entity_id2: int, relation_type: str, step: int = 0) -> None:...


class GraphSystem(iSystem, Protocol):
    "GraphSystem is a logical topology of processors and the edges between them in a directed acyclic graph"
    def add_processor(self, proc: Processor):...
    def add_edge(self, proc1: Processor, proc2: Processor):...
    def remove_processor(self, proc: Type[Processor]):...
    def remove_edge(self, edge: Tuple[Processor, Processor]):...
    def execute(self, querier: iQuerier, step: int, dt: float) -> Dict[str, DataFrame]:...