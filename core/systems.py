# -*- coding: utf-8 -*-
"""
Defines System implementations for orchestrating processor execution,
including the RayDagSystem for distributed, compiled execution.

Explanation and Key Points:
_SnapshotQueryInterface & _CapturingUpdateManager: These proxies are essential for isolating the remote processors. The querier reads from a static snapshot, and the updater captures outputs locally within the actor.
_RemoteProcessorWrapper: This Ray actor holds an instance of a user-defined Processor. Its process_wrapper method sets up the proxies and calls the real processor.process. It handles both sync and async processor methods. Crucially, it returns the captured updates.
RayDagSystem.__init__: Initializes Ray if needed and prepares internal processor storage.
add/remove_processor: Manages the list of processors and invalidates the compiled DAG whenever the processor list changes.
_build_and_compile_dag:
Creates the _RemoteProcessorWrapper actor handles for each processor. (TODO: Need to handle resource requests like GPUs here).
Uses ray.dag.InputNode and binds the process_wrapper method of the remote actors. Assumes a relatively flat DAG for now.
Creates a MultiOutputNode to gather results.
Calls cursor. dag.experimental_compile with recommended flags.
execute:
Snapshotting: Iterates through known component types, gets the committed DataFrame from the querier, and crucially .collect()s it to create a serializable snapshot. This is a key step but potentially expensive. Schemas are also collected.
DAG Execution: Prepares input for the DAG (snapshot, schemas, dt, args) and calls compiled_dag.execute_async. (TODO: Refine arg/kwarg passing).
Result Gathering: Uses ray.get() to wait for the DAG future to complete. ray.get can resolve awaitables from execute_async.
Update Queuing: Iterates through the results (list of dictionaries) from the DAG and uses the real updater (passed into execute) to queue the captured updates for the main commit phase.
Serialization: We are sending collected Daft DataFrames in the snapshot. This relies on Daft/Ray's ability to serialize Arrow data efficiently. Large states could still be a bottleneck.
Async: Uses async def for the wrapper and execute_async for the DAG, aligning with modern Ray practices.
"""

import ray
import time
import asyncio
from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from collections import defaultdict
import pyarrow as pa 
import networkx as nx

import daft
from daft import DataFrame # Use specific import for clarity

# Import from our new structure
from .base import Component, EntityType, Processor, System, _C
from .store import ComponentStore
from .managers import EcsQueryInterface, EcsUpdateManager


# --- Sequential System Implementation --
class SequentialSystem(System):
    """
    Executes processors sequentially.
    """
    def __init__(self, world: World):
        self._world = world
        self._processors: Dict[Type[Processor], Processor] = {}
        

    def add_processor(self, processor: Processor) -> None:
        """Adds a processor instance."""
        
        ptype = type(processor)
        if ptype in self._processors:
            print(f"SequentialSystem Warning: Replacing existing processor of type {ptype.__name__}")
        
        self._processors[ptype] = processor

        if self._world.verbose:
            print(f"SequentialSystem: Added processor {ptype.__name__}.")


    def remove_processor(self, processor_type: Type[Processor]) -> None:
        """Removes all processors of a specific type."""
        if processor_type in self._processors:
            del self._processors[processor_type]
            if self._world.verbose:
                print(f"SequentialSystem: Removed processor {processor_type.__name__}.")
        else:
                print(f"SequentialSystem Warning: Processor type {processor_type.__name__} not found.")

    def get_processor(self, processor_type: Type[Processor]) -> Optional[Processor]:
        """Gets the managed instance of a specific processor type."""
        return self._processors.get(processor_type)

    def execute(self, dt: float, *args: Any, **kwargs: Any) -> None:
        
        for processor in self._processors.values():
            processor.process(dt, *args, **kwargs)
        
        self._world._updater.collect(step=self._world._current_step)


                
class GraphSystem(System):
    """
    Executes processors in parallel using a graph structure. 

    Processors are networkX nodes, and edges are communication channels and relationships. 

    State is maintained within components which 
    """
    def __init__(self, world: World):
        self._world = world
        
        self._graph = nx.DiGraph()
        
        self._nodes: Dict[Type[Processor], Processor] = {}
        self._edges: Dict[Type[Processor], List[Type[Processor]]] = {}


    def add_node(self, processor: Processor) -> None:
        """Adds an entity state processor"""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")
        
        self._graph.add_node(processor)

    def add_nodes(self, processor: Processor, priority: Optional[int] = None) -> None:
        """Adds a processor instance."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")
        
    def remove_node(self, processor: Processor) -> None:
        """Removes a processor instance."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only remove Processor instances.")
        
        self._graph.remove_node(processor)

    def 

    
        
    def add_edges(self, processor: Processor, dependencies: List[Type[Processor]]) -> None:
        """Adds a processor instance."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")
        

    def get_subgraph(self, processors: List[Type[Processor]]) -> nx.DiGraph:
        """Returns a subgraph of the current graph."""
        return self._graph.subgraph(processors)
    
    def get_node(self, entity_id: int) -> Processor:

        return self._nodes[]
    
    def get_neighbors(self, entity_id: int) -> List[Processor]:
        return self._edges[entity_id]
    
    def execute_processors(self):
        pass

    def execute_connectivity_function(self):
        pass

    
    
        
        
