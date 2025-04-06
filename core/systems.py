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


from typing import Any, Dict, List, Type, Optional, TYPE_CHECKING
import networkx as nx

import daft

# Import from our new structure
from .base import Processor, System

# Conditionally import World for type checking only
if TYPE_CHECKING:
    from .world import World 


# --- Sequential System Implementation --
class SequentialSystem(System):
    """
    Executes processors sequentially.
    """
    def __init__(self, world: 'World'):
        self._world = world
        self._processors: Dict[Type[Processor], Processor] = {}
        

    def add_processor(self, processor: Processor) -> None:
        """Adds a processor instance."""
        
        ptype = type(processor)
        if ptype in self._processors:
            print(f"SequentialSystem Warning: Replacing existing processor of type {ptype.__name__}")
        
        self._processors[ptype] = processor

        if self._world._verbose:
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

    def execute(self, dt: float) -> None:
        
        for processor in self._processors.values():
            processor.process(dt)
        
        self._world._updater.collect_and_push_step(step=self._world._current_step)


                
class GraphSystem(System):
    """
    Executes processors in parallel using a graph structure. 

    Processors are networkX nodes, and edges are communication channels and relationships. 

    State is maintained within components which 
    """
    def __init__(self, world: 'World'):
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

    def add_edges(self, processor: Processor, dependencies: List[Type[Processor]]) -> None:
        """Adds edges representing dependencies between processors."""
        if processor not in self._graph:
            raise ValueError(f"Processor {processor} not in graph.")
        for dep in dependencies:
            # Assuming dependencies are processor *types* or instances already in the graph
            # You might need a way to resolve types to instances if they aren't already nodes
            if dep not in self._graph:
                 raise ValueError(f"Dependency {dep} not in graph.")
            self._graph.add_edge(dep, processor) # Edge from dependency to processor

    def get_subgraph(self, processors: List[Type[Processor]]) -> nx.DiGraph:
        """Returns a subgraph containing the specified processors."""
        # Ensure nodes exist before creating subgraph
        nodes_in_graph = [p for p in processors if p in self._graph]
        return self._graph.subgraph(nodes_in_graph)
    
    def get_node(self, processor_instance: Processor) -> Processor:
        """Gets the processor instance if it exists in the graph."""
        if processor_instance in self._graph:
            return processor_instance
        raise ValueError(f"Processor {processor_instance} not found in graph.")
    
    def get_neighbors(self, processor_instance: Processor) -> List[Processor]:
        """Gets the neighbors (predecessors and successors) of a processor."""
        if processor_instance not in self._graph:
            raise ValueError(f"Processor {processor_instance} not found in graph.")
        # networkx neighbors includes both predecessors and successors
        return list(self._graph.neighbors(processor_instance))
    
    def execute_processors(self):
        # Implementation depends on how you want to execute the graph
        # (e.g., topological sort for sequential execution of parallelizable steps)
        pass

    def execute_connectivity_function(self):
        pass

    
    
        
        
