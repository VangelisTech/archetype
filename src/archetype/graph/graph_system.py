from daft import DataFrame
from typing import List, Dict, Type, Tuple, AsyncGenerator, Set, Any
import asyncio
import networkx as nx

from daft import DataFrame, col

from archetype.core.base import BaseSystem, Component
# Import the new AsyncGraphProcessor (to be created) and Graph related components
from .async_processor import AsyncGraphProcessor 
from .components import GraphNodeComponent, GraphEdgeComponent 
from .graph_store import GraphStore # GraphStore will be our iAsyncStore for graph data
from archetype.core.interfaces import iAsyncQuerier, iAsyncStore


class AsyncGraphSystem(BaseSystem):
    """ System that processes entity-component and graph data streams using AsyncGraphProcessors. """
    def __init__(self, max_concurrent_processors: int = 10):
        self.processors: List[AsyncGraphProcessor] = []
        self.processor_semaphore = asyncio.Semaphore(max_concurrent_processors)
        self._tasks: Set[asyncio.Task] = set()

    def add_processor(self, proc: AsyncGraphProcessor):
        self.processors.append(proc)
        self.processors.sort(key=lambda p: p.priority)

    def remove_processor(self, proc_type: Type[AsyncGraphProcessor]):
        self.processors = [p for p in self.processors if not isinstance(p, proc_type)]
    
    async def _process_graph_stream_for_archetype(
        self, 
        archetype_name: str, # Name of the node archetype being processed
        node_df: DataFrame,    # DataFrame of nodes for this archetype
        querier: iAsyncQuerier, # To fetch edges related to these nodes
        graph_store: GraphStore, # To fetch full edge details if needed
        step: int, 
        dt: float
    ) -> Tuple[str, DataFrame, List[DataFrame]]: # Returns node DF and list of edge DFs
        """Process one node archetype and its related edges."""
        current_node_df = node_df
        processed_edge_dfs: Dict[str, DataFrame] = {} # Store processed edge DFs by relation_type

        for proc_instance in self.processors:
            async with self.processor_semaphore:
                if len(current_node_df) == 0 and not processed_edge_dfs: # Optimization
                    break
                
                if len(current_node_df) > 0:
                    current_node_df = await proc_instance.process_stream(archetype_name, current_node_df, dt)
                    if current_node_df is None or len(current_node_df) == 0:
                        current_node_df = DataFrame.from_pydict({}) # Empty
                        break 
                
        final_node_df = current_node_df
        if len(final_node_df) > 0 and self.processors:
             final_node_df = await self.processors[-1].postprocess_stream(final_node_df, step)

        return archetype_name, final_node_df, [] 

    async def execute_streaming(
        self, 
        querier: iAsyncQuerier, 
        store: GraphStore, 
        step: int, 
        dt: float
    ) -> None:
        if not self.processors:
            return

        pending_node_tasks: Dict[str, asyncio.Task] = {}

        async def node_task_done_callback(task: asyncio.Task, archetype_name: str):
            try:
                name, processed_node_df, _ = await task 
                
                if processed_node_df is not None and len(processed_node_df) > 0:
                    await store.async_append(name, processed_node_df) 
                elif processed_node_df is not None and len(processed_node_df) == 0:
                    await store.async_append(name, processed_node_df) 
            except Exception as e:
                print(f"Error processing node archetype {archetype_name}: {e}")
            finally:
                pending_node_tasks.pop(archetype_name, None)
                self._tasks.remove(task)
        
        node_components = self.get_system_node_components()

        async for archetype_name, df in querier(component_types=node_components, step=step):
            if archetype_name in pending_node_tasks: 
                continue
            
            task = asyncio.create_task(self._process_graph_stream_for_archetype(
                archetype_name, df, querier, store, step, dt
            ))
            self._tasks.add(task)
            pending_node_tasks[archetype_name] = task
            task.add_done_callback(lambda t, name=archetype_name: asyncio.create_task(node_task_done_callback(t, name)))
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def get_system_node_components(self) -> Tuple[Type[Component], ...]:
        all_components: Set[Type[Component]] = set()
        for proc in self.processors:
            if proc.components: 
                node_comps_for_proc = [c for c in proc.components if not issubclass(c, GraphEdgeComponent)]
                all_components.update(node_comps_for_proc)
        return tuple(all_components)

    async def shutdown(self):
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

