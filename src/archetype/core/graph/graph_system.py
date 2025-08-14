# graph/system.py

import asyncio
import igraph as ig
from collections import defaultdict
from typing import List, Dict, Any, Set
import daft

from .graph_processor import GraphProcessor, UpdateStage

class GraphSystem:
    def __init__(self):
        self._processors: List[GraphProcessor] = []

    def add_processor(self, proc: GraphProcessor):
        self._processors.append(proc)

    async def execute(
        self,
        df: daft.DataFrame,
        world_state: Dict[str, Any] = None,
        **input_kwargs
    ) -> daft.DataFrame:
        """
        Executes a full tick by running processors in stages according to a
        dynamically inferred dependency graph.
        """
        if world_state is None:
            world_state = {}

        # Loop through stages in a fixed order, creating a synchronization barrier
        for stage in [UpdateStage.PreUpdate, UpdateStage.Update, UpdateStage.PostUpdate]:
            
            # 1. Filter processors by stage and run conditions
            active_procs = [
                p for p in self._processors
                if p.stage == stage and p.run_condition(world_state)
            ]

            if not active_procs:
                continue

            # 2. Automatically infer dependencies and build the graph
            g = self._build_dependency_graph(active_procs)
            
            # Check for cycles, which indicates a logical error in dependencies
            if not g.is_dag():
                raise RuntimeError(f"Circular dependency detected in stage {stage.name}!")

            # 3. Execute the graph in parallel stages
            df = await self._execute_graph(g, df, **input_kwargs)

        return df

    def _build_dependency_graph(self, processors: List[GraphProcessor]) -> ig.Graph:
        """Builds a dependency graph based on read/write conflicts."""
        g = ig.Graph(directed=True)
        g.add_vertices(len(processors))
        g.vs["processor"] = processors

        proc_to_vid = {p: i for i, p in enumerate(processors)}

        for i, p1 in enumerate(processors):
            for j, p2 in enumerate(processors):
                if i == j:
                    continue

                # Check for conflicts to create a dependency edge p1 -> p2
                p1_writes = set(p1.writes)
                p2_reads = set(p2.reads)
                p2_writes = set(p2.writes)

                # Conflict if p1 writes to something p2 reads OR writes
                if not p1_writes.isdisjoint(p2_reads) or not p1_writes.isdisjoint(p2_writes):
                    g.add_edge(proc_to_vid[p1], proc_to_vid[p2])

        return g

    async def _execute_graph(self, g: ig.Graph, df: daft.DataFrame, **input_kwargs) -> daft.DataFrame:
        """Executes a DAG of processors using Kahn's algorithm."""
        in_degrees = g.degree(mode='in')
        # enumerate returns (index, degree); pick indices with zero in-degree
        ready_vids = [i for i, d in enumerate(in_degrees) if d == 0]

        while ready_vids:
            ready_procs = [g.vs[vid]["processor"] for vid in ready_vids]
            
            # Execute all ready processors in parallel
            tasks = [proc.process(df, **input_kwargs) for proc in ready_procs]
            
            # Gather results from this parallel stage
            stage_results = await asyncio.gather(*tasks)
            
            # In a real system, you would have a sophisticated strategy
            # to merge the resulting dataframes. For this example, we'll
            # just take the first result to chain the computation.
            if stage_results:
                df = stage_results[0]

            # Find the next set of processors to run
            next_ready_vids = []
            for vid in ready_vids:
                for dependent_vid in g.neighbors(vid, mode='out'):
                    in_degrees[dependent_vid] -= 1
                    if in_degrees[dependent_vid] == 0:
                        next_ready_vids.append(dependent_vid)
            
            ready_vids = next_ready_vids
            
        return df