from daft import DataFrame, col, lit
from typing import Type, Tuple, AsyncGenerator

from archetype.core.base import Component
from archetype.core.processor import AsyncProcessor
from archetype.core.interfaces import iAsyncQuerier

from .components import GraphNodeComponent, GraphEdgeComponent
# from .graph_store import GraphStore # Avoid circular import if GraphStore might use AsyncGraphProcessor


# Decorator for AsyncGraphProcessors
def async_graph_processor(*component_types: Type[Component], priority: int = 0):
    """Class decorator for AsyncGraphProcessors."""
    def wrap(cls: Type["AsyncGraphProcessor"]):
        if not issubclass(cls, AsyncGraphProcessor):
            raise TypeError("Decorator @async_graph_processor can only be used with AsyncGraphProcessor subclasses.")
        cls.components = component_types # These are primarily NODE components this processor acts on
        cls.priority = priority
        return cls
    return wrap


class AsyncGraphProcessor(AsyncProcessor):
    """
    Base class for processors in an AsyncGraphSystem.
    They act on node archetypes and can query/modify graph edges.
    `process_stream` is for node DataFrame modifications.
    Graph operations use `querier` or `graph_store`.
    """

    async def get_connected_edges(
        self,
        node_df: DataFrame, 
        relation_type: str,
        querier: iAsyncQuerier, 
        direction: str = "out", 
        step: int = -1 
    ) -> AsyncGenerator[Tuple[str, DataFrame], None]:
        """
        Helper to get edge DataFrames connected to nodes in node_df.
        Yields (edge_table_name, edge_dataframe) tuples.
        """
        if len(node_df) == 0:
            async def empty_generator():
                if False:
                    yield
            return empty_generator()

        node_ids_col = node_df.get_column("entity_id")
        # Check if column is empty before trying to_pylist to avoid Daft error on empty column
        if not node_ids_col.has_null_dtype() and len(node_ids_col) == 0:
             async def empty_gen_for_empty_nodes():
                if False: yield
             return empty_gen_for_empty_nodes()
        node_ids = node_ids_col.to_pylist()
        
        filter_on_col_name = None
        if direction == "out":
            filter_on_col_name = "source_node_id"
        elif direction == "in":
            filter_on_col_name = "target_node_id"
        
        if filter_on_col_name:
            # This assumes the querier is an AsyncQueryManager that can handle GraphEdgeComponent queries
            # and has access to the GraphStore's edge tables.
            # The querier will stream (archetype_name_of_edge_table, full_edge_df_for_that_table)
            async for edge_table_name, edge_df in querier(
                component_types=(GraphEdgeComponent,),
                step=[step] 
            ):
                # Filter for the specific relation type and connected nodes
                # This filtering should ideally be pushed down to the Daft query plan for efficiency.
                # For now, filtering after materializing the edge_df from the stream.
                filtered_df = edge_df.where(col("relation_type") == relation_type)
                filtered_df = filtered_df.where(col(filter_on_col_name).is_in(node_ids))
                
                if len(filtered_df) > 0: # Check after filtering
                    yield edge_table_name, filtered_df
        else: # "both" directions or unhandled
            # Implement logic for "both" if needed, possibly two calls to this function
            # or a more complex query to the querier if it supports OR conditions on columns.
            async def empty_gen_both_dir():
                if False: yield
            return empty_gen_both_dir()

@async_graph_processor(components=(GraphNodeComponent,), priority=10)
class ExampleGraphNodeProcessor(AsyncGraphProcessor):
    async def process_stream(self, archetype_name: str, node_df: DataFrame, dt: float) -> DataFrame:
        print(f"Processing nodes from {archetype_name} in ExampleGraphNodeProcessor")
        if len(node_df) == 0:
            return node_df
        return node_df.with_column("graph_node_processed", lit(True))

@async_graph_processor(components=(GraphEdgeComponent,), priority=20)
class ExampleGraphEdgeProcessor(AsyncGraphProcessor):
    async def process_stream(self, archetype_name: str, edge_df: DataFrame, dt: float) -> DataFrame:
        print(f"Processing edges from {archetype_name} in ExampleGraphEdgeProcessor")
        if len(edge_df) == 0:
            return edge_df
        return edge_df.with_column("graph_edge_processed", lit(True))
