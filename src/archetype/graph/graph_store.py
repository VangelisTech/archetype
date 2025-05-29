# Standard Python Libraries
from itertools import count as _count
from typing import Dict, Tuple, List, Type, Optional, Any, AsyncGenerator
from logging import getLogger
from hashlib import blake2b
import ulid 
from datetime import datetime, timezone
from functools import lru_cache
# Technologies
import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Catalog, Table
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa    
from lancedb.pydantic import LanceModel
import time
import networkx as nx 
# Internals
from .interfaces import Component, iStore




# Internals
from archetype.core.interfaces import Component, iStore, iAsyncStore

from archetype.core.store import (
    AsyncArchetypeStore,
    BaseArchetypeTableSchema, 
    PARTITION_KEYS,
    get_datetime_str,
    get_component_prefix, 
    sig_from_components, 
    sig2hash, 
    convert_component_to_pyarrow_schema
)
from .components import GraphNodeComponent, GraphEdgeComponent, NodeID

BaseRelationTableSchema = pa.schema([
    pa.field("simulation", pa.string(), nullable=False),
    pa.field("run", pa.string(), nullable=False),
    pa.field("edge_id", pa.string(), nullable=False),
    pa.field("source_node_id", pa.uint64(), nullable=False),
    pa.field("target_node_id", pa.uint64(), nullable=False),
    pa.field("step", pa.uint64(), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
    pa.field("relation_type", pa.string(), nullable=False),
])
logger = getLogger(__name__)
    
class GraphStore(AsyncArchetypeStore, iAsyncStore):
    """
    The GraphStore manages both entity-component data (via AsyncArchetypeStore)
    and graph relationship data (nodes and edges).
    Nodes are entities. Edges are stored in dedicated Daft tables.
    NetworkX can be used for in-memory operations on graph subsets.
    """
    def __init__(self, 
        uri: str,
        simulation: Optional[str] = None, 
        run: Optional[str] = None,
        entity_namespace: Optional[str] = "archetypes", 
        edge_namespace: Optional[str] = "graph_edges",
        catalog: Optional[Catalog] = None,
        debug: bool = False,
        max_concurrent_requests: int = 10
    ):
        # Initialize parent AsyncArchetypeStore for entity/component data
        super().__init__(
            uri=uri, 
            simulation=simulation, 
            run=run, 
            namespace=entity_namespace, 
            catalog=catalog, 
            debug=debug,
            max_concurrent_requests=max_concurrent_requests
        )
        
        self.edge_namespace = edge_namespace
        self.sess.create_namespace_if_not_exists(self.edge_namespace)
        # Note: self.sess.set_namespace() is already called by parent for entity_namespace.
        # Operations on edge tables will need to qualify the namespace or set it temporarily.

        # Cache for new edges before materialization
        # Key: (relation_type), Value: List of edge data dicts
        self._edge_spawn_cache: Dict[str, List[Dict[str, Any]]] = {}

        # NetworkX graph for LOGICAL operations or small graph analytics in memory.
        # The full, persisted graph lives in Daft tables.
        self.logical_graph = nx.MultiDiGraph() # Use MultiDiGraph for multiple edge types and parallel edges

    # --- Edge Helper Methods ---
    def _get_edge_table_name(self, relation_type: str) -> str:
        # Edges of a specific relation_type could be stored in a table named after that type.
        # Or all edges could go into a single table partitioned by relation_type.
        # For simplicity, let's use one table per relation_type for now.
        return f"edges_{relation_type.lower()}"

    def _get_edge_pyarrow_schema(self, edge_component_type: Type[GraphEdgeComponent]) -> pa.Schema:
        # Base schema for all edge tables
        base_edge_schema = BaseRelationTableSchema
        # Specific fields from the GraphEdgeComponent (or its subclasses)
        # This assumes GraphEdgeComponent holds all dynamic edge data.
        # If different edge types have different components, this needs more logic.
        edge_fields_schema = convert_component_to_pyarrow_schema(edge_component_type)
        
        # Filter out common fields already in BaseRelationTableSchema to avoid duplication
        # This is a bit manual; a more robust way would be to check field names.
        # Fields to exclude: edge_id, source_node_id, target_node_id, relation_type (if derived)
        # For now, let's assume GraphEdgeComponent fields are prefixed.
        # And that core relation fields are handled by BaseRelationTableSchema directly.
        prefix = get_component_prefix(edge_component_type)
        
        # Rename the fields of the component schema with the prefix
        renamed_fields = []
        for i, field_name in enumerate(edge_fields_schema.names):
            # Skip fields that would clash with BaseRelationTableSchema or are managed differently
            if field_name in ["edge_id", "source_node_id", "target_node_id", "relation_type", "step", "is_active"]:
                continue
            field = edge_fields_schema.field(field_name)
            renamed_fields.append(field.with_name(prefix + field_name))
        
        if renamed_fields:
            specific_edge_schema = pa.schema(renamed_fields)
            return pa.unify_schemas([base_edge_schema, specific_edge_schema])
        return base_edge_schema

    async def _ensure_edge_table(self, relation_type: str, edge_component_type: Type[GraphEdgeComponent]) -> Table:
        table_name = self._get_edge_table_name(relation_type)
        pyarrow_schema = self._get_edge_pyarrow_schema(edge_component_type)
        daft_schema = Schema.from_pyarrow_schema(pyarrow_schema)
        
        # Temporarily set namespace for edge table creation
        current_ns = self.sess.get_namespace()
        self.sess.set_namespace(self.edge_namespace)
        try:
            table = self.sess.create_table_if_not_exists(table_name, source=daft_schema)
            logger.info(f"Created/Ensured Daft edge table {self.edge_namespace}.{table_name} with schema: {daft_schema}")
        except Exception as e:
            logger.error(f"Error creating Daft edge table {self.edge_namespace}.{table_name}: {e}")
            raise
        finally:
            self.sess.set_namespace(current_ns) # Reset to original namespace
        return table

    def _new_edge_row_dict(self, 
                           edge_id: str,
                           source_id: NodeID, 
                           target_id: NodeID, 
                           relation_type: str, 
                           step: int, 
                           edge_components: List[GraphEdgeComponent]
                           ) -> Dict[str, Any]:
        row_dict = {
            "simulation": self.simulation,
            "run": self.run,
            "edge_id": edge_id,
            "source_node_id": source_id,
            "target_node_id": target_id,
            "step": step,
            "is_active": True,
            "relation_type": relation_type
        }
        # Assuming only one GraphEdgeComponent instance in edge_components for simplicity now
        # Or that all components in the list are of the same type (the primary edge_component_type)
        for c in edge_components:
            if isinstance(c, GraphEdgeComponent):
                 # Exclude fields already in row_dict from the component's model_dump
                comp_dict = c.model_dump(exclude={"edge_id", "source_node_id", "target_node_id", "relation_type"})
                prefix = get_component_prefix(type(c))
                row_dict.update({prefix + key: value for key, value in comp_dict.items()})
            # else: handle other component types if edges can have non-GraphEdgeComponents
        return row_dict

    # --- Public Graph Methods ---
    async def async_add_edge(
        self, 
        source_id: NodeID, 
        target_id: NodeID, 
        relation_type: str, 
        edge_components: List[GraphEdgeComponent], # Expecting GraphEdgeComponent or its subclasses
        step: int = 0
    ):
        """Add an edge to the spawn cache. Edges are materialized via materialize_spawns."""
        # For now, assume step 0 for initial graph setup
        assert step == 0, "GraphStore does not currently support adding edges in-situ during simulation steps."
        assert len(edge_components) > 0 and isinstance(edge_components[0], GraphEdgeComponent), \
            "Must provide at least one GraphEdgeComponent for an edge."

        edge_id = str(ulid.ULID()) # Generate unique edge ID
        primary_edge_component_type = type(edge_components[0])

        row_dict = self._new_edge_row_dict(
            edge_id, source_id, target_id, relation_type, step, edge_components
        )

        if relation_type not in self._edge_spawn_cache:
            self._edge_spawn_cache[relation_type] = []
        self._edge_spawn_cache[relation_type].append(row_dict)

        # Update logical graph (optional, for in-memory queries if needed during setup)
        # Consider if self.logical_graph should store full component data or just IDs/types/weights.
        # For now, just basic info.
        edge_attrs = edge_components[0].model_dump() if edge_components else {}
        self.logical_graph.add_edge(source_id, target_id, key=edge_id, relation_type=relation_type, **edge_attrs)
        
        return edge_id

    async def async_materialize_graph_spawns(self) -> None:
        """Write the edge spawn cache to the respective Daft tables."""
        current_ns = self.sess.get_namespace()
        self.sess.set_namespace(self.edge_namespace)
        try:
            for relation_type, rows in self._edge_spawn_cache.items():
                if not rows:
                    continue
                
                # Infer the primary GraphEdgeComponent type from the first row's components
                # This is a simplification; robust version would ensure all components match or handle mixed types.
                # For now, assume all edges of a given relation_type use the same GraphEdgeComponent subclass.
                # This requires that edge_components were stored in row_dict or accessible.
                # Let's assume GraphEdgeComponent as default if not specified elsewhere.
                primary_edge_component_type = GraphEdgeComponent 
                # A better way: pass the component type when adding to cache, or inspect first row.
                # For this example, let's assume it was GraphEdgeComponent

                pyarrow_schema = self._get_edge_pyarrow_schema(primary_edge_component_type)
                arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
                df = daft.from_arrow(arrow_table)

                table = await self._ensure_edge_table(relation_type, primary_edge_component_type)
                try:
                    # Temporarily switch session to edge_namespace for append
                    # This is already done at the start of the method block
                    table.append(df)
                    logger.debug(f"Appended {len(rows)} edges to table {self.edge_namespace}.{table.name}")
                except Exception as e:
                    logger.error(f"Error appending {len(rows)} edges to {self.edge_namespace}.{table.name}: {e}")
                    raise
        finally:
            self.sess.set_namespace(current_ns) # Reset namespace

        self._edge_spawn_cache.clear()
    
    # Override materialize_spawns to include graph spawns
    async def materialize_spawns(self) -> None:
        await super().materialize_spawns() # Materialize entity-component data
        await self.async_materialize_graph_spawns() # Materialize graph edge data

    async def async_get_edges_stream(
        self, 
        relation_type: str,
        edge_component_type: Type[GraphEdgeComponent] = GraphEdgeComponent,
        filters: Optional[Dict[str, Any]] = None # e.g., {"source_node_id": 123, "is_active": True}
    ) -> AsyncGenerator[DataFrame, None]:
        """Stream DataFrames for edges of a given relation_type, with optional filters."""
        table_name = self._get_edge_table_name(relation_type)
        current_ns = self.sess.get_namespace()
        self.sess.set_namespace(self.edge_namespace)
        try:
            table = self.sess.get_table(table_name) # This should be an async call if catalog is async
                                                 # For Daft, get_table is sync for now.
            df = table.to_dataframe()
            
            # Apply base simulation/run filters
            df = df.where((col("simulation") == self.simulation) & (col("run") == self.run))

            if filters:
                for key, value in filters.items():
                    if isinstance(value, list):
                        df = df.where(col(key).is_in(value))
                    else:
                        df = df.where(col(key) == value)
            yield df # Yields a single DataFrame for the entire edge table (filtered)
                     # True streaming would involve chunking this if it's very large.
        except Exception as e:
            logger.error(f"Error getting edges from {self.edge_namespace}.{table_name}: {e}")
            # yield or raise based on desired error handling
        finally:
            self.sess.set_namespace(current_ns)

    # Further methods: async_remove_edge, async_update_edge_components, graph query methods (get_neighbors, etc.)
    # These graph query methods would use async_get_edges_stream and then potentially NetworkX for logic.


