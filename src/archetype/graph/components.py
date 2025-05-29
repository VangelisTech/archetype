from pydantic import Field
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import ulid

from archetype.core.base import Component

# Using an alias for entity_id for clarity in graph context
NodeID = Union[int, str] # Entities can be ints, graph nodes might be strings

class GraphNodeComponent(Component):
    """Represents an entity that is a node in a graph."""
    node_id: NodeID = Field(description="Unique identifier for this node, often the entity_id.")
    node_type: Optional[str] = Field(default=None, description="Type of the node, e.g., 'agent', 'concept', 'location'.")
    # Properties could be generic or specific depending on node type
    properties: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary properties of the node.")

class GraphEdgeComponent(Component):
    """Represents a directed edge between two nodes (entities)."""
    edge_id: str = Field(description="Unique identifier for this edge.")
    source_node_id: NodeID
    target_node_id: NodeID
    edge_type: Optional[str] = Field(default="related_to", description="Type of the edge, e.g., 'connects_to', 'influences', 'owns'.")
    weight: float = Field(default=1.0, description="Weight of the edge, if applicable.")
    directed: bool = Field(default=True, description="Is the edge directed?")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary properties of the edge.")

class KnowledgeGraphComponent(Component):
    """
    A component that represents an entity's participation in or view of a knowledge graph.
    This could be attached to an agent to represent its internal knowledge,
    or to a global entity representing a shared knowledge base.
    """
    graph_name: str = Field(default="default_kg", description="Name of the KG this entity is part of/observing.")
    # Storing actual graph data (nodes/edges) directly in a component for many entities
    # might be inefficient for large graphs. Instead, this component might store:
    # - References to nodes/edges relevant to this entity (e.g., its local neighborhood)
    # - Queries or subscriptions to a larger GraphStore
    # - An embedding or summary of its knowledge
    # For simplicity now, let's assume it might hold a small, local graph view.
    local_nodes: List[NodeID] = Field(default_factory=list)
    local_edges: List[str] = Field(default_factory=list) # List of edge_ids

class SpacyDocComponent(Component):
    """
    Represents a spaCy Doc object, to be stored efficiently.
    spaCy Doc objects are not directly serializable to Arrow easily.
    We store key attributes that allow reconstruction or analysis.
    """
    text: str
    # Store tokens as a list of dictionaries or tuples for simplicity
    # (text, lemma_, pos_, tag_, dep_, shape_, is_alpha, is_stop)
    tokens_data: List[Dict[str, Any]] = Field(default_factory=list, description="Data for tokens (text, lemma_, pos_, etc.)")
    # Store entities as (text, start_char, end_char, label_)
    ents_data: List[Tuple[str, int, int, str]] = Field(default_factory=list, description="Data for entities (text, start, end, label)")
    # Store noun chunks as (text, start_char, end_char, root_text, root_dep_, root_head_text)
    noun_chunks_data: List[Dict[str, Any]] = Field(default_factory=list, description="Data for noun chunks")
    sents_data: List[str] = Field(default_factory=list, description="List of sentence texts")
    # vector: Optional[List[float]] = None # If storing document vectors; can be large

    # Add a method to reconstruct (partially) or work with this data,
    # or this component is primarily for UDFs that can process this structured data.
    # For true spaCy Doc reconstruction, one would need a spaCy pipeline. 

# --- New Agent & Communication Components ---

class LLMAgentState(Component):
    """Core state for an LLM-powered agent."""
    agent_id: NodeID 
    current_mode: str = Field(default="idle", description="Agent's current operational mode, e.g., idle, thinking, communicating.")
    persona_description: str = "A helpful AI assistant."
    # Link to its internal knowledge graph, if it has one
    knowledge_graph_name: Optional[str] = None 
    # LLM specific settings
    model_name: str = Field(default="gpt-4o-mini", description="LLM model to use for this agent.")
    temperature: float = Field(default=0.7)
    max_tokens: int = Field(default=1024)
    # Could include things like API keys or references to Ray Serve deployments if managed per agent

class MessageComponent(Component):
    """Represents a message exchanged between agents or systems."""
    message_id: str = Field(default_factory=lambda: str(ulid.ULID()), description="Unique message ID.")
    sender_id: NodeID
    recipient_id: NodeID # Can be an agent ID or a system/channel ID
    content_type: str = Field(default="text/plain", description="MIME type of the content.")
    content: Any # Could be text, structured data (Pydantic model), SpacyDocComponent, etc.
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Routing info, priority, TTL, etc.")

class AgentMailboxComponent(Component):
    """A component attached to an agent entity to hold incoming messages."""
    incoming_messages: List[MessageComponent] = Field(default_factory=list)
    max_size: int = 100 # Max messages to hold

class ChatHistoryComponent(Component):
    """Stores a sequence of messages representing a conversation thread."""
    thread_id: str = Field(default_factory=lambda: str(ulid.ULID()))
    participants: List[NodeID]
    messages: List[MessageComponent] = Field(default_factory=list)
    # This could be linked to a KnowledgeGraphComponent or SpacyDocComponents for semantic indexing
    summary: Optional[str] = None
    last_updated: str = Field(default_factory=lambda: datetime.utcnow().isoformat()) 