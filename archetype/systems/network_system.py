import networkx as nx
import daft
from daft import DataFrame
from typing import List, Dict, Any, Optional

from core.base import Component
from archetype.core.processor import Processor  # for type alias only
from components.network import Inbox, Outbox
from ..core.system import System  # reuse sequential System implementation

class NetworkSystem(System):
    """A System that routes packets between entities based on a NetworkX graph.

    Each node in the directed graph is an *entity_id* (``int``). An edge
    ``u -> v`` means: *v subscribes to packets published by u*.

    Workflow per simulation step:
    1. Read every ``Outbox`` component that is active for the current step.
    2. For every packet in the publisher's outbox, create an ``Inbox`` update
       for **all direct successors** in the graph.
    3. Return a Daft ``DataFrame`` containing the rows to be merged by the
       ``UpdateManager`` (``entity_id`` + ``inbox__packets``). If no messages
       are in-flight, returns ``None``.
    4. Optionally clear the publisher's outbox by setting it inactive, so the
       packets are not re-broadcast next step.
    """

    def __init__(self, world, graph: Optional[nx.DiGraph] = None):
        super().__init__()
        self.world = world
        self.graph: nx.DiGraph = graph.copy() if graph else nx.DiGraph()

    # ------------------------------------------------------------------
    #  Public API for mutating the graph at run-time
    # ------------------------------------------------------------------
    def add_subscription(self, publisher: int, subscriber: int) -> None:
        """Create an edge *publisher -> subscriber* (id form)."""
        self.graph.add_edge(publisher, subscriber)

    def remove_subscription(self, publisher: int, subscriber: int) -> None:
        """Remove an existing subscription edge, if present."""
        if self.graph.has_edge(publisher, subscriber):
            self.graph.remove_edge(publisher, subscriber)

    def clear_subscriptions_for(self, entity_id: int) -> None:
        """Delete all in- and out-edges for *entity_id*. Useful when despawning."""
        self.graph.remove_nodes_from([entity_id])

    # ------------------------------------------------------------------
    #  System execution – called once per simulation step by the World
    # ------------------------------------------------------------------
    def execute(self, dt: float = 0.0) -> DataFrame | None:  # dt kept for API parity
        # 1) Gather all Outbox components that are still active at this step.
        step = self.world.current_step  # convenience alias
        out_df = self.world.store.get_components(Outbox, steps=step)

        if out_df is None or len(out_df) == 0:
            return None  # nothing to route

        # Ensure standardised column names from get_components()
        # It returns columns: entity_id, step, is_active, packets
        # We rename packets -> out_packets to avoid clash
        out_df = out_df.rename_columns({"packets": "out_packets"})

        updates: List[Dict[str, Any]] = []
        removals: List[Dict[str, Any]] = []

        # 2) Iterate row-wise (smallish counts expected – not GPU bound)
        for row in out_df.to_pydict():
            publisher = int(row["entity_id"])
            packets   = row["out_packets"]
            if not packets:
                continue

            # a) dispatch to every subscriber
            if publisher in self.graph:
                for subscriber in self.graph.successors(publisher):
                    updates.append(
                        {
                            "entity_id": subscriber,
                            "inbox__packets": packets,
                        }
                    )

            # b) clear the publisher's Outbox by marking it inactive
            removals.append(
                {
                    "entity_id": publisher,
                    "outbox__packets": [],  # dummy field to satisfy schema
                    "is_active": False,
                }
            )

        if not updates and not removals:
            return None

        merged_df = None
        if updates:
            merged_df = daft.from_pylist(updates)
        if removals:
            rem_df = daft.from_pylist(removals)
            merged_df = rem_df if merged_df is None else merged_df.concat(rem_df)

        return merged_df 