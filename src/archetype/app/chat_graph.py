# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Chat Graph: DAG-based Conversation Threading with Channels
==========================================================

Each world has named channels. Each channel has its own conversation graph
(DAG of ChatNodes). Agents access channels via Resources to read context,
branch conversations, and navigate threads.

Architecture:
    Processor → resources.require(ChatGraphRegistry)
             → registry.channel(world_id, "strategy")
             → graph.active_path()  →  LLM context window

Channels:
    - "general" is the default. If you never specify a channel, everything
      goes there and it behaves like a flat list.
    - Each channel is a separate ChatGraph. No cross-contamination.
    - Agents can list channels, subscribe to specific ones, or browse.

Graph:
    - Nodes are Commands. Edges are parent→child.
    - Cursor tracks the active leaf. Auto-advances on append.
    - active_path() returns root→cursor: the linear context for LLM prompts.
    - branch() forks from any node without destroying the original path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from uuid import UUID

from archetype.app.models import Command

logger = logging.getLogger(__name__)

DEFAULT_CHANNEL = "general"


@dataclass
class ChatNode:
    """A single node in the conversation graph."""

    cmd: Command
    parent_id: UUID | None = None
    children: list[UUID] = field(default_factory=list)
    branch_label: str | None = None  # optional user-facing label


class ChatGraph:
    """
    DAG-based conversation graph for a single (world, channel) pair.

    Sensible defaults:
    - Linear mode: append() auto-parents to cursor. No config needed.
    - Branch mode: branch(parent_id, cmd) forks from any node.
    - Cursor: always points at the active leaf. Auto-advances on append.
    - Path: active_path() returns root→cursor for context assembly.
    """

    def __init__(self, world_id: str | UUID, channel: str = DEFAULT_CHANNEL):
        self.world_id = str(world_id)
        self.channel = channel
        self._nodes: dict[UUID, ChatNode] = {}
        self._roots: list[UUID] = []  # entry points (first messages)
        self._cursor: UUID | None = None  # active leaf

    @property
    def cursor(self) -> UUID | None:
        return self._cursor

    @property
    def size(self) -> int:
        return len(self._nodes)

    def get_node(self, node_id: UUID) -> ChatNode | None:
        return self._nodes.get(node_id)

    def append(self, cmd: Command) -> ChatNode:
        """
        Append a message to the graph, parented to the current cursor.
        If the graph is empty, this becomes a root node.
        Auto-advances the cursor.

        If cmd.parent_id is set, uses that instead of the cursor.
        """
        parent_id = cmd.parent_id or self._cursor
        node = ChatNode(cmd=cmd, parent_id=parent_id)

        self._nodes[cmd.id] = node

        if parent_id is None:
            self._roots.append(cmd.id)
        else:
            parent = self._nodes.get(parent_id)
            if parent is not None:
                parent.children.append(cmd.id)
            else:
                # Orphan — treat as root
                self._roots.append(cmd.id)
                node.parent_id = None

        self._cursor = cmd.id
        return node

    def branch(self, parent_id: UUID, cmd: Command, label: str | None = None) -> ChatNode:
        """
        Create a branch (alternative reply) from an existing node.
        Moves cursor to the new branch head.
        """
        parent = self._nodes.get(parent_id)
        if parent is None:
            raise KeyError(f"Parent node {parent_id} not found in graph")

        node = ChatNode(cmd=cmd, parent_id=parent_id, branch_label=label)
        self._nodes[cmd.id] = node
        parent.children.append(cmd.id)
        self._cursor = cmd.id
        return node

    def navigate(self, node_id: UUID) -> list[UUID]:
        """
        Move cursor to a specific node. Returns the path from root to that node.
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id} not found in graph")
        self._cursor = node_id
        return self._path_to(node_id)

    def active_path(self) -> list[Command]:
        """
        Return the linear command sequence from root → cursor.
        This is what you'd feed into a context window.
        """
        if self._cursor is None:
            return []
        ids = self._path_to(self._cursor)
        return [self._nodes[nid].cmd for nid in ids]

    def branches_at(self, node_id: UUID) -> list[ChatNode]:
        """List all children (branches) of a node."""
        node = self._nodes.get(node_id)
        if node is None:
            return []
        return [self._nodes[cid] for cid in node.children if cid in self._nodes]

    def leaves(self) -> list[UUID]:
        """Return all leaf node IDs (nodes with no children)."""
        return [nid for nid, node in self._nodes.items() if not node.children]

    def auto_nav_to_leaf(self) -> UUID | None:
        """
        Auto-navigate: from the current cursor, follow the rightmost (most recent)
        child at each level until reaching a leaf.
        """
        if self._cursor is None:
            if self._roots:
                self._cursor = self._roots[-1]
            else:
                return None

        current = self._cursor
        while True:
            node = self._nodes.get(current)
            if node is None or not node.children:
                break
            current = node.children[-1]  # rightmost = most recent

        self._cursor = current
        return current

    def _path_to(self, node_id: UUID) -> list[UUID]:
        """Walk parent pointers from node_id back to root, return root→node order."""
        path = []
        current: UUID | None = node_id
        seen: set[UUID] = set()
        while current is not None and current not in seen:
            seen.add(current)
            path.append(current)
            node = self._nodes.get(current)
            current = node.parent_id if node else None
        path.reverse()
        return path

    def subtree(self, node_id: UUID) -> list[UUID]:
        """BFS from node_id, return all descendant IDs (including node_id)."""
        from collections import deque

        if node_id not in self._nodes:
            return []
        result = []
        queue = deque([node_id])
        while queue:
            nid = queue.popleft()
            result.append(nid)
            node = self._nodes.get(nid)
            if node:
                queue.extend(node.children)
        return result

    def prune(self, node_id: UUID) -> int:
        """
        Remove a node and all its descendants. Returns count of removed nodes.
        Also removes the node from its parent's children list.
        """
        ids_to_remove = self.subtree(node_id)
        if not ids_to_remove:
            return 0

        # Unlink from parent
        node = self._nodes.get(node_id)
        if node and node.parent_id:
            parent = self._nodes.get(node.parent_id)
            if parent and node_id in parent.children:
                parent.children.remove(node_id)

        # Remove from roots
        if node_id in self._roots:
            self._roots.remove(node_id)

        for nid in ids_to_remove:
            self._nodes.pop(nid, None)

        # Reset cursor if it was in the pruned subtree
        if self._cursor in ids_to_remove:
            if node and node.parent_id and node.parent_id in self._nodes:
                self._cursor = node.parent_id
            elif self._roots:
                self._cursor = self._roots[-1]
            else:
                self._cursor = None

        return len(ids_to_remove)

    def to_dict(self) -> dict:
        """Serialize the graph for API responses."""
        return {
            "world_id": self.world_id,
            "channel": self.channel,
            "cursor": str(self._cursor) if self._cursor else None,
            "size": self.size,
            "roots": [str(r) for r in self._roots],
            "nodes": {
                str(nid): {
                    "cmd_id": str(node.cmd.id),
                    "cmd_type": node.cmd.type.value,
                    "parent_id": str(node.parent_id) if node.parent_id else None,
                    "children": [str(c) for c in node.children],
                    "branch_label": node.branch_label,
                    "tick": node.cmd.tick,
                }
                for nid, node in self._nodes.items()
            },
        }


class ChatGraphRegistry:
    """
    Manages ChatGraph instances per (world, channel) pair.
    Lazy-creates on first access.

    This is the object injected into Resources:
        world.resources.insert(registry)

    Processors access it via:
        registry = resources.require(ChatGraphRegistry)
        graph = registry.channel(world_id, "strategy")
        context = graph.active_path()
    """

    def __init__(self):
        self._graphs: dict[tuple[str, str], ChatGraph] = {}

    def channel(self, world_id: str | UUID, channel: str = DEFAULT_CHANNEL) -> ChatGraph:
        """Get or create a ChatGraph for a specific (world, channel)."""
        key = (str(world_id), channel)
        if key not in self._graphs:
            self._graphs[key] = ChatGraph(str(world_id), channel)
        return self._graphs[key]

    def get(self, world_id: str | UUID) -> ChatGraph:
        """Shorthand for channel(world_id, "general"). Backward-compatible."""
        return self.channel(world_id, DEFAULT_CHANNEL)

    def channels(self, world_id: str | UUID) -> list[str]:
        """List all channels that have messages for a given world."""
        wid = str(world_id)
        return [ch for (w, ch) in self._graphs if w == wid]

    def remove(self, world_id: str | UUID, channel: str | None = None) -> None:
        """Remove a specific channel graph, or all channels for a world."""
        wid = str(world_id)
        if channel is not None:
            self._graphs.pop((wid, channel), None)
        else:
            keys = [k for k in self._graphs if k[0] == wid]
            for k in keys:
                del self._graphs[k]

    def list_worlds(self) -> list[str]:
        """List all world IDs that have any channel graphs."""
        return list({w for w, _ in self._graphs})
