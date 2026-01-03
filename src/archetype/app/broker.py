# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Command Broker: Universal Simulation Interface
==============================================

The CommandBroker is the critical mediator between external APIs and internal worlds.
It enables recursive/hierarchical simulation where agents can spawn and run their own
simulations (mental models, MCTS, counterfactual reasoning).

Architecture:
    FastAPI/MCP → CommandService → CommandBroker → WorldOrchestrator → AsyncWorld

    Agents inside worlds can also submit commands:
    Agent → Broker.enqueue(CREATE_WORLD) → Child simulation spawned
    Agent → Broker.enqueue(RUN_ROLLOUT) → Mental simulation executed
    Agent → Broker.enqueue(QUERY_WORLD) → Results returned

Features:
- Priority queue per world (heapq-based)
- Async-safe with locks
- Pending/history tracking for audit
- Supports entity, processor, and simulation-level commands
- Debug logging for tracing command flow
"""

import asyncio
import heapq
import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from archetype.app.models import Command

logger = logging.getLogger(__name__)


class CommandBroker:
    """
    Universal simulation interface for command-based interaction with worlds.

    The broker mediates all external and agent-initiated commands:
    - Entity mutations (spawn, despawn, components)
    - Processor mutations (hot-swap behavior)
    - Simulation operations (create/destroy worlds, run rollouts)

    Auth is handled at the API layer before commands reach the broker.
    """

    def __init__(self, max_dequeue: int = 50_000, debug: bool = False):
        self._queues: dict[str, list[Command]] = {}  # world_id -> priority queue
        self._pending: dict[UUID, Command] = {}
        self._history: dict[str, list[Command]] = {}
        self._lock = asyncio.Lock()
        self._max_dequeue = max_dequeue
        self._debug = debug

        # Optional command handlers for simulation-level operations
        self._handlers: dict[str, Callable[[Command], Awaitable]] = {}

    def register_handler(self, command_type: str, handler: Callable[[Command], Awaitable]):
        """Register a handler for simulation-level commands (CREATE_WORLD, RUN_ROLLOUT, etc.)."""
        self._handlers[command_type] = handler

    async def enqueue(self, world_id: str | UUID, cmd: Command):
        """Enqueue a single command for a specific world."""
        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                self._queues[key] = []

            heapq.heappush(self._queues[key], cmd)
            self._pending[cmd.id] = cmd
            self._history.setdefault(key, []).append(cmd)

            if self._debug:
                logger.debug(
                    f"[broker] enqueue: world={key}, type={cmd.type.value}, "
                    f"tick={cmd.tick}, pending={len(self._queues[key])}"
                )

    async def enqueue_bulk(self, world_id: str | UUID, cmds: list[Command]):
        """Enqueue multiple commands for a specific world."""
        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                self._queues[key] = []

            for cmd in cmds:
                heapq.heappush(self._queues[key], cmd)
                self._pending[cmd.id] = cmd
                self._history.setdefault(key, []).append(cmd)

    async def dequeue(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]:
        """Dequeue commands for a specific world."""
        max_items = min(max_items or self._max_dequeue, self._max_dequeue)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues or not self._queues[key]:
                if self._debug:
                    logger.debug(f"[broker] dequeue: world={key}, returned=0, remaining=0")
                return []

            commands = []
            queue = self._queues[key]

            for _ in range(min(max_items, len(queue))):
                if queue:
                    cmd = heapq.heappop(queue)
                    commands.append(cmd)
                    # Remove from pending
                    self._pending.pop(cmd.id, None)

            if self._debug:
                # Group by type for summary
                type_counts = {}
                for cmd in commands:
                    type_counts[cmd.type.value] = type_counts.get(cmd.type.value, 0) + 1
                logger.debug(
                    f"[broker] dequeue: world={key}, returned={len(commands)}, "
                    f"remaining={len(queue)}, types={type_counts}"
                )

            return commands

    # Back-compat alias used by CommandService
    async def dequeue_batch(
        self, world_id: str | UUID, max_items: int | None = None
    ) -> list[Command]:
        return await self.dequeue(world_id, max_items)

    async def peek(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]:
        """Peek at commands without removing them."""
        max_items = min(max_items or self._max_dequeue, self._max_dequeue)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                return []

            # Return sorted copy without modifying queue
            queue = self._queues[key]
            return sorted(queue)[:max_items]

    async def get_pending_count(self, world_id: str | UUID | None = None) -> int:
        """Get count of pending commands."""
        async with self._lock:
            if world_id:
                return len(self._queues.get(str(world_id), []))
            return len(self._pending)

    async def get_history(self, world_id: str | UUID, limit: int = 100) -> list[Command]:
        """Return recent enqueued commands for a world (most recent last)."""
        async with self._lock:
            items = self._history.get(str(world_id), [])
            if not items:
                return []
            return items[-limit:]

    async def clear(self, world_id: str | UUID | None = None):
        """Clear pending commands."""
        async with self._lock:
            if world_id:
                key = str(world_id)
                queue = self._queues.pop(key, [])
                for cmd in queue:
                    self._pending.pop(cmd.id, None)
                self._history.pop(key, None)
            else:
                self._queues.clear()
                self._pending.clear()
                self._history.clear()
