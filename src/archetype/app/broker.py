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
    FastAPI/MCP → CommandService → CommandBroker → WorldService → AsyncWorld

Features:
- Priority queue per world (heapq-based)
- RBAC guardrails via ActorCtx
- Async-safe with locks
- Pending/history tracking for audit
- Tick-aware dequeue (dequeue_due)
"""

import asyncio
import heapq
import logging
from uuid import UUID

from archetype.app.auth.guard import guardrail_allow
from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command

logger = logging.getLogger(__name__)


class CommandBroker:
    """
    Universal simulation interface for command-based interaction with worlds.

    The broker mediates all external and agent-initiated commands with RBAC enforcement.
    """

    def __init__(self, max_dequeue: int = 50_000, debug: bool = False):
        self._queues: dict[str, list[Command]] = {}  # world_id -> priority queue
        self._pending: dict[UUID, Command] = {}
        self._history: dict[str, list[Command]] = {}
        self._lock = asyncio.Lock()
        self._max_dequeue = max_dequeue
        self._debug = debug

    async def enqueue(
        self,
        world_id: str | UUID,
        cmd: Command,
        ctx: ActorCtx | None = None,
    ) -> None:
        """
        Enqueue a single command for a specific world.
        If ctx is provided, validates RBAC permissions and quotas.
        """
        if ctx is not None:
            guardrail_allow(cmd, ctx)

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

    async def enqueue_bulk(
        self,
        world_id: str | UUID,
        cmds: list[Command],
        ctx: ActorCtx | None = None,
    ) -> None:
        """
        Enqueue multiple commands for a specific world.
        All-or-nothing: validates all commands before enqueueing any.
        """
        if ctx is not None:
            for cmd in cmds:
                guardrail_allow(cmd, ctx)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                self._queues[key] = []

            for cmd in cmds:
                heapq.heappush(self._queues[key], cmd)
                self._pending[cmd.id] = cmd
                self._history.setdefault(key, []).append(cmd)

    async def dequeue(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]:
        """Dequeue commands for a specific world (all pending, regardless of tick)."""
        max_items = min(max_items or self._max_dequeue, self._max_dequeue)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues or not self._queues[key]:
                return []

            commands = []
            queue = self._queues[key]

            for _ in range(min(max_items, len(queue))):
                if queue:
                    cmd = heapq.heappop(queue)
                    commands.append(cmd)
                    self._pending.pop(cmd.id, None)

            if self._debug and commands:
                logger.debug(
                    f"[broker] dequeue: world={key}, count={len(commands)}, "
                    f"remaining={len(queue)}"
                )

            return commands

    async def dequeue_due(
        self,
        world_id: str | UUID,
        tick: int,
        limit: int | None = None,
    ) -> list[Command]:
        """
        Pop all commands where cmd.tick <= tick.
        Ordered by (tick, priority, seq).
        """
        limit = min(limit or self._max_dequeue, self._max_dequeue)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues or not self._queues[key]:
                return []

            commands = []
            queue = self._queues[key]

            while queue and len(commands) < limit:
                if queue[0].tick <= tick:
                    cmd = heapq.heappop(queue)
                    commands.append(cmd)
                    self._pending.pop(cmd.id, None)
                else:
                    break

            return commands

    async def ack(self, cmd_ids: list[UUID]) -> None:
        """Remove from pending after successful application."""
        async with self._lock:
            for cid in cmd_ids:
                self._pending.pop(cid, None)

    async def peek(self, world_id: str | UUID, max_items: int | None = None) -> list[Command]:
        """Peek at commands without removing them."""
        max_items = min(max_items or self._max_dequeue, self._max_dequeue)

        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                return []
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
