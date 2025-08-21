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

import asyncio
from typing import List, Dict, Optional
from uuid import UUID
import heapq

from archetype.app.models import Command
from archetype.app.auth.models import ActorCtx
from archetype.app.auth.guard import guardrail_allow


class CommandBroker:
    """
    A simplified command broker for development.
    
    Features:
    - Priority queue per world
    - Basic RBAC checks
    - Async-safe operations
    """

    def __init__(self, max_dequeue: int = 50_000):
        self._queues: Dict[str, List[Command]] = {}  # world_id -> priority queue
        self._pending: Dict[UUID, Command] = {}
        self._history: Dict[str, List[Command]] = {}
        self._lock = asyncio.Lock()
        self._max_dequeue = max_dequeue

    async def enqueue(self, world_id: str | UUID, cmd: Command, ctx: ActorCtx):
        """Enqueue a single command for a specific world."""
        if not await guardrail_allow(cmd, ctx):
            raise PermissionError(f"RBAC deny for command {cmd.id}")

        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                self._queues[key] = []
            
            heapq.heappush(self._queues[key], cmd)
            self._pending[cmd.id] = cmd
            self._history.setdefault(key, []).append(cmd)

    async def enqueue_bulk(self, world_id: str | UUID, cmds: List[Command], ctx: ActorCtx):
        """Enqueue multiple commands for a specific world."""
        # Check permissions for all commands first
        for cmd in cmds:
            if not await guardrail_allow(cmd, ctx):
                raise PermissionError(f"RBAC deny for command {cmd.id}")

        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                self._queues[key] = []
            
            for cmd in cmds:
                heapq.heappush(self._queues[key], cmd)
                self._pending[cmd.id] = cmd
                self._history.setdefault(key, []).append(cmd)

    async def dequeue(self, world_id: str | UUID, max_items: Optional[int] = None) -> List[Command]:
        """Dequeue commands for a specific world."""
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
                    # Remove from pending
                    self._pending.pop(cmd.id, None)
            
            return commands

    # Back-compat alias used by CommandService
    async def dequeue_batch(self, world_id: str | UUID, max_items: Optional[int] = None) -> List[Command]:
        return await self.dequeue(world_id, max_items)

    async def peek(self, world_id: str | UUID, max_items: Optional[int] = None) -> List[Command]:
        """Peek at commands without removing them."""
        max_items = min(max_items or self._max_dequeue, self._max_dequeue)
        
        async with self._lock:
            key = str(world_id)
            if key not in self._queues:
                return []
            
            # Return sorted copy without modifying queue
            queue = self._queues[key]
            return sorted(queue)[:max_items]

    async def get_pending_count(self, world_id: Optional[str | UUID] = None) -> int:
        """Get count of pending commands."""
        async with self._lock:
            if world_id:
                return len(self._queues.get(str(world_id), []))
            return len(self._pending)

    async def get_history(self, world_id: str | UUID, limit: int = 100) -> List[Command]:
        """Return recent enqueued commands for a world (most recent last)."""
        async with self._lock:
            items = self._history.get(str(world_id), [])
            if not items:
                return []
            return items[-limit:]

    async def clear(self, world_id: Optional[str | UUID] = None):
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