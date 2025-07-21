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

import time
from typing import List, Dict
from uuid import UUID
import asyncio
import heapq
from datetime import date
import os

import pyarrow as pa
import pyarrow.parquet as pq

from archetype.core.base import BaseCommandBroker
from archetype.core.command import Command
from archetype.core.auth import ActorCtx, guardrail_allow


class AsyncCommandBroker(BaseCommandBroker):
    """
    A production-hardened, in-memory command broker.
    Features:
    - O(log n) heap for efficient, prioritized command ordering.
    - Async-safe operations with locks.
    - RBAC and quota guardrails on enqueue.
    - Write-behind durable logging to Parquet with periodic flushing.
    """

    def __init__(self, max_dequeue: int = 50_000, semaphore: asyncio.Semaphore = None):
        self._queue: Dict[str, asyncio.PriorityQueue] = {}
        
        self._pending: Dict[UUID, Command] = {}
        self._parquet_buffer: List[pa.RecordBatch] = []
        self._semaphore = semaphore 

        # Configuration
        self._max_dequeue = max_dequeue
        self._flush_size_bytes = 512 * 1024 * 1024  # 512 MB

        # Periodic flush task
        self._flush_task = asyncio.create_task(self._periodic_flush())

    async def _gatekeep_cmds(self, cmds: List[Command], ctx: ActorCtx, world_id: str) -> bool:
        arrow_cmds = []
        for c in cmds:
            
            
            arrow_cmds.append(c.to_arrow())

        allowed_results = (guardrail_results)


    async def enqueue(self, world_id: str, cmd: Command, ctx: ActorCtx):
        
        if not await guardrail_allow(cmd, ctx):
                raise PermissionError(f"RBAC deny for command {c.id} in bulk enqueue.")

        should_flush = False
        async with self._lock:
            self._parquet_buffer.append(cmd.to_arrow())
            if len(self._parquet_buffer) >= self._flush_rows:
                should_flush = True
            heapq.heappush(self._heap, cmd)
            self._pending[cmd.id] = cmd

        if should_flush:
            await self._flush_parquet()

    async def enqueue_bulk(self, world_id: str, cmds: List[Command], ctx: ActorCtx):
        arrow_cmds = []
        for c in cmds:
            if not await guardrail_allow(c, ctx):
                raise PermissionError(f"RBAC deny for command {c.id} in bulk enqueue.")
            arrow_cmds.append(c.to_arrow())

        should_flush = False
        async with self._lock:
            self._parquet_buffer.extend(arrow_cmds)
            if len(self._parquet_buffer) >= self._flush_rows:
                should_flush = True

            for c in cmds:
                heapq.heappush(self._heap, c)
                self._pending[c.id] = c

        if should_flush:
            await self._flush_parquet()

    async def dequeue_due(self, world_id: str, *, tick: int, limit: int | None = None) -> List[Command]:
        limit = limit or self._max_dequeue
        out = []
        async with self._lock:
            while self._heap and len(out) < limit and self._heap[0].tick <= tick:
                out.append(heapq.heappop(self._heap))
        return out

    async def ack(self, world_id: str, cmd_ids: List[UUID]) -> None:
        async with self._lock:
            for cid in cmd_ids:
                self._pending.pop(cid, None)

    async def _flush_parquet(self):
        if not self._parquet_buffer:
            return

        buffer_to_flush = None
        async with self._lock:
            if self._parquet_buffer:
                buffer_to_flush = self._parquet_buffer
                self._parquet_buffer = []

        if not buffer_to_flush:
            return

        log_dir = "command_log"
        os.makedirs(log_dir, exist_ok=True)

        tbl = pa.Table.from_batches(buffer_to_flush)
        pq.write_table(
            tbl,
            os.path.join(log_dir, f"{date.today():%Y-%m-%d}.parquet"),
            schema=Command.arrow_schema(),
            compression="zstd",
        )

    async def _periodic_flush(self):
        while True:
            await asyncio.sleep(self._flush_ms / 1000)
            if self._parquet_buffer:
                await self._flush_parquet()


