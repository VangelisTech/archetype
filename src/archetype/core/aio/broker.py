from typing import List, Dict, Any, Literal
from uuid import UUID
from typing import Any, Dict, Literal
import asyncio
import json
import heapq

import pyarrow as pa
import pyarrow.parquet as pq

from archetype.core.interfaces import iBroker
from archetype.core.command import Command



class InMemoryBroker(iBroker):
    """
    O(log n) push/pop heap + async locks. Durable-log stub omitted for brevity.
    """

    def __init__(self) -> None:
        self._heap: List[Command] = []
        self._lock = asyncio.Lock()
        # ack bookkeeping only; not strictly needed in-memory
        self._pending: Dict[UUID, Command] = {}
        self._parquet_buffer: List[pa.RecordBatch] = []

    # 3-A enqueue / enqueue_bulk
    async def enqueue(self, cmd: Command, ctx: ActorCtx):
        if not guardrail_allow(cmd, ctx):
            raise PermissionError
        # 1 append to in-RAM parquet buffer
        self._parquet_buffer.append(cmd.to_arrow())
        # optional async flush every 500 rows / 500 ms
        if len(self._parquet_buffer) >= 500:
            await self._flush_parquet()
        # 2 push to heap
        async with self._lock:
            heapq.heappush(self._heap, cmd)
            self._pending.add(cmd.id)


    async def enqueue_bulk(self, cmds: List[Command]) -> None:
        async with self._lock:
            for cmd in cmds:
                heapq.heappush(self._heap, cmd)
                self._pending[cmd.id] = cmd

    # 3-B pop what’s due
    async def dequeue_due(self, *, tick: int, limit: int = 1_000) -> List[Command]:
        out: list[Command] = []
        async with self._lock:
            while self._heap and len(out) < limit and self._heap[0].tick <= tick:
                cmd = heapq.heappop(self._heap)
                out.append(cmd)
        return out

    # 3-C ack == drop from pending
    async def ack(self, cmd_ids: List[UUID]) -> None:
        async with self._lock:
            for cid in cmd_ids:
                self._pending.pop(cid, None)

    async def _flush_parquet(self):
        tbl = pa.Table.from_batches(self._parquet_buffer)
        pq.write_table(
            tbl,
            f"command_log/{date.today():%Y-%m-%d}.parquet",
            append=True,
            compression="zstd",
        )
        self._parquet_buffer.clear()