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

# Standard Python Libraries
from typing import Dict, Tuple, List, Optional, Any
from logging import getLogger
import time
import psutil
from dataclasses import dataclass, field

# Technologies
import daft
from daft import col, DataFrame, Schema
from daft.expressions import lit
from daft.session import Session
from daft.catalog import Table
import pyarrow as pa
import asyncio


# Internals
from archetype.core import ArchetypeSignature, Archetype
from .async_interfaces import iAsyncStore

logger = getLogger(__name__)


@dataclass
class MemTable:
    batches: List[pa.RecordBatch] = field(default_factory=list)
    rows:   int = 0
    bytes:  int = 0
    last_mut: float = field(default_factory=time.time)

    def append(self, batch: pa.RecordBatch):
        self.batches.append(batch)
        self.rows  += batch.num_rows
        self.bytes += sum(buf.size for buf in batch.buffers())
        self.last_mut = time.time()

    def to_table(self) -> pa.Table:
        return pa.Table.from_batches(self.batches)

    def clear(self):
        self.batches.clear()
        self.rows = self.bytes = 0


class AsyncCachedStore(iAsyncStore):
    """
    This Store variant holds a cache.

    """
    def __init__(self,
        async_store: iAsyncStore,
        flush_rows=1_000_000, 
        flush_mb=512,
        global_mb=None, 
        idle_sec=30, 
    ):
        self._inner: iAsyncStore = async_store
        self._mem: Dict[ArchetypeSignature, MemTable] = {}
        
        # thresholds
        self.flush_rows   = flush_rows
        self.flush_bytes  = flush_mb << 20
        self.idle_sec     = idle_sec
        self.global_bytes = (global_mb << 20) if global_mb else psutil.virtual_memory().available * 0.7
        
        # background loop turned on/off flag
        self._flush_lock = asyncio.Lock() 
        self._bg_on = True
        asyncio.create_task(self._background_flush(self.idle_sec))  # fire-and-forget

    def _update_total(self, delta: int):
        self.global_bytes += delta
        

    # ---------------------------------------------------
    # Private helpers, Cache Management
    # ---------------------------------------------------

    def _build_arrow_table(self, sig: ArchetypeSignature) -> Optional[pa.Table]:
        mt = self._mem.get(sig)
        return mt.to_table() if mt and mt.rows else None

    async def _background_flush_sig(self, sig: ArchetypeSignature):
        # 1. Build the Arrow table synchronously in a worker thread
        async with self._flush_lock:        # serialises flushes
            tbl = await asyncio.to_thread(self._build_arrow_table, sig)
            if tbl is None:
                return
        flushed_bytes = tbl.nbytes

        # 2. Convert to Daft and flush on the event-loop thread
        df = daft.from_arrow(tbl)
        await self._inner.append(sig, df)

        # 3. Clear the memtable
        self._mem[sig].clear()
        self._update_total(-flushed_bytes)

    async def _background_flush(self, idle_sec=30):
        while True:
            await asyncio.sleep(idle_sec)
            now = time.time()
            
            idle_sigs = [s for s, m in self._mem.items()
                        if now - m.last_mut >= idle_sec]
            
            for sig in idle_sigs:
                await self._background_flush_sig(sig)

    # ---------------------------------------------------
    # Public API
    # ---------------------------------------------------

    async def get_archetype_df(self, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.
        """
        # Get the memtable
        mt = self._mem.get(sig)

        # Read the Archetype Table from Memtable or Disk 
        if mt and mt.rows:
            df  = daft.from_arrow(mt.to_table())

            return df.where(df["world_id"] == world_id) \
               .where(df["run_id"] == run_id)
        
        # If no data in cache, grab from storage
        return await self._inner.get_archetype_df(sig, tick, world_id, run_id)

    
    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Cache driven append with built in flush logic to underlying storage (super) a table with a new dataframe.
        """
        # 1) convert the tiny incoming Daft DataFrame slice → RecordBatch
        added = 0
        for batch in df.to_arrow_iter():
            self._mem.setdefault(sig, MemTable()).append(batch)
            added += sum(buf.size for buf in batch.buffers())

        self._update_total(added)

        # cheap in‑mem stats
        needs_flush = (
            self._mem[sig].rows  >= self.flush_rows or
            self._mem[sig].bytes >= self.flush_bytes or
            sum(m.bytes for m in self._mem.values()) >= self.global_bytes
        )

        if needs_flush:
            await self._background_flush_sig(sig)