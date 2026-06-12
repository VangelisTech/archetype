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
import asyncio
import time
from dataclasses import dataclass, field
from logging import getLogger

# Technologies
import daft
import psutil
import pyarrow as pa
from daft import DataFrame

from archetype.core.archetype import Archetype
from archetype.core.config import CacheConfig

# Internals
from archetype.core.interfaces import ArchetypeSignature, iAsyncStore

logger = getLogger(__name__)


@dataclass
class MemTable:
    batches: list[pa.RecordBatch] = field(default_factory=list)
    rows: int = 0
    bytes: int = 0
    last_mut: float = field(default_factory=time.time)

    def append(self, batch: pa.RecordBatch):
        self.batches.append(batch)
        self.rows += batch.num_rows
        # Compute batch size in bytes; RecordBatch has no buffers() API.
        # Convert to a table and use nbytes which accounts for all columns.
        try:
            tbl = pa.Table.from_batches([batch])
            self.bytes += tbl.nbytes
        except Exception:
            # Fallback: sum column nbytes
            self.bytes += sum(col.nbytes for col in batch.columns)
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

    def __init__(
        self,
        async_store: iAsyncStore,
        cache_config: CacheConfig,
    ):
        self._inner: iAsyncStore = async_store
        self._mem: dict[ArchetypeSignature, MemTable] = {}

        # thresholds
        self.flush_rows = cache_config.flush_rows
        self.flush_bytes = cache_config.flush_mb << 20
        self.idle_sec = cache_config.idle_sec

        # Distinguish between the global memory budget and the current cached usage
        # Default to a conservative 25% of available RAM when unspecified
        self.global_budget_bytes: float = (
            cache_config.global_mb << 20
            if cache_config.global_mb is not None
            else psutil.virtual_memory().available * 0.25
        )
        self.total_cached_bytes: int = 0

        # background loop turned on/off flag
        self._flush_lock = asyncio.Lock()
        self._bg_on = True
        self._bg_task: asyncio.Task | None = asyncio.create_task(
            self._background_flush(self.idle_sec)
        )

    def _update_total_bytes(self, delta: int):
        self.total_cached_bytes += delta

    # ---------------------------------------------------
    # Private helpers, Cache Management
    # ---------------------------------------------------

    def _build_arrow_table(self, sig: ArchetypeSignature) -> pa.Table | None:
        mt = self._mem.get(sig)
        return mt.to_table() if mt and mt.rows else None

    async def _background_flush_sig(self, sig: ArchetypeSignature):
        # 1. Build the Arrow table synchronously in a worker thread
        async with self._flush_lock:  # serialises flushes
            tbl = await asyncio.to_thread(self._build_arrow_table, sig)
            if tbl is None:
                return
        flushed_bytes = tbl.nbytes

        # 2. Convert to Daft and flush on the event-loop thread
        df = daft.from_arrow(tbl)
        await self._inner.append(sig, df)

        # 3. Clear the memtable
        self._mem[sig].clear()
        self._update_total_bytes(-flushed_bytes)

    async def _background_flush(self, idle_sec=30):
        while self._bg_on:
            await asyncio.sleep(idle_sec)
            now = time.time()

            idle_sigs = [s for s, m in self._mem.items() if now - m.last_mut >= idle_sec]

            for sig in idle_sigs:
                await self._background_flush_sig(sig)

    # ---------------------------------------------------
    # Public API
    # ---------------------------------------------------

    async def get_archetype_df(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        run_id: str,
        *,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        active_only: bool = False,
    ) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.

        Reads are a union of already-flushed rows on disk and unflushed rows
        still in the memtable so that callers always see a coherent view of
        the sig's full history regardless of flush state.
        """
        disk_df = await self._inner.get_archetype_df(
            sig,
            world_id,
            run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=active_only,
        )

        mt = self._mem.get(sig)
        if not (mt and mt.rows):
            return disk_df

        target_schema = Archetype.get_archetype_schema(sig)
        mem_table = mt.to_table().select(target_schema.names).cast(target_schema)
        mem_df = daft.from_arrow(mem_table)
        # (Daft stubs type Expression.__eq__ as bool; these are Expressions.)
        wid, rid = str(world_id), str(run_id)
        mem_df = mem_df.where(mem_df["world_id"] == wid)  # ty: ignore[invalid-argument-type]
        mem_df = mem_df.where(mem_df["run_id"] == rid)  # ty: ignore[invalid-argument-type]

        if active_only:
            mem_df = mem_df.where(mem_df["is_active"])

        if ticks is not None:
            mem_df = mem_df.where(mem_df["tick"].is_in(ticks))

        if entity_ids is not None:
            mem_df = mem_df.where(mem_df["entity_id"].is_in(entity_ids))

        return disk_df.concat(mem_df)

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """Delegate to inner store."""
        return await self._inner.list_signatures()

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> None:
        """
        Cache driven append with built in flush logic to underlying storage (super) a table with a new dataframe.
        """
        # 1) convert the tiny incoming Daft DataFrame slice → RecordBatch
        added_bytes = 0
        for batch in df.to_arrow_iter():
            self._mem.setdefault(sig, MemTable()).append(batch)
            added_bytes += batch.nbytes

        self._update_total_bytes(added_bytes)

        # cheap in‑mem stats
        needs_flush = (
            self._mem[sig].rows >= self.flush_rows
            or self._mem[sig].bytes >= self.flush_bytes
            or sum(m.bytes for m in self._mem.values()) >= self.global_budget_bytes
        )

        if needs_flush:
            await self._background_flush_sig(sig)

    async def shutdown(self) -> None:
        """
        Stop background flushing and ensure all pending data is flushed to the inner store.
        """
        # Stop the background task quickly
        self._bg_on = False
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass

        # Flush any remaining signatures
        for sig, mt in list(self._mem.items()):
            if mt.rows:
                await self._background_flush_sig(sig)

        # Delegate shutdown to inner store
        await self._inner.shutdown()
