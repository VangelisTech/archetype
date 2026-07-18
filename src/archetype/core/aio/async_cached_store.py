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
from archetype.core.interfaces import AppendReceipt, ArchetypeSignature, iAsyncStore

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

    def extend(self, other: "MemTable") -> None:
        """Append another memtable without rebuilding its Arrow batches."""
        if not other.rows:
            return
        self.batches.extend(other.batches)
        self.rows += other.rows
        self.bytes += other.bytes
        self.last_mut = max(self.last_mut, other.last_mut)

    @classmethod
    def followed_by(cls, older: "MemTable", newer: "MemTable") -> "MemTable":
        """Return ``older + newer`` while preserving append order."""
        merged = cls(last_mut=max(older.last_mut, newer.last_mut))
        merged.extend(older)
        merged.extend(newer)
        return merged

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
        # A detached batch remains readable while its durable append is in
        # flight. New rows land in ``_mem`` and can never be cleared by that
        # append's completion.
        self._inflight: dict[ArchetypeSignature, MemTable] = {}
        self._committed_sigs: set[ArchetypeSignature] = set()

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
        self._state_lock = asyncio.Lock()
        self._flush_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False
        self._accepting_appends = True
        self._bg_on = True
        self._bg_stop = asyncio.Event()
        self._bg_task: asyncio.Task | None = asyncio.create_task(
            self._background_flush(self.idle_sec)
        )

    def _update_total_bytes(self, delta: int):
        self.total_cached_bytes += delta

    # ---------------------------------------------------
    # Private helpers, Cache Management
    # ---------------------------------------------------

    async def _flush_sig_locked(self, sig: ArchetypeSignature) -> bool:
        """Detach and persist one signature while ``_flush_lock`` is held.

        The durable append is serialized, but the state lock is released
        before I/O so concurrent appends can accumulate in a fresh memtable.
        A failed or cancelled append restores the detached rows *before* any
        newer rows and leaves the byte accounting unchanged.
        """
        async with self._state_lock:
            snapshot = self._mem.get(sig)
            if snapshot is None or not snapshot.rows:
                return False
            self._mem[sig] = MemTable()
            self._inflight[sig] = snapshot

        try:
            tbl = await asyncio.to_thread(snapshot.to_table)
            await self._inner.append(sig, daft.from_arrow(tbl))
        except BaseException:
            async with self._state_lock:
                newer = self._mem.get(sig, MemTable())
                self._mem[sig] = MemTable.followed_by(snapshot, newer)
                self._inflight.pop(sig, None)
            raise

        async with self._state_lock:
            self._inflight.pop(sig, None)
            self._update_total_bytes(-snapshot.bytes)
        self._committed_sigs.add(sig)
        return True

    async def _background_flush_sig(self, sig: ArchetypeSignature) -> bool:
        """Serialize one background/threshold flush against explicit drains."""
        async with self._flush_lock:
            return await self._flush_sig_locked(sig)

    async def _background_flush(self, idle_sec=30):
        while self._bg_on:
            try:
                await asyncio.wait_for(self._bg_stop.wait(), timeout=idle_sec)
            except TimeoutError:
                pass
            if not self._bg_on:
                return
            now = time.time()

            async with self._state_lock:
                idle_sigs = [
                    s for s, m in self._mem.items() if m.rows and now - m.last_mut >= idle_sec
                ]

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
        commit_tokens: list[str] | None = None,
    ) -> DataFrame:
        """
        Get all archetypes that contain all of the specified component types.

        Reads are a union of already-flushed rows on disk and unflushed rows
        still in the memtable so that callers always see a coherent view of
        the signature's full history regardless of flush state. The
        commit-token allowlist applies to both layers, so staged rows are no
        more visible than durable rows.
        """
        disk_df = await self._inner.get_archetype_df(
            sig,
            world_id,
            run_id,
            ticks=ticks,
            entity_ids=entity_ids,
            active_only=active_only,
            commit_tokens=commit_tokens,
        )

        async with self._state_lock:
            inflight = self._inflight.get(sig)
            live = self._mem.get(sig)
            batches = [
                *(inflight.batches if inflight and inflight.rows else []),
                *(live.batches if live and live.rows else []),
            ]
        if not batches:
            return disk_df

        target_schema = Archetype.get_archetype_schema(sig)
        mem_table = pa.Table.from_batches(batches).select(target_schema.names).cast(target_schema)
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

        if commit_tokens is not None:
            visible = (
                mem_df["commit_token"].is_in(commit_tokens) if commit_tokens else daft.lit(False)
            )
            mem_df = mem_df.where(visible)

        return disk_df.concat(mem_df)

    async def list_signatures(self) -> list[ArchetypeSignature]:
        """Delegate to inner store."""
        return await self._inner.list_signatures()

    async def list_committed_signatures(self) -> list[ArchetypeSignature]:
        """Include successful staged appends as well as durable inner writes."""
        inner = set(await self._inner.list_committed_signatures())
        return list(inner | self._committed_sigs)

    async def append(self, sig: ArchetypeSignature, df: DataFrame) -> AppendReceipt:
        """
        Cache driven append with built in flush logic to underlying storage (super) a table with a new dataframe.

        The receipt is staged (durable=False) unless this append tripped a
        flush: rows become durable at flush()/threshold/idle/shutdown. A
        commit coordinator must call flush() before publishing a manifest
        head — a head must never claim RAM-only rows are durable.
        """
        # 1) convert the tiny incoming Daft DataFrame slice → RecordBatch
        pending = MemTable()
        for batch in df.to_arrow_iter():
            pending.append(batch)

        if pending.rows == 0:
            return AppendReceipt(table_id=Archetype.get_name(sig), rows=0, durable=True)

        async with self._state_lock:
            if not self._accepting_appends:
                raise RuntimeError("cannot append to a shut down cached store")
            self._mem.setdefault(sig, MemTable()).extend(pending)
            self._update_total_bytes(pending.bytes)

            # Cheap in-memory stats. ``total_cached_bytes`` includes detached
            # in-flight batches until their durable append succeeds.
            needs_flush = (
                self._mem[sig].rows >= self.flush_rows
                or self._mem[sig].bytes >= self.flush_bytes
                or self.total_cached_bytes >= self.global_budget_bytes
            )

        if needs_flush:
            await self._background_flush_sig(sig)

        self._committed_sigs.add(sig)
        return AppendReceipt(
            table_id=Archetype.get_name(sig), rows=pending.rows, durable=needs_flush
        )

    async def flush(self) -> None:
        """Drain every memtable to the inner store.

        Called by the commit coordinator's owner before a manifest head is
        published, so visibility never outruns durability.
        """
        # Taking the same lock as idle/threshold flushes is the durability
        # barrier. A detached batch may leave ``_mem`` empty while its append
        # still lives in ``_inflight``; checking only ``_mem`` would let a tick
        # manifest publish before that append completed.
        async with self._flush_lock:
            while True:
                async with self._state_lock:
                    sigs = [sig for sig, mt in self._mem.items() if mt.rows]
                if not sigs:
                    break
                for sig in sigs:
                    await self._flush_sig_locked(sig)
            await self._inner.flush()

    async def shutdown(self) -> None:
        """
        Stop background flushing and ensure all pending data is flushed to the inner store.
        """
        async with self._shutdown_lock:
            if self._shutdown_complete:
                return

            async with self._state_lock:
                self._accepting_appends = False
            self._bg_on = False
            self._bg_stop.set()
            if self._bg_task is not None:
                await self._bg_task

            await self.flush()
            await self._inner.shutdown()
            self._shutdown_complete = True
