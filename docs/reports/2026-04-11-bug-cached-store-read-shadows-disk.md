# Bug Report: `AsyncCachedStore.get_archetype_df` returns ONLY the in-memory memtable when both memtable and disk have data — flushed rows are silently invisible

**Date:** 2026-04-11
**Severity:** High (silent partial reads on the cached storage path; query returns half the data with no error)
**Affects:** `archetype.core.aio.async_cached_store.AsyncCachedStore.get_archetype_df` — every world configured with a `CacheConfig` (i.e. anything that uses `WorldFactory.create_world(..., cache_config=...)`)
**Discovered by:** Overnight bug hunt

## Summary

`AsyncCachedStore.get_archetype_df` (`async_cached_store.py:139-155`) reads from the in-memory memtable when it has any rows, and falls back to the inner store *only* when the memtable is empty. The two paths are mutually exclusive — there is no union. After a partial flush (background flush, manual flush, or explicit `_background_flush_sig`), the memtable is empty and freshly-appended rows accumulate. The next read returns *only* those freshly-appended rows; everything that has already been flushed to disk is invisible. The store reports a partial view with no error.

The bug fires the first time a sig has data on disk **and** new data in the memtable simultaneously. That's the steady state for any long-running world: every flush + subsequent append produces a window where the cache shadows the disk. Queries during that window return wrong results.

## Impact

1. **Time-travel queries return partial data.** Any caller that queries an archetype after the world has run for more than one flush cycle gets only the rows that have been written since the last flush. For workloads with `CacheConfig(idle_sec=30)` (the default) and a fast loop, this means roughly half the rows are missing at any random query time. The user has no way to detect this — the cache returns a "successful" DataFrame.
2. **`world.step` reads are corrupted on every tick after the first flush.** `_run_archetype` (`async_world.py:198-209`) calls `self.query_archetype(...)` for the previous-tick state when `prefer_live_reads=False`. That goes through `querier.query_archetype` → `store.get_archetype_df`. If the cache wraps the store, the previous-tick query sees only the memtable contents — losing every prior tick that was flushed. The world's processors then run against an incomplete previous-state DataFrame, which produces a corrupted next-tick state, which is itself flushed, compounding the corruption indefinitely.
3. **The cache wrapper is opt-in but ergonomic.** `WorldFactory` and `StorageService.get_backend` accept a `CacheConfig` and wrap the store automatically when one is supplied (`storage_service.py:69-70`). Anyone reading the `AGENTS.md` "Performance" section and adding `cache_config=CacheConfig()` to a benchmark setup hits this bug 1:1.
4. **The bug is invisible from the test suite.** No test in the entire `tests/` tree exercises the "memtable + disk both have data" path. Every cache-related test either appends without flushing (memtable-only) or doesn't check that disk data is visible after a memtable refill. The defect can sit in main indefinitely.
5. **Data loss compounds with `simulation-service-run-discards-runconfig`.** That bug already makes per-tick `run_id`s diverge across a multi-step run. Combined with this one, a single `simulation_service.run(num_steps=10)` call produces ten distinct run_ids, each potentially queried during a different cache state, producing inconsistent partial results across ticks. The two bugs together make `prefer_live_reads=False + cache_config=...` an unusable combination.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit a60fce3, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: AsyncCachedStore.get_archetype_df reads ONLY the memtable when
the memtable has any data, even when the inner store has prior rows."""
import asyncio
import tempfile

import daft
import pyarrow as pa

from archetype.core.aio import AsyncCachedStore, AsyncStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import CacheConfig, StorageConfig
from archetype.core.runtime.storage import StorageContextFactory


class P(Component):
    x: int = 0


def make_row(eid: int, x: int) -> dict:
    return Archetype.to_row_dict(eid, 0, [P(x=x)], "wid", "rid")


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ctx = StorageContextFactory.build(StorageConfig(uri=tmp, namespace="ns"))
        inner = AsyncStore(ctx)
        cache = AsyncCachedStore(async_store=inner, cache_config=CacheConfig(idle_sec=999))

        sig = Archetype.sig_from_components([P()])
        schema = Archetype.get_archetype_schema(sig)

        # batch 1 — entity 1
        df1 = daft.from_arrow(pa.Table.from_pylist([make_row(1, 10)], schema=schema))
        await cache.append(sig, df1)
        await cache._background_flush_sig(sig)  # flush to disk, clear memtable
        print(f"after flush: memtable rows={cache._mem.get(sig).rows if sig in cache._mem else 0}")

        # batch 2 — entity 2 — stays in memtable
        df2 = daft.from_arrow(pa.Table.from_pylist([make_row(2, 20)], schema=schema))
        await cache.append(sig, df2)
        print(f"after append: memtable rows={cache._mem[sig].rows}")

        # Read via the cache facade.
        df = await cache.get_archetype_df(sig, "wid", "rid")
        eids = sorted(r["entity_id"] for r in df.collect().to_pylist())
        print(f"cache read returned eids = {eids}")

        # Sanity: the inner store *does* have entity 1 on disk.
        inner_df = await inner.get_archetype_df(sig, "wid", "rid")
        inner_eids = sorted(r["entity_id"] for r in inner_df.collect().to_pylist())
        print(f"inner store has eids     = {inner_eids}")

        assert eids == [1, 2], f"BUG: cache read missing rows from disk: {eids}"
        await cache.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
after flush: memtable rows=0
after append: memtable rows=1
cache read returned eids = [2]
inner store has eids     = [1]
AssertionError: BUG: cache read missing rows from disk: [2]
```

The cache returned `[2]` (the memtable content). The inner store still has `[1]` on disk. The cached read is a strict subset of the union — it never read disk because the memtable was non-empty.

### Baseline (proves the bug is the "memtable shadows disk" branch)

When the memtable is empty, the cache correctly reads from the inner store:

```python
await cache.append(sig, df1)
await cache._background_flush_sig(sig)
# memtable rows after flush = 0
df = await cache.get_archetype_df(sig, "wid", "rid")
# cache read = [1]
# OK (baseline): empty memtable → reads from disk correctly.
```

The bug fires only when the memtable has any rows. Empty memtable falls through to `inner.get_archetype_df` correctly.

## Root cause

`src/archetype/core/aio/async_cached_store.py:139-155`:

```python
async def get_archetype_df(
    self, sig: ArchetypeSignature, world_id: str, run_id: str
) -> DataFrame:
    """
    Get all archetypes that contain all of the specified component types.
    """
    # Get the memtable
    mt = self._mem.get(sig)

    # Read the Archetype Table from Memtable or Disk
    if mt and mt.rows:
        df = daft.from_arrow(mt.to_table())

        return df.where(df["world_id"] == str(world_id)).where(df["run_id"] == str(run_id))

    # If no data in cache, grab from storage
    return await self._inner.get_archetype_df(sig, world_id, run_id)
```

The control flow is **either-or**, never **both**. If the memtable has any rows, the function returns from the memtable branch and never touches the inner store. There is no union, no concat, no "merge memtable on top of disk". The comment "Read the Archetype Table from Memtable or Disk" describes the bug.

The append path is correct: `append` (`async_cached_store.py:157-177`) writes to the memtable and flushes via `_background_flush_sig` when thresholds are exceeded. `_background_flush_sig` (`async_cached_store.py:109-123`):

```python
async def _background_flush_sig(self, sig: ArchetypeSignature):
    async with self._flush_lock:
        tbl = await asyncio.to_thread(self._build_arrow_table, sig)
        if tbl is None:
            return
    flushed_bytes = tbl.nbytes

    df = daft.from_arrow(tbl)
    await self._inner.append(sig, df)

    self._mem[sig].clear()
    self._update_total_bytes(-flushed_bytes)
```

Flush correctly writes to the inner store and clears the memtable. But after the flush, the *next* append refills the memtable, and the next read consults only the memtable.

Trace for the MRE:

1. `cache.append(sig, df1)` — writes batch 1 (entity 1) to `_mem[sig]`. `_mem[sig].rows == 1`.
2. `cache._background_flush_sig(sig)` — copies `_mem[sig]` to an Arrow table, calls `inner.append(sig, df)` (inner store now has entity 1 on disk), `_mem[sig].clear()`. `_mem[sig].rows == 0`.
3. `cache.append(sig, df2)` — writes batch 2 (entity 2) to `_mem[sig]`. `_mem[sig].rows == 1`.
4. `cache.get_archetype_df(sig, "wid", "rid")`:
   - `mt = self._mem.get(sig)` → MemTable with 1 row.
   - `if mt and mt.rows:` → True. Enter the memtable branch.
   - `df = daft.from_arrow(mt.to_table())` → only entity 2.
   - `return df.where(...)` → returns only entity 2. **Inner store never queried.**
5. Caller sees `[2]`. Entity 1 is silently absent.

The fundamental defect is that the cache reads as if it owned all the data, but it only owns the unflushed tail. The disk is the source of truth for everything before the last flush, and the cache forgets to read it.

## Why existing tests miss this

`grep -rn "AsyncCachedStore" tests/` returns five matches:

- `tests/core/test_resources_manager.py:30-38` — asserts that `get_backend(cache_config=...)` returns an `AsyncCachedStore` instance. Type check, no behavioural test.
- `tests/app/test_factory.py:91-103` — same shape, factory wiring assertion.
- `tests/aio/test_async_world_execution.py:7,62` — uses `AsyncCachedStore` as the store under a world, runs world.step. Doesn't check that data flushed in tick N is visible at tick N+M.
- `tests/aio/test_async_world_mutations.py:5,44,52` — same shape.

`grep -rn "_background_flush\|memtable\|_mem\b" tests/` returns **zero matches**. No test in the entire suite calls `_background_flush_sig` (manually or via threshold) and then reads back to check that disk data survives a memtable refill. The "memtable + disk" coexistence path is uncovered.

The closest non-cache test is `tests/aio/test_storage_context_namespace_isolation.py` which exercises the underlying `AsyncStore.get_archetype_df` directly. It doesn't go through the cache wrapper and doesn't trigger the bug.

The bug would also be caught by any longer-running integration test that:

1. Writes data over many ticks with a cache wrapping the store.
2. Triggers at least one flush (via threshold or background timer).
3. Continues writing for at least one more tick.
4. Reads back the full history.

But no such test exists. The cache is only ever exercised in append-only scenarios where reads happen *before* the first flush (so the memtable shadows nothing) or reads happen *after* shutdown (which forces a final flush).

## Suggested fixes

**Fix A — concatenate memtable with the inner store result.** The cleanest fix: read both, union them, return:

```diff
 async def get_archetype_df(
     self, sig: ArchetypeSignature, world_id: str, run_id: str
 ) -> DataFrame:
-    # Get the memtable
-    mt = self._mem.get(sig)
-
-    # Read the Archetype Table from Memtable or Disk
-    if mt and mt.rows:
-        df = daft.from_arrow(mt.to_table())
-
-        return df.where(df["world_id"] == str(world_id)).where(df["run_id"] == str(run_id))
-
-    # If no data in cache, grab from storage
-    return await self._inner.get_archetype_df(sig, world_id, run_id)
+    # Always read disk first.
+    disk_df = await self._inner.get_archetype_df(sig, world_id, run_id)
+
+    # If the memtable has freshly-appended rows, union them in.
+    mt = self._mem.get(sig)
+    if mt and mt.rows:
+        mem_df = daft.from_arrow(mt.to_table())
+        mem_df = mem_df.where(mem_df["world_id"] == str(world_id)).where(
+            mem_df["run_id"] == str(run_id)
+        )
+        return disk_df.concat(mem_df)
+
+    return disk_df
```

This is the minimal correct fix. The `disk_df.concat(mem_df)` produces a single DataFrame that the caller can filter/sort/limit downstream as if it were always one source. The order is "disk rows, then memtable rows" — for tick-monotonic data this is also chronological order, which is what time-travel queries want.

A subtlety: if the memtable contains updated versions of rows that are also on disk (e.g., a re-spawn of the same entity_id that was tombstoned earlier), the concat path returns *both* — the consumer needs to handle dedup. The current bug-on-main path doesn't dedupe either, so this is no worse, but it's worth a note in the docstring.

**Fix B — flush the memtable before reading.** Heavier handed:

```diff
 async def get_archetype_df(self, sig, world_id, run_id) -> DataFrame:
-    mt = self._mem.get(sig)
-    if mt and mt.rows:
-        df = daft.from_arrow(mt.to_table())
-        return df.where(...)
-    return await self._inner.get_archetype_df(sig, world_id, run_id)
+    # Force a flush so all data lives in one place before the read.
+    if self._mem.get(sig) and self._mem[sig].rows:
+        await self._background_flush_sig(sig)
+    return await self._inner.get_archetype_df(sig, world_id, run_id)
```

Fix B serializes flushes through the read path, which kills the cache's main performance benefit. It also moves more work out of the background loop into user-visible read latency. Fix A is the right shape; Fix B is what you do if Daft's `concat` has correctness issues (it doesn't, in 0.7.x — `concat` is well-tested).

**Fix C — keep flushes from clearing the memtable, instead mark rows as "flushed".** A more invasive redesign that would let the memtable stay populated as a coherent in-memory cache that always agrees with disk. This is a significant rewrite and out of scope for a stop-gap. Don't pick this; pick Fix A.

## Suggested regression tests

Add to `tests/aio/test_async_world_execution.py` (or a new `tests/aio/test_cached_store.py`):

```python
@pytest.mark.asyncio
async def test_cached_store_read_unions_memtable_with_disk(tmp_path):
    """Regression: a read after a partial flush must return rows from
    BOTH the memtable and the inner store, not just the memtable."""
    import daft
    import pyarrow as pa
    from archetype.core.aio import AsyncCachedStore, AsyncStore
    from archetype.core.archetype import Archetype
    from archetype.core.component import Component
    from archetype.core.config import CacheConfig, StorageConfig
    from archetype.core.runtime.storage import StorageContextFactory

    class P(Component):
        x: int = 0

    ctx = StorageContextFactory.build(StorageConfig(uri=str(tmp_path), namespace="ns"))
    inner = AsyncStore(ctx)
    cache = AsyncCachedStore(async_store=inner, cache_config=CacheConfig(idle_sec=999))

    sig = Archetype.sig_from_components([P()])
    schema = Archetype.get_archetype_schema(sig)

    def make_row(eid: int) -> dict:
        return Archetype.to_row_dict(eid, 0, [P(x=eid)], "wid", "rid")

    # Append, flush — disk has entity 1.
    await cache.append(sig, daft.from_arrow(pa.Table.from_pylist([make_row(1)], schema=schema)))
    await cache._background_flush_sig(sig)

    # Append again — memtable has entity 2, disk still has entity 1.
    await cache.append(sig, daft.from_arrow(pa.Table.from_pylist([make_row(2)], schema=schema)))

    df = await cache.get_archetype_df(sig, "wid", "rid")
    eids = sorted(r["entity_id"] for r in df.collect().to_pylist())
    assert eids == [1, 2], (
        f"cache read missed flushed rows: got {eids}, expected [1, 2]"
    )

    await cache.shutdown()


@pytest.mark.asyncio
async def test_cached_store_world_run_survives_flush_cycle(tmp_path):
    """End-to-end: a multi-step run with a cache wrapper must produce
    queryable rows for every prior tick, not just the rows since the
    last flush."""
    from archetype.core.aio import AsyncCachedStore, AsyncWorld, AsyncSystem
    from archetype.core.aio.async_querier import AsyncQueryManager
    from archetype.core.aio.async_updater import AsyncUpdateManager
    from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig
    from archetype.core.runtime.storage import StorageContextFactory

    class P(Component):
        x: int = 0

    ctx = StorageContextFactory.build(StorageConfig(uri=str(tmp_path), namespace="ns"))
    inner = AsyncStore(ctx)
    cache = AsyncCachedStore(async_store=inner, cache_config=CacheConfig(idle_sec=999))
    world = AsyncWorld(
        world_config=WorldConfig(name="t"),
        querier=AsyncQueryManager(store=cache),
        updater=AsyncUpdateManager(store=cache),
        system=AsyncSystem(),
    )
    rc = RunConfig(num_steps=1)

    eid = await world.create_entity([P(x=0)])
    await world.run(rc)
    # Force a flush to disk.
    sig = Archetype.sig_from_components([P()])
    await cache._background_flush_sig(sig)
    # Run another tick — new rows in the memtable, prior tick on disk.
    await world.run(rc)

    sig = Archetype.sig_from_components([P()])
    df = await cache.get_archetype_df(sig, str(world.world_id), str(rc.run_id))
    rows = df.collect().to_pylist()
    ticks = sorted({r["tick"] for r in rows})
    assert ticks == [0, 1], (
        f"cache read missed earlier ticks after flush: got ticks {ticks}, expected [0, 1]"
    )

    await cache.shutdown()
```

Both tests fail on `main` — the first at `assert eids == [1, 2]` (gets `[2]`), the second at `assert ticks == [0, 1]` (gets `[1]`).

## Notes / scope

- Affects `src/archetype/core/aio/async_cached_store.py:139-155`. Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- Distinct from the twelve other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - The four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is about hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is about quota accounting.
  - `component-prefix-collision` is about schema prefix non-uniqueness.
  - This bug is about the cache wrapper returning a partial view of a sig's data when the memtable has been refilled after a flush. It's the only bug in the storage *cache* layer so far.
- The bug compounds with `simulation-service-run-discards-runconfig`: that bug stamps each tick with a different `run_id`, and the cached read filters by `run_id` (line 152). After a flush, the memtable contains only the most recent tick's rows (run_id = tick_N_uuid). The disk has prior ticks' rows under different run_ids. The cached read filters by *one* run_id, so even if the bug were fixed (Fix A), the user would still only see one tick's worth of data via `simulation_service.run`. The two bugs together obscure the cache layer behavior in spectacular ways.
- The sync world (`SyncWorld`) does not currently have a cache wrapper (`grep -rn "CachedStore" src/archetype/core/sync/` returns no matches). The bug is async-only — for now.
- A follow-up worth a separate hunt: `_background_flush_sig` holds `self._flush_lock` only during the `_build_arrow_table` step (`async_cached_store.py:111-114`). The actual `inner.append(sig, df)` and `mem.clear()` happen *outside* the lock. If two concurrent flushes for the same sig race, they could double-write to disk and double-clear the memtable. Probably benign in single-event-loop asyncio (no preemption inside a function), but the lock scope is suspicious.
