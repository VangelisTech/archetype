# Bug Report: `StorageService.shutdown` aborts on the first store that raises — subsequent stores are never shut down and `_instances`/`_locks` are never cleared

**Date:** 2026-04-11
**Severity:** Medium-High (silent resource leak on `archetype serve` shutdown when any pooled store has trouble closing; partial shutdown leaves the service in an inconsistent state)
**Affects:** `archetype.app.storage_service.StorageService.shutdown` — every `ServiceContainer.shutdown()` call where any one store has a flaky shutdown
**Discovered by:** Overnight bug hunt

## Summary

`StorageService.shutdown` (`storage_service.py:77-86`) iterates `self._instances.values()` and calls each store's `shutdown` method without a `try/except`:

```python
async def shutdown(self):
    """Gracefully shuts down all managed storage backends."""
    for store, _, _ in self._instances.values():
        if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
            await store.shutdown()
        elif hasattr(store, "shutdown"):
            store.shutdown()

    self._instances.clear()
    self._locks.clear()
```

If a single store's `shutdown` raises (file lock contention, network error during a flush, a bug in `AsyncCachedStore._background_flush_sig`, an iceberg catalog write error, anything), the loop aborts. **Every subsequent store is never shut down**, `self._instances.clear()` is never called, and `self._locks.clear()` is never called. The service is in a partial shutdown: some stores are closed, some are still open, the multiton pool dict still has every entry, and the locks dict is unchanged.

A second call to `shutdown()` would re-iterate over the same dict — including the failing store — and abort again at the same point. The remaining stores are unreachable through the public API.

## Impact

1. **Silent resource leak on every flaky shutdown.** A long-running `archetype serve` process that hits any kind of shutdown error (a single failing flush, a network blip while closing a remote backend, a stuck file handle) leaks every store created after the failing one. For a workload with many fork worlds (each potentially creating a new pool entry), even one failing flush during shutdown can leave dozens of stores open.
2. **`AsyncCachedStore.shutdown` is the most likely failure source.** The cache's shutdown does a final flush of every pending memtable (`async_cached_store.py:179-198`). If the inner store's `append` fails during that final flush — for any reason — the cache's `shutdown` raises, and `StorageService.shutdown`'s outer loop aborts. Every store after the failing cache is never shut down.
3. **`ServiceContainer.shutdown` is called from FastAPI's `lifespan` context manager** (`api/app.py:14-24`). A shutdown failure during `lifespan.__aexit__` may surface as an opaque error in uvicorn's shutdown sequence, with no signal to the operator about which stores were leaked. The operator restarts the server thinking it shut down cleanly; resources are still held.
4. **`_locks` is also leaked.** `_locks` accumulates over the process lifetime as new (uri, namespace) pairs are seen. After a partial shutdown, the locks dict is unchanged, so the next `get_backend` call may try to acquire a stale `asyncio.Lock` whose internal state is still `locked=True` if the failing shutdown was holding it. (In practice, the locks are only used as gates around `_create_backend`, which is sync — so this is mostly a memory leak rather than a lockup. But the principle is the same.)
5. **Compounds with the just-filed `remove-world-leaks-broker-state` bug.** If a workload destroys many worlds (each leaking broker state), and *also* hits a shutdown failure, the leaked broker state is never released either — `ServiceContainer.shutdown` doesn't clear the broker, and the partial storage shutdown leaves the world references around. Two shutdown leaks compound.
6. **The fix is one block of code.** Wrap the per-store shutdown in `try/except` and log + continue. Same shape as the standard "drain all" pattern.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit c6d492e, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: a single failing store aborts the StorageService shutdown loop."""
import asyncio
import tempfile

from archetype.app.storage_service import StorageService


class FailingStore:
    async def shutdown(self):
        raise RuntimeError("simulated shutdown failure")


class CountingStore:
    def __init__(self):
        self.shutdown_called = False

    async def shutdown(self):
        self.shutdown_called = True


async def main() -> None:
    ss = StorageService()
    failing = FailingStore()
    counting = CountingStore()
    # Order matters: failing comes first in iteration order.
    ss._instances["fail::ns"] = (failing, None, None)
    ss._instances["ok::ns"] = (counting, None, None)

    print(f"before shutdown: _instances = {sorted(ss._instances.keys())}")
    try:
        await ss.shutdown()
    except RuntimeError as e:
        print(f"shutdown raised as expected: {e}")
    print(f"after shutdown:  _instances = {sorted(ss._instances.keys())}")
    print(f"counting.shutdown_called = {counting.shutdown_called}")
    assert counting.shutdown_called, "second store was never shut down"
    assert ss._instances == {}, "_instances was never cleared"


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
before shutdown: _instances = ['fail::ns', 'ok::ns']
shutdown raised as expected: simulated shutdown failure
after shutdown:  _instances = ['fail::ns', 'ok::ns']
counting.shutdown_called = False
AssertionError: second store was never shut down
```

The failing store's exception propagates out of the loop. The counting store's `shutdown` is never called. `_instances` is not cleared — the dict still contains both entries. A second `shutdown()` call would re-iterate and abort at the same store.

### Baseline (proves the bug is the missing try/except, not a fundamental shutdown design issue)

When no store fails, the loop drains every store and clears `_instances` correctly:

```python
ss = StorageService()
a = CountingStore()
b = CountingStore()
ss._instances["a::ns"] = (a, None, None)
ss._instances["b::ns"] = (b, None, None)

await ss.shutdown()
# a.shutdown_called = True
# b.shutdown_called = True
# _instances = []
# OK (baseline): all stores shut down, _instances cleared.
```

The shutdown design is correct for the happy path. The bug is purely the missing fault tolerance.

## Root cause

`src/archetype/app/storage_service.py:77-86`:

```python
async def shutdown(self):
    """Gracefully shuts down all managed storage backends."""
    for store, _, _ in self._instances.values():
        if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
            await store.shutdown()
        elif hasattr(store, "shutdown"):
            store.shutdown()

    self._instances.clear()
    self._locks.clear()
```

The loop has no exception handling. The post-loop cleanup (`_instances.clear()`, `_locks.clear()`) is a single statement that runs after the loop completes — if the loop aborts via exception, the cleanup never runs.

Trace for the MRE:

1. `await ss.shutdown()` enters the function.
2. `for store, _, _ in self._instances.values():` — iteration order is `[(failing, None, None), (counting, None, None)]` (insertion order in CPython 3.7+).
3. First iteration: `store = failing`. `asyncio.iscoroutinefunction(failing.shutdown)` is True. `await failing.shutdown()` raises `RuntimeError("simulated shutdown failure")`.
4. The exception propagates out of the `for` loop without entering the second iteration.
5. Lines 85-86 (`self._instances.clear()`, `self._locks.clear()`) are NOT executed.
6. The exception propagates out of `shutdown()`, out of `ServiceContainer.shutdown()` (which only calls `storage_service.shutdown` and has no try/except), out of FastAPI's `lifespan` context manager.
7. `_instances` still contains both entries. `counting.shutdown_called` is `False`. `_locks` is unchanged.

The standard pattern for "drain everything safely" is to wrap each iteration in try/except:

```python
for store, _, _ in self._instances.values():
    try:
        if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
            await store.shutdown()
        elif hasattr(store, "shutdown"):
            store.shutdown()
    except Exception as e:
        logger.error(f"Failed to shut down store {store}: {e}")
```

This is missing.

## Why existing tests miss this

`tests/core/test_resources_shutdown_sync.py:13-31::test_storage_service_shutdown_calls_sync_shutdown`:

```python
class FakeSyncStore:
    def shutdown(self):
        self.called = True


@pytest.mark.asyncio
async def test_storage_service_shutdown_calls_sync_shutdown(monkeypatch, tmp_path):
    """Shutdown should call sync shutdown() on stores that are not async."""
    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        async def fake_get_backend(storage_config, cache_config=None):
            store = FakeSyncStore()
            querier = object()
            updater = object()
            svc._instances[f"{cfg.uri}::{cfg.namespace}"] = (store, querier, updater)
            return store, querier, updater

        svc.get_backend = fake_get_backend  # type: ignore
        await svc.get_backend(cfg)
        await svc.shutdown()
    finally:
        svc._instances.clear()
        svc._locks.clear()
```

This test:

1. Sets up exactly **one** store.
2. Has it succeed cleanly.
3. Verifies sync `shutdown()` is called.

It does NOT:

- Test multiple stores in the same `_instances` dict.
- Test what happens when one store's `shutdown` raises.
- Verify that a failing first store doesn't block subsequent stores.
- Verify that `_instances`/`_locks` are cleared even on failure.

The test's `finally` clause manually clears `_instances` and `_locks`, which masks the bug — even if the inner shutdown raised, the manual cleanup would hide the leak.

`grep -rn "shutdown.*raise\|FailingStore\|shutdown_called" tests/` returns no test that exercises the failure path.

## Suggested fixes

**Fix A — wrap each per-store shutdown in `try/except` and continue.** The minimal correct fix:

```diff
 async def shutdown(self):
     """Gracefully shuts down all managed storage backends."""
+    errors: list[Exception] = []
     for store, _, _ in self._instances.values():
-        if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
-            await store.shutdown()
-        elif hasattr(store, "shutdown"):
-            store.shutdown()
+        try:
+            if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
+                await store.shutdown()
+            elif hasattr(store, "shutdown"):
+                store.shutdown()
+        except Exception as e:
+            logger.error(f"Failed to shut down store {store}: {e}")
+            errors.append(e)

     self._instances.clear()
     self._locks.clear()
+
+    # Surface aggregate failure to the caller after best-effort cleanup.
+    if errors:
+        raise RuntimeError(
+            f"StorageService.shutdown failed for {len(errors)} store(s); "
+            f"first error: {errors[0]}"
+        )
```

This makes shutdown best-effort: every store gets a chance, the dicts are always cleared, and the aggregate failure is reported to the caller after the cleanup completes. Lands in `app/`, requires no `core/` approval.

**Fix B — try/finally just for the dict cleanup.** Smaller change, but doesn't continue draining on a single failure:

```diff
 async def shutdown(self):
     """Gracefully shuts down all managed storage backends."""
-    for store, _, _ in self._instances.values():
-        if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
-            await store.shutdown()
-        elif hasattr(store, "shutdown"):
-            store.shutdown()
-
-    self._instances.clear()
-    self._locks.clear()
+    try:
+        for store, _, _ in self._instances.values():
+            if asyncio.iscoroutinefunction(getattr(store, "shutdown", None)):
+                await store.shutdown()
+            elif hasattr(store, "shutdown"):
+                store.shutdown()
+    finally:
+        self._instances.clear()
+        self._locks.clear()
```

Fix B clears `_instances`/`_locks` even on failure but does NOT keep draining stores after the first failure. The remaining stores are dropped from the dict (so they can be GC'd) but never explicitly shut down — their internal cleanup runs only when Python collects them (which may never happen if there are circular references). Fix A is the better shape because it lets every store run its cleanup.

**Fix C — `asyncio.gather(..., return_exceptions=True)`.** For async stores only, use `gather` to drain everything in parallel and collect errors:

```python
async def shutdown(self):
    async_stores = [store for store, _, _ in self._instances.values()
                    if asyncio.iscoroutinefunction(getattr(store, "shutdown", None))]
    sync_stores = [store for store, _, _ in self._instances.values()
                   if not asyncio.iscoroutinefunction(getattr(store, "shutdown", None))
                   and hasattr(store, "shutdown")]

    results = await asyncio.gather(*(s.shutdown() for s in async_stores), return_exceptions=True)
    for s in sync_stores:
        try:
            s.shutdown()
        except Exception as e:
            logger.error(f"Failed to shut down sync store {s}: {e}")

    errors = [r for r in results if isinstance(r, Exception)]
    self._instances.clear()
    self._locks.clear()
    if errors:
        logger.error(f"{len(errors)} async store(s) failed to shut down")
```

Fix C is faster (parallel) but slightly larger. Good for production where shutdown latency matters. Fix A is the cleanest minimal patch.

I'd recommend **Fix A** for the urgent fix, with **Fix C** as a follow-up if shutdown latency becomes a concern.

## Suggested regression tests

Add to `tests/core/test_resources_shutdown_sync.py`:

```python
@pytest.mark.asyncio
async def test_storage_service_shutdown_continues_after_first_store_failure():
    """Regression: a single failing store must not abort the shutdown loop."""
    svc = StorageService()
    try:
        class FailingStore:
            async def shutdown(self):
                raise RuntimeError("simulated failure")

        class CountingStore:
            def __init__(self):
                self.shutdown_called = False

            async def shutdown(self):
                self.shutdown_called = True

        failing = FailingStore()
        counting = CountingStore()
        svc._instances["fail::ns"] = (failing, None, None)
        svc._instances["ok::ns"] = (counting, None, None)

        # Shutdown may surface the aggregate error, but every store must
        # still be drained and _instances must be cleared.
        with pytest.raises((RuntimeError, ExceptionGroup)):
            await svc.shutdown()

        assert counting.shutdown_called, (
            "shutdown aborted on the first failing store; subsequent stores leaked"
        )
        assert svc._instances == {}, "_instances was not cleared after shutdown"
    finally:
        svc._instances.clear()
        svc._locks.clear()


@pytest.mark.asyncio
async def test_storage_service_shutdown_failing_in_middle_drains_all():
    """Regression: a failing store in the MIDDLE of the iteration order
    must not block the stores that come after it."""
    svc = StorageService()
    try:
        class CountingStore:
            def __init__(self):
                self.shutdown_called = False

            async def shutdown(self):
                self.shutdown_called = True

        class FailingStore:
            async def shutdown(self):
                raise RuntimeError("middle failure")

        first = CountingStore()
        middle = FailingStore()
        last = CountingStore()
        svc._instances["a::ns"] = (first, None, None)
        svc._instances["b::ns"] = (middle, None, None)
        svc._instances["c::ns"] = (last, None, None)

        with pytest.raises((RuntimeError, ExceptionGroup)):
            await svc.shutdown()

        assert first.shutdown_called
        assert last.shutdown_called, (
            "store after the failing one in iteration order leaked"
        )
        assert svc._instances == {}
    finally:
        svc._instances.clear()
        svc._locks.clear()
```

Both tests fail on `main` at `assert counting.shutdown_called` (the failing store aborted the loop before reaching the counting store).

## Notes / scope

- Affects `src/archetype/app/storage_service.py:77-86`. This is in `app/`, not `core/`, so the fix can land directly.
- Distinct from the twenty other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - The four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota accounting.
  - `component-prefix-collision`, `component-get-type-by-name-no-recurse`, `cached-store-read-shadows-disk` are Component / cache.
  - `create-world-name-collision-orphan` and `world-id-none-divergence` are `create_world` bugs.
  - `daily-tokens-never-reset` is the missing daily quota scheduler.
  - `storage-pool-key-ignores-cache-and-backend` is the multiton key.
  - `system-execute-strips-var-keyword-kwargs` is the processor kwargs filter.
  - `remove-world-leaks-broker-state` is the broker leak on world destroy.
  - This bug is about the *shutdown* path itself failing partially. None of the previous bugs are about shutdown fault tolerance.
- `ServiceContainer.shutdown` (`container.py:53-55`) only calls `storage_service.shutdown` — it has no own try/except. So a partial shutdown failure here propagates straight through to FastAPI's lifespan context.
- The same fail-hard pattern exists in `world_service.shutdown` (`world_service.py:49-53`): it calls `self.storage_service.shutdown()` then clears `_worlds`/`_world_names`. If storage_service.shutdown raises, the worlds dict is never cleared either. The fix should be applied symmetrically to `world_service.shutdown`.
- After Fix A, the broker should also be wrapped in shutdown for the same fault tolerance: today `ServiceContainer.shutdown` doesn't touch the broker at all (the leak per the just-filed `remove-world-leaks-broker-state` report), but if a future container.shutdown does call broker.clear, the same try/except pattern should apply there.
