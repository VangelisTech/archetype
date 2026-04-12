# Bug Report: `StorageService.get_backend` pools by `(uri, namespace)` only — silently returns the wrong wrapper when `cache_config` or `backend` differs between callers

**Date:** 2026-04-11
**Severity:** Medium-High (silent return of mismatched store wrappers; subsequent callers get a backend with semantics they did not request)
**Affects:** `archetype.app.storage_service.StorageService.get_backend` — every world that shares a (uri, namespace) with another world but has different cache or backend requirements
**Discovered by:** Overnight bug hunt

## Summary

`StorageService.get_backend` (`storage_service.py:34-52`) pools backend triplets in a multiton, keyed by `f"{storage_config.uri}::{storage_config.namespace}"` (`storage_service.py:42`). The key omits **`cache_config`** and **`storage_config.backend`** entirely. The first caller wins: whichever cache and backend choices land in the pool first are silently inherited by every subsequent caller using the same (uri, namespace). A second caller that explicitly passes `cache_config=None` and gets back an `AsyncCachedStore`, or passes `backend=LANCEDB` and gets back an `AsyncStore` (Iceberg), has no signal that their request was ignored.

This is a multiton-key bug. The pool is supposed to share *interchangeable* triplets — but the triplets are not interchangeable when their cache/backend wrappers differ in semantics.

## Impact

1. **Cache opt-in / opt-out is non-deterministic across callers.** A test that creates one world with `CacheConfig(...)` and another with `cache_config=None` (e.g., to compare cached vs uncached read latency) gets the same backend object back from both calls — both are cached. The second test silently runs with the cache it didn't ask for. Performance experiments are invalidated.
2. **Backend choice (Iceberg vs LanceDB) is non-deterministic across callers.** The MRE swap is even more dangerous: a caller that explicitly opts into LanceDB after another caller has already initialised Iceberg at the same path gets an `AsyncStore` back. The user's `backend=StorageBackend.LANCEDB` flag is silently ignored. They write LanceDB-shaped queries against an Iceberg-backed store and get type errors at the LanceDB layer (or worse — silent semantic mismatches if the operations happen to be compatible).
3. **`fork_world` is exposed.** `WorldService.fork_world` (`world_service.py:131-237`) calls `create_world` with the user-supplied `storage_config` and `cache_config`. If the source world used a cache and the fork doesn't (or vice versa), the fork silently gets the source's wrapper. Forks that were supposed to be uncached for write-amplification testing are quietly cached.
4. **The bug compounds with the filed `cached-store-read-shadows-disk` report.** That bug makes cached reads return only the memtable when both memtable and disk have data. Any caller that *thinks* they opted out of the cache here actually gets the cache *and* the read shadow — silent partial reads on a backend they explicitly asked not to cache.
5. **Discovery is silent.** The pool returns the cached triplet identity-equal to the first call. A type assertion downstream (`isinstance(store, AsyncCachedStore)`) catches it, but no in-repo test does that assertion. The wrong-wrapper return is invisible to the rest of the codebase.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 632e172, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: get_backend pool key omits cache_config; second caller gets the
first caller's cached wrapper even when explicitly passing cache_config=None."""
import asyncio
import tempfile

from archetype.app.storage_service import StorageService
from archetype.core.aio import AsyncCachedStore
from archetype.core.config import CacheConfig, StorageConfig


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ss = StorageService()
        cfg = StorageConfig(uri=tmp, namespace="ns")

        # First caller: explicit cache.
        store_a, _, _ = await ss.get_backend(cfg, cache_config=CacheConfig(idle_sec=999))
        print(f"first call  type = {type(store_a).__name__}")
        assert isinstance(store_a, AsyncCachedStore)

        # Second caller: same uri+ns, but cache_config=None — wants no cache.
        store_b, _, _ = await ss.get_backend(cfg, cache_config=None)
        print(f"second call type = {type(store_b).__name__}")
        print(f"same instance? {store_a is store_b}")

        assert not isinstance(store_b, AsyncCachedStore), (
            f"BUG: cache_config=None caller got an AsyncCachedStore from the pool"
        )

        await ss.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
first call  type = AsyncCachedStore
second call type = AsyncCachedStore
same instance? True
AssertionError: BUG: cache_config=None caller got an AsyncCachedStore from the pool
```

The second caller passed `cache_config=None`, expecting an uncached store. They received the *same* `AsyncCachedStore` instance the first caller got.

### Baseline (proves the bug is the missing key components)

When the (uri, namespace) keys differ, the pool correctly produces distinct backends with the right wrappers:

```python
cfg_a = StorageConfig(uri=tmp, namespace="ns_a")
cfg_b = StorageConfig(uri=tmp, namespace="ns_b")

store_a, _, _ = await ss.get_backend(cfg_a, cache_config=CacheConfig(idle_sec=999))
store_b, _, _ = await ss.get_backend(cfg_b, cache_config=None)

# store_a type = AsyncCachedStore, store_b type = AsyncLancedbStore
# same instance? False
# OK (baseline): distinct namespaces → distinct (correctly typed) backends.
```

The pool *does* honour cache_config and backend correctly when the key differs. The bug fires only when two callers share (uri, namespace) but disagree on the wrapper.

## Root cause

`src/archetype/app/storage_service.py:34-52`:

```python
async def get_backend(
    self,
    storage_config: StorageConfig,
    cache_config: CacheConfig | None = None,
) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
    """
    Retrieves or creates a shared backend triplet for the given storage config.
    """
    key = f"{storage_config.uri}::{storage_config.namespace}"

    if key not in self._instances:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        async with self._locks[key]:
            if key not in self._instances:
                self._instances[key] = self._create_backend(storage_config, cache_config)

    return self._instances[key]
```

The key string only depends on `uri` and `namespace`. `cache_config` and `storage_config.backend` (which determines whether `_create_backend` produces an `AsyncStore` or an `AsyncLancedbStore`) do not appear anywhere in the cache key. So:

- First call: `key = "/tmp/x::ns"`. `_instances[key]` doesn't exist. `_create_backend(cfg_with_cache, CacheConfig(...))` builds an `AsyncStore` (or `AsyncLancedbStore`), then wraps it in `AsyncCachedStore`. Stored at `_instances["/tmp/x::ns"]`.
- Second call: `key = "/tmp/x::ns"`. `_instances[key]` already exists. **Return the cached entry without consulting `cache_config`**. The second caller gets the first caller's wrapped tuple, regardless of what they passed.

`_create_backend` (`storage_service.py:54-75`) DOES use both `cache_config` and `storage_config.backend`:

```python
def _create_backend(
    self,
    storage_config: StorageConfig,
    cache_config: CacheConfig | None,
) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
    context = StorageContextFactory.build(storage_config)
    store: iAsyncStore
    if storage_config.use_lancedb:
        store = AsyncLancedbStore(context)
    else:
        store = AsyncStore(context)

    if isinstance(cache_config, bool):
        cache_config = CacheConfig() if cache_config else None

    if cache_config:
        store = AsyncCachedStore(async_store=store, cache_config=cache_config)
    ...
```

Both branches (`use_lancedb` and `cache_config`) produce semantically different wrappers. But they only run on the *first* call for a given (uri, namespace). Subsequent calls bypass `_create_backend` entirely and return whatever the first call produced.

Trace for the MRE:

1. First call: `ss.get_backend(cfg, cache_config=CacheConfig(...))`.
   - `key = "/tmp/.../::ns"`.
   - `key not in _instances` → enter the `async with self._locks[key]:` branch.
   - `_create_backend(cfg, CacheConfig(...))` → builds `AsyncStore(context)` (since `use_lancedb` is True by default, it's actually `AsyncLancedbStore(context)`), wraps in `AsyncCachedStore`, returns the triplet.
   - `_instances[key] = (cached_store, querier, updater)`.
2. Second call: `ss.get_backend(cfg, cache_config=None)`.
   - `key = "/tmp/.../::ns"` (same).
   - `key in _instances` → return `_instances[key]` directly. **`_create_backend` is never called, the new `cache_config=None` is dropped on the floor.**
   - Return the cached triplet from the first call.
3. Caller has an `AsyncCachedStore` despite asking for `cache_config=None`. The wrapping is invisible until they hit `cached-store-read-shadows-disk` or do a `isinstance` check downstream.

## Why existing tests miss this

`tests/core/test_resources_manager.py::test_backend_selection_between_default_and_lancedb` (line 43-62) tests the backend selection but uses **different (uri, namespace)** for each backend type:

```python
cfg_default = StorageConfig(uri=str(tmp_path / "store_default"), namespace="ns_default", ...)
cfg_lance = StorageConfig(uri=str(tmp_path / "store_lance"), namespace="ns_lance", ...)
```

So the test never exercises the "same key, different backend" path. The pool key happens to be unique for each call.

`tests/core/test_resources_manager.py::test_storage_service_cache_wrapper` (line 28-40) tests that the cache wrapper is applied on the *first* call but does not make a second call with a different `cache_config` to verify the pool doesn't hand out a stale wrapper.

`grep -rn "get_backend.*cache" tests/` returns no test that calls `get_backend` twice with the same (uri, namespace) and different `cache_config` / `backend` parameters.

The closest existing test (`test_multiton_concurrent_calls_return_same_instances`, line 65-82) verifies that **identical** calls return the same instance — which is the correct behaviour. But it doesn't check that **non-identical** calls (in cache_config or backend) return *different* instances when they should.

## Suggested fixes

**Fix A — include cache_config and backend in the pool key.** The minimal correct fix:

```diff
 async def get_backend(
     self,
     storage_config: StorageConfig,
     cache_config: CacheConfig | None = None,
 ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]:
     """
     Retrieves or creates a shared backend triplet for the given storage config.
     """
-    key = f"{storage_config.uri}::{storage_config.namespace}"
+    cache_key = ""
+    if cache_config is not None:
+        cache_key = (
+            f"::cache(rows={cache_config.flush_rows},"
+            f"mb={cache_config.flush_mb},"
+            f"global={cache_config.global_mb},"
+            f"idle={cache_config.idle_sec})"
+        )
+    key = (
+        f"{storage_config.uri}"
+        f"::{storage_config.namespace}"
+        f"::backend={storage_config.backend.value}"
+        f"{cache_key}"
+    )
     ...
```

This produces distinct keys for distinct (uri, namespace, backend, cache_config) tuples, so the pool correctly creates a fresh triplet for each unique combination. The downside: many small variations in `CacheConfig` produce many pool entries — but `CacheConfig` is small and hashable, so the pool cost is bounded.

A cleaner shape using a hash of the relevant fields:

```diff
+import hashlib
+import json
+
+def _backend_key(storage_config, cache_config):
+    parts = {
+        "uri": str(storage_config.uri),
+        "namespace": storage_config.namespace,
+        "backend": storage_config.backend.value,
+    }
+    if cache_config is not None:
+        parts["cache"] = cache_config.model_dump()
+    return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]
+
 async def get_backend(self, storage_config, cache_config=None):
-    key = f"{storage_config.uri}::{storage_config.namespace}"
+    key = _backend_key(storage_config, cache_config)
     ...
```

**Fix B — raise on conflict instead of silently sharing.** If the team wants the pool to be strict-by-(uri, namespace) (so only one wrapper config can exist per (uri, namespace) for the lifetime of the process), then the second caller should get a clear error:

```diff
 async def get_backend(self, storage_config, cache_config=None):
     key = f"{storage_config.uri}::{storage_config.namespace}"
+    requested_wrapper = (storage_config.backend, cache_config is not None)
     if key not in self._instances:
         ...
         async with self._locks[key]:
             if key not in self._instances:
                 self._instances[key] = self._create_backend(storage_config, cache_config)
+                self._wrapper_keys[key] = requested_wrapper
+    elif self._wrapper_keys.get(key) != requested_wrapper:
+        raise ValueError(
+            f"Backend at {storage_config.uri}::{storage_config.namespace} was already "
+            f"created with wrapper {self._wrapper_keys[key]}; cannot reopen with "
+            f"{requested_wrapper}. Use a different namespace or close the existing backend first."
+        )
     return self._instances[key]
```

Fix B is more conservative — it surfaces the inconsistency loudly. Fix A is more permissive — it lets multiple wrappers coexist for the same (uri, namespace) by giving each a separate triplet. **Fix A is the right shape**: the pool's purpose is to share storage-level state (the iceberg session, the lance directory handle), not to enforce a single wrapper per path.

A subtlety: the underlying inner store (Iceberg/LanceDB) should still be shared across pool entries with the same (uri, namespace) — only the wrappers differ. Fix A as written creates fully-fresh inner stores for each unique cache_config. A more sophisticated implementation would key the *inner* store by (uri, namespace, backend) and the *wrapped* triplet by (uri, namespace, backend, cache_config), reusing the inner store across wrappers. That's a larger refactor; out of scope for the urgent fix.

## Suggested regression tests

Add to `tests/core/test_resources_manager.py`:

```python
@pytest.mark.asyncio
async def test_get_backend_pool_distinguishes_cache_config(tmp_path):
    """Regression: two callers with the same (uri, namespace) but different
    cache_config must get distinct (correctly-wrapped) backends."""
    from archetype.core.aio import AsyncCachedStore

    svc = StorageService()
    try:
        cfg = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

        store_cached, _, _ = await svc.get_backend(
            cfg, cache_config=CacheConfig(idle_sec=999)
        )
        store_uncached, _, _ = await svc.get_backend(cfg, cache_config=None)

        assert isinstance(store_cached, AsyncCachedStore), (
            "first call should be cached"
        )
        assert not isinstance(store_uncached, AsyncCachedStore), (
            f"second call asked for cache_config=None but got {type(store_uncached).__name__}"
        )
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_get_backend_pool_distinguishes_backend_choice(tmp_path):
    """Regression: two callers with the same (uri, namespace) but different
    backend must get distinct backend instances."""
    from archetype.core.aio import AsyncStore
    from archetype.core.config import StorageBackend
    from archetype.core.storage import AsyncLancedbStore

    svc = StorageService()
    try:
        cfg_iceberg = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.ICEBERG,
        )
        cfg_lance = StorageConfig(
            uri=str(tmp_path / "store"),
            namespace="ns",
            backend=StorageBackend.LANCEDB,
        )

        store_iceberg, _, _ = await svc.get_backend(cfg_iceberg)
        store_lance, _, _ = await svc.get_backend(cfg_lance)

        assert isinstance(store_iceberg, AsyncStore), (
            f"first call should be Iceberg, got {type(store_iceberg).__name__}"
        )
        assert isinstance(store_lance, AsyncLancedbStore), (
            f"second call should be LanceDB, got {type(store_lance).__name__}"
        )
    finally:
        await svc.shutdown()
```

Both tests fail on `main` — the first because the second `get_backend` returns the cached store from the first call; the second because the second `get_backend` returns the Iceberg store from the first call.

## Notes / scope

- Affects `src/archetype/app/storage_service.py:42`. This is in `app/`, not `core/`, so the fix can land directly.
- Distinct from the sixteen other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota debit on partial failure.
  - `component-prefix-collision` and `component-get-type-by-name-no-recurse` are Component registration.
  - `cached-store-read-shadows-disk` is the cache hiding flushed rows.
  - `create-world-name-collision-orphan` is the orphan world leak.
  - `daily-tokens-never-reset` is the missing daily quota scheduler.
  - This bug is in the *storage pool key* layer — a multiton key that's missing two of its dimensions.
- Compounds with `cached-store-read-shadows-disk`: a caller that opts out of the cache here silently gets a cached store *and* the cached store hides flushed rows. Two bugs in one read path.
- The pool is global to the `StorageService` instance, which is global to `ServiceContainer`. There is one StorageService per process (per the single-process model). So the bug is per-process, not per-call.
- A small follow-up worth a separate hunt: `_locks` is also keyed by the same `(uri, namespace)` string. After Fix A, the locks should match the new key shape, otherwise two distinct backend keys could share a lock and serialize unnecessarily.
