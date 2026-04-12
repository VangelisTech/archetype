# Bug Report: `WorldService.create_world` inserts the new world into `_worlds` *before* checking the name collision — failed attempts leak orphaned worlds forever

**Date:** 2026-04-11
**Severity:** Medium-High (silent memory + storage-handle leak on every failed `create_world` name-collision attempt; trivially exploitable from the REST `POST /worlds` endpoint)
**Affects:** `archetype.app.world_service.WorldService.create_world` — every caller that tries to create a world with a name that's already taken (REST, CLI, broker lifecycle, direct in-process)
**Discovered by:** Overnight bug hunt

## Summary

`WorldService.create_world` (`world_service.py:55-93`) creates the world via the factory, injects the broker into its resources, and inserts it into `self._worlds[world.world_id]` (`world_service.py:83`) — and *then* checks whether `config.name` is already taken (`world_service.py:85-87`). When the name is taken, the function raises `ValueError("World with name '...' already exists.")` — but the world has *already* been added to `_worlds`, the storage backend has *already* been created (and pooled in `StorageService`), the broker has *already* been injected as a resource. The exception leaves all of that in place. The new world is unreachable by name (since `_world_names` was never updated), but it lives forever in `_worlds[wid]`, holding a fresh storage backend handle and a broker reference, until the process exits.

Each retry creates another orphan. The MRE submits four `create_world(name="taken")` calls in a row: the first succeeds, the next three each raise — and `_worlds` ends up with 4 entries while `_world_names` has 1. `len(_worlds) - len(_world_names) == 3` orphans, all with `name="taken"`, all holding live storage handles, none reachable through the public API.

## Impact

1. **Silent memory and storage-handle leak on every duplicate-name `create_world` call.** A REST client that retries `POST /worlds` with the same body — a perfectly reasonable thing to do on a 500 timeout or a network blip — leaks one orphan world per retry. There is no upper bound. `archetype serve` accumulates orphans for the lifetime of the process.
2. **Storage backends are pooled by `(uri, namespace)`** in `StorageService` (`storage_service.py:42`), so the orphan worlds typically *share* a backend with the legitimate world (same uri/namespace). This means each orphan is "cheap" in terms of file handles — but each one still holds a `Resources` container, an `_entity2sig` dict, an `_hooks` defaultdict, an `_live` dict, references to the querier/updater/system, and a Daft Session attachment. None of that is reclaimed.
3. **Trivially reachable from `POST /worlds`.** The REST handler in `api/routes/worlds.py:24-47` builds a `Command(type=CREATE_WORLD, payload={...})`, calls `cs.submit("__global__", cmd, ctx)`, and then `cs.apply_world_lifecycle(cmd)`. `apply_world_lifecycle` for `CREATE_WORLD` (`command_service.py:121-130`) calls `self._world_service.create_world(...)`. The leak fires inside that call. The REST client gets a 500 (since the FastAPI handler doesn't wrap `create_world`'s `ValueError` in an HTTPException); the orphan stays.
4. **The leak compounds with the already-filed `lifecycle-commands-leak-broker` report.** Every duplicate-name REST attempt also leaks one zombie command into `broker._pending` and `broker._queues["__global__"]` (per the lifecycle bug). So a workload that retries N times burns N orphan worlds + N zombie commands. A small misconfiguration cascades into a real memory pressure problem.
5. **Discovery is invisible at the test layer.** The existing `test_world_service_duplicate_name_raises` test (`tests/core/test_orchestrator_errors_and_instrumentation.py:9-19`) catches the `ValueError` and stops. It does *not* check `len(_worlds)` afterwards. The orphan leak is uncovered by every test in the suite.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 4e7fffc, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: WorldService.create_world leaves orphaned worlds in _worlds when
the name-collision check raises after the world has already been
inserted."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, WorldConfig


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            ws = container.world_service
            print(f"initial _worlds = {len(ws._worlds)}, _world_names = {len(ws._world_names)}")

            await ws.create_world(WorldConfig(name="taken"), StorageConfig(uri=tmp))
            print(f"after 1st: _worlds={len(ws._worlds)}, _world_names={len(ws._world_names)}")

            for i in range(3):
                try:
                    await ws.create_world(WorldConfig(name="taken"), StorageConfig(uri=tmp))
                except ValueError as e:
                    print(f"attempt {i + 2} raised: {e}")

            print(f"after 4 attempts: _worlds={len(ws._worlds)}, _world_names={len(ws._world_names)}")
            unreachable = [wid for wid in ws._worlds if wid not in ws._world_names.values()]
            print(f"orphaned worlds: {len(unreachable)}")
            for wid in unreachable:
                w = ws._worlds[wid]
                print(f"  {wid} name={getattr(w, 'name', None)!r}")
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
initial _worlds = 0, _world_names = 0
after 1st: _worlds=1, _world_names=1
attempt 2 raised: World with name 'taken' already exists.
attempt 3 raised: World with name 'taken' already exists.
attempt 4 raised: World with name 'taken' already exists.
after 4 attempts: _worlds=4, _world_names=1
orphaned worlds: 3
  019d7efe-b373-7972-a049-bee0e3565b05 name='taken'
  019d7efe-b373-7972-a049-bef650f32ffb name='taken'
  019d7efe-b373-7972-a049-bf02db8ded16 name='taken'
```

`_worlds` grew from 1 to 4 over three failed attempts. `_world_names` stayed at 1. Three orphaned worlds with `name="taken"` are stuck in `_worlds`, unreachable via `get_world_by_name("taken")` (which returns the legitimate one), unreachable via the REST `GET /worlds/{world_id}` (since the caller never received the orphan IDs), and never freed.

### Baseline (proves the leak is scoped to the name-collision raise path)

Distinct names create distinct worlds with no orphans:

```python
for name in ["a", "b", "c", "d"]:
    await ws.create_world(WorldConfig(name=name), StorageConfig(uri=tmp))

# _worlds=4, _world_names=4
# OK (baseline): distinct names create distinct worlds with no orphans.
```

`len(_worlds) == len(_world_names)` — every world is reachable by name. The bug fires only on the duplicate-name failure path.

## Root cause

`src/archetype/app/world_service.py:55-93`:

```python
async def create_world(
    self,
    config: WorldConfig,
    storage_config: StorageConfig,
    cache_config: CacheConfig | None = None,
    system: iAsyncSystem | None = None,
) -> iWorld:
    """
    Creates or retrieves a world based on the provided configuration.
    Idempotent: if a world_id already exists, returns the existing instance.
    Injects CommandBroker into world resources if available.
    """
    world_id = config.world_id or uuid7()

    if world_id in self._worlds:
        return self._worlds[world_id]

    world = await self.factory.create_world(
        world_config=config,
        storage_config=storage_config,
        cache_config=cache_config,
        system=system or AsyncSystem(),
    )

    # Inject broker into world resources for processor access
    if self._broker and isinstance(world, AsyncWorld) and hasattr(world, "resources"):
        world.resources.insert(self._broker)

    self._worlds[world.world_id] = world      # <-- INSERTED *before* the name check

    if config.name:
        if config.name in self._world_names:
            raise ValueError(f"World with name '{config.name}' already exists.")    # <-- raises with the world already in _worlds
        self._world_names[config.name] = world.world_id

    self._persist_entry(world, storage_config)
    self._attach_registry_sync(world)

    return world
```

Trace for the MRE:

1. `await ws.create_world(WorldConfig(name="taken"), StorageConfig(...))` first time:
   - `world_id` is the freshly-generated UUID from `WorldConfig.world_id` default factory.
   - `world_id not in self._worlds` → proceed.
   - `factory.create_world(...)` builds an `AsyncWorld`, allocates a storage backend in the pool.
   - `world.resources.insert(self._broker)` — broker injected.
   - `self._worlds[world.world_id] = world` — `_worlds == {wid_1: w1}`.
   - `"taken" not in self._world_names` → `self._world_names["taken"] = wid_1`.
   - `_persist_entry`, `_attach_registry_sync`.
   - Return `w1`. ✅
2. `await ws.create_world(WorldConfig(name="taken"), StorageConfig(...))` second time:
   - `WorldConfig` builds with a *new* `world_id` (default factory) — different from the first.
   - `world_id not in self._worlds` → proceed.
   - `factory.create_world(...)` builds an `AsyncWorld` `w2` (with the same storage URI, so the backend is **pooled** — same backend reference, but a brand new world instance with its own `_entity2sig`, `_hooks`, `resources`, etc.).
   - `w2.resources.insert(self._broker)`.
   - `self._worlds[wid_2] = w2` — `_worlds == {wid_1: w1, wid_2: w2}`.
   - `"taken" in self._world_names` → **`raise ValueError(...)`**.
   - Control returns to caller with `_worlds == {wid_1: w1, wid_2: w2}` and `_world_names == {"taken": wid_1}`.
   - `w2` is now unreachable: no name maps to it, the caller never received `wid_2`. It lives in `_worlds` forever.
3. Third and fourth attempts repeat the same pattern. `_worlds == {wid_1: w1, wid_2: w2, wid_3: w3, wid_4: w4}`, `_world_names == {"taken": wid_1}`.

The fundamental defect: the `_worlds` insertion at line 83 happens *before* the validity check at line 85-87. The function should either:
- Check name first, then insert, OR
- Roll back the insertion when the check fails.

Neither happens.

## Why existing tests miss this

`tests/core/test_orchestrator_errors_and_instrumentation.py:9-19::test_world_service_duplicate_name_raises`:

```python
@pytest.mark.asyncio
async def test_world_service_duplicate_name_raises(tmp_path):
    """Creating two worlds with the same name should raise to prevent ambiguous name lookups."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
        with pytest.raises(ValueError):
            await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
    finally:
        await ws.shutdown()
```

The test creates the first world, asserts the second raises, and then exits the `with` block. It never:

- Checks `len(ws._worlds)` after the failed second call.
- Verifies that the failed second call left `_worlds` and `_world_names` in agreement.
- Calls `ws.list_worlds()` to confirm only one world is reachable.

The leak is invisible because the assertion stops at the `ValueError`. After Fix A or Fix B (below), this test would still pass; the regression test below catches the leak.

`grep -rn "name.*already.*exists\|World with name\|name_collision" tests/` returns no other matches.

## Suggested fixes

**Fix A — check the name *before* inserting into `_worlds`.** The minimal correct fix:

```diff
 async def create_world(
     self,
     config: WorldConfig,
     storage_config: StorageConfig,
     cache_config: CacheConfig | None = None,
     system: iAsyncSystem | None = None,
 ) -> iWorld:
     """
     Creates or retrieves a world based on the provided configuration.
     Idempotent: if a world_id already exists, returns the existing instance.
     Injects CommandBroker into world resources if available.
     """
     world_id = config.world_id or uuid7()

     if world_id in self._worlds:
         return self._worlds[world.world_id]

+    # Validate name uniqueness BEFORE allocating a storage backend or
+    # constructing a world. Otherwise, a duplicate-name attempt leaks the
+    # half-built world into _worlds while the validity check raises.
+    if config.name and config.name in self._world_names:
+        raise ValueError(f"World with name '{config.name}' already exists.")
+
     world = await self.factory.create_world(
         world_config=config,
         storage_config=storage_config,
         cache_config=cache_config,
         system=system or AsyncSystem(),
     )

     if self._broker and isinstance(world, AsyncWorld) and hasattr(world, "resources"):
         world.resources.insert(self._broker)

     self._worlds[world.world_id] = world

     if config.name:
-        if config.name in self._world_names:
-            raise ValueError(f"World with name '{config.name}' already exists.")
         self._world_names[config.name] = world.world_id

     self._persist_entry(world, storage_config)
     self._attach_registry_sync(world)

     return world
```

This is the smallest patch that closes the bug entirely. The pre-check at the top means the storage backend is never even created on a name collision. There is also no risk of an in-flight world being orphaned because the failure happens *before* `factory.create_world(...)` runs.

**Fix B — try/except + rollback after the insertion.** Catches the bug at a different location and is slightly more defensive against future changes that move the name check:

```diff
 self._worlds[world.world_id] = world

 if config.name:
-    if config.name in self._world_names:
-        raise ValueError(f"World with name '{config.name}' already exists.")
-    self._world_names[config.name] = world.world_id
+    if config.name in self._world_names:
+        # Roll back the _worlds insertion before raising.
+        del self._worlds[world.world_id]
+        # NOTE: factory.create_world may have allocated a pooled backend.
+        # The pool is keyed by (uri, namespace) so we don't shut it down here
+        # — other worlds may still be using it. The world instance itself is
+        # garbage-collected when the local var `world` goes out of scope.
+        raise ValueError(f"World with name '{config.name}' already exists.")
+    self._world_names[config.name] = world.world_id
```

Fix B is one extra line but matches the "roll back partial state on failure" pattern from `enqueue-bulk-quota-debit-on-failure` (the just-filed broker quota bug). Fix A is structurally cleaner — the validity check is at the top of the function, where it can fail fast without doing any work. **Recommend Fix A**.

**Fix C (defence in depth) — make `create_world` fully transactional.** Use a two-phase commit pattern: build the world locally, validate ALL invariants, then commit by inserting into both dicts atomically:

```python
async def create_world(self, config, storage_config, cache_config=None, system=None):
    world_id = config.world_id or uuid7()
    if world_id in self._worlds:
        return self._worlds[world_id]
    # Pre-flight all checks — no side effects yet.
    if config.name and config.name in self._world_names:
        raise ValueError(f"World with name '{config.name}' already exists.")
    # Build (this is the expensive step, allocates a backend).
    world = await self.factory.create_world(...)
    if self._broker and isinstance(world, AsyncWorld):
        world.resources.insert(self._broker)
    # Commit (no checks, no failures past this point).
    self._worlds[world.world_id] = world
    if config.name:
        self._world_names[config.name] = world.world_id
    self._persist_entry(world, storage_config)
    self._attach_registry_sync(world)
    return world
```

Same as Fix A, just with the comment structure made explicit. Ship Fix A.

## Suggested regression tests

Add to `tests/core/test_orchestrator_errors_and_instrumentation.py` (right next to the existing `test_world_service_duplicate_name_raises`):

```python
@pytest.mark.asyncio
async def test_duplicate_name_create_does_not_leak_orphan_world(tmp_path):
    """Regression: a failed duplicate-name create_world must NOT leave a
    half-built world in _worlds."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)
        baseline_worlds = len(ws._worlds)
        baseline_names = len(ws._world_names)

        with pytest.raises(ValueError):
            await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        assert len(ws._worlds) == baseline_worlds, (
            f"create_world leaked an orphan world into _worlds: "
            f"baseline={baseline_worlds}, after={len(ws._worlds)}"
        )
        assert len(ws._world_names) == baseline_names
        # Every world in _worlds must be reachable via _world_names.
        unreachable = [wid for wid in ws._worlds if wid not in ws._world_names.values()]
        assert unreachable == [], f"orphaned worlds: {unreachable}"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_repeated_duplicate_name_creates_do_not_grow_worlds(tmp_path):
    """Regression: 100 retries of a failing duplicate-name create_world
    must not accumulate worlds."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        for _ in range(100):
            with pytest.raises(ValueError):
                await ws.create_world(WorldConfig(name="dup"), storage_config=storage)

        assert len(ws._worlds) == 1, (
            f"100 failed retries leaked {len(ws._worlds) - 1} orphan worlds"
        )
    finally:
        await ws.shutdown()
```

The first test fails on `main` at `assert len(ws._worlds) == baseline_worlds` (1 vs 2). The second fails at `assert len(ws._worlds) == 1` (1 vs 101). Both pass after Fix A.

## Notes / scope

- Affects `src/archetype/app/world_service.py:55-93`. This is in `app/`, not `core/`, so the fix can land directly without `core/` approval.
- Distinct from the fourteen other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is about hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is about quota accounting during partial validation failure.
  - `component-prefix-collision` and `component-get-type-by-name-no-recurse` are about Component registration.
  - `cached-store-read-shadows-disk` is about the cache hiding flushed rows.
  - This bug is a sibling of `enqueue-bulk-quota-debit-on-failure`: same shape (mutation before validation, partial state left behind on raise), different layer (world service vs broker quota). The recommended fix shape is also the same: check first, mutate later.
- The leak compounds with `lifecycle-commands-leak-broker`: every duplicate-name `POST /worlds` request loses one orphan world *and* one zombie broker command. A REST client retry loop on a misconfigured workload accumulates both.
- The same pre-validate-then-mutate pattern should be audited across `world_service.py`. `remove_world` (`world_service.py:121-129`) does the right thing — it checks `if world_id in self._worlds:` before mutating, and idempotently no-ops if missing. `fork_world` calls `create_world` internally and inherits this bug 1:1: a fork that picks a name collision leaves an orphan fork.
