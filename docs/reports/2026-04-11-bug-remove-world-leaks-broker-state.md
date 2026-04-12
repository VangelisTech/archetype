# Bug Report: `WorldService.remove_world` deletes the world but leaves all of its broker state in `_queues`, `_pending`, and `_history`

**Date:** 2026-04-11
**Severity:** Medium-High (silent memory leak on every world destroy; orphaned commands accumulate forever, audit history for destroyed worlds is unreachable)
**Affects:** `archetype.app.world_service.WorldService.remove_world` and (transitively) the broker `DESTROY_WORLD` lifecycle path
**Discovered by:** Overnight bug hunt

## Summary

`WorldService.remove_world` (`world_service.py:121-129`) deletes the world from `_worlds`, removes its name mapping from `_world_names`, and deletes the registry entry. It does **not** call `broker.clear(world_id)` to remove the world's pending commands, in-flight commands, or audit history. After `remove_world`:

- `broker._queues[str(world_id)]` still holds every command that was pending for the destroyed world.
- `broker._pending` still references those commands by id.
- `broker._history[str(world_id)]` still holds the entire audit trail for the destroyed world.

The world is gone from the public API (`get_world` raises `KeyError`, `list_worlds` no longer shows it) but every byte of broker state for that world is still in memory. The leak compounds with every destroy.

## Impact

1. **Unbounded memory leak on every world destroy.** A workload that creates and destroys many worlds (MCTS rollouts, ensemble runs, scenario sweeps — all explicitly motivated in `AGENTS.md`) accumulates one set of broker state per destroyed world. Each set grows with however many commands the world had in flight at destroy time. For a long-running `archetype serve` process driving 100 fork+destroy cycles per minute, the leak is significant within hours.
2. **`broker.get_pending_count()` over-reports indefinitely.** The broker exposes `get_pending_count(world_id=None)` which returns `len(self._pending)` (`broker.py:185-190`). After N destroyed worlds with M commands each, the global count is inflated by N*M. Operators monitoring "queue depth" see a number that includes ghosts from destroyed worlds.
3. **`broker.get_history(world_id, limit)` for a destroyed world returns the orphaned audit trail.** The world is gone from `_worlds`, but `broker._history[str(wid)]` still contains every command. A user querying the destroyed world's history (e.g., "what happened just before I deleted it?") gets results — but `world_service.get_world(wid)` raises `KeyError`. The two views disagree.
4. **The broker has no eviction policy for `_history`.** Even without the destroy bug, `_history` accumulates from process start. Combined with this bug, the destroy path is the obvious place to evict, but doesn't.
5. **Compounds with the filed `lifecycle-commands-leak-broker` report.** That bug leaks zombie CREATE_WORLD/DESTROY_WORLD commands into `_queues["__global__"]`. This bug leaks per-world commands when the destroy fires. Together, every destroy triggers TWO different leaks: the lifecycle command itself never gets acked, AND the destroyed world's pending commands stay in the broker forever.
6. **The fix is one line.** `broker.clear(world_id)` already exists (`broker.py:200-208`) and does exactly the right thing. `remove_world` just doesn't call it.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 336703d, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: remove_world leaves broker state behind."""
import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import StorageConfig, WorldConfig


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            ws = container.world_service
            broker = container.broker
            ctx = ActorCtx(id=uuid7(), roles={"admin"})

            info = await ws.create_world(WorldConfig(name="doomed"), StorageConfig(uri=tmp))
            wid_str = str(info.world_id)

            for _ in range(3):
                cmd = Command(type=CommandType.SPAWN, payload={"components": []})
                await container.command_service.submit(info.world_id, cmd, ctx)

            print(f"before remove: queue={len(broker._queues.get(wid_str, []))}, "
                  f"pending={len(broker._pending)}, "
                  f"history={len(broker._history.get(wid_str, []))}")

            ws.remove_world(info.world_id)

            print(f"after remove:  queue={len(broker._queues.get(wid_str, []))}, "
                  f"pending={len(broker._pending)}, "
                  f"history={len(broker._history.get(wid_str, []))}")

            assert len(broker._queues.get(wid_str, [])) == 0, "queue leaked"
            assert len(broker._pending) == 0, "pending leaked"
            assert len(broker._history.get(wid_str, [])) == 0, "history leaked"
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
before remove: queue=3, pending=3, history=3
after remove:  queue=3, pending=3, history=3
AssertionError: queue leaked
```

The world is gone from `_worlds` (separately verified), but the broker's per-world queue, the global pending dict, and the per-world history are all unchanged.

### Baseline (proves the leak is missing wiring, not a missing capability)

`broker.clear(world_id)` already exists and does exactly the right thing — `remove_world` just doesn't call it:

```python
await broker.clear(wid_str)
ws.remove_world(info.world_id)

# after manual clear+remove: queue=0, pending=0, history=0
# OK (baseline): broker.clear(wid) is the missing call inside remove_world.
```

The fix is to call `broker.clear` from `remove_world`. The capability is one line away.

## Root cause

`src/archetype/app/world_service.py:121-129`:

```python
def remove_world(self, world_id: UUID) -> None:
    """Removes a world from management by its ID."""
    if world_id in self._worlds:
        for name, uid in list(self._world_names.items()):
            if uid == world_id:
                del self._world_names[name]
        del self._worlds[world_id]
    if self._registry is not None:
        self._registry.delete(world_id)
```

The method touches three pieces of state:

1. `self._world_names` — clears the name → id mapping. ✓
2. `self._worlds` — removes the world from the in-memory registry. ✓
3. `self._registry.delete(world_id)` — removes the persistent registry entry. ✓

But it does **not** touch:

1. `self._broker._queues[str(world_id)]` — pending commands.
2. `self._broker._pending` — global pending dict.
3. `self._broker._history[str(world_id)]` — audit history.
4. `self.storage_service` — the world's storage backend (pooled by uri/namespace, may still be referenced by other worlds — correctly NOT shut down here).

`broker.clear` (`broker.py:200-212`) does exactly the cleanup we need:

```python
async def clear(self, world_id: str | UUID | None = None):
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
```

But `remove_world` is sync (not async) and doesn't reference `self._broker` at all. The world service has no broker reference today — it has `self._broker` (injected in `__init__`), and `world_service.py:80-81` uses it to inject the broker into world resources. But `remove_world` ignores it.

Trace for the MRE:

1. `ws.create_world(WorldConfig(name="doomed"), ...)` creates the world.
2. Three `command_service.submit` calls enqueue three SPAWN commands. broker._queues, _pending, _history all have 3 entries each for the world.
3. `ws.remove_world(world.world_id)`:
   - `world_id in self._worlds` → True. Enter the cleanup block.
   - Loop through `_world_names`, remove the entry for "doomed".
   - `del self._worlds[world_id]`.
   - `self._registry is None` (no registry was provided to ServiceContainer) → skip the registry delete.
4. Return. The function does not touch the broker. _queues, _pending, _history all still have 3 entries.

The bug is a missing `broker.clear` call. The fix is one block of code.

## Why existing tests miss this

`tests/app/test_services.py:277-286::test_remove_world` (the canonical test):

```python
@pytest.mark.asyncio
async def test_remove_world(self, tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="w1"), storage)
        container.world_service.remove_world(world.world_id)
        assert len(container.world_service.list_worlds()) == 0
    finally:
        await container.shutdown()
```

The test:

1. Creates a world (no commands submitted).
2. Removes it.
3. Asserts `list_worlds()` is empty.

It does NOT:

- Submit any commands before removing the world.
- Check `broker._pending`, `broker._queues`, or `broker._history` after the remove.
- Use `broker.get_pending_count(None)` to verify the global count is zero.

`tests/core/test_factory_and_orchestrator.py:50` and `tests/core/test_orchestrator_errors_and_instrumentation.py:43,58` all call `remove_world` but neither submits commands first nor checks broker state afterward.

`tests/app/test_registry.py:118::test_remove_world_clears_registry` checks that the registry is cleared, but again does not check the broker.

`grep -rn "remove_world.*broker\|broker.*remove_world\|after.*remove_world" tests/` returns no test that verifies the broker is cleared after a world removal.

The leak is structurally invisible because no test runs the "submit commands → destroy world → check broker is clean" flow.

## Suggested fixes

**Fix A — call `broker.clear` from `remove_world`.** The minimal correct fix. Lands in `app/`:

```diff
-def remove_world(self, world_id: UUID) -> None:
+async def remove_world(self, world_id: UUID) -> None:
     """Removes a world from management by its ID."""
     if world_id in self._worlds:
         for name, uid in list(self._world_names.items()):
             if uid == world_id:
                 del self._world_names[name]
         del self._worlds[world_id]
+        # Clear broker state for the destroyed world to avoid leaks.
+        if self._broker is not None:
+            await self._broker.clear(str(world_id))
     if self._registry is not None:
         self._registry.delete(world_id)
```

The change makes `remove_world` async (because `broker.clear` is async). Existing callers — `command_service.apply_world_lifecycle:DESTROY_WORLD` (`command_service.py:132-135`) and the REST `DELETE /worlds/{id}` handler (`api/routes/worlds.py:80-93`) — already run inside async contexts, so adding `await` is straightforward. Tests that call `remove_world` directly need `await` added too.

A subtlety: `remove_world` is called from `ws.shutdown` indirectly through `_worlds.clear()`, but `shutdown` calls `_worlds.clear()` directly (not through `remove_world`). So the shutdown path is unaffected; `broker.clear()` is implicitly handled by the storage_service shutdown… wait, no, `storage_service.shutdown` doesn't touch the broker. The broker keeps its state across `container.shutdown()` if no one explicitly clears it. That's a separate (smaller) leak; out of scope for this report.

**Fix B — defensive: also clear broker on `world_service.shutdown`.** Same shape, applied in shutdown:

```diff
 async def shutdown(self):
     """Gracefully shuts down all managed resources."""
+    if self._broker is not None:
+        for wid in list(self._worlds.keys()):
+            await self._broker.clear(str(wid))
     await self.storage_service.shutdown()
     self._worlds.clear()
     self._world_names.clear()
```

Fix B catches the case where `container.shutdown()` is called without explicit `remove_world` calls. After Fix A, this is mostly redundant — but if the container is dropped without shutdown, the broker state is held until garbage collection. Fix B ensures the cleanup happens even on uncatched shutdown paths.

**Fix C — keep `remove_world` sync; have `command_service.apply_world_lifecycle:DESTROY_WORLD` call broker.clear separately.** Avoids changing the sync/async signature:

```diff
 # command_service.py
 case CommandType.DESTROY_WORLD:
     target_id = UUID(str(payload["world_id"]))
+    await self._broker.clear(str(target_id))
     self._world_service.remove_world(target_id)
     return None
```

Fix C is smaller and doesn't change `remove_world`'s signature, but it requires every destroy path (REST, CLI, broker, direct call) to remember to clear the broker. Fix A is structurally correct because the cleanup happens at the central removal point.

I'd recommend **Fix A** (and possibly **Fix B** as defence-in-depth).

## Suggested regression tests

Add to `tests/app/test_services.py`:

```python
@pytest.mark.asyncio
async def test_remove_world_clears_broker_pending_and_history(tmp_path):
    """Regression: remove_world must clear the broker's per-world queue,
    pending dict entries, and history for the destroyed world."""
    container = ServiceContainer()
    try:
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="doomed"), storage
        )
        wid_str = str(world.world_id)

        # Submit a few commands to populate broker state.
        for _ in range(3):
            cmd = Command(type=CommandType.SPAWN, payload={"components": []})
            await container.command_service.submit(world.world_id, cmd, ctx)

        baseline_pending = len(container.broker._pending)
        assert baseline_pending >= 3

        # Today this is sync; after Fix A it's async.
        result = container.world_service.remove_world(world.world_id)
        if asyncio.iscoroutine(result):
            await result

        assert len(container.broker._queues.get(wid_str, [])) == 0, (
            "remove_world left commands in broker._queues"
        )
        assert len(container.broker._pending) == baseline_pending - 3, (
            "remove_world left commands in broker._pending"
        )
        assert len(container.broker._history.get(wid_str, [])) == 0, (
            "remove_world left commands in broker._history"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_world_idempotent_clear(tmp_path):
    """Calling remove_world twice for the same world is a no-op on the
    second call (the world is already gone) — and doesn't accumulate
    state in the broker."""
    container = ServiceContainer()
    try:
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(
            WorldConfig(name="t"), storage
        )
        await container.command_service.submit(
            world.world_id,
            Command(type=CommandType.SPAWN, payload={"components": []}),
            ctx,
        )

        result = container.world_service.remove_world(world.world_id)
        if asyncio.iscoroutine(result):
            await result
        result = container.world_service.remove_world(world.world_id)  # second call
        if asyncio.iscoroutine(result):
            await result

        assert len(container.broker._pending) == 0
        assert world.world_id not in container.world_service._worlds
    finally:
        await container.shutdown()
```

The first test fails on `main` at `assert len(container.broker._queues.get(wid_str, [])) == 0` (gets 3). The second fails the same way.

## Notes / scope

- Affects `src/archetype/app/world_service.py:121-129`. This is in `app/`, not `core/`, so the fix can land directly.
- Distinct from the nineteen other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker leak for CREATE_WORLD/DESTROY_WORLD commands themselves (not the per-world entity commands).
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota accounting.
  - `component-prefix-collision`, `component-get-type-by-name-no-recurse`, `cached-store-read-shadows-disk` are Component / cache.
  - `create-world-name-collision-orphan` and `world-id-none-divergence` are `create_world` bugs.
  - `daily-tokens-never-reset` is the missing daily quota scheduler.
  - `storage-pool-key-ignores-cache-and-backend` is the multiton key.
  - `system-execute-strips-var-keyword-kwargs` is the processor kwargs filter.
  - This bug is about the *destroy* path leaking the destroyed world's broker state. It's a sibling of `lifecycle-commands-leak-broker`: that one leaks the lifecycle command itself; this one leaks the entity-level commands the world held.
- Compounds with `lifecycle-commands-leak-broker`: every REST `DELETE /worlds/{id}` request leaks (a) the DESTROY_WORLD command into `_queues["__global__"]` per the lifecycle bug, and (b) all of the world's pending commands and history per this bug. Each REST destroy causes two leaks.
- Storage backends are pooled by `(uri, namespace)` (`storage_service.py:42`), so destroying one world doesn't shut down a backend that other worlds may still use. That part of the design is correct — `remove_world` should NOT call `storage_service.shutdown()`. The bug is purely the missing broker cleanup.
- A small follow-up worth a separate hunt: `_history` has no eviction policy at all, even between alive worlds. Even after Fix A, a long-running world will accumulate `_history[wid]` indefinitely. The broker should grow a per-world `max_history` cap.
