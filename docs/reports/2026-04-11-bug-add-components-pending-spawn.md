# Bug Report: `AsyncWorld.add_components` on a pending-spawn entity pushes a malformed row into `_spawn_cache` and crashes the next step

**Date:** 2026-04-11
**Severity:** High (hard crash + cache corruption on a documented public API)
**Affects:** `archetype.core.aio.async_world.AsyncWorld.add_components` (and `remove_components` by symmetry) when called on an entity whose spawn has not yet been materialised by `step()`
**Discovered by:** Overnight bug hunt

## Summary

`AsyncWorld._move_entity` reads the "previous row" exclusively from `self._live` (`async_world.py:279-285`). When `add_components` is called on an entity created in the same tick — i.e. its spawn row is still sitting in `_spawn_cache[old_sig]` and `_live[old_sig]` does not yet exist — `_move_entity` returns `{}` (the "entity vanished" sentinel), and `add_components` blindly pushes that empty dict into `_spawn_cache[new_sig]`. The next `step()` then throws `KeyError: 'entity_id'` from inside `materialize_mutations`'s spawn dedupe, taking the entire tick down. Even worse, `_entity2sig[eid]` is updated to `new_sig` *before* the crash, so the world is left in a corrupted state where the entity is registered at a signature that has no valid row.

The bug is exactly the "pending-spawn move" footgun the existing `2026-04-11-bug-spawn-despawn-same-tick.md` report flagged in its scope notes ("`add_components` / `remove_components` move rows via `_despawn_cache` + `_spawn_cache` as well … especially `add_components` on an entity whose spawn has not yet been materialised, which is likely to have a sibling bug"). This report confirms it with a reproduction.

## Impact

1. **Hard crash on a perfectly reasonable user flow.** Any caller that constructs an entity from multiple components in two steps — e.g. `eid = await world.create_entity([Agent(...)]); await world.add_components(eid, [Inbox()])` — and then calls `world.step()` gets a `RuntimeError` from inside `materialize_mutations`. The error message is opaque (`KeyError: 'entity_id'` wrapped in the archetype name) and gives no hint that the trigger was the missing intermediate step.
2. **CommandBroker batches that mix `SPAWN` + `ADD_COMPONENTS` for the same entity at the same tick will explode.** This is the natural pattern for "instantiate this fully-configured agent" flows in scenario setup, MCTS rollouts, fork bootstraps, and the agent DSL's `world.spawn(...)` ergonomics. Anything that submits multiple commands per entity per tick is exposed.
3. **Cache and registry corruption survives the crash.** `_entity2sig[eid] = new_sig` runs before the empty row is pushed; when the step throws, `_entity2sig` still points to `new_sig`, `_spawn_cache[old_sig]` still holds the original row, `_despawn_cache[old_sig]` has the entity scheduled for tombstone, and `_spawn_cache[new_sig]` holds a malformed `{}`. Subsequent calls to `add_components`/`remove_entity` for `eid` will operate against `new_sig`, but the actual data is at `old_sig` — desync that no public API can repair.
4. **Documented test coverage is illusory.** `tests/aio/test_async_world_edges.py::test_add_components_before_first_step_handles_empty_live` reads as if it covers exactly this scenario, but its body adds the *same* component type the entity already has, hitting the `new_sig == old_sig` early return at `async_world.py:347-349` and never entering `_move_entity`. False-confidence coverage is how this bug stayed in the tree.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit db6d87f, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: AsyncWorld.add_components on a pending-spawn entity puts an
empty dict into _spawn_cache[new_sig] and the next step crashes.
"""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class A(Component):
    a: int = 0


class B(Component):
    b: int = 0


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"),
                StorageConfig(uri=tmp),
            )
            world = container.world_service.get_world(info.world_id)

            eid = await world.create_entity([A(a=1)])
            await world.add_components(eid, [B(b=2)])
            for sig, rows in world._spawn_cache.items():
                print(f"spawn[{sig}] = {rows}")
            print(f"_despawn_cache = {dict(world._despawn_cache)}")
            print(f"_entity2sig    = {world._entity2sig}")

            try:
                await world.step(RunConfig(num_steps=1))
                print("step completed without crash")
            except Exception as e:
                print(f"step CRASHED: {type(e).__name__}: {e}")
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
spawn[(<class '__main__.A'>,)] = [{'world_id': '...', 'run_id': '', 'entity_id': 1,
                                   'tick': 0, 'is_active': True, 'a__a': 1}]
spawn[(<class '__main__.A'>, <class '__main__.B'>)] = [{}]
_despawn_cache = {(<class '__main__.A'>,): [1]}
_entity2sig    = {1: (<class '__main__.A'>, <class '__main__.B'>)}
step CRASHED: RuntimeError: a_2c_s775941febb020c0b: 'entity_id'
```

The empty `{}` in `_spawn_cache[(A, B)]` is the smoking gun. The `KeyError: 'entity_id'` from inside the dedupe step (`row["entity_id"]` on `{}`) bubbles up as a `RuntimeError` named after the new archetype.

### Baseline (proves the bug is scoped to "before any step")

The same flow with one materialising `step()` between `create_entity` and `add_components` works correctly:

```python
eid = await world.create_entity([A(a=1)])
await world.step(RunConfig(num_steps=1))   # MATERIALISE FIRST
await world.add_components(eid, [B(b=2)])
await world.step(RunConfig(num_steps=1))

# rows[A]    = [{'entity_id': 1, 'tick': 1, 'is_active': True, 'a__a': 1}]
# rows[A,B]  = [{'entity_id': 1, 'tick': 1, 'is_active': True, 'a__a': 1, 'b__b': 2}]
```

`step completed without crash.` The bug fires only when `_live` does not yet contain the entity at the time of the move.

## Root cause

`src/archetype/core/aio/async_world.py:265-310` (`_move_entity`):

```python
async def _move_entity(
    self,
    entity_id: int,
    old_sig: ArchetypeSignature,
    new_sig: ArchetypeSignature,
    mutated_components: list[Component],
) -> dict[str, Any]:
    """
    Returns a row dict that is valid for the NEW archetype.
    Any field that is NOT in `mutated_components` is read from the
    previous most-recent row in the OLD archetype.
    """

    # 1) fetch *only* the single entity from old archetype
    df = self._live.get(
        old_sig,
        daft.from_arrow(
            pa.Table.from_batches([], schema=Archetype.get_archetype_schema(old_sig))
        ),
    )
    df = df.where(col("entity_id") == entity_id) if df is not None else df

    # Materialize and check for emptiness using count_rows to avoid expression truthiness
    df_mat = df.collect()
    if df_mat.count_rows() == 0:
        return {}  # entity vanished, caller decides
    ...
```

`src/archetype/core/aio/async_world.py:340-360` (`add_components`):

```python
async def add_components(self, entity_id: int, components: list[Component]) -> None:
    old_sig = self._entity2sig.get(entity_id)
    if not old_sig:
        logger.warning("add_components: entity %s not found", entity_id)
        return

    new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
    if new_sig == old_sig:
        logger.debug("add_components: no-op; entity %s already has components", entity_id)
        return

    row = await self._move_entity(entity_id, old_sig, new_sig, components)

    # 1) mark *old row* inactive
    self._despawn_cache.setdefault(old_sig, []).append(entity_id)

    # 2) row to *insert* under new signature
    self._spawn_cache.setdefault(new_sig, []).append(row)

    # 3) update bookkeeping – atomically
    self._entity2sig[entity_id] = new_sig
```

Trace for the MRE (`tick == 0`, entity has never been stepped):

1. `create_entity([A(a=1)])` →
   - `_spawn_cache[(A,)] = [row_A(eid=1, is_active=True, a__a=1)]`
   - `_entity2sig[1] = (A,)`
   - `_live` is **empty** (no `step()` has run).
2. `add_components(1, [B(b=2)])`:
   1. `old_sig = (A,)` (still in `_entity2sig`)
   2. `new_sig = (A, B)` — different from `old_sig`, so the early return at line 347-349 does **not** fire.
   3. `_move_entity(1, (A,), (A, B), [B(b=2)])`:
      - Reads `self._live.get((A,), <empty schema-only df>)` — returns the empty fallback because `_live` has nothing for `(A,)`.
      - `df_mat.count_rows() == 0` → returns `{}` (the "entity vanished, caller decides" sentinel — but the caller does *not* in fact decide).
   4. `_despawn_cache[(A,)] = [1]` ← **schedules a tombstone** on a row that doesn't yet exist anywhere in storage.
   5. `_spawn_cache[(A, B)] = [{}]` ← **pushes the empty dict** into the new sig's spawn cache.
   6. `_entity2sig[1] = (A, B)` ← bookkeeping updated.
3. `step()` → `materialize_mutations((A, B))`:
   - Despawn cache for `(A, B)` is empty → skip.
   - Spawn cache for `(A, B)` is `[{}]` → enters the dedupe branch (`async_world.py:251-257`):
     ```python
     if self._spawn_cache.get(sig):
         entities_to_spawn = list({row["entity_id"]: row for row in self._spawn_cache[sig]}.values())
     ```
     `row["entity_id"]` on `{}` raises `KeyError: 'entity_id'`. The exception bubbles up, the future for `(A, B)` is wrapped in `RuntimeError(f"{Archetype.get_name(sig)}: {e}")` (line 167-169), and the entire step aborts.

The other archetype `(A,)` is also in `active_signatures`, but `asyncio.gather` aborts both futures on the first exception (from the way the `if errors: raise` aggregator works). Even if it didn't, the `(A,)` future would have happily persisted entity 1 at the OLD signature (the despawn fires first against an empty df, the spawn concat appends the original row), so the entity would be in the store at the OLD sig forever — the same orphan pattern as the other reports.

The root cause is a single missing lookup: `_move_entity` reads from `_live` only. It does *not* fall back to `_spawn_cache[old_sig]` when `_live` is empty. The function's docstring says "fetched from the previous most-recent row in the OLD archetype" — and the "previous most-recent row" of a same-tick entity *is* the row sitting in `_spawn_cache`, not in `_live`.

## Why existing tests miss this

`tests/aio/test_async_world_edges.py:39-49`:

```python
@pytest.mark.asyncio
async def test_add_components_before_first_step_handles_empty_live(world):
    ent = await world.create_entity([Position(x=1, y=1)])
    # Add the same component type before any step; _move_entity will not find old row in _live
    await world.add_components(ent, [Position(x=2, y=2)])
    # Stepping should succeed and write one active row
    rc = RunConfig()
    await world.step(rc)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    df = await world.query_archetype(sig, rc, ticks=[0])
    assert df.collect().count_rows() == 1
```

The test name and the inline comment both claim to cover the "_move_entity will not find old row in _live" path. But the test adds a `Position` to an entity that *already has* `Position`. `Archetype.add_components((Position,), [Position])` returns `(Position,)` (set union) — `new_sig == old_sig`. `add_components` (line 347-349) hits the early return and **never calls `_move_entity` at all**:

```python
new_sig = Archetype.add_components(old_sig, [type(c) for c in components])
if new_sig == old_sig:
    logger.debug("add_components: no-op; entity %s already has components", entity_id)
    return
```

The test passes — but only because the early return shields it from the buggy code path. The test name is misleading: it doesn't cover the "before first step" case at all, it covers the "no-op early return" case. This is exactly how the bug stayed in the tree.

`tests/aio/test_async_world_mutations.py::test_add_components_moves_to_superset_signature` (line 96-120) does exercise the move-to-superset path correctly, but only **after** `await world.step(rc)` materialises the spawn first. So the materialised path is covered; the pending-spawn path is uncovered.

`remove_components` has the same shape and the same gap: `tests/aio/test_async_world_mutations.py::test_remove_components_moves_to_subset_signature` always steps before calling `remove_components`. There is no test for `create_entity → remove_components → step`.

## Suggested fixes

**Fix A — `_move_entity` falls back to `_spawn_cache[old_sig]` when `_live` is empty.** The cleanest fix: the function's contract is "give me the previous most-recent row for this entity at the old sig", and that row may legitimately live in `_spawn_cache`. Search there before declaring the entity vanished.

```diff
 async def _move_entity(
     self,
     entity_id: int,
     old_sig: ArchetypeSignature,
     new_sig: ArchetypeSignature,
     mutated_components: list[Component],
 ) -> dict[str, Any]:
-    # 1) fetch *only* the single entity from old archetype
-    df = self._live.get(
-        old_sig,
-        daft.from_arrow(
-            pa.Table.from_batches([], schema=Archetype.get_archetype_schema(old_sig))
-        ),
-    )
-    df = df.where(col("entity_id") == entity_id) if df is not None else df
-
-    # Materialize and check for emptiness using count_rows to avoid expression truthiness
-    df_mat = df.collect()
-    if df_mat.count_rows() == 0:
-        return {}  # entity vanished, caller decides
-
-    # 2) take latest tick row
-    row_dict = df_mat.sort(col("tick"), desc=True).limit(1).to_pylist()[0]
+    row_dict: dict[str, Any] | None = None
+
+    # Prefer a pending spawn — that's the most recent state of the entity
+    # when the spawn has not yet been materialised by step().
+    pending = self._spawn_cache.get(old_sig)
+    if pending:
+        for row in reversed(pending):
+            if row.get("entity_id") == entity_id:
+                row_dict = dict(row)  # copy: we will mutate it
+                break
+
+    if row_dict is None:
+        # Fall back to the live snapshot from the previous step.
+        df = self._live.get(
+            old_sig,
+            daft.from_arrow(
+                pa.Table.from_batches([], schema=Archetype.get_archetype_schema(old_sig))
+            ),
+        )
+        df = df.where(col("entity_id") == entity_id) if df is not None else df
+        df_mat = df.collect()
+        if df_mat.count_rows() == 0:
+            return {}  # entity genuinely vanished
+        row_dict = df_mat.sort(col("tick"), desc=True).limit(1).to_pylist()[0]
 
     # 3) overlay components that change with the new ones
     for c in mutated_components:
         row_dict.update(c.to_row_dict())
     ...
```

This also requires `add_components` to **cancel the original spawn entry** when it moves the row, otherwise the same entity will appear in both `_spawn_cache[old_sig]` and `_spawn_cache[new_sig]` and the next step persists it twice. The minimal change is at the call site:

```diff
 row = await self._move_entity(entity_id, old_sig, new_sig, components)
+if not row:
+    return  # genuinely vanished — leave caches alone
+
+# If the row was migrated from a pending spawn, drop the original entry
+# so the old sig doesn't try to materialise it as well.
+pending_old = self._spawn_cache.get(old_sig)
+if pending_old:
+    self._spawn_cache[old_sig] = [r for r in pending_old if r.get("entity_id") != entity_id]
+    if not self._spawn_cache[old_sig]:
+        del self._spawn_cache[old_sig]
+    # The old-sig row never existed in storage, so we don't need a tombstone.
+    despawn_old = self._despawn_cache.get(old_sig, [])
+    if entity_id in despawn_old:
+        despawn_old.remove(entity_id)
+
-# 1) mark *old row* inactive
-self._despawn_cache.setdefault(old_sig, []).append(entity_id)
+else:
+    # Old row was already in the store — schedule a tombstone for it.
+    self._despawn_cache.setdefault(old_sig, []).append(entity_id)
```

`remove_components` needs the symmetric change.

**Fix B — `add_components` rejects the call if `_move_entity` returns `{}`** (defensive minimum):

```diff
 row = await self._move_entity(entity_id, old_sig, new_sig, components)
+if not row:
+    logger.warning(
+        "add_components: entity %s has no materialised row for sig %s; "
+        "call step() before mutating components on a freshly-spawned entity",
+        entity_id, old_sig,
+    )
+    return
 self._despawn_cache.setdefault(old_sig, []).append(entity_id)
 self._spawn_cache.setdefault(new_sig, []).append(row)
 self._entity2sig[entity_id] = new_sig
```

This is a one-block change and prevents the crash + cache corruption, but it makes the documented "you can mutate components inside a tick" pattern impossible. Fix A is the right semantic fix; Fix B is a safe stop-gap.

## Suggested regression tests

Add to `tests/aio/test_async_world_edges.py` (the file that *claims* to cover this path today). These tests should fail on `main` and pass after Fix A.

```python
@pytest.mark.asyncio
async def test_add_components_before_first_step_actually_adds_them(world):
    """Regression: add_components on a pending-spawn entity must move the
    row to the new signature instead of pushing an empty dict into the
    spawn cache."""
    ent = await world.create_entity([Position(x=1, y=1)])
    await world.add_components(ent, [Velocity(dx=2, dy=3)])  # NEW component type
    rc = RunConfig()
    await world.step(rc)  # must NOT crash

    sig_pos_vel = Archetype.sig_from_components([Position(x=0, y=0), Velocity(dx=0, dy=0)])
    df = await world.query_archetype(sig_pos_vel, rc, ticks=[0])
    rows = df.collect().to_pylist()
    assert len(rows) == 1, f"expected 1 row at (Position,Velocity), got {rows}"
    assert rows[0]["entity_id"] == ent
    assert rows[0]["is_active"] is True
    assert rows[0]["position__x"] == 1
    assert rows[0]["position__y"] == 1
    assert rows[0]["velocity__dx"] == 2
    assert rows[0]["velocity__dy"] == 3

    # Old signature must NOT contain a leaked active row.
    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])
    df_old = await world.query_archetype(sig_pos, rc, ticks=[0])
    old_rows = [r for r in df_old.collect().to_pylist() if r["entity_id"] == ent]
    assert all(not r["is_active"] for r in old_rows), (
        f"orphaned active row remains under old sig: {old_rows}"
    )


@pytest.mark.asyncio
async def test_remove_components_before_first_step_does_not_crash(world):
    """Regression: remove_components on a pending-spawn entity must not
    push an empty dict into the spawn cache."""
    ent = await world.create_entity([Position(x=1, y=1), Velocity(dx=2, dy=3)])
    await world.remove_components(ent, [Velocity])
    await world.step(RunConfig())  # must NOT crash

    sig_pos = Archetype.sig_from_components([Position(x=0, y=0)])
    df = await world.query_archetype(sig_pos, RunConfig(), ticks=[0])
    rows = df.collect().to_pylist()
    assert len(rows) == 1
    assert rows[0]["entity_id"] == ent
    assert rows[0]["is_active"] is True
    assert rows[0]["position__x"] == 1
```

The misnamed `test_add_components_before_first_step_handles_empty_live` should also be either renamed to reflect that it only covers the no-op path, or rewritten to actually exercise the bug path (i.e. add a *different* component type).

## Notes / scope

- Affects `src/archetype/core/aio/async_world.py` (`_move_entity` at 265-310, `add_components` at 340-360, `remove_components` at 362-379). Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fixes and does **not** touch the code.
- The sync world (`src/archetype/core/sync/world.py:180-221` + `:253-288`) has the **identical** `_move_entity → _live → return {}` shape and the identical caller code in `add_components`/`remove_components`. The bug is almost certainly present there too — the MRE here uses `AsyncWorld` because `ServiceContainer` wires the async world by default; a sync repro is a few lines away. Out of scope for this report.
- Distinct from the three already-filed bugs:
  - `2026-04-11-bug-spawn-despawn-same-tick.md` is about `create_entity + remove_entity` in the same tick on `AsyncWorld` (no `_move_entity` in the trace).
  - `2026-04-11-bug-sync-spawn-despawn-same-tick.md` is the sync sibling of the above.
  - `2026-04-11-bug-sync-active-signatures-drops-despawn.md` is about `SyncWorld.active_signatures` dropping despawn-only sigs.
  - This one is about `_move_entity` reading from the wrong cache during `add_components`/`remove_components` on a pending-spawn entity. The trigger, the offending function, and the failure mode are different (hard crash + cache corruption rather than silent storage corruption).
- The misnamed `test_add_components_before_first_step_handles_empty_live` is the kind of test that gives the test suite a green checkmark for a path it never actually exercises. Worth a separate audit pass to see if any other `*_handles_*` test in `tests/aio/` is similarly defended by an early return rather than hitting the path it claims to cover.
