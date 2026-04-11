# Bug Report: `SyncWorld` leaks an active row when `create_entity` and `remove_entity` are called in the same tick

**Date:** 2026-04-11
**Severity:** High (data-model correctness / silent desync, sync reference path)
**Affects:** `archetype.core.sync.world.SyncWorld._materialize_mutations` + `SyncWorld.remove_entity` — any caller that creates and immediately removes an entity before `step()` runs
**Discovered by:** Overnight bug hunt

## Summary

If `create_entity(...)` and `remove_entity(eid)` are both called for the same entity **within a single tick boundary** (i.e. before `SyncWorld.step()` has run), the next step persists the entity as `is_active=True` and leaves the world in a desynchronised state. This is the sync sibling of the already-filed `2026-04-11-bug-spawn-despawn-same-tick.md` (which targets `AsyncWorld`). The root cause is the same shape — `_materialize_mutations` applies despawns **before** spawns, and `remove_entity` does not cancel pending entries in `_spawn_cache` — but the offending code lives in a different file (`src/archetype/core/sync/world.py`) and uses a different despawn implementation (a left join, not a `with_column` mask), so the fix has to be applied here separately.

The user's mental model — "if I spawn and immediately despawn, the step produces a world in which that entity never existed" — is violated on `SyncWorld` exactly the way it is on `AsyncWorld`. CommandBroker batches that route through the sync engine produce orphaned rows in the store with no warning.

## Impact

1. **`CommandBroker` batches are unsafe on `SyncWorld`.** Submitting `[SPAWN(components=[...]), DESPAWN(entity_id=N)]` at `tick=0` lands in `world.create_entity()` / `world.remove_entity()` and reproduces the bug 1:1. Any caller that builds transactional batches — scenario setup code, MCTS rollouts that prune a candidate before the first tick, fork bootstrap scripts — ends up with orphaned `is_active=True` rows in the store at `tick=0`.
2. **Silent storage corruption.** The orphan row is written to the store by the updater at `tick=0`. There is no log line, no warning, no exception. Time-travel queries will forever return a "deleted" entity.
3. **`_entity2sig` desync is unrecoverable via the public API.** `remove_entity` (`world.py:244-251`) pops the entity from `_entity2sig` even when the matching spawn is still pending. After the step, no subsequent `remove_entity`, `add_components`, or `remove_components` call can touch the orphan — they all hit the `No entity` warning path.
4. **`_live` snapshots are poisoned.** `SyncWorld._run_archetype` writes `self._live[sig] = df_mat.where(col("is_active"))` (`world.py:126`). The leaked row is `is_active=True`, so it survives the filter and is returned by every subsequent `prefer_live_reads` step.
5. **Sync is the reference engine for the planned Rust port.** `AGENTS.md` describes `core/` as "the foundation everything else builds upon" with the current Python core "optimized for iteration speed" ahead of the Rust rewrite. Reference-implementation correctness bugs are exactly the ones a port will faithfully reproduce if not caught now.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit e963998, no diff)
- Python 3.12, `daft==0.7.5`, `pyiceberg` for the local SQL catalog
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: SyncWorld create_entity + remove_entity in the same tick (before
any step) leaks the entity. Sibling of the filed AsyncWorld bug.
"""
import pathlib
import tempfile

from daft.catalog import Catalog
from daft.session import Session
from pyiceberg.catalog.sql import SqlCatalog

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.sync import (
    QueryManager,
    SyncStore,
    SyncSystem,
    SyncWorld,
    UpdateManager,
)


class Position(Component):
    x: int = 0
    y: int = 0


def make_session(tmp: str) -> Session:
    base = pathlib.Path(tmp)
    catalog = Catalog.from_iceberg(
        SqlCatalog(
            "mre",
            uri=f"sqlite:///{base / 'catalog.db'}",
            warehouse=f"file://{base}",
        )
    )
    sess = Session()
    sess.attach_catalog(catalog)
    sess.create_namespace_if_not_exists("archetypes")
    sess.set_namespace("archetypes")
    return sess


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        sess = make_session(tmp)
        store = SyncStore(uri=f"{tmp}/store", session=sess)
        world = SyncWorld(
            world_config=WorldConfig(name="mre"),
            querier=QueryManager(store=store),
            updater=UpdateManager(store=store),
            system=SyncSystem(),
        )
        rc = RunConfig(num_steps=1)
        sig = Archetype.sig_from_components([Position(x=0, y=0)])

        # Spawn, then immediately despawn — BEFORE any step.
        eid = world.create_entity([Position(x=1, y=2)])
        world.remove_entity(eid)
        world.step(rc)

        df = store.get_archetype_df(
            sig, world_id=str(world.world_id), run_id=str(rc.run_id)
        )
        rows = df.to_pylist()
        for r in rows:
            print(
                f"entity_id={r['entity_id']} tick={r['tick']} "
                f"is_active={r['is_active']}"
            )
        assert not any(r["is_active"] for r in rows), (
            "BUG: removed entity still is_active=True"
        )


if __name__ == "__main__":
    main()
```

### Observed output

```
_spawn_cache   has eid=1? True
_despawn_cache has eid=1? True
_entity2sig    has eid=1? False
store rows after step: 1
    entity_id=1 tick=0 is_active=True
_live has active row for eid=1? True
BUG CONFIRMED: spawn+despawn in same tick leaked an active row.
```

The store ends up with one row at `tick=0` marked `is_active=True` — for an entity the caller explicitly removed before any step ran. `_live` still holds the row.

### Baseline (proves the bug is scoped to "same tick" only)

The same sequence works correctly when the spawn has already been materialised by an earlier step. (The baseline uses two entities so the *separately filed* `sync-active-signatures-drops-despawn` bug does not interfere — that one fires only when removing the **last** entity of a sig.)

```python
eid = world.create_entity([Position(x=1, y=2)])
world.create_entity([Position(x=3, y=4)])  # second entity in same sig
world.step(rc)                              # materialise spawn
world.remove_entity(eid)
world.step(rc)                              # materialise despawn

# Store rows (sorted by entity_id, tick):
# entity_id=1 tick=0 is_active=True
# entity_id=1 tick=1 is_active=False   <-- despawn materialised correctly
# entity_id=2 tick=0 is_active=True
# entity_id=2 tick=1 is_active=True
```

`OK (baseline): despawn fired correctly when spawn was already materialised.`

## Root cause

`src/archetype/core/sync/world.py:139-178`:

```python
def _materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature, run_config: RunConfig):
    # Handle Despawns
    if self._despawn_cache.get(sig):
        # Grab despawn list of dicts and dedupe by most recent mutation command
        self._despawn_cache[sig] = list(dict.fromkeys(reversed(self._despawn_cache[sig])))
        entities_to_despawn = self._despawn_cache[sig]

        # Left Join is O(n+m), better than df['entity_id'].is_in(entities_to_despawn) -> O(n*m)
        mask_df = daft.from_pydict(
            {"entity_id": entities_to_despawn, "is_active": [False] * len(entities_to_despawn)}
        )

        df = (
            df.join(
                mask_df, left_on="entity_id", right_on="entity_id", how="left", suffix="_right"
            )
            .with_column(
                "is_active",
                when(col("is_active_right").is_null(), then=col("is_active")).otherwise(
                    col("is_active_right")
                ),
            )
            .select(*df.column_names)
        )

    # Handle Spawns
    if self._spawn_cache.get(sig):
        # Grab spawn list of dicts
        rows = self._spawn_cache[sig]

        # Dedupe duplicate spawns, prioritizing "most recent cmd" for easy user overwrite
        rows = list({row["entity_id"]: row for row in reversed(rows)}.values())

        # Convert list of dicts to arrow table and eventually daft df
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
        spawns_df = daft.from_arrow(arrow_table)
        df = df.concat(spawns_df)

    return df
```

And `src/archetype/core/sync/world.py:244-251`:

```python
def remove_entity(self, entity_id: int):
    sig = self._entity2sig.pop(entity_id, None)
    if sig:
        self._despawn_cache.setdefault(sig, []).append(entity_id)
    else:
        logger.warning(
            f"World {self.name} ({self.world_id}): Entity Removal Failed: No entity: {entity_id}"
        )
```

Trace for the MRE (`tick == 0`, entity has never been materialised):

1. `create_entity(Position(...))` (`world.py:231-242`):
   - `_spawn_cache[sig] = [row(entity_id=1, is_active=True, ...)]`
   - `_entity2sig[1] = sig`
2. `remove_entity(1)` (`world.py:244-251`):
   - `_entity2sig.pop(1)` — entity gone from the registry, but the spawn row is still in `_spawn_cache`.
   - `_despawn_cache[sig] = [1]`.
3. `step()` (`world.py:81-96`) → `_run_archetype(sig)`:
   1. Previous-tick query at `tick=-1` returns an empty DataFrame (schema only).
   2. `_materialize_mutations` (`world.py:139-178`):
      - **Despawn first.** Builds `mask_df` for entity 1 and left-joins it onto the empty `df`. The left side has zero rows, so the join result also has zero rows. The despawn mask never sees any row to flip.
      - **Spawn second.** Concatenates `spawns_df` (one row, `is_active=True`) to the empty `df`. The just-spawned row is *not* re-checked against the despawn cache.
   3. Updater persists `(entity_id=1, is_active=True, tick=0)` (`world.py:123`).
   4. `self._live[sig] = df_mat.where(col("is_active"))` (`world.py:126`) — filter keeps the row since `is_active=True`.
4. `_clear_caches()` (`world.py:223-225`) wipes both caches. The despawn request is gone forever.

Two compounding design issues, identical in shape to the filed `AsyncWorld` bug:

- **Ordering.** `_materialize_mutations` applies despawns **before** spawns. The despawn join can never mask a row that does not yet exist on the left side.
- **`remove_entity` does not cancel pending spawns.** It unconditionally appends to `_despawn_cache` and pops `_entity2sig`, with no check for a matching pending row in `_spawn_cache`. After the step, `_entity2sig` is empty but the live DF and the store both still hold the row.

## Why existing tests miss this

`tests/sync/test_sync_world.py::test_remove_entity_populates_despawn_cache` (line 192-201) is the only sync test that calls `remove_entity`, and it asserts only the cache contents — it never calls `world.step(...)`.

In fact `grep -n "world\." tests/sync/test_sync_world.py` confirms that **no test in `tests/sync/` calls `world.step(...)` or `world.run(...)` at all**. There is no end-to-end "create → step → query → remove → step → query" coverage anywhere in the sync test module. The async sibling `tests/aio/test_async_world_mutations.py::test_create_and_remove_entity_spawns_and_despawns` always calls `world.step(rc)` *between* the create and the remove, so even the async equivalent never hits the same-tick path; the sync test module does not even have that much.

## Suggested fixes

Either of these individually closes the bug. Fixing both is defence-in-depth, and matches what the existing async report recommends.

**Fix A — cancel pending spawns inside `remove_entity`** (cleanest semantically; keeps `_live` and the store tombstone-free):

```diff
 def remove_entity(self, entity_id: int):
     sig = self._entity2sig.pop(entity_id, None)
-    if sig:
-        self._despawn_cache.setdefault(sig, []).append(entity_id)
-    else:
+    if sig is None:
         logger.warning(
             f"World {self.name} ({self.world_id}): Entity Removal Failed: No entity: {entity_id}"
         )
+        return
+
+    # If the entity's spawn has not yet been materialised, cancel it instead
+    # of scheduling a despawn — otherwise the row sneaks past the despawn
+    # mask in _materialize_mutations and ends up in the store as is_active=True.
+    pending = self._spawn_cache.get(sig)
+    if pending:
+        cancelled = [r for r in pending if r["entity_id"] == entity_id]
+        if cancelled:
+            remaining = [r for r in pending if r["entity_id"] != entity_id]
+            if remaining:
+                self._spawn_cache[sig] = remaining
+            else:
+                del self._spawn_cache[sig]
+            return
+
+    self._despawn_cache.setdefault(sig, []).append(entity_id)
```

**Fix B — reorder `_materialize_mutations` so despawns mask newly-spawned rows too**:

```diff
 def _materialize_mutations(self, df, sig, run_config):
+    # Spawns first — so their rows are visible to the despawn mask.
+    if self._spawn_cache.get(sig):
+        rows = self._spawn_cache[sig]
+        rows = list({row["entity_id"]: row for row in reversed(rows)}.values())
+        pyarrow_schema = Archetype.get_archetype_schema(sig)
+        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
+        spawns_df = daft.from_arrow(arrow_table)
+        df = df.concat(spawns_df)
+
+    # Despawns last — now masks both pre-existing and just-spawned rows.
     if self._despawn_cache.get(sig):
         self._despawn_cache[sig] = list(dict.fromkeys(reversed(self._despawn_cache[sig])))
         entities_to_despawn = self._despawn_cache[sig]
         mask_df = daft.from_pydict(
             {"entity_id": entities_to_despawn, "is_active": [False] * len(entities_to_despawn)}
         )
         df = (
             df.join(mask_df, left_on="entity_id", right_on="entity_id", how="left", suffix="_right")
             .with_column(
                 "is_active",
                 when(col("is_active_right").is_null(), then=col("is_active")).otherwise(
                     col("is_active_right")
                 ),
             )
             .select(*df.column_names)
         )
-
-    if self._spawn_cache.get(sig):
-        rows = self._spawn_cache[sig]
-        rows = list({row["entity_id"]: row for row in reversed(rows)}.values())
-        pyarrow_schema = Archetype.get_archetype_schema(sig)
-        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
-        spawns_df = daft.from_arrow(arrow_table)
-        df = df.concat(spawns_df)

     return df
```

Fix A avoids writing a tombstone row to the store at all, which keeps the time-travel history clean — the entity simply never existed. Fix B is smaller and covers any future caller that reaches into `_spawn_cache` directly. The existing async report makes the same recommendation; applying both engines' fixes in lockstep is the right move because the two engines mirror each other on purpose.

## Suggested regression tests

Add to `tests/sync/test_sync_world.py`. The first test is the direct unit-test for the bug; the second exercises the broker batch flow that real callers will hit.

```python
def test_create_then_remove_in_same_tick_yields_empty_world(tmp_path):
    """Spawn + despawn before any step should leave the world empty."""
    from archetype.core.archetype import Archetype
    from archetype.core.config import RunConfig

    world = _make_sync_world(tmp_path)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    rc = RunConfig(num_steps=1)

    eid = world.create_entity([Position(x=1, y=2)])
    world.remove_entity(eid)
    world.step(rc)

    # Store rows for the cancelled entity should be empty OR is_active=False.
    df = world.querier.query_archetype(
        sig=sig,
        run_config=rc,
        ticks=None,
        entity_ids=[eid],
        components=None,
        world_id=str(world.world_id),
    )
    rows = df.to_pylist()
    assert all(not r["is_active"] for r in rows), (
        f"orphaned active row remains for cancelled entity: {rows}"
    )

    # _live must not contain an active row for the cancelled entity.
    for s, d in world._live.items():
        for r in d.to_pylist():
            assert r.get("entity_id") != eid, (
                f"orphaned row remains in _live[{s}]: {r}"
            )


def test_create_then_remove_in_same_tick_leaves_no_live_snapshot(tmp_path):
    """The cancelled entity must not survive in _live or _entity2sig."""
    from archetype.core.config import RunConfig

    world = _make_sync_world(tmp_path)
    rc = RunConfig(num_steps=1)
    eid = world.create_entity([Position(x=1, y=2)])
    world.remove_entity(eid)
    world.step(rc)

    assert eid not in world._entity2sig
    for s, d in world._live.items():
        active_eids = [r["entity_id"] for r in d.to_pylist() if r["is_active"]]
        assert eid not in active_eids, (
            f"_live[{s}] still has active row for cancelled entity"
        )
```

A third test should exercise the same flow through `CommandService.submit` so the broker batch path is covered end-to-end (mirrors the async report's third test); it is omitted here only because the sync world is not currently wired through `ServiceContainer` and adding the wiring is out of scope for a bug report.

## Notes / scope

- Affects `src/archetype/core/sync/world.py` (`_materialize_mutations` at line 139-178 + `remove_entity` at line 244-251). Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fixes and does **not** touch the code.
- This is a sibling bug to `2026-04-11-bug-spawn-despawn-same-tick.md`, which targets `AsyncWorld`. The two engines are deliberate mirrors, and the bug shape is identical, but the offending code lives in two different files (`sync/world.py` vs `aio/async_world.py`) and uses two different despawn primitives (a left join vs a `with_column(when(is_in)...)` mask), so the fix has to be applied to each separately. Both reports should be addressed in the same change.
- Distinct from `2026-04-11-bug-sync-active-signatures-drops-despawn.md`, which fires *only* when removing the last entity of a sig **after** a successful step. This bug fires when create and remove happen **before any step has run**, regardless of whether the sig has other entities. The two reports are independent and should both be addressed.
- `add_components` / `remove_components` (`world.py:253-288`) push to the same `_spawn_cache` + `_despawn_cache` via `_move_entity`. Their "called on an entity whose spawn has not yet been materialised" semantics should be re-verified once Fix A or B lands — they likely have a sibling bug in the move path. Out of scope for this report.
- `tests/sync/test_sync_world.py` does not currently call `world.step(...)` or `world.run(...)` anywhere. Independent of this bug, the sync test module needs at least one end-to-end "create → step → query → mutate → step → query" test to keep the reference engine honest.
