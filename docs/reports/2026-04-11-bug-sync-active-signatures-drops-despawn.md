# Bug Report: `SyncWorld.active_signatures` silently drops the despawn of the last entity in a signature

**Date:** 2026-04-11
**Severity:** High (data-model correctness / silent desync, sync reference path)
**Affects:** `archetype.core.sync.world.SyncWorld` — any caller that removes the last entity of an archetype after a tick has materialised it
**Discovered by:** Overnight bug hunt

## Summary

`SyncWorld.active_signatures` returns `_entity2sig.values() ∪ _spawn_cache.keys()` — but **omits** `_despawn_cache.keys()`. When the only entity in a signature is removed via `remove_entity` after the spawn has been materialised by an earlier `step()`, the next `step()` does not visit that signature at all, so `_materialize_mutations` is never called, the despawn mask is never applied, and `_clear_caches()` then silently throws the despawn away. The store is left with `is_active=True` for an entity that the user told the world to delete.

`AsyncWorld.active_signatures` (`src/archetype/core/aio/async_world.py:227-232`) does the right thing — it includes `set(self._despawn_cache.keys())` in the union. `SyncWorld.active_signatures` (`src/archetype/core/sync/world.py:132-137`) does not. This is a one-line divergence with a silent-desync footprint.

## Impact

1. **Silent storage corruption on the sync reference world.** Any user of `SyncWorld` that removes the last entity of a signature after the first `step()` ends up with an orphan row marked `is_active=True` at the latest persisted tick. No warning, no log, no exception. Time-travel queries, downstream `QueryManager.query_archetype` calls, and any "snapshot the world" tooling will keep returning the deleted entity forever.
2. **`_entity2sig` desync is unrecoverable via the public API.** `remove_entity` pops the entity from `_entity2sig` (`world.py:245`). After the lossy step, the despawn cache has been cleared, the store still says `is_active=True`, and the only handle that could ask for it (`_entity2sig`) no longer exists. No subsequent `remove_entity` or `add_components` call can touch the orphan; the only way to clean it up is a direct store mutation.
3. **The sync engine is the reference for the planned Rust rewrite.** `AGENTS.md` calls `core/` "the foundation everything else builds upon" and notes that the current Python core is "optimized for iteration speed" ahead of the Rust port. Reference-implementation correctness bugs are exactly the ones a port will faithfully reproduce if not caught now.
4. **Asymmetry with `AsyncWorld` is a footgun for users that mix the two.** Test code, benchmarks, and the docs all use `AsyncWorld` and `SyncWorld` interchangeably as "the same ECS, different concurrency model." A user who validates their flow on `AsyncWorld` and then ports to `SyncWorld` (e.g. for deterministic tests) will hit this without warning.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 85bc355, no diff)
- Python 3.12, `daft==0.7.5`, `pyiceberg` for the local SQL catalog
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: SyncWorld.active_signatures omits despawn-only sigs.

Setup: spawn one entity, step. Then remove_entity, step again.
Expected: after the second step the entity is is_active=False in the store.
Actual:   the second step never visits the signature (because no entity in
          _entity2sig and no entry in _spawn_cache), so the despawn cache
          is silently dropped by _clear_caches() and the entity stays alive
          in the store forever.
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

        # Tick 0: spawn one entity, step.
        eid = world.create_entity([Position(x=1, y=2)])
        world.step(rc)

        # Tick 1: remove the only entity, step.
        world.remove_entity(eid)
        print(f"_despawn_cache    = {world._despawn_cache}")
        print(f"_entity2sig       = {world._entity2sig}")
        print(f"active_signatures = {sorted(map(repr, world.active_signatures))}")
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
_despawn_cache    = {(<class '__main__.Position'>,): [1]}
_entity2sig       = {}
active_signatures = []
entity_id=1 tick=0 is_active=True
AssertionError: BUG: removed entity still is_active=True
```

`active_signatures` is `[]` even though the despawn cache holds the request, and the store ends up with the entity persisted as live.

### Baseline (proves the bug is scoped to "remove the LAST entity of a sig")

Same code, but spawn two entities first. The signature stays in `_entity2sig.values()` after one is removed, so `active_signatures` still includes it, the step visits it, and `_materialize_mutations` runs the despawn mask:

```python
e1 = world.create_entity([Position(x=1, y=2)])
world.create_entity([Position(x=3, y=4)])  # second entity in same sig
world.step(rc)

world.remove_entity(e1)
world.step(rc)

# Store rows (sorted by entity_id, tick):
# entity_id=1 tick=0 is_active=True
# entity_id=1 tick=1 is_active=False   <-- despawn materialised correctly
# entity_id=2 tick=0 is_active=True
# entity_id=2 tick=1 is_active=True
```

`OK (baseline): the despawn materialized; bug is scoped to last-entity case.`

## Root cause

`src/archetype/core/sync/world.py:132-137`:

```python
@property
def active_signatures(self) -> set[ArchetypeSignature]:
    """Get the union of all archetypes that need processing this tick."""
    active_sigs = set(self._entity2sig.values())
    spawned_sigs = set(self._spawn_cache.keys())
    return active_sigs | spawned_sigs
```

Compare with `src/archetype/core/aio/async_world.py:227-232`:

```python
@property
def active_signatures(self) -> set[ArchetypeSignature]:
    """Get the union of all archetypes that need processing this tick."""
    active_sigs = set(self._entity2sig.values())
    spawned_sigs = set(self._spawn_cache.keys())
    despawn_sigs = set(self._despawn_cache.keys())
    return active_sigs | spawned_sigs | despawn_sigs
```

Trace for the MRE:

1. Tick 0: `create_entity([Position(...)])` — `_entity2sig[1] = sig`, `_spawn_cache[sig] = [row]`. `step()` runs the sig, materialises the spawn, persists `(eid=1, is_active=True, tick=0)`, `_clear_caches()` empties the spawn cache, `tick → 1`.
2. `remove_entity(1)` (`world.py:244-251`):
   ```python
   def remove_entity(self, entity_id: int):
       sig = self._entity2sig.pop(entity_id, None)
       if sig:
           self._despawn_cache.setdefault(sig, []).append(entity_id)
   ```
   `_entity2sig` is now empty; `_despawn_cache[sig] = [1]`.
3. `step()` (`world.py:81-96`):
   ```python
   for sig in sorted(self.active_signatures, key=Archetype.get_name):
       self._run_archetype(sig, run_config, **input_kwargs)
   self._clear_caches()
   self.tick += 1
   ```
   `active_signatures` returns `set() | set()` = `∅` (no entities, no spawns; despawns are NOT consulted). The `for` loop body never runs. `_run_archetype` is never called for `sig`, so `_materialize_mutations` is never called for `sig`, so the despawn mask is never applied to the previous-tick query result.
4. `_clear_caches()` (`world.py:223-225`) wipes `_despawn_cache` unconditionally. The despawn request is now gone forever. The store still has the entity at `tick=0` with `is_active=True`, and the next `query_archetype` for `sig` returns it as live.

The asymmetry is one missing line: `SyncWorld.active_signatures` was written without including `set(self._despawn_cache.keys())` in the union.

## Why existing tests miss this

`tests/sync/test_sync_world.py` covers the spawn-cache code path of `active_signatures` (line 208-214) but never covers the despawn-cache path:

```python
def test_active_signatures(self, tmp_path):
    world = _make_sync_world(tmp_path)
    from archetype.core.archetype import Archetype

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    world.create_entity([Position(x=1, y=2)])
    assert sig in world.active_signatures
```

This only asserts the `_spawn_cache.keys()` term. There is no test that:

1. Spawns + steps + removes + steps + reads back from the store.
2. Asserts `sig in world.active_signatures` after `remove_entity` empties `_entity2sig`.

Crucially, **no test in `tests/sync/` calls `world.step(...)` or `world.run(...)` at all** — `grep -n "world\." tests/sync/test_sync_world.py` shows only mutation/property tests, no end-to-end step. The sync world's stepping path is uncovered by the sync test module, which is exactly why a one-liner divergence from `AsyncWorld` slipped in. The async sibling `tests/aio/test_async_world_mutations.py::test_create_and_remove_entity_spawns_and_despawns` exists for `AsyncWorld` but has no sync counterpart.

## Suggested fixes

**Fix A — include despawn sigs in `active_signatures` (one-line, mirrors `AsyncWorld`):**

```diff
 @property
 def active_signatures(self) -> set[ArchetypeSignature]:
     """Get the union of all archetypes that need processing this tick."""
     active_sigs = set(self._entity2sig.values())
     spawned_sigs = set(self._spawn_cache.keys())
-    return active_sigs | spawned_sigs
+    despawn_sigs = set(self._despawn_cache.keys())
+    return active_sigs | spawned_sigs | despawn_sigs
```

This is the minimal change and the one that makes `SyncWorld` consistent with `AsyncWorld`. It ensures `_run_archetype` visits any signature that has pending despawns, so `_materialize_mutations` runs the despawn mask against the previous-tick query result and the updater persists `is_active=False` rows for the deleted entities.

**Fix B (defence-in-depth) — extract a shared `active_signatures` helper.** The sync and async worlds are now drifting on a property whose definition is supposed to be identical. Move the union into a free function or a shared mixin so future mutation caches added to one don't get forgotten in the other:

```python
# src/archetype/core/_active_sigs.py (new file)
def compute_active_signatures(
    entity2sig: dict[int, ArchetypeSignature],
    spawn_cache: dict[ArchetypeSignature, list],
    despawn_cache: dict[ArchetypeSignature, list],
) -> set[ArchetypeSignature]:
    return (
        set(entity2sig.values())
        | set(spawn_cache.keys())
        | set(despawn_cache.keys())
    )
```

Both `SyncWorld.active_signatures` and `AsyncWorld.active_signatures` then call this helper. The smaller fix is A; B prevents the next "I forgot to update one side" regression.

## Suggested regression tests

Add to `tests/sync/test_sync_world.py`. These are the missing coverage that lets the bug exist:

```python
def test_active_signatures_includes_despawn_cache(tmp_path):
    """active_signatures must include sigs that only exist in _despawn_cache."""
    world = _make_sync_world(tmp_path)
    from archetype.core.archetype import Archetype

    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    e = world.create_entity([Position(x=1, y=2)])
    # Step so the spawn is materialised and the spawn cache is cleared.
    from archetype.core.config import RunConfig
    world.step(RunConfig(num_steps=1))
    assert sig in world.active_signatures
    assert sig not in world._spawn_cache
    assert e in world._entity2sig

    world.remove_entity(e)
    assert sig in world._despawn_cache
    assert e not in world._entity2sig
    assert sig in world.active_signatures, (
        "active_signatures dropped a sig whose only mutation is a pending despawn"
    )


def test_remove_last_entity_persists_despawn(tmp_path):
    """Removing the last entity of a signature must mark it inactive in the store."""
    from archetype.core.archetype import Archetype
    from archetype.core.config import RunConfig

    world = _make_sync_world(tmp_path)
    sig = Archetype.sig_from_components([Position(x=0, y=0)])
    rc = RunConfig(num_steps=1)

    eid = world.create_entity([Position(x=1, y=2)])
    world.step(rc)

    world.remove_entity(eid)
    world.step(rc)

    df = world.querier.query_archetype(
        sig=sig,
        run_config=rc,
        ticks=None,
        entity_ids=[eid],
        components=None,
        world_id=str(world.world_id),
    )
    rows = df.to_pylist()
    # The entity should have at least one row with is_active=False at the
    # latest tick — and zero rows with is_active=True at the latest tick.
    latest_tick = max((r["tick"] for r in rows), default=None)
    latest = [r for r in rows if r["tick"] == latest_tick]
    assert latest, "no rows persisted for the removed entity"
    assert all(not r["is_active"] for r in latest), (
        f"removed entity still is_active=True at latest tick: {latest}"
    )
```

The first test is the direct unit-test for the property. The second is the end-to-end integration test that the sync test module lacks today.

## Notes / scope

- Affects `src/archetype/core/sync/world.py:132-137`. Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- This is a *sibling* bug to the already-filed `2026-04-11-bug-spawn-despawn-same-tick.md`, but distinct: the existing report is about spawn+despawn in the *same* tick on `AsyncWorld`, and is fixed by reordering `materialize_mutations` or by `remove_entity` cancelling pending spawns. The bug here triggers in a different scenario (despawn in the tick *after* a successful spawn) on a different class (`SyncWorld`), and the fix is a one-line addition to a different method (`active_signatures`). Both reports should be addressed.
- `SyncWorld._materialize_mutations` (`world.py:139-178`) shares the same despawn-before-spawn ordering as `AsyncWorld.materialize_mutations`, so the sibling "spawn + despawn in the same tick on `SyncWorld`" bug is almost certainly present too. Out of scope for this report (the MRE here does not exercise it), but worth a follow-up hunt.
- `SyncWorld` has zero `step()` coverage in `tests/sync/`. Independent of this bug, the test module should grow at least one end-to-end "create → step → query → remove → step → query" test to keep the reference engine honest.
