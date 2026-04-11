# Bug Report: `remove_entity` silently ignored when paired with `create_entity` in the same tick

**Date:** 2026-04-11
**Severity:** High (data-model correctness / silent desync)
**Affects:** `AsyncWorld` (core), any caller that routes through `CommandService` (`SPAWN` + `DESPAWN` at the same tick)
**Discovered by:** Overnight bug hunt

## Summary

If `create_entity(...)` and `remove_entity(eid)` are both called for the same entity **within a single tick boundary** (i.e. before `AsyncWorld.step()` has run), the subsequent step persists the entity as `is_active=True` and leaves the world in a desynchronised state:

- The entity **is** in `_live[sig]` and the backing store with `is_active=True`.
- The entity **is not** in `_entity2sig`, so it will never be processed again.
- `world.get_components([...])` and any `QueryService.get_state(...)` read path still returns it.
- No warning, no log, no exception — the despawn is silently dropped.

The user's mental model — "if I spawn something and then immediately despawn it, the step produces a world in which that entity never existed" — is violated. This is the exact behaviour every other ECS (Bevy, Flecs, Esper) guarantees and that `CommandBroker` users will assume when batching `SPAWN + DESPAWN` commands for the same tick.

## Impact

1. **CommandBroker batches are unsafe.** Submitting `[SPAWN(components=[...]), DESPAWN(entity_id=N)]` at `tick=0` through `CommandService.apply()` hits this bug 1:1 because `apply()` delegates to `world.create_entity()` / `world.remove_entity()`. Callers that build transactional batches — e.g. scenario setup code, MCTS rollouts that prune a candidate before the first tick, `spawn_world` fork scripts — can end up with orphaned rows.
2. **Silent storage corruption.** The orphan row is written to LanceDB by the updater (`is_active=True`, `tick=0`). Time-travel queries will forever return a "deleted" entity.
3. **`_entity2sig` desync is unrecoverable via the public API.** Because `_entity2sig` no longer has the entity, no future `remove_entity`, `add_components`, or `remove_components` call can touch it — `remove_entity` would hit the `No entity` warning path. The only way to reach it is a direct DataFrame mutation.
4. **`get_components` leaks deleted data.** Downstream processors reading via `get_components` will see and act on data the caller believes was removed.

## Reproduction

### Environment

- Branch: `main` (reproduced on commit 85bc355)
- Python 3.12
- `daft==0.7.5`
- Verified on Linux.

### Minimal Reproducible Example

```python
"""MRE: create_entity + remove_entity in the same tick fails to remove the entity.

Expected: step() produces an empty world.
Actual:   the "removed" entity is persisted with is_active=True, is queryable
          via get_components, and is orphaned from _entity2sig.
"""
import asyncio

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int = 0
    y: int = 0


async def main():
    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="mre"),
            StorageConfig(uri="/tmp/archetype_mre_data"),
        )
        world = container.world_service.get_world(info.world_id)

        # Spawn then immediately despawn — BEFORE any step.
        eid = await world.create_entity([Position(x=1, y=2)])
        await world.remove_entity(eid)

        # Step once. Expectation: no live entities.
        await world.step(RunConfig(num_steps=1))

        df = await world.get_components([Position])
        rows = df.to_pylist()

        print(f"Expected rows from get_components: 0")
        print(f"Actual rows from get_components:   {len(rows)}")
        print(f"_entity2sig has eid={eid}?  {eid in world._entity2sig}")
        print(f"_live has active row eid={eid}?  " + str(any(
            r.get('entity_id') == eid and r.get('is_active')
            for _, d in world._live.items()
            for r in d.to_pylist()
        )))

        assert len(rows) == 0, (
            f"BUG: remove_entity silently failed — entity {eid} is still alive "
            f"with row {rows[0] if rows else None}"
        )
    finally:
        await container.shutdown()


asyncio.run(main())
```

### Observed output

```
Expected rows from get_components: 0
Actual rows from get_components:   1
_entity2sig has eid=1?  False
_live has active row eid=1?  True
AssertionError: BUG: remove_entity silently failed — entity 1 is still alive
with row {'world_id': '...', 'run_id': '...', 'entity_id': 1, 'tick': 0,
          'is_active': True, 'position__x': 1, 'position__y': 2}
```

### Baseline (to prove the bug is scoped to "same tick" only)

This sequence works correctly — the bug does **not** fire when the spawn has already been materialised by a previous step:

```python
eid = await world.create_entity([Position(x=1, y=2)])
await world.step(RunConfig(num_steps=1))   # materialise spawn
await world.remove_entity(eid)
await world.step(RunConfig(num_steps=1))   # materialise despawn
# -> 0 live rows, correct
```

## Root cause

`AsyncWorld.materialize_mutations` (`src/archetype/core/aio/async_world.py:234-263`) applies the despawn mask **before** concatenating spawn rows, and the mask runs against the *previous-tick query result* — not against the pending spawn cache:

```python
def materialize_mutations(self, df: DataFrame, sig: ArchetypeSignature) -> DataFrame:
    # Handle Despawns
    if self._despawn_cache.get(sig):
        entities_to_despawn = list(set(self._despawn_cache[sig]))
        df = df.with_column(
            "is_active",
            when(col("entity_id").is_in(entities_to_despawn), then=False).otherwise(
                col("is_active")
            ),
        )
        self._despawn_cache[sig] = []

    # Handle Spawns
    if self._spawn_cache.get(sig):
        rows = list({row["entity_id"]: row for row in self._spawn_cache[sig]}.values())
        pyarrow_schema = Archetype.get_archetype_schema(sig)
        arrow_table = pa.Table.from_pylist(rows, schema=pyarrow_schema)
        spawns_df = daft.from_arrow(arrow_table)
        df = df.concat(spawns_df)  # <-- is_active=True, never masked
        self._spawn_cache[sig] = []

    return df
```

Trace for the MRE (`tick == 0`, entity has never been materialised):

1. `create_entity(Position(...))` →
   - `_spawn_cache[sig] = [row(eid=1, is_active=True)]`
   - `_entity2sig[1] = sig`
2. `remove_entity(1)` →
   - `_despawn_cache[sig] = [1]`
   - `_entity2sig.pop(1)` — **gone from the registry but still in `_spawn_cache`.**
3. `step()` → `_run_archetype(sig)`:
   1. Previous-tick query returns an **empty** DataFrame (schema only).
   2. `materialize_mutations`:
      - Despawn mask applied to the empty DF → no-op.
      - Spawn concat appends row `(eid=1, is_active=True)` untouched.
   3. Updater persists `(eid=1, is_active=True, tick=0)`.
   4. `self._live[sig] = df.where(col("is_active"))` — still contains the row.
4. `get_components([Position])` walks `_live` and returns the orphaned row.

There are two compounding design issues:

- **Ordering.** Despawns are applied before spawns, so any entity that appears in *both* caches during a single tick keeps its pre-mask `is_active=True` from the spawn side.
- **`remove_entity` does not cancel pending spawns.** `remove_entity` unconditionally appends to `_despawn_cache` and pops `_entity2sig`, without checking whether there is a matching row in `_spawn_cache`. This is what creates the orphan: after the step, `_entity2sig` is empty, but the live DF still holds the row.

## Why existing tests miss this

The only `create` + `remove` test in the suite is
`tests/aio/test_async_world_mutations.py::test_create_and_remove_entity_spawns_and_despawns`,
which always calls `world.step(rc)` **between** the create and the remove. The
"spawn and despawn in the same tick" path is never exercised. Likewise,
`tests/integration/test_broker_messaging.py` and the broker batch tests never
enqueue a `SPAWN + DESPAWN` pair for the same entity at the same tick.

## Suggested fixes

Either of these individually closes the bug. Fixing both is defence-in-depth.

**Fix A — cancel pending spawns inside `remove_entity`** (cleanest semantically; keeps `_live` tombstone-free):

```python
async def remove_entity(self, entity_id: int):
    sig = self._entity2sig.pop(entity_id, None)
    if sig is None:
        logger.warning(
            f"World {self.name} ({self.world_id}): Entity Removal Failed: "
            f"No entity: {entity_id}"
        )
        return

    # If the entity's spawn has not yet been materialised, cancel it instead
    # of scheduling a despawn — otherwise we end up with an orphaned row that
    # `_entity2sig` no longer knows about.
    pending = self._spawn_cache.get(sig)
    if pending:
        cancelled = [row for row in pending if row["entity_id"] == entity_id]
        if cancelled:
            self._spawn_cache[sig] = [
                row for row in pending if row["entity_id"] != entity_id
            ]
            return

    self._despawn_cache.setdefault(sig, []).append(entity_id)
```

**Fix B — reorder `materialize_mutations` so despawns mask newly-spawned rows too**:

```python
def materialize_mutations(self, df, sig):
    # Spawns first — so their rows are visible to the despawn mask.
    if self._spawn_cache.get(sig):
        ...
        df = df.concat(spawns_df)
        self._spawn_cache[sig] = []

    # Despawns last — now masks both pre-existing and just-spawned rows.
    if self._despawn_cache.get(sig):
        ...
        df = df.with_column("is_active", when(...).otherwise(...))
        self._despawn_cache[sig] = []

    return df
```

Fix A avoids writing a tombstone row to the store at all, which is closer to the "entity never existed" mental model and keeps the time-travel history clean. Fix B is smaller and covers the `CommandBroker` batch case even if a caller reaches into the caches directly in the future.

## Suggested regression tests

Add these to `tests/aio/test_async_world_mutations.py`:

```python
@pytest.mark.asyncio
async def test_create_then_remove_in_same_tick_yields_empty_world(world, store_backend):
    """Spawn + despawn before any step should leave the world empty."""
    eid = await world.create_entity([Position(x=1, y=2)])
    await world.remove_entity(eid)
    await world.step(RunConfig())

    # get_components should return no rows
    df = await world.get_components([Position])
    assert df.count_rows() == 0

    # _live should not contain an active row for the cancelled entity
    for sig, d in world._live.items():
        for r in d.to_pylist():
            assert r["entity_id"] != eid, (
                f"orphaned row remains in _live: {r}"
            )


@pytest.mark.asyncio
async def test_broker_spawn_then_despawn_in_same_tick(tmp_path):
    """SPAWN + DESPAWN commands enqueued at the same tick must cancel cleanly."""
    from uuid_utils import uuid7
    from archetype.app.container import ServiceContainer
    from archetype.app.models import Command, CommandType
    from archetype.app.auth.models import ActorCtx

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="batch"),
            StorageConfig(uri=str(tmp_path)),
        )
        ctx = ActorCtx(id=uuid7(), roles={"admin"})

        # Spawn, then despawn entity_id=1, both at tick=0
        await container.command_service.submit(
            info.world_id,
            Command(type=CommandType.SPAWN, payload={"components": [
                {"type": "Position", "x": 1, "y": 2},
            ]}),
            ctx,
        )
        await container.command_service.submit(
            info.world_id,
            Command(type=CommandType.DESPAWN, payload={"entity_id": 1}),
            ctx,
        )

        result = await container.simulation_service.run(
            info.world_id, RunConfig(num_steps=1),
        )

        world = container.world_service.get_world(info.world_id)
        df = await world.get_components([Position])
        assert df.count_rows() == 0
    finally:
        await container.shutdown()
```

## Notes / scope

- Affects `core/` (`AsyncWorld.materialize_mutations` + `remove_entity`). Per
  `CLAUDE.md`, `core/` is read-only for agents without explicit permission — so
  this report stops at diagnosis + suggested fix and does not touch the code.
- The `sync/` world (`src/archetype/core/sync/world.py`) should be audited for
  the same pattern — the architecture is mirrored but was not exercised by this
  bug hunt.
- `add_components` / `remove_components` move rows via `_despawn_cache` +
  `_spawn_cache` as well. Their "same tick" semantics should be re-verified
  once Fix A or B lands — especially `add_components` on an entity whose
  spawn has not yet been materialised, which is likely to have a sibling bug.
