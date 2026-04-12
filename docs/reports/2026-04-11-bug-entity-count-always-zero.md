# Bug Report: `WorldInfo.entity_count` and every REST world endpoint report 0 entities because `AsyncWorld` has no `entity_count` attribute

**Date:** 2026-04-11
**Severity:** Medium (incorrect data on every world-related REST response and service-layer call; monitoring and operator tooling is broken)
**Affects:** `archetype.app.world_service.WorldService.list_worlds`, all REST world endpoints (`GET /worlds`, `GET /worlds/{id}`, `POST /worlds`, `POST /worlds/{id}/fork`)
**Discovered by:** Overnight bug hunt

## Summary

`WorldService.list_worlds` (`world_service.py:107-119`) builds `WorldInfo` objects with `entity_count=getattr(world, "entity_count", 0)`. `AsyncWorld` (and `SyncWorld`) do not define an `entity_count` property or attribute — `getattr` always falls through to the default `0`. The REST endpoints copy this value into `WorldResponse.entity_count`, so every API response says `entity_count: 0` regardless of how many entities the world actually has. The real count is available as `len(world._entity2sig)` but the service layer never reads it.

## Impact

1. **Every REST world response says `entity_count: 0`.** `GET /worlds`, `GET /worlds/{id}`, `POST /worlds`, `POST /worlds/{id}/fork` — four endpoints, all returning wrong data. An operator querying the server to see "how many entities are in my simulation?" gets 0 for every world.
2. **CLI `archetype status` displays 0 entities.** The `status` command calls `GET /worlds` and renders the response — showing `entity_count=0` for every world.
3. **Monitoring dashboards see flat zero.** Any operator tooling that scrapes `entity_count` from the API response sees an unchanging 0 regardless of simulation activity. The metric is useless.
4. **The `WorldInfo` model in `app/models.py:134` has `entity_count: int = 0` with no signal that it's a stub.** The field looks functional. A user writing code that reads `result.entity_count` after `create_world` gets 0 and has no reason to suspect it's wrong.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 171c440, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: list_worlds reports entity_count=0 even after spawning 5 entities."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class P(Component):
    x: int = 0


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"), StorageConfig(uri=tmp)
            )
            world = container.world_service.get_world(info.world_id)
            for i in range(5):
                await world.create_entity([P(x=i)])
            await world.run(RunConfig(num_steps=1))

            actual = len(world._entity2sig)
            reported = container.world_service.list_worlds()[0].entity_count
            print(f"actual = {actual}, reported = {reported}")
            assert reported == actual, f"BUG: reported {reported}, expected {actual}"
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
actual = 5, reported = 0
AssertionError: BUG: reported 0, expected 5
```

### Baseline

`len(world._entity2sig)` correctly returns 5. The data exists — the service layer just doesn't read it.

## Root cause

`src/archetype/app/world_service.py:107-119`:

```python
def list_worlds(self) -> list[WorldInfo]:
    """Returns info for all managed worlds."""
    result = []
    for wid, world in self._worlds.items():
        info = WorldInfo(
            world_id=wid,
            name=getattr(world, "name", None),
            tick=getattr(world, "tick", 0),
            entity_count=getattr(world, "entity_count", 0),
            archetype_signatures=[],
        )
        result.append(info)
    return result
```

`getattr(world, "entity_count", 0)` — `AsyncWorld` has no such attribute. `getattr` returns `0`.

The same pattern at `api/routes/worlds.py:46,72,121`: `entity_count=getattr(world, "entity_count", 0)`.

The fix is to use `len(getattr(world, "_entity2sig", {}))` — or better, add a property to `AsyncWorld`:

```python
@property
def entity_count(self) -> int:
    return len(self._entity2sig)
```

## Why existing tests miss this

`tests/app/test_services.py:268-275::test_list_worlds` checks `len(worlds) == 1` and `worlds[0].world_id == world.world_id` but never checks `entity_count`. No test in the suite asserts `entity_count` from `list_worlds` after spawning entities.

## Suggested fixes

**Fix A — add a property to AsyncWorld (core/, needs approval):**

```diff
 # src/archetype/core/aio/async_world.py
+@property
+def entity_count(self) -> int:
+    """Number of tracked entities (pending + materialized)."""
+    return len(self._entity2sig)
```

**Fix B — use `_entity2sig` in the service layer (app/, no approval needed):**

```diff
 # src/archetype/app/world_service.py
 info = WorldInfo(
     world_id=wid,
     name=getattr(world, "name", None),
     tick=getattr(world, "tick", 0),
-    entity_count=getattr(world, "entity_count", 0),
+    entity_count=len(getattr(world, "_entity2sig", {})),
     archetype_signatures=[],
 )
```

Same change for `api/routes/worlds.py:46,72,121`.

## Suggested regression tests

```python
@pytest.mark.asyncio
async def test_list_worlds_reports_correct_entity_count(tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="t"), storage)
        for _ in range(5):
            await world.create_entity([P(x=0)])
        await world.run(RunConfig(num_steps=1))

        worlds = container.world_service.list_worlds()
        assert worlds[0].entity_count == 5
    finally:
        await container.shutdown()
```

## Notes / scope

- Affects `app/` and `api/`, not `core/`. Fix B lands without `core/` approval. Fix A is cleaner (adds the property where the data lives).
- `archetype_signatures` in `WorldInfo` is also always `[]` (never populated by `list_worlds`). Same shape but separate issue.
- SyncWorld has the same gap — no `entity_count` attribute either.
