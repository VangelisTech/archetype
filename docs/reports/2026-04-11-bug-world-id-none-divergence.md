# Bug Report: `WorldConfig(world_id=None)` creates a world with `world_id=None` — `_worlds` is keyed by `None`, the local `world_id` variable in `create_world` is dead code

**Date:** 2026-04-11
**Severity:** Medium-High (silent state corruption — `_worlds` ends up with `None` as a key, which collapses every subsequent `world_id=None` create into the same entry; round-trip lookups via real UUIDs fail)
**Affects:** `archetype.app.world_service.WorldService.create_world` — every caller that constructs `WorldConfig(world_id=None)` (which the type signature `UUID | None` explicitly permits)
**Discovered by:** Overnight bug hunt

## Summary

`WorldConfig.world_id` is typed as `UUID | None` and has `default_factory=uuid.uuid7` (`config.py:224-227`). The default factory only fires when the field is *not provided*; passing `world_id=None` explicitly produces a `WorldConfig` with `world_id is None`. `WorldService.create_world` then runs:

```python
world_id = config.world_id or uuid7()        # <-- local var: fresh uuid7
...
world = await self.factory.create_world(world_config=config, ...)   # <-- config.world_id is still None
...
self._worlds[world.world_id] = world         # <-- world.world_id == None, key is None
```

The local `world_id` variable on line 67 *should* be the canonical id for the new world. Instead, the function passes `config` (with `world_id=None`) to the factory, and `AsyncWorld.__init__` reads `self.world_id = world_config.world_id` (`async_world.py:59`) — yielding `None`. The freshly-generated UUID on line 67 is discarded; nothing in the rest of the function uses it. `self._worlds` is keyed by `None`. `self._world_names[config.name] = world.world_id` writes `None` as the value.

The function "succeeds" — no exception, the returned `world` has `world_id=None`, the dict has a `None` key. The next call with `WorldConfig(world_id=None)` either collides with the same `None` key (returning the existing world via the `if world_id in self._worlds` early return) or, depending on the local `world_id` behavior, overwrites it.

## Impact

1. **`get_world` lookups by real UUID always fail.** A caller that passes `WorldConfig(world_id=None)`, gets a "successful" return, and then tries to look the world up by the freshly-generated UUID it expected to find sees `KeyError`. The world exists in `_worlds[None]`, not `_worlds[uuid7()]`. The only way to retrieve it is via `_worlds[None]` (un-documented internal access) or by holding the original return value.
2. **Two callers passing `world_id=None` collide on the `None` key.** The first call inserts at `_worlds[None]`. The second call: `world_id = None or uuid7()` (a different fresh uuid). `world_id in self._worlds` checks for the *fresh uuid* in the dict, which has only `None` as a key — so the early return at line 70 doesn't fire. The second call proceeds, builds a new world (also with `world_id=None` from the new config), inserts at `_worlds[None]`, **silently overwriting the first**. The first world is now orphaned: held by the original caller, but not in `_worlds`. The leak compounds with the just-filed `create-world-name-collision-orphan` report.
3. **The local `world_id` variable on line 67 is dead code.** After the assignment, it's never read again. The function should either use the local var to override `config.world_id` (and pass that to the factory), or short-circuit the dead code by removing it. The current state is "did the right thing locally, then threw it away".
4. **REST callers can't trigger this directly** because `CreateWorldRequest` doesn't expose `world_id`, but in-process callers and `discover_worlds` (`world_service.py:266`) and `apply_world_lifecycle.CREATE_WORLD` (`command_service.py:121-130`) all build `WorldConfig` from a payload `cfg = payload.get("config", {})`. If a payload contains `{"config": {"world_id": null}}`, the bug fires through the broker dispatch path. Cross-process state — the registry, the iceberg catalog tables — gets keyed by `"None"` strings.
5. **Discovery is invisible.** No exception, no warning, no log line. The caller's mental model ("world_id=None means generate a fresh one") is documented in `WorldConfig.world_id`'s description (`"If not provided, a new one will be generated."`) but the implementation only honours that path when the field is *omitted*, not when it's *None*. The two paths look identical to a Python user.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit c00edf4, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: WorldConfig(world_id=None) produces a world with world_id=None
because create_world's local fresh uuid7 is dead code."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.config import StorageConfig, WorldConfig


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            ws = container.world_service
            world = await ws.create_world(
                WorldConfig(name="t", world_id=None),
                StorageConfig(uri=tmp),
            )
            print(f"world.world_id = {world.world_id!r}")
            print(f"_worlds keys = {list(ws._worlds.keys())!r}")
            print(f"_world_names = {ws._world_names!r}")
            assert world.world_id is not None, (
                "BUG: WorldConfig(world_id=None) produced world_id=None"
            )
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
world.world_id = None
_worlds keys = [None]
_world_names = {'t': None}
AssertionError: BUG: WorldConfig(world_id=None) produced world_id=None
```

`world.world_id` is `None`. `_worlds` is keyed by `None`. `_world_names` maps the human-readable name to `None`. The freshly-generated UUID on `world_service.py:67` was discarded.

### Baseline (proves the bug is scoped to explicit `world_id=None`)

`WorldConfig()` (no explicit `world_id`) gets a default `uuid7()` from the factory. The dict is keyed by the actual UUID and round-trip lookup works:

```python
world = await container.world_service.create_world(
    WorldConfig(name="t"),  # default factory provides world_id
    StorageConfig(uri=tmp),
)
# world.world_id = UUID('019d7f3a-24fe-74f3-8c6e-046e4e1d723a')
# _worlds keys = [UUID('019d7f3a-24fe-74f3-8c6e-046e4e1d723a')]

looked_up = container.world_service.get_world(world.world_id)
assert looked_up is world
# OK (baseline): WorldConfig() default produces a real UUID and round-trip works.
```

The bug fires only when the caller explicitly sets `world_id=None`. The default-factory path is correct.

## Root cause

`src/archetype/core/config.py:217-228`:

```python
class WorldConfig(BaseModel):
    """
    A world configuration is a container for the world configuration, including:
      - world_id: Optional[UUID] - The unique identifier for the world. If not provided, a new one will be generated.
      - name: Optional[str] - A human-readable alias for the world.
    """

    world_id: UUID | None = Field(
        default_factory=uuid.uuid7,
        description="The unique identifier for the world. If not provided, a new one will be generated.",
    )
    name: str | None = Field(default=None, description="A human-readable alias for the world")
```

The type is `UUID | None`, the default factory is `uuid.uuid7`. **Pydantic's `default_factory` only fires when the field is omitted from the call. If the caller passes `world_id=None` explicitly, the field is `None`** — the `UUID | None` annotation permits it, the factory does not run.

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
    world_id = config.world_id or uuid7()       # <-- local var: fresh uuid7

    if world_id in self._worlds:
        return self._worlds[world_id]

    world = await self.factory.create_world(
        world_config=config,                    # <-- config.world_id is still None
        storage_config=storage_config,
        cache_config=cache_config,
        system=system or AsyncSystem(),
    )

    if self._broker and isinstance(world, AsyncWorld) and hasattr(world, "resources"):
        world.resources.insert(self._broker)

    self._worlds[world.world_id] = world        # <-- world.world_id == None
    ...
```

`src/archetype/core/aio/async_world.py:46-59`:

```python
class AsyncWorld(iAsyncWorld):
    def __init__(
        self,
        world_config: WorldConfig,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
    ):
        """
        Initialize the fully parallel async world.
        """
        # World Properties
        self.name = world_config.name
        self.world_id = world_config.world_id    # <-- reads from config, gets None
```

Trace for the MRE:

1. Caller: `WorldConfig(name="t", world_id=None)`. Pydantic creates a `WorldConfig` with `world_id=None`.
2. `await ws.create_world(config, storage_config)`:
   - Line 67: `world_id = config.world_id or uuid7()`. `config.world_id is None`, so `world_id = uuid7()` (a fresh UUID, e.g. `019d7f3a-24fe-74f3-8c6e-046e4e1d723a`).
   - Line 69: `world_id in self._worlds` — the fresh UUID is not in the dict (this is the first call). Proceed.
   - Line 72-77: `factory.create_world(world_config=config, ...)`. The factory passes `config` to `AsyncWorld.__init__`.
3. `AsyncWorld.__init__`: `self.world_id = world_config.world_id` → `self.world_id = None`. The freshly-generated UUID from line 67 is **never seen** by the world.
4. Back in `create_world`:
   - Line 83: `self._worlds[world.world_id] = world` → `self._worlds[None] = world`. The dict is keyed by `None`.
   - Line 88: `self._world_names["t"] = world.world_id` → `self._world_names["t"] = None`.
5. Function returns `world`. `world.world_id is None`. Caller has a "valid" world with no id.

The fix is one of:

- Use the local `world_id` to construct a corrected `WorldConfig` and pass that to the factory.
- Short-circuit the bug by mutating `config.world_id` (only safe because `WorldConfig.model_config` doesn't set `frozen=True`).
- Validate at the `WorldConfig` level: reject `world_id=None` at construction time, since the docstring says "If not provided, a new one will be generated" — `None` is not a valid value.

## Why existing tests miss this

`grep -rn "world_id=None" tests/` returns **zero matches**. No test in the suite passes `world_id=None` explicitly to `WorldConfig`.

`grep -rn "world_id is None" tests/` also returns zero matches. No test asserts that `world.world_id is not None` after `create_world`.

The closest existing test is `tests/app/test_factory.py::test_world_id_from_config` (line 74-87), which passes `WorldConfig(name="f5", world_id=wid)` with an explicit fresh `wid`. It verifies that the explicit UUID propagates correctly. It does *not* test the `world_id=None` path because the type permits it but no test exercises it.

The implicit invariant ("`WorldConfig.world_id` is always non-None after construction") is documented in the docstring but enforced nowhere. Pydantic's type annotation `UUID | None` advertises the opposite — that `None` is a valid value.

## Suggested fixes

**Fix A — `WorldConfig` rejects `world_id=None` at construction.** The cleanest fix: make the type `UUID` (no `| None`), and let the default factory fill it in when omitted. Lands in `core/`, requires approval:

```diff
 class WorldConfig(BaseModel):
     world_id: UUID | None = Field(
+    world_id: UUID = Field(
         default_factory=uuid.uuid7,
         description="The unique identifier for the world. If not provided, a new one will be generated.",
     )
```

After this change, `WorldConfig(world_id=None)` raises a `ValidationError` at construction time (Pydantic's `UUID` type rejects `None`). The caller is forced to either omit the field or provide a real UUID. The local `world_id = config.world_id or uuid7()` in `create_world` becomes provably dead code — `config.world_id` is always a UUID.

**Fix B — `create_world` uses the local `world_id` and patches the config.** Smaller change, lands in `app/`:

```diff
 async def create_world(
     self,
     config: WorldConfig,
     storage_config: StorageConfig,
     cache_config: CacheConfig | None = None,
     system: iAsyncSystem | None = None,
 ) -> iWorld:
-    world_id = config.world_id or uuid7()
+    if config.world_id is None:
+        # Type permits None, but we treat it as "generate one" per the docstring.
+        config = config.model_copy(update={"world_id": uuid7()})
+    world_id = config.world_id

     if world_id in self._worlds:
         return self._worlds[world_id]

     world = await self.factory.create_world(
         world_config=config,
         ...
     )
```

This is the minimal patch that makes the documented contract real. `config.model_copy(update={...})` is Pydantic's idiomatic way to produce a modified copy without mutating the original (since `WorldConfig` may or may not be frozen).

**Fix C — Pydantic field validator that fills in `None` with a fresh UUID at construction.** Lands in `core/`:

```diff
 class WorldConfig(BaseModel):
     world_id: UUID | None = Field(
         default_factory=uuid.uuid7,
         description="The unique identifier for the world. If not provided, a new one will be generated.",
     )
+
+    @field_validator("world_id", mode="before")
+    @classmethod
+    def _ensure_world_id(cls, v):
+        return v if v is not None else uuid.uuid7()
```

Fix C lets the type stay `UUID | None` (for back-compat with callers that pass `world_id=None`) but normalises None into a fresh UUID at construction. Behaviour matches the docstring.

I'd recommend **Fix A as the right shape** (the type should reflect reality) and **Fix C as a back-compat fallback** if there are callers passing `None` explicitly that would break under Fix A. Fix B is a stop-gap inside `app/` that doesn't require `core/` approval.

## Suggested regression tests

Add to `tests/app/test_factory.py` (or `tests/core/test_orchestrator_errors_and_instrumentation.py`):

```python
@pytest.mark.asyncio
async def test_world_config_with_explicit_none_world_id_gets_real_uuid(tmp_path):
    """Regression: WorldConfig(world_id=None) must produce a world with a
    real UUID, not None."""
    from uuid_utils import UUID

    ws = WorldService(StorageService())
    try:
        world = await ws.create_world(
            WorldConfig(name="t", world_id=None),
            StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
        )
        assert world.world_id is not None, (
            "WorldConfig(world_id=None) produced world_id=None — "
            "the local fresh uuid7 in create_world is dead code"
        )
        assert isinstance(world.world_id, UUID)
        # Round-trip lookup must work.
        assert ws.get_world(world.world_id) is world
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_world_config_explicit_none_does_not_collapse_to_same_dict_key(tmp_path):
    """Regression: two WorldConfig(world_id=None) calls must produce two
    distinct worlds, not collide on the same _worlds[None] entry."""
    ws = WorldService(StorageService())
    try:
        w1 = await ws.create_world(
            WorldConfig(name="a", world_id=None),
            StorageConfig(uri=str(tmp_path / "s1"), namespace="ns"),
        )
        w2 = await ws.create_world(
            WorldConfig(name="b", world_id=None),
            StorageConfig(uri=str(tmp_path / "s2"), namespace="ns"),
        )

        assert w1.world_id is not None
        assert w2.world_id is not None
        assert w1.world_id != w2.world_id, (
            "two WorldConfig(world_id=None) calls collapsed to the same id"
        )
        assert len(ws._worlds) == 2, (
            f"two distinct worlds, but _worlds has {len(ws._worlds)} entries"
        )
    finally:
        await ws.shutdown()
```

The first test fails on `main` at `assert world.world_id is not None`. The second fails at `assert len(ws._worlds) == 2` (gets 1, because the second call's `WorldConfig(world_id=None)` collapses to the same `None` key and overwrites).

## Notes / scope

- Affects `src/archetype/app/world_service.py:67-83` and `src/archetype/core/config.py:224-227`. The `create_world` function is in `app/` (Fix B); `WorldConfig` is in `core/` (Fix A and C, need approval).
- Distinct from the seventeen other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - The four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota accounting.
  - `component-prefix-collision`, `component-get-type-by-name-no-recurse`, `cached-store-read-shadows-disk` are Component / cache.
  - `create-world-name-collision-orphan` is the world-orphan leak on duplicate name.
  - `daily-tokens-never-reset` is the missing daily quota scheduler.
  - `storage-pool-key-ignores-cache-and-backend` is the multiton key.
  - This bug is the *type-vs-runtime* disconnect on `WorldConfig.world_id`. It's a sibling of `create-world-name-collision-orphan`: both are about `WorldService.create_world` letting partial / invalid state through.
- The bug compounds with `create-world-name-collision-orphan`: a caller that retries `create_world(WorldConfig(name="x", world_id=None))` first hits the `_worlds[None]` collapse, then might hit the name-collision orphan if the registry is involved. Together, the two leaks make `world_id=None` a particularly fragile pattern.
- `discover_worlds` (`world_service.py:243-278`) reconstructs `WorldConfig(world_id=wid, name=name)` with the registry-stored `world_id` — but the registry may have been written with `world_id=None` (per this bug) which would then be re-loaded as `None`. Worth verifying that the registry doesn't actually persist a `None` world_id; if it does, every server restart would re-create the bug.
- The `world_id = config.world_id or uuid7()` line on `world_service.py:67` is the *intent* to handle the None case. It just doesn't follow through. The fix is to make that intent real (Fix B) or to remove the dead branch entirely (Fix A).
