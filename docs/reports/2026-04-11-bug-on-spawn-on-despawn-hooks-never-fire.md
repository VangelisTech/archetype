# Bug Report: `on_spawn` and `on_despawn` lifecycle hooks are documented in `add_hook` but never fired anywhere

**Date:** 2026-04-11
**Severity:** Medium-High (silent observability gap on a documented public API; documented extension point is non-functional)
**Affects:** `archetype.core.aio.async_world.AsyncWorld` — `add_hook` accepts `"on_spawn"` and `"on_despawn"` events but `_fire_hooks` is only called for `"pre_tick"` and `"post_tick"`
**Discovered by:** Overnight bug hunt

## Summary

`AsyncWorld.add_hook` (`async_world.py:87-100`) documents four lifecycle events in its docstring:

```
Supported events:
    - "pre_tick": Before any processing (world, tick)
    - "post_tick": After all processing (world, tick, results)
    - "on_spawn": When entity created (world, entity_id, components)
    - "on_despawn": When entity removed (world, entity_id)
```

But `grep -n "_fire_hooks\|on_spawn\|on_despawn" src/archetype/core/aio/async_world.py` shows that `_fire_hooks` is only called in two places, both for tick events:

```
153:        await self._fire_hooks("pre_tick", world=self, tick=self.tick)
184:        await self._fire_hooks("post_tick", world=self, tick=self.tick, results=results)
```

`create_entity` (`async_world.py:320-329`) and `remove_entity` (`async_world.py:331-338`) do not call `_fire_hooks`. Anywhere in the codebase. Anyone who reads the docstring and registers `world.add_hook("on_spawn", my_callback)` gets a silent no-op: the hook is stored in `_hooks["on_spawn"]`, takes up memory, and is never invoked. The world will spawn and despawn entities forever without ever telling the observer.

## Impact

1. **Observability tooling that depends on entity-level hooks is silently broken.** The natural use cases — logging entity creation, attaching telemetry to spawn events, tracking entity lifetimes for debugging, building per-entity audit trails — all silently never fire. Authors who write `world.add_hook("on_spawn", my_logger)` and don't manually verify the hook fires get no observability.
2. **The agent DSL's "compile to hooks" pattern is blocked.** `AGENTS.md` and `LEARNINGS.md` advertise `Hooks: Lifecycle Callbacks` as the way to do "observability and debugging without coupling to processor logic". A future DSL feature like `@behavior.on_spawn` would compile down to `world.add_hook("on_spawn", ...)` — and silently do nothing. The advertised extension point is unusable.
3. **Documentation gap is undetectable from the outside.** There is no warning, no `KeyError`, no exception when registering a hook on an undefined event. `_hooks` is a `defaultdict(list)` (`async_world.py:70`), so any string key is accepted as valid. The user gets the same response (`None` from `add_hook`) as for `pre_tick` and `post_tick`. From the user's perspective, the hook system advertises four events, all four accept registration, two of them work — and there's no signal which two until you instrument and check.
4. **`remove_hook` doesn't error either.** `world.remove_hook("on_spawn", fn)` succeeds (the hook is removed from `_hooks["on_spawn"]`), reinforcing the illusion that the event is real.
5. **Three `core/` mutation cache bugs already filed today exist precisely in the spawn/despawn lifecycle.** A working `on_spawn` / `on_despawn` event would have surfaced those bugs much faster — a hook that prints "spawned eid=1" would have exposed the silent leak in the `add-components-pending-spawn` MRE without requiring a downstream query. The missing observability hook is itself a contributor to how long similar bugs can hide.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit f377315, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: on_spawn / on_despawn hooks are documented but never fired."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int = 0
    y: int = 0


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"), StorageConfig(uri=tmp)
            )
            world = container.world_service.get_world(info.world_id)

            spawn_calls: list = []
            despawn_calls: list = []
            pre_tick_calls: list = []

            async def on_spawn(**kwargs):
                spawn_calls.append(kwargs)

            async def on_despawn(**kwargs):
                despawn_calls.append(kwargs)

            async def on_pre_tick(**kwargs):
                pre_tick_calls.append(kwargs)

            world.add_hook("on_spawn", on_spawn)
            world.add_hook("on_despawn", on_despawn)
            world.add_hook("pre_tick", on_pre_tick)
            print(f"hooks registered = {dict((k, len(v)) for k, v in world._hooks.items())}")

            eid = await world.create_entity([Position(x=1, y=2)])
            await world.run(RunConfig(num_steps=1))
            await world.remove_entity(eid)
            await world.run(RunConfig(num_steps=1))

            print(f"on_spawn calls   = {len(spawn_calls)}")
            print(f"on_despawn calls = {len(despawn_calls)}")
            print(f"pre_tick calls   = {len(pre_tick_calls)}")
            assert spawn_calls, "on_spawn never fired"
            assert despawn_calls, "on_despawn never fired"
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
hooks registered = {'on_spawn': 1, 'on_despawn': 1, 'pre_tick': 1}
on_spawn calls   = 0
on_despawn calls = 0
pre_tick calls   = 2
AssertionError: on_spawn never fired
```

The hooks were registered (the dict shows three keys with one entry each), but neither `on_spawn` nor `on_despawn` was ever called. `pre_tick` correctly fired twice (one per `world.run` call), proving that the hook *plumbing* works for events that `_fire_hooks` is actually called for.

### Baseline (proves the hook plumbing is functional for the events that are wired)

```python
world.add_hook("pre_tick", on_pre)
world.add_hook("post_tick", on_post)

await world.create_entity([Position(x=1)])
await world.run(RunConfig(num_steps=3))

# pre_tick calls  = [0, 1, 2]
# post_tick calls = [1, 2, 3]
# OK (baseline): pre_tick and post_tick fire correctly.
```

`pre_tick` fires three times with `tick=0,1,2` and `post_tick` fires three times with `tick=1,2,3` (the `+1` after `tick += 1`, which matches the docstring at `async_world.py:137`). Hook plumbing is fine; the bug is purely missing wiring for `on_spawn` and `on_despawn`.

## Root cause

`src/archetype/core/aio/async_world.py:87-100` (the docstring promising four events):

```python
def add_hook(self, event: str, fn: HookFn) -> None:
    """
    Register a hook for lifecycle events.

    Supported events:
        - "pre_tick": Before any processing (world, tick)
        - "post_tick": After all processing (world, tick, results)
        - "on_spawn": When entity created (world, entity_id, components)
        - "on_despawn": When entity removed (world, entity_id)

    Example:
        world.add_hook("post_tick", lambda world, tick, **kw: print(f"Tick {tick} done"))
    """
    self._hooks[event].append(fn)
```

`src/archetype/core/aio/async_world.py:106-114` (the only mechanism for firing hooks):

```python
async def _fire_hooks(self, event: str, **kwargs) -> None:
    """Fire all hooks for an event, logging but not raising on errors."""
    for hook in self._hooks[event]:
        try:
            await hook(**kwargs)
        except Exception as e:
            logger.warning(f"Hook {getattr(hook, '__name__', hook)} failed on {event}: {e}")
```

`grep -n "_fire_hooks" src/archetype/core/aio/async_world.py`:

```
106:    async def _fire_hooks(self, event: str, **kwargs) -> None:
153:        await self._fire_hooks("pre_tick", world=self, tick=self.tick)
184:        await self._fire_hooks("post_tick", world=self, tick=self.tick, results=results)
```

Two call sites, both inside `step()`. `create_entity` (`async_world.py:320-329`) and `remove_entity` (`async_world.py:331-338`) both touch `_spawn_cache` / `_despawn_cache` / `_entity2sig` and return without calling `_fire_hooks`:

```python
async def create_entity(self, components: list[Component]) -> int:
    entity_id = self._next_entity_id
    self._next_entity_id += 1
    sig = Archetype.sig_from_components(components)
    self._entity2sig[entity_id] = sig

    # Placeholder run_id; updater will stamp correct run_id on update
    row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, run_id="")
    self._spawn_cache.setdefault(sig, []).append(row_dict)
    return entity_id

async def remove_entity(self, entity_id: int):
    sig = self._entity2sig.pop(entity_id, None)
    if sig:
        self._despawn_cache.setdefault(sig, []).append(entity_id)
    else:
        logger.warning(...)
```

Neither method ever calls `await self._fire_hooks("on_spawn", ...)` or `await self._fire_hooks("on_despawn", ...)`. The same is true for `add_components` (`:340-360`) and `remove_components` (`:362-379`), which also represent "entity lifecycle" events but have no documented hook coverage.

The bug is a documentation-vs-implementation mismatch: the docstring promises four events, the runtime only delivers two.

## Why existing tests miss this

`grep -rn "on_spawn\|on_despawn" tests/` returns **zero matches**. There is no test in the entire suite that registers an `on_spawn` or `on_despawn` hook, let alone asserts that it fires.

`tests/core/test_resources_hooks_messaging.py` contains the hook tests. Searching for `add_hook` in that file shows tests for:

- `test_add_hook` — registers a `pre_tick` hook, asserts it fires.
- `test_remove_hook` — registers and removes a hook.
- `test_post_tick_hook` — registers `post_tick`, asserts.

No test exercises `on_spawn` or `on_despawn`. The docstring's claims are entirely unverified.

`grep -rn "on_spawn\|on_despawn" src/archetype/` outside the docstring returns no matches either: the events are not used by any production code path. They are pure documentation.

## Suggested fixes

**Fix A — wire `_fire_hooks` into `create_entity`, `remove_entity`, `add_components`, `remove_components`.** This requires touching `core/`, so it would need explicit human approval. Diff sketch:

```diff
 async def create_entity(self, components: list[Component]) -> int:
     entity_id = self._next_entity_id
     self._next_entity_id += 1
     sig = Archetype.sig_from_components(components)
     self._entity2sig[entity_id] = sig

     row_dict = Archetype.to_row_dict(entity_id, self.tick, components, self.world_id, run_id="")
     self._spawn_cache.setdefault(sig, []).append(row_dict)
+    await self._fire_hooks("on_spawn", world=self, entity_id=entity_id, components=components)
     return entity_id

 async def remove_entity(self, entity_id: int):
     sig = self._entity2sig.pop(entity_id, None)
     if sig:
         self._despawn_cache.setdefault(sig, []).append(entity_id)
+        await self._fire_hooks("on_despawn", world=self, entity_id=entity_id)
     else:
         logger.warning(...)
```

The same pattern should be applied to `add_components` and `remove_components`, with new event names if a finer-grained signal is desired (e.g. `"on_components_added"` / `"on_components_removed"`), or by re-using `on_spawn` / `on_despawn` with a contextual flag.

A subtlety: hooks fire *immediately* when `create_entity` is called, not at materialisation time. That matches the docstring ("When entity created"), but it means hook implementations see entities in the `_spawn_cache` before the next `step()` writes them to the store. Hook authors should be told this in the docstring.

**Fix B — make `add_hook` raise on unknown event names.** The minimal fix that surfaces the bug instead of silently swallowing it. Lands in `core/`, still needs approval:

```diff
+_VALID_EVENTS = frozenset({"pre_tick", "post_tick", "on_spawn", "on_despawn"})
+
 def add_hook(self, event: str, fn: HookFn) -> None:
     """..."""
+    if event not in _VALID_EVENTS:
+        raise ValueError(
+            f"Unknown hook event {event!r}. Supported: {sorted(_VALID_EVENTS)}"
+        )
     self._hooks[event].append(fn)
```

This catches typos (`"on_step"`, `"pretick"`) at registration time. It does not by itself fix the underlying "on_spawn / on_despawn never fire" bug — Fix A is still needed.

**Fix C — remove `on_spawn` and `on_despawn` from the docstring** and update `LEARNINGS.md`'s "Hooks: Lifecycle Callbacks" section to advertise only the two events that actually work. Lands in `core/` (docstring change) and the docs. This is the most conservative fix — accept that the events aren't implemented and stop promising them. **Don't pick this one as the only fix**: removing the docstring doesn't restore the observability path that the bug breaks. Consider it only as a stop-gap until Fix A lands.

I'd recommend **Fix A as the real fix** (it implements the documented contract) plus **Fix B as a defence** (it catches future typos and surfaces unknown events).

## Suggested regression tests

Add to `tests/core/test_resources_hooks_messaging.py` (the file that already houses hook tests):

```python
@pytest.mark.asyncio
async def test_on_spawn_hook_fires_when_entity_created(tmp_path):
    """Regression: on_spawn hook must fire on create_entity."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="t"), storage_config=storage)

        spawn_events: list = []

        async def on_spawn(**kwargs):
            spawn_events.append(kwargs)

        world.add_hook("on_spawn", on_spawn)
        eid = await world.create_entity([Position(x=1, y=2)])

        assert len(spawn_events) == 1
        assert spawn_events[0]["entity_id"] == eid
        assert spawn_events[0]["world"] is world
        # Components should be passed to the hook so observers can inspect them.
        assert any(isinstance(c, Position) for c in spawn_events[0].get("components", []))
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_on_despawn_hook_fires_when_entity_removed(tmp_path):
    """Regression: on_despawn hook must fire on remove_entity."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="t"), storage_config=storage)

        despawn_events: list = []

        async def on_despawn(**kwargs):
            despawn_events.append(kwargs)

        eid = await world.create_entity([Position(x=1, y=2)])
        world.add_hook("on_despawn", on_despawn)
        await world.remove_entity(eid)

        assert len(despawn_events) == 1
        assert despawn_events[0]["entity_id"] == eid
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_add_hook_rejects_unknown_event_names(tmp_path):
    """Defensive: typos in event names should fail loudly at add_hook
    time, not silently sit in _hooks forever."""
    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(WorldConfig(name="t"), storage_config=storage)

        async def noop(**kw):
            pass

        with pytest.raises(ValueError, match="Unknown hook event"):
            world.add_hook("pretick", noop)  # missing underscore — typo
        with pytest.raises(ValueError, match="Unknown hook event"):
            world.add_hook("on_create", noop)  # wrong name
    finally:
        await ws.shutdown()
```

The first two tests fail on `main` at the `assert len(spawn_events) == 1` / `assert len(despawn_events) == 1` lines. The third fails because `add_hook` happily accepts any string today.

## Notes / scope

- Affects `src/archetype/core/aio/async_world.py:87-100` (the docstring) and the `create_entity` / `remove_entity` / `add_components` / `remove_components` methods that should be calling `_fire_hooks` but aren't. Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- Distinct from the nine other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Two dispatcher silent-no-op bugs (`update`, `remove_component`) and one missing-await bug (`add_processor`) are in `command_service.py`.
  - `simulation-service-run-discards-runconfig` is `SimulationService.run` substituting `RunConfig`.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - This bug is the inverse: the world has *no* documented observation point for spawn/despawn, despite the docstring claiming otherwise.
- The sync world (`SyncWorld`) does not have a hook system at all (`grep -n "_hooks\|add_hook" src/archetype/core/sync/world.py` returns nothing). When/if `SyncWorld` is wired through `ServiceContainer`, this gap will widen — sync users will have *no* hook support of any kind.
- The agent DSL (`archetype.dsl`, mentioned in `LEARNINGS.md`) advertises `@behavior.on_spawn` / `on_despawn` decorators as a future ergonomics layer over hooks. That entire layer is blocked until this bug is fixed.
- Prior overnight bug-hunt iterations would have benefited directly from working `on_spawn` / `on_despawn` hooks: the `add-components-pending-spawn` MRE explicitly checks `_spawn_cache` to confirm the orphan; a `world.add_hook("on_spawn", ...)` would have given the same signal with one line, no internal-state inspection. The lack of this hook makes every other bug in the spawn/despawn area harder to diagnose.
