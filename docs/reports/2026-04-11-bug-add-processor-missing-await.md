# Bug Report: `CommandType.ADD_PROCESSOR` and `REMOVE_PROCESSOR` discard a coroutine without awaiting it — the processor is never added or removed

**Date:** 2026-04-11
**Severity:** High (silent no-op on a documented public API; processor hot-swap from the broker is structurally broken)
**Affects:** `archetype.app.command_service.CommandService.apply` — the `CommandType.ADD_PROCESSOR` and `CommandType.REMOVE_PROCESSOR` dispatch arms
**Discovered by:** Overnight bug hunt

## Summary

`AsyncWorld.add_processor` and `AsyncWorld.remove_processor` are both `async def` (`async_world.py:381-385`). The dispatcher in `command_service.py:172-178` calls them **without `await`**:

```python
case CommandType.ADD_PROCESSOR:
    processor = payload["processor"]
    world.add_processor(processor)        # constructs a coroutine, drops it on the floor

case CommandType.REMOVE_PROCESSOR:
    proc_type = payload["processor_type"]
    world.remove_processor(proc_type)     # same
```

In Python, calling an `async def` function without `await` returns a *coroutine object* and never enters the function body. The coroutine is then garbage-collected unawaited, which produces `RuntimeWarning: coroutine 'AsyncWorld.add_processor' was never awaited`. The dispatcher swallows the warning (it's not an exception), the command is acked, the audit history records it as applied — and the processor list is unchanged.

This is the third silent-no-op in the same dispatcher (after `update-command-silently-noops` and `remove-component-strings-noop`), but the failure mode is different and arguably worse: the previous two return early due to a logical condition (`new_sig == old_sig`); this one **never executes a single line of `add_processor`'s body** because the coroutine is never awaited. It's a pure forgot-to-await bug.

## Impact

1. **Processor hot-swap from the broker is structurally broken.** `AGENTS.md` advertises `ADD_PROCESSOR` and `REMOVE_PROCESSOR` as the way to "Processor mutations (hot-swap behavior)" via the broker. Anyone who reads that and submits `Command(type=ADD_PROCESSOR, payload={"processor": MyProc()})` gets a 200, an audit entry, a quota slot consumed — and a world that runs without `MyProc`. There is no error, no log line above DEBUG, no failure indicator.
2. **`maintainer` and `admin` roles are advertised as having `add_processor` / `remove_processor` permission** (`auth/guard.py:23,30`). Both roles' "I can mutate processors at runtime" capability is broken: the RBAC layer says yes, the dispatcher says yes, the broker acks — but the processor list is untouched.
3. **`RuntimeWarning: coroutine ... was never awaited`** is the only signal, and it only fires at GC time (which can be much later than the `apply` call). It's silently emitted to stderr and not propagated through `drain_and_apply`'s `try/except`. Tests with default warning filters won't see it; production logging will see a stray warning with no command id attached.
4. **Combined with the four other already-filed dispatcher bugs, the broker's command surface is now broken for five of the seven entity-/processor-level command types**:
   - `SPAWN` ✅ works
   - `DESPAWN` ✅ works (modulo the same-tick bugs in `core/`)
   - `UPDATE` ❌ silent no-op (filed)
   - `ADD_COMPONENT` ✅ works (only for new types)
   - `REMOVE_COMPONENT` ❌ silent no-op for JSON callers (filed)
   - `ADD_PROCESSOR` ❌ silent no-op (this report)
   - `REMOVE_PROCESSOR` ❌ silent no-op (this report)
5. **Quota / audit lie.** `auth/guard.py` quota table (`add_processor: 2`, `remove_processor: 2`) is consumed; `_history` records the command as if applied; `drain_and_apply` adds it to `applied_ids`. None of those views reflect the truth ("the coroutine was discarded").

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 04769ce, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: ADD_PROCESSOR drops the coroutine returned by world.add_processor."""
import asyncio
import tempfile
import warnings

from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.aio import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig, WorldConfig


class Position(Component):
    x: int = 0


class NoopProc(AsyncProcessor):
    components = (Position,)
    priority = 5

    async def process(self, df, **kwargs):
        return df


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"), StorageConfig(uri=tmp)
            )
            world = container.world_service.get_world(info.world_id)
            print(f"processors before = {[type(p).__name__ for p in world.system.processors]}")

            cmd = Command(
                type=CommandType.ADD_PROCESSOR,
                payload={"processor": NoopProc()},
            )
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await container.command_service.apply(world, cmd)
            print(f"warnings emitted  = {[str(w.message) for w in caught]}")
            print(f"processors after  = {[type(p).__name__ for p in world.system.processors]}")
            assert any(isinstance(p, NoopProc) for p in world.system.processors), (
                "BUG: processor was not added"
            )
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
processors before = []
warnings emitted  = ["coroutine 'AsyncWorld.add_processor' was never awaited"]
processors after  = []
AssertionError: BUG: processor was not added
```

The coroutine warning is the smoking gun. `apply` returns normally, no exception, but `world.system.processors` is empty.

### Baseline (proves the bug is the missing `await`, not `add_processor` itself)

Calling `world.add_processor` directly with `await` works:

```python
await world.add_processor(NoopProc())
print(f"processors after direct await = {[type(p).__name__ for p in world.system.processors]}")
# processors after direct await = ['NoopProc']
# OK (baseline): awaited world.add_processor works correctly.
```

The world's `add_processor` is functional. The bug is purely the dispatcher dropping the coroutine.

## Root cause

`src/archetype/app/command_service.py:172-178`:

```python
case CommandType.ADD_PROCESSOR:
    processor = payload["processor"]
    world.add_processor(processor)

case CommandType.REMOVE_PROCESSOR:
    proc_type = payload["processor_type"]
    world.remove_processor(proc_type)
```

`src/archetype/core/aio/async_world.py:381-385`:

```python
async def add_processor(self, processor: "AsyncProcessor"):
    await self.system.add_processor(processor)

async def remove_processor(self, processor: type["AsyncProcessor"]):
    await self.system.remove_processor(processor)
```

`src/archetype/core/aio/async_system.py:43-50` (the underlying system, also async):

```python
async def add_processor(self, proc: "AsyncProcessor"):
    self.processors.append(proc)
    self.processors.sort(key=lambda p: p.priority)

async def remove_processor(self, proc_type: type["AsyncProcessor"]):
    self.processors = [p for p in self.processors if not isinstance(p, proc_type)]
```

Trace:

1. Caller submits `Command(type=ADD_PROCESSOR, payload={"processor": NoopProc()})`.
2. `CommandService.apply` matches `case CommandType.ADD_PROCESSOR:`.
3. `world.add_processor(processor)` — Python evaluates this as a function call:
   - `add_processor` is `async def`, so the function call constructs and returns a coroutine object representing the not-yet-started call.
   - The returned coroutine is **not** assigned to a variable and **not** awaited. It is immediately garbage-collected.
4. CPython's GC notices the coroutine was never awaited and emits `RuntimeWarning: coroutine 'AsyncWorld.add_processor' was never awaited`. This warning goes to stderr; it does not raise.
5. `apply` returns normally. `drain_and_apply` (`command_service.py:84-93`) records the command id in `applied_ids` and acks the broker.
6. The world's processor list is unchanged. The next `world.step` runs without `NoopProc`.

The bug exists because `add_processor` and `remove_processor` were async-converted on `AsyncWorld` (presumably because the system needed to be async-safe), but the dispatcher was not updated to await them. The sync world's methods at `sync/world.py:290-294` are *not* async — so the dispatcher's missing `await` would *also* be wrong if `world` were a `SyncWorld` (you'd be calling a sync method without await, which is correct). The dispatcher was probably written when both worlds had sync `add_processor` and never updated when `AsyncWorld` migrated to async.

`SyncWorld` is not currently wired through `ServiceContainer` (only `AsyncWorld` is — see `world_service.py:81`), so this bug fires 100% of the time on the production path.

## Why existing tests miss this

`grep -rn "ADD_PROCESSOR" tests/` returns three matches, all of which are RBAC-only:

- `tests/app/test_auth.py:50` — `cmd = Command(type=CommandType.ADD_PROCESSOR, payload={})`. The test only calls `guardrail_allow(cmd, ctx)` and asserts it raises `PermissionError` for a player. It never calls `command_service.apply`.
- `tests/app/test_auth.py:62` — `guardrail_allow(Command(type=CommandType.ADD_PROCESSOR, payload={}), ctx)` for a maintainer, asserts no raise. Again, never reaches `apply`.
- `tests/integration/test_command_flow.py:93-95`:
  ```python
  proc_cmd = Command(type=CommandType.ADD_PROCESSOR, payload={})
  with pytest.raises(PermissionError):
      await container.command_service.submit(str(world.world_id), proc_cmd, player_ctx)
  ```
  This *does* go through `command_service.submit` — but with a `player` ctx, which is denied at the RBAC layer in `enqueue` before reaching `apply`. Even more importantly, the payload is `{}` (no `"processor"` key). If the test were re-pointed at an admin and the `apply` path were exercised, it would hit `KeyError: 'processor'` long before reaching the missing-await bug — the test was never positioned to catch the actual dispatch defect.

`grep -rn "REMOVE_PROCESSOR" tests/` returns **zero matches**. There is no test in the entire suite that submits `REMOVE_PROCESSOR`.

The closest functional tests are `tests/app/test_services.py::TestSimulationService.test_add_processor` (which calls `simulation_service.add_processor` directly — bypassing the broker dispatch path) and the various `tests/aio/test_async_world_*` tests that call `world.add_processor(...)` directly with `await`. Both use the underlying world API; neither goes through `command_service.apply`.

## Suggested fixes

**Fix A — add `await`.** One-line fix:

```diff
 case CommandType.ADD_PROCESSOR:
     processor = payload["processor"]
-    world.add_processor(processor)
+    await world.add_processor(processor)

 case CommandType.REMOVE_PROCESSOR:
     proc_type = payload["processor_type"]
-    world.remove_processor(proc_type)
+    await world.remove_processor(proc_type)
```

This is the minimal correct fix. `apply` is already `async def` so adding `await` is free. The same change should be applied for any future `async def` method on the world that the dispatcher calls.

**Fix B (defence in depth) — make `apply`'s match arms always `await world.<method>(...)` even if the method is currently sync.** Both async and sync `add_processor` are awaitable in the sense that `await sync_value` raises a TypeError immediately, and any future migration of a sync method to async will already work without re-touching the dispatcher. But this would require auditing every `world.<method>` call site in `apply` and adding `await` everywhere — small effort, but it does change the surface to require everything to be awaitable.

A better Fix B: have `apply` check `inspect.iscoroutine` on the return value of every world call and `await` if needed:

```python
result = world.add_processor(processor)
if inspect.iscoroutine(result):
    await result
```

This is uglier than Fix A. Fix A is right; the only reason to consider Fix B is to defend against future async migrations of currently-sync methods. I'd recommend Fix A and a separate audit.

**Fix C — make the dispatcher itself catch unawaited coroutines.** Wrap `apply` in a `warnings.catch_warnings` filter that turns `RuntimeWarning` for unawaited coroutines into an exception, so the next time someone forgets `await` it surfaces as a hard failure during `drain_and_apply`. This is a defensive harness that catches the *category* of bug, not just this instance:

```python
async def apply(self, world: AsyncWorld, cmd: Command) -> None:
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=RuntimeWarning, message=".*was never awaited.*")
        # ... existing match block ...
```

The catch is that this only fires when the coroutine is GC'd — which may be after `apply` has already returned. So Fix C is unreliable. Fix A is the correct fix; Fix C is at best a smoke alarm.

I'd recommend **Fix A landed today** and a separate audit pass through every `world.<method>` call site in `command_service.apply` for "is this method async?".

## Suggested regression tests

Add to `tests/integration/test_command_flow.py`:

```python
@pytest.mark.asyncio
async def test_add_processor_command_actually_adds_processor(tmp_path):
    """Regression: ADD_PROCESSOR via the dispatcher must actually add the
    processor to the world's system, not silently drop the coroutine."""
    from archetype.core.aio import AsyncProcessor
    from archetype.core.component import Component

    class P(Component):
        x: int = 0

    class TestProc(AsyncProcessor):
        components = (P,)
        priority = 5
        async def process(self, df, **kwargs):
            return df

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)
        assert not any(isinstance(p, TestProc) for p in world.system.processors)

        cmd = Command(type=CommandType.ADD_PROCESSOR, payload={"processor": TestProc()})
        await container.command_service.apply(world, cmd)

        assert any(isinstance(p, TestProc) for p in world.system.processors), (
            "ADD_PROCESSOR dispatcher dropped the coroutine without awaiting"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_remove_processor_command_actually_removes_processor(tmp_path):
    """Regression: REMOVE_PROCESSOR must actually remove the processor."""
    from archetype.core.aio import AsyncProcessor
    from archetype.core.component import Component

    class P(Component):
        x: int = 0

    class TestProc(AsyncProcessor):
        components = (P,)
        priority = 5
        async def process(self, df, **kwargs):
            return df

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)
        await world.add_processor(TestProc())
        assert any(isinstance(p, TestProc) for p in world.system.processors)

        cmd = Command(
            type=CommandType.REMOVE_PROCESSOR,
            payload={"processor_type": TestProc},
        )
        await container.command_service.apply(world, cmd)

        assert not any(isinstance(p, TestProc) for p in world.system.processors), (
            "REMOVE_PROCESSOR dispatcher dropped the coroutine without awaiting"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_apply_does_not_emit_unawaited_coroutine_warning(tmp_path):
    """Defensive: any apply() call that hits a missing-await bug should
    surface as a test failure, not a silent stderr warning."""
    import warnings
    from archetype.core.aio import AsyncProcessor
    from archetype.core.component import Component

    class P(Component):
        x: int = 0

    class TestProc(AsyncProcessor):
        components = (P,)
        priority = 5
        async def process(self, df, **kwargs):
            return df

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await container.command_service.apply(
                world,
                Command(type=CommandType.ADD_PROCESSOR, payload={"processor": TestProc()}),
            )
        unawaited = [str(w.message) for w in caught if "never awaited" in str(w.message)]
        assert not unawaited, f"command_service.apply leaked unawaited coroutines: {unawaited}"
    finally:
        await container.shutdown()
```

The first two tests fail on `main` at the `assert any(isinstance(p, TestProc) ...)` lines. The third fails because the warning *is* present (it was the smoking gun in the MRE). All three pass after Fix A.

## Notes / scope

- Affects `src/archetype/app/command_service.py:172-178`. This is in `app/`, not `core/`, so Fix A can land directly without `core/` approval.
- Distinct from the eight other already-filed bugs:
  - The four `core/` mutation cache bugs are about world internals.
  - `simulation-service-run-discards-runconfig` is about `SimulationService.run` substituting `RunConfig`.
  - `update-command-silently-noops` is about UPDATE routing to the wrong target method.
  - `remove-component-strings-noop` is about REMOVE_COMPONENT not resolving JSON strings.
  - `lifecycle-commands-leak-broker` is about lifecycle commands never being acked.
  - This bug is about the dispatcher not awaiting two of its own arms. It is the most mechanical of the lot.
- This is the third "dispatcher silently no-ops" bug in `command_service.apply`. Three of the seven entity-/processor-level command types are now confirmed broken: UPDATE, REMOVE_COMPONENT, ADD_PROCESSOR/REMOVE_PROCESSOR. The dispatcher needs an audit pass — one PR could land all three fixes (`Fix A` of this report + `Fix A` of the UPDATE report + `Fix A` of the REMOVE_COMPONENT report).
- The same dispatcher is used for `SyncWorld` if/when it gets wired through `ServiceContainer`. `SyncWorld.add_processor` is sync (`sync/world.py:290`), so the dispatcher's missing-await is "correct" for sync but produces the same wrong-result by accident. After Fix A (which adds `await`), submitting `ADD_PROCESSOR` against a `SyncWorld` would raise `TypeError: object NoneType can't be used in 'await' expression`. That's a follow-up issue: the dispatcher needs to be either world-type-aware or the sync world's methods need to be async-compatible.
- Quota-exhaustion attack vector (mirrors the UPDATE / REMOVE_COMPONENT reports): an `admin` or `maintainer` can spam ADD_PROCESSOR, exhaust the `add_processor: 2` quota in `auth/guard.py`, and lock out legitimate processor mutations. None of which would have done anything anyway. Worth flagging in the security review.
