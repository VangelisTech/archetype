# Bug Report: `AsyncSystem.execute` filters `input_kwargs` against named parameters only — processors that catch `**kwargs` silently receive an empty dict

**Date:** 2026-04-11
**Severity:** Medium-High (silent contract violation on the documented `AsyncProcessor.process(self, df, **input_kwargs)` base signature; future processors that read `tick`/`resources`/etc. from `kwargs` get nothing)
**Affects:** `archetype.core.aio.async_system.AsyncSystem.execute` — every processor whose `process` method uses `**kwargs` (catch-all) instead of explicit named parameters
**Discovered by:** Overnight bug hunt

## Summary

`AsyncSystem.execute` filters the kwargs that get forwarded to each processor by `inspect.signature(proc_instance.process).parameters` (`async_system.py:93-96`):

```python
sig_params = inspect.signature(proc_instance.process).parameters
filtered_input_kwargs = {
    k: v for k, v in input_kwargs.items() if k in sig_params
}
df = await proc_instance.process(df, **filtered_input_kwargs)
```

The filter checks each kwarg name against the processor's *named* parameters. It does **not** detect `VAR_KEYWORD` (`**kwargs`) parameters. So a processor with the signature `async def process(self, df, **kwargs)` (the documented base-class signature) has `sig_params = {'self', 'df', 'kwargs'}` — and since none of `tick`, `resources`, etc. are in that set, **every kwarg is filtered out**. The processor's `kwargs` dict is empty.

The contract from the base class `AsyncProcessor.process(self, df: DataFrame, **input_kwargs) -> DataFrame` (`async_processor.py:26`) and the in-repo pattern of `process(self, df, **kwargs)` is completely broken: a processor following the documented signature pattern silently receives no kwargs at all.

## Impact

1. **`AsyncProcessor.process(self, df, **input_kwargs)` — the documented base signature — is unusable.** Any subclass that inherits the kwargs catch-all and tries to read `kwargs.get("tick")`, `kwargs.get("resources")`, `kwargs.get("debug")` from the body silently sees `None`. The framework hands them an empty dict and they have no signal.
2. **Trajectory pipeline processors are *almost* exposed.** `src/archetype/trajectories/processors.py:105-108` defines `SamplingProcessor.process(self, df, resources: Resources | None = None, **kwargs: Any)` and then does `resources = resources or kwargs.get("resources") or Resources()`. The named `resources` param works (the filter sees `'resources' in sig_params`), so the fallback `kwargs.get("resources")` is dead code today. But the processor's *intent* — "let `resources` come from either the named param or the kwargs catch-all" — is broken; the second path never fires.
3. **Future-proofing for `tick`-aware processors is broken.** A processor that wants to access `tick` via `kwargs.get("tick")` (e.g., for tick-gated logic per the `LEARNINGS.md` "Tick-Gated Processing" section) needs to declare `tick` as a named parameter. The natural Python pattern of catching everything via `**kwargs` and pulling out what you need is silently broken.
4. **`debug` is *also* never propagated to processors** (same root cause but slightly different mechanism). `world.execute(df, sig, **input_kwargs)` calls `system.execute(df, sig, resources=self.resources, **input_kwargs)`. system.execute's signature has `debug=False` as a *named* parameter, so the `debug` from input_kwargs binds to it instead of staying in `**input_kwargs`. The body adds `resources` back but never adds `debug`. Inside the per-processor loop, `input_kwargs` contains `tick` and `resources` but **not** `debug`. So even a processor with `debug` as a named parameter never receives it via this path. Two layers of stripping for one kwarg.
5. **Example-level breakage.** `examples/world_mutations.py` and `examples/simulation_script.py` define processors as `process(self, df, **kwargs)` per the documented pattern. They don't access kwargs in the body today, so they're not visibly broken — but the broken pattern is *advertised* in the example code as "this is how you write a processor". A user copy-pasting the example and adding `tick = kwargs.get("tick")` gets `None`.
6. **Discovery is silent.** No exception, no warning, no log line. The kwargs dict is just empty. Processors fall through to default values (None, 0, etc.) and produce subtly wrong results.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 6473fe3, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: AsyncSystem.execute strips all kwargs from a **kwargs-only processor."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.aio import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class P(Component):
    x: int = 0


class CatchAllProc(AsyncProcessor):
    components = (P,)
    priority = 5

    async def process(self, df, **kwargs):
        # Should receive tick, resources, debug from world.execute via the
        # documented kwargs catch-all pattern.
        print(f"CatchAllProc.process kwargs = {sorted(kwargs.keys())}")
        return df


class NamedKwargsProc(AsyncProcessor):
    components = (P,)
    priority = 6

    async def process(self, df, tick=None, debug=None, resources=None, **kwargs):
        print(
            f"NamedKwargsProc.process tick={tick} debug={debug} "
            f"resources={'set' if resources else 'None'} kwargs={sorted(kwargs.keys())}"
        )
        return df


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"), StorageConfig(uri=tmp)
            )
            world = container.world_service.get_world(info.world_id)
            await world.system.add_processor(CatchAllProc())
            await world.system.add_processor(NamedKwargsProc())
            await world.create_entity([P(x=1)])
            await world.run(RunConfig(num_steps=1, debug=True))
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
CatchAllProc.process kwargs = []
NamedKwargsProc.process tick=0 debug=None resources=set kwargs=[]
```

`CatchAllProc.process` received an *empty* kwargs dict despite the system having `tick=0`, `resources=<Resources>`, and `debug=True` (from the user's `RunConfig`). The catch-all pattern receives nothing. `NamedKwargsProc` received `tick=0` and `resources=<Resources>` (because they're named parameters and the filter matched), but `debug=None` even though the user explicitly set `debug=True` — `debug` is never propagated to processors at all.

### Baseline (proves the bug is the **kwargs filter, not the kwargs propagation)

A processor with `tick`/`resources` as explicit named parameters DOES receive them:

```python
class NamedProc(AsyncProcessor):
    components = (P,)
    priority = 5

    async def process(self, df, tick=None, resources=None, **kwargs):
        print(f"NamedProc.process tick={tick} resources={'set' if resources else 'None'}")
        return df

# NamedProc.process tick=0 resources=set
# OK (baseline): named params receive tick/resources correctly.
```

The named-params path works. The bug is purely the filter's failure to detect `**kwargs` catch-alls.

## Root cause

`src/archetype/core/aio/async_system.py:51-109`:

```python
async def execute(
    self,
    df: DataFrame,
    sig: ArchetypeSignature,
    resources: Resources | None = None,
    debug: bool = False,
    **input_kwargs,
) -> DataFrame:
    """..."""
    # Include resources in kwargs for processors that want it
    if resources is not None:
        input_kwargs["resources"] = resources

    archetype_name = Archetype.get_name(sig) if debug else None

    for proc_instance in sorted(self.processors, key=lambda x: x.priority):
        if set(proc_instance.components).issubset(set(sig)):
            proc_name = proc_instance.__class__.__name__

            if debug:
                logger.debug(...)

            try:
                assert isinstance(proc_instance, AsyncProcessor)
                # Filter input_kwargs to only what the processor accepts to avoid unexpected input_kwargs
                sig_params = inspect.signature(proc_instance.process).parameters
                filtered_input_kwargs = {
                    k: v for k, v in input_kwargs.items() if k in sig_params
                }
                df = await proc_instance.process(df, **filtered_input_kwargs)
                ...
```

`inspect.signature(...).parameters` returns an `OrderedDict` of `name -> Parameter`. Each `Parameter` has a `kind` attribute (`POSITIONAL_OR_KEYWORD`, `KEYWORD_ONLY`, `VAR_POSITIONAL`, `VAR_KEYWORD`, etc.). For a method like `process(self, df, **kwargs)`, the OrderedDict is:

```python
{
    'self': <Parameter "self">,                    # POSITIONAL_OR_KEYWORD
    'df': <Parameter "df: DataFrame">,             # POSITIONAL_OR_KEYWORD
    'kwargs': <Parameter "**kwargs">,              # VAR_KEYWORD
}
```

The filter `if k in sig_params` checks if the kwarg's *name* is in the OrderedDict's keys. But the **VAR_KEYWORD** parameter is named `'kwargs'`, not `'tick'` or `'resources'`. So `'tick' in sig_params` returns `False` even though `**kwargs` would happily accept `tick=...`.

A correct implementation must detect `VAR_KEYWORD` separately:

```python
sig_params = inspect.signature(proc_instance.process).parameters
accepts_var_keyword = any(
    p.kind == inspect.Parameter.VAR_KEYWORD for p in sig_params.values()
)
if accepts_var_keyword:
    filtered_input_kwargs = dict(input_kwargs)
else:
    filtered_input_kwargs = {k: v for k, v in input_kwargs.items() if k in sig_params}
```

The `debug` propagation issue is a *second* bug in the same function. `world.execute(df, sig, tick=..., debug=..., **input_kwargs)` (line 215 of `async_world.py`) calls `system.execute(df, sig, resources=self.resources, **input_kwargs)`. `system.execute`'s signature has `debug: bool = False` as a *named* parameter, so the `debug` from `input_kwargs` binds to it instead of being forwarded into `**input_kwargs`. The body uses `debug` for its own internal logging but never re-injects it into `input_kwargs` for the processor loop.

Trace for the MRE:

1. `world.run(RunConfig(num_steps=1, debug=True))` → `world.step(rc)` → `_run_archetype(sig, rc)`.
2. `_run_archetype` (`async_world.py:215`): `df = await self.execute(df, sig, tick=self.tick, debug=run_config.debug, **input_kwargs)`. `input_kwargs` is `{}` (empty). The keyword args become `{tick: 0, debug: True}`.
3. `world.execute(df, sig, **input_kwargs)` (`async_world.py:470`) — accepts `**input_kwargs`. So `world.execute`'s `input_kwargs` is `{'tick': 0, 'debug': True}`. Then it calls `self.system.execute(df, sig, resources=self.resources, **input_kwargs)`.
4. `system.execute`'s signature is `(self, df, sig, resources=None, debug=False, **input_kwargs)`. Binding the call:
   - `df` → first positional
   - `sig` → second positional
   - `resources` → bound to named `resources` parameter (value: `self.resources`)
   - From the **input_kwargs spread: `tick=0` → goes into system.execute's `**input_kwargs` (system.execute has no named `tick`); `debug=True` → binds to system.execute's named `debug` parameter (NOT in input_kwargs)
5. Body: `if resources is not None: input_kwargs["resources"] = resources`. So `input_kwargs == {'tick': 0, 'resources': <Resources>}`. `debug` is NOT in `input_kwargs` — it's stuck as a local variable.
6. For each processor:
   - `sig_params` for `CatchAllProc.process(self, df, **kwargs)` = `{'self', 'df', 'kwargs'}`.
   - Filter: `{k: v for k, v in {'tick': 0, 'resources': ...} if k in {'self', 'df', 'kwargs'}}` = `{}` (empty, neither `tick` nor `resources` is in the named param set).
   - `await proc.process(df, **{})` → `process(self, df)` — kwargs is `{}`. ✗ Bug.
   - `sig_params` for `NamedKwargsProc.process(self, df, tick=None, debug=None, resources=None, **kwargs)` = `{'self', 'df', 'tick', 'debug', 'resources', 'kwargs'}`.
   - Filter: `{tick, resources}` (both are in named params; `debug` is in named params but NOT in input_kwargs because of step 4).
   - `await proc.process(df, tick=0, resources=...)` → `process(self, df, tick=0, debug=None, resources=..., **{})`. `debug=None` because the kwarg was never forwarded.

The contract violation: the documented `AsyncProcessor.process(self, df, **input_kwargs)` signature is supposed to be the canonical pattern for "I accept whatever the system sends me". Today, that pattern receives `{}`.

## Why existing tests miss this

`grep -rn "kwargs.get" tests/` — the only matches are in trajectories tests, and the trajectories processors *also* declare `resources` as a named parameter, so the filter matches and the catch-all fallback is dead code. No test exercises a processor that uses `**kwargs` *only* and then asserts what's in `kwargs`.

Existing tests for `AsyncSystem`:

- `tests/aio/test_async_world_execution.py:29`: `class P1ScaleX(AsyncProcessor): async def process(self, df, scale=1, **kwargs): ...` — uses a named `scale` parameter, no assertion on `kwargs` contents.
- `tests/aio/test_async_world_execution.py:37`: same pattern.
- `tests/core/test_async_world_duplicate_spawn_overwrite.py:18`: `process(self, df, **kwargs)` — body doesn't read kwargs.
- `tests/core/test_async_world_error_propagation.py:18,26`: `process(self, df, **kwargs)` — body doesn't read kwargs.
- `tests/core/test_async_world_cast_paths.py:19`: same pattern.

None of these tests:

1. Define a processor with `process(self, df, **kwargs)` and assert that `kwargs` contains `tick`, `resources`, etc.
2. Define a processor that depends on reading something from `kwargs` and check that the value is non-None.

The bug is structurally invisible to the test suite because no test ever inspects what's in the catch-all kwargs dict.

## Suggested fixes

**Fix A — detect VAR_KEYWORD parameters and pass everything through.** The minimal correct fix:

```diff
 try:
     assert isinstance(proc_instance, AsyncProcessor)
-    # Filter input_kwargs to only what the processor accepts to avoid unexpected input_kwargs
     sig_params = inspect.signature(proc_instance.process).parameters
-    filtered_input_kwargs = {
-        k: v for k, v in input_kwargs.items() if k in sig_params
-    }
+    # If the processor catches **kwargs, pass everything through; otherwise
+    # filter to only what it names explicitly.
+    accepts_var_keyword = any(
+        p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values()
+    )
+    if accepts_var_keyword:
+        filtered_input_kwargs = dict(input_kwargs)
+    else:
+        filtered_input_kwargs = {
+            k: v for k, v in input_kwargs.items() if k in sig_params
+        }
     df = await proc_instance.process(df, **filtered_input_kwargs)
```

Lands in `core/`, requires approval. This makes the documented base-class signature work as advertised.

**Fix B — also propagate `debug` through `system.execute`'s input_kwargs.** Separate fix in the same function. After binding `debug` to the named param, also re-inject it into `input_kwargs` so it reaches processors:

```diff
 async def execute(
     self,
     df: DataFrame,
     sig: ArchetypeSignature,
     resources: Resources | None = None,
     debug: bool = False,
     **input_kwargs,
 ) -> DataFrame:
     """..."""
-    # Include resources in kwargs for processors that want it
+    # Include resources and debug in kwargs for processors that want them
     if resources is not None:
         input_kwargs["resources"] = resources
+    input_kwargs["debug"] = debug
```

Both fixes land in the same file. Apply them together.

**Fix C — drop the filter entirely; rely on Python to raise on unknown kwargs.** The simplest possible fix: pass all kwargs unconditionally and let the processor's signature reject unknowns at call time. Processors that don't accept `**kwargs` and don't name a kwarg will raise `TypeError`, which is *correct* behaviour ("you tried to pass me something I don't accept"):

```diff
-    sig_params = inspect.signature(proc_instance.process).parameters
-    filtered_input_kwargs = {
-        k: v for k, v in input_kwargs.items() if k in sig_params
-    }
-    df = await proc_instance.process(df, **filtered_input_kwargs)
+    df = await proc_instance.process(df, **input_kwargs)
```

This breaks any processor that has a closed signature (no `**kwargs` and no `tick`/`resources` named params) — but those processors are arguably already broken because they're rejecting kwargs that the framework promises to pass. Fix C is the cleanest design but the riskiest in terms of compatibility.

I'd recommend **Fix A + Fix B** as the safe, additive change. **Fix C** for a future cleanup pass.

## Suggested regression tests

Add to `tests/aio/test_async_world_execution.py`:

```python
@pytest.mark.asyncio
async def test_processor_with_var_keyword_receives_all_kwargs(tmp_path):
    """Regression: a processor declared as `process(self, df, **kwargs)`
    must actually receive tick, resources, etc. in its kwargs dict — the
    documented base-class signature must work."""
    received: dict = {}

    class CatchAllProc(AsyncProcessor):
        components = (Position,)
        priority = 5

        async def process(self, df, **kwargs):
            received.update(kwargs)
            return df

    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sys_ = AsyncSystem()
        await sys_.add_processor(CatchAllProc())
        world = await ws.create_world(
            WorldConfig(name="t"), storage_config=storage, system=sys_
        )
        await world.create_entity([Position(x=1, y=1)])
        await world.run(RunConfig(num_steps=1))

        assert "tick" in received, f"**kwargs processor missing tick: {sorted(received.keys())}"
        assert received["tick"] == 0
        assert "resources" in received, f"**kwargs processor missing resources: {sorted(received.keys())}"
    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_processor_receives_debug_flag_from_run_config(tmp_path):
    """Regression: RunConfig(debug=True) must propagate to processors via
    kwargs. Today system.execute consumes `debug` and never re-injects it."""
    received_debug: list = []

    class DebugProbeProc(AsyncProcessor):
        components = (Position,)
        priority = 5

        async def process(self, df, debug=None, **kwargs):
            received_debug.append(debug)
            return df

    ws = WorldService(StorageService())
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        sys_ = AsyncSystem()
        await sys_.add_processor(DebugProbeProc())
        world = await ws.create_world(
            WorldConfig(name="t"), storage_config=storage, system=sys_
        )
        await world.create_entity([Position(x=1, y=1)])
        await world.run(RunConfig(num_steps=1, debug=True))

        assert received_debug == [True], (
            f"debug=True from RunConfig was not propagated to the processor: {received_debug}"
        )
    finally:
        await ws.shutdown()
```

The first test fails on `main` at `assert "tick" in received` (received is empty). The second fails at `assert received_debug == [True]` (gets `[None]`).

## Notes / scope

- Affects `src/archetype/core/aio/async_system.py:93-97` (the kwargs filter) and `src/archetype/core/aio/async_system.py:73-74` (the missing debug re-injection). Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- Distinct from the eighteen other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - The four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution at the SimulationService layer.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota accounting.
  - `component-prefix-collision`, `component-get-type-by-name-no-recurse`, `cached-store-read-shadows-disk` are Component / cache.
  - `create-world-name-collision-orphan` and `world-id-none-divergence` are WorldService leaks.
  - `daily-tokens-never-reset` is the missing daily quota scheduler.
  - `storage-pool-key-ignores-cache-and-backend` is the multiton key.
  - This bug is at the **system → processor** boundary inside `core/aio`. None of the previous reports touched the processor dispatch path.
- This bug is the *third* "catch-all kwargs gets dropped" pattern in the codebase: the documented `AsyncProcessor.process(self, df, **input_kwargs)` signature is broken; `examples/world_mutations.py` and `examples/simulation_script.py` show users this pattern; and the trajectories processors at `processors.py:108,165` have a `kwargs.get("resources")` fallback that exists *because* the author noticed this gap and worked around it for the named-resources case. The fix should be made in core, not papered over in every processor.
- A small follow-up worth a separate hunt: the `debug` flag propagation also breaks `archetype.app.simulation_service.SimulationService.step` (which substitutes a fresh `RunConfig` per the filed bug) and `simulation_service.run` (same). After both are fixed, processors will reliably see `debug=True` from a user's `RunConfig`. Today, the only way to debug-log inside a processor is to read your own state, since the system never tells you whether debug is on.
