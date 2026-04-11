# Bug Report: `SimulationService.run` discards the user's `RunConfig` per tick — `run_id`, `prefer_live_reads`, `debug`, etc. all silently dropped

**Date:** 2026-04-11
**Severity:** High (silent correctness + observability + persistence corruption on the public service-layer API)
**Affects:** `archetype.app.simulation_service.SimulationService.run` — the entry point used by the REST API, the CLI, and any caller that drives multi-step simulations through `ServiceContainer`
**Discovered by:** Overnight bug hunt

## Summary

`SimulationService.run` advances the world by calling `self.step(world_id, RunConfig(num_steps=1), ...)` once per iteration of its for loop. The `RunConfig` it passes is freshly constructed *inside the loop* — it ignores the `RunConfig` the caller handed in. Because `RunConfig.run_id` defaults to `uuid.uuid7()` and the other fields default to `False` / `None`, every per-tick `RunConfig` gets a brand-new `run_id` and resets `prefer_live_reads`, `debug`, `enable_validation`, `suite`, `trial`, `metadata`, `show_rows`, and `explain` to their defaults. The user's `RunConfig` is used only to read `num_steps` and to populate `RunResult.run_id`. Nothing else from it ever reaches the world.

The visible failure modes:

1. **The rows produced by the run are unreachable.** Each tick stamps its rows with a different fresh uuid. `RunResult.run_id` is the user's (which has *never been used to stamp anything*), so a caller that does `query_service.get_state(world_id, run_id=result.run_id)` finds zero rows.
2. **`world.run_id` is never set.** `AsyncWorld.run` sets `self.run_id = str(run_config.run_id)` at the start of its loop; `SimulationService.run` does not, so the world's "current run" pointer remains `None` after a successful service-layer run, breaking default-run queries.
3. **`prefer_live_reads=True` is silently ignored.** Callers that opt in to live reads (especially `tests/integration/test_trajectory_pipeline.py`, which threads `prefer_live_reads=True` through every `simulation_service.run` call) actually run with `prefer_live_reads=False` and pay the full querier round-trip every tick.
4. **`debug=True` is silently ignored.** Setting it on the user's `RunConfig` produces no debug logs because the inner `RunConfig(num_steps=1)` resets `debug` to `False`.

## Impact

1. **REST/CLI users cannot find the data they just produced.** The public flow is `POST /worlds/{id}/run` → `SimulationService.run` → `RunResult{run_id: <user>}`. The user takes that `run_id`, queries by it, and gets zero rows. The data is in the store under per-tick uuids the user has no way to discover. This is a *silent* loss-of-handle on every multi-step run.
2. **Time-travel queries are partitioned across uuids.** A `num_steps=10` run produces ten distinct `run_id`s in the store, none of them the one returned to the caller. `QueryService.get_world_state(world_id, run_id=...)` cannot reassemble a coherent run history because no single `run_id` covers the whole sequence.
3. **`prefer_live_reads` performance optimisation is disabled wherever it matters most.** The fork-and-step pipeline (`test_trajectory_pipeline.py:182, 224, 264, 297, 337, 348, 353, 390`) explicitly opts into `prefer_live_reads=True` because reading from the store on every tick of an MCTS rollout is expensive. Every one of those calls runs with the default `prefer_live_reads=False` because the option is dropped before reaching `world.step`.
4. **Debug logging on the service layer is broken.** A user setting `RunConfig(debug=True)` to inspect a flaky run sees no debug output because the inner `RunConfig` resets it. Same for `enable_validation`, `explain`, `show_rows`.
5. **`suite`, `trial`, `metadata` experiment-tracking fields never reach the world.** Anyone wiring `RunConfig(suite="benchmark", trial=42, metadata={...})` through `simulation_service.run` for ensemble experiments gets the metadata stripped before the world ever sees it.
6. **The bug only fires on `SimulationService.run`.** `AsyncWorld.run` (one layer down) handles `RunConfig` correctly. Callers that bypass the service layer and call `world.run(rc)` directly are unaffected — but the entire REST/CLI/broker flow goes through `SimulationService.run`.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 068b6ac, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: SimulationService.run discards the user's RunConfig per tick."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.archetype import Archetype
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
                WorldConfig(name="mre"),
                StorageConfig(uri=tmp),
            )
            world = container.world_service.get_world(info.world_id)
            await world.create_entity([Position(x=1, y=2)])

            user_rc = RunConfig(num_steps=3, prefer_live_reads=True, debug=True)
            print(f"user run_id       = {user_rc.run_id}")
            print(f"prefer_live_reads = {user_rc.prefer_live_reads}")
            print(f"debug             = {user_rc.debug}")

            result = await container.simulation_service.run(info.world_id, user_rc)
            print(f"\nRunResult.run_id  = {result.run_id}")
            print(f"world.run_id      = {world.run_id}")

            sig = Archetype.sig_from_components([Position(x=0, y=0)])
            df = await world.querier.get_archetype(
                sig, str(world.world_id), str(user_rc.run_id)
            )
            print(f"rows in store under USER run_id: {len(df.collect().to_pylist())}")
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
user run_id       = 019d7e4b-0fa4-7b20-b287-12548016e754
prefer_live_reads = True
debug             = True

RunResult.run_id  = 019d7e4b-0fa4-7b20-b287-12548016e754
world.run_id      = None
rows in store under USER run_id: 0
```

`RunResult.run_id` is the user's UUID, but `world.run_id` is `None` and zero rows are visible under the user's run_id — the data is partitioned across three different fresh uuids the caller has no handle on.

### Baseline (proves the bug is scoped to `SimulationService.run`, not the world)

The same flow run via `world.run(...)` directly (one layer below) works correctly:

```python
user_rc = RunConfig(num_steps=3, prefer_live_reads=True, debug=True)
await world.run(user_rc)  # bypass SimulationService

# Output:
# user run_id = 019d7e4b-45c2-7ba2-985e-5ff684125382
# world.run_id = 019d7e4b-45c2-7ba2-985e-5ff684125382
# rows under user run_id: 3
# OK (baseline): AsyncWorld.run preserves the user's run_id correctly.
```

`AsyncWorld.run` sets `self.run_id = str(run_config.run_id)` (`async_world.py:120`) and reuses the same `run_config` for every step in its loop. The bug is entirely inside `SimulationService.run`.

## Root cause

`src/archetype/app/simulation_service.py:65-89`:

```python
async def run(
    self,
    world_id: UUID,
    run_config: RunConfig,
    **input_kwargs,
) -> RunResult:
    """
    Execute run_config.num_steps ticks.
    Returns RunResult with run_id, ticks completed, final state.
    """
    total_commands = 0

    for _ in range(run_config.num_steps):
        cmds = await self.step(world_id, RunConfig(num_steps=1), **input_kwargs)
        total_commands += cmds

    world = self._world_service.get_world(world_id)

    return RunResult(
        run_id=run_config.run_id,
        world_id=world_id,
        ticks_completed=run_config.num_steps,
        commands_applied=total_commands,
        final_tick=getattr(world, "tick", 0),
    )
```

The offending line is **`simulation_service.py:78`** — `RunConfig(num_steps=1)` is built fresh inside the loop, ignoring the user's `run_config`. The user's object is touched only at:

- Line 77 (`run_config.num_steps`) for the loop bound,
- Line 84 (`run_id=run_config.run_id`) for the response object.

Trace for the MRE:

1. User calls `simulation_service.run(world_id, RunConfig(num_steps=3, prefer_live_reads=True, debug=True, run_id=USER))`.
2. Loop iteration 0: `await self.step(world_id, RunConfig(num_steps=1))` builds a fresh `RunConfig` (`run_id=uuid7_A`, all flags default).
3. `step` (`simulation_service.py:37-63`) forwards that to `world.step(RunConfig(num_steps=1), ...)`.
4. `world.step` calls `_run_archetype(sig, run_config)` for each active sig (`async_world.py:160`).
5. `_run_archetype` calls `await self.update(df, sig, run_config)` (`async_world.py:218`).
6. `update` (`async_world.py:476-485`) calls `self.updater.update(df, sig, ..., run_config.run_id)` — stamping the row with `uuid7_A`.
7. Loop iteration 1: a *new* `RunConfig(num_steps=1)` with `run_id=uuid7_B`. Rows for tick 1 are stamped with `uuid7_B`.
8. Loop iteration 2: `uuid7_C`. Rows for tick 2 are stamped with `uuid7_C`.
9. After the loop: `world.run_id` is still `None` (nothing in this code path ever sets it). `RunResult.run_id = USER`.
10. Caller queries by `USER` → 0 rows. Caller queries by default `world.run_id` → `None` → query fails or returns nothing depending on the querier path.

`AsyncWorld.run` (`async_world.py:114-122`) is the right shape and the contrast makes the bug obvious:

```python
async def run(self, run_config: RunConfig, **input_kwargs) -> None:
    """
    Runs the world for the given run configuration.
    """
    # Track the current run identifier for default queries
    self.run_id = str(run_config.run_id)
    for _ in range(run_config.num_steps):
        await self.step(run_config, **input_kwargs)
```

It sets `self.run_id` from the user's config and reuses the *same* `run_config` for every step. `SimulationService.run` does neither.

## Why existing tests miss this

`tests/app/test_services.py::TestSimulationService.test_run` (line 97-108) is the only test that exercises `SimulationService.run`:

```python
@pytest.mark.asyncio
async def test_run(self, tmp_path):
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="test"), storage)

        result = await container.simulation_service.run(world.world_id, RunConfig(num_steps=3))
        assert result.ticks_completed == 3
        assert result.world_id == world.world_id
    finally:
        await container.shutdown()
```

It checks only `ticks_completed` and `world_id`. It does **not**:

- Spawn any entities, so there are no rows to stamp and no `run_id`s to mismatch.
- Assert anything about `result.run_id`.
- Assert anything about `world.run_id` after the call.
- Pass `prefer_live_reads`, `debug`, or any other `RunConfig` field, so the bug's "all flags reset to defaults" symptom never gets exercised.

`tests/core/test_async_prefer_live_reads.py::test_prefer_live_reads_uses_live_snapshot_when_true` (line 25-47) does test that `prefer_live_reads=True` works — but it calls `await world.run(RunConfig(num_steps=1, prefer_live_reads=True))` directly on the `AsyncWorld`, **not** through `simulation_service.run`. So it passes by going around the bug.

`tests/integration/test_trajectory_pipeline.py` makes ~9 calls of the form `await container.simulation_service.run(world.world_id, RunConfig(num_steps=1, prefer_live_reads=True))`. Every one of these silently runs with `prefer_live_reads=False`. They still pass because the trajectory pipeline tolerates store-backed reads (just slower) — the test asserts data correctness, not the optimisation path.

`tests/app/test_registry.py:101` calls `await c1.simulation_service.run(world.world_id, RunConfig(num_steps=3))` to populate state for a registry test, and similarly does not check the run_id.

There is no test that:

1. Submits a `RunConfig(prefer_live_reads=True)` through the service layer and asserts the world actually used live reads.
2. Asserts `world.run_id == str(user_rc.run_id)` after a service-layer run.
3. Asserts the rows produced by `simulation_service.run` are queryable under `result.run_id`.

## Suggested fixes

**Fix A — pass the user's `run_config` through, and thread it down to `world.step`** (the minimal correct fix):

```diff
 async def run(
     self,
     world_id: UUID,
     run_config: RunConfig,
     **input_kwargs,
 ) -> RunResult:
     """
     Execute run_config.num_steps ticks.
     Returns RunResult with run_id, ticks completed, final state.
     """
     total_commands = 0

+    # Set the world's run_id once for the whole run, mirroring AsyncWorld.run.
+    world = self._world_service.get_world(world_id)
+    if hasattr(world, "run_id"):
+        world.run_id = str(run_config.run_id)
+
     for _ in range(run_config.num_steps):
-        cmds = await self.step(world_id, RunConfig(num_steps=1), **input_kwargs)
+        cmds = await self.step(world_id, run_config, **input_kwargs)
         total_commands += cmds

-    world = self._world_service.get_world(world_id)
-
     return RunResult(
         run_id=run_config.run_id,
         world_id=world_id,
         ticks_completed=run_config.num_steps,
         commands_applied=total_commands,
         final_tick=getattr(world, "tick", 0),
     )
```

This passes the *same* `run_config` object (with the user's `run_id`, `prefer_live_reads`, `debug`, etc.) into every per-tick `step()`. The world's `step` already accepts this — it reads `run_config.prefer_live_reads`, `run_config.debug`, and `run_config.run_id` directly from the object. The world's `run_id` is set up-front so default-run queries work immediately.

There is one subtlety: `step` will receive a `RunConfig` whose `num_steps` is the user's full count, not `1`. That field is unused inside `world.step` — only `RunConfig.num_steps` in `AsyncWorld.run` and `SimulationService.run` reads it. So passing the user's config straight through is safe.

**Fix B — delegate the loop to `world.run` instead of duplicating it**:

```diff
 async def run(
     self,
     world_id: UUID,
     run_config: RunConfig,
     **input_kwargs,
 ) -> RunResult:
-    total_commands = 0
-
-    for _ in range(run_config.num_steps):
-        cmds = await self.step(world_id, RunConfig(num_steps=1), **input_kwargs)
-        total_commands += cmds
-
-    world = self._world_service.get_world(world_id)
+    world = self._world_service.get_world(world_id)
+    total_commands = 0
+
+    # Drain commands at each tick boundary to preserve broker semantics,
+    # but use the world's own run() to keep the user's RunConfig intact.
+    for _ in range(run_config.num_steps):
+        applied = await self._command_service.drain_and_apply(
+            world_id, getattr(world, "tick", 0)
+        )
+        reset_tick_counters()
+        total_commands += len(applied)
+        if isinstance(world, AsyncWorld):
+            await world.step(run_config, **input_kwargs)
+
+    if hasattr(world, "run_id") and world.run_id is None:
+        world.run_id = str(run_config.run_id)

     return RunResult(...)
```

Fix A is smaller and gets the full benefit. Fix B is essentially Fix A inlined, and is only useful if the team wants to make `SimulationService.run` look syntactically more like `AsyncWorld.run`. Either way, the key change is "**don't substitute a fresh RunConfig**".

A separate cleanup: `RunConfig` is documented as immutable (`model_config = dict(frozen=True, arbitrary_types_allowed=True)`, `core/config.py:150`). It is safe to pass the same instance through every iteration.

## Suggested regression tests

Add to `tests/app/test_services.py::TestSimulationService`:

```python
@pytest.mark.asyncio
async def test_run_preserves_user_run_id_in_storage(self, tmp_path):
    """Rows produced by simulation_service.run must be queryable
    under the user's RunConfig.run_id (not a fresh per-tick uuid)."""
    from archetype.core.archetype import Archetype
    from archetype.core.component import Component

    class P(Component):
        x: int = 0

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="t"), storage)
        await world.create_entity([P(x=1)])

        user_rc = RunConfig(num_steps=3)
        result = await container.simulation_service.run(world.world_id, user_rc)

        # The world's run_id pointer must be set to the user's run_id.
        assert str(world.run_id) == str(user_rc.run_id)

        # Rows must be queryable under result.run_id (== user_rc.run_id).
        sig = Archetype.sig_from_components([P()])
        df = await world.querier.get_archetype(
            sig, str(world.world_id), str(result.run_id)
        )
        rows = df.collect().to_pylist()
        assert len(rows) >= 1, "rows produced by run() must be queryable under result.run_id"
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_run_honors_prefer_live_reads(self, tmp_path):
    """RunConfig.prefer_live_reads=True passed through simulation_service.run
    must reach world.step (so the live snapshot path is exercised)."""
    from unittest.mock import patch
    from archetype.core.aio.async_world import AsyncWorld

    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await container.world_service.create_world(WorldConfig(name="t"), storage)
        seen_flags: list[bool] = []
        original = AsyncWorld.step

        async def capturing_step(self, run_config, **kwargs):
            seen_flags.append(run_config.prefer_live_reads)
            await original(self, run_config, **kwargs)

        with patch.object(AsyncWorld, "step", capturing_step):
            await container.simulation_service.run(
                world.world_id,
                RunConfig(num_steps=3, prefer_live_reads=True),
            )

        assert seen_flags == [True, True, True], (
            f"prefer_live_reads dropped by SimulationService.run; saw {seen_flags}"
        )
    finally:
        await container.shutdown()
```

The first test fails on `main` at the `assert str(world.run_id) == str(user_rc.run_id)` line (`world.run_id` is `None`) and at the row-count assert (zero rows under the user's run_id). The second test fails because `seen_flags == [False, False, False]` on `main`.

## Notes / scope

- Affects `src/archetype/app/simulation_service.py:65-89` (and indirectly `:37-63`, where `step` accepts the substituted `RunConfig`). This is **`app/`**, not `core/` — so the fix can be made directly without the `core/`-is-read-only carve-out. No code changes are made by this report; the fix is left for the human reviewer.
- `SimulationService.run_all` (`simulation_service.py:91-101`) calls `self.run(...)` per world via `asyncio.gather`. It inherits the bug 1:1 — every world in the gather sees its own `RunConfig` discarded.
- The five other already-filed bugs (`spawn-despawn-same-tick`, `sync-spawn-despawn-same-tick`, `sync-active-signatures-drops-despawn`, `add-components-pending-spawn`) are all in `core/` and concern the world's mutation caches. This bug is in the service layer and concerns the boundary between `SimulationService.run` and `world.step`. They are independent and should be addressed independently.
- After the fix, the existing `test_async_prefer_live_reads.py` tests should be either re-pointed at `simulation_service.run` (so they exercise the public flow) or duplicated under `tests/app/test_services.py`. Today they only cover the world-direct path.
- One follow-up worth a separate hunt: `world.run_id` is also unset by `SimulationService.step` for the single-step case (`simulation_service.py:37-63`). Whether that matters depends on whether single-step callers expect default-run queries to work. Out of scope for this report.
