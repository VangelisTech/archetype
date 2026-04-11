# Bug Report: `CommandType.UPDATE` is a silent no-op for any component the entity already has

**Date:** 2026-04-11
**Severity:** High (silent data loss on a documented public API; the role system advertises UPDATE permission but the operation does nothing)
**Affects:** `archetype.app.command_service.CommandService.apply` — the `CommandType.UPDATE` dispatch arm
**Discovered by:** Overnight bug hunt

## Summary

`command_service.apply` routes `CommandType.UPDATE` and `CommandType.ADD_COMPONENT` through the same dispatch arm (`command_service.py:162`), forwarding both to `world.add_components(entity_id, components)`. `AsyncWorld.add_components` (`async_world.py:340-360`) hits an early `return` when the new signature equals the old signature — i.e. whenever the entity already has the component types being passed in. The user's "update Position to (99, 99)" command applies cleanly, returns success, and silently leaves the entity at its old values. There is no log line above DEBUG, no exception, no failure indicator anywhere along the path — `command_service.apply` returns normally and `drain_and_apply` adds the command's id to `applied_ids` and acks the broker. From the caller's perspective the update succeeded; from the world's perspective nothing happened.

The role system in `app/auth/guard.py` advertises `update` as a distinct permission for `coder`, `player`, `maintainer`, and `admin` roles, with its own quota slot (`update: 8`). The CommandType enum has it as a separate value (`models.py:42 — UPDATE = "update"`). The intent is clear: UPDATE means "modify the values of components on an existing entity". The implementation doesn't honour that intent.

## Impact

1. **REST/CLI users see updates that never happen.** A request body of `{"type": "update", "payload": {"entity_id": 1, "components": [{"type": "Position", "x": 99, "y": 99}]}}` returns a 200 with the command id, the broker acks the command, the audit history records it as applied — and the entity is unchanged. The user has no way to know it failed.
2. **Player-role users have no working write API.** Per `auth/guard.py:39`, the `player` role's permission set is `{"spawn", "despawn", "update", "message", "custom"}`. `player` does **not** have `add_component` or `remove_component`. The only way for a `player` to mutate an existing entity's component values is through `update` — which is broken. Result: the documented player API has zero working write paths to component state. Multi-agent simulations where agents drive their own state via the broker (the central use case in `AGENTS.md`) cannot land any per-tick component mutations through the `player` role.
3. **`UPDATE` and `ADD_COMPONENT` are not equivalent — but the dispatcher treats them as if they were.** Even ignoring UPDATE's broken semantics, the same dispatch arm means a user who calls `ADD_COMPONENT` on a type the entity already has *also* gets the silent no-op. This is at least defensible for `ADD_COMPONENT` ("you can't add what's already there"), but for `UPDATE` it is a footgun.
4. **Quotas and audit history lie.** Every silently-dropped UPDATE still consumes a quota slot in `guard.py:51` (`"update": 8`) and gets recorded in `_history` as if it had been applied. Quota exhaustion attacks become trivial: spam `UPDATE` commands that no-op, watch the player role hit its quota cap. Audit history shows commands that did nothing.
5. **The CommandType is documented but the semantics are not.** `models.py:39-44` lists `SPAWN`, `DESPAWN`, `UPDATE`, `ADD_COMPONENT`, `REMOVE_COMPONENT` as the entity-level commands. There is no docstring telling the user that UPDATE is structurally a no-op for the natural use case. Anyone reading the enum would reasonably write `Command(type=UPDATE, ...)` and never test that the values actually changed.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit e91329a, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: CommandType.UPDATE silently no-ops on existing components."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
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

            # Spawn an entity with initial Position values, materialise.
            eid = await world.create_entity([Position(x=1, y=2)])
            rc = RunConfig(num_steps=1)
            await world.run(rc)
            print("after spawn :", (await world.get_components([Position])).collect().to_pylist())

            # Submit an UPDATE that should change Position to (99, 99).
            update_cmd = Command(
                type=CommandType.UPDATE,
                payload={
                    "entity_id": eid,
                    "components": [{"type": "Position", "x": 99, "y": 99}],
                },
            )
            await container.command_service.apply(world, update_cmd)
            print(f"_spawn_cache after UPDATE   = {dict(world._spawn_cache)}")
            print(f"_despawn_cache after UPDATE = {dict(world._despawn_cache)}")

            await world.run(rc)
            row = (await world.get_components([Position])).collect().to_pylist()[0]
            print("after UPDATE:", row)

            assert row["position__x"] == 99 and row["position__y"] == 99, (
                f"BUG: UPDATE silently dropped — Position is "
                f"({row['position__x']}, {row['position__y']}), expected (99, 99)."
            )
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
after spawn : [{'world_id': '...', 'run_id': '...', 'entity_id': 1, 'tick': 0,
                'is_active': True, 'position__x': 1, 'position__y': 2}]
_spawn_cache after UPDATE   = {(<class '__main__.Position'>,): []}
_despawn_cache after UPDATE = {}
after UPDATE: {'world_id': '...', 'run_id': '...', 'entity_id': 1, 'tick': 1,
               'is_active': True, 'position__x': 1, 'position__y': 2}
AssertionError: BUG: UPDATE silently dropped — Position is (1, 2), expected (99, 99).
```

The empty `_spawn_cache` value (and empty `_despawn_cache`) confirms the early-return path: `add_components` returned without touching either cache. The next step then runs as a steady-state pass and re-publishes the unchanged row at `tick=1`.

### Baseline (proves the bug is scoped to "UPDATE on existing component types")

`ADD_COMPONENT` with a *new* component type works correctly through exactly the same dispatch:

```python
cmd = Command(
    type=CommandType.ADD_COMPONENT,
    payload={
        "entity_id": eid,
        "components": [{"type": "Velocity", "vx": 7, "vy": 8}],
    },
)
await container.command_service.apply(world, cmd)
await world.run(rc)

# after ADD_COMPONENT (Velocity): [{'entity_id': 1, 'tick': 1, 'is_active': True,
#                                    'position__x': 1, 'position__y': 2,
#                                    'velocity__vx': 7, 'velocity__vy': 8}]
# OK (baseline): ADD_COMPONENT with a new type works correctly.
```

The dispatch path is identical (line 162 routes both UPDATE and ADD_COMPONENT to `add_components`); the only difference is the type already exists on the entity. This isolates the bug to the "type already on entity" branch and confirms the dispatcher itself is functional — it's just plumbed to a method that can't actually update values.

## Root cause

`src/archetype/app/command_service.py:162-165`:

```python
case CommandType.UPDATE | CommandType.ADD_COMPONENT:
    entity_id = payload["entity_id"]
    components = self._hydrate_components(payload.get("components", []))
    await world.add_components(entity_id, components)
```

`src/archetype/core/aio/async_world.py:340-360`:

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

Trace for the MRE:

1. Caller submits `Command(type=UPDATE, payload={"entity_id": 1, "components": [{"type": "Position", "x": 99, "y": 99}]})`.
2. `CommandService.apply` matches `case CommandType.UPDATE | CommandType.ADD_COMPONENT:` (`command_service.py:162`).
3. `_hydrate_components` builds `[Position(x=99, y=99)]`.
4. `await world.add_components(1, [Position(x=99, y=99)])`:
   1. `old_sig = (Position,)` (from `_entity2sig[1]`).
   2. `new_sig = Archetype.add_components((Position,), [Position])` = `tuple(sorted(set((Position,)) | {Position}))` = `(Position,)` (set union with itself).
   3. `new_sig == old_sig` → `logger.debug(...)` and `return`.
5. `_spawn_cache` and `_despawn_cache` are not touched.
6. `command_service.apply` returns normally.
7. `drain_and_apply` records the command id in `applied_ids` (`command_service.py:87`) and acks the broker.
8. The next `world.step` runs the (Position,) archetype as a pure steady-state pass — query previous tick, no mutations, processors run, persist tick=1. The persisted row is exactly the same as tick=0.

The early return at `async_world.py:347-349` is the right behaviour for `ADD_COMPONENT` ("you tried to add a type that's already there — no-op"), but it is the wrong behaviour for `UPDATE` ("you tried to change values — that should hit the world's value-update path"). The dispatcher conflates the two.

The deeper issue is that **`AsyncWorld` has no public method that updates component values without changing the archetype signature**. Component values are normally updated by processors as DataFrame transforms during `world.step`. There is no `world.update_components(eid, components)` and no `world.set_component(eid, component)` API. So the dispatcher had no working method to forward `UPDATE` to in the first place.

## Why existing tests miss this

`tests/app/test_auth.py:54-57` is the only place `CommandType.UPDATE` appears in the test suite:

```python
def test_coder_can_update(self):
    ctx = ActorCtx(id=uuid7(), roles={"coder"})
    cmd = Command(type=CommandType.UPDATE, payload={})
    guardrail_allow(cmd, ctx)  # should not raise
```

This tests only the **RBAC permission** for the command type — that the `coder` role can submit an UPDATE without `guardrail_allow` raising `PermissionError`. It does not:

- Construct a real payload with `entity_id` and `components`.
- Submit through `command_service.submit` or `command_service.apply`.
- Spawn an entity beforehand.
- Step the world.
- Read back any state to check whether the update applied.

`grep -rn "CommandType\.UPDATE" tests/` returns zero matches outside that one auth test. There is no end-to-end test that submits an UPDATE and checks the resulting values. The same is true for `ADD_COMPONENT` — `grep -rn "ADD_COMPONENT" tests/` returns no matches at all.

Test files that exercise `command_service.apply` (`tests/integration/test_command_flow.py::test_submit_spawn_step_verify`, the broker tests in `tests/app/test_broker_extended.py`) only cover `SPAWN`. No test in the entire suite walks the path `submit(UPDATE) → drain_and_apply → world.add_components → next-tick value check`.

## Suggested fixes

There are two layers to fix. The dispatcher needs to send UPDATE somewhere meaningful, and the world (or the dispatcher) needs to have somewhere meaningful to send it to.

**Fix A — split the dispatch arm and write a dedicated update method.** The cleanest fix: give `AsyncWorld` an `update_components(entity_id, components)` method that overlays the new component values onto the entity's most-recent row without changing the signature, and route `CommandType.UPDATE` to it. This requires touching `core/`, so it would need explicit human approval (per `CLAUDE.md`).

```diff
 # src/archetype/app/command_service.py
-case CommandType.UPDATE | CommandType.ADD_COMPONENT:
+case CommandType.ADD_COMPONENT:
     entity_id = payload["entity_id"]
     components = self._hydrate_components(payload.get("components", []))
     await world.add_components(entity_id, components)

+case CommandType.UPDATE:
+    entity_id = payload["entity_id"]
+    components = self._hydrate_components(payload.get("components", []))
+    await world.update_components(entity_id, components)
```

```python
# src/archetype/core/aio/async_world.py — new method (would require core/ approval)
async def update_components(self, entity_id: int, components: list[Component]) -> None:
    """Overlay new component values onto an existing entity without changing
    its archetype signature. Raises if the entity does not have all of the
    component types being updated."""
    sig = self._entity2sig.get(entity_id)
    if sig is None:
        logger.warning("update_components: entity %s not found", entity_id)
        return

    missing = [type(c) for c in components if type(c) not in sig]
    if missing:
        raise ValueError(
            f"update_components: entity {entity_id} does not have component types "
            f"{[t.__name__ for t in missing]}; use add_components instead"
        )

    # Read the latest row from _live (or fall back to _spawn_cache like the
    # add_components-pending-spawn fix would). Overlay the new values, then
    # push back into _spawn_cache[sig] so the next tick's materialize_mutations
    # picks it up. _despawn_cache[sig] gets the entity_id so the old row is
    # tombstoned in the same tick.
    ...
```

**Fix B — fail loudly instead of silently dropping the UPDATE.** Until a real `update_components` method exists, the dispatcher should at least raise instead of silently no-op'ing. This is the minimal change that lands without touching `core/`:

```diff
 case CommandType.UPDATE | CommandType.ADD_COMPONENT:
     entity_id = payload["entity_id"]
     components = self._hydrate_components(payload.get("components", []))
+    if cmd.type is CommandType.UPDATE:
+        # AsyncWorld has no value-update primitive yet. Fail loudly so the
+        # caller knows their command did not apply, instead of silently
+        # no-op'ing whenever the entity already has the component types.
+        raise NotImplementedError(
+            "CommandType.UPDATE is not yet implemented; add_components only "
+            "moves entities between archetype signatures and cannot mutate "
+            "values in-place. Use a processor to update component values."
+        )
     await world.add_components(entity_id, components)
```

The `NotImplementedError` will surface in `drain_and_apply`'s `try/except` (line 88-89) as a logged exception, so the command won't be acked and the audit trail will show it as failed. This is far better than the current silent success.

**Fix C — remove `CommandType.UPDATE` from the enum and from the role permissions until it's implemented.** The strictest fix: don't expose a command that doesn't work. This is a breaking change for any caller using `CommandType.UPDATE`, but those callers are already broken — they just don't know it. Updating `models.py` and `auth/guard.py` would surface the breakage at command construction time instead of at runtime.

I'd recommend **Fix B as a stop-gap landed today**, plus **Fix A as the real implementation in a follow-up PR**.

## Suggested regression tests

Add to `tests/integration/test_command_flow.py`:

```python
@pytest.mark.asyncio
async def test_update_command_actually_updates_component_values(tmp_path):
    """Regression: Command(type=UPDATE) must change component values, not no-op."""
    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        eid = await world.create_entity([Position(x=1, y=2)])
        await world.run(RunConfig(num_steps=1))

        cmd = Command(
            type=CommandType.UPDATE,
            payload={
                "entity_id": eid,
                "components": [{"type": "Position", "x": 99, "y": 99}],
            },
        )
        await container.command_service.apply(world, cmd)
        await world.run(RunConfig(num_steps=1))

        row = (await world.get_components([Position])).collect().to_pylist()[0]
        assert row["position__x"] == 99
        assert row["position__y"] == 99
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_update_command_fails_loudly_when_entity_lacks_type(tmp_path):
    """Updating a component type the entity does not have should NOT silently
    move the entity to a new signature — that's add_components territory."""
    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        eid = await world.create_entity([Position(x=1, y=2)])
        await world.run(RunConfig(num_steps=1))

        cmd = Command(
            type=CommandType.UPDATE,
            payload={
                "entity_id": eid,
                "components": [{"type": "Velocity", "vx": 1, "vy": 2}],
            },
        )
        # Either raise (Fix A's update_components) or be a clear no-op
        # with a warning — but NOT silently move the entity to (Position, Velocity).
        with pytest.raises((ValueError, NotImplementedError)):
            await container.command_service.apply(world, cmd)
    finally:
        await container.shutdown()
```

The first test fails on `main` at the `assert row["position__x"] == 99` line. The second test fails on `main` because `apply` returns normally and the entity silently moves to `(Position, Velocity)` — encoding the difference in semantics between UPDATE and ADD_COMPONENT.

## Notes / scope

- Affects `src/archetype/app/command_service.py:162` (the UPDATE/ADD_COMPONENT shared dispatch arm). This is in `app/`, not `core/`, so Fix B (the loud-failure stop-gap) and Fix C (enum cleanup) can land directly. Fix A (a real `update_components` method) requires touching `src/archetype/core/aio/async_world.py` and would need explicit human approval.
- Distinct from the five other already-filed bugs:
  - The four `*-spawn-despawn-*` and `add-components-pending-spawn` reports are about the world's mutation cache mechanics in `core/`.
  - `simulation-service-run-discards-runconfig` is about `SimulationService.run` substituting a fresh `RunConfig`.
  - This bug is about `command_service.apply` routing two semantically distinct command types through the same code path, where one of them silently does nothing.
- The same "no value-update primitive" gap means `AsyncWorld` callers who want to mutate a component's values today have exactly two options: write a processor, or call `add_components` with a different component type. There is no `world.set_component(...)` or `world.patch_entity(...)` API. This gap is what makes UPDATE a footgun: the dispatcher tried to expose it but had nothing to dispatch to.
- Quota-exhaustion attack vector: an attacker with the `player` role can spam `UPDATE` commands that no-op, exhaust the player's `update: 8` quota in `auth/guard.py:51`, and lock out legitimate UPDATE traffic — none of which would have done anything anyway. Worth noting in the security review (`docs/reports/2026-03-28-security-program-review.md`).
- Sync world (`SyncWorld`) goes through the same `command_service.apply` dispatcher and therefore has the same bug; the routing is shared.
