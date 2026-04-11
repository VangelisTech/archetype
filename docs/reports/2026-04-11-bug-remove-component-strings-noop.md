# Bug Report: `CommandType.REMOVE_COMPONENT` is a silent no-op when `component_types` arrives as strings (the only thing JSON can carry)

**Date:** 2026-04-11
**Severity:** High (silent data loss on a documented public API; the only way to remove components from the wire — REST/CLI — is broken)
**Affects:** `archetype.app.command_service.CommandService.apply` — the `CommandType.REMOVE_COMPONENT` dispatch arm
**Discovered by:** Overnight bug hunt

## Summary

`command_service.apply` forwards `payload["component_types"]` straight to `world.remove_components` without resolving string type names to `Component` subclasses (`command_service.py:167-170`). `Archetype.remove_components` then computes `tuple(sorted(set(sig) - set(component_types)))` (`archetype.py:71`), comparing element-by-element. A string `"Velocity"` never equals the class object `Velocity`, so the set difference yields the original set unchanged, `new_sig == old_sig`, and `AsyncWorld.remove_components` early-returns. The command is acked, the audit history records it as applied, the quota slot is consumed — and the component is still attached to the entity.

This is the same shape as the just-filed `update-command-silently-noops` bug, but on a different command type and with a different underlying defect. UPDATE has the wrong target method; REMOVE_COMPONENT has the right target method but the dispatcher fails to convert wire-format strings into Python types before calling it. Both produce indistinguishable user-visible behaviour: "I sent a command, the server said OK, nothing happened."

## Impact

1. **REST/CLI users cannot remove components.** A REST request body of `{"type": "remove_component", "payload": {"entity_id": 1, "component_types": ["Velocity"]}}` returns success and the server's audit log records the command as applied. The entity still has Velocity. There is no error response, no log line above DEBUG, no failure indicator anywhere along the path.
2. **The MCP / FastAPI surface is structurally incapable of carrying anything but strings.** JSON has no concept of Python class objects. Every wire-protocol caller (`api/`, `cli/`, MCP servers, the tests in `tests/integration/test_command_flow.py`) is forced to use strings, so every wire-protocol caller hits the bug. The only callers that work are direct in-process Python that hand-build a `Command` with class objects in the payload — which defeats the purpose of having a command broker in the first place.
3. **`coder` and `maintainer` roles are stripped of their documented write API.** Per `auth/guard.py:19,28-37`, `coder = {"add_component", "remove_component", "update"}` and `maintainer` includes `remove_component`. Combined with the previously-filed `update-command-silently-noops` (which kills `update`), the `coder` role's *entire* write API to existing entities is now broken: `add_component` works (but only for new types), `remove_component` no-ops, `update` no-ops. A `coder` that removes a component, gets a 200 back, and re-reads the entity will see it unchanged, with no signal that anything went wrong.
4. **Quotas and audit history lie.** Every silently-dropped REMOVE_COMPONENT consumes the role's `remove_component: 4` quota slot (`auth/guard.py:54`) and gets recorded in `_history` as if applied. Quota-exhaustion attacks are trivial: spam REMOVE_COMPONENT, watch the quota cap fall on no-op commands.
5. **Wide blast radius for any cleanup workflow.** Anything that removes optional components after a state transition (e.g., removing an `Inbox` component after a one-shot reply, dropping a `Stunned` debuff component once timer expires, retiring `Loading` spinners once data arrives) is broken when driven through commands. The only working path is to write a processor — defeating the broker's purpose for these one-shot mutations.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit df75e82, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: REMOVE_COMPONENT silently no-ops when component_types is a list of strings."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int = 0
    y: int = 0


class Velocity(Component):
    vx: int = 0
    vy: int = 0


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"),
                StorageConfig(uri=tmp),
            )
            world = container.world_service.get_world(info.world_id)

            eid = await world.create_entity([Position(x=1, y=2), Velocity(vx=3, vy=4)])
            rc = RunConfig(num_steps=1)
            await world.run(rc)
            print(f"sig before = {Archetype.get_name(world._entity2sig[eid])}")

            cmd = Command(
                type=CommandType.REMOVE_COMPONENT,
                payload={"entity_id": eid, "component_types": ["Velocity"]},  # JSON-style
            )
            await container.command_service.apply(world, cmd)
            print(f"_spawn_cache after  = {dict(world._spawn_cache)}")
            print(f"_despawn_cache after = {dict(world._despawn_cache)}")

            await world.run(rc)
            print(f"sig after  = {Archetype.get_name(world._entity2sig[eid])}")

            df_pv = await world.get_components([Position, Velocity])
            print(f"rows in (Position, Velocity) after = {df_pv.collect().to_pylist()}")
            assert Velocity not in set(world._entity2sig[eid]), "BUG: Velocity not removed"
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
sig before = a_2c_s61a85f4fb3436059
_spawn_cache after  = {(<class '__main__.Position'>, <class '__main__.Velocity'>): []}
_despawn_cache after = {}
sig after  = a_2c_s61a85f4fb3436059
rows in (Position, Velocity) after = [{'world_id': '...', 'run_id': '...', 'entity_id': 1,
                                       'tick': 1, 'is_active': True,
                                       'position__x': 1, 'position__y': 2,
                                       'velocity__vx': 3, 'velocity__vy': 4}]
AssertionError: BUG: Velocity not removed
```

The signature is identical before and after (`a_2c_...` = 2-component archetype, same hash). The `(Position, Velocity)` query still returns the row. `_despawn_cache` is empty — `remove_components` returned via the early-return path without touching it.

### Baseline (proves the bug is scoped to "strings vs class objects")

The same command, with `component_types` set to actual class objects, works correctly:

```python
cmd = Command(
    type=CommandType.REMOVE_COMPONENT,
    payload={"entity_id": eid, "component_types": [Velocity]},  # CLASS, not string
)
await container.command_service.apply(world, cmd)
await world.run(rc)

# sig after = ('Position',)
# rows in (Position, Velocity) = []
# rows in (Position,)          = [{'entity_id': 1, 'tick': 1, 'is_active': True,
#                                  'position__x': 1, 'position__y': 2}]
# OK (baseline): REMOVE_COMPONENT works when given actual class objects.
```

The dispatcher and `world.remove_components` work fine when handed real Python types. The bug is the missing string→type conversion in the dispatcher. This is *only* reachable from in-process callers that import the component classes; every wire-protocol caller hits the broken path.

## Root cause

`src/archetype/app/command_service.py:167-170`:

```python
case CommandType.REMOVE_COMPONENT:
    entity_id = payload["entity_id"]
    component_types = payload.get("component_types", [])
    await world.remove_components(entity_id, component_types)
```

Compare with `src/archetype/app/command_service.py:154-165` for SPAWN/UPDATE/ADD_COMPONENT:

```python
case CommandType.SPAWN:
    components = self._hydrate_components(payload.get("components", []))
    await world.create_entity(components)

...

case CommandType.UPDATE | CommandType.ADD_COMPONENT:
    entity_id = payload["entity_id"]
    components = self._hydrate_components(payload.get("components", []))
    await world.add_components(entity_id, components)
```

`_hydrate_components` (`command_service.py:96-107`) converts dicts in the components list back to `Component` instances by reading the `"type"` field and calling `Component.get_type_by_name(name)`. There is **no equivalent helper** for resolving a list of type-name strings into `list[type[Component]]`. The REMOVE_COMPONENT arm just forwards `payload.get("component_types", [])` raw.

`src/archetype/core/aio/async_world.py:362-379`:

```python
async def remove_components(
    self, entity_id: int, component_types: list[type[Component]]
) -> None:
    old_sig = self._entity2sig.get(entity_id)
    if old_sig is None:
        return

    new_sig = Archetype.remove_components(old_sig, component_types)
    if new_sig == old_sig:
        return
    ...
```

`src/archetype/core/archetype.py:63-71`:

```python
@staticmethod
def remove_components(
    sig: ArchetypeSignature, component_types: list[type[Component]]
) -> ArchetypeSignature:
    """
    Generate a new archetype signature by removing components from an existing signature.
    """
    return tuple(sorted(list(set(sig) - set(component_types)), key=lambda t: t.__name__))
```

Trace for the MRE:

1. Caller submits `Command(type=REMOVE_COMPONENT, payload={"entity_id": 1, "component_types": ["Velocity"]})`.
2. `CommandService.apply` matches `case CommandType.REMOVE_COMPONENT:` (`command_service.py:167`).
3. `component_types = ["Velocity"]` — a list of strings.
4. `await world.remove_components(1, ["Velocity"])`:
   1. `old_sig = (Position, Velocity)` (the tuple of *class* objects).
   2. `new_sig = Archetype.remove_components((Position, Velocity), ["Velocity"])`:
      - `set((Position, Velocity)) - set(["Velocity"])`
      - `{Position, Velocity} - {"Velocity"}`
      - = `{Position, Velocity}` (string `"Velocity"` is not equal to class `Velocity`)
      - `tuple(sorted(...))` = `(Position, Velocity)` (same as `old_sig`)
   3. `new_sig == old_sig` → `return`.
5. `_spawn_cache` and `_despawn_cache` are not touched. The annotated type hint `list[type[Component]]` is ignored at runtime.
6. `apply` returns normally; `drain_and_apply` records the command id in `applied_ids` and acks the broker.
7. The next `world.step` runs the (Position, Velocity) archetype as a steady-state pass and re-publishes the unchanged row at `tick=1`.

There is no runtime type check anywhere along the path. Python's type hints are advisory; `set()` happily accepts mixed strings and classes; and the set difference operation silently produces "remove nothing" instead of raising. The bug is silent at every layer.

## Why existing tests miss this

`grep -rn "CommandType\.REMOVE_COMPONENT\|\"remove_component\"" tests/` returns **zero matches**. There is no test in the suite that submits a `REMOVE_COMPONENT` command, neither through `command_service.apply` nor through the broker. The closest existing tests are direct calls to `world.remove_components(...)`:

- `tests/aio/test_async_world_mutations.py:124::test_remove_components_moves_to_subset_signature` — calls `await world.remove_components(e1, [Meta])` with the class object directly.
- `tests/aio/test_async_world_edges.py:78::test_remove_components_nonexistent_entity_is_noop` — `await world.remove_components(777777, [Position])`, class object directly.
- `tests/aio/test_async_world_edges.py:87::test_remove_components_no_change_is_noop` — `await world.remove_components(ent, [])`, empty list (no type resolution to test).
- `tests/core/test_archetype_core_signatures.py:28::test_add_and_remove_components_are_set_like_and_sorted` — `Archetype.remove_components(added, [A])`, class object directly at the lowest layer.

Every test bypasses `command_service.apply` and calls `world.remove_components` (or `Archetype.remove_components`) directly with class objects. The dispatcher's missing string→type resolution path is not exercised by any test.

`tests/integration/test_command_flow.py::test_submit_spawn_step_verify` (the only end-to-end command-flow test) submits only `SPAWN`. There is no equivalent for `REMOVE_COMPONENT`. There is also no test that submits a `Command` whose payload was constructed by serializing through JSON and then deserializing — i.e. the only situation where the wire-format constraint forces strings into the payload.

## Suggested fixes

**Fix A — write a `_resolve_component_types` helper and use it in the REMOVE_COMPONENT arm.** Mirrors the existing `_hydrate_components` pattern. Lands entirely in `app/`, no `core/` changes:

```diff
 # src/archetype/app/command_service.py

 @staticmethod
 def _hydrate_components(raw: list) -> list:
     """Convert dicts in a component list back to Component instances."""
     from archetype.core.component import Component

     result = []
     for item in raw:
         if isinstance(item, dict):
             result.append(Component.from_dict(dict(item)))  # copy to avoid mutating payload
         else:
             result.append(item)
     return result

+@staticmethod
+def _resolve_component_types(raw: list) -> list:
+    """Convert type-name strings in a component_types list back to Component subclasses.
+
+    The wire format (JSON payload from REST/MCP/CLI) can only carry strings.
+    Direct in-process callers can also pass class objects directly; both are handled.
+    """
+    from archetype.core.component import Component
+
+    result = []
+    for item in raw:
+        if isinstance(item, str):
+            result.append(Component.get_type_by_name(item))
+        else:
+            result.append(item)
+    return result
+
 ...

 case CommandType.REMOVE_COMPONENT:
     entity_id = payload["entity_id"]
-    component_types = payload.get("component_types", [])
+    component_types = self._resolve_component_types(payload.get("component_types", []))
     await world.remove_components(entity_id, component_types)
```

**Fix B (defence in depth) — make `Archetype.remove_components` raise on unresolved types instead of silently no-op'ing.** This requires touching `core/`, so it would need explicit human approval. But it would catch *any* future caller that forgets to resolve strings:

```diff
 # src/archetype/core/archetype.py

 @staticmethod
 def remove_components(
     sig: ArchetypeSignature, component_types: list[type[Component]]
 ) -> ArchetypeSignature:
     """
     Generate a new archetype signature by removing components from an existing signature.
     """
+    bad = [t for t in component_types if not isinstance(t, type)]
+    if bad:
+        raise TypeError(
+            f"remove_components requires a list of Component subclasses, "
+            f"got non-type values: {bad!r}. "
+            f"Did the caller forget to resolve string type names?"
+        )
     return tuple(sorted(list(set(sig) - set(component_types)), key=lambda t: t.__name__))
```

The same defence should be added to `Archetype.add_components` since it has the identical shape and the identical risk if a future dispatcher forgets to resolve strings.

I'd recommend **Fix A landed today** (it's a single new helper plus a one-line dispatcher change in `app/`), and **Fix B as a follow-up** that requires `core/` approval but pays off across every future code path that touches type signatures.

## Suggested regression tests

Add to `tests/integration/test_command_flow.py`:

```python
@pytest.mark.asyncio
async def test_remove_component_command_with_string_type_names_works(tmp_path):
    """Regression: REMOVE_COMPONENT with JSON-format string type names must
    actually remove the components, not silently no-op."""
    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        eid = await world.create_entity([Position(x=1, y=2), Velocity(vx=3, vy=4)])
        await world.run(RunConfig(num_steps=1))

        # Wire-format: strings only.
        cmd = Command(
            type=CommandType.REMOVE_COMPONENT,
            payload={"entity_id": eid, "component_types": ["Velocity"]},
        )
        await container.command_service.apply(world, cmd)
        await world.run(RunConfig(num_steps=1))

        # The entity must no longer be in the (Position, Velocity) archetype.
        assert Velocity not in set(world._entity2sig[eid])
        assert Position in set(world._entity2sig[eid])

        # And the (Position, Velocity) query must return no rows.
        df = await world.get_components([Position, Velocity])
        assert df.collect().count_rows() == 0


@pytest.mark.asyncio
async def test_remove_component_command_round_trips_through_json(tmp_path):
    """The dispatcher must handle a payload that was actually JSON-encoded and
    decoded — the natural REST/MCP shape. This is the strictest version of
    the regression test and proves there's no hidden type smuggling."""
    import json

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)
        eid = await world.create_entity([Position(x=1, y=2), Velocity(vx=3, vy=4)])
        await world.run(RunConfig(num_steps=1))

        wire_payload = json.loads(json.dumps({
            "entity_id": eid,
            "component_types": ["Velocity"],
        }))
        cmd = Command(type=CommandType.REMOVE_COMPONENT, payload=wire_payload)
        await container.command_service.apply(world, cmd)
        await world.run(RunConfig(num_steps=1))

        assert Velocity not in set(world._entity2sig[eid])
```

Both tests fail on `main` at the `Velocity not in set(world._entity2sig[eid])` line because the silent no-op leaves the signature unchanged.

A symmetric pair should be added to `tests/sync/` as soon as the sync world is wired through the service layer (it isn't today — see notes below).

## Notes / scope

- Affects `src/archetype/app/command_service.py:167-170`. This is **`app/`**, not `core/`, so Fix A can land directly without the `core/`-is-read-only carve-out. Fix B (the defensive type check) does require `core/` approval.
- Distinct from the six other already-filed bugs:
  - The four `*-spawn-despawn-*` and `add-components-pending-spawn` reports are about `core/` mutation cache mechanics.
  - `simulation-service-run-discards-runconfig` is about `SimulationService.run` substituting a fresh `RunConfig`.
  - `update-command-silently-noops` is the *sibling* of this report — same dispatcher, same silent-no-op shape, different command type, different underlying defect (UPDATE has the wrong target method; REMOVE_COMPONENT has the right target method but the dispatcher fails to coerce strings to types). They should be addressed in the same PR.
- After both `update-command-silently-noops` and this report land, the dispatcher in `command_service.py:147-195` should grow a small audit pass: every `case` that touches `payload[...]` should be checked for "what happens when the payload was JSON-decoded?" Both bugs were caused by one missing helper call at one line. There may be more.
- The same string-vs-class trap exists for any future code that does `set(component_types)` without first asserting `isinstance(t, type) for t in component_types`. The `Archetype.add_components` method has the same `set(sig).union(component_types)` shape (`archetype.py:61`); a future dispatcher that forgets to hydrate would hit a sibling bug — the new `(Position, "Position")` archetype would have a string in the sig and break in interesting downstream ways.
- Quota-exhaustion attack vector (same as the UPDATE report): `coder` role can spam REMOVE_COMPONENT, exhaust the `remove_component: 4` quota, lock out legitimate REMOVE_COMPONENT traffic. None of which would have done anything anyway. Worth flagging in `2026-03-28-security-program-review.md`.
- Sync world (`SyncWorld`) goes through the same `command_service.apply` dispatcher and therefore has the same bug; the routing is shared.
