# Bug Report: `Component.get_type_by_name` only checks `cls.__subclasses__()` (direct subclasses) — transitively-inherited Component subclasses cannot be hydrated from the wire format

**Date:** 2026-04-11
**Severity:** Medium-High (silent rejection of any Component hierarchy with depth > 1; breaks the wire-protocol SPAWN/UPDATE/ADD_COMPONENT path for hierarchical components)
**Affects:** `archetype.core.component.Component.get_type_by_name` and (transitively) `Component.from_dict` + `command_service._hydrate_components`
**Discovered by:** Overnight bug hunt

## Summary

`Component.get_type_by_name` (`component.py:26-33`) iterates `cls.__subclasses__()` — Python's *immediate* subclass list — to find a Component subclass by name. It does not recurse. Any class that inherits from a Component subclass (e.g., `class SpecialPosition(Position)` where `Position(Component)`) is in `Position.__subclasses__()`, not `Component.__subclasses__()`. So `Component.get_type_by_name("SpecialPosition")` raises `ValueError: Component type 'SpecialPosition' not found` even though `SpecialPosition` *is* a `Component` (transitively).

This propagates through `Component.from_dict` (`component.py:35-42`), which is the only documented entry point for "build a Component instance from a JSON-style dict". And `from_dict` is used by `command_service._hydrate_components` (`command_service.py:96-107`) — the wire-protocol payload deserialization helper. So:

- Wire-protocol `SPAWN` / `UPDATE` / `ADD_COMPONENT` of a hierarchical component fails with `ValueError`.
- Direct in-process callers using `Component.from_dict({"type": "SpecialPosition", ...})` get the same failure.
- The error is *not* silent (this isn't a no-op like several other filed bugs) — it raises — but the failure happens deep inside the dispatcher and surfaces as a logged exception in `drain_and_apply` (`command_service.py:88-89`), at which point the command is silently dropped, just acked, and the audit history records it as applied.

## Impact

1. **Component hierarchies of depth > 1 are unusable through the wire protocol.** Any project that defines a base component type and specializes it via inheritance — a natural pattern for "Position with extras", "Agent with personality", "Actor with role" — gets a hard `ValueError` the first time anyone tries to spawn one through a JSON command. The error message says "Component type X not found", which is confusing because X *is* defined as a Component subclass (transitively).
2. **The error is silent at the broker layer.** `drain_and_apply` (`command_service.py:84-89`) wraps `apply` in a `try/except Exception:` that logs and continues. So a `ValueError` from `_hydrate_components` is caught, logged at exception level, the cmd is removed from `applied_ids`, but the broker still acks the rest of the batch. The actor's quota was consumed (per the filed `enqueue-bulk-quota-debit-on-failure` report), the audit history shows the cmd, and the user gets no signal at the REST layer that anything failed.
3. **Documentation contradicts the implementation.** `AGENTS.md` says "Components — Use composition over inheritance" as a *recommendation*, not a hard rule. The framework should still support inheritance correctly when users follow the natural Python pattern, or at minimum reject inheritance at registration with a clear error. Today it does neither.
4. **`Component.from_dict` is the only public hydration API.** There is no escape hatch to register custom subclass-name resolvers. Users with hierarchical components have to bypass `from_dict` entirely (manual `cls(**dict_minus_type)` construction) — losing the wire-protocol support, the broker dispatch, and the JSON-payload ergonomics.
5. **The bug is structurally invisible.** No test in the suite uses a component hierarchy. `tests/core/test_component_core.py::test_get_type_by_name_and_from_dict` (the canonical test) defines `class DemoUnique(Component)` — a direct subclass — and asserts the lookup works. The transitive case is uncovered. The bug can sit in main indefinitely.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 9511540, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: Component.get_type_by_name only checks Component.__subclasses__()
and misses transitive descendants."""
from archetype.core.component import Component


class Position(Component):
    x: int = 0
    y: int = 0


class SpecialPosition(Position):
    """A position with extra metadata. Inherits from Position, not Component."""
    label: str = ""


def main() -> None:
    # Direct subclass: works.
    direct = Component.from_dict({"type": "Position", "x": 1, "y": 2})
    print(f"Position hydrated: {direct}")
    assert isinstance(direct, Position)

    print(f"Component subclasses: {[c.__name__ for c in Component.__subclasses__()]}")
    print(f"Position subclasses:  {[c.__name__ for c in Position.__subclasses__()]}")

    # Indirect (transitive) subclass: should also work.
    try:
        special = Component.from_dict({"type": "SpecialPosition", "x": 1, "y": 2, "label": "home"})
        print(f"SpecialPosition hydrated: {special}")
    except ValueError as e:
        print(f"BUG: {e}")
        raise


if __name__ == "__main__":
    main()
```

### Observed output

```
Position hydrated: x=1 y=2
Component subclasses: ['Position']
Position subclasses:  ['SpecialPosition']
BUG: Component type 'SpecialPosition' not found.
Traceback (most recent call last):
  ...
ValueError: Component type 'SpecialPosition' not found.
```

`Component.__subclasses__()` returns `['Position']` — `SpecialPosition` is missing. It lives one level down in `Position.__subclasses__()`. The lookup raises `ValueError`.

### Baseline (proves the bug is the missing recursion, not a registration issue)

Direct subclasses are hydrated correctly:

```python
direct = Component.from_dict({"type": "Position", "x": 1, "y": 2})
# Position hydrated: x=1 y=2
```

`Position` is a direct subclass of `Component`, so `Component.__subclasses__()` finds it. The bug fires only when there's at least one extra level of inheritance between the target class and `Component`.

## Root cause

`src/archetype/core/component.py:26-33`:

```python
@classmethod
def get_type_by_name(cls, name: str) -> type["Component"]:
    """Finds a Component subclass by its name."""
    # This could be optimized with a cache if needed
    for subclass in cls.__subclasses__():
        if subclass.__name__ == name:
            return subclass
    raise ValueError(f"Component type '{name}' not found.")
```

`cls.__subclasses__()` returns only the *direct* descendants of `cls`. From the Python docs:

> `class.__subclasses__()` — Each class keeps a list of weak references to its immediate subclasses.

There is no recursion. To find a transitive descendant, the function would need a depth-first walk:

```python
def _all_subclasses(cls):
    seen = set()
    stack = list(cls.__subclasses__())
    while stack:
        sub = stack.pop()
        if sub in seen:
            continue
        seen.add(sub)
        stack.extend(sub.__subclasses__())
    return seen
```

Trace for the MRE:

1. `class Position(Component)` registers `Position` in `Component.__subclasses__()`.
2. `class SpecialPosition(Position)` registers `SpecialPosition` in `Position.__subclasses__()` — but **not** in `Component.__subclasses__()`. This is how Python's MRO works.
3. `Component.from_dict({"type": "SpecialPosition", ...})` calls `Component.get_type_by_name("SpecialPosition")`.
4. The function loops `for subclass in Component.__subclasses__():` — i.e. `for subclass in [Position]:`. It finds `Position.__name__ == "Position" != "SpecialPosition"`, so the loop ends.
5. `raise ValueError(f"Component type 'SpecialPosition' not found.")`.

The fix is to walk all transitive subclasses. The "could be optimized with a cache" comment in the existing code suggests the author was aware of the linear scan but didn't notice the missing recursion.

## Why existing tests miss this

`tests/core/test_component_core.py:28-36::test_get_type_by_name_and_from_dict`:

```python
class DemoUnique(Component):
    a: int


def test_get_type_by_name_and_from_dict():
    """Dynamic type lookup and dict-based construction should work for LanceModel-based components."""
    T = Component.get_type_by_name("DemoUnique")
    assert T.__name__ == DemoUnique.__name__
    inst = Component.from_dict({"type": DemoUnique.__name__, "a": 5})
    assert isinstance(inst, DemoUnique)
    assert inst.a == 5
```

`DemoUnique` is a *direct* subclass of `Component`. The test passes because `Component.__subclasses__()` returns `[..., DemoUnique, ...]`. The transitive case is not exercised.

`tests/core/test_component_core.py:39-44::test_get_type_by_name_raises_for_missing` only checks that a *truly* unknown name raises. It does not check that a *known but transitive* name resolves correctly.

`grep -rn "get_type_by_name\|from_dict" tests/` returns no test that defines a Component hierarchy and tries to hydrate the deeper class.

## Suggested fixes

**Fix A — recursively walk all subclasses.** The minimal correct fix:

```diff
 @classmethod
 def get_type_by_name(cls, name: str) -> type["Component"]:
-    """Finds a Component subclass by its name."""
-    # This could be optimized with a cache if needed
-    for subclass in cls.__subclasses__():
-        if subclass.__name__ == name:
-            return subclass
-    raise ValueError(f"Component type '{name}' not found.")
+    """Finds a Component subclass by its name (recursively)."""
+    # DFS over all transitive descendants of cls.
+    stack = list(cls.__subclasses__())
+    seen: set[type] = set()
+    while stack:
+        sub = stack.pop()
+        if sub in seen:
+            continue
+        seen.add(sub)
+        if sub.__name__ == name:
+            return sub
+        stack.extend(sub.__subclasses__())
+    raise ValueError(f"Component type '{name}' not found.")
```

This is the smallest change that makes the function honour its name (`get_type_by_name`, not `get_direct_subclass_by_name`). Lands in `core/`, requires approval.

**Fix B — maintain a registry via `__init_subclass__`.** Faster than DFS-on-every-call and surfaces the prefix-collision bug from the just-filed `component-prefix-collision.md` for free:

```diff
 class Component(LanceModel):
+    _registry: dict[str, type["Component"]] = {}
+
+    def __init_subclass__(cls, **kwargs):
+        super().__init_subclass__(**kwargs)
+        existing = Component._registry.get(cls.__name__)
+        if existing is not None and existing is not cls:
+            raise ValueError(
+                f"Component name collision: {existing.__module__}.{existing.__qualname__} "
+                f"and {cls.__module__}.{cls.__qualname__} both register as {cls.__name__!r}"
+            )
+        Component._registry[cls.__name__] = cls
+
     @classmethod
     def get_type_by_name(cls, name: str) -> type["Component"]:
-        for subclass in cls.__subclasses__():
-            if subclass.__name__ == name:
-                return subclass
-        raise ValueError(f"Component type '{name}' not found.")
+        if name in Component._registry:
+            return Component._registry[name]
+        raise ValueError(f"Component type {name!r} not found in Component registry")
```

Fix B is O(1) lookup, catches name collisions at definition time (subsumes one of the fixes from `component-prefix-collision.md`), and naturally handles transitive descendants because every Component subclass at any depth ends up in the registry. The downside: it requires touching `core/` and changes the registration model.

I'd recommend **Fix B** as the right shape — it solves two filed bugs at once and the registry pattern is the standard approach. Fix A is a smaller patch that closes only this report.

## Suggested regression tests

Add to `tests/core/test_component_core.py`:

```python
class _BasePosition(Component):
    x: int = 0
    y: int = 0


class _SpecialPosition(_BasePosition):
    """Subclass of _BasePosition, not direct subclass of Component."""
    label: str = ""


def test_get_type_by_name_finds_transitive_subclasses():
    """Regression: Component.get_type_by_name must find subclasses at any
    depth in the inheritance tree, not just direct subclasses."""
    T = Component.get_type_by_name("_SpecialPosition")
    assert T is _SpecialPosition


def test_from_dict_hydrates_transitive_subclasses():
    """Regression: Component.from_dict must work for hierarchical components."""
    inst = Component.from_dict(
        {"type": "_SpecialPosition", "x": 1, "y": 2, "label": "home"}
    )
    assert isinstance(inst, _SpecialPosition)
    assert inst.x == 1
    assert inst.y == 2
    assert inst.label == "home"


@pytest.mark.asyncio
async def test_command_service_spawn_with_hierarchical_component(tmp_path):
    """End-to-end: a SPAWN command with a transitive Component subclass
    in the payload must successfully apply, not raise ValueError silently."""
    from archetype.app.container import ServiceContainer
    from archetype.app.models import Command, CommandType
    from archetype.core.config import RunConfig, StorageConfig, WorldConfig

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        cmd = Command(
            type=CommandType.SPAWN,
            payload={
                "components": [
                    {"type": "_SpecialPosition", "x": 5, "y": 6, "label": "spawn"}
                ]
            },
        )
        # Must NOT raise (today, this raises ValueError inside _hydrate_components).
        await container.command_service.apply(world, cmd)
        await world.run(RunConfig(num_steps=1))

        df = await world.get_components([_SpecialPosition])
        rows = df.collect().to_pylist()
        assert rows
        assert rows[0]["_specialposition__label"] == "spawn"
    finally:
        await container.shutdown()
```

The first two tests fail on `main` with `ValueError: Component type '_SpecialPosition' not found`. The third fails the same way, swallowed by `command_service.apply`'s outer caller and visible as either a logged exception (via `drain_and_apply`) or a re-raised `ValueError` (via direct `apply` call).

## Notes / scope

- Affects `src/archetype/core/component.py:26-33`. Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- Distinct from the thirteen other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is about hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is about quota accounting.
  - `component-prefix-collision` is about lowercased class names colliding in `get_prefix`.
  - `cached-store-read-shadows-disk` is about the cache hiding flushed rows.
  - This bug is about the *type lookup* layer not matching the (transitive) class hierarchy. It's a sibling of `component-prefix-collision` — both are issues with how `Component` indexes/identifies its subclasses.
- Combines very poorly with `remove-component-strings-noop` and `update-command-silently-noops` already filed: those bugs make the dispatcher silently no-op for some command types; this one makes the dispatcher silently raise for some component shapes. A user with a hierarchical component model + JSON wire payloads gets: SPAWN raises, UPDATE no-ops, REMOVE_COMPONENT no-ops. Three of four entity-write paths are broken or partially broken from the wire.
- After Fix B lands, the `Component._registry` is a natural place to enforce the prefix-collision check from the `component-prefix-collision` report. The two reports should be addressed in the same PR.
- A small follow-up worth a separate hunt: the same `__subclasses__()` non-recursive pattern may exist elsewhere in the codebase. `grep -rn "__subclasses__" src/archetype/` will find them. Fast scan: only `component.py` uses it today, but agents and DSL layers being added in the future are likely to repeat the pattern.
