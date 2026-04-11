# Bug Report: `Component.get_prefix` lowercases class names — same-prefix components silently overwrite each other's row data

**Date:** 2026-04-11
**Severity:** Medium-High (silent data corruption when two components share a lowercased class name; surface area widens with every new third-party component package)
**Affects:** `archetype.core.component.Component.get_prefix` + `archetype.core.archetype.Archetype.to_row_dict` — and any user that defines two `Component` subclasses whose `__name__.lower()` collide
**Discovered by:** Overnight bug hunt

## Summary

`Component.get_prefix` returns `cls.__name__.lower() + "__"` (`component.py:45-47`). It uses *only* the lowercased class name — no module qualifier, no UUID, no namespace. Two `Component` subclasses with names that differ only in case (e.g. `Position` and `position`), or two same-named classes from different modules (the realistic case after a refactor or a third-party component package), produce the **same** prefix. When both are placed on the same entity, `Archetype.to_row_dict` (`archetype.py:120-130`) builds the row dict by iterating components and calling `row_dict.update({prefix + key: value for ...})` — the second component's `position__x` silently overwrites the first component's `position__x`. Arrow's `unify_schemas` accepts the merged schema because both fields have the same name and (in the MRE) the same dtype. The collision is invisible: no warning, no error, no log line. The user gets a row that mixes data from two completely unrelated components under the same key.

## Impact

1. **Silent data corruption when two third-party component packages happen to choose the same class name.** A package providing `Position(x: int, y: int)` and a different package providing `Position(latitude: float, longitude: float)` cannot coexist on the same entity. The second-imported `Position`'s `position__x` etc. will overwrite the first's. The schemas are different shapes — the row dict ends up with whichever component wrote last, plus *some* of the other component's fields if the field names don't overlap. Cross-package component mixing is structurally unsafe.
2. **Refactors that move components between modules can silently break old data.** Moving `app.physics.Position` to `app.world.Position` doesn't change the class name, so the prefix stays `position__`. But if old serialised data referenced a *different* Position from elsewhere in the codebase, mixing them produces silent overwrites — and the bug is invisible at the time of the rename.
3. **The "auto-generate components from a JSON schema" pattern (a common ergonomic for DSLs and config-driven worlds) is unsafe.** Two generated component types with the same `name` field in the schema collide under the same prefix. The DSL has no way to detect or prevent this without inspecting prefixes itself.
4. **There is no register-time check.** `Component.__init_subclass__` is not used; `Archetype.get_archetype_schema` calls `pa.unify_schemas` which silently merges duplicate field names if their dtypes match. The first time the bug fires is at row construction, and even then it's invisible — only the values are wrong.
5. **Discovery is structural.** The fix is a one-line change to `get_prefix`, but requires touching `core/`. Until then, every Archetype user implicitly assumes class-name uniqueness across the entire process — which is a constraint Python itself does *not* guarantee, and which gets weaker the more component packages a project depends on.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 848726f, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: two Component subclasses with the same lowercased name share a
prefix; the second component silently overwrites the first under the
shared 'position__x' key."""
import asyncio
import tempfile

from archetype.app.container import ServiceContainer
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


class Position(Component):
    x: int = 0


class position(Component):  # noqa: N801 — intentional case difference
    x: int = 0


async def main() -> None:
    print(f"Position prefix = {Position.get_prefix()!r}")
    print(f"position prefix = {position.get_prefix()!r}")
    assert Position.get_prefix() == position.get_prefix()
    print(f"Position(x=1).to_row_dict()  = {Position(x=1).to_row_dict()}")
    print(f"position(x=99).to_row_dict() = {position(x=99).to_row_dict()}")

    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            info = await container.world_service.create_world(
                WorldConfig(name="mre"), StorageConfig(uri=tmp)
            )
            world = container.world_service.get_world(info.world_id)
            await world.create_entity([Position(x=1), position(x=99)])
            await world.run(RunConfig(num_steps=1))
            row = (await world.get_components([Position, position])).collect().to_pylist()[0]
            print(f"row = {row}")
            assert row["position__x"] == 1, (
                f"BUG: position(x=99) silently overwrote Position(x=1); "
                f"got position__x={row['position__x']}"
            )
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
Position prefix = 'position__'
position prefix = 'position__'
Position(x=1).to_row_dict()  = {'position__x': 1}
position(x=99).to_row_dict() = {'position__x': 99}
rows = [{'world_id': '...', 'run_id': '...', 'entity_id': 1, 'tick': 0,
         'is_active': True, 'position__x': 99}]
AssertionError: BUG: position(x=99) silently overwrote Position(x=1);
                got position__x=99
```

The row dict contains exactly **one** `position__x` value: `99`. `Position(x=1)` was lost completely. There is no warning, no error during spawn, no error during step, and no signal anywhere that the data corruption occurred.

### Baseline (proves the bug is scoped to "shared lowercased name")

Components with distinct class names produce distinct prefixes and don't collide:

```python
class Position(Component):
    x: int = 0

class Velocity(Component):
    x: int = 0  # SAME field name, but different class name → different prefix

await world.create_entity([Position(x=1), Velocity(x=99)])
await world.run(RunConfig(num_steps=1))
row = (await world.get_components([Position, Velocity])).collect().to_pylist()[0]
# row = {... 'position__x': 1, 'velocity__x': 99}
# OK (baseline): distinct class names → distinct prefixes → no collision.
```

`position__x` and `velocity__x` are distinct keys; both values survive. The bug is purely the case-only difference in class names (or, equivalently, two same-named classes from different modules).

## Root cause

`src/archetype/core/component.py:44-47`:

```python
@classmethod
def get_prefix(cls) -> str:
    """Generate a standardized prefix for this component type's fields."""
    return cls.__name__.lower() + "__"
```

`src/archetype/core/archetype.py:120-132`:

```python
row_dict = {
    "world_id": str(world_id),
    "run_id": str(run_id),
    "entity_id": entity_id,
    "tick": tick,
    "is_active": True,
}

for c in components:
    prefix = c.get_prefix()
    row_dict.update({prefix + key: value for key, value in c.model_dump().items()})

return row_dict
```

Trace for the MRE:

1. `create_entity([Position(x=1), position(x=99)])` calls `Archetype.to_row_dict`.
2. The component loop iterates in caller order:
   - `Position(x=1)`: `prefix = "position__"`, updates `row_dict["position__x"] = 1`.
   - `position(x=99)`: `prefix = "position__"`, updates `row_dict["position__x"] = 99` — **overwrites** the previous value.
3. `row_dict` ends with `{"position__x": 99, ...}`. The Position contribution is gone.
4. `Archetype.get_archetype_schema` (`archetype.py:91-101`) calls `pa.unify_schemas([base, Position.prefixed, position.prefixed])`. Both `Position.get_prefixed_schema()` and `position.get_prefixed_schema()` produce a field named `position__x` of type `int32`. Arrow's `unify_schemas` happily merges identical-named identical-typed fields into one — no error.
5. The store persists `(entity_id=1, position__x=99)`. Both components are queryable via `get_components([Position, position])` because both classes match the sig (both are in `_entity2sig[1]`), and the projection schema includes `position__x` once.
6. The user reads back `position__x=99` regardless of which component they meant to query.

The defect cascades: `get_prefix` produces non-unique prefixes → `to_row_dict` overwrites silently → `unify_schemas` merges silently → the store persists corrupt data. There is no layer in between that catches it.

`unify_schemas` would have raised if the two `position__x` fields had different dtypes. Try the MRE with `Position.x: int` and `position.x: str` and you get a different failure mode (schema unification raises during world setup). The "silently merge" path is the worst case because it succeeds.

## Why existing tests miss this

`grep -rn "get_prefix\|prefix.*collision\|same.*name" tests/` returns six matches, all of which use distinct class names:

- `tests/core/test_component_core.py:13` — `assert c.get_prefix() == "mixed__"` (single component, no collision possible).
- `tests/core/test_archetype_core_signatures.py:45-50::test_get_archetype_schema_unifies_base_and_components` — uses `A(x=1), B(y=2)` (distinct class names, **distinct field names**, intentionally non-colliding).
- `tests/aio/test_async_world_querying.py:84` — `Position.get_prefixed_schema()` (single component).

There is no test in the suite that:

1. Defines two `Component` subclasses with the same lowercased class name.
2. Asserts that placing both on the same entity preserves both components' values.
3. Asserts that `Component.get_prefix` returns unique prefixes for distinct subclasses.

`Archetype.get_archetype_schema` is tested only for the "non-colliding" case. The collision case is uncovered.

The implicit invariant ("all `Component` subclasses in a process must have unique lowercased names") is documented nowhere and enforced nowhere. Users discover it the way I just did — by writing the wrong code and noticing the values are wrong.

## Suggested fixes

**Fix A — disambiguate the prefix with the module qualifier and a stable hash.** Mirrors the rest of the codebase's "use a hash for uniqueness" pattern (`Archetype.get_name` already does this with a SHA-256 suffix). Lands in `core/`, requires approval:

```diff
 # src/archetype/core/component.py
+import hashlib
+
 @classmethod
 def get_prefix(cls) -> str:
     """Generate a standardized prefix for this component type's fields."""
-    return cls.__name__.lower() + "__"
+    # Disambiguate by fully qualified name so two components with the same
+    # lowercased class name from different modules don't collide.
+    fqn = f"{cls.__module__}.{cls.__qualname__}"
+    suffix = hashlib.sha256(fqn.encode()).hexdigest()[:6]
+    return f"{cls.__name__.lower()}_{suffix}__"
```

The stable 6-char hash suffix ensures `Position` from `app.physics` and `Position` from `plugin.geo` get *different* prefixes (`position_a1b2c3__` vs `position_d4e5f6__`) while still being readable. This is a breaking change for any caller that depends on the exact prefix string today (e.g. processors that hard-code `col("position__x")`). Migration: callers should always go through `Component.get_prefix()` instead of hard-coding the prefix.

**Fix B — register-time uniqueness check.** Use `__init_subclass__` to assert that no two `Component` subclasses share a prefix at class-definition time. Smaller change but only catches the bug at import time, not at runtime:

```diff
 # src/archetype/core/component.py
+_REGISTERED_PREFIXES: dict[str, type] = {}
+
 class Component(LanceModel):
+    def __init_subclass__(cls, **kwargs):
+        super().__init_subclass__(**kwargs)
+        prefix = cls.__name__.lower() + "__"
+        existing = _REGISTERED_PREFIXES.get(prefix)
+        if existing is not None and existing is not cls:
+            raise ValueError(
+                f"Component prefix collision: {cls.__module__}.{cls.__qualname__} "
+                f"and {existing.__module__}.{existing.__qualname__} both produce "
+                f"prefix {prefix!r}. Rename one of them or change Component.get_prefix "
+                f"to disambiguate."
+            )
+        _REGISTERED_PREFIXES[prefix] = cls
```

Fix B catches `class position(Component)` after `class Position(Component)` at import time with a clear `ValueError`. It is the *minimum* defensible fix, but it only catches the bug for users who actually define two same-prefix components — it doesn't change the underlying schema layout or fix any existing in-the-wild collision.

**Fix C — defensive check at row build time.** Catch the `to_row_dict` overwrite and raise:

```diff
 # src/archetype/core/archetype.py
 row_dict = { ... base columns ... }

+seen_keys: set[str] = set(row_dict.keys())
 for c in components:
     prefix = c.get_prefix()
-    row_dict.update({prefix + key: value for key, value in c.model_dump().items()})
+    new = {prefix + key: value for key, value in c.model_dump().items()}
+    overlap = seen_keys & new.keys()
+    if overlap:
+        raise ValueError(
+            f"Component prefix collision in to_row_dict: {type(c).__module__}.{type(c).__name__} "
+            f"already wrote keys {overlap} via another component in this entity. "
+            f"Two Component subclasses share the prefix {prefix!r}."
+        )
+    seen_keys.update(new.keys())
+    row_dict.update(new)
```

I'd recommend **Fix A as the real fix** (it makes prefixes structurally unique) plus **Fix B as a defence** (it catches future collisions at registration time). Fix C is a fallback if `core/` approval for A isn't possible — it at least surfaces the bug as a hard failure instead of silent corruption.

## Suggested regression tests

Add to `tests/core/test_component_core.py`:

```python
def test_get_prefix_is_unique_for_distinct_subclasses():
    """Regression: two Component subclasses with names that differ only
    in case must produce different prefixes (or fail at registration)."""
    class Foo(Component):
        x: int = 0

    # On main, this raises ValueError (Fix B) or produces a different
    # prefix (Fix A). On unfixed main, both produce 'foo__'.
    try:
        class foo(Component):  # noqa: N801
            x: int = 0
    except ValueError:
        return  # Fix B is in place

    # Otherwise, prefixes must differ (Fix A).
    assert Foo.get_prefix() != foo.get_prefix()


def test_to_row_dict_rejects_prefix_collision():
    """Regression: to_row_dict must not silently overwrite when two
    components share a prefix. With Fix A this is impossible by
    construction; with Fix C it raises ValueError."""
    class Bar(Component):
        x: int = 0

    class bar(Component):  # noqa: N801
        x: int = 0

    # Either a ValueError at to_row_dict time (Fix C), or both values
    # preserved under distinct keys (Fix A).
    try:
        row = Archetype.to_row_dict(
            entity_id=1, tick=0,
            components=[Bar(x=1), bar(x=99)],
            world_id="w", run_id="r",
        )
    except ValueError:
        return  # Fix C is in place

    # Fix A path: distinct keys.
    bar_keys = sorted(k for k in row if "bar" in k or "x" in k)
    assert any(row[k] == 1 for k in bar_keys), f"Bar(x=1) lost: {row}"
    assert any(row[k] == 99 for k in bar_keys), f"bar(x=99) lost: {row}"


@pytest.mark.asyncio
async def test_two_same_prefix_components_on_same_entity_round_trip(tmp_path):
    """Regression: spawning an entity with two same-prefix components
    must either fail loudly or preserve both components' values."""
    class Baz(Component):
        x: int = 0

    class baz(Component):  # noqa: N801
        x: int = 0

    container = ServiceContainer()
    try:
        info = await container.world_service.create_world(
            WorldConfig(name="t"), StorageConfig(uri=str(tmp_path))
        )
        world = container.world_service.get_world(info.world_id)

        try:
            await world.create_entity([Baz(x=1), baz(x=99)])
            await world.run(RunConfig(num_steps=1))
        except (ValueError, RuntimeError):
            return  # any of Fix A/B/C surface the error here

        row = (await world.get_components([Baz, baz])).collect().to_pylist()[0]
        baz_values = sorted(v for k, v in row.items() if "baz" in k)
        assert 1 in baz_values, f"Baz(x=1) silently lost: {row}"
        assert 99 in baz_values, f"baz(x=99) silently lost: {row}"
    finally:
        await container.shutdown()
```

All three tests fail on `main` — the third with `assert 1 in baz_values` because the second component overwrote the first.

## Notes / scope

- Affects `src/archetype/core/component.py:44-47` (`get_prefix`) and `src/archetype/core/archetype.py:120-130` (`to_row_dict`). Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- Distinct from the eleven other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - The four `command_service.apply` bugs are about dispatcher routing/typing.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker leak.
  - `on-spawn-on-despawn-hooks-never-fire` is about hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is about quota accounting.
  - This bug is about the schema layer assuming class-name uniqueness when Python itself doesn't.
- Real-world likelihood: today's project has tests with class names like `Position`, `Velocity`, `Pos` — all distinct. The bug doesn't fire in the existing test suite. As soon as a third-party component package or auto-generated DSL component arrives with a name collision, it will fire silently. Fix A or B should land before any such package enters the dependency tree.
- The same lowercase-prefix shape exists in `src/archetype/core/sync/world.py` because `to_row_dict` is shared with `AsyncWorld`. The sync engine has the bug too.
- A small follow-up worth noting: `Component.get_type_by_name` (`component.py:26-33`) iterates `cls.__subclasses__()` and matches by `__name__` only. Two classes with the same `__name__` from different modules will return the *first* match — non-deterministic order, very fun debugging session.
