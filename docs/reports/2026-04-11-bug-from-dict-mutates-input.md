# Bug Report: `Component.from_dict` destructively mutates its input dict via `data.pop("type")` — a second call on the same dict silently returns the wrong type

**Date:** 2026-04-11
**Severity:** Medium (silent type corruption on reuse of the input dict; the docstring does not warn about the destructive mutation)
**Affects:** `archetype.core.component.Component.from_dict` — every caller that reuses the dict after the call (config-driven workflows, templating, round-trip serialization, retry logic)
**Discovered by:** Overnight bug hunt

## Summary

`Component.from_dict` (`component.py:36-42`) calls `data.pop("type", None)` to extract the component type name from the input dict. `dict.pop` is destructive — it removes the key from the caller's dict in-place. After the call, the input dict no longer contains the `"type"` key. If any subsequent code reads the same dict (a second `from_dict` call, logging, JSON re-serialization, a retry loop), the `"type"` field is gone and the hydration silently produces a bare `Component()` instead of the intended subclass.

The in-repo caller `command_service._hydrate_components` defends against this with `Component.from_dict(dict(item))` — a deliberate shallow copy to avoid mutating the payload. The defensive comment `# copy to avoid mutating payload` documents that the author *knew* about the mutation. But the public API `Component.from_dict` itself is the footgun: any external caller who reads the docstring ("Create a component instance from a dictionary") and calls `from_dict(my_dict)` gets a silently mutated dict.

## Impact

1. **Config-driven entity templates are broken on reuse.** A workload that keeps a dict template and calls `from_dict(template)` for each entity gets the correct type on the first call and a bare `Component()` on every subsequent call. The second entity's components are all wrong.
2. **Retry logic silently changes the component type.** A caller that retries `from_dict(payload)` after a transient failure gets a different type on the second call — `Component()` instead of `Position()`. The retry "succeeds" but produces wrong data.
3. **JSON round-trip loses the type key.** `original_dict -> from_dict -> ... -> json.dumps(original_dict)` produces JSON without the `"type"` key, because `from_dict` popped it. Any system that serializes the input dict after hydration (audit logging, debug dumps, message forwarding) loses the type information.
4. **The `_hydrate_components` defense proves the bug exists.** The comment "copy to avoid mutating payload" at `command_service.py:104` is a per-caller workaround, not a fix. Every *new* caller of `from_dict` must independently discover that the function mutates its input and add their own copy. This is error-prone.
5. **The fix is one character.** Change `data.pop("type", None)` to `data.get("type", None)` and then exclude `"type"` from the `**data` spread. Or copy the dict inside `from_dict` so the caller's dict is never touched.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 74823c2, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: Component.from_dict pops "type" from the input dict."""
from archetype.core.component import Component


class Position(Component):
    x: int = 0
    y: int = 0


def main() -> None:
    template = {"type": "Position", "x": 1, "y": 2}
    print(f"before: {template}")

    c1 = Component.from_dict(template)
    print(f"after 1st from_dict: {template}")
    print(f"  c1 type = {type(c1).__name__}")

    c2 = Component.from_dict(template)
    print(f"after 2nd from_dict: {template}")
    print(f"  c2 type = {type(c2).__name__}")

    assert type(c2) is Position, (
        f"BUG: second from_dict returned {type(c2).__name__}, expected Position"
    )


if __name__ == "__main__":
    main()
```

### Observed output

```
before: {'type': 'Position', 'x': 1, 'y': 2}
after 1st from_dict: {'x': 1, 'y': 2}
  c1 type = Position
after 2nd from_dict: {'x': 1, 'y': 2}
  c2 type = Component
AssertionError: BUG: second from_dict returned Component, expected Position
```

The first call pops `"type"` from the dict. The second call sees no `"type"` key → `component_type_name = None` → falls through to `return cls(**data)` → returns a bare `Component` instead of `Position`.

### Baseline (proves the bug is the destructive pop, not the hydration logic)

Constructing a fresh dict per call avoids the mutation:

```python
c1 = Component.from_dict({"type": "Position", "x": 1, "y": 2})
c2 = Component.from_dict({"type": "Position", "x": 1, "y": 2})
# c1 type=Position, c2 type=Position
# OK (baseline): fresh dicts avoid the mutation bug.
```

The hydration logic is correct. The bug is purely the in-place mutation of the input dict.

## Root cause

`src/archetype/core/component.py:35-42`:

```python
@classmethod
def from_dict(cls, data: dict[str, Any]) -> "Component":
    """Create a component instance from a dictionary."""
    component_type_name = data.pop("type", None)
    if component_type_name:
        ComponentType = cls.get_type_by_name(component_type_name)
        return ComponentType(**data)
    return cls(**data)
```

`data.pop("type", None)` removes the `"type"` key from the caller's dict. After the call, `data == {"x": 1, "y": 2}` — the type information is gone.

The fix is to avoid mutating the input:

```python
component_type_name = data.get("type", None)  # read without removing
if component_type_name:
    ComponentType = cls.get_type_by_name(component_type_name)
    fields = {k: v for k, v in data.items() if k != "type"}
    return ComponentType(**fields)
return cls(**data)
```

Or copy-on-entry: `data = dict(data)` at the top of the function. Both approaches are equivalent; the `get` + exclude pattern avoids the allocation.

## Why existing tests miss this

`tests/core/test_component_core.py:28-36::test_get_type_by_name_and_from_dict`:

```python
def test_get_type_by_name_and_from_dict():
    T = Component.get_type_by_name("DemoUnique")
    assert T.__name__ == DemoUnique.__name__
    inst = Component.from_dict({"type": DemoUnique.__name__, "a": 5})
    assert isinstance(inst, DemoUnique)
    assert inst.a == 5
```

The test:

1. Constructs a **fresh dict literal** per call (no reuse).
2. Does NOT inspect the dict after `from_dict` returns.
3. Does NOT call `from_dict` twice with the same dict.

There is no test that checks `"type" in data` after the call, or that calls `from_dict(same_dict)` twice and asserts the second result has the correct type.

## Suggested fixes

**Fix A — use `data.get` instead of `data.pop` and exclude `"type"` from the spread.** Lands in `core/`, requires approval:

```diff
 @classmethod
 def from_dict(cls, data: dict[str, Any]) -> "Component":
     """Create a component instance from a dictionary."""
-    component_type_name = data.pop("type", None)
+    component_type_name = data.get("type", None)
     if component_type_name:
         ComponentType = cls.get_type_by_name(component_type_name)
-        return ComponentType(**data)
+        return ComponentType(**{k: v for k, v in data.items() if k != "type"})
     return cls(**data)
```

**Fix B — copy the dict on entry.** Also lands in `core/`:

```diff
 @classmethod
 def from_dict(cls, data: dict[str, Any]) -> "Component":
     """Create a component instance from a dictionary."""
+    data = dict(data)  # don't mutate the caller's dict
     component_type_name = data.pop("type", None)
     if component_type_name:
         ComponentType = cls.get_type_by_name(component_type_name)
         return ComponentType(**data)
     return cls(**data)
```

Fix B is what `_hydrate_components` already does at the call site. Moving the copy into `from_dict` eliminates the need for every caller to remember to copy.

Fix A is slightly cheaper (no allocation), Fix B is mechanically simpler (one-line addition). Either works.

## Suggested regression tests

Add to `tests/core/test_component_core.py`:

```python
def test_from_dict_does_not_mutate_input():
    """Regression: from_dict must not pop 'type' from the caller's dict."""
    data = {"type": "DemoUnique", "a": 5}
    Component.from_dict(data)
    assert "type" in data, "from_dict mutated the input dict by popping 'type'"


def test_from_dict_same_dict_twice_returns_correct_type():
    """Regression: calling from_dict twice with the SAME dict must produce
    the correct type both times."""
    data = {"type": "DemoUnique", "a": 5}
    c1 = Component.from_dict(data)
    c2 = Component.from_dict(data)
    assert isinstance(c1, DemoUnique)
    assert isinstance(c2, DemoUnique), (
        f"second from_dict returned {type(c2).__name__}, expected DemoUnique"
    )
```

Both tests fail on `main`. The first fails at `assert "type" in data`. The second fails at `assert isinstance(c2, DemoUnique)`.

## Notes / scope

- Affects `src/archetype/core/component.py:38`. Per `CLAUDE.md`, `core/` is read-only for agents without explicit permission, so this report stops at diagnosis + suggested fix and does **not** touch the code.
- The in-repo caller `command_service._hydrate_components` (`command_service.py:104`) is defended: it passes `Component.from_dict(dict(item))`, creating a shallow copy before the destructive call. After Fix A or B, the shallow copy at the call site becomes unnecessary but harmless.
- `Component.from_dict` is the canonical wire-format deserialization entry point (per the filed `component-get-type-by-name-no-recurse` report). Any new callers — DSL layers, CLI parsers, MCP integrations — will hit this footgun unless they independently discover the need to copy.
- `pydantic.BaseModel.model_validate` (the Pydantic v2 equivalent) does NOT mutate its input. `from_dict` is an archetype-specific extension that violates the convention established by the parent class's own validation API.
