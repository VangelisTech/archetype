---
applyTo: "src/archetype/core/**"
---

# Core Module Review Guidelines

This is the most sensitive part of the codebase — human-curated ECS primitives. Changes require extra scrutiny.

## Key Invariants

- Components must produce deterministic prefixes via `get_prefix()`. Changing prefix logic breaks all existing stored data.
- `AsyncProcessor.process()` must be a pure DataFrame transform — no side effects, no `.collect()`, returns a new DataFrame.
- `Resources` is a type-keyed DI container. `require(T)` raises `KeyError` if missing; `get(T)` returns `None`. Never swallow `require()` errors silently.
- Tick lifecycle order is strict: pre_tick -> process archetypes (parallel) -> persist -> update snapshots -> increment tick -> post_tick. Do not reorder.
- All data must be Arrow-serializable for LanceDB. No `dict`, `list[dict]`, or custom objects without JSON encoding to `str`.

## What to Flag

- Any change to Component prefix logic or `to_row_dict()`
- `.collect()` calls inside processors
- Side effects in `process()` methods
- Changes to tick lifecycle ordering in `async_world.py` or `async_system.py`
- New dependencies added to core (it should stay minimal)
