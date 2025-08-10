# Archetype Tracy Profiling Plan

This folder contains an opt-in profiling layer for Tracy that keeps core classes clean via instrumented subclasses and a tiny shim. When disabled, overhead is near-zero.

## Goals
- No clutter in core (`AsyncWorld`, `AsyncSystem`).
- Opt-in via `ARCT_TRACY=1` environment variable.
- Stable zone names and per-tick frame marks.
- Avoid expensive metrics (no unintended materialization).

## Toggle
- Set `ARCT_TRACY=1` to enable instrumentation.
- Shim lives in `shim.py` and exposes:
  - `zone(name)`: context manager for zones
  - `frame_mark(name=None)`
  - `message(text)`
  - `plot(name, value)`
  - `set_thread_name(name)`

## Instrumented subclasses
We provide wrappers that override hot entry points and call `super()` inside Tracy zones:

- `InstrumentedAsyncWorld(AsyncWorld)`
  - `run`: set thread name; zone "world.run".
  - `step`: frame mark per tick; zone "world.step"; optional cheap plots.
  - `_run_archetype`: zone `archetype[{sig}]`.
  - `_materialize_mutations`: zone "world.materialize" with nested zones for despawn/spawn sections.

- `InstrumentedAsyncSystem(AsyncSystem)`
  - `execute`: outer zone per signature; inner per-processor zones `proc[{Name}]`.

Factory selects instrumented classes when enabled.

## Initial insertion points
- Frame marks:
  - `AsyncWorld.step`: `frame_mark(f"tick:{self.tick}")` at start.

- Zones:
  - `world.run`, `world.step`, `archetype[{sig}]`, `world.materialize`
  - `system.execute[{sig}]`, `proc[{ProcessorName}]`

## Cheap plots (optional)
- `active_signatures` per step (length of set only).

## Non-goals for v1
- No store/querier/updater instrumentation (can be added later).
- No FastAPI/Ray middleware in v1 (can be added later).

## Integration
- `WorldFactory` chooses instrumented classes if `ARCT_TRACY=1`.
- No other call sites change.

## Safety
- Avoids data materialization purely for metrics.
- All overrides call `super()` to preserve behavior.
