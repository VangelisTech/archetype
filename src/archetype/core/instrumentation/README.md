# Archetype Profiling and Observability (VizTracer + Shims)

This module provides an opt-in profiling layer (VizTracer) and lightweight validation/logging shims. Instrumentation is separated from core classes; when disabled, overhead is near-zero.

## Goals

- Keep core clean (`AsyncWorld`, `AsyncSystem`).
- Opt-in via environment variable.
- Stable, meaningful zones and per-tick frame marks.
- Avoid expensive metrics (no unintended materialization).

## Toggle (Profiling)

- Set `ARCT_VIZTRACER=1` to enable profiling.
- The profiling shim (`profiling_shim.py`) exposes:
  - `zone(name)`: context manager for pseudo-zones
  - `frame_mark(name=None)`
  - `message(text)`
  - `plot(name, value)`

These map to VizTracer instant events so you can see timelines in the VizTracer UI.

## Instrumented subclasses

Wrappers override hot entry points and call `super()` inside zones:

- `InstrumentedAsyncWorld(AsyncWorld)`
  - `run`: zone `world.run`.
  - `step`: frame mark per tick; zone `world.step`.
  - `_run_archetype`: zone `archetype[{sig}]`.
  - `_materialize_mutations`: zone `world.materialize` with nested despawn/spawn.

- `InstrumentedAsyncSystem(AsyncSystem)`
  - `execute`: zone `system.execute[{sig}]`; inner per-processor zones `proc[{Name}]`.

The `WorldFactory` selects instrumented classes automatically when profiling is enabled.

## Usage

- CLI tracing of benchmarks:

  ```bash
  cd archetype
  PYTHONPATH=src ARCT_VIZTRACER=1 uv run -q viztracer \
    --tracer_entries 8000000 \
    --include_files 'archetype/src/archetype/.*' \
    -o trace.html -m archetype.benchmarks.run
  vizviewer trace.html
  ```

- Programmatic tracing (example):

  ```python
  from viztracer import VizTracer
  with VizTracer(output_file='trace.html', tracer_entries=8_000_000):
      await world.run(run_config)
  ```

Tips:

- Increase `--tracer_entries` for large runs; use `--include_files`/`--min_duration` to filter noise.

## Validation shim

- File: `validation_shim.py`
- Functions: `validate_materialized`, `validate_pre_update`, `validate_post_update`.
- Purpose: cheap invariants (required columns, duplicate detection) without materializing heavy data.
- Enable via `RunConfig(enable_validation=True)` or `RunConfig.validate(...)` (used by instrumented world/system when toggled).

## Logging shim

- File: `logging_shim.py`
- Features:
  - Structured log lines via `log_event`/`dbg`/`dbg_sig`.
  - Optional rich TTY output and a lightweight live dashboard (requires `rich`).
  - Per-world file logging to `ARCT_LOG_DIR` (default `.archetype_logs`).
- Enable richer debug outputs via `RunConfig(debug=True, show_rows=..., explain=...)`.

## Safety

- Avoids data materialization purely for metrics.
- All overrides call `super()` to preserve behavior.
