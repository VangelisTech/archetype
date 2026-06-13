# Rust Core HTN Plan

**Document type:** Implementation plan.
**Scope:** `crates/`, `src/archetype/core/aio/`, and the Python adapter layer that
will bridge them.

This plan describes the migration from the current Python async core prototype
to a Rust core engine built on `arrow-rs`, `tokio`, the Arrow C Data Interface,
and an append-only Parquet store. It is an HTN-style plan: each phase decomposes
into ordered tasks, explicit products, acceptance checks, and dependencies.

## Current Definition of Done

The next feature is not "rewrite Archetype in Rust." The feature is:

> A Rust Arrow/Parquet tick kernel, callable from the existing Python runtime
> boundary, passing core parity tests against the Python async world, with
> benchmarks showing reduced per-tick overhead without taking over app,
> runtime, auth, audit, or broker semantics.

### Rust Owns

- Arrow schema composition after Python supplies component schemas.
- Archetype table descriptors after Python supplies table names.
- Spawn, despawn, update, component migration, and tick materialization.
- Active live snapshots below the service layer.
- Processor scheduling for native processors.
- Local append-only Parquet storage for the benchmark/control backend.
- Arrow C Data Interface import/export and ABI diagnostics.

### Python Owns

- `ArchetypeRuntime`, service container, API, CLI, and docs examples.
- RBAC, quotas, audit emission, command broker ordering, and command policy.
- Python component classes and table-name hashing.
- Python processors, hooks, resources, and object lifetimes.
- Daft/Iceberg/LanceDB production paths until a later storage boundary is
  explicitly accepted.

### Non-Goals for This Feature

- No Rust ownership of auth, audit, command routing, or runtime handles.
- No Iceberg catalog/transaction implementation.
- No LanceDB implementation.
- No DataFusion query planner unless parity proves simple Parquet scans are the
  bottleneck.
- No `cuTile-rs` integration inside the engine. GPU work is a processor backend
  experiment, not world-state ownership.
- No PyO3 requirement. Arrow C Data remains the native boundary unless packaging
  forces a separate decision.

## 0. Constraints

The following decisions are fixed for this migration:

- The async Python implementation is the behavioral reference.
- A component is an Arrow schema. Rust does not model Python component classes as
  the canonical component identity.
- Rust does not own table-name hashing. Table names are supplied by the caller,
  preserving the current Python naming policy until a separate policy change is
  made.
- Data crosses the native boundary through the Arrow C Data Interface.
- Lance storage is out of scope for the first Rust engine. The first native
  store is append-only Parquet.
- Python remains the runtime, API, CLI, auth, audit, and beginner-facing surface.
- `cuTile-rs` is not part of the core engine plan. It remains a possible future
  processor accelerator for dense numeric component columns.

Daft's native extension authoring guide is the reference for the boundary shape:
use `ArrowSchema` and `ArrowArray` as the stable ABI, and convert into
`arrow-rs` types inside Rust.

## 1. Target Architecture

```text
Python runtime / app / API / CLI
        |
        | Arrow C Data Interface
        v
crates/archetype-ffi
        |
        v
crates/archetype-core
  - Arrow schema composition
  - world state machine
  - mutation buffers
  - tick materialization
  - query/update/store traits
        |
        v
crates/archetype-parquet
  - append-only local Parquet store
```

The Python service layer continues to enforce governance and audit. Rust owns the
engine invariants below the service layer.

## 2. HTN Root Task

**Task:** make Rust the canonical implementation of Archetype's core engine
semantics without breaking the Python public surface.

The root decomposes into these phases:

1. Preserve and name current async semantics.
2. Add the Rust workspace and core crates.
3. Implement Arrow-native schema and world primitives.
4. Implement append-only Parquet storage.
5. Add the Arrow C Data Interface boundary.
6. Bridge Python to Rust behind the existing async core API.
7. Migrate runtime/service paths incrementally.
8. Retire duplicated Python core semantics.
9. Explore optional native processor acceleration.

## 2.1 Execution HTN

The executable task network is intentionally narrower than the long-term
migration. Each leaf has a concrete artifact and can run when its dependencies
are satisfied.

```text
R0 Define done for Rust backend
├── R0.1 Record ownership boundary
├── R0.2 Record non-goals
└── R0.3 Record crate split

R1 Harden native processor ABI
├── R1.1 Add ABI error diagnostics
├── R1.2 Add Python adapter error surfacing
├── R1.3 Add negative Arrow C tests
└── R1.4 Document build/load path

R2 Move movement into shared Rust kernel
├── R2.1 Add movement processor module in archetype-core
├── R2.2 Reuse it from archetype-ffi
├── R2.3 Reuse it from archetype-bench
└── R2.4 Keep benchmark output stable

R3 Establish parity harness
├── R3.1 Mark Python async tests that define engine semantics
├── R3.2 Add Rust parity tests for materialization edge cases
├── R3.3 Add native-mode Python tests through the service boundary
└── R3.4 Keep native mode opt-in

R4 Benchmark the hot loop
├── R4.1 Validate final DataFrames match by backend
├── R4.2 Split timing into plan/query/process/persist phases
├── R4.3 Compare Daft live-read, Daft cached-read, and Rust Parquet
└── R4.4 Publish reproducible JSON fixtures

R5 Decide storage graduation
├── R5.1 Stay local Parquet if tick-loop overhead is already solved
├── R5.2 Add object_store only for remote/local abstraction
├── R5.3 Add DataFusion only for Rust-owned query planning
└── R5.4 Add Iceberg only for Rust-owned table transactions
```

### Parallel Leaves

- `R0` can run immediately and is documentation-only.
- `R1.3` can run in parallel with `R2` because adapter tests define behavior
  without changing the Rust kernel shape.
- `R2.1` through `R2.3` are sequential inside one Rust ownership lane.
- `R3` depends on `R1` and `R2` because parity tests need the hardened ABI and
  shared movement kernel.
- `R4` depends on `R3.1` for correctness checks, but timing collection can
  evolve in parallel after the benchmark schema is stable.
- `R5` is a decision gate only. It must not add crates before `R4` shows the
  actual bottleneck.

### Crate Admission Rules

The current crates stay minimal:

| Crate | Responsibility |
|---|---|
| `archetype-core` | Engine invariants, Arrow schemas, materialization, processor traits |
| `archetype-parquet` | Local append-only Parquet `Store` implementation |
| `archetype-ffi` | C ABI and Arrow C Data Interface |
| `archetype-bench` | Benchmark binaries only |

New crates require a concrete boundary:

| Candidate crate | Admission trigger |
|---|---|
| `archetype-object-store` | Local filesystem is no longer enough for storage tests or deployment |
| `archetype-datafusion` | Rust must own query planning, expressions, or predicate pushdown |
| `archetype-iceberg` | Rust must own Iceberg commits, catalogs, snapshots, or transactions |
| `archetype-pyo3` | C ABI is insufficient for packaging or lifecycle management |

Dependencies should not leak upward into `archetype-core`; the core crate should
stay free of storage engine, Python, and catalog policy.

### Execution Status

| Node | Status | Notes |
|---|---|---|
| `R0` | Done | The ownership boundary, non-goals, crate split, and definition of done are recorded in this document. |
| `R1` | Done for movement ABI | The FFI boundary reports thread-local last errors. The dedicated movement ABI now treats missing required columns as errors. |
| `R2` | Done | Movement is implemented once in `archetype-core` and reused from FFI and benchmark paths. |
| `R3` | Kernel done, service bridge pending | Rust core parity tests cover spawn, despawn, reserved IDs, metadata, live snapshots, filters, and component-table migration. Python service native-mode parity remains pending because the runtime adapter is not implemented yet. |
| `R4` | Done for movement envelope | Movement benchmark records correctness for both backends. Rust reports setup, read-prior, materialize, process, append, live-snapshot, profiled tick, query, and total phases; Python reports the existing setup/run/query phases. |
| `R5` | Done as decision gate | No new storage/query crates are admitted yet. Local Parquet remains the control backend until benchmarks prove a storage/planner bottleneck. |

### Decisions Made During Execution

- Dedicated processor ABI functions are strict. `arct_movement_process` validates
  its required columns before scheduling through `NativeSystem`; missing columns
  are caller errors, not silent skips.
- Generic system scheduling remains permissive. A processor registered with
  `NativeSystem` still skips batches that do not contain its required columns,
  matching the archetype-subset scheduling model.
- ABI diagnostics are thread-local strings exposed through
  `arct_last_error_message()`. The primary ABI still returns integer status
  codes so C callers stay simple.
- Benchmarks now carry correctness fields. Timing claims are not considered valid
  unless the final row count and final position sums match the expected movement
  model.
- Rust world profiling belongs in the core executor, not in benchmark-only
  wrappers. `step_profiled()` keeps `step()` behavior intact while exposing the
  phase timings needed to analyze the hot loop.
- Component migration is represented as a caller-supplied new table plus
  caller-supplied component batch. Rust preserves entity IDs and materializes the
  old table tombstone and new table active row; Python still owns deciding table
  names and component schemas.
- No `object_store`, DataFusion, Iceberg, PyO3, or `cuTile-rs` crate was added.
  The current bottleneck must be demonstrated before widening dependencies.

## 3. Phase 1: Preserve Current Async Semantics

**Intent:** prevent the rewrite from changing behavior accidentally.

### Tasks

1. Inventory the async core contracts from `src/archetype/core/aio/`.
2. Add contract tests for any behavior currently covered only implicitly.
3. Mark sync/async divergences as migration risks, not Rust requirements.
4. Decide which current behaviors are bugs before porting them.

### Required Contracts

- `AsyncWorld.step()` lifecycle:
  pre-tick hook, query previous tick, materialize mutations, execute processors,
  persist, increment tick, post-tick hook.
- `active_signatures` is the union of registered entity signatures, pending spawn
  tables, and pending despawn tables.
- Spawn materialization deduplicates same-entity rows with last-write-wins.
- Despawn materialization marks previous rows inactive; it does not delete rows.
- `update_entity` overlays values without changing archetype/table identity.
- `add_components` and `remove_components` migrate an entity between tables by
  appending an inactive row to the old table and an active row to the new table.
- Store writes are append-only.
- Persistence failures must become observable. The current Python updater logs
  and returns a stamped DataFrame; Rust must return an error.

### Acceptance

- A contract matrix exists mapping Python tests to Rust test cases.
- Known semantic divergences are documented with a decision: preserve, fix, or
  defer.
- The Phase-6 entry gate suite `crates/archetype-core/tests/migration_gate.rs`
  passes.  That suite is the normative contract matrix: each test cites its
  Python counterpart and gaps are marked `#[ignore]` with rationale.

## 4. Phase 2: Rust Workspace

**Intent:** introduce Rust without changing Python behavior.

### Tasks

1. Add a root `Cargo.toml` workspace.
2. Add `crates/archetype-core`.
3. Add `crates/archetype-parquet`.
4. Add `crates/archetype-ffi`.
5. Add `cargo test --workspace` to local validation docs and eventually CI.

### Crate Boundaries

| Crate | Responsibility |
|---|---|
| `archetype-core` | Arrow schemas, world state, mutation materialization, traits |
| `archetype-parquet` | Append-only local Parquet `Store` implementation |
| `archetype-ffi` | C ABI over Arrow C Data Interface |

### Acceptance

- `cargo test --workspace` passes.
- Python tests still pass without importing Rust.

## 5. Phase 3: Arrow-Native Core

**Intent:** move the actual engine semantics into Rust.

### Tasks

1. Define base columns:
   `world_id`, `run_id`, `entity_id`, `tick`, `is_active`.
2. Define component schemas as caller-provided Arrow schemas.
3. Define archetype table descriptors as caller-provided table name plus Arrow
   schema.
4. Validate no missing required base columns after composition.
5. Implement `WorldState`.
6. Implement `MutationBuffer`.
7. Implement spawn queueing from Arrow batches.
8. Implement despawn queueing.
9. Implement materialization over prior tick Arrow batches.
10. Return explicit errors for schema mismatch, missing columns, invalid types,
    and failed appends.

### Non-Goals

- No table-name hashing.
- No Python class lookup.
- No LanceDB.
- No Daft processor execution in Rust.

### Acceptance

- Rust tests cover spawn, duplicate spawn overwrite, despawn, empty prior batch,
  and metadata stamping.
- Rust materialization returns Arrow `RecordBatch` values that Python can ingest
  through Arrow C.

## 6. Phase 4: Append-Only Parquet Store

**Intent:** provide a simple native durable backend for the Rust core.

### Layout

```text
<root>/<namespace>/<table_name>/part-<uuid>.parquet
```

### Tasks

1. Implement `Store::append`.
2. Implement `Store::read_table`.
3. Implement filter application for `world_id`, `run_id`, `tick`, `entity_id`,
   and `active_only`.
4. Keep writes append-only; never rewrite existing files.
5. Defer predicate pushdown until correctness is stable.

### Acceptance

- Appending twice produces two files.
- Reading a table returns the concatenation of all parts.
- Filters match the Python async store semantics.

## 7. Phase 5: Arrow C Data Interface

**Intent:** make the native boundary stable and Daft-compatible.

### Tasks

1. In `archetype-ffi`, define exported C ABI functions over `ArrowSchema` and
   `ArrowArray`.
2. Convert `ArrowSchema`/`ArrowArray` to `arrow-rs` `SchemaRef` and
   `RecordBatch`.
3. Convert returned Rust `RecordBatch` values back to `ArrowArray`.
4. Define ownership rules for every pointer crossing the boundary.
5. Add a memory-safety test harness.

Phase 5's split-step ownership, error-code, panic, and partial-failure contract
is specified in `docs/design/split-step-ffi.md`.

### Acceptance

- A caller can pass an Arrow batch into Rust and receive an Arrow batch back
  without Python object serialization.
- FFI functions never panic across the boundary.

## 8. Phase 6: Python Adapter

**Intent:** route existing Python async core operations through Rust while
preserving Python APIs.

The normative ABI for this phase is specified in
[`docs/design/split-step-ffi.md`](../design/split-step-ffi.md): Rust owns
materialization, stamping, persistence, and live snapshots; Python processors
execute between `arct_step_begin` and `arct_step_commit`.

### Tasks

1. Add an internal adapter under `src/archetype/core/native/`.
2. Convert Daft/PyArrow batches to Arrow C.
3. Call `archetype-ffi`.
4. Convert returned Arrow C data back into PyArrow/Daft.
5. Gate native engine usage behind an explicit feature flag or config setting.

Phase 6's adapter shape, native fallback behavior, and parity/benchmark gate are
specified in `docs/design/split-step-ffi.md`.

### Phase-6 Entry Gate

Before any Phase-6 Python-adapter work begins, the migration-gate suite
`crates/archetype-core/tests/migration_gate.rs` must pass in full.  The suite
encodes every behavioral contract the Python async reference pins:

| Contract | Test | Python counterpart |
|---|---|---|
| x₀ raw at spawn tick / f next tick (first spawn) | `contract1a_x0_raw_at_spawn_tick_f_applied_next_tick` | `test_initial_conditions_persist_at_spawn_tick` |
| x₀ raw at spawn tick / f next tick (mid-run) | `contract1b_mid_run_spawn_lands_raw_older_entities_keep_advancing` | `test_mid_run_spawn_persists_initial_conditions` |
| Same-tick spawn-cancel: no tombstone, no active row | `contract2_same_tick_spawn_cancel_no_tombstone_no_active_row` | `AsyncWorld.remove_entity` same-tick cancel semantics |
| Spawn order deterministic (first-seen, last-write-wins) | `contract3_spawn_order_deterministic_first_seen_last_write_wins` | `test_duplicate_spawn_same_entity_overwrites` |
| Despawn tombstone at current tick; active_only excludes it | `contract4_despawn_tombstone_at_current_tick_active_only_excludes_it` | `despawn_marks_prior_row_inactive` / `live_snapshot_after_despawn_excludes_inactive_entities` |
| Update/overlay semantics | `contract5_update_overlay_semantics_gap` (**`#[ignore]`** — not yet implemented) | `AsyncWorld.update_entity` |
| Metadata stamping (world/run/tick) on every row | `contract6_metadata_stamped_on_every_persisted_row` | `async_world_persists_world_run_and_tick_metadata` |

The `archetype-bench` crate also includes a `mid_run_spawn` scenario that
exercises the tick-zero-correct spawn ordering for entities arriving at
different ticks.  Its correctness output (`"correct": true`) must hold before
Phase-6 work proceeds.

### Acceptance

- The Phase-6 entry gate suite (`crates/archetype-core/tests/migration_gate.rs`)
  passes: `cargo test --package archetype-core --test migration_gate` is green.
- `cargo run --package archetype-bench --bin mid_run_spawn` reports `"correct": true`.
- Existing async world tests pass with native mode off.
- A narrow native-mode test passes for spawn/materialize/update.
- Failures surface as Python exceptions, not logged-only errors.

## 9. Phase 7: Incremental Migration

**Intent:** migrate behavior in dependency order.

### Order

1. Schema composition and metadata validation.
2. Spawn/despawn materialization.
3. Update materialization.
4. Add/remove component migration.
5. Query filtering.
6. Parquet append/read.
7. Runtime integration.

Each step must leave the Python public surface stable.

### Acceptance

- At each migration point, Python and Rust contract tests both pass.
- The app/runtime layers still call through `CommandService`.

## 10. Phase 8: Retirement

**Intent:** remove duplicated Python semantics only after Rust parity is proven.

### Tasks

1. Replace Python async core state-machine logic with native calls.
2. Keep Python processors and hooks as adapter-level features.
3. Remove or freeze sync-core duplication.
4. Keep Python fallback behind a temporary compatibility switch until release.

### Acceptance

- No semantic drift remains between sync and async implementations because there
  is only one native engine path.
- Rust is the normative core implementation.

## 11. Phase 9: Optional Processor Acceleration

**Intent:** leave room for GPU work without contaminating core storage/state.

### Candidate Work

- Native Rust processors over Arrow arrays.
- Dense tensor component views for numeric columns.
- `cuTile-rs` experiments for fused numeric processors.

### Rule

GPU acceleration must be an optional processor backend. It must not own world
state, append-only history, table naming, or governance.

## 12. First Milestone

The first implementation milestone was:

> Given a caller-supplied table name, an Arrow schema, a prior tick Arrow batch,
> and queued spawn/despawn mutations, Rust produces the next tick Arrow batch and
> can append/read it through an append-only Parquet store.

This proves the kernel before touching Python runtime behavior.

The next implementation milestone is:

> Given a single Arrow batch from Python, Rust executes the shared movement
> processor through the Arrow C Data Interface with stable errors, then the same
> processor is reused by the Rust benchmark world.

Acceptance:

- `cargo test --workspace` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `uv run pytest tests/core/test_native_movement_adapter.py
  tests/bench/test_movement_compare.py` passes.
- Missing columns, wrong dtypes, multi-batch tables, missing libraries, and ABI
  mismatch produce explicit Python failures.
- The benchmark JSON schema stays stable.
