# Split-Step FFI ABI for Python Processors

**Status:** design for Phase 6.
**Scope:** `crates/archetype-ffi`, `crates/archetype-core`, and the future
Python adapter under `src/archetype/core/native`.

This document defines the C ABI that lets the Rust kernel own tick
materialization, stamping, persistence, and live snapshots while Python still
executes Python processors. It is intentionally a split-step ABI:

1. Rust exports one materialized table batch.
2. Python runs the processor chain.
3. Python returns the processed batch to Rust.
4. Rust finishes the table commit.

The design is constrained by the existing Arrow C Data Interface helpers in
`crates/archetype-ffi/src/lib.rs`, the Rust world state in
`crates/archetype-core/src/aio/async_world.rs`, and the Python tick contract in
`src/archetype/core/aio/async_world.py`.

## 1. Call Sequence Per Tick Table

The split-step ABI is table-scoped, but Rust owns the tick epoch. A tick epoch
starts lazily on the first successful `arct_step_begin` for the world's current
tick. Rust freezes the active table set for that epoch from its own world state,
using the same model as `WorldState::active_tables()` in
`crates/archetype-core/src/aio/async_world.rs`. The Python adapter must call
begin and then commit or abort for every table it chooses to process in that
epoch.

The table ABI is:

- `arct_step_begin(world, table, out_array, out_schema) -> status`
- Python imports the exported Arrow C Data as a `pyarrow.RecordBatch`, converts
  to the existing DataFrame representation, and runs the Python processor chain.
- `arct_step_commit(world, table, processed_array, processed_schema) -> status`
- `arct_step_abort(world, table) -> status` on the failure path after a
  successful begin.

`world` is an opaque native world handle. `table` is a borrowed, null-terminated
UTF-8 table-name string for the duration of the call. The table name maps to an
`ArchetypeTable` already registered with the world. This preserves the current
plan in `docs/guide/rust-core-plan.md`: Python owns component classes and table
name hashing; Rust owns table descriptors after Python supplies names and Arrow
schemas.

### Begin

`arct_step_begin` performs the Rust-owned part of the pre-processor lifecycle:

1. Validate that the world handle and table name are valid.
2. If no tick epoch is active, freeze the current tick number and active table
   set.
3. Reject the call if the table is not active for the frozen epoch.
4. Read the prior active table batch from Rust live state or the native store,
   matching `AsyncWorld._run_archetype`'s previous-state read.
5. Apply staged despawns for this table.
6. Do **not** concatenate staged spawns.
7. Export the resulting materialized batch through Arrow C Data.
8. Mark the table `in_flight`.

The exported batch is the processor input batch. It includes prior rows with
despawns applied, but it excludes raw spawn/reset rows staged for the same tick.

### Initial Conditions Placement

Spawn and reset rows belong after Python processor execution, in
`arct_step_commit`, not in `arct_step_begin`.

The initial-conditions contract from the Rust HTN lane is:

- spawn/reset values land raw at their materialization tick;
- processors first apply to those rows on the following tick;
- the live snapshot after the materialization tick includes the raw rows.

If `arct_step_begin` concatenated spawns before Python processors, a processor
would observe the new row during the same tick and could transform `x0` into
`f(x0)` before persistence. That would erase the raw initial condition from the
ledger. The same problem appears for reset-style rows that deliberately set a
known baseline state.

Therefore the Phase 6 split is:

- despawns are applied before processors, because removed entities should not
  remain active through the tick's processor input;
- raw spawns are concatenated after processors, because new entities should be
  visible in the tick's committed/live output but should not receive processor
  effects until the next tick.

This intentionally differs from the current one-piece
`WorldState::materialize_table()` implementation in
`crates/archetype-core/src/aio/async_world.rs`, which consumes despawns and
spawns together before native processing. Phase 6 must split that operation into
two internal Rust operations:

- `materialize_table_for_processors`: read prior state and apply despawns only;
- `commit_processed_table`: validate processor output, concatenate deduplicated
  raw spawns, stamp metadata, persist, and refresh the live snapshot.

### Commit

`arct_step_commit` consumes the processed Arrow batch from Python and performs
the Rust-owned post-processor lifecycle:

1. Validate the world, table, and tick epoch.
2. Move the processed Arrow C Data into a Rust `RecordBatch`.
3. Validate that the processed batch schema matches the registered table schema.
4. Concatenate staged raw spawns for the table, deduplicated last-write-wins by
   `entity_id`, matching the existing dedupe contract in
   `AsyncWorld.materialize_mutations`.
5. Stamp `tick`, `world_id`, and `run_id` over the full committed batch.
6. Persist non-empty batches through the native append-only store.
7. Refresh the Rust live snapshot for this table to active rows only.
8. Consume the table's staged despawn and spawn buffers.
9. Mark the table `committed`.
10. If every table in the frozen epoch is committed, advance the world tick once
    and close the epoch.

Staging raw spawns until commit means the ledger for tick `N` contains:

- processed prior rows for table `T`;
- inactive rows for despawns applied at tick `N`;
- raw active rows for spawns/resets staged at tick `N`.

At tick `N + 1`, those raw rows are part of the prior/live input exported by
`arct_step_begin`, so Python processors apply to them for the first time.

### Abort

`arct_step_abort` is the cleanup path after `arct_step_begin` succeeds and
Python cannot produce a processed batch for the table. It:

1. Validates the world and table.
2. Restores any table-local mutation buffers moved into the in-flight state by
   begin.
3. Marks the frozen tick epoch failed.
4. Releases Rust in-flight state for the table.
5. Does not advance the tick.
6. Does not release any Arrow C Data owned by Python.

Abort is table-scoped. It cannot roll back rows already appended by successful
commits for other tables in the same tick epoch.

## 2. Arrow C Data Ownership And Move Rules

The ABI follows the existing helpers in `crates/archetype-ffi/src/lib.rs`:

- `record_batch_from_raw(array, schema)` consumes caller-provided
  `FFI_ArrowArray` and `FFI_ArrowSchema` values and converts them into a Rust
  `RecordBatch`.
- `record_batch_into_raw(batch, out_array, out_schema)` writes a Rust
  `RecordBatch` into caller-provided writable Arrow C Data structs.
- Record batches cross as top-level Arrow `StructArray` values.
- On move, the input C structs are left released/empty. The caller must treat
  `release == NULL` as "already moved".

The null-on-move convention is mandatory for Phase 6. After Rust consumes an
Arrow C Data value, both the `ArrowArray.release` and `ArrowSchema.release`
callbacks must be null. Python cleanup helpers may call release in `finally`,
but they must first check that the pointer is not null and the release callback
is not null. This is the same defensive cleanup shape as the current adapter
prototype under `src/archetype/core/native/arrow_c.py` in the Rust-core working
tree.

### Begin Output Ownership

`arct_step_begin(world, table, out_array, out_schema)` exports Rust-owned batch
data to Python.

Caller responsibilities:

- Allocate the `ArrowArray` and `ArrowSchema` struct shells.
- Pass writable pointers to those shells.
- The shells must be empty on entry. If either shell already has a non-null
  release callback, Rust must return `ARCT_ERR_OUTPUT_NOT_EMPTY` rather than
  overwrite a live value.
- On success, import the exported Arrow C Data into PyArrow or release it
  exactly once through the standard Arrow release callback.

Rust responsibilities:

- Validate that `out_array` and `out_schema` are non-null.
- Export with the same semantics as `record_batch_into_raw`.
- Transfer ownership of the exported Arrow C Data to the caller on success.
- Leave both output structs empty on error.
- Never release the exported batch after a successful return.

After PyArrow imports the begin output, ownership has moved to PyArrow. The
adapter should either set its C Data variables to a moved state or call the
checked release helpers, which become no-ops when `release == NULL`.

### Commit Input Ownership

`arct_step_commit(world, table, processed_array, processed_schema)` imports
Python-owned processed data into Rust.

Caller responsibilities before the call:

- Export the processed Python batch to Arrow C Data as one top-level
  `StructArray`.
- Pass mutable pointers to the exported `ArrowArray` and `ArrowSchema`.
- Treat the data as moved if Rust nulls both release callbacks, regardless of
  the returned status.
- If Rust returns before moving the data and release callbacks remain non-null,
  release them exactly once.

Rust responsibilities:

- Validate the pointer shape before import.
- Move the Arrow C Data with the same semantics as `record_batch_from_raw`.
- Null the release callbacks on successful move.
- Drop the imported Rust `RecordBatch` after commit processing.
- Never retain borrowed pointers into Python-owned C Data after return.

The adapter must not assume that non-zero status means the input was not moved.
It must inspect the release callbacks or use checked release helpers. This
prevents double-free on schema-validation or storage errors that occur after
Rust has imported the batch.

### Abort Ownership

`arct_step_abort(world, table)` has no Arrow C Data parameters. It does not own,
release, or inspect any begin output that Python may still hold. Python is
responsible for dropping the `pyarrow.RecordBatch` or releasing unimported C
Data in its own failure path.

### Double-Free Prevention Rules

Every exported split-step function must follow these rules:

- Never overwrite a non-empty output `ArrowArray` or `ArrowSchema`.
- Never import the same input pair twice.
- Always clear moved inputs through the Arrow C Data release/null convention.
- Always leave output structs empty on failure.
- Python cleanup always checks `release != NULL` before calling release.
- Python sets local C Data variables to moved/none after successful PyArrow
  import when possible.
- Rust does not store raw Arrow C pointers in world state. World state stores
  only safe Rust `RecordBatch` values or internal Arrow arrays.

## 3. Error And Panic Code Space

The current FFI returns `0` for success, `1` for Arrow errors, and `2` for
panics. Phase 6 keeps those values stable and reserves ranges for split-step
world errors.

| Code or range | Meaning |
|---:|---|
| `0` | `ARCT_OK`: success; `LAST_ERROR` is cleared. |
| `1` | `ARCT_ERR_ARROW`: Arrow C Data import/export, Arrow schema, or Arrow compute error. |
| `2` | `ARCT_ERR_PANIC`: Rust panic caught at the FFI boundary. |
| `3` | `ARCT_ERR_NULL_POINTER`: null world, table, array, or schema pointer. |
| `4` | `ARCT_ERR_OUTPUT_NOT_EMPTY`: output Arrow C Data shell has a live release callback on entry. |
| `5` | `ARCT_ERR_INVALID_UTF8`: table name or other C string is not valid UTF-8. |
| `6` | `ARCT_ERR_BAD_HANDLE`: opaque world handle is invalid, freed, or wrong type. |
| `7` | `ARCT_ERR_ABI_VERSION`: Python requested a split-step ABI not provided by this library. |
| `8` to `99` | Reserved for common FFI errors shared across exported functions. |
| `100` to `149` | World lifecycle and tick-epoch errors. |
| `150` to `199` | Table registration and table-state errors. |
| `200` to `249` | Native store read/append errors. |
| `250` to `299` | Native mutation-buffer errors. |
| `300` to `349` | Processed-batch contract errors, including schema mismatch and invalid metadata columns. |
| `350` to `399` | Threading and runtime-boundary errors. |
| `400` to `999` | Reserved for future stable ABI errors. |

Concrete split-step codes in the reserved ranges:

| Code | Meaning |
|---:|---|
| `100` | `ARCT_ERR_STEP_ALREADY_IN_FLIGHT`: same table already has an in-flight begin. |
| `101` | `ARCT_ERR_NO_STEP_IN_FLIGHT`: commit or abort called without a matching begin. |
| `102` | `ARCT_ERR_STEP_TABLE_MISMATCH`: table does not belong to the frozen tick epoch. |
| `103` | `ARCT_ERR_PARTIAL_TICK`: world is in a failed partial-tick state. |
| `104` | `ARCT_ERR_MUTATION_DURING_STEP`: caller tried to mutate world state while a tick epoch is open. |
| `150` | `ARCT_ERR_UNKNOWN_TABLE`: table is not registered with this world. |
| `151` | `ARCT_ERR_TABLE_NOT_ACTIVE`: table is registered but not active in the frozen epoch. |
| `200` | `ARCT_ERR_STORE_READ`: prior-state read failed. |
| `201` | `ARCT_ERR_STORE_APPEND`: append failed or durability is unknown. |
| `250` | `ARCT_ERR_BUFFER_STATE`: mutation buffer could not be moved, restored, or consumed consistently. |
| `300` | `ARCT_ERR_SCHEMA_MISMATCH`: processed batch schema does not match the registered table schema. |
| `301` | `ARCT_ERR_METADATA_COLUMN`: processed batch has invalid base metadata columns. |
| `350` | `ARCT_ERR_RUNTIME_REENTRANT`: synchronous FFI tried to block on an incompatible Tokio runtime context. |
| `351` | `ARCT_ERR_WORLD_BUSY`: world lock could not be acquired without violating the reentrancy rules. |

### Panic Policy

All exported split-step functions must wrap their bodies in
`catch_unwind(AssertUnwindSafe(...))`, matching
`arct_record_batch_roundtrip` and `arct_movement_process` in
`crates/archetype-ffi/src/lib.rs`.

For supported Python wheels and local development builds, `archetype-ffi` must
compile with unwinding panics at the FFI boundary. If the crate is compiled with
`panic = "abort"`, Rust cannot surface a panic as an error code; the process
will abort before `LAST_ERROR` can be set. That build mode is not acceptable for
the Phase 6 Python adapter. CI should include a panic harness that proves a
test-only panicking FFI function returns `ARCT_ERR_PANIC` instead of aborting.

### LAST_ERROR Plumbing

`LAST_ERROR` remains thread-local, as implemented in
`crates/archetype-ffi/src/lib.rs`.

Rules for split-step functions:

- `arct_step_begin`, `arct_step_commit`, and `arct_step_abort` clear
  `LAST_ERROR` on entry.
- On `ARCT_OK`, they leave `LAST_ERROR` cleared.
- On non-zero status, they set `LAST_ERROR` to a sanitized UTF-8 diagnostic
  string with embedded NUL bytes replaced.
- Panic status sets `LAST_ERROR` to a stable message beginning with
  `panic crossing Archetype FFI boundary`.
- Diagnostics should include the world id when available, table name, tick, and
  current table state.

`arct_last_error_message()` returns a borrowed pointer valid until the next
Archetype FFI call on the same thread. Callers must not free it.
`arct_clear_last_error()` clears the current thread's error. `arct_ffi_version()`
does not mutate `LAST_ERROR`.

## 4. Reentrancy And Threading Rules

The split-step functions are synchronous C ABI calls. Native store reads and
appends are async Rust operations behind that ABI, so the world handle must own
or reference a Tokio runtime.

### Tokio Boundary

The world handle stores an internal runtime handle created by the native layer.
`arct_step_begin` enters that runtime to read prior state. `arct_step_commit`
enters it to append committed rows. `arct_step_abort` is synchronous unless it
needs async cleanup in a future implementation.

The supported Phase 6 path is:

- Python calls synchronous FFI from a Python worker thread when the operation can
  block on Rust async I/O.
- Rust uses its own multi-thread Tokio runtime and `block_on` inside the FFI
  wrapper.
- The wrapper detects calls made from an incompatible Tokio runtime worker and
  returns `ARCT_ERR_RUNTIME_REENTRANT` rather than risking a nested-runtime
  panic.

The adapter should use `asyncio.to_thread` or an equivalent executor wrapper for
begin and commit when they can block. The Python processor chain remains async
and runs on the normal Python event loop after begin returns the batch.

### Table Concurrency

Concurrent `arct_step_begin` calls are allowed for different tables in the same
world and for any tables in different worlds.

Within one world:

- The first begin freezes the tick epoch.
- Different tables may begin concurrently; Rust serializes mutation-buffer and
  epoch-state updates with an internal world lock.
- The same table may not have more than one in-flight begin; a second begin
  returns `ARCT_ERR_STEP_ALREADY_IN_FLIGHT`.
- Commits may arrive in any order for in-flight tables.
- Mutations that change active tables or buffers are rejected while any tick
  epoch is open.

This preserves the per-archetype parallelism documented in
`docs/guide/system-execution.md` while making the world-state transitions
explicit and serialized.

### World Handle Lifecycle

The opaque world handle is valid across begin, commit, abort, and completed
ticks until the caller frees it through the world-handle lifecycle API. The
lifecycle API is outside this document, but split-step functions require these
handle semantics:

- A handle cannot be freed while a split-step call is executing.
- A handle cannot be freed while a table is in flight unless the free operation
  first aborts or marks the world failed.
- Between begin and commit/abort, the handle remains valid but the table is
  locked to that in-flight state.
- Across ticks, the same handle remains valid and carries the world tick,
  table registry, mutation buffers, live snapshots, and store handle.
- After a partial-tick failure, the handle remains valid for diagnostics and
  destruction, but native stepping returns `ARCT_ERR_PARTIAL_TICK` until a
  future recovery API is implemented.

## 5. Partial-Failure Semantics Mid-Tick

The append-only store is not transactional across tables. The design therefore
does not claim tick-level atomicity.

This aligns with the existing caveat implied by:

- `AsyncWorld.step()` in `src/archetype/core/aio/async_world.py`, which runs
  table tasks through `asyncio.gather(..., return_exceptions=True)`;
- each table task persists through `_run_archetype` before `step()` inspects all
  errors;
- `docs/guide/specification.md`, which states that store `append()` and
  `world.step()` are not idempotent and that processor failure continuation must
  be explicit;
- `docs/guide/system-execution.md`, which documents independent per-archetype
  execution.

### If Some Tables Commit And Others Abort

If `arct_step_commit` has returned `ARCT_OK` for tables `A` and `B`, and
`arct_step_abort` is then called for table `C` in the same tick epoch:

- Tables `A` and `B` have appended tick `N` rows to the native store.
- Tables `A` and `B` have consumed their staged despawn and spawn buffers.
- Tables `A` and `B` have refreshed their Rust live snapshots to active rows
  from their committed tick `N` batches.
- Table `C` has not appended rows for tick `N`.
- Table `C` has its staged buffers restored to the pre-begin state.
- Tables not yet begun retain their staged buffers.
- The world tick remains `N`; it does not advance to `N + 1`.
- The world enters `partial_failed` state.

After `partial_failed`, the adapter must surface an exception to the caller. It
must not retry native stepping on that handle, because retrying at tick `N`
would duplicate rows for already committed tables. The handle may still be used
for diagnostics, explicit destruction, or a future recovery API. Recovery is out
of scope for Phase 6.

This is weaker than full tick atomicity, but it is honest about the current
append-only storage model. Phase 6 tests must assert this behavior instead of
assuming rollback.

### Commit Failure

If `arct_step_commit` fails before any append is attempted, Rust restores the
table's in-flight buffers, marks the epoch failed, and leaves the table
uncommitted.

If `arct_step_commit` fails during or after append, durability may be unknown.
Rust returns `ARCT_ERR_STORE_APPEND`, marks the epoch failed, does not advance
the tick, and sets `LAST_ERROR` with enough detail for the adapter to report
that the world is not safe to retry.

## 6. Versioning

`arct_ffi_version()` currently returns `1` for the movement/roundtrip ABI in
`crates/archetype-ffi/src/lib.rs`. Split-step world ownership is a breaking ABI
addition because it introduces world handles, tick-epoch state, and stronger
pointer-state requirements. Phase 6 must bump `arct_ffi_version()` to `2`.

Policy:

- Breaking ABI changes increment the integer returned by `arct_ffi_version()`.
- Non-breaking additions do not require a bump, but may bump if the Python
  adapter needs to distinguish optional capabilities.
- The Python split-step adapter requires exactly version `2` unless a later
  compatibility table explicitly permits newer versions.

Breaking changes include:

- changing any existing exported function signature;
- changing ownership or release rules for an existing pointer parameter;
- changing the meaning of an existing status code;
- changing the top-level Arrow representation away from `StructArray`;
- changing tick advancement, partial-failure, or spawn-after-processor
  semantics;
- making `LAST_ERROR` lifetime or thread-local behavior incompatible.

Non-breaking changes include:

- adding a new exported function;
- adding a new status code in a reserved range;
- adding optional diagnostics retrievable through a new function;
- allowing additional table metadata while preserving existing schema and
  ownership rules.

At startup, the Python adapter loads the native library, resolves
`arct_ffi_version`, and checks the returned value before creating any native
world handle. If native mode is `required`, mismatch raises a Python exception
that includes the loaded path, expected version, actual version, and
`arct_last_error_message()` if present. If native mode is `auto`, mismatch
disables native stepping and falls back to the pure-Python path. If native mode
is `off`, no native library is loaded.

## 7. Python Adapter Sketch

The adapter belongs under `src/archetype/core/native/`. The interface is
deliberately internal; public runtime APIs remain Python-first.

### Boundary Choice

Use `ctypes` for dynamic library loading and function dispatch, plus
`pyarrow.cffi` for allocating Arrow C Data structs.

Rationale:

- It matches the current native adapter prototype in the Rust-core working tree.
- It preserves the `docs/guide/rust-core-plan.md` non-goal: no PyO3 requirement
  for this migration.
- It keeps Arrow buffers on the Arrow C Data Interface rather than serializing
  through Python objects.
- It avoids a generated cffi extension build while still using PyArrow's C Data
  import/export hooks.

PyO3 remains a future packaging option only if the C ABI cannot express
lifecycle management cleanly.

### Internal Interface

The adapter should expose an internal object with this shape:

- `NativeStepKernel.available() -> bool`
- `NativeStepKernel.version() -> int`
- `NativeStepKernel.begin(world_handle, table_name) -> pyarrow.RecordBatch`
- `NativeStepKernel.commit(world_handle, table_name, batch) -> None`
- `NativeStepKernel.abort(world_handle, table_name) -> None`
- `NativeStepKernel.last_error() -> str | None`

`begin` allocates empty Arrow C Data shells, calls `arct_step_begin`, checks the
status, imports the output into PyArrow, and uses checked release helpers in
`finally`.

`commit` converts Daft/PyArrow output to one `pyarrow.RecordBatch`, exports it
to Arrow C Data, calls `arct_step_commit`, checks the status, and uses checked
release helpers in `finally`.

`abort` calls `arct_step_abort` and reports non-zero status without touching any
Arrow C Data.

### AsyncWorld Delegation

`AsyncWorld.step` keeps the pure-Python path as the default. Native split-step
is selected only behind an explicit config flag, for example a future
`native_core` setting with values `off`, `auto`, and `required`.

Native-enabled step flow:

1. Fire `PreTick` from Python, preserving hook ordering.
2. Determine the table/signature list using the current Python bookkeeping for
   Phase 6. The Rust epoch separately validates that each table belongs to its
   frozen active set.
3. For each table, call `NativeStepKernel.begin` in a worker thread if the FFI
   call can block.
4. Convert the returned `pyarrow.RecordBatch` to the current DataFrame shape.
5. Run the existing Python processor chain through `AsyncSystem.execute`.
6. Convert the processed DataFrame back to a single `pyarrow.RecordBatch`.
7. Call `NativeStepKernel.commit` in a worker thread if the FFI call can block.
8. If Python processing or conversion fails after begin, call
   `NativeStepKernel.abort`.
9. If any table fails, raise a Python exception and mark the native world handle
   unusable for further stepping.
10. On full success, Rust advances the tick; Python mirrors the new tick value
    from the native handle before firing `PostTick`.

Pure-Python fallback:

- When native mode is `off`, keep the current `_run_archetype` path.
- When native mode is `auto` and the library is missing, the ABI version
  mismatches, or required symbols are missing, log/debug-record the reason and
  use the pure-Python path.
- When native mode is `required`, missing native support raises before the first
  step.

The fallback path is mandatory until the migration-gate suite proves parity and
the release plan removes the compatibility switch.

## 8. Test Plan

The Phase 6 implementation must add tests at three levels: memory safety,
contract parity, and benchmark comparison.

### Memory-Safety Harness

Rust unit/integration tests in `crates/archetype-ffi`:

- `begin` success exports a batch, Python/Rust test imports it once, and a
  second checked release is a no-op.
- `commit` success consumes input C Data and leaves both release callbacks null.
- `commit` schema mismatch after import returns `ARCT_ERR_SCHEMA_MISMATCH`; the
  input is still moved and checked release is a no-op.
- null world/table/array/schema pointers return `ARCT_ERR_NULL_POINTER`.
- non-empty output shells passed to `begin` return `ARCT_ERR_OUTPUT_NOT_EMPTY`
  without overwriting or leaking the original release callbacks.
- `abort` after begin restores buffers and does not touch caller-owned Arrow C
  Data.
- double `begin` for the same table returns `ARCT_ERR_STEP_ALREADY_IN_FLIGHT`.
- commit without begin returns `ARCT_ERR_NO_STEP_IN_FLIGHT`.
- panic harness returns `ARCT_ERR_PANIC` and sets `LAST_ERROR`.

Sanitizer runs:

- Run the FFI tests under AddressSanitizer with leak detection enabled.
- Run the Python ctypes adapter tests under `valgrind` or ASan-enabled Python
  where available, covering success, schema error, panic error, and abort.
- Run `cargo miri test -p archetype-ffi` for helper-level tests that do not
  require dynamic C loading. If Arrow dependencies block Miri, document the
  unsupported cases and keep ASan/valgrind as the gate.

### Contract Parity Migration Gate

The migration gate runs every scenario twice: once with native mode disabled and
once with native split-step required. Both runs use the same world id, run id,
reserved entity IDs, table names, processor set, and temporary storage layout.

The assertion compares append-only ledger state, not just latest live rows:

- collect every table from the Python path and native path;
- normalize row order by `(table_name, tick, entity_id, is_active)`;
- compare Arrow schemas exactly;
- compare row values exactly for all base and component columns;
- compare live active snapshots after every step;
- compare final tick and entity registry state.

Required scenarios:

- tick-zero spawn writes raw `x0` at tick `0`;
- processor effect first appears for that entity at tick `1`;
- mid-run spawn at tick `N` writes raw values at tick `N` and processed values at
  tick `N + 1`;
- despawn applies before processors and persists an inactive row;
- same-tick spawn followed by despawn cancels the pending spawn;
- duplicate spawns for the same entity are last-write-wins;
- update/reset rows preserve raw materialization semantics;
- component migration writes an old-table tombstone and a new-table raw active
  row;
- processor failure after one committed table produces the documented
  partial-tick state and a Python exception.

Existing tests to mirror or extend include:

- `tests/aio/test_async_world_mutations.py`
- `tests/aio/test_async_world_lifecycle.py`
- `tests/core/test_async_world_duplicate_spawn_overwrite.py`
- `tests/integration/test_command_flow.py`
- `tests/sync/test_sync_stack_contracts.py`

The C3 migration-gate suite described in
`/Users/everettkleven/conductor/workspaces/archetype/taipei/.context/htn-execution-plan.md`
should become the Phase 6 entry criterion before this adapter is enabled beyond
`auto` mode.

### Benchmark Comparison

The benchmark reports the native-vs-python tick overhead number without GPU
work.

Fixture:

- one table with `Position(x, y)` and `Velocity(dx, dy)`;
- one Python movement processor equivalent to the existing benchmark processor;
- deterministic initial values: `x = entity_id`, `y = 0`, `dx = 1`, `dy = -0.5`;
- cases: `1000x50` and `10000x50` entities-by-ticks;
- five measured trials and one warmup trial per backend;
- temporary local storage per trial.

Backends:

- pure Python/Daft path with native mode `off`;
- native split-step path with Python processors and native begin/commit;
- both paths must use live reads or store reads consistently for the selected
  comparison.

Correctness gate before timing is accepted:

- final row count matches entity count;
- final position sums match the closed-form movement model;
- append-only ledger parity matches the migration-gate comparison for a smaller
  fixture.

Reported primary metric:

- `delta_ms_per_tick = 1000 * (median_python_tick_sec - median_native_tick_sec)`.

Reported supporting metrics:

- `speedup = median_python_tick_sec / median_native_tick_sec`;
- `median_python_tick_sec`;
- `median_native_tick_sec`;
- native phase medians for begin, Python processing, commit, and total tick;
- rows per second for each backend.

The benchmark should extend the existing tick-loop benchmark shape in
`bench/core/tick_loop_overhead.py` from the Rust-core working tree and must not
run GPU benchmarks.
