# Specification

This page is the entry point for Archetype's contract documents.

It does two things:

1. names the current sources of truth for the contracts we established in this
   session
2. defines the broader engine and application contracts that every runtime,
   adapter, and orchestration layer must preserve

## Contract Inventory

The current contract set is split across design docs and executable tests.

| Contract source | Scope | Notes |
|---|---|---|
| `docs/guide/specification.md` | Engine, application, and top-level runtime contracts | This page. Normative behavior from storage through app/runtime boundaries, including sugar/runtime constraints. |
| [`tests/app/test_sugar_runtime.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_sugar_runtime.py) | Executable runtime contracts | Enforces activation single-flight, runtime-vs-world lifetime, fork isolation, spawn visibility, governance, and smoke paths. |
| [`tests/sync/test_sync_stack_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/sync/test_sync_stack_contracts.py) | Executable sync engine contracts | Enforces store/querier/updater/world behavior, mutation materialization, component migration, and despawn semantics. |
| [`tests/integration/test_command_flow.py`](https://github.com/VangelisTech/archetype/blob/main/tests/integration/test_command_flow.py) | Reserved spawn chain | Verifies reserved `entity_id` survives submit -> drain -> apply -> materialize. |
| [`tests/app/test_services.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_services.py) | Service-layer execution contracts | Covers simulation service boundaries, processor metadata, and read-service expectations. |
| [`tests/cli/test_cli.py`](https://github.com/VangelisTech/archetype/blob/main/tests/cli/test_cli.py) | CLI adapter contracts | Covers base URL handling, client lifecycle, error formatting, and server-backed command behavior. |

## Session-Hardened Contracts

The work in this session surfaced and hardened the following contract families:

- Top-level sugar/runtime contracts:
  pure construction, single-flight activation, honest `spawn()` return values,
  explicit runtime ownership, world-local shutdown, fork isolation, and
  backwards-compatible exports.
- Multi-world lifetime contracts:
  one world's shutdown must not invalidate sibling worlds, and runtime teardown
  must remain separate from per-world teardown.
- Script ceremony contracts:
  ergonomics may improve, but runtime boundaries, governance, and broker timing
  must remain explicit.
- Brokered spawn contracts:
  `spawn()` may return an `entity_id` only if that `entity_id` is reserved and
  preserved all the way through the command chain.
- Sync engine contracts:
  append/read consistency, active-state querying, deterministic
  last-write-wins duplicate spawn handling, safe component migration, and
  despawn-only archetype processing.
- Adapter contracts:
  service and CLI layers must preserve the underlying engine/runtime semantics
  rather than invent new ones.

## Status

This document defines the contracts that govern Archetype from storage through
world execution into the application layer.

It also includes the top-level sugar/runtime requirements that were previously
tracked separately. The goal is a single specification that describes the
engine contracts any runtime, API, CLI, or future orchestration layer must
preserve.

Normative language:

- `MUST` means required for a conforming implementation.
- `SHOULD` means strongly preferred unless a documented exception exists.
- `CURRENT GAP` marks behavior that is inconsistent, incomplete, or not yet
  aligned with the intended contract.

## Scope

This specification covers:

- component and archetype identity
- store, querier, updater, system, and world contracts
- mutation materialization and world lifecycle events
- brokered command flow in the application layer
- top-level sugar/runtime API constraints
- multi-world orchestration and world forking
- idempotency expectations and non-idempotent boundaries

This specification does not authorize direct edits to `src/archetype/core/`.
It defines the behavior that higher layers must preserve and that future
implementation work must satisfy.

## Core Terms

| Term | Meaning |
|---|---|
| `Component` | Typed schema fragment persisted as prefixed columns |
| `ArchetypeSignature` | Canonical sorted tuple of component types |
| `Tick` | One world step boundary |
| `Run` | A sequence of ticks sharing one `run_id` |
| `Live snapshot` | The in-memory active DataFrame per signature for the latest completed tick |
| `Mutation cache` | The staged spawn/despawn data applied at the next tick |
| `World lifecycle command` | Create, destroy, or fork world operations |
| `Runtime` | The process-scoped composition root that owns shared services |

## Layer Boundaries

The stack is strictly layered:

1. `Store`: durable append-only persistence
2. `Querier`: read facade over store-backed state
3. `Updater`: write facade that stamps metadata and persists rows
4. `System`: processor orchestration
5. `World`: query -> mutate -> execute -> persist lifecycle
6. `Application services`: broker, command routing, multi-world orchestration
7. `API / CLI / sugar`: outer adapters over the service layer

Each layer may depend downward. No lower layer may depend upward.

## Data Model Contracts

### Components

- A `Component` class defines a schema fragment.
- Persisted component columns MUST be prefixed as
  `<component_name_lower>__<field_name>`.
- `Component.to_payload()` MUST include a `"type"` discriminator so the app
  layer can reconstruct the original concrete component type.
- Untyped payload dicts MUST fail loudly rather than silently degrading to the
  base `Component`.

### Archetype signatures

- `ArchetypeSignature` MUST be canonicalized as a sorted tuple of component
  types.
- Signature identity is order-invariant. `(A, B)` and `(B, A)` describe the
  same archetype.
- The base persisted columns for every archetype row are:
  `world_id`, `run_id`, `entity_id`, `tick`, and `is_active`.

## Store Contracts

- The store MUST be append-only. Updating a world means appending new rows, not
  mutating prior rows in place.
- A store MUST create archetype tables on demand from the signature schema.
- Store reads MUST be scoped by `world_id` and `run_id`.
- Store writes MUST resolve through the same table identity as reads for the
  same signature. A write path may not silently drift to a different table
  lookup mechanism.
- The store itself MUST NOT impose active-state semantics; `is_active` and
  historical filtering belong above the raw store.
- Empty appends SHOULD be safe no-ops.
- Store shutdown SHOULD be safe to call more than once.

Idempotency:

- Store `append()` is not idempotent. Repeating the same append writes duplicate
  rows unless a higher layer deduplicates.
- Store `get_archetype_df()` is idempotent for the same persisted data.
- Cached-store shutdown MUST be idempotent even if called multiple times.

CURRENT GAP:

- `AsyncUpdateManager` currently logs append failures and still returns a
  stamped DataFrame. Durability success is therefore ambiguous. The contract
  should be hardened so failed persistence is observable to callers.

## Querier Contracts

- The querier is the active-state read facade over the store.
- The querier MUST read through the store and then apply:
  `is_active == true`, optional tick filters, optional entity filters, and
  optional component projection.
- Component projection MUST use the canonical schema column list for the
  requested component set.
- The querier MUST be read-only.
- Full append history remains part of the storage model, but the current
  querier contract is an active-state projection, not a full history API.

Idempotency:

- Querier operations are idempotent for the same persisted data and filter set.

## Updater Contracts

- The updater MUST normalize rows before persistence.
- The updater MUST stamp `tick`, `world_id`, and `run_id` on every write.
- The updater MUST normalize `entity_id` to the storage type expected by the
  schema.
- The updater MUST append through the store and return a DataFrame that matches
  the persisted shape.

Idempotency:

- Updater `update()` is not inherently idempotent. Repeating it appends another
  version of the rows.
- Idempotency for world mutation replay must therefore be provided by world or
  command semantics, not by the updater.

CURRENT GAP:

- Failed appends are currently swallowed and logged. The updater contract should
  distinguish successful persistence from a stamped-but-uncommitted DataFrame.

## System and Processor Contracts

- A processor is a `DataFrame -> DataFrame` transform over one archetype at a
  time.
- A processor MUST declare the component set it depends on.
- A processor matches a signature when its required component set is a subset
  of the archetype signature.
- Within one archetype, processors MUST execute in ascending `priority`.
- Across different archetypes, execution MAY proceed concurrently.
- Only kwargs explicitly accepted by a processor should be passed through.
- Shared resources MAY be injected through the world resource container.

Failure policy:

- Processor failures MUST NOT silently corrupt world bookkeeping.
- If the engine continues after a processor failure, that continuation policy
  MUST be explicit and tested.

Current behavior:

- Processor errors are logged and execution continues with the last good
  DataFrame.

Idempotency:

- Processor execution is only idempotent if the processor itself is pure with
  respect to the input DataFrame and resources. The engine does not guarantee
  semantic idempotency for arbitrary processors.

## World Contracts

### World state ownership

A world owns:

- the `world_id` and human-readable name
- entity-to-signature bookkeeping
- the next world-local entity ID counter
- staged spawn/despawn caches
- the live in-memory active snapshot
- lifecycle hooks
- the system, querier, and updater integration

### World execution order

One tick MUST follow this order:

1. fire `pre_tick` hooks
2. determine active signatures from live state plus staged mutations
3. for each signature:
   - load previous state
   - materialize staged mutations
   - execute processors
   - persist through the updater
4. replace the live snapshot with active rows only
5. increment the world tick
6. fire `post_tick` hooks

### Previous-state reads

- Between ticks, the live snapshot is the authoritative in-memory view of the
  latest completed tick.
- Store-backed reads are the durability path.
- `get_components()` is a live-snapshot API, not a historical store query.

CURRENT GAP:

- `RunConfig.prefer_live_reads` exists, but current async world behavior
  effectively prefers `_live` whenever it is available. The config flag should
  either be enforced or removed.

### Run contract

- A `RunConfig` describes a sequence of steps that share one `run_id`.
- `world.run(run_config)` MUST preserve that same `run_id` across every tick in
  the run.
- Query defaults that rely on the current run SHOULD use the world's active
  `run_id`.

## Mutation Contracts

### Spawn

- `create_entity()` creates a new world-local entity ID and stages a spawn row.
- The entity does not become part of the live active snapshot until the next
  materialization boundary.
- When the app layer reserves an entity ID before enqueue, the same entity ID
  MUST survive submit -> broker -> drain -> apply -> materialize.

### Despawn

- `remove_entity()` stages removal at the next materialization boundary.
- Removing an unknown entity SHOULD be a no-op with observability, not silent
  corruption.

### Add/remove components

- Component addition and removal are archetype moves.
- The old signature receives a despawn marker.
- The new signature receives a spawned row built from the latest visible entity
  state plus the requested mutation.
- When migration materializes into an existing DataFrame, staged spawn rows MUST
  be cast or otherwise normalized to the target schema before concat.
- Adding already-present components or removing already-absent components SHOULD
  be a no-op.

### Mutation materialization

- Duplicate despawns for the same entity in one tick MUST collapse.
- Duplicate spawns for the same entity in one tick MUST resolve
  deterministically.
- The current deterministic contract is last-write-wins by entity ID within the
  tick.
- Despawn-only signatures MUST still be processed during the next tick, even if
  no active entities remain in that archetype after bookkeeping updates.

CURRENT GAP:

- `AsyncWorld._move_entity()` can currently return an empty row when the old
  entity is not found in `_live`, and callers do not validate this before
  staging a spawn. That boundary needs an explicit error or no-op contract.

## Lifecycle Hook Contracts

- `pre_tick` and `post_tick` are observability hooks, not transactional
  mutation hooks.
- Hook execution order relative to the tick lifecycle MUST remain stable.
- Hook failures SHOULD be logged and suppressed unless a future opt-in
  fail-fast mode is added.
- Hook removal SHOULD be idempotent.

CURRENT GAP:

- Documentation mentions `on_spawn` and `on_despawn`, but the async world does
  not currently fire them. The docs and implementation should be aligned.

## Application Layer Contracts

### StorageService

- `StorageService` is the multiton owner for backend triplets:
  `(store, querier, updater)`.
- Worlds sharing the same `(uri, namespace)` MUST reuse the same backend
  triplet.
- Concurrent backend acquisition for the same key MUST single-flight so only
  one backend is built.
- Service shutdown MUST shut down every managed backend exactly once per
  instance.

### WorldFactory

- `WorldFactory` is the seam between app and core.
- It MUST obtain the backend triplet from `StorageService` and assemble an
  `AsyncWorld` with a system, querier, and updater.

### WorldService

- `WorldService` owns the in-memory catalog of active worlds.
- `create_world()` MUST be idempotent by explicit `world_id`.
- Name lookup is a convenience index; names are unique, but they are not the
  idempotency key.
- Broker injection into world resources is an app-layer responsibility.
- `remove_world()` SHOULD be safe to call on a missing world.
- `fork_world()` MUST create a new `world_id`, clone the source world's visible
  state, and let source and fork diverge independently.
- Forking MUST reject source worlds with pending un-materialized mutations.

CURRENT GAP:

- `create_world()` currently inserts the world into `_worlds` before duplicate
  name validation, which can leave behind an unintended cached world on error.

### CommandBroker

- The broker is the external command queue and audit surface.
- Commands are ordered by `(tick, priority, seq)`.
- Queues are partitioned by world key.
- RBAC and quota validation MUST occur before enqueue when actor context is
  supplied.
- The broker MUST preserve pending and history state for observability.

Idempotency:

- Enqueue is not deduplicating. Submitting the same logical command twice
  yields two queued commands.
- Dequeue is destructive. Once a command is removed from the queue, replay is
  the caller's responsibility.

### CommandService

- `submit()` and `submit_batch()` are enqueue-only APIs. They return command IDs
  and do not apply mutations immediately.
- `submit_spawn()` is the special case that reserves a world-local entity ID
  before enqueue so `spawn()` can honestly return `entity_id`.
- Reservation MUST be serialized per world.
- `drain_and_apply()` is the command application boundary at tick time.
- World lifecycle commands are dispatched through `apply_world_lifecycle()`.

CURRENT GAPS:

- `submit()` does not currently validate that the target world exists before
  enqueue.
- `drain_and_apply()` logs failed applies but does not retry or requeue them, so
  failed commands are effectively dropped.

### SimulationService

- `step()` is the authoritative world execution boundary.
- `step()` MUST apply due commands before world execution.
- `step()` MUST receive an explicit `RunConfig` from the caller; the service
  MUST NOT mint a fresh `RunConfig` per call. Callers drive a multi-tick run
  by reusing the same `RunConfig` across every step so the `run_id` is stable.
- `run()` MUST preserve one logical `run_id` across all steps in the run by
  threading the caller's `RunConfig` into every `step()` call.
- `run_all()` MAY execute multiple worlds concurrently.

### QueryService

`QueryService` is the read facade for external callers. It sits directly
over `AsyncWorld` and its querier. It MUST NOT reimplement union, projection,
or filtering logic that belongs in the core layer.

#### Q1. Public read surface

The public read surface is exactly four methods:

- `get_world_state(world_id, tick=None) -> WorldSnapshot`
- `get_entity(world_id, entity_id, tick=None) -> dict`
- `get_components(world_id, component_types, entity_ids=None, tick=None, ...) -> dict`
- `get_command_history(world_id, limit=100) -> list[Command]`

Any additional read method requires a spec revision.

#### Q2. World existence

Every read method MUST raise `KeyError` when the target world is unknown.
The API layer translates `KeyError` to HTTP 404. `get_command_history()`
MUST follow the same rule as the other three — returning an empty list on
an unknown world is forbidden.

#### Q3. Entity existence

`get_entity` MUST raise `KeyError` when the entity is not present in the
requested state (current or historical). Returning a success payload with
empty component data is forbidden, because it prevents the API layer from
distinguishing "entity missing" from "entity has no components."

#### Q4. Component type resolution

`component_types: list[str]` MUST resolve via `Component.get_type_by_name`.
Unknown names MUST raise `ValueError`, which the API layer translates to
HTTP 400. Silently dropping unknown names and returning partial results is
forbidden.

#### Q5. Response encoding

Component payloads in response dicts MUST key by the component class name
(e.g. `"Position"`, not `"position"`). Field names within a component MUST
be unprefixed using `comp_type.get_prefix()`. Splitting column names on
`"__"` is forbidden — component field names MAY legitimately contain
double underscores.

#### Q6. Base-column hiding

The base persisted columns (`world_id`, `run_id`, `entity_id`, `tick`,
`is_active`) MUST NOT appear inside component payloads. `entity_id` and
`tick` MAY appear as top-level fields of the response. The other three
MUST be stripped.

#### Q7. Live vs store routing

The effective tick is:

- `world.tick - 1` if `tick is None` (the latest completed tick)
- the caller-supplied `tick` otherwise

If the effective tick equals `world.tick - 1`, reads MUST go through
`AsyncWorld.get_components()` or `AsyncWorld._live` directly. Otherwise
reads MUST route through `AsyncWorld.query_archetype(sig, ticks=[tick])`.

`QueryService` MUST NOT reconstruct union or projection logic across
archetype signatures. That is owned by `AsyncWorld.get_components`. If a
historical multi-archetype read is needed, `AsyncWorld.get_components`
MUST grow an optional `tick` parameter — it is not duplicated in the
service.

#### Q8. Counting

Row counts MUST use `DataFrame.count_rows()`. `df.collect().to_pylist()`
followed by `len(...)` is forbidden. Counting MUST not materialize a
Python list the caller does not see.

#### Q9. Materialization boundary

The REST-facing methods MUST collect to Python dicts exactly once, at the
service boundary. An internal variant (`as_dataframe=True`, or a sibling
method) MAY return a lazy `daft.DataFrame` unchanged, so in-process
consumers — processors, client-side query code, sugar runtime — stay in
Daft's execution engine. This is the data-centric principle (`LEARNINGS.md`)
applied to the read path.

#### Q10. Filter expressions

If `get_components` accepts a filter, the filter MUST be expressed as a
`daft.Expression` object built from real column references. A string DSL
over columns (e.g. `"position__x > 0.5"`) is forbidden. For the REST
surface, a SQL fragment passed through `daft.sql_expr` is acceptable; a
custom parser is not.

#### Q11. Limits and projections

`limit` and `count_only` options are orthogonal. `count_only=True` MUST
skip materialization and return only the row count via `count_rows()`.
`limit=N` MUST apply before materialization. Combining `count_only=True`
with `limit` is forbidden and MUST raise `ValueError`.

#### Q12. Compilable client surface

A typed Python entry point MUST be provided that accepts `Component`
*classes* (not strings) and returns a lazy `daft.DataFrame`:

```python
df = await world.query(Position, Velocity)
df = df.where(q(Position).x > 0.5).limit(10)
rows = df.collect().to_pylist()  # explicit materialization
```

Required properties:

- The Component arguments MUST be real imported classes, so static type
  checkers and editor tooling see them.
- `q(Component).field` MUST return a `daft.Expression` equivalent to
  `col(Component.get_prefix() + "field")`.
- The returned object MUST be a lazy `DataFrame`, not a pre-materialized
  list. Callers materialize explicitly.
- The string-based REST path (`types=Position,Velocity`) is the wire
  format for HTTP clients. It MUST NOT be the primary API for
  in-process consumers.

CURRENT GAP:

- Most read methods are currently stubs that echo metadata rather than
  querying actual world state. (Tracked in #75.)
- `_entity2sig` is current-state only. Historical tick queries resolve
  the entity's *current* archetype signature, which is incorrect for
  entities that have migrated archetypes. Either `AsyncWorld` MUST grow
  tick-indexed signature resolution, or historical reads MUST document
  and reject cross-tick signature changes explicitly.
- `AsyncWorld.get_components()` does not yet accept a `tick` parameter.
  Until it does, Q7's "historical multi-archetype read" contract cannot
  be satisfied without duplicating logic in the service — which Q7
  itself forbids. This is a core-layer prerequisite for full
  `QueryService` implementation.
- `Component.get_type_by_name` walks `__subclasses__()`. For Q4 to be
  reliable across dynamically imported Component packages, the lookup
  must be resilient to import order.
- `get_command_history()` behaves differently from the other read
  methods on unknown worlds.

### ServiceContainer and runtime lifetime

- `ServiceContainer` is the process-scoped composition root.
- It owns one shared `StorageService`, one shared `CommandBroker`, and the
  world, command, simulation, and query services built on top of them.
- Container shutdown MUST be explicit and distinct from per-world removal.
- Container shutdown order MUST clear broker state and then shut down world and
  storage services.

## Multi-World Contracts

- Multiple worlds may coexist in one runtime.
- Worlds MUST be isolated by `world_id`.
- Storage rows are scoped by both `world_id` and `run_id`.
- Broker queues and history are partitioned per world key.
- `run_all()` may step multiple managed worlds concurrently.
- A fork shares runtime infrastructure, but not world identity.
- Shutting down or removing one world MUST NOT invalidate sibling worlds that
  share the same runtime.

CURRENT GAP:

- `remove_world()` only removes the world from the world catalog and registry.
  It does not explicitly clear per-world broker state or provide true
  world-local shutdown semantics.

## Top-Level Runtime and Sugar Contracts

### Purpose

This section defines the minimum contracts for any top-level "sugar" API that
wraps Archetype's service layer. These requirements exist to prevent a
convenience API from weakening the engine's concurrency guarantees, world
lifecycle isolation, or broker-based command semantics.

The sugar API may improve ergonomics. It may not change the underlying
behavioral contracts unless that change is explicitly designed, versioned, and
tested.

### Scope

These requirements apply to:

- Any proposed top-level `World`, `Processor`, `Archetype`, `Runtime`, or
  `run_sync` sugar API
- Any wrapper that hides `ServiceContainer`, `WorldService`,
  `SimulationService`, or `CommandService`
- Any re-export change that alters the default public API surface

These requirements do not authorize changes to `src/archetype/core/`, which
remains read-only unless separately approved.

### Core Principle

Sugar wraps the service layer. Sugar does not bypass the service layer, weaken
its guarantees, or silently change the semantics of commands, world identity,
or execution.

### Concurrency Contract

#### C1. Pure construction

Constructing a sugar wrapper such as `World(...)` must be pure and side-effect
free.

Required behavior:

- No I/O during object construction
- No implicit world creation during object construction
- No mutation of process-global runtime state during object construction
- No background task startup during object construction

#### C2. Single-flight activation

The first activation of a lazily initialized wrapper must be serialized.

Required behavior:

- If multiple coroutines concurrently activate the same wrapper, exactly one
  backing world may be created
- Every caller must observe the same backing world identity after activation
- Activation must be idempotent after the first successful initialization

Minimum implementation expectation:

- Activation must be guarded by an async lock or equivalent single-flight
  mechanism

#### C3. No partially initialized observable state

The sugar layer must not expose half-initialized runtime state.

Required behavior:

- Properties that depend on an activated world must either:
  - wait for activation to complete, or
  - raise a clear error indicating the world is not yet active
- Callers must never observe an object whose processors, resources, or backing
  world registration are only partially applied

#### C4. Serialized lifecycle transitions

Activation, shutdown, and fork are mutually sensitive lifecycle operations and
must not race.

Required behavior:

- `fork()` may not race with first activation
- `shutdown()` may not race with first activation
- `shutdown()` may not invalidate in-flight `run()`, `step()`, or `query()`
  calls without a defined error contract

#### C5. Honest command return values

Sugar methods must not claim stronger return semantics than the service layer
can provide.

Required behavior:

- `spawn()` must not claim to return an entity ID unless the architecture can
  reserve that entity ID before broker enqueue
- If entity identity is only known after broker drain and apply, `spawn()` must
  return a command ID, a handle with explicit semantics, or no value
- Return types and docstrings must match actual runtime behavior

#### C6. Broker semantics remain intact

Command ordering and tick-boundary application must remain true under sugar.

Required behavior:

- Enqueued commands must still be subject to broker ordering
- Enqueued commands must still be applied at the documented tick boundary
- Sugar must not directly mutate worlds in ways that contradict the public
  brokered mutation contract unless that method is explicitly documented as a
  lower-level escape hatch

### Multi-World Lifetime Contract

#### L1. Separate runtime lifetime from world lifetime

The runtime/container lifetime and individual world lifetimes must be modeled as
different scopes.

Required behavior:

- A process-scoped runtime must not be implicitly treated as world-scoped
- A world wrapper must not own the entire container by default
- Destroying or shutting down a world must not automatically tear down the
  runtime that may serve sibling worlds

#### L2. World shutdown is world-local

`World.shutdown()` must have world-local semantics.

Required behavior:

- It must detach, destroy, or close only that world's handle and registrations
- It must not tear down shared storage pools, the broker, or sibling worlds
- If full runtime teardown is needed, it must occur through an explicit
  runtime-level API

#### L3. Explicit runtime teardown

Container teardown must be explicit and process-scoped.

Required behavior:

- Runtime teardown must be performed through a dedicated runtime object or
  runtime-level function
- The API surface must clearly distinguish:
  - world shutdown
  - runtime shutdown

Recommended shape:

- `async with ArchetypeRuntime() as runtime: ...`
- `await runtime.shutdown()`

#### L4. Forks share runtime, not world identity

Forked worlds must share runtime infrastructure while remaining distinct world
lifecycles.

Required behavior:

- A fork must receive its own world identity
- A fork may share storage pools and broker infrastructure through the runtime
- Shutting down a source world must not invalidate the fork
- Shutting down a fork must not invalidate the source world

#### L5. Test isolation

The sugar runtime must not make deterministic testing harder.

Required behavior:

- Tests must be able to create isolated runtime instances without inheriting
  process-global state from previous tests
- Global singletons, if used at all, must have an explicit reset or opt-out
  path for tests
- Test suites must be able to exercise multiple runtimes in one process

### Script Ceremony Contract

#### S1. Minimal ceremony, explicit boundary

The sugar API should reduce ceremony for scripts, but execution boundaries must
remain explicit.

Required behavior:

- Users may define `World(...)` wrappers declaratively
- The start of runtime ownership must be explicit somewhere in the script
- The API must make it clear where startup and teardown occur

Acceptable shapes include:

- `async with ArchetypeRuntime() as app:`
- `async with Archetype() as app:`
- `with Archetype.sync() as app:`

#### S2. Context management belongs at runtime scope

If a context manager is used to manage process resources, it should exist at the
runtime level, not implicitly at each world wrapper.

Required behavior:

- Entering a runtime context may create or attach the container
- Exiting a runtime context may shut down the container
- Exiting a world context must not tear down process-shared infrastructure
  unless the world context is explicitly defined as owning a dedicated runtime

#### S3. Sync helpers must not hide process lifetime

Sync conveniences are allowed, but they must not obscure resource ownership.

Required behavior:

- `run_sync()` must document whether it creates a temporary runtime or uses an
  existing one
- Repeated sync calls must not silently create and destroy incompatible runtime
  state around objects that outlive a single call
- Sync entry points must not leave shared global state in an ambiguous state

#### S4. Preserve public API compatibility unless versioned

Top-level sugar must not silently redefine long-standing public imports.

Required behavior:

- Existing default exports such as `World` and `Processor` must remain stable
  unless changed as part of an explicit breaking release
- If new sugar types are introduced, prefer additive names first
- Any future alias swap requires migration notes and compatibility tests

#### S5. Ergonomics must not bypass governance

Script ergonomics must not come from removing safety mechanisms.

Required behavior:

- If sugar claims to preserve RBAC, audit history, or broker semantics, those
  paths must actually flow through the governing services
- If a method intentionally bypasses governance, that bypass must be explicit in
  naming and documentation
- Direct resource mutation must not be described as governed by the broker

### Sugar Runtime Acceptance Criteria

No sugar API may be considered ready for implementation until the design can
show how it satisfies all of the following:

- Concurrent first-use of the same wrapper creates exactly one world
- `spawn()` return semantics are correct and tested
- One world's shutdown does not break a sibling world in the same runtime
- Runtime teardown is explicit and distinct from world teardown
- Forked worlds remain valid after the source world is shut down
- Async and sync script entry points have a clear resource ownership model
- Existing public imports remain compatible, or the change is explicitly marked
  as breaking and tested accordingly

### Non-Goals

This section does not choose the final user-facing API names. It establishes
the constraints that any acceptable design must satisfy.

## Idempotency Matrix

| Operation | Expected contract |
|---|---|
| `StorageService.get_backend(key)` | Idempotent per `(uri, namespace)` within one service instance |
| `WorldService.create_world(world_id=X)` | Idempotent by explicit `world_id` |
| `WorldService.remove_world(missing)` | Safe no-op |
| `AsyncCachedStore.shutdown()` | Idempotent |
| `CommandBroker.enqueue()` | Not idempotent; duplicate logical commands remain distinct |
| `CommandService.submit()` | Not idempotent; duplicate submits create duplicate commands |
| `CommandService.submit_spawn()` | Returns one reserved `entity_id` per successful call; repeated calls create new entities unless the caller reuses an explicit reservation |
| `AsyncWorld.create_entity()` | Not idempotent; each call allocates a new world-local entity ID |
| `AsyncWorld.remove_entity(missing)` | Safe no-op with observability |
| Duplicate despawn in one tick | Idempotent collapse by entity ID |
| Duplicate spawn for same entity in one tick | Deterministic last-write-wins |
| `add_components()` with no signature change | Idempotent no-op |
| `remove_components()` with no signature change | Idempotent no-op |
| `world.step()` | Not idempotent; advances tick and appends new rows |
| `world.run()` | Not idempotent; performs multiple steps under one run contract |
| `QueryManager.query_archetype()` | Idempotent for fixed persisted state |

## Required Hardening Work

The following items should be treated as implementation requirements for a
coherent engine contract:

1. Make updater durability failures explicit instead of log-only.
2. Define and implement world-lifecycle command ack semantics so API create,
   destroy, and fork are not left in ambiguous broker state.
3. Decide whether command submission to an unknown world is allowed; if not,
   reject at submit time.
4. Fix `WorldService.create_world()` duplicate-name failure ordering so failed
   creation does not cache a hidden world.
5. Align hook documentation and implementation for spawn/despawn lifecycle
   events.
6. Give `QueryService` a real read contract or clearly mark it as provisional.
7. Define world-local teardown semantics that do not leak broker or shared
   runtime state.

## Acceptance Criteria

This specification should be considered satisfied only when tests demonstrate
all of the following:

- stable component payload round-trips
- deterministic signature canonicalization
- append-only persistence scoped by world and run
- stable processor ordering within an archetype
- stable cross-archetype execution without world bookkeeping corruption
- stable reserved-entity spawn semantics through the broker
- explicit multi-world isolation and fork divergence
- explicit runtime-vs-world lifetime boundaries
- clear distinction between idempotent and non-idempotent operations
