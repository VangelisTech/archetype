# Specification

This page is the entry point for Archetype's contract documents.

It does two things:

1. names the current sources of truth for Archetype contracts
2. defines the broader engine and application contracts that every runtime,
   adapter, and orchestration layer must preserve

## Contract Inventory

The current contract set is split across design docs and executable tests.

| Contract source | Scope | Notes |
|---|---|---|
| `docs/guide/specification.md` | Umbrella contract overview | This page. Broad contracts plus links to focused specifications. |
| [Runtime](runtime.md) | Script boundary | `ArchetypeRuntime`, `RuntimeWorld`, sync parity, lifecycle, gate-only access. |
| [Service Protocols](service-protocols.md) | Application service interfaces | `iCommandService` and the services it gates. |
| [Command Gate](command-gate.md) | Authorization and roles | Four-role model, permissions matrix, audit emission shape. |
| [Execution Hierarchy](execution-hierarchy.md) | Step/run/episode/rollout | Simulation levels and rollout fork semantics. |
| [World Lifecycle](world-lifecycle.md) | Create/fork/destroy | Append-only lifecycle, info-class downgrade, fork sharing/copy rules. |
| [Audit Log](audit-log.md) | Audit rows | Append-only audit history and query contract. |
| [`tests/app/test_sugar_runtime.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_sugar_runtime.py) | Executable runtime contracts | Enforces activation single-flight, runtime-vs-world lifetime, fork isolation, spawn visibility, governance, and smoke paths. |
| [`tests/sync/test_sync_stack_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/sync/test_sync_stack_contracts.py) | Executable sync engine contracts | Enforces store/querier/updater/world behavior, mutation materialization, component migration, and despawn semantics. |
| [`tests/integration/test_command_flow.py`](https://github.com/VangelisTech/archetype/blob/main/tests/integration/test_command_flow.py) | Reserved spawn chain | Verifies reserved `entity_id` survives submit -> drain -> apply -> materialize. |
| [`tests/app/test_services.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_services.py) | Service-layer execution contracts | Covers simulation service boundaries, processor metadata, and read-service expectations. |
| [`tests/cli/test_cli.py`](https://github.com/VangelisTech/archetype/blob/main/tests/cli/test_cli.py) | CLI adapter contracts | Covers base URL handling, client lifecycle, error formatting, and server-backed command behavior. |

## Contract Families

The current specification set covers the following contract families:

- Top-level runtime contracts:
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

This document defines broad contracts from storage through world execution into
the application layer. Focused specification pages are more precise for their
areas and take precedence when they define a newer contract.

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
- gated command flow in the application layer
- top-level runtime API constraints
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
6. `Application services`: command gate, audit, broker, multi-world orchestration
7. `Runtime / API / CLI`: outer adapters over the service layer

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

1. fire `PreTick` hooks
2. determine active signatures from live state plus staged mutations
3. for each signature:
   - load previous state
   - materialize staged mutations
   - execute processors
   - persist through the updater
4. replace the live snapshot with active rows only
5. increment the world tick
6. fire `PostTick` hooks

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

- `PreTick` and `PostTick` are observability hooks, not transactional
  mutation hooks.
- Hook execution order relative to the tick lifecycle MUST remain stable.
- Hook failures SHOULD be logged and suppressed unless a future opt-in
  fail-fast mode is added.
- Hook removal SHOULD be idempotent.
- Spawn, despawn, and component migration hooks SHOULD fire from every public
  mutation path that queues the corresponding mutation.

## Application Layer Contracts

### StorageService

- `StorageService` is the multiton owner for backend triplets:
  `(store, querier, updater)`.
- Worlds sharing the same effective storage pool key `(uri, namespace, backend,
  cache config)` MUST reuse the same backend triplet.
- Concurrent backend acquisition for the same key MUST single-flight so only
  one backend is built.
- Backend selection and storage-resource construction are app/runtime
  composition concerns.
- Core stores MUST receive backend-native inputs rather than a generic runtime
  storage context.
- The default catalog-backed path MAY construct a Daft `Session` and Daft
  `Catalog` through `StorageService`.
- When `StorageConfig.io_config` is provided for catalog-backed storage, it
  MUST be bound to the store and passed explicitly to Daft Iceberg
  read/write operations.
- Per-store credentials MUST NOT rely solely on process-global Daft planning
  config.
- The LanceDB path MUST NOT construct a Daft `Session` or Daft `Catalog`.
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
- `destroy_world()` SHOULD be safe to call on a missing world.
- `fork_world()` MUST create a new `world_id`, clone the source world's visible
  state, and let source and fork diverge independently.
- Forking MUST transfer pending spawn/despawn caches so spawn-then-fork before
  the next tick materializes in both worlds.

CURRENT GAP:

- `create_world()` currently inserts the world into `_worlds` before duplicate
  name validation, which can leave behind an unintended cached world on error.

### CommandBroker

- The broker is a pure queue for tick-deferred commands.
- Commands are ordered by `(tick, priority, seq)`.
- Queues are partitioned by world key.
- RBAC, quota validation, and audit emission happen at `iCommandService`.
- The broker MAY preserve pending and history state for queue observability.

Idempotency:

- Enqueue is not deduplicating. Submitting the same logical command twice
  yields two queued commands.
- Dequeue is destructive. Once a command is removed from the queue, replay is
  the caller's responsibility.

### CommandService

- `iCommandService` is the policy enforcement point for external operations.
- Direct methods authorize, delegate, audit, and return a result immediately.
- `submit()` and `submit_batch()` are tick-deferred APIs. They return command IDs
  and enqueue work for later application.
- `submit_spawn()` is the special case that reserves a world-local entity ID
  before enqueue so `spawn()` can honestly return `entity_id`.
- Reservation MUST be serialized per world.
- `submit()`, `submit_batch()`, and `submit_spawn()` MUST reject submissions to
  an unknown `world_id` by raising `archetype.app.errors.WorldNotFoundError`
  before any quota debit, broker enqueue, or audit emit.
- `drain_and_apply()` is the command application boundary at tick time.
- World lifecycle operations use direct gated methods such as `create_world`,
  `fork_world`, and `destroy_world`.

CURRENT GAPS:

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
- Episodes and rollouts follow [Execution Hierarchy](execution-hierarchy.md).

### QueryService

- `QueryService` is the internal read facade below the gate.
- External reads go through `iCommandService`.
- Read behavior SHOULD be consistent with the underlying core world and querier
  contracts.
- Query methods SHOULD either validate world existence consistently or
  intentionally document which routes are world-agnostic.

CURRENT GAP:

- Most read methods are currently stubs that echo metadata rather than querying
  actual world state.
- Audit history is served by `iAuditLog` through `iCommandService`.

### ServiceContainer and runtime lifetime

- `ServiceContainer` is the process-scoped composition root.
- It owns one shared `StorageService`, one shared `CommandBroker`, one
  append-only audit log, and the world, mutation, command, simulation, and
  query services built on top of them.
- Container shutdown MUST be explicit and distinct from per-world removal.
- Container shutdown order MUST clear broker state, flush/shut down audit, and
  then shut down world and storage services.

## Multi-World Contracts

- Multiple worlds may coexist in one runtime.
- Worlds MUST be isolated by `world_id`.
- Storage rows are scoped by both `world_id` and `run_id`.
- Broker queues are partitioned per world key.
- A fork shares runtime infrastructure, but not world identity.
- Shutting down or destroying one world MUST NOT invalidate sibling worlds that
  share the same runtime.

CURRENT GAP:

- `destroy_world()` only removes the world from the world catalog and registry.
  It does not explicitly clear per-world broker state or provide true
  world-local shutdown semantics.

## Top-Level Runtime Contracts

### Purpose

This section defines the minimum contracts for any top-level runtime API that
wraps Archetype's service layer. These requirements exist to prevent a
convenience API from weakening the engine's concurrency guarantees, world
lifecycle isolation, or gate-based command semantics.

The runtime API may improve ergonomics. It may not change the underlying
behavioral contracts unless that change is explicitly designed, versioned, and
tested.

### Scope

These requirements apply to:

- Any proposed top-level `World`, `Processor`, `Archetype`, `Runtime`, or
  `run_sync` runtime API
- Any wrapper that hides `ServiceContainer`, `WorldService`,
  `SimulationService`, or `CommandService`
- Any re-export change that alters the default public API surface

These requirements do not authorize changes to `src/archetype/core/`, which
remains read-only unless separately approved.

### Core Principle

Runtime wraps the service layer. Runtime does not bypass the service layer, weaken
its guarantees, or silently change the semantics of commands, world identity,
or execution.

### Concurrency Contract

#### C1. Pure construction

Constructing a runtime wrapper such as `World(...)` must be pure and side-effect
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

The runtime layer must not expose half-initialized runtime state.

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

Command ordering and tick-boundary application must remain true under runtime.

Required behavior:

- Enqueued commands must still be subject to broker ordering
- Enqueued commands must still be applied at the documented tick boundary
- Runtime must not directly mutate worlds in ways that contradict the public
  gated mutation contract unless that method is explicitly documented as a
  lower-level escape hatch

#### C7. Same-tick composition must be defined

Deferred materialization at the tick boundary must not make ordered command
semantics ambiguous.

Required behavior:

- If multiple commands targeting the same entity are drained in one tick, the
  implementation MUST define whether later commands observe earlier staged
  mutations from that same drain cycle
- If the public contract claims ordered command semantics for runtime mutation
  verbs, later commands SHOULD observe earlier same-tick mutations for the same
  entity even though none of them become query-visible until `step()` completes
- If the implementation does not provide that composition guarantee, the
  weaker behavior MUST be documented explicitly in user-facing runtime docs and
  examples

CURRENT GAP:

- `UPDATE` followed by `ADD_COMPONENT` for the same entity in one drain cycle
  does not currently compose intuitively. The second command reads from `_live`
  rather than from the staged update row, so command order and final
  materialized state can diverge.

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

The runtime must not make deterministic testing harder.

Required behavior:

- Tests must be able to create isolated runtime instances without inheriting
  process-global state from previous tests
- Global singletons, if used at all, must have an explicit reset or opt-out
  path for tests
- Test suites must be able to exercise multiple runtimes in one process

#### L6. Actor-bound aliases share one world lifecycle

Runtime may expose multiple actor-bound handles to one logical world, but those
handles must not become independent world lifecycles by accident.

Required behavior:

- `world.as_actor(ctx)` MUST be pure before activation
- Actor-bound aliases MUST resolve to the same backing world identity after
  activation
- First activation MUST remain single-flight across all aliases of the same
  logical world
- Shutting down one alias MUST invalidate all aliases of that world, but MUST
  NOT invalidate sibling worlds in the same runtime
- `fork()` from an actor-bound alias SHOULD preserve the caller's actor binding
  on the returned fork handle

### Script Ceremony Contract

#### S1. Minimal ceremony, explicit boundary

The runtime API should reduce ceremony for scripts, but execution boundaries must
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

Top-level runtime exports must not silently redefine long-standing public imports.

Required behavior:

- Existing default exports such as `World` and `Processor` must remain stable
  unless changed as part of an explicit breaking release
- If new runtime types are introduced, prefer additive names first
- Any future alias swap requires migration notes and compatibility tests

#### S5. Ergonomics must not bypass governance

Script ergonomics must not come from removing safety mechanisms.

Required behavior:

- If runtime claims to preserve RBAC, audit history, or command semantics, those
  paths must actually flow through the governing services
- If a method intentionally bypasses governance, that bypass must be explicit in
  naming and documentation
- Direct resource mutation must not be described as governed by the broker

#### S6. Recommended runtime APIs should be mutation-complete

If a runtime wrapper is presented as the recommended script boundary, it should
cover the common governed mutation verbs without forcing the user to drop to
the service layer.

Required behavior:

- The recommended runtime world handle SHOULD expose gated entity mutation
  verbs for `spawn`, `despawn`, `update`, `add_components`, and
  `remove_components`
- The recommended runtime world handle SHOULD expose gated processor
  mutation verbs for `add_processor` and `remove_processor`
- Runtime audit access such as audit history SHOULD remain available without
  requiring direct container access

#### S7. Declarative scaffolding must remain explicit

Some runtime operations are declarative handle construction rather than
governed simulation mutations. That distinction must be explicit.

Required behavior:

- World-handle construction and actor rebinding may be immediate runtime
  operations rather than gated commands
- Activation, hook registration, resource attachment, mutation, simulation,
  read, fork, and destroy operations MUST flow through `iCommandService`
- Documentation MUST distinguish handle construction from gated operations

### Runtime Acceptance Criteria

No runtime API may be considered ready for implementation until the design can
show how it satisfies all of the following:

- Concurrent first-use of the same wrapper creates exactly one world
- `spawn()` return semantics are correct and tested
- Actor-bound aliases are pure before activation and share one world identity
- One world's shutdown does not break a sibling world in the same runtime
- Runtime teardown is explicit and distinct from world teardown
- Forked worlds remain valid after the source world is shut down
- Recommended runtime mutation verbs cover entity, component, and processor
  mutations without dropping to the service layer
- Gate-preserving scaffolding boundaries are documented and tested
- Same-entity same-tick mutation composition is either guaranteed and tested or
  explicitly documented as weaker
- Async and sync script entry points have a clear resource ownership model
- Existing public imports remain compatible, or the change is explicitly marked
  as breaking and tested accordingly

### Non-Goals

This section does not choose the final user-facing API names. It establishes
the constraints that any acceptable design must satisfy.

## Idempotency Matrix

| Operation | Expected contract |
|---|---|
| `StorageService.get_or_create_store(key)` | Idempotent per `(uri, namespace, backend, cache config)` within one service instance |
| `WorldService.create_world(world_id=X)` | Idempotent by explicit `world_id` |
| `WorldService.destroy_world(missing)` | Safe no-op |
| `AsyncCachedStore.shutdown()` | Idempotent |
| `CommandBroker.enqueue()` | Not idempotent; duplicate logical commands remain distinct |
| `CommandService.submit()` | Not idempotent; duplicate submits create duplicate commands |
| `CommandService.submit_spawn()` | Returns one reserved `entity_id` per successful call; repeated calls create new entities unless the caller reuses an explicit reservation |
| `AsyncWorld.create_entity()` | Not idempotent; each call allocates a new world-local entity ID |
| `AsyncWorld.remove_entity(missing)` | Safe no-op with observability |
| `RuntimeWorld.as_actor(ctx)` | Idempotent as handle binding only; creates another alias, not another world |
| Duplicate despawn in one tick | Idempotent collapse by entity ID |
| Duplicate spawn for same entity in one tick | Deterministic last-write-wins |
| `RuntimeWorld.history()` | Idempotent for fixed audit history |
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
3. ~~Decide whether command submission to an unknown world is allowed; if not,
   reject at submit time.~~ Resolved: `CommandService.submit*` raise
   `archetype.app.errors.WorldNotFoundError` before any side effect.
4. Fix `WorldService.create_world()` duplicate-name failure ordering so failed
   creation does not cache a hidden world.
5. Align hook documentation and implementation for spawn/despawn lifecycle
   events.
6. Give `QueryService` a real read contract or clearly mark it as provisional.
7. Define world-local teardown semantics that do not leak broker or shared
   runtime state.
8. Resolve or explicitly codify same-entity same-tick mutation composition so
   broker command order and final materialized state cannot diverge silently.

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

## Runtime Boundary

The runtime boundary separates process lifetime from world lifetime. This
prevents the user-facing API from collapsing three separate concerns:

1. **Concurrency** — first-use initialization races
2. **Multi-world lifetime** — world shutdown vs process/runtime shutdown
3. **Script ceremony** — making simple scripts ergonomic without hiding real
   lifecycle boundaries

The safe top-level abstraction is `ArchetypeRuntime`, not a world-scoped
context manager. A world handle can be lazy, but the shared runtime/container
needs an explicit boundary.

### Runtime Contracts

- `spawn()` must reserve and return a real `entity_id` all the way through the
  chain. Returning a command ID is a contract violation.
- World-handle construction must be pure: no I/O, no registration, no backend
  allocation.
- First activation must be single-flight. Concurrent first calls must produce
  exactly one backing world.
- A world must never expose partially initialized state.
- Shutdown, fork, and activation must have defined race behavior.
- World shutdown must be world-local.
- Runtime shutdown must be process-scoped and explicit.
- Forked worlds share a runtime, but not world identity or lifecycle.
- The recommended script boundary is `async with ArchetypeRuntime()` or
  `with ArchetypeRuntime.sync()`, not implicit per-call global setup/teardown.
- Top-level `World` and `Processor` exports should remain stable unless there
  is an intentional versioned breaking change. Add runtime ergonomics additively first.

### Contract Tests

These contracts should not live only in docs. They need executable tests.

High-value contract tests include:

- concurrent first-use activation
- shutdown vs init and fork vs init races
- multi-world lifetime isolation
- spawn materialization timing
- async/sync smoke paths
- example script smoke execution

### Sync-Core Coverage

Contract-focused tests should cover correctness issues that happy-path tests
often miss:

- store append/read consistency across table lookup and namespace context
- query projection schema selection
- duplicate spawn last-write-wins behavior
- component migration between signatures
- moving an entity from a missing source archetype
- despawn-only signature materialization

If a contract test feels "too specific," it may be testing a real semantic
boundary.

### Docs and examples are part of the contract

The recommended public API now lives at the runtime layer, so beginner docs
and quickstarts must teach `ArchetypeRuntime`, not the lower-level service
container. Low-level docs can still document `ServiceContainer`,
`CommandService`, broker semantics, audit semantics, and raw ECS flows, but they should be
explicit that they are lower-level interfaces.

Examples also need to be executed in CI. An example that "looks right" but is
never run is not documentation; it is an unverified claim.

LLM-backed examples need explicit credential gating or graceful degraded
behavior when keys are missing.

### Specification Ownership

Focused specification pages are now the source of truth for their areas, with
this page serving as the umbrella entry point. Tests enforce the contracts and
contributor docs point back to the specification group.
