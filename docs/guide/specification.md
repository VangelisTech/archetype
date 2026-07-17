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
| [Durable Discovery](durable-discovery.md) | Control catalog and cold reads | Catalog authority, `discover_worlds`/`open_world_readonly`, fail-closed cold queries. |
| [Atomic Visibility](atomic-visibility.md) | Tick commit identity | Manifest-published ticks, commit tokens, writer fencing, epoch-0 legacy reads. |
| [Durable Facts](durable-facts.md) | External-fact ingestion | Typed Iceberg tables, Daft file processors, content identity, and claim-backed receipt compatibility. |
| [Dataset and Evaluation Ontology](dataset-eval-ontology.md) | Dataset/eval identity and vocabulary | Dataset-vs-runtime coordinates, trial/episode cardinality, typed-fact ownership, and grader composition. |
| [Audit Log](audit-log.md) | Audit rows | Append-only audit history and query contract. |
| [`tests/app/test_runtime_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_contracts.py) | Executable runtime contracts | Enforces activation single-flight, runtime-vs-world lifetime, fork isolation, spawn visibility, governance, and smoke paths. |
| [`tests/app/test_runtime_fork_storage.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_fork_storage.py) | Runtime fork storage contracts | Enforces fork storage inheritance through the runtime layer, lineage reads on fork handles, fork run_id minting, and gate-side storage resolution. |
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
- typed external facts and dataset/evaluation identity

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
- `"type"` is reserved payload metadata; component subclasses MUST NOT
  declare it as a model field.
- Untyped payload dicts MUST fail loudly rather than silently degrading to the
  base `Component`.

### Archetype signatures

- `ArchetypeSignature` MUST be canonicalized as a sorted tuple of component
  types.
- Signature identity is order-invariant. `(A, B)` and `(B, A)` describe the
  same archetype.
- The base persisted columns for every archetype row are:
  `world_id`, `run_id`, `entity_id`, `tick`, `is_active`, and — since the
  atomic-visibility amendment ([Atomic Visibility](atomic-visibility.md)) —
  `commit_token` and `writer_epoch`. v0.2 tables without the commit columns
  remain readable as implicit epoch-0 history under their legacy table ids.
- Component projections exclude the commit columns; raw archetype reads
  expose them.

## Store Contracts

- The store MUST be append-only. Updating a world means appending new rows, not
  mutating prior rows in place.
- Archetype's built-in Iceberg factory MUST create only its concrete local
  SQLite-catalog lakehouse. It MUST NOT pair remote data with a hidden
  host-local metadata catalog.
- Remote or managed Iceberg MUST enter through a caller-configured Daft
  `Session`. `StorageConfig.io_config` is passed directly to Daft data I/O and
  MUST NOT be translated into catalog credential properties.
- A store MUST create archetype tables on demand from the signature schema.
- Store reads MUST be scoped by `world_id` and `run_id`.
- Store writes MUST resolve through the same table identity as reads for the
  same signature. A write path may not silently drift to a different table
  lookup mechanism.
- Registered signatures and committed signatures are distinct. Read-created
  tables appear only in `list_signatures()`; `list_committed_signatures()` adds
  a signature only after the store accepts an append. Cached-store receipts
  separately report whether those accepted rows are already durable.
- The store itself MUST NOT impose active-state semantics; `is_active` and
  historical filtering belong above the raw store.
- Empty appends SHOULD be safe no-ops.
- Store shutdown SHOULD be safe to call more than once.

Idempotency:

- Store `append()` is not idempotent. Repeating the same append writes duplicate
  rows unless a higher layer deduplicates.
- Store `get_archetype_df()` is idempotent for the same persisted data.
- Cached-store shutdown MUST be idempotent even if called multiple times.

Failure observability:

- A failed append MUST raise to the caller. Stores log the failure and
  re-raise; they MUST NOT return as if the write happened. Empty appends
  (zero rows or empty schema) remain safe no-ops.
- Contract tests: `tests/core/test_async_store_updater_failures.py`.

## Querier Contracts

- The querier is the active-state read facade over the store.
- The querier MUST read through the store and then apply:
  `is_active == true`, optional tick filters, optional entity filters, and
  optional component projection.
- Component projection MUST use the canonical schema column list for the
  requested component set.
- Exact-signature guards and component discovery MUST use committed signatures.
  A world adds its live signatures separately so pending spawns remain queryable.
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

Failure observability:

- The updater MUST raise when the store append fails. Persistence success is
  observable: a returned DataFrame means the rows were committed. A
  schemaless empty frame is skipped as a no-op before stamping.
- Contract tests: `tests/core/test_async_store_updater_failures.py`,
  `tests/sync/test_sync_stack_contracts.py::test_sync_update_manager_raises_on_store_errors`.

## System and Processor Contracts

- A processor is a `DataFrame -> DataFrame` transform over one archetype at a
  time.
- A processor MUST declare the component set it depends on.
- A processor matches a signature when its required component set is a subset
  of the archetype signature.
- Within one archetype, processors MUST execute in ascending `priority`.
- Across different archetypes, execution MAY proceed concurrently.
- Processor registration is instance-based; removal is type-based.
  `remove_processor(ProcessorType)` removes every registered instance of that
  type, and removing a type with no registered instances is a no-op. Sync and
  async stacks share this contract.
- Only kwargs explicitly accepted by a processor should be passed through.
- Shared resources MAY be injected through the world resource container.

Failure policy:

- Processor failures MUST NOT silently corrupt world bookkeeping.
- The step is two-phase: every archetype's tick frame is computed (no
  writes, no cache consumption) before any archetype appends. A processor
  failure therefore fails the WHOLE tick: the error is logged, `step()`
  raises, the tick counter does not advance, nothing is appended for any
  archetype, and staged mutations survive for retry.
- A store failure during the commit phase preserves the failed archetype's
  staged mutations; archetypes whose appends committed consume their caches
  with the append.
- Contract tests: `tests/core/test_async_world_error_propagation.py`
  (`test_async_world_processor_error_fails_the_step`,
  `test_failed_tick_commits_nothing_and_is_retryable`,
  `test_one_failing_archetype_blocks_all_appends`).

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
   - apply staged despawns to the existing population
   - execute processors over the existing population
   - concat staged spawn rows, raw
   - persist through the updater
4. replace the live snapshot with active rows only
5. increment the world tick
6. fire `PostTick` hooks

### Initial conditions

- An entity's first persisted row is its raw spawn values at the tick it
  materializes. Processors first apply on the following tick.
- Formally: `x_0` is given; `x_{t+1} = f(x_t)`. The ledger contains the full
  sequence `x_0, f(x_0), f^2(x_0), ...` — initial conditions included.
- The same semantics apply to staged overlays: `update_entity`,
  `add_components`, and `remove_components` re-insert the mutated row, so
  the mutated values persist raw at their materialization tick and are
  first transformed on the following tick. An overlay is new given state:
  the engine records what was set before the dynamics resume.
- Contract tests: `tests/core/test_initial_conditions_contract.py`.

### Previous-state reads

- Between ticks, the live snapshot is the authoritative in-memory view of the
  latest completed tick.
- Store-backed reads are the durability path.
- `get_components()` is a live-snapshot API, not a historical store query.

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
  state plus the requested mutation. Latest visible state is the entity's
  same-tick staged spawn row when one exists, otherwise its last persisted
  row; a consumed staged row MUST NOT also materialize under the old
  signature.
- When migration materializes into an existing DataFrame, staged spawn rows MUST
  be cast or otherwise normalized to the target schema before concat.
- Adding already-present components or removing already-absent components SHOULD
  be a no-op.

### Mutation materialization

- Duplicate despawns for the same entity in one tick MUST collapse.
- Duplicate spawns for the same entity in one tick MUST resolve
  deterministically.
- Spawn rows legitimately staged under the same signature resolve
  last-write-wins by entity ID within the tick.
- A tick-deferred spawn carrying an explicit reserved entity ID MUST use the
  guarded `spawn_with_reserved_id` mutation path. Once that ID is registered,
  a replay is rejected and cannot replace the first staged spawn.
- Despawn-only signatures MUST still be processed during the next tick, even if
  no active entities remain in that archetype after bookkeeping updates.

- `AsyncWorld._move_entity()` returns an empty row only when the entity has
  neither a staged row nor a persisted row; `update_entity`,
  `add_components`, and `remove_components` treat that as a logged no-op and
  stage nothing. Contract tests:
  `tests/core/test_same_tick_mutation_composition.py`.

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

### Service error taxonomy

- Public cross-service error contracts MUST live in `archetype.app.errors`.
  Private service implementations subclass those contracts; transport adapters
  MUST NOT import private implementation modules to classify failures.
- The REST adapter maps `WorldNotFoundError` to HTTP 404, `ConflictError` to
  HTTP 409, and `AvailabilityError` to HTTP 503. Conflict and availability
  responses expose only the contract's client-safe `public_detail`; internal
  exception text remains server-side. App services remain transport-agnostic
  and do not depend on HTTP.
- Errors without a public client-recovery contract fail closed as HTTP 500.
  `CatalogSchemaMismatchError` is an integrity failure in this category; its
  internal detail MUST NOT be exposed to the client. This includes a durable
  control catalog whose schema version is newer than the running build.

### StorageService

- `StorageService` is the multiton owner for backend triplets:
  `(store, querier, updater)`.
- Worlds sharing the same effective storage pool key `(uri, namespace, backend,
  Daft IOConfig fingerprint, cache config)` MUST reuse the same backend
  triplet.
- Concurrent backend acquisition for the same key MUST single-flight so only
  one backend is built.
- Backend selection and storage-resource construction are app/runtime
  composition concerns.
- Core stores MUST receive backend-native inputs rather than a generic runtime
  storage context.
- The default catalog-backed path MAY construct a Daft `Session` and Daft
  `Catalog` through `StorageService`.
- A caller-configured `Session` MUST pass through unchanged; its attached
  catalog, namespace, and catalog credentials are authoritative.
- One injected `Session` MUST be bound to one storage URI and namespace.
  `StorageService` MUST reject a mismatched config rather than mutate the
  shared session namespace or silently route tables to the wrong namespace.
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
- Duplicate-name validation MUST happen before a new world is inserted into the
  live registry. A rejected create MUST leave both the ID and name indexes
  unchanged.
- If durable catalog registration or writer-fence acquisition fails after
  construction, `create_world()` MUST remove the new live world before
  propagating the failure.
- Broker injection into world resources is an app-layer responsibility.
- `destroy_world()` SHOULD be safe to call on a missing world.
- `fork_world()` MUST create a new `world_id`, clone the source world's visible
  state, and let source and fork diverge independently.
- Forking MUST transfer pending spawn/despawn caches so spawn-then-fork before
  the next tick materializes in both worlds.

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
- Generic deferred submission MUST accept only commands with a tick-boundary
  dispatcher, plus the intentional `MESSAGE`, `CUSTOM`, and `QUERY_WORLD`
  application envelopes. All other command types MUST be rejected before quota
  debit, audit emission, or broker enqueue.
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
- Archetype and component reads MUST resolve storage per call and query durable
  rows by `world_id` and `run_id`; they do not require the world to be live in
  the process registry.
- Coordinated reads MUST restrict results to catalog-published commit tokens.
- Fork-aware reads MUST compose persisted lineage segments with the fork's own
  rows without requiring a live source world.
- `get_lineage()` reads persisted ancestry, and `list_signatures()` reads the
  selected store's registered archetypes.
- Audit history is served by `iAuditLog` through `iCommandService`.
  `QueryService.get_command_history()` remains a compatibility read over queued
  audit rows, not an in-memory broker-history contract.

### ServiceContainer and runtime lifetime

- `ServiceContainer` is the process-scoped composition root.
- It owns one shared `CommandBroker`, one append-only audit log, and the world,
  mutation, command, simulation, and query services built on top of them. It
  owns a `StorageService` that it creates and borrows one supplied by a caller.
- Container shutdown MUST be explicit and distinct from per-world removal.
- Container shutdown order MUST clear broker state, flush/shut down audit, and
  then shut down storage the container owns. An injected `StorageService` MUST
  remain open for its caller to manage.

## Multi-World Contracts

- Multiple worlds may coexist in one runtime.
- Worlds MUST be isolated by `world_id`.
- Storage rows are scoped by both `world_id` and `run_id`.
- Broker queues are partitioned per world key.
- A fork shares runtime infrastructure, but not world identity.
- Shutting down or destroying one world MUST NOT invalidate sibling worlds that
  share the same runtime.
- The gated `CommandService.destroy_world()` path MUST clear the target world's
  pending and historical broker state before delegating world removal.
- World removal MUST preserve durable rows, lineage, audit history, shared
  storage backends, and sibling-world state. The durable catalog records the
  world as destroyed rather than deleting its identity.

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

Resolved: same-tick mutations compose. A later mutation for the same entity
bases its row on the earlier staged spawn row (consuming it) rather than the
last persisted tick, so command order and final materialized state agree.
Contract tests: `tests/core/test_same_tick_mutation_composition.py`.

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
| `StorageService.get_or_create_store(key)` | Idempotent per `(uri, namespace, backend, Daft IOConfig fingerprint, cache config)` within one service instance |
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
| Duplicate staged spawn rows for the same entity in one tick | Deterministic last-write-wins at materialization |
| Replay of an already-registered reserved spawn through `CommandService` | First spawn applies; replay is rejected |
| `RuntimeWorld.history()` | Idempotent for fixed audit history |
| `add_components()` with no signature change | Idempotent no-op |
| `remove_components()` with no signature change | Idempotent no-op |
| `world.step()` | Not idempotent; advances tick and appends new rows |
| `world.run()` | Not idempotent; performs multiple steps under one run contract |
| `QueryManager.query_archetype()` | Idempotent for fixed persisted state |
| Store `append()` replay | Not idempotent; repeating an append persists duplicate rows |
| Updater `update()` replay | Not idempotent; repeating an update appends another row version |
| Store `get_archetype_df()` replay | Idempotent for the same persisted data |
| `QueryService` fixed-state reads | Idempotent for fixed rows, history, and signature catalog |
| Catalog re-registration | Same identity and content is an idempotent no-op; different content conflicts loudly |
| Coordinated tick retry after failed publish | Unpublished attempts stay invisible; retry produces exactly one visible attempt |
| Cold discovery and reads | Repeated cold discovery and reads return stable durable state |
| Fenced mutable resume | Resume continues from the last visible tick and stale-writer retries stay invisible |
| `ingest_fact()` replay | Identical external identity and payload converges on one visible fact; changed payload conflicts |
| `ingest_fact()` crash recovery | Lease takeover completes an appended orphan without creating a second visible fact |
| `evaluate()` replay | Same evaluation identity, subject, and contract returns one receipt without re-grading |
| Hard process crash and cold resume | Unpublished physical rows do not advance a fresh process beyond the last visible tick |
| Independent writer-process race | Exactly one fenced writer publishes the contested tick |
| Independent process `ingest_fact()` replay | Concurrent processes converge on one visible external fact |
| Independent process `evaluate()` replay | Concurrent processes grade once, and changed subjects conflict before grading |

The durable rows above summarize the normative amendments in
[Durable Discovery](durable-discovery.md),
[Atomic Visibility](atomic-visibility.md),
[World Lifecycle](world-lifecycle.md), and [Durable Facts](durable-facts.md).
The idempotency eval manifest must match this table exactly; `make
idempotency-audit` is the fast static drift check, while `make eval-idem`
executes the behavioral scenarios.

The behavioral suite is an independent oracle: it does not call or import the
feature tests under `tests/app/`. Each task builds fresh storage and asserts
primarily on service-facing outcomes and durable query results. Deterministic
fault injection targets only the documented manifest-publish and
claim-completion boundaries; selected internal counters provide secondary
failure diagnostics. Coverage is deliberately symmetric: repeated no-ops must
collapse, non-idempotent simulation calls must remain distinct, concurrent
durable submissions must converge, interrupted commits must recover, and
identity reuse with changed content must fail loudly. A new matrix row
therefore requires a registered behavioral task, and a new task must trace
back to at least one matrix row.

Four tasks cross real OS-process boundaries over shared LanceDB and SQLite
files: hard process death followed by cold resume, a two-writer fence race,
eight-process fact replay, and eight-process evaluation replay with an
external grader-call ledger. The `infrastructure-idempotency` GitHub Actions
job creates a disposable Iceberg table under a unique Cloudflare R2 object
storage prefix and runs the integration under `tests/infrastructure/`; local
development does not require Docker. The catalog metadata is isolated in a
runner-local SQLite database, so this gate proves Archetype/Daft/PyIceberg R2
I/O and commit visibility, not the Cloudflare Data Catalog control plane.

## Required Hardening Work

This register retains stable item numbers used by issues and tests. `Open`
means the contract is not yet implemented; `Resolved` requires both shipped
behavior and an executable oracle.

| Item | Status | Contract or remaining work | Oracle or tracking |
|---|---|---|---|
| 1 | Resolved | Async and sync updater/store failures raise instead of returning a stamped-but-uncommitted frame. | `tests/core/test_async_store_updater_failures.py`; `tests/sync/test_sync_stack_contracts.py` |
| 2 | Resolved | Tick-deferred submission is allowlisted to dispatched commands and intentional application envelopes; all direct operations fail before quota, audit, or enqueue. | `tests/integration/test_command_flow.py::test_direct_only_commands_cannot_enter_tick_deferred_broker`; Issues #368, #415, #418 |
| 3 | Resolved | `CommandService.submit*` reject an unknown world with `WorldNotFoundError` before quota, enqueue, or audit side effects. | `tests/integration/test_command_flow.py::test_submit_to_unknown_world_rejected` |
| 4 | Resolved | Duplicate-name and catalog-registration failures leave no hidden live world. | `tests/core/test_orchestrator_errors_and_instrumentation.py`; `tests/app/test_durable_discovery.py::test_failed_catalog_registration_leaves_no_live_world` |
| 5 | Resolved | Spawn, despawn, and component migration hooks fire from their public mutation paths with the documented queue-time semantics. | `tests/core/test_resources_hooks_messaging.py`; `tests/core/test_batch_spawn_contract.py`; `tests/sync/test_sync_world.py` |
| 6 | Resolved | `QueryService` performs durable archetype, component, lineage, signature, and audit-backed history reads. | `tests/app/test_atomic_visibility.py`; `tests/app/test_runtime_fork_storage.py`; `tests/app/test_audit_contracts.py` |
| 7 | Resolved | Gated destroy clears only the target world's broker state and preserves shared runtime and durable state. | `tests/integration/test_fork_destroy_contracts.py` |
| 8 | Open | Make same-entity, same-tick mutations compose in broker order or explicitly codify weaker behavior. | Issue #193 |

## Durability Posture (v0.3, issue #276)

The durable substrate (#272–#275) gives each remaining self-assessment an
explicit contract. Every subsystem below is either durable by mechanism or
deliberately advisory with the durable alternative named — none is an
undocumented gap.

| Subsystem | Contract |
|---|---|
| CommandBroker | **Advisory by contract**: tick-boundary batching for live worlds; in-memory, non-deduplicating, drain failures log. Durable external data belongs in typed fact tables; callers needing claim-backed cross-process deduplication retain legacy `ingest_fact` ([Durable Facts](durable-facts.md)). Routing either requirement through the broker is a misuse, not a gap. |
| AuditLog | **Advisory durability class** in v0.3: bounded batches append to a dedicated Iceberg `audit_rows` table, and `_emit` never raises (an audit failure must not fail the gated operation). A full batch that cannot flush rejects new rows with observable backpressure instead of growing memory without bound; lossless audit during an outage requires deployment-level admission control. Typed facts and claim-backed receipts remain the durable evidence boundaries. Compaction is a separate storage-maintenance feature. |
| Mutation idempotency | **Simulation mutations stay non-idempotent by design** — `create_entity` twice is two entities, because spawns are simulation events, not external ones. Typed fact writes deduplicate logical keys within their serialized writer; legacy `ingest_fact` supplies claim-backed cross-process deduplication. Deterministic replays use reserved entity ids. |
| API auth | **Development-grade by contract**: the default admin `ActorCtx` is for the reference deployment. A production front must inject authenticated `ActorCtx` (roles resolved by the deployment's identity layer) and never expose the default. |
| RBAC quota state | **Process-local and advisory** in v0.3 (daily token budgets reset on restart). Durable quota accounting is a control-catalog follow-up; deployments needing hard budgets enforce them at the identity layer above. |

Receipts and facts carry no authority (enforced by `spec.receipt_authority_firewall`); the audit log records who asked, the ledger records what happened, and every durable guarantee in this table traces to the visibility contract in [Atomic Visibility](atomic-visibility.md).

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
