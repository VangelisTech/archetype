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
| [Runtime](runtime.md) | Trusted script boundary | `ArchetypeRuntime`, `RuntimeWorld`, sync parity, lifecycle, and actor-free application access. |
| [Observability](observability.md) | Safe advisory signals | Vendor-neutral trace/metric vocabulary, bounded failure semantics, context, and process-host provider ownership. |
| [Application Architecture](application-architecture.md) | Supported boundaries and dependency policy | Normative current ownership plus the accepted v0.5 target family DAG, composition, encapsulation, and lint inputs. |
| [Service Protocols](service-protocols.md) | Internal application ports | Active family interfaces behind `iRuntimeApplication` and `iCommandGateway`. |
| [Command Gate](command-gate.md) | Authorization and roles | Four-role model, permissions matrix, audit emission shape. |
| [Execution Hierarchy](execution-hierarchy.md) | Step/run/episode/rollout | Simulation levels and rollout fork semantics. |
| [World Lifecycle](world-lifecycle.md) | Create/fork/destroy | Append-only lifecycle, info-class downgrade, fork sharing/copy rules. |
| [Durable Discovery](durable-discovery.md) | Control catalog and cold reads | Catalog authority, `discover_worlds`/`open_world_readonly`, fail-closed cold queries. |
| [Atomic Visibility](atomic-visibility.md) | Tick commit identity | Manifest-published ticks, commit tokens, writer fencing, epoch-0 legacy reads. |
| [Artifacts](artifacts.md) | External-artifact ingestion | App-layer Daft Catalog registration, typed Iceberg tables, file/media scans, occurrence identity, and content-addressed objects. |
| [Agent Missions V1](agent-missions.md) | Coding-agent software factory | Typed task graphs, revision-bound validators, immutable candidates, independent exact-head critic receipts, durable repair findings, and terminal mission rollup. |
| [Dataset and Evaluation Ontology](dataset-eval-ontology.md) | Dataset/eval identity and vocabulary | Dataset-vs-runtime coordinates, trial/episode cardinality, typed-ingestion ownership, and grader composition. |
| [Audit Log](audit-log.md) | Audit rows | Append-only audit history and query contract. |
| [Repository Harness](repository-harness.md) | Executable evidence | Focused tests, contract matrices, repository scenarios, benchmarks, static audits, and mutation probes. |
| [`tests/app/test_runtime_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_contracts.py) | Executable runtime contracts | Enforces activation single-flight, runtime-vs-world lifetime, fork isolation, spawn visibility, the actor-free trust boundary, and smoke paths. |
| [`tests/storage/test_runtime_fork_storage.py`](https://github.com/VangelisTech/archetype/blob/main/tests/storage/test_runtime_fork_storage.py) | Runtime fork storage contracts | Enforces fork storage inheritance through the runtime layer, lineage reads on fork handles, fork run_id minting, and gate-side storage resolution. |
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
  ergonomics may improve, but trust boundaries, lifecycle, and scheduler timing
  must remain explicit.
- Deferred spawn contracts:
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
- `ACCEPTED TARGET` marks ratified migration behavior that is normative for its
  owning slice but is not a claim about the current implementation. The slice
  must land its executable oracle with the behavior.

## Scope

This specification covers:

- component and archetype identity
- store, querier, updater, system, and world contracts
- mutation materialization and world lifecycle events
- gated command flow in the application layer
- top-level runtime API constraints
- multi-world orchestration and world forking
- idempotency expectations and non-idempotent boundaries
- typed external artifacts and dataset/evaluation identity
- typed coding-agent task graphs, committed dispatch, and validator-gated transitions

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
| `RuntimeApplication` | Actor-free application facade shared by trusted runtime and authorized gateway paths |
| `Runtime` | Trusted scripting facade and process-lifetime owner |

## Layer Boundaries

Core execution is composed from a store-backed querier and updater, a system,
and a world. The application layer owns actor-free product semantics through
`RuntimeApplication` and its world, storage, query, artifact, evaluation,
commands, audit, and research families.

Trusted Python scripts use `ArchetypeRuntime -> RuntimeApplication`. Untrusted
clients use `CLI/HTTP -> API authentication -> CommandGateway authorization ->
RuntimeApplication`. The CLI is an HTTP client except for server startup.
Lower packages never depend on their outer consumers.

The complete allowed service edges, composition rules, and public/internal
classification are normative in
[Application Architecture](application-architecture.md). Diagrams are
explanatory views of those written rules.

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
- A processor's component declaration is a match predicate, not a component
  mutation. Widening and narrowing flow through the world mutation API. The
  carried row materializes under its target signature on the migration tick;
  processors newly matched by that signature first transform it on the next
  tick.
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
- The public failure type is `archetype.core.errors.TickExecutionError`, a
  `RuntimeError` subclass (issue #444). `failures` MUST preserve every failed
  table identity (`table_id`) and original exception object, in ascending
  table-id order; `phase` MUST be `"compute"` or `"commit"`. Sync and async
  stacks share this contract; the async stack chains the originals as an
  `ExceptionGroup` cause, the fail-fast sync stack chains its single original
  directly. Task cancellation MUST propagate unwrapped.
- The aggregate's message names failed tables and the phase only; original
  exception text MUST NOT enter it. Callers distinguish a provider timeout or
  rate limit from a processor bug by `isinstance` on `failure.error`, never
  by parsing message text.
- Runtime and app layers propagate `TickExecutionError` unchanged. The REST
  adapter has no public client-recovery contract for it and fails closed as
  HTTP 500 with a redacted detail
  (`tests/api/test_errors.py::test_tick_execution_error_remains_a_redacted_internal_error`).
- Contract tests: `tests/core/test_async_world_error_propagation.py`
  (`test_async_world_processor_error_fails_the_step`,
  `test_failed_tick_commits_nothing_and_is_retryable`,
  `test_one_failing_archetype_blocks_all_appends`,
  `test_step_preserves_ordered_structured_compute_failures`,
  `test_step_preserves_ordered_structured_commit_failures`,
  `test_step_does_not_wrap_task_cancellation`);
  `tests/sync/test_sync_stack_contracts.py::test_sync_world_processor_error_fails_step_without_commit`;
  `tests/app/test_runtime_contracts.py::TestStructuredStepFailures`.
- Composed public-boundary evidence: the `processor_adversarial` capability
  eval combines advisory hook failures, one-table processor failure, atomic
  retry, and signature-aware matching across component migrations.

Idempotency:

- Processor execution is only idempotent if the processor itself is pure with
  respect to the input DataFrame and resources. The engine does not guarantee
  semantic idempotency for arbitrary processors.

## World Contracts

### World state ownership

A world owns:

- the `world_id` and human-readable name
- one immutable UUIDv7 `run_id`
- entity-to-signature bookkeeping
- the next world-local entity ID counter
- staged spawn/despawn caches
- the live in-memory active snapshot
- lifecycle hooks
- the system, querier, and updater integration

### World execution order

One tick MUST follow this order:

1. materialize due durable commands against the exact already-locked world
2. fire `PreTick` hooks
3. determine active signatures from live state plus staged mutations
4. compute every signature without writing:
   - load previous state
   - apply staged despawns to the existing population
   - execute processors over the existing population
   - concat staged spawn rows, raw
5. append every computed frame, flush durable storage, then publish the tick
   manifest and command settlement
6. if the publish response is uncertain, retain the exact prepared commit and
   reconcile it before admitting any mutation or later tick; exact-token
   recovery releases only coordinator-local staging and MUST NOT replay
   materialization, processing, append, or a second fenced catalog write
7. consume staged mutations and replace the live snapshot with active rows
8. increment the world tick and record the manifest-bound
   `CommittedTickReceipt`
9. fire advisory `PostTick` hooks
10. return the already-recorded receipt

Managed simulation retains that receipt until its one required projector
acknowledges the exact receipt identity. Projection occurs after commit and
outside advisory hooks. A projection failure does not roll back or replay the
tick, and its retained receipt MUST be retried before another managed tick.
Caller cancellation after publication cannot discard the recorded receipt:
the managed boundary retains it before propagating cancellation. The required
projector executes under exact-world authority and may only persist
deterministic, idempotent intent; provider and sandbox I/O belong to a
downstream resource consumer outside the world lock.

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

- A world owns one active `run_id`. A new world mints it at construction,
  mutable resume restores it, and a fork mints a fresh UUIDv7 identity for its
  new lineage. Managed lifecycle registers the final identity before it binds
  the writer coordinator and constructs the world.
- `RunConfig` contains execution policy only. It cannot supply or replace a
  world's identity, and `world.run_id` is read-only.
- `world.run(run_config)` MUST stamp the world's active `run_id` across every
  tick in the call and across repeated calls on that world.
- Query defaults that rely on the current run SHOULD use the world's active
  `run_id`.

## Mutation Contracts

### Spawn

- `create_entity()` creates a new world-local entity ID and stages a spawn row.
- The entity does not become part of the live active snapshot until the next
  materialization boundary.
- When the app layer reserves an entity ID before enqueue, the same entity ID
  MUST survive reserve -> durable admission -> lease -> apply -> materialize.

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
- A required post-commit projector is a separate managed-world port. It is
  idempotent by `(consumer_name, receipt.identity)`, is never registered in the
  hook bus, persists only deterministic intent under exact-world authority,
  and reports failure as committed-but-unprojected work. Provider or sandbox
  I/O MUST NOT execute through this port.

## Application Layer Contracts

[Application Architecture](application-architecture.md) owns service placement
and dependency direction. Concrete services and `ServiceContainer` are internal
implementation machinery. The target policy boundary is `CommandGateway :
iCommandGateway` for untrusted ingress and `RuntimeApplication :
iRuntimeApplication` for actor-free application execution.

### Service error taxonomy

- Public cross-family error contracts MUST live in `archetype.errors`.
  `archetype.app.errors` is an identity-preserving compatibility re-export
  until the application-facade teardown. Private implementations subclass the
  canonical contracts; transport adapters MUST NOT import private
  implementation modules to classify failures.
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

- Physical storage authority lives in the top-level `archetype.storage`
  family. Temporary `archetype.app.storage` imports MUST preserve object
  identity and MUST NOT contain a second implementation.
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
- Control-catalog bootstrap MUST be captured once in an immutable
  `ControlCatalogConfig`. `ServiceContainer` may call `from_env()` while
  composing its owned service; ordinary storage/catalog operations MUST NOT
  reread ambient application configuration.
- `pin_visibility()` MUST return an immutable world/run manifest-token
  snapshot. `scan_visible_world_rows()` MUST apply only physical world/run,
  manifest-token, and optional maximum-tick filtering; entity liveness,
  same-tick active/inactive resolution, component ownership, lineage meaning,
  resumed tick, and next entity ID remain world-family rules.
- `append_world_rows()` and `read_world_rows()` MUST resolve the durable
  world/run from the control catalog. Callers cannot supply those envelope
  columns, and conditional keys MUST be extended with both coordinates.
- A commit coordinator is construction-bound to one world, run, and writer
  epoch. Its write methods do not accept caller-supplied world/run identity,
  and publication MUST reject a context from another writer epoch before any
  catalog write. Cross-segment `visible_tokens` reads remain explicit.
- Local SQLite MAY colocate directory and per-world control records. The remote
  topology MUST NOT imply a global transaction: only the target world's
  control authority atomically combines manifest publication, command
  settlement, and durable control-outbox append after data flush; directory
  discovery and Iceberg commits remain separate authorities.
- The LanceDB path MUST NOT construct a Daft `Session` or Daft `Catalog`.
- Service shutdown MUST shut down every managed backend exactly once per
  instance.

### Managed world construction

- `build_world(...)` is the module-level seam between the world family and
  core.
- `WorldLifecycle` MUST obtain the backend triplet through `iStorageService`
  and assemble an `AsyncWorld` with a system, querier, updater, commit
  coordinator, and construction-injected command materializer.
- `WorldRegistry` owns the in-memory catalog of active worlds, exact-world
  locks, storage coordinates, cleanup leases, and required-projection receipt
  retention.
- `WorldLifecycle.create_world()` MUST be idempotent by explicit `world_id`.
- Name lookup is a convenience index; names are unique, but they are not the
  idempotency key.
- Duplicate-name validation MUST happen before a new world is inserted into the
  live registry. A rejected create MUST leave both the ID and name indexes
  unchanged.
- If durable catalog registration or writer-fence acquisition fails after
  construction, `create_world()` MUST remove the new live world before
  propagating the failure.
- Live resource injection is an application-layer responsibility.
- `destroy_world()` SHOULD be safe to call on a missing world.
- `fork_world()` MUST create a new `world_id`, clone the source world's visible
  state, and let source and fork diverge independently.
- Before selecting a live fork snapshot, managed execution MUST retry retained
  projection and reconcile any prepared source publication under its exact
  identity. An unresolved outcome fails without registering a child.
- Forking MUST transfer pending spawn/despawn caches so spawn-then-fork before
  the next tick materializes in both worlds.

### Command ledger, scheduler, and dispatcher

- Deferred admission is durable before the caller receives `command_id`.
- Commands are partitioned by world and ordered by a durable
  `(scheduled_tick, priority, sequence)` key.
- A command ID is an idempotency identity. Repeating admission with the same ID
  and content returns the existing record; changed content conflicts. Distinct
  IDs remain distinct logical commands.
- Draining leases commands; it never destructively removes an unsettled
  command. A crashed worker's lease expires and becomes retryable.
- Permanent domain failures become `REJECTED`; transient failures become
  `RETRYABLE`; exhausted attempts become `DEAD_LETTER`.
- Same-tick commands apply sequentially in ledger order. A permanent rejection
  may be skipped and later commands continue; a transient failure aborts the
  unprocessed tail so dependent ordering is preserved.
- A command becomes `APPLIED` only when the tick manifest that makes its effect
  visible is published. Manifest and outcome settlement are one control-plane
  transaction.
- RBAC and quota admission happen only at `iCommandGateway`; trusted local
  admission records an explicit local origin.

### Command gateway

- `iCommandGateway` is the policy enforcement point for untrusted operations.
- Direct methods authorize, delegate to `iRuntimeApplication`, access-audit,
  and return a result immediately.
- `submit()` and `submit_batch()` are tick-deferred APIs. They return command IDs
  and durably admit work for later application.
- Generic deferred submission MUST accept only commands with a tick-boundary
  dispatcher, plus the intentional `MESSAGE`, `CUSTOM`, and `QUERY_WORLD`
  application envelopes. All other command types MUST be rejected before quota
  debit, audit emission, or durable admission.
- `submit_spawn()` is the special case that reserves a world-local entity ID
  before enqueue so `spawn()` can honestly return `entity_id`.
- Reservation MUST be serialized per world.
- `submit()`, `submit_batch()`, and `submit_spawn()` MUST reject submissions to
  an unknown `world_id` by raising `archetype.errors.WorldNotFoundError`
  before any quota debit, durable admission, or audit emit.
- Command-family dispatch is the application boundary at tick time; the
  gateway has no drain method.
- World lifecycle operations use direct gated methods such as `create_world`,
  `fork_world`, and `destroy_world`.

Leasing is non-destructive. Applied outcomes settle with tick visibility;
retryable failures remain recoverable and exhausted failures become terminal.

### Managed world execution

- `archetype.world.simulation.step()` is the authoritative managed-world
  execution boundary.
- Managed `step()` MUST rely on the world's construction-injected scheduler
  materializer, which applies due commands before `PreTick` and active-signature
  discovery while the exact world operation lock is already held.
- Managed `step()` MUST receive an explicit `RunConfig` from the caller; it
  MUST NOT mint a fresh `RunConfig` per call. The world's active `run_id`, not
  reuse of a particular config object, provides continuity across calls.
- `run()` MUST thread the caller's `RunConfig` into every `step()` call while
  preserving and reporting the world's active `run_id`.
- After publication, a configured required projector MUST consume and
  acknowledge the stable `CommittedTickReceipt`. Failure is post-commit: the
  receipt remains retained and retryable, and the tick MUST NOT be replayed.
- After durable rows are flushed, an uncertain manifest response MUST retain
  the exact prepared context and frames. Exact-token visibility permits only
  local staging acknowledgment and receipt completion, even after fence
  handoff; it does not authorize a second catalog write or later work from the
  stale writer. An explicit missing tick from the fenced authority permits a
  fresh attempt. An unreadable result, a legacy `None`, or a different token
  fails closed. Until resolved, entity, processor, hook, and resource mutation
  MUST reject without changing live state.
- The core records the committed receipt before advisory `PostTick`. Managed
  cancellation after publication MUST retain that receipt and retry required
  projection before any later tick.
- A live boundary that branches, counts new work, reports a tick, or persists
  tick attribution MUST reconcile retained projection and prepared publication
  before selecting that boundary. This includes fork, run/episode start,
  live world-info snapshots, and artifact occurrence context. Multi-world
  reporting MUST NOT hold sibling world locks while recovery fires advisory
  hooks or a required projector.
- Episodes and rollouts follow [Execution Hierarchy](execution-hierarchy.md).

### Durable world reads

- `archetype.world.query` is the internal durable ECS read surface below the
  application facade.
- Trusted runtime reads go through `iRuntimeApplication`; untrusted reads go
  through `iCommandGateway` and then the same application operation.
- Archetype and component reads MUST resolve storage per call and query durable
  rows by `world_id` and `run_id`; they do not require the world to be live in
  the process registry.
- Coordinated reads MUST restrict results to catalog-published commit tokens.
- Fork-aware reads MUST compose persisted lineage segments with the fork's own
  rows without requiring a live source world.
- `get_lineage()` reads persisted ancestry. `list_signatures()` combines the
  selected store's process-local registry with its durable control-catalog
  records, resolving imported component classes by schema fingerprint and
  exact durable table identity. Unresolvable historical records emit a warning
  and are skipped so unrelated schema drift cannot disable storage-wide
  discovery. Exact process-local class identities take precedence over catalog
  reconstruction. Catalog outages degrade discovery to the process-local
  subset; mutable resume and commit-visibility checks remain strict.
- Audit and command history are served by `iAuditLog` through the application
  or authorized gateway boundary. Durable world query has no audit dependency.

### ServiceContainer and runtime lifetime

- `ServiceContainer` is the internal process-scoped wiring root and the only
  app module that imports concrete implementations across families.
- It exposes actor-free `application` and authorized `gateway` ports, owns the
  command/control and audit infrastructure, and owns a `StorageService` it
  creates while borrowing one supplied by a caller.
- Container shutdown MUST be explicit and distinct from per-world removal.
- Shutdown stops admission, drains admitted operations, stops command leasing,
  reconciles audit projection, closes worlds, and then releases owned storage.
  It attempts every phase and aggregates failures. Injected storage remains
  caller-owned.

## Multi-World Contracts

- Multiple worlds may coexist in one runtime.
- Worlds MUST be isolated by `world_id`.
- Storage rows are scoped by both `world_id` and `run_id`.
- Broker queues are partitioned per world key.
- A fork shares runtime infrastructure, but not world identity.
- Shutting down or destroying one world MUST NOT invalidate sibling worlds that
  share the same runtime.
- Destroying a world cancels or terminally settles its pending command records
  according to the commands contract before removing the live world.
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
- Any wrapper over the internal application gateway and service graph
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
- Runtime shutdown MUST stop admitting new calls, wait for any call already
  holding a world's operation lock, and close shared services only after that
  admitted work completes.
- Calls queued behind an in-flight operation MAY fail with the runtime's closed
  error once shutdown has started; they have not yet been admitted.

#### C5. Honest command return values

Sugar methods must not claim stronger return semantics than the service layer
can provide.

Required behavior:

- `spawn()` must not claim to return an entity ID unless the architecture can
  reserve that entity ID before durable admission
- If entity identity is only known after scheduler drain and apply, `spawn()` must
  return a command ID, a handle with explicit semantics, or no value
- Return types and docstrings must match actual runtime behavior

#### C6. Durable scheduler semantics remain intact

Command ordering and tick-boundary application must remain true under runtime.

Required behavior:

- Admitted commands must still be subject to durable scheduler ordering
- Admitted commands must still be applied at the documented tick boundary
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
- It must not tear down shared storage pools, the command scheduler, or sibling worlds
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
- A fork may share storage pools and command infrastructure through the runtime
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

- Trusted runtime calls are actor-free and do not claim RBAC enforcement;
  untrusted API calls must flow through `iCommandGateway`
- Access audit and domain outcome evidence must be attributed to their actual
  owning boundaries
- Direct resource mutation must not be described as governed by the gateway or scheduler

#### S6. Recommended runtime APIs should be mutation-complete

If a runtime wrapper is presented as the recommended script boundary, it should
cover the common governed mutation verbs without forcing the user to drop to
the service layer.

Required behavior:

- The recommended runtime world handle SHOULD expose actor-free entity mutation
  verbs for `spawn`, `despawn`, `update`, `add_components`, and
  `remove_components`
- The recommended runtime world handle SHOULD expose actor-free processor
  mutation verbs for `add_processor` and `remove_processor`
- Runtime audit access such as audit history SHOULD remain available without
  requiring direct container access

#### S7. Declarative scaffolding must remain explicit

Some runtime operations are declarative handle construction rather than
governed simulation mutations. That distinction must be explicit.

Required behavior:

- World-handle construction is an immediate trusted runtime operation.
- Activation, hook registration, resource attachment, mutation, simulation,
  read, fork, and destroy operations flow through `iRuntimeApplication`.
- Untrusted ingress performs the corresponding operation through
  `iCommandGateway` first.

### Runtime Acceptance Criteria

No runtime API may be considered ready for implementation until the design can
show how it satisfies all of the following:

- Concurrent first-use of the same wrapper creates exactly one world
- `spawn()` return semantics are correct and tested
- Runtime handles are actor-free and share one application-level world lock
- One world's shutdown does not break a sibling world in the same runtime
- Runtime teardown is explicit and distinct from world teardown
- Forked worlds remain valid after the source world is shut down
- Recommended runtime mutation verbs cover entity, component, and processor
  mutations without dropping to the service layer
- Trust-boundary-preserving scaffolding is documented and tested
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
| `WorldLifecycle.create_world(world_id=X)` | Idempotent by explicit `world_id` |
| `WorldLifecycle.destroy_world(missing)` | Safe no-op |
| `AsyncCachedStore.shutdown()` | Idempotent |
| Command admission with the same `command_id` and content | Idempotent; returns the existing durable record |
| Command admission with the same `command_id` and changed content | Conflicts |
| Command admission with a new `command_id` | Creates a distinct logical command |
| Deferred spawn | Returns one reserved `entity_id` per successful admission; replay of the same command converges on that reservation |
| `AsyncWorld.create_entity()` | Not idempotent; each call allocates a new world-local entity ID |
| `AsyncWorld.remove_entity(missing)` | Safe no-op with observability |
| Duplicate despawn in one tick | Idempotent collapse by entity ID |
| Duplicate staged spawn rows for the same entity in one tick | Deterministic last-write-wins at materialization |
| Replay of an already-settled reserved spawn command | Returns or observes the existing terminal outcome; never materializes twice |
| `RuntimeWorld.history()` | Idempotent for fixed audit history |
| `add_components()` with no signature change | Idempotent no-op |
| `remove_components()` with no signature change | Idempotent no-op |
| `world.step()` | Not idempotent; advances tick and appends new rows |
| `world.run()` | Not idempotent; performs multiple steps under one run contract |
| `QueryManager.query_archetype()` | Idempotent for fixed persisted state |
| Store `append()` replay | Not idempotent; repeating an append persists duplicate rows |
| Updater `update()` replay | Not idempotent; repeating an update appends another row version |
| Store `get_archetype_df()` replay | Idempotent for the same persisted data |
| Durable world fixed-state reads | Idempotent for fixed rows, lineage, and signature catalog |
| Catalog re-registration | Same identity and content is an idempotent no-op; different content conflicts loudly |
| Coordinated tick retry after failed publish | Unpublished attempts stay invisible; retry produces exactly one visible attempt |
| Cold discovery and reads | Repeated cold discovery and reads return stable durable state |
| Fenced mutable resume | Resume continues from the last visible tick and stale-writer retries stay invisible |
| Artifact ingestion replay | Not idempotent; every submission records a new UUIDv7 occurrence while identical bytes reuse one content-addressed object |
| `evaluate()` replay | Same evaluation identity, subject, and contract returns the persisted result without re-grading |
| Hard process crash and cold resume | Unpublished physical rows do not advance a fresh process beyond the last visible tick |
| Independent writer-process race | Exactly one fenced writer publishes the contested tick |

The durable rows above summarize the normative amendments in
[Durable Discovery](durable-discovery.md),
[Atomic Visibility](atomic-visibility.md),
[World Lifecycle](world-lifecycle.md), and [Artifacts](artifacts.md).
The idempotency eval manifest must match this table exactly; `make
idempotency-audit` is the fast static drift check, while `make eval-idem`
executes the behavioral scenarios.

The behavioral suite is an independent oracle: it does not call or import the
feature tests under `tests/app/`. Each task builds fresh storage and asserts
primarily on service-facing outcomes and durable query results. Deterministic
fault injection targets the documented manifest-publish boundary; selected
internal counters provide secondary failure diagnostics. Coverage is
deliberately symmetric: repeated no-ops must collapse, explicitly
occurrence-based submissions must remain distinct, interrupted tick commits
must recover, and identity reuse with changed content must fail loudly. A new matrix row
therefore requires a registered behavioral task, and a new task must trace
back to at least one matrix row.

Two tasks cross real OS-process boundaries over shared LanceDB and SQLite
files: hard process death followed by cold resume and a two-writer fence race.
The `infrastructure-idempotency` GitHub Actions
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
| 2 | Resolved | Tick-deferred submission is allowlisted to dispatched commands and intentional application envelopes; all direct operations fail before quota, audit, or admission. | `tests/integration/test_command_flow.py::test_direct_only_commands_cannot_enter_tick_deferred_scheduler`; Issues #368, #415, #418 |
| 3 | Resolved | `CommandGateway.submit*` reject an unknown world with `WorldNotFoundError` before quota, enqueue, or audit side effects. | `tests/integration/test_command_flow.py::test_submit_to_unknown_world_rejected` |
| 4 | Resolved | Duplicate-name and catalog-registration failures leave no hidden live world. | `tests/core/test_orchestrator_errors_and_instrumentation.py`; `tests/app/test_durable_discovery.py::test_failed_catalog_registration_leaves_no_live_world` |
| 5 | Resolved | Spawn, despawn, and component migration hooks fire from their public mutation paths with the documented queue-time semantics. | `tests/core/test_resources_hooks_messaging.py`; `tests/core/test_batch_spawn_contract.py`; `tests/sync/test_sync_world.py` |
| 6 | Resolved | `archetype.world.query` performs durable archetype, component, lineage, and signature reads; application history comes from `iAuditLog`. | `tests/world/test_query_contracts.py`; `tests/world/test_atomic_visibility.py`; `tests/app/test_audit_contracts.py` |
| 7 | Resolved | Gated destroy cancels only the target world's unsettled command state and preserves shared runtime and durable history. | `tests/integration/test_fork_destroy_contracts.py` |
| 8 | Resolved | Same-entity, same-tick mutations compose in durable scheduler order. | `tests/core/test_same_tick_mutation_composition.py`; `evals/suites/idempotency/tasks.py::task_duplicate_same_tick_mutations_collapse`; Issue #193 |

## Durability Posture (v0.3, issue #276)

The durable substrate (#272–#275) gives each remaining self-assessment an
explicit contract. Every subsystem below is either durable by mechanism or
deliberately advisory with the durable alternative named — none is an
undocumented gap.

| Subsystem | Contract |
|---|---|
| Command ledger | **Durable**: admission, order, leases, retries, terminal outcomes, and dead letters survive process loss. Applied outcomes settle atomically with tick publication. |
| Audit journal | **Durable journal, eventually consistent projection**: authoritative transitions append transactional outbox events. Iceberg is a deduplicated analytical projection with an observable watermark. |
| Mutation idempotency | **Commands are replay-safe by command identity** while direct simulation mutations remain distinct events. Replaying a staged command cannot materialize its effect twice. |
| API auth | **Trust-boundary contract**: production ingress authenticates a stable principal and fails closed. Any anonymous-admin development mode is explicit and uses a stable process principal. The trusted runtime is actor-free. |
| RBAC quota state | **Process-local and advisory** in v0.3 (daily token budgets reset on restart). Durable quota accounting is a control-catalog follow-up; deployments needing hard budgets enforce them at the identity layer above. |

Receipts report a committed authority decision but do not create authority by
themselves. The control authority records visibility and command/publication
state; the audit projection reports those events. Durable guarantees trace to
the visibility contract in [Atomic Visibility](atomic-visibility.md).

## Acceptance Criteria

This specification should be considered satisfied only when tests demonstrate
all of the following:

- stable component payload round-trips
- deterministic signature canonicalization
- append-only persistence scoped by world and run
- stable processor ordering within an archetype
- stable cross-archetype execution without world bookkeeping corruption
- advisory hook isolation, whole-tick processor failure atomicity, and
  signature-aware matching across explicit component migrations
  (`processor_adversarial` repository check)
- stable reserved-entity spawn semantics through durable admission
- explicit multi-world isolation and fork divergence (`fork_divergence` repository check)
- exact actor-local quota boundaries and UTC rollover (`quota_boundaries` repository check)
- live/cold historical-read parity and resumed run continuity
  (`time_travel_and_run_id` repository check)
- explicit runtime-vs-world lifetime boundaries
- lazy single-flight activation, wait-then-close runtime shutdown, handle
  invalidation, and sync/async handle parity (`runtime_contracts` repository check)
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
- shutdown vs admitted work, shutdown vs init, and fork vs init races
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
container. Maintainer docs can still explain `ServiceContainer`,
`CommandGateway`, durable scheduler semantics, audit semantics, and raw ECS flows,
but they MUST label concrete services and the container as internal.

Examples also need to be executed in CI. An example that "looks right" but is
never run is not documentation; it is an unverified claim.

LLM-backed examples need explicit credential gating or graceful degraded
behavior when keys are missing.

### Specification Ownership

Focused specification pages are now the source of truth for their areas, with
this page serving as the umbrella entry point. Tests enforce the contracts and
contributor docs point back to the specification group.
