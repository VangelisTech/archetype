# Service Protocols

**Document type:** Normative.
**Scope:** `src/archetype/app/interfaces.py` — the public contract surface of the application layer.

## 1. Overview

The application layer is a set of services that wrap the core ECS engine. Every service has a corresponding `Protocol` class in `app/interfaces.py` that defines its public contract. Implementations live in their own module (`app/{name}_service.py`). Composition happens in `app/container.py`.

The gate (`iCommandService`) sits in front of the other services and applies authorization, audit, and downgrade-to-info-class. Nothing below the gate knows about `ActorCtx`.

## 2. Service dependency graph

```text
iStorageService -> iWorldService -> iMutationService
                              +---> iSimulationService
                -> iQueryService -> iEvalService
                -> iAuditLog
iStorageService + iWorldService -> iFactService

iWorldService + iMutationService + iSimulationService + iQueryService
    + iFactService + iAuditLog + iCommandBroker
    -> iCommandService (the ActorCtx-aware gate)
```

A tier MUST depend only on tiers strictly below it. Circular dependencies are forbidden.

`iCommandService` is the only service that knows about `ActorCtx`. It is the only service that emits audit rows directly. It is the only service the runtime calls.

## 3. Protocol summaries

Each entry below is a one-line summary. The full signatures live in `app/interfaces.py`.

### `iStorageService`

Creates and pools async stores. Multiton on
`(uri, namespace, backend, Daft IOConfig fingerprint, cache)` within one process.
The built-in Iceberg factory is a concrete local SQLite-catalog lakehouse.
Managed or remote Iceberg enters through a caller-configured Daft `Session`;
`IOConfig` passes directly to Daft data I/O. Leaf service — no required
dependencies. One injected session serves one storage URI and namespace;
mismatches fail closed. A `ServiceContainer` borrowing an injected-session
service requires `audit_storage_config`; construction constrains the shared
session to that same identity before any world can bind it elsewhere.

```python
async def get_or_create_store(storage_config, cache_config=None) -> iAsyncStore
async def get_iceberg_context(storage_config) -> IcebergCatalogContext
async def shutdown() -> None
```

### `iWorldService`

World lifecycle. Internally owns `WorldFactory`, `WorldRegistry`, `WorldOrchestrator`. Returns `iWorld` instances for internal callers; the gate downgrades to `WorldInfo`.

```python
async def create_world(config, storage_config=None, cache_config=None, system=None) -> iWorld
def get_world(world_id) -> iWorld
def get_world_by_name(name) -> iWorld
def list_worlds() -> list[iWorld]
async def fork_world(source_world_id, name=None, storage_config=None, cache_config=None) -> iWorld
async def destroy_world(world_id) -> None        # in-memory only
async def add_resource(world_id, resource) -> None
async def add_hook(world_id, event_type, fn) -> HookHandle
async def remove_hook(world_id, handle) -> None
async def list_processors(world_id) -> list[iAsyncProcessor]
async def list_hooks(world_id) -> list[HookHandle]
async def list_resources(world_id) -> list[object]
async def shutdown() -> None
```

Fork semantics: `world-lifecycle.md` § 4. Destroy semantics: `world-lifecycle.md` § 5.

### `iMutationService`

Mutates world contents: entities, components, processors. No `ActorCtx` argument; the gate enforces.

```python
async def create_entity(world_id, components) -> int
async def remove_entity(world_id, entity_id) -> None
async def add_components(world_id, entity_id, components) -> None
async def remove_components(world_id, entity_id, component_types) -> None
async def add_processor(world_id, processor) -> None
async def remove_processor(world_id, proc_type) -> None
```

### `iSimulationService`

Execution engine. All four levels of the execution hierarchy live here.

```python
async def step(world_id, run_config, **kw) -> None
async def run(world_id, run_config, **kw) -> RunResult
async def run_episode(world_id, config: EpisodeConfig, **kw) -> EpisodeResult
async def run_rollout(world_id, config: RolloutConfig, **kw) -> RolloutResult
```

Episode and rollout semantics: `execution-hierarchy.md`. `run_rollout` calls `iWorldService.fork_world` directly for each fork; those fork operations are not gated individually.

### `iQueryService`

Direct read path to storage. Independent of `iWorldService`.

```python
async def query_archetype(sig, world_id, run_id, storage_config=None,
                           *, ticks=None, entity_ids=None, components=None,
                           lineage=None) -> DataFrame
async def query_components(components, world_id, run_id, storage_config=None,
                           *, ticks=None, entity_ids=None, lineage=None,
                           visibility_tokens=None) -> DataFrame
async def list_signatures(storage_config=None) -> list[ArchetypeSignature]
```

`visibility_tokens` is an exact commit-token allowlist for immutable reads.
Persisted evaluation uses it with snapshot-bounded ticks so a concurrently
advancing world cannot change the rows attributed to a receipt. It cannot be
combined with `lineage`, whose segments each require their own visibility set.

### `iEvalService`

Dataframe-first evaluation over rows already persisted by Archetype. It
depends only on `iQueryService`: it does not run simulations, own experiment
lifecycle, choose a scoring schema, or persist scores on the caller's behalf.

```python
async def query_components(components, *, world_id, run_id,
                           storage_config=None, ticks=None,
                           entity_ids=None, lineage=None) -> DataFrame
async def query_episode(episode, *, components, run_id=None,
                        storage_config=None, entity_ids=None,
                        lineage=None) -> DataFrame
async def query_trajectory_component(component, *, world_id, run_id,
                                     trajectory_ids=None, episode_ids=None,
                                     rollout_ids=None, task_ids=None,
                                     trial_idxs=None, **query_options) -> DataFrame
async def run_graders(df, graders) -> list[object]
async def grade_trajectory_component(component, *, world_id, run_id,
                                     graders, **query_options) -> list[object]
```

`run_graders` MUST reject an empty grader set and any grader that returns no
outputs. Such an evaluation is undefined; it must not be representable as a
vacuous success through Python's `all([])` behavior. Grader outputs are returned
to the caller and remain opaque to Archetype. If a score should be durable, the
caller writes it as a component.

### `iFactService`

Writes typed external facts to the same Iceberg catalog as their world. It
depends on `iStorageService` for the authoritative Daft catalog and `IOConfig`,
and on `iWorldService` to resolve a live world's storage configuration.

```python
async def ingest_files(world_id, paths, processor,
                       storage_config=None) -> FactWriteReceipt
async def write_facts(world_id, table_name, facts,
                      storage_config=None) -> FactWriteReceipt
async def read_facts(world_id, table_name,
                     storage_config=None) -> DataFrame
```

The envelope, logical key, batching, schema, and writer-concurrency contracts
are normative in [Durable Facts](durable-facts.md).

### `iCommandBroker`

Priority queue + history of submitted commands. Pure queue — RBAC, quotas, and audit emission happen at the gate, not here.

```python
async def enqueue(world_id, cmd) -> None
async def enqueue_bulk(world_id, cmds) -> None
async def dequeue(world_id, max_items=None) -> list[Command]
async def dequeue_due(world_id, tick, limit=None) -> list[Command]
async def ack(cmd_ids) -> None
async def remove(world_id, cmd_id) -> None
async def peek(world_id, max_items=None) -> list[Command]
async def get_pending_count(world_id=None) -> int
async def get_history(world_id, limit=100) -> list[Command]
async def clear(world_id=None) -> None
```

Concurrency: implementations MUST be safe for concurrent enqueue/dequeue from multiple coroutines.

### `iAuditLog`

Append-only record of accepted-and-applied commands. Rows are buffered in a
bounded batch and appended to a dedicated Iceberg table. Schema is fixed in
[Audit Log](audit-log.md).

```python
async def record(row: AuditRow) -> None
async def flush() -> None
async def query(world_id=None, *, tick_range=None, actor_id=None,
                 idempotency_key=None, status=None, limit=None) -> DataFrame
async def shutdown() -> None
```

NO `drop_*` method. Append-only is non-negotiable.

### `iCommandService` — the gate

Policy enforcement point. Every external mutation, lifecycle operation, and read flows through here. `iCommandService` is a thin reverse proxy: each method does `guardrail_allow → delegate → audit.record`.

Construction:

```python
def __init__(
    self,
    mutations: iMutationService,
    worlds: iWorldService,
    simulation: iSimulationService,
    queries: iQueryService,
    broker: iCommandBroker,
    audit: iAuditLog,
    facts: iFactService,
) -> None: ...
```

#### Mutations (sync-direct gates)

```python
async def create_entity(ctx, world_id, components) -> int
async def remove_entity(ctx, world_id, entity_id) -> None
async def add_components(ctx, world_id, entity_id, components) -> None
async def remove_components(ctx, world_id, entity_id, component_types) -> None
async def add_processor(ctx, world_id, processor) -> None
async def remove_processor(ctx, world_id, proc_type) -> None
async def ingest_files(ctx, world_id, paths, processor,
                       storage_config=None) -> FactWriteReceipt
async def write_facts(ctx, world_id, table_name, facts,
                      storage_config=None) -> FactWriteReceipt
```

#### Lifecycle (return WorldInfo, not iWorld)

```python
async def create_world(ctx, config, storage_config=None, cache_config=None) -> WorldInfo
async def fork_world(ctx, source_world_id, name=None, *, storage_config=None, cache_config=None) -> WorldInfo
async def destroy_world(ctx, world_id) -> None
async def get_world_info(ctx, world_id) -> WorldInfo
async def list_worlds(ctx) -> list[WorldInfo]
```

#### Simulation

```python
async def step(ctx, world_id, run_config, **kw) -> None
async def run(ctx, world_id, run_config, **kw) -> RunResult
async def run_episode(ctx, world_id, config, **kw) -> EpisodeResult
async def run_rollout(ctx, world_id, config, **kw) -> RolloutResult
```

`run_rollout` emits ONE rollout-level audit row (not per-fork). The audit row's payload captures the resulting fork ids.

#### Queries (gated reads)

```python
async def query_archetype(ctx, sig, world_id, run_id, storage_config=None,
                           *, ticks=None, entity_ids=None, components=None) -> DataFrame
async def query_components(ctx, components, world_id, run_id, storage_config=None,
                           *, ticks=None, entity_ids=None) -> DataFrame
async def list_signatures(ctx, storage_config=None) -> list[ArchetypeSignature]
async def query_facts(ctx, world_id, table_name,
                      storage_config=None) -> DataFrame
```

#### Resource attachment

```python
async def add_resource(ctx, world_id, resource) -> None
async def add_hook(ctx, world_id, event_type, fn) -> HookInfo
async def remove_hook(ctx, world_id, handle) -> None
```

Used at runtime activation to wire resources and staged hooks from `runtime.world(..., resources=[...], hooks=[...])` after `create_world`.

#### Read introspection (downgrades to Info classes)

```python
async def list_processors(ctx, world_id) -> list[ProcessorInfo]
async def list_hooks(ctx, world_id) -> list[HookInfo]
async def list_resources(ctx, world_id) -> list[ResourceInfo]
async def get_audit_history(ctx, world_id=None, *, tick_range=None, actor_id=None,
                             idempotency_key=None, limit=None) -> DataFrame
```

#### Tick-deferred path (queued submit)

```python
async def submit(ctx, world_id, cmd: Command) -> UUID
async def submit_batch(ctx, world_id, cmds: list[Command]) -> list[UUID]
async def submit_spawn(ctx, world_id, components, *, tick=0, priority=0) -> int
async def drain_and_apply(world_id, tick) -> list[Command]   # called by SimulationService at step boundary
```

`drain_and_apply` is the one method without `ActorCtx` — commands carry their own context that was validated at submit time.

## 4. Extension patterns

### When to add a new service

A new service is justified when:

1. The work has a distinct responsibility from existing services (not just a method that fits on `iWorldService` or `iSimulationService`).
2. The work has its own storage or external resource (audit log, broker, payment facilitator).
3. Other services depend on it as a leaf primitive.

Otherwise, add a method to an existing service. The bar for new services is high.

### When to add a new method to `iCommandService`

A new gated method is needed when:

1. The runtime layer needs to call into a new service capability.
2. The capability requires `ActorCtx` enforcement.
3. The capability is user-visible (not just internal orchestration between services).

Adding a method to `iCommandService` requires:

- A corresponding `CommandType` entry in `app/models.py`, or explicit reuse of
  an existing command type when the new surface has the same permission class
  (typed fact writes reuse `INGEST_FACT`; fact reads reuse `QUERY_WORLD`).
- An entry in `COMMANDS_BY_ROLE` (see `command-gate.md`).
- A test in the role-permissions parametrized suite.
- The thin proxy implementation: `guardrail_allow → delegate → audit.record`.

### When to add a new `Info` class

When the gate needs to return a live object that user code shouldn't hold (an `iWorld`, an `iAsyncProcessor`, a callable, an arbitrary user resource), a `*Info` class downgrades it to an immutable snapshot. See `world-lifecycle.md` § 6.

Pattern: pydantic `BaseModel` with `frozen=True`. Carry only metadata that's safe to expose. NO live references.

## 5. Composition (`app/container.py`)

```python
class ServiceContainer:
    def __init__(
        self,
        storage_service: StorageService | None = None,
        audit_storage_config: StorageConfig | None = None,
    ):
        self.broker = CommandBroker()
        self._owns_storage_service = storage_service is None
        self.storage_service = (
            storage_service if storage_service is not None else StorageService()
        )
        self.audit = AuditLog(self.storage_service, audit_storage_config)
        self.world_service = WorldService(self.storage_service)
        self.mutation_service = MutationService(self.world_service)
        self.simulation_service = SimulationService(self.world_service)
        self.query_service = QueryService(self.storage_service)
        self.eval_service = EvalService(self.query_service)
        self.fact_service = FactService(self.storage_service, self.world_service)
        self.command_service = CommandService(
            mutations=self.mutation_service,
            worlds=self.world_service,
            simulation=self.simulation_service,
            queries=self.query_service,
            broker=self.broker,
            audit=self.audit,
            facts=self.fact_service,
        )

    async def shutdown(self) -> None:
        await self.broker.clear()
        await self.audit.flush()
        await self.audit.shutdown()
        if self._owns_storage_service:
            await self.world_service.shutdown()
```

Shutdown order matters: clear pending broker work; flush + close audit; close
container-owned stores last. An injected `StorageService` is caller-owned and
remains open.

## 6. Companion specs

- `runtime.md` — what the runtime calls into.
- `command-gate.md` — `ActorCtx`, roles, audit emission shape.
- `audit-log.md` — append-only audit row schema and query semantics.
- `execution-hierarchy.md` — what `iSimulationService` does in detail.
- `world-lifecycle.md` — what `iWorldService` does in detail.
