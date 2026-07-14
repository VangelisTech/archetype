# Services

The service layer wraps the core ECS engine with multi-world management, command governance, audit history, and storage lifecycle. The core layer has no knowledge of actors, roles, commands, or process-level orchestration.

For normative signatures, see [Service Protocols](service-protocols.md).

```text
archetype.app
  ServiceContainer          Wires everything
    |
    +-- StorageService       Multiton storage pool
    +-- LedgerService        Durable catalog + exact manifests
    +-- CommandBroker        Pure priority queue
    +-- AuditLog             Append-only audit rows
    |
    +-- WorldService         World lifecycle, lookup, fork, destroy
    +-- MutationService      Entity, component, and processor mutations
    +-- SimulationService    Step, run, episode, rollout
    +-- QueryService         Internal storage-backed reads
    |
    +-- CommandService       The gate: auth, audit, delegation
```

## ServiceContainer

`ServiceContainer` is the lower-level composition root. Script users usually start with `ArchetypeRuntime`; host processes and tests use `ServiceContainer` when they need explicit service wiring.

```python
from archetype.app.container import ServiceContainer

container = ServiceContainer()
```

Construction is synchronous. Storage backends are opened lazily on first use.

## Dependency Graph

Services depend only on lower tiers:

```text
iStorageService
    ├──> iWorldService ──> iMutationService / iSimulationService
    ├──> iLedgerService
    ├──> iQueryService ──> iEvalService
    └──> iAuditLog

iWorldService + iLedgerService + iQueryService + iMutationService
    + iSimulationService + iAuditLog + iCommandBroker
        └──> iCommandService
```

`iCommandService` is the only `ActorCtx`-aware service. It is also the only service the runtime calls.

## StorageService

`StorageService` creates and pools async stores. It is a multiton keyed by effective storage configuration.

```python
store = await container.storage_service.get_or_create_store(
    storage_config,
    cache_config,
)
```

See [Stores](stores.md) for backend behavior.

## LedgerService

`LedgerService` owns restart-safe ledger discovery and exact immutable manifest reads. In A1 it
creates generation-zero ledgers and uses `StorageService`'s colocated SQLite capability for true
cross-process uniqueness and CAS. It never registers or mutates a live world.

See [Durable Ledgers](durable-ledgers.md) for the authority and A1/A2 boundary.

## WorldService

`WorldService` manages live world instances:

- `create_world` establishes a new world identity.
- `fork_world` snapshots a source world into a new world identity.
- `destroy_world` removes the live in-memory world from the registry.
- lookup methods return live `iWorld` objects for internal service callers.

External callers do not receive live `iWorld` objects. `iCommandService` downgrades lifecycle returns to `WorldInfo`.

World lifecycle details are normative in [World Lifecycle](world-lifecycle.md).

## MutationService

`MutationService` mutates world contents after the gate has authorized the operation:

- create and remove entities
- update existing components
- add and remove component types
- add and remove processors

It has no `ActorCtx` parameter. Authorization belongs to `iCommandService`.

## SimulationService

`SimulationService` owns the execution hierarchy:

- `step`: one tick
- `run`: N steps, no termination, no fork
- `run_episode`: step until termination or cap on the supplied world
- `run_rollout`: fork N worlds and run one episode in each

Rollout-internal forks use `iWorldService` directly. The gated `run_rollout` call is the audit unit, not each internal fork.

See [Execution Hierarchy](execution-hierarchy.md).

## QueryService

`QueryService` is the internal storage-backed read path. It has no `ActorCtx` argument because it sits below the gate.

External reads go through `iCommandService`:

- `query_archetype`
- `list_signatures`
- `get_world_info`
- `get_audit_history`
- `list_processors`
- `list_hooks`
- `list_resources`

Cataloged ledger descriptions and generation-zero pinned queries additionally use `LedgerService`
and an explicit trusted component registry.

The `viewer` role is meaningful at the gate. See [Command Gate](command-gate.md).

## CommandBroker

`CommandBroker` is a pure queue for tick-deferred commands. It stores, orders, dequeues, acknowledges, and clears commands.

It does not own RBAC, quota checks, or user-facing audit history. Those belong to `iCommandService` and `iAuditLog`.

See [Command Broker](broker.md).

## AuditLog

`AuditLog` is append-only. It records accepted-and-applied gated operations and backs `world.history(...)` through `iCommandService.get_audit_history(...)`.

Broker history is queue introspection; audit history is the durable record.

See [Audit Log](audit-log.md).

## CommandService

`CommandService` is the policy enforcement point. Every external mutation, lifecycle operation, simulation control call, and read flows through it.

Each gated method follows the same shape:

```text
guardrail_allow(command, ctx)
delegate to one underlying service
audit.record(row)
return downgraded/user-safe result
```

There are two paths through the gate:

- Direct calls apply now and return a result, such as `create_world`, `create_entity`, `run`, and `query_archetype`.
- Tick-deferred calls use `submit`, `submit_batch`, and `submit_spawn`; `SimulationService.step` later calls `drain_and_apply`.

Live objects do not escape the gate. `create_world`, `fork_world`, and `get_world_info` return `WorldInfo`; list methods return `ProcessorInfo`, `HookInfo`, and `ResourceInfo`.

## How Services Connect to the API

The [API Layer](api-layer.md) exposes the gate and selected queue/introspection endpoints through FastAPI. Route handlers translate HTTP into typed service calls and pass an `ActorCtx` from auth middleware.

The CLI is a thin HTTP client.

## Source Reference

- Service container: `src/archetype/app/container.py`
- Service protocols: `src/archetype/app/interfaces.py`
- Command service: `src/archetype/app/command_service.py`
- Command broker: `src/archetype/app/broker.py`
- World service: `src/archetype/app/world_service.py`
- Mutation service: `src/archetype/app/mutation_service.py`
- Simulation service: `src/archetype/app/simulation_service.py`
- Query service: `src/archetype/app/query_service.py`
- Ledger service: `src/archetype/app/ledger_service.py`
- Storage service: `src/archetype/app/storage_service.py`
