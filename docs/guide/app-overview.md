# App Overview

The app layer wraps the core ECS engine with service boundaries for storage, world lifecycle, mutation, simulation, reads, command queuing, and audit history. The API layer exposes those boundaries over HTTP.

For normative ownership and dependency rules, see
[Application Architecture](application-architecture.md). For active internal
ports, see [Service Protocols](service-protocols.md). Concrete services and
`ServiceContainer` are internal.

## Layers

```text
application -> runtime -> RuntimeApplication
CLI         -> API -> CommandGateway -> RuntimeApplication
RuntimeApplication -> internal app-family capabilities -> core
```

Dependencies point downward. Core does not import app. The CLI does not import app except for `serve`; it talks to the server over HTTP.

## What Core Does Not Do

The core layer defines and implements ECS mechanics:

- `AsyncWorld`
- `AsyncProcessor`
- storage/query/update facades
- system execution
- lifecycle hooks
- resources

It does not know about:

- actors or roles
- command authorization
- audit rows
- REST routes
- multi-world API hosting

The app layer adds those concerns.

## World family boundary

`archetype.world` owns managed world state and behavior. `WorldRegistry` owns
live identities, storage coordinates, exact-world locks, close leases, and
unacknowledged post-commit receipts. `WorldLifecycle` constructs, discovers,
resumes, forks, and closes worlds through `iStorageService`. The module-level
mutation, simulation, query, and handler functions remain stateless family
behavior over those two state owners.

`build_world(...)` is the single module-level construction seam into core. It
wires the shared store, query/update managers, system, resources, hooks,
construction-injected command materializer, and optional required projector.

## Services

**WorldRegistry** serializes every live mutation and execution against one
exact world while allowing different worlds to progress concurrently.

**WorldLifecycle** owns create, discover, cold-open, mutable resume, fork, and
retryable close. Internal lifecycle operations may return `AsyncWorld`; the
application and gateway boundaries return immutable `WorldInfo`.

**World mutation and simulation functions** stage entity/component/processor
changes and execute step, run, episode, and rollout under the registry lease.
Rollout-internal forks remain implementation details; the gated rollout call
is the audit unit.

**World query functions** are the storage-backed read path. They have no
`ActorCtx` and do not require a live world. Trusted reads enter through
`RuntimeApplication`; untrusted reads first pass `CommandGateway`.

**CommandLedger/Dispatcher** durably admits, orders, leases, applies, retries,
and settles tick-deferred commands. It does not own RBAC.

**Audit** owns journals, transactional outboxes, and the analytical projection.

**MissionService** composes the mission family's task entities,
relationships, processors, committed-intent outbox, and sandbox resource. The
processors own transitions; the service owns graph materialization and the
tick-to-external-I/O loop. `ServiceContainer` injects its factory into
`RuntimeApplication`; `RuntimeMissions` consumes the resulting
`iMissionService` port without importing the concrete service.

**PhysicalAIService** turns typed task-evaluation and instruction-sweep
requests into one batched world, drives a bounded episode, and projects
terminal results from persisted `ManipStatus` rows. Environment and policy
providers remain family-owned resources; callers reach this workflow through
`ArchetypeRuntime`, never through raw service parameters.

**RuntimeApplication** is the actor-free application facade consumed by the
runtime and gateway. **CommandGateway** is the only ActorCtx-aware application
boundary and is consumed by API/untrusted adapters only.

## Trusted and authorized flow

Direct operation:

```text
Runtime
  -> RuntimeApplication.<method>(...)
  -> owning family workflow

API
  -> CommandGateway.<method>(ctx, ...)
  -> guardrail_allow
  -> RuntimeApplication.<method>(...)
  -> AuditJournal.record_access
```

Tick-deferred operation:

```text
RuntimeApplication or authorized gateway
  -> CommandScheduler.admit
  -> CommandLedger
  -> AsyncWorld construction-injected materializer
  -> CommandDispatcher
  -> lock-held world mutation + tick settlement
```

See [Data Flow](data-flow.md) for details.

## Creating a World

`create_world` is an actor-free application operation; untrusted calls are
authorized before delegation:

```text
1. Runtime handle or authorized API route
   -> RuntimeApplication.create_world(WorldConfig(...), storage, cache)

2. RuntimeApplication -> iWorldLifecycle.create_world(...)

3. WorldLifecycle / build_world(...)
   -> iStorageService backend triplet
   -> AsyncWorld(..., materialize_commands=...)
   -> WorldRegistry.insert(...)

4. RuntimeApplication -> return WorldInfo
```

Runtime activation then adds staged processors, resources, and hooks through
their application operations.

## API and CLI

The API layer injects the command-gateway port and `ActorCtx` into route
handlers. The current implementation type is `CommandGateway`. Routes
translate HTTP payloads into gateway calls and return response models.

The CLI is an HTTP client against that server.

See [API Layer](api-layer.md).

## Source Reference

- World state and behavior: `src/archetype/world/`
- Container: `src/archetype/app/container.py`
- Service protocols: `src/archetype/app/<family>/interfaces.py`
- World ports: `src/archetype/world/interfaces.py`
- Storage port: `src/archetype/storage/interfaces.py`
- Core interfaces: `src/archetype/core/interfaces.py`
