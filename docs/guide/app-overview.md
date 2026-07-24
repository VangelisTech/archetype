# App Overview

The application layer composes workflows over the core ECS engine and the
storage/world families. The top-level commands family owns exact governed
entry, durable scheduling, policy, and audit projection. The API layer exposes
the temporary application/gateway adapters over HTTP.

For normative ownership and dependency rules, see
[Application Architecture](application-architecture.md). For active internal
ports, see [Service Protocols](service-protocols.md). Concrete services and
`ServiceContainer` are internal.

## Layers

```text
application -> runtime -> RuntimeApplication adapter -> CommandDispatcher
CLI -> API -> CommandGateway adapter -> CommandDispatcher
CommandDispatcher -> registered family handler -> world/storage/core
temporary workflow bridge -> RuntimeApplication -> app-family capability
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

The world, commands, application, and API families above core add those
concerns according to their ownership boundaries.

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

**OperationRegistry** binds each exact operation model to its handler,
permission, quota scope, availability, bounded summary, and optional durable
materializer.

**CommandDispatcher and Policy** own trusted versus actor-aware entry, RBAC,
quotas, admission lifetime, and bounded access evidence.

**CommandScheduler** durably admits, orders, leases, materializes, retries, and
stages settlement for portable tick operations.

**AuditLog** projects bounded access rows and transactional command outboxes
into the analytical audit table. The command ledger and tick manifest remain
authoritative.

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

**RuntimeApplication** is the temporary actor-free facade adapter consumed by
the runtime. **CommandGateway** is the temporary transport-shaped,
`ActorCtx`-aware adapter consumed by API/untrusted adapters. Registered
world/audit methods on both construct the same exact family models and enter
`CommandDispatcher`; a finite bridge still reaches `RuntimeApplication` for
the staged workflows whose registrations land next.

## Trusted and authorized flow

Direct operation:

```text
Runtime
  -> RuntimeApplication.<method>(...)
  -> CommandDispatcher.apply(exact operation)
  -> OperationRegistry handler

API
  -> CommandGateway.<method>(ctx, ...)
  -> CommandDispatcher.apply_as(ctx, exact operation)
  -> Policy + OperationRegistry handler
  -> AuditLog.record_access(bounded evidence)
```

Tick-deferred operation:

```text
RuntimeApplication or CommandGateway adapter
  -> CommandDispatcher.defer / defer_as
  -> OperationRegistry durable eligibility
  -> CommandScheduler.admit
  -> control-catalog command ledger
  -> AsyncWorld construction-injected materializer
  -> CommandScheduler.materialize(actual_world, tick)
  -> registered lock-held materializer + tick settlement
```

See [Data Flow](data-flow.md) for details.

## Creating a World

`create_world` is a registered direct operation. Trusted and untrusted adapters
construct the same `CreateWorld` model; actor-aware entry authorizes before
the registered handler:

```text
1. Runtime handle or authorized API route
   -> RuntimeApplication or CommandGateway adapter

2. CommandDispatcher.apply / apply_as(CreateWorld(...))

3. Registered handler -> WorldLifecycle.create_world(...) / build_world(...)
   -> iStorageService backend triplet
   -> AsyncWorld(..., materialize_commands=...)
   -> WorldRegistry.insert(...)

4. Adapter -> return WorldInfo
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
- Governed entry, scheduler, policy, and audit: `src/archetype/commands/`
- Service protocols: `src/archetype/app/<family>/interfaces.py`
- World ports: `src/archetype/world/interfaces.py`
- Storage port: `src/archetype/storage/interfaces.py`
- Core interfaces: `src/archetype/core/interfaces.py`
