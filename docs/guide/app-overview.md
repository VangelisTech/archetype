# App Overview

The application layer composes workflows over the core ECS engine and the
storage/world families. The top-level commands family owns exact governed
entry, durable scheduling, policy, and audit projection. The API layer exposes
actor-aware dispatcher entry over HTTP.

For normative ownership and dependency rules, see
[Application Architecture](application-architecture.md). For active internal
ports, see [Service Protocols](service-protocols.md). Concrete services and
process wiring are internal.

## Layers

```text
application -> runtime -> CommandDispatcher.apply / defer
CLI -> API -> authentication -> CommandDispatcher.apply_as / defer_as
CommandDispatcher -> registered family handler -> world/storage/core
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
registered handler and runtime/API boundaries return immutable `WorldInfo`.

**World mutation and simulation functions** stage entity/component/processor
changes and execute step, run, episode, and rollout under the registry lease.
Rollout-internal forks remain implementation details; the gated rollout call
is the audit unit.

**World query functions** are the storage-backed read path. They have no
`ActorCtx` and do not require a live world. Trusted reads use `apply`;
untrusted reads authenticate and use `apply_as`.

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
tick-to-external-I/O loop. Process wiring registers exact submit, run, and
restore handlers. `RuntimeMissions` constructs those models and retains no
concrete service.

**Physical-AI handlers** turn typed task-evaluation and instruction-sweep
operations into one batched world, drive a bounded episode, and project
terminal results from persisted `ManipStatus` rows. They are family-owned free
workflows over declared storage and world ports, not an application service.
Environment and policy providers transfer to process ownership before the
first effect, hold an exclusive identity lease for the workflow, and close
through `RuntimeResources`; callers reach this workflow through
`ArchetypeRuntime`. Each handler retires its live writer before releasing the
lease, leaving durable query evidence rather than an attachable provider
execution path.

**RuntimeResources** is the explicit process owner. It owns dispatcher
admission, supervised work, workflow and world handles, audit, and storage
through ordered retryable teardown. **archetype.wiring** is the one concrete
cross-family composition transaction; it registers exact operations and
returns the resource owner.

## Trusted and authorized flow

Direct operation:

```text
Runtime
  -> construct exact family operation
  -> CommandDispatcher.apply(exact operation)
  -> OperationRegistry handler

API
  -> authenticate ActorCtx and construct exact family operation
  -> CommandDispatcher.apply_as(ctx, exact operation)
  -> Policy + OperationRegistry handler
  -> AuditLog.record_access(bounded evidence)
```

Tick-deferred operation:

```text
Runtime or authenticated API adapter
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
   -> construct CreateWorld(...)

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

The API lifespan owns one `RuntimeResources`. Routes inject its
`CommandDispatcher` and an authenticated `ActorCtx`, translate HTTP payloads
into exact family models, call actor-aware dispatcher entry, and return
response models.

The CLI is an HTTP client against that server.

See [API Layer](api-layer.md).

## Source Reference

- World state and behavior: `src/archetype/world/`
- Process composition: `src/archetype/wiring.py`
- Process lifetime: `src/archetype/runtime_resources.py`
- Governed entry, scheduler, policy, and audit: `src/archetype/commands/`
- Physical-AI models, state, views, and handlers: `src/archetype/physical_ai/`
- Family protocols: `src/archetype/<family>/interfaces.py` or another focused family module
- World ports: `src/archetype/world/interfaces.py`
- Storage port: `src/archetype/storage/interfaces.py`
- Core interfaces: `src/archetype/core/interfaces.py`
