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

## WorldFactory Boundary

`WorldFactory` is where the store resolved by `WorldService` is composed into
core. It gives that same store to `AsyncQueryManager` and
`AsyncUpdateManager`, constructs the system, resources, and hooks, and returns
an `AsyncWorld`.

The factory constructs. The core executes.

## Services

**WorldService** manages live world identity, lookup, fork, destroy, hooks, resources, and processor listings. Internal callers receive live `iWorld` objects; the gate downgrades user-visible returns to info classes.

**MutationService** applies entity, component, and processor mutations after authorization has happened at the gate.

**SimulationService** owns step, run, episode, and rollout. Rollout-internal forks are implementation details; the gated rollout call is the audit unit.

**QueryService** is the internal storage-backed read path. It has no `ActorCtx`;
trusted reads enter through RuntimeApplication and untrusted reads first pass
CommandGateway.

**CommandLedger/Dispatcher** durably admits, orders, leases, applies, retries,
and settles tick-deferred commands. It does not own RBAC.

**Audit** owns journals, transactional outboxes, and the analytical projection.

**AgentMissionService** composes the mission family's task entities,
relationships, processors, committed-intent outbox, and sandbox resource. The
processors own transitions; the service owns graph materialization and the
tick-to-external-I/O loop. `ServiceContainer` injects its factory into
`RuntimeApplication`; `RuntimeMissions` consumes the resulting
`iAgentMissionService` port without importing the concrete service.

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
  -> SimulationService.step
  -> CommandDispatcher
  -> MutationService + tick settlement
```

See [Data Flow](data-flow.md) for details.

## Creating a World

`create_world` is an actor-free application operation; untrusted calls are
authorized before delegation:

```text
1. Runtime handle or authorized API route
   -> RuntimeApplication.create_world(WorldConfig(...), storage, cache)

2. RuntimeApplication -> WorldService.create_world(...)

3. WorldService / WorldFactory
   -> StorageService.get_or_create_store(...)
   -> AsyncWorld(...)
   -> register world by id/name

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

- Factory: `src/archetype/app/world/service.py`
- Container: `src/archetype/app/container.py`
- Service protocols: `src/archetype/app/<family>/interfaces.py`
- Core interfaces: `src/archetype/core/interfaces.py`
