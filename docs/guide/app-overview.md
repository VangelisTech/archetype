# App Overview

The app layer wraps the core ECS engine with service boundaries for storage, world lifecycle, mutation, simulation, reads, command queuing, and audit history. The API layer exposes those boundaries over HTTP.

For normative service contracts, see [Service Protocols](service-protocols.md).

## Layers

```text
archetype.api / cli          HTTP surface and thin client
       |
archetype.runtime            Script boundary, RuntimeWorld handles
       |
archetype.app                Services, command gate, audit, broker
       |
archetype.core               AsyncWorld, processors, resources, storage
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

`WorldFactory` is where app-level storage resolution plugs concrete core interfaces into `AsyncWorld`.

```python
class WorldFactory:
    async def create_world(self, world_config, storage_config, cache_config=None, system=None):
        store = await self._storage_service.get_or_create_store(
            storage_config,
            cache_config,
        )
        return AsyncWorld(
            world_config=world_config,
            querier=AsyncQueryManager(store),
            updater=AsyncUpdateManager(store),
            system=system or AsyncSystem(),
        )
```

The factory constructs. The core executes.

## Services

**WorldService** manages live world identity, lookup, fork, destroy, hooks, resources, and processor listings. Internal callers receive live `iWorld` objects; the gate downgrades user-visible returns to info classes.

**MutationService** applies entity, component, and processor mutations after authorization has happened at the gate.

**SimulationService** owns step, run, episode, and rollout. Rollout-internal forks are implementation details; the gated rollout call is the audit unit.

**StorageService** pools data stores and owns the optional local SQLite control catalog lifecycle.

**LedgerService** creates and discovers durable ledger manifests without registering a live world.

**QueryService** is the internal storage-backed read path. Cataloged reads additionally depend on
`LedgerService`; it has no `ActorCtx`, and external reads go through `iCommandService`.

**CommandBroker** is a pure queue for tick-deferred commands. It does not own RBAC or audit history.

**AuditLog** is append-only and records gated operations.

**CommandService** is the command gate. It is the only ActorCtx-aware service and the only service the runtime calls.

## Gate-Centric Flow

Direct operation:

```text
Runtime / API
  -> CommandService.<method>(ctx, ...)
  -> guardrail_allow
  -> delegate to WorldService / LedgerService / MutationService / SimulationService / QueryService
  -> AuditLog.record
  -> return result or info snapshot
```

Tick-deferred operation:

```text
Runtime / API
  -> CommandService.submit(ctx, world_id, cmd)
  -> guardrail_allow
  -> CommandBroker.enqueue
  -> SimulationService.step
  -> CommandService.drain_and_apply
  -> MutationService / WorldService
```

See [Data Flow](data-flow.md) for details.

## Creating a World

`create_world` is a direct gated lifecycle call:

```text
1. API route or runtime handle
   -> CommandService.create_world(ctx, WorldConfig(...), storage, cache)

2. CommandService
   -> guardrail_allow(CommandType.CREATE_WORLD, ctx)
   -> WorldService.create_world(...)

3. WorldService / WorldFactory
   -> StorageService.get_or_create_store(...)
   -> AsyncWorld(...)
   -> register world by id/name

4. CommandService
   -> AuditLog.record(...)
   -> return WorldInfo
```

Runtime activation then adds staged processors, resources, and hooks through their own gated methods.

## API and CLI

The API layer injects `CommandService` and `ActorCtx` into route handlers. Routes translate HTTP payloads into service calls and return response models.

The CLI is an HTTP client against that server.

See [API Layer](api-layer.md).

## Source Reference

- Factory: `src/archetype/app/factory.py`
- Container: `src/archetype/app/container.py`
- Service protocols: `src/archetype/app/interfaces.py`
- Core interfaces: `src/archetype/core/interfaces.py`
