# Overview

Archetype is a data-centric Entity-Component-System simulation engine. World state is stored as columnar DataFrames, and each tick appends new rows instead of overwriting previous state. That storage model supports time-travel queries, forking, replay, and audit.

![Archetype architecture](../assets/archetype_diagram2.png)

## Layers

```text
archetype.api / cli          REST API and thin HTTP client
       |
archetype.runtime            ArchetypeRuntime, RuntimeWorld
       |
archetype.app                ServiceContainer, iCommandService, audit, broker
       |
archetype.core               AsyncWorld, AsyncProcessor, Resources, Storage
```

`ArchetypeRuntime` is the recommended script boundary. It owns a `ServiceContainer`, returns lazy world handles, binds each handle to an `ActorCtx`, and forwards every operation through `iCommandService`.

```python
from archetype import ArchetypeRuntime, Component


class Position(Component):
    x: float = 0.0
    y: float = 0.0


async with ArchetypeRuntime() as runtime:
    world = runtime.world("demo")
    entity_id = await world.spawn(Position(x=0, y=0))
    await world.run(steps=10)
```

Drop to `ServiceContainer` for lower-level hosting, explicit command routing, or tests that need direct service access.

## Command Gate

All external operations flow through `iCommandService`, the policy enforcement point.

```text
Runtime / API / caller
  -> iCommandService
  -> guardrail_allow
  -> delegate to one service
  -> iAuditLog.record
  -> return result
```

The broker is only the queue for tick-deferred commands. It is not the authorization boundary and it is not the audit log.

## Roles

Roles are flat:

| Role | Intent |
|---|---|
| `viewer` | Read-only operations |
| `player` | Entity participation: spawn, despawn, update, message, custom |
| `operator` | Schema, processors, hooks, resources, simulation control, fork, destroy |
| `admin` | All commands, including world creation |

To combine capabilities, give an actor multiple roles. `operator` is not implicitly `viewer` unless both roles are present in the actor context.

See [Command Gate](command-gate.md).

## Execution

The simulation hierarchy is:

```text
step     one tick
run      N steps, no termination, no fork
episode  step until termination/cap on the supplied world
rollout  N forked episodes from a base world
```

See [Execution Hierarchy](execution-hierarchy.md).

## Tick Lifecycle

A tick has two service-level phases:

```text
SimulationService.step(world_id, run_config)
  |
  1. CommandService.drain_and_apply(world_id, tick)
  |    CommandBroker.dequeue_due(world_id, tick)
  |    MutationService / WorldService applies due commands
  |
  2. AsyncWorld.step(run_config)
       |
       a. Query previous state
       b. Materialize pending structural mutations
       c. Execute matching processors
       d. Persist appended rows
       e. Refresh live state and hooks
```

Processors are trusted internal code once registered. External callers do not bypass the gate.

## World Lifecycle

World lifecycle has three operations:

- `create_world`: admin-only identity creation.
- `fork_world`: create a new world from the source snapshot.
- `destroy_world`: remove the live in-memory world; storage and audit rows remain.

Forks receive a new `world_id`, a new `run_id`, the source tick, copied pending mutation caches, copied hook registrations, and shared processor/resource instances by default.

See [World Lifecycle](world-lifecycle.md).

## Deep Dives

### Specifications

- [Runtime](runtime.md)
- [Service Protocols](service-protocols.md)
- [Command Gate](command-gate.md)
- [Execution Hierarchy](execution-hierarchy.md)
- [World Lifecycle](world-lifecycle.md)
- [Audit Log](audit-log.md)

### Core

- [Archetype](archetype.md)
- [Components](components.md)
- [Processors](processors.md)
- [Systems](system-execution.md)
- [Worlds](worlds.md)
- [Lifecycle Hooks](hooks.md)
- [Resources](resources.md)
- [Stores](stores.md)
- [Querier](querier.md)
- [Updater](updater.md)
- [Configuration](run-config.md)

### App

- [App Overview](app-overview.md)
- [Services](services.md)
- [Command Broker](broker.md)
- [API Layer](api-layer.md)
- [Data Flow](data-flow.md)
