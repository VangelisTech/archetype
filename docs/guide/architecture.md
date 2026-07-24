# Overview

Archetype is a data-centric Entity-Component-System simulation engine. World state is stored as columnar DataFrames, and each tick appends new rows instead of overwriting previous state. That storage model supports time-travel queries, forking, replay, and audit.

This page is an explanatory overview. The written rules and dependency tables
in [Application Architecture](application-architecture.md) are normative;
visual layout is not.

## Layers

```text
application code -> ArchetypeRuntime -> RuntimeApplication -> CommandDispatcher
CLI -> REST API -> CommandGateway -> CommandDispatcher
CommandDispatcher -> registered family handler -> world/storage/core
temporary workflow bridge -> RuntimeApplication -> app-family capability
```

The trusted runtime bypasses authorization; the API authenticates and enters
through the gateway. For registered world/audit operations, both temporary
adapters construct the same exact family model and converge on
`CommandDispatcher`; only actor-aware entry invokes `Policy` and bounded access
evidence. `ServiceContainer` and concrete services are internal machinery.

`ArchetypeRuntime` is the recommended script boundary. It owns process lifetime,
returns lazy actor-free world handles, and forwards operations through
`iRuntimeApplication`.

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

Applications do not construct `ServiceContainer` or call concrete services.
Repository composition modules and focused implementation tests may use them
because they are internal seams.

## Command Gate

All untrusted operations flow through `iCommandGateway`, the transport ingress
port. Commands-owned `CommandDispatcher` and `Policy` are the policy
enforcement point.

```text
API / untrusted caller
  -> iCommandGateway adapter constructs exact operation
  -> CommandDispatcher.apply_as / defer_as
  -> OperationRegistry + Policy
  -> registered family handler or CommandScheduler
  -> AuditLog.record_access(bounded evidence)
  -> return result
```

`OperationRegistry`, dispatcher, policy, durable scheduler/ledger, and
`AuditLog` belong to the top-level commands family. `RuntimeApplication` and
`CommandGateway` remain temporary facade adapters and own none of that state.
A finite gateway-to-application bridge remains for staged workflows awaiting
their exact registrations.

## Roles

Roles are flat:

| Role | Intent |
|---|---|
| `viewer` | Read-only operations |
| `player` | Viewer permissions plus spawn, batch create, despawn, and update |
| `operator` | Player permissions plus schema, processors, hooks, resources, simulation control, fork, and destroy |
| `admin` | Operator permissions plus world creation and mutable resume |

Role labels are flat inputs and an actor's grants are unioned. The built-in
permission sets above explicitly include the preceding row; no unknown
permission is inferred from a role name, including `admin`.

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

A managed tick holds the exact world's registry operation lease. Durable
commands are part of the tick itself:

```text
archetype.world.simulation.step(registry, world_id, run_config)
  |
  a. Retry an unacknowledged required-projector receipt, if any
  b. AsyncWorld materializes due portable commands
  c. Fire advisory PreTick hooks
  d. Discover active signatures
  e. Compute every archetype without consuming mutation caches
  f. Append and flush rows
  g. Publish the manifest and settle staged command outcomes atomically
  h. Consume mutation caches and advance the tick
  i. Fire advisory PostTick hooks
  j. Return a stable CommittedTickReceipt
  k. Run and acknowledge the required projector, when configured
```

Processors are trusted internal code once registered. External callers do not bypass the gate.

A tick is a world execution and commit boundary. It does not necessarily imply
a task, mission, or physical-workflow state transition. Public hook failures
are advisory. A required-projector failure is post-commit: the receipt remains
retryable, and retry does not recompute or republish the committed tick.

## World Lifecycle

World lifecycle has three operations:

- `create_world`: admin-only identity creation.
- `fork_world`: create a new world from the source snapshot.
- `destroy_world`: remove the live in-memory world; storage and audit rows remain.

Forks receive a new `world_id`, a new `run_id`, the source tick, copied pending mutation caches, copied hook registrations, and shared processor/resource instances by default.

See [World Lifecycle](world-lifecycle.md).

## Deep Dives

### Specifications

- [Application Architecture](application-architecture.md)
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
- [Durable Commands](durable-commands.md)
- [API Layer](api-layer.md)
- [Data Flow](data-flow.md)
