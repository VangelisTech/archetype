# Overview

Archetype is a data-centric Entity-Component-System (ECS) simulation engine. World state is columnar DataFrames. Every tick is an append-only write to storage. This gives you time-travel, forking, and replay for free.

## Core Abstractions

```mermaid
classDiagram
    class Component {
        +to_row_dict()
        +get_prefix()
    }
    class AsyncWorld {
        +world_id
        +tick
        +resources
        +create_entity()
        +add_components()
        +remove_entity()
        +step()
        +run()
    }
    class AsyncProcessor {
        +components
        +priority
        +process()
    }
    class AsyncSystem {
        +add_processor()
        +remove_processor()
        +execute()
    }
    class Resources {
        +insert()
        +require()
        +get()
    }
    class AsyncStore {
        +get_archetype_df()
        +append()
        +shutdown()
    }
    class QueryManager {
        +get_archetype()
        +query_archetype()
    }
    class UpdateManager {
        +update()
    }
    class CommandBroker {
        +enqueue()
        +dequeue_due()
        +get_history()
    }
    class ServiceContainer {
        +world_service
        +command_service
        +simulation_service
        +query_service
        +broker
    }
    AsyncWorld --> AsyncSystem
    AsyncWorld --> Resources
    AsyncWorld --> QueryManager : reads
    AsyncWorld --> UpdateManager : writes
    AsyncSystem --> AsyncProcessor
    QueryManager --> AsyncStore
    UpdateManager --> AsyncStore
    ServiceContainer --> CommandBroker
    ServiceContainer --> AsyncWorld
    AsyncProcessor --> Component : requires
```

## Layers

```text
archetype.api / cli          External interface (REST + HTTP client)
       |
archetype.app                Services, RBAC, CommandBroker, WorldRegistry
       |
archetype.core               AsyncWorld, AsyncProcessor, Resources, Storage
```

The system runs as a single `archetype serve` process. The CLI is a thin HTTP client.

## Service Layer

The service layer mediates all access to worlds.

### ServiceContainer

Wires everything together:

```python
from archetype.app.container import ServiceContainer

container = ServiceContainer()
# container.world_service     -- world lifecycle
# container.command_service   -- command submission
# container.simulation_service -- tick stepping
# container.query_service     -- read path
# container.broker            -- command queue
# container.storage_service   -- storage backends
```

### Command Flow

All mutations from external actors flow through the command pipeline:

1. **CommandService.submit()** -- accepts a `Command` with type, payload, tick, priority
2. **CommandBroker.enqueue()** -- validates RBAC via `ActorCtx`, enforces quotas, queues by priority
3. **SimulationService.step()** -- drains due commands, applies them to the world, steps processors
4. **QueryService** -- reads world state (current or historical)

### RBAC

Every command submission requires an `ActorCtx` specifying the actor's roles:

| Role | Permissions |
|------|-------------|
| `viewer` | Read-only (query, get state, get world) |
| `player` | spawn, despawn, update, message, custom |
| `coder` | add/remove components, update |
| `operator` | trajectory ingestion and labeling |
| `maintainer` | spawn, despawn, components, processors, update |
| `admin` | All commands (wildcard) |

Quotas: 500 commands per tick, 200k token budget per day. See [Token Costs and Quotas](token-quotas.md) for details.

## Deep Dives

The Architecture section covers each subsystem in detail:

**Core Primitives** -- the building blocks of the ECS:

- [Archetype](archetype.md) -- signatures, naming, schemas, and how entities are grouped
- [Components](components.md) -- data-only value objects that define entity state
- [Processors](processors.md) -- pure DataFrame transforms that run each tick
- [Systems](system-execution.md) -- execution model, subset rule, priority ordering, parallelism

**Infrastructure** -- the runtime machinery:

- [Worlds](worlds.md) -- simulation coordinator, tick lifecycle, mutations, hooks, forking
- [Stores](stores.md) -- storage backends, Daft catalogs, persistence
- [Querier](querier.md) -- read path, filtering, projections
- [Updater](updater.md) -- write path, housekeeping stamps, append operations
