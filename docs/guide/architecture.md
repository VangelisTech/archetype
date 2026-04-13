# Overview

Archetype is a data-centric Entity-Component-System (ECS) simulation engine. World state is stored as columnar DataFrames. Every tick appends new rows to storage without overwriting previous state, which enables time-travel queries, world forking, and replay.

![Archetype architecture](../assets/archetype_diagram2.png)

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

Instantiates and exposes all service-layer subsystems:

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

## Tick Lifecycle

Each `step()` call follows this sequence:

```text
SimulationService.step(world_id, run_config)
  |
  1. drain_and_apply(world_id, tick)
  |    CommandBroker.dequeue_due(world_id, tick)
  |    CommandService.apply(world, cmd) for each command
  |    → spawn/despawn/update mutations queue in world caches
  |
  2. reset_tick_counters()
  |    Clear per-actor command counts for next tick
  |
  3. world.step(run_config)
       |
       a. For each archetype (concurrently via asyncio.gather):
       |    i.   Query previous state from store (or _live cache)
       |    ii.  Materialize spawn/despawn caches into the DataFrame
       |    iii. Execute matching processors in priority order
       |    iv.  Persist result via updater → store
       |    v.   Update _live cache
       |
       b. Increment tick counter
       c. Fire post_tick hooks
```

Commands applied in step 1 produce deferred mutations (spawn/despawn caches). Those mutations materialize in step 3a-ii of the same tick. Cross-archetype communication (messages, spawns targeting different archetypes) takes effect on the next tick.

For full details: [Worlds](worlds.md) covers the internal tick cycle, [Data Flow](data-flow.md) covers the command pipeline, [System Execution](system-execution.md) covers processor dispatch.

## Deep Dives

### Core

The simulation engine. No auth awareness, no multi-world management.

- [Archetype](archetype.md) -- signatures, naming, schemas, entity-to-table mapping
- [Components](components.md) -- Pydantic models with Arrow serialization, column prefixing, field types
- [Processors](processors.md) -- DataFrame transforms, resource injection, LLM integration
- [Systems](system-execution.md) -- subset rule, priority ordering, per-archetype parallelism
- [Worlds](worlds.md) -- tick lifecycle, deferred mutations, `_live` cache, hooks, forking
- [Resources](resources.md) -- type-keyed dependency injection for world-level shared state
- [Stores](stores.md) -- storage backends, append-only model, write-behind cache
- [Querier](querier.md) -- filtered reads by tick, entity, and component projection
- [Updater](updater.md) -- metadata stamping before append
- [Configuration](run-config.md) -- RunConfig, WorldConfig, StorageConfig, CacheConfig

### App

The service layer. RBAC, command pipeline, multi-world orchestration.

- [Overview](app-overview.md) -- how core connects through services to the API
- [Services](services.md) -- ServiceContainer, dependency graph, each service's role
- [Command Broker](broker.md) -- priority queue, RBAC guardrails, audit trail
- [API Layer](api-layer.md) -- FastAPI routes, dependency injection, CLI
- [Data Flow](data-flow.md) -- read/write split, command pipeline, RBAC boundary, drain cycle
