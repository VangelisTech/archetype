# Services

The service layer mediates all external access to worlds. It enforces RBAC, manages storage lifecycles, and drives the simulation loop. The core layer has no knowledge of auth, commands, or multi-world management -- all of that lives here.

```text
archetype.app
  ServiceContainer          Wires everything; single construction point
    |
    +-- StorageService       Multiton (uri, namespace) -> (store, querier, updater)
    +-- CommandBroker        Priority queue with RBAC guard
    +-- WorldRegistry?       Optional persistent JSON discovery
    |
    +-- WorldService         World lifecycle, forking, name lookup
    +-- CommandService       Submit, drain, apply commands
    +-- SimulationService    Tick stepping (drain -> reset -> step)
    +-- QueryService         Read-only access, no ActorCtx
```

## ServiceContainer

`ServiceContainer` is the single point of construction. It wires services in dependency order:

```python
from archetype.app.container import ServiceContainer

container = ServiceContainer()
```

### Dependency Chain

Construction order matters because each service depends on the ones above it:

```text
StorageService                    (no dependencies)
CommandBroker                     (no dependencies)
WorldRegistry?                    (optional, from registry_path)
    |
WorldService(StorageService, CommandBroker, WorldRegistry?)
    |
CommandService(CommandBroker, WorldService)
    |
SimulationService(WorldService, CommandService)
QueryService(WorldService, CommandBroker)
```

`ServiceContainer.__init__` builds the full graph synchronously. No async initialization is required -- storage backends are created lazily on first use.

### Registry

Pass `registry_path` to enable persistent world discovery across server restarts:

```python
container = ServiceContainer(registry_path="./worlds.json")
```

`WorldRegistry` stores a JSON array of world entries (world_id, name, storage_uri, namespace, tick). On startup, `WorldService.discover_worlds()` rehydrates each entry through `create_world()`, restoring tick counters from the registry.

A `post_tick` hook is automatically attached to each world to keep the registry tick in sync. The hook captures the entry in a closure so each tick writes without read-modify-write races.

## StorageService

Manages shared storage backends using a multiton pattern. For any `(uri, namespace)` pair, only one `(store, querier, updater)` triplet is created and reused.

```python
store, querier, updater = await container.storage_service.get_backend(
    storage_config, cache_config
)
```

Backend selection:

| `storage_config.backend` | Store class |
|--------------------------|-------------|
| `StorageBackend.LANCEDB` (default) | `AsyncLancedbStore` |
| `StorageBackend.ICEBERG` | `AsyncStore` (Iceberg via Daft catalog) |

If a `CacheConfig` is provided, the store is wrapped in `AsyncCachedStore` for write-behind caching. See [Stores](stores.md) for backend details.

Creation is guarded by per-key `asyncio.Lock` to prevent duplicate instantiation under concurrent access.

## WorldService

Manages the lifecycle of multiple worlds: creation, lookup, forking, and shutdown.

### create_world

Idempotent -- if a world with the given `world_id` already exists, returns the existing instance. Otherwise:

1. Delegates to `WorldFactory` to build an `AsyncWorld` with the correct storage triplet (see [App Overview -- The Integration Seam](app-overview.md#the-integration-seam-worldfactory))
2. Injects the `CommandBroker` into `world.resources` so processors can submit commands
3. Registers the world by ID and name
4. Persists the entry to the registry (if configured)
5. Attaches a `post_tick` hook for registry sync

### fork_world

Creates a new world from a snapshot of an existing one. See [Worlds -- Forking Internals](worlds.md#forking-internals) for the cloning algorithm.

### Lookup

```python
world = container.world_service.get_world(world_id)
world = container.world_service.get_world_by_name("my-sim")
worlds = container.world_service.list_worlds()
```

## CommandService

Owns the submit-drain-apply pipeline for all external mutations. See [Command Broker](broker.md) for the queue internals and RBAC details.

### submit

Accepts a `Command` with type, payload, tick, and priority. Requires an `ActorCtx`. The broker's `guardrail_allow()` enforces RBAC, per-tick quota (500 commands), and daily token budget (200k tokens) before enqueueing.

### drain_and_apply

Called by `SimulationService.step()`. Dequeues all commands where `cmd.tick <= current_tick`, applies each to the target world, and acknowledges on success.

### apply

Pattern-matches on `CommandType` and calls the corresponding `AsyncWorld` mutation. See [Data Flow -- Command Dispatch](data-flow.md#command-dispatch) for the full dispatch table.

## SimulationService

Drives the per-tick simulation loop:

```python
await container.simulation_service.step(world_id, run_config)
```

Each `step()` call:

1. `command_service.drain_and_apply(world_id, tick)` -- apply due commands
2. `broker.reset_tick_counters()` -- clear per-actor command counts
3. `world.step(run_config)` -- execute processors via the core tick lifecycle

## QueryService

Read-only access to world state.

Current status:

- The current implementation takes no `ActorCtx`, performs no RBAC checks, and
  involves no broker coordination.
- That behavior is provisional.
- The durable target contract is defined in
  [Committed Read Model](committed-read-model.md).

```python
state = await container.query_service.get_world_state(world_id, tick=5)
entity = await container.query_service.get_entity(world_id, entity_id)
components = await container.query_service.get_components(world_id, [Position, Health])
history = await container.query_service.get_command_history(world_id)
```

See [Data Flow -- Read Path](data-flow.md#read-path) for the current
implementation and [Committed Read Model](committed-read-model.md) for the
normative target contract.

## How Services Connect to the API

The [API Layer](api-layer.md) exposes these services over HTTP via FastAPI's `Depends()` injection. Each route handler receives a service instance and delegates to it. The [CLI](api-layer.md#cli) is a thin httpx client that calls the same endpoints.

For the full integration story -- how core interfaces get wired through services to the API -- see [App Overview](app-overview.md).

## Source Reference

- Service container: `src/archetype/app/container.py`
- Storage service: `src/archetype/app/storage_service.py`
- World service: `src/archetype/app/world_service.py`
- Command service: `src/archetype/app/command_service.py`
- Simulation service: `src/archetype/app/simulation_service.py`
- Query service: `src/archetype/app/query_service.py`
- World registry: `src/archetype/app/registry.py`
- World factory: `src/archetype/app/factory.py`
- Command broker: `src/archetype/app/broker.py`
