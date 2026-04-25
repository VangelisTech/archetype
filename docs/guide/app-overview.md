# App Overview

Archetype's core layer is a protocol-driven ECS engine. It defines interfaces, not services. The app layer wraps those interfaces with multi-world management, command governance, and lifecycle orchestration. The API layer exposes everything over HTTP. This page walks through how the three layers connect.

## The Three Layers

```text
archetype.api / cli          HTTP surface (FastAPI + thin httpx client)
       |
       |  Depends(get_service)    ← FastAPI DI injects from singleton container
       |
archetype.app                Services, CommandBroker, RBAC, WorldRegistry
       |
       |  constructs + wraps      ← WorldFactory plugs core interfaces together
       |
archetype.core               AsyncWorld, AsyncProcessor, Resources, Storage
```

Each layer has a strict downward dependency. The core layer does not import from `archetype.app`. The app layer does not import from `archetype.api`. The CLI does not import from `archetype.app` at all (except `serve`) -- it talks to the server over HTTP.

## What the Core Layer Does Not Do

The core layer defines Protocol classes in `archetype.core.interfaces`:

- `iAsyncWorld` -- simulation lifecycle (step, run, create/remove entities)
- `iAsyncStore` -- persistence (get, append, shutdown)
- `iAsyncQueryManager` -- read facade (query_archetype)
- `iAsyncUpdateManager` -- write facade (update with tick/world/run stamping)
- `iAsyncSystem` -- processor orchestration (add, remove, execute)

These are structural contracts, not implementations with opinions. The core layer has no knowledge of:

- **Auth** -- no roles, no permissions, no quotas
- **Multi-world management** -- a single `AsyncWorld` does not know other worlds exist
- **Command queuing** -- mutations are direct method calls, not queued messages
- **HTTP** -- no routes, no request models, no serialization for the wire

The app layer adds all of this.

## The Integration Seam: WorldFactory

The narrowest point where core meets app is `WorldFactory.create_world()`. This is where protocol interfaces get their concrete wiring:

```python
class WorldFactory:
    def __init__(self, storage_service: StorageService):
        self._storage_service = storage_service

    async def create_world(self, world_config, storage_config, cache_config=None, system=None):
        # 1. App layer resolves storage backend
        store, querier, updater = await self._storage_service.get_backend(
            storage_config, cache_config
        )

        # 2. Core interfaces get concrete implementations
        return AsyncWorld(
            world_config=world_config,
            querier=querier,      # iAsyncQueryManager
            updater=updater,      # iAsyncUpdateManager
            system=system or AsyncSystem(),  # iAsyncSystem
        )
```

`StorageService.get_backend()` handles backend selection (LanceDB vs Iceberg), optional cache wrapping, and multiton deduplication. The factory does not care about any of that -- it receives three interfaces and plugs them into a world.

This is the pattern: **the app layer constructs and configures; the core layer executes**. The core's `AsyncWorld` never knows whether its store is LanceDB with a write-behind cache or Iceberg backed by S3. It talks to `iAsyncQueryManager` and `iAsyncUpdateManager`.

## What Services Add

After the factory creates a world, the service layer wraps it with governance and lifecycle management:

**[WorldService](services.md#worldservice)** adds multi-world orchestration. It tracks worlds by ID and name, provides idempotent creation, injects the `CommandBroker` into `world.resources` so processors can submit commands, and manages forking. With a `WorldRegistry`, it persists world metadata to JSON and rehydrates on server restart.

**[CommandBroker](broker.md)** adds external command governance. All mutations from outside the process must pass through the broker's priority queue. The broker enforces RBAC (role permissions, per-tick quotas, daily token budgets) via `guardrail_allow()` before enqueueing. Processors bypass the broker entirely -- they write directly through the updater.

**[CommandService](services.md#commandservice)** bridges the broker and the world. `submit()` puts commands into the broker. `drain_and_apply()` pulls them out at tick time and dispatches each to the corresponding world mutation (`create_entity`, `add_components`, etc.).

**[SimulationService](services.md#simulationservice)** drives the tick loop. Each `step()` call drains commands, resets per-tick quotas, and calls `world.step()`. It also supports `run_all()` for stepping multiple worlds concurrently.

**[QueryService](services.md#queryservice)** provides read-only access. No `ActorCtx` required, no RBAC checks. Reads go straight through the world to the querier. See [Data Flow](data-flow.md) for why the read path has no auth overhead.

**[StorageService](services.md#storageservice)** manages shared backends. For each effective storage pool key `(uri, namespace, backend, cache config)`, one `(store, querier, updater)` triplet is created and reused across worlds.

## How the API Exposes Services

The API layer is thin. `ServiceContainer` is a module-level singleton in `deps.py`. FastAPI's `Depends()` injects individual services into route handlers:

```python
@router.post("/worlds/{world_id}/step")
async def step_world(
    world_id: str,
    sim: SimulationService = Depends(get_simulation_service),
):
    result = await sim.step(UUID(world_id))
    return {"world_id": world_id, "commands_applied": result}
```

Routes construct `Command` objects from request payloads and delegate to services. They do not contain business logic. See [API Layer](api-layer.md) for the full route architecture.

The CLI is a thin httpx client. `archetype serve` starts a uvicorn server with the FastAPI app. Every other command (`world create`, `step`, `query`, etc.) makes REST calls against that server. The CLI does not import the service layer -- all worlds live in a single server event loop.

## End-to-End: Creating a World

To see how the layers connect, trace `archetype world create my-sim` from the CLI down to storage and back:

```text
1. CLI
   archetype world create --name my-sim
   → POST http://localhost:8000/worlds  {"name": "my-sim"}

2. API Route (routes/worlds.py)
   create_world(req, cs=Depends(get_command_service), ctx=Depends(get_actor_ctx))
   → Constructs Command(type=CREATE_WORLD, payload={...})
   → cs.submit("__global__", cmd, ctx)

3. CommandService.submit()
   → broker.enqueue("__global__", cmd, ctx)
   → guardrail_allow(cmd, ctx)     ← RBAC check (role, tick quota, token budget)
   → heapq.heappush(queue, cmd)    ← queued by (tick, priority, seq)

4. CommandService.apply_world_lifecycle(cmd)
   → WorldService.create_world(config, storage_config)
   → WorldFactory.create_world(world_config, storage_config)

5. WorldFactory (the integration seam)
   → StorageService.get_backend(storage_config)
   → backend-specific storage factory          ← app/runtime composition
   → AsyncLancedbStore(...) or AsyncStore(...) ← core store
   → AsyncQueryManager(store)                  ← core read facade
   → AsyncUpdateManager(store)                 ← core write facade
   → AsyncWorld(querier, updater, system)      ← core world

6. WorldService post-creation
   → world.resources.insert(broker)    ← inject broker for processor access
   → registry.upsert(world_id, {...}) ← persist metadata
   → world.add_hook(PostTick, _sync_tick)     ← registry sync

7. Response
   → WorldResponse(world_id, name, tick=0)
   → 200 OK → CLI prints world ID
```

Steps 1-3 are the API and governance layer. Step 4-5 are where app meets core. Step 6 is the service layer adding its bookkeeping on top of a fully constructed core world.

The same pattern holds for every operation: the API constructs a command, the broker validates it, the service applies it to the core, and the core executes.

## Source Reference

- Factory: `src/archetype/app/factory.py`
- Container: `src/archetype/app/container.py`
- Core interfaces: `src/archetype/core/interfaces.py`
