# API Layer

Archetype runs as a single `archetype serve` process. The API layer is a FastAPI application that exposes the service layer over HTTP. The CLI is a thin httpx client that talks to that server. All worlds live in one event loop, and both interfaces share the same `ServiceContainer`.

## Application Factory

`create_app()` builds the FastAPI application with four routers and a lifespan manager:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    container = get_container()
    await container.world_service.discover_worlds()  # rehydrate from registry
    try:
        yield
    finally:
        await container.shutdown()
        set_container(None)

def create_app() -> FastAPI:
    app = FastAPI(title="Archetype ECS", version="0.1.0", lifespan=lifespan)
    app.include_router(worlds.router)       # /worlds
    app.include_router(commands.router)     # /worlds/{id}/commands
    app.include_router(simulation.router)   # /worlds/{id}/step, /run
    app.include_router(query.router)        # /worlds/{id}/state, /entities, /components
    return app
```

On startup, `discover_worlds()` reads the `WorldRegistry` and rehydrates any previously created worlds so state survives server restarts. On shutdown, all storage backends are flushed and closed.

## Dependency Injection

`deps.py` holds a module-level singleton `ServiceContainer` and provides getter functions for FastAPI's `Depends()`:

```python
_container: ServiceContainer | None = None
_default_ctx = ActorCtx(id=uuid7(), roles={"admin"})

def get_container() -> ServiceContainer:
    global _container
    if _container is None:
        _container = ServiceContainer(registry_path=default_registry_path())
    return _container

def get_world_service() -> WorldService:
    return get_container().world_service

def get_command_service() -> CommandService:
    return get_container().command_service

# ... one getter per service
```

Route handlers inject services via `Depends()`:

```python
@router.post("/worlds/{world_id}/step")
async def step_world(
    world_id: str,
    sim: SimulationService = Depends(get_simulation_service),
):
    ...
```

The container is initialized lazily on the first `get_container()` call. The `set_container(None)` call during shutdown ensures a fresh container on restart.

### Default Actor Context

In v0.1, there is no real auth. `get_actor_ctx()` returns a default `ActorCtx` with `roles={"admin"}`, granting wildcard permissions. Route handlers pass this context to `CommandService.submit()`, which forwards it to the broker's RBAC guardrails.

## Route Structure

Routes are thin. They validate request payloads, construct `Command` objects, delegate to services, and return response models.

### Worlds

| Endpoint | Method | What it does |
|----------|--------|-------------|
| `/worlds` | POST | Create world via CommandService (RBAC + audit), then apply immediately |
| `/worlds` | GET | List all managed worlds |
| `/worlds/{id}` | GET | Get world by ID |
| `/worlds/{id}` | DELETE | Remove world via CommandService |
| `/worlds/{id}/fork` | POST | Fork world via CommandService |

World mutations (create, remove, fork) go through `CommandService.submit()` for RBAC validation and audit logging, then `apply_world_lifecycle()` executes immediately -- world lifecycle commands are not tick-scheduled.

### Commands

| Endpoint | Method | What it does |
|----------|--------|-------------|
| `/worlds/{id}/commands` | POST | Submit single command to broker |
| `/worlds/{id}/commands/batch` | POST | Submit batch (all-or-nothing RBAC) |
| `/worlds/{id}/commands` | GET | Get command history |
| `/worlds/{id}/commands/pending` | GET | Get pending command count |

### Simulation

| Endpoint | Method | What it does |
|----------|--------|-------------|
| `/worlds/{id}/step` | POST | Execute one tick (drain + step) |
| `/worlds/{id}/run` | POST | Execute N ticks |
| `/worlds/{id}/processors` | GET | List active processors |

### Query

| Endpoint | Method | What it does |
|----------|--------|-------------|
| `/worlds/{id}/state` | GET | World snapshot at optional tick |
| `/worlds/{id}/entities/{eid}` | GET | Single entity state |
| `/worlds/{id}/components` | GET | Query by component types |
| `/worlds/{id}/history` | GET | Command history (read path) |

See the [REST API Reference](../reference/rest-api.md) for full request/response schemas.

## Route Pattern

Every mutation route follows the same pattern. Here is `create_world` as an example:

```python
@router.post("", response_model=WorldResponse)
async def create_world(
    req: CreateWorldRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    # 1. Construct a Command from the request payload
    cmd = Command(
        type=CommandType.CREATE_WORLD,
        payload={"config": {"name": req.name}, "storage_uri": req.storage_uri, ...},
    )

    # 2. Submit through the broker (RBAC + audit)
    await cs.submit("__global__", cmd, ctx)

    # 3. Apply (world lifecycle commands execute immediately)
    world = await cs.apply_world_lifecycle(cmd)

    # 4. Return response model
    return WorldResponse(world_id=str(world.world_id), name=world.name, tick=0)
```

The route does not contain business logic. It translates HTTP into a `Command`, submits it for governance, and returns the result. The service layer handles everything else.

## CLI

The CLI (`archetype` command) is a thin HTTP client built with Typer. It does not import the service layer.

```text
archetype serve              Starts uvicorn with the FastAPI app
archetype world create       POST /worlds
archetype world list         GET /worlds
archetype world inspect      GET /worlds/{id}
archetype world fork         POST /worlds/{id}/fork
archetype world remove       DELETE /worlds/{id}
archetype step               POST /worlds/{id}/step
archetype run                POST /worlds/{id}/run
archetype query              GET /worlds/{id}/state
archetype history            GET /worlds/{id}/history
archetype status             GET /worlds
```

Each command creates a fresh `httpx.Client`, makes one request, and exits. The server URL defaults to `http://localhost:8000` and can be overridden with the `ARCHETYPE_URL` environment variable.

See the [CLI Reference](../reference/cli.md) for full command documentation.

### Why a Thin Client

This design means:

- **All worlds share one event loop.** The server process owns all simulation state. Multiple CLI invocations (or API clients) interact with the same worlds.
- **The CLI is stateless.** No local state to manage, no import of heavy dependencies.
- **Distributed access.** Point `ARCHETYPE_URL` at a remote server and the CLI works identically.

## Source Reference

- App factory: `src/archetype/api/app.py`
- Dependency injection: `src/archetype/api/deps.py`
- Request/response models: `src/archetype/api/models.py`
- Routes: `src/archetype/api/routes/worlds.py`, `commands.py`, `simulation.py`, `query.py`
- CLI: `src/archetype/cli/main.py`
