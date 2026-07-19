# API Layer

Archetype runs as a single `archetype serve` process. The API layer is a FastAPI application over the service layer, and the CLI is a thin HTTP client.

External API operations enter through `iCommandGateway`. The gateway
authorizes and delegates to the same actor-free `iRuntimeApplication` semantics
used by the trusted runtime.

## Application Factory

`create_app()` builds the FastAPI app with routers for worlds, commands, simulation, and queries:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_host_observability(service_name="archetype-api")
    container = get_container()
    try:
        yield
    finally:
        await container.shutdown()
        set_container(None)
```

Imports and `create_app()` perform no logging or telemetry setup. Each worker
configures its host from the lifespan path, which keeps reload and multi-worker
startup explicit and idempotent. The factory does not automatically invoke
Logfire FastAPI instrumentation; optional Logfire or OTLP export is selected
through the vendor-neutral host adapter.

All worlds live in the server event loop. CLI invocations and remote clients talk to that process over HTTP.

## Dependency Injection

`deps.py` owns a module-level `ServiceContainer` and exposes service getters for FastAPI `Depends()`.

Routes that expose user-visible operations should inject:

- `iCommandGateway` for reads, writes, lifecycle, and simulation control.
- `ActorCtx` from auth middleware.

Lower-level services may still be injected for internal/admin diagnostics, but they are not the normal public boundary.

```python
@router.post("/worlds/{world_id}/run")
async def run_world(
    world_id: str,
    gateway: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    return await gateway.run(ctx, UUID(world_id), RunConfig(num_steps=10))
```

## Authentication context

Production ingress authenticates a stable principal and fails closed when
credentials are absent or invalid. A development-only anonymous-admin mode, if
enabled explicitly, uses one stable process principal so quotas and audit
identity do not reset on every request. The trusted runtime has no actor
context. See [Command Gate](command-gate.md).

## Route Structure

Routes are thin translators: validate payloads, authenticate, call
`iCommandGateway`, and return response models.

### Worlds

| Endpoint | Method | What it does |
|---|---|---|
| `/worlds` | POST | Create world through the gate |
| `/worlds` | GET | List managed worlds / world info |
| `/worlds/{id}` | GET | Get `WorldInfo` |
| `/worlds/{id}` | DELETE | Destroy the live world; persisted data remains |
| `/worlds/{id}/fork` | POST | Fork a world through the gate |

Lifecycle operations are direct gate calls, not tick-deferred scheduler commands.

### Commands

| Endpoint | Method | What it does |
|---|---|---|
| `/worlds/{id}/commands` | POST | Authorized durable deferred admission |
| `/worlds/{id}/commands/batch` | POST | Tick-deferred batch submit |
| `/worlds/{id}/commands` | GET | Audit-backed command history |

Command-ledger pending state is an implementation detail. User-facing history is
audit-backed through `/worlds/{id}/history` and `/worlds/{id}/commands`.

### Simulation

| Endpoint | Method | What it does |
|---|---|---|
| `/worlds/{id}/step` | POST | Execute one tick |
| `/worlds/{id}/run` | POST | Execute N ticks |
| `/worlds/{id}/episode` | POST | Run until termination or cap on this world |
| `/worlds/{id}/rollout` | POST | Fork N episodes and aggregate |
| `/worlds/{id}/processors` | GET | List processor info |

See [Execution Hierarchy](execution-hierarchy.md).

### Query

| Endpoint | Method | What it does |
|---|---|---|
| `/worlds/{id}/state` | GET | Query world state through the gate |
| `/worlds/{id}/entities/{eid}` | GET | Query one entity projection |
| `/worlds/{id}/components` | GET | Lazily filter, limit, or count component projections |
| `/worlds/{id}/history` | GET | Audit history through `get_audit_history` |

Reads are authorized at `iCommandGateway`; `iQueryService` remains the internal
read implementation behind RuntimeApplication.
The component route accepts one inert comparison through `where`, then applies either the
`show` row limit or the `count` terminal. Filtering happens before either terminal and before
row serialization. All three options require at least one component type; `show` and `count` are
mutually exclusive. The filter grammar is deliberately small: one component column, one of `>`,
`>=`, `<`, `<=`, `==`, or `!=`, and one scalar value. Calls, attribute access, arithmetic, and
Boolean composition are rejected rather than evaluated.

See the [REST API Reference](../reference/rest-api.md) for generated schemas.

## Route Pattern

Example `create_world` route:

```python
@router.post("", response_model=WorldResponse)
async def create_world(
    req: CreateWorldRequest,
    gateway: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    info = await gateway.create_world(
        ctx,
        WorldConfig(name=req.name),
        StorageConfig(uri=req.storage_uri) if req.storage_uri else None,
    )
    return WorldResponse(
        world_id=str(info.world_id),
        name=info.name,
        tick=info.tick,
    )
```

The route does not construct a lifecycle command or bypass the gate.

## CLI

The CLI (`archetype` command) is a thin HTTP client.

`serve` is the sole CLI process-host path: it configures observability before
starting Uvicorn. Every other CLI command remains an HTTP client and performs
no local provider or handler setup.

```text
archetype serve              Starts uvicorn with the FastAPI app
archetype world create       POST /worlds
archetype world list         GET /worlds
archetype world inspect      GET /worlds/{id}
archetype world fork         POST /worlds/{id}/fork
archetype world destroy      DELETE /worlds/{id}
archetype entity spawn       POST /worlds/{id}/entities
archetype step               POST /worlds/{id}/step
archetype run                POST /worlds/{id}/run
archetype episode            POST /worlds/{id}/episode
archetype rollout            POST /worlds/{id}/rollout
archetype query              GET /worlds/{id}/state or /worlds/{id}/components
archetype history            GET /worlds/{id}/history
archetype processors list    GET /worlds/{id}/processors
archetype hooks list         GET /worlds/{id}/hooks
archetype resources list     GET /worlds/{id}/resources
```

The server URL defaults to `http://localhost:8000` and can be overridden with
`ARCHETYPE_URL` or per command with `--url`. HTTP commands accept the developer
auth shortcut `--role` / `-r` and the bearer-token option `--token`.

Without component types, `query` returns the world-state projection. Pass comma-separated
component types positionally to use the lazy component-query path:

```bash
archetype query <world-id> Agent,Score --where "score__value > 0.5" --show 5
archetype query <world-id> Agent,Score --where "score__value > 0.5" --count
```

`--types` remains available as a compatibility spelling for the positional component list.

## Source Reference

- App factory: `src/archetype/api/app.py`
- Dependency injection: `src/archetype/api/deps.py`
- Request/response models: `src/archetype/api/models.py`
- Routes: `src/archetype/api/routes/`
- CLI: `src/archetype/cli/main.py`
