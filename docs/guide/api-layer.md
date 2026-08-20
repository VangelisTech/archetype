# API Layer

Archetype runs as a single `archetype serve` process. The API layer is a
FastAPI application over one process-owned dispatcher, and the CLI is a thin
HTTP client.

External API operations authenticate an `ActorCtx`, construct exact family
operation models, and call actor-aware `CommandDispatcher` methods. Trusted
runtime calls construct the same models and use actor-free dispatcher entry.

## Application Factory

`create_app()` builds the FastAPI app with routers for worlds, commands, simulation, and queries:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_host_observability(service_name="archetype-api")
    resources = build_runtime_resources(RuntimeBootstrapConfig.from_env())
    app.state.resources = resources
    try:
        yield
    finally:
        await resources.aclose()
```

Imports and `create_app()` perform no logging or telemetry setup. Each worker
configures its host from the lifespan path, which keeps reload and multi-worker
startup explicit and idempotent. The factory does not automatically invoke
Logfire FastAPI instrumentation; optional Logfire or OTLP export is selected
through the vendor-neutral host adapter.

All worlds live in the server event loop. CLI invocations and remote clients talk to that process over HTTP.

## Dependency Injection

The FastAPI lifespan owns one `RuntimeResources`. `deps.py` exposes only its
dispatcher through `get_dispatcher(request)`.

Routes that expose user-visible operations should inject:

- `CommandDispatcher` for reads, writes, lifecycle, and simulation control.
- `ActorCtx` from auth middleware.

Routes do not receive concrete application services or the process wiring graph.

```python
@router.post("/worlds/{world_id}/run")
async def run_world(
    world_id: str,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    return await dispatcher.apply_as(
        ctx,
        Run(world_id=world_id, run_config=RunConfig(num_steps=10)),
    )
```

## Authentication context

The served API is a single-tenant development surface, and this is the
deliberate v0.6 posture. There is no authentication system: every caller is
the tenant. A request with no `Authorization` header receives the admin role;
`Bearer <role>` self-asserts one of admin/operator/player/viewer. Roles
therefore scope cooperating agents — a client instructed to send
`Bearer viewer` is genuinely viewer-scoped by the command gate — but they are
not a security boundary against an adversarial caller, and binding the server
beyond loopback exposes an unauthenticated admin API. The credentials that
matter are the storage credentials in Daft's `IOConfig`. Real
authentication/authorization against an external identity provider is a
future hardening slice. The trusted runtime has no actor context. See
[Command Gate](command-gate.md).

### Mission service principals

Agent Missions defines a separate authentication seam for the forthcoming
MissionRun control surface. It resolves an opaque bearer credential to a
stable principal id, explicit `mission:*` capabilities, and an execution-profile
allowlist. Developer role labels are never credentials on that seam. The
provisioning document stores either the name of an environment variable holding
the credential or its SHA-256 verifier; it does not store a plaintext credential.
Missing, malformed, unknown, expired, and revoked credentials fail closed.

This contract does not add a parallel mission-control router. A REST route may
be published only when it can construct the exact Missions operation and enter
the governed actor-aware boundary. Authentication does not mint a run id, and
a successful profile check is not a `202 Accepted` MissionRun. The durable
MissionRun owner remains the sole authority for run identity, ownership grants,
lifecycle, and pinned profile identity.

When the Missions library is installed, supported `archetype serve` startup on
a non-loopback bind requires a configured principal directory. Existing ECS
developer routes otherwise retain the deliberate v0.6 behavior above.

### MissionRun control surface

The durable MissionRun lifecycle is exposed as a small agent-safe REST
surface under `/v1/mission-runs`:

| Endpoint | Method | What it does |
|---|---|---|
| `/v1/mission-runs` | POST | Accept one durable run for a verified principal (`202`) |
| `/v1/mission-runs/{run_id}` | GET | Bounded run status projection |
| `/v1/mission-runs/{run_id}/events` | GET | Ordered durable events, `?after=<cursor>&limit=<n>` |
| `/v1/mission-runs/{run_id}/result` | GET | One immutable terminal result (`425` while open) |
| `/v1/mission-runs/{run_id}/cancel` | POST | Durably record cancellation intent (`202`, idempotent) |

Submission requires an `Idempotency-Key` header and a body carrying only
`profile_id`, repository coordinates, mission name, and an explicit bounded
task DAG with command validators. The same principal, key, and canonical
request digest return the original run; a changed digest under the same key
is a `409` conflict, and both survive an API-process restart. Route handlers
authenticate the verified principal, authorize capability, ownership, and
profile policy, then dispatch the exact registered `accept_mission_run`,
`get_mission_run`, `get_mission_run_events`, and `cancel_mission_run`
operations. They never open a runtime handle, construct Mission components,
start background tasks, or own recovery — supervision and recovery stay with
the missions-owned MissionRun lifecycle.

Events carry a deterministic `(run_id, cursor)` identity, a schema version,
a timestamp, a phase/type, and a sanitized bounded payload appended in the
same transaction as the durable transition, so `after` replay across
reconnects has no gaps, reordering, or duplicated logical events. Cancel
records intent durably before reporting acceptance; `cancelling` stays
distinct from `cancelled`, and completion races resolve to the committed
execution fact. Client disconnect is never cancellation.

Executing a REST-accepted run requires host composition: the execution
profile bound through the `world_library_configs` wiring input resolves the
exact pinned `(profile_id, version, digest)` to its trusted
`AgentMissionConfig` factory. An unbound host still accepts, observes, and
cancels runs; supervision records an honest `failed` run instead of
fabricating provider work. Reason text is redacted before it becomes a
durable fact and again at every projection, and terminal task facts bound
their commit lists, so provider errors cannot leak credential-shaped
content to any `mission:read` principal.

## Route Structure

Routes are thin translators: validate payloads, authenticate, call
actor-aware dispatcher entry, and return response models.

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

API routes authorize reads through `CommandDispatcher.apply_as`; trusted
runtime reads use `apply`. Both construct registered query models. The
dispatcher invokes handlers backed by `archetype.world.query` for
durable ECS reads and commands-owned `AuditLog` for `GetAuditHistory`. Neither
path requires a live world.
Routes may import frozen supported values from `archetype.world.models`; they
must not import world registry, lifecycle, mutation, simulation, query, or
handler behavior directly.
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
@router.post("", response_model=WorldInfo)
async def create_world(
    req: CreateWorldRequest,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    info = await dispatcher.apply_as(
        ctx,
        CreateWorld(
            config=req.world_config(),
            storage_config=req.storage(),
            cache_config=req.cache_config,
        ),
    )
    return info
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

- App factory: `packages/archetype-ecs/src/archetype/api/app.py`
- Dependency injection: `packages/archetype-ecs/src/archetype/api/deps.py`
- Request/response models: `packages/archetype-ecs/src/archetype/api/models.py`
- Routes: `packages/archetype-ecs/src/archetype/api/routes/`
- CLI: `packages/archetype-ecs/src/archetype/cli/main.py`
