# Runtime

**Document type:** Normative.
**Scope:** `src/archetype/runtime/` — the script-boundary layer.

## 1. Purpose

The runtime is the **script boundary**. It is what users import to drive Archetype from a Python script — distinct from the API layer (HTTP / FastAPI) and the CLI layer.

It exists to do four things, and nothing else:

1. Own process-lifetime state — one `ServiceContainer`, one default `ActorCtx`, a registry of live world handles.
2. Provide an ergonomic, typed user surface. Verbose service-layer signatures end at the gate; the runtime absorbs the noise.
3. Forward every operation through `iCommandService` with the handle's bound `ActorCtx`. The runtime is the *only* layer that holds an `ActorCtx`-script default.
4. Provide a sync facade for users who don't want to type `await`.

## 2. Hard requirements

### R1 — Single-gate enforcement

The runtime module imports from `archetype.app` ONLY:

- `archetype.app.command_service`
- `archetype.app.container`
- `archetype.app.models`
- `archetype.app.auth.models`

Imports from `archetype.app.{mutation_service, simulation_service, query_service, world_service, broker}` are forbidden. Any such import is a spec violation; the gate is leaking.

### R2 — Handles hold `world_id`, never `iWorld`

`RuntimeWorld` MUST hold only `world_id: UUID` (plus the bound `ActorCtx`, runtime reference, and configuration). It MUST NOT hold a reference to an `iWorld` / `AsyncWorld` instance.

This is what makes R1 enforceable. With no instance to hold, there is no path to bypass the gate — every operation MUST flow through `iCommandService` because there is no other path.

### R3 — One handle, one `ActorCtx`

Every world handle is bound to exactly one `ActorCtx`. Operations on the handle forward that ctx to `CommandService` automatically — users never pass `ActorCtx` per call.

A handle MAY be re-bound to a different `ActorCtx` to produce a sibling handle that shares the underlying world (`world.as_actor(ctx)`).

### R4 — Default `ActorCtx` is `{admin}`

Construction of the runtime without an explicit `ActorCtx` succeeds and uses `ActorCtx(id=uuid7(), roles={"admin"})`. The runtime is the script boundary; in single-tenant context, that boundary IS the platform admin.

Users testing under a constrained role do so explicitly via `as_actor`. See `command-gate.md` § 3.

### R5 — Async context manager is the canonical lifecycle

```python
async with ArchetypeRuntime() as runtime: ...
```

`__aexit__` MUST:

1. Shut down all live world handles (LIFO order).
2. Call `container.shutdown()`.
3. Be idempotent. Errors during shutdown are aggregated and re-raised after best-effort completion.

### R6 — Sync parity is part of the contract

`SyncArchetypeRuntime` exposes the same surface as the async runtime, implemented via `asyncio.Runner`. Every method on `RuntimeWorld` has a matching method on `SyncRuntimeWorld`.

The sync facade owns its own `asyncio.Runner` and does NOT share with any outer event loop.

### R7 — World handles are declarative

```python
world = runtime.world(
    "demo",
    storage="./data",
    cache=CacheConfig(...),
    processors=[Movement(), AI()],
    resources=[some_shared_state],
    hooks=[(PreTick, on_tick), (OnSpawn, on_spawn)],
)
```

The factory call constructs a handle and captures configuration. The world is created on first operation that needs it (lazy single-flight init).

All initial configuration — processors, resources, hooks — is supplied at handle creation. Post-init reconfiguration uses methods (`world.add_processor(...)`, etc.). Pre-activation `add_hook` calls raise.

### R8 — Lazy single-flight activation

The first ergonomic call on a `RuntimeWorld` triggers world creation. Concurrent first-uses single-flight via an init lock; only one `CommandService.create_world` call is made per handle.

Activation flow at the runtime layer:

```text
1. Build WorldConfig (serializable identity only).
2. command_service.create_world(ctx, config, storage, cache) -> WorldInfo
3. For each init_processor: command_service.add_processor(ctx, world_id, proc)
4. For each init_resource:  command_service.add_resource(ctx, world_id, resource)
5. For each init_hook:      command_service.add_hook(ctx, world_id, event, fn)
6. Cache world_id in handle state; mark initialized.
```

WorldConfig stays serializable. Resources, processors, hooks are arbitrary Python objects and flow through dedicated gated paths — never through WorldConfig.

### R9 — Multi-runtime per process

A Python process MAY hold multiple `ArchetypeRuntime` instances. Each owns its own `ServiceContainer`. Forks happen within one runtime; cross-runtime handle transfer is out of scope.

### R10 — Audit-backed history

`world.history(...)` reads from `iAuditLog` via `iCommandService.get_audit_history`. The runtime does not maintain a separate history.

### R11 — Info-class downgrade at the gate

The runtime returns immutable info-class snapshots in place of live objects:

- `iCommandService.create_world` / `fork_world` / `get_world_info` → `WorldInfo`
- `iCommandService.list_processors` → `list[ProcessorInfo]`
- `iCommandService.list_hooks` → `list[HookInfo]`
- `iCommandService.list_resources` → `list[ResourceInfo]`

Field access on info objects is sync; the fetch is async (gated). See `world-lifecycle.md` § 5.

### R12 — Storage and cache configs accept friendly types

`storage: str | Path | StorageConfig | None` and `cache: CacheConfig | None`. String / Path becomes `StorageConfig(uri=str(value))`. None → defaults. This is the only configuration coercion the runtime is allowed; richer transformations live in `core.config`.

### R13 — Forks and destruction are first-class handles

- `world.fork(name?)` returns a new world handle bound to the same `ActorCtx`. Fork goes through `iCommandService.fork_world`. The new handle is registered for shutdown.
- `world.destroy()` calls `iCommandService.destroy_world`. The handle becomes invalid; subsequent operations raise. Storage and audit rows are NEVER deleted.

### R14 — Attached handles do not own their world

`runtime.attach(world_id)` returns a handle for a world that already exists in this runtime — an episode world left behind by a rollout, an autoresearch lab world, or any world created elsewhere in the process. The id is validated on first operation. Shutting the handle down never destroys the world; only an explicit, gated `destroy()` can. Reads resolve the world's recorded storage through the gate, so an attached handle finds rows wherever the world actually wrote them.

### R15 — AutoResearch and grading route through the gate

- `world.autoresearch(config, evaluator, ...)` is gated as `CommandType.AUTORESEARCH` (operator+) and emits one loop-level audit row; per-attempt provenance lives on the experiment's lab world. The base world is never mutated. The handle's op lock is held only for activation, so `evaluator`, `prepare_candidate`, and `on_iteration` may call back into runtime handles (`query`, `attach`, `grade`) without deadlocking.
- `world.grade(*components, graders=[...])` composes the gated, lineage-resolved `query` with `EvalService.run_graders`. Graders receive one lazy Daft DataFrame of the full append-only history and decide what to compute; the runtime materializes nothing. Empty grader lists and empty grader outputs are rejected, never vacuous successes.

### R16 — Observability is quiet by default, one flag turns it up

Scripts own their stdout: no span or log output unless asked. `ARCHETYPE_LOG=debug|info|warning|error` (or `ArchetypeRuntime(log=...)`) wires the stdlib `archetype` logger hierarchy at the runtime boundary; at `debug` it also enables console span output. Every layer *emits* on module loggers — core included, since processor failures and store writes are exactly what debugging needs — but only the runtime *configures* handlers, levels, and sinks. `RunConfig(debug=True)` remains separate: it is per-run dataframe inspection (it materializes frames) and is never switched by an environment variable.

### R17 — Tracing is vendor-neutral OpenTelemetry

Archetype emits spans through the OpenTelemetry *API* only (`archetype._obs`); installing archetype pulls no telemetry vendor. Backend selection happens once, at the runtime boundary, in this order: a provider the host application already registered is respected untouched; `LOGFIRE_TOKEN`/`LOGFIRE_API_KEY`/`LOGFIRE_SEND_TO_LOGFIRE` select Logfire when the `archetype-ecs[logfire]` extra is installed (Logfire is itself an OTel SDK); `OTEL_EXPORTER_OTLP_ENDPOINT` selects the standard OTLP exporter (`archetype-ecs[otlp]`) and works with any collector; `ARCHETYPE_LOG=debug` gets a terse one-line console exporter on stderr with no extras at all; otherwise the no-op OTel API stays and tracing costs nothing. Nothing under `src/` may import `logfire` except the guarded backend selection and `contrib/logfire_observer`.

### R18 — Typed facts are explicit Iceberg surfaces

`world.ingest_files(paths, processor)` and `world.write_facts(table_name, df)`
delegate to `iFactService` through the gate. `world.facts(table_name)` is the
corresponding gated read. The runtime does not inspect files, compute content
identity, translate credentials, or touch a catalog directly. Async and sync
handles expose identical fact surfaces. The older
`world.ingest(..., external_id=...)` remains the claim-backed compatibility
path described in [Durable Facts](durable-facts.md). Typed facts are scoped to
the handle's current world and run; fork handles do not inherit ancestor fact
rows.

### R19 — Public API is gate-addressable, and the machine checks

`@public_api` (top-level export) marks archetype's supported callable surface
and carries an enforced contract: **a public callable may not accept raw
services**. Public capability must be expressible through `ArchetypeRuntime`
and its gated handles, so every mutation carries command-audit provenance —
a function taking `world_service=`/`simulation_service=` forces callers to
hand-roll a `ServiceContainer` and bypass the gate (observed twice before
this rule existed). `scripts/check_api_import_boundaries.py` enforces the
signature rule and the import scopes (`experiments` may import app *models*
only; the runtime is the sole public consumer of app). Deprecated
service-shaped bridge parameters live in the checker's allowlist with a
removal deadline.

`@archetype.entrypoint()` is the script boundary as a decorator — an
evolution of the runtime, not a surface beside it: it constructs the runtime
(env-driven configuration per R16/R17), bridges sync/async, injects the
runtime as the first argument, and guarantees teardown. Eval helpers
(`archetype.experiments.eval_rollouts.run_task_eval`,
`archetype.experiments.instruction_sweep.run_instruction_sweep`) are
runtime-first: pass `runtime=`; their raw-service keyword forms are
deprecated bridges removed in v0.6.

## 3. Ergonomic surface

The full canonical surface, async and sync:

```python
# Construction (factory on runtime)
world = runtime.world(name, storage=..., cache=..., processors=..., resources=..., hooks=...)
world = runtime.attach(world_id)   # handle for an existing world (episode/lab worlds)

# Mutations
eid = await world.spawn(Position(x=0), Velocity(dx=1))
ids = await world.spawn_batch(Position(x=0), 10_000)
ids = await world.spawn_many([[Position(x=float(i))] for i in range(100)])
await world.despawn(eid)
await world.update(eid, Position(x=10))                  # OVERLAY values
await world.add_components(eid, Health(hp=100))          # EXTEND schema
await world.remove_components(eid, Position, Velocity)

# External facts (Iceberg)
receipt = await world.ingest_files("inputs/**/*.json", JsonFacts())
receipt = await world.write_facts("measurements", existing_daft_pipeline)

# Processors
await world.add_processor(MyProcessor())
await world.remove_processor(MyProcessor)

# Simulation
await world.step()
await world.run()                                        # 1 step
await world.run(steps=10)
await world.run(config=RunConfig(...))
await world.run_episode(EpisodeConfig(...))
await world.run_rollout(RolloutConfig(...))
await world.autoresearch(AutoResearchConfig(...), evaluator)  # optimization loop

# Lifecycle
await world.fork(name="branch_a")                        # → new handle
await world.destroy()                                    # in-memory only

# Reads
df = await world.query(Position, Velocity)
outs = await world.grade(Position, graders=[my_grader])  # query + graders
info = await world.info()                                # WorldInfo snapshot
df = await world.history(limit=100)
df = await world.facts("measurements")
procs = await world.list_processors()
hooks = await world.list_hooks()
res = await world.list_resources()

# Hooks (post-activation only; pre-activation goes via runtime.world(..., hooks=[...]))
handle = await world.add_hook(PreTick, my_handler)
await world.remove_hook(handle)

# Identity rebinding
sibling = world.as_actor(other_actor_ctx)

# Sync-readable (no round-trip)
world.world_id, world.name

# Lifecycle (rare)
await world.shutdown()
```

`SyncRuntimeWorld` matches identically without `await`.

### 3.1 — Variadic component args

Component instances and types are passed as `*args`:

```python
world.spawn(Position(), Velocity())                  # instances
world.remove_components(eid, Position, Velocity)     # types
world.query(Position, Velocity)                      # types
```

The verbose service-layer signatures (`components: list[Component]`, `component_types: list[type[Component]]`) end at the gate.

`spawn_batch(template, count)` repeats one archetype template:

```python
ids = await world.spawn_batch(Position(x=0), 10_000)
ids = await world.spawn_batch(Position(x=0), Velocity(dx=1), count=10_000)
```

Use `spawn_many(...)` when each entity has distinct initial values:

```python
ids = await world.spawn_many([[Position(x=float(i))] for i in range(10_000)])
```

### 3.2 — `update` vs. `add_components` are distinct

- `update(eid, *components)` — overlays values on existing components. Same archetype.
- `add_components(eid, *components)` — extends the entity's archetype with new component types.

Distinct user intents → distinct methods.

## 4. Out of scope

- HTTP / FastAPI integration. The API layer uses `CommandService` directly with `ActorCtx` from auth middleware.
- Distributed multi-process runtime.
- Cross-runtime fork or world-handle transfer.
- Async-iterator surfaces over hook events.
- Plugin / extension registration at the runtime layer.
- Schema migration across handle reuses.
- Direct broker access from the runtime.

## 5. Module layout

```text
src/archetype/runtime/
├── __init__.py        public exports
├── runtime.py         ArchetypeRuntime, SyncArchetypeRuntime, run_sync
├── world.py           RuntimeWorld, SyncRuntimeWorld, _RuntimeWorldState
├── session.py         configure_session (Iceberg)
└── _actor.py          default_actor_ctx() factory
```

## 6. Canonical example

```python
import asyncio
from archetype import ArchetypeRuntime, AsyncProcessor, Component
from archetype.core.hooks import PreTick
from daft import DataFrame, col


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


class Movement(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__dx"),
            "position__y": col("position__y") + col("velocity__dy"),
        })


async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "demo",
            processors=[Movement()],
            hooks=[(PreTick, lambda e: print(f"tick {e.tick}"))],
        )
        eid = await world.spawn(Position(), Velocity(dx=1, dy=2))
        await world.run(steps=3)
        df = await world.query(Position)
        print(df.collect().to_pylist())


asyncio.run(main())
```

Sync equivalent (component / processor definitions identical):

```python
from archetype import ArchetypeRuntime

with ArchetypeRuntime.sync() as runtime:
    world = runtime.world("demo", processors=[Movement()])
    eid = world.spawn(Position(), Velocity(dx=1, dy=2))
    world.run(steps=3)
    print(world.query(Position).collect().to_pylist())
```

## 7. Companion specs

- `command-gate.md` — Policy enforcement point, roles, audit emission.
- `execution-hierarchy.md` — Step / Run / Episode / Rollout semantics.
- `world-lifecycle.md` — Fork, destroy, info-class downgrade, append-only invariant.
- `service-protocols.md` — `iCommandService` and the services it gates.
- `audit-log.md` — Append-only audit row schema and query semantics.
