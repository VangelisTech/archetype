# Runtime

**Document type:** Normative.
**Scope:** `src/archetype/runtime/` — the trusted Python scripting boundary.

Ordinary runtime/world operations depend on the actor-free
`iRuntimeApplication` port. They never depend on the command gateway or
authorization models. Agent Missions follows the same boundary: its specialized
runtime handle receives an `iMissionService` workflow through
`iRuntimeApplication`, while the container selects the concrete service.

## 1. Purpose

`ArchetypeRuntime` is the primary supported Python API. It:

1. owns one internal `ServiceContainer` and its actor-free
   `RuntimeApplication`;
2. owns lazy world handles and process lifetime;
3. provides ergonomic async and sync scripting semantics; and
4. delegates canonical world operations to `iRuntimeApplication`.

The runtime is a trusted in-process boundary. Possession of the runtime grants
the host the capabilities it was constructed with; it does not fabricate a
default administrator or simulate RBAC. Untrusted callers use
`CommandGateway` through an ingress adapter.

## 2. Hard requirements

### R1 — Application-port-only execution

Ordinary runtime modules may import only:

- the `app.application` port and boundary-safe models;
- supported cross-boundary component/configuration/result types; and
- the internal container from runtime composition code only.

They may not import `app.gateway`, `app.auth`, concrete family services,
  command schedulers, ledgers, backend clients, or API modules.

### R2 — Handles hold identity, never live capabilities

`RuntimeWorld` holds its runtime/application reference, configuration, local
lifecycle state, and a `world_id` after activation. It never holds an
`AsyncWorld`, concrete app service, backend client, or container reference.

### R3 — Runtime is actor-free

`ArchetypeRuntime`, `RuntimeWorld`, and their sync variants do not accept or
retain `ActorCtx`. The supported runtime surface has no `as_actor()` operation.
Role testing and multi-tenant embedding exercise `CommandGateway` through
focused security fixtures or an authorized host adapter.

Trusted deferred submissions persist an explicit local origin. They do not
record a fictional authorization decision.

### R4 — Async context manager is canonical

```python
async with ArchetypeRuntime() as runtime:
    ...
```

Shutdown must:

1. stop admitting new runtime and handle operations;
2. wait for every already-admitted world operation;
3. close handles without destroying attached or runtime-owned durable worlds;
4. call `container.shutdown()` only after admitted work drains;
5. attempt every cleanup step and aggregate failures; and
6. be idempotent.

### R5 — Sync parity

`SyncArchetypeRuntime` and `SyncRuntimeWorld` expose the same product semantics
as the async classes without `await`. The sync facade owns an `asyncio.Runner`
and does not reuse an outer event loop.

### R6 — World handles are declarative and lazy

```python
world = runtime.world(
    "demo",
    storage="./data",
    cache=CacheConfig(...),
    processors=[Movement()],
    resources=[shared_state],
    hooks=[(PreTick, on_tick)],
)
```

The call captures configuration but does not create a world. The first
operation single-flights activation through `iRuntimeApplication`. Processors,
resources, and hooks are installed in declared order. A failed activation must
clean up any partially created live world and remain retryable when the failure
is transient.

### R7 — Per-world operation serialization

All application operations that target the same live world are serialized by
the application layer, regardless of which runtime handle or API request
originated them. A handle-local lock may improve ergonomics but is not the
concurrency authority. Different worlds may proceed concurrently.

### R8 — Runtime and world lifetimes are distinct

One runtime may own many handles. Closing a handle waits for work admitted
through that handle and invalidates the local view; it does not tear down shared
process services. Runtime shutdown owns shared services.

`runtime.attach(world_id)` returns a non-owning handle. Closing it never
destroys the world. Destruction is an explicit application operation and does
not delete append-only durable rows.

### R9 — Boundary-safe results

The runtime receives immutable information snapshots, identifiers, typed
receipts, supported result/configuration models, and explicitly specified
DataFrames. It never receives a live world, service, registry, container,
credential, or backend client.

### R10 — Storage and cache coercion

`storage: str | Path | StorageConfig | None` and `cache: CacheConfig | None` are
accepted at the scripting boundary. A string or path becomes
`StorageConfig(uri=str(value))`; richer backend policy remains below runtime.

### R11 — Evaluation and research

`world.grade(...)` delegates to the application evaluation workflow. The
workflow owns snapshot pinning, grader execution, outcome validation, and
durable receipts where requested. The runtime does not compose QueryService and
EvaluationService itself.

`world.autoresearch(...)` delegates to the research-family workflow. Callback
execution must not hold a runtime handle lock that would deadlock reentrant
runtime operations.

`runtime.evaluate_physical_task(...)` and
`runtime.sweep_physical_instructions(...)` delegate typed requests to the
physical-AI application workflow. The runtime does not install processors,
reset provider state, spawn trial entities, run episodes, or collect terminal
rows itself. Returned reports carry the durable world/run identity from which
their values were derived. The sync runtime exposes the same operations.

### R12 — Typed artifacts and transcript evidence

The supported target vocabulary is artifact/evidence publication. Runtime
methods may ingest files, structured rows, or content and return typed artifact
receipts. The runtime does not inspect storage catalogs, implement content
identity, complete publication claims, or expose generic domain "artifacts."

`world.ingest_claude_transcript(source)` is the recommended coding-agent
transcript boundary. `ClaudeTranscriptSource` carries local input configuration
and stable project/session identity. The application workflow snapshots and
redacts the file, parses only the sanitized copy, publishes its lightweight
trajectory/source claim, and appends normalized rows to the Iceberg transcript
table. The returned `TranscriptIngestionReceipt` reports both authorities and
their replay outcome. The runtime does not open the source file, write
narrative Components, or coordinate those steps itself. Sync world handles
expose the same operation.

The artifact-bundle DTOs exported from the top-level `archetype` package are
supported runtime contracts: `ArtifactBundleRequest`, `ArtifactCandidate`,
`ArtifactIndexRecord`, `ArtifactPublishReceipt`, `ArtifactReconcileResult`,
`ArtifactSourceResolver`, `BoundedArtifactSourceResolver`,
`ArtifactStoreConfig`, and `MaterializedArtifact`.
Their physical home is the `archetype.artifacts.bundles` family module
(#558); application code imports the top-level names. A top-level path does
not make additional names supported, and nothing here grants access to the
concrete artifact service.

`world.ingest_files()`, `world.write_artifacts()`, and `world.artifacts()` are
the lower-level typed-table surfaces used by other domains and custom
processors. `world.publish()` is the claim-backed Component artifact surface.
They are separate because an artifact table row, a source claim, and a portable
artifact bundle have different durability and replay contracts.

### R13 — Observability is host-configured and quiet by default

Scripts own stdout. `ARCHETYPE_LOG=debug|info|warning|error` or
`ArchetypeRuntime(log=...)` configures the `archetype` logger at the runtime
boundary. Core and app layers emit records but configure no handlers. Imports
remain silent. At construction an otherwise unconfigured runtime owns at most
one package handler: a null handler by default, or a stderr handler when
logging is enabled. A later default runtime preserves an already enabled
handler. The adapter does not alter root logging, the global `LogRecordFactory`,
or foreign handlers and filters, so machine-readable stdout remains
deterministic.

When logging is enabled, the owned handler's fail-open filter replaces reserved
correlation fields with the active lowercase trace/span IDs and the validated
safe signal context. Callers cannot forge those fields through `extra`; with no
active context the fields are absent. Its formatter omits exception and stack
metadata from its own stderr rendering and substitutes placeholders for
non-primitive arguments, then restores those producer fields for later
host-owned handlers. Correlation fields remain enriched. Producer-side policy
remains responsible for sensitive text already supplied as a primitive string.

Tracing uses the OpenTelemetry API. A host-registered provider is respected;
optional Logfire or OTLP backends are selected only at the host/runtime
boundary. With no configured backend the API remains a no-op and does not
prevent a later host from registering one. Signal names, safe attributes,
failure handling, and metric cardinality follow the normative
[Observability contract](observability.md); telemetry never changes a runtime
result or exception.

`ARCHETYPE_OTLP_TRACES_ENDPOINT` is the explicit endpoint for Archetype's
filtered HTTP/protobuf traces. Standard generic or trace-specific OTLP
endpoints are accepted as compatibility inputs, consumed before Daft import,
and kept out of child environments. Only
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` opts Daft into native physical-execution
telemetry; Daft logs and traces are not enabled by an Archetype host. See the
process-host endpoint matrix in the observability contract before combining
Archetype with an externally configured OTel process.

### R14 — Public callables do not accept raw services

Supported callables may accept `ArchetypeRuntime`, handles, configuration,
components, callbacks, and safe models. They may not require callers to pass a
concrete service or `ServiceContainer`. Repository checks enforce this rule.

### R15 — Multiple runtimes

A process may hold multiple runtimes. Each owns its container unless an
explicit internal host composition injects one. Cross-runtime live-handle
transfer is out of scope; durable identity and storage coordinates are the
interchange boundary.

### R16 — Agent Missions V1

`runtime.missions(name, config=..., storage=...)` returns an async
`RuntimeMissions` handle. It configures one mission-capable world with the
built-in Components, graph view, transition processors, post-tick outbox, and
injected Sandbox Backend and coding-agent driver. The family-owned Sandbox
Service retains live Sessions. Authors submit typed tasks and never wire that
bundle themselves.

The handle owns the specialized mission-world lifetime. Closing it closes the
sandbox resource and its world handle; closing it does not close the parent
runtime. A terminal run closes that mission's provider session. The runtime
tracks live mission handles and closes them before its remaining world handles,
so runtime shutdown remains the outer process boundary even after a failed or
abandoned mission.

`RuntimeMissions` obtains its internal `iMissionService` through
`iRuntimeApplication`; it neither imports the concrete service nor receives the
container. V1 is still async-only, so sync parity under R5 remains a hardening
gap. See
[Agent Missions V1, current hardening gaps](agent-missions.md#current-hardening-gaps).

## 3. Canonical surface

The async surface below has sync parity:

```python
world = runtime.world(
    name,
    storage=...,
    cache=...,
    processors=...,
    resources=...,
    hooks=...,
)
world = runtime.attach(world_id, storage=...)

missions = runtime.missions(
    "software-factory",
    config=AgentMissionConfig(
        sandbox_backend=my_backend,
        sandbox_environment="provider-image@sha256:digest",
    ),
    storage=...,
)
submitted = await missions.submit(
    repository="owner/repository",
    branch="agent/change",
    tasks=(AgentTask(...),),
)
mission_result = await missions.run(submitted)

eid = await world.spawn(Position(x=0), Velocity(dx=1))
ids = await world.spawn_batch(Position(x=0), count=10_000)
ids = await world.spawn_many([[Position(x=float(i))] for i in range(100)])
await world.despawn(eid)
await world.update(eid, Position(x=10))
await world.add_components(eid, Health(hp=100))
await world.remove_components(eid, Velocity)

artifact = await world.ingest_artifact(...)

await world.add_processor(MyProcessor())
await world.remove_processor(MyProcessor)

await world.step()
result = await world.run(steps=10)
episode = await world.run_episode(EpisodeConfig(...))
rollout = await world.run_rollout(RolloutConfig(...))
research = await world.autoresearch(AutoResearchConfig(...), evaluator)

branch = await world.fork(name="branch-a")
await world.destroy()

df = await world.query(Position, Velocity)
outcomes = await world.grade(Position, graders=[grader])
info = await world.info()
history = await world.history(limit=100)
processors = await world.list_processors()
hooks = await world.list_hooks()
resources = await world.list_resources()

hook = await world.add_hook(PreTick, on_tick)
await world.remove_hook(hook)
await world.shutdown()
```

Component instances and types remain variadic at the ergonomic boundary.
`update` overlays existing component types; `add_components` changes the
entity's archetype. These intents remain distinct.

## 4. Out of scope

- HTTP/FastAPI and authentication;
- authorization or role simulation;
- direct command-ledger, scheduler, audit-store, or backend access;
- cross-runtime live-object transfer;
- distributed process coordination inside the runtime package; and
- schema migration policy.

## 5. Module layout

```text
src/archetype/runtime/
  __init__.py
  runtime.py       ArchetypeRuntime and SyncArchetypeRuntime
  missions.py      async Agent Missions authoring/lifecycle handle
  world.py         RuntimeWorld and SyncRuntimeWorld
  entrypoint.py    managed script decorator
  _config.py       scripting-boundary coercion
```

Storage session construction lives below runtime so the app layer never imports
outward from this package.

## 6. Canonical example

```python
from archetype import ArchetypeRuntime

with ArchetypeRuntime.sync() as runtime:
    world = runtime.world("demo", processors=[Movement()])
    entity_id = world.spawn(Position(), Velocity(dx=1, dy=2))
    world.run(steps=3)
    print(world.query(Position).collect().to_pylist())
```

## 7. Companion specifications

- [Application Architecture](application-architecture.md)
- [Command Gate](command-gate.md)
- [Execution Hierarchy](execution-hierarchy.md)
- [World Lifecycle](world-lifecycle.md)
- [Service Protocols](service-protocols.md)
- [Audit Log](audit-log.md)
- [Agent Missions V1](agent-missions.md)
