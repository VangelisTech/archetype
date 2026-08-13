# Runtime

**Document type:** Normative.
**Scope:** `packages/archetype-ecs/src/archetype/runtime/` — the trusted Python
scripting boundary, plus typed adapters supplied by installed world libraries.

Ordinary runtime/world operations construct exact family operation models and
enter the process-owned `CommandDispatcher` through `apply()` or `defer()`.
They never import authentication or actor models. World-library adapters follow
the same boundary: they dispatch exact family operations, while each library's
private extension composes its internal workflow only inside process ownership
provided by the framework.

## 1. Purpose

`ArchetypeRuntime` is the primary supported Python API. It:

1. owns one explicit `RuntimeResources` process graph;
2. creates lazy, strongly registered world and workflow handles;
3. provides ergonomic async and sync scripting semantics; and
4. sends canonical exact operations through the shared `CommandDispatcher`.

The runtime is a trusted in-process boundary. Possession of the runtime grants
the host the capabilities it was constructed with; it does not fabricate a
default administrator or simulate RBAC. Untrusted callers authenticate at an
ingress adapter, which enters the same dispatcher through actor-aware methods.

## 2. Hard requirements

### R1 — Exact-operation dispatcher execution

Ordinary runtime modules may import only:

- the commands dispatcher contract and exact family operation models;
- supported cross-boundary component/configuration/result types; and
- `RuntimeResources` lifetime state.

They may not import API authentication, concrete family services, command
schedulers, ledgers, backend clients, or API modules. Concrete construction is
late-bound through `archetype.wiring`.

### R2 — Handles hold identity, never live capabilities

`RuntimeWorld` holds its runtime/resources reference, configuration, local
lifecycle state, and a `world_id` after activation. It never holds an
`AsyncWorld`, concrete app service, backend client, or wiring graph.

### R3 — Runtime is actor-free

`ArchetypeRuntime`, `RuntimeWorld`, and their sync variants do not accept or
retain `ActorCtx`. The supported runtime surface has no `as_actor()` operation.
Role testing and multi-tenant embedding exercise
`CommandDispatcher.apply_as()`/`defer_as()` through focused security fixtures
or an authenticated host adapter.

Trusted deferred submissions persist an explicit local origin. They do not
record a fictional authorization decision.

### R4 — Async context manager is canonical

```python
async with ArchetypeRuntime() as runtime:
    ...
```

Shutdown must:

1. stop admitting new runtime and handle operations;
2. wait for every already-admitted operation;
3. cancel every supervised task across all owners, then await their completion;
4. close workflow and world handles without destroying durable attached state;
5. attempt every independent cleanup step in the current phase and aggregate
   failures; and
6. be idempotent.

`RuntimeResources` executes the exact phase order `admission`,
`supervised-tasks`, `workflow-handles`, `world-handles`, `audit`, `storage`.
Every complete runtime or handle call is admitted by exact task into both the
process-operation and dispatcher gates. Shutdown synchronously publishes stop
intent to both gates before waiting on either lock, then finishes the
process-operation stop and drain before the dispatcher stop and drain. An
already-admitted exact task may cross its first dispatcher boundary and finish
same-task nested operations after stop intent; fresh direct/API work and a new
task, including a child task, have no inherited admission.
At the same close-start boundary, every extant owner gate enters process-stop:
raw owner-only work rejects, while an exact task already admitted by the
process or dispatcher may still cross its first owner boundary. An owner
created by such a continuation is born process-stopped. After both global
gates drain, shutdown re-snapshots the owner inventory, tightens every owner
gate to strict stop, and drains them all before supervised cancellation or
resource cleanup.
Supervised cancellation is broadcast before any owner is awaited, so one
cancellation-resistant task cannot delay cancellation of its peers. Closing
from the current admitted or supervised task rejects deterministically instead
of waiting on itself.
If a phase fails, the runtime rejects public work and retains only the failed
owners plus their dependencies. A later serialized `shutdown()` retries that
phase before advancing. `RuntimeShutdownError` reports the phase and ordered
owner-labelled original causes. Calls after successful finalization are
no-ops. Only terminal success detaches handler/dependency graphs; retryable
failure retains them intact.

### R5 — Sync parity

`SyncArchetypeRuntime` and `SyncRuntimeWorld` expose the same product semantics
as the async classes without `await`. The sync facade owns an `asyncio.Runner`
and does not reuse an outer event loop. A retryable teardown failure retains
that runner and the supported `shutdown()` method retries the same process
owner; the runner is released only after successful finalization.

This requirement covers the generic framework surface. A separately installed
world library may publish an async-only typed adapter and must document that
gap explicitly; temporary sync aliases do not promote the adapter into the
framework contract.

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

The call captures configuration and synchronously reserves a world-handle owner
but performs no backend I/O. The first operation single-flights activation
through the dispatcher. Processors, resources, and hooks are installed in
declared order. A failed activation must clean up any partially created live
world and remain retryable when the failure is transient. Activation and its
compensation are one admitted handle operation, so shutdown cannot close
dispatcher admission between `CreateWorld` and later installation or rollback.

### R7 — Per-world operation serialization

All operations that target the same live world are serialized by
`WorldRegistry`, regardless of which runtime handle or API request originated
them. A handle-local lock may improve ergonomics but is not the concurrency
authority. Different worlds may proceed concurrently.

### R8 — Runtime and world lifetimes are distinct

One runtime may own many handles. Closing a handle waits for work admitted
through that handle and invalidates the local view; it does not tear down shared
process services. Local admission closes before that wait, so late work is
rejected even when cleanup remains retryable. Runtime shutdown owns shared
services.

`runtime.attach(world_id)` returns a non-owning handle. Closing it never
destroys the world. Destruction is an explicit application operation and does
not delete append-only durable rows. Destroy first rejects late handle calls
and drains calls admitted earlier, then performs the durable destroy effect.
A failed destroy reopens the handle for work and retry; a successful effect
closes and releases the handle.

### R9 — Boundary-safe results

The runtime receives immutable information snapshots, identifiers, typed
receipts, supported result/configuration models, and explicitly specified
DataFrames. It never receives a live world, service, registry, wiring graph,
credential, or backend client.

### R10 — Storage and cache coercion

`storage: str | Path | StorageConfig | None` and `cache: CacheConfig | None` are
accepted at the scripting boundary. A string or path becomes
`StorageConfig(uri=str(value))`; richer backend policy remains below runtime.

### R11 — Evaluation and world-library adapters

`world.grade(...)` queries the handle's append-only history and dispatches
`RunGraders` over that lazy frame; its outputs are ephemeral.
`world.evaluate(...)` instead dispatches an exact `Evaluate` operation. The
registered family handler owns snapshot pinning, grader execution, outcome
validation, and durable receipt persistence. The runtime supplies the handle's
explicit storage coordinates but does not pin or persist that evaluation
itself.

Durable `world.evaluate(...)` receipts require the world handle to use an
explicit `StorageConfig(..., backend=StorageBackend.ICEBERG)`. Omitted, string,
and path storage forms select LanceDB and cannot persist evaluation receipts;
use `world.grade(...)` when persistence is not required.

`Research(world).autoresearch(...)` dispatches the exact direct-only `AutoResearch`
operation to the research-family handler. Trusted and actor-aware immediate
entry share that handler; actor-aware use requires `operator`, resolves a
`live_world` quota coordinate, and charges
`200 * max(max_iterations, 1)`. Deferred entry rejects before catalog effects
because evaluator, preparer, and iteration callbacks are live capabilities.
The typed Research adapter is async. For the 0.6 migration only, the installed
manifest supplies the former `SyncRuntimeWorld.autoresearch` alias and forwards
synchronous callbacks through the sync runner.

The dispatcher synchronously awaits the entire outer workflow inside one
process admission. Callback execution does not hold a runtime handle or named
world lock, so ordinary inner world and storage operations remain available
without recursive dispatch. A ledgered workflow does retain its experiment-key
admission: direct same-task reentry for that experiment fails fast, while a
separately scheduled same-key call remains a normal waiter and must not be
awaited before the callback returns. Sync callbacks must likewise resume the
same experiment only after the outer call returns. Runtime shutdown therefore
joins an admitted AutoResearch call before closing shared dependencies, without
a research-owned task, owner reservation, or finalizer.

Physical evaluation enters through
`PhysicalAI(world).run_hosted_episode(...)`, which
dispatches the exact `RunHostedEpisode` operation to the registered
physical-AI handler. The world handle must retain explicit storage coordinates. The
handler owns hosted Activity admission, remote Modal execution or
reconciliation by stable operation identity, and durable result publication;
the runtime does not run episodes or collect terminal rows itself. The typed
world-library adapter is async. During the 0.6 migration, `SyncRuntimeWorld`
offers manifest-driven compatibility forwarding for the former world method;
the framework does not import Physical AI. See [Physical AI](physical-ai.md)
and [World Libraries](world-libraries.md).

### R12 — Typed artifacts and transcript evidence

`world.ingest_artifacts(*sources)` is the supported file-ingestion boundary.
Each `ArtifactSource` names one exact file or Daft-readable glob and may supply
an explicit portable logical path. Recursive discovery is expressed by the
glob itself. The artifacts-family handler receives the runtime's exact effective
storage configuration, resolves the durable run and published tick head, scans
metadata, computes SHA-256 and XXH3-64 in one pass, writes an immutable
content-addressed object, publishes any media-specific index, and then
publishes the common `artifact_files` row.
It returns one `ArtifactRef` per occurrence. A repeated submission is a new
UUIDv7 occurrence that may point to the same content object.

`world.artifacts()` returns the current world's current-run common file index
as a Daft DataFrame. Table registration, world/run envelope columns, schema
checking, and Iceberg append semantics remain internal to the storage
substrate reached through the artifacts-family view. The runtime neither
inspects `daft.Catalog` nor exposes the storage service, family handlers, or
process wiring.

`MissionWorld(world).ingest_claude_transcript(source)` is the recommended coding-agent
transcript boundary. `ClaudeTranscriptSource` carries local input configuration
and stable project/session identity. The application workflow snapshots and
redacts the file, parses only the sanitized copy, ingests that copy as an
artifact, and appends normalized rows to the Iceberg transcript table. The
returned `TranscriptIngestionResult` identifies the sanitized `ArtifactRef`,
row count, trajectory linkage, and redaction outcome. The runtime does not open
the source file, write
narrative Components, or coordinate those steps itself. The Missions adapter
has synchronous parity. `MissionWorld(world).transcript_rows()` returns the
normalized session and turn rows for the current run. Compatibility forwarding
through the former world methods is manifest-driven and temporary.

Artifact and transcript capabilities require the handle to retain explicit
storage coordinates. A handle created with `runtime.attach(world_id)` without
`storage=...` may still use live-world capabilities, but these storage-addressed
methods reject before dispatch instead of recovering coordinates from
process-local world state.

`ArtifactSource`, `ArtifactRef`, and `ArtifactStoreConfig` are the supported
top-level file contracts. Family-owned handlers and views, the storage port,
and process wiring remain internal implementation surfaces.

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

### R14 — Public callables do not accept raw services

Supported callables may accept `ArchetypeRuntime`, handles, configuration,
components, callbacks, and safe models. They may not require callers to pass a
concrete service or `RuntimeResources`. Repository checks enforce this rule.

### R15 — Multiple runtimes

A process may hold multiple runtimes. Each owns its `RuntimeResources` unless
an explicit internal host composition injects one. Cross-runtime live-handle
transfer is out of scope; durable identity and storage coordinates are the
interchange boundary.

### R16 — Agent Missions V1

`Missions(runtime, name, config=..., storage=...)` returns an async Missions
handle. It configures one mission-capable world with the
built-in Components, graph view, transition processors, durable
author-and-critic Activity binding, and injected Sandbox Backend plus
coding-agent and critic drivers. The V1 workflow admits only the Modal sandbox backend
for end-to-end missions; submission rejects any other configured backend
deterministically before admission. The family-owned
Sandbox Service retains the author Session and owns fresh candidate-scoped
critic Sessions. Authors submit typed tasks and critic policies; they never
wire that bundle themselves. A custom critic driver declares `driver_id`, and
every submitted task policy must name that configured identity.

Passing validators and publishing the exact head moves a task to `candidate`.
The runtime returns terminal success only after a separate critic sandbox has
verified the exact base/head/diff and a processor has accepted its
identity-bound receipt. Blocking findings become the next author dispatch's
durable repair input. Reviewer outages do not consume author dispatches;
exhausted review budget raises while leaving the task pending review.

The handle owns a strongly registered workflow reservation. Its first submit
or run constructs and binds the internal mission service exactly once; later
operations resolve that same owner without a parallel service registry.
`SubmittedMission` carries the exact durable World identity. Therefore a
replacement process can recreate the handle with the same storage coordinates
and call `run(submitted)` directly: wiring binds the Activity projector before
mutable World reconstruction, reinstalls process-local processors, resources,
and hooks, and reconciles provider-bound work instead of replaying it. Closing the handle
strictly stops and drains the reservation's exact-task admission before it
joins supervised critic work and closes sandbox resources plus its exact
mission-world cleanup without closing the parent runtime. Facade calls and the
registered direct `SubmitMission`, `RunMission`, and
`RestoreMissionSandbox` handlers share that owner gate, beginning before first
service construction or lookup. Therefore close drains work admitted through
either ingress, while late direct work rejects before construction or provider
effect. A failed cleanup
retains the facade, service, world, and dependencies for retry. Workflow
handles close before ordinary world handles during runtime teardown. Once
exact-world cleanup finishes, a later mission-world close failure retries only
the world-close stage rather than reusing the consumed cleanup lease.

`Missions` imports no concrete application service. It dispatches
`SubmitMission`, `RunMission`, and `RestoreMissionSandbox`; wiring constructs
the handler-side service with the same reservation. `RuntimeMissions` remains
a 0.6 import alias for source compatibility. V1 is still async-only, so sync
parity under R5 remains a hardening gap. See
[Agent Missions V1, current hardening gaps](agent-missions.md#current-hardening-gaps).

## 3. Canonical surface

Generic world operations below have sync parity. The installed Missions and
Research adapters shown here are async-only:

```python
from archetype.missions import AgentMissionConfig, AgentTask, Missions
from archetype.research import AutoResearchConfig, Research

world = runtime.world(
    name,
    storage=...,
    cache=...,
    processors=...,
    resources=...,
    hooks=...,
)
world = runtime.attach(world_id, storage=...)

missions = Missions(
    runtime,
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
# Checkpoint references are evidence only. Workflow restore fails
# explicitly until a checkpoint is bound into immutable Activity admission.

eid = await world.spawn(Position(x=0), Velocity(dx=1))
ids = await world.spawn_batch(Position(x=0), count=10_000)
ids = await world.spawn_many([[Position(x=float(i))] for i in range(100)])
await world.despawn(eid)
await world.update(eid, Position(x=10))
await world.add_components(eid, Health(hp=100))
await world.remove_components(eid, Velocity)

refs = await world.ingest_artifacts(ArtifactSource(...))

await world.add_processor(MyProcessor())
await world.remove_processor(MyProcessor)

await world.step()
result = await world.run(steps=10)
episode = await world.run_episode(EpisodeConfig(...))
rollout = await world.run_rollout(RolloutConfig(...))
research = await Research(world).autoresearch(AutoResearchConfig(...), evaluator)

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
packages/archetype-ecs/src/archetype/runtime/
  __init__.py
  runtime.py       ArchetypeRuntime and SyncArchetypeRuntime
  world.py         RuntimeWorld and SyncRuntimeWorld
  entrypoint.py    managed script decorator
  _config.py       scripting-boundary coercion

packages/archetype-missions/src/archetype/missions/runtime.py
  Missions and MissionWorld typed adapters

packages/archetype-physical-ai/src/archetype/physical_ai/runtime.py
  PhysicalAI typed adapter

packages/archetype-research/src/archetype/research/runtime.py
  Research typed adapter
```

Storage session construction lives below runtime. World-library adapters live
with the behavior they expose, and the framework never imports those packages
by name.

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
