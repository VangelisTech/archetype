# Application families and wiring

The internal application layer composes workflows over the top-level world,
commands, storage, and domain families. Concrete services and
process wiring are not supported application APIs. Use
`ArchetypeRuntime`, REST, or CLI.

Normative dependency rules live in
[Application Architecture](application-architecture.md); active ports live in
[Service Protocols](service-protocols.md).

## Process composition and lifetime

`build_runtime_resources(RuntimeBootstrapConfig)` is the sole enclosing
process-composition transaction. It constructs the domain-free framework,
resolves installed world-library manifests, and invokes each private installer
with one bounded `WorldLibraryContext` before returning `RuntimeResources`:

```text
RuntimeResources
  +-> CommandDispatcher
  +-> supervised tasks
  +-> workflow-handle owners
  +-> world-handle owners
  +-> AuditLog
  +-> owned StorageService
```

Construction is synchronous; stores and catalogs open lazily. Shutdown stops
and drains dispatcher admission, joins supervised work, closes workflow and
world handles, flushes `AuditLog`, and finally releases owned storage.
Failures retain the necessary ownership graph for a retry.
Per-world destroy is a separate lifecycle operation.

## Wiring overview

Arrows mean consumer to dependency:

```text
ArchetypeRuntime -> CommandDispatcher.apply / defer
REST API -> authentication -> CommandDispatcher.apply_as / defer_as
CommandDispatcher -> OperationRegistry
CommandDispatcher -> registered family handler
CommandDispatcher -> Policy + CommandScheduler + AuditLog.record_access

archetype.wiring
  +-> WorldRegistry + WorldLifecycle
  +-> CommandScheduler -> world.handlers.materialize_locked
  +-> AuditLog -> StorageService + scheduler outbox callbacks
  +-> artifact handlers and views -> StorageService
  +-> evaluation handlers -> storage + world.query
  +-> resolved manifests -> bounded WorldLibraryContext

archetype.missions._extension
  +-> transcript + trajectory services -> declared framework families
  +-> exact Mission handlers -> reservation-owned MissionService
archetype.research._extension
  +-> AutoResearch handler + per-runtime admissions -> storage/world ports
archetype.physical_ai._extension
  +-> hosted-episode handler -> per-world Activity binding + storage/world ports
```

Framework wiring injects `CommandScheduler.materialize` when lifecycle constructs
each managed `AsyncWorld`. Core owns only the callable shape; it never imports
the commands family. It also connects the scheduler's transactional outbox to
`AuditLog`'s analytical projection. The framework never imports a world library
by package name; each library's private adapter constructs only that library's
internals during the enclosing installation transaction.

## Storage family

`StorageService` pools async stores and resolves local SQLite or remote control
catalogs for a storage identity. It is the canonical physical authority for
terminal Daft execution, visibility pinning and scans, generic world/run
envelopes, commit coordination, app-table registration, schema alignment,
reads, writes, and optimistic conflict retry. It owns those physical and
execution concerns, not the meaning of a tick, command, artifact, or evaluation
commit.

The SQLite or Durable Object control catalog owns fences, manifests, commands,
and workflow leases. Daft Catalog, Iceberg snapshots, and object storage are the
data plane. `StorageService` composes these distinct authorities; neither
substitutes for the other.

See [Stores](stores.md).

## World family

`WorldRegistry` owns live identities, storage coordinates, exact-world locks,
retryable cleanup leases, required-projector bindings, and retained committed
receipts. `WorldLifecycle` composes an `AsyncWorld` from a shared store,
querier, updater, system, resources, hooks, and the construction-injected
command materializer.

Module-level family behavior is split by concern:

- `archetype.world.mutation` stages entity, component, processor, resource,
  and hook changes;
- `archetype.world.simulation` owns step, required projection, run, episode,
  and rollout execution;
- `archetype.world.query` performs durable ECS reads without a live world; and
- `archetype.world.handlers` adapts frozen operation models to exact family
  functions, including the lock-held portable command path.

Live worlds never escape the application boundary. See
[World Lifecycle](world-lifecycle.md) and
[Execution Hierarchy](execution-hierarchy.md).

Durable component, signature, and lineage reads belong to the world family.
Audit history belongs to `AuditLog`; command outcome authority remains the
command ledger/outbox.

## Artifacts family

`archetype.artifacts` owns file values, its cohesive lazy pipeline and bounded
scanners, storage-backed views, and exact free handlers. Each operation carries
explicit durable coordinates. Before file effects, the handler resolves the
recorded run and verifies the catalog-published tick head; it never consults
the live registry or invents default storage. It then scans declared sources,
persists content-addressed objects, writes typed media metadata, and publishes
the common artifact index last.

`StorageService` stamps the durable world/run envelope, selects plain or
caller-keyed conditional append, and owns table registration, schema
comparison, Daft execution, and the Iceberg commit. Other owning families call
that substrate directly for their typed rows. There is no general ingestion or
application artifact facade and no artifact claim, lease, receipt, or
reconciliation state machine. See [Artifacts](artifacts.md).

## Evaluation and installed world-library families

The top-level evaluation family pins persisted world state through storage and
world-query authority, executes caller-provided graders, validates typed
outcomes, and appends one durable evaluation result directly through
`StorageService`. Exact `Evaluate` and `RunGraders` models are registered to
free family handlers; no application evaluation facade or live-registry
fallback participates.

The three domain world libraries are separately installed distributions. The
framework supplies their private installers with an already composed context;
it does not construct their services, handlers, provider adapters, or family
state itself.

`archetype-research` owns the multi-iteration rollout workflow and durable
research ledger. Its private `archetype.research._extension` installer creates
one `AutoResearchAdmissions` map per runtime graph, closes the free handler over
that map plus the world/storage capabilities, and registers the exact
`autoresearch` operation. Ledgered calls for the same experiment serialize;
ledgerless calls bypass that map and use invocation-unique rollout names. The
dispatcher awaits the handler inside its existing admission, so shutdown drains
it without a second owner reservation or detached task. Scoring remains an
explicit callback contract.

`archetype-physical-ai` owns physical state, canonical provider protocols,
hosted-episode values, provider recovery, and the whole-episode Activity
workflow. Its private `archetype.physical_ai._extension` installer registers the
single `run_hosted_episode` operation. On first use for a world it constructs
and retains that world's provider-backed Activity binding through
`RuntimeResources`, registers the required projector, and rejects a later
attempt to change the world's provider namespace. Remote provider work never
runs inside a retryable tick.

`archetype-missions` owns mission state, sandboxes, coding-agent sessions,
transcripts, and trajectory evidence. Its private
`archetype.missions._extension` installer constructs transcript and trajectory
services, registers every manifest-declared operation, and leaves each
`MissionService` to be constructed inside its pre-reserved workflow owner when
a Mission operation is admitted. These library internals do not move into the
framework composition root.

## Commands family

The top-level commands family owns exact operation registration,
trusted/actor-aware dispatch, instance-owned policy, durable scheduling, and
access/outbox projection. `CommandScheduler` admits, leases, materializes,
retries, settles, and inspects portable tick-deferred operations. Applied
outcomes settle atomically with the tick visibility manifest; authoritative
events are written to its outbox.

See [Durable Commands](durable-commands.md).

## Commands-owned audit projection

`AuditLog` projects access events and command-outbox events into append-only
Iceberg rows. The outbox/command ledger is authoritative for workflow outcome;
the analytical projection can lag. Audit is not a parallel application family.

See [Audit Log](audit-log.md).

## Trusted runtime entry

`ArchetypeRuntime` and its handles construct exact framework operation models
and enter trusted `CommandDispatcher.apply` or `defer` methods. An installed
world library's typed adapter constructs its own family models over those
generic handles; the framework runtime does not import that library by package
name. Neither surface owns policy, queue state, or durable state.

No `ActorCtx` is invented for local scripting.

## Actor-aware API entry

Base FastAPI routes authenticate `ActorCtx`, construct exact framework models,
and enter `CommandDispatcher.apply_as` or `defer_as`. Optional library routes
arrive through manifest-declared router factories and construct only that
library's models over the same actor-aware boundary. The commands-owned
`Policy` and dispatcher perform RBAC, quota, admission, and bounded access
evidence. The transport does not own those mechanisms or persistence.

The CLI remains an HTTP client.

## Source reference

- composition root: `packages/archetype-ecs/src/archetype/wiring.py`
- process lifetime: `packages/archetype-ecs/src/archetype/runtime_resources.py`
- trusted runtime: `packages/archetype-ecs/src/archetype/runtime/`
- actor-aware transport: `packages/archetype-ecs/src/archetype/api/`
- governed and durable commands: `packages/archetype-ecs/src/archetype/commands/`
- world family: `packages/archetype-ecs/src/archetype/world/`
- physical storage family: `packages/archetype-ecs/src/archetype/storage/`
- artifact values, pipeline, scanners, views, and handlers: `packages/archetype-ecs/src/archetype/artifacts/`
- evaluation values, grading, pinned views, handlers, and receipt schema: `packages/archetype-ecs/src/archetype/evaluation/`
- world-library manifest, discovery, and context contracts: `packages/archetype-ecs/src/archetype/world_libraries/`
- missions state, workflows, evidence, and private adapter: `packages/archetype-missions/src/archetype/missions/`
- research values, ledger, views, admission, and handler: `packages/archetype-research/src/archetype/research/`
- physical-AI models, state, views, and handlers: `packages/archetype-physical-ai/src/archetype/physical_ai/`
- command/access audit projection: `packages/archetype-ecs/src/archetype/commands/audit.py`
