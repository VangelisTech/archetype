# Internal workflow protocols

**Document type:** Normative.

**Scope:** Internal structural interfaces under
`src/archetype/app/<family>/interfaces.py` plus focused, construction-injected
family workflow ports described here.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

Agent Missions V1 uses the family-owned `SandboxService`, `SandboxBackend`,
`SandboxSession`, coding-agent harness, and exact-head critic harness beneath
the app-owned `iMissionService` workflow. Physical evaluation instead uses
family-owned environment/policy protocols and free top-level handlers over
declared storage and world ports; there is no `iPhysicalAIService`.

The top-level `archetype.commands` family deliberately owns concrete
`OperationRegistry`, `CommandDispatcher`, `Policy`, `CommandScheduler`, and
`AuditLog` machinery rather than the deleted application-family scheduler and
audit protocols. Their composition edges are listed here so this port map
remains complete; they are not application ports.

## 1. Policy

Application protocols are internal dependency boundaries unless a focused
specification explicitly promotes one. Importability does not make them public.
Their value types may live in a supported top-level domain family without
promoting the protocol, its implementation, or process wiring. Port
ownership and value-contract ownership are separate decisions.

Every active protocol has:

- one owning family;
- named consumers and one implementation;
- the complete method surface used by those consumers;
- structural conformance checked by the repository type gate and focused tests;
- an allowed edge in the merged `quality/architecture.toml` policy and its
  `quality/architecture.d/` fragments; and
- negative architecture evidence rejecting undeclared concrete edges.

Protocols are co-located with their family. There is no root
`app/interfaces.py` compatibility module.

## 2. Dependency overview

Arrows point from consumer to dependency:

```text
ArchetypeRuntime -> commands.CommandDispatcher.apply / defer
FastAPI + ActorCtx -> commands.CommandDispatcher.apply_as / defer_as
commands.CommandDispatcher -> OperationRegistry -> exact family handler

evaluation.handlers -> iStorageService + archetype.world.query
artifacts.handlers + artifacts.views -> iStorageService
iTranscriptIngestionService
  -> artifacts.handlers + redaction structural port + iStorageService
iWorldLifecycle    -> iWorldRegistry + iStorageService
CommandDispatcher  -> OperationRegistry + Policy + CommandScheduler
                   -> AuditLog.record_access
CommandScheduler   -> storage control catalog
                   -> world.handlers lock-held materialization
research.handlers  -> iWorldRegistry + iWorldLifecycle + iStorageService
                   -> world simulation + exact owned-world cleanup
physical_ai.handlers
  -> iWorldRegistry + iWorldLifecycle + iStorageService
  -> archetype.world mutation/simulation/query
  -> construction-injected PhysicalClientLifetimeRegistrar
AuditLog           -> iStorageService + CommandScheduler outbox callbacks

RuntimeMissions -> CommandDispatcher -> registered mission handler
registered mission handler -> reservation-owned iMissionService
iMissionService
  -> missions.SandboxService -> missions.SandboxBackend
  -> missions.CodingAgentHarness -> missions.SandboxSession
  -> missions.CriticHarness -> missions.CriticDriver
                            -> missions.SandboxSession
```

`archetype.wiring` selects concrete implementations, registers exact handlers,
and returns `RuntimeResources`. Agent Missions reaches its workflow only
through those handlers and its exact pre-reserved owner.

## 3. Active mapping

| Port | Implementation | Principal consumers | Responsibility |
|---|---|---|---|
| `iStorageService` | `StorageService` | world, commands, artifacts, evaluation, transcripts, research, physical AI | Store/session lifetime, control authority, physical visibility, world/run row envelope, terminal Daft execution, and app-table catalog/read/write/retry authority |
| `iWorldRegistry` | `WorldRegistry` | lifecycle, mutation, simulation, research, physical AI | Live identity, storage coordinates, exact-world synchronization, retryable close ownership, and committed-receipt retention |
| `iWorldLifecycle` | `WorldLifecycle` | wiring, research, physical AI | Managed construction, durable discovery, readonly open, fenced mutable resume, fork, and close |
| `iWorldCleanup` | `WorldCleanup` | reservation-owned mission cleanup | Exact-world, close-lease-bound retained updates, teardown staging, commit, and finish |
| `iTranscriptIngestionService` | `TranscriptIngestionService` | registered transcript handlers | Snapshot and redact a coding-agent transcript, ingest the sanitized file, and append normalized mission rows |
| Structural `MissionRedactor` / `TranscriptRedactor` | canonical `archetype.redaction.RedactionService` | mission execution and transcript ingestion | Provider-neutral pre-durability scanning, deterministic redaction, safe receipts, and quarantine |
| `iMissionService` | `MissionService` | registered mission handlers | Materialize task graphs, own the batteries-included world bundle, drain committed author and critic intents into external work, stage factual observations, and project terminal results |
| `iTrajectoryService` | `TrajectoryService` | registered trajectory query/grade handlers | Compose durable episode selection with evaluation graders without creating a second trajectory authority |
| Family lifetime port `PhysicalClientLifetimeRegistrar` | wiring-owned runtime registrar | physical-AI handlers | Transfer unique live providers before the first effect, hold an identity-ordered exclusive lease for the complete workflow, and yield scoped exact-world retirement authority |
| Family resource service `missions.SandboxService` | `missions.SandboxService` | `MissionService` | Select a configured backend and acquire, reuse, close, and shut down mission-keyed sessions; no task-transition authority |
| Family resource port `missions.SandboxBackend` | configured Apple Container, Docker, or Modal adapter | `missions.SandboxService` | Create or restore provider-owned isolated sessions |
| Family resource port `missions.SandboxSession` | provider session adapter | `CodingAgentHarness`, `CriticHarness`, `missions.SandboxService` | Expose capability, process, status, checkpoint, and close operations for one live sandbox |
| Family resource port `missions.CriticDriver` | `CodexCriticDriver` or configured adapter | `CriticHarness` | Invoke one independent structured review with model capability but no Git publication capability |

### Commands-owned machinery

| Component | Principal consumers | Responsibility |
|---|---|---|
| `OperationRegistry` | `CommandDispatcher`, `CommandScheduler`, composition root | Exact model/name registration, handler metadata, and optional durable decoder/materializer |
| `CommandDispatcher` | runtime and API adapters | Trusted and actor-aware direct/durable entry, admission lifetime, policy order, and bounded evidence |
| `Policy` | `CommandDispatcher` | Pure role preauthorization plus instance-owned world/tick and daily-token quotas |
| `CommandScheduler` | `CommandDispatcher`, world materializer, wiring-provided destroy callback | Canonical durable admission, reservation, leasing, retry, settlement staging, cancellation, and outbox access |
| `AuditLog` | `CommandDispatcher`, registered `GetAuditHistory`, `RuntimeResources` shutdown | Bounded access rows and transactional command-outbox projection into analytical storage |

The research family deliberately has no application service port.
`archetype.research.handlers.handle_autoresearch` is a free handler closed over
the world/storage ports, exact cleanup callback, and one process-shared
`AutoResearchAdmissions` instance by `archetype.wiring`.
The physical-AI family likewise has no application service port. Wiring closes
its two free handlers over the world/storage ports and one narrow
runtime-lifetime registrar.

## 4. Boundary rules

### Runtime and API adapters

Runtime methods construct exact family models and enter
`CommandDispatcher.apply()` or durable variants. They expose boundary-safe
results, never concrete services, process wiring, or live worlds. Runtime-only
ergonomics and lazy handle state remain in `archetype.runtime`.

API routes parse transport, authenticate `ActorCtx`, construct the same exact
models, and enter `CommandDispatcher.apply_as()` or durable variants. The
commands-owned dispatcher and `Policy` perform authorization, quota admission,
and bounded evidence. Routes own no policy counters, world, command ledger,
audit log, grader, artifact ingestion, or storage.

### World ports and operation surfaces

Live-world returns from `iWorldLifecycle` and leases from `iWorldRegistry` are
legal only below the application boundary. Stateful world authority is limited
to those two family-owned ports. Mutation, simulation, durable query, and
externally operable adapters are public module functions in
`archetype.world.{mutation,simulation,query,handlers}` rather than
single-implementation service protocols.

Simulation imports neither commands nor API. Lifecycle receives the
scheduler materializer as a construction callable and wires it into every
managed world. Bounded episode termination reduces a lazy frame through
`iStorageService` before the scalar enters Python control flow.

### Durable workflow ports

`CommandDispatcher` is the governed entry point; `CommandScheduler` is the
durable control-catalog authority beneath it. The scheduler admits exact
portable models, leases them in ledger order, invokes the registered lock-held
materializer, and stages successful IDs. Tick publication performs terminal
applied settlement. Neither is an application-family protocol.
Wiring supplies only narrow materialization, cancellation, and teardown
callbacks to lower owners; those families do not import or retain the concrete
scheduler.
The artifacts family exposes free storage-backed handlers and views rather
than single-implementation application protocols. Exact operations carry
explicit durable world and storage coordinates. The handlers verify the
recorded run and published tick head before file effects, discover and scan
sources, persist immutable content-addressed objects, write optional
media-specific indexes, and publish the common file index last.
`iStorageService` owns the corresponding physical boundary: the
catalog-derived world/run envelope, plain or caller-keyed conditional append,
terminal Daft admission, `daft.Catalog` table registration, schema alignment,
lazy table reads, Iceberg writes, and optimistic-conflict retry.

There is no artifact claim, lease, receipt, reconciliation protocol, generic
ingestion facade, or live-registry fallback around that path.
Provider checkpoints remain sandbox recovery objects rather than artifact
workflow stages. Agent Missions does not implicitly crawl a sandbox after a
task decision: a provider export must first select and sanitize declared files,
then submit valid `ArtifactSource` values through the registered artifact
operation. Future live-event,
OTel, and proxy exporters consume the same redaction port; they do not fork
scanner policy.

`iTranscriptIngestionService` is a composition port, not another storage
authority. Its implementation snapshots and redacts through
the canonical `RedactionService` through a narrow structural port, parses with
the pure missions-family adapter, redacts normalized rows, publishes the
sanitized snapshot through the artifacts-family handler, verifies its digest,
and appends normalized rows through `iStorageService`.
Raw narrative never crosses a durability boundary.
Each ingestion is a new artifact occurrence; normalized row identity is scoped
to that source artifact. Commands-owned `AuditLog` is an analytical
projection/read component, not the authority for command outcome.

AutoResearch follows the same free-handler direction without becoming a
durable workflow. The exact `AutoResearch` model carries live callbacks, so
trusted `apply` and operator-authorized `apply_as` are available while both
deferred modes reject before catalog effects. A ledgered call enters the
process-shared `autoresearch:{experiment_id}` admission; a ledgerless call
bypasses it and receives invocation-unique rollout names. The dispatcher
awaits the outer handler synchronously inside its existing process admission,
and inner world/storage work calls owning families directly. Research creates
no service facade, recursive dispatch, detached task, or second lifetime owner.

### Agent Missions V1

`archetype.missions` owns the complete reusable coding-agent capability:
`SandboxService`, the `SandboxBackend` and `SandboxSession` protocols, sandbox
value contracts, coding and critic harness values, Components, relations, and
processors.
The configured backend creates or restores a provider-owned session;
`SandboxService` selects that backend and single-flights acquisition by a
`SandboxKey`. The application service uses one mission-keyed author session and
one dispatch-keyed critic session per candidate. They may share a backend and
pinned image, but never a sandbox identity or Git publication capability.

Apple Container is the macOS operational adapter, Docker is the Linux/CI
protocol reference, and Modal is the paid remote adapter. Checkpoint-capable
sessions preserve their owned writable filesystem, excluding credential and
external mounts, through a provider-native reference carrying environment,
owner, locality, expiry, and integrity metadata. Capture occurs only after the
task decision commits and remains best-effort evidence. Restore explicitly
closes and replaces the mission's retained live session; it is not automatic
fleet recovery or process-restart mission continuation.

`iMissionService` is the app-internal workflow port implemented by
`MissionService`. The service composes a structural mission world with the
built-in Components, processors, relationships, graph view, committed
author/critic outboxes, both repository harnesses, and sandbox service. After a
tick commits,
`TaskDispatchOutbox` projects newly persisted `TaskDispatch` data into external
work requests. The service acquires the mission-keyed session and invokes the
harness only from that post-commit path, then stages the returned factual
observations for a later tick.

Graph materialization records each authored `TaskValidator`. The harness then
prepares the repository, runs the coding agent and those validator commands,
performs Git publication, and returns facts that the service records as
`AgentExecution`, `ValidationResult`, `Commit`, and `FrictionLog`
Components and relations. Complete passing exact-revision evidence plus one
published final head becomes a `Candidate`, not acceptance. While the author
works, the service prewarms a fresh critic sandbox. `CriticReviewOutbox`
projects committed candidates into exact base/head/diff review requests;
`CriticHarness` verifies the remote subject, invokes `CriticDriver`, and returns
bounded findings and a receipt. Critic sandboxes receive no publication secret,
the configured driver's declared identity must match the task policy, they are
never checkpointed, and they close after their evidence is durable. Close
failure is surfaced and retryable across `run()` calls; cancellation propagates
without discarding cleanup ownership. Critic execution facts carry the observed
sandbox status and whether acquisition succeeded, so unavailable synthetic
identities remain durable failures rather than fabricated healthy lifecycles.

The sandbox identity is staged immediately after acquisition; bounded
`SandboxEvent` callbacks expose it synchronously for live,
non-authoritative operator updates. Validator success is derived from expected
and actual return codes; neither the harness, sandbox, nor service decides task
state. Processors alone create candidate state, validate exact independent
receipt bindings, accept or repair a task, exhaust its author budget, unlock
dependent tasks, and roll terminal task states up to the mission. Reviewer
infrastructure failure consumes only its bounded review budget and leaves the
candidate pending.

The registered submit handler takes the backend configured by
`AgentMissionConfig`, constructs `SandboxService`, and constructs
`MissionService` once inside the pre-reserved workflow owner. It injects that
owner for critic supervision plus a narrow exact-world cleanup factory.
`RuntimeMissions` supplies only supported configuration and exact operations;
it never receives the service. No Component, processor, relation, harness
value, or sandbox implementation moves into `app`.

See [Agent Missions V1](agent-missions.md).

### Physical AI

`EnvClient`, `PolicyClient`, and `PhysicalClientLifetimeRegistrar` are genuine
protocols in `archetype.physical_ai.interfaces`; configuration, operation, and
report values live in `archetype.physical_ai.models`. External simulator and
model resources are implementations beneath those capabilities. The free
`archetype.physical_ai.handlers` workflows compose
world lifecycle, entity/processor mutation, episode execution, persisted world
reads, and storage-admitted terminal report projection; they do not own those
lower-family authorities. There is no app mirror or single-implementation
service protocol.

`EvaluatePhysicalTask` and `SweepPhysicalInstructions` are exact trusted-only,
direct, non-durable, application-scoped registrations. The handlers accept no
actor context and emit no parallel summary Component. They return typed reports
carrying the authoritative `(world_id, run_id)` coordinates.

Each live provider must expose `async aclose()`. Before ownership or effects, a
construction-injected registrar validates every supplied role with Daft's exact
serializer; accepted providers are serializable non-owning handles to
host-owned backing resources. It then transfers every unique provider identity
to `RuntimeResources` synchronously before world creation or a provider call
and holds an identity-ordered exclusive lease through the complete workflow.
Reuse and dual-role clients are deduplicated for the process lifetime;
operations that share any provider serialize, while disjoint providers may
proceed concurrently. Cancellation retains ownership; failed closes retain
only the failed owner for retry. Raw-client processors are internal and cannot
be installed as a supported ownership bypass. Daft 0.7.19 has no deterministic
`@daft.cls` teardown hook, so worker-local provider Specs are unsupported. The
host-owned provider close is authoritative; serialized processor handles may
reconnect but may not own independent closeable or I/O-backed worker-local
resources. The exact in-memory MuJoCo cartpole scratch exception is non-I/O and
has no application-controlled close. Issue #667 tracks a future safe provider
construction contract.

The registrar pre-reserves a process-owned cleanup slot before the handler may
create a private evidence world. That world is active for tick materialization
but carries immutable `writer_mode="cleanup_only"`. Each handler synchronously
binds its closing-world lease to the scoped lifetime token before the next
workflow await, then retires that exact writer before releasing the provider
lease. The reserved owner holds a deferred cleanup resource from the moment it
is created. Normal retention binds a lazy canonical exact cleanup before
fallible lease validation. If validation or metadata retention fails, the
compensation authority restores that same already-owned target without
repeating the failed gate. Cleanup revalidates through the retained
`WorldCleanup` handle; there is no unregistered lifecycle-destroy fallback.
A failed attempt remains process-owned, provider close joins its shutdown
retry, and multiple failures preserve every cause, including distinct caller
and cleanup-originated cancellation. Registered public destroy and the
retained physical handle join one process-owned cleanup transaction. Returned
world/run coordinates remain durable read evidence, while mutable resume
rejects the cleanup-only identity before storage or fencing and cannot
reactivate the provider processors.

The credential-free contract tests prove provider transfer ordering, identity
deduplication, cancellation retention, retryable close, paired seeds, complete
denominators, policy reset, runtime/sync parity, and ledger addressability.

See [Physical AI](physical-ai.md).

## 5. Values crossing family ports

Cross-family values are immutable or frozen where identity matters, but their
Python modeling technology does not decide their layer. Persistent ECS schema
is a `Component` and belongs in `archetype.<family>.components`. Supported
reusable Pydantic/dataclass values belong in the top-level family's
`contracts.py` or another specifically named family module. Application
commands, application-workflow authority records, backend workflow state,
authorization values, and service ports remain under
`archetype.app.<family>`. The reviewed physical-storage exception is
`archetype.storage`: control-catalog records and implementations, physical
visibility, commit coordination, and the generic durable world/run envelope
live there while application families retain workflow meaning.

An internal app protocol may therefore accept or return a top-level family
value. The protocol still lives in `archetype.app.<family>.interfaces`, its
concrete service and process wiring remain internal. The top-level family
never imports that port in return. Public classification is explicit and is
not inferred from either side of the annotation.

The artifacts family owns the supported `ArtifactSource`, `ArtifactRef`, and
`ArtifactStoreConfig` file contracts, one reusable `FileIngestionPipeline`, its
pure bounded scanners, storage-backed views, and exact free handlers. The
canonical `archetype.storage` family owns the generic durable world/run
envelope, published-head authority, and physical execution. There are no
application artifact or ingestion facades. The evaluation family completed its
workflow pull-forward under issue #650: `EvalReceipt` lives in
`archetype.evaluation.components`; grading values and identity digests live in
`archetype.evaluation.models` and `contracts`; and free handlers pin, grade,
lease, recover, and append through explicit storage coordinates. There is no
application evaluation facade or live-registry fallback. The
research family completed #585 and #652: supported values, ledger Components,
the runner decoder, storage-backed views, experiment admission, and the free
workflow handler live under `archetype.research`. There is no
`archetype.app.research` or `iResearchService`. The trajectory split completed
issue #586: schemas, authoring values, and structural transforms live under
`archetype.missions.trajectories`; `iTrajectoryService` composes durable query
with the evaluation family's pure grader runner.

The physical-AI pull-forward completed #666: canonical provider and lifetime
protocols live in `archetype.physical_ai.interfaces`; operation, request, and
report values live in `archetype.physical_ai.models`; views and free handlers
compose the declared storage and world ports in the same family.
`archetype.physical_ai.contracts` is a one-release object-identical re-export
shim for the moved value contracts. The former app mirror,
`iPhysicalAIService`, and root application command-envelope boundary are gone.

The root policy and its `quality/architecture.d/` fragments currently carry no
migration exceptions; no wildcard compatibility package is implied.
Redaction, audit, command, world, and other authority-specific models remain
with their owning families unless a focused specification classifies an
individual value as a reusable family contract.

The V1 mission split is the implemented example: Components, processors,
relations, authoring and coding-harness values, and sandbox resources live under
`archetype.missions`; graph materialization and cross-boundary workflow
composition live under `archetype.app.missions`.

## 6. Construction and shutdown

`archetype.wiring.build_runtime_resources()` is the sole concrete cross-family
composition transaction. It builds:

```text
OperationRegistry + Policy + CommandScheduler + CommandDispatcher
WorldRegistry + WorldLifecycle + AuditLog + StorageService
application-family services + registered exact handlers
RuntimeResources
```

Runtime retains `RuntimeResources`; API lifespan retains the same process
owner and dependency injection exposes only its dispatcher. Mission handlers
construct and resolve `iMissionService` inside the exact workflow reservation.
Neither concrete services nor process wiring are exposed to mission authors.

Shutdown stops and drains dispatcher admission, joins supervised work, closes
workflow then world handles, flushes the audit projection, and finally closes
owned storage. Failed phases retain exact ownership for retry.

## 7. Executable enforcement

- `scripts/check_architecture.py`
- `quality/architecture.toml`
- `quality/architecture.d/`
- `tests/scripts/test_check_architecture.py`
- `tests/app/test_service_protocols.py`
- `make typecheck`

## 8. Companion specifications

- [Application Architecture](application-architecture.md)
- [Runtime](runtime.md)
- [Command Gate](command-gate.md)
- [Durable Commands](durable-commands.md)
- [Audit Log](audit-log.md)
- [Execution Hierarchy](execution-hierarchy.md)
- [Artifacts](artifacts.md)
- [Agent Missions V1](agent-missions.md)
