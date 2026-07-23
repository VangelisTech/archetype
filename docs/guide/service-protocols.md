# Application family protocols

**Document type:** Normative.

**Scope:** Internal structural interfaces under
`src/archetype/app/<family>/interfaces.py`.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

Agent Missions V1 uses the family-owned `SandboxService`, `SandboxBackend`,
`SandboxSession`, coding-agent harness, and exact-head critic harness beneath
the app-owned `iMissionService` workflow. Physical evaluation uses the same
ownership pattern: family-owned environment/policy protocols beneath the
app-owned `iPhysicalAIService`.

## 1. Policy

Application protocols are internal dependency boundaries unless a focused
specification explicitly promotes one. Importability does not make them public.
Their value types may live in a supported top-level domain family without
promoting the protocol, its implementation, or the service container. Port
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
ArchetypeRuntime -> iRuntimeApplication <- iCommandGateway <- FastAPI

iRuntimeApplication
  -> iWorldRegistry + iWorldLifecycle + iStorageService
  -> world.mutation + world.simulation + world.query
  -> iArtifactService
  -> iTranscriptIngestionService
  -> iEvaluationService
  -> iCommandScheduler
  -> iAuditLog
  -> iResearchService
  -> iMissionService
  -> iPhysicalAIService

iEvaluationService
  -> iIngestionService + iStorageService + iWorldRegistry
iRedactionService -> no lower application family
iIngestionService  -> iStorageService + iWorldRegistry
iArtifactService   -> iIngestionService + iStorageService + iWorldRegistry
iTranscriptIngestionService
  -> iArtifactService + iIngestionService
  -> iRedactionService + iStorageService + iWorldRegistry
iWorldLifecycle    -> iWorldRegistry + iStorageService
iCommandScheduler  -> world.handlers lock-held materialization
iResearchService   -> iWorldRegistry + iWorldLifecycle + iStorageService
iPhysicalAIService
  -> iWorldRegistry + iWorldLifecycle
  -> iEvaluationService + iStorageService
iAuditLog          -> iStorageService

RuntimeMissions -> iRuntimeApplication -> iMissionService
iMissionService
  -> missions.SandboxService -> missions.SandboxBackend
  -> missions.CodingAgentHarness -> missions.SandboxSession
  -> missions.CriticHarness -> missions.CriticDriver
                            -> missions.SandboxSession
```

`ServiceContainer` selects concrete implementations across families. Agent
Missions reaches its container-selected workflow through the actor-free
application facade.

## 3. Active mapping

| Port | Implementation | Principal consumers | Responsibility |
|---|---|---|---|
| `iRuntimeApplication` | `RuntimeApplication` | runtime, `CommandGateway` | Actor-free canonical product operations and per-world serialization |
| `iCommandGateway` | `CommandGateway` | FastAPI and other untrusted adapters | RBAC/quota authorization, delegation, access audit |
| `iStorageService` | `StorageService` | world, ingestion, artifacts, evaluation, transcripts, research, physical AI, audit | Store/session lifetime, control authority, physical visibility, world/run row envelope, terminal Daft execution, and app-table catalog/read/write/retry authority |
| `iWorldRegistry` | `WorldRegistry` | lifecycle, mutation, simulation, ingestion, artifacts, evaluation, transcripts, research, physical AI, application | Live identity, storage coordinates, exact-world synchronization, retryable close ownership, and committed-receipt retention |
| `iWorldLifecycle` | `WorldLifecycle` | application, research, physical AI | Managed construction, durable discovery, readonly open, fenced mutable resume, fork, and close |
| `iIngestionService` | `IngestionService` | artifacts, transcripts, evaluation | Select live storage configuration and delegate typed row publication |
| `iArtifactService` | `ArtifactService` | application, transcript ingestion | Discover and scan files, persist content-addressed objects, publish typed media indexes, then expose the common file index |
| `iTranscriptIngestionService` | `TranscriptIngestionService` | application | Snapshot and redact a coding-agent transcript, ingest the sanitized file, and append normalized mission rows |
| `iRedactionService` | `RedactionService` | transcript ingestion; future telemetry/proxy adapters | Provider-neutral pre-durability scanning, deterministic text redaction, safe receipts, and quarantine |
| `iEvaluationService` | `EvaluationService` | application, physical AI | Pin persisted world state, lease grader execution through the shared control authority, and append one typed evaluation result |
| `iCommandScheduler` | `CommandScheduler` | application | Durable admission, leasing, dispatch, retry, settlement and outbox inspection |
| `iAuditLog` | `AuditLog` | application, gateway | Append-only access rows, command-outbox projection, and application history |
| `iResearchService` | `AutoResearchService` | application | Multi-run autoresearch workflow and research ledger |
| `iMissionService` | `MissionService` | application, `RuntimeMissions` | Materialize task graphs, own the batteries-included world bundle, drain committed author and critic intents into external work, stage factual observations, and project terminal results |
| `iPhysicalAIService` | `PhysicalAIService` | application | Create batched evaluation worlds, install physical processors, run episodes, and derive typed reports from persisted state |
| Family resource service `missions.SandboxService` | `missions.SandboxService` | `MissionService` | Select a configured backend and acquire, reuse, close, and shut down mission-keyed sessions; no task-transition authority |
| Family resource port `missions.SandboxBackend` | configured Apple Container, Docker, or Modal adapter | `missions.SandboxService` | Create or restore provider-owned isolated sessions |
| Family resource port `missions.SandboxSession` | provider session adapter | `CodingAgentHarness`, `CriticHarness`, `missions.SandboxService` | Expose capability, process, status, checkpoint, and close operations for one live sandbox |
| Family resource port `missions.CriticDriver` | `CodexCriticDriver` or configured adapter | `CriticHarness` | Invoke one independent structured review with model capability but no Git publication capability |

## 4. Boundary rules

### Runtime application

`iRuntimeApplication` is consumed by both trusted runtime and authorized
gateway. It exposes ID-oriented operations and boundary-safe results. It does
not expose concrete services, the container, or live worlds. Runtime-only
ergonomics and lazy handle state remain in `archetype.runtime`; HTTP parsing
remains in `archetype.api`.

### Command gateway

Every `iCommandGateway` operation accepts `ActorCtx`, authorizes, delegates to
`iRuntimeApplication`, and attempts an access event. It has no tick-drain
method and owns no world, command ledger, grader, artifact ingestion, or storage.

### World ports and operation surfaces

Live-world returns from `iWorldLifecycle` and leases from `iWorldRegistry` are
legal only below the application boundary. Stateful world authority is limited
to those two family-owned ports. Mutation, simulation, durable query, and
externally operable adapters are public module functions in
`archetype.world.{mutation,simulation,query,handlers}` rather than
single-implementation service protocols.

Simulation imports neither commands nor gateway. Lifecycle receives the
scheduler materializer as a construction callable and wires it into every
managed world. Bounded episode termination reduces a lazy frame through
`iStorageService` before the scalar enters Python control flow.

### Durable workflow ports

`iCommandScheduler` exposes the current combined scheduling/dispatch port over
the control catalog. Tick publication performs terminal applied settlement.
`iIngestionService` owns the general typed-ingestion policy boundary: it
selects the live storage configuration and delegates typed publication. It has
no knowledge of files, media, transcripts, or graders. `iStorageService` owns
the corresponding physical boundary: the catalog-derived world/run envelope,
plain or caller-keyed conditional append, terminal Daft admission,
`daft.Catalog` table registration, schema alignment, lazy table reads, Iceberg
writes, and optimistic-conflict retry.

`iArtifactService` specializes that primitive for files. It discovers and
scans sources, persists immutable content-addressed objects, writes optional
media-specific indexes, and publishes the common file index last. There is no
artifact claim, lease, receipt, or reconciliation protocol around that path.
Provider checkpoints remain sandbox recovery objects rather than artifact
workflow stages. Agent Missions does not implicitly crawl a sandbox after a
task decision: a provider export must first select and sanitize declared files,
then submit valid `ArtifactSource` values through this port. Future live-event,
OTel, and proxy exporters consume the same redaction port; they do not fork
scanner policy.

`iTranscriptIngestionService` is a composition port, not another storage
authority. Its implementation snapshots and redacts through
`iRedactionService`, parses with the pure missions-family adapter, ingests the
sanitized snapshot through `iArtifactService`, and appends normalized rows
through `iIngestionService`. Raw narrative never crosses a durability boundary.
Each ingestion is a new artifact occurrence; normalized row identity is scoped
to that source artifact.
`iAuditLog` is a projection/read port, not the authority for command outcome.

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

`ServiceContainer` takes the backend configured by `AgentMissionConfig`,
constructs `SandboxService` around it, passes that service to
`MissionService`, and injects the concrete mission-service factory into
`RuntimeApplication`.
`RuntimeMissions` supplies only its runtime-owned world factory and supported
configuration, then consumes the returned port. No Component, processor,
relation, harness value, or sandbox implementation moves into `app`.

See [Agent Missions V1](agent-missions.md).

### Physical AI

`EnvClient` and `PolicyClient` belong to the top-level physical-AI family
because external simulator and model resources are implementations beneath
that capability. `iPhysicalAIService` is the app-internal workflow port. Its
implementation composes world lifecycle, entity/processor mutation, episode
execution, persisted evaluation reads, and storage-admitted terminal report
projection; it does not own those authorities.

`RuntimeApplication` is the only consumer exposed to the runtime. The
application service has no public constructor contract, accepts no gateway or
actor context, and emits no parallel summary Component. It returns typed
reports carrying the authoritative `(world_id, run_id)` coordinates. The
credential-free contract tests prove paired seeds, complete denominators,
policy reset, runtime/sync parity, and ledger addressability.

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
concrete service remains internal, and `ServiceContainer` remains unsupported.
The top-level family never imports that port in return. Public classification
is explicit and is not inferred from either side of the annotation.

The artifacts family owns the supported `ArtifactSource`, `ArtifactRef`, and
`ArtifactStoreConfig` file contracts. `archetype.ingestion` owns one reusable
`FileIngestionPipeline` and its pure bounded scanners; application policy and
authority remain under `archetype.app.ingestion`, `archetype.app.artifacts`,
and the canonical `archetype.storage` family. Storage owns the generic durable
world/run envelope; ingestion selects the live storage configuration rather
than duplicating persistence mechanics. The evaluation family completed its
split under issue #557: `EvalReceipt` lives in
`archetype.evaluation.components`, and the grading value contracts and
identity digests live in `archetype.evaluation.contracts`. Current paths
that predate this rule are migration state, not alternate ownership. The
research family completed #585: ledger Components and the pure runner decoder
live under `archetype.research`, while loop coordination remains under
`archetype.app.research`. The trajectory split completed #586: schemas,
authoring values, and structural transforms live under
`archetype.missions.trajectories`; `iTrajectoryService` composes only query and
evaluation ports.

The physical-AI split completed #589: typed request/report values and pure
optimization live under `archetype.physical_ai`, while
`iPhysicalAIService` composes world, mutation, simulation, evaluation, and
storage ports under `archetype.app.physical_ai`. The root `app/models.py`
boundary-model split remains owned by #560.

The root policy and its `quality/architecture.d/` fragments currently carry no
migration exceptions; no wildcard compatibility package is implied.
Redaction, audit, command, world, and other authority-specific models remain
with their app owners unless a focused specification classifies an individual
value as a reusable family contract.

The V1 mission split is the implemented example: Components, processors,
relations, authoring and coding-harness values, and sandbox resources live under
`archetype.missions`; graph materialization and cross-boundary workflow
composition live under `archetype.app.missions`.

## 6. Construction and shutdown

`ServiceContainer` in `app/container.py` is the sole concrete cross-family
wiring root. It exposes:

```text
application:      iRuntimeApplication
command_gateway:  iCommandGateway
application.agent_mission_service(...):
  iMissionService
```

Runtime consumes `application`; API dependency injection consumes
`command_gateway`. The container injects the concrete
mission factory into `RuntimeApplication`; the runtime implementation consumes
only `iRuntimeApplication` and the returned `iMissionService`. Neither the
concrete service nor the container is exposed to mission authors.

Shutdown stops new application admission, closes retained sandbox handles,
flushes the audit projection, then closes container-owned world/storage
resources.

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
