# Application family protocols

**Document type:** Normative.

**Scope:** Internal structural interfaces under
`src/archetype/app/<family>/interfaces.py`.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

Agent Missions V1 uses the family-owned `SandboxService`, `SandboxBackend`, and
`SandboxSession` resource stack beneath the app-owned `iMissionService`
workflow. Physical evaluation uses the same ownership pattern: family-owned
environment/policy protocols beneath the app-owned `iPhysicalAIService`.

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
  -> iWorldService + iMutationService + iSimulationService
  -> iQueryService
  -> iArtifactService
  -> iTranscriptIngestionService
  -> iEvaluationService
  -> iCommandScheduler
  -> iAuditLog
  -> iResearchService
  -> iMissionService
  -> iPhysicalAIService

iEvaluationService
  -> iQueryService + iIngestionService + iStorageService + iWorldService
iRedactionService -> no lower application family
iIngestionService  -> iStorageService + iWorldService
iArtifactService   -> iIngestionService + iStorageService + iWorldService
iTranscriptIngestionService
  -> iArtifactService + iIngestionService
  -> iRedactionService + iStorageService + iWorldService
iQueryService      -> iStorageService + iAuditLog
iWorldService      -> iStorageService
iMutationService   -> iWorldService
iSimulationService -> iWorldService + iStorageService + injected callbacks
iCommandScheduler  -> iWorldService + iMutationService
iResearchService   -> iWorldService + iSimulationService + iStorageService
iPhysicalAIService
  -> iWorldService + iMutationService + iSimulationService
  -> iEvaluationService + iStorageService
iAuditLog          -> iStorageService

RuntimeMissions -> iRuntimeApplication -> iMissionService
iMissionService
  -> missions.SandboxService -> missions.SandboxBackend
  -> missions.CodingAgentHarness -> missions.SandboxSession
```

`ServiceContainer` selects concrete implementations across families. Agent
Missions reaches its container-selected workflow through the actor-free
application facade.

## 3. Active mapping

| Port | Implementation | Principal consumers | Responsibility |
|---|---|---|---|
| `iRuntimeApplication` | `RuntimeApplication` | runtime, `CommandGateway` | Actor-free canonical product operations and per-world serialization |
| `iCommandGateway` | `CommandGateway` | FastAPI and other untrusted adapters | RBAC/quota authorization, delegation, access audit |
| `iStorageService` | `StorageService` | world, simulation, query, ingestion, artifacts, evaluation, transcripts, research, physical AI, audit | Store/session lifetime, control authority, terminal Daft execution, and app-table catalog/read/write/retry authority |
| `iWorldService` | `WorldService` | mutation, simulation, commands, ingestion, artifacts, evaluation, transcripts, research, physical AI, application | Live-world lifecycle, durable discovery, coordinate lookup |
| `iMutationService` | `MutationService` | application, commands, physical AI | Entity/component/processor mutation staging |
| `iSimulationService` | `SimulationService` | application, research, physical AI | Step, run, episode and rollout execution |
| `iQueryService` | `QueryService` | application, evaluation | Persisted ECS reads, signature/lineage discovery and compatibility history |
| `iIngestionService` | `IngestionService` | artifacts, transcripts, evaluation | Add world/run identity and select plain or caller-keyed conditional append through storage |
| `iArtifactService` | `ArtifactService` | application, transcript ingestion | Discover and scan files, persist content-addressed objects, publish typed media indexes, then expose the common file index |
| `iTranscriptIngestionService` | `TranscriptIngestionService` | application | Snapshot and redact a coding-agent transcript, ingest the sanitized file, and append normalized mission rows |
| `iRedactionService` | `RedactionService` | transcript ingestion; future telemetry/proxy adapters | Provider-neutral pre-durability scanning, deterministic text redaction, safe receipts, and quarantine |
| `iEvaluationService` | `EvaluationService` | application, physical AI | Pin persisted world state, lease grader execution through the shared control authority, and append one typed evaluation result |
| `iCommandScheduler` | `CommandScheduler` | application | Durable admission, leasing, dispatch, retry, settlement and outbox inspection |
| `iAuditLog` | `AuditLog` | application, gateway, query | Append-only access rows and command-outbox projection |
| `iResearchService` | `AutoResearchService` | application | Multi-run autoresearch workflow and research ledger |
| `iMissionService` | `MissionService` | application, `RuntimeMissions` | Materialize task graphs, own the batteries-included world bundle, drain committed dispatches into external work, stage factual observations, and project terminal results |
| `iPhysicalAIService` | `PhysicalAIService` | application | Create batched evaluation worlds, install physical processors, run episodes, and derive typed reports from persisted state |
| Family resource service `missions.SandboxService` | `missions.SandboxService` | `MissionService` | Select a configured backend and acquire, reuse, close, and shut down mission-keyed sessions; no task-transition authority |
| Family resource port `missions.SandboxBackend` | configured Apple Container, Docker, or Modal adapter | `missions.SandboxService` | Create or restore provider-owned isolated sessions |
| Family resource port `missions.SandboxSession` | provider session adapter | `CodingAgentHarness`, `missions.SandboxService` | Expose capability, process, status, checkpoint, and close operations for one live sandbox |

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

### World ports

Live-world returns from `iWorldService` are legal only below the application
boundary. `iMutationService` and `iSimulationService` are siblings over that
port. Simulation imports neither commands nor gateway; the container supplies
its drain and quota-reset callables. Its bounded episode-termination reduction
is admitted through `iStorageService` before the scalar enters Python control
flow.

### Durable workflow ports

`iCommandScheduler` exposes the current combined scheduling/dispatch port over
the control catalog. Tick publication performs terminal applied settlement.
`iIngestionService` owns the general typed-ingestion policy boundary: it
supplies the world/run envelope and selects either a plain append or a
caller-keyed conditional append. It has no knowledge of files, media,
transcripts, or graders. `iStorageService` owns the corresponding physical
boundary: terminal Daft admission, `daft.Catalog` table registration, schema
alignment, lazy table reads, Iceberg writes, and optimistic-conflict retry.

`iArtifactService` specializes that primitive for files. It discovers and
scans sources, persists immutable content-addressed objects, writes optional
media-specific indexes, and publishes the common file index last. There is no
artifact claim, lease, receipt, or reconciliation protocol around that path.
Provider checkpoints remain sandbox recovery objects rather than artifact
workflow stages. Future live-event, OTel, and proxy exporters consume the same
redaction port; they do not fork scanner policy.

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
value contracts, coding-harness values, Components, relations, and processors.
The configured backend creates or restores a provider-owned session;
`SandboxService` selects that backend and single-flights acquisition by a
`SandboxKey`. For V1 the application service uses a mission-keyed value, so
tasks in one mission can reuse the retained session without making that live
handle durable authority.

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
built-in Components, processors, relationships, graph view, committed-intent
outbox, coding harness, and sandbox service. After a tick commits,
`TaskDispatchOutbox` projects newly persisted `TaskDispatch` data into external
work requests. The service acquires the mission-keyed session and invokes the
harness only from that post-commit path, then stages the returned factual
observations for a later tick.

Graph materialization records each authored `TaskValidator`. The harness then
prepares the repository, runs the coding agent and those validator commands,
performs Git publication, and returns facts that the service records as
`AgentExecution`, `ValidationResult`, `Commit`, and `FrictionLog`
Components and relations. The sandbox identity is staged immediately after
acquisition; bounded `SandboxEvent` callbacks expose it synchronously for live,
non-authoritative operator updates. Validator success is derived from expected
and actual return codes; neither the harness, sandbox, nor service decides task
state. Processors alone accept a task, retry it, exhaust its dispatch budget,
unlock dependent tasks, and roll terminal task states up to the mission.

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
commands, authority records, backend state, authorization values, and service
ports remain under `archetype.app.<family>`.

An internal app protocol may therefore accept or return a top-level family
value. The protocol still lives in `archetype.app.<family>.interfaces`, its
concrete service remains internal, and `ServiceContainer` remains unsupported.
The top-level family never imports that port in return. Public classification
is explicit and is not inferred from either side of the annotation.

The artifacts family owns the supported `ArtifactSource`, `ArtifactRef`, and
`ArtifactStoreConfig` file contracts. `archetype.ingestion` owns one reusable
`FileIngestionPipeline` and its pure bounded scanners; application policy and
authority remain under `archetype.app.ingestion`, `archetype.app.artifacts`,
and `archetype.app.storage`. The evaluation family completed its split under
issue #557: `EvalReceipt` lives in
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
