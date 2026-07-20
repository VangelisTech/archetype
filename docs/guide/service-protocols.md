# Application family protocols

**Document type:** Normative.

**Scope:** Internal structural interfaces under
`src/archetype/app/<family>/interfaces.py`.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

Agent Missions V1 uses the family-owned `AgentMissionSandbox` resource protocol
behind the app-owned `iAgentMissionService` workflow. Physical evaluation uses
the same ownership pattern: family-owned environment/policy protocols beneath
the app-owned `iPhysicalAIService`. The older `iMission*` and `iSandbox*` ports
are a separate compatibility subsystem.

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
- an allowed edge in `quality/architecture.toml`; and
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
  -> iArtifactService + iArtifactTableService + iArtifactBundleService
  -> iEvaluationService
  -> iCommandScheduler
  -> iAuditLog
  -> iResearchService
  -> iAgentMissionService
  -> iPhysicalAIService

iEvaluationService -> iQueryService + iArtifactService
iArtifactBundleService -> iRedactionService + iStorageService + iWorldService
iRedactionService -> no lower application family
iArtifactService   -> iStorageService + iWorldService
iArtifactTableService -> iStorageService + iWorldService
iQueryService      -> iStorageService + iAuditLog
iWorldService      -> iStorageService
iMutationService   -> iWorldService
iSimulationService -> iWorldService + injected callbacks
iCommandScheduler  -> iWorldService + iMutationService
iResearchService   -> iWorldService + iSimulationService
iPhysicalAIService
  -> iWorldService + iMutationService + iSimulationService
  -> iEvaluationService
iAuditLog          -> iStorageService

RuntimeMissions -> iRuntimeApplication -> iAgentMissionService
iAgentMissionService -> AgentMissionSandbox

# Legacy mission-attempt compatibility stack
iMissionService    -> typed mission rows (no service dependency)
iMissionAttemptClaimService -> ControlCatalog + iRedactionService
iMissionAttemptExecutionService
  -> iMissionService + iMissionAttemptClaimService
  -> iMissionArtifactFinalizer + FencedAttemptRunner
iMissionArtifactFinalizer -> iArtifactBundleService
iSandboxService    -> registered iSandboxBackend providers
```

`ServiceContainer` selects concrete implementations across families. In the
legacy stack, the mission execution service receives both mission authorities
through their protocols and accepts a structural sandbox runner per attempt;
it is orchestration, not another composition root. V1 reaches its container-
selected mission workflow through the actor-free application facade.

## 3. Active mapping

| Port | Implementation | Principal consumers | Responsibility |
|---|---|---|---|
| `iRuntimeApplication` | `RuntimeApplication` | runtime, `CommandGateway` | Actor-free canonical product operations and per-world serialization |
| `iCommandGateway` | `CommandGateway` | FastAPI and other untrusted adapters | RBAC/quota authorization, delegation, access audit |
| `iStorageService` | `StorageService` | world, query, artifacts, audit | Store pooling, catalog/control-authority and storage-context lifetime |
| `iWorldService` | `WorldService` | mutation, simulation, commands, artifacts, research, physical AI, application | Live-world lifecycle, durable discovery, coordinate lookup |
| `iMutationService` | `MutationService` | application, commands, physical AI | Entity/component/processor mutation staging |
| `iSimulationService` | `SimulationService` | application, research, physical AI | Step, run, episode and rollout execution |
| `iQueryService` | `QueryService` | application, evaluation | Persisted ECS reads, signature/lineage discovery and compatibility history |
| `iArtifactService` | `ArtifactService` | application, evaluation | Claim-backed component publication and immutable snapshot pinning |
| `iArtifactTableService` | `ArtifactTableService` | application | Typed file/row ingestion and contextual reads |
| `iArtifactBundleService` | `ArtifactBundleService` | application | Portable evidence publication, indexing, and reconciliation |
| `iRedactionService` | `RedactionService` | artifact bundles, mission attempt claims; future telemetry/proxy adapters | Provider-neutral pre-durability scanning, deterministic text redaction, safe receipts, and quarantine |
| `iEvaluationService` | `EvaluationService` | application, physical AI | Query, grade, validate and publish evaluation evidence |
| `iCommandScheduler` | `CommandScheduler` | application | Durable admission, leasing, dispatch, retry, settlement and outbox inspection |
| `iAuditLog` | `AuditLog` | application, gateway, query | Append-only access rows and command-outbox projection |
| `iResearchService` | `AutoResearchService` | application | Multi-run autoresearch workflow and research ledger |
| `iAgentMissionService` | `AgentMissionService` | application, `RuntimeMissions` | Materialize task graphs, own the batteries-included world bundle, coordinate committed I/O, validate receipts, and project terminal results |
| `iPhysicalAIService` | `PhysicalAIService` | application | Create batched evaluation worlds, install physical processors, run episodes, and derive typed reports from persisted state |
| Family resource port `AgentMissionSandbox` | `ModalAgentMissionSandbox` | `AgentMissionService` | V1 isolated execution and typed receipts; no task-transition authority |
| Legacy `iMissionService` | `MissionService` | legacy attempt orchestration | Single-row validator normalization, transition graph, retry/exhaustion, and outcome application |
| Legacy `iMissionAttemptClaimService` | `MissionAttemptClaimService` | legacy attempt orchestration and recovery workers | Pre-durability redaction, durable claims, fencing, recovery decisions, acknowledgement, settlement, and terminal-winner reread |
| Legacy `iMissionArtifactFinalizer` | application-owned `MissionArtifactFinalizer` | legacy attempt execution and recovery workers | Prepared artifact publication and reconciliation feedback |
| Legacy `iMissionAttemptExecutionService` | `MissionAttemptExecutionService` | legacy coding-agent processors and supervisors | Claim/arm, provider admission, heartbeat, acknowledgement, finalization, settlement, and replay |
| Legacy `iSandboxService` | `SandboxService` | container, legacy mission orchestration | Provider selection and process-local create/restore/resume/close lifetime |
| Legacy `iSandboxBackend` | host-selected provider adapters | legacy sandbox service | Provider-specific isolated execution and checkpoint recovery |

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
method and owns no world, command ledger, grader, artifact claim, or storage.

### World ports

Live-world returns from `iWorldService` are legal only below the application
boundary. `iMutationService` and `iSimulationService` are siblings over that
port. Simulation imports neither commands nor gateway; the container supplies
its drain and quota-reset callables.

### Durable workflow ports

`iCommandScheduler` exposes the current combined scheduling/dispatch port over
the control catalog. Tick publication performs terminal applied settlement.
`iArtifactService` and `iEvaluationService` expose separate claim-backed
workflows. `iArtifactBundleService` owns full attempt-bundle publication and
reconciliation while provider checkpoints remain recovery objects. It consumes
`iRedactionService` before its control, object, manifest, and index durability
boundaries. Future live-event, OTel, and proxy exporters consume that same port;
they do not fork scanner policy.
`iAuditLog` is a projection/read port, not the authority for command outcome.

### Agent Missions V1

`AgentMissionSandbox` belongs to `archetype.missions.contracts` because coding
agents and their execution resources are implementations beneath the missions
family. Its `run_many`, `close_mission`, and `close` methods exchange typed
`TaskExecutionRequest` and `TaskExecutionReceipt` values. The sandbox reports
observations; mission processors remain transition authority.

`iAgentMissionService` is the app-internal workflow port implemented by
`AgentMissionService`. The service composes a structural mission world with the
built-in Components, processors, relationships, graph view, committed-intent
outbox, and sandbox resource. It owns graph materialization, the post-commit I/O
loop, receipt validation, lifecycle, and terminal projection. It does not own
readiness, acceptance, retry, exhaustion, or rollup policy.

`ServiceContainer` injects the concrete service factory into
`RuntimeApplication`. `RuntimeMissions` supplies only its runtime-owned world
factory and supported configuration, then consumes the returned port. No
Component, processor, relation, or sandbox implementation moves into `app`.

See [Agent Missions V1](agent-missions.md).

### Physical AI

`EnvClient` and `PolicyClient` belong to the top-level physical-AI family
because external simulator and model resources are implementations beneath
that capability. `iPhysicalAIService` is the app-internal workflow port. Its
implementation composes world lifecycle, entity/processor mutation, episode
execution, and persisted evaluation reads; it does not own those authorities.

`RuntimeApplication` is the only consumer exposed to the runtime. The
application service has no public constructor contract, accepts no gateway or
actor context, and emits no parallel summary Component. It returns typed
reports carrying the authoritative `(world_id, run_id)` coordinates. The
credential-free contract tests prove paired seeds, complete denominators,
policy reset, runtime/sync parity, and ledger addressability.

See [Physical AI](physical-ai.md).

### Legacy mission-attempt ports

Within the legacy stack, `iMissionService` is the sole single-row
mission/task/attempt transition authority. It is
a pure row transformer: it owns no provider, live handle, world, storage
client, or authorization context. Consumers persist its result through the
ordinary world tick. Preparation canonicalizes validator names, commands,
return codes, timeouts, and defaults before external state exists, and binds
the retry budget and required finalization phase into the durable request and
its identities. Public current-write `apply_attempt` categorically rejects an
`indexed` phase and every artifact staging, linkage, finalized-authority, or
nonzero-snapshot field. It is the only public projection operation on
`iMissionService`; the protocol exposes no settled-row application method.

`iMissionAttemptClaimService` is the provider-submission control authority. It
persists immutable request identity through the per-world storage control
catalog, issues leased and fenced recovery decisions, records provider
acknowledgement, and terminally settles canonical outcomes. Its
`iRedactionService` dependency is required: canonical request and provider
capabilities quarantine before claim creation, provider identity quarantines
before acknowledgement, and semantic outcome IDs/references quarantine before
projection or settlement. The active policy ID is immutable claim identity.
Typed receipts for request, provider, acknowledgement, outcome, and last error
share that policy and remain durable with the claim.

Narrative outcome and error values are deterministically redacted before a
mission projection or catalog CAS. A prepared `RedactedRecord` retains the
original finding receipt through defensive validation. Non-terminal policy
drift fails closed, while a settled sanitized record remains readable without
requiring the retired policy implementation.

When artifact finalization enriches that sanitized outcome, the claim authority
rescans the exact terminal mapping before settlement. Its final receipt carries
the terminal canonical byte count while retaining the original redaction
status, count, and rule identifiers, so exact coverage cannot erase finding
evidence.

Arming creates one opaque execution nonce for the fence; `consume_execution`
atomically spends that nonce under the live catalog lease, and acknowledgement
requires that consumption. Settlement requires a complete replayable outcome
whose status agrees with the authoritative mission-derived attempt status.
Generic settlement rejects `finalizing`; only `settle_finalized` accepts the
claim-bound, service-sealed result returned by the claim authority after it
rereads an exact terminal `INDEXED` or `EXPIRED` artifact row from the same
storage-bound control catalog and binds it to the staged request. A public
receipt value is orchestration feedback, never settlement authority.
Mission outcomes of `accepted` or `incomplete` derived from provider acceptance
require consumed-grant evidence; checkpoint provider and agent session evidence
must match the claim and its durable acknowledgement. The service owns no
provider client or live sandbox handle. Its control-plane transaction does not
advance a task. Its `require_settled(world_id, claim_key)` operation rereads the
catalog, requires the terminal winner, and authenticates its canonical payload
at the boundary that creates projection authority. A detached or
caller-replaced `AttemptClaim` DTO is never equivalent to that read. The
execution service consumes this operation immediately before its private row
transformation and the ordinary world tick. See the
[Legacy mission attempt kernel](legacy-mission-attempt-kernel.md).

`iMissionAttemptExecutionService` is the supported orchestration port for one
attempt. It prepares and acquires through the two authorities above, then
invokes an injected `FencedAttemptRunner`. Claim acquisition always uses the
runner's own `provider_execution_capabilities`; the orchestration method does
not accept a separate capability object that could describe another adapter or
execution specification. The runner receives one callback that atomically
consumes the execution grant immediately before provider work and another that
persists provider acknowledgement before validation. The service runs a
durable-lease heartbeat for the entire runner lifetime. A heartbeat failure
cancels and awaits the runner; caller cancellation cancels
and awaits both local tasks. This cleanup makes no claim that a remote provider
operation was terminated; that remains adapter-specific or reconciled from
`possibly_submitted`. After successful runner completion, the service renews
the claim once more before it validates evidence through the claim service's
pre-durability redaction boundary. When policy requires `indexed`, it asks
`iMissionArtifactFinalizer.prepare` for an exact request without external I/O,
atomically stages that request and sanitized outcome on the claim, then calls
`publish` only with the reconstructed staged projection. Accepted and rejected
attempts with restorable checkpoints use this path. A cold `finalizing` claim
repeats publication only: it never calls the runner, model, validators,
repository finalizer, or checkpoint capture. Only an exact durable `INDEXED`
row may upgrade the outcome. Durable expiry likewise requires the exact
`EXPIRED` artifact row and cannot be inferred from the claim or a process-local
exception. Either result is authenticated, prepared, and sealed by the claim
authority, then passed to `settle_finalized` before any mission projection.
The execution service calls `iMissionAttemptClaimService.require_settled` with
the stable world and claim keys, authenticates the durable winner returned by
that reread, and invokes an implementation-private mission row transformer.
This path is absent from `iMissionService`, so a caller-supplied claim value
cannot mint settled projection authority. If the claim is already settled, the
service follows that same reread-and-private-transform path without calling the
runner or finalizer. The
mission-owned `FencedAttemptRunner` protocol includes the provider-capability
property and prevents a static dependency on the sandbox family while allowing
its common kernel to conform structurally.

`iMissionService` remains an internal application port and `MissionService`
remains its concrete implementation. Its row transforms consume
`archetype.missions` Components and pure transition types; moving those
reusable values does not promote the service, claim DTOs, execution
authorization, or recovery contracts.

### Legacy sandbox ports

These ports are not used by the Modal Agent Missions V1 resource.
`iSandboxService` owns external resource lifetime and provider selection; it
never decides whether a task advances. `ServiceContainer` constructs an empty
provider registry unless a trusted host supplies adapters. The common attempt
kernel consumes a mission-owned immutable `FencedExecutionAuthorization` and
uses the injected admission callback rather than importing the claim service.
It emits typed phase evidence without importing Modal, Apple Container, or
another provider SDK. See [Sandbox Execution](sandbox-execution.md).

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

The artifacts family completed this migration under #558: `ArtifactMeta` and
`AssetRef` live in `archetype.artifacts.components`, the typed-table and
content-addressing contracts live in `archetype.artifacts.contracts`, and the
bundle value contracts live in `archetype.artifacts.bundles`. The evaluation
family completed it under #557: `EvalReceipt` lives in
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
`iPhysicalAIService` composes world, mutation, simulation, and evaluation
ports under `archetype.app.physical_ai`. The root `app/models.py`
boundary-model split remains owned by #560.

`quality/architecture.toml` currently carries no migration exceptions; no
wildcard compatibility package is implied. Redaction, audit, sandbox, command,
world, and other authority-specific models remain with their app owners unless
a focused specification classifies an individual value as a reusable family
contract.

The V1 mission split is the implemented example: Components, processors,
relations, authoring/receipt values, and sandbox resources live under
`archetype.missions`; graph materialization and cross-boundary workflow
composition live under `archetype.app.missions`. The retained claim, lease,
fence, finalization, and app-sandbox types are legacy internal machinery, not a
second V1 ownership pattern.

## 6. Construction and shutdown

`ServiceContainer` in `app/container.py` is the sole concrete cross-family
wiring root. It exposes:

```text
application:      iRuntimeApplication
command_gateway:  iCommandGateway
application.agent_mission_service(...):
  iAgentMissionService
mission_attempt_workflow(storage_config):
  iMissionService + iMissionAttemptClaimService
  + iMissionArtifactFinalizer + iMissionAttemptExecutionService
```

Runtime consumes `application`; API dependency injection consumes
`command_gateway`. A trusted mission reconciler must request the explicit
per-storage workflow: the factory binds the claim catalog and artifact
finalizer to the same copied `StorageConfig`, including during cold recovery
before a world has been opened in that process. There is no advertised
unbound/default-catalog mission finalizer. Focused implementation tests may
inspect internal members without creating compatibility.

Agent Missions V1 follows the same rule. The container injects the concrete
mission factory into `RuntimeApplication`; the runtime implementation consumes
only `iRuntimeApplication` and the returned `iAgentMissionService`. Neither the
concrete service nor the container is exposed to mission authors.

Shutdown stops new application admission, closes retained sandbox handles,
flushes the audit projection, then closes container-owned world/storage
resources.

## 7. Executable enforcement

- `scripts/check_architecture.py`
- `quality/architecture.toml`
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
- [Artifact Finalization](artifact-finalization.md)
- [Agent Missions V1](agent-missions.md)
- [Legacy Mission Attempt Kernel](legacy-mission-attempt-kernel.md)
- [Sandbox Execution](sandbox-execution.md)
