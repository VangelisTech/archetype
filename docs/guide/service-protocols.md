# Internal workflow protocols

**Document type:** Normative.

**Scope:** Genuine family-owned protocols and focused,
construction-injected lower-family ports described here.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

Agent Missions V1 uses the family-owned `MissionService`, `SandboxService`,
`SandboxBackend`, `SandboxSession`, coding-agent harness, exact-head critic
harness, and combined Activity binding. The single-implementation Mission,
trajectory, and transcript service mirrors are deleted.

The top-level `archetype.commands` family deliberately owns concrete
`OperationRegistry`, `CommandDispatcher`, `Policy`, `CommandScheduler`, and
`AuditLog` machinery rather than the deleted application-family scheduler and
audit protocols. Their composition edges are listed here so this port map
remains complete; they are not application ports.

## 1. Policy

Family protocols are internal dependency boundaries unless a focused
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

Protocols are co-located with their family. There is no compatibility protocol
module.

## 2. Dependency overview

Arrows point from consumer to dependency:

```text
ArchetypeRuntime -> commands.CommandDispatcher.apply / defer
FastAPI + ActorCtx -> commands.CommandDispatcher.apply_as / defer_as
commands.CommandDispatcher -> OperationRegistry -> exact family handler

evaluation.handlers -> iStorageService + archetype.world.query
artifacts.handlers + artifacts.views -> iStorageService
missions.TranscriptIngestionService
  -> artifacts.handlers + redaction structural port + iStorageService
iWorldLifecycle    -> iWorldRegistry + iStorageService
iWorldLifecycle    -> iWorldActivationOwner (private cleanup-only creation)
CommandDispatcher  -> OperationRegistry + Policy + CommandScheduler
                   -> AuditLog.record_access
CommandScheduler   -> storage control catalog
                   -> world.handlers lock-held materialization
research.handlers  -> iWorldRegistry + iWorldLifecycle + iStorageService
                   -> world simulation + exact owned-world cleanup
physical_ai.hosted_workflow
  -> iWorldRegistry + hosted Activity binding
  -> archetype.world mutation/simulation
AuditLog           -> iStorageService + CommandScheduler outbox callbacks

RuntimeMissions -> CommandDispatcher -> registered mission handler
registered mission handler -> reservation-owned missions.MissionService
missions.MissionService
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
| Structural `MissionRedactor` / `TranscriptRedactor` | canonical `archetype.redaction.RedactionService` | mission execution and transcript ingestion | Provider-neutral pre-durability scanning, deterministic redaction, safe receipts, and quarantine |
| Family resource service `missions.SandboxService` | `missions.SandboxService` | `MissionService` | Select a configured backend and acquire, reuse, close, and shut down mission-keyed sessions; no task-transition authority |
| Family resource port `missions.SandboxBackend` | configured Apple Container, Docker, or Modal adapter | `missions.SandboxService` | Create or restore provider-owned isolated sessions |
| Family resource port `missions.SandboxSession` | provider session adapter | `CodingAgentHarness`, `CriticHarness`, `missions.SandboxService` | Expose capability, process, status, checkpoint, and close operations for one live sandbox |
| Family resource port `missions.CriticDriver` | admitted Modal `CodexAppServerCriticDriver`; capability-only `CodexCriticDriver`; or configured adapter | `CriticHarness` | Invoke one independent structured review with model capability but no Git publication capability |

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

`TranscriptIngestionService` is a family-owned workflow, not another storage
authority. It snapshots and redacts through
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
The configured backend creates a provider-owned session; `SandboxService`
selects that backend and single-flights acquisition by a `SandboxKey`. Modal is
the supported end-to-end Mission backend. Apple Container and Docker remain
sandbox capabilities but Mission submission rejects them before admission
until they have equivalent fail-closed Activity adapters.

`MissionService` is the family-owned workflow. It composes a structural mission
world with the built-in Components, processors, relationships, graph view, one
combined author-and-critic Activity binding, both repository harnesses, and the
sandbox service. Each committed tick is read through the exact required
projector. Author dispatches and exact candidate reviews are admitted directly
into the generic coordinator, executed or reconciled outside the world lock,
staged for a later tick, and settled only by an exact receipt bound to the
recorded Activity result reference and digest. A dispatch or review ID alone is
insufficient. Sandbox providers and processors do not acquire Activity
transition authority.

Graph materialization records each authored `TaskValidator`. The harness then
prepares the repository, runs the coding agent and those validator commands,
performs Git publication, and returns observations that the service records as
`AgentExecution`, `ValidationResult`, `Commit`, and `FrictionLog`
Components and relations. Complete passing exact-revision evidence plus one
published final head becomes a `Candidate`, not acceptance. After that
candidate commits, `CriticHarness` verifies the remote subject in a fresh
sandbox, invokes `CriticDriver`, and returns
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
owner with the combined Activity binding plus a narrow exact-world cleanup factory.
`RuntimeMissions` supplies only supported configuration and exact operations;
it never receives the service. No Component, processor, relation, harness
value, or sandbox implementation moves into process composition.

See [Agent Missions V1](agent-missions.md).

### Physical AI

The public distributed surface is one exact trusted-only direct operation,
`RunHostedEpisode`, on a `RuntimeWorld`. Its public values live in
`archetype.physical_ai.models`, and its provider configuration identifies one
exact Modal namespace.

`archetype.physical_ai.hosted_workflow` commits intent, invokes the
world-scoped Activity worker outside the World lock, and commits the complete
observation in a later tick. The family-owned binding supplies exact-receipt
projection, provider reconciliation, content-addressed values, and observation
staging. `RuntimeResources` owns that binding and worker; world-owned required
projection fans out deterministically by consumer name when multiple Activity
families share a World.

No public operation installs remote environment or policy clients in a tick.
The `EnvClient` and `PolicyClient` protocols remain internal support for
explicit in-process processor composition. Pure in-memory paths such as the
MuJoCo cart-pole processor remain supported examples; distributed execution
crosses only the whole-episode Activity boundary.

Modal permanent-start evidence and the first complete provider result are
authoritative. Lease expiry does not authorize replay. Exact recovery returns
the existing result, confirmed absence requires the provider retry guard, and
unknown start state fails closed.

See [Physical AI](physical-ai.md) and [Activities](activities.md).

## 5. Values crossing family ports

Cross-family values are immutable or frozen where identity matters, but their
Python modeling technology does not decide their layer. Persistent ECS schema
is a `Component` and belongs in `archetype.<family>.components`. Supported
reusable Pydantic/dataclass values belong in the top-level family's
`contracts.py` or another specifically named family module. Workflow authority
and genuine ports remain with their named families. The reviewed
physical-storage substrate is
`archetype.storage`: control-catalog records and implementations, physical
visibility, commit coordination, and the generic durable world/run envelope
live there while consuming families retain workflow meaning. Public
classification is explicit and is not inferred from package placement.

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
workflow handler live under `archetype.research`. There is no research service
mirror. The trajectory split completed
issue #586: schemas, authoring values, and structural transforms live under
`archetype.missions.trajectories`; `TrajectoryService` composes durable query
with the evaluation family's pure grader runner.

The physical-AI family owns the hosted operation, request and observation
values, provider recovery, content contracts, and free workflow over declared
world and storage ports. The former app mirror, direct per-step distributed
operations, compatibility shim, `iPhysicalAIService`, and root application
command-envelope boundary are gone.

The root policy and its `quality/architecture.d/` fragments currently carry no
migration exceptions; no wildcard compatibility package is implied.
Redaction, audit, command, world, and other authority-specific models remain
with their owning families unless a focused specification classifies an
individual value as a reusable family contract.

Agent Missions is the implemented example: Components, processors, relations,
authoring and coding-harness values, sandbox resources, graph materialization,
and cross-boundary workflow composition live under `archetype.missions`.

## 6. Construction and shutdown

`archetype.wiring.build_runtime_resources()` is the sole concrete cross-family
composition transaction. It builds:

```text
OperationRegistry + Policy + CommandScheduler + CommandDispatcher
WorldRegistry + WorldLifecycle + AuditLog + StorageService
family services + registered exact handlers
RuntimeResources
```

Runtime retains `RuntimeResources`; API lifespan retains the same process
owner and dependency injection exposes only its dispatcher. Mission handlers
construct and resolve `MissionService` inside the exact workflow reservation.
Neither concrete services nor process wiring are exposed to mission authors.

Shutdown stops and drains dispatcher admission, joins supervised work, closes
workflow then world handles, flushes the audit projection, and finally closes
owned storage. Failed phases retain exact ownership for retry.

## 7. Executable enforcement

- `scripts/check_architecture.py`
- `quality/architecture.toml`
- `quality/architecture.d/`
- `tests/scripts/test_check_architecture.py`
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
