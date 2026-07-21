# Application families and wiring

The internal application layer wraps the core ECS engine with multi-world
lifecycle, durable commands, artifacts, evaluation, audit projection, and
storage ownership. Concrete services and `ServiceContainer` are not supported
application APIs. Use `ArchetypeRuntime`, REST, or CLI.

Normative dependency rules live in
[Application Architecture](application-architecture.md); active ports live in
[Service Protocols](service-protocols.md).

## ServiceContainer

`ServiceContainer` is the sole concrete cross-family composition root. It
constructs two outward objects:

```text
container.application      RuntimeApplication (trusted, actor-free)
container.command_gateway  CommandGateway (authorized ingress)
```

Construction is synchronous; stores and catalogs open lazily. Shutdown stops
new application admission, flushes audit projection, then closes container-owned
world/storage resources.

## Wiring overview

Arrows mean consumer to dependency:

```text
ArchetypeRuntime -> RuntimeApplication <- CommandGateway <- REST API
                         |
                         +-> MutationService -> WorldService -> StorageService
                         +-> SimulationService -> WorldService + StorageService
                         +-> QueryService -> StorageService
                         +-> IngestionService -> StorageService + WorldService
                         +-> ArtifactService -> IngestionService + StorageService + WorldService
                         +-> EvaluationService -> QueryService + IngestionService + StorageService + WorldService
                         +-> AutoResearchService -> WorldService + SimulationService + StorageService
                         +-> PhysicalAIService -> WorldService + MutationService + SimulationService + EvaluationService + StorageService
                         +-> CommandScheduler -> WorldService + MutationService
                         +-> AuditLog -> StorageService
```

The container injects `RuntimeApplication.drain_and_apply` and the quota-reset
callable into `SimulationService`. These named callbacks avoid reverse static
imports. It also connects `CommandScheduler`'s transactional outbox to
`AuditLog`'s analytical projection.

## Storage family

`StorageService` pools async stores and resolves local SQLite or remote control
catalogs for a storage identity. It is also the sole application authority for
terminal Daft execution and for app-table registration, schema alignment,
reads, writes, and optimistic conflict retry. It owns those physical and
execution concerns, not the meaning of a tick, command, artifact, or evaluation
commit.

The SQLite or Durable Object control catalog owns fences, manifests, commands,
and workflow leases. Daft Catalog, Iceberg snapshots, and object storage are the
data plane. `StorageService` composes these distinct authorities; neither
substitutes for the other.

See [Stores](stores.md).

## World family

`WorldService` owns live-world lifecycle and registry access. Its world factory
composes an `AsyncWorld` from a shared store, querier, updater, system,
resources, and hooks. `MutationService` and `SimulationService` are siblings
over that lifecycle port:

- mutation stages entity, component, processor, resource, and hook changes;
- simulation owns step, run, episode, and rollout execution.

Live worlds never escape the application boundary. See
[World Lifecycle](world-lifecycle.md) and
[Execution Hierarchy](execution-hierarchy.md).

## Query family

`QueryService` reads persisted component state and durable signature/lineage
metadata without requiring a live world. Runtime/application callers receive
Daft DataFrames or safe descriptors. Its current audit dependency serves the
history compatibility read; command outcome authority remains the command
ledger/outbox.

## Ingestion and artifact families

`IngestionService` supplies the world/run envelope and selects either a plain
append or a caller-keyed conditional append. `StorageService` then owns table
registration, schema comparison, Daft execution, and the Iceberg commit. The
ingestion service does not know whether its rows describe files, transcripts,
evaluations, or a future tabular source.

`ArtifactService` is the one file-specialized workflow. It scans declared
sources, persists content-addressed objects, writes typed media metadata, and
publishes the common artifact index last. It composes `IngestionService`; it
does not add a claim, lease, receipt, or reconciliation state machine. See
[Artifacts](artifacts.md).

## Evaluation and research families

`EvaluationService` pins persisted world state through storage/query authority,
executes caller-provided graders, validates typed outcomes, and appends one
durable evaluation result through `iIngestionService`.

`AutoResearchService` owns the multi-iteration rollout workflow and its durable
research ledger. It depends on world and simulation ports, and uses storage for
bounded persisted-control reads; scoring remains an explicit callback contract.

`PhysicalAIService` composes world, mutation, simulation, and evaluation ports.
It uses storage to materialize the bounded terminal projection from which it
builds a typed report; the report is not a second state authority.

## Commands family

`CommandScheduler` admits, leases, dispatches, retries, settles, and inspects
durable tick-deferred commands. It does not authorize users. Applied outcomes
settle atomically with the tick visibility manifest; authoritative events are
written to its outbox.

See [Durable Commands](durable-commands.md).

## Audit family

`AuditLog` projects access events and command-outbox events into append-only
Iceberg rows. The outbox/command ledger is authoritative for workflow outcome;
the analytical projection can lag.

See [Audit Log](audit-log.md).

## RuntimeApplication

`RuntimeApplication` is the canonical actor-free application facade. It owns
operation admission and per-world serialization while delegating each workflow
to its family port. It does not own concrete services or durable state.

Trusted local runtime calls terminate here. No `ActorCtx` is invented for local
scripting.

## CommandGateway

`CommandGateway` is the policy boundary for untrusted adapters. It accepts
`ActorCtx`, checks RBAC/quota policy, delegates to `iRuntimeApplication`, and
attempts one access-audit emission. It does not implement evaluation, world
lifecycle, command dispatch, or persistence.

FastAPI consumes `iCommandGateway`; the CLI remains an HTTP client.

## Source reference

- composition root: `src/archetype/app/container.py`
- application facade: `src/archetype/app/application/`
- gateway: `src/archetype/app/gateway/`
- durable commands: `src/archetype/app/commands/`
- world family: `src/archetype/app/world/`
- storage family: `src/archetype/app/storage/`
- query family: `src/archetype/app/query/`
- ingestion envelope and append selection: `src/archetype/app/ingestion/`
- reusable file-ingestion pipeline and scanners: `src/archetype/ingestion/`
- file artifacts: `src/archetype/app/artifacts/`
- artifact file contracts: `src/archetype/artifacts/`
- evaluation: `src/archetype/app/evaluation/`
- evaluation domain contracts and receipt schema: `src/archetype/evaluation/`
- research: `src/archetype/app/research/`
- audit: `src/archetype/app/audit/`
