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
                         +-> WorldRegistry + WorldLifecycle
                         +-> world.mutation / world.simulation / world.query
                         +-> IngestionService -> storage/world ports
                         +-> ArtifactService -> ingestion/storage/world ports
                         +-> EvaluationService -> ingestion/storage/world ports
                         +-> AutoResearchService -> storage/world ports
                         +-> PhysicalAIService -> evaluation/storage/world ports
                         +-> CommandScheduler -> world.handlers.materialize_locked
                         +-> AuditLog -> StorageService
```

The container injects `CommandScheduler.materialize` when lifecycle constructs
each managed `AsyncWorld`. Core owns only the callable shape; it never imports
the commands family. The container also connects the scheduler's transactional
outbox to `AuditLog`'s analytical projection.

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

## Ingestion and artifact families

`IngestionService` selects the live storage configuration and delegates typed
publication. `StorageService` resolves and stamps the durable world/run
envelope, selects plain or caller-keyed conditional append, and owns table
registration, schema comparison, Daft execution, and the Iceberg commit. The
ingestion service does not know whether its rows describe files, transcripts,
evaluations, or a future tabular source.

`ArtifactService` is the one file-specialized workflow. It scans declared
sources, persists content-addressed objects, writes typed media metadata, and
publishes the common artifact index last. It composes `IngestionService`; it
does not add a claim, lease, receipt, or reconciliation state machine. See
[Artifacts](artifacts.md).

## Evaluation and research families

`EvaluationService` pins persisted world state through storage and world-query authority,
executes caller-provided graders, validates typed outcomes, and appends one
durable evaluation result through `iIngestionService`.

`AutoResearchService` owns the multi-iteration rollout workflow and its durable
research ledger. It depends on world registry/lifecycle and storage ports and
calls world simulation functions. The container injects the application-owned
world teardown callback so rollout forks follow committed-work reconciliation,
durable command cancellation, then lifecycle close. Scoring remains an explicit
callback contract.

`PhysicalAIService` composes world registry/lifecycle, evaluation, and storage
ports and calls world mutation/simulation functions. It uses storage to
materialize the bounded terminal projection from which it builds a typed
report; the report is not a second state authority.

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

## RuntimeApplication

`RuntimeApplication` is the temporary actor-free application facade. It
constructs exact family models and enters trusted `CommandDispatcher` methods
while delegating remaining staged workflows to their family ports. It does not
own policy, queue state, or durable state.

Trusted local runtime calls terminate here. No `ActorCtx` is invented for local
scripting.

## CommandGateway

`CommandGateway` is the temporary transport-shaped adapter for untrusted
callers. It accepts `ActorCtx`, constructs exact family models, and enters the
actor-aware `CommandDispatcher`. The commands-owned `Policy` and dispatcher
perform RBAC, quota, admission, and bounded access evidence. The gateway does
not own those mechanisms or persistence.

FastAPI consumes `iCommandGateway`; the CLI remains an HTTP client.

## Source reference

- composition root: `src/archetype/app/container.py`
- application facade: `src/archetype/app/application/`
- gateway: `src/archetype/app/gateway/`
- governed and durable commands: `src/archetype/commands/`
- world family: `src/archetype/world/`
- physical storage family: `src/archetype/storage/`
- typed-publication routing: `src/archetype/app/ingestion/`
- reusable file-ingestion pipeline and scanners: `src/archetype/ingestion/`
- file artifacts: `src/archetype/app/artifacts/`
- artifact file contracts: `src/archetype/artifacts/`
- evaluation: `src/archetype/app/evaluation/`
- evaluation domain contracts and receipt schema: `src/archetype/evaluation/`
- research: `src/archetype/app/research/`
- command/access audit projection: `src/archetype/commands/audit.py`
