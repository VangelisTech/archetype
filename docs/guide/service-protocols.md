# Internal workflow protocols

**Document type:** Normative.

**Scope:** Genuine family-owned protocols and focused,
construction-injected lower-family ports described here.

[Application Architecture](application-architecture.md) owns dependency order,
public/internal classification, wiring, and enforcement. This document owns the
purpose and active mapping of each family port.

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
migration.workflow
  -> iStorageService + storage.MigrationControlCatalog
  -> artifacts migration participant + composition-supplied cold verifier
iWorldLifecycle    -> iWorldRegistry + iStorageService
iWorldLifecycle    -> iWorldActivationOwner (private cleanup-only creation)
CommandDispatcher  -> OperationRegistry + Policy + CommandScheduler
                   -> AuditLog.record_access
CommandScheduler   -> storage control catalog
                   -> world.handlers lock-held materialization
research.handlers  -> iWorldRegistry + iWorldLifecycle + iStorageService
                   -> world simulation + exact owned-world cleanup
AuditLog           -> iStorageService + CommandScheduler outbox callbacks

```

`archetype.wiring` composes the domain-free framework, resolves manifests, and
returns `RuntimeResources`. Each world library's private `_extension.py`
adapter constructs that library's internals and registers only its declared
handlers over the bounded framework context.

## 3. Active mapping

| Port | Implementation | Principal consumers | Responsibility |
|---|---|---|---|
| `iStorageService` | `StorageService` | world, commands, artifacts, evaluation, Research, migration | Store/session lifetime, control authority, physical visibility, world/run row envelope, terminal Daft execution, app-table catalog/read/write/retry authority, and pinned table-transfer evidence |
| `iWorldRegistry` | `WorldRegistry` | lifecycle, mutation, simulation, research | Live identity, storage coordinates, exact-world synchronization, retryable close ownership, and committed-receipt retention |
| `iWorldLifecycle` | `WorldLifecycle` | framework wiring plus installer-bound Research handlers | Managed construction, durable discovery, readonly open, fenced mutable resume, fork, and close |
| `iWorldCleanup` | `WorldCleanup` | reservation-owned world cleanup | Exact-world, close-lease-bound retained updates, teardown staging, commit, and finish |
| `MigrationControlCatalog` | local `SqliteControlCatalog` | local v1 storage migration | Versioned exact control export, immutable-plan reservation, hidden staging, fence-floor import, World activation last, and receipt completion |
| `ColdMigrationVerifier` | composition-supplied fresh destination verifier | local v1 storage migration | Reopen destination-only resources and return bounded discovery, table, Artifact, resume, fence, and later-tick evidence |

### Commands-owned machinery

| Component | Principal consumers | Responsibility |
|---|---|---|
| `OperationRegistry` | `CommandDispatcher`, `CommandScheduler`, framework wiring, private world-library adapters | Exact model/name registration, handler metadata, and optional durable decoder/materializer |
| `CommandDispatcher` | runtime and API adapters | Trusted and actor-aware direct/durable entry, admission lifetime, policy order, and bounded evidence |
| `Policy` | `CommandDispatcher` | Pure role preauthorization plus instance-owned world/tick and daily-token quotas |
| `CommandScheduler` | `CommandDispatcher`, world materializer, wiring-provided destroy callback | Canonical durable admission, reservation, leasing, retry, settlement staging, cancellation, and outbox access |
| `AuditLog` | `CommandDispatcher`, registered `GetAuditHistory`, `RuntimeResources` shutdown | Bounded access rows and transactional command-outbox projection into analytical storage |

The research family deliberately has no application service port.
`archetype.research.handlers.handle_autoresearch` is a free handler closed over
the world/storage ports, exact cleanup callback, and one process-shared
`AutoResearchAdmissions` instance by the private
`archetype.research._extension` installer.
## 4. Boundary rules

### Runtime and API adapters

Framework runtime methods construct exact framework operation models and enter
`CommandDispatcher.apply()` or durable variants. Installed world-library typed
adapters construct their own models over the generic runtime handles. Both
expose boundary-safe results, never concrete services, process wiring, or live
worlds. Framework-only ergonomics and lazy handle state remain in
`archetype.runtime`.

Base API routes parse transport, authenticate `ActorCtx`, construct the same
exact framework models, and enter `CommandDispatcher.apply_as()` or durable
variants. Manifest-declared world-library router factories add only their
library's models over that same actor-aware boundary. The commands-owned
dispatcher and `Policy` perform authorization, quota admission, and bounded
evidence. Routes own no policy counters, world, command ledger, audit log,
grader, artifact ingestion, or storage.

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

The migration family likewise exposes a free administrative workflow rather
than an application service facade. It consumes `iStorageService` for namespace
enumeration and pinned table transfer, the storage-owned
`MigrationControlCatalog` for exact SQLite state transfer and activation, and
the artifacts-family migration participant for verified object relocation and
the one permitted `artifact_files.object_uri` transformation. A narrow
`ColdMigrationVerifier` callable is injected by composition so the final proof
uses fresh destination-only resources. None of these ports makes migration a
World operation, deferred command, or second storage authority. Local v1 is
offline Iceberg-to-Iceberg and SQLite-to-SQLite into an empty destination;
remote endpoints and any Activity history fail closed. See
[Storage Migration](storage-migration.md).

There is no artifact claim, lease, receipt, reconciliation protocol, generic
ingestion facade, or live-registry fallback around that path.
Providers must select and sanitize declared files before submitting
`ArtifactSource` values. Exporters consume the canonical redaction port.

AutoResearch follows the same free-handler direction without becoming a
durable workflow. The exact `AutoResearch` model carries live callbacks, so
trusted `apply` and operator-authorized `apply_as` are available while both
deferred modes reject before catalog effects. A ledgered call enters the
process-shared `autoresearch:{experiment_id}` admission; a ledgerless call
bypasses it and receives invocation-unique rollout names. The dispatcher
awaits the outer handler synchronously inside its existing process admission,
and inner world/storage work calls owning families directly. Research creates
no service facade, recursive dispatch, detached task, or second lifetime owner.

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
mirror.

The migration family owns invocation-scoped endpoint bindings plus
credential-free immutable plans, ordered orchestration, retry convergence,
cold-verification requests/evidence, and receipts. Storage owns table and
control snapshot values; artifacts owns object-inventory and relocation values.
These contracts preserve the direction `migration -> artifacts -> storage`
plus the direct `migration -> storage` edge without promoting endpoint
capabilities or credentials into persistent values.

The root policy and its `quality/architecture.d/` fragments currently carry no
migration exceptions; no wildcard compatibility package is implied.
Redaction, audit, command, world, and other authority-specific models remain
with their owning families unless a focused specification classifies an
individual value as a reusable family contract.

## 6. Construction and shutdown

`archetype.wiring.build_runtime_resources()` is the sole enclosing
process-composition transaction. It builds the domain-free framework and then
invokes the resolved private world-library installers:

```text
OperationRegistry + Policy + CommandScheduler + CommandDispatcher
WorldRegistry + WorldLifecycle + AuditLog + StorageService
framework handlers + bounded WorldLibraryContext
private library adapters + their declared exact handlers
RuntimeResources
```

Runtime retains `RuntimeResources`; API lifespan retains the same process
owner and dependency injection exposes only its dispatcher. Concrete services
and process wiring remain internal.

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
- [Storage Migration](storage-migration.md)
