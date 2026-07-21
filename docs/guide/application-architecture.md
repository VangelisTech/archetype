# Application Architecture

**Document type:** Normative.

**Scope:** Supported boundaries, application-family ownership, dependency
direction, wiring, encapsulation, trust boundaries, and the rules from which
architecture lint is derived.

Behavior-specific specifications remain authoritative for lifecycle,
durability, authorization, and execution semantics. This document owns where
those behaviors live and which dependencies may implement them.

## 1. Authority and notation

Written ownership rules and dependency tables are normative. Diagrams are
derived explanatory views; their rank, grouping, and arrow routing are not
architecture. In dependency tables, `A -> B` means **A consumes B**.

The machine-readable architecture policy encodes these rules. It may not invent
new dependencies or silently preserve implementation drift. The policy is
`quality/architecture.toml` plus per-family fragments under
`quality/architecture.d/`: each family's registration and migration exceptions
live in that family's fragment, so two family changes never edit the same
file. Fragments may declare only rule and exception arrays; version, reserved
infrastructure, and other scalar policy stay in the root file, and the checker
rejects fragments that declare anything else or duplicate a rule name.

## 2. Supported and internal boundaries

| Surface | Classification | Contract |
|---|---|---|
| `ArchetypeRuntime` and runtime world handles | Primary supported Python API | Trusted local scripting boundary |
| Components, processors, resources, configuration, and runtime result models | Supported extension/signature surface | May cross the runtime boundary where explicitly documented |
| REST and CLI behavior | Supported adapter surfaces | Preserve approved application semantics through the authorized server path |
| Concrete classes under `archetype.app` | Internal | No direct compatibility promise |
| `ServiceContainer` | Internal wiring root | Construction and ownership mechanism, not an application API |
| App-family protocols | Internal architecture ports by default | Enforce dependencies and substitutability inside the repository |
| Core engine types retained at top level | Compatibility or extension surface | Classification is owned by API Stability |

Concrete app services and `ServiceContainer` are absent from the top-level
export surface. External code receives capabilities through the runtime, REST,
or CLI boundary; it does not assemble the internal service graph.

## 3. Canonical application paths

`RuntimeApplication` is the actor-free application facade. It exposes canonical
ID-oriented product operations and delegates each workflow to its owning
family. It owns no transport, authentication, authorization, storage backend,
grader implementation, or durable queue state.

Trusted local scripts use it through the ergonomic runtime:

```text
application code
  -> ArchetypeRuntime / RuntimeWorld
  -> RuntimeApplication
  -> app-family ports
  -> core
```

Untrusted callers use an authorized server boundary:

```text
CLI or remote client
  -> REST API over HTTP
  -> authentication adapter
  -> CommandGateway(ActorCtx)
  -> RuntimeApplication
  -> app-family ports
  -> core
```

The CLI is an HTTP client except for the server-startup entrypoint. It does not
authorize calls or import application/runtime implementation code. FastAPI
translates transport models and authenticates principals. `CommandGateway`
authorizes; it does not translate HTTP or implement domain workflows.

Other untrusted ingress, including MCP tools, sandboxed agents, or multi-tenant
embeddings, uses the same gateway even when HTTP is not involved.

The concrete `ArchetypeRuntime` is not a dependency of `archetype.app`.
`RuntimeApplication` is the lower actor-free seam shared by runtime and gateway,
so scripting-only handles, sync wrappers, callbacks, and local lifetime do not
leak into the server.

## 4. Package direction and family layout

Repository package ownership is normative:

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, and reusable projections | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Durable authority, cross-family orchestration, internal service ports, and concrete application services | `archetype.app.<family>` |
| Transport, authentication, application facade, and composition | `archetype.api`, `archetype.app.gateway`, `archetype.app.application`, and `archetype.app.container` |

A top-level domain-family package owns reusable ECS state and pure domain
behavior. It may depend on `archetype.core`, itself, third-party libraries, and
only reviewed lower top-level family contracts declared in the merged
architecture policy. It must not import `archetype.app`,
`archetype.runtime`, `archetype.api`, or `archetype.cli`, and it does not
configure process-global providers or exporters, storage backends, process
hosts, or the service container. The application layer may import a registered top-level
family contract; the reverse edge is forbidden. Undeclared top-level
family-to-family dependencies are denied. Every first-party package or module
directly beneath `archetype` is classified as reserved infrastructure or
registered as a domain family with one exact dependency disposition.
Unclassified scopes fail the architecture audit, and the complete registered
family graph must be acyclic. Imports through the `archetype` root facade are
resolved to the module that owns the exported package or symbol before these
rules are applied. If its static export map cannot be parsed exactly, the audit
fails rather than degrading root-facade enforcement.

A reviewed family may own a capability-scoped `Resource` implementation or
provider adapter when the protocol and lifecycle vocabulary belong to that
family. It must not become process-global configuration or cross-family
authority. `archetype.missions.sandboxes` is the concrete example: it executes
mission requests, while the app workflow owns composition and the processors
own transitions.

Naming states semantic ownership:

- `components.py` contains `Component` subclasses and component-local
  construction helpers;
- `processors.py` contains processor implementations;
- `contracts.py` contains supported Pydantic or dataclass value contracts;
- `interfaces.py` contains internal application ports;
- `transitions.py` contains pure typed transition graphs; and
- `service.py` contains application authority or orchestration.

A `Component` is persistent ECS schema even though its implementation uses
Pydantic. It is not an application DTO and does not belong anywhere under
`archetype.app`. Conversely, a top-level path does not automatically make a
symbol public. Supported names remain an explicit classification owned by
[API Stability](api-stability.md); concrete services and `ServiceContainer`
remain internal.

The `archetype.missions` family consumes the lower `archetype.graph` family,
as declared in `quality/architecture.d/missions.toml`. It owns mission/task Components,
typed authoring and execution values, task relationships, DataFrame-first
transition processors, reusable projections/resources, and coding-agent
sandbox implementations. Sandboxes are mission-family resources; they are not
peer application authorities. The family never imports app, runtime, API, or
CLI code.

`archetype.app.missions` owns durable mission composition and orchestration.
For Agent Missions V1 that means graph materialization, tick/external-I/O
coordination, observation staging, and result projection. Family-package
exports are deliberate and do not promote a concrete application service to
the `archetype` root.

The application-authority layout is:

```text
src/archetype/app/
  application/       RuntimeApplication, its port, and boundary-safe models
  world/             world lifecycle, mutation, and simulation
  storage/           stores, control authority, Daft execution and app tables
  query/             persisted ECS read paths
  ingestion/         world/run envelopes and append-operation selection
  artifacts/         file discovery, immutable object storage and media indexes
  redaction/         pre-durability secret scanning, receipts and quarantine
  evaluation/        grading orchestration, snapshot pinning and receipt writes
  commands/          durable ledger, scheduling, dispatch, settlement
  gateway/           authorization policy boundary
  audit/             journals, outboxes, projections
  research/          autoresearch and multi-run research workflows
  missions/          mission graph and external-I/O composition
  physical_ai/       batched evaluation and instruction-sweep workflow
  errors.py          cross-family application error contracts
  container.py       sole concrete cross-family wiring root
```

The mission-adjacent cleanup direction is recorded in
[Agent Missions V1, section 9](agent-missions.md#9-family-direction-after-v1).
Dataset evidence identity has moved into evaluation and the datasets umbrella
is gone. HTN resolution now lives under `archetype.missions.planning`. The
physical-AI Components, processors, policy contracts, and external-boundary
helpers now live in the registered `archetype.physical_ai` family. Research
ledger Components and the pure runner decoder live in `archetype.research`,
while `archetype.app.research` retains workflow authority. Typed trajectory
schemas and pure transforms live under `archetype.missions.trajectories`; the
mission trajectory service composes query and evaluation ports. Physical
evaluation values and pure instruction optimization live under
`archetype.physical_ai`, while `archetype.app.physical_ai` composes the world,
mutation, simulation, evaluation, and storage ports. Claude transcript parsing
now lives under `archetype.missions.trajectories`;
`archetype.app.artifacts` owns its redact-before-durability workflow. The former
production
`archetype.experiments` umbrella is gone. The repository-root `experiments/`
directory remains a consumer-side harness, not a package or authority family.

Every application family co-locates its internal protocols, boundary models,
and authority implementation. It imports reusable domain values from their
top-level family once those values have moved. A generic `services/` bucket
and a monolithic `app/interfaces.py` are prohibited; the architecture checker
rejects edges that recreate them.

The allowed outer-package direction is:

```text
application code -> archetype.runtime
CLI              -> REST API over HTTP
runtime          -> app.application contracts and safe models
API              -> app.gateway contracts, safe models, and app errors
gateway          -> app.application port plus auth/audit ports
app families     -> top-level family contracts, approved app-family ports, core
top-level family -> core and explicitly declared lower top-level families
core             -> foundation and third-party libraries only
```

Forbidden reverse edges include:

- core importing app, runtime, API, CLI, or registered domain families;
- a top-level family importing app, runtime, API, or CLI;
- a top-level family importing another registered family without a declared
  lower-family edge;
- app importing runtime, API, or CLI;
- runtime importing gateway, auth, concrete app services, or API;
- API routes importing `RuntimeApplication` or concrete app services;
- CLI command implementations bypassing HTTP.

The proposed graph-family design in
[PR #545](https://github.com/VangelisTech/archetype/pull/545) is a supporting
design record, not a competing normative source. Its graph and projection
stages ([#546](https://github.com/VangelisTech/archetype/issues/546) and
[#547](https://github.com/VangelisTech/archetype/issues/547)) consume this
package policy and register any reviewed family edge here before importing it.

### Observability dependency boundary

Core and application families may emit only through the private,
vendor-neutral `archetype._obs` API and stdlib logging. They may not import an
OTel SDK, exporter, collector, or vendor package. Runtime and server process
hosts own provider/exporter and handler configuration. Module import and API
factory construction are not host boundaries; trusted runtime construction,
CLI server startup, and worker lifespan are. The host adapter may attach its
filter only to its own package handler and may not replace the global
`LogRecordFactory` or mutate root logging. Because core imports `_obs`, the
signal boundary cannot import the application redaction family; it uses the
closed schema defined by the normative
[Observability contract](observability.md). Content-bearing outer adapters
still consume the redaction port before their own durable or external write.

Telemetry is never an authority edge. It cannot authorize a command, prove a
commit, settle a durable outcome, or choose a retry. The owning family's typed
result and durable record remain authoritative when telemetry is disabled,
dropped, or failing.

Every callable member of every application-family `Protocol`, wherever that
protocol is declared inside the family package, has one exact observation
disposition in `quality/observability/<family>.toml`. The manifest records
intent without turning telemetry into a dependency or authority edge. See
[Observability](observability.md#6-family-dispositions) for the complete
manifest and root/child policy.

## 5. Core world and application-family ownership

`StorageService` resolves and pools an `iAsyncStore`. It is also the sole
application authority for terminal Daft execution and app-owned table
registration, schema alignment, reads, writes, and optimistic-commit retry.
`WorldService` owns the world factory and live registry. A world composes one
querier, updater, system, resource registry, and hook registry. The querier and
updater consume the same store. A live world is an internal capability and
never crosses an application, gateway, runtime, API, or CLI boundary.

| Consumer/family | Responsibility | Allowed app dependencies |
|---|---|---|
| Storage | Store and session lifetime; control authority; terminal Daft execution; app-table registration, schema, read/write, and retry | None |
| World lifecycle | Create, lookup, fork, resume, destroy, live registry | Storage port |
| Mutation | Mutate a resolved live world | World port |
| Simulation | Step, run, episode, rollout, and bounded terminal-condition reduction | World and storage ports plus named command-drain and quota-reset callables |
| Query | Persisted ECS reads, durable discovery, and compatibility history reads | Storage and audit ports |
| Redaction | Provider-neutral secret scanning, deterministic text redaction, safe receipts and quarantine | None |
| Ingestion | Add world/run identity and select plain or logical-key-conditional append | Storage and world-coordinate ports |
| Artifacts | File discovery, metadata scans, immutable content-addressed objects, and common/media indexes | Ingestion, storage and world-coordinate ports |
| Evaluation | Snapshot pinning, grader contracts, grading, evidence and durable results | Query, ingestion, storage and world-coordinate ports |
| Commands | Durable admission, order, leasing, dispatch, retry, settlement and dead letters | Control catalog plus world and mutation ports |
| Audit | Transactional journal/outbox and analytical projection | Storage or control-authority ports |
| Research | Multi-run research workflows and bounded persisted-control reads | World, simulation, and storage ports plus explicit evaluator callbacks |
| Physical AI | Batched evaluation and instruction-sweep workflows with typed terminal reports | World, mutation, simulation, evaluation, and storage ports |
| Missions | Graph materialization, tick/external-I/O composition, terminal projection, transcript ingestion, and trajectory query/evaluation composition. Family processors retain transition authority; trajectory evidence cannot advance tasks. | Consumes a structural mission world, family-owned sandbox resource, artifact/ingestion/redaction ports for transcripts, and query/evaluation ports for trajectory reads. |
| RuntimeApplication | Canonical actor-free application facade and per-world operation serialization | Approved family workflow ports only |
| CommandGateway | Authorization, safe downgrade, access-audit notification, delegation | RuntimeApplication port, authorizer, audit-journal port |
| ServiceContainer | Concrete construction, ownership, and callback wiring | Every concrete implementation it constructs |

Mutation and simulation are siblings over WorldService. Query intentionally
reads storage without requiring a live world. Evaluation owns the product
evaluation transaction; the gateway never pins snapshots, invokes graders, or
persists evaluation receipts.

## 6. Gateway and trust-boundary policy

The authorized boundary names are `CommandGateway` and `iCommandGateway`.

The gateway:

1. accepts an authenticated `ActorCtx`;
2. authorizes the requested operation and applicable resource/cost effects;
3. delegates to `RuntimeApplication`;
4. emits access-decision evidence through the audit port; and
5. returns a boundary-safe result.

The gateway owns no worlds, services, command queue, grading workflow,
ingestion transaction, durable result, or audit storage. It is stateless policy
machinery over injected ports.

`ActorCtx` does not cross below the gateway. When an admitted operation needs
durable provenance, the gateway converts the principal into an immutable
application-owned admission record. Trusted local operations use an explicit
local origin rather than fabricating an admin authorization event.

Authentication belongs to the ingress adapter. Authorization belongs to the
gateway. The CLI merely transports credentials.

## 7. Commands, commits, artifacts, and audit

Durability is family-specific rather than one service-level flag:

| Boundary | Authority | Commit condition |
|---|---|---|
| Deferred command admission | Command ledger | `PENDING` record, order, payload version and principal/origin are durable |
| Tick | Store plus commit coordinator | All tick rows are durable and the visibility manifest is published |
| Deferred command outcome | Commit coordinator plus command ledger | Terminal applied outcomes settle atomically with the manifest that makes them visible |
| Agent Mission dispatch | Mission world tick plus post-tick outbox | A `dispatched` task row is durably visible before any sandbox request leaves the world |
| Agent Mission acceptance | Mission processors plus world tick | Revision-bound validation, execution, and pushed-commit observations are staged as data; the next task-decision tick accepts, retries, or exhausts the task |
| Typed ingestion | `IngestionService` policy plus `StorageService` and Iceberg | The world/run envelope is fixed, the registered schema accepts the rows, and one Iceberg append makes the selected rows visible |
| Artifact ingestion | `ArtifactService` plus `IngestionService` | The immutable object and any media-specific rows are durable before the common `artifact_files` occurrence becomes visible |
| Coding-agent transcript | Redaction, artifact, and typed-ingestion authorities | Raw narrative never becomes durable; the sanitized artifact is indexed before normalized rows keyed to its `artifact_id` are appended |
| Evaluation | Evaluation workflow plus `IngestionService` | Subject and grader contract are pinned and the typed evaluation result is appended |
| Audit | Transactional outbox plus projection | Authoritative event is durable; analytical Iceberg projection may lag |

The store/updater owns physical tick append and flush. `StorageService` owns
the application execution lane and the physical app-table operation. The
owning workflow still defines the logical unit, and a coordinator publishes
tick visibility only after physical durability. `StorageService` does not
decide what a tick, artifact, evaluation, or command outcome means.

The durable scheduler/dispatcher belongs to the commands family, not to the gateway. Both
trusted runtime operations and authorized remote admission may use it. The
gateway authorizes remote admission and delegates; simulation invokes a named
commands-family drain callback at the tick boundary.

`IngestionService` owns the world/run envelope and chooses between a plain
append and a caller-keyed conditional append. `StorageService` owns
`daft.Catalog` registration, schema comparison, execution, Iceberg writes, and
conflict retry. `ArtifactService` specializes the ingestion path for files and
media metadata. Mission-owned `TranscriptIngestionService` composes those
ports with redaction and the pure missions parser; it creates no third storage
authority.
Durable external material is described as an artifact, evidence object, typed
dataset row, or evaluation receipt—never as a universal fact.

### Storage execution authority

Archetype-owned terminal Daft work in the application layer MUST enter through
`iStorageService`. `materialize()` admits a lazy plan and returns its completed
frame. `read_table()` returns a lazy app-table read; `append_table()` and
`append_missing()` own registration, schema alignment, materialization, and
Iceberg commit retry. Other application families may build lazy DataFrame
plans, but they MUST NOT call Daft collection, Iceberg read/write, or catalog
table-creation primitives directly. A bounded conversion to Python control
state may call `to_pylist()` only on a frame first returned by
`iStorageService.materialize()`.

The execution gate is reentrant within one task so a cached append can flush
through the same authority. It coordinates local Daft submissions; it is not a
second distributed transaction protocol. Iceberg remains authoritative for
atomic table snapshots and optimistic concurrency. On a conditional-append
conflict, storage refreshes the table and recomputes the anti-join before
retrying so stale pending rows cannot duplicate an already-committed logical
key.

The durable control plane is separate from that data plane. The local SQLite
`ControlCatalog`, or its remote Durable Object implementation, owns world
identity, writer fences, visibility manifests, deferred commands, and narrow
workflow leases. Daft Catalog and Iceberg own table metadata, snapshots, and
data files. Storage composes both authorities without treating either one as a
replacement for the other.

## 8. Protocol policy and wiring

An active protocol must have:

- at least one named consumer;
- at least one named implementation;
- the complete method surface used by those consumers;
- static implementation conformance; and
- a negative architecture fixture proving a forbidden edge is rejected.

Protocols are co-located with their owning family. Concrete constructors depend
on a protocol when substitution is intentional. Prefer a narrow callable or
data port for one interaction. Unused or incomplete protocols are completed,
narrowed, or removed.

`app/container.py` is the only app module allowed to import concrete
implementations across families. It constructs both outward application paths:

```text
container.application -> RuntimeApplication
container.command_gateway -> CommandGateway
container -> RuntimeApplication.agent_mission_service -> iMissionService
```

Runtime and API lifespan code may construct or receive the internal container,
but ordinary runtime and route modules consume only their approved port.

For Agent Missions V1, the container injects the concrete `MissionService`
factory into `RuntimeApplication`. `RuntimeMissions` supplies a runtime-owned
world factory and supported mission configuration, then consumes only the
returned `iMissionService` port. The app service installs the built-in
processor/resource bundle and owns mission-world lifecycle; the runtime handle
does not import or construct a concrete app service.

Concrete services compose collaborators and never inherit another concrete
service. Intentional inheritance is limited to components, processors,
hook/event contracts, protocols/abstract extension contracts, and the
application error taxonomy unless the architecture policy records another
reviewed family.

## 9. Runtime callbacks and cycles

Object wiring may contain named callbacks without creating reverse static
imports. Simulation currently consumes command-drain and quota-reset callables.
The container injects them; SimulationService does not import the concrete
commands dispatcher, gateway, or auth implementation.

Every callback cycle requires a named port, owning wiring root, defined ordering
and failure behavior, and an explicit architecture-policy entry.

## 10. Supported durability profile

The supported runtime and server paths always use coordinated worlds. A core
world constructed without a commit coordinator is an explicitly ephemeral,
internal extension mode with no atomic-visibility or recovery guarantee. It is
not a coequal public execution profile.

A tick is a compute/commit/observation boundary, not a generic workflow-state
transition. Mission, coding-agent, and physical-workflow transition policy
belongs to the workflow and its validators.

## 11. Static enforcement

Together, the repository's architecture and observability checkers must:

- cover every declared source scope and fail if a required scope is empty;
- reject missing, stale, duplicate, or empty top-level family registrations;
- reject any first-party top-level package or module that lacks an explicit
  reserved-infrastructure or registered-family classification;
- require one exact cross-family dependency disposition for every registered
  top-level family;
- reject cycles in the complete registered top-level family graph;
- derive core's ban on domain-family imports from that registry, so registering
  a family cannot bypass the reverse-dependency rule;
- reject top-level-family imports of app, runtime, API, or CLI and reject
  undeclared top-level family-to-family imports;
- resolve root-facade package and symbol imports to their owning module before
  enforcing package direction;
- fail closed when the root-facade export map is missing a valid static
  disposition for any declared entry;
- allow application authority to consume registered top-level family
  contracts without treating that path as public-API promotion;
- reject direct `Component` subclasses anywhere under `archetype.app`;
- enforce the existing outer-package and application-family dependency rules;
- confine `ActorCtx` to gateway/auth code and approved adapter construction;
- restrict concrete cross-family construction to `container.py`;
- reject concrete-service inheritance;
- reserve application-owned terminal Daft, Iceberg, and catalog-table
  operations to `StorageService`, while allowing only storage-materialized
  frames to cross into bounded Python control flow;
- reject live-world, container, backend-client, and concrete-service leaks;
- verify active protocol consumer/implementation mappings;
- confine provider/exporter and logging configuration to explicit process-host
  callables and require one exact observation disposition for every callable
  application-family protocol member;
- support only exact, issue-owned migration exceptions with release deadlines
  and objective expiry conditions; wildcard package exceptions are invalid;
- report the forbidden edge, governing rule, and supported alternative.

Representative invalid fixtures prove every rule fires. Passing the current
repository without rejection tests is not an executable architecture contract.

## 12. Current enforcement state

The family packages, actor-free application facade, authorized gateway,
durable command scheduler, local/remote control authority, artifact and
evaluation ownership, and co-located protocols are implemented. Runtime calls
do not fabricate `ActorCtx`; API routes depend on `iCommandGateway`; concrete
services and the container are not top-level exports.

Agent Missions V1 is implemented under `archetype.missions`,
`archetype.app.missions.service`, and `archetype.runtime.missions`. The
top-level mission-family edge to `archetype.graph` is machine-declared and
supports temporal `DependsOn` and `PartOfMission` entities plus previous-tick
`GraphView` joins. Coding-agent and sandbox implementations remain subordinate
resources within the mission family.

`quality/architecture.toml` contains the scalar policy and application-family
DAG. Per-family fragments under `quality/architecture.d/` register the
top-level dispositions for `artifacts`, `evaluation`, `graph`, `missions`,
`physical_ai`, `projections`, and `research`.
`scripts/check_architecture.py` enforces their package direction, protocol
imports, concrete construction, concrete inheritance, and persistent
Component placement.

The ingestion/artifact split is complete. `archetype.ingestion` owns the
reusable `FileIngestionPipeline` and its pure bounded scanners.
`archetype.app.ingestion.IngestionService` owns world/run enrichment and
append-operation selection; `StorageService` owns app-table catalog and
execution authority. `archetype.artifacts` owns `ArtifactSource`, `ArtifactRef`,
and `ArtifactStoreConfig`; `archetype.app.artifacts` retains the single
file-ingestion workflow and object-storage authority.

The evaluation relocation (#557) is complete: `EvalReceipt` lives in
`archetype.evaluation.components`, the grading value contracts and identity
digests live in `archetype.evaluation.contracts`, and
`archetype.app.evaluation` retains orchestration and receipt-write authority
while importing those domain definitions inward.

The research, trajectory, physical-AI, physical-workflow, ontology, HTN, and
transcript stages have landed. The physical workflow is reachable only through
`RuntimeApplication` and `ArchetypeRuntime`; its former raw-service bridges and
all six Issue #589 architecture exceptions are gone. Transcript ingestion is
reachable through the runtime and writes only sanitized narrative to typed
rows linked to the common artifact occurrence. It does not implicitly spawn
mission Components. The provisional `archetype.experiments` package and its two unsafe
logging exceptions are gone. The architecture manifest currently has no owned
migration exceptions.

Independent manifests under `quality/observability/` declare each family's
operation dispositions. `scripts/check_observability.py` enforces their exact
coverage, source-backed positive signal claims, and the vendor-neutral
signal/configuration boundary without a live collector. It validates root
syntax and exclusivity but does not invent call graphs or runtime topology: the
three existing gateway decorators remain children, and Issue #515 owns
coherent ingress roots. The existing footgun reviewer complements this
deterministic audit with semantic observability review.

Other deliberately retained implementation seams are documented rather than
hidden: `QueryService` uses `iAuditLog` for compatibility history reads, and
the root `app/models.py` holds cross-family boundary models. Changing either is
a separate contract/model-ownership decision, not undocumented drift.

## 13. Change discipline

Architecture changes update this normative document, the machine policy, its
negative fixtures, affected family protocol tests, and contract registry in
one change. Diagrams and generated reference pages follow those authorities;
they never create dependency rules by themselves.
