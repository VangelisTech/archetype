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
| Concrete family services | Internal | No direct compatibility promise |
| `archetype.wiring` | Internal composition root | Sole concrete cross-family construction transaction |
| `RuntimeResources` | Internal process owner | Admission drain, supervised work, handle lifetime, audit, and storage teardown |
| Core engine types retained at top level | Compatibility or extension surface | Classification is owned by API Stability |

Concrete family services, `RuntimeResources`, and wiring helpers are absent from
the top-level export surface. External code receives capabilities through the
runtime, REST, or CLI boundary; it does not assemble the internal graph.

## 3. Canonical application paths

Trusted local scripts enter through the ergonomic runtime:

```text
application code
  -> ArchetypeRuntime / RuntimeWorld
  -> CommandDispatcher.apply / defer
  -> registered family handler
```

Untrusted callers use an authorized server boundary:

```text
CLI or remote client
  -> REST API over HTTP
  -> authentication adapter
  -> ActorCtx
  -> CommandDispatcher.apply_as / defer_as
  -> the same registered family handler
```

The CLI is an HTTP client except for the server-startup entrypoint. It does not
authorize calls or import application/runtime implementation code. FastAPI
translates transport models, authenticates principals, constructs exact
operations, and delegates actor-aware entry. It does not own authorization
policy or implement domain workflows.

Other untrusted ingress, including MCP tools, sandboxed agents, or multi-tenant
embeddings, must authenticate an `ActorCtx` and use the same actor-aware
dispatcher methods even when HTTP is not involved.

The concrete `ArchetypeRuntime` is not a dependency of domain families.
Runtime and API are parallel trusted and actor-aware adapters over the same
commands dispatcher and family models. Scripting-only handles, sync wrappers,
callbacks, and local lifetime therefore do not leak into the server.

## 4. Package direction and family layout

Repository package ownership is normative:

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, reusable projections, and family-owned free workflows over declared lower-family ports | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Generic Activity identity, claims, attempts, fences, result references, and settlement | `archetype.activities` |
| Physical storage, control catalogs, commit coordination, and generic durable world/run envelopes | `archetype.storage` |
| Family-owned workflows and internal lower-family ports | `archetype.<family>` |
| Transport and authentication | `archetype.api` |
| Process composition and lifetime | `archetype.wiring` and `archetype.runtime_resources` |

A top-level domain-family package owns reusable ECS state and pure domain
behavior. It may depend on `archetype.core`, itself, third-party libraries, and
only reviewed lower top-level family contracts declared in the merged
architecture policy. It must not import `archetype.app`,
`archetype.runtime`, `archetype.runtime_resources`, `archetype.wiring`,
`archetype.api`, or `archetype.cli`, and it does not configure process-global
providers or exporters, storage backends, process hosts, or wiring.
Undeclared top-level family-to-family dependencies are denied. Every
first-party package or module
directly beneath `archetype` is classified as reserved infrastructure or
registered as a domain family with one exact dependency disposition.
Unclassified scopes fail the architecture audit, and the complete registered
family graph must be acyclic. Imports through the `archetype` root facade are
resolved to the module that owns the exported package or symbol before these
rules are applied. If its static export map cannot be parsed exactly, the audit
fails rather than degrading root-facade enforcement.

`archetype.storage` is the reviewed physical-substrate family. It owns storage
execution, control-catalog implementations and records, physical visibility,
commit coordination, and the generic durable world/run envelope. Application
families consume that substrate through the staged `iStorageService` port and
retain workflow meaning and orchestration.

A reviewed family may own a capability-scoped `Resource` implementation or
provider adapter when the protocol and lifecycle vocabulary belong to that
family. It must not become process-global configuration or cross-family
authority. `archetype.missions.sandboxes` is the concrete example: it executes
mission requests, while the app workflow owns composition and the processors
own transitions.

A Resource is tick-time capability access whose process-local lifetime is not
durable workflow truth. An Activity coordinates work admitted from one
committed tick and observed by a later committed tick. The accepted
`archetype.activities` family owns only generic delivery mechanics and consumes
the lower physical catalog owned by `archetype.storage`. Provider-specific
recovery meaning stays in the owning family or adapter; application families
own intent projection, execution choreography, and observation staging. See
[Activities](activities.md).

Naming states semantic ownership:

- `components.py` contains `Component` subclasses and component-local
  construction helpers;
- `processors.py` contains processor implementations;
- `contracts.py` contains supported Pydantic or dataclass value contracts;
- `models.py` contains supported values and exact operation models when a
  family has one canonical model surface;
- `views.py` contains reusable storage-backed projections;
- `handlers.py` contains family-owned free workflows over declared lower-family
  ports;
- `interfaces.py` contains family-owned ports;
- `transitions.py` contains pure typed transition graphs; and
- `service.py` contains family-owned workflow authority or orchestration.

A `Component` is persistent ECS schema even though its implementation uses
Pydantic. It is not an application DTO and belongs to its named family.
Conversely, a top-level path does not automatically make a
symbol public. Supported names remain an explicit classification owned by
[API Stability](api-stability.md); concrete services and process wiring remain
internal.

The `archetype.missions` family consumes the lower `archetype.graph` family,
as declared in `quality/architecture.d/missions.toml`. It owns mission/task Components,
typed authoring and execution values, task relationships, DataFrame-first
transition processors, reusable projections/resources, and coding-agent
sandbox implementations. It also owns graph materialization,
tick/external-I/O coordination through one author-and-critic Activity binding,
observation staging, and result projection. Sandboxes are mission-family
resources; they are not peer authorities. The family never imports runtime,
API, CLI, or composition code. Family-package
exports are deliberate and do not promote a concrete application service to
the `archetype` root.

The current authority layout is:

```text
src/archetype/
  errors.py          stable shared boundary-error bases
  storage/           physical rows, catalogs, commits, scans and app tables
  world/             lifecycle, mutation, simulation, query and exact operation models
  commands/          registry, policy, dispatch, durable scheduling and audit projection
  redaction/         canonical pre-durability scanning, receipts and quarantine
  evaluation/        grading, snapshot pinning, leases and durable receipts
  artifacts/         file values, scans, immutable objects, indexes, views and handlers
  research/          AutoResearch values, ledger state, views and free workflow handler
  physical_ai/       physical state, models, views and free workflow handlers
  missions/          mission state, resources, Activities and family workflows
  runtime_resources.py explicit process-lifetime owner
  wiring.py          sole concrete cross-family composition root
```

The Activity migration adds `activities/` as a top-level family over the
storage-owned Activity catalog. Hosted whole-episode choreography is owned by
`archetype.physical_ai`; no application mirror is created.

The mission-adjacent cleanup direction is recorded in
[Agent Missions V1, section 9](agent-missions.md#9-family-direction-after-v1).
Dataset evidence identity has moved into evaluation and the datasets umbrella
is gone. HTN resolution now lives under `archetype.missions.planning`. The
physical-AI Components, internal provider processors, genuine protocols,
models, views, free handlers, and external-boundary helpers now live in the registered
`archetype.physical_ai` family. Research
values, ledger Components, views, pure runner decoder, experiment admission,
and the directly awaited workflow handler live in `archetype.research`; there
is no application research facade or service protocol. Typed trajectory
schemas and pure transforms live under `archetype.missions.trajectories`; the
mission trajectory service composes world-query functions with the evaluation
family's pure grader runner. Physical evaluation values, provider protocols,
pure instruction optimization, terminal views, and the current direct
world/storage workflow handlers all live under `archetype.physical_ai`.
Hosted Activity handling retains those reusable contracts and keeps
intent-to-Activity-to-observation choreography in `archetype.physical_ai`
without a single-implementation facade protocol. Claude transcript parsing
now lives under `archetype.missions.trajectories`;
`archetype.missions.transcript_service` owns its
redact-before-durability workflow and consumes the artifacts family directly.

Physical environment and policy providers transfer synchronously into
`RuntimeResources`, and an identity-ordered exclusive lease covers every
provider-backed workflow effect. Before releasing that lease, the handler must
retire its live world writer through world lifecycle authority. The returned
world/run coordinates remain durable read evidence; they do not authorize a
later attach/step path to the internal provider processors.
Composition coalesces retirement by the exact sticky world-cleanup lease and
registers that complete transaction with `RuntimeResources`. A failed
retirement remains process-owned for the `workflow-handles` retry phase. Each
associated provider close first joins those exact retirements, so only
successful world cleanup can release the cleanup owner and permit provider
shutdown. Before a private cleanup-only world can issue its first catalog
registration, world lifecycle synchronously binds the exact catalog and
`WorldRecord` to the pre-reserved workflow owner. Registry insertion then
promotes that same owner, without an intervening await, to the canonical sticky
cleanup lease. Ambiguous registration cleanup, canonical world cleanup, and
their retries therefore remain one process-owned transaction; no handler may
bypass it with direct lifecycle destruction.
The former
production
`archetype.experiments` umbrella is gone. The repository-root `experiments/`
directory remains a consumer-side harness, not a package or authority family.

Every family co-locates its genuine protocols, boundary models, and workflow
implementation. A generic `services/` bucket and a monolithic interface module
are prohibited; the architecture checker rejects edges that recreate them.

The allowed outer-package direction is:

```text
application code -> archetype.runtime
CLI              -> REST API over HTTP
runtime          -> commands dispatcher plus family models/contracts
API              -> commands dispatcher, auth models, family models, shared errors
commands         -> storage and world
world            -> storage
top-level family -> core and explicitly declared lower top-level families
core             -> foundation and third-party libraries only
wiring           -> every concrete implementation it composes
```

Forbidden reverse edges include:

- core importing app, runtime, API, CLI, or registered domain families;
- a top-level family importing app, runtime, API, or CLI;
- a top-level family importing another registered family without a declared
  lower-family edge;
- runtime importing API auth, concrete family services, or API modules;
- API routes importing concrete family services or process wiring;
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

`StorageService` resolves and pools an `iAsyncStore`. It is the canonical
physical authority for terminal Daft execution and app-owned table
registration, schema alignment, reads, writes, and optimistic-commit retry.
`WorldRegistry` owns live world identity, storage coordinates, exact-world
locks, close leases, required-projector bindings, and retained committed
receipts. `WorldLifecycle` owns construction, discovery, resume, fork, and
close. The module-level world mutation, simulation, query, and handler
operations are stateless behavior over those owners. A live world is an
internal capability and never crosses an application-service, runtime, API,
or CLI boundary.

| Consumer/family | Responsibility | Allowed dependencies |
|---|---|---|
| Storage | Store and session lifetime; control authority; physical visibility; commit coordination; generic world/run envelope; terminal Daft execution; app-table registration, schema, read/write, and retry | None |
| World registry/lifecycle | Live ownership, exact-world synchronization, create, discovery, readonly open, fenced resume, fork, and retryable close | Storage port |
| World mutation | Module functions that mutate a world under its exact registry lease | World-registry port |
| World simulation | Module functions for step, stable committed receipts, required projection, run, episode, and rollout | World-registry and storage ports; construction-injected command materializer |
| Durable world reads | Module functions for persisted ECS state, lineage, and signature discovery without a live world | Storage port |
| Redaction | Provider-neutral secret scanning, deterministic text redaction, safe receipts and quarantine | None |
| Artifacts | File values, discovery, metadata scans, immutable content-addressed objects, common/media indexes, storage-backed views, and exact free handlers | Storage port; operations carry explicit durable world and storage coordinates |
| Evaluation | Snapshot pinning, grader contracts, grading, leasing, recovery, evidence and durable results | Storage port plus world-query functions; operations carry explicit world and storage coordinates |
| Commands | Exact registration, authorization policy, governed direct/deferred entry, durable admission, order, leasing, lock-held materialization, retry, settlement, dead letters, transactional outbox and analytical audit projection | Storage/control catalog plus exact world handlers |
| Activities *(accepted target)* | Generic immutable admission, claims, attempts, leases, fences, provider-operation binding, bounded result references/digests, and later-receipt settlement; no family recovery policy | Storage-owned Activity catalog |
| Research | AutoResearch values, ledger state, bounded persisted-control reads, experiment-keyed admission, and the directly awaited multi-run workflow | World registry/lifecycle and storage ports plus world simulation functions and explicit evaluator callbacks |
| Physical AI | Reusable physical state, schemas, providers, views, and current direct free workflows; hosted Activity choreography moves to `app.physical_ai` | World registry/lifecycle and storage ports plus world mutation/simulation/query functions; accepted app workflow also consumes Activities |
| Missions *(accepted Activity target)* | Graph materialization, committed-intent Activity composition, terminal projection, transcript ingestion, and trajectory query/evaluation composition. Family processors retain transition authority; Activity or trajectory evidence cannot advance tasks. | Consumes Activities, a structural mission world, family-owned sandbox resource, artifact-family handlers plus redaction/storage ports for transcripts, and world-query plus pure evaluation-grading functions for trajectory reads. |
| Runtime/API adapters | Construct exact family operations and select trusted or actor-aware dispatcher entry | Commands dispatcher plus family models |
| `RuntimeResources` | Process admission, supervised work, handle ownership, and phased retryable teardown | Dispatcher, audit projection, storage, and registered owners |
| `archetype.wiring` | Concrete construction, registration, and callback wiring | Every concrete implementation it constructs |

World mutation and simulation functions share the registry's exact-world
authority. Durable world query intentionally reads storage without requiring a
live world. Evaluation owns the product evaluation transaction; API transport
never pins snapshots, invokes graders, or persists evaluation receipts.
Research enters through one exact direct-only `autoresearch` registration. Its
outer handler is one synchronously awaited dispatcher admission; inner world
and storage calls do not redispatch. Wiring owns one process-shared
experiment-keyed admission map, while each state boundary uses the registry's
named world lock. Research creates no second workflow owner, detached task, or
shared-service finalizer.

## 6. Dispatcher and trust-boundary policy

The actor-aware boundary is `CommandDispatcher.apply_as()` or `defer_as()`.
An ingress adapter:

1. accepts an authenticated `ActorCtx`;
2. constructs the exact family operation model;
3. enters the commands-owned dispatcher through an actor-aware mode; and
4. returns a boundary-safe result.

The commands-owned `Policy` and `CommandDispatcher` perform authorization,
quota debit, admission, handler dispatch, and bounded advisory access evidence.
The ingress adapter owns no policy counter, worlds, services, command queue, grading
workflow, ingestion transaction, durable result, or audit storage.

`ActorCtx` crosses ingress only into commands-owned policy/dispatch
machinery; it never reaches a registered family handler. When an admitted
operation needs durable provenance, commands snapshot the principal into its
immutable admission record. Trusted local operations use an explicit local
origin rather than fabricating an admin authorization event.

Authentication belongs to the ingress adapter. Authorization belongs to the
commands policy/dispatcher. The CLI merely transports credentials.

## 7. Commands, commits, artifacts, and audit

Durability is family-specific rather than one service-level flag:

| Boundary | Authority | Commit condition |
|---|---|---|
| Deferred command admission | Command ledger | `PENDING` record, order, payload version and principal/origin are durable |
| Tick | Store plus commit coordinator | All tick rows are durable and the visibility manifest is published |
| Deferred command outcome | Commit coordinator plus command ledger | Terminal applied outcomes settle atomically with the manifest that makes them visible |
| Agent Mission dispatch *(current baseline)* | Mission world tick plus process-local post-tick outbox | A `dispatched` task row is durably visible before any sandbox request leaves the world; process restart delivery is not yet proved |
| Agent Mission Activity *(accepted target)* | Required projector, Activity coordinator, family adapter, and later mission tick | The exact committed dispatch admits `(world_id, kind, activity_id)`; a bounded result is durable before staging; settlement requires Mission completeness evidence bound to that result reference/digest in the exact later receipt |
| Agent Mission acceptance | Mission processors plus world tick | Revision-bound validation and exact-head publication first produce an immutable candidate; a separate critic sandbox stages a complete receipt bound to that candidate's base, head, diff, validator bundle, and policy; only a later task-decision tick accepts, repairs, or exhausts the task |
| Hosted Physical-AI Activity *(accepted target)* | Physical app workflow, Activity coordinator, durable Arrow/artifact publication, and later physical tick | The complete hosted result is durable by stable operation identity before its bounded reference is observed; a seeded simulator reuses that result rather than assuming GPU replay determinism |
| Typed family rows | Owning family workflow plus `StorageService` and Iceberg | Storage resolves and stamps the durable world/run envelope, the registered schema accepts the rows, and one Iceberg append makes the selected rows visible |
| Artifact ingestion | Artifacts-family handler plus `StorageService` | The published durable tick is selected before file effects; the immutable object and any media-specific rows are durable before the common `artifact_files` occurrence becomes visible |
| Coding-agent transcript | Redaction, artifacts-family handler, and storage authority | Raw narrative never becomes durable; the sanitized artifact is indexed and its digest verified before normalized rows keyed to its `artifact_id` are appended |
| Evaluation | Family handler plus `StorageService` and its control catalog | Subject and grader contract are pinned, one key-conditional result append is durable, and the evaluation lease is settled |
| Audit | Transactional outbox plus projection | Authoritative event is durable; analytical Iceberg projection may lag |

The store/updater owns physical tick append and flush. `StorageService` owns
the application execution lane and the physical app-table operation. The
owning workflow still defines the logical unit, and a coordinator publishes
tick visibility only after physical durability. `StorageService` does not
decide what a tick, artifact, evaluation, or command outcome means.

The landed Agent Missions V1 preservation baseline already separates authored
green work from acceptance: successful revision-bound validators plus
publication create an immutable candidate, and an independent critic reviews
that exact candidate in a distinct sandbox. Blocking findings become durable
repair input; missing, stale, malformed, wrong-head, or same-author evidence
cannot accept. The mission service crosses its committed dispatch/review
observation seams as described in
[Agent Missions V1](agent-missions.md). The required committed-tick projector
in section 13 is the current retryable seam for those intents.

The durable scheduler/dispatcher belongs to the commands family. Both trusted
runtime operations and actor-aware remote admission use it. Runtime or API
constructs and delegates the exact operation; the dispatcher authorizes
actor-aware admission. Simulation invokes a named commands-family materializer
callback at the tick boundary.

`StorageService` owns the catalog-derived world/run envelope, extends
caller-keyed conditional keys with that identity, and owns `daft.Catalog`
registration, schema comparison, execution, Iceberg writes, and conflict
retry. The artifacts family's free handlers require explicit storage
coordinates, verify the durable world/run and published head before file
effects, and specialize that substrate for files and media metadata.
Mission-owned `TranscriptIngestionService` composes those handlers and the
storage port with redaction and the pure missions parser; it creates no third
storage authority.
Durable external material is described as an artifact, evidence object, typed
dataset row, or evaluation receipt—never as a universal fact.

### Storage execution authority

Archetype-owned terminal Daft work outside storage MUST enter through the
canonical `archetype.storage.StorageService` authority through the narrow
`iStorageService` port.
`materialize()` admits a lazy plan and returns its completed frame.
`read_table()` returns a lazy app-table read; `append_table()` and
`append_missing()` own registration, schema alignment, materialization, and
Iceberg commit retry. `append_world_rows()` and `read_world_rows()` own the
generic durable world/run envelope. Other families may build lazy
DataFrame plans, but they MUST NOT call Daft collection, Iceberg read/write,
or catalog table-creation primitives directly. A bounded conversion to Python
control state may call `to_pylist()` only on a frame first returned by
`iStorageService.materialize()`.

`pin_visibility()` captures an immutable manifest-token allowlist.
`scan_visible_world_rows()` applies only physical world/run, manifest-token,
and optional maximum-tick filters. It MUST NOT resolve entity liveness,
same-tick active/inactive ties, component ownership, lineage meaning, resume
tick, or the next entity ID; those remain world-family interpretation.
Coordinator construction binds the exact `(world_id, run_id, writer_epoch)`
before tick publication.

One `StorageService` serializes terminal Daft submissions within one process.
Its execution gate is reentrant within one task so a cached append can flush
through the same authority. It is not a second distributed transaction
protocol. Iceberg remains authoritative for
atomic table snapshots and optimistic concurrency. On a conditional-append
conflict, storage refreshes the table and recomputes the anti-join before
retrying so stale pending rows cannot duplicate an already-committed logical
key.

Managed ECS appends follow the same storage authority without changing core:
the private Iceberg adapter materializes one Arrow payload, retains its commit
token and physical table identity, and refreshes/retries only an exact catalog
compare-and-swap conflict. Retry is bounded and uses full jitter. A
commit-state-unknown response cannot prove absence, so v0.5 does not retry it
or claim exact reconciliation. Storage raises `AmbiguousCommitError` with the
exact table/world/run/tick identity and commit token, then rejects later
non-empty appends to that table for the managed store's remaining lifetime.
This also prevents a restored cached batch from replaying. The manifest-last
protocol keeps any unconfirmed rows invisible (issue #709).

The v0.5 API exposes no general schema-evolution contract. Physical layout
tuning, compaction, and snapshot expiry are also deferred. Visibility pinning
uses an explicit manifest-token allowlist whose size is linear in committed
tick count.

The durable control plane is separate from that data plane. The local SQLite
`ControlCatalog`, or its remote Durable Object implementation, owns world
identity, writer fences, visibility manifests, deferred commands, and narrow
workflow leases. Daft Catalog and Iceberg own table metadata, snapshots, and
data files. Local SQLite may combine directory and per-world control records in
one database. The remote deployment may separate directory discovery from
each world's control Durable Object, and Iceberg always commits separately.
Only the target world's control authority atomically publishes its manifest,
command settlement, and durable control outbox after data flush; no global
transaction spans the directory authority and Iceberg.

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

`archetype.wiring` is the only module allowed to import concrete
implementations across families. It constructs one process graph:

```text
build_runtime_resources(...)
  -> OperationRegistry + Policy + CommandScheduler + CommandDispatcher
  -> WorldRegistry + WorldLifecycle + AuditLog + StorageService
  -> application-family services
  -> RuntimeResources
```

Runtime construction and API lifespan code may call the wiring transaction.
Ordinary runtime and route modules retain only `RuntimeResources` or its
dispatcher, never the concrete service graph.

For Agent Missions V1, wiring registers exact submit/run/restore handlers. The
submit handler constructs `MissionService` exactly once inside the
pre-reserved workflow owner, retains the combined author-and-critic Activity
binding for that owner, and creates exact-world cleanup authority only after
close begins. The family service installs the built-in processor/resource
bundle and owns mission-world orchestration; the runtime handle neither
imports nor retains the concrete service.

Concrete services compose collaborators and never inherit another concrete
service. Intentional inheritance is limited to components, processors,
hook/event contracts, protocols/abstract extension contracts, and the
application error taxonomy unless the architecture policy records another
reviewed family.

## 9. Runtime callbacks and cycles

Object wiring may contain named callbacks without creating reverse static
imports. `AsyncWorld` consumes a core-owned `CommandMaterializer` callable.
`WorldLifecycle` receives the scheduler method at composition and wires it
fresh into create, resume, and fork. The world family does not import the
concrete scheduler, dispatcher policy, or auth implementation.

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
- allow family workflows to consume declared lower-family contracts without
  treating that path as public-API promotion;
- reject misplaced direct `Component` subclasses outside their named family;
- enforce the existing outer-package and family dependency rules;
- confine commands-owned `ActorCtx` to policy/dispatch and approved adapter
  construction;
- restrict concrete cross-family construction to `archetype.wiring`;
- reject concrete-service inheritance;
- reserve family-owned terminal Daft, Iceberg, and catalog-table
  operations to `StorageService`, while allowing only storage-materialized
  frames to cross into bounded Python control flow;
- reject live-world, runtime-resource, backend-client, and concrete-service
  leaks;
- verify active protocol consumer/implementation mappings;
- confine provider/exporter and logging configuration to explicit process-host
  callables and require one exact observation disposition for every callable
  family-owned protocol member;
- support only exact, issue-owned migration exceptions with release deadlines
  and objective expiry conditions; wildcard package exceptions are invalid;
- report the forbidden edge, governing rule, and supported alternative.

Representative invalid fixtures prove every rule fires. Passing the current
repository without rejection tests is not an executable architecture contract.

## 12. Current enforcement state

The family packages, commands-owned
registry/policy/dispatcher/scheduler/audit projection, local/remote control
authority, artifact and evaluation ownership, `RuntimeResources`, and
co-located protocols are implemented. Runtime calls do not fabricate
`ActorCtx`; API routes depend directly on the lifespan-owned dispatcher;
concrete services and process wiring are not top-level exports.

Agent Missions V1 is implemented under `archetype.missions` and
`archetype.runtime.missions`. The
top-level mission-family edge to `archetype.graph` is machine-declared and
supports temporal `DependsOn` and `PartOfMission` entities plus previous-tick
`GraphView` joins. Coding-agent and sandbox implementations remain subordinate
resources within the mission family.

`quality/architecture.toml` contains the scalar policy and family
DAG. Per-family fragments under `quality/architecture.d/` register the
top-level dispositions for `artifacts`, `commands`, `episodes`, `evaluation`,
`graph`, `missions`, `physical_ai`, `projections`, `redaction`,
`research`, `storage`, and `world`.
`scripts/check_architecture.py` enforces their package direction, protocol
imports, concrete construction, concrete inheritance, and persistent
Component placement.

The artifacts pull-forward (#651) is complete. `archetype.artifacts` owns
`ArtifactSource`, `ArtifactRef`, `ArtifactStoreConfig`, the cohesive
`FileIngestionPipeline`, bounded scanners, storage-backed views, and exact free
handlers. `archetype.storage.StorageService` owns the durable world/run
envelope plus app-table catalog and execution authority. There is no standalone
ingestion package, application artifact facade, live-registry fallback, or
default-storage fallback.

The evaluation workflow pull-forward (#650) is complete:
`archetype.evaluation` owns `EvalReceipt`, grading values, identity digests,
exact operation models, snapshot views, and free handlers. Those handlers pin,
grade, lease, recover, and append through explicit `iStorageService`
coordinates. There is no application evaluation facade, ingestion fallback, or
live-world-registry dependency.

The research workflow pull-forward (#652) is complete:
`archetype.research` owns the frozen `AutoResearch` operation, supported values,
ledger Components, views, process-shared keyed admission type, and free
handler. Its reviewed graph is `research → storage, world`; the deleted
the former research facade and its service protocol have no
compatibility facade.

The trajectory, physical-AI, physical-workflow, ontology, HTN, and transcript
stages have landed. Physical workflows are reachable through exact
dispatcher operations exposed by `ArchetypeRuntime`; their former raw-service
bridges and all six Issue #589 architecture exceptions are gone. Transcript
ingestion is reachable through a trusted runtime operation and writes only
sanitized narrative to typed rows linked to the common artifact occurrence;
its PR4 registration is not actor-aware. It does not implicitly spawn mission
Components. The provisional
`archetype.experiments` package and its two unsafe logging exceptions are gone.
The architecture manifest currently has no owned migration exceptions.

Independent manifests under `quality/observability/` declare each family's
operation dispositions. `scripts/check_observability.py` enforces their exact
coverage, source-backed positive signal claims, and the vendor-neutral
signal/configuration boundary without a live collector. It validates root
syntax and exclusivity but does not invent call graphs or runtime topology.
The existing footgun reviewer complements this deterministic audit with
semantic observability review.

Durable ECS reads belong to `archetype.world.query`; audit history is the
commands-owned `GetAuditHistory`/`AuditLog` projection. `ActorCtx` and exact
operation models live with the commands or owning family. There is no generic
command envelope, facade bridge, or compatibility auth re-export.

## 13. Accepted v0.5 target architecture

This section records the ratified v0.5 architecture and its status after PR4.
The dispatcher, exact-operation, composition, and runtime-resource ownership
described here are current. Later behavioral migrations are explicitly marked
as targets and do not override their current focused specifications. Every
move must update policy, focused specifications, and executable oracles
atomically.

Families own behavior. Commands validate. Dispatcher governs entry. Scheduler
owns command durability. The Activity coordinator owns between-tick delivery.
World owns state/tick/run identity. RuntimeResources owns process lifetime.

Runtime, API, and CLI are thin supported surfaces rather than alternate
implementations of family workflows.

### Current family dependency graph

All arrows point from consumer to dependency. `core` has no top-level-family
dependency. `errors` is the exact common-family module; `runtime`, `api`,
`cli`, and `wiring` are reserved surfaces outside the family graph.

| Consumer | Allowed top-level family dependencies |
|---|---|
| `storage` | none |
| `world` | `storage` |
| `commands` | `storage`, `world` |
| `artifacts` | `storage` |
| `redaction` | none |
| `evaluation` | `storage`, `world` |
| `research` | `storage`, `world` |
| `physical_ai` | `storage`, `world` |
| `episodes` | `artifacts`, `evaluation` |
| `graph` | none |
| `missions` | `artifacts`, `episodes`, `graph` |
| `projections` | `graph` |

The A2 Activity slice adds one reviewed top-level edge:
`activities -> storage`. The current graph above remains unchanged until that
package, its policy fragment, and its executable architecture oracle land
together.

Every family may also import `archetype.core`, stable shared boundary-error
bases from `archetype.errors`, itself, and third-party libraries. Another
domain-family edge requires the normal same-change documentation, policy, and
cycle review. Family operation models do not import `commands`. `wiring.py` registers
model/handler pairs and is the sole concrete cross-family composition root.
`runtime` and `api` consume commands plus the family models and projections they
expose; CLI remains an HTTP client except for server startup.

The physical-AI handlers exercise exactly the `storage` and `world` edges.
Physical task and sweep reports remain family-owned terminal projections; the
workflow does not import or delegate report authority to `evaluation`.

The current package ownership is:

```text
src/archetype/
  core/          kernel; only the approved tick/run-identity changes
  errors.py      stable shared boundary-error bases
  storage/       Daft execution, catalogs, commits, scans, signatures, session
  world/         registry, lifecycle, simulation, mutation, query, handlers
  commands/      operation registry, dispatch, policy, scheduler, access audit
  graph/
  evaluation/
  research/
  physical_ai/
  artifacts/
  episodes/
  missions/
  redaction/
  projections/
  runtime/
  api/
  cli/
  runtime_resources.py
  wiring.py      constructs and returns RuntimeResources
```

Historical note (superseded): before PR4, the design used
`RuntimeApplication`, `CommandGateway`, `ServiceContainer`, a generic command
envelope, and facade protocols. The refactor deleted those mirrors rather than
retaining compatibility layers. Genuine resource/provider protocols and
stateful owners remain in their named families.

### Execution and durability boundaries

`AsyncWorld.step()` receives a construction-supplied command materializer.
Before `PreTick` and before discovering active signatures, it materializes
commands due to the exact `(world, tick)` into that world's mutation caches.
Infrastructure failure fails the tick. Per-command rejection, retry, and
dead-letter policy remains scheduler-owned. Successfully staged command IDs
settle only in the control-authority transaction that publishes the tick
manifest. Public hooks remain advisory and failure-isolated.

The world owns its identity, immutable UUIDv7 run identity, tick, entity and
signature maps, live frames, mutation caches, and tick algorithm.
`WorldRegistry` owns the collection of live worlds and uses a structural
registry lock plus one state-change lock per world. No callback or inherited
task context grants lock-bypass authority. Compound behavior acquires once and
calls an explicitly lock-held helper; multi-world behavior acquires locks in
sorted world-ID order.

One frozen Pydantic command model represents each externally operable family
behavior. `CommandDispatcher.apply()` is trusted actor-free entry;
`apply_as()` adds policy/RBAC and bounded access-decision evidence.
`CommandScheduler` adds durable options, canonical serialization, leases,
attempts, and settlement without redefining the family command. Direct and
deferred world mutations call the same module-level behavior.

The generic post-commit seam is a manifest-bound committed-tick receipt plus a
required projector/acknowledgment path outside `HookRegistry`. A receipt carries
identity and a pinned visibility reference, never live frames. Required
projection may be retried without rerunning the tick. Public `PostTick`
observers cannot suppress or acknowledge it. Mission-specific dispatch and
review intent are consumers of this seam, not special hook semantics. The
accepted Activity coordinator durably admits that intent after projection;
workers claim outside the world lock and settle only against the later receipt
that commits their factual observation.

### Lifetime and workflow ownership

`wiring.py` returns `RuntimeResources`, the explicit owner of the dispatcher,
scheduler, registry, storage, audit projection, shared policies, supervised
tasks, and strongly registered workflow handles. Construction reserves
ownership before a factory or task can become active. `aclose()` stops
admission, drains admitted work, and closes dependency phases in order. It
attempts every independent cleanup in the current phase, aggregates labelled
errors, retains failed ownership and its dependencies, and retries that phase
on a later serialized call. Only successful finalization is idempotent.

The complete `RuntimeMissions.run()` operation is inside that admission and
ownership boundary. Its dispatcher registration is direct-only unless the
missions family explicitly supplies a portable durable encoding; calling the
mission service directly is not a second entry path. Admission reserves the
operation before task or provider construction can begin. Work admitted before
shutdown may finish binding resources to that reservation, and shutdown waits
for it; work arriving after admission closes cannot create a handle, task, or
provider effect. Internal cleanup uses an exact-world, non-inheritable
capability and cannot reopen public admission or operate on a sibling world.

The runtime-resource boundary reports an incomplete shutdown as
`RuntimeShutdownError` from `archetype.errors`. It identifies the failed
dependency phase and retains the non-empty ordered causes from every
independent cleanup attempted in that phase. Cancellation during cleanup is
retained as an `asyncio.CancelledError` cause of this retryable boundary error:
it does not mark the runtime closed, release ownership, or skip peer cleanup.
A later successful `aclose()` completes normally; only calls after successful
finalization are no-ops.

Agent Missions keeps live sandboxes, provider processes, checkpoints,
publication, supervision, and cleanup in explicit process owners. ECS
Components and relations are the durable intent/evidence record, and
processors alone decide readiness, priority, repair, acceptance, and terminal
transitions. A required projector turns committed ECS intent into one durable
Activity keyed by world, kind, and dispatch or review identity. The family
adapter reconciles provider effects with that identity and fails closed on an
ambiguous started outcome. Bounded observations return through a later tick,
and the Activity settles only against matching result-digest completeness
evidence in its exact receipt. Provider callbacks and Activity catalog state
never decide task state.

Planners emit typed, provider-neutral task-graph, dependency, priority,
validator, critic, and artifact-policy proposals for validation and commit.
They receive no live capability and cannot mutate a world, publish an
artifact, or accept a task. Checkpoints, artifacts, transcripts, and episodes
are first-class recovery/evidence references, but their existence has no
implicit acceptance authority. Only an explicit typed policy may require
their publication for a transition.

Persistent behavioral evidence converges on `episode_id`: evidence rows are
keyed by `episode_id` and `seq`, and a trajectory is a derived learning-facing
DataFrame selected from episode evidence with no persistent identity. The
contract is documented in [Mission Trajectories](trajectories.md). The
episode-schema change was an intentional pre-1.0 v0.5 migration — no 0.4
backfill, dual reads, or compatibility aliases.

### v0.5 migration oracle status

The v0.5 migration is made executable slice by slice. This table preserves
the review decomposition and distinguishes the shipped PR4 substrate from
remaining work; a current test must exercise the claimed owner and failure
boundary rather than relying on a pre-refactor baseline.

| Contract | Executable evidence | Status after PR4 |
|---|---|---|
| command materialization and manifest-coupled settlement | command-flow and durable-command integration contracts | Landed in world/commands |
| lock and shutdown admission | runtime lifecycle and admitted-work race contracts | Landed in world/runtime resources |
| UUIDv7 run identity and fork/resume continuity | command-flow, fork-storage, and world-resume contracts | Landed in world |
| episode identity and trajectory derivation | trajectory-domain, trajectory-service, and episode-join contracts | Landed: evidence keyed by `episode_id`, trajectory is a derived view |
| stable task base, immutable candidate, exact-head critic | coding-agent, critic, mission-service, and capability-eval contracts | Current preservation baseline; author and critic Activity cutovers must preserve it |
| sandbox cleanup and retryable phased teardown | sandbox-service, runtime-contract, and mission-service race contracts | Process lifetime landed; Activity worker ownership remains |
| committed required projection and provider reconciliation | generic seam and mission-consumer failpoint contracts | Generic world seam landed; Activity catalog and Mission consumers are A2–A5 |

No row permits an implementation to claim completion from an old baseline test
alone.

## 14. Change discipline

Architecture changes update this normative document, the machine policy, its
negative fixtures, affected family protocol tests, and contract registry in
one change. Diagrams and generated reference pages follow those authorities;
they never create dependency rules by themselves.
