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
new dependencies or silently preserve implementation drift.

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
| Durable authority, cross-family orchestration, internal service ports, and concrete implementations | `archetype.app.<family>` |
| Transport, authentication, application facade, and composition | `archetype.api`, `archetype.app.gateway`, `archetype.app.application`, and `archetype.app.container` |

A top-level domain-family package owns reusable ECS state and pure domain
behavior. It may depend on `archetype.core`, itself, third-party libraries, and
only reviewed lower top-level family contracts declared in
`quality/architecture.toml`. It must not import `archetype.app`,
`archetype.runtime`, `archetype.api`, or `archetype.cli`, and it does not
configure providers, exporters, storage backends, process hosts, or the
service container. The application layer may import a registered top-level
family contract; the reverse edge is forbidden. Undeclared top-level
family-to-family dependencies are denied. Every first-party package or module
directly beneath `archetype` is classified as reserved infrastructure or
registered as a domain family with one exact dependency disposition.
Unclassified scopes fail the architecture audit, and the complete registered
family graph must be acyclic. Imports through the `archetype` root facade are
resolved to the module that owns the exported package or symbol before these
rules are applied. If its static export map cannot be parsed exactly, the audit
fails rather than degrading root-facade enforcement.

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

The `archetype.missions` family is a leaf disposition with no allowed
top-level family dependencies. It owns only persistent mission Components,
component-local validation, world-state enums, and the pure completed-attempt
transition graph. Its family-package exports are deliberate; none are added to
the `archetype` root facade. Claims, recovery actions, execution
authorization, redaction, storage integration, ports, errors, and concrete
services remain under `archetype.app.missions`.

The application-authority layout is:

```text
src/archetype/app/
  application/       RuntimeApplication, its port, and boundary-safe models
  world/             world lifecycle, mutation, and simulation
  storage/           stores, catalog/control authority, backend construction
  query/             persisted ECS read paths
  artifacts/         ingestion, publication claims, storage and indexes
  redaction/         pre-durability secret scanning, receipts and quarantine
  evaluation/        grading orchestration, snapshot pinning and receipt writes
  commands/          durable ledger, scheduling, dispatch, settlement
  gateway/           authorization policy boundary
  audit/             journals, outboxes, projections
  research/          autoresearch and multi-run research workflows
  missions/          redaction-gated claims, fencing and orchestration
  sandboxes/         provider-neutral isolated execution and live-handle lifetime
  errors.py          cross-family application error contracts
  container.py       sole concrete cross-family wiring root
```

The mission-adjacent package map is adjudicated in
[Agent Mission Transitions, section 7](agent-missions.md#7-adjacent-capability-package-map).
It assigns the provisional experiment modules to a future `research` family,
mission-owned trajectory modules, and a future `physical_ai` family; folds
HTN under `archetype.missions` as its planning subdomain; folds dataset
identity into `archetype.evaluation.contracts`; and creates no new top-level
family edge. Those target packages enter this concrete layout only as their
focused implementation issues land; the current source tree and manifest
remain the implementation truth until then.

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

`StorageService` resolves and pools an `iAsyncStore`. `WorldService` owns the
world factory and live registry. A world composes one querier, updater, system,
resource registry, and hook registry. The querier and updater consume the same
store. A live world is an internal capability and never crosses an application,
gateway, runtime, API, or CLI boundary.

| Consumer/family | Responsibility | Allowed app dependencies |
|---|---|---|
| Storage | Store, catalog, control-authority, and storage-context lifecycle | None |
| World lifecycle | Create, lookup, fork, resume, destroy, live registry | Storage port |
| Mutation | Mutate a resolved live world | World port |
| Simulation | Step, run, episode, and rollout | World port plus named command-drain and quota-reset callables |
| Query | Persisted ECS reads, durable discovery, and compatibility history reads | Storage and audit ports |
| Redaction | Provider-neutral secret scanning, deterministic text redaction, safe receipts and quarantine | None |
| Artifacts | Durable ingestion, immutable content, contextual links, publication claims and indexes | Redaction, storage and world-coordinate ports |
| Evaluation | Snapshot pinning, grader contracts, grading, evidence and durable receipts | Query and artifact ports |
| Commands | Durable admission, order, leasing, dispatch, retry, settlement and dead letters | Control catalog plus world and mutation ports |
| Audit | Transactional journal/outbox and analytical projection | Storage or control-authority ports |
| Research | Multi-run research workflows | World and simulation ports plus explicit evaluator callbacks |
| Missions | App-internal validator normalization, redaction-gated policy-bound attempt identity, provider-submission claims, single-use provider-call grants, runner-lifetime lease supervision, fencing, structural attempt orchestration, retry/exhaustion and evidence gates; consumes the top-level mission world domain | Redaction port, storage control catalog, injected structural sandbox runner and mission-owned artifact-finalizer port |
| Sandboxes | Six-phase attempt execution, provider registry, checkpoints, and live handles | App-owned mission execution authorization, admission callback, and recovery action; provider adapters point inward |
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
publication claim, durable receipt, or audit storage. It is stateless policy
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
| Mission provider submission | Mission attempt claim plus redaction and control authorities | Canonical request/provider metadata are quarantined before claim creation; the immutable policy ID, normalized request, runner metadata, and typed scan receipts are durable under a continuously renewed live fence; sanitized outcome/error plus receipts precede projection and terminal CAS |
| Mission indexed finalization | Mission attempt claim plus artifact publication outbox and index | Sanitized outcome and exact prepared request are staged before object/index I/O; the claim authority rereads the matching terminal `INDEXED` or `EXPIRED` catalog row, produces the claim-bound sealed settlement, settles the claim, and then `require_settled` rereads/authenticates the durable winner before the execution service's private row projection |
| Artifact ingestion | Artifact workflow plus publication claim | Content/rows are durable and their contextual index is published |
| Evaluation | Evaluation workflow | Subject and grader contract are pinned and the typed receipt is published |
| Audit | Transactional outbox plus projection | Authoritative event is durable; analytical Iceberg projection may lag |

The store/updater owns physical append and flush. The owning workflow defines
the logical unit. A coordinator publishes visibility only after physical
durability. `StorageService` does not decide what a tick, artifact, evaluation,
or command outcome means.

The durable scheduler/dispatcher belongs to the commands family, not to the gateway. Both
trusted runtime operations and authorized remote admission may use it. The
gateway authorizes remote admission and delegates; simulation invokes a named
commands-family drain callback at the tick boundary.

`ArtifactService` owns claim-backed component publication;
`ArtifactTableService` owns typed file/row ingestion. Both are artifact-family
workflows. Durable external material is described as an artifact, evidence
object, typed dataset row, or evaluation receipt—never as a universal fact.

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
container.mission_attempt_workflow(storage_config)
  -> storage-bound claim + artifact finalizer + execution services
```

Runtime and API lifespan code may construct or receive the internal container,
but ordinary runtime and route modules consume only their approved port.

`app/missions/execution_service.py` is a family-internal orchestrator, not a
composition root. It receives `iMissionService` and
`iMissionAttemptClaimService`, `iMissionArtifactFinalizer`, and the
mission-owned structural sandbox runner from its caller. It may not import a
sandbox implementation, storage catalog, or another application family.
`app/application/mission_artifacts.py` is the narrow cross-family workflow
adapter: it implements the mission-owned finalizer port over
`iArtifactBundleService` and immutable boundary models, without importing an
artifact concrete service. The container remains the sole concrete
cross-family composition root. Its mission workflow factory requires an
explicit `StorageConfig` and binds both the claim catalog and artifact adapter
to that same identity; cold recovery never discovers or defaults storage from
live world state.

`MissionAttemptClaimService` is a reviewed missions-to-redaction dependency and
has no optional scanner path. Any composition site constructing it must inject
`iRedactionService` together with the control catalog. Mission orchestration
consumes the claim port; it does not fork redaction policy or scan with a
sandbox-specific filter.

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

`quality/architecture.toml` contains both the application-family DAG and the
registered top-level family dispositions for `datasets`, `experiments`, `htn`,
and `missions`. `scripts/check_architecture.py` enforces their package direction,
protocol imports, concrete construction, concrete inheritance, and persistent
Component placement.

The remaining reverse edges from provisional `archetype.experiments` and the
remaining Components under app-family `models.py` are preserved only by exact
migration entries. Evaluation and artifacts point to relocation issues #557
and #558. The design gate in #561 has been adjudicated: research Components and
runner imports now point to #585, trajectory recorder imports point to #586,
and physical-AI rollout/sweep reverse imports point to #589. The transcript,
physical-AI domain, ontology, HTN-adapter, and final umbrella-removal stages are
Issues #587, #588, #590, #591, and #592. No architecture exception remains
owned by Issue #561. Mission Components and pure world transitions moved under
Issue #559, so no mission migration exception remains. These entries make no
app symbol supported.

Independent manifests under `quality/observability/` declare each family's
operation dispositions. `scripts/check_observability.py` enforces their exact
coverage, source-backed positive signal claims, and the vendor-neutral
signal/configuration boundary without a live collector. It validates root
syntax and exclusivity but does not invent call graphs or runtime topology: the
three existing gateway decorators remain children, and Issue #515 owns
coherent ingress roots. The existing footgun reviewer complements this
deterministic audit with semantic observability review.

`MissionService` remains app-internal pure transition authority over persisted
row values and now consumes `archetype.missions`; the mechanical extraction
does not rename or promote it.
The same family now owns `MissionAttemptClaimService`, a control-catalog-backed
pre-execution authority. It durably fences external provider submission and
stores replayable terminal outcomes, but owns no provider client and cannot
advance a task outside the world tick commit boundary. Its required redaction
port quarantines canonical request/provider/acknowledgement and semantic outcome
identity before each durable edge, binds the policy ID into immutable claim
identity, and stores typed phase receipts. Narrative outcome/error values are
redacted before projection or terminal CAS; the original finding receipt is
preserved. Non-terminal policy drift fails closed, while settled sanitized
replay remains readable. Arm mints one execution nonce for the fence; catalog
consumption is atomic, single-use, live-lease checked, and required before
provider preparation. Acknowledgement requires a consumed grant.
`MissionAttemptExecutionService` is the family-local structural path joining
claim acquisition/arm, grant consumption, provider acknowledgement, direct-row
application, indexed artifact finalization, claim settlement, settled-row
application, and terminal replay.
`MissionService` normalizes
validators before claim acquisition and persists the retry budget and
finalization threshold in request identity. It also persists the first
observation tick as non-identity evidence, and recovery restores that durable
request before runner or artifact work. The execution service derives
provider metadata from the selected runner rather than accepting an unrelated
caller value. It supervises the entire runner lifetime with a lease heartbeat,
cancels and awaits its local child tasks on failure or caller cancellation, and
renews once after completion before staging, direct application, or settlement.
Remote provider cancellation is adapter-specific; unresolved work remains
`possibly_submitted` for reconciliation. Provider-accepted results require
consumed-grant evidence; checkpoint provider and agent session evidence must
match the claim. For an indexed policy, accepted and rejected recoverable
outcomes enter `finalizing` with an exact prepared artifact request before any
publication I/O. Recovery resumes that request through the artifact outbox and
never invokes the runner. Only a bound terminal indexed or expired catalog row
can produce the service-sealed finalization settlement; process-local receipts
cannot, and generic settlement cannot finish a finalizing claim. Public
`MissionService.apply_attempt` categorically rejects an indexed phase and any
artifact staging, linkage, finalized authority, or nonzero snapshot. The claim
service must authenticate the terminal row and settle first; the execution
service then calls `require_settled(world_id, claim_key)` to reread and
authenticate the durable winner before invoking an implementation-private
mission row transformer and the ordinary world commit. `iMissionService`
exposes no settled-projection method, and a detached or caller-replaced
`AttemptClaim` DTO is never authority. Provider capability metadata never
authorizes execution; uncertain and acknowledged claims reconcile. The sandbox
family consumes only the mission-owned immutable execution authorization,
callbacks, and recovery action; it does not import the claim service or storage
catalog.

Other deliberately retained implementation seams are documented rather than
hidden: `QueryService` uses `iAuditLog` for compatibility history reads, and
the root `app/models.py` holds cross-family boundary models. Changing either is
a separate contract/model-ownership decision, not undocumented drift.

## 13. Change discipline

Architecture changes update this normative document, the machine policy, its
negative fixtures, affected family protocol tests, and contract registry in
one change. Diagrams and generated reference pages follow those authorities;
they never create dependency rules by themselves.
