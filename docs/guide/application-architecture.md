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

The application layout is:

```text
src/archetype/app/
  application/       RuntimeApplication, its port, and boundary-safe models
  world/             world lifecycle, mutation, and simulation
  storage/           stores, catalog/control authority, backend construction
  query/             persisted ECS read paths
  artifacts/         ingestion, publication claims, content and indexes
  redaction/         pre-durability secret scanning, receipts and quarantine
  evaluation/        graders, snapshot pinning, receipts
  commands/          durable ledger, scheduling, dispatch, settlement
  gateway/           authorization policy boundary
  audit/             journals, outboxes, projections
  research/          autoresearch and multi-run research workflows
  missions/          typed mission/task/attempt transition authority
  sandboxes/         provider-neutral isolated execution and live-handle lifetime
  errors.py          cross-family application error contracts
  container.py       sole concrete cross-family wiring root
```

Every family co-locates its protocols, safe models, and implementation. A
family package exports only the contracts intended for other families. A
generic `services/` bucket and a monolithic `app/interfaces.py` are prohibited;
the architecture checker rejects edges that recreate them.

The allowed outer-package direction is:

```text
application code -> archetype.runtime
CLI              -> REST API over HTTP
runtime          -> app.application contracts and safe models
API              -> app.gateway contracts, safe models, and app errors
gateway          -> app.application port plus auth/audit ports
app families     -> approved lower app-family ports and core
core             -> foundation and third-party libraries only
```

Forbidden reverse edges include:

- core importing app, runtime, API, CLI, or experiments;
- app importing runtime, API, CLI, or experiments;
- runtime importing gateway, auth, concrete app services, or API;
- API routes importing `RuntimeApplication` or concrete app services;
- experiments importing the container or concrete app services; and
- CLI command implementations bypassing HTTP.

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
| Missions | Deterministic attempt identity, typed task transitions, retry/exhaustion and evidence gates | None |
| Sandboxes | Six-phase attempt execution, provider registry, checkpoints, and live handles | No other app family; provider adapters point inward |
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
```

Runtime and API lifespan code may construct or receive the internal container,
but ordinary runtime and route modules consume only their approved port.

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

The architecture checker must:

- cover every declared source scope and fail if a required scope is empty;
- enforce outer package and family dependency rules;
- confine `ActorCtx` to gateway/auth code and approved adapter construction;
- restrict concrete cross-family construction to `container.py`;
- reject concrete-service inheritance;
- reject live-world, container, backend-client, and concrete-service leaks;
- verify active protocol consumer/implementation mappings;
- support exact, owned, release-expiring migration exceptions; and
- report the forbidden edge, governing rule, and supported alternative.

Representative invalid fixtures prove every rule fires. Passing the current
repository without rejection tests is not an executable architecture contract.

## 12. Current enforcement state

The family packages, actor-free application facade, authorized gateway,
durable command scheduler, local/remote control authority, artifact and
evaluation ownership, and co-located protocols are implemented. Runtime calls
do not fabricate `ActorCtx`; API routes depend on `iCommandGateway`; concrete
services and the container are not top-level exports.

`quality/architecture.toml` contains the complete allowed family DAG and zero
migration exceptions. `scripts/check_architecture.py` enforces package
direction, protocol imports, concrete construction, and concrete inheritance.

The mission family is pure transition authority over persisted row values. It
does not own provider clients or write around the world tick commit boundary.

Two deliberately retained implementation seams are documented rather than
hidden: `QueryService` uses `iAuditLog` for compatibility history reads, and
the root `app/models.py` holds cross-family boundary models. Changing either is
a separate contract/model-ownership decision, not undocumented drift.

## 13. Change discipline

Architecture changes update this normative document, the machine policy, its
negative fixtures, affected family protocol tests, and contract registry in
one change. Diagrams and generated reference pages follow those authorities;
they never create dependency rules by themselves.
