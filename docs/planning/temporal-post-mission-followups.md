# Post-Mission Temporal Follow-up Requirements

- Date: 2026-08-30
- Status: implementation-ready requirements; product decisions called out below
- Baseline: `f4ebac6d` and `1a6b6bcb`
- Parent plan: [Temporal orchestration migration](temporal-orchestration-migration.md)
- Authority audit: [Temporal responsibility audit](../reports/temporal-responsibility-audit.md)

## Purpose

This packet turns the reconciled follow-up ledger into executable requirements
for four post-Mission tracks:

1. Evaluation;
2. Rollouts and simulation fan-out;
3. remaining hosted Physical-AI Activities; and
4. AutoResearch.

It does not authorize a cutover. Each track still requires its own portable
contract, local parity proof, process-death proof, drain decision, route-epoch
flip, and deletion proof. Mission release is a dependency, not evidence that a
different family's legacy path is safe to remove.

The current code and tests cited here are implementation evidence, while the
parent plan and responsibility audit remain normative when they disagree with
an implementation detail.

## Shared scope and authority boundary

### In scope

- Deterministic Workflow and child-Workflow identities.
- Durable sequencing, waits, timers, retries, cancellation intent, bounded
  progress, worker replacement, and Continue-As-New.
- Family-owned, JSON/reference-only Workflow inputs and outputs.
- Registered, versioned implementations in place of live callbacks, component
  classes, clients, or process-local resources.
- Immutable admission-to-Workflow handoff and a route epoch that excludes
  simultaneous legacy and Temporal ownership.
- Family-specific provider reconciliation, first-result recovery, exact
  collection, cancellation, and cleanup.
- Exact Archetype result publication, committed observation, and settlement.
- Freeze, drain, archive, or explicit import of work admitted before cutover.

### Out of scope

- Moving tick computation, manifests, writer fencing, world lineage, storage
  visibility, or required-projector correctness into Temporal.
- Treating Temporal visibility or Workflow history as the authoritative
  `has_unsettled(world_id)` answer.
- Serializing Python callbacks, classes, DataFrames, backend clients,
  credentials, raw `StorageConfig`, processors, resources, or hooks into
  Workflow history.
- Retrying an ambiguous external effect merely because an Activity lease or
  timeout expired.
- Genericizing provider-job code before at least two completed family paths
  prove the same abstraction.
- Changing evaluation conclusions into promotion authority, rollout results
  into storage authority, or AutoResearch frontier selection into deployment
  authorization.
- Migrating command scheduling, artifact ingestion, storage migration, or the
  full world lifecycle as part of these slices.
- Running paid GPU, model, Git, or remote-storage gates in ordinary CI.

### Authority matrix

| Concern | Temporal owns | Archetype or provider retains |
|---|---|---|
| Admission | Workflow deduplication after immutable handoff | Authentication, authorization, logical identity, route epoch, source receipt, and execution prebinding |
| Execution | Phase order, waits, timers, retry policy, worker replacement | Family effect semantics and provider-side idempotency or reconciliation |
| State | Bounded orchestration state and reference identities | ECS rows, committed receipts, control-catalog records, storage snapshots, evidence bytes, and provider first results |
| Cancellation | Durable request and phase sequencing | Whether an effect is reversible, exact provider cancellation, and domain terminal facts |
| Results | Waiting for and carrying a bounded result reference | Validation, keyed publication, observation staging, manifest-atomic settlement, and acceptance |
| Cleanup | Retrying an exact cleanup Activity | Resource identity, provider cleanup semantics, retained evidence, and irreversible-action limits |
| Process lifetime | Worker task processing | Client construction, secrets, implementation registries, task queues, and phased host shutdown in wiring/runtime resources |

### Required admission sequence

Every migrated family must use this order:

1. The existing governed entry point authorizes the request and canonicalizes
   its logical identity.
2. Archetype pins the exact source evidence and atomically records the route
   epoch, immutable request digest, and deterministic Temporal execution
   identity before any provider effect.
3. The caller starts or updates the deterministic Workflow. A lost start
   acknowledgement repeats the same Workflow ID and request digest.
4. Activities resolve registered implementations and external value references
   from host wiring. Workflow history never receives the live objects.
5. The sole-spawn Activity either recovers an existing provider result/call or
   starts once. Observe, collect, cancel, and cleanup phases cannot spawn.
6. Large or sensitive results are durably published outside Workflow history;
   the Workflow carries only their bounded identities.
7. A family-owned operation validates and publishes the exact domain result.
   Where the family crosses ECS state, a later committed receipt observes and
   settles that result exactly once.

If step 2 commits and step 3 is interrupted, required projection or the
admission adapter must repeat step 3. It must never create a second legacy
claim or a differently identified Workflow.

## Shared portable contract rules

The names below describe required shapes, not a mandate to create one global
base class. Each owning family should define its own frozen values until the
completed Evaluation and Physical-AI paths prove a truly common contract.

### Bounded value reference

Every value retained in Workflow history must include:

- `ref`: non-empty, at most 4,096 characters;
- `digest`: lowercase SHA-256 over canonical bytes and a contract domain;
- `size_bytes`: positive and checked against the family limit;
- `media_type`: exact versioned media type; and
- `schema_version`: positive, supported version.

The default Workflow-history payload limit is 1 MiB. Larger evidence remains
in family storage and crosses the Workflow boundary only by reference. Reading
a reference rechecks media type, size, digest, schema, and immutable identity.

### Durable execution reference

An effectful provider phase needs a bounded reference containing:

- family and logical operation ID;
- immutable request digest;
- provider and deployment/namespace digest;
- exact remote call, job, or reservation ID when one exists; and
- protocol/schema version.

An identity mismatch is a permanent conflict and touches nothing. A permanent
start marker without exact provider evidence is `Unknown`; it is not permission
to spawn again. `Unknown` is a terminal orchestration outcome until an explicit
operator reconciliation produces new authoritative evidence.

### Portable failure envelope

Python exception identity does not cross a durable boundary. Each family needs
a bounded envelope with:

- stable error code;
- phase;
- affected logical/child identity;
- `retryable`, `cancelled`, and `unknown` flags;
- bounded public detail or an external diagnostic reference; and
- deterministic precedence data when several children fail.

Tracebacks, exception objects, host paths, credentials, and unbounded provider
responses stay outside history.

### Registered implementations

A portable implementation reference contains an ID, implementation version,
deployment digest, configuration digest, and optional secret/value references.
Wiring resolves it from an allowlisted registry. Resolution must fail closed on
missing, conflicting, or drifted registrations. Import strings and arbitrary
module loading are not a registry.

The first durable routes support only deployment-pinned implementations.
Process-local custom callbacks remain on explicitly direct-only compatibility
paths until their product disposition is decided.

## Mission pattern: what transfers and what does not

The Mission work through the stated baseline proves useful mechanics:

- deterministic Workflow IDs bound to immutable authority;
- claim-free ECS execution prebinding;
- canonical request bytes outside history;
- sole-spawn `start`, result-first `poll`, read-only `collect`, exact `cancel`,
  and idempotent `cleanup` phases;
- durable remote call self-registration before the external effect boundary;
- terminal `Unknown` rather than unsafe replay;
- bounded polling with Continue-As-New; and
- deployment-pinned provider configuration and a family-specific task queue.

The following Mission details must not leak into generic orchestration:

- author/critic roles, redaction policy, sandboxes, Git branches, pushes, and
  pull requests;
- Mission review budgets or event schemas;
- the assumption that every job has one Modal sandbox cohort; and
- Mission's 1 MiB canonical JSON result as a universal evidence format.

Physical AI already has canonical Arrow batches and first-result manifests;
Evaluation has pinned snapshots and keyed `EvalReceipt` rows; Rollouts own a
world tree rather than a provider call; and AutoResearch is a long-lived
coordinator with ledger-head decisions. Those are retained family contracts,
not variants to squeeze into a Mission type.

## Unresolved cross-track decisions

These questions require a product or maintainer answer before the identified
cutover PR. The recommended default is safe enough for contract work to begin.

| ID | Concrete question | Recommended default |
|---|---|---|
| SH-1 | Should existing public methods return a durable handle immediately or preserve their current awaited result? | Preserve the blocking/awaited typed API behind a Temporal adapter first. Add an optional handle API in a later, separate contract. |
| SH-2 | May one Worker queue host all follow-up families? | Use a versioned queue per owning family, composed by wiring. Share only provider-neutral client/Worker construction helpers. |
| SH-3 | How is a route switched without dual authority? | Add a strongly consistent family route epoch (`legacy`, `draining`, `temporal`) checked during admission. Existing work keeps its recorded owner. |
| SH-4 | What is the initial payload bound? | Use 1 MiB for any value placed directly in history and references for everything larger. A family may choose a smaller bound. |
| SH-5 | What does cancellation promise? | Stop future scheduling and cancel only exact known provider/child identities. Preserve committed effects and evidence; never promise rollback of hardware actions, ticks, or published rows. |
| SH-6 | When may shared Modal/provider mechanics be extracted? | After Mission and Physical AI both pass their paid reattachment gates and a code review identifies an identical stable seam. Do not make extraction a cutover prerequisite. |

## Dependency graph and recommended order

| Order | Slice | Depends on | May run in parallel with |
|---|---|---|---|
| 0 | Finish Mission live Modal/Git reattachment and route cutover | Current Mission work | Requirements and local contract work below |
| 1A | Evaluation portable grader and value contracts | Shared orchestration package | Rollout lifecycle foundation; Physical-AI provider split |
| 1B | Rollout exact fork plan and behavior recipe | Caller-chosen world ID and absolute advance already landed; new caller-chosen run ID/reconciliation still required | Evaluation contracts; Physical-AI provider split |
| 1C | Physical-AI split provider protocol and remote self-registration | Mission live call-ID proof and pinned deployment pattern | Evaluation and rollout foundation |
| 2A | Evaluation Workflow, parity, cutover, and lease drain | 1A | Rollout child Workflow; Physical-AI Workflow |
| 2B | Rollout child then parent Workflow | 1B | Evaluation and Physical AI |
| 2C | Physical-AI Workflow, claim-free settlement, paid gate, and cutover | 1C | Evaluation and rollout |
| 3 | AutoResearch portable candidate/evaluator/frontier contracts | Evaluation's registered-provider pattern and rollout's durable child contract | Physical-AI cleanup/deletion |
| 4 | AutoResearch coordinator, parity, cutover, and drain | Track 3 contracts plus completed Evaluation and Rollout cutovers | Family-specific deletion work |
| 5 | Delete replaced legacy mechanics family by family | That family's inventory, drain/import, rollback, and parity gates | Other families that retain their own legacy path |

AutoResearch is the only strict cross-track serialization point: it composes a
candidate, a rollout, an evaluation, and a frontier decision, so its durable
route cannot precede portable Rollout and evaluator contracts. Remaining
Physical AI can proceed independently after the Mission live-call gate.

## Track 1 — Evaluation

### Current contract and migration scope

`Evaluate` currently pins one exact visible world snapshot, derives a subject
digest from its manifest heads and selector, executes one live `FrameGrader`,
and key-appends one `EvalReceipt` by `evaluation_id`. The control catalog's
`evaluation_leases` record, polling loop, heartbeat, owner token, and expiry
recovery are the mechanics to replace.

The durable path includes persisted `Evaluate` only. Ephemeral `RunGraders`
and `world.grade()` remain direct computation and do not acquire a Temporal
lifecycle merely because they use the same grading helpers.

Evaluation retains authority for:

- pinned subject identity, including inherited lineage segments;
- the distinction between caller-supplied `evaluation_id`, subject digest,
  and grader-contract digest;
- component/tick/entity selection;
- typed `Outcome` validation;
- `EvalReceipt` meaning and keyed result acceptance; and
- evidence publication and storage coordinates.

Temporal replaces only evaluator execution supervision: admission handoff,
provider phase order, waits, retries, cancellation, recovery, and progress.
Evaluation receipts remain evidence, never promotion or deployment authority.

### Portable identities, inputs, and outputs

#### `EvaluationAdmission`

The immutable admission record must contain or reference:

- `evaluation_id`;
- exact `world_id`, `run_id`, storage endpoint ID/fingerprint, snapshot tick,
  head tokens, and effective lineage segments;
- canonical component selectors as `(type_name, schema_fingerprint)`, plus
  sorted optional ticks and entity IDs;
- recomputable `subject_digest`;
- the existing `GraderContract` payload and `contract_digest`;
- a registered `GraderImplementationRef`;
- canonical request digest and schema version;
- deterministic Workflow ID/execution prebinding; and
- family route epoch.

The grader contract payload and every selector are canonical and bounded at
admission. Arbitrary configuration dictionaries are not permission to place an
unbounded value in Workflow history.

Pinning happens before Workflow start. A retry may re-read and verify the
pinned snapshot but must not silently pin a newer head.

`evaluation_id` deliberately stays independent from subject plus contract.
The same subject and contract under a new ID is a new grader trial; the same ID
with different subject or contract is a permanent conflict.

#### `GraderImplementationRef`

The reference must include `grader_id`, implementation version, deployment
digest, execution mode, configuration digest, and provider namespace when
applicable. Two initial execution modes are allowed:

- `deterministic-local`: a registered pure/read-only grader whose result is a
  deterministic function of the pinned frame, contract, and seed; and
- `durable-provider`: an adapter with sole-spawn `start`, result-first `poll`,
  exact `collect`, `cancel`, `cleanup`, and provider first-result semantics.

The existing `FrameGrader` callback, live component classes, Daft frame, and
raw storage configuration do not cross this boundary. An Activity resolves
component types and the grader from deployment-pinned registries, and wiring
resolves the storage endpoint.

#### `EvaluationResultRef`

The provider/local result is published before Workflow completion. Its bounded
descriptor binds:

- evaluation, subject, contract, grader implementation, and request digests;
- typed outcome status and optional finite score;
- canonical evidence value/reference and evidence digest;
- provider operation/result identity when applicable; and
- schema/media type and completeness metadata.

The Workflow returns the bounded result reference. A read-first
`publish_receipt` Activity validates it, checks for an existing keyed row,
appends exactly one `EvalReceipt`, and re-reads that row before reporting
success. Full evidence bytes do not enter Workflow history.

### Required phase behavior

1. `pin_and_admit` runs under Evaluation/storage authority and records the
   exact immutable admission plus Temporal execution identity.
2. `start` is the only operation allowed to execute or submit a grader.
3. `poll` reads a provider first result before provider call state. A durable
   result wins over an expired or failed remote call.
4. `collect` is read-only and revalidates identity, typed outcome, evidence,
   and bounds.
5. `publish_receipt` is read-first and key-idempotent. An existing row is
   accepted only when evaluation, subject, contract, grader, outcome, finite
   score, evidence digest, and schema all match; any conflict fails
   permanently.
6. `cancel` stops only an exact provider job. It cannot retract an appended
   receipt.
7. `cleanup` removes only exact transient provider resources and preserves the
   admission, first result, receipt, and audit evidence.

A deterministic-local grader may collapse provider `poll` internally, but it
must still publish its first result before returning from the effectful
Activity. A lost Activity response then reads that result instead of running
the grader again.

### Evaluation decisions

| ID | Concrete question | Recommended default |
|---|---|---|
| E-1 | Which operations migrate? | Migrate persisted `Evaluate` only. Keep `RunGraders` and `world.grade()` direct and explicitly non-durable. |
| E-2 | How are graders registered? | Wiring-owned allowlist keyed by `(grader_id, implementation_version, deployment_digest)`. Reject import strings and callbacks on the durable route. |
| E-3 | Can an arbitrary nondeterministic grader be retried? | No. Require either deterministic-local certification or a durable-provider adapter with a first-result record. |
| E-4 | How is large evidence represented? | Keep `EvalReceipt` compatibility with canonical JSON up to an initial 64 KiB evidence limit; above that, put a content-addressed reference envelope in `evidence_json`. Workflow history carries only the result ref. Make the limit deployment-configured and contract-tested. |
| E-5 | Does cancellation remove a result that won the race? | No. If a valid first result or keyed receipt exists, return/settle it; cancellation only prevents future work. |
| E-6 | May an open legacy lease be imported? | Only when an exact provider operation/result identity already exists and passes the new contract. Current callback-only leases do not, so default to drain or fail closed. |

### Evaluation acceptance scenarios

#### Local contract and replay

- **Pinned head never moves:** Given a committed admission at head H, when the
  world advances before a Workflow retry, then the grader still reads H and
  the subject digest is unchanged.
- **Trial identity is preserved:** Given the same subject and contract under
  two evaluation IDs, when both run, then two trials may exist; reusing one ID
  with different identity fails before grading.
- **Callbacks stay direct-only:** Given a live callback or unregistered
  component class, when durable admission is requested, then no control row,
  Workflow, or provider effect is created.
- **Start is sole-spawn:** Given an existing start/result record, when poll,
  collect, publish, cancel, or cleanup is retried, then grader invocation count
  remains one.
- **Result-first recovery:** Given a first result and a missing/failed provider
  call, when polled, then the evaluation is ready and is not resubmitted.
- **Ambiguous start is unknown:** Given a permanent start without call or
  result evidence, when recovery runs, then status is `Unknown` and invocation
  count does not increase.
- **Keyed publication is exact:** Given a provider result, when receipt append
  succeeds but its acknowledgement is lost, then retry returns the same row;
  one `evaluation_id` row exists.
- **Outcome is revalidated:** Given corrupted, non-finite, wrong-contract, or
  oversized provider output, when collected, then no receipt is appended.
- **History stays bounded:** Given more polls than the configured history
  threshold, when the Workflow continues as new, then identity, call/result
  refs, cancellation state, and poll cursor are preserved.
- **Route epoch excludes dual ownership:** Given `draining` or `temporal`, when
  a legacy worker requests a lease, then it cannot acquire one.

#### Process-death gates

- Kill the evaluator Worker after provider submission and before call/result
  acknowledgement; a new process attaches to the exact provider operation and
  produces one result.
- Kill after first-result publication and before `EvalReceipt` append; a new
  process collects the same result and appends one row.
- Kill after keyed append and before orchestration acknowledgement; a new
  process reads the existing row and completes without grading again.
- Kill while cancellation is in progress; a replacement finishes exact
  cancellation/cleanup or returns an already published result, with no new
  submission.
- Restart against a real Temporal test server and persistent control/data
  stores, not an in-memory Workflow environment alone.

#### Paid external release gate

Use one explicitly authorized, registered external judge with a provider-side
idempotency key and unique storage namespace. Hard-kill the Worker after the
provider accepts the job, then cold-start another Worker. Evidence must show
one billable provider job, one immutable provider result, one keyed
`EvalReceipt`, unchanged subject/contract digests, exact cleanup, and no secret
or evidence payload in Workflow history. This gate is release-only. A track
that ships only deterministic-local graders has no invented paid gate; it must
still pass the persistent process-death gate.

### Evaluation drain, cutover, and deletion gates

Before switching the route epoch:

1. Freeze legacy `Evaluate` admission and snapshot every `evaluation_leases`
   row with its world/run/evaluation identity and corresponding result row.
2. Complete/archive leases whose exact keyed result already exists.
3. Let known live owners finish under the legacy route.
4. For a remaining active/expired callback-only lease with no first-result
   identity, record an operator-visible unresolved outcome. Do not infer that
   the grader did or did not run, and do not import it as retry authority.
5. Prove zero legacy owners can acquire or renew a lease after the epoch flip.
6. Run old/new parity for subject digest, contract digest, outcome, score,
   evidence, and keyed-row behavior over deterministic fixtures.
7. Retain a rollback window that can read both historical formats but admits
   new work through only one route.

Only then delete `_acquire_evaluation`, `_heartbeat_evaluation`, lease polling,
lease owner/expiry mutation, and the evaluation lease catalog methods/table.
Preserve snapshot pinning, views, grading helpers, `EvalReceipt`, result rows,
and keyed acceptance. Update every control-snapshot schema, migration, remote
catalog implementation, and architecture/documentation oracle that names
`evaluation_leases`.

### Evaluation implementation slices

| Issue/PR | Reviewable outcome | Required gate |
|---|---|---|
| E1 | Frozen portable admission, grader implementation, value-ref, result, and failure contracts plus registry interface | Canonical bytes, bounds, identity conflicts, import/architecture checks |
| E2 | Exact snapshot pin/admission record and route epoch; no Temporal execution yet | Concurrent admission and lost-start-ack tests |
| E3 | Family Workflow and split local/provider Activities with Continue-As-New | Deterministic replay and provider-double crash matrix |
| E4 | Read-first keyed receipt publication and current API adapter | Local parity over existing evaluation receipt suites |
| E5 | Persistent subprocess recovery and optional authorized paid judge proof | All process/paid scenarios above |
| E6 | Freeze/drain/import report, route flip, rollback proof | Zero renewable legacy leases and old/new read compatibility |
| E7 | Delete replaced lease/poll/heartbeat machinery and schema | Full catalog matrix, docs, architecture, and release verification |

### Evaluation source anchors

- `archetype.evaluation.models.Evaluate` currently carries component classes,
  a callback, and `StorageConfig`.
- `archetype.evaluation.contracts.subject_digest` and
  `evaluation_identity_digest` define retained identity semantics.
- `archetype.evaluation.handlers.evaluate` owns pin, grading, keyed append, and
  the lease lifecycle; only the latter lifecycle is replaced.
- `archetype.evaluation.components.EvalReceipt` and
  `archetype.evaluation.views` remain the durable evidence surface.

## Track 2 — Rollouts and simulation fan-out

### Current contract and migration scope

`run_rollout` currently reads one live base world, forks `num_episodes` worlds,
runs one bounded episode on each, optionally destroys the forks, and returns an
ordered `RolloutResult`. It implements parallel task creation, caller
cancellation draining, cleanup shielding, and nuanced multi-failure precedence
inside one Python process.

Temporal may replace that parent/child supervision. Archetype retains:

- the exact pinned source head and ancestry;
- world and run allocation, lineage, catalog status, locks, and writer epochs;
- tick computation, required projection, commit manifests, and absolute
  advancement;
- episode termination meaning and compact `EpisodeResult`/`RolloutResult`;
- installed processor/resource/hook behavior; and
- destroy semantics and `has_unsettled` checks.

The first durable route covers only a portable behavior recipe and termination
policy. A callback-bearing or dynamically configured direct rollout remains an
explicitly non-durable compatibility path unless a separate API decision
removes it. Its continued existence is not evidence of dual ownership because
the route is selected and recorded before admission; it does mean the team
must not claim that every possible rollout is migrated.

### Prerequisite: exact fork planning

Caller-chosen destination world IDs and absolute-target advancement exist, but
`fork_world` still allocates `run_id` inside the effect. A retry after partial
catalog activation can therefore neither prove exact identity nor safely
recreate the same fork.

Before a child Workflow is retryable, Archetype must provide a catalog-first
`ensure_exact_fork` operation with:

- exact source world/run/head receipt and storage fingerprint;
- caller-allocated UUIDv7 destination world and run IDs;
- expected parent, name, storage/cache fingerprints, and lineage digest;
- idempotent return of the exact existing fork;
- permanent conflict on any different identity or source head;
- reconciliation of registered-but-not-live, fenced-but-not-inserted, and
  lineage-published-but-response-lost activation windows; and
- no second fork on a lost response.

UUIDv7 identities are allocated once into a strongly consistent frozen rollout
plan; they are not recomputed as UUIDv5 values and are not freshly minted by a
retry. The plan itself is immutable, digest-bound, and externally referenced
from Workflow history. The first durable version admits only a fully published
source head with no process-local pending mutation cache. It fails before any
fork intent when that precondition is not met.

### Portable identities, inputs, and outputs

#### `PinnedWorldHeadRef`

The source reference binds storage endpoint/fingerprint, `world_id`, `run_id`,
committed tick, visibility token(s), lineage digest, and any required receipt
identity. Exact fork reads that frozen snapshot even if the parent later
advances. If the pinned snapshot is unavailable or its receipt no longer
validates, fork fails rather than silently pinning the parent's newer head.

#### `RolloutPlanRef` and child plan

The parent plan is keyed by `rollout_id` and request digest. For every child
index it freezes:

- deterministic child Workflow ID;
- exact destination world and run UUIDv7 values;
- exact fork name and storage/cache identities;
- one exact, unique child `episode_id` plus child index;
- behavior recipe and termination-policy digests;
- absolute episode start and maximum target tick;
- cleanup policy; and
- compact result slot identity.

The child index, not Python task identity, defines output ordering. Repeating
plan creation returns the exact same plan; a changed count, source, recipe,
policy, or input digest conflicts.

#### `BehaviorRecipeRef`

Cold reconstruction cannot copy live objects from a vanished source process.
A deployment-pinned recipe must identify factories for processors, resources,
hooks, and required projectors by ID, version, and digest. Wiring resolves and
installs the recipe idempotently after exact fork recovery.

Credentials and clients remain host-owned. A recipe that contains an
irreconcilable effectful `OnDestroy` hook is not eligible for automatic cleanup
until that hook has its own durable identity and reconciliation contract.

#### `TerminationPolicyRef`

Portable built-ins may express value-based termination as component type name,
schema fingerprint, field, and `terminal_all`. Arbitrary `EpisodeConfig`
component classes and `termination` callbacks require a registered,
deployment-pinned policy. The policy returns only a bounded decision over a
committed head; it does not mutate a world.

`RunConfig`, input kwargs, and metadata must be canonical finite JSON or an
external value reference. Host paths, clients, callables, and unregistered
types are rejected before plan creation.

#### Workflow and result values

`RolloutWorkflowInput` carries `rollout_id`, request digest, source-head ref,
plan ref/digest, scheduling mode, bounded concurrency/history policy, route
epoch, and schema version. It does not carry all world rows or live behavior.

Each child returns a compact `RolloutChildResultRef` that binds child index,
world/run IDs, episode ID, start/final tick, termination decision, duration,
head receipt, cleanup status, and failure reference. Large evidence remains in
Archetype storage. The parent publishes an ordered `RolloutResultRef`; the
public adapter reconstructs the existing `RolloutResult` shape from validated
compact child records.

### Required child and parent behavior

One parent Workflow owns scheduling; one deterministic child Workflow owns
each plan entry.

The child phases are:

1. `ensure_exact_fork`;
2. `install_recipe` and verify its digest;
3. inspect termination at the exact committed head;
4. advance to a Workflow-chosen absolute next target (never “N more steps”);
5. repeat inspect/advance until termination or the frozen maximum target;
6. publish the compact child result; and
7. when requested, run non-interruptible exact cleanup and record its result.

An advance retry against an already reached target is a no-op. A world beyond
the target is a permanent conflict. Continue-As-New preserves the plan,
current absolute target, child result reference, cancellation state, and
cleanup requirement.

The parent starts only the configured bounded set, waits durably, preserves
index order, requests cancellation of known children, and drains every started
child through its cleanup policy before returning. It uses a portable failure
envelope and a characterization test for current precedence; it never relies
on Python exception-object identity.

### Rollout decisions

| ID | Concrete question | Recommended default |
|---|---|---|
| R-1 | What identifies a durable retry? | Require a stable `rollout_id`/idempotency identity at the adapter boundary. Generate it once for a new call and return/reuse it; never regenerate it during recovery. |
| R-2 | How are UUIDv7 child world/run IDs chosen? | Allocate all IDs once in an Archetype rollout plan under strong consistency, then pass them to `ensure_exact_fork`. |
| R-3 | Which behavior is eligible? | Only deployment-pinned `BehaviorRecipeRef` and `TerminationPolicyRef` values. Keep callback/dynamic behavior on a recorded direct-only route. |
| R-4 | What happens to fork evidence? | Preserve `destroy_forks_on_complete` exactly. `false` retains worlds on success, failure, and cancellation; `true` drains exact cleanup on every path. Never heuristically delete by name. |
| R-5 | What are the initial fan-out/history bounds? | Default to at most 32 active children and Continue-As-New after 64 terminal child events. `parallel=False` remains one-at-a-time. Make limits deployment-configured and contract-tested. |
| R-6 | Does a zero-episode rollout remain valid? | Yes. Preserve current `num_episodes=0` as an immediate ordered empty result with no fork effects. |
| R-7 | How is multi-failure precedence defined? | Characterize the current cancellation/episode/teardown behavior in portable fixtures, then freeze it as explicit phase and observation-order rules before refactoring. |
| R-8 | Can automatic destroy run arbitrary `OnDestroy` callbacks? | No. Require a registered idempotent/reconcilable cleanup recipe, or retain the fork and report cleanup as blocked. |
| R-9 | May the first durable route fork a source with process-local pending mutations? | No. Require a clean published head and reject before plan creation. Serializing pending mutation caches is a separately versioned feature. |
| R-10 | Do all children preserve today's reused `EpisodeConfig.episode_id`? | No. Allocate a unique episode UUIDv7 per child in the frozen plan and expose that compatibility change explicitly; the rollout ID plus child index remains ordering identity. |

### Rollout acceptance scenarios

#### Local contract and replay

- **Plan creation is exact:** Given one rollout identity and pinned source, when
  plan creation races or repeats, then every child has the same UUIDv7
  world/run pair and digest; a changed input conflicts.
- **Lost fork response is safe:** Given catalog activation succeeded but the
  response was lost, when `ensure_exact_fork` retries, then it returns the same
  world/run and creates no second lineage record.
- **Partial activation reconciles:** Given each catalog/fence/lineage/live
  insertion crash window, when recovery runs, then it either completes the
  exact planned fork or fails closed with one durable record.
- **No tick overshoot:** Given an advance response is lost, when the Activity
  retries, then the child stops at the same absolute target and never commits
  an extra tick.
- **Frozen source survives parent movement:** Given all fork intents reference
  one clean pinned head, when the parent advances or is destroyed after plan
  commit, then every child still forks the same frozen snapshot or fails
  closed; no child re-pins a moving parent.
- **Cold recipe parity:** Given a process loss, when a child resumes, then the
  registered processors/resources/hooks and termination result match the
  original recipe digest.
- **Ordered results under parallel completion:** Given children finish out of
  order, when the parent returns, then results remain in child-index order.
- **Cancellation drains started work:** Given partial fan-out, when the caller
  cancels, then no new child starts and every started child reaches its exact
  configured cleanup/retention terminal state.
- **Cleanup failure precedence is stable:** Given episode, caller cancellation,
  and cleanup failures in the characterized combinations, when the parent
  fails, then the same portable primary error and ordered additional failures
  are returned.
- **History is bounded:** Given more children/ticks than one run threshold,
  when parent and child Workflows continue as new, then no identity, result,
  cancellation, or cleanup obligation is lost.
- **Parent is unchanged:** Every scenario proves the pinned base world's run,
  tick, rows, writer epoch, and lineage remain unchanged.

#### Process-death gates

- Kill after only a prefix of child Workflows is started; replacement starts
  only the missing planned children.
- Kill after a fork is cataloged but before the Activity response; replacement
  recovers the same world/run pair.
- Kill during an episode; replacement cold-resumes the same child with a higher
  writer epoch and advances to the exact target without duplicate ticks.
- Kill after child result publication but before parent acknowledgement;
  replacement reuses the result and completes exact cleanup once.
- Kill during cancellation/cleanup; replacement drains every started child and
  preserves all retained forks when cleanup policy is false.
- Run against a real Temporal test server and persistent storage; verify one
  visible row set and visibility token per committed tick.

#### External and paid applicability gate

The baseline Rollout contract has no inherently paid provider, so do not spend
GPU/model funds merely to label it external. Its required external gate uses a
real Temporal service plus the supported remote control/data catalogs and
performs cold recovery with the source process unavailable. If a shipped
behavior recipe invokes a paid simulator or model, that recipe adds a
separately authorized gate proving one provider operation per planned child,
bounded concurrency/spend, exact cancellation, and no duplicate ticks or
provider work.

### Rollout drain, cutover, and deletion gates

Current process-local rollouts have no durable parent plan that can safely be
reconstructed after process loss. Therefore:

1. Before cutover, add a durable route epoch plus active-admission/child
   ownership record to the legacy adapter, or schedule a maintenance freeze
   with another authoritative process-task count. Names and the command audit
   are not a rollout ledger.
2. Switch admission to `draining` and join every live process-owned rollout.
3. Inventory world-catalog children created during the drain using captured
   runtime evidence. Names alone are not import or deletion authority.
4. Preserve and report any ambiguous/orphan fork; never infer ownership and
   destroy it by prefix.
5. Admit no old rollout into Temporal unless an exact source head, full child
   UUIDv7 world/run plan, recipe digest, and per-child status can be proven.
6. Run parity for serial/parallel order, zero count, termination, failure
   precedence, cancellation, retained forks, and destroy-on-complete.
7. Flip the route epoch only after no legacy task can start another fork.

After parity, remove the replaced task fan-out, shielding, polling, and
exception-object supervision from the portable path. Retain `run_episode`,
absolute advancement, world lifecycle/storage authority, result models, and
the explicitly direct-only compatibility path if R-3 preserves it. Full
deletion of process-local rollout supervision requires either portable support
for every supported public config or a separately approved deprecation of the
non-portable route.

### Rollout implementation slices

| Issue/PR | Reviewable outcome | Required gate |
|---|---|---|
| R1 | Frozen portable source, recipe, termination, plan, result, and failure contracts | Canonicalization, bounds, and current failure-precedence characterization |
| R2 | Catalog-first rollout plan plus `ensure_exact_fork` with caller world/run IDs | Every partial-activation/lost-response window |
| R3 | Idempotent cold recipe installation and portable termination inspection | Same-process and reconstructed-process behavior parity |
| R4 | Child Workflow using exact fork, absolute advancement, result, and cleanup | Deterministic replay and child hard-kill matrix |
| R5 | Parent fan-out Workflow with bounded parallelism, ordering, cancellation, and Continue-As-New | Partial fan-out and multi-failure tests |
| R6 | Existing API adapter, route epoch, and local parity | Existing episode/rollout integration suite plus portable fixtures |
| R7 | Persistent external recovery and optional recipe-specific paid proof | All process/external scenarios above |
| R8 | Drain report, route flip, then narrowly delete replaced supervision | Zero legacy starters, orphan report, full verification |

### Rollout source anchors

- `archetype.world.models.EpisodeConfig` contains live component/callback
  fields; `RolloutConfig` contains the logical ID, fan-out, order, and cleanup
  choices to preserve.
- `archetype.world.simulation.run_rollout` contains the process-local fan-out,
  cancellation drain, cleanup, and exception precedence to characterize.
- `archetype.world.simulation.advance_world_to_tick` is the retry-safe
  absolute advancement primitive.
- `archetype.world.lifecycle.fork_world` accepts a destination world ID but
  still mints the run ID and copies process-local behavior inside the effect.

## Track 3 — Remaining hosted Physical-AI Activities

### Current contract and migration scope

Hosted Physical AI already has the strongest family data contract in this
packet. One world-scoped provider operation binds a canonical Arrow request
batch, complete trajectory, derived episode results, and manifest. Provider
start/result records live in a named Modal Dict, payloads live in a named
Volume, and a later committed `HostedEpisodeObservation` settles the generic
Activity.

The current migration gap is orchestration shape:

- generic Activity claims, attempts, fences, leases, and a process-local worker
  still supervise the family;
- `ModalHostedEpisodeProvider.execute` combines start, wait, and recovery;
- the host records `FunctionCall.object_id` only after `spawn` returns; and
- the remote function does not yet self-register that call identity before
  episode or hardware effects.

Temporal replaces only this supervision. Physical AI retains:

- `physical_ai.hosted_episode/v1` Arrow schemas and digest domains;
- world-scoped provider operation, episode, step, and result identities;
- permanent start markers, call/result records, and reconciliation meaning;
- complete-trajectory and manifest validation;
- trajectory, frame, episode-result, and manifest publication;
- `HostedEpisodeIntent` and `HostedEpisodeObservation` semantics; and
- exact required-projector admission, observation staging, and ECS settlement.

The direct in-process per-step Physical-AI path is not part of this migration.

### Portable identities, inputs, and outputs

#### Workflow identity and input

One Workflow represents one committed hosted Activity batch, keyed by family,
world ID, activity ID/provider operation ID, and provider deployment namespace.
Its input contains:

- exact Activity kind, world/activity IDs, and Temporal execution identity;
- `hosted_episode_provider_operation_id(world_id, activity_id)`;
- canonical request ref, digest, byte size, Arrow media type, and episode count;
- exact committed source receipt identity;
- provider protocol epoch plus deployment/namespace digest;
- bounded poll/history policy, route epoch, and schema version; and
- optional exact call ref and poll cursor on Continue-As-New.

The existing request Arrow bytes, `StorageConfig`, `ModalHostedEpisodeConfig`,
credentials, and trajectories remain outside history. Wiring resolves a
deployment-pinned provider config from its digest.

#### Provider deployment and call reference

Production configuration must bind the exact workspace, environment, App,
deployed Function version/object ID, preprovisioned Dict, preprovisioned Volume,
protocol epoch, image/deployment digest, and secret references. Production
lookup uses `create_if_missing=false`; worker startup verifies every durable
object and Function receipt before polling work.

`HostedEpisodeJobRef` binds provider operation and request digests,
deployment/namespace digest, protocol epoch, and exact
`FunctionCall.object_id`. The remote controller must atomically self-register
this reference before it resets an environment, invokes a policy, moves
hardware, writes a trajectory, or performs another episode effect. Competing
calls fail the identity fence before that boundary.

#### Result and observation

The provider publishes Volume payloads before one immutable Dict result index.
The bounded collected result references the existing canonical request,
trajectory, episode-results, and manifest values plus completeness counts. The
Workflow never carries those Arrow bytes.

Collection re-runs `validate_hosted_provider_result`; observation staging then
reconstructs the exact existing `HostedEpisodeObservation`. A later committed
receipt remains the only successful ECS settlement proof.

### Required phase behavior

The family provider protocol becomes:

1. `start`: read result/call/start in that order; if absent, install the
   permanent start and spawn once; return an exact self-registered call ref or
   `Unknown`.
2. `poll`: read the immutable result index first, otherwise reattach to the
   exact call; never spawn.
3. `collect`: read and validate the exact four canonical payloads; never wait,
   execute, cancel, or clean.
4. `cancel`: cancel only the exact call after validating request and deployment
   identity. A start without a call ID is `Unknown` and touches no mutable
   provider name.
5. `cleanup`: release only exact per-job transient resources; preserve start,
   call, result index, canonical payloads, and audit evidence.

The Workflow uses result-first polling and bounded Continue-As-New, then records
the result through the claim-free Activity settlement index and stages the
existing observation. Workflow cancellation runs exact cancel and cleanup; it
does not erase a provider result that already won the race.

Unlike Mission, this path has no author/critic role, sandbox cohort, redaction
policy, or Git publication. Unlike a small Mission JSON result, trajectory and
frame evidence can be large and remains in Physical-AI/storage artifacts.
Unlike a simulator, real hardware movement cannot be compensated by Temporal.

### Physical-AI decisions

| ID | Concrete question | Recommended default |
|---|---|---|
| P-1 | What is the first paid cutover target? | A seeded Modal-hosted simulator batch using the existing Arrow contract. Keep real-hardware activation disabled until a separate safety and cancellation review. |
| P-2 | Is one request batch atomic or may partial episodes settle? | Preserve current all-or-nothing batch completeness. A partial trajectory cannot build a manifest or ECS observation and remains `Unknown`. |
| P-3 | One Workflow per batch or per episode? | One Workflow and one provider call per committed Activity batch, preserving the existing operation/manifest contract. Episode fan-out is a separate future design. |
| P-4 | What does cleanup delete? | Exact ephemeral per-call resources only. Preserve shared Dict/Volume objects and immutable result evidence; release-proof teardown may delete its unique namespace after evidence export. |
| P-5 | What happens when cancellation races completion? | A valid first result wins and is collected/settled. Otherwise cancel the exact call and record cancellation; never replay the episode. |
| P-6 | Can old claim attempts be imported? | Prefer drain. Import only when request, execution owner, deployment digest, exact call ID, and/or first result all reconcile; a marker without call evidence is `Unknown`. |

### Physical-AI acceptance scenarios

#### Local contract and replay

- **Canonical request identity survives Workflow encoding:** Given a request
  batch, when encoded, stored, referenced, and decoded in another process, then
  exact Arrow bytes, digest, operation ID, trial coverage, and config quarantine
  are unchanged.
- **Remote self-registration closes response loss:** Given Modal accepted a
  call before the host received `spawn`, when the controller starts, then it
  registers its exact call ID before any episode effect and replacement polling
  reattaches to it.
- **Duplicate calls are fenced:** Given two accidental controller calls, when
  both self-register, then only the immutable winner may reset an environment,
  invoke a policy, touch hardware, or publish payloads.
- **Start is sole-spawn:** Given any existing start, call, or result record,
  when poll/collect/cancel/cleanup runs, then remote spawn count is unchanged.
- **Result-first poll:** Given a complete result index and a failed/expired
  Function output, when polled, then status is ready from the index.
- **Terminal call without result is unknown:** Given a completed/failed call
  and no result index, when polled, then no episode is restarted.
- **Collect is exact and read-only:** Repeated collection returns the same
  validated refs/counts and performs no provider or cleanup effect.
- **Partial batch cannot settle:** Given one missing or malformed episode,
  trajectory, result row, or manifest binding, when collected, then no Activity
  result or observation is published.
- **Cancellation is exact:** A wrong world, activity, request, deployment,
  namespace, protocol, or call ID cancels nothing.
- **Cleanup preserves evidence:** Repeated cleanup leaves immutable start/call/
  result records and canonical payloads readable.
- **Claim-free exact settlement:** Given a ready result, when observation is
  staged and committed, then the exact digest settles once with zero legacy
  attempts or fences.
- **History stays bounded:** Continue-As-New preserves call ref, result ref,
  cursor, cancellation state, and ECS settlement obligation.

#### Process-death gates

- Kill the Worker after remote self-registration and during episode execution;
  replacement reattaches to the same call and performs no second reset/policy
  run.
- Kill after result-index publication but before claim-free result recording;
  replacement collects the same four payloads and records one result.
- Kill after result recording but before observation staging; replacement
  restages the exact observation without provider work.
- Kill after staging and before tick commit; replacement idempotently restages
  and commits one observation.
- Kill after observation commit and before settlement; required projection
  settles the exact receipt with no extra tick.
- Kill during exact cancellation/cleanup; replacement finishes the same call's
  terminal path and preserves evidence.
- Reconstruct the Activity index, world, provider adapter, Worker, and value
  store in a fresh process against a real Temporal test server and persistent
  storage.

#### Paid external release gate

Use an explicitly authorized deployed Modal Function, preprovisioned Dict and
Volume, pinned image/deployment receipt, unique R2/Iceberg namespace, and one
small seeded GPU episode batch. Hard-kill the Temporal Worker after the remote
controller self-registers but before completion, then cold-start a replacement.

Required evidence is one `FunctionCall.object_id`, one Modal call, one episode
execution per admitted trial, one immutable result index, exact four-payload
validation, one committed `HostedEpisodeObservation`, one Activity settlement,
no tick overshoot, and exact cleanup. The release harness may delete its unique
Modal/R2 resources only after persisting this evidence. Prior paid
non-Temporal A7b evidence does not satisfy this Worker-replacement gate.

### Physical-AI drain, cutover, and deletion gates

1. Freeze new legacy hosted claims while leaving result delivery and
   settlement active.
2. Inventory every `physical_ai.hosted_episode` admission, execution owner,
   attempt/lease, request digest, provider operation, start marker, call record,
   first-result index, pending result, observation, and settlement receipt.
3. Drain known live claims. Reconcile completed provider results into existing
   observations before changing ownership.
4. Import only an exact, fully matching call/result into a prebound Workflow.
   A lease alone, confirmed absence alone, or a permanent start without call
   identity is not import authority.
5. Prove old workers cannot claim after the Temporal route epoch and new
   Workflows cannot create legacy attempts.
6. Run old/new parity over canonical payloads, recovery classifications,
   observation rows, settlement, cancellation, and cleanup.
7. Keep a read-compatible rollback window; rollback may resume only work owned
   by its recorded route and may not steal a Temporal admission.

After the gate, delete the Physical-AI claim/lease worker choreography and its
family-specific attempt/retry plumbing. Preserve the exact-receipt projector,
intent/observation Components, canonical value store, provider markers,
first-result/call records, reconciliation/validation, observation staging,
settlement, and `has_unsettled`. Generic claim/attempt tables are removed only
after every remaining Activity family has completed its own cutover.

### Physical-AI implementation slices

| Issue/PR | Reviewable outcome | Required gate |
|---|---|---|
| P1 | Frozen Workflow/job refs and split provider protocol over existing Arrow contracts | Identity mismatch, sole-spawn, result-first, and read-only collect tests |
| P2 | Deployment-pinned remote self-registration and duplicate-call effect fence | All spawn-before-host-record crash windows with provider double |
| P3 | Family Temporal Workflow/Activities with bounded history and exact cancellation/cleanup | Replay and local crash matrix |
| P4 | Claim-free Activity admission/result/observation adapter | Exact ECS settlement with zero claims/attempts |
| P5 | Persistent subprocess recovery through a reconstructed live world/projector | All process scenarios above |
| P6 | Authorized paid Modal/R2 proof and receipt | One call, one batch, one observation, exact cleanup |
| P7 | Freeze/drain/import report and route flip | Zero provider-bound legacy claims and parity |
| P8 | Delete only replaced Physical-AI claim/lease supervision | Full Physical-AI, Activity, architecture, docs, and release verification |

### Physical-AI source anchors

- `archetype.physical_ai.hosted_episode` defines the retained canonical Arrow
  contract, identities, digest domains, and completeness checks.
- `hosted_activity_contracts` defines retained intent/observation and provider
  recovery meaning.
- `hosted_activities` contains the claim/lease worker choreography to replace
  and the projector/settlement behavior to preserve.
- `hosted_modal.ModalHostedEpisodeProvider.execute` currently combines phases;
  `ModalNamedHostedEpisodeRuntime.spawn` returns the call before the host writes
  the call record, which is the self-registration gap.
- `hosted_workflow.run_hosted_episode` is the current awaited public behavior
  the first adapter must preserve.

## Track 4 — AutoResearch

### Current contract and migration scope

AutoResearch currently holds one process-local keyed lock for a ledgered
experiment, attaches or creates its lab world, records a `RUNNING` Run, invokes
live candidate/evaluator callbacks around `run_rollout`, records `SUCCEEDED` or
`FAILED`, and advances `BranchHead` when the score clears the configured
threshold. A second invocation resumes from the ledger's next terminal
iteration; an active Run fails closed.

Temporal replaces:

- the process-local same-experiment admission lock;
- iteration scheduling, waits, retry/cancellation supervision, and recovery;
- orchestration of candidate, Rollout, and evaluator children; and
- the in-memory iteration loop and correctness-significant callback timing.

Research retains:

- experiment/config identity and deterministic run/iteration identity;
- lab-world `Experiment`, `Run`, `Result`, and `BranchHead` facts;
- candidate and evaluation evidence meaning;
- comparison/frontier policy and current-head acceptance;
- budget reservations and spend evidence; and
- idempotent terminal/head settlement against an exact committed ledger head.

A selected candidate is an experiment result, not permission to merge, deploy,
or promote anything. Mission publication and product promotion remain separate
authorities.

### Coordinator and portable contracts

#### Coordinator identity

One long-lived coordinator Workflow is keyed by storage fingerprint,
`experiment_id`, and the immutable experiment configuration digest. Display
name is not identity. A conflicting base world, config, evaluator, rollout, or
frontier digest fails closed rather than joining the existing coordinator.

The lab world and run are allocated exactly once before iteration effects. The
admission record contains explicit caller-allocated UUIDv7 world/run IDs and a
reconcilable create/attach contract; name lookup alone is insufficient after a
lost create response.

#### `AutoResearchWorkflowInput`

The initial immutable input contains or references:

- experiment ID/name and exact base `PinnedWorldHeadRef`;
- exact lab world/run identity and storage endpoint/fingerprint;
- canonical config digest and schema version;
- registered `CandidatePolicyRef`, `ResearchEvaluatorRef`,
  `RolloutContractRef`, and `FrontierPolicyRef`;
- default episode/rollout value refs;
- route epoch, history bounds, and initial durable cursor; and
- no live callback, runtime, world handle, component class, or client.

The current semantic config identity remains stable. Invocation-only count and
observer choices do not silently change the experiment identity.

#### `RunIterationsRequest`

Clients submit an idempotent durable Update with:

- caller-stable `request_id`;
- positive iteration `count`;
- immutable `ResearchBudgetRef`;
- optional bounded client correlation metadata; and
- expected experiment/config digest.

Duplicate request ID plus identical content joins or returns the same result;
different content conflicts. Requests for one experiment are FIFO by accepted
Workflow event order. Different experiment coordinators remain concurrent.

The existing awaited API waits for its Update result. A later handle/cursor API
may expose the same coordinator without changing Workflow correctness.

#### Iteration plan and intent

Before any candidate effect, Research commits a versioned iteration intent
that binds:

- deterministic iteration and `run_id = "{experiment_id}:iter{n}"`;
- Update request ID and exact prior lab/head receipt;
- candidate policy/operation identity and planned candidate world identity;
- Rollout plan/Workflow identity;
- evaluator operation identity;
- frontier policy and budget reservation refs; and
- current phase/result references.

This may be a new component adjacent to `Run` or a versioned extension, but it
must be an Archetype ledger fact. Temporal history alone cannot become the
research experiment's source of truth.

#### Registered subcontracts

`CandidatePolicyRef` identifies a built-in no-op/base candidate or a registered
versioned adapter with durable start/reconcile/result semantics. A candidate
adapter that creates a world or external artifact must use caller-chosen
identities and persist a first result before returning.

`RolloutContractRef` resolves to the completed durable Rollout contract and a
frozen configuration/recipe digest. AutoResearch does not call the old
process-local fan-out inside one Activity.

`ResearchEvaluatorRef` returns a finite score, evaluator identity, bounded
evidence ref, and metadata ref. It may adapt the completed Evaluation provider
pattern, but Research's `EvaluationResult(score, evaluator, evidence,
metadata)` is not automatically the same product contract as framework
`EvalReceipt(outcome, score, ...)`.

`FrontierPolicyRef` is initially the built-in scalar policy
`score > incumbent + improvement_threshold`, versioned as a deployment-pinned
contract. The frontier Activity reads the exact current lab head and atomically
settles terminal Run, Result, budget, and optional BranchHead advancement in
Archetype. A stale expected head retries the read/decision; it does not apply a
decision computed against another incumbent.

#### Budget contract

`ResearchBudgetRef` points to an immutable approved envelope with integer
ceilings for iterations, episodes, ticks, provider launches, and
provider-specific spend units. Before starting a child effect, Research
strongly reserves its declared worst-case units; completion settles actual
receipts and releases only unused units. Temporal decides when to ask for a
reservation but is not the spend ledger.

Budget exhaustion is a normal terminal request result. It starts no additional
candidate, rollout, or evaluator and survives Continue-As-New. Do not claim a
hard currency cap for a provider that cannot reserve or bound its operation;
such an adapter is ineligible for a hard-budget durable route.

#### Result and observer contract

Each iteration publishes a bounded `ResearchIterationResultRef` over candidate,
Rollout, evaluation, terminal Run/Result receipt, and optional new head receipt.
The Update returns an `AutoResearchRequestResultRef` with completed count,
initial/final score, iteration refs, lab world ID, terminal reason, and next
cursor. Large evidence stays in its owning family.

`on_iteration` cannot execute inside Workflow correctness. It becomes a client
observer over durable iteration cursors. Delivery is at least once; clients
deduplicate by experiment ID plus iteration/run ID. Observer failure never
rolls back or blocks a committed frontier decision.

### Required coordinator behavior

For each accepted Update, the coordinator:

1. validates or attaches the exact lab and current head;
2. reserves budget and commits the next iteration intent/`RUNNING` fact;
3. starts or reattaches the exact candidate operation;
4. starts or reattaches the exact Rollout child Workflow;
5. starts or reattaches the exact evaluator operation;
6. asks Research to settle terminal Run/Result and the frontier decision
   atomically against the expected ledger head;
7. publishes the iteration cursor/result and settles budget; and
8. repeats until request count, cancellation, or budget termination.

A same-coordinator active iteration always reattaches. A foreign Workflow or
different request identity fails closed. Continue-As-New preserves the active
request, bounded FIFO queue, iteration plan, child refs, budget refs, head ref,
cancellation state, and cursor.

Cancellation is request-scoped by default. It prevents new iterations,
cancels exact active children where their family permits, drains required
cleanup, and preserves all committed Runs, Results, world ticks, spend
receipts, and head decisions. It does not terminate the experiment coordinator
or roll back an already committed improvement.

### AutoResearch decisions

| ID | Concrete question | Recommended default |
|---|---|---|
| A-1 | What happens to `record_to_ledger=false`? | Keep it direct-only and explicitly non-recoverable for compatibility in the first cutover. Do not route it through the durable coordinator or count it as migrated. Open a later deprecation/ledger-requirement decision. |
| A-2 | What scopes one coordinator? | `(storage_fingerprint, experiment_id, config_digest)`, with explicit base and lab identities validated on every admission. |
| A-3 | How do repeated awaited calls compose? | Each call sends an idempotent FIFO `RunIterationsRequest`; the existing API awaits that request only, not the coordinator lifetime. |
| A-4 | Which candidate preparers are allowed? | Built-in base/no-op or registered versioned durable adapters. Live `prepare_candidate` callbacks stay direct-only. |
| A-5 | Is the Research evaluator the framework Evaluation operation? | Keep distinct public meanings; share the registered provider mechanics and allow an explicit adapter where a framework `EvalReceipt` can produce the Research score contract. |
| A-6 | Which frontier policy ships first? | The current scalar threshold policy as a pinned `scalar-greater-than/v1` implementation. Defer arbitrary/Pareto policies to separate contracts. |
| A-7 | What are queue/history bounds? | Accept at most 64 queued requests and Continue-As-New after 32 terminal iterations, preserving the active request and cursor. Make limits deployment-configured. |
| A-8 | What does cancellation target? | One Update request. Keep the coordinator available for later requests; add explicit experiment termination only as a separate API. |
| A-9 | What represents an unreconcilable legacy `RUNNING` row? | Add an explicit terminal `UNKNOWN`/migration outcome with evidence and no head advance rather than falsely claiming success or failure. This schema choice must be approved before drain tooling. |
| A-10 | Are provider-spend budgets hard guarantees? | Only for adapters that declare and reserve a worst-case integer cost before launch. Otherwise label spend accounting advisory and reject hard-cap mode. |

### AutoResearch acceptance scenarios

#### Local contract and replay

- **Configuration identity is stable:** Given the same current semantic config,
  when encoded by another process, then its existing frozen digest is
  unchanged; invocation count and observers are excluded.
- **Same experiment queues:** Given two different request IDs for one
  coordinator, when both are accepted, then they run FIFO at contiguous
  iterations and the second observes the first's committed head.
- **Duplicate Update joins:** Given one request ID repeated with identical
  content, when replayed before or after Continue-As-New, then it causes no new
  iteration; changed content conflicts.
- **Unrelated experiments stay concurrent:** Two coordinator identities may
  enter candidate work simultaneously without a global lock.
- **Intent precedes effects:** Given a candidate provider is about to start,
  then the exact Run/iteration/candidate/Rollout/evaluator/budget intent is
  already committed to the lab.
- **Active iteration reattaches:** Given process loss during any child, when the
  same coordinator resumes, then it attaches to exact child identities and
  does not create another candidate, fork, or evaluation.
- **Foreign identity fails closed:** A different Workflow, config, base, lab,
  policy, provider namespace, or request digest cannot adopt an active Run.
- **Frontier advances once:** Given a successful result and a lost settlement
  response, when retried, then one terminal Run/Result exists and BranchHead
  advances at most once against the exact prior head.
- **Stale frontier is re-evaluated:** Given another accepted request advances
  the head first, when settlement sees a stale expected receipt, then it reads
  the new head and applies the pinned policy once; it never writes a stale
  decision.
- **Budget stops before launch:** Given the next child would exceed any bound,
  when scheduling continues, then the request ends budget-exhausted with no
  new provider operation or fork.
- **Observer is non-authoritative:** Given observer delivery fails or repeats,
  when the client reconnects at a cursor, then ledger/head state is unchanged
  and each committed iteration remains discoverable.
- **Callbacks stay direct-only:** A live evaluator, preparer, observer, or
  callback-bearing termination policy cannot enter durable admission.
- **History stays bounded:** Continue-As-New preserves queued request order,
  active iteration, child/result/budget refs, cancellation, head, and cursor.

#### Process-death gates

- Kill after lab allocation/attach and before first Run intent; replacement
  uses the same lab world/run and creates one genesis.
- Kill after `RUNNING`/intent commit but before candidate start; replacement
  starts the planned candidate once.
- Kill after candidate result and before Rollout start; replacement reuses the
  candidate and starts one planned Rollout.
- Kill during Rollout or Evaluation; replacement reattaches to their exact
  child Workflows/provider jobs.
- Kill after evaluation result but before terminal/head settlement;
  replacement records one Result and advances the head at most once.
- Kill after settlement but before Update acknowledgement; client retry returns
  the same request result and cursor.
- Kill across Continue-As-New with a queued second request and exhausted budget;
  replacement preserves queue order, head, and terminal budget outcome.
- Run against a real Temporal test server and persistent lab/base storage with
  the original process unavailable.

#### Paid external release gate

After Evaluation and Rollout pass their own release gates, run the smallest
authorized experiment that exercises the paid adapters intended for release:
recommended `count=2`, one episode per iteration, the minimum tick bound, and
an immutable worst-case provider budget. Hard-kill the coordinator Worker
between child selection and result ingestion.

Evidence must show the approved number of candidate/provider launches, one
Rollout and one evaluator result per iteration, no duplicate paid operation,
contiguous Run IDs, exact budget reservations/settlements, one head advance per
qualifying iteration, preserved queued request/cursor across Continue-As-New,
and exact child cleanup. If the release configuration contains no paid adapter,
use persistent external storage/Temporal only and do not manufacture spend.

### AutoResearch drain, cutover, and deletion gates

1. Freeze new ledgered legacy admissions per experiment and join every live
   process-owned handler.
2. Inventory lab worlds, config digests, every `RUNNING` Run, candidate world,
   Result, BranchHead, and any child/provider evidence.
3. Existing `RUNNING` rows lack durable candidate/Rollout/evaluator identities
   in the current schema and therefore are not automatically importable.
4. Drain a known live owner; otherwise require operator reconciliation and the
   approved A-9 terminal migration outcome. Never skip or overwrite the active
   row and never advance its head without evidence.
5. Ledgerless invocations have no shared experiment ledger or import path; let
   admitted calls finish/cancel before route shutdown and retain the explicit
   direct-only disposition.
6. Create/attach the deterministic coordinator at the exact next ledger
   iteration, then prove the old admission map cannot accept the same
   experiment.
7. Run parity for config collision, contiguous IDs, initial/incumbent score,
   candidate provenance, failure evidence, threshold decisions, retained/
   destroyed forks, authorization/quota entry, and current result shape.
8. Retain a rollback reader for old ledgers; rollback admission may not adopt a
   Temporal-owned active iteration.

After parity, delete `AutoResearchAdmissions` and the replaced ledgered
in-memory iteration/single-flight loop. Preserve Research Components, config
identity, views, candidate/evaluation/frontier meaning, lab settlement, and the
direct ledgerless implementation if A-1 retains it. Do not claim complete
AutoResearch migration or delete all process-local looping while that
compatibility path remains supported.

### AutoResearch implementation slices

| Issue/PR | Reviewable outcome | Required gate |
|---|---|---|
| A1 | Product decisions A-1/A-9 plus frozen coordinator, Update, registry, budget, iteration, result, and failure contracts | Canonicalization, bounds, old config-digest compatibility, architecture |
| A2 | Exact lab admission/create-or-attach, iteration intent, budget reservation, and idempotent terminal/head settlement | Concurrent CAS, lost-response, and no-head-advance failure tests |
| A3 | Coordinator Workflow with FIFO idempotent Updates, request cancellation, cursors, and Continue-As-New | Deterministic replay, duplicate/conflict, and bounded-history tests |
| A4 | Registered candidate adapter and integration with durable Rollout/Evaluation children | Full local per-phase crash matrix |
| A5 | Awaited public adapter plus non-authoritative observer cursor | Existing runtime/admission/authorization parity |
| A6 | Persistent subprocess and authorized external/paid recovery proof | All process/external scenarios above |
| A7 | Legacy freeze/inventory/reconciliation report and route flip | Zero active import-ambiguous Runs and no legacy same-experiment admission |
| A8 | Delete only replaced ledgered admission/loop code | Research, Rollout, Evaluation, architecture, docs, and release verification |

### AutoResearch source anchors

- `archetype.research.models.AutoResearchConfig` defines retained semantic
  configuration, while `AutoResearch` currently carries the non-portable
  evaluator, candidate preparer, and observer callbacks.
- `archetype.research.handlers.AutoResearchAdmissions` is the process-local
  single-flight mechanism to replace.
- `handlers._config_identity`, `_attach_ledger`, `_record_running`, and
  `_record_terminal` contain identity/settlement meaning to preserve and make
  idempotent.
- `archetype.research.views.next_iteration` correctly fails closed on an active
  Run today; migration must add explicit reconciliation rather than bypass it.
- `Experiment`, `Run`, `Result`, and `BranchHead` remain Research-owned ledger
  facts.

## Cross-track definition of done

No follow-up track is complete until its issue checklist proves all of the
following with current-state evidence:

- [ ] Every public durable input is canonical, bounded, JSON/reference-only,
  versioned, and revalidated after recovery.
- [ ] Every logical, Workflow, child, provider, result, world, run, and request
  identity is stable and conflict-checked.
- [ ] Admission atomically records Temporal ownership before provider effects,
  and the family route epoch prevents dual admission.
- [ ] Only `start` can spawn; poll is result-first; collect is read-only;
  cancellation and cleanup use exact identities.
- [ ] Ambiguous provider or lifecycle outcomes fail closed as `Unknown` and are
  never converted into retry authority by timeout alone.
- [ ] Workflow replay and Continue-As-New tests prove bounded history without
  losing cancellation, queue, cursor, result, cleanup, or settlement state.
- [ ] Same-process local parity covers all current semantic fixtures, including
  failure and cancellation precedence.
- [ ] A real Temporal server plus persistent stores survives hard process death
  at every named effect/settlement window.
- [ ] Applicable paid external gates are explicitly authorized, minimally
  bounded, receipt-backed, and never ordinary CI.
- [ ] Existing legacy work has a checked-in inventory and a per-record
  drain/archive/import/unknown disposition; no heuristic adoption or deletion.
- [ ] Rollback reads both historical formats but cannot create two active
  owners.
- [ ] Deletion search and architecture checks prove only replaced supervision
  was removed; ECS receipts, settlement, storage authority, provider
  reconciliation, and supported direct-only paths remain.
- [ ] Documentation, formatting, lint, type, focused integration, package, and
  release gates appropriate to the changed packages pass.

## Issue and PR policy

Each row in the four implementation-slice tables is one issue and normally one
reviewable PR. A PR may be smaller; it should not combine two rows merely to
reduce issue count. In particular:

- contract/registry changes land before Workflows;
- idempotent Archetype primitives land before callers retry them;
- local/process proofs land before a route flip;
- a paid proof records receipts but does not hide unrelated fixes;
- drain/inventory is reviewable before deletion; and
- deletion is the final family PR, not mixed with the cutover that establishes
  parity.

The Mermaid Mission dogfood PR belongs to the Mission release and remains the
single user-requested external PR for that migration. These follow-up slices
are a planning backlog; this packet does not open or authorize additional PRs.
