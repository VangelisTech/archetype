# Temporal Orchestration Migration Plan

Date: 2026-08-30  
Status: active  
Decision record: [Temporal responsibility audit](../reports/temporal-responsibility-audit.md)

## Objective

Make Temporal Archetype's single durable orchestration substrate while keeping
Archetype authoritative for ECS state, committed receipts, world lineage,
storage visibility, provider reconciliation, and exact result settlement.

The migration is complete only when a real agent Mission can survive worker
loss, reattach to provider work without duplicating it, settle its result into
an exact later ECS receipt, and publish an authorized pull request.

## Non-negotiable boundary

Temporal owns:

- workflow lifecycle, waiting, timers, retry policy, cancellation, and worker
  replacement;
- durable sequencing of long-running and effectful operations;
- orchestration progress and bounded JSON/reference state.

Archetype owns:

- ECS commands, tick computation, committed receipts, projections, and
  manifest-atomic settlement;
- world catalog state, lineage, writer epochs, locks, resume reconstruction,
  and storage visibility;
- stable provider operation identities, reconciliation, bounded result
  references, and fail-closed unknown outcomes;
- the strongly consistent `has_unsettled(world_id)` lifecycle gate until a
  per-world Workflow can prove equivalent admission consistency.

There must never be two active admission or lifecycle authorities for the
same operation.

## Delivery plan

### Phase 1 — Shared orchestration boundary

Deliverables:

- Introduce provider-neutral `archetype.orchestration.temporal` runtime
  helpers and JSON/reference-only contracts.
- Keep domain Workflows in their owning packages.
- Compose Temporal clients and Worker lifetimes from application wiring.
- Prevent domain code from importing Temporal SDK types outside adapter
  boundaries.

Gate:

- Architecture checks prove the orchestration adapter is infrastructure, not
  a dependency inversion through `archetype.activities`.

### Phase 2 — Idempotent world recovery proof

Deliverables:

- A trivial ECS Activity is admitted from an exact receipt, executed through
  Temporal, observed in a later receipt, and settled.
- Fork is refused while that Activity remains unsettled.
- A child process owns a forked world, commits progress, and is hard-killed.
- A replacement process resumes the same world and run with preserved
  lineage, a higher writer epoch, an unchanged parent, one visible row set per
  tick, and no overshoot.
- Explicit destroy runs `OnDestroy` exactly once; a destroyed world refuses
  resume.
- Add Workflow-chosen fork destination IDs and an idempotent
  `advance_world_to_tick(absolute_target)` operation.

Gate:

- The proof passes against persistent storage and a real Temporal test server
  after process death, not merely same-process Worker replacement.

### Phase 3 — Generic Activity execution cutover

Deliverables:

- Temporal replaces generic queues, claims, leases, attempt fences, expiry
  scans, retry timers, and process-local recovery scans.
- Archetype retains a slim admission/settlement oracle containing the source
  receipt, operation identity, bounded result reference, observation receipt,
  and settlement state.
- Family Activities preserve provider identities and reconciliation logic.

Gate:

- Existing committed-receipt and lifecycle-invariant suites pass without a
  second lease or retry state machine.

### Phase 4 — Modal reattachment proof

Deliverables:

- Split provider work into stable `start`, `poll`, `collect`, and `cleanup`
  operations.
- Persist a reattachable Modal call or sandbox ID against the stable Archetype
  operation identity.
- Prove crash windows before spawn, after spawn before ID persistence, after
  ID persistence, after result publication, and before ECS settlement.
- Preserve fail-closed `Unknown` when Modal cannot prove whether an effect
  occurred.

Gate:

- Worker loss never duplicates paid compute, Git publication, or cleanup, and
  a replacement process can collect the original provider result.

### Phase 5 — MissionRun cutover

Deliverables:

- Split Mission execution into `submit`, `start`, `poll`, `collect`, and
  `cleanup` Activities.
- Make Temporal authoritative for Mission lifecycle, cancellation, durable
  goal amendment, event cursors, and worker recovery.
- Preserve the REST, MCP, and ACP-facing mission contract through an adapter.
- Drain, archive, or explicitly import legacy open SQLite MissionRuns before
  disabling legacy admission.

Gate:

- Two real Missions run concurrently; one loses its Worker during provider
  execution; both finish without duplicate provider work or publication.

### Phase 6 — Dogfood and retire legacy supervision

Deliverables:

- Run the Mermaid documentation repair through the Temporal backend and have
  it open an authorized pull request.
- Migrate author, critic, and Physical-AI workers, followed by evaluation,
  rollout, AutoResearch, and storage-migration orchestration.
- Consider command scheduling and artifact ingestion only after their
  idempotent contracts are proven.
- Remove legacy catalogs, leases, coordinators, and supervisors only after
  parity, migration, and rollback gates pass.

Gate:

- The Mermaid PR is reviewable, lifecycle/event APIs remain compatible, and
  no legacy orchestration path can admit new work.

## Validation matrix

Every phase must include:

- deterministic Workflow replay tests;
- process-death recovery tests;
- duplicate-effect and lost-response tests;
- cancellation and cleanup tests;
- bounded payload/history tests;
- exact ECS receipt and settlement assertions;
- architecture, formatting, lint, type, and focused integration gates.

## Immediate sequence

1. Move shared Temporal helpers out of `archetype.activities` into the
   provider-neutral orchestration adapter.
2. Finish the hard-kill proof to the acceptance criteria above.
3. Implement Workflow-chosen fork IDs and absolute-target world advancement.
4. Cut over generic Activity execution mechanics.
5. Run the Modal reattachment spike before enabling retries for real Missions.

## Mission durable-job behavior checklist

Use these scenarios as executable behavior contracts.  `local` scenarios use
deterministic provider doubles, `process` scenarios kill a real worker process,
and `external` scenarios use paid Modal/Git/R2 infrastructure.

### Already proven

- [x] **Temporal admission excludes legacy ownership** (`local`): Given an
  Activity admitted with an immutable Temporal execution identity, when a
  legacy worker claims it, then the claim is rejected before any attempt or
  lease is created.
- [x] **Exact admission replay is idempotent** (`local`): Given a Temporal
  admission, when the exact admission is repeated, then it returns the same
  execution identity; a missing or conflicting identity fails closed.
- [x] **Claim-free result settlement** (`local`): Given a Temporal-owned
  Activity result, when it is recorded and later observed by a committed tick,
  then it settles exactly once with zero legacy attempts.
- [x] **Worker replacement preserves a world** (`process`): Given a fork
  advancing toward an absolute tick, when its owner process dies, then a new
  worker resumes the same world/run without duplicated ticks.
- [x] **Durable Modal call reattachment** (`local`): Given a persisted call ID,
  when the first runtime disappears, then a replacement attaches to that exact
  call and does not spawn another.
- [x] **Concurrent callers converge** (`local`): Given two callers for one
  provider operation, when both observe the durable call record, then they
  converge on one call and one first result.
- [x] **Exact-call cancellation** (`local`): Given a durable call ID, when
  cancellation is requested, then only that call is cancelled.
- [x] **Ambiguous cancellation fails closed** (`local`): Given a start marker
  without a durable call ID, when cancellation is requested, then no resource
  is cancelled by mutable name.

### Contract tests to write next

- [ ] **Canonical author request** (`local`): Given an author request, when it
  is encoded and decoded, then its bytes are canonical, bounded to 1 MiB, and
  bind the active redaction policy.
- [ ] **Recovered author result is revalidated** (`local`): Given a durable
  author result, when another process decodes it, then a mismatched policy or
  newly unsafe text is rejected.
- [ ] **Start is the only spawning operation** (`local`): Given an existing
  start or call record, when `poll`, `collect`, or `cleanup` runs, then none can
  spawn provider work.
- [ ] **Marker before spawn is non-replayable** (`local`): Given a permanent
  marker but no provider call evidence, when start is retried, then it returns
  `Unknown` and performs zero additional spawns.
- [ ] **Remote call self-registration closes response loss** (`local`): Given
  Modal accepted a spawn before the host stored its ID, when the remote
  controller self-registers, then a replacement attaches to that exact ID.
- [ ] **Duplicate remote calls cannot cross the effect boundary** (`local`):
  Given two accidentally spawned controller calls, when both register, then
  only the call matching the immutable record may create sandboxes or touch
  Git.
- [ ] **Poll is result-first** (`local`): Given a first-result record and a
  missing, expired, or failed Function output, when polled, then the job is
  `Ready` from the durable result.
- [ ] **Terminal call without result is Unknown** (`local`): Given a remote
  exception and no first result, when polled, then the job becomes `Unknown`
  and is never automatically restarted.
- [ ] **Collect is read-only and exact** (`local`): Given a ready job, when
  collected repeatedly, then it returns the same typed/redacted value and
  performs no execution or cleanup.
- [ ] **Partial sandbox creation is recoverable** (`local`): Given durable
  resource intent and only the auth or mission sandbox exists, when cleanup
  runs, then it removes only the exact recorded generation.
- [ ] **Cleanup is idempotent and preserves evidence** (`local`): Given a
  completed or cancelled job, when cleanup is repeated, then exact resources
  are absent and markers, call records, results, and audit evidence remain.
- [ ] **Identity mismatch touches nothing** (`local`): Given a wrong request,
  family, namespace, deployment, policy, call ID, sandbox ID, role, or cohort,
  when any operation runs, then it fails closed without effects.

### Process integration tests

- [ ] **Temporal worker dies during provider execution** (`process`): Given one
  running durable Mission job, when the Activity worker is hard-killed, then a
  fresh process polls the same call ID without another spawn.
- [ ] **Result survives worker death before ECS settlement** (`process`): Given
  a published provider result, when the worker dies before observation, then a
  replacement collects and settles the exact result once.
- [ ] **Cancellation survives worker replacement** (`process`): Given a running
  call and exact resource records, when the cancelling worker dies, then a
  replacement completes exact cancellation and cleanup.
- [ ] **Projector crash reuses the deterministic Workflow** (`process`): Given
  committed admission but a crash before Workflow-start acknowledgement, when
  projection repeats, then it uses the existing Workflow and request digest.

### Paid external release gates

- [ ] **Real Modal reattachment** (`external`): Given a deployed controller
  Function and running coding job, when the Temporal worker is hard-killed,
  then a replacement reconstructs the same `FunctionCall.object_id`; exactly
  one Modal call and one coding execution occur.
- [ ] **Git push without result never duplicates publication** (`external`):
  Given Git publication succeeds but durable result publication is interrupted,
  when recovery runs, then either the original result appears or the Mission is
  `Unknown`; there is exactly one push and no respawn.
- [ ] **Mermaid Mission dogfood** (`external`): Given the Mermaid repair
  Mission, when one worker is replaced during execution, then it produces one
  branch, one authorized pull request, one first result, exact cleanup, and an
  exactly settled ECS observation.

## Follow-up subsystem ledger

This ledger records work that remains after the first Mission cutover.  A
subsystem is not ready for deletion merely because the Mission path passes.
Each row requires its own authority boundary, migration path, and parity gate.

### Shared prerequisites

- Temporal remains the only durable orchestration authority for a migrated
  path; no dual admission, retry, cancellation, or lifecycle state machine.
- Archetype retains committed ECS receipts, exact observation settlement,
  writer epochs, lineage, storage visibility, and provider reconciliation.
- Every effectful operation has a stable, deterministic identity and a
  provider-side first-result or reconciliation record.
- Existing work is frozen, drained, archived, or imported before legacy
  admission is disabled.
- Legacy code is removed only after local contract, process-death, and parity
  tests pass.

### 1. Evaluation orchestration

Status: **scope paused; not migrated**.

- Replace evaluator leases, polling loops, heartbeats, retry scheduling, and
  worker-failure recovery with Temporal Workflows and Activities.
- Retain evaluation definitions, evidence, scores, observations, and result
  acceptance in Archetype.
- Prerequisites: deterministic evaluation identity, bounded result contract,
  idempotent provider submission, and an import/drain policy for open leases.
- Gate: kill an evaluator Worker mid-provider run; replacement recovers one
  evaluation and records one result with unchanged evidence.
- Deletion candidate after parity: evaluation lease and recovery machinery.

### 2. Rollouts and simulation fan-out

Status: **scope paused; not migrated**.

- Move durable fork fan-out, waits, cancellation, compensation, ordered
  failure handling, and long-running supervision to Temporal.
- Retain world state, exact fork lineage, target ticks, writer fencing,
  simulation computation, and committed manifests in Archetype.
- Prerequisites: caller-chosen fork IDs and absolute-target advancement are
  complete; define deterministic rollout and child-Workflow identities.
- Gate: run multiple forks, kill the supervisor, recover the same children,
  and prove no duplicate forks or ticks and an unchanged parent.
- Deletion candidate after parity: process-local rollout supervision only.

### 3. AutoResearch

Status: **scope paused; not migrated**.

- Move the single-flight guard, iteration lifecycle, experiment scheduling,
  waits, retries, cancellation, and recovery to Temporal.
- Retain research hypotheses, experiment evidence, frontier decisions, and
  ledger facts in Archetype.
- Prerequisites: stable experiment/policy IDs, explicit budget accounting,
  idempotent effect launch, and bounded history or Continue-As-New policy.
- Gate: kill the Worker between experiment selection and result ingestion;
  replacement performs no duplicate experiment and advances the same ledger.
- Deletion candidate after parity: local single-flight and iteration loops.

### 4. Storage migration orchestration

Status: **not yet scoped for implementation**.

- Move durable sequencing, waits, retries, progress, cancellation, and
  compensation around copy/validate/activate/rollback to Temporal.
- Retain migration reservations, plan digests, staged data, activation CAS,
  cold evidence, and storage authority in Archetype.
- Prerequisites: idempotent phase commands, resumable copy handles, and exact
  reconciliation for ambiguous activation outcomes.
- Gate: kill the Worker in every phase; resume the same plan without duplicate
  activation or loss of rollback evidence.
- Deletion candidate after parity: process-local migration sequencing only.

### 5. Command scheduling

Status: **separate spike required; migration is not assumed**.

- Candidate Temporal responsibility: delayed execution, durable waits, retry
  supervision, cancellation, and long-running command processes.
- Retain authorization, validation, routing, logical `scheduled_tick`, command
  ordering, transactional outbox, and manifest-atomic settlement in Archetype.
- Prerequisites: prove a Workflow can preserve logical-tick ordering and the
  exact publication transaction without becoming a second command authority.
- Gate: process death around scheduled execution preserves command order and
  produces one manifest-visible settlement.
- Never delete: transactional outbox or manifest publication authority.

### 6. Artifact ingestion

Status: **blocked on deterministic identities**.

- Candidate Temporal responsibility: long-running fetch/transform/validate
  sequencing, waits, retries, and cancellation.
- Retain content addressing, artifact metadata, provenance, validation facts,
  and publication authority in Archetype.
- Prerequisites: deterministic ingestion and occurrence IDs; current retry
  behavior may mint a new occurrence and therefore cannot be retried safely.
- Gate: crash after external fetch and after blob publication; replacement
  converges on one content-addressed artifact and one occurrence.
- Deletion candidate after parity: local ingestion retry/supervision loops.

### 7. Remaining Physical-AI Activities

Status: **durable Modal primitive proven locally; family cutover remains**.

- Reuse the Mission durable-job pattern for hosted Physical-AI executions:
  immutable start/call/result records, result-first polling, exact cancellation,
  and exact resource cleanup.
- Retain episode definitions, trajectories, manifests, result validation,
  provider first-result registers, and ECS settlement in Archetype.
- Prerequisites: paid live call-ID reattachment proof and family-specific
  collect/cleanup contracts.
- Gate: hard-kill a Worker during a real hosted episode; recover one Modal call,
  one result, and one exact observation.
- Deletion candidate after parity: Physical-AI claim/lease worker machinery.

### 8. Generic Activity legacy removal

Status: **claim-free path exists; legacy removal intentionally deferred**.

- Remove claims, attempts, leases, fences, confirmed-absence retry guards,
  expiry scans, incomplete-activity scans, and generic recovery workers only
  after every Activity family is exclusively Temporal-owned.
- Retain the slim strongly consistent admission/settlement index unless a
  per-world Workflow replacement proves lifecycle-lock consistency.
- Prerequisites: schema migration, catalog inventory, freeze/drain/archive or
  import of all unresolved legacy attempts, and zero legacy admission routes.
- Gate: catalog inventory proves no open legacy attempt or provider-bound work;
  every family passes process-death and exact-settlement parity.
- Never substitute Temporal visibility search for `has_unsettled(world_id)`.

### 9. World lifecycle orchestration

Status: **idempotent fork/advance primitives proven; broader scope remains**.

- Temporal may supervise long-running create, fork, advance, pause, resume,
  destroy, and cleanup processes.
- Archetype retains catalog status, lineage, writer epochs, locks, committed
  ticks, manifests, reconstruction, and `OnDestroy` semantics.
- Prerequisites: deterministic Workflow IDs for lifecycle operations,
  reconciliation of ambiguous storage commits, and explicit terminal-state
  rules.
- Gate: hard-kill each lifecycle phase and prove the same world/run resumes,
  writer epoch advances, parent remains unchanged, and destroy occurs once.
- Never move tick computation or storage commit authority into Temporal.

### Sequencing after Mission release

1. Finish and delete the migrated Mission-specific legacy path.
2. Resume detailed scopes for Evaluation, Rollouts, and AutoResearch.
3. Migrate remaining Physical-AI Activities using the already-proven Modal
   primitive.
4. Scope and migrate Storage Migration orchestration.
5. Run separate feasibility spikes for Command Scheduling and Artifact
   Ingestion; do not assume either cutover.
6. Remove generic Activity legacy machinery only after all families are clean.
7. Expand World lifecycle orchestration incrementally while preserving ECS and
   storage authority.

## Completed-agent follow-up audit

This section reconciles findings from completed implementation and review
agents.  Items remain open until explicitly marked resolved; passing an adjacent
test does not close them implicitly.

### Temporal cutover review

- [x] Bind Temporal execution ownership atomically at Activity admission; do
  not wait until result recording.
- [x] Store orchestration execution identity separately from family provider
  operation identities, expose it in snapshots, and reject legacy claims.
- [x] Add caller-chosen fork IDs and absolute-target world advancement.
- [x] Add canonical bounded author request encoding and recovered-result
  redaction-policy revalidation.
- [x] Persist and reattach exact Modal `FunctionCall.object_id` values.
- [x] Add remote call-ID self-registration and reject duplicate controller
  calls before the sandbox/Git effect boundary.
- [ ] Persist immutable sandbox resource intent and each exact role ID/cohort
  as resources are created.  Current author/critic cleanup identity is still
  published only with a terminal result, leaving partial creation unsafe.
- [ ] Require a deployment-pinned sandbox `image_id` on the provider-native
  production route; do not use the fallback image construction path.
- [ ] Reject custom process-local author/critic drivers, arbitrary callbacks,
  and runtime redactors on the first provider-native route.  Only deployment-
  pinned built-in drivers and policy may cross the Function boundary.
- [ ] Keep Mission poll history bounded or use Continue-As-New; the current
  transitional Workflow event tuple grows without a bound.
- [ ] Make Workflow cancellation stop the exact durable provider call and run
  exact compensation/cleanup; merely recording cancellation is insufficient.
- [ ] Add required-projector recovery for a crash between committed admission
  and Workflow-start acknowledgement using deterministic Workflow start with
  an exact request digest.
- [ ] Define and test legacy MissionRun freeze/drain/archive/import before
  disabling SQLite admission; do not operate two lifecycle authorities.

### Named Modal runtime adapter

- [x] Verify named Dict access, deployed Function lookup, `spawn`, durable call
  identity, `FunctionCall.from_id`, and zero-time polling against the pinned
  Modal 1.5.2 API surface without external execution.
- [ ] Eliminate ambiguity between Modal's built-in `TimeoutError` for an
  unfinished zero-time poll and a remote controller that raises the same Python
  exception.  The deployed controller must canonicalize all remote failures and
  never allow raw built-in `TimeoutError` to escape.
- [ ] Add a real deployed-Function compatibility test before production
  routing; fake-Modal API shape is necessary but not sufficient.

### Local subprocess crash harness

- [x] Hard-exit after remote self-registration and prove replacement polling
  reuses one durable call with one spawn.
- [x] Hard-exit after claim-free result recording but before settlement and
  prove exact idempotent settlement with zero attempts/fences.
- [ ] Add cancellation resumption after the public durable-job cancel contract
  exists; the current client/runtime has no complete cancel-and-resume port.
- [ ] Replace the persistent provider-double collection seam with the production
  family `collect` adapter once routing lands.
- [ ] Extend settlement recovery from the real SQLite Activity authority to a
  reconstructed live ECS world and required projector.
- [ ] Do not treat these tests as proof of live Modal behavior; retain the paid
  deployed-Function process-kill gate.

### Identity-only controller implementation

- [x] Accept exactly family, operation ID, canonical request bytes, and
  namespace digest as remote inputs.
- [x] Validate canonical author/critic requests under the deployment policy,
  self-register before effects, and return only a bounded identity receipt.
- [x] Provide deterministic before/after-registration failpoints with no
  production Mission routing.
- [ ] Replace the identity-only boundary with the built-in author/critic
  controller job only after sandbox resource intent and exact cleanup are
  durable.
- [ ] Keep deterministic failpoints deployment-fixed and off by default in any
  production deployment.

### Repository hygiene noted during agent validation

- [ ] `tests/missions/test_mission_run_lifecycle.py` has an unrelated existing
  Ruff format-check failure.  It was not modified during these slices; address
  it separately rather than mixing it into the Temporal changes.
