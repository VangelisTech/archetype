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
