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

