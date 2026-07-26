# Activity-boundary refactor

**Status:** Active replacement plan.

**Baseline:** `origin/main` at
`60ff6a8d33d265067170c081f903fd882316ee89`.

**Normative contract:** [Activities](../guide/activities.md).

This plan replaces the remaining v0.5 sequencing assumptions that placed
consequential Mission and Physical-AI work inside retryable ticks or expected a
Resource host to recover it. The release is time-constrained, so every pull
request below is independently reviewable and has an explicit stop line.

## Objective

Land one honest distributed boundary without redesigning processors, Daft,
worlds, or commands:

```text
committed decision
    -> durable Activity
    -> local or Modal execution/reconciliation
    -> durable factual result
    -> later committed observation
```

Processors remain DataFrame state transitions. Resources remain tick-time
capabilities whose process-local lifetime is not correctness state. Owning
top-level families compose Activities around family-owned intent and
observations.

## Atomic pull-request stack

The merge order is strict through A4. Later consumers validate and narrow the
shared substrate; they do not expand A2 speculatively.

| PR | Scope | Merge gate |
|---|---|---|
| A1 — Contract and plan | Canonical Resource/Activity definitions, authority split, Mission-author crash matrix, refactor-plan disposition, and documentation navigation | Documentation build, links, lint, and architecture consistency |
| A2 — Minimal local catalog | `storage.activity_catalog` physical records/SQLite port plus `activities` contracts/coordinator for immutable admission, claim/lease/fence, attempt identity, provider binding, bounded retry-guard/result references and digests, and exact-receipt settlement | Catalog reconstruction and two-instance restart contracts cover every generic transition required by the A1 matrix, including equal family-local IDs across worlds and kinds |
| A3a — Mission author seam | Family-owned request/result and provider-recovery values, Activity adapter, fenced worker choreography, and an explicitly incomplete execution-fact scaffold | Real bare-Git provider proof recovers an exact atomically published operation result after reconstruction; wrong receipt, provider, result, and partial-fact paths fail closed |
| A3b — Mission world integration | Exact receipt-pinned storage reader, idempotent complete ECS stager, required-projector/worker composition, candidate/provenance continuation, and exact-world lifecycle gate | Kill/reconstruct tests cross staging and tick publication without duplicate facts, omitted result meaning, or a stranded candidate; existing transition processors remain authoritative |
| A4a — Modal provider safety | Namespace-complete operation identity plus permanent operation/run markers that select one hosted-start winner and fail closed after ambiguous creation | Real Modal races prove one winner under the exact workspace, Environment, App, and protocol epoch; legacy/pre-barrier operations remain unknown |
| A4 — Modal parity | Run the same complete author Activity through the guarded Modal sandbox capability, with stable provider operation identity and reconciliation | Real Modal mission proves the same request/result/settlement contract, exact Git head, cleanup, and installed-artifact path |
| A5 — Mission critic | Project and execute exact-candidate critic work through the proven Activity seam; remove only critic process-local delivery after parity | Existing exact-head, separate-sandbox, bounded-subject, and fail-closed critic oracles pass through restart |
| A6 — Hosted Physical-AI contract | Reconcile one canonical whole-episode Arrow request/result schema and result publication contract before cutover | Robot adapter and simulator agree on episode/trial cardinality, transition budget, terminal meaning, and canonical digests |
| A7 — Hosted Physical-AI Activity | Add provider-neutral family choreography under `archetype.physical_ai`, execute one seeded batch of whole episodes locally, publish the complete trajectory/results/manifest, and stage its factual observation | Cold reconstruction crosses provider publication, generic result recording, staging, and later settlement without a second episode; start-without-result remains unknown |
| A7b — Hosted Physical-AI Modal parity | Bind the same family provider protocol to exact Modal namespace identity, atomic start admission, and provider-durable first-result publication without importing Mission barriers | Real Modal/GPU execution recovers its first complete result by stable operation identity and uses the exact A6 request/result contract |
| A8 — Consolidation and refactor resume | Extract only mechanics shared by author, critic, and Physical AI; delete superseded outboxes and distributed per-step effect paths; reconcile broad docs and topology plan | No duplicate authority, no process-local durable queue, full release verification, and three end-to-end traces remain recognizable |

### A1 — Contract and plan

This PR changes documentation only. It does not add a catalog, change runtime
behavior, or merge proof-only hosted episode code. The completed real-resource
proof remains supporting evidence; its Physical-AI schema must be reconciled in
A6 before that code becomes a production contract.

### A2 — Minimal local catalog

A2 implements only the generic mechanics exercised by the crash matrix:

- immutable logical admission and input digest conflict detection;
- non-empty durable source and observation visibility-token validation;
- one fenced claim attempt at a time;
- lease expiry without an implication that an external effect is safe to
  repeat;
- stable provider operation binding before every external effect;
- fenced reconciliation that records recovered result, confirmed absence, or
  unknown without turning those conclusions into a family recovery policy;
- fresh execution authorization only after confirmed absence and a bounded
  provider retry guard are recorded under the live reconciliation fence;
- bounded result reference and digest recording;
- completed-result discovery after process restart;
- settlement against an exact committed observation receipt; and
- a world-scoped unsettled-work oracle for the later exact-lock lifecycle gate.

A2 does not add a generic recovery policy, public status enum, executor
framework, Modal dependency, family Component, or command-scheduler adapter.
Catalog state names remain implementation details until multiple consumers
prove shared meaning.

### A3a and A3b — Mission author, local restart

A3a establishes the Mission-owned seam and provider crash proof without
claiming a world integration. It includes the content-addressed values, generic
coordinator adapter, fenced worker choreography, provider identity checks,
an execution-fact bundle scaffold, and bare-Git reconciliation oracle. Its
semantic request/result values, provider protocol, and recovery facts live in
`archetype.missions`; the projector and worker choreography live in
`archetype.app.missions`. Its reader and stager are test adapters. It does not
register a required projector, stage real world mutations, or change the
supported delivery path.

A3b completes the first vertical consumer:

1. The processor commits `TaskDispatch`.
2. `MissionAuthorActivityProjector` reads the exact committed snapshot,
   content-addresses `TaskDispatchRequest`, and idempotently admits
   `(world_id, kind="missions.author", activity_id=dispatch_id)`.
3. `MissionAuthorActivityWorker` claims a fenced attempt and binds
   `provider_operation_id` before external effect or fails closed.
4. An unbound no-effect attempt may be reclaimed. A provider-bound reclaimed
   attempt always reconciles.
5. The worker records a bounded result reference and digest.
6. An idempotent stager repeatedly stages completed-unobserved observations.
7. A later tick commits `AgentExecution`, related evidence, and a
   family-owned completeness binding to the exact Activity result digest.
8. The projector settles only when that complete binding appears in the exact
   later receipt; `dispatch_id` alone is insufficient.

The A3a provider test atomically publishes the authored branch and exact
operation-result reference, crashes before generic Activity result recording,
reconstructs the catalog and service, advances the public branch independently,
and recovers the pinned authored revision without a second execution. A
separate identity oracle creates the same world-local `dispatch_id` in two
worlds and proves their control records and provider operation identities
cannot collide.

A3b owns the harder world guarantees: its reader must prove the exact manifest
token before reading; its stager must stage the complete result-derived fact
batch idempotently, including sandbox/bind meaning, provenance relations, and
exactly one digest-bound `Candidate` for an author-green result (zero for a
non-green result), with the completion marker last. Fork/destroy must refuse
under the exact-world lock while the source has unsettled Activities. The
supported Mission path remains unchanged until those oracles pass.

### A4a and A4 — Modal parity

A4a is an inert provider prerequisite. It binds the Modal workspace,
Environment, App, and a post-cutover protocol epoch into the operation
identity. Permanent named Dict operation/run markers may select execution
authority only for Activities admitted under that barrier-aware epoch from
birth. Missing markers for legacy or already-in-flight Missions are unknown,
never confirmed absence. Marker objects must not be deleted or recreated.
Ambiguous creation and lost winners deliberately sacrifice liveness rather
than permit duplicate starts. The public initial and retry paths each couple
acknowledged operation-marker creation, run-marker creation, and the single
named-sandbox start in one coroutine. No structural guard or permit is accepted
as execution authority, and reconstruction after either success or ambiguity
cannot replay the start—even after the live sandbox names have been released.

Modal supplies placement, sandbox lifetime, and process execution behind the
same Activity request/result contract. It does not become workflow authority.
The PR must preserve exact task base, validator, candidate, publication, and
cleanup behavior and must not add Modal-specific state to world Components.

The supported Mission cutover occurs only after local restart and real Modal
parity both pass. If A4 misses the release cut, the existing supported path
remains in place rather than shipping a half-cut-over workflow.

### A6 — Hosted Physical-AI contract

`archetype.physical_ai.hosted_episode` is the one provider-neutral v1 data
boundary shared by the local simulator and external robot adapters. One request
row is one logical trial, seed, and episode; one stable `operation_id` may
transport a batch of episodes without becoming their episode identity.

Reset is trajectory row zero. `max_transitions` counts only subsequent actions,
and the direct-path bridge is `max_transitions = max_steps - 1`. Provider
`environment_done` remains distinct from the complete-episode `terminal`
decision. Per-step `step_id` and per-episode `episode_result_id` make both
publication levels idempotent.

Request, complete trajectory, derived per-episode results, and the batch
manifest use deterministic Arrow IPC with contract- and payload-domain-separated
digests. The manifest validates exact episode coverage and counts; it cannot be
built for a partial trajectory. Replay configuration recursively rejects
activation, placement, timing, credential, and host-path facts, and frame
evidence crosses the boundary only as content-addressed references. A7 may
execute this contract but may not redefine it.

### A7 and A7b — Hosted Physical-AI execution

A7 creates the hosted choreography directly in `archetype.physical_ai`; it
does not create an `archetype.app.physical_ai` mirror. A committed
`HostedEpisodeIntent` references the canonical A6 request. The required
projector admits `kind="physical_ai.hosted_episode"`, and the out-of-lock
worker binds the world-scoped stable provider operation before execution or
reconciliation. Complete request, trajectory, derived episode results, and
manifest bytes are content-addressed before the generic catalog records one
bounded descriptor. `HostedEpisodeObservation` binds all four payloads and
their exact completeness counts, and only a later matching committed receipt
settles the Activity.

The local restart oracle destroys and reconstructs the SQLite catalog,
coordinator, value store, worker, and provider adapter across four separate
windows:

1. provider result published before generic result recording;
2. generic result recorded before observation staging;
3. observation staged before its tick commits; and
4. provider start present without a complete result.

The first three redeliver without a second episode. The fourth remains
permanently unknown; lease expiry and deterministic seeds are not replay
authority. Partial trajectory publication cannot produce a manifest or
Activity result. The existing direct per-step path remains supported through
this slice.

A7 deliberately leaves the remote provider fail-closed behind the same family
protocol. A7b supplies the Modal implementation under an exact workspace,
Environment, App, Function, named Dict, named Volume, and protocol epoch. One
atomic Dict put selects the start winner; canonical A6 payloads are committed
to the Volume before the bounded first-result index is atomically installed in
the Dict. Completion-response loss and worker reconstruction recover that
first result without a second episode. Start-without-result remains unknown.

The adapter does not import Mission-owned barriers or move Physical-AI
recovery meaning into generic mechanics. A8 may extract only the identity,
atomic-admission, and immutable-result mechanics that the completed Mission
and Physical-AI implementations actually prove to be shared.

Paid A7b evidence on 2026-07-26 used one L40S-backed function in stopped Modal
App `ap-nXLKIwCqS18C4j6UuGFMSU`. The remote completion reported one visible GPU
and first-result index digest
`ad70beb7153d4b83cd0c12fb6218b1e312a265199e5013eae8d94f6f42327fb0`.
After an injected worker failure before generic Activity result recording, a
separate process recovered the exact two-episode result without another Modal
call and staged result digest
`784811d10f376faf3b45284c5f2b180ba179f4a35f9575b7af37444d08678cf9`.
Provider evidence remains in named Dict
`arch-a7b-results-20260726-185158-c3261f` and named Volume
`arch-a7b-values-20260726-185158-c3261f` in workspace/environment
`vangelis-tech/main`.

## Authority and dependency target

| Area | Target |
|---|---|
| `archetype.activities` | Generic Activity contracts, coordinator port/service, and mechanics only |
| `archetype.storage.activity_catalog` | Flattened physical records, structural catalog port, and local SQLite implementation; remote parity remains a later slice |
| `archetype.missions` | Mission state, processors, harness contracts, provider-specific facts and reconciliation, Activity choreography, observation staging, and result composition |
| `archetype.physical_ai` | Physical schemas, processors, providers, hosted-episode request/result meaning and reconciliation, and hosted intent-to-Activity-to-observation choreography |
| `RuntimeResources` / `wiring` | Worker process lifetime and concrete executor composition |

`archetype.activities` consumes only the lower physical-storage port it needs;
storage never gains Mission or Physical-AI meaning. Owning top-level families
may consume Activity and lower-family contracts through declared DAG edges.
Architecture policy changes land in the PR that creates each package or edge.

## Disposition of the paused v0.5 plan

| Previous item | Disposition |
|---|---|
| Resource contract/WorldHost implementation gate | **Frozen.** Preserve branches and reports as evidence; do not merge the `AsyncResources` prototype into the Activity path. |
| PR-9 episodes and transcripts | **Split and reassessed.** Activity work does not depend on transcript migration. Preserve the current transcript contract until its own owning slice; episode schema reconciliation becomes A6. |
| PR-10 Missions family move and committed projection | **Superseded.** A3–A5 replace process-local author and critic delivery with family-owned Activities. |
| PR-11 delete all remaining `app/` packages | **Restored as A8.** Mission choreography moves under `archetype.missions`; hosted Physical-AI choreography is born under `archetype.physical_ai`; single-implementation facades and compatibility packages do not survive. |
| Broad final topology cleanup | **Deferred to A8.** It may consolidate or delete proven duplication but may not invent another execution model. |

The `everettVT/resource-world-host-spike` branch and the Resource dossier at
`everettVT/resource-contract-design` head `89f9dbc7` remain inspectable
evidence. Neither is a merge dependency. `WorldHost` may later describe an
executor implementation detail, but it is not the durable semantic owner of
Mission or whole-episode work.

## Release cut lines

The release consumes only the highest contiguous green slice:

- A1 is documentation-only and safe to review independently.
- A2 is inert generic substrate; it does not switch a supported workflow.
- A3a and A4a are inert prerequisites and do not switch a supported workflow.
- A3b must not delete the supported Mission delivery path before its exact
  reader, complete stager, lifecycle, and local crash/restart oracles pass.
- A4 cuts the supported hosted author path over only after the real Modal proof
  passes against the same contract.
- A5–A8 may follow the release unless they independently meet their gates.

No release deadline weakens the crash matrix. If a cutover slice is not green,
leave the prior supported behavior intact and release the last complete slice.
Do not merge a status-only implementation, a process-local durability claim, or
a provider path that treats lease expiry as replay permission.

## Standing rules

1. Keep each PR responsible for one contract change and list its exact
   executable oracle.
2. Do not edit `src/archetype/core/` for this migration.
3. Required projectors persist deterministic intent only; provider I/O remains
   outside the world lock.
4. Activity results stage facts. Processors retain every semantic transition.
5. Result payload publication precedes the bounded catalog reference.
6. Fences reject stale control writes; adapters reconcile external truth.
7. Local and Modal workers consume the same immutable request and produce the
   same typed result contract.
8. Preserve family-owned workflow meaning where declared lower-family
   choreography is real; do not recreate an `archetype.app` layer.
9. Run focused contracts first, then architecture, lazy-boundary, docs, typing,
   and the appropriate release profile.
10. Open each PR and stop; never arm or merge it manually.
