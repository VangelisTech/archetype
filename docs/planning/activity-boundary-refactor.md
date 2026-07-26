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
| A5a — Mission critic value contract | Family-owned `missions.critic` request/result/receipt values, deterministic bounded redacted codecs, explicit domain-review attempt identity, and a digest-bound byte-budgeted subject transport contract | Canonical round trips preserve exact base/head/diff/policy/validator and separate-sandbox identity; diff bytes never enter the command line; over-budget subjects fail closed before inference |
| A5b — Mission critic Activity integration | Purely project exact-current-candidate intent, execute/reconcile it through generic Activities, stage a complete result-bound critic fact bundle, and settle its exact later receipt; retain legacy delivery until A8 | Real Git and managed-world crash/restart oracles prove one provider execution, exact file-bound subject identity, fresh critic sandbox, marker-last atomic staging, idempotent redelivery, and exact-receipt settlement without `CriticReviewOutbox` |
| A6 — Hosted Physical-AI contract | Reconcile one canonical whole-episode Arrow request/result schema and result publication contract before cutover | Robot adapter and simulator agree on episode/trial cardinality, transition budget, terminal meaning, and canonical digests |
| A7 — Hosted Physical-AI Activity | Add family-owned choreography under `archetype.physical_ai`, execute a whole seeded episode locally and on Modal, publish the full trajectory, and stage its factual observation | First result is recovered by operation identity; correctness does not depend on byte-identical GPU replay |
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

A3b now supplies those adapters as an explicit per-world binding. Its receipt
reader rejects a later head, sibling or ambiguous head token, wrong world/run,
missing visibility membership, and tokenless receipts. Its v2 stager rolls back
a mixed-signature mutation prefix on cancellation or hook failure, survives
fresh stager and world reconstruction without duplicate facts, and preserves
the committed request's candidate predecessor even when results are delivered
out of order, while leaving provider execution outside the world lock. Its
separately named v2 completion marker also preserves the durable schema identity
of A3a's v1 marker. The exact registry can be injected into maintainer runtime
composition so one real `MissionAuthorActivityBinding` owns both callbacks and
its worker. The local Git oracle atomically publishes the canonical
bounded/redacted observation with the authored revision and recovers that exact
payload without rerunning validators. A4 now installs the same binding for the
supported Modal `MissionService` path; non-Modal backends preserve the direct
local path.

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
Before starting a sandbox pair, the adapter opens one namespace-complete named
Modal Dict that acts as the provider-native first-result register. Raw harness
output never enters it: the Mission-owned codec first redacts, bounds, and
canonically encodes the observation, then the adapter publishes it with an
atomic first-write operation. An ambiguous write is successful only when an
exact read returns the same operation-bound, request-bound value. The result
Dict is durable protocol state and, like the permanent start markers, must not
be deleted or recreated.

Read-only marker absence is not provider execution authority. It permits the
Activity catalog to mint a fresh workflow fence and route one attempt back
through `start_retry`; that method still must atomically create the permanent
operation and run markers and immediately start the sandbox pair. A permanent
start marker without an exact provider result remains unknown forever. No
worker may infer replay permission from stopped or missing sandbox names.

The PR must preserve exact task base, validator, candidate, publication, and
cleanup behavior and must not add Modal-specific state to world Components.

The supported Modal Mission cutover passed local restart and real Modal parity
on 2026-07-26. The proof committed dispatch at tick 1, settled the exact author
result at tick 2, published and independently approved the exact Git head, and
recovered the provider result from a separate cold process without another
sandbox start. See the
[A4 proof report](../reports/2026-07-26-modal-mission-author-activity-proof.md).

### A5a and A5b — Mission critic

A5a does not switch critic delivery topology. It does add the family contract,
persisted subject-size policy, and a live transport hardening that moves exact
diff bytes from command arguments into a provider-owned temporary file.
`kind="missions.critic"` uses `activity_id=review_id`, where `review_id` binds
the candidate, critic policy, and **domain review attempt**. The generic
Activity claim/delivery attempt is a different control-plane counter:
reclaiming or reconciling the same Activity must not consume `max_reviews`.
Only a later committed `CriticExecution` observation consumes one domain review
attempt.

The admitted request contains no diff payload. It binds the exact base, head,
diff digest, validator-bundle digest, critic-policy digest, author sandbox
identity, and a policy-owned UTF-8 byte budget. The executor verifies the exact
Git subject and supplies large diff bytes through a sandbox-local file or
standard input. A successful receipt records the observed content and metadata
sizes, aggregate subject digest, transport reference, and every exact identity
above. An over-budget subject fails closed with only its digest and sizes; it
is never truncated into a potentially approving review. Free text is redacted
and bounded before result or receipt durability.

A5a intentionally leaves the existing `CriticReceipt` v1 ECS schema unchanged.
The live legacy stager therefore does not persist the new subject-binding
fields. A5b owns a separately versioned complete Activity fact bundle and
marker that makes those fields durable without changing the identity of
already-recorded v1 receipt tables.

The subject-size budget participates in the current critic-policy digest.
A persisted candidate whose legacy digest did not bind that budget is not
silently reinterpreted: committed-intent projection fails closed until an
explicit migration or a newly admitted candidate supplies the current digest.

A5b adds the opt-in required projector, fenced worker, provider reconciliation,
content-addressed value store, idempotent complete observation stager, and
exact-receipt settlement. Its pure committed-intent projection does not
instantiate or depend on `CriticReviewOutbox`. One atomic world mutation batch
contains a fresh critic `Sandbox`, `CriticExecution`, exact `Reviews` and
`RunsIn` edges, every `CriticFinding`/`ProducedBy` pair, the optional v1
`CriticReceipt`/`ProducedBy` pair, and a separately versioned
`CompleteCriticActivityObservation` marker staged last. That marker binds the
complete fact digest and durable result reference/digest, including exact
subject evidence when a receipt exists.

The critic still receives a fresh sandbox distinct from the author sandbox and
no Git publication secret. A generic claim retry never consumes Mission review
budget; only a later committed `CriticExecution` advances the domain review
attempt. Existing processors remain the sole task-transition authority.
`CriticReviewOutbox` and its process-local queued sets remain compatibility
machinery through A3–A7 and are deleted during A8 consolidation; critic prewarm
may remain only as a correctness-independent Resource optimization.

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

## Authority and dependency target

The following is the landing topology, not an instruction to mix package moves
into the A3–A7 behavior proofs. Those slices may use the existing
`archetype.app` modules as an interim owner. A8 performs the mechanical rehome
only after author, critic, and Physical-AI workflows prove which choreography
is actually family-specific.

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
- A5a does not switch critic delivery, though it hardens its live subject
  transport and persists the explicit subject-size policy.
- A5b–A8 may follow the release unless they independently meet their gates.

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
