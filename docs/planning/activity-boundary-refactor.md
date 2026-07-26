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
capabilities whose process-local lifetime is not correctness state. Application
families compose Activities around family-owned intent and observations.

## Atomic pull-request stack

The merge order is strict through A4. Later consumers validate and narrow the
shared substrate; they do not expand A2 speculatively.

| PR | Scope | Merge gate |
|---|---|---|
| A1 — Contract and plan | Canonical Resource/Activity definitions, authority split, Mission-author crash matrix, refactor-plan disposition, and documentation navigation | Documentation build, links, lint, and architecture consistency |
| A2 — Minimal local catalog | `storage.activity_catalog` physical records/SQLite port plus `activities` contracts/coordinator for immutable admission, claim/lease/fence, attempt identity, provider binding, bounded retry-guard/result references and digests, and exact-receipt settlement | Catalog reconstruction and two-instance restart contracts cover every generic transition required by the A1 matrix, including equal family-local IDs across worlds and kinds |
| A3 — Mission author, local restart | Required projector, content-addressed author request, `kind="missions.author"` plus `activity_id=dispatch_id`, local worker, provider reconciliation, idempotent result stager, and result-digest-bound completeness settlement | Kill/reconstruct tests recover Git publication without a second author execution; partial observation facts cannot settle; existing transition processors remain unchanged |
| A4 — Modal parity | Run the same author Activity through the existing Modal sandbox capability, with stable provider operation identity and reconciliation | Real Modal mission proves the same request/result/settlement contract, exact Git head, cleanup, and installed-artifact path |
| A5 — Mission critic | Project and execute exact-candidate critic work through the proven Activity seam; remove only critic process-local delivery after parity | Existing exact-head, separate-sandbox, bounded-subject, and fail-closed critic oracles pass through restart |
| A6 — Hosted Physical-AI contract | Reconcile one canonical whole-episode Arrow request/result schema and result publication contract before cutover | Robot adapter and simulator agree on episode/trial cardinality, transition budget, terminal meaning, and canonical digests |
| A7 — Hosted Physical-AI Activity | Add `archetype.app.physical_ai` choreography, execute a whole seeded episode locally and on Modal, publish the full trajectory, and stage its factual observation | First result is recovered by operation identity; correctness does not depend on byte-identical GPU replay |
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

### A3 — Mission author, local restart

The Mission author is the first vertical consumer:

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

The decisive test crashes after Git publication but before result recording,
reconstructs the catalog and service, reconciles the remote head, and proves
the author executed once. A separate identity oracle creates the same
world-local `dispatch_id` in two worlds and proves their control records and
provider operation identities cannot collide. The supported Mission cutover
also requires an exact-world lifecycle gate that refuses fork and destroy
while the source has unsettled Activities; A3 proves the oracle but does not
change supported lifecycle wiring.

### A4 — Modal parity

Modal supplies placement, sandbox lifetime, and process execution behind the
same Activity request/result contract. It does not become workflow authority.
The PR must preserve exact task base, validator, candidate, publication, and
cleanup behavior and must not add Modal-specific state to world Components.

The supported Mission cutover occurs only after local restart and real Modal
parity both pass. If A4 misses the release cut, the existing supported path
remains in place rather than shipping a half-cut-over workflow.

## Authority and dependency target

| Area | Target |
|---|---|
| `archetype.activities` | Generic Activity contracts, coordinator port/service, and mechanics only |
| `archetype.storage.activity_catalog` | Flattened physical records, structural catalog port, and local SQLite implementation; remote parity remains a later slice |
| `archetype.missions` | Mission state, processors, harness contracts, provider-specific facts and reconciliation |
| `archetype.app.missions` | Mission intent projection, Activity execution choreography, observation staging and result composition |
| `archetype.physical_ai` | Physical schemas, processors, providers, hosted-episode request/result meaning and reconciliation |
| `archetype.app.physical_ai` | Hosted intent-to-Activity-to-observation choreography |
| `RuntimeResources` / `wiring` | Worker process lifetime and concrete executor composition |

`archetype.activities` consumes only the lower physical-storage port it needs;
storage never gains Mission or Physical-AI meaning. Application families may
consume Activity and domain-family contracts in the outer direction.
Architecture policy changes land in the PR that creates each package or edge.

## Disposition of the paused v0.5 plan

| Previous item | Disposition |
|---|---|
| Resource contract/WorldHost implementation gate | **Frozen.** Preserve branches and reports as evidence; do not merge the `AsyncResources` prototype into the Activity path. |
| PR-9 episodes and transcripts | **Split and reassessed.** Activity work does not depend on transcript migration. Preserve the current transcript contract until its own owning slice; episode schema reconciliation becomes A6. |
| PR-10 Missions family move and committed projection | **Superseded.** The family/app split remains; A3–A5 replace process-local author and critic delivery with Activities. |
| PR-11 delete all remaining `app/` packages | **Canceled.** `archetype.app.missions` remains, and `archetype.app.physical_ai` is the accepted hosted-workflow owner. |
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
- A3 must not delete the supported Mission delivery path before its local
  crash/restart oracle passes.
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
8. Preserve application-family workflow owners where cross-family choreography
   is real.
9. Run focused contracts first, then architecture, lazy-boundary, docs, typing,
   and the appropriate release profile.
10. Open each PR and stop; never arm or merge it manually.
