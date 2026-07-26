# Activities

**Document type:** Normative accepted-target contract.

**Status:** The boundary and migration order are ratified. The generic catalog,
canonical hosted Physical-AI data contract, and family-owned local hosted
Activity choreography exist. Mission cutover and remote hosted-provider parity
remain migration slices. Each implementation slice must land its executable
oracle with the behavior it enables.

**Scope:** Durable work admitted after one committed tick and observed by a
later committed tick. This specification refines the required-projector rule
in [Atomic Tick Visibility](atomic-visibility.md) without changing tick
atomicity.

## 1. Resource and Activity

### Resource

> A Resource is a capability available while executing a tick. Correctness
> must not depend on its process-local lifetime. It should be read-only,
> reconstructible, or safely idempotent.

### Activity

> An Activity is durably coordinated work admitted from one committed tick and
> observed by a later committed tick.

Consequential or hosted work derived from committed world intent is an
Activity. Expensive or long-lived work may also use this boundary when its
result must survive worker loss even if the underlying computation is
deterministic and safe to repeat. This rule does not reclassify direct handlers
such as artifact ingestion that do not follow the
intent-in-one-tick/observation-in-a-later-tick protocol.

The existing `archetype.core.Resources` object remains the plain type-keyed
container passed to processors. A persistent client can still be a useful
optimization for inference, messaging, or local simulation. Its process
lifetime belongs to its runtime or executor owner; world correctness may not
depend on that Python object surviving.

An Activity is not:

- a generic ECS Component;
- a live sandbox, simulator, model, or robot handle;
- permission for a provider result to advance workflow state directly;
- a second workflow state machine competing with processors; or
- an exactly-once promise for arbitrary external effects.

Family Components remain the semantic record. Activity control records exist
to deliver work, recover it, and bind a factual result to the later committed
receipt that observed it.

## 2. The committed-state protocol

```text
tick T computes a family decision
        |
        v
tick T commits semantic intent
        |
        v
required projector admits one logical Activity
        |
        v
worker claims a fenced attempt
        |
        v
family adapter executes or reconciles provider work
        |
        v
bounded result reference and digest become durable
        |
        v
owning family stages factual observations
        |
        v
tick U commits those observations
        |
        v
Activity settles against tick U's exact receipt
        |
        v
later processors decide the next semantic transition
```

The required projector reads the exact visibility snapshot named by a
`CommittedTickReceipt`. It deterministically admits Activity intent by the
owning family's stable logical identity. Repeating projection for the same
receipt and logical identity is an idempotent no-op only when the immutable
request identity and digest agree. A conflicting replay fails closed.

Both the source receipt and the later observation receipt MUST carry a durable,
non-empty visibility token. Tokenless bare-core or uncoordinated ticks do not
name the pinned snapshot required for Activity admission or settlement and
cannot use this contract.

The projector performs no provider, sandbox, Git, simulator, model, or hardware
I/O. Activity workers claim outside the world lock. One claim names an attempt
and carries a fence so a stale worker cannot record or settle a result after a
newer claimant has taken authority.

Fencing limits who may change the control record. It cannot undo an external
effect and cannot prove that an expired worker did nothing. Before any external
effect, the worker MUST durably bind a stable logical provider operation
identity. An adapter that cannot bind that identity fails closed without
executing. A provider-returned execution handle may arrive later, but it is
supplemental evidence; its absence is not permission to replay.

An expired attempt that is still unbound and performed no external effect may
be reclaimed under a new fence. Once provider operation identity is bound, a
replacement claimant must reconcile, retrieve, resume, compensate, or fail
closed according to the owning adapter. Lease expiry alone never authorizes
blind replay.

Provider-family reconciliation reaches one of three factual conclusions. These
are recovery outcomes, not a generic lifecycle enum:

| Reconciliation conclusion | Catalog action under the live fence | Execution permission |
|---|---|---|
| Recovered result | Record the bounded result reference and digest | No fresh execution; proceed toward observation |
| Confirmed absent | Record confirmed absence and a bounded provider retry-guard reference/digest | A fresh attempt is permitted only when that guard proves atomic provider deduplication by logical operation identity or proves that every stale claimant is irrevocably unable to start |
| Unknown | Retain the provider-bound work for later reconciliation | No fresh execution; retry reconciliation or require family/operator intervention |

Confirmed absence is authoritative only when recorded under the live
reconciliation fence. An in-memory lookup result, timeout, missing
provider-returned handle, or lease expiry is not confirmed absence. Confirmed
absence alone is also not execution permission: after an absence check, a stale
worker could still start the original effect. The provider adapter must first
produce a retry guard backed by atomic create-if-absent/idempotency for the same
logical provider operation, a provider-enforced fence, or other durable proof
that the stale claimant can no longer start. The Activity catalog records the
bounded guard reference and digest; the owning adapter retains its meaning.

The complete bounded result, or a durable reference to a larger result, is
recorded before its ECS observation is staged. Staging is idempotent and may be
repeated after restart until a later tick commits the factual observation.
Family-owned observation completion evidence MUST bind the Activity kind and
identity to the exact recorded result reference/digest and prove that the
family's complete result-derived fact set is present. The Activity settles only
when that binding appears in the exact later `CommittedTickReceipt`. A
correlation ID by itself is not completion evidence. Settlement does not decide
mission acceptance, physical success, retry, or any other family transition.

## 3. Commands and Activities are different

| Boundary | Command | Activity |
|---|---|---|
| Direction | Enters world execution | Leaves one committed tick and returns to a later tick |
| Admission | Before materialization | After semantic intent is committed |
| Execution | Materialized under exact-world authority | Performed outside the world lock |
| Settlement | Coupled to the target tick manifest | Coupled to the later receipt that commits its observation |
| Meaning | Request to apply registered world/application behavior | Durable coordination of external work |

Activities do not reuse `CommandScheduler`. The two mechanisms may share
physical control-catalog techniques, but they have different authority,
ordering, and settlement boundaries.

## 4. Authority

| Owner | Responsibility |
|---|---|
| `archetype.activities` | Generic contracts, coordinator port/service, logical identity, immutable admission, claims, attempts, leases, fences, bounded provider retry-guard and result references/digests, and observation settlement |
| `archetype.storage.activity_catalog` | Flattened physical records, structural catalog port, and local SQLite implementation; a future remote implementation is a separate parity slice |
| Owning top-level family | Semantic intent and observation schemas, provider protocol and reconciliation facts, intent-to-Activity projection, worker choreography, observation staging, and declared lower-family composition |
| `RuntimeResources` and `archetype.wiring` | Process-lifetime worker ownership, admission/drain, construction, and executor binding |
| Iceberg or `archetype.artifacts` | Large or unbounded result payloads published before their bounded Activity reference |

`archetype.activities` owns mechanics, not a universal recovery policy. A Git
publication, seeded simulation, and real robot may share claim and fence
machinery without sharing a replay decision:

- Mission Git work reconciles the exact repository, branch, base, provider
  operation identity, and published head.
- A seeded simulation reuses the first durable result for its stable operation
  identity; byte-identical GPU replay is not a correctness assumption.
- Real hardware follows its adapter's explicit reconciliation or operator
  intervention path and never blindly repeats a consequential action.

No generic recovery-policy enum or lifecycle-status enum is ratified by this
document. The initial catalog records durable facts and permits only the
transitions required by the crash oracles. Shared vocabulary may be extracted
after the Mission author, critic, and hosted Physical-AI consumers demonstrate
that it has the same meaning.

`archetype.missions` owns Agent Missions workflow authority, including its
intent-to-Activity-to-observation choreography. `archetype.physical_ai` owns
the corresponding hosted-physical choreography. Both families retain their
Components, processors, value contracts, provider protocols, and recovery
meaning; neither requires an `archetype.app` mirror.

## 5. Identity and bounded durability

The generic control key is `(world_id, kind, activity_id)`. Mission
`dispatch_id` is currently only world-local because it is derived as
`sha256(entity_id:sequence)`, and unrelated Activity kinds may produce the same
family-local identifier in one world. Source `run_id`, committed tick, and
visibility token are immutable bindings on that key. Provider operation
identity must namespace the same family/kind and `world_id` with the
family-local identifier; a bare `dispatch_id` or `world_id:dispatch_id` is not
globally sufficient.

The generic control record must preserve enough immutable identity to reject a
different operation masquerading as a retry:

- logical Activity identity and kind;
- source world, run, committed tick, and visibility token;
- immutable input reference and digest;
- attempt identity, lease, and fence;
- stable logical provider operation identity bound before provider work starts;
- bounded provider retry-guard reference and digest before a provider-bound
  absence authorizes another attempt;
- bounded result reference and digest once a result exists; and
- the exact later committed receipt that observed the result.

Both receipt bindings include their durable visibility token; world/run/tick
coordinates without that token are insufficient.

Secrets, live handles, provider clients, complete transcripts, repositories,
trajectories, frames, and other unbounded values do not belong in the Activity
catalog. Large results are first made durable through storage or artifacts. The
catalog then records their bounded reference and digest. Repeating the same
write is idempotent; the same identity with a different immutable digest is a
conflict.

Attempt identity is not logical Activity identity. A logical Activity may
acquire more than one fenced attempt across worker loss. All attempts still
refer to the same provider operation and semantic request. A new attempt does
not create permission to repeat an ambiguous effect.

### World lifecycle interaction

V1 does not transfer or duplicate an in-flight Activity across world lineage.
Before an Activity-backed family is wired into a fork-capable world, the
application lifecycle path MUST hold the source world's exact-world lock,
reconcile its retained required-projector receipt, and refuse both `fork_world`
and `destroy_world` while that source world has any admitted Activity without
an exact later-receipt settlement. The catalog therefore exposes one
world-scoped unsettled-work oracle for lifecycle integration.

This check is conservative by design. A fork is permitted after every source
Activity visible at the selected head has settled; its complete factual
observation is then ordinary lineage-visible ECS state. A fork never inherits
a source control record, reprojects an ancestor tick, adopts a provider
operation, or silently starts replacement work. Destroy likewise cannot make
an admitted Activity permanently unobservable. Transfer, cancellation, and
orphan policies require a separate family-owned proposal if a later consumer
proves that blocking is insufficient.

## 6. Mission-author crash matrix

The first executable consumer is one Agent Mission author dispatch. Its
logical `activity_id` is the processor-created `dispatch_id`. This table
derives the required catalog behavior; it does not prescribe a public status
enum.

| Crash window | Durable evidence after restart | Required behavior |
|---|---|---|
| Before the `TaskDispatch` tick commits | No visible dispatch and no Activity | Do nothing. The failed tick remains governed by ordinary tick retry. |
| After dispatch commit, before required projection | Exact committed receipt and dispatch exist | Retry projection from that receipt without rerunning the tick. |
| After Activity admission, before projector acknowledgement | Same immutable request and digest exist | Duplicate admission is a no-op; a different digest fails closed. |
| After claim, before provider binding or external effect | Fenced attempt exists with no provider operation identity and the adapter has performed no effect | After the old lease loses authority, a new fenced attempt may bind its stable provider operation identity and execute. |
| The adapter cannot bind stable provider operation identity | Unbound attempt exists and no provider effect is permitted | Fail closed. Do not invoke the provider. |
| After stable provider identity is bound, before or during author execution | Provider-bound attempt exists; a provider-returned handle may or may not exist | Under the live fence, record a recovered result, or record confirmed absence plus provider retry-guard evidence before a fresh attempt. Without that guard, retain unknown work; the stale claimant may still start after the absence check. |
| After Git publication, before result recording | Exact target branch/base plus provider identity exist; remote head may have advanced | Reconcile the remote branch and head, reconstruct the same factual result, and keep author execution count at one. |
| After result payload publication, before catalog reference | Content-addressed payload may exist without a control reference | Reuse the exact payload by digest or leave it unreferenced; never publish a conflicting result under the same identity. |
| After result recording, before ECS staging | Result reference and digest exist | Reconstruct the service and restage the same observations idempotently. |
| After ECS staging, before the observation tick commits | Result remains durable; staged mutations are not yet visible | Restage or retry the tick through normal mutation semantics; do not re-execute the provider. |
| After observation commit, before Activity settlement | Exact later receipt contains a family completeness binding to the recorded Activity result reference/digest | Reconcile that complete binding and settle idempotently without another tick or provider call. A dispatch ID or partial fact set cannot settle the Activity. |
| An expired worker returns after a new fence exists | Old attempt may have performed an effect | Reject stale recording and settlement; the live claimant still reconciles provider truth before acting. |
| Duplicate result or settlement delivery | Existing immutable result or observation receipt exists | Accept exact duplicates and reject conflicting digests or receipts. |
| Two worlds derive the same `dispatch_id` | Distinct world IDs and source receipts exist | Keep independent control records and provider operation identities; no claim, result, or settlement crosses worlds. |
| Two Activity kinds derive the same family-local ID in one world | Distinct kind-qualified logical identities exist | Keep independent control records and provider operation identities; `kind` is part of the generic key. |
| Fork or destroy while an Activity is unsettled | Exact-world lifecycle lock, reconciled required projection, and world-scoped unsettled-work evidence | Refuse the lifecycle operation. V1 neither transfers nor abandons the Activity. |

The local restart oracle must destroy and reconstruct the service and catalog,
not merely retry in one process. In particular, a crash after Git publication
but before result recording must recover the published head and stage the same
facts without running the author again.

For the Mission author slice:

- `MissionAuthorActivityProjector` receives the exact committed receipt, reads
  the matching post-commit snapshot, content-addresses the request, and admits
  `(world_id, kind="missions.author", activity_id=dispatch_id)`;
- a family-owned author-observation completion record binds that identity to the
  exact Activity result reference/digest and complete result-derived facts;
  the projector settles only when that binding appears in the committed
  receipt;
- the Mission author worker MUST bind a stable `provider_operation_id` before
  external effect or fail closed;
- an unbound no-effect attempt may be reclaimed under a new fence, while every
  provider-bound reclaimed attempt reconciles;
- completed-but-unobserved results are repeatedly restaged through an
  idempotent family-owned stager; and
- Mission readiness, candidate creation, repair, acceptance, and rollup remain
  processor decisions.

## 7. Hosted Physical-AI crash matrix

The hosted Physical-AI consumer uses
`kind="physical_ai.hosted_episode"`. Its committed `HostedEpisodeIntent`
contains only a family-local Activity identity, the world-scoped stable
provider operation identity, and the exact content-addressed request identity.
Its later `HostedEpisodeObservation` binds the bounded Activity result
descriptor to the request, complete trajectory, derived episode results,
manifest, and exact completeness counts.

| Crash window | Durable evidence after restart | Required behavior |
|---|---|---|
| After intent commit, before projection | Exact receipt and request reference | Retry required projection; admit the same immutable Activity. |
| After provider result publication, before generic result recording | Permanent operation start plus complete provider result index | Reconcile by operation identity, publish the same family payloads, and do not execute another episode. |
| After generic result recording, before observation staging | Bounded descriptor plus complete content-addressed payloads | Reconstruct and restage the exact marker without provider work. |
| After staging, before observation commit | Result remains unobserved; staged mutation may be lost | Restage the same marker idempotently until a tick commits it. |
| After observation commit, before settlement | Exact later receipt contains the complete marker | Settle against that receipt without provider work or another tick. |
| Permanent provider start exists without a complete result | Stable operation identity but ambiguous external truth | Remain unknown. Lease expiry and deterministic seeds do not authorize replay. |
| Provider returns a partial trajectory | No valid complete manifest can be built | Publish no Activity result; remain unknown after the permanent start. |

The local provider proof uses an atomic permanent start marker and a
provider-durable first-result index. It deliberately sacrifices liveness after
an ambiguous start rather than assume a seeded GPU rerun is equivalent.

The Modal parity adapter preserves that contract under an exact workspace,
Environment, App, Function, named Dict, named Volume, and protocol epoch. An
atomic Dict `put(..., skip_if_exists=True)` selects one start winner. The remote
function commits the four canonical Arrow payloads to the Volume before it
atomically installs the bounded first-result index in the Dict. A lost function
response is therefore recoverable; a permanent start without that index
remains unknown and cannot be replayed. Provider placement diagnostics never
enter canonical payloads or Components.

The local family value store and SQLite Activity catalog remain executable
proof substrates, not claims of remote storage parity. The Modal Volume and
Dict prove provider-side start and first-result durability only. Production
trajectory and frame publication still belongs in Iceberg or the artifact
substrate, while a remote control-catalog implementation is its own parity
slice.

## 8. Resource-spike disposition

The `AsyncResources`/WorldHost prototype is retained as architecture evidence
and is frozen. It is not the implementation path for Agent Missions or
whole-episode Physical AI.

This decision preserves the useful part of the Resource finding: process hosts
may still own long-lived clients, placement, readiness, and teardown. It rejects
the stronger assumption that world-host lifetime machinery should carry the
durability of consequential work performed between committed states.

The core `Resources` bag does not change in the Activity migration. A later
focused Resource proposal may address an in-tick capability that cannot be
handled as read-only, reconstructible, safely idempotent access or as an
Activity. Evidence from the frozen spike should inform that proposal rather
than silently entering the current refactor.

## 9. Verification gates

The Activity migration is conforming only when focused contracts prove:

- projection is exact-receipt-bound, deterministic, and idempotent;
- tokenless uncoordinated receipts are rejected for both admission and
  settlement;
- provider I/O never occurs in the required projector or under the world lock;
- only the live fenced attempt can bind provider work or record its result,
  while only the exact later receipt with complete family evidence can settle;
- provider-bound recovery reconciles instead of blindly replaying;
- recovered-result, confirmed-absence, and unknown reconciliation paths are
  fenced; only recorded confirmed absence plus a provider retry guard permits
  a fresh execution-authorized attempt;
- a complete result becomes durable before ECS staging;
- restart repeatedly restages completed-but-unobserved results;
- settlement binds the exact observation receipt and matching result
  reference/digest completeness evidence;
- equal world-local Activity IDs in two worlds remain isolated in both control
  and provider operation identity;
- equal family-local Activity IDs from two kinds in one world remain isolated;
- family processors remain the only semantic transition authority;
- local and Modal executors satisfy the same Mission author contract; and
- large hosted Physical-AI outputs use bounded catalog references to durable
  Arrow or artifact payloads.

The implementation sequence and release cut lines are tracked in
[Activity-boundary refactor](../planning/activity-boundary-refactor.md).
