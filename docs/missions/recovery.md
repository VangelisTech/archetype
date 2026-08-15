---
title: Mission Activity Recovery
description: Recover Mission author and critic Activities without duplicate external work
---

## Mission Activity recovery

**Document type:** Normative contract.

Mission author and critic work follows the generic
[Activity](../guide/activities.md) delivery contract, but Missions owns the logical
identities, provider reconciliation, completeness evidence, and semantic
recovery rules below.

The author Activity uses the processor-created `dispatch_id`; the critic uses
`review_id`. A `dispatch_id` is world-local because it is derived as
`sha256(entity_id:sequence)`. The complete generic keys are therefore
`(world_id, kind="missions.author", activity_id=dispatch_id)` and
`(world_id, kind="missions.critic", activity_id=review_id)`. Provider operation
identity must also include the world and kind; neither a bare family ID nor
`world_id:activity_id` is globally sufficient.

### Mission-author crash matrix

This table derives the required Missions behavior; it does not prescribe a
public generic Activity status enum.

| Crash window | Durable evidence after restart | Required behavior |
|---|---|---|
| Before the `TaskDispatch` tick commits | No visible dispatch and no Activity | Do nothing. The failed tick remains governed by ordinary tick retry. |
| After dispatch commit, before required projection | Exact committed receipt and dispatch exist | Retry projection from that receipt without rerunning the tick. |
| After Activity admission, before projector acknowledgement | Same immutable request and digest exist | Duplicate admission is a no-op; a different digest fails closed. |
| After claim, before provider binding or external effect | Fenced attempt exists with no provider operation identity and the adapter has performed no effect | After the old lease loses authority, a new fenced attempt may bind its stable provider operation identity and execute. |
| The adapter cannot bind stable provider operation identity | Unbound attempt exists and no provider effect is permitted | Fail closed. Do not invoke the provider. |
| After stable provider identity is bound, before or during author execution | Provider-bound attempt exists; a provider-returned handle may or may not exist | Under the live fence, record a recovered result, or record confirmed absence plus provider retry-guard evidence before a fresh attempt. Without that guard, retain unknown work; the stale claimant may still start after the absence check. |
| After Git publication, before result recording | Exact target branch/base, provider identity, and atomically published canonical bounded/redacted observation exist; remote head may have advanced | Recover the originally published observation byte-for-byte. Do not rerun validators or synthesize a replacement result; keep author and validator execution counts at one. |
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
but before result recording must recover the published head and its original
canonical observation, stage the same digest-bound facts, and run neither the
author nor its validators again.

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

The supported Modal Mission author path installs this binding after lazy world
activation and before the first tick. Its provider adapter binds the exact
workspace, Environment, App, protocol epoch, and logical operation before
sandbox start. A named Modal Dict retains the first canonical redacted result;
a permanent start marker without that result remains unknown and cannot be
replayed. The 2026-07-26 paid proof published one exact Git head, settled its
Activity on a later tick, and recovered the same result from a separate cold
process without a second sandbox start.

The v2 Mission author observation is one all-or-none mixed-signature mutation
batch. It contains one `Sandbox`, optional sandbox `PartOfMission`, one
`AgentExecution` with `Executes` and `RunsIn`, every validation/commit/friction
fact with one `ProducedBy` edge, and exactly one `Candidate` plus
`CandidateFor`/`AuthoredBy` and optional `Supersedes` only for authored-green
evidence. Non-green evidence contains zero candidates. The
`CompleteAuthorActivityObservation` marker is staged last and digests every
fact value, provenance edge, and entity identity. It is a separately named
schema-v2 Component: the schema and table identity of the A3a
`AuthorActivityObservation` marker remain unchanged, so already-durable v1
rows remain resolvable.

Atomicity here covers the world's mutation cache: cancellation or a failing
`OnSpawn` handler restores the entity sequence and every staged mutation before
the error escapes, so processors cannot consume a prefix on a later tick. A
handler that already ran may have an advisory process-local side effect and
cannot be undone; therefore `OnSpawn` hooks must not own Mission correctness.
Durable atomic visibility remains the ordinary manifest-last tick commit.

### Mission critic Activity

`review_id` binds the exact candidate, critic-policy digest, and Mission domain
review attempt. Generic claim attempts and fences are delivery mechanics and
do not consume that domain budget. Only a `CriticExecution` visible in a later
committed tick advances the next domain review attempt.

The admitted request contains exact base/head, diff and validator-bundle
digests, policy, validation evidence, author sandbox identity, and a bounded
subject policy. It contains no diff bytes and no provider subject path. The
provider recomputes the exact binary diff, places it in a provider-owned file
or stdin, verifies its digest and total byte budget, and removes temporary
subject storage on every unwind path. Provider operation identity is stable
across generic retries. A replacement worker retrieves the exact published
result, proves guarded absence before a safe retry, or remains unknown and
fails closed.

One critic result is staged as an all-or-none mixed-signature batch:

- one fresh critic `Sandbox` distinct from the author sandbox;
- one `CriticExecution`, with exact `Reviews` and `RunsIn` relations;
- every `CriticFinding` and its `ProducedBy` relation;
- an optional existing-v1 `CriticReceipt` and its `ProducedBy` relation; and
- one `CompleteCriticActivityObservation` staged last.

The separately named completion marker preserves the existing
`TaskCriticPolicy` and `CriticReceipt` table identities. It binds the durable
result reference/digest, every result-derived fact and relation identity, the
author/critic sandbox separation, and the complete exact-subject binding when
a receipt exists. A marker with missing or conflicting facts cannot settle.
After restart, an exact committed bundle makes result redelivery a no-op; the
projector settles only against the exact receipt of the tick that committed
that bundle. Processors alone decide acceptance, repair, failure, or another
review.
