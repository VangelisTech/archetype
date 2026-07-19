# Agent mission transitions

**Document type:** Normative.

**Scope:** Provider-neutral mission, task, and completed-attempt state under
`src/archetype/app/missions/`.

This specification owns when a coding task may advance. Sandbox execution,
provider credentials, artifact publication, and repository automation are
separate capabilities and cannot bypass this authority.

## 1. Ownership and persistence boundary

The `missions` family owns:

- immutable task-plan parsing;
- deterministic task and attempt identity;
- durable provider-submission claims, leases, and fences;
- fail-closed recovery decisions and claim-fenced attempt orchestration;
- retry and exhaustion policy;
- validator, commit, checkpoint, and finalization gates;
- the typed mission and attempt-claim transition graphs; and
- terminal mission state.

It does not own a provider client, live sandbox handle, credential, world
lifecycle, or ingress authorization decision. `MissionService` is a pure
transformer over one persisted ECS row. A world processor may call it, but the
world tick remains the commit boundary for the returned row.

`MissionAttemptClaimService` is the separate control-plane authority used
before external provider I/O. It persists through the storage family's
`ControlCatalog`, because a submission fence must be durable before a world
tick can contain the completed result. Claim durability never advances a task;
only the ordinary world commit can make the corresponding completed-attempt
edge visible.

`MissionAttemptExecutionService` is the supported structural orchestration
path. It joins `MissionService`, `MissionAttemptClaimService`, and an injected
`FencedAttemptRunner` without importing a sandbox implementation. Production
callers do not manually interleave claim, sandbox, row-transition, and
settlement operations. For an indexed gate it also consumes the mission-owned
`iMissionArtifactFinalizer` port; the application layer adapts that port to the
artifact family without giving either family authority over the other's state
machine.

The component columns are strings because LanceDB's Pydantic-to-Arrow bridge
does not support Python enum fields. That storage constraint does not make
states free-form: `MissionStatus`, `TaskStatus`, `AttemptStatus`,
`CheckpointStatus`, `FinalizationPhase`, and `MissionTransitionEvent` are the
only accepted values. Component validators reject invalid construction, and
`MissionTransitionGraph` parses every persisted value again before use.

The control catalog has the same typed-state rule. `AttemptClaimStatus`,
`AttemptClaimEvent`, and `AttemptClaimTransitionGraph` own every persisted
submission edge; storage adapters implement compare-and-swap persistence but
do not invent recovery policy.

Completed checkpoint state is `created`, `failed`, or `disabled`; `pending` is
the component's pre-attempt default. The sandbox family's transport-level
`ready` outcome is translated explicitly to durable `created` at
`MissionService.apply_attempt`. This boundary translation keeps the two
families independent while preserving one persisted vocabulary.

## 2. Typed mission/task/attempt transition graph

One immutable graph owns the state of all three scopes. Its source is the
persisted `(mission status, current-task status)` pair. Its edge records one
attempt result and its event. Its target is the next persisted mission/task
pair.

Legal source pairs are:

| Mission | Current task | Meaning |
|---|---|---|
| `ready` | `ready` | No attempt has completed. |
| `running` | `ready` | A prior task advanced and the next task is ready. |
| `running` | `retryable` | The current task retained control after an unsuccessful attempt. |

Each active source admits the same eight explicit edges:

| Event | Attempt | Target mission | Target task |
|---|---|---|---|
| `rejected_retry` | `rejected` | `running` | `retryable` |
| `incomplete_retry` | `incomplete` | `running` | `retryable` |
| `failed_retry` | `failed` | `running` | `retryable` |
| `rejected_exhausted` | `rejected` | `failed` | `exhausted` |
| `incomplete_exhausted` | `incomplete` | `failed` | `exhausted` |
| `failed_exhausted` | `failed` | `failed` | `exhausted` |
| `task_advanced` | `accepted` | `running` | `ready` |
| `mission_succeeded` | `accepted` | `succeeded` | `passed` |

Terminal states have no outgoing edge. A row outside this graph fails closed;
the service does not repair it by guessing.

Every completed attempt persists its graph edge in `Attempt`:

- `mission_status_before` and `task_status_before`;
- `transition_event` and authoritative terminal `status`;
- `mission_status_after` and `task_status_after`; and
- the provider's raw terminal status separately as `provider_status`.

Append-only world history therefore retains the edge even when a successful
multi-task attempt selects the next task in the same committed row.

## 3. Attempt and evidence gate

`prepare_attempt`:

1. parses and validates the persisted source state;
2. selects the immutable plan entry at `step_index`;
3. validates the non-empty prompt and deterministically normalizes every
   validator;
4. requires coherent attempt counters below `max_attempts`;
5. derives the next positive attempt index;
6. parses the typed minimum finalization phase;
7. hashes the canonical full plan into a stable plan identity;
8. hashes world, run, entity, source state, plan, task, and attempt identity
   together with `max_attempts` and the finalization threshold into one
   deterministic idempotency key; and
9. derives stable mission, task, attempt, and semantic request identities that
   retain both policy values.

Validator normalization requires at least one validator, unique non-empty
names, no caller-supplied reserved `git_tree_change` gate, a non-empty command
sequence containing only strings, a numeric `expected_returncode`, and a
positive numeric `timeout_seconds`. Omitted values canonically default to
return code `0` and timeout `900` seconds. This happens during
`prepare_attempt`, before claim acquisition or arming, so malformed or
ambiguous validator input cannot create provider-submission state.

The request carries its source state, step index, full-plan identity,
normalized validators, `max_attempts`, required finalization phase,
provider-neutral request fingerprint, and the stable correlation needed to
reconstruct it after process loss. The request also persists the exact
non-negative `observation_tick` at which the claim was first prepared. That
tick is evidence, not provider-submission identity: it is excluded from the
request fingerprint, idempotency key, provider fingerprint, and claim key.
Preparing an otherwise unchanged task on a later tick therefore reacquires the
same claim and restores the first durable request rather than replacing its
observation. Artifact preparation derives its bundle tick only from that
persisted value. Migrated v7 requests, which predate the field, recover with
`observation_tick = 0`.

`apply_attempt` rejects a stale request if any of them, the retry budget, or the
required finalization phase changed before settlement. This prevents an
in-flight result from becoming final or advancing against policy or a plan that
was edited after execution began, even when its current step text stayed
unchanged.

An attempt advances only when all of the following hold:

- the receipt identity matches the request;
- validator details are non-empty JSON objects;
- the provider reports an accepted result;
- a checkpoint has `created` status, a non-empty state reference, and is
  restorable;
- finalization meets the task's typed minimum phase; and
- a non-empty commit SHA exists.

Provider acceptance without complete recovery/finalization evidence becomes
the authoritative attempt state `incomplete`. It is not silently treated as
success. Rejection, incomplete evidence, and provider failure remain committed
attempts and retain the current task until the retry budget is exhausted.

## 4. Tick and task advancement semantics

A world tick is a commit opportunity, not a synonym for model execution. A
processor may execute no attempt, one attempt, or only recovery work. An
unsuccessful attempt does not abort the tick: the rejected or incomplete graph
edge is valuable state and must remain queryable.

For a successful non-final task, the same output row records the accepted
attempt edge and selects the next task with status `ready` and attempt count
zero. For the final task, the graph enters `succeeded/passed`. Exhaustion enters
`failed/exhausted`. Booleans such as `finished`, `succeeded`, and `passed` are
compatibility projections and must agree with their typed status.

Only the world's ordinary two-phase tick commit makes a returned transition
durable and visible. `MissionService` never writes around that boundary.

## 5. Durable pre-execution claim and recovery

No sandbox or model invocation is authorized until a deterministic attempt
claim is durable. Its immutable identity binds world, run, mission, task,
attempt, mission idempotency key, canonical request JSON, request fingerprint,
normalized validators, retry and finalization policy, provider identity,
provider request fingerprint, and declared recovery capabilities. On the
supported path, those provider fields come directly from
`runner.provider_execution_capabilities`, which fingerprints the selected
adapter and its effective execution specification. The caller cannot pair an
unrelated capability record with the runner. Reusing a claim identity with
changed immutable input is a conflict.

`MissionAttemptClaimService` requires an injected `iRedactionService`; there is
no unscanned construction path. Before the first catalog claim, it scans the
canonical request JSON and runner-derived provider capabilities. A finding in
either semantic payload raises typed quarantine and leaves no claim row. The
active `redaction_policy_id` is included in the immutable claim fingerprint, so
the same request and provider under a different policy are not silently treated
as equivalent.

Each accepted phase retains a typed `RedactionReceipt` in the claim's fixed
redaction-evidence schema:

| Evidence key | Input and disposition |
|---|---|
| `request` | Canonical mission request; any finding quarantines before claim creation. |
| `provider` | Runner provider identity and capability metadata; any finding quarantines before claim creation. |
| `acknowledgement` | Provider session/request identity; any finding quarantines before acknowledgement CAS. |
| `outcome` | Semantic identity and references quarantine; narrative values are deterministically redacted. |
| `last_error` | Narrative error text is redacted and bounded before terminal CAS. |

All receipts carry the same policy ID and contain counts and rule IDs, never
matched text. The catalog retains immutable acquisition evidence separately
from the evolving latest phase evidence, so later acknowledgement or settlement
cannot weaken reacquisition checks. If outcome redaction finds a narrative
secret, settlement preserves that original finding receipt; a defensive rescan
of the sanitized value cannot replace it with a misleading clean receipt.

Outcome fields that determine source meaning or recovery are never rewritten.
Attempt and provider IDs, idempotency and request fingerprints, statuses,
session and checkpoint identity, artifact/trace/filesystem/Git/context
references, commit SHA, validator names and command arguments, and result keys
are safe-metadata fields. A finding in one of those fields quarantines the
outcome without projection or settlement. Narrative fields such as validator
output, friction, messages, and error detail may be redacted. The only
subsequent semantic advancement is the claim-owned `finalizing -> settled`
upgrade described below: it preserves the staged source outcome and adds the
exact authority from the durable `INDEXED` row. The sanitized outcome is
validated before either current-write projection or a catalog CAS. Public
`MissionService.apply_attempt` is intentionally narrower: it categorically
rejects an `indexed` phase and any current artifact staging, linkage, finalized
authority, or nonzero snapshot. Only the execution service may project those
fields: it asks the storage-bound claim service to
`require_settled(world_id, claim_key)`, then immediately invokes the mission
service's private settled-row transformer. The authority is that durable
reread, never a detached or caller-replaced `AttemptClaim` value, so a caller
cannot create artifact authority with a self-consistent-looking outcome. Only
the redacted error and canonical claim outcome are eligible for a catalog CAS.

Policy drift fails closed for every non-terminal claim operation, including
renewal, grant consumption, acknowledgement, outcome preparation, and
settlement. A settled claim remains readable after policy rollout: duplicate
acquisition and terminal replay use its persisted policy identity and sanitized
outcome without mutating or reinterpreting the historical record.

Acquisition is a fenced lease:

- the first claimant creates `claimed` at fence epoch one;
- the same live claimant observes `owned` without changing the fence;
- a different claimant cannot acquire a live lease;
- after expiry, one recovery claimant takes ownership at the next fence epoch;
- callers use a unique claimant identity for each worker incarnation rather
  than a shared pool or deployment name;
- a database uniqueness constraint permits only one claim key for each
  `(world, mission, task, attempt)` identity;
- every renew or transition is a claimant-and-fence compare-and-swap, so a
  displaced worker cannot acknowledge or settle; and
- a settled claim replays as `duplicate` instead of creating new work.

Before returning an `execute` authorization, the service durably transitions
`claimed -> possibly_submitted` through `arm_submission` and stores a fresh,
opaque execution nonce for that fence. The authorization carries this
single-use grant, but the official sandbox path must still consume it through
the catalog immediately before provider preparation or invocation. This
ordering is deliberately conservative: a crash immediately before the network
send and a crash immediately after it both recover as `possibly_submitted`.
Catalog state therefore never asserts that an external request was not sent
when it cannot prove that fact.

Arm is a strict status compare-and-swap, not an idempotent same-target write.
If two decisions race under the same claimant and fence, exactly one can move
`claimed -> possibly_submitted`, mint the fence's nonce, and receive `execute`;
the loser re-reads the claim and receives `reconcile`. The same strict rule
prevents stale acknowledgement or settlement retries from silently replacing
different evidence. Identical acknowledgement and terminal evidence may
converge only after the claim service explicitly compares the stored values.
Artifact staging and final settlement follow the same rule: a lost-response
retry may converge only when the stored canonical outcome, exact prepared
request, digests, publication key, policy, and terminal artifact row all match.

The complete claim graph is:

| Source | Event | Target | Meaning |
|---|---|---|---|
| `claimed` | `arm_submission` | `possibly_submitted` | Persist uncertainty and the fence's single-use execution nonce. |
| `possibly_submitted` | `acknowledge_provider` | `provider_acknowledged` | After grant consumption, persist a provider session or request identity. |
| `provider_acknowledged` | `stage_finalization` | `finalizing` | Persist the sanitized outcome and exact prepared artifact request before artifact I/O. |
| `claimed` | `settle_without_submission` | `settled` | Finish work proven not to require provider submission. |
| `possibly_submitted` | `settle_after_reconciliation` | `settled` | Reconciliation produced the terminal outcome. |
| `provider_acknowledged` | `settle_acknowledged` | `settled` | The acknowledged operation produced the terminal outcome. |
| `finalizing` | `settle_finalized` | `settled` | Bind a service-sealed outcome derived from the exact durable `INDEXED` or `EXPIRED` artifact row. |

Every other edge fails closed. Consumption is a separate atomic catalog
compare-and-swap over `possibly_submitted`, claimant, fence, nonce, an
unconsumed marker, and an unexpired lease. Success records
`execution_consumed_at`. A stale fence, expired lease, duplicate consumption,
changed nonce, or settled claim cannot consume the grant. Provider
acknowledgement also requires that this grant was consumed.

For a task whose required finalization phase is `indexed`, the supported path
stages portable evidence for every recoverable provider `accepted` or
`rejected` outcome. Both require a restorable checkpoint and captured,
checkpointed, uploaded, or legacy published handoff evidence; an accepted
outcome additionally requires its non-empty commit SHA. A rejected attempt is
still published for review and recovery, but remains authoritatively rejected
after indexing and cannot advance the task.

Artifact preparation performs no source, object-store, or index I/O. It binds
the claim's redaction policy and produces one exact projection:

- canonical artifact `request_json`;
- `request_digest`, the SHA-256 of that exact JSON including the bound policy;
- deterministic `publication_key`;
- policy-independent `producer_digest`; and
- `redaction_policy_id`.

The `stage_finalization` CAS persists the exact JSON, sanitized provisional
outcome, and outcome digest in the claim. That provisional outcome carries all
four linkage markers: publication key, exact request digest, producer digest,
and redaction-policy identity. The claim also stores the prepared projection
fields independently, and reaches `finalizing` only after the two copies agree
and all of that identity is durable. No artifact publication may be
reconstructed from mutable world input or a caller object after this edge. The
artifact family receives only the persisted projection and durably drives its
own `PENDING -> UPLOADED -> INDEXED` outbox.

An `INDEXED` row is authoritative for the mission only when its bundle ID,
exact request JSON and digest, producer digest, redaction policy, and attempt
identity equal the staged projection, its manifest reference is non-empty, and
its index snapshot is an exact positive signed-64-bit integer. An `EXPIRED` row
is authoritative only when its terminal status and the same staged identity
agree. The claim service rereads that row from its storage-bound control
catalog; a caller-supplied receipt is never settlement authority. Only then
does it construct and seal the corresponding prepared settlement. Generic
`settle(...)` categorically rejects a current `finalizing` claim. The seal is
process-local and binds the claim/fence, staged request and policy, result kind
and status, and redacted outcome; cold recovery remints it from durable state.
The execution service passes that seal to `settle_finalized(...)` first. After
the terminal CAS succeeds or converges with an identical prior settlement, it
calls `require_settled(world_id, claim_key)` to reread and authenticate the
winning terminal row, then projects only that stored outcome through the
mission service's private settled-row transformer. A supplied or replaced
`AttemptClaim` DTO cannot enter that transformer through `iMissionService`.
Changed staging or durable evidence is a conflict; byte-identical lost-response
retries are idempotent.

Legacy mission phase `published` is an explicit compatibility value, not an
artifact-publication state. It can satisfy only the historical `pending`,
`captured`, `checkpointed`, and `published` gates. It never proves `uploaded`
or `indexed`; under an `indexed` gate it is only eligible input to the staged
outbox and must still reach and authenticate the exact durable `INDEXED` row.
Likewise, a raw sandbox outcome cannot self-assert `indexed`, even when the
task requires only an earlier phase: claim validation requires the staged
request linkage. Already-settled legacy outcomes remain readable, but no new
unbound indexed fact may be created.

That compatibility is explicit and migration-proven. The v7-to-v8 catalog
migration sets `legacy_unbound_eligible` only for claims that were already
`settled` under an `indexed` gate before the new artifact-authority columns
existed. Non-indexed v7 claims continue through ordinary historical assessment;
the read boundary also normalizes any such row overmarked by an early
phase-agnostic v8 backfill. Projection still requires the narrow accepted
`published` or `indexed` legacy shape with no staged or finalized artifact
authority. A qualifying replay exposes
`Finalization.legacy_unbound=true` in the world row; current v8 writes always
expose `false`. This is a compatibility classification, not synthesized
artifact provenance: the canonical legacy outcome remains byte-for-byte
unchanged. Migration-proven v7 rows remain exclusively legacy even if their
stored JSON contains fields whose names resemble current artifact authority;
those extras are inert and are never projected as a bundle, manifest, or
snapshot. The same authority-shaped extras on a v8 row fail closed.

Terminal settlement accepts only a complete replayable sandbox outcome bound
to the claimed attempt, idempotency key, attempt index, and normalized sandbox
request. It validates provider status and `accepted`, validator and result
shape, checkpoint coherence, finalization phase, and accepted commit evidence.
For a direct non-finalizing outcome, the official execution service first uses
strict current-write `MissionService.apply_attempt`; its authoritative
`AttemptStatus` is the status stored by direct settlement and must agree with
the outcome's provider semantics. A finalized `INDEXED` or `EXPIRED` outcome
uses the claim-bound order instead: authenticate the terminal durable row,
construct and seal the prepared settlement, settle `finalizing -> settled`,
reread and authenticate the winning row through `require_settled`, and only
then invoke the execution workflow's private settled-row transformer. Public
`iMissionService` exposes no settled projection operation, and `apply_attempt`
categorically rejects that indexed or linkage-bearing authority.
An accepted provider outcome requires durable grant-consumption evidence,
whether the mission derives authoritative `accepted` or `incomplete` from its
checkpoint/finalization gate. The outcome's checkpoint provider must equal the
provider bound into the claim, and its agent session must equal the durable
provider acknowledgement. Settlement stores the derived status, canonical
outcome JSON and digest, and any retained error.

A crash after claim settlement but before the world tick commits can therefore
reconstruct the original request and replay the complete terminal outcome into
the completed-row transition without another provider call. First completion
and every terminal replay call `require_settled(world_id, claim_key)` to reread
the claim that won the CAS and authenticate its canonical outcome JSON, digest,
settlement status, attempt identity, and explicit legacy classification. The
execution service then invokes the mission service's private settled-row
transformer; public `apply_attempt` remains strict for every new write. A
detached or caller-replaced claim DTO cannot authorize this path, and the same
attempt never re-enters the runner or artifact outbox after terminal
settlement.

The remote control catalog probes `catalog_protocol_version >= 4` and the
`attempt_claim_execution_v2` capability before claim acquisition, every
status/evidence transition, and execution-grant consumption. Those operations
use only the versioned `acquire-v2`, `transition-v2`, and `consume-v2` routes.
A mixed old-Worker/new-client deployment therefore fails before claim mutation
or paid provider admission instead of discovering incompatibility after
execution.

Recovery policy is derived only from durable claim state:

- a fresh `claimed` attempt is durably armed with a new execution nonce and
  then receives `execute`; the provider call still requires atomic grant
  consumption;
- `possibly_submitted` always receives `reconcile`;
- `provider_acknowledged` always receives `reconcile`;
- `finalizing` always receives `finalize`, which publishes only the persisted
  artifact projection; and
- a settled attempt receives `settled`, which never authorizes model work.

Cold `finalize` recovery does not invoke `FencedAttemptRunner`, a model,
validators, repository finalization, or checkpoint creation/refresh. If the
artifact family's durable row is still `PENDING`, its resolver may read or
restore the already-bound checkpoint solely to materialize the staged portable
files. Once that row is `UPLOADED`, recovery requires neither the sandbox nor
the checkpoint. If the exact `PENDING` publication has durably expired, recovery
requires the exact terminal `EXPIRED` artifact row, binds it to the staged
projection, and settles the provider outcome as incomplete or rejected
with the generic `artifact_publication_expired` marker. A claim alone cannot
mint expiry. None of these cases re-enters attempt execution; any later work is
a newly identified attempt.

Provider request fingerprints, idempotency keys, session identities, and
capability flags remain durable metadata for a future concrete recovery
adapter. Metadata alone never authorizes inference. The current claim service
never issues `replay_idempotent` or `resume_session`, and the sandbox rejects
both actions because no provider transport implements them.

Every authorization binds both layers of request identity: the immutable claim
fingerprint and the exact normalized sandbox invocation fingerprint. The latter
covers prompt, normalized validator commands and defaults, task name, attempt
index, prior session and validator evidence, and correlation. An `execute`
authorization additionally carries the current fence's opaque nonce. Sandbox
correlation must match the claimed world, run, entity-derived mission, and task
step before any mutation occurs.

The supported orchestration order is:

1. `MissionService.prepare_attempt` normalizes validator input and derives the
   deterministic request, including retry and finalization policy. Invalid
   input fails before any claim is acquired.
2. `MissionAttemptExecutionService` acquires the claim under a worker-
   incarnation identity using `runner.provider_execution_capabilities`, then
   asks the claim service for one fenced decision. The claim service quarantines
   the canonical request or provider metadata before catalog acquisition. The
   execution service accepts no independent caller-supplied capability record.
3. A fresh claim is armed with one nonce before `execute`; an uncertain claim
   enters only `reconcile`.
4. For either runnable decision, the execution service starts the structural
   runner and a lease heartbeat together. The heartbeat renews the active claim
   for the runner's entire lifetime. Renewal failure cancels and awaits the
   runner before failing closed; caller cancellation cancels and awaits both
   local child tasks, so no local runner or renewal task is orphaned. This does
   not prove that a remote provider operation terminated.
5. The structural sandbox runner validates the authorization, exact invocation,
   and any matching receipts. If provider execution is required, it invokes the
   execution service's authorization callback to consume that exact nonce
   atomically through the catalog before preparing or calling the provider.
   Failed consumption stops the attempt.
6. Immediately after provider execution returns, the runner invokes the
   acknowledgement callback before validation or any later phase. A durable
   provider session or request identity moves a consumed claim to
   `provider_acknowledged`; an unconsumed grant cannot be acknowledged.
7. The sandbox completes validation, repository finalization, evidence,
   checkpoint, and artifact handoff, or reconciles those phases from matching
   receipts without inference.
8. After the runner completes, the execution service stops and awaits the
   heartbeat, renews the active claim once more, verifies consumed-grant and
   provider-session evidence, quarantines semantic outcome fields, redacts
   narrative outcome values, and assesses the sanitized result. An indexed
   gate with eligible accepted or rejected recovery evidence is prepared and
   moved to `finalizing` before artifact I/O. Other outcomes pass through
   `MissionService.apply_attempt` and direct terminal settlement.
9. A finalizing worker publishes only the exact persisted projection while a
   claim heartbeat remains active. The artifact family independently recovers
   its durable `PENDING -> UPLOADED -> INDEXED` state machine.
10. The claim authority rereads and validates the exact durable `INDEXED` or
    `EXPIRED` artifact row, constructs and seals the only permitted finalized
    outcome from the staged outcome, and settles `finalizing -> settled`. It
    then calls `require_settled(world_id, claim_key)` to re-read and authenticate
    the outcome that won the terminal CAS and invokes the private settled-row
    transformer, so first completion and replay expose the same row projection.
    Public `iMissionService` offers no settled-projection method, and
    `apply_attempt` cannot accept the indexed or linkage-bearing outcome.
11. If acquisition finds `finalizing`, the execution service performs step 9
    directly without invoking the sandbox runner. If it finds `settled`, it
    verifies and applies the stored outcome without invoking the runner or
    artifact finalizer.

All sandbox entry, including `reconcile`, requires the live lease carried by
the current fence, and supported orchestration maintains that lease until the
runner has fully stopped. Reconciliation without matching repository or final
durable evidence fails closed; it cannot fall through to model execution.

The crash matrix is consequently:

| Crash point | Durable recovery state | Permitted next action |
|---|---|---|
| Before claim acquisition | No claim | Acquire first; provider execution is forbidden. |
| After acquisition, before arm | `claimed` | Recover the expired fence, arm uncertainty, then execute. |
| Concurrent arm under one claimant/fence | One `possibly_submitted` with one nonce; loser observes it | One `execute`; every loser receives `reconcile`. |
| After arm, before grant consumption | `possibly_submitted`; nonce unconsumed | Reconcile only; the issued grant is not minted again. |
| Concurrent grant consumption | One `execution_consumed_at` write | One caller may enter provider preparation; every duplicate fails closed. |
| After grant consumption, before send | `possibly_submitted`; nonce consumed | Reconcile only; never infer that no send occurred. |
| Lease heartbeat fails during runner work | Last durable uncertain or acknowledged state | Cancel and await the local runner task; do not apply or settle. Any remote operation remains adapter-specific and must reconcile. |
| Caller cancels during runner work | Last durable claim state | Cancel and await both local child tasks. Remote work may remain `possibly_submitted`; adapter-specific cancellation or reconciliation owns it. |
| After send, before acknowledgement callback commits | `possibly_submitted` | Reconcile only; missing receipts cannot authorize another model call. |
| After acknowledgement, before validation/finalization | `provider_acknowledged` | Reconcile only against provider or sandbox evidence. |
| After repository receipt, before evidence/checkpoint/handoff | Claim remains uncertain or acknowledged | With a live lease, reconcile through the receipt; skip model, validators, commit, and push. |
| After final sandbox receipt, before artifact preparation | `provider_acknowledged` plus sandbox receipt | Reconcile the receipt without model or repository work, sanitize it, and deterministically prepare again. |
| After preparation, before staging CAS | `provider_acknowledged`; prepared projection existed only in memory | Reconcile the same receipt, reproduce the exact projection, and retry staging. No artifact I/O was authorized. |
| During or after staging CAS response loss | Either `provider_acknowledged` or `finalizing` | Re-read. Retry only the exact staging values; changed JSON, digests, key, policy, or outcome fails closed. |
| After `finalizing`, before artifact publication claim | `finalizing` plus exact projection and provisional outcome | Cold `finalize`; do not invoke the runner, model, validators, repository finalization, or checkpoint creation. |
| After artifact claim, before upload | Mission `finalizing`; artifact `PENDING` plus exact request | Resume the artifact outbox from its durable request; the resolver may read the already-bound checkpoint. |
| Artifact retry window elapses before upload | Mission `finalizing`; exact artifact row is `EXPIRED` | Bind the terminal row to the staged projection, add only `artifact_publication_expired`, and settle incomplete or rejected. A bare claim or process-local exception cannot assert expiry; the same attempt never invokes the runner or publication again. |
| During upload | Mission `finalizing`; artifact `PENDING` plus content-addressed objects | Read back and byte-verify present payload and manifest objects; reuse exact objects and replace truncated or corrupt ones before persisting upload metadata. |
| After upload metadata | Mission `finalizing`; artifact `UPLOADED` plus complete records | Index without sandbox or checkpoint access. |
| After Iceberg commit, before artifact catalog completion | Query rows visible; artifact `UPLOADED`; mission `finalizing` | Verify deterministic rows, mark the artifact `INDEXED`, and continue finalization. |
| After artifact `INDEXED`, before mission settlement | Mission `finalizing` plus exact terminal artifact row | Reread and bind the row, construct the sealed settlement, and settle; do not project or invoke external attempt work yet. |
| During or after final settlement response loss | Either `finalizing` or `settled` | Re-read. Retry only the exact finalized outcome and status; once settled, authenticate the durable winner with `require_settled`. Conflicting evidence fails closed. |
| After claim settlement, before world commit | `settled` plus canonical indexed or direct outcome | Call `require_settled(world_id, claim_key)`, then let the execution service's private row transformer apply that durable winner into the same completed-attempt edge without entering the sandbox or outbox. A supplied claim DTO is not authority. |
| After world commit | `settled` claim plus visible completed-attempt row | Duplicate recovery performs no provider or publication work; ordinary world history is authoritative. |

This is a one-live-executor, one-authorized-provider-call, and no-blind-replay
guarantee, not an exactly-once provider-side-effect claim. The single-use grant
atomically controls entry into provider preparation, but its consumption and
the provider transport cannot be one transaction. A crash or transport failure
after consumption may leave the external side effect absent, complete, or
unknown. That uncertainty remains explicit until provider or sandbox evidence
lets reconciliation prove a safe result.

## 6. Executable enforcement

- `tests/app/test_mission_transition_authority.py` exhausts every graph edge,
  terminal rejection, validator normalization, stale plan and policy requests,
  evidence gate, retry, and multi-task advancement.
- `tests/app/test_mission_attempt_claims.py` exhausts the claim graph, concurrent
  acquisition and arm decisions, single-use execution grants, cold restart and
  takeover, stale-fence rejection, complete outcome, status, consumed-grant,
  checkpoint-provider, and acknowledged-session binding, proof that capability
  metadata does not authorize execution, request/provider/acknowledgement and
  semantic-reference quarantine, narrative outcome/error redaction, policy
  drift, exact finalization staging and settlement, accepted and rejected
  indexed outcomes, cold `finalize`, conflicting CAS replay, and semantic
  terminal replay.
- `tests/app/test_mission_artifact_finalizer.py` proves deterministic prepared
  requests, policy binding, exact prepared publication, receipt identity, and
  fail-closed mismatches at the mission/artifact family boundary.
- `tests/app/test_indexed_finalization_crash_oracle.py` cold-restarts the
  storage-bound public workflow at eight durable boundaries from preparation
  through mission settlement. It uses real local objects, Iceberg, and SQLite
  state to prove one provider admission, one logical publication, exact
  original observation tick, query/raw-row parity, and side-effect-free
  terminal replay.
- `tests/app/test_sandbox_kernel.py` proves invocation binding, live-lease
  enforcement, runner-derived provider capability binding, unsupported
  replay/resume rejection, acknowledgement ordering, execution-service
  heartbeat and cancellation lifetime, post-run renewal, settlement, receipt
  reconciliation, crash recovery, and settled replay without a second model
  call.
- `tests/app/test_remote_catalog_parity.py` proves the local SQLite and remote
  control-catalog implementations expose the same identity, strict-CAS,
  single-use grant, staged artifact fields, `finalizing` recovery, and claim
  lifecycle semantics.
- `evals/suites/capability/agent_missions.py` grades a credential-free rejected,
  incomplete, then accepted sequence without importing the feature tests.
- `mission_attempt_claim_recovery` grades cold claim recovery and settlement as
  a blocking capability eval.
- `mission_indexed_finalization_gate` grades fail-closed pre-index state, cold
  `finalize` recovery, exact durable row identity, and side-effect-free terminal
  replay as a blocking capability eval.
- `quality/contracts.toml` registers
  `missions.transition.evidence_gated`, `missions.attempt.claim_fenced`, and
  `missions.attempt.indexed_finalization` as blocking PR contracts.
- `quality/architecture.toml` limits the claim service to mission-owned models
  and the storage control catalog, permits only mission-family dependencies in
  the execution service, and lets the sandbox consume only the immutable fenced
  authorization and recovery action.
