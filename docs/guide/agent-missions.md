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
settlement operations.

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
reconstruct it after process loss. A world tick observes the attempt; it is not
provider-submission identity and is excluded from the durable request,
fingerprint, and claim key. Preparing an otherwise unchanged task on a later
tick therefore reacquires the same claim identity instead of creating or
conflicting with provider work; lease ownership determines whether its fence is
retained or incremented.

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

Outcome fields that determine meaning or recovery are never rewritten. Attempt
and provider IDs, idempotency and request fingerprints, statuses, session and
checkpoint identity, artifact/trace/filesystem/Git/context references, commit
SHA, validator names and command arguments, and result keys are safe-metadata
fields. A finding in one of those fields quarantines the outcome without
projection or settlement. Narrative fields such as validator output, friction,
messages, and error detail may be redacted. The sanitized outcome is validated
again before `MissionService.apply_attempt`, and the redacted error and outcome
are the only values eligible for the catalog settlement CAS.

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

The complete claim graph is:

| Source | Event | Target | Meaning |
|---|---|---|---|
| `claimed` | `arm_submission` | `possibly_submitted` | Persist uncertainty and the fence's single-use execution nonce. |
| `possibly_submitted` | `acknowledge_provider` | `provider_acknowledged` | After grant consumption, persist a provider session or request identity. |
| `claimed` | `settle_without_submission` | `settled` | Finish work proven not to require provider submission. |
| `possibly_submitted` | `settle_after_reconciliation` | `settled` | Reconciliation produced the terminal outcome. |
| `provider_acknowledged` | `settle_acknowledged` | `settled` | The acknowledged operation produced the terminal outcome. |

Every other edge fails closed. Consumption is a separate atomic catalog
compare-and-swap over `possibly_submitted`, claimant, fence, nonce, an
unconsumed marker, and an unexpired lease. Success records
`execution_consumed_at`. A stale fence, expired lease, duplicate consumption,
changed nonce, or settled claim cannot consume the grant. Provider
acknowledgement also requires that this grant was consumed.

Terminal settlement accepts only a complete replayable sandbox outcome bound
to the claimed attempt, idempotency key, attempt index, and normalized sandbox
request. It validates provider status and `accepted`, validator and result
shape, checkpoint coherence, finalization phase, and accepted commit evidence.
The official execution service first applies that outcome through
`MissionService`; the resulting authoritative `AttemptStatus` is the status
stored by settlement and must agree with the outcome's provider semantics.
An accepted provider outcome requires durable grant-consumption evidence,
whether the mission derives authoritative `accepted` or `incomplete` from its
checkpoint/finalization gate. The outcome's checkpoint provider must equal the
provider bound into the claim, and its agent session must equal the durable
provider acknowledgement. Settlement stores the derived status, canonical
outcome JSON and digest, and any retained error.

A crash after claim settlement but before the world tick commits can therefore
reconstruct the original request and replay the complete terminal outcome into
the completed-row transition without another provider call. The replay passes
through `MissionService.apply_attempt` again, so it is checked by the same
identity, evidence, and transition semantics as the original application.

Recovery policy is derived only from durable claim state:

- a fresh `claimed` attempt is durably armed with a new execution nonce and
  then receives `execute`; the provider call still requires atomic grant
  consumption;
- `possibly_submitted` always receives `reconcile`;
- `provider_acknowledged` always receives `reconcile`; and
- a settled attempt receives `settled`, which never authorizes model work.

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
   narrative outcome values, and only then calls `MissionService.apply_attempt`
   with the sanitized value. The claim service redacts the terminal error,
   validates, and settles that complete outcome with the derived terminal
   attempt status.
9. If acquisition finds `settled`, the execution service verifies and applies
   the stored outcome without invoking the sandbox at all.

All sandbox entry, including `reconcile`, requires the live lease carried by
the current fence, and supported orchestration maintains that lease until the
runner has fully stopped. Reconciliation without matching repository or final
receipt fails closed; it cannot fall through to model execution.

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
| After final sandbox receipt, before claim settlement | Claim remains uncertain or acknowledged | Reconcile the cached outcome, apply the typed row transition, and settle. |
| After claim settlement, before world commit | `settled` plus canonical outcome | Apply the stored outcome into the same completed-attempt edge without entering the sandbox. |

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
  drift, conflicting settlement, and semantic terminal replay.
- `tests/app/test_sandbox_kernel.py` proves invocation binding, live-lease
  enforcement, runner-derived provider capability binding, unsupported
  replay/resume rejection, acknowledgement ordering, execution-service
  heartbeat and cancellation lifetime, post-run renewal, settlement, receipt
  reconciliation, crash recovery, and settled replay without a second model
  call.
- `tests/app/test_remote_catalog_parity.py` proves the local SQLite and remote
  control-catalog implementations expose the same identity, strict-CAS,
  single-use grant, and claim lifecycle semantics.
- `evals/suites/capability/agent_missions.py` grades a credential-free rejected,
  incomplete, then accepted sequence without importing the feature tests.
- `mission_attempt_claim_recovery` grades cold claim recovery and settlement as
  a blocking capability eval.
- `quality/contracts.toml` registers
  `missions.transition.evidence_gated` and `missions.attempt.claim_fenced` as
  blocking PR contracts.
- `quality/architecture.toml` limits the claim service to mission-owned models
  and the storage control catalog, permits only mission-family dependencies in
  the execution service, and lets the sandbox consume only the immutable fenced
  authorization and recovery action.
