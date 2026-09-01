# Activities

**Document type:** Normative accepted-target contract.

**Status:** Temporal is the single authority for durable orchestration. The
generic Archetype Activity boundary retains only committed admission, bounded
result evidence, ECS observation, and exact-receipt settlement.

**Scope:** Work admitted from one committed tick whose factual result is
observed by a later committed tick. This specification refines the
required-projector rule in [Atomic Tick Visibility](atomic-visibility.md)
without changing tick atomicity.

## 1. Resource and Activity

> A Resource is a capability available while executing a tick. Correctness
> must not depend on its process-local lifetime.
> An Activity is external work derived from committed world intent and
> supervised by the application's durable orchestrator.

Temporal owns scheduling, retries, timers, cancellation, worker replacement,
and workflow history. Archetype does not duplicate those facilities in an
Activity catalog. Family Components remain the semantic record, while the
slim Activity settlement index binds committed intent to a bounded result and
the later receipt that observed it.

An Activity is not:

- a generic ECS Component;
- a live sandbox, provider client, simulator, model, or robot handle;
- a SQLite-backed attempt, lease, claim, or fence;
- permission for a provider result to advance workflow state directly; or
- a second workflow state machine competing with Temporal or ECS processors.

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
Temporal supervises provider work outside the world lock
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
`CommittedTickReceipt`. It deterministically admits Activity intent using the
owning family's stable logical identity. Repeating projection is an idempotent
no-op only when immutable identity and request digest agree; conflicts fail
closed.

Both source and observation receipts carry a durable, non-empty visibility
token. Tokenless or uncoordinated ticks cannot admit or settle an Activity.
The projector performs no provider, sandbox, Git, simulator, model, or
hardware I/O.

Temporal execution records the bounded result, or a durable reference to a
larger result, before ECS observation staging. Staging is idempotent and may
repeat after restart. Family-owned completion evidence binds Activity kind and
identity to the exact result reference and digest. Settlement occurs only when
that binding appears in the exact later committed receipt. Settlement never
decides mission acceptance, physical success, retry, or any other semantic
transition.

The receipt reader fails closed unless the requested world, run, tick, and
visibility token identify the current committed head. Historical, sibling,
ambiguous, missing, wrong-world, or tokenless receipts cannot admit or settle
work.

## 3. Commands and Activities are different

| Boundary | Command | Activity |
|---|---|---|
| Direction | Enters world execution | Leaves one committed tick and returns to a later tick |
| Admission | Before materialization | After semantic intent is committed |
| Execution | Materialized under exact-world authority | Supervised by Temporal outside the world lock |
| Settlement | Coupled to the target tick manifest | Coupled to the later receipt that commits its observation |
| Meaning | Request to apply registered behavior | External work whose result becomes an ECS fact |

Atomic commands remain in Archetype. Commands that start a durable process may
delegate orchestration to Temporal, but Activity settlement does not reuse the
command scheduler.

## 4. Authority

| Owner | Responsibility |
|---|---|
| Temporal | Workflow history, scheduling, timers, retries, cancellation, signals, queries, and worker recovery |
| `archetype.activities` | Logical identity, immutable admission, bounded result references/digests, unsettled-work queries, and observation settlement |
| `archetype.storage.activity_catalog` | The slim settlement index and its local SQLite representation; it stores ECS facts, not orchestration state |
| Owning top-level family | Semantic intent and observation schemas, provider protocol, projection, result staging, and domain decisions |
| Provider adapter | Stable provider-operation identity, reattachment, cancellation, cleanup, and bounded result publication |
| Iceberg or `archetype.artifacts` | Large or unbounded payloads published before their bounded Activity reference |

No generic recovery-policy or lifecycle-status enum exists in this boundary.
Mission Git work, simulations, and hardware retain family-specific provider
meaning while sharing Temporal orchestration and ECS settlement.

## 5. Identity and bounded durability

The settlement key is `(world_id, kind, activity_id)`. Source `run_id`, source
tick, visibility token, input reference, and input digest are immutable. The
index stores only:

- logical Activity identity and kind;
- source world, run, committed tick, and visibility token;
- immutable input reference and digest;
- bounded result reference and digest, once available; and
- the exact later committed receipt that observed the result.

Provider call IDs and workflow IDs belong to Temporal history and provider
integration, not the SQLite settlement index. Secrets, live handles,
transcripts, repositories, trajectories, and frames are excluded. Large
results are first persisted through storage or artifacts, then referenced by
digest. Repeating the same write is idempotent; a different digest for the same
identity is a conflict.

### World lifecycle interaction

V1 does not transfer an unsettled Activity across world lineage. Under the
source world's exact-world lock, `fork_world` and `destroy_world` refuse while
the settlement index reports admitted work without a later observation
receipt. Once settled, its observation is ordinary lineage-visible ECS state.
A fork never inherits a settlement row or adopts a provider operation.

## 6. Resource-spike disposition

The `AsyncResources`/WorldHost prototype remains architecture evidence, not an
Activity durability implementation. Process hosts may own clients, placement,
readiness, and teardown; Temporal owns durable orchestration. The core
`Resources` bag remains a plain type-keyed capability container.

## 7. Verification gates

The boundary conforms when tests prove:

- exact-receipt-bound, deterministic, idempotent admission;
- fail-closed rejection of wrong, historical, ambiguous, or tokenless receipts;
- no provider I/O in projectors or under the world lock;
- Temporal-owned execution produces no SQLite attempts, claims, leases, or fences;
- a bounded result is durable before idempotent ECS staging;
- settlement requires exact later-receipt completeness evidence;
- equal family-local IDs remain isolated across worlds and kinds;
- family processors remain the only semantic transition authority; and
- large outputs use durable bounded references.

The implementation sequence and release cut lines are tracked in
[Activity-boundary refactor](../planning/activity-boundary-refactor.md).
