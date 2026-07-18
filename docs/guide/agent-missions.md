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
- retry and exhaustion policy;
- validator, commit, checkpoint, and finalization gates;
- the typed transition graph; and
- terminal mission state.

It does not own a provider client, live sandbox handle, credential, world
lifecycle, or authorization decision. `MissionService` is a pure transformer
over one persisted ECS row. A world processor may call it, but the world tick
remains the commit boundary for the returned row.

The component columns are strings because LanceDB's Pydantic-to-Arrow bridge
does not support Python enum fields. That storage constraint does not make
states free-form: `MissionStatus`, `TaskStatus`, `AttemptStatus`,
`CheckpointStatus`, `FinalizationPhase`, and `MissionTransitionEvent` are the
only accepted values. Component validators reject invalid construction, and
`MissionTransitionGraph` parses every persisted value again before use.

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
3. validates non-empty prompt and validator objects;
4. requires coherent attempt counters below `max_attempts`;
5. derives the next positive attempt index; and
6. hashes the canonical full plan into a stable plan identity; and
7. hashes world, run, entity, source state, plan, task, and attempt identity
   into one deterministic idempotency key.

The request carries its source state, step index, and full-plan identity.
`apply_attempt` rejects a stale request if any of them changed before
settlement. This prevents an in-flight result from becoming final or advancing
against a plan that was edited after execution began, even when its current
step text stayed unchanged.

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

## 5. Current execution guarantee and next authority

This contract proves persisted completed-attempt edges. It does **not** yet
claim exactly-once model submission.

Before provider execution is production-ready, the control authority must add
a durable pre-execution claim and an explicit `possibly_submitted` recovery
state. The required crash distinction is:

- a claimed attempt that never crossed the submission boundary may execute;
- a possibly submitted attempt must reconcile provider/session evidence and
  may not be blindly submitted again; and
- terminal settlement replay must converge on the same graph edge.

That control-plane claim is intentionally separate from this completed-row
graph. It will be exercised by reliability evals for claim replay, crash after
submission, duplicate tick delivery, settlement replay, and checkpoint
reattachment.

## 6. Executable enforcement

- `tests/app/test_mission_transition_authority.py` exhausts every graph edge,
  terminal rejection, stale request, evidence gate, retry, and multi-task
  advancement.
- `evals/suites/capability/agent_missions.py` grades a credential-free rejected,
  incomplete, then accepted sequence without importing the feature tests.
- `quality/contracts.toml` registers
  `missions.transition.evidence_gated` as a blocking PR contract.
- `quality/architecture.toml` confines the family to its own models and graph.
