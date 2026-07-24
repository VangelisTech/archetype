---
title: Token costs and quotas
description: Commands-owned token costs, world/tick quotas, and daily budgets
---

Every registered actor-aware operation has an explicit token cost.
`CommandDispatcher` resolves that cost from the exact `OperationSpec`, and the
instance-owned `Policy` authorizes and debits it before the family effect or
durable admission.

There is no unknown-command default. An unregistered model fails exact registry
resolution; an unknown permission is denied for every built-in role.

## Token costs

Current world-operation registrations use:

| Operation | Cost |
|---|---:|
| `get_world_info`, `list_worlds`, `discover_worlds`, `open_world_readonly` | 2 |
| `list_signatures`, `list_world_signatures`, `list_processors`, `list_hooks`, `list_resources` | 2 |
| `despawn`, `remove_components`, `remove_processor`, `remove_hook` | 5 |
| `query_components`, `query_archetype`, `get_audit_history` | 5 |
| `update`, `add_components` | 8 |
| `spawn`, `create_entities`, internal reservation/spawn | 10 |
| `destroy_world`, `step`, `add_resource`, `add_hook` | 10 |
| `add_processor` | 15 |
| `create_world`, `resume_world`, `run` | 50 |
| `fork_world` | 100 |
| `run_rollout` | 200 |
| `run_episode` | 500 |

The finite PR3 bridge charges:

| Operation | Cost |
|---|---:|
| `query_artifacts` | 5 |
| `ingest_artifacts`, `evaluate` | 10 |
| `autoresearch` | 200 per requested iteration, minimum one |

Those bridge costs move into exact registrations when their owning families
land. Family models do not carry costs, and callers cannot override them.

## Quotas

Two independent limits apply per `Policy` instance:

| Quota | Default | Coordinate |
|---|---:|---|
| Commands per world/tick | 500 | `(actor_id, world_id, target_tick)` |
| Daily token budget | 200,000 | actor identity and UTC date |

Changing the live world tick selects a new quota generation; no world callback,
process-global reset, or shared module counter exists. Actors, worlds, target
ticks, and runtime/container instances are isolated.

Application-scoped operations such as `create_world` and `list_worlds` do not
invent a world/tick bucket. They consume only the daily token budget.

The UTC daily generation rolls lazily on the next authorization. A naive clock
is treated as UTC; an aware clock is normalized to UTC.

## Enforcement order

```text
exact registration
    |
pure role preauthorization
    |
availability and bounded coordinates
    |
Policy.authorize / authorize_application
  1. validate coordinates and cost
  2. project world/tick count
  3. roll and project daily tokens
  4. commit both debits atomically
    |
family effect or scheduler admission
```

Role denial happens before target-tick resolution or quota state. A full quota
denial happens before the handler or scheduler and may produce bounded denial
evidence.

Deferred calls use `DurableOptions.target_tick` directly. Direct live-world
calls use the current target tick. Durable-world reads use the live tick when
available and the reviewed tick-zero bucket only when the live binding is
absent or closing.

## Atomic batches

`defer_batch_as` first preauthorizes every item, verifies every durable
disposition, and derives every bounded request. `Policy.authorize_batch` then
projects all world/tick counts and the combined daily cost before changing
state.

If any item is denied or either projected limit is exceeded:

- no quota debit is committed;
- no scheduler admission occurs; and
- no reservation is allocated.

One same-world catalog admission follows only after the atomic policy debit.

## Roles and quotas

Quota availability never grants permission. A viewer with unused budget cannot
spawn; an operator with unused budget cannot create or resume a world. See
[Command gate](command-gate.md) for the exact role matrix.

Trusted actor-free `CommandDispatcher.apply` and `defer` calls do not fabricate
a principal or debit an actor budget. Hosts exposing trusted entry to
untrusted code violate the boundary.

## Source reference

- policy and role grants: `src/archetype/commands/policy.py`
- per-operation costs and quota scopes: `src/archetype/app/container.py`
- enforcement order and batches: `src/archetype/commands/dispatch.py`
- durable options: `src/archetype/commands/models.py`
- executable policy contracts:
  `tests/commands/test_dispatch_policy_contracts.py` and
  `tests/app/test_tick_quota_reset.py`
