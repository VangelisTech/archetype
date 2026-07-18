---
title: Token Costs and Quotas
description: Gate-level quota system, token costs per command, and rate limits
---

Every gated command has a token-cost estimate. `guardrail_allow()` enforces role
permissions, per-tick command limits, and daily token budgets before the
operation is accepted.

The command gate is `iCommandGateway`; `iCommandScheduler` owns durable
tick-deferred admission after authorization.

## Token Costs

Most command types have a fixed cost. `autoresearch` scales with its requested
iteration count because its internal rollouts are not gated separately:

| Command Type | Token Cost | Description |
|---|---:|---|
| `get_world_info` | 2 | Read world identity/tick info |
| `list_signatures` | 2 | List known archetype signatures |
| `list_worlds` | 2 | List live world identities |
| `list_processors` | 2 | List world processors |
| `list_hooks` | 2 | List world hooks |
| `list_resources` | 2 | List world resources |
| `message` | 3 | Agent-to-agent messaging |
| `despawn` | 5 | Remove an entity |
| `remove_component` | 5 | Remove a component type from an entity |
| `remove_processor` | 5 | Remove a processor |
| `query_world` | 5 | Read world state |
| `get_audit_history` | 5 | Read audit history |
| `remove_hook` | 5 | Remove a hook |
| `update` | 8 | Overlay existing component values |
| `add_component` | 8 | Extend an entity archetype |
| `publish` | 10 | Append a durable external artifact |
| `evaluate` | 10 | Claim and grade one evaluation |
| `spawn` | 10 | Create a new entity |
| `custom` | 10 | Submit an application-defined command |
| `destroy_world` | 10 | Destroy live world state; persisted rows remain |
| `step` | 10 | Execute one tick |
| `add_hook` | 10 | Register a hook |
| `add_resource` | 10 | Attach a resource |
| `add_processor` | 15 | Register a processor |
| `run` | 50 | Execute N steps |
| `create_world` | 50 | Create a world identity |
| `fork_world` | 100 | Fork world state |
| `run_rollout` | 200 | Run N forked episodes |
| `autoresearch` | 200 per iteration | Run the optimization loop over rollouts |
| `run_episode` | 500 | Run until termination or cap on one world |

Unknown command types default to a cost of 10.

## Quotas

Two quotas are enforced per actor:

| Quota | Limit | Scope |
|---|---:|---|
| Per-tick | 500 commands | Reset at tick boundary |
| Daily | 200,000 tokens | Reset at midnight UTC |

Both direct gated calls and tick-deferred `submit` calls consume quota.
`submit_batch` counts and charges every command, and rejects the whole batch
without a partial debit when its projected total would cross either limit.
Counters are process-local and keyed by actor identity; see the durability
posture in the [Specification](specification.md#durability-posture-v03-issue-276).

## Enforcement Flow

```text
iCommandGateway.<method>(ctx, ...)
    |
guardrail_allow(command, ctx)
  1. Role permission
  2. Per-tick quota
  3. Daily token budget
    |
delegate or reject
```

Rejected commands are not delegated or admitted.

## Roles and Permissions

The current role model is flat:

| Role | Permitted command families |
|---|---|
| `viewer` | Reads and introspection |
| `player` | Reads plus spawn, despawn, update, message, custom |
| `operator` | Player capabilities plus schema, processors, hooks, resources, simulation control, fork, destroy |
| `admin` | All commands, including create world |

A `viewer` actor with quota available still cannot `spawn`; the role check runs first.

See [Command Gate](command-gate.md) for the authoritative matrix.

## Budget Planning

| Scenario | Commands | Tokens |
|---|---|---:|
| Spawn 100 entities | 100 spawn | 1,000 |
| 50 ticks of messaging, 10 messages per tick | 500 message | 1,500 |
| Fork plus rollout | 1 fork + 1 rollout | 300 |
| Full episode | 1 run_episode | 500 |
| Heavy day: 1000 spawns, 5000 messages, 10 forks | 6,010 commands | 26,000 |

At these rates, the daily budget supports substantial workloads. The per-tick limit is the more common burst constraint.

## Source Reference

The quota system is defined in `src/archetype/app/gateway/auth/guard.py` and `src/archetype/app/gateway/auth/permissions.py`:

- `COMMANDS_BY_ROLE`
- command token costs
- `MAX_CMDS_PER_TICK`
- `MAX_TOKENS_PER_DAY`
- `guardrail_allow()`
