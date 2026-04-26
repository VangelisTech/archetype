---
title: Token Costs and Quotas
description: Gate-level quota system, token costs per command, and rate limits
---

Every gated command has a token cost. `guardrail_allow()` enforces role permissions, per-tick command limits, and daily token budgets before the operation is accepted.

The command gate is `iCommandService`; the broker is only used for tick-deferred queueing.

## Token Costs

Each command type has a fixed token cost:

| Command Type | Token Cost | Description |
|---|---:|---|
| `message` | 3 | Agent-to-agent messaging |
| `despawn` | 5 | Remove an entity |
| `remove_component` | 5 | Remove a component type from an entity |
| `remove_processor` | 5 | Remove a processor |
| `query_world` | 5 | Read world state |
| `get_world_info` | 5 | Read world identity/tick info |
| `get_audit_history` | 5 | Read audit history |
| `update` | 8 | Overlay existing component values |
| `add_component` | 8 | Extend an entity archetype |
| `spawn` | 10 | Create a new entity |
| `custom` | 10 | User-defined command |
| `destroy_world` | 10 | Destroy live world state; persisted rows remain |
| `add_processor` | 15 | Register a processor |
| `add_hook` | 15 | Register a hook |
| `add_resource` | 15 | Attach a resource |
| `step` | 25 | Execute one tick |
| `run` | 50 | Execute N steps |
| `create_world` | 50 | Create a world identity |
| `fork_world` | 100 | Fork world state |
| `run_rollout` | 200 | Run N forked episodes |
| `run_episode` | 500 | Run until termination or cap on one world |

Unknown command types default to a cost of 10.

## Quotas

Two quotas are enforced per actor:

| Quota | Limit | Scope |
|---|---:|---|
| Per-tick | 500 commands | Reset at tick boundary |
| Daily | 200,000 tokens | Reset at day boundary |

Both direct gated calls and tick-deferred `submit` calls consume quota.

## Enforcement Flow

```text
iCommandService.<method>(ctx, ...)
    |
guardrail_allow(command, ctx)
  1. Role permission
  2. Per-tick quota
  3. Daily token budget
    |
delegate or reject
```

Rejected commands are not delegated and are not enqueued.

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

The quota system is defined in `src/archetype/app/auth/guard.py` and `src/archetype/app/auth/permissions.py`:

- `COMMANDS_BY_ROLE`
- command token costs
- `MAX_CMDS_PER_TICK`
- `MAX_TOKENS_PER_DAY`
- `guardrail_allow()`
