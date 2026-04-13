---
title: Token Costs and Quotas
description: RBAC quota system, token costs per command, and rate limits
---

Every command submitted through the broker has a token cost. The RBAC guard enforces per-tick and daily quotas to prevent runaway resource consumption.

## Token Costs

Each command type has a fixed token cost:

| Command Type | Token Cost | Description |
|-------------|-----------|-------------|
| `message` | 3 | Agent-to-agent messaging |
| `despawn` | 5 | Remove an entity |
| `remove_component` | 5 | Remove a component from an entity |
| `remove_processor` | 5 | Remove a processor |
| `query_world` | 5 | Read state from a world |
| `update` | 8 | Update entity state |
| `add_component` | 8 | Add a component to an entity |
| `spawn` | 10 | Create a new entity |
| `custom` | 10 | User-defined command |
| `destroy_world` | 10 | Tear down a child world |
| `add_processor` | 15 | Hot-swap a processor |
| `create_world` | 50 | Spawn a child world |
| `fork_world` | 100 | Clone world state for branching |
| `run_rollout` | 200 | Run N steps in a world |
| `run_episode` | 500 | Full episode with sampled initial conditions |

Unknown command types default to a cost of 10.

## Quotas

Two quotas are enforced per actor:

| Quota | Limit | Scope |
|-------|-------|-------|
| **Per-tick** | 500 commands | Reset at start of each tick |
| **Daily** | 200,000 tokens | Reset at day boundary |

### Per-Tick Limit

Each actor can submit at most **500 commands per tick**, regardless of command type. The counter is shared across all command types and all worlds.

### Daily Token Budget

Each actor has a daily budget of **200,000 tokens**. Token costs accumulate across all commands submitted throughout the day. The budget is shared across all command types and all worlds.

## What Happens When a Quota Is Exceeded

Both quota violations raise `PermissionError`, which the API layer returns as **HTTP 403 Forbidden**.

**Per-tick violation:**

```text
PermissionError: Actor {id} exceeded per-tick quota (500 commands)
```

**Daily token violation:**

```text
PermissionError: Actor {id} exceeded daily token budget (200000 tokens)
```

The command is rejected immediately and is not enqueued. No partial execution occurs.

## Enforcement Flow

Quotas are checked during `CommandBroker.enqueue()`, before the command enters the priority queue:

```text
CommandService.submit(world_id, cmd, ctx)
    |
    v
CommandBroker.enqueue(world_id, cmd, ctx)
    |
    v
guardrail_allow(cmd, ctx)
  1. RBAC check -- does the actor's role permit this command type?
  2. Per-tick quota -- has the actor hit 500 commands this tick?
  3. Daily token budget -- would this command exceed 200k tokens?
    |
    v
[Enqueued] or [PermissionError raised]
```

## Roles and Permissions

Quotas apply equally to all roles. However, roles determine **which** command types an actor can submit:

| Role | Permitted Commands |
|------|-------------------|
| `viewer` | `get_state`, `get_world`, `get_run`, `query_world` |
| `coder` | `add_component`, `remove_component`, `update` |
| `operator` | `spawn`, `despawn`, `update`, `get_state`, `get_world`, `get_run`, `query_world` |
| `maintainer` | `spawn`, `despawn`, `add_component`, `remove_component`, `add_processor`, `remove_processor`, `update` |
| `player` | `spawn`, `despawn`, `update`, `message`, `custom` |
| `admin` | All commands (wildcard `*`) |

A `viewer` actor with 500 commands available still cannot `spawn` -- the RBAC check runs first.

## Budget Planning

Use these costs to estimate whether a workload fits within the daily budget:

| Scenario | Commands | Tokens |
|----------|----------|--------|
| Spawn 100 entities | 100 spawn | 1,000 |
| 50 ticks of messaging (10 msgs/tick) | 500 message | 1,500 |
| Fork + run 10-step rollout | 1 fork + 1 rollout | 300 |
| Full episode | 1 run_episode | 500 |
| Heavy day (1000 spawns + 5000 messages + 10 forks) | 6,010 commands | 26,000 |

At these rates, the 200k daily budget supports substantial workloads. The per-tick limit of 500 is the more common constraint for burst scenarios.

## Source Reference

The quota system is defined in `src/archetype/app/auth/guard.py`:

- `ROLE_PERMS` -- role-to-permission mapping
- `_TOKEN_COSTS` -- cost per command type
- `MAX_CMDS_PER_TICK` -- per-tick limit (500)
- `MAX_TOKENS_PER_DAY` -- daily budget (200,000)
- `guardrail_allow()` -- the enforcement function
- `reset_tick_counters()` -- called each tick by SimulationService
- `reset_daily_tokens()` -- called at day boundary
