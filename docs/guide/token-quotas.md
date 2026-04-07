# Token Costs and Quotas

The RBAC guard enforces two independent quota systems: a per-tick command limit and a daily token budget. Both are checked before a command is enqueued.

## How Quotas Work

Every call to `CommandBroker.enqueue()` runs `guardrail_allow(cmd, ctx)`, which:

1. **Checks role permissions** — does the actor's role allow this command type?
2. **Checks per-tick quota** — has the actor submitted too many commands this tick?
3. **Checks daily token budget** — would this command push the actor over the daily limit?

If any check fails, a `PermissionError` is raised and the command is rejected.

## Token Cost Per Command Type

Each command type has an estimated token cost:

| Command type | Token cost |
|---|---|
| `message` | 3 |
| `despawn` | 5 |
| `remove_component` | 5 |
| `remove_processor` | 5 |
| `query_world` | 5 |
| `update` | 8 |
| `add_component` | 8 |
| `spawn` | 10 |
| `custom` | 10 |
| `destroy_world` | 10 |
| `add_processor` | 15 |
| `create_world` | 50 |
| `fork_world` | 100 |
| `run_rollout` | 200 |
| `run_episode` | 500 |

Any command type not in this table defaults to a cost of **10**.

## Quota Limits

| Limit | Value | Scope |
|-------|-------|-------|
| Per-tick command limit | **500 commands** | Per actor, per tick |
| Daily token budget | **200,000 tokens** | Per actor, rolling day |

## What Happens When a Quota Is Exceeded

Both quota violations raise `PermissionError` with a descriptive message:

```python
# Per-tick quota exceeded:
PermissionError("Actor <id> exceeded per-tick quota (500 commands)")

# Daily token budget exceeded:
PermissionError("Actor <id> exceeded daily token budget (200000 tokens)")
```

The command is **not** enqueued. The simulation continues normally for other actors and other commands.

## Resetting Quotas

Quotas are stored in module-level dicts in `guard.py`. They are never reset automatically — you must call the reset functions explicitly at the appropriate boundaries:

```python
from archetype.app.auth.guard import reset_tick_counters, reset_daily_tokens

# Call at the start of each tick (SimulationService does this automatically)
reset_tick_counters()

# Call at the day boundary (your application must schedule this)
reset_daily_tokens()
```

`SimulationService` calls `reset_tick_counters()` automatically at the start of every tick. Daily token resets are the responsibility of your application layer — schedule them with a cron job or a startup check.

## Role Permissions

Roles control which command types an actor can submit. An actor may hold multiple roles simultaneously.

| Role | Permitted command types |
|------|------------------------|
| `viewer` | `get_state`, `get_world`, `get_run`, `query_world` |
| `coder` | `add_component`, `remove_component`, `update` |
| `operator` | `spawn`, `despawn`, `update`, `get_state`, `get_world`, `get_run`, `query_world` |
| `maintainer` | `spawn`, `despawn`, `add_component`, `remove_component`, `add_processor`, `remove_processor`, `update` |
| `player` | `spawn`, `despawn`, `update`, `message`, `custom` |
| `admin` | All commands (`*` wildcard) |

Unknown roles are implicitly denied — `guardrail_allow()` only grants access through explicitly configured role entries.

## Estimating Token Usage

Use `estimate_token_cost` to look up the cost of a command before submitting:

```python
from archetype.app.auth.guard import estimate_token_cost
from archetype.app.models import Command, CommandType

cmd = Command(type=CommandType.FORK_WORLD, payload={"source_world_id": "..."})
cost = estimate_token_cost(cmd)  # → 100
```

To estimate a batch:

```python
total = sum(estimate_token_cost(c) for c in commands)
print(f"Batch cost: {total} tokens")
```

## Planning for Quota Limits

### Per-tick quota (500 commands/tick)

Each actor is limited to 500 commands per tick. For workloads with many agents, spread commands across actors:

```python
# Instead of one actor submitting everything:
single_ctx = ActorCtx(id=uuid7(), roles={"operator"})

# Use per-agent contexts so each agent has its own 500-command budget:
agent_ctx = ActorCtx(id=agent_entity_id, roles={"player"})
```

### Daily budget (200,000 tokens)

The daily budget covers a typical workload:
- 200,000 ÷ 10 (spawn) = **20,000 entity spawns**
- 200,000 ÷ 8 (update) = **25,000 entity updates**
- 200,000 ÷ 100 (fork_world) = **2,000 world forks**

For high-throughput simulations that will exceed the budget, use separate `ActorCtx` instances per logical actor so their budgets are isolated from each other.

## Example: Handling Quota Errors

```python
from archetype.app.auth.guard import MAX_CMDS_PER_TICK, MAX_TOKENS_PER_DAY

async def safe_submit(command_service, world_id, cmd, ctx):
    try:
        return await command_service.submit(world_id, cmd, ctx)
    except PermissionError as e:
        msg = str(e)
        if "per-tick quota" in msg:
            # Too many commands this tick — retry next tick
            return None
        elif "daily token budget" in msg:
            # Budget exhausted — stop submitting for today
            raise
        else:
            # RBAC denial — actor lacks permission
            raise
```

## Source Reference

Quota constants and the `guardrail_allow` function live in:

```
src/archetype/app/auth/guard.py
```

The relevant symbols:
- `MAX_CMDS_PER_TICK` — per-tick command limit
- `MAX_TOKENS_PER_DAY` — daily token budget
- `_TOKEN_COSTS` — token cost per command type
- `estimate_token_cost(cmd)` — look up a command's cost
- `guardrail_allow(cmd, ctx)` — enforce all three checks
- `reset_tick_counters()` — reset the per-tick counters
- `reset_daily_tokens()` — reset the daily token budgets
