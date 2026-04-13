# Command Broker

The `CommandBroker` is the choke point between external actors and internal worlds. Every mutation that originates outside the simulation process -- from an API call, a CLI command, or an agent submitting actions -- passes through the broker. It enforces access control, orders commands by priority, and maintains an audit trail.

Processors bypass the broker entirely. They write directly through the updater as trusted internal code. The trust boundary is at processor registration, not at each write.

```text
External actor
    |
CommandService.submit(world_id, cmd, ctx: ActorCtx)
    |
CommandBroker.enqueue(world_id, cmd, ctx)
    |  ← guardrail_allow(cmd, ctx)
    |     1. RBAC role check
    |     2. Per-tick quota
    |     3. Daily token budget
    |
    |  [queued by (tick, priority, seq)]
    |
SimulationService.step()
    → CommandService.drain_and_apply(world_id, tick)
    → CommandService.apply(world, cmd)
    → AsyncWorld mutation
```

## Priority Queue

The broker maintains one `heapq` priority queue per world, keyed by `str(world_id)`. World lifecycle commands (create, destroy, fork) use the key `"__global__"`.

Commands are ordered by `(tick, priority, seq)`:

- **tick** -- Commands targeting a future tick stay queued until that tick arrives. `dequeue_due(world_id, tick)` only pops commands where `cmd.tick <= tick`.
- **priority** -- Lower values execute first. Default is 0.
- **seq** -- A global counter (`itertools.count()`) assigned at command creation. Breaks ties within the same tick and priority, giving FIFO ordering.

The `Command.__lt__` method defines this ordering:

```python
def __lt__(self, other: "Command") -> bool:
    return (self.tick, self.priority, self.seq) < (
        other.tick, other.priority, other.seq,
    )
```

### Dequeue Modes

| Method | Behavior |
|--------|----------|
| `dequeue_due(world_id, tick)` | Pop commands where `cmd.tick <= tick`. Used by `drain_and_apply()` each tick |
| `dequeue(world_id)` | Pop all pending commands regardless of tick |
| `peek(world_id)` | Return sorted commands without removing them |

All dequeue methods respect `max_dequeue` (default 50,000) as a safety limit.

## RBAC Guardrails

When an `ActorCtx` is provided, `enqueue()` calls `guardrail_allow(cmd, ctx)` before touching the queue. This function runs three checks:

### 1. Role Permissions

Each role maps to a set of permitted command types:

| Role | Permissions |
|------|-------------|
| `viewer` | get_state, get_world, get_run, query_world |
| `player` | spawn, despawn, update, message, custom |
| `coder` | add_component, remove_component, update |
| `operator` | spawn, despawn, update, get_state, get_world, get_run, query_world |
| `maintainer` | spawn, despawn, components, processors, update |
| `admin` | `*` (wildcard -- all command types) |

An actor can hold multiple roles. If any role grants the command type, the check passes.

### 2. Per-Tick Quota

Each actor is limited to 500 commands per tick. The counter increments on each `guardrail_allow()` call and resets when `SimulationService.step()` calls `reset_tick_counters()` between ticks.

### 3. Daily Token Budget

Each command type has a token cost (e.g., `spawn=10`, `fork_world=100`, `run_episode=500`). Each actor has a daily budget of 200,000 tokens. The guard deducts the cost before enqueueing and rejects commands that would exceed the budget.

See [Token Costs and Quotas](token-quotas.md) for the full cost table.

## Audit Trail

The broker tracks two collections:

- **`_history`** (`dict[str, list[Command]]`) -- Every command that was successfully enqueued, per world. Ordered by enqueue time.
- **`_pending`** (`dict[UUID, Command]`) -- Commands that have been enqueued but not yet applied. Keyed by command ID.

`ack(cmd_ids)` removes commands from `_pending` after `CommandService.apply()` succeeds. Commands remain in `_history` regardless of whether they were applied successfully.

```python
# After successful application
await broker.ack([cmd.id for cmd in applied_commands])
```

`get_history(world_id, limit=100)` returns recent commands. `get_pending_count(world_id)` returns the queue depth.

## Concurrency

All queue mutations are guarded by a single `asyncio.Lock`. This serializes enqueue and dequeue operations within the broker, preventing race conditions when multiple API requests or processors submit commands concurrently.

`enqueue_bulk()` is all-or-nothing: it validates RBAC for every command in the batch before enqueueing any of them. If the third command in a batch of five fails the role check, none are enqueued.

```python
async def enqueue_bulk(self, world_id, cmds, ctx=None):
    if ctx is not None:
        for cmd in cmds:
            guardrail_allow(cmd, ctx)  # validate all first
    async with self._lock:
        for cmd in cmds:
            heapq.heappush(self._queues[key], cmd)
```

## The Command Model

`Command` is a frozen Pydantic model with fields that drive both queuing and dispatch:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `id` | `UUID` | uuid7 | Unique identifier, used for ack/pending tracking |
| `tick` | `int` | 0 | Target tick (commands wait in queue until this tick) |
| `actor_id` | `UUID?` | None | Submitting actor (for audit) |
| `type` | `CommandType` | CUSTOM | Determines RBAC check and dispatch target |
| `payload` | `dict` | {} | Type-specific data (components, entity_id, etc.) |
| `priority` | `int` | 0 | Lower executes first within same tick |
| `seq` | `int` | auto | Global counter for FIFO within same tick+priority |

`CommandType` covers entity mutations (spawn, despawn, update, components), processor mutations, world lifecycle (create, destroy, fork), simulation operations (rollout, episode), messaging, and custom extensions.

## Processor Access

Processors that need to submit commands access the broker through [Resources](resources.md):

```python
class SpawnerProcessor(AsyncProcessor):
    components = (Agent,)

    async def process(self, df, resources=None, tick=0, **kwargs):
        broker = resources.get(CommandBroker) if resources else None
        if broker:
            cmd = Command(
                type=CommandType.SPAWN,
                tick=tick,
                payload={"components": [Agent(name="child").to_payload()]},
            )
            await broker.enqueue("my_world", cmd)
        return df
```

The broker is injected into `world.resources` by `WorldService.create_world()`. Commands submitted by processors during tick N are enqueued for the next drain cycle -- they are not applied within the current tick.

## Source Reference

- Broker: `src/archetype/app/broker.py`
- Command model: `src/archetype/app/models.py`
- RBAC guard: `src/archetype/app/auth/guard.py`
- Actor model: `src/archetype/app/auth/models.py`
