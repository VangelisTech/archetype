# Data Flow

Every interaction with world state follows one of two paths: a **read path** through the querier or a **write path** through the command pipeline and updater. The two paths are structurally separated at the core layer and the service layer enforces different access policies on each.

## Read/Write Split

The `AsyncWorld` never touches the store directly. It delegates through two interfaces:

```text
                  AsyncWorld
                 /          \
       QueryManager        UpdateManager
            |                   |
        AsyncStore          AsyncStore
         (reads)            (appends)
```

`AsyncQueryManager` owns every read. `AsyncUpdateManager` owns every write. Both hold a reference to the same `AsyncStore`, but the world can only reach the store through one of these two facades.

This split exists at the core layer, independent of any auth or service machinery. The interfaces are defined in `archetype.core.interfaces` as `iAsyncQueryManager` and `iAsyncUpdateManager`.

## Write Path

All mutations from external actors flow through the command pipeline:

```text
External actor
    |
CommandService.submit(cmd, ctx: ActorCtx)
    |
CommandBroker.enqueue(cmd, ctx)
    |  ← guardrail_allow(cmd, ctx)
    |     1. RBAC role check
    |     2. Per-tick quota (500 cmds/tick)
    |     3. Daily token budget (200k tokens)
    |
    |  [queued by (tick, priority, seq)]
    |
SimulationService.step()
    |
CommandService.drain_and_apply(world_id, tick)
    |  ← broker.dequeue_due(world_id, tick)
    |
CommandService.apply(world, cmd)
    |  ← dispatches to world.create_entity(), world.add_components(), etc.
    |
AsyncWorld internal mutation
    |
UpdateManager.update(df, sig, tick, world_id, run_id)
    |  ← stamps tick, world_id, run_id, casts entity_id
    |
AsyncStore.append(sig, df)
```

Every step in this chain is mandatory. There is no way to reach `UpdateManager.update()` from outside the process without going through `CommandService.submit()`, which requires an `ActorCtx`. The broker calls `guardrail_allow()` before enqueueing, so RBAC and quota enforcement cannot be bypassed.

### Command Dispatch

`CommandService.apply()` pattern-matches on `CommandType` and calls the corresponding world mutation:

| CommandType | World method |
|-------------|-------------|
| `SPAWN` | `create_entity(components)` |
| `DESPAWN` | `remove_entity(entity_id)` |
| `UPDATE`, `ADD_COMPONENT` | `add_components(entity_id, components)` |
| `REMOVE_COMPONENT` | `remove_components(entity_id, types)` |
| `ADD_PROCESSOR` | `add_processor(processor)` |
| `REMOVE_PROCESSOR` | `remove_processor(proc_type)` |
| `CREATE_WORLD`, `DESTROY_WORLD`, `FORK_WORLD` | `WorldService` lifecycle methods |
| `MESSAGE` | No-op (processors read from broker history) |
| `CUSTOM` | No-op by default (subclass to handle) |

See [Custom Commands](custom-commands.md) for extending the dispatch.

### Drain Cycle

`SimulationService.step()` drives the per-tick drain:

1. `drain_and_apply(world_id, tick)` -- dequeue all commands where `cmd.tick <= tick`, apply each to the world, ack on success
2. `reset_tick_counters()` -- clear per-actor command counts for the next tick
3. `world.step(run_config)` -- execute processors, which read from the querier and write through the updater

Commands are ordered by `(tick, priority, seq)` within the broker's priority queue. Lower priority values execute first. Commands targeting a future tick remain queued until that tick arrives.

## Read Path

Reads bypass the command pipeline entirely:

```text
External actor
    |
QueryService.get_world_state(world_id, tick?)
QueryService.get_entity(world_id, entity_id, tick?)
QueryService.get_components(world_id, component_types)
QueryService.get_command_history(world_id)
    |
AsyncWorld
    |
QueryManager.query_archetype(sig, world_id, ticks?, entity_ids?, components?)
    |  ← filters: is_active, ticks, entity_ids, component projection
    |
AsyncStore.get_archetype_df(sig, world_id, run_id)
```

`QueryService` methods take no `ActorCtx`. There is no RBAC check, no quota deduction, no broker involvement. Reads are unconditionally allowed because they have no side effects on world state.

The `viewer` role exists in `ROLE_PERMS` to gate the `query_world` *command type* -- a command that can be submitted through the broker for audit purposes. But direct reads through `QueryService` do not require it.

## RBAC Boundary

The read/write split determines where RBAC applies:

```text
                    ┌─────────────────────┐
                    │   Service Layer      │
                    │                      │
  QueryService ─────┤  (no ActorCtx)       │
                    │                      │
                    │ ─ ─ ─ RBAC fence ─ ─ │
                    │                      │
  CommandService ───┤  ActorCtx required   │
                    │  guardrail_allow()   │
                    └─────────────────────┘
                              |
                    ┌─────────────────────┐
                    │   Core Layer         │
                    │                      │
                    │  QueryManager        │
                    │  UpdateManager       │
                    │  (no auth awareness) │
                    └─────────────────────┘
```

The core layer has no knowledge of RBAC. `QueryManager` and `UpdateManager` are pure data facades -- they don't check permissions, they don't know about actors. Auth is enforced entirely at the service layer boundary, and the structural separation between the two facades is what makes that enforcement complete.

This means:

- **Processors run without auth.** Once a tick starts, processors read from `QueryManager` and write through `UpdateManager` with no RBAC overhead. They are trusted internal code.
- **External actors are always gated.** The only external entry point for mutations is `CommandService.submit()`, which requires `ActorCtx`.
- **The read path is a fast path.** No auth checks, no broker, no queue -- `QueryService` goes straight to the querier.

## Internal Writes (Processors)

Processors write to the updater as part of normal tick execution, outside the command pipeline:

```text
AsyncSystem.execute(resources, tick)
    |
processor.process(df, resources=resources, tick=tick)
    |  ← returns transformed DataFrame
    |
AsyncWorld._update_archetype(sig, df, run_config)
    |
UpdateManager.update(df, sig, tick, world_id, run_id)
```

These writes are not command-gated because processors are registered by a `maintainer` or `admin` at setup time. The trust boundary is at processor registration, not at each write.

Processors that need to submit commands (e.g., spawning new entities mid-tick) access the `CommandBroker` through [Resources](resources.md) and enqueue commands for the next drain cycle.

## Source Reference

- Command service: `src/archetype/app/command_service.py`
- Command broker: `src/archetype/app/broker.py`
- Simulation service: `src/archetype/app/simulation_service.py`
- Query service: `src/archetype/app/query_service.py`
- RBAC guard: `src/archetype/app/auth/guard.py`
- Actor model: `src/archetype/app/auth/models.py`
- Querier: `src/archetype/core/aio/async_querier.py`
- Updater: `src/archetype/core/aio/async_updater.py`
