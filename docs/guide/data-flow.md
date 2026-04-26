# Data Flow

World state has separate read and write facades in core, while the service layer adds a gate for external access.

The important boundary is:

- Below the gate, services such as `iQueryService`, `iMutationService`, and `iWorldService` do not know about `ActorCtx`.
- At the boundary, `iCommandService` authorizes, delegates, audits, and returns user-safe results.

## Core Read/Write Split

`AsyncWorld` does not touch storage directly:

```text
                  AsyncWorld
                 /          \
       QueryManager        UpdateManager
            |                   |
        AsyncStore          AsyncStore
         reads              appends
```

`QueryManager` owns reads. `UpdateManager` owns writes. This split is independent of auth.

## External Direct Path

Most runtime and API calls use a direct gated path:

```text
Runtime / API / caller
    |
iCommandService.<method>(ctx, ...)
    |
guardrail_allow(command, ctx)
    |
delegate to one service
    |
iAuditLog.record(row)
    |
return result
```

Examples:

- `create_world` delegates to `iWorldService` and returns `WorldInfo`.
- `create_entity` delegates to `iMutationService` and returns `entity_id`.
- `run` delegates to `iSimulationService` and returns `RunResult`.
- `query_archetype` delegates to `iQueryService` and returns a DataFrame.

Reads are gated at this external boundary. The internal `iQueryService` remains ActorCtx-free.

## Tick-Deferred Path

When a caller wants work applied at a tick boundary, the gate enqueues a command:

```text
Runtime / API / caller
    |
iCommandService.submit(ctx, world_id, cmd)
    |
guardrail_allow(command, ctx)
    |
iCommandBroker.enqueue(world_id, cmd)
    |
SimulationService.step()
    |
iCommandService.drain_and_apply(world_id, tick)
    |
MutationService / WorldService
    |
AsyncWorld internal mutation
```

`drain_and_apply` has no `ActorCtx` argument because submitted commands were validated before entering the queue.

Commands are ordered by `(tick, priority, seq)` within the broker queue.

## Internal Writes

Processors are trusted internal code. During a tick, processors transform DataFrames and the world persists the result through the updater:

```text
AsyncSystem.execute(resources, tick)
    |
processor.process(df, resources=resources, tick=tick)
    |
AsyncWorld._update_archetype(sig, df, run_config)
    |
UpdateManager.update(df, sig, tick, world_id, run_id)
```

These writes are not individually command-gated. The trust boundary is processor registration, which is an operator/admin operation through the gate.

## Lifecycle Flow

World lifecycle operations are direct gated methods:

- `create_world`: admin only; returns `WorldInfo`.
- `fork_world`: operator/admin; returns `WorldInfo`.
- `destroy_world`: operator/admin; removes the live world only.

Destroy does not delete storage or audit rows. See [World Lifecycle](world-lifecycle.md).

## Audit Flow

Every gated call emits one audit row. Broker queue history is not the audit log.

`RuntimeWorld.history(...)` and API history reads use `iCommandService.get_audit_history(...)`, which authorizes the read and delegates to `iAuditLog.query(...)`.

See [Audit Log](audit-log.md).

## Source Reference

- Command service: `src/archetype/app/command_service.py`
- Command broker: `src/archetype/app/broker.py`
- Simulation service: `src/archetype/app/simulation_service.py`
- Query service: `src/archetype/app/query_service.py`
- RBAC guard: `src/archetype/app/auth/guard.py`
- Querier: `src/archetype/core/aio/async_querier.py`
- Updater: `src/archetype/core/aio/async_updater.py`
