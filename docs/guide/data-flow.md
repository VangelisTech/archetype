# Data Flow

World state has separate read and write facades in core, while the application
layer adds actor-free product semantics and an optional authorization boundary
for untrusted access.

The important boundary is:

- App families and `iRuntimeApplication` do not know about `ActorCtx`.
- `iCommandGateway` authorizes untrusted calls, delegates to
  `iRuntimeApplication`, records access evidence, and returns safe results.
- Trusted runtime calls `iRuntimeApplication` directly.

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

## Direct paths

Trusted runtime:

```text
RuntimeWorld -> iRuntimeApplication -> owning family -> safe result
```

Untrusted adapter:

```text
API / untrusted caller
    |
iCommandGateway.<method>(ctx, ...)
    |
guardrail_allow(command, ctx)
    |
delegate to iRuntimeApplication
    |
iAuditJournal.record_access(event)
    |
return result
```

Examples:

- `create_world` delegates to `iWorldService` and returns `WorldInfo`.
- `create_entity` delegates to `iMutationService` and returns `entity_id`.
- `run` delegates to `iSimulationService` and returns `RunResult`.
- `query_archetype` delegates to `iQueryService` and returns a DataFrame.

Untrusted reads are gated. Trusted runtime and internal workflows use the same
actor-free application/query semantics directly.

## Tick-Deferred Path

When a caller wants work applied at a tick boundary, the commands family durably
admits it. An untrusted caller is authorized before admission:

```text
RuntimeApplication or iCommandGateway after authorization
    |
iCommandScheduler.admit(world_id, cmd, origin/principal)
    |
iCommandLedger (durable PENDING)
    |
SimulationService.step()
    |
iCommandDispatcher.lease_and_stage_due(world_id, tick)
    |
MutationService / WorldService
    |
AsyncWorld internal mutation -> manifest publication + command settlement
```

No commands-family operation accepts `ActorCtx`. The gateway converts a
principal into an immutable admission snapshot. Commands are ordered by a
durable per-world `(scheduled_tick, priority, sequence)` key.

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

## Audit flow

Gateway calls emit access events. Product transitions append outbox events in
the transaction that establishes their authority. The audit projector exports
deduplicated events to Iceberg and exposes a watermark. Command-ledger history
is operational truth; audit history is the analytical projection.

`RuntimeWorld.history(...)` reads through RuntimeApplication. API history reads
authorize through the gateway before invoking the same application operation.

See [Audit Log](audit-log.md).

## Source Reference

- Gateway: `src/archetype/app/gateway/service.py`
- Durable command scheduler: `src/archetype/app/commands/service.py`
- Simulation service: `src/archetype/app/world/simulation.py`
- Query service: `src/archetype/app/query/service.py`
- RBAC guard: `src/archetype/app/gateway/auth/guard.py`
- Querier: `src/archetype/core/aio/async_querier.py`
- Updater: `src/archetype/core/aio/async_updater.py`
