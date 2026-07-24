# Data Flow

World state has separate read and write facades in core, while the application
layer adds actor-free product semantics and an optional authorization boundary
for untrusted access.

The important boundary is:

- App families and `iRuntimeApplication` do not know about `ActorCtx`.
- `iCommandGateway` is the temporary transport adapter for untrusted calls. It
  constructs exact operation models and enters actor-aware dispatcher methods.
- Trusted runtime calls `iRuntimeApplication`, which constructs the same models
  and enters actor-free dispatcher methods.
- A finite gateway-to-application bridge remains only for staged workflows
  whose family registrations land in the next refactor wave.

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
RuntimeWorld
    -> iRuntimeApplication adapter
    -> CommandDispatcher.apply(exact operation)
    -> OperationRegistry handler
    -> safe result
```

Untrusted adapter:

```text
API / untrusted caller
    |
iCommandGateway.<method>(ctx, ...)
    |
construct exact family operation
    |
CommandDispatcher.apply_as(ctx, operation)
    |
OperationRegistry -> Policy preauthorization/quotas -> registered handler
    |
commands.AuditLog.record_access(bounded evidence)
    |
return safe result
```

Examples:

- `create_world` delegates to `iWorldLifecycle` and returns `WorldInfo`.
- `create_entity` calls `archetype.world.mutation` through `iWorldRegistry`
  and returns `entity_id`.
- `run` calls `archetype.world.simulation` and returns `RunResult`.
- `query_archetype` calls `archetype.world.query` and returns a DataFrame.

Untrusted reads are gated. Trusted runtime and untrusted adapters resolve the
same exact registration and handler; only actor-aware entry runs policy and
access evidence.

## Tick-Deferred Path

When a caller wants work applied at a tick boundary, the commands family durably
admits it. An untrusted caller is authorized before admission:

```text
RuntimeApplication or iCommandGateway compatibility adapter
    |
CommandDispatcher.defer / defer_as(exact operation, DurableOptions)
    |
OperationRegistry exact durable eligibility; Policy for actor-aware entry
    |
CommandScheduler.admit(operation, options, origin/principal snapshot)
    |
control catalog (durable PENDING)
    |
AsyncWorld.step() construction-injected materializer
    |
CommandScheduler.materialize(actual_world, tick)
    |
registered DurableOperation -> archetype.world.handlers.materialize_locked
    |
AsyncWorld internal mutation -> manifest publication + command settlement
```

Only the actor-aware `CommandDispatcher` entry points accept `ActorCtx`; the
scheduler receives an immutable principal/origin snapshot, never the live
actor object. Commands are ordered by a durable per-world
`(scheduled_tick, priority, sequence)` key.

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
- `destroy_world`: operator/admin; begins exact-world close, reconciles committed
  work, cancels that world's unsettled commands, and publishes durable
  destroyed state before releasing the live identity.

Destroy does not delete storage or audit rows. See [World Lifecycle](world-lifecycle.md).

## Audit flow

Actor-aware dispatcher calls attempt bounded access events through
commands-owned `AuditLog`. Product transitions append outbox events in the
transaction that establishes their authority. `AuditLog` exports deduplicated
events to Iceberg; scheduler/control-catalog outbox progress exposes the
projection watermark. Command-ledger history is operational truth; audit
history is the analytical projection.

`RuntimeWorld.history(...)` reads through RuntimeApplication. API history reads
authorize through the gateway. Both adapters construct the registered
`GetAuditHistory` operation and reach the same commands-owned projection.

See [Audit Log](audit-log.md).

## Source Reference

- Gateway: `src/archetype/app/gateway/service.py`
- Governed dispatcher and durable scheduler: `src/archetype/commands/`
- Managed simulation: `src/archetype/world/simulation.py`
- Durable world reads: `src/archetype/world/query.py`
- World mutation adapters: `src/archetype/world/mutation.py`
- Instance-owned RBAC and quotas: `src/archetype/commands/policy.py`
- Querier: `src/archetype/core/aio/async_querier.py`
- Updater: `src/archetype/core/aio/async_updater.py`
