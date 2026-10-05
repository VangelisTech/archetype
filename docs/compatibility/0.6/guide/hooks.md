# Lifecycle Hooks

Hooks are typed lifecycle callbacks attached to a single world. They are
intended for observability, integration glue, and lightweight side effects that
need to run at known world boundaries.

The hook catalogue lives in `archetype.core.hooks` and is executed by
`AsyncWorld`. `RuntimeWorld` routes hook operations to that engine;
`SyncRuntimeWorld` adapts blocking callbacks onto the same asynchronous bus.

## Type Model

Hook events are frozen dataclasses. Every concrete event inherits from the
nominal base class `HookEvent`, which carries the world identity:

```python
from archetype.core.hooks import HookEvent, PostTick

assert issubclass(PostTick, HookEvent)
```

The handler types are explicit about runtime mode:

| Surface | Runtime | Callable shape |
|------|---------|----------------|
| `AsyncHookHandler[E]` | `AsyncWorld` / `RuntimeWorld` | `async def handler(event: E) -> None` |
| Blocking facade callback | `SyncRuntimeWorld` | `def handler(event: E) -> None` |

`E` is bound to `HookEvent`, so `world.add_hook(PostTick, handler)` ties the
event class and handler argument to the same event type.

Core uses dataclasses rather than Pydantic models here because hooks are in the
world hot path. Events are small immutable payloads, not validation boundaries.

## Registering Hooks

For an already-active `RuntimeWorld`:

```python
from archetype.core.hooks import HookHandle, PostTick

async def log_tick(event: PostTick) -> None:
    print(f"world={event.world_id} tick={event.tick}")

handle: HookHandle = await world.add_hook(PostTick, log_tick)

# Later:
await world.remove_hook(handle)
```

`RuntimeWorld.add_hook()` returns an opaque `HookHandle`. Store the handle if
the hook should be removed later. Handles are registry-scoped, so a handle
minted by one world cannot unregister a same-shaped hook in another world.
Direct `AsyncWorld.add_hook()` mutates its local registry synchronously;
ordinary applications use the awaited runtime operation shown above.

For a complete runnable example, see
[`examples/07_hooks.py`](https://github.com/VangelisTech/archetype/blob/main/examples/07_hooks.py).

## Event Catalogue

| Event | Payload fields | Fires when |
|-------|----------------|------------|
| `PreTick` | `world_id`, `tick` | At the start of `AsyncWorld.step()`, before any archetype runs |
| `PostTick` | `world_id`, `tick`, `results` | After all archetypes process, `_live` refreshes, and `tick` increments |
| `OnSpawn` | `world_id`, `entity_id`, `components` | After `create_entity()` or `spawn_reserved()` registers the entity |
| `OnDespawn` | `world_id`, `entity_id` | After `remove_entity()` cancels a pending spawn or queues a despawn row |
| `OnComponentAdded` | `world_id`, `entity_id`, `components` | After `add_components()` moves the entity to a wider archetype |
| `OnComponentRemoved` | `world_id`, `entity_id`, `component_types` | After `remove_components()` moves the entity to a narrower archetype |
| `OnDestroy` | `world_id` | Before in-memory world cleanup begins |

Payloads carry `world_id`, not the world object. A handler that needs the world
should close over it at registration time.

## Tick Semantics

`PreTick.tick` is the tick about to run.

`PostTick.tick` is the newly incremented tick after the step completes. The tick
that just completed is `event.tick - 1`.

```text
step(tick=N)
  -> PreTick(tick=N)
  -> query, materialize mutations, execute processors, persist, refresh _live
  -> tick = N + 1
  -> PostTick(tick=N+1)
```

Spawn/despawn/component hooks fire when the world mutation is queued in memory,
not when the row is later materialized and persisted during the tick.

## Fire Modes

The `AsyncWorld` hook bus supports two fire modes. Async and blocking runtime
handles forward the selected mode to that same bus:

| Mode | Behavior |
|------|----------|
| `"blocking"` | Await the handler inline. This is the default. The tick waits for the hook. |
| `"spawn"` | Run the handler detached with `asyncio.create_task()`. The tick does not wait. |

Use `"spawn"` for telemetry or integration sinks that should not block the tick
path:

```python
await world.add_hook(PostTick, publish_metrics, mode="spawn")
```

Detached hook failures are logged. They are not raised into the tick caller.

## Blocking Runtime Hooks

`SyncRuntimeWorld.add_hook()` accepts an ordinary callable and adapts it to the
production asynchronous hook bus. The blocking facade does not select a
separate synchronous engine:

```python
from archetype import ArchetypeRuntime
from archetype.core.hooks import PreTick

def trace_tick(event: PreTick) -> None:
    print(event.tick)

with ArchetypeRuntime.sync() as runtime:
    world = runtime.world("observed", hooks=[(PreTick, trace_tick)])
    world.spawn()
    world.step()
```

Dynamic registration on either runtime facade requires an already-activated
world. Use the `hooks=` argument shown above when a hook must observe the first
operation. The `"spawn"` mode is available through the blocking facade too,
but a plain callback still runs on the engine's event-loop thread and should
remain lightweight.

## Failure Policy

Hook exceptions are logged at warning level and do not abort the world step.
Hooks should not be used for transactional invariants that must stop mutation or
persistence on failure. Use processors, services, or explicit command handling
for behavior that must participate in simulation correctness.

## Managed-world usage

Managed tick correctness does not depend on a public hook. Command
materialization happens before `PreTick`; manifest publication and command
settlement happen before `PostTick`; and required post-commit projection uses a
separate construction-injected projector. A projector failure retains the
exact committed receipt for retry and never replays the tick.

Application code should register hooks through the public world API. It should
not reach into `world._hooks` or private fire methods.

## Forking

Forked worlds inherit hook registrations that exist at fork time. New
registrations on either side after the fork do not propagate.

Handlers often close over process-local state, so be intentional about hooks
that are copied into forks.
