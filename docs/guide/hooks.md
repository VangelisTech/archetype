# Lifecycle Hooks

Hooks are typed lifecycle callbacks attached to a single world. They are
intended for observability, integration glue, and lightweight side effects that
need to run at known world boundaries.

The hook catalogue lives in `archetype.core.hooks`, a neutral core module used
by both `AsyncWorld` and `SyncWorld`.

## Type Model

Hook events are frozen dataclasses. Every concrete event inherits from the
nominal base class `HookEvent`, which carries the world identity:

```python
from archetype.core.hooks import HookEvent, PostTick

assert issubclass(PostTick, HookEvent)
```

The handler types are explicit about runtime mode:

| Type | Runtime | Callable shape |
|------|---------|----------------|
| `AsyncHookHandler[E]` | `AsyncWorld` | `async def handler(event: E) -> None` |
| `SyncHookHandler[E]` | `SyncWorld` | `def handler(event: E) -> None` |

`E` is bound to `HookEvent`, so `world.add_hook(PostTick, handler)` ties the
event class and handler argument to the same event type.

Core uses dataclasses rather than Pydantic models here because hooks are in the
world hot path. Events are small immutable payloads, not validation boundaries.

## Registering Hooks

```python
from archetype.core.hooks import HookHandle, PostTick

async def log_tick(event: PostTick) -> None:
    print(f"world={event.world_id} tick={event.tick}")

handle: HookHandle = world.add_hook(PostTick, log_tick)

# Later:
world.remove_hook(handle)
```

`add_hook()` returns an opaque `HookHandle`. Store the handle if the hook should
be removed later. Handles are registry-scoped, so a handle minted by one world
cannot unregister a same-shaped hook in another world.

For a complete runnable example, see
[`examples/07_hooks.py`](https://github.com/VangelisTech/archetype/blob/main/examples/07_hooks.py).

## Event Catalogue

| Event | Payload fields | Fires when |
|-------|----------------|------------|
| `PreTick` | `world_id`, `tick` | At the start of `World.step()`, before any archetype runs |
| `PostTick` | `world_id`, `tick`, `results` | After all archetypes process, `_live` refreshes, and `tick` increments |
| `OnSpawn` | `world_id`, `entity_id`, `components` | After `create_entity()` or `spawn_reserved()` registers the entity |
| `OnDespawn` | `world_id`, `entity_id` | After `remove_entity()` cancels a pending spawn or queues a despawn row |
| `OnComponentAdded` | `world_id`, `entity_id`, `components` | After `add_components()` moves the entity to a wider archetype |
| `OnComponentRemoved` | `world_id`, `entity_id`, `component_types` | After `remove_components()` moves the entity to a narrower archetype |

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

## Async Fire Modes

`AsyncWorld.add_hook()` supports two fire modes:

| Mode | Behavior |
|------|----------|
| `"blocking"` | Await the handler inline. This is the default. The tick waits for the hook. |
| `"spawn"` | Run the handler detached with `asyncio.create_task()`. The tick does not wait. |

Use `"spawn"` for telemetry or integration sinks that should not block the tick
path:

```python
world.add_hook(PostTick, publish_metrics, mode="spawn")
```

Detached hook failures are logged. They are not raised into the tick caller.

## Sync Hooks

`SyncWorld.add_hook()` uses the same event dataclasses and `HookHandle` type,
but handlers are plain callables and there is no `"spawn"` mode:

```python
from archetype.core.hooks import PreTick

def trace_tick(event: PreTick) -> None:
    print(event.tick)

handle = world.add_hook(PreTick, trace_tick)
world.remove_hook(handle)
```

## Failure Policy

Hook exceptions are logged at warning level and do not abort the world step.
Hooks should not be used for transactional invariants that must stop mutation or
persistence on failure. Use processors, services, or explicit command handling
for behavior that must participate in simulation correctness.

## Service-Layer Usage

`WorldService` attaches a `PostTick` hook when a `WorldRegistry` is configured.
That hook updates the persisted registry tick after each completed step.

Application code should register hooks through the public world API. It should
not reach into `world._hooks` or private fire methods.

## Forking

Forked worlds do not inherit source-world hooks. A hook closes over process-local
state and belongs to the specific world where it was registered. Register new
hooks on the fork when needed.
