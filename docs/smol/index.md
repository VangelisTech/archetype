# Smol

| | |
|---|---|
| Distribution | `archetype-smol` |
| Import package | `archetype.smol` |
| Public surface | `Component`, `Processor`, `World`, `RunResult` |
| Dependency on `archetype-ecs` | No |

Smol is a tiny synchronous, in-memory DataFrame ECS for learning the core
Archetype model. It is a separate engine, not a compatibility facade, storage
backend, or world library for `archetype-ecs`.

## Install

<!-- markdownlint-disable MD046 -->

=== "uv"

    ```bash
    uv add archetype-smol
    ```

=== "pip"

    ```bash
    pip install archetype-smol
    ```

<!-- markdownlint-enable MD046 -->

## Learn the loop

```python
from daft import col

from archetype.smol import Component, Processor, World


class Position(Component):
    x: float = 0.0


class Move(Processor):
    components = (Position,)

    def process(self, df, *, tick):
        return df.with_column("position__x", col("position__x") + 1)


world = World(processors=[Move()])
entity_id = world.spawn(Position(x=2))
world.step()

assert world.query(Position).to_pylist() == [
    {
        "entity_id": entity_id,
        "tick": 1,
        "is_active": True,
        "position__x": 3.0,
    }
]
```

The loop is deliberately visible:

1. component types define an archetype table;
2. matching processors transform its Daft DataFrame in priority order;
3. Smol materializes each table at the in-memory boundary;
4. the world publishes every successful table together as the next tick; and
5. `query()` and `history()` return reviewable DataFrames.

## Surface

| API | Contract |
|---|---|
| `Component` | Frozen, validated Pydantic value whose fields become prefixed DataFrame columns |
| `Processor.process(df, *, tick)` | Synchronous lazy transform for every archetype containing the declared `components` |
| `World(processors=...)` | Create one single-threaded in-memory world |
| `add_processor()` / `remove_processor()` | Change the priority-ordered processor set between steps |
| `spawn()` / `update()` / `despawn()` | Mutate current state immediately without changing an entity's component signature |
| `step()` | Compute every active archetype and publish all successful results at the next tick |
| `run(steps)` | Execute zero or more steps and return `RunResult` |
| `query(*component_types)` | Return active current rows containing the requested component subset |
| `history(*component_types)` | Return the retained per-tick snapshots, including despawn tombstones |

There is at most one snapshot for an entity at a tick. Multiple immediate
mutations before the next `step()` update that current snapshot; a successful
step publishes the next one. Passing no component types to `query()` or
`history()` selects metadata for every matching entity.

## Deliberate limits

| Smol includes | Smol omits |
|---|---|
| Frozen typed Component records | Persistent or remote storage |
| Synchronous DataFrame Processors | Async execution and per-table concurrency |
| Priority and component-subset matching | Commands, permissions, and audit |
| Immediate spawn, update, and despawn | Activities and provider recovery |
| Atomic in-memory steps | Hooks and Resources |
| Current and historical snapshots | Runtime/API/CLI hosting |

Processors must preserve input columns, metadata, and each entity exactly
once. A failed transform publishes no state, history, or tick. Python side
effects inside a processor cannot be rolled back, so processors should remain
pure.

## Choose the production framework when

Use [`archetype-ecs`](../guide/runtime.md) when work needs durable worlds,
append-only storage, crash recovery, concurrent execution, commands,
Activities, artifacts, evaluation, API hosting, or separately installed world
libraries. There is no migration or alias promise between the two engines;
their shared vocabulary exists to make the production architecture easier to
understand.
