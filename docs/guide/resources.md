# Resources

`Resources` is a type-keyed dependency injection container for world-level shared state. It holds configuration, services, and context that processors need but that is not entity data.

```python
class Resources:
    def insert(self, resource: T) -> None: ...
    def get(self, resource_type: type[T]) -> T | None: ...
    def require(self, resource_type: type[T]) -> T: ...
    def remove(self, resource_type: type[T]) -> T | None: ...
    def contains(self, resource_type: type[T]) -> bool: ...
    def items(self) -> ItemsView[type, object]: ...
```

## How It Works

Resources are the shared state layer of the ECS. Where [Components](components.md) hold per-entity data in DataFrame columns, Resources hold per-world singletons that live outside the columnar storage path entirely.

The container is a `dict[type, object]` -- each resource is keyed by its concrete Python type. This means there is exactly one instance of any given type at a time. Inserting a second instance of the same type overwrites the first.

Every `AsyncWorld` owns a `.resources` instance. During tick execution, `AsyncSystem.execute()` passes it into each processor's `process()` method as a keyword argument.

Runtime users usually stage resources when creating a handle:

```python
world = runtime.world("sim", resources=[SimConfig(gravity=9.8)])
```

Post-activation resource attachment is a gated operator/admin action through `iCommandService.add_resource(...)`.

```text
runtime.world(..., resources=[SimConfig(...)])
       |
AsyncSystem.execute(resources=world.resources)
       |
processor.process(df, resources=resources, tick=tick)
```

## API

### insert

Store a resource, keyed by its type. This is the core `AsyncWorld` API; runtime/API callers should use staged resources or gated resource attachment.

```python
world.resources.insert(SimConfig(gravity=9.8))
```

Calling `insert()` with a second instance of the same type replaces the first.

### get

Retrieve a resource by type, returning `None` if absent:

```python
broker = resources.get(CommandBroker)
if broker:
    await broker.enqueue(world_id, cmd)
```

### require

Retrieve a resource by type, raising `KeyError` if absent:

```python
config = resources.require(SimConfig)
```

Use `require()` when the processor cannot function without the resource. Use `get()` when the resource is optional.

### remove

Remove and return a resource, or `None` if not present:

```python
old_config = resources.remove(SimConfig)
```

### contains

Check whether a resource type is registered. Also supports the `in` operator:

```python
resources.contains(SimConfig)   # True
SimConfig in resources          # True
```

## What Goes in Resources

Resources are not entity data. They are the scaffolding around it:

| Category | Examples |
|----------|---------|
| Environment parameters | `SimConfig(gravity=9.8)`, `PhysicsConfig(...)` |
| Shared services | `CommandBroker`, `LabelingConfig` |
| Simulation context | `SamplingConfig`, budget trackers |

In RL terms: MDP parameters, hyperparameters, shared infrastructure.

## Usage in Processors

Processors receive `resources` as a keyword argument in `process()`:

```python
class PhysicsProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 5

    async def process(self, df: DataFrame, resources: Resources = None, **kwargs) -> DataFrame:
        config = resources.require(SimConfig) if resources else SimConfig()
        return df.with_column(
            "velocity__vy",
            col("velocity__vy") - config.gravity,
        )
```

### Submitting Commands from Processors

Processors are trusted internal code once registered. If a processor needs delayed scheduling, it may use a sanctioned broker resource or another internal path. External user actions should still go through `iCommandService`.

```python
class SpawnerProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 50

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

## World Forking

When a world is forked, resources are shared by default. The source and fork point at the same `Resources` instance, so mutations to a stateful resource are visible to both worlds.

```python
fork = await world.fork()
# fork uses the same resource instances as the source by default
```

For isolated resources, create a fork through a workflow that explicitly attaches replacement resource instances after fork. That API is outside the v1 lifecycle contract.

## Source Reference

Resources: `src/archetype/core/resources.py`
