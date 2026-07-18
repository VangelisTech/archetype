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

Every per-table compute task in that world receives the same container and the
same stored objects. A resource mutation can therefore be visible to another
archetype during the same tick. Per-table DataFrames are separate; resources
are not isolated. Stateful resources must expose concurrency-safe operations
or protect compound mutations with synchronization such as an `asyncio.Lock`.
Do not rely on task scheduling to order cross-archetype resource access.

Runtime users usually stage resources when creating a handle:

```python
world = runtime.world("sim", resources=[SimConfig(gravity=9.8)])
```

Post-activation resource attachment is a gated operator/admin action through `iCommandGateway.add_resource(...)`.

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
client = resources.get(PhysicsClient)
if client:
    await client.sync()
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
| Shared clients | `PhysicsClient`, `LabelingConfig` |
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

### Workflow clients in processors

The framework does not inject `CommandScheduler`, `CommandGateway`, or
`ServiceContainer` into world resources. Doing so would cross the application
boundary from inside tick execution and can create re-entrant lifecycle locks.
Processors transform their DataFrame and may use application-supplied,
concurrency-safe domain clients. Schedule later work from the owning workflow
outside the processor call.

## World Forking

When a world is forked, resources are shared by default. The source and fork point at the same `Resources` instance, so mutations to a stateful resource are visible to both worlds.

```python
fork = await world.fork()
# fork uses the same resource instances as the source by default
```

For isolated resources, create a fork through a workflow that explicitly attaches replacement resource instances after fork. That API is outside the v1 lifecycle contract.

## Source Reference

Resources: `src/archetype/core/resources.py`
