# Writing Processors

A processor is an `AsyncProcessor` subclass that transforms a Daft DataFrame each tick. The system executes a processor on every archetype whose signature is a superset of the processor's declared `components` tuple.

```python
class AsyncProcessor(iAsyncProcessor):
    components: tuple[type["Component"], ...] = ()
    priority: int = 10

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame:
        return df
```

## Basic Processor

```python
from daft import DataFrame, col
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)  # Only runs on entities with BOTH
    priority = 10                       # Lower = runs earlier

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return (
            df
            .with_column("position__x", col("position__x") + col("velocity__vx"))
            .with_column("position__y", col("position__y") + col("velocity__vy"))
        )
```

Key points:

- `components` declares required component types — the system only passes entities that have all of them
- `priority` controls execution order within a tick (lower runs first)
- `process()` receives a Daft DataFrame and must return a DataFrame
- Column names are prefixed: `ComponentName__field_name`

## Accessing Resources

Processors receive the world's `Resources` container via kwargs:

```python
from dataclasses import dataclass
from archetype.core.resources import Resources

@dataclass
class SimConfig:
    gravity: float = 9.8
    max_speed: float = 100.0

class PhysicsProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 5

    async def process(self, df: DataFrame, resources: Resources = None, **kwargs) -> DataFrame:
        config = resources.require(SimConfig) if resources else SimConfig()
        return (
            df
            .with_column("velocity__vy", col("velocity__vy") - config.gravity)
            .with_column("position__x", col("position__x") + col("velocity__vx"))
            .with_column("position__y", col("position__y") + col("velocity__vy"))
        )
```

Setup:

```python
world.resources.insert(SimConfig(gravity=9.8))
await world.system.add_processor(PhysicsProcessor())
```

## LLM-Powered Processors

`daft.functions.prompt` executes LLM calls across all rows in the DataFrame concurrently:

```python
from daft.functions import prompt

class Agent(Component):
    name: str = ""
    role: str = ""
    last_thought: str = ""

class ThinkProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__last_thought",
            prompt(
                col("agent__role") + "\nYou are " + col("agent__name")
                + ". Tick " + str(tick) + ". What do you do next? One sentence.",
                system_message="You are an agent in a simulation. Stay in character.",
                model="gpt-5-mini",
                max_output_tokens=60,
            ),
        )
```

Daft parallelizes prompt execution across the DataFrame. Batching is handled by the Daft execution engine.

### Structured LLM Outputs

Use Pydantic models for type-safe LLM responses:

```python
from pydantic import BaseModel

class Decision(BaseModel):
    action: str
    target: str
    confidence: float

class DecisionProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "decision",
            prompt(
                col("agent__role") + ": Choose an action.",
                return_format=Decision,
                model="gpt-5-mini",
            ),
        ).unnest("decision")
```

## Tick and Run Context

Processors receive useful context via kwargs:

```python
async def process(self, df, tick=0, resources=None, **kwargs):
    # tick: current tick number
    # resources: world's Resources container
    # Additional kwargs from RunConfig or world.step()
    ...
```

## Processor Ordering

Processors run in `priority` order within each tick. Use this to chain logic:

```python
class InputProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 1      # Runs first — gather input

class ThinkProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10     # Runs second — process input

class ActionProcessor(AsyncProcessor):
    components = (Agent, Position)
    priority = 20     # Runs third — execute actions

class CleanupProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 100    # Runs last — bookkeeping
```

## Adding Processors to a World

```python
# Direct (Python API)
await world.system.add_processor(ThinkProcessor())

# Via SimulationService
container.simulation_service.add_processor(world_id, ThinkProcessor())
```

## Interacting with the Broker

Processors can submit commands via the broker in Resources:

```python
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType

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

This enables agents to spawn new entities, send messages, or trigger world-level operations from within the simulation loop.

## Testing Processors

```python
import pytest
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.config import WorldConfig, RunConfig

@pytest.fixture
async def world():
    # Minimal world setup for testing
    querier = InMemoryQuerier()
    updater = InMemoryUpdater(querier)
    system = AsyncSystem()
    return AsyncWorld(WorldConfig(name="test"), querier, updater, system)

@pytest.mark.asyncio
async def test_movement(world):
    await world.system.add_processor(MovementProcessor())
    await world.create_entity([Position(x=0, y=0), Velocity(vx=1, vy=2)])

    await world.run(RunConfig(num_steps=3))

    for _sig, df in world._live.items():
        rows = df.collect().to_pylist()
        assert rows[0]["position__x"] == 3.0
        assert rows[0]["position__y"] == 6.0
```
