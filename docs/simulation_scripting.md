The typical **usage pattern for Archetype simulation scripts** (using the `archetype` library) is modeled after the ECS (Entity-Component-System) paradigm, but designed for high-performance, reproducible, and composable simulation workflows. The pattern is both familiar to simulation authors and designed for easy migration between synchronous and asynchronous execution, including the new episode/streaming architecture.

Below is a summary of the standard workflow, with code snippets and explanations:

---

## 1. **World Creation**

You first create a simulation world, which manages all entities, components, systems, and storage.
You choose between synchronous (classic) or asynchronous (for episode/streaming parallelism).

```python
from archetype import make_simple_world, make_async_world

# Synchronous (classic, step-based)
world = make_simple_world(uri="data/", world_id="sim1")

# Asynchronous (for episode/streaming/parallelism)
async_world = make_async_world(uri="data/", world_id="sim1")
```

---

## 2. **Component and Processor Definition**

You define simulation state as **Components** (Pydantic/LanceModel data models), and logic as **Processors**.
Processors can be synchronous or asynchronous, and are registered with the world.

```python
from archetype.core.base import Component
from archetype.core.processor import processor, Processor

class Position(Component):
    x: float
    y: float

class Velocity(Component):
    dx: float
    dy: float

@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df, dt):
        # Update positions by velocity
        return df.with_column("position__x", df["position__x"] + df["velocity__dx"] * dt)\
                 .with_column("position__y", df["position__y"] + df["velocity__dy"] * dt)

world.add_processor(MovementProcessor())
```

For async/episode systems, you use `AsyncProcessor` and may write `async def process(...)`.

---

## 3. **Entity/Component Spawning**

Entities are created with one or more components.
Spawns are typically batched at the start, but can occur during simulation.

```python
id1 = world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=0))
id2 = world.spawn(Position(x=10, y=5), Velocity(dx=0, dy=-1))
```

---

## 4. **Simulation Loop**

You step the simulation forward, usually by calling `world.step(dt=...)` in a loop.

**Synchronous:**
```python
for t in range(100):
    world.step(dt=0.1)
```

**Asynchronous/Episode:**
```python
import asyncio

async def run_sim():
    for t in range(100):
        await async_world.step(dt=0.1)

asyncio.run(run_sim())
```

Or, for episode-based execution:
```python
from archetype.core.aio.episode_world import EpisodeWorld

ep_world = EpisodeWorld(world, episode_size=10)
stats = asyncio.run(ep_world.run_episode(dt=0.1))
```

---

## 5. **Querying and Analysis**

You can query state at any time for analysis, using the world's query interface:

```python
df = world.get_archetypes(step=10)
# or async:
df = await async_world.get_archetype_for_entity(entity_id, Position)
```

---

## 6. **Persistence & Reproducibility**

All entity/component updates, steps, and outputs are stored in Daft/LanceDB tables, enabling:

- Time travel (querying any step)
- Reproducible runs
- Parallel/streaming execution

---

## **Summary**

**Archetype simulation scripts** follow this pattern:

1. **Create a world** (sync or async)
2. **Define components/processors**
3. **Spawn entities**
4. **Step the simulation** (sync: `world.step()`, async: `await world.step()`, or episode-based)
5. **Query/analyze state**
6. (Optionally) **Leverage episodes, streaming, or synchronization points** for advanced parallelism

---

**Note:**
- For large/long simulations, prefer the async/episode/streaming pattern for scalability and throughput.
- The API is intentionally similar between sync and async worlds to support easy migration and testing.
- Advanced patterns (episodes, streaming queries, selective synchronization) are available for large-scale scenarios.
