# Archetype

**Data-centric ECS simulation runtime built on Daft DataFrames.**

Archetype is designed for building large-scale multi-agent simulations where:
- State is columnar (Arrow/LanceDB)
- Behavior is DataFrame transforms
- Time is append-only tick logs

## Key Features

- **Agent DSL** - Ergonomic `@behavior` decorators and `World` context managers
- **spawn_world()** - Fork simulations for MCTS and counterfactual reasoning
- **Daft-native** - LLM calls via `daft.functions.prompt`, vectorized transforms
- **Time travel** - Query any tick, replay runs deterministically

## Quick Install

```bash
pip install archetype
# or
uv add archetype
```

## Minimal Example

```python
import asyncio
from archetype import Component
from archetype.dsl import World, behavior

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 1.0
    vy: float = 1.0

@behavior(Position, Velocity)
async def move(agent, ctx):
    agent.position.x += agent.velocity.vx * ctx.dt
    agent.position.y += agent.velocity.vy * ctx.dt

async def main():
    async with World("physics") as world:
        world.register(move)
        await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=0.5))
        
        for _ in range(10):
            await world.step(dt=0.1)
        
        for agent in world.find(Position):
            print(f"Final: ({agent.position.x}, {agent.position.y})")

asyncio.run(main())
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    archetype.dsl                     │
│  World, @behavior, spawn_world, AgentProxy          │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                    archetype.app                     │
│  CommandBroker, WorldOrchestrator, WorldFactory     │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                    archetype.core                    │
│  AsyncWorld, AsyncSystem, Resources, LanceDB Store  │
└─────────────────────────────────────────────────────┘
```
