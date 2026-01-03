# Quickstart

Archetype is a data-centric ECS simulation runtime built on Daft DataFrames.

## Install

```bash
# From PyPI (when released)
pip install archetype

# From source
git clone https://github.com/vangelis-tech/archetype.git
cd archetype
uv sync
```

## Hello World with the DSL

The simplest way to use Archetype is through the DSL layer:

```python
import asyncio
from archetype import Component
from archetype.dsl import World, behavior

# Define components (state schema)
class Greeter(Component):
    name: str = ""
    message: str = ""

# Define behavior (pure transform)
@behavior(Greeter)
async def greet(agent, ctx):
    agent.greeter.message = f"Hello from {agent.greeter.name}! (tick {ctx.tick})"

async def main():
    async with World("hello") as world:
        world.register(greet)
        
        # Spawn agents
        await world.spawn(Greeter(name="Alice"))
        await world.spawn(Greeter(name="Bob"))
        
        # Run simulation
        await world.step()
        
        # Query results
        for agent in world.find(Greeter):
            print(agent.greeter.message)

asyncio.run(main())
```

Output:
```
Hello from Alice! (tick 1)
Hello from Bob! (tick 1)
```

## LLM-Powered Agents

Using `daft.functions.prompt` for AI-driven behavior:

```python
import daft
from archetype import Component
from archetype.dsl import World, behavior

class Debater(Component):
    perspective: str = ""
    argument: str = ""
    history_json: str = "[]"

@behavior(Debater)
async def debate(agent, ctx):
    # Build prompt from state
    prompt = f"""You are debating from the {agent.debater.perspective} perspective.
    Previous arguments: {agent.debater.history_json}
    Provide your next argument in 2-3 sentences."""
    
    # LLM call happens in Daft's vectorized prompt function
    # (actual implementation uses df.with_column + daft.functions.prompt)
    agent.debater.argument = prompt  # Simplified for example

async def main():
    async with World("debate") as world:
        world.register(debate)
        await world.spawn(Debater(perspective="optimist"))
        await world.spawn(Debater(perspective="pessimist"))
        
        for tick in range(3):
            await world.step()

asyncio.run(main())
```

## Inner Simulations (MCTS)

Fork worlds for counterfactual reasoning:

```python
from archetype.dsl import World, behavior, spawn_world

@behavior(Agent)
async def think(agent, ctx):
    # Fork the world to explore possibilities
    async with spawn_world(ctx.world, fork_state=True) as inner:
        inner.register(simple_behavior)
        
        # Run hypothetical scenarios
        for _ in range(5):
            await inner.step()
        
        # Evaluate outcome
        results = list(inner.find(Agent))
        best_score = max(a.state.score for a in results)
    
    # Use insight in outer world
    agent.state.decision = f"Best possible score: {best_score}"
```

## Next Steps

- [Architecture](./architecture.md) - How the layers fit together
- [Core Concepts](./core-concepts.md) - ECS fundamentals
- [DSL Guide](./dsl.md) - Full DSL reference
- [Storage](./storage.md) - Persistence and time-travel
