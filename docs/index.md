# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

World state is columnar tables. Every tick is an append-only write to storage. This gives you time-travel queries, world forking, and full audit trails out of the box.

## Get Started

```bash
pip install archetype-ecs
```

```python
import asyncio
from daft import DataFrame, col
from archetype.core.component import Component
from archetype.dsl import World, behavior

class Agent(Component):
    name: str = ""
    skill: float = 1.0
    experience: float = 0.0

@behavior
class GainExperience:
    requires = [Agent]
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__experience",
            col("agent__experience") + col("agent__skill") * 2.0,
        )

async def main():
    async with World("my-sim") as world:
        world.add_behavior(GainExperience)
        await world.spawn(Agent(name="Alice", skill=3.0))
        await world.spawn(Agent(name="Bob", skill=2.0))
        await world.run(ticks=10)

        for agent in world.agents:
            print(f"{agent.name}: exp={agent.experience}")

asyncio.run(main())
```

## CLI

```bash
archetype serve
archetype world create my-sim
archetype run <world-id> --steps 100
archetype query <world-id>
archetype world fork <world-id> --name branch-A
archetype query <world-id> --tick 42
```

## Try It Live

``` { .python .live }
import json

# Define a world with agents
world = {"name": "debate", "tick": 0, "agents": [
    {"name": "Ada",  "role": "scientist",   "energy": 100, "mood": "curious"},
    {"name": "Rex",  "role": "explorer",    "energy": 100, "mood": "bold"},
    {"name": "Iris", "role": "philosopher", "energy": 100, "mood": "pensive"},
]}

# Run 5 ticks — energy decays, mood shifts
for tick in range(1, 6):
    world["tick"] = tick
    for agent in world["agents"]:
        agent["energy"] -= 12
        if agent["energy"] > 60:
            agent["mood"] = "energized"
        elif agent["energy"] > 30:
            agent["mood"] = "focused"
        else:
            agent["mood"] = "tired"

print(f"After {world['tick']} ticks:\n")
for a in world["agents"]:
    print(f"  {a['name']:5} ({a['role']:12}) energy={a['energy']:3}  mood={a['mood']}")
```

## Next Steps

- **[Quickstart](guide/quickstart.md)** -- full walkthrough with forking and time-travel
- **[Architecture](guide/architecture.md)** -- how the layers fit together
- **[Processors](guide/processors.md)** -- build custom simulation logic
- **[Examples](guide/examples.md)** -- patterns and working demos
