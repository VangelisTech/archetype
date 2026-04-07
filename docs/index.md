# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

World state is columnar tables. Every tick is an append-only write to storage. This gives you time-travel queries, world forking, and full audit trails out of the box.

## Get Started

```bash
pip install archetype-ecs

archetype serve                          # start the server
archetype world create my-sim            # create a world
archetype run <world-id> --steps 100     # run 100 ticks
archetype query <world-id>              # see the result
```

## Fork a World

Branch a world to explore alternatives:

```bash
archetype world fork <world-id> --name branch-A
archetype run <fork-id> --steps 100
```

Source and fork diverge independently. Use this for MCTS, counterfactual reasoning, or A/B experiments.

## Time-Travel

Every tick is preserved. Query any point in history:

```bash
archetype query <world-id> --tick 42
```

## How It Works

Each `archetype run` step:

1. Drains pending commands from the priority queue (RBAC-enforced)
2. Applies them to the world (spawn/despawn/update entities)
3. Runs all registered processors (DataFrame transforms, optionally LLM-powered)
4. Appends the new state to LanceDB storage

Nothing is overwritten. That's how forking and time-travel work.

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
