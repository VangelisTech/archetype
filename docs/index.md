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

## Next Steps

- **[Quickstart](guide/quickstart.md)** -- full walkthrough with forking and time-travel
- **[Architecture](guide/architecture.md)** -- how the layers fit together
- **[Processors](guide/processors.md)** -- build custom simulation logic
- **[Examples](guide/examples.md)** -- patterns and working demos
