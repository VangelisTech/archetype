# Quickstart

Get a simulation running in under 2 minutes.

## Install

```bash
pip install archetype-ecs
```

Python 3.12+ required.

## Start the Server

```bash
archetype serve
```

This starts the simulation engine on `http://localhost:8000`. Keep it running.

## Create and Run a Simulation

In another terminal:

```bash
# Create a world
archetype world create my-sim

# Run 100 ticks
archetype run <world-id> --steps 100

# See what happened
archetype query <world-id>

# Full command history
archetype history <world-id>
```

## Fork a World

Branch a world to explore alternatives:

```bash
archetype world fork <world-id> --name branch-A

# Run the fork independently
archetype run <fork-id> --steps 100

# Compare states
archetype query <world-id>
archetype query <fork-id>
```

## Time-Travel

Query any previous tick:

```bash
archetype query <world-id> --tick 42
```

## What's Happening

Each `archetype run` step:

1. Drains pending commands from the priority queue
2. Applies them to the world (spawn/despawn/update entities)
3. Runs all registered processors (DataFrame transforms)
4. Appends the new state to storage (LanceDB)

Nothing is overwritten. Every tick is preserved. That's how forking and time-travel work.

## Next Steps

- [Architecture](./architecture.md) -- how the layers fit together
- [Writing Processors](./processors.md) -- build custom simulation logic with LLM-powered DataFrame transforms
- [API Reference](./api-reference.md) -- full REST endpoint docs
- [CLI Reference](./cli-reference.md) -- all CLI commands
