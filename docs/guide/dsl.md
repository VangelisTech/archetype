# DSL Guide

> **Note**: This guide covers DSL v1. For the new DataFrame-first DSL v2, see:
> - [DSL v2 Migration Guide](../DSL_V2_MIGRATION.md)
> - [DSL Design Philosophy](../DSL_PHILOSOPHY.md)
> - [DSL Comparison: v1 vs v2](../DSL_COMPARISON.md)

The Archetype DSL v1 provides an ergonomic, agent-centric programming model that compiles down to DataFrame operations.

**When to use v1:**
- Prototyping and rapid development
- Small entity counts (<100)
- Complex inter-entity logic
- When ergonomics matter more than performance

**When to use v2:**
- Production systems
- Large entity counts (>100)
- Performance-critical code
- When you want to honor the core DataFrame architecture

## World Context Manager

The `World` class is an async context manager that handles setup and teardown:

```python
from archetype.dsl import World

async with World("my_simulation") as world:
    # Register behaviors
    world.register(move, think, communicate)
    
    # Spawn entities
    await world.spawn(Agent(name="Alice"))
    
    # Run simulation
    for _ in range(100):
        await world.step(dt=0.1)
```

### World Options

```python
async with World(
    name="simulation",
    storage_uri="./data",      # Persistence path
    run_id="experiment_1",     # Group ticks into runs
) as world:
    ...
```

## @behavior Decorator

Define agent behaviors with the `@behavior` decorator:

```python
from archetype.dsl import behavior

@behavior(Position, Velocity)
async def move(agent, ctx):
    """Update position based on velocity."""
    agent.position.x += agent.velocity.vx * ctx.dt
    agent.position.y += agent.velocity.vy * ctx.dt
```

### Decorator Options

```python
@behavior(
    *components,           # Required component types
    priority=10,           # Execution order (lower = earlier)
    filter=lambda a: ...,  # Only run on matching agents
)
async def my_behavior(agent, ctx):
    ...
```

### Context Object

The `ctx` parameter provides:

| Attribute | Type | Description |
|-----------|------|-------------|
| `ctx.tick` | `int` | Current tick number |
| `ctx.dt` | `float` | Time delta for this step |
| `ctx.world` | `World` | Reference to parent world |
| `ctx.resources` | `Resources` | DI container |

## AgentProxy

The `agent` parameter is an `AgentProxy` that provides natural attribute access:

```python
@behavior(Position, Velocity, Health)
async def update(agent, ctx):
    # Read component fields
    x = agent.position.x
    hp = agent.health.current
    
    # Write component fields (mutations are tracked)
    agent.position.x = x + 1.0
    agent.health.current = hp - 10
```

### JSON Fields

For complex types (lists, dicts), use `_json` suffix fields:

```python
class Memory(Component):
    history_json: str = "[]"  # JSON-encoded list

@behavior(Memory)
async def remember(agent, ctx):
    # Read as Python object
    history = json.loads(agent.memory.history_json)
    history.append({"tick": ctx.tick, "event": "something"})
    
    # Write back as JSON
    agent.memory.history_json = json.dumps(history)
```

## Spawning Entities

Create entities with `world.spawn()`:

```python
# Single entity
await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=0))

# Multiple entities
for i in range(100):
    await world.spawn(
        Agent(name=f"agent_{i}"),
        Position(x=i * 10, y=0)
    )
```

## Querying Entities

Find entities with `world.find()` and `world.find_one()`:

```python
# All entities with Position component
for agent in world.find(Position):
    print(agent.position.x, agent.position.y)

# Single entity by filter
alice = world.find_one(Agent, filter=lambda a: a.agent.name == "Alice")
if alice:
    print(alice.agent.name)
```

## Broadcasting Messages

Send messages to all agents:

```python
from archetype.dsl import broadcast

@behavior(Agent, Inbox)
async def communicate(agent, ctx):
    # Send to everyone
    await broadcast(ctx.world, {
        "from": agent.agent.name,
        "content": "Hello everyone!"
    })
```

## spawn_world() - Inner Simulations

Fork the world for MCTS or counterfactual reasoning:

```python
from archetype.dsl import spawn_world

@behavior(Planner)
async def plan(agent, ctx):
    best_action = None
    best_score = float('-inf')
    
    for action in ["left", "right", "forward"]:
        # Fork world state
        async with spawn_world(ctx.world, fork_state=True) as inner:
            inner.register(simple_physics)
            
            # Apply hypothetical action
            inner_agent = inner.find_one(Planner)
            inner_agent.planner.action = action
            
            # Simulate forward
            for _ in range(10):
                await inner.step()
            
            # Evaluate
            score = evaluate(inner)
            if score > best_score:
                best_score = score
                best_action = action
    
    # Apply best action in real world
    agent.planner.action = best_action
```

### spawn_world Options

```python
async with spawn_world(
    parent_world,
    fork_state=True,     # Copy current entity state
    inherit_broker=True, # Share command broker
) as child:
    ...
```

## Parallel Rollouts

Run multiple simulations concurrently:

```python
from archetype.dsl import parallel_rollouts

async def run_episode(seed: int) -> float:
    async with World(f"rollout_{seed}") as world:
        world.register(agent_behavior)
        await world.spawn(Agent(seed=seed))
        
        for _ in range(100):
            await world.step()
        
        return world.find_one(Agent).score

# Run 10 rollouts in parallel
results = await parallel_rollouts(
    run_episode,
    seeds=range(10),
    max_concurrent=4
)
```

## Integration with Daft

For LLM-powered behaviors, use `daft.functions.prompt`:

```python
import daft

@behavior(Debater)
async def debate(agent, ctx):
    # The actual LLM call happens via Daft's prompt function
    # in the compiled processor's DataFrame transform
    prompt = f"""
    You are arguing from the {agent.debater.perspective} perspective.
    History: {agent.debater.history_json}
    
    Provide your next argument (2-3 sentences).
    """
    agent.debater.pending_prompt = prompt
```

The DSL's behavior compiler integrates with Daft to batch LLM calls efficiently across all agents.
