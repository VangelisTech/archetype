# DSL Comparison: v1 vs v2

## Side-by-Side Example

### Simple Movement

**DSL v1:**
```python
from archetype.dsl import World, behavior

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

@behavior
class Move:
    requires = [Position, Velocity]
    priority = 10
    
    async def act(self, agent, world, tick):
        agent.position.x += agent.velocity.vx
        agent.position.y += agent.velocity.vy

async with World("sim") as world:
    world.add_behavior(Move)
    await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=2))
    await world.run(ticks=10)
    
    for agent in world.agents:
        print(agent.position.x, agent.position.y)
```

**DSL v2:**
```python
from archetype.dsl import WorldV2, processor

class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

@processor
class Move:
    requires = [Position, Velocity]
    priority = 10
    
    def transform(self, arch, tick, dt):
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }

async with WorldV2("sim") as world:
    world.register(Move)
    await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=2))
    await world.run(ticks=10, dt=1.0)
    
    for agent in world.query(Position, Velocity):
        print(agent.position.x, agent.position.y)
```

### With Filtering

**DSL v1:**
```python
@behavior
class DamageMoving:
    requires = [Health, Velocity]
    filter = lambda agent: agent.velocity.vx != 0
    
    async def act(self, agent, world, tick):
        agent.health.current -= 10
```

**DSL v2:**
```python
@processor
class DamageMoving:
    requires = [Health, Velocity]
    
    @staticmethod
    def filter(arch):
        return arch.velocity.vx != 0
    
    def transform(self, arch, tick, dt):
        return {
            "health__current": arch.health.current - 10
        }
```

### Complex Logic

**DSL v1:**
```python
@behavior
class ComplexBehavior:
    requires = [Position, Health, Energy]
    
    async def act(self, agent, world, tick):
        # Can write arbitrary Python
        if agent.health.current < 50:
            agent.position.x = 0  # Teleport home
            agent.energy.current = agent.energy.max  # Full restore
        else:
            # Find nearby entities
            nearby = await world.find(
                lambda a: abs(a.position.x - agent.position.x) < 10
            )
            agent.state.nearby_count = len(nearby)
```

**DSL v2:**
```python
# Split into multiple processors

@processor
class TeleportLowHealth:
    requires = [Position, Health, Energy]
    
    @staticmethod
    def filter(arch):
        return arch.health.current < 50
    
    def transform(self, arch, tick, dt):
        return {
            "position__x": 0,
            "energy__current": arch.energy.max,
        }

# For nearby counting, need separate pre-computation processor
# that calculates spatial relationships and stores in component
```

## Feature Comparison

| Feature | v1 | v2 | Notes |
|---------|----|----|-------|
| **Ergonomics** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | v1 more natural for imperative code |
| **Performance** | ⭐⭐ | ⭐⭐⭐⭐⭐ | v2 is 7x faster |
| **Scalability** | ⭐⭐ | ⭐⭐⭐⭐⭐ | v1 struggles >100 entities |
| **Type Safety** | ⭐⭐⭐ | ⭐⭐⭐⭐ | v2 enables better compile-time checks |
| **Debuggability** | ⭐⭐⭐⭐ | ⭐⭐⭐ | v1 easier to step through |
| **Optimization** | ⭐⭐ | ⭐⭐⭐⭐⭐ | v2 leverages Daft optimizer |
| **Flexibility** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | v1 allows arbitrary Python |
| **Core Alignment** | ⭐⭐ | ⭐⭐⭐⭐⭐ | v2 honors DataFrame architecture |

## What Works Better in v1

1. **Prototyping**: Faster to write, less thinking about expressions
2. **Complex conditionals**: Can use if/else freely
3. **Entity queries**: Can find and read other entities mid-behavior
4. **LLM calls**: Direct `await world.prompt()` integration
5. **Message passing**: Auto-realization built in
6. **Debugging**: Can print(), inspect values easily

## What Works Better in v2

1. **Performance**: Pure DataFrame operations
2. **Scalability**: No collect-and-loop overhead
3. **Optimization**: Daft can optimize entire pipeline
4. **Memory**: No AgentProxy allocation per entity
5. **Simplicity**: Smaller implementation, fewer bugs
6. **Correctness**: No mutation tracking edge cases

## When Each Makes Sense

### Choose v1 When:
- Building a proof-of-concept
- Entity count will stay < 100
- Need complex inter-entity logic
- Speed of development > runtime performance
- Team is more comfortable with imperative code

### Choose v2 When:
- Building for production
- Entity count > 100 (or will grow)
- Performance is critical
- Want to leverage Daft's optimization
- Team understands DataFrame operations
- Building long-term maintainable code

## Migration Path

For projects starting with v1:

1. **Start in v1**: Get the concept working
2. **Profile**: Identify performance bottlenecks
3. **Port hot paths to v2**: Convert critical behaviors
4. **Keep v1 for complex logic**: No need to port everything
5. **Gradually increase v2**: As team becomes comfortable

You can mix v1 and v2 in the same project:
- Use v2 for core simulation loop (position, physics, etc.)
- Use v1 for complex decision-making (planning, communication)

## Common Pitfalls

### v1 Pitfalls

**1. Query in hot path**
```python
# ❌ BAD: Queries every entity, every tick
@behavior
class BadQuery:
    async def act(self, agent, world, tick):
        all_agents = world.agents  # Expensive!
        nearby = [a for a in all_agents if close_to(a, agent)]
```

**2. Side effects**
```python
# ❌ BAD: Modifies external state
@behavior
class BadSideEffect:
    async def act(self, agent, world, tick):
        global some_counter
        some_counter += 1  # Race conditions!
```

### v2 Pitfalls

**1. Trying to query in transform**
```python
# ❌ BAD: Can't query world in transform
@processor
class BadTransform:
    def transform(self, arch, tick, dt):
        # agents = world.query(...)  # ← Not available!
        return {}
```

**2. Complex conditionals**
```python
# ❌ BAD: Can't use if/else
@processor
class BadConditional:
    def transform(self, arch, tick, dt):
        if arch.health.current < 50:  # ← Won't work!
            return {"health__regen": True}
        return {}
```

Solution: Use filters or multiple processors.

## The Big Picture

DSL v1 and v2 represent different philosophies:

**v1: "Agents are objects"**
- Think imperatively
- Modify agent state directly
- Natural for OOP programmers
- Hides DataFrame implementation

**v2: "Agents are rows"**
- Think declaratively
- Specify column transforms
- Natural for data engineers
- Exposes DataFrame architecture

Neither is universally better. Choose based on:
- Team expertise
- Performance requirements
- Entity scale
- Code complexity

The goal of v2 is not to replace v1 for all use cases, but to provide a performant option when v1's trade-offs don't work.

## Roadmap

**Short term:**
- v1 and v2 coexist
- v2 gains missing features (spawn_world, messages)
- Documentation for both

**Medium term:**
- v2 becomes recommended default
- v1 still supported for complex logic
- Migration tools and guides

**Long term:**
- v2 is the standard
- v1 maintained but not actively developed
- New features target v2

## Questions?

- **"Can I mix v1 and v2?"** Not in the same World, but you can use both DSLs in the same project for different simulations.

- **"Do I need to rewrite everything?"** No. v1 is not being removed. Port when it makes sense.

- **"Which should I learn first?"** v1 for learning concepts, v2 for production code.

- **"What about the examples?"** Most examples use v1. We're adding v2 examples gradually.

- **"Is v1 deprecated?"** Not yet. It will be marked deprecated once v2 is feature-complete.
