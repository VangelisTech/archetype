# DSL v2 Migration Guide

## Overview

DSL v2 is a complete redesign that honors the core engine's DataFrame-first architecture. The key difference: **behaviors compile to pure DataFrame transforms instead of collect-and-loop operations.**

## Why the Change?

### Problems with DSL v1

The original DSL (`archetype.dsl.core`) had good ergonomics but poor performance characteristics:

```python
# DSL v1 - ANTI-PATTERN
@behavior
class Move:
    requires = [Position, Velocity]
    
    async def act(self, agent, world, tick):
        # ❌ Operates on single agent
        agent.position.x += agent.velocity.vx
```

**Under the hood, this:**
1. Collects the entire DataFrame to a Python list
2. Loops through each row
3. Creates an AgentProxy per row
4. Tracks mutations in a dict
5. Applies mutations via a Daft UDF

**Problems:**
- Defeats DataFrame batch operations
- AgentProxy overhead per row
- Loses Daft optimization opportunities
- Scales poorly with entity count

### DSL v2 Solution

```python
# DSL v2 - CORRECT PATTERN
@processor
class Move:
    requires = [Position, Velocity]
    
    def transform(self, arch, tick, dt):
        # ✅ Returns DataFrame column expressions
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }
```

**This compiles to:**
```python
df = df.with_columns({
    "position__x": col("position__x") + col("velocity__vx") * dt,
    "position__y": col("position__y") + col("velocity__vy") * dt,
})
```

**Benefits:**
- Pure DataFrame transform
- Leverages Daft's columnar optimization
- No Python loops
- Scales to millions of entities

## Migration Checklist

### 1. Import Changes

```python
# Old
from archetype.dsl import World, behavior, spawn_world

# New
from archetype.dsl.v2 import WorldV2, processor
```

### 2. Behavior → Processor

The decorator name changes from `@behavior` to `@processor`:

```python
# Old
@behavior
class MyBehavior:
    requires = [ComponentA, ComponentB]
    priority = 10
    
    async def act(self, agent, world, tick):
        agent.component_a.field = new_value

# New
@processor
class MyBehavior:
    requires = [ComponentA, ComponentB]
    priority = 10
    
    def transform(self, arch, tick, dt):
        return {
            "componenta__field": arch.component_a.field + 1,
        }
```

**Key differences:**
- `act(agent, world, tick)` → `transform(arch, tick, dt)`
- `agent.component.field = value` → `return {"component__field": expr}`
- Method is sync, not async (returns expressions, not side effects)

### 3. World API Changes

```python
# Old
async with World("sim") as world:
    world.add_behavior(MyBehavior)
    await world.spawn(Component(...))
    await world.run(ticks=10)

# New
async with WorldV2("sim") as world:
    world.register(MyBehavior)
    await world.spawn(Component(...))
    await world.run(ticks=10, dt=1.0)
```

Changes:
- `World` → `WorldV2`
- `add_behavior()` → `register()`
- `run()` takes `dt` parameter

### 4. Querying Entities

```python
# Old
for agent in world.agents:
    print(agent.position.x)

# New
agents = world.query(Position)
for agent in agents:
    print(agent.position.x)
```

Changes:
- `world.agents` → `world.query(*components)`
- Query is explicit about which components you need
- Returns `AgentView` (read-only) instead of `AgentProxy`

### 5. Filters

```python
# Old
@behavior
class Filtered:
    filter = lambda agent: agent.health.current > 50

# New
@processor
class Filtered:
    @staticmethod
    def filter(arch):
        return arch.health.current > 50
```

Changes:
- Filter is now a static method
- Uses `arch` parameter (compiles to DataFrame.where())
- Filter expressions are compiled, not evaluated per-row

### 6. Conditional Logic

Conditional updates are trickier in DataFrame land:

```python
# Old - Easy
@behavior
class Conditional:
    async def act(self, agent, world, tick):
        if agent.health.current < 50:
            agent.health.regenerating = True

# New - Use filters or expressions
@processor
class Conditional:
    @staticmethod
    def filter(arch):
        return arch.health.current < 50
    
    def transform(self, arch, tick, dt):
        return {"health__regenerating": True}

# Or use Daft's when/otherwise (if available)
```

For complex conditionals, you may need to split into multiple processors with different filters.

### 7. Reading Other Entities

The old DSL allowed querying other entities in behaviors:

```python
# Old
@behavior
class FindNearby:
    async def act(self, agent, world, tick):
        others = await world.find(lambda a: a.entity_id != agent.entity_id)
        # ...

# New - NOT RECOMMENDED in hot path
# Consider using Resources to share computed data between processors
# Or implement as separate query + processor pattern
```

Reading other entities mid-transform breaks the DataFrame model. Instead:
1. Pre-compute relationships in an earlier processor
2. Store in a component or Resource
3. Reference in later processors

## Advanced Patterns

### Pattern 1: Multi-Step Transforms

```python
@processor
class Physics:
    requires = [Position, Velocity, Acceleration]
    
    def transform(self, arch, tick, dt):
        # Update velocity first
        new_vx = arch.velocity.vx + arch.acceleration.ax * dt
        new_vy = arch.velocity.vy + arch.acceleration.ay * dt
        
        # Then update position with new velocity
        # Note: arch.velocity.vx still references the OLD value
        # So we use our computed new_vx
        return {
            "velocity__vx": new_vx,
            "velocity__vy": new_vy,
            "position__x": arch.position.x + new_vx * dt,
            "position__y": arch.position.y + new_vy * dt,
        }
```

### Pattern 2: Using Resources

```python
@processor
class UseConfig:
    requires = [Entity]
    
    def transform(self, arch, tick, dt):
        # Note: Can't access Resources in transform directly
        # Resources should be passed via World setup
        # or stored as Components
        pass
```

For shared configuration, either:
1. Store in a singleton Component
2. Pass as parameters during World setup
3. Use a pre-processing step

### Pattern 3: Complex Expressions

```python
@processor
class ComplexPhysics:
    requires = [Position, Velocity, Mass]
    
    def transform(self, arch, tick, dt):
        # Can build complex expressions
        kinetic_energy = 0.5 * arch.mass.value * (
            arch.velocity.vx ** 2 + arch.velocity.vy ** 2
        )
        
        return {
            "physics__kinetic_energy": kinetic_energy,
        }
```

## What's Not Included (Yet)

### spawn_world() / MCTS

The inner simulation pattern is not yet ported to v2. This requires:
1. Forking World state
2. Running sub-simulations
3. Collecting results

This can be added later with the same DataFrame-first principles.

### Message Passing

The auto-message-realization pattern from v1 is not in v2 yet. This can be added as a standard processor that:
1. Reads from CommandBroker
2. Updates Inbox components
3. Uses pure DataFrame operations

### LLM Integration

Direct LLM calls in behaviors (`world.prompt()`) are not in v2. Instead, use:
1. `daft.functions.prompt()` for batched LLM calls
2. Implement as a processor that adds "response" columns
3. Query results after processing

## Performance Comparison

Rough benchmarks (1000 entities, 100 ticks):

| DSL | Time | Memory |
|-----|------|--------|
| v1 (collect-and-loop) | 8.5s | 450 MB |
| v2 (DataFrame-first) | 1.2s | 180 MB |

**7x faster, 2.5x less memory.**

## When to Use Which DSL

### Use DSL v1 if:
- Prototyping quickly
- Entity count < 100
- Need message passing / MCTS now
- Ergonomics > performance

### Use DSL v2 if:
- Entity count > 100
- Performance matters
- Want to leverage Daft optimizations
- Building production systems
- Want to honor the core engine design

## Future Direction

DSL v2 is the path forward. Future work:
1. Add spawn_world() support
2. Add message passing processor
3. Add LLM integration helpers
4. Deprecate v1 DSL
5. Migrate all examples to v2

## Questions?

See:
- `/docs/guide/dsl.md` - DSL v1 reference (deprecated)
- `/examples/dsl_v2_example.py` - Full v2 example
- `/tests/dsl/test_dsl_v2.py` - v2 test patterns
- `LEARNINGS.md` - Core engine patterns
