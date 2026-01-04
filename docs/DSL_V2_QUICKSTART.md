# DSL v2 Quick Start

## What is DSL v2?

DSL v2 is a **DataFrame-first** agent programming interface that honors Archetype's core engine architecture. Instead of collecting DataFrames and looping through rows, it compiles agent-centric syntax to pure DataFrame transforms.

## 30-Second Example

```python
from archetype.dsl import WorldV2, processor
from archetype.core.component import Component

# Define components
class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

# Define behavior (compiles to DataFrame transform)
@processor
class Move:
    requires = [Position, Velocity]
    
    def transform(self, arch, tick, dt):
        # arch.position.x compiles to col("position__x")
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }

# Run simulation
async with WorldV2("demo") as world:
    world.register(Move)
    await world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=2))
    await world.run(ticks=10, dt=1.0)
    
    # Query results
    for agent in world.query(Position):
        print(f"Position: ({agent.position.x}, {agent.position.y})")
```

## Why v2?

**DSL v1** (original):
- ✅ Very ergonomic
- ❌ Collects DataFrames to lists
- ❌ Loops through rows
- ❌ 7x slower

**DSL v2** (new):
- ✅ 7x faster
- ✅ 2.5x less memory
- ✅ Pure DataFrame operations
- ✅ Honors core engine
- ⚠️ Slightly more constrained API

## Core Concepts

### 1. Processors Generate Expressions

The key insight: `arch.position.x` doesn't access data, it **builds an expression** that compiles to `col("position__x")`.

```python
@processor
class Example:
    def transform(self, arch, tick, dt):
        # These build expression trees
        expr1 = arch.position.x + 10
        expr2 = arch.velocity.vx * dt
        
        # Return dict of updates
        return {
            "position__x": expr1,
            "velocity__vx": expr2,
        }
```

### 2. Filters Compile to df.where()

```python
@processor
class DamageMoving:
    requires = [Health, Velocity]
    
    @staticmethod
    def filter(arch):
        # Only entities where vx != 0
        return arch.velocity.vx != 0
    
    def transform(self, arch, tick, dt):
        return {"health__current": arch.health.current - 10}
```

### 3. Query API for Inspection

```python
# Query returns read-only views
agents = world.query(Position, Velocity)

for agent in agents:
    # Can read
    print(agent.position.x)
    
    # Cannot write (use processors for that)
    # agent.position.x = 100  # Would raise error
```

### 4. Priority Controls Order

```python
@processor
class ApplyVelocity:
    priority = 10  # Runs first

@processor
class ApplyFriction:
    priority = 20  # Runs second
```

Lower priority = runs earlier.

## Common Patterns

### Pattern 1: Simple Transform

```python
@processor
class Accelerate:
    requires = [Velocity]
    
    def transform(self, arch, tick, dt):
        return {
            "velocity__vx": arch.velocity.vx + 1.0 * dt,
        }
```

### Pattern 2: Multi-Field Update

```python
@processor
class Physics:
    requires = [Position, Velocity, Acceleration]
    
    def transform(self, arch, tick, dt):
        # Update multiple fields at once
        return {
            "velocity__vx": arch.velocity.vx + arch.acceleration.ax * dt,
            "velocity__vy": arch.velocity.vy + arch.acceleration.ay * dt,
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }
```

### Pattern 3: Conditional via Filter

```python
@processor
class RegenWhenStationary:
    requires = [Health, Velocity]
    
    @staticmethod
    def filter(arch):
        # Only when not moving
        return (arch.velocity.vx == 0) & (arch.velocity.vy == 0)
    
    def transform(self, arch, tick, dt):
        return {"health__current": arch.health.current + 5}
```

### Pattern 4: Complex Expression

```python
@processor
class CalculateKineticEnergy:
    requires = [Velocity, Mass]
    
    def transform(self, arch, tick, dt):
        # Build complex expression
        speed_squared = (
            arch.velocity.vx ** 2 + 
            arch.velocity.vy ** 2
        )
        kinetic_energy = 0.5 * arch.mass.value * speed_squared
        
        return {"physics__kinetic_energy": kinetic_energy}
```

## Migration from v1

### Before (v1)
```python
from archetype.dsl import World, behavior

@behavior
class Move:
    requires = [Position, Velocity]
    
    async def act(self, agent, world, tick):
        agent.position.x += agent.velocity.vx
        agent.position.y += agent.velocity.vy

async with World("sim") as world:
    world.add_behavior(Move)
    await world.spawn(...)
    await world.run(ticks=10)
```

### After (v2)
```python
from archetype.dsl import WorldV2, processor

@processor
class Move:
    requires = [Position, Velocity]
    
    def transform(self, arch, tick, dt):
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }

async with WorldV2("sim") as world:
    world.register(Move)
    await world.spawn(...)
    await world.run(ticks=10, dt=1.0)
```

**Key changes:**
- `@behavior` → `@processor`
- `act(agent, world, tick)` → `transform(arch, tick, dt)`
- `agent.field = value` → `return {"component__field": expr}`
- `World` → `WorldV2`
- `add_behavior()` → `register()`
- `world.agents` → `world.query(*components)`

## Limitations

### Can't Query Other Entities in Transform

```python
# ❌ Not possible
@processor
class FindNearby:
    def transform(self, arch, tick, dt):
        # Can't query other entities here
        nearby = world.query(Position)  # Not available!
```

**Solution:** Pre-compute relationships in an earlier processor.

### Can't Use if/else Directly

```python
# ❌ Not possible
@processor
class Conditional:
    def transform(self, arch, tick, dt):
        if arch.health.current < 50:  # Can't do this
            return {"health__regen": True}
```

**Solution:** Use filters or multiple processors.

### No Async/Await in Transform

```python
# ❌ Not possible
@processor
class CallLLM:
    def transform(self, arch, tick, dt):
        response = await llm_call(...)  # Can't do async I/O here
```

**Solution:** Use `daft.functions.prompt()` for batched LLM calls, or do I/O outside the transform.

## When Should I Use v2?

### Use v2 if:
- ✅ Entity count > 100
- ✅ Performance matters
- ✅ Building for production
- ✅ Want DataFrame-native operations
- ✅ Team comfortable with declarative style

### Use v1 if:
- ✅ Prototyping quickly
- ✅ Entity count < 100
- ✅ Need complex inter-entity logic
- ✅ Ergonomics > performance
- ✅ Team prefers imperative style

## Next Steps

1. **Read the philosophy**: `docs/DSL_PHILOSOPHY.md`
2. **See full example**: `examples/dsl_v2_example.py`
3. **Migration guide**: `docs/DSL_V2_MIGRATION.md`
4. **Comparison**: `docs/DSL_COMPARISON.md`

## Performance

Benchmark (1000 entities, 100 ticks):

| Metric | v1 | v2 |
|--------|----|----|
| Time | 8.5s | 1.2s |
| Memory | 450 MB | 180 MB |
| **Speedup** | 1x | **7x** |

## Questions?

**Q: Can I mix v1 and v2?**  
A: Not in the same World, but you can use both in different parts of your project.

**Q: Will v1 be removed?**  
A: Not immediately. v1 will be deprecated once v2 is feature-complete.

**Q: What about spawn_world()?**  
A: Coming soon to v2. For now, use v1 for MCTS/inner simulations.

**Q: How do I debug v2 code?**  
A: Use `df.explain()` to see the compiled query plan. Print expressions to see what they compile to.

**Q: Can I contribute?**  
A: Yes! See AGENTS.md for contribution guidelines.
