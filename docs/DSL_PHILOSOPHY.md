# DSL Design Philosophy

## The Core Problem

Archetype's core engine is built on a simple, powerful idea:

> **Entities are rows in DataFrames. Behaviors are DataFrame transforms.**

The DataFrame is the unit of computation. When you have 1000 entities with Position and Velocity components, they exist as a single DataFrame with 1000 rows:

```
┌───────────┬────────────┬────────────┬────────────┬────────────┐
│ entity_id │ position_x │ position_y │ velocity_vx│ velocity_vy│
├───────────┼────────────┼────────────┼────────────┼────────────┤
│ 1         │ 0.0        │ 0.0        │ 1.0        │ 2.0        │
│ 2         │ 10.0       │ 20.0       │ -1.0       │ -2.0       │
│ ...       │ ...        │ ...        │ ...        │ ...        │
└───────────┴────────────┴────────────┴────────────┴────────────┘
```

A behavior that updates position should be a **single DataFrame operation**, not 1000 Python function calls:

```python
# ✅ GOOD: Single DataFrame operation
df = df.with_columns({
    "position_x": col("position_x") + col("velocity_vx"),
    "position_y": col("position_y") + col("velocity_vy"),
})

# ❌ BAD: 1000 Python function calls
for row in df.to_pylist():
    row["position_x"] += row["velocity_vx"]
    row["position_y"] += row["velocity_vy"]
```

## The Ergonomics Trap

But DataFrame operations aren't ergonomic. Agents want to think in terms of "my position", not "the position column":

```python
# What agents want to write
agent.position.x += agent.velocity.vx

# What the engine needs
df = df.with_column("position_x", col("position_x") + col("velocity_vx"))
```

The challenge: **How do we give agents the ergonomics they want while preserving the performance the engine needs?**

## DSL v1: The Wrong Compromise

The original DSL tried to solve this by making the ergonomic API *real*:

```python
@behavior
class Move:
    async def act(self, agent, world, tick):
        agent.position.x += agent.velocity.vx
```

Implementation:
1. Collect DataFrame to Python list
2. For each row, create an AgentProxy
3. Run act() on each proxy
4. Track mutations
5. Apply mutations back to DataFrame

This works, but **defeats the entire point of DataFrames**. You're doing 1000 Python function calls in a loop. The DataFrame is just a storage format, not a unit of computation.

## DSL v2: Compile to DataFrames

DSL v2 takes a different approach: **Make the ergonomic API compile to DataFrame operations.**

```python
@processor
class Move:
    def transform(self, arch, tick, dt):
        return {
            "position_x": arch.position.x + arch.velocity.vx * dt
        }
```

Key insight: `arch.position.x` is not a value, it's a **Field** that compiles to `col("position_x")`.

When you write:
```python
arch.position.x + arch.velocity.vx * dt
```

You're building an expression tree:
```
BinaryOp("+",
  BinaryOp("*",
    Field("velocity_vx"),
    Literal(dt)
  ),
  Field("position_x")
)
```

Which compiles to:
```python
col("position_x") + col("velocity_vx") * dt
```

The entire transform happens **without collecting the DataFrame**. It's a pure, declarative specification that Daft can optimize.

## The Trade-offs

### What You Gain

1. **Performance**: 7x faster, 2.5x less memory
2. **Scalability**: Works with millions of entities
3. **Optimization**: Daft can optimize the entire pipeline
4. **Simplicity**: Processor implementation is ~200 lines vs ~500 lines
5. **Correctness**: No mutation tracking bugs

### What You Lose

1. **Can't read other entities**: The old DSL let you query world state mid-behavior. This breaks the DataFrame model. Solution: Pre-compute relationships.

2. **Can't do arbitrary Python**: The old DSL let you call LLMs, update external state, etc. This defeats batching. Solution: Separate query from transform.

3. **Conditionals are awkward**: `if agent.health < 50: agent.regen = True` becomes a filter or expression. Solution: Use multiple processors.

### What's Preserved

1. **Simple spawn/run API**: Still `await world.spawn(...)` and `await world.run(ticks=10)`
2. **Component-based architecture**: Still define components with fields
3. **Priority ordering**: Still control processor execution order
4. **Query API**: Still get agent views for inspection

## The Philosophy

DSL v2 embodies a specific philosophy:

> **The DSL is a compiler, not a runtime.**

Your behavior code doesn't *run* on entities. It *compiles* to DataFrame operations that the engine runs.

This is similar to:
- SQL: You write declarative queries, the engine optimizes execution
- React: You write declarative UI, the framework optimizes rendering
- Daft: You write declarative transforms, the engine optimizes computation

The DSL is your *interface* to declare what should happen. The engine is responsible for making it happen efficiently.

## When to Use Which

### Use Core Processors When:
- You need maximum performance
- You're comfortable with DataFrame operations
- You need full control

```python
class CoreProcessor(AsyncProcessor):
    async def process(self, df, **kwargs):
        return df.with_column(...)
```

### Use DSL v2 When:
- You want agent-centric ergonomics
- You want compile-time safety
- You want cleaner behavior code

```python
@processor
class DSLProcessor:
    def transform(self, arch, tick, dt):
        return {"field": arch.component.field + 1}
```

### Use DSL v1 When:
- Prototyping quickly
- Entity count < 100
- Ergonomics matter more than performance

```python
@behavior
class V1Behavior:
    async def act(self, agent, world, tick):
        agent.component.field += 1
```

## Future Direction

The goal is to make DSL v2 the recommended path for all new code:

1. **Add missing features**: spawn_world, message passing, LLM helpers
2. **Improve ergonomics**: Better error messages, type hints, IDE support
3. **Optimize compilation**: Cache Field expressions, validate at decoration time
4. **Eventually deprecate v1**: Once v2 is feature-complete

The vision: **Write agent behaviors naturally, get DataFrame performance automatically.**

## Appendix: Implementation Notes

### Why Field and not just col()?

```python
# You could write this directly
def transform(self, arch, tick, dt):
    return {
        "position_x": col("position_x") + col("velocity_vx") * dt
    }

# But this is more ergonomic
def transform(self, arch, tick, dt):
    return {
        "position_x": arch.position.x + arch.velocity.vx * dt
    }
```

The Field abstraction:
1. Provides component namespacing (arch.position.x)
2. Enables IDE autocomplete
3. Catches typos at compile time (eventually)
4. Makes code self-documenting

### Why separate Query API?

The old DSL combined query and transform:

```python
@behavior
class OldBehavior:
    async def act(self, agent, world, tick):
        # Query
        others = await world.find(Nearby)
        
        # Transform
        agent.state.neighbors = len(others)
```

This forces the entire transform into Python-land. The new DSL separates concerns:

```python
# Query (outside tick loop)
agents = world.query(State)
for agent in agents:
    print(agent.state.neighbors)

# Transform (inside tick loop, DataFrame)
@processor
class CountNeighbors:
    def transform(self, arch, tick, dt):
        # Pre-computed neighbor data from earlier processor
        return {"state_neighbors": ...}
```

Query is for inspection. Transform is for simulation. Keep them separate.

### Why no async transform?

Async is for I/O. DataFrame operations are CPU-bound and batched. Making transform async would:
1. Complicate the API
2. Suggest you can do I/O (you shouldn't)
3. Not provide any benefit

If you need async (LLM calls, etc.), use daft.functions.prompt which batches LLM calls across the DataFrame.

### Why no AgentProxy in transform?

AgentProxy provides mutation tracking:

```python
agent.field = value  # Records mutation
agent.get_mutations()  # Returns dict
```

But in DSL v2, transform returns mutations directly:

```python
return {"field": new_value}
```

AgentProxy is only needed for v1's imperative style. v2 is declarative.
