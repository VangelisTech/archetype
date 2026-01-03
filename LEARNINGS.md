# Learnings: Daft 0.7.x & Archetype Patterns

*Documented from sessions with Everett, December 2024 – January 2025*

> **For AI Agents:** This document contains hard-won architectural knowledge. Read it before proposing changes to core patterns.

---

## The Meta-Goal (Jan 2025)

Archetype has a recursive purpose:

> **Use Archetype to build the harness that evaluates and improves Archetype.**

The `spawn_world()` primitive enables:
1. **Benchmarking** — Thousands of simulation scenarios in parallel
2. **Evaluation** — Compare behavioral outcomes across configurations
3. **Self-improvement** — Agents proposing and testing architectural changes

The `core/` module is human-curated, but everything else is fair game for agent contribution. The plan:
- **Now:** Python core, optimized for iteration
- **Next:** Rust rewrite for production performance
- **Future:** Agents continuously improving the system

---

## Core Insight: DataFrames ARE the Batch

The DataFrame is already columnar. Operations on columns are inherently batched/vectorized. Don't overthink it.

```python
# Just use expressions when possible
df = df.with_column("score", col("reward") * 0.5 + col("bonus"))
df = df.where(col("score") > 0.5)
df = df.groupby("env_id").agg(col("reward").mean())
```

UDFs are the **escape hatch** for when you can't express logic as DataFrame operations (e.g., calling an LLM, loading a custom model).

---

## Daft 0.7.x UDF Patterns

### 1. `@daft.func` — Stateless, Row-by-Row

For simple element-wise transforms.

```python
@daft.func(return_dtype=DataType.float64())
def double(x: float) -> float:
    return x * 2.0

df = df.with_column("doubled", double(col("value")))
```

### 2. `@daft.func.batch` — Stateless, Series→Series

For batch operations where you need the full Series.

```python
from daft import Series

@daft.func.batch(return_dtype=DataType.string())
def process_batch(values: Series) -> Series:
    results = [transform(v) for v in values.to_pylist()]
    return Series.from_pylist(results)
```

### 3. `@daft.cls()` — Stateful Class

For expensive initialization that should happen **once per worker** (models, connections).

```python
@daft.cls()
class ModelInference:
    def __init__(self):
        # Runs ONCE per worker
        self.model = load_expensive_model()
    
    # Default: row-by-row (like @daft.func)
    def predict(self, x: str) -> str:
        return self.model(x)
    
    # Explicit batch (like @daft.func.batch)
    @daft.method.batch
    def predict_batch(self, xs: Series) -> Series:
        return Series.from_pylist(self.model.batch_predict(xs.to_pylist()))

inference = ModelInference()
df = df.with_column("pred", inference.predict(col("input")))
```

**Key insight**: All public methods on `@daft.cls()` are automatically `daft.method` (row-by-row). Use `@daft.method.batch` explicitly when you want Series→Series.

---

## When to Use Batch

**Only use `@daft.func.batch` or `@daft.method.batch` if:**

1. The underlying operation actually supports batching
2. You need access to the full Series (not just individual values)

| Library | Supports Batch? | Pattern |
|---------|-----------------|---------|
| vLLM | ✓ Yes | `@daft.method.batch` — batch all prompts |
| Transformers | ✓ Yes | `@daft.method.batch` — batch inputs |
| OpenAI API | ✗ No (rate limited) | Default row-by-row |
| Ollama | ✗ No | Default row-by-row |
| PyTorch inference | ✓ Yes | `@daft.method.batch` |
| Simple transforms | — | Just use DataFrame expressions |

If the model doesn't batch, using `@daft.method.batch` and looping internally is just extra complexity for no gain.

---

## Struct Field Access (0.7.x)

**Old way (deprecated):**
```python
col("result").struct.get("field")  # ✗ No longer works
```

**New way:**
```python
col("result")["field"]  # ✓ Use indexing
```

---

## Archetype ECS Pattern

### Components = Data Bags

```python
from archetype import Component

class EnvironmentComponent(Component):
    gravity: float = 9.8
    friction: float = 0.2
```

Components are prefixed in the DataFrame:
- `EnvironmentComponent.gravity` → `environmentcomponent__gravity`

### Processors = DataFrame Transforms

```python
from archetype import Processor

class PhysicsProcessor(Processor):
    components = (EnvironmentComponent,)  # Declares dependencies
    priority = 10  # Lower = runs first
    
    def process(self, df: daft.DataFrame, **kwargs) -> daft.DataFrame:
        # Transform and return
        return df.with_column("result", col("environmentcomponent__gravity") * 2)
```

### The Loop

```
Entities (DataFrame) → Processor 1 → Processor 2 → ... → Store (LanceDB)
         ↑                                                      |
         └──────────────────────────────────────────────────────┘
                              (next tick)
```

---

## Arrow Serialization

Everything must be Arrow-serializable for LanceDB storage:

| Python Type | Arrow-Compatible? | Solution |
|-------------|-------------------|----------|
| `str`, `int`, `float`, `bool` | ✓ | Direct |
| `list[str]` | ✓ | Direct |
| `dict` | ✗ | JSON-encode to `str` |
| `igraph.Graph` | ✗ | Store as edge list JSON |
| `torch.Tensor` | ✗ | Save to file, store path |
| Custom objects | ✗ | Serialize to JSON/bytes |

---

## Common Mistakes I Made

1. **Used `@daft.udf`** — Deprecated in 0.7.0, removed in 0.8.0. Use `@daft.func.batch` instead. ✅ *All UDFs migrated Jan 2025*

2. **Used `.struct.get()`** — Use `[]` indexing instead

3. **Thought `@daft.cls()` required `batch`** — No, default methods are row-by-row

4. **Over-engineered UDFs** — Many transforms are just DataFrame expressions

5. **Used `batch` without actual batching** — If you loop inside a batch UDF, you're not batching

---

## File Handling: `daft.File`

For weights-as-data pattern:

```python
@daft.cls()
class Trainer:
    @daft.method.batch
    def train(self, data: Series, weights_file: Series) -> Series:
        # weights_file contains daft.File objects
        for wf in weights_file.to_pylist():
            with wf.to_tempfile() as tmp:
                state = torch.load(tmp.name)
        # ...
```

---

---

## Two "Resources" Concepts (Jan 2025)

There are two distinct concepts, unfortunately both historically called "resources":

| Concept | Module | Purpose |
|---------|--------|---------|
| **Resources** | `core/resources.py` | Type-safe DI container for processors |
| **StorageBackendManager** | `app/storage_manager.py` | Pools storage backends (Lance/Iceberg) |

**StorageBackendManager** (formerly `StorageResourceManager`) is infrastructure plumbing—it creates and pools `(Store, Querier, Updater)` triplets using a multiton pattern. The orchestrator owns one.

**Resources** is the runtime DI container that passes services to processors. Each world has one.

```python
# Infrastructure (app layer)
orchestrator = WorldOrchestrator()  # owns a StorageBackendManager internally

# Runtime DI (core layer, per-world)
world.resources.insert(CommandBroker())
world.resources.insert(MemoryBank.default())

# In processor
broker = resources.require(CommandBroker)
```

---

## Resources: Type-Safe DI (Jan 2025)

Processors often need shared state (configs, brokers, services). The `Resources` container provides type-safe dependency injection:

```python
from archetype.core.resources import Resources
from archetype.app.broker import CommandBroker

# Register resources on the world
world.resources.insert(CommandBroker())
world.resources.insert(SimConfig(tick_duration=1.0))

# Access in processors
class MyProcessor(AsyncProcessor):
    async def process(self, df, resources: Resources = None, **kwargs):
        broker = resources.require(CommandBroker)  # Raises if missing
        config = resources.get(SimConfig)  # Returns None if missing
        # ...
```

**API:**
- `insert(obj)` — Store by type
- `get(Type)` → `T | None`
- `require(Type)` → `T` (raises `KeyError` if missing)
- `remove(Type)` → `T | None`
- `Type in resources` — Check existence

---

## Hooks: Lifecycle Callbacks (Jan 2025)

For observability and debugging without coupling to processor logic:

```python
async def on_pre_tick(world, tick, **kwargs):
    print(f"Starting tick {tick}")

async def on_post_tick(world, tick, results, **kwargs):
    print(f"Finished tick {tick}, processed {len(results)} archetypes")

world.add_hook("pre_tick", on_pre_tick)
world.add_hook("post_tick", on_post_tick)
```

**Events:**
- `pre_tick` — Before any processing (tick=N)
- `post_tick` — After all processing (tick=N+1, results=list of DataFrames)

Hooks are async, errors are logged but don't crash the world.

---

## Agent Communication: MESSAGE Command (Jan 2025)

Agent-to-agent messaging via the CommandBroker:

```python
from archetype.app.models import Command, CommandType

# In a processor, send a message
cmd = Command(
    type=CommandType.MESSAGE,
    tick=tick,
    payload={
        "sender_id": entity_id,
        "receiver_id": target_id,
        "content": "Hello!",
    },
)
await broker.enqueue(world_id, cmd)
```

**Key insight:** Messages enqueued at tick N are realized at tick N+1. This maintains tick-boundary consistency.

**Components pattern:**
```python
class Inbox(Component):
    messages: list[str] = []  # JSON-encoded, not list[dict] (LanceDB limitation)

class Outbox(Component):
    pending: list[str] = []
```

---

## Tick Lifecycle

```
Tick N:
  1. pre_tick hook fires (tick=N)
  2. For each archetype (parallel):
     a. Query previous state (tick N-1)
     b. Materialize mutations (spawn/despawn)
     c. Execute processors (priority order, lower first)
     d. Persist to store (tick=N)
  3. Update _live snapshots
  4. Increment tick → N+1
  5. post_tick hook fires (tick=N+1)
```

---

## Debug Logging

Enable with `run_config.debug = True`:

```
[archetype] {"event": "tick_start", "world_id": "...", "tick": 0}
[archetype] processor_start: PhysicsProcessor (priority=10)
[archetype] processor_end: PhysicsProcessor (rows_out=100)
[broker] enqueue: world=demo, type=message, pending=6
[broker] dequeue: world=demo, returned=6, types={'message': 6}
```

---

## Daft Lazy Evaluation Gotcha (Jan 2025)

Daft is lazily evaluated. Intermediate `.select(...).collect()` calls break the DAG and may cause upstream operations (like `prompt()`) to execute on a **separate plan** that discards downstream work.

```python
# ❌ WRONG: This breaks the DAG
df = df.with_column("response", prompt(col("input"), ...))
debug = df.select("response").limit(1).collect()  # Materializes SEPARATE plan!
df = df.with_column("next", col("response") + "...")  # response may be empty!

# ✅ RIGHT: Keep all columns in plan until final collect
df = df.with_column("response", prompt(col("input"), ...))
df = df.with_column("next", col("response") + "...")
result = df.collect()  # Single materialization
```

**Key insight:** When debugging Daft pipelines, use `df.explain()` to inspect the DAG rather than intermediate collects.

---

## Row-wise `@daft.func` vs `@daft.func.batch` (Jan 2025)

For simple row transforms, prefer `@daft.func` over `@daft.func.batch`:

```python
# ✅ Clean: Row-wise with automatic type inference
@daft.func
def update_history(history_json: str, agent: str, response: str) -> str:
    history = json.loads(history_json) if history_json else []
    history.append({"agent": agent, "statement": response})
    return json.dumps(history)

# ❌ Unnecessary: Batch when you're just looping anyway
@daft.func.batch(return_dtype=DataType.string())
def update_history_batch(history: Series, agent: Series, response: Series) -> Series:
    results = []
    for h, a, r in zip(history.to_pylist(), agent.to_pylist(), response.to_pylist()):
        hist = json.loads(h) if h else []
        hist.append({"agent": a, "statement": r})
        results.append(json.dumps(hist))
    return Series.from_pylist(results)
```

**Rule of thumb:** Use `.batch` only when the underlying operation actually benefits from batching (vectorized NumPy, batch inference, etc.).

---

## Tick-Gated Processing (Jan 2025)

For expensive operations (LLM calls, inner simulations), gate on tick to avoid unnecessary work:

```python
class SuperjectiveInnerWorldProcessor(AsyncProcessor):
    async def process(self, df, tick: int = 0, **kwargs):
        if tick != 2:  # Only run on final round
            return df
        
        # Expensive inner simulation only happens once
        df = df.with_column("scenarios", prompt(...))
        return df
```

This pattern is especially useful for:
- Inner world simulations (superjective reasoning)
- Checkpoint/save operations
- Aggregation that only makes sense at simulation end

---

## JSON-Encoding Complex Types (Jan 2025)

LanceDB/Arrow can't store `list[dict]` directly. JSON-encode to `str`:

```python
class DebateState(Component):
    history_json: str = "[]"  # ✅ JSON string, not list[dict]

# Writing
history.append({"agent": name, "statement": text})
df = df.with_column("history_json", daft.lit(json.dumps(history)))

# Reading
history = json.loads(row["debatestate__history_json"])
for entry in history:
    print(f"{entry['agent']}: {entry['statement']}")
```

**Also applies to:** nested dicts, custom objects, anything non-primitive.

---

## Agent DSL: Ergonomic Layer (Jan 2025)

The `archetype.dsl` module provides agent-centric ergonomics on top of the DataFrame engine:

```python
from archetype.dsl import World, behavior, spawn_world, Inbox

@behavior
class Debater:
    requires = [Perspective, DebateState, Inbox]
    priority = 10
    runs_on = "every_tick"  # or "final_tick", "first_tick", tick number
    filter = lambda agent: agent.perspective.type == "special"  # optional
    
    async def act(self, agent, world, tick):
        # Agent-centric access (not col("perspective__name"))
        name = agent.perspective.name
        
        # Direct mutation (auto-serializes lists/dicts)
        agent.debate_state.history.append({"round": tick})
        
        # LLM call
        response = await world.prompt("Your prompt", model="gpt-4o-mini")
        
        # Broadcast to all other agents
        await world.broadcast(response, sender=agent, exclude=[agent])

async with World("my_sim", storage="./data") as world:
    world.add_behavior(Debater)
    await world.spawn(Perspective(...), DebateState(), Inbox())
    await world.run(ticks=3)
    
    for agent in world.agents:
        print(agent.perspective.name)
```

### spawn_world() for Inner Simulations / MCTS

```python
async with spawn_world("scenario_1", parent=world, fork_state=True) as inner:
    inner.add_behavior(ScenarioBehavior)
    await inner.spawn(...)
    await inner.run(ticks=5)
    
    # Analyze results
    score = calculate_consensus(inner.agents)
```

Use cases:
- **MCTS**: Explore action sequences
- **Counterfactual reasoning**: "What if agent X said Y?"
- **Mental simulation**: Agent imagines consequences

---

## Summary

1. **DataFrames are batched by nature** — use expressions first
2. **`@daft.func`** for simple row-wise transforms (auto type inference)
3. **`@daft.func.batch`** only when operation actually benefits from batching
4. **`@daft.cls()`** for stateful (models), methods are row-by-row by default
5. **`@daft.method.batch`** only when the model actually supports batching
6. **`col("x")["field"]`** for struct access
7. **JSON-encode** complex types (`list[dict]`, nested objects) for Arrow compatibility
8. **Resources** for type-safe DI in processors
9. **Hooks** for observability without processor coupling
10. **MESSAGE commands** for agent communication via broker
11. **Tick-gating** for expensive operations (LLM calls, inner worlds)
12. **Keep columns in DAG** — avoid intermediate `.collect()` breaking lazy evaluation
13. **Agent DSL** for ergonomic agent-centric code that compiles to DataFrames
14. **spawn_world()** for inner simulations, MCTS, counterfactual reasoning
