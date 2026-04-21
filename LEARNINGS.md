# Learnings: Daft 0.7.x & Archetype Patterns

Architectural notes and Daft patterns accumulated while building Archetype. Read before proposing changes to processor or UDF code. For normative contracts, see `docs/guide/specification.md`.

---

## Daft DataFrames are lazy, columnar, and vectorized

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

## Expression namespaces were deprecated v0.7.x

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

```text
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

1. **Used `@daft.udf`** — Deprecated in 0.7.0, removed in 0.8.0. Use `@daft.func.batch` instead. ✅ *All UDFs migrated Jan 2026*

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

## Two "Resources" Concepts (Jan 2026)

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

## Resources: Type-Safe DI (Jan 2026)

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

## Hooks: Lifecycle Callbacks (Jan 2026)

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

## The Data-Centric Principle (Mar 2026)

Archetype is **data-centric**. The DataFrame is the source of truth. Processors are pure functions `DataFrame → DataFrame`. So long as the data looks right at the end of a tick, nothing else matters — not how the LLM was called, not whether it was async or sync, not how long it took.

This means:

1. **Never break the lazy DAG unless you must.** `.collect().to_pylist()` pulls data out of Daft's execution engine. You lose lazy evaluation, automatic parallelism, and plan optimization. The default instinct — collect everything, loop in Python, push back — is wrong for this codebase.

2. **Use `@daft.func` (row-wise) by default.** If your "batch" UDF is just a for-loop over `Series.to_pylist()`, it should be `@daft.func`. Daft supports async `@daft.func` natively.

3. **Use `@daft.cls()` for non-serializable state.** API clients, model weights, DB connections — anything that can't be pickled goes in `@daft.cls().__init__()`. Methods are row-wise. Daft recreates the class per worker.

4. **Only `.collect()` for cross-row context.** Message routing (sender → receiver) requires global visibility. Name lookups across entities require global visibility. These are justified collects. Document them inline.

5. **Don't import actor patterns.** No `asyncio.gather` over collected rows. No building dicts from pylist loops and feeding them back through batch UDFs. If you find yourself doing this, you're fighting the execution model.

```python
# ❌ WRONG: Imperative actor pattern in a data-centric system
rows = df.select("entity_id", "agent__name", "inbox__messages").collect().to_pylist()
results = await asyncio.gather(*[call_llm(row) for row in rows])
response_by_id = {r["id"]: r["text"] for r in results}

@daft.func.batch(return_dtype=...)
def write_back(entity_ids: Series) -> list:
    return [response_by_id.get(eid, "") for eid in entity_ids.to_pylist()]

# ✅ RIGHT: Row-wise, Daft manages execution
@daft.func
async def think_and_respond(name: str, role: str, inbox: list[str]) -> list[str]:
    response = await client.messages.create(...)  # Daft handles concurrency
    return [json.dumps({"receiver_id": target, "content": response.content[0].text})]

df = df.with_column("outbox__messages", think_and_respond(col("agent__name"), ...))
```

**The serialization constraint:** `@daft.func` closures must be picklable. API clients, mocks, and anything with network state are NOT picklable. Use `@daft.cls()` for these — the client lives in `__init__`, reconstructed per worker, never serialized.

```python
# ✅ Production pattern: @daft.cls() for non-serializable clients
@daft.cls()
class ClaudeAgent:
    def __init__(self):
        import anthropic
        self.client = anthropic.AsyncAnthropic()

    async def respond(self, name: str, role: str, inbox: list[str]) -> list[str]:
        response = await self.client.messages.create(model="claude-sonnet-4-6", ...)
        return [json.dumps({...})]

agent = ClaudeAgent()
df = df.with_column("outbox__messages", agent.respond(col("agent__name"), ...))
```

---

## Agent Communication: Messaging Pipeline (Mar 2026)

Agent-to-agent messaging is processor-driven, not broker-driven.

**The broker is governance only** — RBAC, quotas, command queuing. It does NOT own message delivery or conversation structure.

**Components:**

```python
class Outbox(Component):
    messages: list[str] = []  # JSON-encoded: {"receiver_id": int, "channel": str, "content": str}

class Inbox(Component):
    messages: list[str] = []  # JSON-encoded: {"sender_id": int, "channel": str, "content": str, "tick": int}
```

**Delivery pipeline:**

```text
Agent processor writes to Outbox (priority 10+)
        ↓
MessageDeliveryProcessor (priority -100, runs first next tick)
  ├── reads Outbox via DataFrame
  ├── validates: receiver exists? not self-messaging?
  ├── routes to recipient Inbox via @daft.func
  ├── updates ChatGraph Resource (if present)
  └── rejected → sender's DeliveryReceipt
        ↓
Downstream processors read Inbox for LLM context
```

**Key insight:** Messages written to Outbox at tick N are delivered to Inbox at tick N+1. This enforces causal ordering — no agent can read a message from the same tick it was sent.

**ChatGraph** is a Resource (not entity data). It tracks conversation structure as a DAG per (world, channel):

```python
registry = resources.require(ChatGraphRegistry)
graph = registry.channel(world_id, "strategy")
context = graph.active_path()  # root → cursor, for LLM context windows
```

**Channels** are first-class routing keys. Each channel gets its own independent conversation graph. Default is `"general"`.

**`append_history` toggle:** a command with `payload={"append_history": False}` (or equivalent) makes a message ephemeral — delivered but not recorded in broker history or ChatGraph. Use for heartbeats, probes, system messages.

---

## Tick Lifecycle

```text
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

```text
[archetype] {"event": "tick_start", "world_id": "...", "tick": 0}
[archetype] processor_start: PhysicsProcessor (priority=10)
[archetype] processor_end: PhysicsProcessor (rows_out=100)
[broker] enqueue: world=demo, type=message, pending=6
[broker] dequeue: world=demo, returned=6, types={'message': 6}
```

---

## Daft Lazy Evaluation Gotcha (Jan 2026)

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

## Row-wise `@daft.func` vs `@daft.func.batch` (Jan 2026)

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

## Tick-Gated Processing (Jan 2026)

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

## JSON-Encoding Complex Types (Jan 2026)

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
10. **Messaging pipeline** — Outbox/Inbox components + MessageDeliveryProcessor (not broker)
11. **Tick-gating** for expensive operations (LLM calls, inner worlds)
12. **Keep columns in DAG** — avoid intermediate `.collect()` breaking lazy evaluation

---

## Single Process, Single Event Loop (Apr 2026)

Archetype runs as **one `archetype serve` process**. This is a hard architectural constraint — do not design for multi-process or multi-server deployments. Daft owns the cores — it manages thread pools, memory, and parallelism internally. `SimulationService.run_all` drives all worlds concurrently via `asyncio.gather` in a single event loop. `AsyncWorld.step` parallelizes across archetypes the same way.

**Consequences:**

- **The CLI is a thin HTTP client.** Every command (except `serve`) is an `httpx` call to the running server. The CLI never instantiates a `ServiceContainer` — that would create an isolated, ephemeral process that can't participate in the server's event loop or share world state.
- **Never spin up a second server.** There is no multi-node or multi-process coordination layer. If you need more compute, scale the Daft cluster, not the server count.
- **World lifecycle mutations route through the CommandBroker.** `CREATE_WORLD`, `DESTROY_WORLD`, `FORK_WORLD` go through `CommandService.submit()` → broker lock → `apply_world_lifecycle()`. This gives RBAC, audit history, and serialized writes for free. Use `tick=0` for immediate execution (not tick-scheduled).

**State across restarts:**

A `WorldRegistry` (JSON file at `./archetype_data/archetype_registry.json`) catalogs world metadata (id, name, storage URI, namespace, tick). The server calls `discover_worlds()` on startup to rehydrate. This is a **boot catalog**, not a coordination mechanism — single writer, no locking needed.

---

## Daft 0.7.x: `with_column` Not `with_columns` (Apr 2026)

`DataFrame.with_columns(expr1, expr2)` raises `TypeError: too many positional arguments` in Daft 0.7.x. The method accepts **one expression at a time**. Chain calls instead:

```python
# ❌ WRONG — multiple positional args
df = df.with_columns(
    (col("position__x") + col("velocity__vx")).alias("position__x"),
    (col("position__y") + col("velocity__vy")).alias("position__y"),
)

# ✅ RIGHT — chain single expressions
df = df.with_column("position__x", col("position__x") + col("velocity__vx"))
df = df.with_column("position__y", col("position__y") + col("velocity__vy"))
```
