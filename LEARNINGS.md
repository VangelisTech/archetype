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

## UDF column args resolve to the column's FINAL value (read-then-overwrite footgun)

Daft folds a `@daft.func`'s column arguments to that column's **final definition in
the plan**, not its value at the `with_column` call site. So a processor that reads a
column with a UDF and then overwrites that same column later in the chain feeds the UDF
the **post-overwrite** value — even across separate `with_column`/`with_columns` calls,
and even through an intermediate snapshot column.

```python
# ✗ BROKEN: append reads seq_next, then seq_next is bumped -> append sees the BUMPED value
df = df.with_column("plan", append_plan(col("plan"), col("seq_next"), ...))   # reads seq_next=1, not 0!
df = df.with_column("seq_next", col("seq_next") + 1)
# A plain projection snapshot does NOT save you once a UDF consumes it:
df = df.with_column("snap", col("seq_next"))   # snap also folds to the bumped value
df = df.with_column("out",  some_udf(col("snap")))
```

This silently corrupts any "read the pre-mutation value" logic — e.g. a state-hash taken
*before* applying an effect, or a sequence counter — and the bug is invisible until you
assert on exact values.

**Fix:** consolidate every read-then-overwrite into **one** struct-returning UDF that
reads each input once and returns all results as struct fields, then split the struct
back into columns. Because the output columns derive *from* the UDF, Daft cannot fold the
UDF's own inputs to those outputs (that would be a cycle), so it reads the genuine
pre-mutation values.

```python
# ✓ CORRECT: one UDF reads originals, returns a struct; split it back out
df = df.with_column("eff", apply_effect(col("atoms_json"), col("plan_json"), col("seq_next"), ...))
for f in ("atoms_json", "plan_json", "seq_next", "pre_state_sig"):
    df = df.with_column(f, col("eff")[f])
df = df.exclude("eff")
```

Plain projections (no UDF, e.g. a column swap via `with_columns({"a": col("b"), "b": col("a")})`)
*do* read input values atomically — the folding hazard is specific to UDF arguments.
Discovered building `src/archetype/htn/` (see `EffectProcessor` / `udfs.apply_effect`).

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

Use `daft.from_files` for local or remote globs. It creates lazy `daft.File`
references; `daft.functions.file_path` preserves the canonical source URI and
`File.open()` streams content through the configured `IOConfig`.

```python
from daft import col
from daft.functions import file_path

files = daft.from_files("s3://bucket/inputs/**/*.json", io_config=io_config)
files = files.with_column("source_uri", file_path(col("file")))
```

For weights-as-data patterns:

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

## Hooks: Typed Lifecycle Events (Apr 2026)

Hooks are registered against typed dataclass events from
`archetype.core.hooks`. `world.add_hook` returns a `HookHandle`; pass it back
to `remove_hook` to unregister. See `docs/guide/hooks.md` for the canonical
architecture notes.

```python
from archetype.core.hooks import OnSpawn, PostTick, PreTick

async def on_pre_tick(event: PreTick) -> None:
    print(f"Starting tick {event.tick}")

async def on_post_tick(event: PostTick) -> None:
    print(f"Finished tick {event.tick}, processed {len(event.results)} archetypes")

handle = world.add_hook(PreTick, on_pre_tick)
world.add_hook(PostTick, on_post_tick)
world.remove_hook(handle)
```

**Events** (all payloads inherit from `HookEvent` and carry `world_id: UUID`,
never the world itself):

- `PreTick(tick)` — before any archetype runs
- `PostTick(tick, results)` — after `_live` has been refreshed; `tick` is the just-completed tick
- `OnSpawn(entity_id, components)` — fires from every spawn path (`create_entity`, `spawn_reserved`)
- `OnDespawn(entity_id)` — fires from `remove_entity` when the entity existed
- `OnComponentAdded(entity_id, components)` — fires when `add_components` changes the archetype signature
- `OnComponentRemoved(entity_id, component_types)` — fires when `remove_components` changes the archetype signature

Pass `mode="spawn"` to `AsyncWorld.add_hook` to run the handler detached from
the tick (via `asyncio.create_task`) for observability sinks that must not
block. Handler errors are logged at WARNING and never abort the tick.

**Handler types:** `AsyncWorld.add_hook` takes an `AsyncHookHandler`;
`SyncWorld.add_hook` takes a `SyncHookHandler`. Both use the same event
dataclasses and `HookHandle` type, but sync hooks have no `"spawn"` mode
because there is no event loop to defer to.

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

## Lazy-Audit UDF-Boundary Exemption (Jun 2026)

``scripts/check_lazy_audit.py`` gates every ``.collect()`` and
``.to_pylist()`` call in ``src/`` against ``lazy_audit.toml``.  There is
**one sanctioned exception** that does not require an allowlist entry:

> ``Series.to_pylist()`` called on a *parameter* of a function decorated
> with ``@daft.method.batch`` or ``@daft.func.batch``.

When Daft invokes such a function the batch is already materialised by the
executor — the function receives concrete ``Series`` objects.  Converting
those parameters to Python lists is the expected interface at the C-library
or RPC boundary, not premature materialisation.  The checker detects this
pattern via AST analysis and reports these sites as
*"udf-boundary (sanctioned)"*.

```python
# ✅ SANCTIONED — no lazy_audit.toml entry needed
@daft.cls()
class Stepper:
    @daft.method.batch(return_dtype=_STATE_STRUCT)
    def step(self, cart_pos: Series, pole_angle: Series) -> Series:
        cp = cart_pos.to_pylist()   # ← sanctioned: param of @daft.method.batch
        pa = pole_angle.to_pylist() # ← sanctioned: param of @daft.method.batch
        ...

# ❌ GATED — requires a lazy_audit.toml entry with a specific technical reason
def query_rows(df):
    return df.to_pylist()  # ← DataFrame-level; still audited
```

Rules:

- ``Series.to_pylist()`` on a **batch-UDF parameter** → exempt, no entry.
- ``DataFrame.to_pylist()`` anywhere → requires entry.
- ``DataFrame.collect()`` anywhere → requires entry.
- ``Series.to_pylist()`` **outside** a batch-UDF → requires entry.
- ``collect()`` inside a batch-UDF on a DataFrame (not a parameter) → requires entry.

See ``lazy_audit.toml`` for the authoritative policy header and
``tests/scripts/test_check_lazy_audit.py`` for positive/negative coverage.

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
  1. PreTick hook fires (tick=N)
  2. For each archetype (parallel):
     a. Query previous state (tick N-1)
     b. Apply staged despawns (flip is_active=False on the existing population)
     c. Execute processors over the existing population (priority order, lower first)
     d. Concat staged spawns as raw initial conditions (x_0 given; first transformed at N+1)
     e. Persist to store (tick=N)
  3. Increment tick → N+1
  4. PostTick hook fires (tick=N+1)
```

Initial-conditions semantics apply to every staged insert, not just brand-new
entities: `update_entity`, `add_components`, and `remove_components` re-insert
the mutated row through the despawn+spawn mechanism, so the mutated values land
raw at tick N and processors first see them at tick N+1. An overlay is new
given state — the engine records what you set before the dynamics resume.

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

---

## The Blessed LIBERO Run Recipe (Jun 2026)

LIBERO/VLA-JEPA are genuinely broken research code, but re-solving their
packaging from scratch every deploy is failure-mode **D5**. The recipe is frozen
once in **`docs/guide/libero-recipe.md`** — read it before touching anything in
`bench/libero/` or arguing about torch pins / a Modal split. Highlights:

- **LIBERO runs in-process**, one Python 3.12 interpreter shared with Archetype.
  `bench/libero/image.py` builds the env; `bench/libero/in_process.py`
  (`InProcessLiberoEnvClient`) drives `OffScreenRenderEnv` directly. **No Modal
  interpreter split** — `modal_worker.py`/`vla_jepa_worker.py` are legacy.
- **Two commands:** `modal run bench/libero/image.py` (in-process smoke) and
  `modal run bench/libero/image.py::eval_task` (batched eval).
- **One real constraint:** Linux + EGL offscreen rendering + GPU. The pins we
  removed were laziness, not law — `torch<2.6` (the one `torch.load`
  `weights_only` flip, patched in-process), Python 3.8–3.10 → 3.12,
  `robosuite==1.4.1` → `>=1.5` (the one empirical unknown; fallback is 1.4.1 on
  3.12).
- **Architecture:** one control-plane world + N trial entities batch-stepped via
  `SimulationService.run_episode` (B1 quota reset + B2 all-done termination),
  graded from raw `ManipStatus` by the eval service. No `EvalTrialResult` (E1).
