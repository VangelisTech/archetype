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
Discovered building `src/archetype/missions/planning/` (see `EffectProcessor` /
`udfs.apply_effect`).

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

Construct exact sources as `daft.File` values and use Daft's glob scan for
declared local or remote patterns. This distinction keeps query parameters in
exact signed URLs out of glob syntax. `daft.functions.file_path` preserves the
canonical source URI and `File.open()` streams content through the configured
`IOConfig`. Ask `File.mime_type()` for Daft's built-in MIME classification; do
not reintroduce a Python `mimetypes` fallback.

```python
from daft import col
from daft.functions import file_path

files = daft.from_files("s3://bucket/inputs/**/*.json", io_config=io_config)
files = files.with_column("source_uri", file_path(col("file")))
```

Archetype's reusable file path is intentionally cohesive:

- `archetype.artifacts.pipeline.FileIngestionPipeline` keeps scan, UUIDv7
  occurrence identity, MIME classification, content-addressed persistence,
  durable-object reopening, and every common or media-specific Daft index in
  one readable graph;
- `archetype.artifacts.scanners` contains only pure metadata algorithms,
  streaming where the format permits it;
- the UUIDv7 supplies both `artifact_id` and `ingested_at`;
- SHA-256, XXH3-64, and byte size are computed in one streaming pass; and
- nested metadata structs are unnested directly into each narrow index.

`BUFFER_COPY` controls the size of each sequential read. It is not a total file
size limit. Local content-addressed persistence stages and hashes the bytes in
one pass, then atomically publishes the digest path without rereading either
the source or an existing destination.

Keep resize, resample, transcode, OCR, and embeddings as explicit derivative
artifact workflows. They must not silently mutate the submitted occurrence.

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

## World Resources vs Storage Services (updated Jul 2026)

There are two distinct concepts, unfortunately both historically called "resources":

| Concept | Module | Purpose |
|---------|--------|---------|
| **Resources** | `core/resources.py` | Type-safe DI container for processors |
| **StorageService** | `storage/service.py` | Admits terminal Daft execution; owns app tables, store pools, and control catalogs |

**StorageService** is the internal physical authority. The sole
`archetype.wiring.build_runtime_resources()` transaction selects one for the
process graph, injects it explicitly into application services, and gives its
lifetime to `RuntimeResources` when the process owns it. It is the
materialization point for Archetype-owned Daft plans: app code delegates
terminal execution through `materialize`, and table producers use
`read_table`, `append_table`, or `append_missing`. Table registration, typed
schema comparison, Iceberg writes, and optimistic conflict retry remain beside
that execution gate. It is never placed in a world's resource container.

`StorageService` also owns the generic durable world/run envelope and extends
conditional-append keys with that identity. Family workflows provide explicit
durable coordinates and delegate typed publication directly to that substrate;
they do not create a second storage authority. File policy belongs to the
artifact family's free handlers, which configure `FileIngestionPipeline` and
publish typed indexes before the common visibility root.

**Resources** is the runtime DI container that passes services to processors. Each world has one.

```python
# Runtime DI (core layer, per-world)
world.resources.insert(MemoryBank.default())

# In processor
memory = resources.require(MemoryBank)
```

---

## Resources: Type-Safe DI (Jan 2026)

Processors often need shared state (configs and concurrency-safe domain
clients). The `Resources` container provides type-safe dependency injection:

```python
from archetype.core.resources import Resources
# Register resources on the world
world.resources.insert(MemoryBank.default())
world.resources.insert(SimConfig(tick_duration=1.0))

# Access in processors
class MyProcessor(AsyncProcessor):
    async def process(self, df, resources: Resources = None, **kwargs):
        memory = resources.require(MemoryBank)  # Raises if missing
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

3. **Use `@daft.cls()` for worker-local state derived from serializable
   configuration.** Daft serializes the decorated class and its constructor
   arguments before recreating it per worker. Constructor code may build
   non-I/O scratch from serializable values; passing an unpicklable live client,
   model, or connection as an argument does not bypass serialization.
   Independently owned closeable or I/O-backed worker resources also require a
   deterministic executor-teardown contract.

4. **Only `.collect()` for cross-row context or a narrow reusable execution
   boundary.** Message routing and name lookups require global visibility. A
   call-scoped identity frame may also be materialized once when an expensive
   or mutable source feeds more than one downstream branch. Reassign the
   returned frame (`df = df.collect()`); reusing the original lazy frame runs
   its upstream plan again. Never materialize history or payload rows for this.
   Within `archetype.app`, build the reduction lazily and ask
   `StorageService.materialize()` to admit the terminal work instead of calling
   `.collect()` directly.

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

**The serialization constraint:** `@daft.func` closures and `@daft.cls`
constructor arguments pass through Daft's serializer. Use serializable
configuration and construct only lifetime-safe worker-local scratch inside
`__init__`. A live provider may instead expose a Daft-serializable, non-owning
reconnect handle to a resource owned and closed at the host boundary. Do not
construct an independently owned socket, process, or closeable client per
worker unless the executor contract provides deterministic teardown.

```python
# ✅ Worker-local non-I/O scratch from serializable configuration
@daft.cls()
class Normalizer:
    def __init__(self, prefix: str):
        import re

        self.prefix = prefix
        self.whitespace = re.compile(r"\s+")

    def normalize(self, value: str) -> str:
        return self.prefix + self.whitespace.sub(" ", value).strip()

normalizer = Normalizer(prefix="agent:")
df = df.with_column("agent__normalized", normalizer.normalize(col("agent__name")))
```

---

## Lazy-Audit UDF-Boundary Exemption (Jun 2026)

``scripts/check_lazy_audit.py`` gates execution-capable DataFrame terminals in
``src/`` against ``lazy_audit.toml``. This includes conversions, iterators,
diagnostics, and writes such as ``.collect``, ``.count_rows()``, ``.show()``,
``.to_arrow()``, ``.to_arrow_iter()``, ``.to_pydict()``, and ``.write_*()``.
There is
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
- DataFrame diagnostics, conversions, iterators, and writes → require entries.
- ``Series.to_pylist()`` **outside** a batch-UDF → requires entry.
- ``collect()`` inside a batch-UDF on a DataFrame (not a parameter) → requires entry.

Application code has an additional ownership rule: terminal Daft, Iceberg,
and Catalog-table operations belong to `StorageService`. A bounded app
`to_pylist()` conversion is legal only after awaiting
`StorageService.materialize()`, and the conversion still needs its specific
`lazy_audit.toml` reason.

Entries are keyed by ``(path, symbol, expr, method)``. ``symbol`` is the dotted
path of the enclosing def/class (``<module>`` at file scope) and ``expr`` is the
normalized source of the audited attribute access. Adding an unrelated line
above the call does not invalidate the entry; changing what is materialized
does. Run ``python scripts/check_lazy_audit.py --list`` to read the current key
for every site, and paste the emitted ``[[entries]]`` block when the audit
reports a new one.

See ``lazy_audit.toml`` for the authoritative policy header and
``tests/scripts/test_check_lazy_audit.py`` for positive/negative coverage.

---

## Agent Communication: Queueing vs Delivery (updated Jul 2026)

Archetype provides ECS composition primitives, not a portable framework
message command or delivery policy:

- The legacy `CommandType.MESSAGE` compatibility envelope is rejected before
  quota debit, evidence, or durable admission.
- `CommandScheduler` durably orders only the six registered portable mutation
  models and is not injected into processor resources.
- `Resources`, processors, and hooks are the primitives applications can
  compose into routing and realization behavior.

The framework does not define a message payload schema, recipient validation,
inbox/outbox components, delivery receipts, channels, or a conversation graph.
There is no default `MESSAGE` no-op or durable decoder. A host that needs
messaging must supply its own state, consumer, and delivery semantics.

[`examples/04_messaging.py`](examples/04_messaging.py) demonstrates one such
policy entirely in application code:

```text
MessageRealizationProcessor (priority -100)
  └── drains the example-local Mailbox resource into Inbox components
GreetingProcessor (priority 10)
  └── deposits new greetings into Mailbox
MoodProcessor (priority 20)
  └── reads the realized Inbox state
```

Because realization runs before greeting generation, work deposited during
tick N remains pending until the realization pass in tick N+1. That causal
delay is a property of this example's priorities and shared `Mailbox`; it is
not a framework command guarantee. The example's `Mailbox`, `Inbox`, `Outbox`,
and processors are local definitions, not exports from `archetype`.

The mailbox is also mutable world-shared state. The demo keeps all agents in
one archetype table. A composition that accesses the same mailbox from
multiple table tasks must add synchronization or use another explicit
deferred boundary; processor priority orders work within a table, not across
concurrent table tasks.

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
[archetype] processor_end: PhysicsProcessor (archetype=a_2c_s...)
```

Debug diagnostics describe orchestration and lazy plan shape. They do not
execute a DataFrame to manufacture row counts or previews; execution remains
at the owning terminal boundary.

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
10. **Messaging delivery is application composition** — the legacy `MESSAGE`
    envelope is rejected; applications define state, routing, and realization
11. **Tick-gating** for expensive operations (LLM calls, inner worlds)
12. **Keep columns in DAG** — avoid intermediate `.collect()` breaking lazy evaluation
13. **Route app-owned terminal Daft work through `StorageService`** — keep
    execution admission, Catalog operations, schema checks, and Iceberg retry
    in one authority

---

## Process and Coordination Boundaries (Apr 2026, updated Jul 2026)

One `ArchetypeRuntime` or `archetype serve` process owns its live world objects,
explicit `RuntimeResources` graph, and event loop. Daft owns data-plane
parallelism inside that process: it manages worker pools and executes lazy
processor plans across rows and archetypes. Two server processes do not share
an in-memory world registry, so a request that needs a particular live world
must reach the process hosting that world.

That live-process boundary is not the durability boundary. Persisted worlds can
be discovered and queried from a fresh process. Mutable cold resume reconstructs
a world from visible rows and manifests, then acquires a writer fence. The local
SQLite control catalog coordinates processes on one host; deployments that need
cross-host fencing use the remote Cloudflare Durable Object control catalog. One
live writer per world is the invariant, not one process for the whole
deployment. See
`docs/guide/durable-discovery.md`, `docs/guide/atomic-visibility.md`, and
`docs/guide/world-lifecycle.md` for the normative contracts.

**Consequences:**

- **The CLI is a thin HTTP client.** Every command except `serve` is an HTTP
  call to a running server. The CLI does not construct its own
  `RuntimeResources` graph, because that would create an unrelated live-world
  scope.
- **Scale simulation work through Daft.** Adding an API process does not split
  one live world's in-memory execution. Multi-process discovery, cold reads,
  and fenced resume are control-plane capabilities, not a replacement for
  Daft's data-plane execution model.
- **Keep control and data durability separate.** SQLite or the remote Durable
  Object owns small transactional control state: world records, writer fences,
  commands, and manifests. Iceberg owns append-only data tables, atomic
  snapshots, and optimistic multi-writer commits. An in-process Daft execution
  gate controls local materialization pressure and ordering; it does not
  replace Iceberg conflict detection or turn the control catalog into a wrapper
  transaction around the data table.
- **World lifecycle operations are registered direct calls.** The untrusted
  API adapter constructs an exact lifecycle model and enters
  `CommandDispatcher.apply_as`; commands-owned `Policy` performs RBAC and the
  dispatcher attempts bounded access evidence. Trusted runtime handles
  construct the same model and enter `CommandDispatcher.apply`.
  `CommandScheduler` durably owns only portable tick-deferred operations; it
  is neither the lifecycle nor authorization boundary.

**State across restarts:**

The control catalog attached to a storage identity records durable world
metadata, signatures, writer fences, and tick manifests. `discover_worlds()`
reads that catalog without constructing live worlds; `resume_world()` rebuilds
a mutable world and acquires its fence. There is no JSON boot registry in the
current architecture.

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

## The Blessed LIBERO Run Recipe (Jun 2026 — moved to robot-evals Jul 2026)

LIBERO/VLA-JEPA are genuinely broken research code, but re-solving their
packaging from scratch every deploy is failure-mode **D5**. The recipe — and
the whole harness — now lives in **`everettVT/robot-evals`** (extracted
2026-07-16 with history; consumes archetype from the package index). Read its
recipe before touching torch pins or arguing about a Modal split. Highlights
(still true, recorded here because the lessons are archetype-shaped):

- **LIBERO and the VLA-JEPA policy run in-process**, one Python 3.12
  interpreter shared with Archetype. In robot-evals,
  `src/robot_evals/image.py` builds both images and owns the **RUN LEDGER**
  (what has actually executed, with dates — check it before citing any number
  from this surface); `src/robot_evals/in_process.py` drives
  `OffScreenRenderEnv` directly; `src/robot_evals/in_process_policy.py` runs
  the VLA in the same container. **No Modal interpreter split** —
  `modal_worker.py`/`vla_jepa_worker.py` were deleted 2026-07-15 (git history).
- **Two commands** (in robot-evals): `modal run src/robot_evals/image.py`
  (env-only smoke) and `...::colocated_eval_task` (policy-driven eval).
- **One real constraint:** Linux + EGL offscreen rendering + GPU. The pins we
  removed were laziness, not law — `torch<2.6` (the one `torch.load`
  `weights_only` flip, patched in-process), Python 3.8–3.10 → 3.12. **Keep
  `robosuite==1.4.1`**: 1.5 removed `SingleArmEnv`/`load_controller_config`, so
  LIBERO @ the pinned SHA fails at *import* on 1.5 — float 1.4.1's transitive
  pins to cp312 wheels instead, and keep `numpy<2`.
- **Architecture:** one control-plane world + N trial entities batch-stepped via
  the registered `RunEpisode` operation exposed by
  `RuntimeWorld.run_episode` (B1 quota reset + B2 all-done termination), graded
  from raw `ManipStatus` by the evaluation workflow. No `EvalTrialResult` (E1).

## GL Rendering Is Thread-Bound — Daft UDF Threads Go Blind (Jul 2026)

OpenGL/EGL offscreen contexts belong to the thread that created them. MuJoCo
**physics** survives cross-thread calls; **rendering** from another thread
silently returns garbage frames (static noise) with no exception. Inside
archetype this is a landmine because Daft's native runner executes UDFs on
worker threads: an env created/reset on the driver thread renders clean frames
at reset and garbage on every UDF-thread `step()` — proprio stays perfectly
correct, all files write successfully, every contract test passes, and a
vision policy simply goes blind after its first chunk.

This cost 5 GPU runs of 0% LIBERO success (2026-07-15/16) and survived a
seven-way single-variable elimination because every step-0 probe (reset-path)
was bit-perfect. It was caught only by a full-episode A/B against upstream's
single-threaded loop: at the second inference, state diff 1.5 mm, frame diff
mean 65/255 — the step-7 PNG was EGL noise.

**Rule: any renderer (MuJoCo/EGL, OpenGL, most GPU sims) driven from archetype
processors must marshal ALL calls — creation, reset, step — onto one
persistent thread.** Reference implementation (in `everettVT/robot-evals`):
`src/robot_evals/in_process.py::_EnvThread` (a daemon worker thread;
`ThreadPoolExecutor` holds container shutdown hostage). Verification: dump an
actual mid-episode frame and look at it — file-write success and correct
proprio prove nothing about pixels.
