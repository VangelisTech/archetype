---
name: daft-patterns
description: Enforces correct Daft DataFrame patterns and the Daft mental model. Use when writing or editing Python that touches Daft, DataFrames, UDFs, column expressions, multimodal IO, lakehouse sinks, or physical-AI boundaries — anywhere under src/, tests/, or examples/.
paths: "src/**/*.py,tests/**/*.py,examples/**/*.py"
user_invocable: true
---

## Mental model (internalize first)

One-sentence essence: Daft is a **lazy, streaming query engine with a DataFrame
skin**, built so that a multi-megabyte image or a GPU model inference is just
another column expression — treated by the optimizer like `col("a") + 1`.

Four core ideas:

1. **You are building a plan, not running code.** Transformations append to a
   `LogicalPlan`. Nothing runs until a materializing action (`collect`, `show`,
   `write_*`, `to_pydict`, Archetype's `StorageService.materialize`). The
   optimizer must see the whole pipeline at once.
2. **Data streams in bounded batches.** Memory stays bounded except at
   inflationary ops (decode, explode), blocking ops (sort, agg, join),
   memory-hungry UDFs, and over-concurrent downloads.
3. **An AI / multimodal op is just an expression.** `prompt(...)`, `download`,
   `decode_image`, and Python UDFs are plan nodes. The optimizer isolates
   expensive projections and runs them as late as correctness allows — so it
   does not waste GPU work on rows a later filter would discard.
4. **Logic is runner-agnostic.** Same plan on Swordfish (local) or Flotilla
   (Ray). Correctness transfers; shuffle/cost characteristics do not.

What this predicts in practice:

- Printing a DataFrame shows schema + "not materialized" — that is success, not a bug.
- An expensive `prompt` written early may execute last (or not at all on filtered rows).
- A UDF bug surfaces at materialization, not at the `with_column` line.
- Calling `.collect()` twice re-runs the whole plan unless you hold the concrete result.
- `show(n)` is cheap (limit pushes upstream); `collect()` computes everything.

Full distillation lives in-repo under this skill's Mental model section.
Workspace notes (optional): `.context/daft-deep-wiki-mental-model.md`.
For silently-wrong runtime bugs, use `/footgun-detector`. For wrong-shape
Daft usage on a diff, use `/daft-antipatterns`.

## Daft built-ins to reach for first

Before writing a UDF, check whether Daft already provides what you need. Built-ins run inside the query plan, skip serialization overhead, and benefit from Daft's native parallelism.

### AI functions (`daft.functions`)

#### `prompt()` — LLM inference at scale

Replaces: custom `@daft.cls` wrapping OpenAI/Anthropic clients.

```python
from daft.functions import prompt

# Basic text
df = df.with_columns({
    "agent__response": prompt(
        col("agent__role") + "\nContext: " + col("agent__memory"),
        model="gpt-5-mini",
        system_message="You are a simulation agent.",
    ),
})

# Structured output — pass a Pydantic model, get a struct column
from pydantic import BaseModel
class Decision(BaseModel):
    action: str
    reasoning: str
    confidence: float

df = df.with_columns({
    "agent__decision": prompt(
        col("agent__context"),
        model="gpt-5-mini",
        return_format=Decision,
    ),
})
# Access fields: col("agent__decision")["action"]
# Or flatten: df = df.unnest("agent__decision")

# Multimodal — pass images, PDFs, files alongside text
from daft.functions import file
df = df.with_columns({
    "analysis": prompt(
        messages=[col("description"), col("image"), file(col("doc_path"))],
        model="gpt-5-mini",
    ),
})

# Tool calling
tools = [{"type": "function", "function": {"name": "search", ...}}]
df = df.with_columns({
    "tool_result": prompt(col("query"), model="gpt-5-mini", tools=tools),
})
```

**Key parameters:** `model`, `messages` (list of expressions — text, image, file), `system_message`, `return_format` (Pydantic model), plus provider options (`tools`, `tool_choice`, `max_output_tokens`, `use_chat_completions`, …).

#### `embed_text()` / `embed_image()` — embeddings

Replaces: custom `@daft.cls` wrapping HuggingFace or OpenAI embedding APIs.

```python
from daft.functions import embed_text, embed_image

df = df.with_columns({
    "text_embedding": embed_text(col("text"), model="text-embedding-3-small", provider="openai"),
})
df = df.with_columns({
    "image_embedding": embed_image(col("image"), model="openai/clip-vit-base-patch32"),
})
```

#### `classify_text()` / `classify_image()` — zero-shot classification

Replaces: custom classification UDFs with HuggingFace pipelines.

```python
from daft.functions import classify_text

df = df.with_columns({
    "category": classify_text(
        col("text"),
        labels=["positive", "negative", "neutral"],
        model="facebook/bart-large-mnli",
    ),
})
```

### Modality types and operations

#### Images

Prefer standalone `daft.functions.*` or direct Expression methods for image operations. The legacy `.image.*` accessor still works but is deprecated.

```python
from daft.functions import download, decode_image, resize, crop, encode_image

# URL → image pipeline (no UDF needed)
df = df.with_column("bytes", download(col("image_url")))
df = df.with_column("image", decode_image(col("bytes")))

# As standalone functions
df = df.with_column("resized", resize(col("image"), 224, 224))
df = df.with_column("cropped", crop(col("image"), col("bbox")))
df = df.with_column("encoded", encode_image(col("image"), "jpeg"))

# Or equivalently as Expression methods (no namespace)
df = df.with_column("resized", col("image").resize(224, 224))
df = df.with_column("cropped", col("image").crop(col("bbox")))
```

#### Audio

```python
from daft.functions import audio_file, audio_metadata, resample

df = df.with_column("audio", audio_file(col("path")))
df = df.with_column("meta", audio_metadata(col("audio")))  # duration, sample_rate
df = df.with_column("resampled", resample(col("audio"), sample_rate=16000))
```

#### Video

```python
from daft.functions import video_file, video_metadata, video_keyframes

df = df.with_column("video", video_file(col("path")))
df = df.with_column("meta", video_metadata(col("video")))  # duration, resolution, codec
df = df.with_column("keyframes", video_keyframes(col("video")))
```

#### Files and documents

```python
from daft.functions import file

# Wrap any path/URL as a daft.File — for PDFs, CSVs, HTML, Markdown
df = df.with_column("doc", file(col("doc_path")))
# Pass to prompt() for document understanding
df = df.with_columns({
    "summary": prompt(messages=[col("doc")], model="gpt-5-mini"),
})
```

Inside a UDF, use `daft.File.to_tempfile()` for libraries that need a file path:
```python
@daft.func
def extract_text(f: daft.File) -> str:
    with f.to_tempfile() as tmp:
        doc = fitz.open(tmp.name)
        return doc[0].get_text()
```

#### JSON — `jq()`, `deserialize()`, and bracket indexing

```python
from daft.functions import jq, deserialize

# Extract from JSON string columns without a UDF
df = df.with_column("name", jq(col("metadata_json"), ".name"))
# Or as Expression method
df = df.with_column("name", col("metadata_json").jq(".name"))

# Deserialize JSON string to typed struct
df = df.with_column("parsed", deserialize(col("json_col"), "json", DataType.struct({"a": DataType.int64()})))

# Struct field access (from prompt structured output, etc.)
df = df.with_column("action", col("decision")["action"])

# Flatten struct into top-level columns
df = df.unnest("decision")
```

### String, numeric, and utility functions

Replaces: most simple `@daft.func` string/math UDFs. Prefer standalone `daft.functions.*` or direct Expression methods. The legacy `.str.*` accessor still works but is deprecated — use the flat API instead.

```python
from daft.functions import (
    contains, lower, upper, replace, split, strip, length,
    startswith, endswith, tokenize_encode, tokenize_decode,
    normalize, concat_ws, regexp_extract, like,
)

# String ops — as Expression methods (no namespace prefix)
col("text").contains("keyword")
col("text").lower()
col("text").upper()
col("text").replace("old", "new")
col("text").split(",")
col("text").strip()
col("text").length()
col("text").startswith("prefix")
col("text").endswith("suffix")
col("text").like("pattern%")
col("text").regexp_extract(r"(\d+)", 1)

# Or equivalently as standalone functions
lower(col("text"))
contains(col("text"), "keyword")
split(col("text"), ",")
concat_ws(" ", col("first"), col("last"))
normalize(col("text"), lowercase=True, remove_punct=True)

# Tokenization
tokenize_encode(col("text"), "cl100k_base")
tokenize_decode(col("tokens"), "cl100k_base")
```

```python
from daft.functions import abs, ceil, floor, round, log2, sqrt, between, fill_null, when

# Numeric — direct methods or standalone functions
col("x").abs()
col("x").ceil()
col("x").floor()
col("x").round(2)
col("x").log2()
col("x").sqrt()
col("x").between(0, 100)

# Conditional — use `when` for if/else chains
when(col("score") > 0.8, "high").when(col("score") > 0.5, "mid").otherwise("low")

# Null handling
col("x").is_null()
col("x").not_null()
col("x").fill_null(0)
col("x").fill_nan(0.0)
```

### Distance and similarity functions

Replaces: custom cosine/L2 UDFs for embedding comparison.

```python
from daft.functions import (
    cosine_distance, cosine_similarity,
    euclidean_distance, dot_product,
)

df = df.with_column("sim", cosine_similarity(col("emb_a"), col("emb_b")))
df = df.with_column("dist", cosine_distance(col("emb_a"), col("emb_b")))
df = df.with_column("l2", euclidean_distance(col("emb_a"), col("emb_b")))
df = df.with_column("dot", dot_product(col("emb_a"), col("emb_b")))
```

### MinHash deduplication

Replaces: custom shingling + hashing UDFs.

```python
from daft.functions import minhash

df = df.with_column("signature", minhash(
    col("text"),
    num_hashes=128,
    ngram_size=5,
    seed=42,
))
```

### Connectors — read/write without custom I/O code

```python
# Lance / LanceDB (archetype's default store)
df = daft.read_lance("s3://bucket/table.lance")
df.write_lance("./local.lance", mode="append")  # modes: create, append, overwrite

# Iceberg
from pyiceberg.catalog import load_catalog
table = load_catalog("cat").load_table("ns.table")
df = daft.read_iceberg(table)
df.write_iceberg(table)

# SQL (uses ConnectorX — Rust, zero-copy to Arrow)
df = daft.read_sql("SELECT * FROM users", "postgresql://...")

# Parquet, CSV, JSON (local or S3)
df = daft.read_parquet("s3://bucket/data/*.parquet")
df = daft.read_csv("data.csv")
df = daft.read_json("data.jsonl")
```

---

## Rules

These are non-negotiable. Violating them produces broken pipelines.

### 1. DataFrames are lazy. Do not break the DAG.

Never `.collect()` mid-pipeline. It materializes a separate plan and downstream columns may be empty.

```python
# WRONG — breaks the DAG
df = df.with_column("response", prompt(col("input"), ...))
debug = df.select("response").limit(1).collect()  # separate plan!
df = df.with_column("next", col("response") + "...")  # response may be empty

# RIGHT — single materialization
df = df.with_column("response", prompt(col("input"), ...))
df = df.with_column("next", col("response") + "...")
result = df.collect()
```

Use `df.explain()` to debug, not intermediate collects.

### 2. UDF decision tree

Check in this order — stop at the first match:

1. **Built-in function?** → Use it (`prompt`, `embed_text`, `classify_text`, `minhash`, distance functions, etc.)
2. **Column expression?** → Use it (`col("x") + col("y")`, `.lower()`, `.contains()`, `.resize()`, etc.)
3. **Simple row transform?** → `@daft.func` (auto type inference, supports async)
4. **Vectorized batch op?** → `@daft.func.batch` (NumPy, vLLM, PyTorch — must actually batch)
5. **Expensive init?** → `@daft.cls()` (model loading, API clients — once per worker)
6. **Stateful + batch?** → `@daft.cls()` with `@daft.method.batch`

| Decorator | When | Example |
|-----------|------|---------|
| Built-in function | Daft provides it | `prompt()`, `embed_text()`, `cosine_distance()` |
| DataFrame expressions | Always prefer over UDFs | `col("x") + col("y")`, `.contains()`, `.lower()` |
| `@daft.func` | Row-wise transform, auto type inference | Simple string parsing, JSON encoding |
| `@daft.func.batch` | Operation actually batches (NumPy, vLLM, PyTorch) | Vectorized math, batch inference |
| `@daft.cls()` | Expensive init, once per worker | Model loading, DB connections |
| `@daft.method.batch` | Stateful + actual batching | Model with batch predict |

**`@daft.udf` is removed.** Deprecated 0.7.0, gone 0.8.0. Never use it. Use `@daft.func.batch` instead.

**If you loop inside a batch UDF for a pure Python transform, you're not
batching.** Use `@daft.func` row-wise instead.

```python
# WRONG — looping inside batch, no batching benefit
@daft.func.batch(return_dtype=DataType.string())
def process(values: Series) -> Series:
    return Series.from_pylist([transform(v) for v in values.to_pylist()])

# RIGHT — row-wise with auto type inference
@daft.func
def process(value: str) -> str:
    return transform(value)
```

**Exception — Physical AI / external-system boundaries.** A loop (or
`series_to_rows` → filtered live rows → one batched client call / C step)
inside `@daft.method.batch` is the *sanctioned* pattern when:

- state is loaded once in `@daft.cls.__init__` (model, env handle, Modal stub),
- the UDF talks to an external system that cannot be a lazy expression, and
- inactive/`done` rows are pass-through via `external_call_indices`.

See `src/archetype/physical_ai/boundary.py` and Rule 7 below. Do **not**
false-flag `_EnvStepper` / `_PolicyCaller` / `_CartpoleStepper` style UDFs.

### 3. Struct access

```python
# WRONG — deprecated
col("result").struct.get("field")

# RIGHT
col("result")["field"]
```

### 4. Column expressions first

The DataFrame is already columnar. Most transforms are just expressions:

```python
df = df.with_column("score", col("reward") * 0.5 + col("bonus"))
df = df.where(col("score") > 0.5)
df = df.groupby("env_id").agg(col("reward").mean())
```

UDFs are the escape hatch, not the default.

### 5. `with_columns` over chained `with_column`

Use `with_columns(dict)` for multiple column updates. Chained `with_column` can cause Daft plan dependency issues.

```python
# Prefer
df = df.with_columns({
    "col_a": col("x") + 1,
    "col_b": col("y") * 2,
})
```

### 6. UDF patterns reference

**Async row-wise** — for I/O-bound work (API calls). Daft manages concurrency:
```python
@daft.func
async def call_api(text: str) -> str:
    async with httpx.AsyncClient() as client:
        resp = await client.post(URL, json={"text": text})
        return resp.json()["result"]
```

**Generator** — expand one row into many (1→N). Other columns auto-broadcast:
```python
@daft.func
def explode_chunks(text: str, chunk_size: int) -> str:
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
```

**Stateful class** — non-serializable state (API clients, models) lives in `__init__`, reconstructed per worker:
```python
@daft.cls()
class LLMAgent:
    def __init__(self):
        import anthropic
        self.client = anthropic.AsyncAnthropic()

    async def respond(self, name: str, context: str) -> str:
        resp = await self.client.messages.create(
            model="claude-sonnet-4-6",
            messages=[{"role": "user", "content": context}],
        )
        return resp.content[0].text

agent = LLMAgent()
df = df.with_column("response", agent.respond(col("agent__name"), col("context")))
```

**Retry semantics** — for transient failures:
```python
@daft.func(max_retries=3, on_error="raise")
def flaky_call(x: str) -> str:
    return external_api(x)
```

### 7. Physical AI / external-boundary stack

External systems (MuJoCo, env RPC, policy inference) are escape hatches, not
expressions. Canonical stack in Archetype:

1. Module-level `@daft.cls` (must be importable — dynamic/factory classes fail to pickle).
2. `@daft.method.batch` → `series_to_rows` → call external system → struct `Series`.
3. Processor: UDF via `col(...)` → `unpack_struct` → exclude scratch.

```python
from archetype.physical_ai.boundary import (
    external_call_indices,
    series_to_rows,
    unpack_struct,
)

@daft.cls()
class _EnvStepper:
    def __init__(self, spec):
        self.client = spec.build()  # live handle — created on the worker

    @daft.method.batch(return_dtype=OBS_STRUCT)
    def step(self, is_active: Series, done: Series, action: Series) -> Series:
        rows = series_to_rows(
            ["is_active", "done", "action"], is_active, done, action
        )
        out = [None] * len(rows)
        for i in external_call_indices(rows):
            out[i] = self.client.step(rows[i]["action"])
        for i, row in enumerate(rows):
            if out[i] is None:
                out[i] = {...passthrough from row...}
        return Series.from_pylist(out)
```

Invariants:

- **Specs in Resources, live handles in `__init__`.** Non-picklable sockets /
  stubs must not ride in Resources across workers.
- **`Series.to_pylist` on batch-UDF params is sanctioned** (lazy_audit exemption).
  `DataFrame.collect().to_pylist()` mid-processor is not.
- **Lifecycle freeze.** Only `is_active && !done` cross the boundary.
- **Success latches.** `success = obs["success"] or prior_success`.
- **Policy before env.** Policy processor priority < env priority so ledger
  provenance stays `a_t = π(obs_{t-1})`, `obs_t = env.step(a_t)`.
- **Thin ledger columns.** Prefer `ManipFrameRef` volume refs over image blobs
  on components.
- **App grading boundary.** Await `StorageService.materialize(...)` before
  converting terminal frames to Python at HTTP/report boundaries.
- **GL/EGL is thread-bound.** Marshal all create/reset/step/render onto one
  persistent thread — Daft UDF worker threads go blind (LEARNINGS Jul 2026).
- **Torch/CUDA:** first `import torch` in the driver/caller process before the
  class-UDF runs on a worker (daft-physical-ai hands facade lesson).

### 8. Materialization boundaries

| Intent | Reach for |
|--------|-----------|
| Inspect during development | `df.show(n)` or `df.limit(n).collect()` once |
| Large result to storage | `write_parquet` / `write_iceberg` / `write_lance` (streams out) |
| Full result in memory for Python work | `collect()` — rare, deliberate |
| Archetype terminal / grading | `await StorageService.materialize(df)` then `to_pylist` |
| Debug plan shape | `df.explain()` — never mid-pipeline collect |

Re-materializing re-runs the plan. If an expensive stage must be reused,
materialize once and hold the concrete result (or write a checkpoint parquet /
Lance table — the daft-examples pattern after costly AI).

Filter and `limit` **before** explode/decode/frame expansion (LeRobot,
EgoDex, DROID lesson): never decode video/HDF5 then filter episodes.

### 9. Lakehouse / object IO (eventual-analytics practice)

- Prefer lazy `sess.read_table` → `where` / `groupby` / `Window` / `daft.functions`
  until the sink.
- `collect()` once before merge/upload when the plan would otherwise re-scan
  and re-execute side effects.
- Discover files with `daft.from_glob_path` + URI/`IOConfig`, not pathlib walks
  of object-store data. `Path` is fine for local catalog roots only.
- Explicit schemas; never yield empty `RecordBatch`es into Iceberg (null-typed
  columns reject).
- Stream + aggregate huge event feeds inside DataSource tasks; fan out tasks
  for parallelism.
- Custom streaming IO that Daft cannot express yet → `@daft.func` inside the
  plan (constant-memory chunked upload), not a driver-side `for path in paths`.

### 10. Judgment calls (taste)

1. **Expression vs UDF.** Exhaust built-ins first. Every UDF is a known
   performance liability.
2. **AI function vs hand-rolled client.** Use `prompt` / `embed_*` /
   `classify_*` + `daft.set_provider(...)` for provider calls. Hand-roll
   `@daft.cls` only for local/custom models or exotic batching.
3. **Structured LLM output.** Pass a Pydantic `return_format` to `prompt()`;
   index struct fields. Do not regex free-text.
4. **Dirty multimodal IO.** Pass `on_error="null"` on `download` /
   `decode_image`, then quarantine nulls.
5. **Inflation points.** `into_batches(small_n)` and capped `max_connections`
   before decode/explode; decode late after selective filters.
6. **UDF memory knobs.** `concurrency × batch_size` is the budget. OOM-kill
   churn → lower concurrency first.
7. **Thin UDF returns.** Write large media/tensors to disk/Volume; return
   paths + metadata structs (cosmos3 / pose_sequence lesson).
8. **Fuse GPU stages** when host round-trips dominate (VAD→ASR→diarize in one
   device-resident actor), rather than splitting across separate UDF stages.
9. **Read-then-overwrite.** UDF column args resolve to the column's *final*
   value in a plan. Consolidate every read-then-overwrite into one
   struct-returning UDF (LEARNINGS). Do not `with_column("x", udf(col("x")))`
   then read the pre-overwrite `x` elsewhere in the same projection set.
10. **Know when not to use Daft.** Batch/streaming transform engine — not a
    serving layer, transactional store, or interactive BI cache.
