---
name: daft-patterns
description: Enforces correct Daft DataFrame patterns. Auto-triggers when writing or editing Python files that use Daft, DataFrames, UDFs, or column expressions.
paths: "src/**/*.py,tests/**/*.py,examples/**/*.py"
---

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
        response_format=Decision,
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

**Key parameters:** `model`, `messages` (list of expressions — text, image, file), `system_message`, `response_format` (Pydantic model), `tools`, `tool_choice`, `max_output_tokens`, `use_chat_completions` (set `True` for vLLM/OpenAI-compatible endpoints).

#### `embed()` / `embed_image()` — embeddings

Replaces: custom `@daft.cls` wrapping HuggingFace or OpenAI embedding APIs.

```python
from daft.functions import embed, embed_image

df = df.with_columns({
    "text_embedding": embed(col("text"), model="text-embedding-3-small", provider="openai"),
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

```python
from daft.functions import download, decode_image

# URL → image pipeline (no UDF needed)
df = df.with_column("bytes", download(col("image_url")))
df = df.with_column("image", decode_image(col("bytes")))

# Expression namespace operations
df = df.with_column("resized", col("image").image.resize(224, 224))
df = df.with_column("cropped", col("image").image.crop(col("bbox")))
df = df.with_column("encoded", col("image").image.encode("jpeg"))
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

#### JSON — `.jq()` and bracket indexing

```python
# Extract from JSON string columns without a UDF
df = df.with_column("name", col("metadata_json").str.json_extract("$.name"))

# Struct field access (from prompt structured output, etc.)
df = df.with_column("action", col("decision")["action"])

# Flatten struct into top-level columns
df = df.unnest("decision")
```

### String and numeric expressions

Replaces: most simple `@daft.func` string/math UDFs.

```python
# String ops (expression namespace — no UDF needed)
col("text").str.contains("keyword")
col("text").str.lower()
col("text").str.upper()
col("text").str.replace("old", "new")
col("text").str.split(",")
col("text").str.strip()
col("text").str.length()
col("text").str.starts_with("prefix")
col("text").str.ends_with("suffix")

# Tokenization
col("text").str.tokenize_encode("cl100k_base")
col("text").str.tokenize_decode("cl100k_base")

# Numeric (all columnar, no UDF)
col("x").abs()
col("x").ceil()
col("x").floor()
col("x").round(2)
col("x").log2()
col("x").sqrt()

# Conditional
col("x").if_else(col("a"), col("b"))  # if x then a else b

# Null handling
col("x").is_null()
col("x").not_null()
col("x").fill_null(0)
```

### Distance functions

Replaces: custom cosine/L2 UDFs for embedding comparison.

```python
from daft.functions import cosine_distance, l2_distance

df = df.with_column("similarity", cosine_distance(col("emb_a"), col("emb_b")))
df = df.with_column("distance", l2_distance(col("emb_a"), col("emb_b")))
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

1. **Built-in function?** → Use it (`prompt`, `embed`, `classify_text`, `minhash`, distance functions, etc.)
2. **Column expression?** → Use it (`col("x") + col("y")`, `.str.*`, `.image.*`, etc.)
3. **Simple row transform?** → `@daft.func` (auto type inference, supports async)
4. **Vectorized batch op?** → `@daft.func.batch` (NumPy, vLLM, PyTorch — must actually batch)
5. **Expensive init?** → `@daft.cls()` (model loading, API clients — once per worker)
6. **Stateful + batch?** → `@daft.cls()` with `@daft.method.batch`

| Decorator | When | Example |
|-----------|------|---------|
| Built-in function | Daft provides it | `prompt()`, `embed()`, `cosine_distance()` |
| DataFrame expressions | Always prefer over UDFs | `col("x") + col("y")`, `.str.contains()` |
| `@daft.func` | Row-wise transform, auto type inference | Simple string parsing, JSON encoding |
| `@daft.func.batch` | Operation actually batches (NumPy, vLLM, PyTorch) | Vectorized math, batch inference |
| `@daft.cls()` | Expensive init, once per worker | Model loading, DB connections |
| `@daft.method.batch` | Stateful + actual batching | Model with batch predict |

**`@daft.udf` is removed.** Deprecated 0.7.0, gone 0.8.0. Never use it. Use `@daft.func.batch` instead.

**If you loop inside a batch UDF, you're not batching.** Use `@daft.func` row-wise instead.

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
