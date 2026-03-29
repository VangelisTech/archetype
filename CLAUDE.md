# Archetype: Rules for AI Agents

> **This is not optional.** These are hard constraints, not suggestions.
> Violating them produces code that compiles but breaks the architecture.

## The One Rule

**Archetype is data-centric. The DataFrame is the source of truth.**

Processors are pure functions: `DataFrame → DataFrame`. If the data looks right at the end of a tick, nothing else matters. Not how you called the LLM. Not whether it was async. Not how long it took.

## What You're Building

Archetype is an experiment harness disguised as an ECS engine. World state is DataFrames. Mutations are commands. Processors are transforms. Storage is append-only. **Storage is the version control** — no git ceremony for experimentation. Fork the world, tweak the processor, run again. Both versions coexist in LanceDB, queryable side by side.

Current focus: **trajectory analysis** — evaluating agent session trajectories through configurable sampling regimes and labeling techniques described in natural language.

## Hard Constraints

### 1. Never break the lazy DAG unless you must

`.collect().to_pylist()` pulls data out of Daft's lazy execution engine. You lose plan optimization, automatic parallelism, and composability.

**The ONLY justified `.collect()` is for cross-row context** — when you genuinely need global visibility across entities (name lookups, message routing). Document every `.collect()` with an inline comment explaining why.

```python
# ❌ WRONG — imperative loop over collected rows
rows = df.select("entity_id", "agent__name").collect().to_pylist()
for row in rows:
    results.append(do_something(row))

# ✅ RIGHT — row-wise UDF, Daft manages execution
@daft.func
def do_something(name: str) -> str:
    return transform(name)

df = df.with_column("result", do_something(col("agent__name")))
```

### 2. Use `@daft.func` by default, not `@daft.func.batch`

If your batch UDF is just a for-loop over `Series.to_pylist()`, it must be `@daft.func` instead. `@daft.func` supports async natively.

```python
# ❌ WRONG — batch UDF that just loops
@daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
def write_outbox(entity_ids: daft.Series) -> list:
    return [messages.get(eid, []) for eid in entity_ids.to_pylist()]

# ✅ RIGHT — row-wise
@daft.func
def write_outbox(entity_id: int) -> list[str]:
    return messages.get(entity_id, [])
```

Use `@daft.func.batch` ONLY when the operation actually benefits from batching (vectorized NumPy, batch model inference, etc.).

### 3. Use `@daft.cls()` for non-serializable state

API clients (Anthropic, OpenAI), model weights, DB connections — anything that can't be pickled. The client lives in `__init__`, which runs once per worker and is never serialized.

```python
@daft.cls()
class ClaudeAgent:
    def __init__(self):
        import anthropic
        self.client = anthropic.AsyncAnthropic()

    async def respond(self, name: str, role: str, inbox: list[str]) -> list[str]:
        response = await self.client.messages.create(...)
        return [json.dumps({...})]

agent = ClaudeAgent()
df = df.with_column("outbox__messages", agent.respond(col("agent__name"), ...))
```

**Never** capture non-serializable objects in `@daft.func` closures. Daft will raise a pickling error.

### 4. No `asyncio.gather` over collected rows

This pattern is always wrong in Archetype:

```python
# ❌ WRONG — actor pattern in a data-centric system
rows = df.collect().to_pylist()
results = await asyncio.gather(*[call_llm(row) for row in rows])
response_by_id = {r["id"]: r["text"] for r in results}
```

Let Daft manage concurrency via async `@daft.func` or `@daft.cls()`.

### 5. The broker is governance only

The `CommandBroker` validates RBAC and quotas. It does NOT own message delivery or conversation structure. Those are processor responsibilities.

- **Outbox/Inbox components** on entities handle message transport
- **MessageDeliveryProcessor** (priority -100) routes Outbox → Inbox
- **ChatGraphRegistry** (Resource) tracks conversation structure

### 6. Tick boundaries are sacred

Messages written to Outbox at tick N are delivered to Inbox at tick N+1. This enforces causal ordering. Do not circumvent this.

### 7. Resources for shared state, not entity data

`world.resources.insert(obj)` is for world-scoped services: `ChatGraphRegistry`, `CommandBroker`, simulation configs. Components are for entity data. Don't mix them.

Processors opt into Resources via function signature: `async def process(self, df, resources: Resources, ...)`.

## Architecture Quick Reference

```
Components (entity data, DataFrame columns):
  Outbox(messages: list[str])     — agent writes here
  Inbox(messages: list[str])      — messages land here after delivery
  DeliveryReceipt(receipts: list[str]) — rejection feedback

Resources (world-scoped, DI container):
  ChatGraphRegistry  — conversation DAGs per (world, channel)
  CommandBroker      — governance (RBAC, quotas, command queuing)

Processors (DataFrame → DataFrame):
  MessageDeliveryProcessor  priority=-100  routes Outbox → Inbox, updates ChatGraph
  [Your processors]         priority=10+   read Inbox, call LLMs, write Outbox

Column naming:  componentname__fieldname
  outbox__messages, inbox__messages, agent__name, agent__role
```

## Dev Workflow

```bash
make sync-dev       # Install all deps (uses uv dependency-groups, not optional-deps)
make ci             # THE gate: lint + lock-check + tests w/ coverage (what CI runs)
make test           # Fast tests, no coverage
make check          # Auto-format + lint (ruff, writes files)
make lint-fix       # Auto-fix lint issues
```

For the full dev workflow reference — all Make targets, CI job mappings,
pre-commit hooks, and contribution process — see [CONTRIBUTING.md](CONTRIBUTING.md).

### Dependencies

Use **`uv sync --group dev`** (dependency-groups), not `uv sync --dev` (optional-deps).

### Testing

- `make ci` is the single CI gate — always run this before pushing
- Coverage threshold: 70% with branch coverage
- Tests live in `tests/` with subdirs: `core/`, `app/`, `api/`, `cli/`, `integration/`, `aio/`, `storage/`, `sync/`

### Code Quality

- **Formatter/linter:** ruff (not black, not flake8)
- **Config:** `[tool.ruff]` in pyproject.toml — line-length 100, target py312
- **Lint rules:** E, F, I, UP, B (with E501 ignored — formatter handles line length)
- Pre-commit hooks enforce ruff, lock-check, license headers, and standard hygiene

## Project Structure

- **`src/archetype/core/`** — ECS engine. **Read-only.** Do not modify without explicit approval.
- **`src/archetype/app/`** — Service layer. Extend carefully.
- **`src/archetype/api/`** + **`cli/`** — REST API and CLI. Safe to modify freely.
- **`src/archetype/trajectories/`** — Trajectory analysis pipeline.
- **`tests/`** — Every new feature needs tests.

## Key Files

| File | Purpose |
|------|---------|
| `src/archetype/core/resources.py` | Type-safe DI container |
| `src/archetype/core/aio/async_processor.py` | AsyncProcessor base class |
| `src/archetype/core/aio/async_system.py` | Processor execution + Resources injection |
| `src/archetype/app/messaging.py` | (PLANNED, not yet implemented) Outbox, Inbox, MessageDeliveryProcessor |
| `src/archetype/app/chat_graph.py` | (PLANNED, not yet implemented) ChatGraph DAG, ChatGraphRegistry |
| `src/archetype/app/broker.py` | CommandBroker (governance only) |
| `src/archetype/app/models.py` | Command model (append_history, parent_id, channel) |
| `CONTRIBUTING.md` | Dev workflow, Make targets, CI mappings, contribution process |
| `LEARNINGS.md` | Extended architectural knowledge — read before major changes |

## Conventions

- **Commits:** conventional commits — `feat:`, `fix:`, `docs:`, `refactor:`, `test:`
- **Components:** `_json` suffix for complex types serialized as strings
- **Processors:** one concern each, use `priority` for ordering (lower = first)
- **Imports:** ruff handles sorting (isort rules via `I` select)

## What NOT to Do

- Don't run `uv sync --dev` in CI — use `--group dev`
- Don't bypass `make ci` with raw pytest invocations for validation
- Don't modify `core/` without discussion
- Don't add deps without checking `uv lock --check` passes
