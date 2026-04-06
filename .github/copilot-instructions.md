# Archetype — Copilot Code Review Guidelines

Archetype is an AI-native Entity-Component-System (ECS) simulation runtime built on **Daft DataFrames** and **LanceDB**. World state is columnar DataFrames; behaviors are pure DataFrame transforms via Processors; storage is append-only (enabling time-travel and forking).

## Architecture

```
API/CLI -> ServiceContainer -> CommandBroker (RBAC) -> AsyncWorld -> LanceDB
```

All mutations route through `CommandBroker` with `ActorCtx` for RBAC enforcement. Never bypass the broker for state changes.

## Daft DataFrame Rules (CRITICAL)

1. **Prefer expressions over UDFs.** `col()`, `with_columns()`, `where()`, `groupby().agg()` are always preferred. UDFs are the escape hatch, not the default.
2. **Never use `.collect()` mid-pipeline.** It breaks the lazy DAG. Only collect at the final materialization point (storage write or explicit result).
3. **`@daft.func`** = stateless row-by-row. Use for simple transforms.
4. **`@daft.func.batch`** = stateless Series->Series. Only use when the operation actually benefits from batching (vectorized NumPy, batch inference). If you'd just loop inside, use `@daft.func` instead.
5. **`@daft.cls()`** = stateful class (models, connections). `__init__` runs once per worker. Methods are row-by-row by default; use `@daft.method.batch` only for actual batch operations.
6. **`@daft.udf` is removed.** It was deprecated in 0.7.0. Flag any usage.
7. **Struct access uses indexing:** `col("x")["field"]`, not `.struct.get()`.
8. **Expression namespaces are deprecated** in Daft 0.7.x.

## Component Rules

- Components extend `Component` (which extends `LanceModel`). Fields are auto-prefixed: `Position.x` -> `position__x` in DataFrames.
- Use `_json` suffix for complex types that need JSON serialization: `history_json: str = "[]"` (Arrow can't store `list[dict]`).
- Keep components small and focused. Prefer composition over inheritance.

## Processor Rules

- Processors declare `components = (Comp1, Comp2)` tuple and `priority: int` (lower = runs first).
- `process()` must return a new DataFrame. Never mutate in place.
- One processor = one concern.
- Use `daft.functions.prompt` for LLM calls inside processors.
- Gate expensive operations (LLM, inner sims) on tick number when appropriate.

## Code Style

- Python 3.12+, Ruff-linted: 100-char lines, double quotes, rules E/F/I/UP/B.
- `B008` is ignored in `api/routes/` (FastAPI `Depends()` pattern).
- Apache 2.0 copyright header on all new files.
- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`.

## Protected Areas

- `src/archetype/core/` is human-curated. Changes here need extra scrutiny — propose via issues first.
- The `app/` service layer should be extended carefully; respect the CommandBroker->World flow.

## Common Review Catches

- `.collect()` inside a processor (breaks lazy evaluation)
- Using `@daft.func.batch` when just looping internally (use `@daft.func`)
- Storing `list[dict]` in Components without JSON encoding
- Bypassing CommandBroker for mutations
- Missing `ActorCtx` on command submissions
- Using deprecated `@daft.udf` or `.struct.get()`
