# Architecture

Archetype is an ECS runtime where “state” is a columnar table, “behavior” is a DataFrame transform, and “time” is an append-only tick log.

## High-level mental model

```
Processors (pure transforms)
    │
    ▼
System (priority-ordered)
    │ execute(df, sig)
    ▼
World.step()
  1) query previous tick
  2) materialize spawns/dispawns
  3) execute processors
  4) persist tick output
    │
    ▼
Store (LanceDB tables per archetype signature)
```

## Code map (where things live)

### Core ECS runtime

- `src/archetype/core/component.py`: component base class + schema prefixing
- `src/archetype/core/archetype.py`: archetype signatures + unified Arrow schema + stable archetype naming
- `src/archetype/core/interfaces.py`: protocol interfaces (sync + async)

### Sync runtime (simple scripting)

- `src/archetype/core/sync/world.py`: `SyncWorld` tick loop
- `src/archetype/core/sync/system.py`: processor execution / priority ordering

### Async runtime (parallel rollouts)

- `src/archetype/core/aio/async_world.py`: `AsyncWorld` tick loop (parallel-by-archetype) + live snapshot reads
- `src/archetype/core/aio/async_system.py`: async processor execution

### Storage

- `src/archetype/core/runtime/storage.py`: `StorageContextFactory` (local vs remote object store + Iceberg catalog init)
- `src/archetype/core/storage/lancedb.py`: LanceDB-backed store (async) + index creation

### Application layer (multi-world orchestration)

- `src/archetype/app/orchestrator.py`: `WorldOrchestrator` (create/run/shutdown many worlds)
- `src/archetype/app/container.py`: `ServiceContainer` wiring
- `src/archetype/app/episodes/episode.py`: `Episode` wrapper for trajectory collection

### RL + dataflow training primitives

- `src/archetype/rl/grpo/pipeline.py`: Daft-native rollouts → rewards → group-relative advantages
- `src/archetype/rl/grpo/rollout_transformers.py`: CPU-friendly rollout engine (artifact contract)
- `src/archetype/rl/grpo/rollout_vllm.py`: vLLM rollout engine (artifact contract)
- `src/archetype/rl/grpo/train_udf.py`: “weights as data” PyTorch trainer UDF

### MCP server

- `src/archetype/mcp/server.py`: MCP tool surface (world lifecycle + command submission)

## The storage/time-travel idea

Each archetype signature corresponds to a physical table. Each tick appends a new “state row” per active entity with:

- `(world_id, run_id, tick, entity_id)`
- component fields (prefixed)
- `is_active` (soft delete)

That gives you replay/debuggability: “what did the world look like at tick N?” becomes a query, not a reconstruction.
