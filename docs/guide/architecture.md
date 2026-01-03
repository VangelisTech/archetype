# Architecture

Archetype is structured in three layers: **DSL** → **App** → **Core**.

## Layer Overview

```
┌─────────────────────────────────────────────────────┐
│                  archetype.dsl                       │
│                                                      │
│  World           - Ergonomic context manager         │
│  @behavior       - Decorator compiles to Processor   │
│  spawn_world()   - Fork for MCTS/counterfactuals    │
│  AgentProxy      - Natural attribute access          │
└─────────────────────────────────────────────────────┘
                         │ compiles to
                         ▼
┌─────────────────────────────────────────────────────┐
│                  archetype.app                       │
│                                                      │
│  CommandBroker      - Priority queue for commands    │
│  WorldOrchestrator  - Multi-world lifecycle          │
│  WorldFactory       - Storage-aware world creation   │
│  StorageBackendManager - Lance/Iceberg backends      │
└─────────────────────────────────────────────────────┘
                         │ orchestrates
                         ▼
┌─────────────────────────────────────────────────────┐
│                  archetype.core                      │
│                                                      │
│  AsyncWorld     - Tick loop, entity management       │
│  AsyncSystem    - Processor execution                │
│  AsyncProcessor - DataFrame transform interface      │
│  Resources      - Type-safe DI container             │
│  LanceDB Store  - Columnar persistence               │
└─────────────────────────────────────────────────────┘
```

## Data Flow (One Tick)

```
1. World.step() called
       │
2. Hooks: pre_tick callbacks
       │
3. CommandBroker: dequeue due commands
       │
4. System.execute():
   ├─ Query latest state (DataFrame per archetype)
   ├─ Run processors in priority order
   └─ Each processor: df_in → df_out (pure transform)
       │
5. Updater: persist tick output to LanceDB
       │
6. Hooks: post_tick callbacks
       │
7. Return control
```

## Key Design Decisions

### State as DataFrames

Entity state is stored in columnar tables (LanceDB/Arrow). Each **archetype** (unique set of components) has its own table with schema:

```
world_id | run_id | entity_id | tick | is_active | component__field | ...
```

This enables:
- Vectorized transforms (Daft)
- Time-travel queries ("state at tick N")
- Efficient bulk operations

### Behaviors Compile to Processors

The `@behavior` decorator creates a `BehaviorSpec` that compiles to an `AsyncProcessor` at registration time. The DSL handles:
- Wrapping row-wise logic in DataFrame transforms
- JSON serialization for complex types
- Mutation tracking and batching

### Resources for Shared State

The `Resources` container provides type-safe dependency injection for world-level services:

```python
# Register
world.resources.register(CommandBroker, broker)

# Retrieve
broker = world.resources.get(CommandBroker)
```

### Hooks for Extensibility

Lifecycle hooks allow external code to observe or modify behavior:

```python
world.hooks.add_pre_tick(lambda w, t: print(f"Starting tick {t}"))
world.hooks.add_post_tick(lambda w, t: save_checkpoint(w))
```

## File Map

```
src/archetype/
├── __init__.py          # Re-exports Component, etc.
├── core/
│   ├── component.py     # Component base class
│   ├── archetype.py     # Archetype signatures
│   ├── resources.py     # DI container
│   ├── config.py        # Configuration
│   ├── aio/             # Async runtime
│   │   ├── async_world.py
│   │   ├── async_system.py
│   │   └── async_processor.py
│   ├── sync/            # Sync runtime
│   │   ├── world.py
│   │   └── system.py
│   └── storage/
│       └── lancedb.py   # LanceDB store
├── app/
│   ├── broker.py        # CommandBroker
│   ├── orchestrator.py  # WorldOrchestrator
│   ├── factory.py       # WorldFactory
│   └── models.py        # Command, CommandType
└── dsl/
    ├── core.py          # World, @behavior, spawn_world
    └── primitives.py    # Inbox, broadcast
```
