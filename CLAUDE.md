# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Archetype is a lazy distributed dataframe-based ECS (Entity Component System) simulation engine using Daft for processing and LanceDB/Iceberg for storage. The system enables high-performance simulations with efficient querying and archetype-based entity organization.

The project includes both synchronous (core) and asynchronous (aio) execution models, with the aio module implementing episode-based temporal coordination for concurrent archetype processing.

## Common Commands

```bash
# Run tests
pytest

# Run specific test files
pytest tests/core/test_store.py
pytest tests/core/test_async_episode_system.py

# Run tests with asyncio support (configured in pyproject.toml)
pytest tests/ -v

# Run examples
python examples/simple_simulation.py
python examples/wall_collision_simulation.py

# Interactive development with Jupyter
jupyter notebook examples/querying_simulation_history.ipynb

# Install dependencies
uv sync
# or
pip install -e .
```

## Core Architecture

### Entity Component System (ECS) Design
- **Entities**: Unique IDs (uint64) that group components together
- **Components**: Data structures inheriting from `Component` (LanceModel base) 
- **Archetypes**: Tables organized by component signatures (e.g., entities with Position+Velocity components share a table)
- **Processors**: Systems that operate on entities with specific component combinations using the `@processor` decorator

### Synchronous Core Module (`src/archetype/core/`)

#### ArchetypeStore (`store.py`)
- Manages entity storage using Daft+Iceberg with archetype-based table organization
- Tables are partitioned by `["simulation", "run", "step"]` for temporal queries
- Component schemas are prefixed (e.g., `position__x`, `velocity__vx`) to avoid column conflicts
- Supports spawn caching for batch entity creation via `materialize_spawns()`

#### SimpleWorld (`world.py`)
- Main simulation coordinator implementing the ECS loop: query → process → update → repeat
- Facade for Store, QueryManager, UpdateManager, and System operations
- Manages simulation stepping and entity lifecycle (`spawn`/`despawn`)

#### Processor System (`processor.py`)
- Processors define transformation logic using `@processor(Component1, Component2)` decorator
- Three-phase execution: `preprocess()` → `process()` → `postprocess()`
- `preprocess()`: Fetches relevant archetype data from store
- `process()`: Applies business logic transformations to DataFrame
- `postprocess()`: Ensures step consistency and finalization

### Asynchronous AIO Module (`src/archetype/core/aio/`)

The aio module implements episode-based temporal coordination replacing rigid step-based synchronization with flexible temporal boundaries for concurrent archetype processing.

#### Episode System (`episode.py`)
- **Episode**: Temporal boundary with variable duration, different archetypes can progress at different rates
- **EpisodeCoordinator**: Manages episode progression across concurrent archetype streams
- Episodes provide synchronization points when needed while allowing independent archetype progression

#### AsyncEpisodeSystem (`async_episode_system.py`)
- Replaces rigid step-based synchronization with flexible episodes
- Each archetype can be in a different episode
- Natural handling of fast/slow archetypes with coordination points
- Episode boundaries provide checkpointing opportunities

#### AsyncProcessor (`async_processor.py`)
- Async version of processors with semaphore-controlled I/O operations
- Uses `@async_processor` decorator for concurrent execution per archetype
- Priority-based ordering within archetypes
- Exception isolation prevents cascade failures

#### AsyncWorld (`async_world.py`)
- Async version of SimpleWorld wrapping existing sync infrastructure
- Concurrent archetype processing using AsyncSystem
- Maintains compatibility with sync world's store/querier/updater

### Data Flow

#### Synchronous Flow
1. Entities spawn with components → ArchetypeStore organizes by signature into tables
2. Each simulation step: QueryManager fetches archetype DataFrames → Processors transform data → UpdateManager persists changes
3. Tables use simulation/run/step partitioning for temporal queries and history tracking

#### Asynchronous Flow
1. Episode coordination replaces global lock-step execution
2. Archetypes process independently within episodes using streaming semantics
3. Backpressure control via semaphores prevents resource exhaustion
4. Lazy synchronization only at episode boundaries when required

## Integration Patterns

### Creating Synchronous Processors
```python
@processor(Position, Velocity, priority=1)
class MovementProcessor(Processor):
    def process(self, df: DataFrame, dt: float) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
```

### Creating Asynchronous Processors
```python
@async_processor(Position, Velocity, priority=1)
class AsyncMovementProcessor(AsyncProcessor):
    async def process_stream(self, archetype_name: str, df: DataFrame, dt: float) -> DataFrame:
        async with self.io_semaphore:
            result = await external_api_call(df)
        return df.with_columns(result)
```

### World Setup
```python
# Synchronous
from archetype import make_simple_world
world = make_simple_world(uri="/path/to/data")
world.add_processor(MovementProcessor())
world.spawn(Position(x=0, y=0), Velocity(vx=1, vy=1))
world.materialize_spawns()
world.step(dt=0.1)

# Asynchronous with Episodes
from archetype.core.aio import AsyncWorld, EpisodeCoordinator
coordinator = EpisodeCoordinator(episode_size=10)
async_world = AsyncWorld(world, max_concurrent_archetypes=10)
async_world.add_processor(AsyncMovementProcessor())
await async_world.step_async(dt=0.1)
```

### Querying Data
```python
# Current state
df = world.query(Position, Velocity)

# Historical data
history = world.get_history(Position, step=[0, 1, 2])

# Episode-aware async queries
async for df in async_world.query_with_history(Position, episodes=["ep_1", "ep_2"]):
    process_data(df)
```

## Data Storage Details

- **Catalog**: SQLite-based Iceberg catalog for table metadata (`catalog.db`)
- **Tables**: Arrow/Parquet format with automatic partitioning by simulation/run/step
- **Namespace**: All archetype tables live under "archetypes" namespace
- **Schema**: Base schema includes `simulation`, `run`, `entity_id`, `step`, `is_active` + component fields with prefixes
- **Episodes**: Episode metadata tracked for temporal coordination in async execution

## Testing Strategy

The test suite covers both synchronous and asynchronous functionality:
- `tests/core/test_*.py`: Core synchronous ECS functionality
- `tests/core/test_async_*.py`: Asynchronous episode system and processors
- Configured with `pytest-asyncio` for async test support
- Tests use pytest framework with asyncio support configured via `pyproject.toml`

## Performance Characteristics

### Synchronous Core
- Sequential processor execution with Dict[str,DataFrame] aggregation
- Global lock-step execution where all archetypes complete before advancing

### Asynchronous AIO
- **Target**: 10x improvement over synchronous execution for I/O-bound processors
- **Concurrency**: Support 1000+ concurrent archetype streams
- **Memory**: Constant memory usage through streaming semantics
- **CPU Utilization**: >90% on multi-core systems through concurrent processing