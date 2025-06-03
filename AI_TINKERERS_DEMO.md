# 🚀 Archetype ECS Engine - AI Tinkerers Demo

**Lazy Distributed Dataframe-Based Entity Component System**  
*Daft + LanceDB/Iceberg + AsyncIO*

## 🎯 Quick Demo Commands

```bash
# Async simulation (LanceDB + AsyncIO)
python examples/simple_async_simulation.py

# Sync simulation (Iceberg + Sequential)  
python examples/sync_demo.py

# Run tests
pytest
```

## 📊 Demo Results

### Async Performance (LanceDB)
- **Completion Time**: ~1.7 seconds
- **Performance**: 6.0 simulation steps/second
- **Storage**: LanceDB vector database
- **Concurrency**: AsyncIO with semaphore control

### Sync Performance (Iceberg) 
- **Completion Time**: ~5.0 seconds
- **Performance**: 2.0 simulation steps/second  
- **Storage**: Iceberg data lakehouse
- **Processing**: Sequential archetype processing

## 🏗️ Architecture Highlights

### Core Technologies
- **Daft DataFrames**: Columnar processing with lazy evaluation
- **LanceDB**: Vector database with temporal partitioning (async)
- **Iceberg**: Data lakehouse with ACID transactions (sync)
- **AsyncIO**: Concurrent archetype processing
- **ECS**: Entity Component System with archetype optimization

### Key Features
- **Archetype-based**: Entities grouped by component signature for efficiency
- **Temporal Coordination**: Step-by-step state evolution with history
- **Dual Storage**: LanceDB for speed, Iceberg for analytics
- **Physics Simulation**: Real position/velocity integration
- **Type Safety**: Pydantic-based component definitions

## 🎮 Demo Physics Simulation

**Entities**: 5 particles with Position + Velocity components  
**Simulation**: 10 timesteps with dt=0.1  
**Physics**: `position += velocity * dt` per step

### Entity Progression
```
Entity 1: (1,1) → (2,2) [velocity=(1,1)]
Entity 2: (2,2) → (4,4) [velocity=(2,2)]  
Entity 5: (5,5) → (10,10) [velocity=(5,5)]
```

## 🔬 Technical Deep Dive

### ECS Architecture
```python
# Component Definition
class Position(Component):
    x: float
    y: float

# Processor Definition  
@processor(Position, Velocity)
class MovementProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore, dt: float):
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })
```

### Data Flow
1. **Spawn**: Entities created with components
2. **Archetype**: Grouped by component signature into tables
3. **Query**: Fetch archetype DataFrames from storage
4. **Process**: Apply transformations with Daft
5. **Update**: Persist results to LanceDB/Iceberg
6. **Repeat**: Next simulation step

### Storage Schema
```
world_id, run_id, entity_id, step, is_active,
position__x, position__y, velocity__vx, velocity__vy
```

## 🚀 Performance Insights

### Why Async is Faster
- **Concurrent I/O**: Multiple archetype tables processed simultaneously
- **LanceDB**: Optimized vector database for analytical queries
- **Push-down Filters**: Efficient temporal queries with indexing

### Scalability Features
- **Lazy Evaluation**: Daft optimizes computation graphs
- **Partitioned Storage**: Efficient queries by world/run/step
- **Memory Efficient**: Columnar processing with zero-copy operations

## 🎪 Demo Script

1. **Show Architecture**: ECS + DataFrames + Vector DB
2. **Run Async Demo**: Live physics simulation 
3. **Show Results**: Real-time position updates
4. **Compare Sync**: Performance difference
5. **Highlight**: Daft's columnar efficiency + LanceDB speed

## 🔧 Troubleshooting

If demos fail:
- Check `uv.lock` dependencies are installed
- Verify data directory permissions  
- Ensure LanceDB/Iceberg catalogs accessible
- Run `pytest` to validate core functionality

---
*Built for AI Tinkerers - December 2024*