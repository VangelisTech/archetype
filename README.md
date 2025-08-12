# Archetype

**An AI-Native Entity Component System (ECS) for large scale simulation and data engineering**

![Archetype Diagram](./assets/archetype_diagram.png)

Archetype is an ECS simulation engine designed for scalability from local development to distributed Ray clusters. It leverages incredible performance of daft dataframes and lancedb to provide big data scalability for Multi-modal AI processors.


## References and Prior Art
- [Esper](https://github.com/benmoran56/esper) - was the ECS system I initially cloned and evolved from.
- The [archetype pattern](https://ajmmertens.medium.com/building-an-ecs-2-archetypes-and-vectorization-fe21690805f9), as described by AJ Mertens helped me understand how the archetype pattern leverages entity creation definitions based on exact combinations of components for powerful data processing isolation and decoupling.
- [This Daft Article on Scaling LLM inference](https://blog.getdaft.io/p/we-cloned-over-15000-repos-to-find?subscribe_prompt=free) and accompanying repository [Sashimi4Talent](https://github.com/everettVT/Sashimi4Talent/tree/main) introduced me to semaphore usage patterns for the async module.


## Roadmap
- [v0.1 - Full Async Multi-World Simulations Engine] 
- [v0.2 - MCP Support, Integration with Agent Terminals]
- [v0.3 - Native LLM Processor and Component Composition](#llm-processors)
  - AsyncOpenAI with API Keys
  - AsyncOpenAI with vLLM & Ray
  - Structured Generation w/ guidance
  - MCP compatible Tools, Resources
- [v0.4 - Graph Module ](#graph-module)
  - Graph System 
    - Graph Processor nodes and edges for arbitrary processor DAGs
    - Extending priority system with igraph topological sort  
    - integrating w/ Ray Compiled Graphs
  - Graph Stores 
    - Knowledge Graphs (storing edge lists in lancedb)
- Orchestration, Coordination, Communication Patterns
- RL


## Contents
- **Simulation Script Usage Patterns**
  - [Sync Usage](#basic-usage)




## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd archetype

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### Async Usage (new API)

```python
import asyncio
from daft import col, DataFrame
from archetype.core import Component
from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.runtime.storage import StorageSessionFactory
from archetype.core.aio import AsyncWorld, AsyncSystem, AsyncProcessor, AsyncQueryManager, AsyncUpdateManager, AsyncStore

# 1) Define components
class Position(Component):
    x: float
    y: float

class Velocity(Component):
    vx: float
    vy: float

# 2) Define processors
class Movement(AsyncProcessor):
    components = (Position, Velocity)
    priority = 1
    async def process(self, df: DataFrame, dt: float = 0.1):
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * dt,
            "position__y": col("position__y") + col("velocity__vy") * dt,
        })

async def main():
    # 3) Build storage runtime context (side-effect boundary)
    storage = StorageConfig(uri=".archetype_data", namespace="demo", obs_namespace="obs")
    context = StorageSessionFactory.build(storage)

    # 4) Assemble world explicitly (DI): store → managers → system → world
    store = AsyncStore(context)
    querier = AsyncQueryManager(store)
    updater = AsyncUpdateManager(store)
    system = AsyncSystem()
    world = AsyncWorld(WorldConfig(name="w1"), querier, updater, system)
    await world.add_processor(Movement())

    # 5) Create entities
    await world.create_entity([Position(x=0, y=0), Velocity(vx=1, vy=1)])
    await world.create_entity([Position(x=10, y=10), Velocity(vx=-1, vy=-1)])

    # 6) Run via DI-only RunConfig
    rc = RunConfig.dev(steps=10, prefer_live_reads=True)
    for _ in range(rc.num_steps):
        await world.step(rc, dt=0.1)

asyncio.run(main())
```

### Orchestrator (multi-world)

```python
import asyncio
from archetype.core.config import StorageConfig, WorldConfig, RunConfig
from archetype.core.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncSystem

async def run_multi():
    orch = WorldOrchestrator()
    try:
        storage = StorageConfig(uri=".archetype_data", namespace="ns")
        w1 = await orch.create_world(WorldConfig(name="a"), system=AsyncSystem(), storage_config=storage)
        w2 = await orch.create_world(WorldConfig(name="b"), system=AsyncSystem(), storage_config=storage)
        await orch.run_all_worlds(RunConfig.dev(steps=5))
    finally:
        await orch.shutdown()

asyncio.run(run_multi())
```

### RunConfig helpers

- `RunConfig.dev(...)`: quick local runs with live reads and debug-friendly defaults
- `RunConfig.benchmark(steps=..., suite="benchmark", trial=..., metadata=...)`: consistent labeling for perf suites
- `RunConfig.validate(...)`: validation-focused runs



### Parameter sweeps (quick pattern)

```python
from archetype.core.config import RunConfig

for i, (lr, seed) in enumerate([(1e-3, 0), (1e-3, 1), (3e-4, 0)]):
    rc = RunConfig.ensemble(steps=50, trial=i, metadata={"lr": lr, "seed": seed})
    await world.step(rc)
```



### LLM Module
v0.2
#### AsyncOpenAI Processor w/ API Key
(coming soon)

#### AsyncOpenAI on vLLM
(next)

#### AsyncOpenAI on vLLM on Ray Serve LLM 
(next)

#### Structured Generation 
(next)

### Graph Module
v0.3




## Star History

<a href="https://www.star-history.com/#vangelistech/archetype&Date">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=vangelistech/archetype&type=Date&theme=dark" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=vangelistech/archetype&type=Date" />
 </picture>
</a>


