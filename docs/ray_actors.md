# Ray Actor Agent Abstraction

## Overview

The Ray actor-based agent abstraction provides an alternative to the DSL that enables:

1. **Distributed agent execution** — Agents run as independent Ray actors
2. **Async by default** — Full async/await support for agent actions
3. **Vectorized services** — Inference and training leverage Archetype's DataFrame engine for batching
4. **State tracking** — Agent state synchronized to ECS DataFrames for querying and analysis
5. **Composite AI systems** — Agents request services that are automatically batched and vectorized

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Ray Actor Agents                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Agent 1  │  │ Agent 2  │  │ Agent 3  │  │ Agent N  │   │
│  │ (Actor)  │  │ (Actor)  │  │ (Actor)  │  │ (Actor)  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│       │             │             │             │           │
│       └─────────────┼─────────────┼─────────────┘           │
│                     ▼                                        │
│           ┌──────────────────────┐                          │
│           │   RayAgentWorld      │                          │
│           │  (Service Router)    │                          │
│           └──────────┬───────────┘                          │
│                      │                                       │
│         ┌────────────┼────────────┐                         │
│         ▼            ▼             ▼                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                    │
│  │ Inference│ │Embedding │ │ Custom   │  (Vectorized)      │
│  │ Service  │ │ Service  │ │ Service  │                    │
│  └─────┬────┘ └─────┬────┘ └─────┬────┘                    │
│        │            │            │                          │
│        └────────────┼────────────┘                          │
│                     ▼                                        │
│          ┌────────────────────┐                             │
│          │  Archetype Core    │                             │
│          │ (DataFrame Engine) │                             │
│          └────────────────────┘                             │
│                     │                                        │
│                     ▼                                        │
│          ┌────────────────────┐                             │
│          │   ECS State Store  │                             │
│          │  (LanceDB/Iceberg) │                             │
│          └────────────────────┘                             │
└─────────────────────────────────────────────────────────────┘
```

## Key Concepts

### RayAgent

Base class for actor-based agents. Each agent:
- Runs as an independent Ray actor
- Maintains local state
- Can request vectorized services
- Syncs state to ECS for tracking

```python
import ray
from archetype.ray import RayAgent

@ray.remote
class MyAgent(RayAgent):
    async def act(self, tick, context):
        # Request inference (automatically batched)
        response = await self.request_service("inference", {
            "prompt": "What is 2+2?",
            "model": "gpt-4o-mini"
        })
        
        # Update local state
        self.state["response"] = response
        
        # Sync to ECS
        await self.sync_state()
        
        return response
```

### RayAgentWorld

Manages Ray actors and coordinates with Archetype core:
- Service registration and routing
- Request batching (automatic)
- State synchronization
- Agent lifecycle management

```python
from archetype.ray import RayAgentWorld
from archetype.ray.services import InferenceService

async with RayAgentWorld("simulation") as world:
    # Register services
    world.register_service("inference", InferenceService())
    
    # Create agents
    agents = [MyAgent.remote(f"agent_{i}", world.get_ref()) 
              for i in range(10)]
    
    # Run simulation
    for tick in range(5):
        results = await world.step(tick, agents)
```

### VectorizedService

Services that process batches efficiently using DataFrames:

```python
from archetype.ray.services import VectorizedService

class MyService(VectorizedService):
    async def process_batch(self, requests):
        # Requests automatically batched
        # Use Daft DataFrames for vectorization
        df = daft.from_pydict({
            "input": [r["input"] for r in requests]
        })
        
        # Vectorized processing
        df = df.with_column("output", process_udf(df["input"]))
        
        # Return results
        return df["output"].to_pylist()
```

## Ray vs DSL Comparison

| Feature | DSL (`archetype.dsl`) | Ray Actors (`archetype.ray`) |
|---------|----------------------|------------------------------|
| **Execution Model** | Synchronous tick-based | Async actors |
| **Distribution** | Single process | Multi-node via Ray |
| **State Management** | DataFrame rows | Local state + ECS sync |
| **Service Batching** | Manual via processors | Automatic via batchers |
| **Agent Isolation** | Shared DataFrame | Separate Ray actors |
| **Scalability** | Vertical (single machine) | Horizontal (cluster) |
| **Best For** | Tight simulations, MCTS | Distributed AI systems |

## When to Use Ray Actors

Use Ray actors when you need:

1. **Distribution** — Agents across multiple machines
2. **Isolation** — Strong isolation between agents
3. **Heterogeneous agents** — Different agent types with different resource needs
4. **Long-running agents** — Agents that maintain state across many steps
5. **Service-oriented** — Agents that primarily request external services

Use DSL when you need:

1. **Tight loops** — Fast tick-based simulation
2. **spawn_world()** — Inner simulations and MCTS
3. **Simplicity** — Quick prototyping without Ray setup
4. **DataFrame queries** — Complex queries on agent state

## Example: Multi-Agent Debate

```python
import ray
from archetype.ray import RayAgent, RayAgentWorld
from archetype.ray.services import InferenceService

@ray.remote
class DebateAgent(RayAgent):
    async def act(self, tick, context):
        # Build prompt from history
        prompt = self._build_prompt(context["topic"])
        
        # Request inference (batched with other agents)
        response = await self.request_service("inference", {
            "prompt": prompt,
            "model": "gpt-4o-mini",
            "max_tokens": 200
        })
        
        # Update state
        self.state["history"].append(response)
        await self.sync_state()
        
        return response

async def main():
    ray.init()
    
    async with RayAgentWorld("debate") as world:
        # Register inference service (vectorized)
        world.register_service("inference", InferenceService())
        
        # Create 4 debate agents as Ray actors
        agents = [
            DebateAgent.remote(f"agent_{i}", world.get_ref())
            for i in range(4)
        ]
        
        # Initialize agents
        await asyncio.gather(*[a.initialize.remote() for a in agents])
        
        # Run debate for 5 rounds
        for round in range(5):
            context = {"topic": "AI Safety", "round": round}
            responses = await world.step(round, agents, context)
            
            for i, resp in enumerate(responses):
                print(f"Agent {i}: {resp}")
        
        # Query final states from ECS
        states = world.query_agent_states()
        print(f"Processed {len(states)} agent states")

if __name__ == "__main__":
    asyncio.run(main())
```

## Request Batching

The Ray actor abstraction automatically batches service requests:

1. **Agent requests** — Each agent calls `request_service()`
2. **Batching window** — Requests collected for `batch_window_ms` (default 50ms)
3. **Batch processing** — All pending requests processed as one DataFrame operation
4. **Response routing** — Results returned to individual agents

This provides:
- **Efficiency** — Fewer API calls, better GPU utilization
- **Transparency** — Agents don't need to know about batching
- **Flexibility** — Adjust batch window and max size per service

## State Tracking

Agents sync state to Archetype's ECS:

```python
# In agent
self.state["key"] = "value"
await self.sync_state()  # Writes to ECS

# In world
states = world.query_agent_states(
    filter_fn=lambda s: s["key"] == "value"
)
```

This enables:
- **Querying** — Use DataFrames to analyze agent states
- **Persistence** — State stored in LanceDB/Iceberg
- **Time travel** — Query historical states
- **Analysis** — Aggregate and analyze agent behaviors

## Performance Considerations

### Ray Actor Overhead

- **Startup cost** — ~100ms per actor
- **Message passing** — ~1ms per remote call
- **Serialization** — Pickle overhead for large objects

**Recommendation:** Use Ray actors when benefits (distribution, isolation) outweigh overhead.

### Batching Effectiveness

- **Window size** — Larger windows = bigger batches but higher latency
- **Max batch size** — Prevents memory issues with large batches
- **Request rate** — Higher rates = better batching

**Recommendation:** Tune `batch_window_ms` based on request patterns.

### ECS Sync Frequency

- **Sync cost** — Each sync writes to storage
- **Sync frequency** — More syncs = more overhead

**Recommendation:** Sync after significant state changes, not every action.

## Integration with Existing Code

The Ray actor abstraction is **complementary** to the DSL:

```python
# Use DSL for inner simulations
async with World("inner_sim") as inner:
    inner.add_behavior(MyBehavior)
    await inner.run(ticks=10)
    result = evaluate(inner)

# Use Ray actors for distributed execution
async with RayAgentWorld("outer_sim") as world:
    agents = [MyAgent.remote(i, world.get_ref()) for i in range(100)]
    await world.step(0, agents)
```

Both can access the same Archetype core services and storage.

## Future Enhancements

Planned improvements:

1. **Automatic service discovery** — Services register themselves
2. **Dynamic batching** — Adaptive batch windows based on load
3. **Actor pooling** — Reuse actors across simulations
4. **Distributed ECS** — Sharded state storage for massive scale
5. **Ray Serve integration** — Deploy services as Ray Serve endpoints

## See Also

- [examples/ray_agent_example.py](../examples/ray_agent_example.py) — Full working example
- [AGENTS.md](../AGENTS.md) — General agent development guide
- [LEARNINGS.md](../LEARNINGS.md) — Architectural patterns
- [Ray Documentation](https://docs.ray.io/) — Ray framework docs
