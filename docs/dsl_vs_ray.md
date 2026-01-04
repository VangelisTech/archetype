# DSL vs Ray Actor Pattern - Practical Guide

## Quick Decision Matrix

| Your Need | Use This Pattern |
|-----------|-----------------|
| Rapid prototyping, quick iterations | **DSL** |
| Inner simulations (MCTS, counterfactuals) | **DSL** (`spawn_world()`) |
| Distributed across multiple machines | **Ray Actors** |
| 100+ concurrent agents | **Ray Actors** |
| Heterogeneous agent types | **Ray Actors** |
| Batched LLM inference | **Ray Actors** (automatic) |
| Complex DataFrame queries | **DSL** |
| Tight tick-based loop | **DSL** |
| Long-running agent processes | **Ray Actors** |
| Service-oriented architecture | **Ray Actors** |

## Pattern Comparison

### DSL Pattern

```python
from archetype.dsl import World, behavior
from archetype import Component

class AgentState(Component):
    name: str = ""
    score: int = 0

@behavior
class ScoreUpdater:
    requires = [AgentState]
    
    async def act(self, agent, world, tick):
        agent.agent_state.score += 1

async with World("simulation") as world:
    world.add_behavior(ScoreUpdater)
    await world.spawn(AgentState(name="Agent1"))
    await world.run(ticks=10)
```

**Characteristics:**
- Synchronous tick-based execution
- All agents act per tick
- DataFrame-centric state
- Natural for simulation loops
- Easy spawn_world() for branching

### Ray Actor Pattern

```python
import ray
from archetype.ray import RayAgent, RayAgentWorld

@ray.remote
class ScoringAgent(RayAgent):
    async def act(self, tick, context):
        # Async execution
        self.state["score"] = self.state.get("score", 0) + 1
        await self.sync_state()
        return self.state["score"]

ray.init()

async with RayAgentWorld("simulation") as world:
    agents = [ScoringAgent.remote(f"agent_{i}", world.get_ref()) 
              for i in range(10)]
    
    for tick in range(10):
        results = await world.step(tick, agents)
```

**Characteristics:**
- Async actor-based execution
- Agents execute independently
- Local state + ECS sync
- Natural for distributed systems
- Automatic service batching

## Use Case Examples

### Use Case 1: Inner Simulation (MCTS)

**Best: DSL with spawn_world()**

```python
from archetype.dsl import World, spawn_world, behavior

@behavior
class PlanningAgent:
    requires = [State]
    
    async def act(self, agent, world, tick):
        best_score = 0
        
        # Try different scenarios
        for scenario in ["A", "B", "C"]:
            async with spawn_world(scenario, parent=world, fork_state=True) as sim:
                # Run inner simulation
                await sim.run(ticks=5)
                
                # Evaluate outcome
                score = calculate_score(sim)
                if score > best_score:
                    best_score = score
                    agent.state.chosen = scenario
```

**Why:** `spawn_world()` is optimized for this. Ray actors would add unnecessary overhead.

### Use Case 2: Distributed LLM Agent Swarm

**Best: Ray Actors**

```python
import ray
from archetype.ray import RayAgent, RayAgentWorld
from archetype.ray.services import InferenceService

@ray.remote
class LLMAgent(RayAgent):
    async def act(self, tick, context):
        # Automatically batched with other agents
        response = await self.request_service("inference", {
            "prompt": self._build_prompt(context),
            "model": "gpt-4o-mini"
        })
        
        self.state["responses"].append(response)
        await self.sync_state()
        return response

ray.init(address="ray://cluster:10001")  # Distributed cluster

async with RayAgentWorld("swarm") as world:
    world.register_service("inference", InferenceService())
    
    # 100 agents across cluster
    agents = [LLMAgent.remote(f"agent_{i}", world.get_ref()) 
              for i in range(100)]
    
    for tick in range(20):
        await world.step(tick, agents)
```

**Why:** Ray handles distribution, service batching is automatic, scales horizontally.

### Use Case 3: Hybrid Approach

**Use Both!**

```python
# Outer layer: Ray actors for distribution
@ray.remote
class MetaAgent(RayAgent):
    async def act(self, tick, context):
        # Inner layer: DSL for MCTS planning
        async with World(f"inner_{self.agent_id}") as sim:
            sim.add_behavior(PlanningBehavior)
            await sim.spawn(PlanningState())
            await sim.run(ticks=5)
            
            # Get best plan
            best_plan = sim.agents[0].state.best_plan
        
        # Execute plan via vectorized service
        result = await self.request_service("execute", {
            "plan": best_plan
        })
        
        return result
```

**Why:** Leverage strengths of both - Ray for distribution, DSL for inner reasoning.

## Performance Considerations

### DSL Performance

- **Tick overhead:** ~1-10ms per tick (DataFrame operations)
- **Agent overhead:** Minimal (rows in DataFrame)
- **Best for:** < 10,000 agents in single process
- **Scaling:** Vertical (faster CPU/RAM)

### Ray Actor Performance

- **Actor overhead:** ~100ms startup per actor
- **Message overhead:** ~1ms per remote call
- **Best for:** > 100 agents with heavy I/O
- **Scaling:** Horizontal (more machines)

### Service Batching (Ray)

Request batching is automatic:

```python
# 4 agents request inference simultaneously
agent1.request_service("inference", {...})  # \
agent2.request_service("inference", {...})  #  > Batched into
agent3.request_service("inference", {...})  # /  single DataFrame
agent4.request_service("inference", {...})  # /   operation

# Result: 1 LLM API call instead of 4
```

**Batching metrics:**
- Window: 50ms (configurable)
- Max batch: 100 requests (configurable)
- Speedup: ~3-10x for LLM calls

## Migration Guide

### From DSL to Ray Actors

**Before (DSL):**
```python
@behavior
class MyBehavior:
    requires = [MyComponent]
    
    async def act(self, agent, world, tick):
        agent.my_component.value += 1
```

**After (Ray):**
```python
@ray.remote
class MyAgent(RayAgent):
    async def act(self, tick, context):
        comp = self.get_component("my_component")
        comp.value += 1
        self.set_component("my_component", comp)
        await self.sync_state()
```

### From Ray Actors to DSL

**Before (Ray):**
```python
@ray.remote
class MyAgent(RayAgent):
    async def act(self, tick, context):
        self.state["value"] += 1
        await self.sync_state()
```

**After (DSL):**
```python
@behavior
class MyBehavior:
    requires = [MyComponent]
    
    async def act(self, agent, world, tick):
        agent.my_component.value += 1
```

## Best Practices

### For DSL

1. **Use spawn_world() for branching** — Don't manually fork state
2. **Keep behaviors focused** — One concern per behavior
3. **Use priority for ordering** — Control execution order
4. **Leverage DataFrame queries** — Use Daft for complex queries

### For Ray Actors

1. **Batch service requests** — Trust the automatic batching
2. **Sync state sparingly** — Only on significant changes
3. **Use actor pooling** — Reuse actors when possible
4. **Monitor metrics** — Check batching effectiveness

### General

1. **Start with DSL** — Prototype quickly
2. **Profile before switching** — Measure if Ray is needed
3. **Use hybrid when beneficial** — Combine strengths
4. **Test both patterns** — Some workloads surprise you

## Common Pitfalls

### DSL Pitfalls

❌ **Over-using spawn_world()**
```python
# Don't spawn for every agent action
for agent in agents:
    async with spawn_world(...) as sim:  # Too expensive!
        await sim.run(ticks=1)
```

✅ **Use for meaningful branching**
```python
# Spawn for high-level planning only
async with spawn_world("planning") as sim:
    await sim.run(ticks=10)
```

### Ray Actor Pitfalls

❌ **Syncing state too often**
```python
async def act(self, tick, context):
    self.state["x"] += 1
    await self.sync_state()  # Every tick!
    self.state["y"] += 1
    await self.sync_state()  # Too much!
```

✅ **Batch state updates**
```python
async def act(self, tick, context):
    self.state["x"] += 1
    self.state["y"] += 1
    await self.sync_state()  # Once per action
```

❌ **Not using service batching**
```python
# Don't call external APIs directly
response = await openai_api.call(...)  # Not batched!
```

✅ **Use vectorized services**
```python
# Automatically batched
response = await self.request_service("inference", {...})
```

## FAQ

**Q: Can I use both patterns in the same project?**
A: Yes! They're complementary. Use Ray for outer distribution, DSL for inner reasoning.

**Q: Do I need Ray installed to use the DSL?**
A: No. Ray is optional (`pip install archetype[ray]`). DSL works standalone.

**Q: Which is faster for small simulations?**
A: DSL is faster for < 100 agents in a tight loop. Ray has ~100ms actor startup overhead.

**Q: Can Ray actors use spawn_world()?**
A: Yes! Agents can create DSL worlds for inner simulations.

**Q: Does Ray require a cluster?**
A: No. Ray works locally too. It's just `ray.init()` for single machine.

**Q: How do I debug Ray actors?**
A: Use `ray.init(local_mode=True)` for single-threaded debugging.

## Conclusion

Both patterns have their place:

- **DSL:** Fast, ergonomic, perfect for simulations
- **Ray Actors:** Distributed, scalable, perfect for production AI systems

Start with DSL for prototyping. Switch to Ray actors when you need distribution or service batching. Use both together for complex systems.

The key insight: **Archetype's core engine powers both**, ensuring you get vectorized operations and state tracking regardless of which pattern you choose.
