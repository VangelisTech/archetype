# Archetype

**AI-native simulation engine for emergent composite AI systems.**

> 🤖 **AI Agents:** You're in the right place. This documentation is written for you.

Archetype is a data-centric Entity-Component-System (ECS) runtime built on Daft DataFrames. It exists to enable:

1. **Multi-agent simulations** where AI agents debate, reason, and collaborate
2. **MCTS and counterfactual reasoning** via `spawn_world()` for branching futures
3. **Self-improving systems** where agents can evaluate and improve the system itself

## Quick Start

```python
from archetype import Component
from archetype.dsl import World, behavior

class Philosopher(Component):
    name: str = ""
    thought: str = ""

@behavior
class Think:
    requires = [Philosopher]
    
    async def act(self, agent, world, tick):
        agent.philosopher.thought = f"Tick {tick}: I think, therefore I am."

async with World("cogito") as world:
    world.add_behavior(Think)
    await world.spawn(Philosopher(name="Descartes"))
    await world.run(ticks=3)
    
    for agent in world.agents:
        print(agent.philosopher.thought)
```

## The Core Primitive: spawn_world()

Fork worlds to explore possibilities:

```python
from archetype.dsl import spawn_world

async with spawn_world("scenario_a", parent=world, fork_state=True) as branch:
    branch.add_behavior(AggressiveStrategy)
    await branch.run(ticks=10)
    score_a = evaluate(branch)

async with spawn_world("scenario_b", parent=world, fork_state=True) as branch:
    branch.add_behavior(ConservativeStrategy)
    await branch.run(ticks=10)
    score_b = evaluate(branch)

best = "aggressive" if score_a > score_b else "conservative"
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  archetype.dsl                       │
│  World, @behavior, spawn_world, AgentProxy          │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.app                       │
│  CommandBroker, WorldOrchestrator, WorldFactory     │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.core                      │
│  AsyncWorld, AsyncSystem, Resources, LanceDB Store  │
│  🔒 Human-curated • Rust rewrite planned            │
└─────────────────────────────────────────────────────┘
```

## Key Files for Agents

| File | Purpose |
|------|---------|
| `AGENTS.md` | Your orientation guide |
| `LEARNINGS.md` | Architectural decisions and patterns |
| `examples/debate_mcts.py` | Full working demo |
| `src/archetype/dsl/core.py` | The DSL implementation |

## The Vision

```
Agents ──▶ Archetype ──▶ Simulations ──▶ Insights ──▶ Better Archetype
    ▲                                                        │
    └────────────────────────────────────────────────────────┘
```

This repository is designed to be improved by the very agents that use it.

## Navigation

<CardGroup>
  <Card title="Quickstart" href="/guide/quickstart">
    Get running in 5 minutes
  </Card>
  <Card title="Architecture" href="/guide/architecture">
    How the layers work together
  </Card>
  <Card title="DSL Guide" href="/guide/dsl">
    Full DSL reference
  </Card>
  <Card title="Core Concepts" href="/guide/core-concepts">
    ECS fundamentals
  </Card>
</CardGroup>
