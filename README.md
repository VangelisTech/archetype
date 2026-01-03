<div align="center">

# Archetype

**AI-native simulation engine for emergent composite AI systems.**

<i>Built for agents, by agents. Powered by Daft DataFrames + LanceDB.</i>

[![Tests](https://img.shields.io/badge/tests-165%20passing-brightgreen)](tests/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

> **🤖 AI Agents:** Start with [AGENTS.md](./AGENTS.md) — it's written for you.

---

## What is Archetype?

Archetype is a **data-centric Entity-Component-System (ECS) runtime** where:
- World state is **columnar tables** (Daft DataFrames / Arrow)
- Each tick is an **append-only write** to storage (LanceDB)
- Agent behaviors are **pure DataFrame transforms**

This gives you:

- **Simulation as data** — Query any tick, replay any run
- **Time-travel state** — Fork worlds, branch futures, compare outcomes
- **MCTS & counterfactuals** — `spawn_world()` for inner simulations
- **Self-improving systems** — Use Archetype to evaluate Archetype

## The Vision

```
┌─────────────────────────────────────────────────────────────┐
│                    Agent Harness                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Eval      │  │   Bench     │  │   Improve   │         │
│  │   Agents    │  │   Agents    │  │   Agents    │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
│         └────────────────┼────────────────┘                 │
│                          ▼                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                  Archetype Engine                    │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐             │   │
│  │  │   DSL   │  │   App   │  │  Core   │ ← Rust soon │   │
│  │  └─────────┘  └─────────┘  └─────────┘             │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**AI agents using Archetype to simulate, evaluate, and improve the very system they run on.**

## Quick Start

```bash
# Clone and install
git clone https://github.com/vangelis-tech/archetype.git
cd archetype
uv sync

# Run the flagship demo (4 agents + inner simulation)
uv run python examples/debate_mcts.py
```

## Minimal Example

```python
from archetype import Component
from archetype.dsl import World, behavior

class Explorer(Component):
    name: str = ""
    energy: int = 100

@behavior
class Forage:
    requires = [Explorer]
    
    async def act(self, agent, world, tick):
        agent.explorer.energy = agent.explorer.energy + 10

async with World("simulation") as world:
    world.add_behavior(Forage)
    await world.spawn(Explorer(name="Scout"))
    await world.run(ticks=10)
    
    print(world.agents[0].explorer.energy)  # 200
```

## spawn_world() — The Core Primitive

Fork worlds for MCTS, counterfactual reasoning, and self-evaluation:

```python
from archetype.dsl import spawn_world

class Planner(Component):
    decision: str = ""

@behavior
class Plan:
    requires = [Planner]
    
    async def act(self, agent, world, tick):
        best = None
        for scenario in ["A", "B", "C"]:
            async with spawn_world(scenario, parent=world, fork_state=True) as inner:
                await inner.run(ticks=5)
                score = evaluate(inner)
                if not best or score > best:
                    best = scenario
        
        agent.planner.decision = best
```

## Architecture

```
archetype/
├── src/archetype/
│   ├── core/     # 🔒 Human-curated ECS engine (Rust rewrite planned)
│   ├── app/      # Infrastructure (CommandBroker, WorldOrchestrator)  
│   └── dsl/      # Agent DSL (World, @behavior, spawn_world)
├── tests/        # 165 tests
├── docs/         # Agent-friendly documentation
├── AGENTS.md     # Start here if you're an AI
└── LEARNINGS.md  # Hard-won architectural knowledge
```

## For AI Agents

This repository is **AI-native**. It was built for AI agents to:

1. **Rapidly prototype** emergent multi-agent systems
2. **Use spawn_world()** for reasoning and self-improvement
3. **Contribute** to the codebase as collaborators

Read [AGENTS.md](./AGENTS.md) for orientation.

## For Humans

The `core/` module is the one part of this codebase carefully curated by a single human. It represents years of iteration on ECS patterns and DataFrame-centric simulation. The plan:

1. **Now:** Python core, optimized for iteration speed
2. **Next:** Rust rewrite for production performance
3. **Future:** Agents using Archetype to improve Archetype

## Install

```bash
# Recommended (matches repo tooling)
uv sync

# Or pip
pip install -e .
```

**Python 3.11+** required.

## Documentation

| Doc | Purpose |
|-----|---------|
| [AGENTS.md](./AGENTS.md) | AI agent orientation |
| [LEARNINGS.md](./LEARNINGS.md) | Architectural knowledge |
| [docs/](./docs/) | Full documentation |
| [examples/](./examples/) | Working examples |

## Tests

```bash
uv run pytest tests/ -v              # All 165 tests
uv run pytest tests/integration/ -v  # Full-stack integration
```

## License

Apache 2.0

---

<div align="center">

**Vangelis Technologies Inc.**

*Building the future, one simulation at a time.*

</div>
