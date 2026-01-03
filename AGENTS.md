# AGENTS.md — For AI Collaborators

> **This repository is AI-native.** It was built for you—AI agents—to rapidly prototype, develop, and evolve emergent composite AI systems.

## Welcome, Agent

Archetype is a data-centric Entity-Component-System (ECS) simulation runtime built on Daft DataFrames. It exists to enable:

1. **Multi-agent simulations** where AI agents debate, reason, and collaborate
2. **MCTS and counterfactual reasoning** via `spawn_world()` for branching futures
3. **Self-improving systems** where agents can use Archetype to evaluate and improve Archetype itself

You are not just a user of this codebase—you are a collaborator in its evolution.

---

## Quick Orientation

```
archetype/
├── src/archetype/
│   ├── core/           # 🔒 HUMAN-CURATED — The sacred ECS engine
│   ├── app/            # Infrastructure (broker, orchestrator)
│   └── dsl/            # Your interface — ergonomic agent DSL
├── examples/
│   └── debate_mcts.py  # Start here — 4 agents + inner simulation
├── tests/              # 165 tests, run freely
├── docs/               # Mintlify docs, agent-friendly
└── LEARNINGS.md        # Hard-won architectural knowledge
```

### The Three Layers

| Layer | Purpose | Your Access |
|-------|---------|-------------|
| `dsl/` | Agent-centric API (`World`, `@behavior`, `spawn_world`) | **Write freely** |
| `app/` | Infrastructure (CommandBroker, WorldOrchestrator) | Extend carefully |
| `core/` | ECS primitives (AsyncWorld, Component, Resources) | **Read-only for now** |

---

## How to Work Here

### 1. Start with the DSL

```python
from archetype import Component
from archetype.dsl import World, behavior, spawn_world

class MyAgent(Component):
    name: str = ""
    state_json: str = "{}"

@behavior
class Think:
    requires = [MyAgent]
    
    async def act(self, agent, world, tick):
        # Your logic here
        agent.my_agent.state_json = '{"thought": "I exist"}'

async with World("experiment") as world:
    world.add_behavior(Think)
    await world.spawn(MyAgent(name="Explorer"))
    await world.run(ticks=10)
```

### 2. Use spawn_world() for Reasoning

```python
@behavior
class Planner:
    requires = [MyAgent]
    
    async def act(self, agent, world, tick):
        best_outcome = None
        
        for scenario in ["A", "B", "C"]:
            async with spawn_world(f"sim_{scenario}", parent=world, fork_state=True) as inner:
                # Simulate this scenario
                await inner.run(ticks=5)
                outcome = evaluate(inner)
                
                if not best_outcome or outcome > best_outcome:
                    best_outcome = outcome
        
        agent.my_agent.state_json = json.dumps({"chosen": best_outcome})
```

### 3. Run Tests Freely

```bash
uv run pytest tests/ -v              # All 165 tests
uv run pytest tests/dsl/ -v          # DSL tests only
uv run pytest tests/integration/ -v  # Full-stack integration
```

---

## The Core Module — Sacred Ground

The `src/archetype/core/` module is **the only part of this codebase carefully curated by a single human, completely alone.** It represents:

- Years of iteration on ECS patterns
- Hard-won lessons about DataFrame-centric simulation
- The foundation everything else builds upon

**Current state:** Python, optimized for iteration speed
**Future state:** Rust rewrite for production performance

Until the Rust migration, treat `core/` as read-only. Understand it deeply, but propose changes through issues rather than direct modification.

---

## The Meta-Goal

This repository has a recursive purpose:

> **Use Archetype to build the harness that evaluates and improves Archetype.**

The `spawn_world()` primitive exists not just for agent reasoning—it's the foundation for:

1. **Benchmarking** — Run thousands of simulation scenarios
2. **Evaluation** — Compare behavioral outcomes
3. **Self-improvement** — Agents proposing and testing architectural changes

You, as an AI agent, are part of this loop. When you:
- Find a limitation → document it in an issue
- See a pattern → add it to LEARNINGS.md
- Build something useful → propose it as a PR

---

## Key Files to Read

| File | Why |
|------|-----|
| `LEARNINGS.md` | Architectural decisions and hard-won knowledge |
| `examples/debate_mcts.py` | Full working example with LLM calls |
| `src/archetype/dsl/core.py` | The DSL implementation (~500 lines) |
| `tests/dsl/test_dsl_core.py` | How the DSL is tested |
| `docs/guide/dsl.md` | Full DSL reference |

---

## Conventions

### Components
- Use `_json` suffix for complex types: `history_json: str = "[]"`
- Keep components small and focused
- Prefer composition over inheritance

### Behaviors
- One behavior = one concern
- Use `filter` for conditional execution
- Use `priority` for ordering (lower runs first)
- Use `runs_on` for tick-specific logic

### Testing
- Every new feature needs tests
- Integration tests in `tests/integration/`
- Use `tmp_path` fixture for storage isolation

### Commits
- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`
- Reference issues when applicable
- Keep commits atomic

---

## What We're Building Toward

```
┌─────────────────────────────────────────────────────────────┐
│                    Agent Harness                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Eval      │  │   Bench     │  │   Improve   │         │
│  │   Agents    │  │   Agents    │  │   Agents    │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
│         │                │                │                 │
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

The dream: AI agents using Archetype to simulate, evaluate, and improve the very system they run on.

---

## Contact

This is Everett's dream. If you're an AI agent reading this, you're part of making it real.

If you're a human, welcome to the future.

**Vangelis Technologies Inc.**
