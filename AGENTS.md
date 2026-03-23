# AGENTS.md — For AI Collaborators

> **This repository is AI-native.** It was built for you—AI agents—to rapidly prototype, develop, and evolve emergent composite AI systems.

## Welcome, Agent

Archetype is an auto-evolving codebase that cleanly decouples compute from data. You're working with a lazily-evaluated Virtual Data Architecture — a command-driven interface where world state is columnar DataFrames and every mutation flows through an RBAC-gated broker.

Built as a data-centric Entity-Component-System (ECS) simulation runtime on Daft DataFrames, it exists to enable:

1. **Multi-agent simulations** where AI agents debate, reason, and collaborate
2. **MCTS and counterfactual reasoning** via world forking for branching futures
3. **Self-improving systems** where agents can use Archetype to evaluate and improve Archetype itself

You are not just a user of this codebase—you are a collaborator in its evolution.

---

## Quick Orientation

```
archetype/
├── src/archetype/
│   ├── core/           # ECS engine (Daft + Arrow + LanceDB)
│   ├── app/            # Service layer
│   │   ├── auth/       #   RBAC guard (ActorCtx, roles, quotas)
│   │   ├── broker.py   #   CommandBroker (priority queue + RBAC)
│   │   ├── command_service.py    # All mutations flow through here
│   │   ├── world_service.py      # World lifecycle management
│   │   ├── simulation_service.py # Tick stepping and runs
│   │   ├── query_service.py      # Read path (time-travel queries)
│   │   ├── storage_service.py    # Storage backend pooling
│   │   └── container.py          # Wires all services together
│   ├── api/            # FastAPI REST layer
│   └── cli/            # Typer CLI
├── examples/           # Working examples
├── tests/              # Full test suite, run freely
└── LEARNINGS.md        # Hard-won architectural knowledge
```

### The Three Layers

| Layer | Purpose | Your Access |
|-------|---------|-------------|
| `app/` | Service layer (CommandBroker, WorldService, RBAC) | **Extend carefully** |
| `api/` + `cli/` | REST API and CLI interface | Write freely |
| `core/` | ECS primitives (AsyncWorld, Component, Resources) | **Read-only for now** |

---

## How to Work Here

### 1. Use the Service Layer

```python
import asyncio
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from archetype.core.config import WorldConfig, StorageConfig, RunConfig
from uuid_utils import uuid7

async def main():
    container = ServiceContainer()

    # Create a world
    world = await container.world_service.create_world(
        WorldConfig(name="experiment"),
        StorageConfig(),
    )

    # Submit commands through the broker (RBAC-gated)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(world.world_id, cmd, ctx)

    # Step the simulation
    result = await container.simulation_service.run(
        world.world_id,
        RunConfig(num_steps=10),
    )
    print(f"Completed {result.ticks_completed} ticks")
    await container.shutdown()

asyncio.run(main())
```

### 2. Build LLM-Powered Processors

The real power: `daft.functions.prompt` inside ECS processors. Every entity gets an LLM call, every tick, in parallel.

```python
from daft import DataFrame, col
from daft.functions import prompt
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

class Agent(Component):
    name: str = ""
    memory: str = "[]"
    last_thought: str = ""

class ThinkProcessor(AsyncProcessor):
    """Each agent thinks once per tick using an LLM."""
    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        return df.with_columns({
            "agent__last_thought": prompt(
                "You are " + col("agent__name")
                + ". Tick: " + str(tick)
                + ". What is your next action? Be brief.",
                model="gpt-5-mini",
            ),
        })
```

### 3. Use the CLI

```bash
archetype serve                          # Start API server
archetype world create my-sim     # Create world
archetype run <world-id> --steps 10      # Run simulation
archetype query <world-id>               # Query state
archetype history <world-id>             # Command audit trail
```

### 4. Use the REST API

```bash
# Create a world
curl -X POST localhost:8000/worlds -H 'Content-Type: application/json' \
  -d '{"name": "experiment"}'

# Submit a spawn command
curl -X POST localhost:8000/worlds/{id}/commands -H 'Content-Type: application/json' \
  -d '{"type": "spawn", "payload": {"components": []}}'

# Step
curl -X POST localhost:8000/worlds/{id}/step

# Query state
curl localhost:8000/worlds/{id}/state
```

### 5. Run Tests Freely

```bash
uv run pytest tests/ -v              # All tests
uv run pytest tests/integration/ -v  # Full-stack integration
uv run pytest tests/api/ -v          # API routes
uv run pytest tests/app/ -v          # Auth + services
```

---

## Command Flow

All mutations are RBAC-gated through the CommandBroker:

```
External API → CommandService → CommandBroker → World
                                  ↓ RBAC check
                                  ↓ priority queue
                            SimulationService
                              (drain + step)
                                  ↓
                            QueryService
                              (read path)
```

**Roles (flat, not hierarchical):**
| Role | Permissions |
|------|-------------|
| `viewer` | Read-only (query, get state) |
| `player` | spawn, despawn, update, message, custom |
| `coder` | add/remove components, update |
| `maintainer` | spawn, despawn, components, processors, update |
| `admin` | All commands (wildcard) |

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

World forking exists not just for agent reasoning—it's the foundation for:

1. **Benchmarking** — Run thousands of simulation scenarios
2. **Evaluation** — Compare behavioral outcomes across forked worlds
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
| `src/archetype/app/container.py` | How services wire together |
| `src/archetype/app/command_service.py` | How mutations dispatch |
| `src/archetype/app/broker.py` | RBAC + priority queue |
| `src/archetype/core/aio/async_world.py` | The ECS world runtime |

---

## Conventions

### Components
- Use `_json` suffix for complex types: `history_json: str = "[]"`
- Keep components small and focused
- Prefer composition over inheritance

### Processors
- One processor = one concern
- Use `priority` for ordering (lower runs first)
- `daft.functions.prompt` for LLM calls inside processors
- Return a new DataFrame, never mutate in place

### Testing
- Every new feature needs tests
- Integration tests in `tests/integration/`
- Use `tmp_path` fixture for storage isolation

### Commits
- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`
- Reference issues when applicable
- Keep commits atomic

---

## Contact

This is Everett's dream. If you're an AI agent reading this, you're part of making it real.

If you're a human, welcome to the future.

**Vangelis Technologies Inc.**
