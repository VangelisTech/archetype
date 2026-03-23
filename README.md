<div align="center">

# Archetype

**AI-native simulation engine for emergent composite AI systems.**

<i>Built for agents, by agents. Powered by Daft DataFrames + LanceDB.</i>

[![Tests](https://img.shields.io/badge/tests-182%20passing-brightgreen)](tests/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

> **AI Agents:** Start with [AGENTS.md](./AGENTS.md) — it's written for you.

<details>
<summary><b>Special Note for Humans</b></summary>

<br>



    Welcome to the future of AI engineering.

    The DSL presented in this release is a nominal implementation from Claude 4.5 Opus. It represents the first time I genuinely ran my first multi-agent simulation on top of the columnar ECS engine I designed and built by hand. It has taken several years and hundreds of attempts to finally reach this stage. I wish I could say it was me that improved—undoubtedly I have... But really it's been the model's exponentially improving capacity to engineer and develop software that has truly enabled this project to finally be realized.

    The codebase is built to be rewritten. With the exception of the core modules, you could re-write the entire DSL to your liking if you wanted. The decoupling patterns have been sufficiently scoped to support arbitrary experimentation both in the simulation and in the design of the DSL.

    Build what you want.
    Build what brings you joy.
    Build what inspires you.

    I know I have.
    And it's made all the difference...

    — **Everett Kleven**
    *Founder of Vangelis Technologies*
    *Creator of Archetype*

</details>

---

## What is Archetype?

Archetype is a **data-centric Entity-Component-System (ECS) runtime** where:
- World state is **columnar tables** (Daft DataFrames / Arrow)
- Each tick is an **append-only write** to storage (LanceDB)
- Agent behaviors are **pure DataFrame transforms**

This gives you:

- **Simulation as data** — Query any tick, replay any run
- **Time-travel state** — Fork worlds, branch futures, compare outcomes
- **MCTS & counterfactuals** — Fork worlds for inner simulations
- **Self-improving systems** — Use Archetype to evaluate Archetype

## Core Engine Diagram

![archetype diagram](assets/archetype_diagram2.png)

## Quick Start

```bash
# Clone and install
git clone https://github.com/vangelis-tech/archetype.git
cd archetype
uv sync

# Start the API server
archetype serve

# Or use the CLI directly
archetype world create --name my-sim
archetype run <world-id> --steps 100
archetype query <world-id>
```

## Minimal Example

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
        WorldConfig(name="my-sim"),
        StorageConfig(),
    )

    # Submit a spawn command
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

## Architecture

```
archetype/
├── src/archetype/
│   ├── core/          # ECS engine (Daft + Arrow + LanceDB)
│   ├── app/           # Service layer
│   │   ├── auth/      #   RBAC guard (ActorCtx, role permissions, quotas)
│   │   ├── broker.py  #   CommandBroker (priority queue, RBAC, history)
│   │   ├── command_service.py    # Command dispatch
│   │   ├── world_service.py      # World lifecycle
│   │   ├── simulation_service.py # Tick stepping / runs
│   │   ├── query_service.py      # Read path (time-travel queries)
│   │   ├── storage_service.py    # Storage backend pooling
│   │   └── container.py          # Wires all services together
│   ├── api/           # FastAPI REST layer
│   │   ├── routes/    #   worlds, commands, simulation, query
│   │   ├── deps.py    #   Dependency injection
│   │   └── app.py     #   App factory with lifespan
│   └── cli/           # Typer CLI
├── tests/             # 182 tests
├── AGENTS.md          # Start here if you're an AI
└── LEARNINGS.md       # Hard-won architectural knowledge
```

## API

Start with `archetype serve` (default: `http://localhost:8000`).

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/worlds` | Create a world |
| GET | `/worlds` | List worlds |
| GET | `/worlds/{id}` | Get world info |
| DELETE | `/worlds/{id}` | Remove a world |
| POST | `/worlds/{id}/fork` | Fork a world |
| POST | `/worlds/{id}/commands` | Submit a command |
| POST | `/worlds/{id}/commands/batch` | Submit batch |
| GET | `/worlds/{id}/commands` | Command history |
| POST | `/worlds/{id}/step` | Single tick |
| POST | `/worlds/{id}/run` | Run N ticks |
| GET | `/worlds/{id}/state` | World snapshot |
| GET | `/worlds/{id}/entities/{eid}` | Entity state |
| GET | `/worlds/{id}/components` | Query components |
| GET | `/worlds/{id}/history` | Command history |
| GET | `/worlds/{id}/processors` | List processors |

## CLI

```
archetype serve [--host] [--port]     # Start API server
archetype status                      # Show all worlds
archetype world create --name NAME    # Create world
archetype world list                  # List worlds
archetype world inspect WORLD_ID      # Inspect world
archetype world remove WORLD_ID       # Remove world
archetype run WORLD_ID --steps N      # Run simulation
archetype step WORLD_ID               # Single tick
archetype query WORLD_ID [--tick T]   # Query state
archetype history WORLD_ID            # Command history
```

## Service Layer

All mutations flow through the **CommandBroker** with RBAC enforcement:

```
External API → CommandService → CommandBroker (RBAC + queue) → World
                                     ↓
                              SimulationService (drain + step)
                                     ↓
                              QueryService (read path)
```

**Roles:** `viewer` (read-only), `player` (spawn/despawn/message), `coder` (processors), `maintainer` (worlds), `admin` (all)

**Quotas:** 500 commands/tick, 200k token budget/day

## For AI Agents

This repository is **AI-native**. It was built for AI agents to:

1. **Rapidly prototype** emergent multi-agent systems
2. **Fork worlds** for reasoning and self-improvement
3. **Contribute** to the codebase as collaborators

Read [AGENTS.md](./AGENTS.md) for orientation.

## Install

```bash
# Recommended
uv sync

# Or pip
pip install -e .
```

**Python 3.12+** required.

## Tests

```bash
uv run pytest tests/ -v              # All 182 tests
uv run pytest tests/integration/ -v  # Full-stack integration
uv run pytest tests/api/ -v          # API routes
uv run pytest tests/app/ -v          # Auth + services
```

## License

Apache 2.0

---

<div align="center">

**Vangelis Technologies Inc.**



</div>
