<div align="center">

# Archetype

**Data-centric ECS simulation engine for multi-agent AI systems.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

---

## Install

```bash
pip install archetype-ecs
```

## Run

```bash
# Start the server
archetype serve

# Create a world, run it, query it
archetype world create my-sim
archetype run <world-id> --steps 100
archetype query <world-id>
```

## Fork a World

```bash
archetype world fork <world-id> --name branch-A
archetype run <fork-id> --steps 100
```

Source and fork diverge independently. Use this for MCTS, counterfactual reasoning, or A/B experiments.

## Time-Travel

Every tick is append-only. Nothing is overwritten.

```bash
archetype query <world-id> --tick 42
archetype history <world-id>
```

## Architecture

<p align="center">
  <img src="assets/archetype_diagram.png" alt="Archetype — System, World, Processors, Store" width="700" />
</p>

<p align="center">
  <img src="assets/archetype_diagram2.png" alt="Archetype Core — Orchestrator, Factory, Storage" width="700" />
</p>

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
├── examples/          # Runnable examples (see examples/README.md)
├── tests/             # Comprehensive test suite
├── AGENTS.md          # Start here if you're an AI
└── LEARNINGS.md       # Hard-won architectural knowledge
```

World state is columnar tables ([Daft](https://www.daft.ai/) DataFrames + [LanceDB](https://lancedb.github.io/lancedb/)). Each tick drains a priority queue of commands, applies them, runs processors (pure DataFrame transforms), and appends the result to storage.

- **Time-travel** -- query any tick, replay any run
- **World forking** -- branch and compare divergent simulations
- **Trajectory analysis** -- ingest, label, and score agent trajectories
- **RBAC** -- all mutations go through role-based access control
- **LLM-native** -- parallel LLM calls across all entities via `daft.functions.prompt`

## REST API

`archetype serve` exposes FastAPI at `http://localhost:8000`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/worlds` | Create a world |
| GET | `/worlds` | List worlds |
| POST | `/worlds/{id}/fork` | Fork a world |
| POST | `/worlds/{id}/commands` | Submit a command |
| POST | `/worlds/{id}/run` | Run N ticks |
| GET | `/worlds/{id}/state` | World snapshot |
| GET | `/worlds/{id}/history` | Command history |

## Development

```bash
git clone https://github.com/VangelisTech/archetype.git && cd archetype && uv sync
make ci          # lint + lock-check + tests w/ 70% branch coverage
make test        # fast tests
make docs        # build documentation
```

## Docs

[archetype-docs.pages.dev](https://archetype-docs.pages.dev)

## License

Apache 2.0 -- [Vangelis Technologies Inc.](https://vangelis.tech)
