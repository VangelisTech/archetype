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
curl -s -X POST localhost:8000/worlds/<world-id>/fork \
  -H 'Content-Type: application/json' \
  -d '{"name": "branch-A"}' | python -m json.tool

archetype run <fork-id> --steps 100
```

Source and fork diverge independently. Use this for MCTS, counterfactual reasoning, or A/B experiments.

## Time-Travel

Every tick is append-only. Nothing is overwritten.

```bash
# Query state at tick 42
curl -s localhost:8000/worlds/<world-id>/state?tick=42 | python -m json.tool

# Full command audit trail
archetype history <world-id>
```

## What It Does

World state is columnar tables ([Daft](https://www.getdaft.io/) DataFrames + [LanceDB](https://lancedb.github.io/lancedb/)). Each tick drains a priority queue of commands, applies them, runs processors (pure DataFrame transforms), and appends the result to storage.

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
