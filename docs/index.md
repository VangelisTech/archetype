# Archetype

**AI-native simulation engine for emergent composite AI systems.**

> 🤖 **AI Agents:** You're in the right place. This documentation is written for you.

Archetype is a data-centric Entity-Component-System (ECS) runtime built on Daft DataFrames. It exists to enable:

1. **Multi-agent simulations** where AI agents debate, reason, and collaborate
2. **MCTS and counterfactual reasoning** via world forking for branching futures
3. **Self-improving systems** where agents can evaluate and improve the system itself

## Quick Start

Install and start the server:

```bash
pip install archetype-ecs
archetype serve
```

Then, in another terminal, create a world and run a few ticks:

```bash
archetype world create hello
archetype run <world-id> --steps 10
archetype query <world-id>
```

That's it — you have a running simulation. See [Quickstart](guide/quickstart.md) for the Python API and HTTP equivalents.

## World Forking

Fork worlds to explore possibilities — the core primitive for MCTS, counterfactual reasoning, and A/B experiments:

```bash
archetype world fork <world-id> --name branch-A
archetype run <fork-id> --steps 10
```

Source and fork diverge independently. See [`examples/fork_counterfactual.py`](https://github.com/VangelisTech/archetype/blob/main/examples/fork_counterfactual.py) for a full walkthrough.

## Architecture

```text
┌─────────────────────────────────────────────────────┐
│              archetype.api / archetype.cli          │
│         FastAPI REST + Typer (HTTP client)          │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.app                       │
│  ServiceContainer, CommandBroker, WorldService,     │
│  SimulationService, QueryService, RBAC              │
└─────────────────────────────────────────────────────┘
                         │
┌─────────────────────────────────────────────────────┐
│                  archetype.core                      │
│  AsyncWorld, AsyncSystem, Resources, LanceDB Store  │
│  🔒 Human-curated • Rust rewrite planned            │
└─────────────────────────────────────────────────────┘
```

See [Architecture](guide/architecture.md) for the full class diagram.

## Key Files for Agents

| File | Purpose |
|------|---------|
| [`AGENTS.md`](https://github.com/VangelisTech/archetype/blob/main/AGENTS.md) | Your orientation guide |
| [`LEARNINGS.md`](https://github.com/VangelisTech/archetype/blob/main/LEARNINGS.md) | Architectural decisions and patterns |
| [`examples/simulation_script.py`](https://github.com/VangelisTech/archetype/blob/main/examples/simulation_script.py) | Full working demo (components, processors, run) |
| [`examples/fork_counterfactual.py`](https://github.com/VangelisTech/archetype/blob/main/examples/fork_counterfactual.py) | World forking for A/B experiments |

## The Vision

```text
Agents ──▶ Archetype ──▶ Simulations ──▶ Insights ──▶ Better Archetype
    ▲                                                        │
    └────────────────────────────────────────────────────────┘
```

This repository is designed to be improved by the very agents that use it.

## Navigation

- [Quickstart](guide/quickstart.md) — Get running in 5 minutes
- [Architecture](guide/architecture.md) — How the layers work together
- [REST API Reference](reference/rest-api.md) — Auto-generated from OpenAPI schema
- [CLI Reference](reference/cli.md) — Auto-generated from Typer commands
- [Python API Reference](reference/python-api.md) — Key classes and their docstrings
