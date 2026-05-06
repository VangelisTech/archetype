<div align="center">

<img src="assets/archetype_diagram2.png" alt="Archetype" width="100%" />

# Archetype

**A dataframe-first, append-only ECS runtime for simulations and AI agents.**

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![PyPI](https://img.shields.io/pypi/v/archetype-ecs?color=blue)](https://pypi.org/project/archetype-ecs/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-archetype--docs.pages.dev-blue)](https://archetype-docs.pages.dev)

</div>

World state lives in columnar archetype tables. Behavior is a DataFrame transform. Every tick is a new snapshot, never an overwrite. Time-travel, forking, and large-batch agent inference fall out of the storage model.

```bash
pip install archetype-ecs
```

## Features

<table>
<tr>
<td width="220"><strong>Append-only world state</strong></td>
<td>Every tick writes a new snapshot. Old rows are never mutated, so any prior tick is a real query — not a reconstruction.</td>
</tr>
<tr>
<td><strong>DataFrame-native processors</strong></td>
<td>Behavior is a Daft <code>DataFrame &rarr; DataFrame</code> transform over an entire archetype. No per-entity loops.</td>
</tr>
<tr>
<td><strong>First-class world forking</strong></td>
<td>Branch a world by ID. Source and fork share processors and resources, then diverge in storage independently.</td>
</tr>
<tr>
<td><strong>Async ECS</strong></td>
<td>Different archetypes run concurrently. Within an archetype, processors run in ascending <code>priority</code>.</td>
</tr>
<tr>
<td><strong>LLM-batched agents</strong></td>
<td>Async UDFs let one processor call an LLM for thousands of agents at once, with throughput shared across the archetype.</td>
</tr>
<tr>
<td><strong>Gated commands</strong></td>
<td>Every external mutation flows through <code>CommandService</code>: RBAC, per-tick quotas, daily token budgets, audit emission.</td>
</tr>
<tr>
<td><strong>Pluggable storage</strong></td>
<td>LanceDB-backed or Daft-catalog-backed archetype tables behind one async contract. Backends pool across worlds.</td>
</tr>
<tr>
<td><strong>One runtime, three surfaces</strong></td>
<td>Drive the same world from a Python script (<code>ArchetypeRuntime</code>), a FastAPI server, or a Typer CLI.</td>
</tr>
<tr>
<td><strong>Logfire built in</strong></td>
<td>Gate spans, per-phase tick spans, and opt-in per-entity hooks ship by default. <code>logfire.configure()</code> runs automatically.</td>
</tr>
</table>

## Quickstart

`ArchetypeRuntime` is the recommended entry point. It owns the shared container, activates a world lazily on first use, and returns a real `entity_id` from `spawn()`.

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    dx: float = 0.0
    dy: float = 0.0


class MovementProcessor(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__dx"),
                "position__y": col("position__y") + col("velocity__dy"),
            }
        )


async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("demo", processors=[MovementProcessor()])

        await world.spawn(Position(x=0, y=0), Velocity(dx=1, dy=2))
        await world.run(steps=3)

        df = await world.query(Position)
        df.show()


asyncio.run(main())
```

For sync scripts, use `with ArchetypeRuntime.sync() as runtime:` and drop the `await`s.

Two things to know:

- Processor columns are flattened as `componentname__field` (e.g. `position__x`).
- `ArchetypeRuntime` is the script boundary. Drop to `ServiceContainer` only when you need explicit RBAC, custom command routing, or a non-script host.

## Concepts

<table>
<tr>
<td width="160"><strong>Component</strong></td>
<td>A typed <code>LanceModel</code>. Fields flatten to columns: <code>Health(hp, max_hp)</code> &rarr; <code>health__hp</code>, <code>health__max_hp</code>.</td>
</tr>
<tr>
<td><strong>Archetype</strong></td>
<td>The exact set of component types attached to an entity. Signatures are canonicalized by sorted type name.</td>
</tr>
<tr>
<td><strong>Processor</strong></td>
<td>A DataFrame transform selected by <em>subset match</em>. If an archetype has at least the required components, the processor runs on it.</td>
</tr>
<tr>
<td><strong>World</strong></td>
<td>Owns entity-to-archetype bookkeeping, pending mutation caches, hooks, and the live snapshot for the latest tick.</td>
</tr>
<tr>
<td><strong>Tick</strong></td>
<td>Four phases: query &rarr; materialize &rarr; execute &rarr; update. Each is its own Logfire span.</td>
</tr>
<tr>
<td><strong>Fork</strong></td>
<td>New <code>world_id</code> and <code>run_id</code>; preserves tick position; copies entity mappings, pending caches, and hooks at fork time.</td>
</tr>
<tr>
<td><strong>Command gate</strong></td>
<td><code>CommandService</code> authorizes, delegates (direct or tick-deferred via <code>CommandBroker</code>), and audits every external mutation.</td>
</tr>
<tr>
<td><strong>Roles</strong></td>
<td><code>viewer</code>, <code>player</code>, <code>operator</code>, <code>admin</code>. Permissions, per-tick quotas, and daily token budgets are enforced at the gate.</td>
</tr>
</table>

## Use Cases

Simulations where tick-by-tick history is part of the model:

- multi-agent worlds and societies
- counterfactual branches and forks
- rollout-heavy evaluation
- LLM-powered processors over thousands of entities

## CLI

The CLI is a thin HTTP client. Except for `serve`, every command talks to a running FastAPI server.

```bash
archetype serve                                          # start the server
archetype world create demo
archetype world list
archetype entity spawn <world-id> --components '[{"type":"Position","x":0,"y":0}]'
archetype run <world-id> --steps 10
archetype episode <world-id> --max-steps 100
archetype rollout <world-id> --num-episodes 4 --max-steps 100
archetype world fork <world-id> --name branch-a
archetype world destroy <world-id>                       # drop live world; storage and audit remain
archetype history <world-id>
```

Configure with `ARCHETYPE_URL` (default `http://localhost:8000`). Per-command flags: `--url`, `--role` / `-r` (`admin` | `operator` | `player` | `viewer`), `--token`, `--json`.

## REST API

`archetype serve` exposes a FastAPI app:

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/worlds` | Create a world |
| `GET` | `/worlds` | List worlds |
| `GET` | `/worlds/{world_id}` | Inspect one world |
| `DELETE` | `/worlds/{world_id}` | Destroy a live world |
| `POST` | `/worlds/{world_id}/fork` | Fork a world |
| `POST` | `/worlds/{world_id}/entities` | Spawn an entity |
| `DELETE` | `/worlds/{world_id}/entities/{entity_id}` | Despawn an entity |
| `PATCH` | `/worlds/{world_id}/entities/{entity_id}` | Update entity components |
| `POST` | `/worlds/{world_id}/entities/{entity_id}/components` | Add components |
| `DELETE` | `/worlds/{world_id}/entities/{entity_id}/components` | Remove components |
| `POST` | `/worlds/{world_id}/commands` | Submit one command |
| `POST` | `/worlds/{world_id}/commands/batch` | Submit multiple commands |
| `GET` | `/worlds/{world_id}/commands` | Audit-backed command history |
| `POST` | `/worlds/{world_id}/step` | Run one tick |
| `POST` | `/worlds/{world_id}/run` | Run multiple ticks |
| `POST` | `/worlds/{world_id}/episode` | Run one episode |
| `POST` | `/worlds/{world_id}/rollout` | Run a rollout |
| `GET` | `/worlds/{world_id}/processors` | List processors |
| `GET` | `/worlds/{world_id}/hooks` | List hooks |
| `GET` | `/worlds/{world_id}/resources` | List resources |
| `GET` | `/worlds/{world_id}/state` | Query world snapshot |
| `GET` | `/worlds/{world_id}/entities/{entity_id}` | Query one entity |
| `GET` | `/worlds/{world_id}/components` | Query component projections |
| `GET` | `/worlds/{world_id}/history` | Query audit history |

## Examples

```bash
uv run python examples/01_world_mutations.py
uv run python examples/02_fork_counterfactual.py
uv run python examples/03_time_travel.py
uv run python examples/04_messaging.py
uv run python examples/05_llm_agents.py             # requires OPENAI_API_KEY
uv run python examples/06_trajectory_analysis.py    # parts require OPENAI_API_KEY
uv run python examples/07_hooks.py
```

## Repository Layout

```text
archetype/
├── src/archetype/
│   ├── runtime/      # ArchetypeRuntime — recommended top-level API
│   ├── core/         # ECS primitives and storage contracts (Daft + Arrow + LanceDB)
│   ├── app/          # Service layer: command gate, broker, world/sim/query/storage services
│   ├── api/          # FastAPI server
│   └── cli/          # Typer CLI (thin HTTP client)
├── examples/         # Runnable examples
├── tests/            # Test suite
├── docs/             # MkDocs site
├── AGENTS.md         # Repo-specific collaborator guidance
└── LEARNINGS.md      # Daft patterns and architectural notes
```

## Status

- The core runtime and append-only write path are the most mature parts.
- The Python service layer is richer than the REST read models.
- The FastAPI layer currently uses a default admin `ActorCtx` — not multi-tenant auth yet.

Start with `ArchetypeRuntime`. Read `core/` and `app/` to understand how it works underneath.

## Development

```bash
make test        # fast test suite
make test-cov    # coverage run
make check       # format + lint
make ci          # CI gate
make docs        # build docs
```

Clone and bootstrap:

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

## Documentation

- Docs site: <https://archetype-docs.pages.dev>
- Examples index: [`examples/README.md`](examples/README.md)
- Specifications: [`docs/guide/runtime.md`](docs/guide/runtime.md), [`docs/guide/service-protocols.md`](docs/guide/service-protocols.md), [`docs/guide/command-gate.md`](docs/guide/command-gate.md), [`docs/guide/execution-hierarchy.md`](docs/guide/execution-hierarchy.md), [`docs/guide/world-lifecycle.md`](docs/guide/world-lifecycle.md), [`docs/guide/audit-log.md`](docs/guide/audit-log.md)
- Architecture notes: [`LEARNINGS.md`](LEARNINGS.md)

## Star History

<a href="https://www.star-history.com/?repos=VangelisTech%2Farchetype&type=date&legend=top-left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=VangelisTech/archetype&type=date&theme=dark&legend=top-left" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=VangelisTech/archetype&type=date&legend=top-left" />
    <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=VangelisTech/archetype&type=date&legend=top-left" />
  </picture>
</a>

## License

Apache 2.0 — see [`LICENSE`](LICENSE).
