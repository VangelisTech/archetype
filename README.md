# Archetype

[![CI](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml/badge.svg)](https://github.com/VangelisTech/archetype/actions/workflows/python-tests.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Archetype is a dataframe-first ECS runtime for simulations and agent workflows.
Define state with components, transform populations with processors, and keep
each tick as queryable history. Use a fork to continue from an earlier state
without overwriting the original run.

It is built on [Daft](https://www.daft.ai/) and LanceDB. The default Python
entry point is `ArchetypeRuntime`; HTTP services and the CLI use the same
command layer when you need a multi-user host.

## Install

```bash
pip install archetype-ecs
```

For a checkout, install the development environment with `make sync-dev`.

## Run a simulation

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


class Move(AsyncProcessor):
    components = (Position, Velocity)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__dx"),
                "position__y": col("position__y") + col("velocity__dy"),
            }
        )


async def main() -> None:
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("demo", processors=[Move()])
        await world.spawn(Position(), Velocity(dx=1, dy=2))
        await world.run(steps=3)

        history = await world.query(Position)
        print(history.collect().to_pylist())


asyncio.run(main())
```

For a regular script without `async`, use `with ArchetypeRuntime.sync() as
runtime:` and omit `await`.

## What it gives you

- Columnar processors run one DataFrame transform over every matching entity.
- Every tick is append-only, so historical reads are ordinary queries.
- Forks inherit source history and create an independent future.
- The service layer can authorize and audit mutations before a tick applies
  them.

## Documentation

Start with the [quickstart](https://archetype.vangelis.tech/docs/guide/quickstart/),
then use the guides for [components](https://archetype.vangelis.tech/docs/guide/components/),
[processors](https://archetype.vangelis.tech/docs/guide/processors/), and
[worlds](https://archetype.vangelis.tech/docs/guide/working-with-worlds/).

The site also includes the current [Python API](https://archetype.vangelis.tech/docs/reference/python-api/),
[CLI](https://archetype.vangelis.tech/docs/reference/cli/), and
[REST API](https://archetype.vangelis.tech/docs/reference/rest-api/) references.

Runnable examples live in [`examples/`](examples/README.md). Most run without
credentials:

```bash
uv run python examples/01_world_mutations.py
uv run python examples/02_fork_counterfactual.py
uv run python examples/03_time_travel.py
uv run python examples/04_messaging.py
uv run python examples/07_hooks.py
```

`examples/05_llm_agents.py` and parts of `examples/06_trajectory_analysis.py`
require `OPENAI_API_KEY`.

## Development

```bash
make sync-dev  # install development dependencies
make test      # run the fast test suite
make check     # format and lint
make docs      # generate references and build the docs site
make ci        # run the merge gate
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing the engine. The
normative contracts are under [`docs/guide/`](docs/guide/specification.md).

## Status

Archetype is alpha software. The append-only world, history, and fork paths
are the most mature parts of the project. The HTTP layer uses development-mode
authentication by default; supply your own authentication before exposing it
to untrusted users.

## License

Apache-2.0. See [LICENSE](LICENSE).
