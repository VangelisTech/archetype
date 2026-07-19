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

The world below steps the logistic map:

```math
x_{t+1} = r \, x_t \, (1 - x_t)
```

At `r = 3.9999` the map is chaotic: nearby trajectories separate
exponentially. One world runs. A fork takes a 1e-9 nudge. A join over the two
histories measures the separation. The blocks form one script.

### State is components

```python
import asyncio

from daft import DataFrame, col

from archetype import ArchetypeRuntime, AsyncProcessor, Component


class Node(Component):
    r: float = 3.9999
    x: float = 0.5
```

Components are typed state. Fields become Arrow columns; entities that share
a component set share a table.

### Behavior is processors

```python
class LogisticMap(AsyncProcessor):
    components = (Node,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        x = col("node__x")
        return df.with_column("node__x", col("node__r") * x * (1.0 - x))
```

A processor is one DataFrame transform, applied every tick to every entity
that has the declared components.

### Agents are state plus behavior

```python
import os

from daft.functions import prompt


class Analyst(Component):
    evidence: str = ""
    verdict: str = ""


class Review(AsyncProcessor):
    components = (Analyst,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        ask = "In one sentence, what does this divergence imply? " + col("analyst__evidence")
        return df.with_column("analyst__verdict", prompt(ask, model="gpt-5-mini"))
```

An agent is a component for its state and a processor for its behavior. The
model call is a columnar expression; its output persists like any other
state.

### A counterfactual is a join

```python
async def divergence(prime, fork, since: int) -> list[dict]:
    base = (await prime.query(Node)).select("tick", "node__x")
    nudged = (await fork.query(Node)).select(
        (col("tick") - 1).alias("tick"), col("node__x").alias("nudged")
    )
    return (
        base.join(nudged, on="tick")
        .where(col("tick") >= since)
        .with_column("delta", (col("node__x") - col("nudged")).abs())
        .sort("tick")
        .to_pylist()
    )
```

Every tick persists as immutable rows, and a fork inherits its source's
history. Comparing two runs is a query, not a re-run. An update persists
before processors resume, so the fork writes one tick behind its source; its
ticks shift by one in the join.

### A world composes them

```python
async def main() -> None:
    async with ArchetypeRuntime() as runtime:
        # simulate
        prime = runtime.world("prime", processors=[LogisticMap()])
        node = await prime.spawn(Node())
        await prime.run(steps=13)

        # fork and nudge
        x12 = (await prime.query(Node)).where(col("tick") == 12).to_pylist()[0]["node__x"]
        fork = await prime.fork("nudged")
        await fork.update(node, Node(x=x12 + 1e-9))
        await prime.run(steps=24)
        await fork.run(steps=25)

        # measure
        deltas = await divergence(prime, fork, since=12)
        print("  ".join(f"t{r['tick']}: {r['delta']:.0e}" for r in deltas[::6]))

        # review
        if os.getenv("OPENAI_API_KEY"):
            analyst = runtime.world("analyst", processors=[Review()])
            await analyst.spawn(Analyst(evidence=", ".join(f"{r['delta']:.0e}" for r in deltas)))
            await analyst.run(steps=2)
            report = (await analyst.query(Analyst)).where(col("tick") == 1)
            print(report.to_pylist()[0]["analyst__verdict"])


asyncio.run(main())
```

```text
t12: 1e-09  t18: 3e-08  t24: 1e-06  t30: 3e-04  t36: 2e-02
```

The gap doubles each tick — the map's Lyapunov exponent, ln 2, read from the
ledger. Without `OPENAI_API_KEY` the script prints the divergence and skips
the agent.
[`examples/02_fork_counterfactual.py`](examples/02_fork_counterfactual.py)
runs three regimes;
[`examples/05_llm_agents.py`](examples/05_llm_agents.py) shows richer agent
patterns.

For a regular script without `async`, use `with ArchetypeRuntime.sync() as
runtime:` and omit `await`.

## What it gives you

- Columnar processors run one DataFrame transform over every matching entity.
- Every tick is append-only, so historical reads are ordinary queries.
- Forks inherit source history and create an independent future.
- Agents are entities: an LLM call is one more columnar processor writing to
  the same history.
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
