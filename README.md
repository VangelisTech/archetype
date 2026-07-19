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

The example below steps a chaotic function for a few ticks, forks the world,
and changes one value in the fork by 1e-9. Both branches then run forward, and
the divergence between them comes from joining the two stored histories tick
by tick. Every tick persists as immutable rows, so comparing two runs is a
query over history, not a second experiment. The optional last stage hands the
measured divergence to an LLM agent for review.

```python
import asyncio
import os

from daft import DataFrame, col
from daft.functions import prompt

from archetype import ArchetypeRuntime, AsyncProcessor, Component


class Node(Component):
    r: float = 3.9999  # chaotic regime of the logistic map
    x: float = 0.5


class LogisticMap(AsyncProcessor):
    components = (Node,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        x = col("node__x")
        return df.with_column("node__x", col("node__r") * x * (1.0 - x))


class Analyst(Component):
    evidence: str = ""
    verdict: str = ""


class Review(AsyncProcessor):
    components = (Analyst,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        question = (
            "Two runs of one simulation diverged after a 1e-9 nudge: "
            + col("analyst__evidence")
            + "\nIn one sentence: what kind of system is this?"
        )
        return df.with_column("analyst__verdict", prompt(question, model="gpt-5-mini"))


async def main() -> None:
    async with ArchetypeRuntime() as runtime:
        prime = runtime.world("prime", processors=[LogisticMap()])
        node = await prime.spawn(Node())
        await prime.step()  # tick 0 persists the raw initial conditions
        await prime.run(steps=12)

        # Fork the world; change one value in the fork by 1e-9.
        last = (await prime.info()).tick - 1
        x = (await prime.query(Node)).where(col("tick") == last).to_pylist()[0]["node__x"]
        fork = await prime.fork("nudged")
        await fork.update(node, Node(x=x + 1e-9))

        await prime.run(steps=24)
        await fork.run(steps=25)  # the update persists first; processors apply next tick

        # Compare the two runs by joining their stored histories tick by tick.
        a = (await prime.query(Node)).select(
            (col("tick") - last).alias("k"), col("node__x").alias("a")
        )
        b = (await fork.query(Node)).select(
            (col("tick") - last - 1).alias("k"), col("node__x").alias("b")
        )
        deltas = (
            a.join(b, on="k")
            .where(col("k") >= 0)
            .with_column("delta", (col("a") - col("b")).abs())
            .sort("k")
            .to_pylist()
        )
        print("  ".join(f"k={row['k']}: {row['delta']:.0e}" for row in deltas[::6]))

        # Optional: an LLM agent reviews the divergence. Its verdict is stored
        # as world state like everything else.
        if os.getenv("OPENAI_API_KEY"):
            analyst = runtime.world("analyst", processors=[Review()])
            evidence = ", ".join(f"tick {row['k']}: {row['delta']:.1e}" for row in deltas)
            await analyst.spawn(Analyst(evidence=evidence))
            await analyst.run(steps=2)  # spawned rows persist first; Review runs next tick
            report = await analyst.query(Analyst)
            print(report.where(col("tick") == 1).to_pylist()[0]["analyst__verdict"])


asyncio.run(main())
```

```text
k=0: 1e-09  k=6: 3e-08  k=12: 1e-06  k=18: 3e-04  k=24: 2e-02
```

The nudge doubles every tick. The agent stage is optional — without
`OPENAI_API_KEY` the script prints the divergence and stops. The full
three-regime version of the counterfactual lives in
[`examples/02_fork_counterfactual.py`](examples/02_fork_counterfactual.py);
richer agent patterns in [`examples/05_llm_agents.py`](examples/05_llm_agents.py).

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
