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

## Python API

```python
import asyncio
from daft import DataFrame, col
from archetype.core.component import Component
from archetype.dsl import World, behavior

class Agent(Component):
    name: str = ""
    skill: float = 1.0
    experience: float = 0.0

@behavior
class GainExperience:
    requires = [Agent]
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column(
            "agent__experience",
            col("agent__experience") + col("agent__skill") * 2.0,
        )

async def main():
    async with World("my-sim") as world:
        world.add_behavior(GainExperience)
        await world.spawn(Agent(name="Alice", skill=3.0))
        await world.spawn(Agent(name="Bob", skill=2.0))
        await world.run(ticks=10)

        for agent in world.agents:
            print(f"{agent.name}: exp={agent.experience}")

asyncio.run(main())
```

## CLI

```bash
archetype serve
archetype world create my-sim
archetype run <world-id> --steps 100
archetype query <world-id>
archetype world fork <world-id> --name branch-A
archetype query <world-id> --tick 42
```

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
