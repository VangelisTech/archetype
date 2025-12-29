<div align="center">

# Archetype

Data-centric simulation engine for scalable agent worlds.

<i>Powered by Daft (dataframes/compute) + LanceDB (storage) + PyTorch (training).</i>

</div>

![Archetype Diagram](./assets/archetype_diagram2.png)

Archetype is an **AI-native simulation engine**: an ECS runtime where *world state is a columnar table* (Daft DataFrames / Arrow) and each tick is an append-only write to storage (LanceDB). This gives you:

- **Simulation as data**: processors are pure DataFrame transforms (lazy until execution)
- **Time-travel state**: query any `tick`/`run_id`/`world_id`
- **Parallel worlds and episodes**: async execution primitives for rollouts
- **RL-friendly artifact contracts**: rollouts emit token IDs + per-token logprobs (no retokenization)
- **“Weights as data”**: training steps write new checkpoints and pass paths forward

If you want the shortest path to “does this work?”, run the end-to-end GRPO example: `examples/grpo_text_end_to_end.py`.

## Install

Archetype targets **Python 3.12**.

From this repo:

```bash
cd archetype

# Option A: uv (recommended; matches repo tooling)
uv sync

# Option B: pip (editable)
python -m pip install -e .
```

Optional extras:

- `pip install -e ".[inference]"` for vLLM rollouts
- `pip install -e ".[dev]"` for tests/lint tooling

## Quick start (sync world)

The sync API is the quickest way to prototype processors:

```python
import archetype
from archetype import Component, Processor
from daft import DataFrame, col


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    vx: float
    vy: float


class Movement(Processor):
    components = (Position, Velocity)
    priority = 1

    def process(self, df: DataFrame, dt: float = 0.1) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx") * dt,
                "position__y": col("position__y") + col("velocity__vy") * dt,
            }
        )


sim = archetype.init("./archetype_data")
world_id = sim.spawn_world("physics")
sim.add_processor_to_world(world_id, Movement())
sim.spawn_entity(world_id, Position(x=0, y=0), Velocity(vx=1, vy=1))
sim.step_world(world_id, dt=0.1)
```

## Quick start (async, multi-world)

For parallel rollouts, use the application layer:

```python
import asyncio
from archetype.app import ArchetypeApp


async def main() -> None:
    app = await ArchetypeApp.create(storage_uri="./archetype_data")
    world = await app.create_world("rollouts")
    await app.run_world(world.world_id, steps=10)
    await app.shutdown()


asyncio.run(main())
```

## RL: GRPO building blocks (rollouts + training)

Archetype includes a small, explicit GRPO toolkit under `archetype.rl.grpo`:

- **Rollouts**: `rollout_transformers.py` (CPU-friendly dev) and `rollout_vllm.py` (fast inference)
- **Pipeline glue**: `pipeline.py` builds a Daft DataFrame with GRPO artifacts
- **Training**: `train_udf.py` is a “weights as data” trainer UDF; `pytorch_grpo.py` contains loss helpers

Run the end-to-end demo:

```bash
cd archetype
PYTHONPATH=src uv run python examples/grpo_text_end_to_end.py
```

## Datasets: image understanding curation

There’s an Archetype-native dataset job that curates less-biased VLM multiple-choice samples via ablations + structured judges:

```bash
cd archetype
PYTHONPATH=src uv run python examples/build_image_understanding_dataset.py --limit 50
```

## MCP server (agent-native simulation control plane)

Archetype exposes world management via MCP:

```bash
cd archetype
python -m archetype.mcp
```

## Docs

- Start here: `docs/README.md`
- Architecture overview: `docs/guide/architecture.md`
- Glossary: `docs/guide/glossary.md`
- Design notes (historical): `docs/design/`
