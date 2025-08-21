<div align="center">

# Archetype

AI‑Native Simulation Engine for data‑centric, secure, and scalable agent worlds.

<i>Powered by Daft (compute/dataframes), LanceDB/Iceberg (storage), optional Ray (distributed), and optional Unity Catalog (security).</i>

</div>

![Archetype Diagram](./assets/archetype_diagram.png)



## Why Archetype

Archetype is a simulation runtime and toolkit that treats your simulation as data:

- Entity‑Component data model with columnar storage and vectorized processing
- Async worlds and systems for high throughput and parallelism
- Pluggable storage (Iceberg by default; LanceDB supported) with partitioned, append‑only history
- Optional Unity Catalog integration for enterprise‑grade authorization and temporary credentials
- Built‑in instrumentation, profiling, and benchmarks

Archetype helps you move fast on agent behavior while keeping observability, reproducibility, and security first‑class.

## Core concepts

- `Component`: Typed, LanceModel‑backed records. Each component defines a prefixed Arrow schema.
- `Archetype` (signature): A tuple of component types; maps to a physical table.
- `AsyncWorld`: Owns entity → signature mapping, live snapshots, and tick execution.
- `AsyncSystem` + `AsyncProcessor`: Orchestrates ordered processors per signature.
- `AsyncStore`/`AsyncQueryManager`/`AsyncUpdateManager`: Storage facades.
- `WorldOrchestrator`: Creates worlds, coordinates parallel runs.
- `RunConfig`/`WorldConfig`/`StorageConfig`: Configuration models. `StorageConfig.uri` accepts `str | pathlib.Path`. Unity Catalog lives under `StorageConfig.uc_config`.

## Quick start

Install in editable mode with all dev tooling:

```bash
make dev-install
```

Run the minimal pyglet demo (PYTHONPATH pre‑set and data dir created automatically):

```bash
# Optional: customize data directory used by examples/benchmarks
ARCHETYPE_DATA_DIR=/tmp/archetype_data make run-example EX=examples/pyglet_example.py
```

Programmatic minimal example:

```python
from pathlib import Path
from archetype.core import WorldConfig, RunConfig, StorageConfig
from archetype.core.aio import AsyncSystem
from archetype.core.orchestrator import WorldOrchestrator

storage = StorageConfig(uri=Path("./archetype_data"), namespace="quickstart")
orch = WorldOrchestrator()
world = await orch.create_world(WorldConfig(name="demo"), AsyncSystem(), storage)
await orch.run_world(world.world_id, RunConfig(num_steps=1))
```

## Unity Catalog (optional, enterprise)

Archetype can enforce authorization and attach short‑lived credentials around reads/writes via UC wrappers.

- Configure UC via nested `StorageConfig.uc_config`:

```python
from archetype.core.config import StorageConfig, UnityCatalogConfig

storage = StorageConfig(
  uri="./archetype_data",
  namespace="archetypes",
  uc_config=UnityCatalogConfig(
    enabled=True,
    enforce_permissions=True,
    endpoint="http://localhost:8080/api/2.1/unity-catalog",
    token="<admin-or-user-token>",
    catalog="unity",
    schema="default",
  ),
)
```

- Bootstrap a local UC (catalog/schema and basic grants) if you don’t have one:

```bash
make uc-bootstrap UC_ENDPOINT=http://localhost:8080/api/2.1/unity-catalog \
                 UC_ADMIN_TOKEN=$(make uc-admin-token) \
                 UC_CATALOG=unity UC_SCHEMA=default USER_EMAIL=you@example.com
```

## Instrumentation & Profiling

- Structured logging and an instrumented world/system/querier/updater live under `src/archetype/core/instrumentation/`.
- Quick profiling run with VizTracer:

```bash
make profile CMD="pytest -q tests/app/test_simulation_service.py"
```

## Benchmarks

- Command broker throughput:

```bash
make bench-broker
```

This prints enqueue/dequeue TPS for SPSC and MPMC patterns. You can adjust loads inside `bench/app/bench_command_broker.py`.

- Storage/processing sweeps live under `tests/benchmarks/` and can be invoked directly (plots optional).

## Make targets

- `make dev-install` – install package in editable mode
- `make test` – run tests
- `make run-example EX=…` – run any example with PYTHONPATH pre‑set
- `make bench-broker` – run broker benchmark
- `make uc-bootstrap` – create catalog/schema and grant basic permissions
- `make profile CMD="…"` – run a command under VizTracer
- `make build` – build wheel/sdist
- `make publish` – upload to PyPI (requires `PYPI_TOKEN`)

## Project layout

```
src/archetype/
  core/            # ECS core, async world/system, storage facades
  app/             # Application layer (services, broker, auth, UC wrappers)
  api/             # REST API scaffolding (WIP)
  integrations/    # External services (e.g., Unity Catalog client glue)
  benchmarks/      # Core iteration micro-benchmarks (to be moved to /bench)
tests/
  app/             # Service/world tests
  aio/ core/ ...   # Core async/store tests
  benchmarks/      # Broker & storage sweep benches
examples/          # Pyglet/pygame + service examples
scripts/           # Utilities (e.g., uc_bootstrap)
```

## Contributing

PRs welcome! Please:

- Match the code style (typed, readable, guard clauses, avoid deep nesting)
- Add tests for new behavior
- Keep performance/allocations in mind in hot paths

## License

Apache 2.0 © Vangelis Technologies Inc.

## Roadmap (high‑level)

- API module (FastAPI) routes with end‑to‑end tests
- Batch world mutations and per‑tick UC set‑based checks
- Top‑level `bench/` consolidation with JSON/plot outputs
- Temp‑credential lifecycle (refresh) and richer logging panels

