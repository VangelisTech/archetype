# Archetype docs

This folder is the living documentation for the `archetype` Python package.

If you’re new, start with the guides. The `design/` folder contains historical design notes and long-form decision records.

## Guides (recommended)

- `guide/quickstart.md`: install + first world + first processor
- `guide/architecture.md`: how the runtime fits together + code map
- `guide/core-concepts.md`: ECS-as-data (components, archetypes, processors, worlds, ticks)
- `guide/storage.md`: time-travel storage model (world/run/tick/entity) + LanceDB/Iceberg notes
- `guide/episodes.md`: episodes + trajectory collection
- `guide/grpo.md`: GRPO pipeline + rollout artifact contract + “weights as data”
- `guide/mcp.md`: MCP server and tool surface
- `guide/datasets.md`: dataset builders (image understanding curation job)
- `guide/glossary.md`: shared vocabulary

## Design notes (historical / in-progress)

- `weights-as-data-discovery.md`: original “weights as data” breakthrough writeup
- `rl-architecture-decisions.md`: decision record for RL architecture
- `simulation_scripting.md`: (legacy) scripting patterns; see `guide/quickstart.md` for the current API
- `design/`: broader architecture notes and acceptance criteria
