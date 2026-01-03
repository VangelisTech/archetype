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

## Mintlify (local preview)

From `archetype/`:

- `make docs`: build docs (`mint build`)
- `make docs-serve`: local server (`mint dev`)
- `make docs-test`: broken link check (`mint broken-links`)

Notes:

- Mintlify expects a `mint.json` in this folder (`archetype/docs/mint.json`).
- Mintlify depends on `sharp`, which requires Node `^18.17.0 || ^20.3.0 || >=21.0.0` (so Node `20.0.x` will fail).
- If you have Bun installed, the `make docs*` targets will prefer `bunx mint ...` over `npx mint ...` (Mintlify officially targets Node, but Bun often works in practice).
