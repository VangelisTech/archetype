# archetype-missions

> **Retired.** Part of the [Archetype](https://github.com/VangelisTech/archetype)
> project, which is no longer under active development. The 0.6 line remains
> installable; no further releases are planned.

The separately installable Coding-Agent Missions world library for Archetype.

```bash
uv add archetype-missions
```

`archetype.missions.Missions` provided workflow-scoped author/critic execution
and `archetype.missions.MissionWorld` provided transcript and trajectory
evidence. Both adapters were imported from `archetype.missions`; installing
the library did not add Missions methods or values to the generic `archetype`
runtime surface.

The agent-facing interface was the Mission MCP server, installed as
`archetype-missions-mcp` (`python -m archetype.missions.mcp`). ACP-capable
clients used that stdio server; Archetype never shipped an ACP implementation.
See the repository [README](https://github.com/VangelisTech/archetype) for the
retirement notice and the historical tool surface.
