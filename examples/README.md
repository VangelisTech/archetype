# Examples

Runnable examples demonstrating Archetype's core features. Each example is self-contained and numbered to match the recommended onboarding order.

```bash
uv run python examples/<filename>.py
```

| # | Example | Description | Requires |
|---|---------|-------------|----------|
| 0 | [`00_quickstart.py`](00_quickstart.py) | Smallest complete component + processor + runtime simulation | None |
| 1 | [`01_world_mutations.py`](01_world_mutations.py) | Trusted actor-free mutations: spawn/despawn, update, component and processor changes, fork, and history | None |
| 2 | [`02_fork_counterfactual.py`](02_fork_counterfactual.py) | Butterfly effect: fork a branch, nudge it by 1e-9, and diff the two append-only histories with a single join — three dynamical regimes, three fates | None |
| 3 | [`03_time_travel.py`](03_time_travel.py) | Rewind to any past tick by filtering the `tick` column, then fork a counterfactual branch and diff it against the source | None |
| 4 | [`04_messaging.py`](04_messaging.py) | Agent-to-agent messaging via an application-local mailbox resource, priority-ordered processors, and lifecycle hooks | None |
| 5 | [`05_llm_agents.py`](05_llm_agents.py) | LLM-powered agents — each entity gets a parallel LLM call every tick via `daft.functions.prompt` | `OPENAI_API_KEY` |
| 6 | [`06_trajectory_analysis.py`](06_trajectory_analysis.py) | Persist, select, and grade normalized mission trajectory evidence | None |
| 7 | [`07_hooks.py`](07_hooks.py) | Lifecycle hooks for audit logs, tick metrics, and temporary debug traces | None |
| 8 | [`08_htn_resolution.py`](08_htn_resolution.py) | HTN plan resolution as a fan-out AND/OR forest | None |
| 9 | [`09_cloud_storage.py`](09_cloud_storage.py) | Cloud storage configurations through `StorageConfig` and the runtime API | Optional cloud credentials |
| 10 | [`10_autoresearch.py`](10_autoresearch.py) | Multi-run autoresearch through the runtime workflow | None |
| 11 | [`11_coding_agent_mission.py`](11_coding_agent_mission.py) | Typed coding-agent task graph with validators, the supported Modal end-to-end backend, checkpoints, and direct Modal monitoring; Apple Container/Docker are sandbox capabilities only, rejected at mission admission | Codex subscription + GitHub + Modal credentials; dry-run needs none |
| 11 | [`11_graph_relationships.py`](11_graph_relationships.py) | Edge entities: build a hierarchy, traverse it, read the graph at an earlier tick, cascade after a despawn | None |
| 12 | [`12_prefabs.py`](12_prefabs.py) | PreFabs: author a template with a subtree, instantiate copies with overrides and IsA lineage, upgrade by re-instantiation | None |
| 13 | [`13_biome_rts.py`](13_biome_rts.py) | Biome-inspired prefab asset library composed into an RTS command hierarchy, minimap, fog of war, and possessed-unit view | None |
| 14 | [`14_biome_agent.py`](14_biome_agent.py) | Agent observes and controls Sander Mertens' actual Biome game, then records native Drill evidence in Archetype | Running Biome/Flecs REST, or `--launch` with git, CMake, and a C toolchain |
| 15 | [`15_mission_factory_assets.py`](15_mission_factory_assets.py) | ECS-authored software factory: instantiate a `BugFixLine`, compile its durable rules into real Agent Missions values, and export nine AI-ready 3D briefs | None |

## Supplementary

| Example | Description |
|---------|-------------|
| [`pr_triage.py`](pr_triage.py) | PR triage agent that dogfoods Archetype |
| [`simulation_script.py`](simulation_script.py) | Standalone simulation script for quick prototyping |
