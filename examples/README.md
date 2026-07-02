# Examples

Runnable examples demonstrating Archetype's core features. Each example is self-contained and numbered to match the recommended onboarding order.

```bash
uv run python examples/<filename>.py
```

| # | Example | Description | Requires |
|---|---------|-------------|----------|
| 1 | [`01_world_mutations.py`](01_world_mutations.py) | Every mutation type: spawn, despawn, add_processor, RBAC, fork, audit history | None |
| 2 | [`02_fork_counterfactual.py`](02_fork_counterfactual.py) | Fork a world three times, run each branch, compare results | None |
| 3 | [`03_time_travel.py`](03_time_travel.py) | Rewind to any past tick by filtering the `tick` column, then fork a counterfactual branch and diff it against the source | None |
| 4 | [`04_messaging.py`](04_messaging.py) | Agent-to-agent messaging via tick-deferred `MESSAGE` commands, resources, and lifecycle hooks | None |
| 5 | [`05_llm_agents.py`](05_llm_agents.py) | LLM-powered agents — each entity gets a parallel LLM call every tick via `daft.functions.prompt` | `OPENAI_API_KEY` |
| 6 | [`06_trajectory_analysis.py`](06_trajectory_analysis.py) | Trajectory analysis — ingest, label, and compare agent trajectories using world forking | Optional: `OPENAI_API_KEY` |
| 7 | [`07_hooks.py`](07_hooks.py) | Lifecycle hooks for audit logs, tick metrics, and temporary debug traces | None |
| 8 | [`08_htn_resolution.py`](08_htn_resolution.py) | HTN plan resolution as a fan-out AND/OR forest | None |
| 9 | [`09_cloud_storage.py`](09_cloud_storage.py) | Cloud storage configurations through `StorageConfig` and the runtime API | Optional cloud credentials |

### Supplementary

| Example | Description |
|---------|-------------|
| [`pr_triage.py`](pr_triage.py) | PR triage agent that dogfoods Archetype |
| [`simulation_script.py`](simulation_script.py) | Standalone simulation script for quick prototyping |
