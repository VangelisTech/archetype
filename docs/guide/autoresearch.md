# AutoResearch

AutoResearch is a pattern for autonomous software optimization: track a single branch head, evaluate candidate commits against it, and advance the head only when a run improves a user-defined metric. The shape — experiment, run, result, keep / discard / crash — follows Andrej Karpathy's framing of autonomous software optimization as a research direction.

**Status: in development.** The ECS-native lifecycle components are implemented. The loop controller that advances the branch head is not yet in this repo.

## What's Implemented

`archetype.experiments` models the lifecycle state as ordinary Components, so runs become entities in an archetype world — forkable, time-travelable, and queryable with the same tools as any other simulation state.

| Component | Role |
|---|---|
| `Experiment` | The setup for a family of runs: repo, branch, metadata. No scoring fields. |
| `Run` | A single attempt: one VM, one agent, one task, one commit. Mirrors `archetype-runner`'s record shape. |
| `Result` | Opaque eval envelope attached to a `Run`. User code decides the metric. |
| `BranchHead` | Persisted "current best commit" for an experiment, advanced by the user's loop. |

The library deliberately does not define what "better" means. `Result.outputs_json` and `BranchHead.descriptor_json` are free-form — a scalar metric, a Pareto point, an LLM judge verdict, a pytest report, a tournament record. The library persists; the user's eval code scores.

### Ingesting from archetype-runner

`archetype-runner` is a separate tool that executes coding agents in VMs and records agent runs to SQLite. Its records can be ingested into an archetype world row-for-row:

```python
from archetype.experiments import ingest_runner_state, load_runner_state_db

rows = load_runner_state_db("/path/to/runner/state.db")
await ingest_runner_state(world_id, rows, container)
```

After ingestion, runs are queryable as entities in the world — filter by `experiment_name`, join with `Result`, time-travel to a historical snapshot, or fork the world to explore "what if run X had won instead."

## What's In Development

The loop orchestration itself. A controller that:

- reads the current `BranchHead` for an experiment
- launches a bounded run against that commit
- waits for the user-defined evaluator to emit a `Result`
- compares the result to the incumbent using user-defined logic
- updates the `BranchHead` on improvement, otherwise leaves it

This is deliberately the last piece. The primitives are intentionally scoring-agnostic so the loop can be built against any comparison — scalar, Pareto, LLM judge, tournament, human vote — without the components having to encode a preference.

## Why ECS-Native

Modeling experiments as ECS state means:

- **Forking** — replay what would have happened if a different run had advanced the head
- **Time-travel queries** — inspect an experiment's state at any historical tick
- **Concurrency** — process many experiments as separate archetypes on the same engine
- **Audit** — every state transition is an appended row, not a mutation

Experiment state gets the same operational properties as any other simulation in Archetype, without a parallel storage layer.

## References

- Andrej Karpathy's framing of autonomous software optimization and branch-frontier agent workflows
- `src/archetype/experiments/` — the current component implementations
- `archetype-runner` — the agent-in-VM runner whose registry feeds this schema
