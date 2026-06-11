# AutoResearch

AutoResearch is a pattern for autonomous software optimization: track a single branch head, evaluate candidate commits against it, and advance the head only when a run improves a user-defined metric. The shape — experiment, run, result, keep / discard / crash — follows Andrej Karpathy's framing of autonomous software optimization as a research direction.

**Status: the loop runs on the ledger.** The lifecycle components are implemented, and `AutoResearchService` records its own state as entities in a lab world — the loop is itself an archetype simulation.

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

## The Loop

`AutoResearchService.run(world_id, config, evaluator)` is the controller:
each iteration forks the base world, runs a rollout, hands the result to
your evaluator, and advances the head when the score beats the incumbent.
The service stays scoring-agnostic — the evaluator is yours: scalar,
Pareto, LLM judge, tournament, human vote.

**The loop's own state lives on the ledger.** Each experiment gets a lab
world named `autoresearch:{experiment_name}`, sharing the base world's
storage:

- **tick 0** — genesis: the `Experiment` and a seed `BranchHead`, persisted
  as raw initial conditions
- **every subsequent tick** — one iteration: a `Run` row, a `Result` row
  (score, episode world ids, full provenance), and the `BranchHead`
  advance when the iteration improved
- **resume is a read** — a second `run()` of the same experiment reads the
  incumbent from the latest `BranchHead` row and continues iteration
  numbering from the lab tick. There is no in-memory loop state to lose.

```python
result = await container.autoresearch_service.run(
    base_world_id,
    AutoResearchConfig(experiment_name="exp", max_iterations=10),
    evaluator=my_score_fn,
)

lab = container.world_service.get_world(result.lab_world_id)
heads = await lab.query_archetype(sig=(BranchHead,), ticks=[t])  # head at any tick
```

Because the lab world is an ordinary world, the experiment itself is
forkable: fork the lab at any tick and replay "what if a different run had
advanced the head." Contract tests: `tests/app/test_autoresearch_ledger.py`.

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
