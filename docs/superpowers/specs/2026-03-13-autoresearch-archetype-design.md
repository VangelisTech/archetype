# AutoResearch-on-Archetype Design

## Goal

Define the minimal `Archetype v0.1` surface needed to express Karpathy-style autonomous software optimization: a tracked branch frontier, experiments against that frontier, bounded runs, recorded results, and keep/discard branch advancement semantics.

## Design Summary

`Archetype v0.1` should model an `AutoResearch` loop as an experiment engine over a single tracked branch head:

`branch head -> experiment -> run -> result -> keep|discard|crash -> maybe advance branch head`

The design intentionally stays close to Karpathy's terminology:

- `experiment`: the hypothesis under test
- `run`: the bounded execution of that experiment
- `commit`: the concrete git state associated with the experiment
- `branch`: the tracked frontier path
- `result`: the metrics emitted by the run
- `keep` / `discard` / `crash`: the selection outcomes

## Architectural Decisions

### 1. World-per-experiment

For `v0.1`, the preferred shape is one world per experiment, not a single world containing many competing experiments.

Why:

- It matches `autoresearch`'s branch-local evaluation loop closely.
- It uses Archetype's strongest existing capability: isolated worlds with bounded runs.
- It keeps higher-order experiment selection outside ordinary per-tick processors.
- It preserves a clean path to richer multi-world search later.

### 2. One tracked branch head

For `v0.1`, each autonomous loop tracks exactly one authoritative branch head.

- The frontier does not live on `main`.
- The frontier lives on the dedicated optimization branch, like `autoresearch/<tag>`.
- Every new experiment starts from the current tracked branch head.
- A kept experiment advances that branch head.
- A discarded or crashed experiment leaves the branch head unchanged.

This mirrors `autoresearch` directly while leaving merge-back-to-main policy out of scope.

### 3. Git-aware, but not git-driven core

Archetype should understand git coordinates and frontier semantics, but should not turn the core engine into a shell wrapper.

Framework-owned responsibilities:

- repository, branch, and commit identity
- tracked branch-head state
- experiment/run/result state machine
- frontier comparison rules
- branch-head advancement decisions

App-layer responsibilities:

- checkout and worktree materialization
- patch application
- `git commit`
- rollback/reset/cleanup
- launching concrete training or evaluation commands

This yields the intended boundary:

**Archetype models and decides; the app layer materializes and executes.**

### 4. Transactional git adapter in the app layer

The git side effects should live in a contained app-layer module that behaves like a transactional adapter, not just a utility helper.

The adapter should own a small transaction boundary:

1. resolve tracked branch head
2. materialize checkout/worktree
3. apply experiment change
4. commit or rollback
5. emit resulting git coordinates back to Archetype

The important safety properties are:

- idempotence
- serialization per tracked branch
- crash recovery with enough journal state to reconcile partial progress

## Core Model

The minimum persistent conceptual model is:

- `Repository`
- `BranchHead`
- `Commit`
- `Experiment`
- `Run`
- `Result`

### Repository

Identifies the repo under optimization.

Suggested fields:

- `repository_id`
- `canonical_path` or remote URL
- `default_branch`

### BranchHead

Represents the single tracked frontier for the active loop.

Suggested fields:

- `repository_id`
- `branch_name`
- `current_commit_hash`
- `frontier_metric_name`
- `frontier_metric_direction` (`min` or `max`)
- `frontier_metric_value`

### Commit

Represents a concrete git state.

Suggested fields:

- `repository_id`
- `branch_name`
- `commit_hash`
- `parent_commit_hash`
- `message`
- `created_at`

For `v0.1`, this can start as a thin record over git facts while remaining first-class in the model.

### Experiment

Represents the proposed advancement of the tracked frontier.

An experiment is composed of two layers:

- a git layer, which identifies the software state being tested
- a runtime layer, which declares how Archetype should instantiate and evaluate that state

An `Experiment` is therefore not itself a `World` or a `RunConfig`. Instead, it carries the declarative recipe needed to create a world and derive a concrete run configuration at execution time.

Suggested fields:

- `experiment_id`
- `repository_id`
- `branch_name`
- `base_commit_hash`
- `proposal_summary`
- `world_spec_json`
- `run_spec_json`
- `evaluation_spec_json`
- `status`
- `created_at`

Suggested meanings:

- `world_spec_json`: how to instantiate the world for this experiment
- `run_spec_json`: how to derive the concrete `RunConfig` for execution
- `evaluation_spec_json`: how to interpret the resulting metrics and compare against the frontier

### Canonical spec shapes for v0.1

For `v0.1`, these specs should start with a small canonical shape instead of unconstrained blobs.

Suggested `world_spec_json`:

```json
{
  "world_name": "experiment-world",
  "storage": {
    "uri": "./archetype_data",
    "namespace": "archetypes",
    "backend": "lancedb"
  },
  "cache": {
    "flush_rows": 1000000,
    "flush_mb": 512,
    "idle_sec": 30.0
  },
  "resources": {},
  "metadata": {}
}
```

Suggested `run_spec_json`:

```json
{
  "budget_kind": "steps",
  "budget_value": 1,
  "debug": false,
  "prefer_live_reads": true,
  "show_rows": 0,
  "suite": "autoresearch",
  "trial": null,
  "metadata": {}
}
```

Suggested `evaluation_spec_json`:

```json
{
  "primary_metric_name": "val_bpb",
  "direction": "min",
  "secondary_metric_names": [
    "peak_vram_mb",
    "training_seconds",
    "total_seconds"
  ],
  "crash_is_failure": true,
  "metadata": {}
}
```

The intent is not to freeze these forever, only to keep the first implementation typed and interoperable.

### Run

Represents one bounded execution of an experiment.

Suggested fields:

- `run_id`
- `experiment_id`
- `world_id`
- `status`
- `budget_type`
- `budget_value`
- `started_at`
- `finished_at`
- `artifact_uri` or log reference

### Result

Represents the metrics and terminal outcome of a run.

Suggested fields:

- `result_id`
- `experiment_id`
- `run_id`
- `primary_metric_name`
- `primary_metric_value`
- `primary_metric_direction`
- `secondary_metrics_json`
- `runtime_metadata_json`
- `failure_metadata_json`

## State Machine

### Experiment states

- `pending`
- `running`
- `succeeded`
- `crashed`
- `kept`
- `discarded`

### Run states

- `pending`
- `running`
- `completed`
- `crashed`
- `timed_out`

### Transition rules

1. Create an `Experiment` from the current tracked `BranchHead`.
2. Create a `Run` for that experiment.
3. Move both to `running`.
4. If execution fails: `Run -> crashed|timed_out`, `Experiment -> crashed`.
5. If execution completes: `Run -> completed`, `Experiment -> succeeded`.
6. Compare the `Result` to the tracked branch frontier.
7. If the result advances the frontier: `Experiment -> kept` and advance `BranchHead`.
8. Otherwise: `Experiment -> discarded` and leave `BranchHead` unchanged.

Important distinction:

- `succeeded` means the run completed successfully.
- `kept` means the experiment improved the tracked frontier.

## Run and Result Contract

The intended relationship between experiment-time and runtime primitives is:

- `Experiment`: declarative definition of what to test and how to instantiate the runtime
- `World`: instantiated runtime environment for that experiment
- `RunConfig`: concrete execution budget/config derived from the experiment's `run_spec`
- `Run`: realized execution record for that world and run config

For `v0.1`, a run is a bounded execution of exactly one experiment against one concrete repository/branch/base-commit tuple.

A result must provide:

- exactly one designated primary frontier metric
- the direction of comparison (`min` or `max`)
- optional secondary metrics
- runtime and cost metadata
- failure metadata when relevant

For `autoresearch`, the primary metric is `val_bpb`, but the engine should not hard-code that metric name.

## Boundary with Existing Archetype Layers

### Core

Core remains the sacred runtime substrate: tick execution, world stepping, update/query semantics, persistence.

### App

The app layer is the correct home for:

- the transactional git adapter
- experiment orchestration
- bounded run execution against concrete repos
- reference `autoresearch`-style controller logic

### DSL

DSL support is explicitly out of scope for `v0.1`.

The goal is to first make the experiment system correct and minimal without depending on additional DSL sugar.

## Explicit Non-Goals for v0.1

- no merge-back-to-main policy
- no multi-branch frontier racing
- no population-wide search semantics
- no RL rollout or trajectory terminology in the public model
- no requirement to embed git shelling directly into core
- no requirement to expose the first implementation through the DSL

## Open Questions

These remain intentionally flexible:

1. Which model records must be fully persisted in Archetype tables from day one?
2. How thin or rich should the `Commit` record be initially?
3. Should bounded runs support only wall-clock budgets first, or also tick-count budgets?
4. How much journal state should the transactional git adapter persist for crash recovery?

## Risks

- `Core/app boundary drift`: git orchestration pressure may leak into `core/` unless the transaction and state-machine logic stays in `app/`.
- `Dual-state divergence`: git state and Archetype state can disagree if commit creation and experiment/result persistence are not updated atomically.
- `Crash recovery ambiguity`: long-running loops need enough journal state to distinguish not-started, materialized, committed, rolled-back, and unknown-after-crash states.
- `Branch-head race conditions`: retries or multiple workers can attempt to advance the same tracked frontier from stale base commits.
- `Non-reproducible runs`: underspecified runtime or environment details can make a kept experiment hard to reproduce later.
- `Metric comparability drift`: keep/discard decisions are only meaningful if the evaluation contract stays stable across experiments.
- `Spec over-flexibility`: unconstrained JSON specs will quickly become a schema trap unless the canonical shapes stay small and explicit.
- `Result/commit mismatch`: a run can succeed while pointing at the wrong git state if the execution boundary is not tightly coupled to the git transaction.
- `Artifact sprawl`: worktrees, logs, checkpoints, and result artifacts will accumulate rapidly unless ownership and cleanup rules are defined early.
- `Security and side effects`: autonomous software optimization assumes arbitrary code execution, so sandbox and trust boundaries must be stated even if v0.1 does not fully solve them.

## Recommendation

Implement the framework primitives and state machine in the app/framework boundary first, then build a reference `autoresearch` controller on top. That yields a working end-to-end loop without prematurely coupling the design to DSL ergonomics or deeper multi-world search policies.
