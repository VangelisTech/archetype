# Dataset and Evaluation Ontology

**Document type:** Normative.

**Scope:** Dataset readers and exporters, physical-AI evaluation, typed dataset
artifact tables, and every public surface that names benchmarks, suites, tasks,
trials, or dataset episodes.

This page defines public vocabulary and identity. It does not define the trial
execution state machine tracked by issue #322, a particular reader schema, or
the `evals/` repository-check runner. That runner reuses ordinary words such as
task and trial as local implementation labels; it does not extend this model.

The vocabulary contract is normative. `TaskRef`, `EpisodeRef`, `RuntimeSlice`,
`Trial`, `GraderKind`, `Grader`, `Rubric`, and `Eval` live in
`archetype.evaluation.contracts`. Dataset identity is evidence used by
evaluation; it does not justify a separate runtime or application family.

## 1. The contract in one view

```text
Dataset      1—N  Suite
Suite        1—N  Task
Task         1—N  Dataset Episode
Trial        1—1  Dataset Episode      one seeded execution produces evidence
Dataset Episode = 1 Trajectory + 0..N FrameStreams

Benchmark    = Dataset + task-bound evaluation
Eval         = 1 Task + 1 Rubric
Rubric       = 1..N Graders
Grader       ∈ {check, test, judge}
EvalSuite    = the eval for each task in a Suite; it is derived, not primitive
```

The shortest useful laws are:

- **a dataset records; a benchmark judges**;
- **datasets are frozen trials**;
- dataset identity and runtime provenance travel together when both exist,
  but one never substitutes for the other;
- graders consume persisted evidence, not privileged live process state.

## 2. Definitions

### Dataset and benchmark

A **Dataset** records what happened. It contains task-bound episodes and their
signals, organized into suites. It does not contain an acceptance decision.

A **Benchmark** is the dataset plus grading: rubrics bound to tasks and the
resulting evaluation evidence. Dataset and benchmark commonly share a name
such as `libero`, but the words describe different responsibilities.

### Suite and task

A **Suite** is a named set of tasks inside a benchmark. It is only a task
collection; a collection of graders is a rubric, not a suite. A benchmark MAY
have one suite.

A **Task** is the stable label and instruction for what is attempted. Its
natural key is `(benchmark, suite, task_key)`. Dataset-native identifiers such
as a LIBERO integer task id MUST be normalized to the string `task_key` at the
adapter boundary.

### Dataset episode, trajectory, and frame stream

A **Dataset Episode** is the frozen evidence from one trial. Its natural key is
`(benchmark, episode_id)`, where `episode_id` is a zero-based integer allocated
by the dataset curator or exporter. A bare integer is not globally unique.

An episode contains exactly one **Trajectory**: the time-indexed state and
action signals for the subject. It may also contain zero or more
**FrameStreams**, one per camera or other framed sensor. Trajectory and frame
stream records MUST carry explicit sampling rates; path names and row counts
do not define timing.

Episode is not a synonym for trajectory. One episode can be represented across
several strict typed artifact tables as long as each row carries the same dataset
coordinates and the streams retain their own timing.

### Trial

A **Trial** is one seeded execution of one task. A trial produces exactly one dataset episode.
The trial is the act; the episode is its frozen evidence.

`archetype.evaluation.contracts.Trial` is an immutable evidence-side record. It
is not a pending/running orchestration object. Submission, polling, retry, and
terminal execution state belong to application orchestration.

### Evaluation vocabulary

An **Eval** binds exactly one task to exactly one non-empty **Rubric**. A rubric
composes named **Graders**:

- **check** — a mechanical predicate over stored state;
- **test** — a deterministic behavioral assertion over episode dynamics;
- **judge** — qualitative, model-graded scoring.

An **EvalSuite** is derived by binding an eval to every task in a suite. It is
not another primitive container.

Evaluation receipts remain evidence, never authority. A pass under one grader
contract does not itself mean accepted, promoted, or safe to deploy; the layer
above evaluation owns those decisions.

## 3. Dataset coordinates and runtime provenance

There are two coordinate systems, and conforming adapters preserve the
difference.

| System | Coordinates | Meaning |
|---|---|---|
| Dataset | `benchmark`, `suite`, `task_key`, `episode_id: int` | What curated episode this is |
| Runtime | `world_id`, `run_id`, `entity_id`, `start_tick`, `final_tick` | Where a live trial's ledger evidence can be queried |

**Dataset coordinates are natural keys.** They are allocated by the dataset
curator or exporter and remain stable when data moves between storage systems.

**Runtime coordinates are provenance.** They are surrogate locations minted by
Archetype. They explain where live evidence came from; they are not dataset
episode identity.

Consequences:

- An imported DROID, EgoDex, LeRobot, or similar episode can have dataset
  coordinates with no Archetype runtime provenance.
- A live trial has both once exported: dataset coordinates identify the frozen
  episode, while `RuntimeSlice` locates the source entity and ticks.
- `entity_id` is required in runtime provenance because one world/run may host
  several trial entities.
- An adapter MUST NOT place a UUID runtime episode id in the integer dataset
  `episode_id` field, or use dataset coordinates as world identifiers.
- The task and episode bound by one trial MUST name the same benchmark.

The complete dataset coordinate tuple for a trial is
`(benchmark, suite, task_key, episode_id)`. Episode identity remains the
shorter `(benchmark, episode_id)` key; suite and task describe its binding.

## 4. Runtime Episode is a different noun

`EpisodeConfig` and `EpisodeResult` in the
[Execution Hierarchy](execution-hierarchy.md) describe a control-flow call:
step one world until termination or a bound. Their `episode_id` is a UUID and
their scope is a runtime world.

A runtime episode MAY batch many trials. The current colocated manipulation
eval does exactly this: one `(world_id, run_id)` contains several trial
entities, and each entity's `ManipTask` and ledger rows identify its slice.
Therefore these relationships are invalid:

```text
trial == EpisodeResult                 # invalid
dataset episode_id == runtime UUID     # invalid
trial provenance == (world_id, run_id) # incomplete without entity/ticks
```

The valid bridge is:

```text
runtime EpisodeResult
    ├─ trial entity A + tick slice ──freeze──> dataset episode 17
    ├─ trial entity B + tick slice ──freeze──> dataset episode 18
    └─ trial entity C + tick slice ──freeze──> dataset episode 19
```

This is what “datasets are frozen trials” means. It does not require one
runtime world, run, or `EpisodeResult` per trial.

## 5. Persistence in typed ingestion tables

Dataset readers and exporters write domain rows through the app-layer typed
Iceberg ingestion path. The division of
responsibility is strict:

| Owner | Columns / concern |
|---|---|
| `IngestionService` | Owning `world_id` / `run_id`, registered table schema, caller-declared logical key, strict append and dedup behavior |
| Dataset adapter | Table name and logical key; `benchmark`, `suite`, `task_key`, `episode_id`, stream/timing fields, domain payload |
| Live-trial exporter | Optional source `RuntimeSlice` provenance in addition to dataset coordinates |

**The IngestionService envelope is storage ownership, not dataset identity.**
Its `world_id` and `run_id` scope the table view. The dataset adapter still
declares its natural logical key. That envelope **does not replace dataset
coordinates** and does not prove where an imported episode originally ran.

For example, importing an external dataset creates an Archetype world/run for
the ingestion operation. Those envelope values name the owner of the typed
table rows, not a fictional original simulation. Conversely, exporting a live
trial MAY persist its source `RuntimeSlice` as typed payload provenance.

Typed tables fail on schema drift. Adapters MUST normalize native vocabulary
before the `IngestionService` boundary and MUST NOT depend on silent widening.
Large media is ingested through `ArtifactService` and referenced by
`ArtifactRef` or `artifact_id` from domain rows rather than represented by an
opaque filesystem path.

## 6. Grading symmetry and receipts

Graders SHOULD be query-backed over persisted rows. Given the same normalized
episode schema, a grader must not branch merely because evidence came from an
external reader or a live rollout. Runtime provenance may support diagnosis,
but its absence is not a grading result.

The durable evaluation-receipt contract is adjacent but distinct:

- a receipt pins one grader contract and one immutable subject snapshot;
- receipt `evaluation_id` identifies a grader execution and is not a dataset
  `episode_id`;
- repeated nondeterministic grader trials can produce distinct receipts over
  the same dataset episode;
- receipts carry conclusions and evidence, never promotion authority.

The repository-check runner's internal `TrialResult` records one repeated
execution of a framework check. It is not the physical-AI dataset `Trial`
defined here.

## 7. Native vocabulary mapping

Dataset-native nouns stop at their adapter.

| Native term | Source | Archetype term |
|---|---|---|
| `episode_index` | LeRobot | dataset `episode_id` |
| language instruction / annotation | DROID, LeRobot | task instruction |
| wrist / exterior camera | DROID, EgoDex, LIBERO | frame stream |
| joint / effector / action series | physical-AI datasets | trajectory |
| `libero_spatial`, `libero_object`, … | LIBERO | suite |
| BDDL task id and language | LIBERO | `task_key` and instruction |
| one seeded environment entity | colocated eval | trial |

## 8. Executable mirror and current gaps

The immutable identity vocabulary lives in
`archetype.evaluation.contracts`. Readers, exporters, and evaluation code MUST
import these definitions rather than create competing meanings for the same
nouns. The spec eval checks the cardinality, key types, coordinate separation,
and exact grader kinds.

Current implementation truth:

- typed artifact tables and query-backed colocated evaluation exist;
- the colocated evaluator keeps N trial entities in one ledger and derives its
  report from persisted rows;
- the canonical dataset episode row schemas, exporters, reader adapters, and
  shared reader conformance suite do not exist yet;
- the durable submit/poll trial lifecycle remains issue #322;
- allocation of zero-based dataset episode ids belongs to the future
  reader/exporter boundary, not `SimulationService`.

These are **CURRENT GAP** items, not behavior the documentation pretends is
already implemented. A first reader or exporter must add one shared
conformance suite covering natural keys, trajectory/frame separation, sampling
rates, and optional runtime provenance.
