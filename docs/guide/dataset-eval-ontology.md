# Dataset & Eval Ontology

**Document type:** Normative.
**Scope:** Physical-AI dataset readers (DROID, EgoDex, LIBERO, LeRobot), the eval
service, bench harnesses, and every surface that names benchmarks, tasks,
episodes, trials, or checks.

## 1. Why this page exists

Archetype is growing several dataset readers and several eval surfaces at
once. Each external dataset ships its own vocabulary, and each harness is
tempted to redefine the same words. This page is the single source of meaning:
readers and eval code import these nouns; dataset-native vocabulary is
converted to them at the adapter boundary and goes no further.

## 2. The nouns

```text
Benchmark   1—N  Suite         named curation (DROID, EgoDex, LIBERO, LeRobot)
Suite       1—N  Task          a named set of tasks — never anything else
Task        1—N  Episode       a label + instruction: what the robot did
Episode     =    1 Trajectory + 0..N FrameStreams   the captured artifact
Trial       1—1  Episode       one seeded execution; produces the episode
Eval        1—1  Task          how a task is graded
Eval        1—N  Check         one grader predicate/metric
EvalSuite   ≅    Suite         a suite with checks bound to each task
```

- **Benchmark** — a named, versioned curation of suites, tasks, and episodes.
  Identity: `benchmark` (string name). A dataset and a benchmark are the same
  object; "benchmark" is the word once grading is attached.
- **Suite** — a named set of tasks within a benchmark (LIBERO's
  `libero_spatial` is a suite). Identity: `(benchmark, suite)`.
- **Task** — a label plus instruction describing what the robot does
  ("put the mug on the shelf"). Identity: `(benchmark, suite, task_key)`.
- **Episode** — all data captured for one robot in one simulation or
  real-world capture: exactly one trajectory plus zero or more frame streams.
  Identity: `(benchmark, episode_id)` where `episode_id` is a
  **dataset-scoped integer**. Bare `episode_id` is not globally unique.
- **Trajectory** — the proprioceptive/motor signal stream of an episode
  (joint states, effector poses, gripper actions). Not interchangeable with
  episode in any schema, even though the field uses the words loosely.
- **FrameStream** — one camera's image/video stream within an episode
  (wrist cam, exterior cam). An episode may have several.
- **Trial** — one seeded execution of a policy on a task. A trial produces
  exactly one episode and carries execution provenance (seed, policy,
  runtime coordinates).
- **Check** — a single grader: a predicate or metric over an episode
  (success flag, distance-to-goal, constraint violation).
- **Eval** — the grading of one task: a task plus its checks, applied to
  that task's episodes. An eval binds exactly one task.
- **EvalSuite** — derived, not primitive. Because eval ↔ task is 1:1, a
  suite of evals and a suite of tasks are the same set viewed from two
  sides: `eval_suite(suite) = {eval(t) for t in suite}`.

## 3. Two coordinate systems

Every row in this domain is addressed by one of two key systems. They are
never conflated.

| System | Keys | Minted by | Example |
|---|---|---|---|
| Dataset coordinates | `benchmark`, `suite`, `task_key`, `episode_id: int` | The dataset curator | `("libero", "libero_spatial", 3, 41)` |
| Runtime coordinates | `world_id`, `run_id`, tick range, execution UUIDs | The Archetype runtime | `EpisodeResult.world_id`, `run_id` |

Rules:

- Dataset coordinates are natural keys. Runtime coordinates are surrogate
  keys. A schema MUST NOT use one where the other is meant.
- An episode read from disk (DROID, EgoDex) has **no runtime coordinates**.
- An episode minted by a live trial has **both**: the trial records
  `world_id` / `run_id` / tick range as provenance, and the output dataset
  assigns the integer `episode_id`.
- Eval reports carry both systems side by side (as `TaskEvalReport` already
  does with `suite`, `task_id`, `world_id`, `run_id`): dataset coordinates
  say *what* was graded, runtime coordinates say *where the evidence lives*.

## 4. Episode: dataset noun vs. runtime execution

[Execution Hierarchy](execution-hierarchy.md) defines `run_episode` — a
control-flow construct (step-until-termination) whose `EpisodeConfig` /
`EpisodeResult` carry a UUID `episode_id`. That is a **runtime execution**,
not the dataset noun, and its UUID is a runtime coordinate.

The bridge is the trial: a trial wraps one runtime episode execution
(typically one rollout fork), and its persisted output — trajectory rows,
frame references, provenance — indexed with a dataset-scoped integer, **is**
the dataset episode.

```text
run_episode(...)          runtime execution   (UUID, execution-hierarchy.md)
    └─ wrapped by Trial   seed + policy + runtime provenance
        └─ produces       Episode             (int id, this page)
```

Stated as a slogan: **datasets are frozen trials.** DROID and EgoDex are
collections of trials someone else ran; LIBERO benchmarking mints new ones.

## 5. The symmetry law

Graders MUST NOT distinguish reader episodes from rollout episodes.

- Readers and live trials emit the **same episode schema**.
- Grading is query-backed: checks run over stored rows, never over live
  in-process state. (This is what makes re-grading from the ledger possible.)
- Trial provenance is present when the episode was minted live and absent
  when it was read from an external dataset. Absence of provenance is the
  only schema-visible difference, and no check may branch on it.

## 6. Naming rules

- **"Suite" only ever means a set of tasks.** The graders bound to an eval
  are its *checks* and MUST NOT be called a suite. (`evals/suites/` — the
  framework's own regression/capability meta-evals — is internal tooling and
  exempt; its name does not license the other sense in this domain.)
- An eval binds exactly one task. A report that spans tasks is an eval-suite
  report, not an eval.
- `trajectory` and `episode` are distinct schema words per §2, even where
  the literature uses them interchangeably.
- Dataset-native vocabulary stays inside its adapter (`bench/libero/`,
  reader modules) and is converted to these nouns at the boundary. LIBERO's
  `task_id: int` maps to `task_key`; LeRobot's `episode_index` maps to
  `episode_id`.

## 7. Reader mapping

| Native term | Dataset | Ontology noun |
|---|---|---|
| `episode_index` | LeRobot | `episode_id` |
| language instruction / annotation | DROID, LeRobot | Task instruction |
| wrist / exterior camera streams | DROID, EgoDex | FrameStreams |
| proprio / joint / effector series | all | Trajectory |
| `libero_spatial`, `libero_object`, … | LIBERO | Suite |
| BDDL task + instruction | LIBERO | Task |
| seeded policy run | LIBERO bench | Trial |

## 8. Conformance

- **One vocabulary module.** The typed rows for these nouns live in exactly
  one module; readers and eval services import them and never redefine them.
- **Reader conformance suite.** One parametrized test suite that every
  reader (DROID, EgoDex, LIBERO, LeRobot) must pass: same episode schema,
  integer dataset-scoped `episode_id`, trajectory/frames separation, task
  binding present. Adding a reader means adding a fixture, not assertions.
- **Merge-gated contracts.** Ontology contracts belong in
  `evals/suites/spec_contracts.py`; the `evals` job is a required status
  check, so vocabulary violations cannot merge.

Status: this page is normative now. The vocabulary module and reader
conformance suite land with the first reader PR and the #252 rebase; until
then, new code MUST NOT introduce vocabulary that conflicts with §2 and §6.
