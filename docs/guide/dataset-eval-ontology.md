# Dataset & Eval Ontology

**Document type:** Normative.
**Scope:** Physical-AI dataset readers (DROID, EgoDex, LIBERO, LeRobot), the eval
service, bench harnesses, and every surface that names benchmarks, tasks,
episodes, trials, graders, or rubrics.

## 1. Why this page exists

Archetype is growing several dataset readers and several eval surfaces at
once. Each external dataset ships its own vocabulary, and each harness is
tempted to redefine the same words. This page is the single source of meaning:
readers and eval code import these nouns; dataset-native vocabulary is
converted to them at the adapter boundary and goes no further.

The executable mirror of this page is `archetype.datasets.definitions`, and
the `spec` eval suite binds the two together as a required merge gate.

## 2. Definitions

This glossary is the iteration surface. Change the meaning here first; code
follows.

```text
Benchmark   =    Dataset + judgment (rubrics, eval results)
Dataset     1—N  Suite         just what happened
Suite       1—N  Task          a named set of tasks — never anything else
Task        1—N  Episode       a label + instruction: what the robot did
Episode     =    1 Trajectory + 0..N FrameStreams   the captured artifact
Trial       1—1  Episode       one seeded execution; produces the episode
Eval        1—1  Task          how a task is graded
Eval        =    Task + Rubric
Rubric      1—N  Grader        the skeleton of graders composing an eval
Grader      ∈    {check, test, judge}
EvalSuite   ≅    Suite         a suite with rubrics bound to each task
```

- **Dataset** — just what happened: episodes (observation and state),
  organized by suites and tasks. A dataset contains no eval analysis.
  Identity: `benchmark` (string name — see Benchmark).
- **Benchmark** — what happened *plus* the analysis of what happened: a
  dataset with rubrics bound to its tasks, and room for eval results.
  Stated as law: **a dataset records; a benchmark judges.** The two share
  one name string; "benchmark" is the word once grading is attached.
- **Suite** — a named set of tasks within a benchmark (LIBERO's
  `libero_spatial` is a suite). A suite is only ever a set of tasks.
  A benchmark MAY have exactly one suite. Identity: `(benchmark, suite)`.
- **Task** — a label plus instruction describing what the robot does
  ("put the mug on the shelf"). Identity: `(benchmark, suite, task_key)`.
- **Episode** — all data captured for one robot in one simulation or
  real-world capture: exactly one trajectory plus zero or more frame
  streams. Identity: `(benchmark, episode_id)` where `episode_id` is a
  **zero-based, dataset-scoped integer**. Bare `episode_id` is not
  globally unique. Episodes have no fixed length — real captures run for
  varying numbers of ticks — and every signal stream in an episode MUST
  carry its sampling rate: the sampling rate is the fidelity at which the
  dataset is curated.
- **Trajectory** — the proprioceptive/motor signal stream of an episode
  (joint states, effector poses, gripper actions), with its sampling rate
  in Hz. Not interchangeable with episode in any schema, even though the
  field uses the words loosely.
- **FrameStream** — one camera's image/video stream within an episode
  (wrist cam, exterior cam), with its FPS. An episode may have several.
- **Trial** — one seeded assignment of a task to an eval's execution.
  Running a trial produces exactly one episode. The trial is the act; the
  episode is the evidence. (See §5 for the readings this word does NOT
  have.)
- **Grader** — anything that scores an episode against a task. Three kinds:
  - **Check** — a mechanical lint: a deterministic predicate over stored
    state (success flag set, constraint never violated).
  - **Test** — a deterministic behavioral assertion over episode dynamics
    (reached the goal region within N ticks, gripper closed before lift).
  - **Judge** — model-graded, qualitative scoring (LLM-as-judge). Use
    sparingly: qualitative grading is the least reliable kind.
- **Rubric** — the skeleton of graders that composes an eval. Rubrics are
  named compositions; graders are their members.
- **Eval** — the grading of exactly one task: a task plus its rubric,
  applied to that task's episodes across many trials.
- **EvalSuite** — derived, not primitive. Because eval ↔ task is 1:1, a
  suite of evals and a suite of tasks are the same set viewed from two
  sides: `eval_suite(suite) = {eval(t) for t in suite}`.

## 3. Two coordinate systems

Every row in this domain is addressed by one of two key systems. They are
never conflated.

| System | Keys | Minted by | Example |
|---|---|---|---|
| Dataset coordinates | `benchmark`, `suite`, `task_key`, `episode_id: int` | The dataset curator | `("libero", "libero_spatial", "3", 41)` |
| Runtime coordinates | `world_id`, `run_id`, tick range, execution UUIDs | The Archetype runtime | `EpisodeResult.world_id`, `run_id` |

Rules:

- Dataset coordinates are natural keys. Runtime coordinates are surrogate
  keys. A schema MUST NOT use one where the other is meant.
- An episode read from disk (DROID, EgoDex) has **no runtime coordinates**.
- An episode minted by a live trial has **both**: the trial records
  `world_id` / `run_id` / tick range as provenance, and the output dataset
  assigns the integer `episode_id`.
- Eval reports carry both systems side by side: dataset coordinates say
  *what* was graded, runtime coordinates say *where the evidence lives*.

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

**Runtime alignment (required change):** the dataset episode number is not
currently stamped by any durable abstraction in Archetype. The simulation
service MUST stamp a zero-based, dataset-scoped integer `episode_id` on
trial outputs. Execution UUIDs (uuid7) remain runtime coordinates and do
not satisfy this requirement.

## 5. Trial: the readings this word does not have

"Trial" is the easiest word in this domain to confuse. Three readings are
rejected explicitly:

1. A trial is **not** a task. The task is the label; the trial is one
   execution of it.
2. A trial is **not** many episodes of a task. Running a rubric over many
   episodes is the *eval's* execution loop; each pass through that loop is
   one trial.
3. A trial is **not** the (task, episode) pair as data. That pair is the
   trial's *identity*, but the trial itself is the seeded act of execution
   — policy, seed, runtime provenance — from which the episode data falls
   out.

Two adjacent usages to keep quarantined: the meta-eval harness
(`evals/types.py`) has an internal `TrialResult` for its own grader runs,
and PR #247's "trial execution lifecycle" must be reconciled with this
definition when it lands.

## 6. The symmetry law

Graders MUST NOT distinguish reader episodes from rollout episodes.

- Readers and live trials emit the **same episode schema**.
- Grading is query-backed: graders run over stored rows, never over live
  in-process state. (This is what makes re-grading from the ledger
  possible.)
- Trial provenance is present when the episode was minted live and absent
  when it was read from an external dataset. Absence of provenance is the
  only schema-visible difference, and no grader may branch on it.

## 7. Naming rules

- **A suite is only ever a set of tasks.** The graders bound to an eval
  form its *rubric* and MUST NOT be called a suite. (`evals/suites/` — the
  framework's own regression/capability meta-evals — is internal tooling
  and exempt; its name does not license the other sense in this domain.)
- An eval binds exactly one task. A report that spans tasks is an
  eval-suite report, not an eval.
- `trajectory` and `episode` are distinct schema words per §2, even where
  the literature uses them interchangeably.
- Dataset-native vocabulary stays inside its adapter (`bench/libero/`,
  reader modules) and is converted to these nouns at the boundary. LIBERO's
  `task_id: int` maps to `task_key`; LeRobot's `episode_index` maps to
  `episode_id`.

## 8. Reader mapping

| Native term | Dataset | Ontology noun |
|---|---|---|
| `episode_index` | LeRobot | `episode_id` |
| language instruction / annotation | DROID, LeRobot | Task instruction |
| wrist / exterior camera streams | DROID, EgoDex | FrameStreams |
| proprio / joint / effector series | all | Trajectory |
| `libero_spatial`, `libero_object`, … | LIBERO | Suite |
| BDDL task + instruction | LIBERO | Task |
| seeded policy run | LIBERO bench | Trial |

## 9. Conformance

- **Definitions module.** The typed vocabulary lives in exactly one module:
  `archetype.datasets.definitions`. Readers and eval services import it and
  never redefine the nouns. Identity vocabulary lands now; full row schemas
  (episode contents, sampling metadata) land with the readers.
- **Spec-contract gate.** `evals/suites/spec_contracts.py` binds this page
  to the definitions module structurally (episode id is an int, eval binds
  one task, trial produces one episode, grader kinds are exactly
  check/test/judge). The `spec` suite runs in the required `evals` status
  check, so vocabulary violations cannot merge.
- **Reader conformance suite.** One parametrized test suite that every
  reader (DROID, EgoDex, LIBERO, LeRobot) must pass: same episode schema,
  integer dataset-scoped `episode_id`, trajectory/frames separation, task
  binding present, sampling rates present. Adding a reader means adding a
  fixture, not assertions. Lands with the first reader PR.
