# Repository Checks

The `evals/` package runs end-to-end checks against Archetype. It complements
pytest by assembling realistic public or service boundaries and grading the
observable result independently of the focused test that motivated a change.

This package is one part of the [Repository Harness](repository-harness.md),
not Archetype's product evaluation workflow. New bugs still require a focused,
deterministic pytest regression. Add a repository scenario only when a broader
composition across entry points, backends, lifecycle states, or concurrency
schedules provides additional evidence.

Its `suite`, `task`, and `trial` labels are runner implementation terms. They
do not define the public evaluation model, dataset identity, or physical-AI
trial lifecycle described in the
[Dataset and Evaluation Ontology](dataset-eval-ontology.md).

## Run or inspect checks

```bash
make eval-conformance  # blocking regression + spec profile
make eval-reliability  # blocking idempotency/recovery profile
make eval-capability   # blocking architectural capability profile
make eval              # every suite; writes eval-results.json

uv run python -m evals.run --suite spec
uv run python -m evals.run --profile reliability --trials 3
uv run python -m evals.run --list
uv run python -m evals.run --list --suite capability
```

`--list` reads the live registry built by `evals.run.build_harness()` and does
not execute tasks. That command is the task inventory; this guide deliberately
does not duplicate it in a hand-maintained table.

`quality/eval_profiles.toml` is the machine-readable authority for profile
membership and failure semantics. The short aliases `make eval-reg`,
`make eval-idem`, and `make eval-cap` remain available for focused local use.

## Execution model

A registered task is a callable that returns one or more `GraderResult`
objects. The runner executes it once by default or `--trials N` times when a
scenario needs repeated scheduling evidence. Empty grader output and uncaught
exceptions are explicit failed trials.

`--trials n` must be a positive integer. Each task runs `n` times, counts the
successful trials `c`, and reports three deliberately different success
quantities.

Reports use literal measurements:

| Field | Meaning |
|---|---|
| `trial_count` | Number of executions recorded for the task |
| `pass_rate` | Raw empirical success fraction `c / n` for one task |
| `pass@k` | Expected probability that at least one sample passes in a size-`k` subset of the `n` observations |
| `pass^n` | `1.0` only when every observed trial passed |
| `avg_score` | Mean grader score across executions |
| `all_passed` | Whether every grader passed in every recorded execution |

Pass@k uses the unbiased closed-form estimator from Chen et al.,
[*Evaluating Large Language Models Trained on Code*](https://arxiv.org/abs/2107.03374):

```text
1 - C(n - c, k) / C(n, k), for n >= k
```

The harness computes the exact expectation over subsets for each task, then
averages those task estimates into the suite curve. This is not the biased
shortcut `1 - (1 - c/n)^k`, and it is not another name for `c/n`. Confidence
intervals require bootstrapping across tasks/questions; resampling trials
inside one task answers a different question and is intentionally not done by
the current runner.

The default is one observed trial. Repeated trials are useful when a boundary
involves process scheduling or another source of nondeterminism; they do not
turn a deterministic failure into an acceptable result. Gate status continues
to use `all_passed`, independently of the reporting metrics.

Every JSON result uses the common provenance envelope from `quality/results.py`:
schema and result kind, selected profile and suites, declared failure policy,
outcome and timing, Git revision, Python/platform environment, trial
configuration, and per-task contract IDs. Evals and benchmarks therefore
retain comparable execution context without sharing a runner.

## What passes the gate

Profiles define release semantics; suites remain useful implementation and
diagnostic groupings:

| Profile | Suites | CLI exit condition |
|---|---|---|
| Conformance | Regression + spec | Every required task passes every trial |
| Reliability | Idempotency | Every retry, replay, crash, race, and recovery task passes every trial |
| Capability | Capability | Every architectural scenario passes every trial |

An empty requested profile or suite is a failure, never a vacuous success. A
full unprofiled run requires all four suites to be present and passing.

The local PR verification profile runs static validation and the fast pytest
suite on Python 3.12. Coverage, conformance, capability, reliability,
installed-wheel smoke checks, executable examples, documentation,
credentialed infrastructure, and compatibility evidence do not block ordinary
pull requests. The full and release profiles retain that deeper evidence so it
cannot silently disappear from a release.

## Suite package layout

Each suite owns a package under `evals/suites/`. Package entry points expose
`register(harness)` while implementation modules group related scenarios. The
runner uses `evals.suites.catalog.register_all()` so adding a module does not
grow another import list in the CLI. Task IDs, not module paths, are the stable
traceability keys; modules may split as a suite grows.

`make idempotency-audit` is the fast static check that the normative matrix and
its registered scenarios still correspond. `quality/contracts.toml` maps
stable task IDs to normative contracts, and `make contract-audit` rejects
missing, unknown, or orphaned mappings.

## Grader and logging behavior

Code graders live in `evals/graders.py`. A task may compose exact comparisons,
state checks, numeric thresholds, expected exceptions, or code-quality
measurements. Every task must return non-empty evidence; `state_check` likewise
rejects an empty mapping.

Some regression scenarios deliberately submit malformed commands. The CLI
filters only the known records produced by those inputs so the report stays
readable. Unexpected Archetype logs remain visible, and the filter does not
change library logging or `RunConfig(debug=True)`.

## Add or change a check

First ask whether a focused or parameterized pytest contract proves the whole
behavior. The `regression` and `spec` suites contain valuable historical
coverage, but they are migration surfaces rather than the default destination
for new exact bugs or static rules.

When a repository scenario is warranted:

1. Put it in the suite that owns its failure semantics.
2. Drive the highest stable boundary that proves the contract. Use lower-level
   seams only when the task is about that seam or must construct a crash state
   that public calls cannot produce deterministically.
3. Grade externally visible outcomes instead of duplicating implementation
   logic in the assertion.
4. Register the task in the suite's `register(harness)` function and map its
   stable ID in `quality/contracts.toml`.
5. Run the focused suite, its traceability audit, and the broader gate
   appropriate to the changed boundary.

When semantics genuinely change, update the normative source, implementation,
task, registry mapping, and executable evidence as one reviewable unit.

## Relationship to other checks

- Pytest provides focused unit, integration, race, and contract diagnosis.
- Repository scenarios provide independent architectural outcomes and
  traceability.
- [Mutation testing](mutation-testing.md) probes whether focused assertions
  detect controlled implementation changes.
- [Benchmarks](benchmarking.md) measure cost and trends, not correctness.

Use the smallest oracle that proves the local behavior, then run broader gates
in proportion to the boundary crossed.
