# Repository Checks

The `evals/` package runs end-to-end checks against Archetype. It complements
pytest by assembling realistic public or service boundaries and grading the
observable result independently of the focused test that motivated a change.

Its `suite`, `task`, and `trial` labels are runner implementation terms. They
do not define the public evaluation model, dataset identity, or physical-AI
trial lifecycle described in the
[Dataset and Evaluation Ontology](dataset-eval-ontology.md).

## Run or inspect checks

```bash
make eval          # run every group; optionally write eval-results.json
make eval-reg      # required behavior checks
make eval-idem     # retry and replay checks
make eval-cap      # broader end-to-end scenarios

uv run python -m evals.run --suite spec
uv run python -m evals.run --suite regression --trials 3
uv run python -m evals.run --out eval-results.json
uv run python -m evals.run --list
uv run python -m evals.run --list --suite capability
```

`--list` reads the live registry built by `evals.run.build_harness()` and does
not execute tasks. That command is the task inventory; this guide deliberately
does not duplicate it in a hand-maintained table.

## Execution model

A registered task is a callable that returns one or more `GraderResult`
objects. The runner executes it once by default or `--trials N` times when a
scenario needs repeated scheduling evidence. Empty grader output and uncaught
exceptions are explicit failed trials.

Reports use literal measurements:

| Field | Meaning |
|---|---|
| `trial_count` | Number of executions recorded for the task |
| `pass_rate` | Fraction of those executions whose graders all passed |
| `avg_score` | Mean grader score across executions |
| `all_passed` | Whether every recorded execution passed |

These are repository results, not statistical estimates of model capability.

## Execution groups

The four group names select checks and determine CLI failure behavior. They are
not a product taxonomy.

| Group | Contents | Nonzero exit |
|---|---|---|
| `regression` | Established behavior | Any missed grader or task error |
| `spec` | Structural checks derived from normative guides | Any missed grader or task error |
| `idempotency` | Repetition, replay, race, and crash-recovery boundaries | Any missed grader or task error |
| `capability` | Broader end-to-end scenarios | A scenario error; missed graders remain visible but advisory |

An explicitly requested empty group fails instead of succeeding vacuously. A
full run also requires the regression, spec, and idempotency groups to exist.

`make ci` runs regression and idempotency checks. Pytest executes the spec
group and its CLI smoke contract. GitHub's eval job runs regression and
capability separately. `make idempotency-audit` is the fast static check that
the normative matrix and its registered scenarios still correspond.

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

1. Put the scenario in the group whose exit behavior matches its purpose.
2. Exercise the highest stable boundary that proves the behavior.
3. Grade externally visible outcomes rather than duplicating implementation
   logic in the assertion.
4. Register the task in that module's `register(harness)` function.
5. Run the focused group and the broader gate appropriate to the changed
   boundary.

Keep task identifiers stable when specifications or issue receipts cite them.
When semantics change, update the normative source, implementation, and
executable evidence together.

## Relationship to other evidence

- Pytest provides focused diagnosis for units, integrations, races, and local
  contracts.
- Repository checks compose those boundaries into independent outcomes.
- [Mutation testing](mutation-testing.md) probes whether focused assertions
  detect controlled implementation changes.
- [Benchmarks](benchmarking.md) measure cost and trends, not correctness.

Use the smallest oracle that proves the local behavior, then run broader gates
in proportion to the boundary crossed.
