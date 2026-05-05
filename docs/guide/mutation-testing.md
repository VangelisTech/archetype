# Mutation Testing

Coverage answers "did the test exercise this line?" Mutation testing answers
the harder question: "if this line were wrong, would any test fail?" A
codebase can hit 100% coverage without a single meaningful assertion;
mutation testing exposes that.

Archetype uses [`mutmut`](https://github.com/boxed/mutmut) for mutation
testing. It is **not** part of `make ci` — every mutation runs the pilot
test suite as a separate process, so even a narrow sweep takes minutes.
Run it on-demand when you want to assess assertion strength on a module.

## What mutmut does

Given a source file, mutmut applies small AST mutations — flip `>` to
`<`, replace `True` with `False`, drop a statement, change a constant —
re-runs the configured tests, and records:

| Outcome | Symbol | Meaning |
|---|---|---|
| Killed | 🎉 | A test failed under the mutation. Good — the test caught the bug. |
| Survived | 🫥 | All tests passed despite the mutation. Bad — no test is asserting that behavior. |
| Timeout | ⏰ | The mutated code hung. Usually treated as killed. |
| Suspicious | 🤔 | The mutation made tests run noticeably slower; investigate. |

A surviving mutant is a pointer to a missing assertion, not a code bug.
The fix is usually to tighten a test, not to revert the mutation.

## Pilot scope

Configuration lives in `pyproject.toml` under `[tool.mutmut]`. The
initial scope is intentionally tight:

- `paths_to_mutate`: `src/archetype/core/component.py`
- `tests_dir`: `tests/core/test_component_core.py`

`component.py` is small, pure Python, and has dedicated tests — a good
shape for a first run. Expand `paths_to_mutate` and `tests_dir` together
as the workflow proves out. The constraint is wall-clock: a module with
N mutations and a T-second test suite takes roughly N × T seconds.

## Running

```bash
make mutmut          # run the full pilot sweep
make mutmut-results  # list surviving mutants from the last run
make mutmut-browse   # interactive TUI to inspect mutants
make mutmut-clean    # delete the mutants/ working tree and cache
```

The first `make mutmut` writes a `mutants/` directory at the repo root.
It is gitignored. `mutmut` reuses it across runs, so subsequent sweeps
only re-test mutants whose source has changed.

## Reading a surviving mutant

```bash
uv run mutmut show <mutant-id>
```

This prints the diff between the original and the mutant. If a test
should have caught the change but didn't, add or strengthen an
assertion. If the mutation is semantically equivalent (e.g., reordering
commutative operations), there's nothing to fix — record it and move
on. Equivalent mutants are an inherent noise floor in mutation testing.

## Constraints and gotchas

- **Don't add to `make ci`.** Mutation testing is too slow to gate every
  PR. Treat it like `make complexity` — an on-demand quality probe.
- **Keep `paths_to_mutate` narrow.** Pointing it at `src/archetype/`
  whole would generate thousands of mutants. Pick one module at a time.
- **Tests must be deterministic and fast.** Flaky tests poison the
  signal; slow tests blow up the wall clock.
- **`also_copy` stages the rest of the package.** mutmut copies only
  `paths_to_mutate` into `mutants/` by default, but our pytest
  `pythonpath = ["src", "."]` makes `mutants/src` the import root.
  Without the `also_copy = ["src/archetype"]` entry, `tests/conftest.py`
  fails to import `archetype.app`. The unmutated copy is harmlessly
  overwritten when mutmut writes each mutant.
- **LanceDB fork warning.** `lance is not fork-safe`. The pilot module
  doesn't touch LanceDB during import, so the warning is benign. If
  you expand the scope into modules that do, switch the multiprocessing
  start method or scope tests away from LanceDB-touching paths.

## Suggested expansion order

After the pilot, candidate modules in rough order of value-per-effort:

1. `src/archetype/core/resources.py` — small, well-tested, central.
2. `src/archetype/core/hooks.py` — semantic surface with dedicated tests.
3. `src/archetype/app/auth/` — RBAC logic where assertion gaps are
   most consequential.

Each expansion is a separate commit that updates `[tool.mutmut]` and
addresses the surviving mutants.
