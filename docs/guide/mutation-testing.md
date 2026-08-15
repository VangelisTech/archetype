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
initial scope is intentionally tight and walks the dependency tree
upward from the keystone invariants:

- `paths_to_mutate`: `core/component.py`, `core/archetype.py`
- `tests_dir`: `tests/core/test_component_core.py`,
  `tests/core/test_archetype_core_signatures.py`

`component.py` and `archetype.py` are small, pure-Python, and encode
the framework's foundational data contract: components serialize into
prefixed Arrow fields, and archetypes canonicalize a sorted tuple of
component types into a unified schema. The specification spells out
why these are keystone:

> `ArchetypeSignature` MUST be canonicalized as a sorted tuple of
> component types. Signature identity is order-invariant.
> — `docs/guide/specification.md`

A mutation that survives in either module is a missing assertion on
something the storage layer depends on. Expand `paths_to_mutate` and
`tests_dir` together as the workflow proves out. The constraint is
wall-clock: a module with N mutations and a T-second test suite takes
roughly N × T seconds.

## Behavioral vs structural — pick the right tool

Not every module is a mutation-testing target. Before adding a module
to `paths_to_mutate`, ask whether its contract is **behavioral** or
**structural**:

| Contract type | Example modules | Right tool |
|---|---|---|
| Behavioral — input→output, invariants, side effects | `core/component.py`, `core/archetype.py`, `core/hooks.py` dispatch logic | mutmut |
| Structural — this thing has these methods with these signatures | `core/interfaces.py` (Protocols), Pydantic schemas in `core/config.py` | type checker (mypy / pyright / `ty`) |

`core/interfaces.py` is the canonical example of the wrong target:
~99% of its body is `def foo(self) -> X: ...`. Mutmut has nothing to
mutate, and the few mutations it can generate (default values, type
aliases) aren't *executed* — Protocols are *checked*. The same logic
applies to Pydantic models: schema correctness is the type checker's
job, not the mutation runner's. Mutate the validators, not the fields.

This is the same data-centric framing the project uses elsewhere:

> Archetype is data-centric. The DataFrame is the source of truth.
> Processors are pure functions `DataFrame → DataFrame`. So long as
> the data looks right at the end of a tick, nothing else matters.
> — `LEARNINGS.md`

Mutation testing on the data-shaping layer (components, archetypes,
processors) directly probes that contract. Mutation testing on
shape-declaration layers (Protocols, schemas) does not.

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
- **Keep `paths_to_mutate` narrow.** Pointing it at `packages/archetype-ecs/src/archetype/`
  whole would generate thousands of mutants. Pick one module at a time.
- **Tests must be deterministic and fast.** Flaky tests poison the
  signal; slow tests blow up the wall clock.
- **`also_copy` stages the rest of the package.** mutmut copies only
  `paths_to_mutate` into `mutants/` by default, but our pytest
  `pythonpath` makes the staged package source root importable. Without the
  `also_copy = ["packages/archetype-ecs/src/archetype"]` entry,
  `tests/conftest.py` fails to import the full package. The unmutated copy is
  harmlessly overwritten when mutmut writes each mutant.
- **LanceDB fork warning.** `lance is not fork-safe`. The pilot module
  doesn't touch LanceDB during import, so the warning is benign. If
  you expand the scope into modules that do, switch the multiprocessing
  start method or scope tests away from LanceDB-touching paths.

## Reading a "no tests" survivor

Mutmut marks a survivor as `no tests` when its stats-collection phase
observed zero tests entering the mutated function. That's a stronger
signal than a normal survivor: not "tests ran but missed the mutation"
but "no test exercises this code path at all." When the function is
called from production code elsewhere, that's an unverified contract.

The pilot's expansion to `archetype.py` surfaced exactly this case. At the
time, `Archetype.__init__` was reached by parallel synchronous and asynchronous
queriers, but no test in the dedicated suite ever constructed an instance. A
single contract test that asserts the constructor populates `sig`, `name`, and
`schema` consistently with the staticmethod API killed all seven survivors at
once. The parallel synchronous core was retired in 0.6; the production oracle
now exercises `AsyncQueryManager`.

This is consistent with the project's stated testing posture:

> Prefer contract tests over happy-path coverage… If a test feels
> "too specific," it is usually testing the real semantic boundary.
> — `AGENTS.md`

The mutation runner is a way to find the missing contract tests.

## Suggested expansion order

After the current pilot (`component.py`, `archetype.py`), candidate
modules in rough order of value-per-effort:

1. `core/hooks.py` — real dispatch logic (register, fire, ordering,
   fire modes). Behavioral. Async fixtures need to assert observed
   dispatch, not just absence of exceptions.
2. `core/resources.py` — lifecycle ordering and insert/get/require
   contract. Source is 81 lines but `test_resources_hooks_messaging.py`
   (~680 lines, async) sprawls; scope `tests_dir` to
   `test_resources_manager.py` only to keep wall-clock sane.
3. `commands/policy.py` — RBAC logic where assertion gaps are most
   consequential (quota math, role gating).

Each expansion is a separate commit that updates `[tool.mutmut]` and
addresses the surviving mutants. Modules whose contract is structural
(Protocols, schemas) belong on the type-checker side of the divide,
not here.
