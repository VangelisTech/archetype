# Repository Harness

Archetype has two evaluation surfaces with opposite dependency directions.

| Surface | Location | What it evaluates |
|---|---|---|
| Product evaluation | `src/archetype/` | Work performed inside Archetype: persisted trajectories, dataset episodes, graders, and receipts |
| Repository harness | `tests/`, `evals/`, `bench/`, and development tooling | Archetype itself: correctness, architecture, robustness, and cost |

The product surface ships in the wheel. The repository harness does not. It is
an outer consumer of the library and MAY exercise any public boundary needed
to prove a contract. Production code MUST NOT import it.

This is why the self-harness stays at the repository root. Moving it into
`src/archetype/core/` would reverse the dependency graph: the lowest engine
layer would own code that depends on the whole stack, developer tooling, and
test-only infrastructure. “Harness” is the composition of the evidence below,
not one runtime package.

## Evidence types

Each tool answers a different question.

| Evidence | Location | Question |
|---|---|---|
| Normative contract | `docs/guide/` | What must callers observe? |
| Focused test | `tests/` | Did this exact behavior or bug regress? |
| Contract matrix | Parameterized tests, usually in `tests/` | Does the same guarantee hold across its named backends, entry points, or lifecycle states? |
| Repository scenario | `evals/` | Does a broader architectural invariant survive a realistic composition of boundaries? |
| Benchmark | `bench/` | What does one defined operation cost on a controlled machine? |
| Static audit | Ruff, `ty`, and `scripts/check_*` | Does repository structure obey a rule without executing the behavior? |
| Executable documentation | `examples/` and the docs build | Do the surfaces Archetype teaches remain runnable and internally consistent? |
| Mutation probe | `mutmut` | Would the focused assertions detect a controlled implementation error? |

BDD describes how a change is developed: state observable behavior before
implementation. It is not another test directory. In this repository the
sharper name is **contract-first development with executable contract tests**.

## Choosing the smallest oracle

Start with the narrowest evidence that can fail for the intended reason.

1. Give the behavior a focused normative clause. Prefer an existing focused
   specification and stable section identifier.
2. Add one deterministic test for the exact failure. A bug fix is incomplete
   without this regression witness.
3. Parameterize that test when the contract explicitly names several
   backends, entry points, failure stages, or schedules.
4. Add a repository scenario only when composing those dimensions reveals a
   meaningful invariant that no focused test owns by itself.
5. Use mutation testing selectively for high-risk assertions whose strength
   is otherwise hard to judge.

A repository scenario supplements the exact regression test; it never
replaces it. “The cache never loses an acknowledged append” needs a
deterministic append-versus-flush race test before it becomes a durability
scenario spanning flush triggers and storage backends.

## Scenario admission

Add or retain a task in `evals/` when all of the following are true:

- it grades externally observable outcomes rather than an implementation
  detail;
- it composes multiple meaningful dimensions, such as public entry point,
  backend, lifecycle state, or concurrency schedule;
- it provides evidence beyond the focused pytest oracle; and
- its stable task identifier traces to a normative contract.

Exact model validation, one endpoint response, and one previously reported
bug normally belong only in pytest. Structural import and manifest rules
normally belong in a static audit. The current `regression` and `spec` runner
groups predate this distinction; preserve them while existing coverage is
migrated, but do not grow them by default.

The most valuable current runner work is family-oriented: durability
atomicity, same-world serialization, runtime lifecycle, read purity, and
identity/quota behavior across the surfaces where those guarantees apply.

## Benchmark admission

A supported benchmark must:

- name the boundary being timed;
- keep setup, warmup, and measurement visibly separate;
- reject an incorrect result before writing timing data;
- record the workload configuration, revision, and environment; and
- have a documented command and an executable test for its workload/report
  contract.

One-off measurements remain experiments until they meet that bar. Benchmarks
record measurements; they do not become CI regression gates without a stable
runner, durable retention, a comparison window, and an owner who will respond
to the signal.

## Gate ownership

The ordinary Python matrix owns product tests and static checks. Repository
scenario groups run once on the harness job rather than once per supported
Python interpreter. Benchmarks stay user-triggered because shared CI hardware
does not provide a trustworthy performance baseline.

Use these entry points:

```bash
make ci          # static checks + pytest with coverage
make eval        # all current repository-check groups
make bench       # supported local ECS snapshot
make bench-query # supported local query snapshot
make mutmut      # on-demand assertion-strength probe
```

See [Repository Checks](evals.md), [Performance Benchmarking](benchmarking.md),
and [Mutation Testing](mutation-testing.md) for their focused workflows.
