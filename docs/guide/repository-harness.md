# Repository Harness

**Document type:** Normative repository-evidence policy.

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

## The harness inside a software factory

An Agent Mission may invoke repository checks, but it does not absorb or own
them. Mission validators name the exact harness commands that authorize one
task transition. Changing those validators changes the factory's acceptance
policy without moving pytest, architecture audits, or CI machinery into the
missions family.

This also makes expected failure useful evidence. A regression task can require
a focused test to exit nonzero before an implementation task becomes ready;
the later task can require that same test to pass. See
[Agent Missions V1](agent-missions.md#repository-validators-are-authority)
for the dogfooded protocol.

A changed-path validator must not rely on `git status --porcelain`: an agent
may commit before the validator runs. Mission validators receive the task's
stable base SHA as `ARCHETYPE_TASK_BASE_REVISION`. A complete path inventory
combines committed and untracked changes, for example:

```bash
test -n "$ARCHETYPE_TASK_BASE_REVISION" \
  && git merge-base --is-ancestor "$ARCHETYPE_TASK_BASE_REVISION" HEAD \
  || exit 1
{
  git diff --name-only "$ARCHETYPE_TASK_BASE_REVISION" --
  git ls-files --others --exclude-standard
} | sort -u
```

If the base is missing or no longer an ancestor, repository policy should fail
closed rather than silently narrowing the inspected delta.

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

## Operational scenarios and retained receipts

`quality/operational_scenarios.toml` is the complete inventory for numbered
examples and release dogfood. Each row names one stable scenario, owning paths,
source command, applicability, evidence tier, prerequisites and explicit
missing-prerequisite policy, timeout, semantic oracle, exercised contract IDs,
cleanup policy, artifact schema, and required cadence.

`scripts/validate_operational_scenarios.py` fails closed when a numbered
example is absent, a path or contract identifier is stale, a required scenario
has no executable semantic oracle, a credentialed skip can look like a pass,
or an external workflow omits an owning path. A retained baseline declaration
also binds its JSON receipt to an exact commit, clean-tree requirement,
repository-relative in-checkout invocation, scenario/task identity, and
required grader set. Changing a revision string by hand is not evidence.
Retained receipts live under `quality/baselines/` and MUST NOT be the output
path of a verification target. Root-level eval and operational results are
ignored, transient run artifacts even when CI uploads them; running one gate
must not make a later gate report a dirty checkout.

`scripts/run_operational_scenarios.py` executes each selected scenario in a
separate temporary working and storage directory. Source mode must import from
the declared source checkout. Wheel mode removes repository `PYTHONPATH`,
installs the built artifact into an isolated environment, and rejects source
or editable-checkout leakage. The runner enforces timeouts, closes the complete
owned process group, records package identity, and classifies each outcome as
`passed`, `failed`, or `not_run`. It writes the result envelope even when
scenario setup or execution fails. Failure to remove the runner-owned isolated
working/storage tree also fails the envelope and is recorded as leaked cleanup.

The evidence tiers become applicable incrementally:

| Tier | Evidence | First blocking point |
|---:|---|---|
| 0 | Manifest, ownership, path, and provenance audit | Every PR |
| 1 | Credential-free semantic examples in isolated storage | Every PR |
| 2 | Representative scenarios against the installed wheel | Every PR |
| 3 | Loopback server, real CLI, and durable command roundtrip | Wiring/dispatcher PR |
| 4 | Process, race, crash, and leak evidence | Owning spine PR, main, release |
| 5 | Remote storage and local container providers | Applicable PR and release |
| 6 | Paid/external model, agent, GPU, and Apple Container dogfood | Release candidate |

The PR-0 inventory declares `main` and `release` obligations; it does not by
itself prove that the current release workflow enforces them. Platform-split
execution and receipt retention land with the owning release-gate slices. A
declared cadence MUST NOT be reported as satisfied until its workflow invokes
the scenario and retains the resulting receipt.

`not_run` is never a pass. It is acceptable only when the manifest makes the
lane optional at the current cadence; release-required external evidence must
name the exact release-candidate commit and installed package. An exit code
without the declared semantic oracle is not a passing operational scenario.
Only executable `pytest` and `eval` references are supported semantic oracles.
A captured JSON receipt is oracle input and retained evidence; its mere
presence or syntactic validity never proves scenario semantics.

Every deterministic example exposes
`async run_demo(storage_uri: str, ...) -> dict[str, object]`. The returned
value is portable bounded JSON and must not contain its temporary storage
location or a live capability. Human-readable `main()` remains the teaching
surface, so the runner first executes the row's declared `source_command` in
its own isolated working and storage directory. It then executes `run_demo`
once in a separate receipt-capture process and gives that exact captured value
to the focused semantic oracle. Operational JSON is limited to 1 MiB and 32
nested collection levels. An oracle that independently reruns the example is
not evidence for the captured execution. Credentialed examples therefore run
the declared teaching entry point and receipt capture separately; a future
standardized CLI receipt mode may collapse them only if it preserves both
entry-point coverage and exact semantic binding.

The generic `archetype.operational-results/v1` envelope records harness and
tested-subject provenance, Python/package identity, duration, normalized
semantics, log digests, and cleanup state. A more specific `artifact_schema`
may claim only fields its executable validator enforces. The credential-free
Agent Missions capability result is baseline eval evidence until the missions
slice supplies the full candidate/critic operational-receipt schema; grader
names alone are not that stronger receipt.

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

## Observability enforcement

Observability uses two complementary repository oracles. Independent family
manifests under `quality/observability/` declare an exact disposition for every
callable application-family protocol member and any explicitly instrumented
internal workflow. `scripts/check_observability.py` deterministically validates
that coverage plus obvious boundary, vocabulary, secret-safety, logging, and
cardinality violations. It consumes the literal vocabulary in
`archetype._obs`; it does not copy an allowlist, inspect exported telemetry, or
require a live collector.

The observability footgun lens owns the semantic remainder: whether telemetry
has become authority, whether a value is unsafe despite using an approved key,
and whether dimensions are bounded in the actual workflow. Two independent
reviewer receipts feed the shared deterministic aggregate without adding a
second required context. Focused contract tests remain the oracle for durable
outcome authority and retry/failure behavior.

## Gate ownership

The ordinary Python matrix owns product tests and static checks. Repository
scenario groups run once on the harness job rather than once per supported
Python interpreter. Benchmarks stay user-triggered because shared CI hardware
does not provide a trustworthy performance baseline.

Use these entry points:

```bash
make ci          # complete pull-request verification profile
make observability-audit # signal safety and exact family dispositions
make operational-audit   # scenario inventory, policy, and provenance
make examples-local      # Tier-1 semantic examples
make operational-wheel   # Tier-2 installed-artifact scenarios
make eval        # all current repository-check groups
make bench       # supported local ECS snapshot
make bench-query # supported local query snapshot
make mutmut      # on-demand assertion-strength probe
```

See [Repository Checks](evals.md), [Performance Benchmarking](benchmarking.md),
and [Mutation Testing](mutation-testing.md) for their focused workflows.
