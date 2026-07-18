# Contract-centered quality system implementation record

Status: **Implemented foundation; non-normative implementation record.** The
contract registry, architecture policy, evidence taxonomy, named eval
profiles, benchmark registry, result envelope, CI topology, installed-wheel
smoke test, and generated traceability view landed on 2026-07-17. Remaining
items in this document are explicitly labeled future extensions.

Audit status: **Phase 0 convergence accepted as of 2026-07-17.** The normative
application architecture is recorded in
[`application-architecture.md`](../guide/application-architecture.md).
Dependency lint enforcement, internal concrete services/container,
`RuntimeApplication`, trusted versus authorized ingress, gateway naming,
family-owned protocols, durable command/control authority, transactional audit
outbox, artifact ownership, and specification-over-diagram authority are
accepted directions.

Blocking enforcement is active through `quality/architecture.toml` and
`make architecture-audit`: package direction, symbol-level service-port edges,
concrete construction and inheritance, active protocol surfaces, and reviewed
exceptions. The current exception ledger is empty. Protocol completeness is
also checked by focused conformance tests and the type checker.

This record captures the chosen shape for repository testing, validation,
evaluation, robustness, benchmarking, and release checks. Normative behavior
still lives in the focused guides indexed by `quality/contracts.toml`; this
planning record explains the system and preserves the rationale without
becoming a competing specification.

The implementation proceeded contract slice by contract slice. A slice may be implemented
once its normative behavior, ownership, and oracle are clear, even while other
contract families remain under audit. Existing checks continue to protect
current behavior, but their existence is not by itself evidence that the
behavior is the desired long-term contract.

## Outcome

Build one contract-centered quality system without collapsing every kind of
evidence into one test suite:

```text
Approved normative contracts
            |
            v
      Contract registry
       /    |    |    \
      /     |    |     \
 static  pytest evals  benchmarks
 checks         |       and release
                |
          robustness probes
            |
            v
 quick -> PR -> main -> nightly -> release
```

Static checks, deterministic tests, behavioral evals, robustness probes,
benchmarks, and artifact validation answer different questions. They should
share contract traceability, execution profiles, result metadata, and CI
orchestration rather than a single runner or a single pass/fail policy.

### Repository harness boundary

The repository harness and Archetype's product evaluation features have
different subjects:

- The top-level repository harness (`tests/`, `evals/`, `bench/`, static
  scripts, documentation checks, and CI) evaluates Archetype itself.
- Product evaluation under `src/archetype/` evaluates work performed by a
  simulation or application built with Archetype.

Shared vocabulary or report primitives do not make these the same subsystem.
The repository harness should remain outside the published library unless a
specific reusable primitive has an independently justified product use case.
Tests, repository evals, and benchmarks should not move into `core/` merely to
make the quality system look physically unified.

## Phase 0: converge the contracts first

This phase is the authority-setting track, not a global stop-the-world gate.
It runs ahead of, and may overlap with, implementation slices whose contracts
are already unambiguous.

### Audit method

For each behavior that appears normative today:

1. Inventory the focused specification, umbrella specification, executable
   checks, implementation behavior, and teaching material that describe it.
2. Rewrite the candidate contract as an externally observable statement. Avoid
   preserving an implementation mechanism unless the mechanism itself is part
   of the public guarantee.
3. Record disagreements instead of selecting a winner implicitly.
4. Decide whether to **retain**, **rewrite**, **retire**, **defer**, or mark the
   claim as **non-normative guidance**.
5. Define success, failure, retry, concurrency, durability, and lifecycle
   semantics wherever they are relevant.
6. Assign the owning layer, risk, and decision owner.
7. Identify an independent executable oracle, or explicitly record why the
   claim is design-only and cannot yet be executed.
8. Approve a stable contract ID only after the behavior is accepted.

Each accepted contract should make its preconditions and scope, observable
action and result, ordering, failure atomicity, retry behavior, applicable
backends and public entry points, and explicit non-goals reviewable without
reading its implementation.

The audit should begin with focused specifications, then compare executable
contracts, the umbrella specification, implementation, guides, examples, and
current issue reports. Existing source priority helps locate discrepancies; it
must not predetermine which disputed behavior survives the audit.

### Audit order

The audit should resolve decisions that change the meaning of all downstream
evidence before cataloging individual clauses:

**Accepted architectural direction:** dependency order and clean encapsulation
from the application service layer downward will be enforced by blocking static
lint. The audit determines the exact service wiring, the acyclic static
dependency graph derived from it, active interfaces, intentional inheritance
families, and exceptions; it does not determine whether enforcement is
optional.

**Accepted public-surface decision:** concrete application services and
`ServiceContainer` are internal implementation machinery, not supported public
APIs. Their former top-level exports were removed rather than preserved as a
supported surface.

**Accepted application-facade decision:** `RuntimeApplication` is the canonical
actor-free application seam. Trusted Python uses
`ArchetypeRuntime -> RuntimeApplication`; untrusted ingress uses authentication
`-> CommandGateway -> RuntimeApplication`. The runtime carries no `ActorCtx`.

**Accepted gateway decision:** `CommandGateway` is a stateless authorization
gateway rather than a peer domain service. Command
scheduling, dispatch, and durable state belong to the commands family. Access
audit is gateway-adjacent; workflow evidence belongs to transactional family
outboxes and an eventual audit projection.

**Accepted gateway naming:** the concrete class and protocol names are
`CommandGateway` and `iCommandGateway`.

**Accepted convergence policy:** written ownership/dependency rules and the
machine-readable policy are authoritative; diagrams are explanatory.
Ordinary naming, protocol completion, test placement, and migration sequencing
proceed by best judgment. Escalation is reserved for public compatibility,
core invariants, materially different durability/concurrency/security
semantics, destructive migrations, or new trust boundaries.

1. Complete the supported public-surface classification and family wiring
   around those decisions, then derive the static dependency graph.
2. Decide whether service protocols are normative extension boundaries,
   documentation interfaces, or both; reconcile them with concrete wiring and
   the composition policy.
3. Name the supported execution profiles, including whether coordinated
   service/runtime worlds and uncoordinated core/sync worlds intentionally have
   different guarantees.
4. Build the contract and contradiction inventory with one authoritative home
   for each approved requirement.
5. Map existing pytest, static checks, and eval tasks to that inventory without
   treating current coverage as automatic ratification.
6. Reclassify test scale and evidence purpose, then consolidate architecture
   checks around the approved wiring and derived static dependency graph.
7. Decide which broader scenario families, performance questions, and release
   compatibility guarantees justify additional evidence.
8. Simplify CI only after evidence ownership and failure policies are explicit.

### Audit deliverables

- An approved inventory of normative contracts.
- A decision ledger containing retained, rewritten, retired, deferred, and
  non-normative claims, with rationale.
- A stable ID namespace and an owner and risk classification for each approved
  contract.
- Explicit decisions for ambiguous failure, retry, ordering, lifetime,
  concurrency, and compatibility semantics.
- A contradiction and coverage-gap list linking specifications,
  implementation, tests, evals, examples, and open issues.
- A support policy for public APIs, serialized data, storage backends, Python
  versions, CLI behavior, and release artifacts.
- A complete public-surface table that classifies every top-level export,
  documented import path, generated reference entry, CLI family, and REST
  family, including a migration treatment for unsupported service exports.
- One approved, machine-readable architecture policy defining dependency
  edges, active service interfaces, intentional inheritance, and reviewed
  exceptions from the service layer downward.

### Exit criteria

Phase 0 is complete only when:

- the target contract set is reviewable without reading the implementation;
- every known contradiction has a decision or an explicitly owned deferral;
- every approved contract has an owning layer and observable acceptance
  criteria;
- the contract ID and change-control conventions are accepted; and
- the service wiring, derived static dependency graph, and encapsulation policy
  are precise enough to encode without the checker inventing architecture; and
- implementation work can distinguish behavior to preserve from behavior that
  merely happens to exist today.

No registry migration, suite reclassification, or release gate should
hard-code semantics that remain materially disputed. This does not block
architecture lint or focused contract work whose normative source is already
accepted.

## Evidence model

Checks are organized into six lanes. A scenario may contribute to
more than one lane, but each check should have one primary purpose and one clear
failure policy.

| Lane | Question answered | Typical tools | Default policy |
|---|---|---|---|
| Static validation | Is the repository internally consistent before execution? | Ruff, ty, dependency-DAG and encapsulation lint, lock checks, documentation and license checks, security scanners | Blocking |
| Deterministic tests | Does a focused behavior produce the required result? | Pytest unit, contract, integration, race, process, and smoke tests | Blocking |
| Behavioral evals | Does a realistic public-boundary scenario satisfy an independent oracle? | Task -> trial -> grader harness | Blocking or advisory by named suite |
| Robustness probes | Does the behavior survive broader inputs, schedules, and controlled failures? | Property tests, fault injection, mutation, fuzzing, repeated trials, soak tests | Targeted on PRs; broader on a schedule |
| Performance benchmarks | What does correct behavior cost, and how is that cost changing? | Micro, service, contention, recovery, scaling, resource, and end-to-end suites | Advisory until a stable runner and baseline exist |
| Artifact and compatibility validation | Does the thing being released work as installed and remain compatible with the support policy? | Wheel/sdist smoke tests, API snapshots, prior-version fixtures, provenance and dependency checks | Release-blocking |

Coverage and AI-assisted review are supporting signals, not additional proof
lanes. Coverage finds unexecuted code; it does not prove that assertions are
meaningful. AI review can identify suspicious patterns; deterministic evidence
must still establish the claimed behavior.

Every blocking gate also needs its own truthful contract: a passing result must
prove the named postcondition, not merely that a command exited zero. Required
scopes and result sets must fail when empty, external publication must be
verified when it is part of success, and infrastructure uncertainty must fail
closed unless the check is explicitly advisory. Review findings remain claims
to adjudicate with repository evidence, not authority by themselves.

## Contract registry

`quality/contracts.toml` is the machine-readable index of approved normative
behavior. Its records use this shape:

```toml
[[contract]]
id = "runtime.shutdown.drains_admitted_work"
source = "docs/guide/runtime.md"
section = "Runtime shutdown"
owner = "runtime"
risk = "high"
pytest = ["tests/app/test_runtime_contracts.py"]
evals = ["reliability.runtime_shutdown"]
benchmarks = []
profiles = ["pr", "main", "release"]
```

`scripts/validate_contracts.py` validates these properties:

- every approved normative requirement has one stable ID;
- each ID points to its focused normative source and owning layer;
- each contract has at least one executable oracle or an explicit design-only
  exception;
- contract tests and eval tasks declare which IDs they witness;
- documentation tables and traceability checks are generated from the
  registry instead of maintained as parallel hand-written inventories; and
- deleted or superseded IDs remain explainable through the decision ledger.

Only normative behavior belongs in the registry. Ordinary unit tests,
implementation details, code-style rules, and teaching examples do not need a
contract ID. Traceability should clarify authority, not turn every test into
governance metadata.

The pytest interface is:

```python
@pytest.mark.contract("runtime.shutdown.drains_admitted_work")
async def test_shutdown_waits_for_admitted_work() -> None:
    ...
```

The registry validator fails on an unknown ID, a normative contract with
no oracle or approved exception, an eval with no contract mapping, or a stale
source link. `scripts/generate_contract_traceability.py` renders the registry
into `docs/reference/contract-traceability.md`; `make contract-audit` also
rejects a stale generated view.

## Deterministic test structure

Keep tests physically close to their owning layer, such as `tests/core/`,
`tests/app/`, `tests/api/`, and `tests/runtime/`. Do not reorganize the entire
tree merely to encode execution cost.

Orthogonal pytest markers are registered in `pyproject.toml`:

- `unit`: one local seam with no meaningful cross-layer lifecycle;
- `contract(<id>)`: an approved normative behavior;
- `integration`: multiple real repository layers;
- `race`: a controlled concurrency schedule;
- `process`: subprocess, crash, or independent-writer behavior;
- `smoke`: a shallow public-path or artifact check;
- `external`: credentials or external infrastructure required; and
- `slow`: unsuitable for the quick local profile.

A test may carry multiple markers, for example `contract`, `race`, and
`integration`. Directory answers who owns the behavior; markers answer what
kind of evidence it is and where it runs.

Concurrency tests should coordinate with events, barriers, failpoints, and
observable state transitions. Sleeps may enforce a timeout but should not
define the winning schedule. Every fixed defect should first receive the
smallest deterministic regression oracle that would have caught it.

## Behavioral eval structure

The independent task -> trial -> grader model is grouped by named profiles
whose policy lives in `quality/eval_profiles.toml`:

| Profile | Current suites | Failure policy |
|---|---|---|
| Conformance | `regression`, `spec` | Every required task and trial passes |
| Reliability | `idempotency` | Every required retry, replay, crash, race, and recovery trial passes |
| Capability | `capability` | Every architectural scenario and trial passes |

The task IDs remain stable even where their suite is grouped under a newer
profile name. Structural manifest checks also run as static validation;
behavioral specification claims run in conformance; retry, replay, process,
and fault scenarios run in reliability; and capability scoring cannot silently
change its exit policy independently of the registry.

Pytest and evals may share public fixture builders and data factories. They
should not share expected-result logic: an eval is useful precisely because its
oracle does not simply call the assertion helper used by the feature test.

## Robustness program

Layer broader evidence on top of deterministic contract tests:

- Use property testing for serialization round trips, signature algebra,
  bounds, identifiers, filters, and command state machines.
- Add deterministic failpoints around append, flush, cancellation, catalog
  registration, activation, publication, and stale-writer fencing.
- Keep mutation testing targeted at high-risk invariants. Archive surviving
  mutants and expand scope only when the runtime cost is understood.
- Repeat race and subprocess scenarios with the seed and schedule inputs in the
  result artifact.
- Run fuzzing against parsers, command payloads, filter expressions, and
  serialized compatibility boundaries that the audit confirms as supported.
- Add soak profiles for memory growth, cache churn, history growth, repeated
  activation, and shutdown.

Use branch-aware coverage and a ratchet or diff-coverage policy to expose new
gaps. Use mutation results, not line coverage alone, to test whether important
assertions can detect plausible faults.

## Execution profiles

Intent-oriented profiles are exposed locally and in CI. The feedback ranges
below remain planning targets, not service-level objectives.

| Profile | Intended contents | Target feedback |
|---|---|---|
| Quick | Changed-file static checks and focused unit/contract tests | Under 1 minute |
| PR | All static validation, deterministic test matrix, coverage once, conformance once, docs/examples/package smoke | Around 10 minutes |
| Main | PR profile plus subprocess and available external-infrastructure checks | Around 20 minutes |
| Nightly | Repeated reliability trials, backend matrix, property/fuzz tests, mutation, security, soak, and stable-runner benchmarks | Scheduled; no PR budget |
| Release | Passed main commit plus built-artifact, compatibility, required eval, version, API, documentation, provenance, and dependency checks | Release-blocking |

Implemented leaf targets:

```text
make static
make test-unit
make test-contract
make test-integration
make test-process
make eval-conformance
make eval-reliability
make package-smoke
make verify-pr
make verify-full
make verify-release
```

CI calls narrow leaf targets and exposes one aggregate required check. Local
composite targets call the same leaves. Version-independent linting, coverage,
evals, documentation, examples, and installed-package evidence are not hidden
inside every Python-version matrix cell.

The PR topology is:

- static validation once on the primary Python version;
- deterministic tests on every supported Python version, with coverage on one;
- conformance evals once;
- documentation, examples, and package smoke once;
- external infrastructure only when credentials and event trust permit it;
- one aggregate quality-gate job that depends on the required jobs.

## Common result envelope

`quality/results.py` provides the common metadata envelope used by eval and
benchmark JSON artifacts:

- schema version;
- commit, dirty state, and invocation profile;
- operating system, runner identity, Python, and dependency versions;
- suite, configuration, trial count, seed, and relevant dimensions;
- start time, duration, outcome, and failure policy; and
- links or paths to the native detailed report.

JUnit, coverage, mutation, and security tools retain their native formats. The
common JSON envelope supports trend reporting and auditability where the
repository owns the result schema without forcing different evidence types
into a misleading shared score.

## Benchmarking policy

`quality/benchmarks.toml` inventories the lightweight, provenance-bearing
benchmark snapshots and their correctness oracles. The validator rejects an
unknown entry point, missing correctness oracle, undeclared metric, or a
blocking comparison without a stable runner. Broader future suites may cover:

- micro operations;
- service-boundary latency;
- contention and concurrency;
- recovery and cold-resume cost;
- scaling curves;
- memory and resource use; and
- end-to-end workflows.

Every benchmark should assert correctness before recording time. Reports should
prefer distributions such as p50 and p95, throughput, memory, and scaling slope
over one elapsed-time number.

PRs on shared hosted runners should validate that the benchmark harness runs
and produces a valid report, not gate on noisy timings. A regression gate is
appropriate only after a stable named runner, durable history, sufficient
compatible baselines, and an owner and response policy are in place.

## Release and compatibility policy

The implemented release profile validates the built artifact rather than only
the source checkout:

1. Build the wheel and source distribution.
2. Install the wheel into a fresh environment.
3. Smoke-test public imports, runtime operations, API startup, and CLI behavior
   from the installed wheel.
4. Run static, deterministic, process, conformance, reliability, and docs
   checks against the release tree.

Version parity, approved API snapshots, prior-release fixtures, SBOMs, and tag
provenance remain future extensions. They become blocking only after a
compatibility window and support policy make their outcomes meaningful.

## Promotion rule for new findings

When a source scan, incident, or user report reveals suspicious behavior:

1. Add the smallest deterministic pytest regression that reproduces it.
2. Map it to a contract only if the audit has approved that behavior as
   normative; otherwise record the contract decision that is still needed.
3. Add an independent eval when the defect represents a public scenario family
   rather than one local branch.
4. Add a property or fault-injection probe when the risk spans many inputs or
   schedules.
5. Add a benchmark only when cost is part of the requirement.
6. Use targeted mutation to confirm that the assertions detect the fault class
   when the invariant is high risk.

This keeps a minimal reproduction useful immediately without accidentally
turning every bug's current implementation context into a permanent contract.

## Resolved implementation record

The migration resolved the observations that motivated this plan:

- static validation, the Python-version test matrix, conformance, capability,
  package smoke, examples, docs, process/reliability, and credentialed
  infrastructure now run as named CI jobs behind one aggregate quality gate;
- Ruff scope and version, local targets, and CI invocation are aligned;
- concrete services and `ServiceContainer` were removed from the supported
  top-level surface, while `ArchetypeRuntime` remains the recommended facade;
- family-local protocols are complete, constructor dependencies are port-typed,
  and protocol conformance has executable checks;
- durable command admission, scheduling, dispatch, retry, dead-letter, and
  settlement replaced the in-memory queue as the command authority;
- actor-free `RuntimeApplication` serves trusted runtime callers, while
  `CommandGateway` adds RBAC only for untrusted ingress;
- pytest markers distinguish evidence type and cost without relocating tests;
- capability scenarios are deterministic, credential-free, and blocking;
- package smoke installs the built wheel in a clean environment and exercises
  imports, runtime, API, and CLI surfaces; and
- benchmarks remain advisory snapshots registered with correctness oracles
  until a stable runner and response policy exist.

## Implementation sequence and status

### Phase 1: metadata — complete

- Contract and benchmark registries, validators, pytest markers, common result
  metadata, leaf Make targets, and generated traceability are active.

### Phase 2: execution topology — complete

- Dependency-order and encapsulation lint is blocking and has negative tests.
- Version-independent checks are separated from the Python matrix.
- CI exposes one aggregate required quality gate with explicit retention.

### Phase 3: deterministic gaps — complete for the audited contract set

- Accepted findings have focused contract, race, process, package, and
  migration regressions. Concurrency oracles coordinate observable states
  rather than choosing winners with sleeps.

### Phase 4: evals and robustness — complete for current profiles

- Conformance, reliability, and capability have explicit, tested failure
  semantics. Retry, replay, crash, fencing, and independent-process scenarios
  are part of reliability; targeted mutation remains on demand.

### Phase 5: performance and release — foundation complete

- Benchmark schema, registry, correctness validation, result provenance, and
  advisory comparison policy are active.
- Release validation builds and smoke-tests the installed wheel.
- Stable-runner budgets, prior-release fixtures, and SBOM/provenance policy are
  intentionally deferred until their ownership and compatibility windows are
  decided; they are extensions, not hidden incomplete gates.

## Material decisions and delegated defaults

The application-family wiring, active protocols, checker design, task
placement, concrete gateway naming, and internal role-specific names are now
implemented under
[Application Architecture](../guide/application-architecture.md).

Future choices still require explicit compatibility or product decisions:

- the supported prior-release storage/data compatibility window;
- whether the three artifact representations eventually undergo a data
  migration or remain separate durable models;
- stable performance runners, product-relevant budgets, and response owners;
- required external/model-backed eval thresholds and budgets; and
- release provenance, SBOM, signing, and retention policy.

## Non-goals

- Ratifying the current specification or executable contracts by copying them
  into a new registry.
- Replacing focused pytest tests with evals, or evals with pytest.
- Requiring every unit test or implementation rule to cite a normative
  contract.
- Reorganizing the full test directory merely to encode profile selection.
- Moving repository-only tests, evals, or benchmarks into the published core
  library simply to create a single physical harness module.
- Treating line coverage, benchmark timing, or AI review as proof of semantic
  correctness.
- Gating performance on shared-runner timings without a stable baseline.
- Treating a future compatibility or performance policy as implicitly
  approved because the harness can technically execute it.

## Definition of done for the overall program

The implemented quality-system foundation satisfies its definition of done:
approved contracts are traceable to independent evidence, every active
execution profile has an explicit failure and retention policy, CI runs each
evidence type at the cheapest useful cadence, service-layer dependency order
and encapsulation are enforced by a blocking lint with negative rule tests,
and releases validate installed artifacts. Future compatibility promises must
extend the registry and release profile when they are approved.

Contributors can answer three questions without reverse-engineering workflow
files:

1. What behavior are we promising?
2. Which evidence proves it?
3. When and where does that evidence run?
