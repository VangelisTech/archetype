# Repository Harness

**Document type:** Normative repository-evidence policy.

Archetype has two evaluation surfaces with opposite dependency directions.

| Surface | Location | What it evaluates |
|---|---|---|
| Product evaluation | `packages/archetype-ecs/src/archetype/` | Work performed inside Archetype: persisted trajectories, dataset episodes, graders, and receipts |
| Repository harness | `tests/`, `evals/`, `bench/`, and development tooling | Archetype itself: correctness, architecture, robustness, and cost |

The product surface ships in the wheel. The repository harness does not. It is
an outer consumer of the library and MAY exercise any public boundary needed
to prove a contract. Production code MUST NOT import it.

This is why the self-harness stays at the repository root. Moving it into
`packages/archetype-ecs/src/archetype/core/` would reverse the dependency graph: the lowest engine
layer would own code that depends on the whole stack, developer tooling, and
test-only infrastructure. “Harness” is the composition of the evidence below,
not one runtime package.

## The harness inside a software factory

External agents may invoke repository checks; the repository owns their meaning.
A regression task can require a focused test to fail before implementation and
pass afterward. A changed-path validator must compare with an explicit,
verified base revision, since an agent may commit before validation. Include
both committed changes and untracked files. Missing or unrelated base revisions
must fail closed rather than silently narrowing the inspected delta.

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

## Deterministic model-review evidence

Schema-conforming reviewer prose is not sufficient evidence of repository
inspection. Every independent lens result MUST declare `review_status` as
`complete` or `blocked`, and only `complete` may become a verdict-bearing
reviewer receipt. A reviewer MUST report `blocked` when any required changed
file, diff, rulebook, or protected-base source could not be inspected because
of a tool, sandbox, permission, or other admission failure. It MUST NOT turn
that failure into an empty clean verdict.

The exact-scope normalizer rejects `blocked` as a verdict and gives the seat
its single bounded retry. If inspection remains blocked, the workflow records
a neutral infrastructure-failure receipt rather than findings or a clean
result. A surviving seat still owns its lens verdict; a lens whose entire
bench is neutral fails aggregation closed.

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
| 6 | Paid/external model and native Biome evidence | Release candidate |

The PR-0 inventory declares `main` and `release` obligations; it does not by
itself prove that the current release workflow enforces them. Platform-split
execution and receipt retention land with the owning release-gate slices. A
declared cadence MUST NOT be reported as satisfied until its workflow invokes
the scenario and retains the resulting receipt.

### Release profile and publisher identities

The operator-dispatched tag workflow builds the current distribution matrix
after the source profile: one wheel and one source distribution for each of
`archetype-ecs`, `archetype-research`, and the independent
`archetype-smol` teaching engine.
It validates all three wheels, package-smokes the two-package world stack and
Smol independently, rebuilds all three source distributions through isolated
PEP 517, and repeats both probes against the rebuilt wheels. Credential-free
release scenarios run against an isolated install of the exact two-wheel
world stack; OpenAI, R2, and live Biome scenario
lanes use that same stack. Publication is gated by an aggregate receipt check:
every release-required scenario must have passed, every receipt must name the
release commit and both world-stack wheel digests, and no result may be
`not_run`. The publish job uploads the recorded six files without rebuilding
them.

Before the first coordinated release, both registries need the complete OIDC
publisher matrix below. Every row uses repository `VangelisTech/archetype`.

| Project | Workflow | TestPyPI environment | PyPI environment |
|---|---|---|---|
| `archetype-ecs` | `release.yml` | `release-testpypi` | `release-pypi` |
| `archetype-research` | `publish-archetype-research.yml` | `release-testpypi` | `release-pypi` |
| `archetype-smol` | `publish-archetype-smol.yml` | `release-testpypi` | `release-pypi` |

Register pending Trusted Publishers to preconfigure the OIDC identities for
project names that do not yet exist. This registration does not reserve or
claim a name: each new name remains claimable until the first successful OIDC
publication creates the project on that registry.
Pending GitHub publishers are unique by repository, workflow, and environment,
so multiple not-yet-created projects cannot all use `release.yml` with the same
environment. PyPI also does not support reusable workflows as Trusted
Publisher identities. The release therefore keeps the established ECS identity
in `release.yml` and dispatches one direct, package-specific workflow for each
new project. The parent records the exact returned child run IDs in an immutable
allowlist; every child verifies that allowlist and the still-running authorized
parent before it can reach a protected environment. Each child publisher job is
checkout-free and receives only the two files for its distribution.
Configure both GitHub environments to permit only `v*` tags, require approval
from `everettVT`, and disable administrator bypass. The publisher action emits
PEP 740 attestations. Index
preflight permits an exact partial retry only when every existing file has the
expected publisher identity and digest-bound publish attestation; token or
manual uploads cannot satisfy that recovery path. The gate then uses pinned
`pypi-attestations` tooling to verify the Sigstore signature and transparency
evidence against the served artifact.

Registry selection and Sigstore trust selection are deliberately independent.
The verifier downloads the exact URL reported by each registry, requires
`test-files.pythonhosted.org` for TestPyPI and `files.pythonhosted.org` for
PyPI, checks those bytes against the release manifest, and then supplies the
already-fetched provenance to the pinned verifier. The pinned publisher action
signs uploads to both registries with production Sigstore trust, so TestPyPI
verification MUST NOT select the Sigstore staging roots.

Each OIDC publisher remains checkout-free. Immediately before publication it
runs one inline `git ls-remote` check against the literal canonical repository
without checking out or executing repository files or scripts. The exact
canonical `vMAJOR.MINOR.PATCH` tag must still
resolve to the workflow's original `GITHUB_SHA`; annotated tags are compared by
their peeled commit. No repository script executes with the publish identity.

Release execution is operator-only. The `Release tags — everettVT only`
repository ruleset permits only `everettVT` to create `v*` tags; the separate
immutable-tag ruleset continues to deny tag updates and deletion for everyone.
The workflow requires both `github.actor` and `github.triggering_actor` to be
`everettVT`, so another user's run cannot become authorized through a rerun. A
tag push does not start release work: the operator dispatches `release.yml` at
the existing tag and supplies the same tag as its confirmation input.

The live Biome example runs at demand cadence on a logged-in Apple Silicon
host with an active GUI/Metal session. Run `make operational-demand-biome`
from that host. The target verifies the exact artifact manifest and emits an
installed-wheel receipt. Its guardian, liveness-at-publication and cleanup
checks remain mandatory. The registry requires Darwin, git, cmake, cargo and
pkg-config, with explicit `ARCHETYPE_BIOME_LIVE=1` opt-in.

The aggregate `release-evidence-gate` requires exactly the receipts of the
release-cadence registry rows: the hermetic verification profile plus the
OpenAI and Cloudflare R2 lanes. Each receipt must be
passing installed-wheel evidence bound to the clean release commit and exact
two-wheel artifact matrix, with closed cleanup and no failed or `not_run`
result.

For each release, use this order:

```bash
# 1. Create the reviewed tag. The ruleset rejects every other GitHub actor.
git fetch origin main
git tag -a v0.6.0 origin/main -m "Release v0.6.0"
git push origin refs/tags/v0.6.0

# 2. Dispatch only at the same immutable tag.
gh workflow run release.yml \
  --repo VangelisTech/archetype \
  --ref v0.6.0 \
  -f tag=v0.6.0
```

The dispatch is the release decision. The authorize job's exact-actor and
immutable-tag checks are the admission control, and the `release-testpypi`
and `release-pypi` environments carry no reviewer gate, so the run proceeds
unattended through the evidence gate, both indexes, and the GitHub release.
The ECS publisher remains in the parent run; each remaining distribution is a
separately dispatched direct workflow run whose exact run IDs the parent
awaits and requires to succeed.

Release-lane authentication is explicit and provider-scoped:

| Lane | Authentication path |
|---|---|
| OpenAI | The job receives only `OPENAI_API_KEY` from the Actions secret of the same name. |
| Cloudflare R2 | `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` authenticate the account, while `R2_API_ENDPOINT` and `R2_BUCKET` select the exact substrate. |
| Biome | No cloud credential is accepted. The explicitly enabled release target builds and launches the pinned upstream Biome/Flecs sources on the logged-in Mac, and the live test proves the active GUI/Metal process, loopback REST readiness, native mission result, durable Archetype evidence, and cleanup. |

The operator-dispatched release workflow is serialized under one release
concurrency group: one tag, one immutable artifact set, one publish sequence
at a time.

`not_run` is never a pass, and a demand-cadence scenario is not a `not_run`:
demand cadence is a declared registry decision about when evidence is
produced, while `not_run` is an execution-time gap inside submitted evidence.
It is acceptable only when the manifest makes the lane optional at the current
cadence; release-required external evidence must name the exact
release-candidate commit and installed package. An exit code
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
may claim only fields its executable validator enforces. Grader names alone
are not proof of a stronger receipt contract.

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

The required PR workflow owns static checks and fast product tests on Python
3.12. Repository scenarios, coverage, packaging, examples, documentation, and
compatibility evidence belong to full or release validation. Benchmarks stay
user-triggered because shared CI hardware does not provide a trustworthy
performance baseline.

Use these entry points:

```bash
make ci          # required PR profile: static checks + fast tests
make observability-audit # signal safety and exact family dispositions
make operational-audit   # scenario inventory, policy, and provenance
make examples-local      # Tier-1 semantic examples
make operational-wheel   # Tier-2 installed-artifact scenarios
make operational-release # Credential-free release scenarios on one recorded wheel
make eval        # all current repository-check groups
make bench       # supported local ECS snapshot
make bench-query # supported local query snapshot
make mutmut      # on-demand assertion-strength probe
```

See [Repository Checks](evals.md), [Performance Benchmarking](benchmarking.md),
and [Mutation Testing](mutation-testing.md) for their focused workflows.
