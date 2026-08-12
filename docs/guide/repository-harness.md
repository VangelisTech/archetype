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
| 6 | Paid/external model, Modal Agent Mission, GPU, and Apple Container dogfood | Release candidate |

The PR-0 inventory declares `main` and `release` obligations; it does not by
itself prove that the current release workflow enforces them. Platform-split
execution and receipt retention land with the owning release-gate slices. A
declared cadence MUST NOT be reported as satisfied until its workflow invokes
the scenario and retains the resulting receipt.

The operator-dispatched tag workflow builds one wheel after the source profile,
records its digest, and runs every credential-free release scenario against
that exact installed artifact. OpenAI, Docker, R2, Apple Container, and the
Modal Agent Mission execute in mandatory platform/provider lanes that download
the same immutable distribution. Publication is gated by an aggregate receipt
check: every release-required scenario must have passed, every receipt must name
the release commit and wheel digest, and no result may be `not_run`.

Release execution is operator-only. The `Release tags — everettVT only`
repository ruleset permits only `everettVT` to create `v*` tags; the separate
immutable-tag ruleset continues to deny tag updates and deletion for everyone.
The workflow requires both `github.actor` and `github.triggering_actor` to be
`everettVT`, so another user's run cannot become authorized through a rerun. A
tag push does not start release work: the operator dispatches `release.yml` at
the existing tag and supplies the same tag as its confirmation input.

The Apple lane has a second, infrastructure-level boundary. Organization runner
group `archetype-release-macos` allows only this repository and an exact
tag-qualified `.github/workflows/release.yml`; the job must request that group,
not merely a guessable label. Environment `release-apple-macos` permits only
`v*` tags and requires approval from `everettVT` before the job reaches the Mac.
The runner MUST be ephemeral, MUST be registered only after the group is pinned
to the exact release tag, and MUST run from a disposable directory. A separate
macOS login is not required: the runner may use the operator's current account,
but it inherits that account's host permissions while the one authorized job is
active. Never install this runner as a persistent service. The Apple job uses
`uv` to provision Python in that account's cache; it does not use
`actions/setup-python` or create GitHub's `/Users/runner/hostedtoolcache` path.
These controls are required because GitHub does not treat a self-hosted runner
for a public repository as an isolated trusted machine.

For each release, use this order:

```bash
# 1. Create the reviewed tag. The ruleset rejects every other GitHub actor.
git fetch origin main
git tag -a v0.5.0 origin/main -m "Release v0.5.0"
git push origin refs/tags/v0.5.0

# 2. Before registering any runner, pin its group to the immutable tag.
uv run python scripts/configure_release_runner_group.py v0.5.0

# 3. In a fresh disposable runner directory, use the current organization-runner
#    package and a one-hour registration token. The current macOS account is OK.
export ARCHETYPE_RUNNER_TOKEN="$(
  gh api --method POST orgs/VangelisTech/actions/runners/registration-token \
    --jq .token
)"
./config.sh \
  --url https://github.com/VangelisTech \
  --token "$ARCHETYPE_RUNNER_TOKEN" \
  --runnergroup archetype-release-macos \
  --name archetype-release-macos \
  --labels archetype-apple-container-macos-26 \
  --ephemeral \
  --unattended
unset ARCHETYPE_RUNNER_TOKEN
./run.sh

# 4. From a second terminal, dispatch only at the same immutable tag.
gh workflow run release.yml \
  --repo VangelisTech/archetype \
  --ref v0.5.0 \
  -f tag=v0.5.0
```

Approve `release-apple-macos`, then approve `release-pypi` only after its
evidence gate succeeds. The ephemeral runner deregisters after the Apple job;
discard its working directory before preparing a rerun or future release.

Release-lane authentication is explicit and provider-scoped:

| Lane | Authentication path |
|---|---|
| OpenAI | The job receives only `OPENAI_API_KEY` from the Actions secret of the same name. |
| Docker | The runner's local Docker context and daemon authorize the parity operation; the lane does not perform registry login. |
| Cloudflare R2 | `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` authenticate the account, while `R2_API_ENDPOINT` and `R2_BUCKET` select the exact substrate. |
| Physical AI (Modal + R2) | The same scoped Modal and R2 credentials run one T4-backed seeded episode. Modal owns provider execution and immutable result blobs; the Archetype World commits intent and observation evidence to a unique R2 prefix, then a fresh runtime cold-resumes and reconciles the same digest without replay. |
| Apple Container | A one-job ephemeral, bare-metal Apple Silicon runner in the exact-workflow-restricted `archetype-release-macos` group supplies the local host authority. It may run under the operator's current macOS account and bears `self-hosted`, `macOS`, `ARM64`, and `archetype-apple-container-macos-26`. The job provisions Python 3.12 through `uv`, verifies macOS 26, and starts the local `container` service; it creates no macOS login or `/Users/runner` tool cache, and accepts no Apple cloud token. GitHub-hosted arm64 macOS runners are not substitutes because they do not expose the Virtualization.framework VM support required by Apple Container. |
| Modal Agent Mission | `MODAL_TOKEN_ID` plus `MODAL_TOKEN_SECRET` authenticate the Modal control plane. Actions repository variable `CODING_AGENT_MODAL_PROFILE` becomes the SDK selector `MODAL_PROFILE`; `CODING_AGENT_MODAL_ENVIRONMENT` becomes both the Archetype selector and SDK selector `MODAL_ENVIRONMENT`, while the workspace and remaining Environment-scoped variables bind all named objects. `CODEX_AUTH_VOLUME` supplies the separately device-authenticated Codex `auth.json`. `CODING_AGENT_GITHUB_SECRET` names the Modal Secret resolved for the isolated publisher, but the paid live lane deliberately pushes to a provider-local bare remote and never attaches that secret or mutates GitHub. Deterministic broker contracts separately prove that only the exact GitHub push process can receive `GITHUB_TOKEN`. Modal Connect Tokens authenticate the two transient viewport URLs. |

The operator-dispatched release workflow is serialized under one release
concurrency group because its configured Codex auth Volume is a static mutable
credential broker. Concurrent release runs must use distinct auth Volumes
before that serialization can be relaxed. Agent Mission provider details and
operator setup are normative in
[Agent Missions](agent-missions.md#authentication-paths-by-provider).

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

The required PR workflow owns static checks and fast product tests on Python
3.12. Repository scenarios, coverage, packaging, examples, documentation, and
compatibility evidence belong to full or release validation. Product-runtime
benchmarks stay user-triggered because shared CI hardware does not provide a
trustworthy performance baseline. The advisory CI-runner benchmark is the
narrow exception: the shared runner substrate is itself the subject, so it
runs paired jobs only after `main` moves and never becomes a regression gate.

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
