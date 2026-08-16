# Contributing

Archetype changes are contract work. A good contribution states the behavior it
is preserving or changing, lands in the layer that owns that behavior, and
leaves behind an executable reason to believe the result.

Most changes begin as issues and are implemented in a dedicated branch or
workspace. The issue is a compact decision surface, not paperwork: it records
the problem, the relevant contract, the likely owner, and what will count as
done. Direct commits remain appropriate for typo fixes and small documentation
edits whose behavior is not in question.

## Choose the Owning Package First

Package location states architectural ownership before any symbol is exported:

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, and reusable projections | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Generic Activity identity, claims, attempts, fences, result references, and settlement | `archetype.activities` |
| Physical storage, control catalogs, commit coordination, and generic durable world/run envelopes | `archetype.storage` |
| Family-owned workflows and internal lower-family ports | `archetype.<family>` |
| Transport and authentication | `archetype.api` |
| Concrete composition and process lifetime | `archetype.wiring` and `archetype.runtime_resources` |

A top-level family may depend on `archetype.core`, itself, third-party
libraries, and only reviewed lower top-level family contracts declared in
the root `quality/architecture.toml` policy and per-family fragments under
`quality/architecture.d/`. It never imports `archetype.app`,
`archetype.runtime`, `archetype.runtime_resources`, `archetype.wiring`,
`archetype.api`, or `archetype.cli`. Only declared lower-family contracts may
be consumed.
Every first-party top-level package or module must be classified as reserved
infrastructure or registered as a family with one exact dependency
disposition. The complete family graph is acyclic, and root-facade imports are
checked against the module that owns the exported name.

`archetype.storage` is the reviewed physical-substrate family. It owns storage
execution, control-catalog implementations and records, physical visibility,
commit coordination, and the generic durable world/run envelope. Consuming
families retain workflow meaning and orchestration while using that
substrate through the staged `iStorageService` port.

Use semantic module names: `components.py` for persistent ECS schema,
`processors.py` for processors, `contracts.py` for supported value contracts,
`transitions.py` for pure typed transition graphs, `interfaces.py` for genuine
family ports, and `service.py` for family workflow authority. A top-level
location does not make every symbol public; supported exports remain explicit
under [API Stability](api-stability.md).

For example, `archetype.missions` consumes its declared lower families. It
contains mission/task Components, relations, transition processors, authoring
values, capability-scoped sandbox resources, and the family workflow. Package
placement alone does not add a symbol to the `archetype` root facade. See
[Agent Missions V1](agent-missions.md#3-architecture-and-ownership).

## Source of Truth

When repository sources disagree, use this order:

1. The focused normative specification for the affected behavior.
2. Executable contract tests and evals.
3. The umbrella [Specification](specification.md).
4. Teaching material: guides, README, examples, and `LEARNINGS.md`.

This order is a way to locate authority, not permission to ignore a conflict.
If a focused specification and its executable oracle disagree, record the
mismatch. Stop for adjudication only when it changes public compatibility, a
core invariant, durability/concurrency/security semantics, an irreversible
migration, or a trust boundary. Otherwise follow the accepted specification,
fix or register the stale evidence, and continue. A stale teaching example
should become its own documentation fix rather than quietly steering an
unrelated implementation.

## Contract-First Issue Loop

Use this loop for bugs, features, refactors, and contract clarifications:

1. **Orient.** Read the focused specification, the nearest executable contract,
   and the implementation seam that owns the behavior.
2. **State the contract.** Describe what callers should observe, including the
   behavior that must remain unchanged.
3. **Reconcile the evidence.** Compare specification, tests, implementation,
   and teaching docs. Record contradictions instead of resolving them silently.
4. **Split the work.** Open separate issues for drift that is real but not
   required by the current change. Keep one patch responsible for one decision.
5. **Choose the oracle.** Identify the boundary test that will prove the change.
   Add one when the contract has no executable witness.
6. **Implement narrowly.** Change the lowest safe layer that owns the behavior.
   Prefer app or runtime composition when a core change is unnecessary.
7. **Validate proportionally.** Start with the closest test, then run the static
   audits and broader suites appropriate to the risk.
8. **Close the loop.** Report the resulting behavior and exact validation. Close
   the issue only when the published change is merged, not merely when a local
   patch exists.

The loop should make small changes faster by removing ambiguity. It should not
turn a typo into an architecture exercise.

## Read These First

These documents are the current orientation pack for contributors:

| Document | Why it matters |
|---|---|
| [`README.md`](https://github.com/VangelisTech/archetype/blob/main/README.md) | Project overview, installation, quickstart, and public-facing framing |
| [`LEARNINGS.md`](https://github.com/VangelisTech/archetype/blob/main/LEARNINGS.md) | Hard-won architecture and Daft/runtime patterns. Read this before proposing structural changes. |
| [`AGENTS.md`](https://github.com/VangelisTech/archetype/blob/main/AGENTS.md) | Repository conventions, architecture boundaries, testing expectations, and contribution norms |
| [`CLAUDE.md`](https://github.com/VangelisTech/archetype/blob/main/CLAUDE.md) | Local development workflow and repo-specific guardrails for coding agents |
| [Repository Harness](repository-harness.md) | Evidence types, dependency boundary, and how to choose the smallest executable oracle |
| [Specification Overview](specification.md) | Umbrella contract and historical context |
| [Application Architecture](application-architecture.md) | Normative supported boundaries, service ownership, dependency order, and lint inputs |
| [Observability](observability.md) | Safe signal vocabulary, family dispositions, process-host ownership, and telemetry authority boundaries |
| [Command Gate](command-gate.md) | Policy enforcement point, roles, and audit emission |
| [Activities](activities.md) | Resource/Activity distinction, between-tick delivery, provider reconciliation, and settlement |
| [Service Protocols](service-protocols.md) | Normative app service interfaces |
| [Runtime](runtime.md) | Script-boundary runtime contract |
| [Architecture](architecture.md) | High-level ECS and service-layer design |
| [Quickstart](quickstart.md) | Fastest way to get oriented with the current API surface |

For a behavior change, do not skip the focused specification that owns it.
Before writing processors or Daft UDFs, `LEARNINGS.md` is also mandatory; it
records execution-model footguns that are too implementation-specific for the
normative contracts.

## Advisory AI review

Codex and Cursor Bugbot can provide review suggestions on pull requests.
Their findings are advisory inputs to maintainer judgment: neither reviewer is
a required status context, and provider failure cannot block a merge. Convert
confirmed behavioral findings into deterministic tests, lints, audits, or
operational scenarios so the same issue class does not require repeated model
review.

The retired deterministic multi-lens gate and merge-queue orchestration are
preserved only as historical incident evidence under
`quality/quarantine/review-gate/`. Do not move those files back into active
workflow or test paths. Any future review automation requires a new,
cost-bounded design and explicit approval.

## Contribution Policy

This repository is opinionated about where changes should land.

| Area | Guidance |
|---|---|
| `packages/archetype-ecs/src/archetype/core/` | Treat as curated and effectively read-only unless the change has been explicitly approved |
| `packages/archetype-ecs/src/archetype/<family>/` | Domain state, behavior, resources, and family-owned workflows; obey the declared top-level family DAG |
| `packages/archetype-ecs/src/archetype/api/`, `packages/archetype-ecs/src/archetype/cli/`, `docs/`, `examples/`, `tests/` | Good contribution targets |

If you are proposing a core behavior change, you should document the contract
first and only then change the implementation.

## Contracts We Enforce

Contributors are expected to preserve, and when needed document, the contracts
that now govern this codebase.

### Engine and app contracts

See [Specification](specification.md), [Service Protocols](service-protocols.md), [Command Gate](command-gate.md), [World Lifecycle](world-lifecycle.md), and [Execution Hierarchy](execution-hierarchy.md).

These cover:

- component and archetype identity
- append-only store behavior
- querier, updater, and processor ordering semantics
- world execution and mutation materialization
- gated command flow
- multi-world runtime isolation
- idempotent versus non-idempotent boundaries

### Runtime contracts

See [Runtime](runtime.md).

These cover:

- pure wrapper construction
- single-flight lazy activation
- honest `spawn()` return semantics
- runtime-vs-world lifetime boundaries
- explicit script ceremony
- explicit versioned public-surface changes
- governance-preserving ergonomics

### Executable contract suites

`quality/contracts.toml` is the machine-readable contract index. It maps each
approved contract to its normative source, owner, risk, pytest/static/eval
oracles, benchmarks, and execution profiles. The generated
[Contract Traceability](../reference/contract-traceability.md) page is the
reviewable view; edit the registry, never the generated table.

Some of the most important contracts are enforced directly in tests:

- [`tests/app/test_runtime_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_contracts.py)
- [`tests/storage/test_runtime_fork_storage.py`](https://github.com/VangelisTech/archetype/blob/main/tests/storage/test_runtime_fork_storage.py)
- [`tests/core/test_no_legacy_sync_kernel.py`](https://github.com/VangelisTech/archetype/blob/main/tests/core/test_no_legacy_sync_kernel.py)
- [`tests/integration/test_command_flow.py`](https://github.com/VangelisTech/archetype/blob/main/tests/integration/test_command_flow.py)
- [`tests/app/test_services.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_services.py)
- [`tests/cli/test_cli.py`](https://github.com/VangelisTech/archetype/blob/main/tests/cli/test_cli.py)

If you change behavior, update or add the contract test that proves the new
behavior is intentional. `make contract-audit` rejects unknown or orphaned
contract references, stale source anchors, and a generated traceability page
that no longer matches the registry.

## Issue Template Guidance

The best issues are usable implementation prompts. For non-trivial work, add a
contract card:

```text
Behavior:
Owning layer:
Normative source:
Existing executable oracle:
Invariants at risk:
Required validation:
Documentation affected:
```

Keep each field concrete. "Owning layer: commands dispatcher" is useful;
"backend" is not. "Invariants at risk: rejected commands must not debit quota or enter the
durable scheduler" gives a reviewer something to verify. Empty fields are
useful signals too: if no executable oracle exists, the issue has identified a
test that needs to be written.

Good examples:

- "Make managed world `run()` preserve one logical `run_id` across the full run and add regression coverage."
- "Document `RuntimeWorld`'s world-local shutdown contract and add smoke tests."
- "Fix durable world query so it either implements real reads or is clearly documented as provisional."

Do not force a contract card onto a spelling fix. Use it when implementation
choices, public behavior, or architectural ownership could reasonably diverge.

## Development Workflow

### Setup

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

### Useful commands

```bash
make test             # full parallel pytest suite
make test-contract    # tests carrying approved contract IDs
make test-integration # multi-layer tests
make test-process     # subprocess/crash/independent-writer tests
make observability-audit # signal safety and exact family dispositions
make static           # format, lint, types, lock, contracts, benchmarks
make eval-conformance # blocking regression + specification evidence
make eval-reliability # blocking retry/replay/crash/recovery evidence
make eval-capability  # blocking architectural capability evidence
make verify-pr        # static checks + fast tests required on pull requests
make verify-release   # installed-artifact release profile
make bench            # record one local ECS microbenchmark report
make bench-query      # record materialized durable-world read latency
make docs
```

Test directories identify the owning subsystem. Orthogonal markers identify
evidence type and cost: `unit`, `contract(<id>)`, `integration`, `race`,
`process`, `smoke`, `external`, and `slow`. Do not move a test merely to make a
profile select it; mark it and keep it beside its owner.

Adding, removing, or renaming a callable member of any `Protocol` anywhere
under an application family also updates that family's exact
`quality/observability/<family>.toml` disposition. Do not use wildcard or
class-wide rows. A positive span or metric disposition names same-owner
`emission_workflows`; each referenced workflow is checked against the literal
signals emitted by its exact callable, and the disposition must equal their
union. Workflow rows remain optional for unrelated internal emitters, and the
audit never infers a call graph. The deterministic audit owns syntax, declared
coverage, and that source binding; the existing footgun reviewer owns semantic
observability boundary, authority, safety, and cardinality review.

### Before you open a PR

Validation follows risk rather than patch size alone:

| Change | Expected validation |
|---|---|
| Typo or prose-only documentation | `git diff --check`; build the affected docs when navigation, links, or rendering may change |
| Executable example or public snippet | Run the example or snippet path, then build the docs |
| API, CLI, runtime, or app behavior | Closest contract/regression tests, then `make static`; use `make verify-pr` when behavior crosses service or lifecycle boundaries |
| Core, storage, concurrency, or durability | Prior contract discussion, focused failure/race coverage, `make verify-full`, and the relevant eval or infrastructure gate |
| Dependency or release metadata | Lock check plus the workflow-specific build or release validation |

Report the commands that actually ran and their outcomes. Do not write "tests
pass" when only one test ran; that one test may be exactly the right evidence,
but name it. Warnings should be identified as new, pre-existing, or unrelated.

## How to Structure Changes

Keep the work narrow and contract-driven.

- Start with the behavioral contract, not the implementation detail.
- Add or update a regression test before or alongside the fix.
- Prefer service-layer changes over core changes when both could solve the same
  problem.
- Do not bypass actor-aware dispatcher entry, runtime, or world lifecycle
  semantics just to make a wrapper API feel shorter.
- Do not introduce `coder` or `maintainer` in new docs or examples; use the four-role model in [Command Gate](command-gate.md).
- If a proposed ergonomic change weakens a contract, document the tradeoff and
  get agreement before implementing it.

## Documentation Register

Write documentation in the same register the architecture expects from code:

- Lead with the observable behavior or decision.
- Name ownership and lifecycle boundaries directly.
- Explain why an invariant exists when that reason changes how someone should
  extend the system.
- Distinguish normative requirements, current gaps, compatibility behavior,
  and historical lessons. Do not present an obsolete implementation as a hard
  architectural law.
- Prefer the recommended runtime surface in beginner material. Use service and
  core APIs only when the document is explicitly teaching those lower layers.
- Keep examples executable in spirit and syntax. If a snippet is intentionally
  abbreviated, say what was omitted.
- Link to the focused specification instead of reproducing a second, subtly
  different contract.

The goal is calm precision: enough context to make the right change, without
turning implementation history into ceremony.

## Pull Requests

PRs should explain:

- the behavior that changed
- the owning layer and why it is the right seam
- the contract preserved, introduced, or modified
- the executable oracle and exact validation results
- any adjacent drift deliberately left to a separate issue

If behavior changed, include the test that proves it.

If the change touches docs, examples, and code, keep them aligned in the same
PR whenever practical.

## What Reviewers Will Look For

Review in this repository is not just "does it work."

Reviewers should ask:

- Does this preserve the existing contract?
- If it changes the contract, is that documented explicitly?
- Is the right layer changing?
- Are the tests proving behavior or just making the suite green?
- Does this align with `LEARNINGS.md` and the spec documents?

## When to Avoid a PR

Do not send an implementation PR first when the real question is architectural.

Open an issue first if:

- the change would alter command semantics
- the change crosses runtime or world lifetime boundaries
- the change affects multi-world behavior
- the change touches `packages/archetype-ecs/src/archetype/core/`
- the change conflicts with a documented contract

That discussion should happen before code lands.
