# Contributing

Most changes to this repo are written as issues first and implemented in a branch with a PR. The intent is to keep work explicit, reviewable, and grounded in the contracts the repo is trying to preserve. Direct commits are fine for typo fixes and small docs edits.

## Recommended Workflow

If you want to contribute, the recommended path is:

1. Open an issue that describes the work as a prompt.
2. Include scope, acceptance criteria, and any contract constraints.
3. Have an agent work the issue in a dedicated workspace or branch.
4. Review the resulting PR as you would review any other engineering change.

For simple typo fixes or small docs changes, you can still contribute directly.
For runtime, service-layer, or engine behavior changes, issue-first is the
recommended default.

## Read These First

These documents are the current orientation pack for contributors:

| Document | Why it matters |
|---|---|
| [`README.md`](https://github.com/VangelisTech/archetype/blob/main/README.md) | Project overview, installation, quickstart, and public-facing framing |
| [`LEARNINGS.md`](https://github.com/VangelisTech/archetype/blob/main/LEARNINGS.md) | Hard-won architecture and Daft/runtime patterns. Read this before proposing structural changes. |
| [`AGENTS.md`](https://github.com/VangelisTech/archetype/blob/main/AGENTS.md) | Repository conventions, architecture boundaries, testing expectations, and contribution norms |
| [`CLAUDE.md`](https://github.com/VangelisTech/archetype/blob/main/CLAUDE.md) | Local development workflow and repo-specific guardrails for coding agents |
| [Specification Overview](specification.md) | Umbrella contract and historical context |
| [Command Gate](command-gate.md) | Policy enforcement point, roles, and audit emission |
| [Service Protocols](service-protocols.md) | Normative app service interfaces |
| [Runtime](runtime.md) | Script-boundary runtime contract |
| [Architecture](architecture.md) | High-level ECS and service-layer design |
| [Quickstart](quickstart.md) | Fastest way to get oriented with the current API surface |

If you skip one document, do not skip `LEARNINGS.md`.

## Contribution Policy

This repository is opinionated about where changes should land.

| Area | Guidance |
|---|---|
| `src/archetype/core/` | Treat as curated and effectively read-only unless the change has been explicitly approved |
| `src/archetype/app/` | Safe to extend carefully; this is where most orchestration and service work belongs |
| `src/archetype/api/`, `src/archetype/cli/`, `docs/`, `examples/`, `tests/` | Good contribution targets |

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
- public export stability
- governance-preserving ergonomics

### Executable contract suites

Some of the most important contracts are enforced directly in tests:

- [`tests/app/test_runtime_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_contracts.py)
- [`tests/app/test_runtime_fork_storage.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_runtime_fork_storage.py)
- [`tests/sync/test_sync_stack_contracts.py`](https://github.com/VangelisTech/archetype/blob/main/tests/sync/test_sync_stack_contracts.py)
- [`tests/integration/test_command_flow.py`](https://github.com/VangelisTech/archetype/blob/main/tests/integration/test_command_flow.py)
- [`tests/app/test_services.py`](https://github.com/VangelisTech/archetype/blob/main/tests/app/test_services.py)
- [`tests/cli/test_cli.py`](https://github.com/VangelisTech/archetype/blob/main/tests/cli/test_cli.py)

If you change behavior, update or add the contract test that proves the new
behavior is intentional.

## Issue Template Guidance

The best issues are written as implementation prompts.

A good issue should include:

- the user-facing or system-facing problem
- the affected layer
  for example `core`, `app`, `api`, `cli`, `docs`, or `examples`
- whether this is a bug fix, contract clarification, feature, or refactor
- acceptance criteria
- any relevant specification or requirements references
- test expectations

Good examples:

- "Make `SimulationService.run()` preserve one logical `run_id` across the full run and add regression coverage."
- "Document the world-local shutdown contract for the sugar runtime and add smoke tests."
- "Fix `QueryService` so it either implements real reads or is clearly documented as provisional."

## Development Workflow

### Setup

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
uv sync --group dev
```

### Useful commands

```bash
make test        # fast test suite
make test-cov    # test suite with coverage report
make check       # format + lint
make ci          # main gate: lint + lock-check + tests with coverage
uv run mkdocs build
```

### Before you open a PR

At minimum:

- run `make test`
- run `make check` or the relevant lint/format commands
- run `uv run mkdocs build` if you touched docs

For broad changes, prefer `make ci`.

## How to Structure Changes

Keep the work narrow and contract-driven.

- Start with the behavioral contract, not the implementation detail.
- Add or update a regression test before or alongside the fix.
- Prefer service-layer changes over core changes when both could solve the same
  problem.
- Do not bypass `iCommandService`, runtime, or world lifecycle semantics just to make
  a wrapper API feel shorter.
- Do not introduce `coder` or `maintainer` in new docs or examples; use the four-role model in [Command Gate](command-gate.md).
- If a proposed ergonomic change weakens a contract, document the tradeoff and
  get agreement before implementing it.

## Pull Requests

PRs should explain:

- what changed
- why the change is needed
- what contract it preserves, introduces, or modifies
- how it was validated

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
- the change touches `src/archetype/core/`
- the change conflicts with a documented contract

That discussion should happen before code lands.
