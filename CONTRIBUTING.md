# Contributing to Archetype

This guide covers the local workflow, CI, and pull requests. Read
`LEARNINGS.md` before changing engine behavior; the specification pages define
the contracts that tests enforce.

## Contribution Policy

Archetype is Apache-2.0 and developed in the open, but pull requests are
currently maintainer-only while the contract surface stabilizes: a workflow
closes outside PRs automatically. Bug reports and design discussion are
welcome as issues.

Outside contributions, when they are accepted, require commit-level provenance:

- **DCO** — every commit carries a `Signed-off-by` line (`git commit -s`)
  certifying you have the right to submit the work under Apache-2.0.

The repository also contains the intended
[Contributor License Agreement](CLA.md), which grants Vangelis Technologies
the additional rights described there. Automated CLA collection is disabled
while pull requests remain maintainer-only. Before outside contributions
reopen, Vangelis will establish an organization-wide signing and retention
process; a repository-local signatures branch is not the legal authority.

Maintainer- and bot-authored pull requests are exempt from the DCO and outside
contribution gates; their allowlists live in `.github/workflows/` (`dco.yml`
and `pr-gate.yml`).

## Choose Package Ownership First

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, and reusable projections | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Generic durable authority and framework orchestration | The owning framework family under `packages/archetype-ecs/src/archetype/` |
| Missions, Physical AI, or Research behavior | The owning world-library distribution under `packages/archetype-<library>/` |
| A library's trusted framework composition adapter | Its private `archetype.<family>._extension` module only |
| Transport and authentication | `archetype.api` |
| Concrete composition and process lifetime | `archetype.wiring` and `archetype.runtime_resources` |

Top-level domain families depend inward on core and only explicitly declared
lower family contracts. Ordinary domain modules never import runtime,
runtime-resources, wiring, API, or CLI packages. A world library depends on
`archetype-ecs`, never on another world library; only its private extension
adapter receives the reviewed framework composition context.
Every first-party top-level package or module is classified explicitly, and the
declared family graph must remain acyclic. Importing a root-facade name has the
same architectural disposition as importing its owning module. Package
placement does not make a symbol public. See the normative
[Application Architecture](docs/guide/application-architecture.md) before
adding a package or module, or moving a domain type.

## Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** (package manager)

## Setup

```bash
git clone https://github.com/VangelisTech/archetype.git
cd archetype
make sync-dev          # Install runtime + dev dependencies
make precommit-install # Set up pre-commit hooks
```

**Important:** Always use `uv sync --group dev` (dependency-groups), never
`uv sync --dev` (optional-deps). They are different commands with different
behavior. The Makefile handles this correctly.

## Make Targets

Every target below is a `.PHONY` rule. Run `make help` for the quick version.

> **Always use `make <target>`**, not the underlying commands directly.
> The Makefile runs everything through `uv run` with the correct `PYTHONPATH`.
> The "Command" column below is shorthand for readability — the Makefile is the
> source of truth.

### Setup

| Target | What it does | Purpose |
|--------|-------------|---------|
| `make sync` | `uv sync --all-packages --all-extras` | Install all workspace distributions and runtime extras |
| `make sync-dev` | the same workspace sync plus `--group dev` | Install workspace, runtime extras, and dev deps |
| `make precommit-install` | `uv run pre-commit install` | Install git hooks |
| `make precommit-run` | `uv run pre-commit run --all-files` | Run all hooks manually |

### Quality

| Target | What it does | Purpose |
|--------|-------------|---------|
| `make format` | `uv run ruff format` over package and harness roots | Auto-format code (writes files) |
| `make format-check` | `uv run ruff format --check` over package and harness roots | Check formatting (read-only) |
| `make lint` | architecture/static audits, then `ruff check` over package and harness roots | Lint and validate package boundaries (read-only) |
| `make lint-fix` | `ruff check --fix` over package and harness roots | Lint + auto-fix |
| `make check` | `format` + `lint` | Auto-format then lint (writes files) |

### Tests

| Target | What it does | Purpose |
|--------|-------------|---------|
| `make test` | `pytest -q -n auto` with all four package source roots | Fast test run, no coverage |
| `make test-cov` | `pytest --cov --cov-branch --cov-fail-under=70` with all package roots | Tests with 70% branch coverage gate |
| `make test-all` | `pytest -v --tb=short` with all package roots | Verbose test run |

### CI Gate

| Target | Steps | Purpose |
|--------|-------|---------|
| `make ci` | `make static` + `make test` | **The required PR profile. Run before every push.** |

`make ci` is the single command that must pass for any PR to merge. It runs
the same two targets as the required GitHub Actions jobs.

### Build & Release

| Target | Command | Purpose |
|--------|---------|---------|
| `make version` | reads `packages/archetype-ecs/pyproject.toml` | Print current release-line version |
| `make lock-check` | `uv lock --check` | Verify lockfile is in sync |
| `make build` | `uv build --all-packages --no-sources` | Build all four sdists and wheels into `dist/` |
| `make release-artifact` | build + wheel/sdist smoke + immutable manifest | Record the exact four-wheel/four-sdist matrix |
| `make verify-release-artifact` | verify `dist/` against `release-artifact.json` | Reject changed, missing, or extra release artifacts |
| `make verify-release` | full source profile + installed release-artifact evidence | Run the release verification profile |
| `make release-check` | sync development dependencies + `verify-release` | Prepare and verify a manual release candidate |
| `make verify-test-index` | exact-byte query + five isolated TestPyPI installs | Verify a TestPyPI rehearsal |
| `make verify-published` | exact-byte query + five isolated PyPI installs | Verify the published release |

Publication is intentionally available only through the hosted release workflow. Its
environment-scoped OIDC identities produce the provenance required by the same
workflow's registry preflight and post-publish verification. Local token uploads are
not a supported release path: even matching bytes would lack the required publisher
identity and make a partial release impossible to resume. Run `make release-check`,
then tag and dispatch the hosted workflow; never rebuild between attestation and
publication.

### Docs

| Target | Command | Purpose |
|--------|---------|---------|
| `make docs` | Generate references + `mkdocs build` | Build the docs site |
| `make docs-serve` | Build + `wrangler pages dev site/` | Preview the production Pages artifact |
| `make docs-lint` | typos + markdownlint-cli2 + lychee | Run all doc quality checks locally |

### Cleanup

| Target | Purpose |
|--------|---------|
| `make clean` | Remove `dist/`, `build/`, `__pycache__`, egg-info |
| `make clean-all` | Above + `.pytest_cache`, `.ruff_cache`, `.coverage`, `.venv` |

## CI / GitHub Actions

### `python-tests.yml` (CI) — on push to main + PRs

The primary CI workflow intentionally contains two required jobs:

| Job | What it runs | Required to merge? |
|-----|--------------|-------------------|
| `Static` | `make static` | **Yes** |
| `Tests (3.12)` | `make test` | **Yes** |

Concurrency: grouped by `ci-${{ github.ref }}`, cancels in-progress runs on
the same branch.

Coverage, evals, external infrastructure, examples, packaging, docs, and
compatibility evidence are not part of the PR merge path. They remain in the
full and release profiles. The retired deterministic review and merge-queue
workflows are preserved under `quality/quarantine/review-gate/`.

### `release.yml` (Release) — operator dispatch from an immutable `v*` tag

Create and push a version tag, select that tag in GitHub Actions, then dispatch the
Release workflow with the same tag as its input. The workflow authorizes the release
operator, requires the tag commit to be on `main`, and verifies that the tag agrees
with the package version.

The release profile builds four wheels and four sdists once, package-smokes the wheels,
rebuilds every sdist and probes their combined stack, records every digest, and runs
installed-artifact evidence.
The external-provider and
Apple lanes download that same matrix; Python 3.13 compatibility runs alongside them.
The evidence gate requires every release receipt to name all four wheel digests.

Publication then proceeds as one fail-closed chain: an unprivileged job rejects any
conflicting TestPyPI files, then the coordinator and package-specific OIDC workflows
upload exact distribution slices from the original `dist/` artifact. Isolated
environments install the base, each selective library, and the full stack from
TestPyPI. The same preflight, upload, exact-byte index verification, and five-shape
install matrix run against PyPI. The GitHub Release is created only after PyPI serves
all eight attested files and the registry-installed matrix passes. Exact matching
partial uploads are resumable; an existing filename with different bytes fails before
either publishing job receives an OIDC token. A matching pre-existing file is accepted
only when the registry Integrity API binds its publish attestation to this repository,
workflow, environment, filename, and digest, and the pinned `pypi-attestations`
verifier validates the Sigstore proof for the served file.

Before the first four-project release, register pending Trusted Publishers for the
three new project names on both PyPI and TestPyPI. This preconfigures their OIDC
identities; it does not reserve or claim the names. Each new name remains claimable
until the first successful OIDC publication creates the project on that registry.
All four projects on each registry must trust repository `VangelisTech/archetype`,
the exact workflow below, and the environment for that registry: `release-pypi` or
`release-testpypi`.

| Project | Trusted Publisher workflow |
|---|---|
| `archetype-ecs` | `release.yml` |
| `archetype-missions` | `publish-archetype-missions.yml` |
| `archetype-physical-ai` | `publish-archetype-physical-ai.yml` |
| `archetype-research` | `publish-archetype-research.yml` |

The coordinator publishes ECS directly and dispatches each other distribution's
direct workflow with an exact-run allowlist. Protect both GitHub environments to
allow only `v*` tags, require the release operator's review, and disable administrator
bypass.

The hosted OIDC chain is the sole publishing authority. The read-only
`make verify-test-index` and `make verify-published` targets are available for operator
diagnosis without creating an alternate upload path.

### `claude.yml` (Claude Code) — on issue/PR comments

Runs Claude Code via `@claude` mentions in issues and PR comments. Restricted
to the `everettVT` actor.

### `docs.yml` (Docs) — on docs-related pull requests and pushes to `main`

The workflow checks prose, links, and a generated MkDocs build. Pushes to
`main` deploy the built site to Cloudflare Pages; pull requests get a preview
when the Cloudflare secret is available.

Documentation quality checks:

| Job | What it runs | Required to merge? |
|-----|--------------|-------------------|
| `spelling` | typos-cli spell check (config: `_typos.toml`) | **Yes** (Tier 0) |
| `markdown-lint` | markdownlint-cli2 (config: `.markdownlint.yaml`) | **Yes** (Tier 0) |
| `link-check` | lychee link validation (config: `lychee.toml`) | **Yes** (Tier 0) |
| `build` | generate references + `mkdocs build` | **Yes** (Tier 0) |

These checks are advisory for merges and run only when documentation inputs
change.

### `daily-security-audit.yml` — daily at 09:00 UTC + manual

Runs `pip-audit` against exported dependencies, then uses Claude Code to
produce a security audit report. Creates/updates a GitHub issue with findings.

Schedule-only — does not run on pull requests.

## Make Targets vs CI Jobs

This table maps local commands to what CI actually runs, so you know exactly
what to run locally to reproduce a CI failure.

| CI Job | Local equivalent | Notes |
|--------|------------------|-------|
| `Static` | `make static` | Formatting, lint, types, lock, contract, and benchmark audits |
| `Tests (3.12)` | `make test` | Fast parallel test suite without coverage |
| `spelling` | `typos` (via `make docs-lint`) | Requires typos-cli installed locally |
| `markdown-lint` | `markdownlint-cli2` (via `make docs-lint`) | Requires markdownlint-cli2 or npx |
| `link-check` | `lychee` (via `make docs-lint`) | Requires lychee installed locally |
| `build` | `make docs` | Generates references before building |
| Release `test` | `make test-all` | Uses `pytest -v --tb=short` |
| Release `build` | `make build` | Builds all four sdists and wheels without workspace source overrides |

## Pre-commit Hooks

Installed via `make precommit-install`. Runs automatically on `git commit`:

| Hook | Source | What it checks |
|------|--------|----------------|
| `ruff` | `ruff-pre-commit` | Lint + auto-fix (`--fix`) on package and harness Python files |
| `ruff-format` | `ruff-pre-commit` | Format check on package and harness Python files |
| `uv-lock-check` | local | `uv lock --check` — lockfile in sync |
| `check-license-headers` | local | Apache 2.0 headers on first-party package Python files |
| `trailing-whitespace` | `pre-commit-hooks` | No trailing whitespace |
| `end-of-file-fixer` | `pre-commit-hooks` | Files end with newline |
| `check-yaml` | `pre-commit-hooks` | Valid YAML syntax |
| `check-added-large-files` | `pre-commit-hooks` | No accidentally committed binaries |
| `check-merge-conflict` | `pre-commit-hooks` | No unresolved conflict markers |

## Code Quality Rules

- **Formatter + linter:** ruff (not black, not flake8)
- **Config:** `[tool.ruff]` in `pyproject.toml`
- **Line length:** 100
- **Target:** Python 3.12
- **Lint rules:** E, F, I, UP, B (E501 ignored — formatter handles line length)
- **Import sorting:** ruff `I` rules (isort-compatible)

## Project Structure & Modification Zones

```text
packages/
  archetype-ecs/src/archetype/
    core/             # ECS engine — discuss before modifying.
    storage/          # Physical data and control authority.
    world/            # Generic world lifecycle and operations.
    commands/         # Dispatch, policy, scheduling, and audit.
    world_libraries/  # Trusted manifest contracts and discovery.
    runtime/          # Generic supported scripting API.
    api/              # Domain-free REST host.
    cli/              # Domain-free HTTP client and server startup.
  archetype-missions/src/archetype/missions/
    _extension.py     # Private trusted composition adapter.
    ...               # Coding agents, sandboxes, transcripts, trajectories.
  archetype-physical-ai/src/archetype/physical_ai/
    _extension.py     # Private trusted composition adapter.
    ...               # Physical state, policies, hosted episodes.
  archetype-research/src/archetype/research/
    _extension.py     # Private trusted composition adapter.
    ...               # AutoResearch values, ledger, and workflow.

tests/
  # Repository-level cross-package and contract harness.

packages/archetype-*/tests/
  # Distribution-owned focused tests.

examples/   # Load-bearing documentation — must run against current API.
bench/      # Benchmarks.
docs/       # MkDocs site — deployed at archetype.vangelis.tech/docs
```

The checkout is one uv workspace and one lockfile, but release artifacts are
four independently installable distributions. See
[World Libraries](docs/guide/world-libraries.md) before adding a domain package,
manifest contribution, adapter, or compatibility alias.

## Testing

- **Coverage threshold:** 70% branch coverage (enforced by `make test-cov`)
- **Test layout:** package-owned tests live beside their distribution;
  repository-level contract, integration, process, and packaging evidence stays
  under `tests/`.
- **Every new feature needs tests.** Every bug fix needs a regression test.
- `make ci` is the single gate — always run it before pushing

## Commit Conventions

Use [conventional commits](https://www.conventionalcommits.org/):

```text
feat:      New feature
fix:       Bug fix
docs:      Documentation only
refactor:  Code change that neither fixes a bug nor adds a feature
test:      Adding or updating tests
chore:     Build process, CI, deps
```

## Pull Request Process

1. Create a feature branch from `main`
2. Make changes, ensure `make ci` passes locally
3. Push and open a PR targeting `main`
4. CI runs `Static` and `Tests (3.12)`
5. Address concrete Codex and Cursor Bugbot findings
6. Squash merge manually when both required checks pass

## Dependencies

- Add a shipped dependency to `[project.dependencies]` in the owning
  `packages/archetype-*/pyproject.toml`.
- Add optional provider dependencies to the owning distribution's
  `[project.optional-dependencies]`; do not make the framework depend directly
  on a world library except through its installation convenience extras.
- Add repository-only dev or docs dependencies to `[dependency-groups]` in the
  root `pyproject.toml`.
- After changing deps: `uv lock` then verify with `uv lock --check`
- **Never** commit a lockfile that fails `uv lock --check`

## Docs

Docs use [MkDocs](https://www.mkdocs.org/) with the Material theme. The source
is `docs/`; `mkdocs.yml` defines navigation. The build generates the Python,
CLI, and REST references before rendering the site.

Public API tiers and the docstring standard are defined in
[`docs/guide/api-stability.md`](docs/guide/api-stability.md). Review that policy
before adding an export or documenting a new public workflow.

```bash
make docs-serve  # Build and preview the exact Pages artifact at /docs/
make docs        # Generate references and build the static site
```

Production documentation lives at `https://archetype.vangelis.tech/docs/`.
`https://archetype.vangelis.tech/` is a small landing page; it does not redirect.
Cloudflare Pages project configuration lives in `.github/workflows/docs.yml`.

## Related Documents

| Document | Purpose |
|----------|---------|
| [CLAUDE.md](CLAUDE.md) | Repo-specific coding rules and architectural constraints |
| [LEARNINGS.md](LEARNINGS.md) | Extended architectural knowledge — Daft patterns, ECS design |
