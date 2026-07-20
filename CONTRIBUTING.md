# Contributing to Archetype

This guide covers the local workflow, CI, and pull requests. Read
`LEARNINGS.md` before changing engine behavior; the specification pages define
the contracts that tests enforce.

## Choose Package Ownership First

| Kind | Canonical location |
|---|---|
| Components, processors, pure DataFrame transforms, transition graphs, and reusable projections | `archetype.<family>` |
| Supported family value contracts | `archetype.<family>.contracts` or another specifically named family module |
| Capability-scoped resources and provider adapters implementing a family-owned protocol | A named subpackage of `archetype.<family>` |
| Durable authority, cross-family orchestration, internal service ports, and concrete application services | `archetype.app.<family>` |
| Transport, authentication, application facade, and composition | `archetype.api`, `archetype.app.gateway`, `archetype.app.application`, and `archetype.app.container` |

Top-level domain families depend inward on core and only explicitly declared
lower family contracts. They never import app, runtime, API, or CLI packages;
application authority may consume their contracts in the other direction.
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
| `make sync` | `uv sync` | Install runtime deps only |
| `make sync-dev` | `uv sync --group dev` | Install runtime + dev deps |
| `make precommit-install` | `uv run pre-commit install` | Install git hooks |
| `make precommit-run` | `uv run pre-commit run --all-files` | Run all hooks manually |

### Quality

| Target | What it does | Purpose |
|--------|-------------|---------|
| `make format` | `uv run ruff format src tests` | Auto-format code (writes files) |
| `make format-check` | `uv run ruff format --check src tests` | Check formatting (read-only) |
| `make lint` | `uv run ruff check src tests` | Lint code (read-only) |
| `make lint-fix` | `uv run ruff check src tests --fix` | Lint + auto-fix |
| `make check` | `format` + `lint` | Auto-format then lint (writes files) |

### Tests

| Target | What it does | Purpose |
|--------|-------------|---------|
| `make test` | `PYTHONPATH=src uv run pytest -q` | Fast test run, no coverage |
| `make test-cov` | `PYTHONPATH=src uv run pytest --cov --cov-branch --cov-fail-under=70` | Tests with 70% branch coverage gate |
| `make test-all` | `PYTHONPATH=src uv run pytest -v --tb=short` | Verbose test run |

### CI Gate

| Target | Steps | Purpose |
|--------|-------|---------|
| `make ci` | `format-check` + `lint` + `lock-check` + `test-cov` | **The CI gate. Run before every push.** |

`make ci` is the single command that must pass for any PR to merge. It runs
exactly what the `ci` job in GitHub Actions runs.

### Build & Release

| Target | Command | Purpose |
|--------|---------|---------|
| `make version` | reads `pyproject.toml` | Print current version |
| `make lock-check` | `uv lock --check` | Verify lockfile is in sync |
| `make build` | `uv build` | Build sdist + wheel into `dist/` |
| `make release-check` | `sync-dev` + `check` + `test-cov` + `lock-check` + `build` | Full pre-release validation |
| `make publish-test` | `uv publish` to TestPyPI | Publish to TestPyPI |
| `make publish` | `uv publish` to PyPI | Publish to PyPI |

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

Four workflows live in `.github/workflows/`. Here is exactly what each one
does and when it runs.

### `python-tests.yml` (Tests) — on push to main + PRs

The primary CI workflow. Contains three jobs:

| Job | What it runs | Required to merge? |
|-----|--------------|-------------------|
| `ci (3.12)` | `make ci` (format-check + lint + lock-check + tests w/ 70% coverage) | **Yes** |
| `format` | `ruff format --check src/ tests/` | **Yes** |
| `typecheck` | `pyright src/archetype/` | No (`continue-on-error: true`) |

The `ci` job also uploads coverage to Codecov (`fail_ci_if_error: false` — informational only).

Concurrency: grouped by `ci-${{ github.ref }}`, cancels in-progress runs on
the same branch.

### `release.yml` (Release) — on `v*` tags

Triggered by pushing a version tag (e.g. `git tag v0.5.0 && git push origin v0.5.0`).

Pipeline: `test` → `build` → `publish-testpypi` → `publish-pypi` → `github-release`

Uses trusted publishing (OIDC `id-token: write`) for both PyPI and TestPyPI.
GitHub Release is auto-created with generated release notes.

### `claude.yml` (Claude Code) — on issue/PR comments

Runs Claude Code via `@claude` mentions in issues and PR comments. Restricted
to the `everettVT` actor.

### `docs.yml` (Docs) — on every pull request and docs-related push to `main`

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

The checks run on every pull request so required status checks are never
missing.

### `daily-security-audit.yml` — daily at 09:00 UTC + manual

Runs `pip-audit` against exported dependencies, then uses Claude Code to
produce a security audit report. Creates/updates a GitHub issue with findings.

Schedule-only — does not run on pull requests.

## Make Targets vs CI Jobs

This table maps local commands to what CI actually runs, so you know exactly
what to run locally to reproduce a CI failure.

| CI Job | Local equivalent | Notes |
|--------|------------------|-------|
| `ci (3.12)` | `make ci` | Exact match — same Makefile target |
| `format` | `make format-check` | Read-only check; use `make format` to fix |
| `typecheck` | `uv run pyright src/archetype/` | No Makefile target yet; non-blocking in CI |
| `spelling` | `typos` (via `make docs-lint`) | Requires typos-cli installed locally |
| `markdown-lint` | `markdownlint-cli2` (via `make docs-lint`) | Requires markdownlint-cli2 or npx |
| `link-check` | `lychee` (via `make docs-lint`) | Requires lychee installed locally |
| `build` | `make docs` | Generates references before building |
| Release `test` | `make test-all` | Uses `pytest -v --tb=short` |
| Release `build` | `make build` | Builds sdist + wheel |

## Pre-commit Hooks

Installed via `make precommit-install`. Runs automatically on `git commit`:

| Hook | Source | What it checks |
|------|--------|----------------|
| `ruff` | `ruff-pre-commit` | Lint + auto-fix (`--fix`) on `src/` and `tests/` |
| `ruff-format` | `ruff-pre-commit` | Format check on `src/` and `tests/` |
| `uv-lock-check` | local | `uv lock --check` — lockfile in sync |
| `check-license-headers` | local | Apache 2.0 headers on `src/**/*.py` |
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
src/archetype/
  core/     # ECS engine — READ-ONLY. Do not modify without explicit approval.
  app/      # Service layer — extend carefully, always add tests.
  api/      # REST API — safe to modify freely.
  cli/      # CLI — safe to modify freely.

tests/
  core/     app/     api/     cli/
  integration/   aio/   storage/   sync/

examples/   # Load-bearing documentation — must run against current API.
bench/      # Benchmarks.
docs/       # MkDocs site — deployed at archetype.vangelis.tech/docs
```

## Testing

- **Coverage threshold:** 70% branch coverage (enforced by `make test-cov`)
- **Test layout:** mirrors `src/` structure under `tests/`
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
4. CI runs `ci` + `format` + `typecheck` (typecheck is non-blocking)
5. PRs require `ci` and `format` to pass before merge

## Dependencies

- Add runtime deps to `[project.dependencies]` in `pyproject.toml`
- Add dev deps to `[dependency-groups.dev]` in `pyproject.toml`
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
