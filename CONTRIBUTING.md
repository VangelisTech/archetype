# Contributing to Archetype

This is the single reference for development workflow, CI, and contribution
process. CLAUDE.md links here; LEARNINGS.md covers architecture.

## Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** (package manager)
- **Node.js or Bun** (docs only)

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
| `make docs` | `npx --yes mintlify build` | Build docs (requires Node.js or Bun) |
| `make docs-serve` | `npx --yes mintlify dev` | Serve docs locally with hot reload |
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

### `docs.yml` (Docs) — planned, on PRs touching `docs/**` or `**/*.md`

> **Note:** This workflow does not exist yet. The checks below can be run
> locally via `make docs-lint`. See [#65](https://github.com/VangelisTech/archetype/issues/65)
> for the tracking issue.

Documentation quality checks (planned jobs):

| Job | What it runs | Required to merge? |
|-----|--------------|-------------------|
| `spelling` | typos-cli spell check (config: `_typos.toml`) | **Yes** (Tier 0) |
| `markdown-lint` | markdownlint-cli2 (config: `.markdownlint.yaml`) | **Yes** (Tier 0) |
| `link-check` | lychee link validation (config: `lychee.toml`) | **Yes** (Tier 0) |
| `mintlify-build` | `npx --yes mintlify build` in `docs/` | **Yes** (Tier 0) |

Only triggers when docs-related files change.

### `daily-security-audit.yml` — daily at 09:00 UTC + manual

Runs `pip-audit` against exported dependencies, then uses Claude Code to
produce a security audit report. Creates/updates a GitHub issue with findings.

Schedule-only — does not run on PRs. (See [#65](https://github.com/VangelisTech/archetype/issues/65)
for planned PR-triggered doc/security checks.)

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
| `mintlify-build` | `make docs` | Requires Node.js or Bun |
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
docs/       # Mintlify docs — deployed to archetype.vangelis.tech
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

Docs use [Mintlify](https://mintlify.com/) and deploy to
`archetype.vangelis.tech`.

```bash
make docs-serve  # Local preview with hot reload
make docs        # Build (validates pages compile)
```

Config lives in `docs/mint.json`. Navigation is defined there.

## Related Documents

| Document | Purpose |
|----------|---------|
| [CLAUDE.md](CLAUDE.md) | Hard constraints for AI agents — architectural rules |
| [LEARNINGS.md](LEARNINGS.md) | Extended architectural knowledge — Daft patterns, ECS design |
