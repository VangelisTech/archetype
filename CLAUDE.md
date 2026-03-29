# CLAUDE.md — Dev Workflow Reference

## Quick Commands

```bash
make sync-dev       # Install all deps (uses uv dependency-groups, not optional-deps)
make ci             # THE gate: lint + lock-check + tests w/ coverage (what CI runs)
make test           # Fast tests, no coverage
make check          # Format + lint (ruff)
make lint-fix       # Auto-fix lint issues
```

## Dependencies

Use **`uv sync --group dev`** (dependency-groups), not `uv sync --dev` (optional-deps).

The `[dependency-groups] dev` in pyproject.toml is what CI installs. The `[project.optional-dependencies] dev` exists separately for notebook/viz extras (matplotlib, viztracer, ipykernel).

## Testing

- `make ci` is the single CI gate — always run this before pushing
- Coverage threshold: 70% with branch coverage
- Tests live in `tests/` with subdirs: `core/`, `app/`, `api/`, `cli/`, `integration/`, `aio/`, `storage/`, `sync/`

## Code Quality

- **Formatter/linter:** ruff (not black, not flake8)
- **Config:** `[tool.ruff]` in pyproject.toml — line-length 100, target py312
- **Lint rules:** E, F, I, UP, B (with E501 ignored — formatter handles line length)
- Pre-commit hooks enforce ruff, lock-check, license headers, and standard hygiene

## Project Structure

- **`src/archetype/core/`** — ECS engine. **Read-only.** Do not modify without explicit approval.
- **`src/archetype/app/`** — Service layer. Extend carefully.
- **`src/archetype/api/`** + **`cli/`** — REST API and CLI. Safe to modify freely.
- **`tests/`** — Every new feature needs tests.

## Conventions

- **Commits:** conventional commits — `feat:`, `fix:`, `docs:`, `refactor:`, `test:`
- **Components:** `_json` suffix for complex types serialized as strings
- **Processors:** one concern each, use `priority` for ordering (lower = first)
- **Imports:** ruff handles sorting (isort rules via `I` select)

## What NOT to Do

- Don't run `uv sync --dev` in CI — use `--group dev`
- Don't bypass `make ci` with raw pytest invocations for validation
- Don't modify `core/` without discussion
- Don't add deps without checking `uv lock --check` passes
