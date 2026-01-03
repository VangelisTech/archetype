SHELL := /bin/bash

.DEFAULT_GOAL := help

# ==============================================================================
# Archetype dev workflow (uv + ruff + pre-commit)
# ==============================================================================

PYTHONPATH ?= src

.PHONY: help
help:
	@echo "Archetype Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  make sync           Install runtime deps (uv)"
	@echo "  make sync-dev       Install runtime + dev deps (uv --group dev)"
	@echo ""
	@echo "Quality:"
	@echo "  make format         ruff format (src + tests)"
	@echo "  make lint           ruff check (src + tests)"
	@echo "  make lint-fix       ruff check --fix (src + tests)"
	@echo "  make check          format + lint (src + tests)"
	@echo "  make format-all     ruff format (everything)"
	@echo "  make lint-all       ruff check (everything)"
	@echo ""
	@echo "Tests:"
	@echo "  make test           pytest (fast; uses PYTHONPATH=$(PYTHONPATH))"
	@echo "  make test-cov       pytest with coverage report"
	@echo ""
	@echo "Pre-commit:"
	@echo "  make precommit-install   install git hooks"
	@echo "  make precommit-run       run hooks on all files"
	@echo ""
	@echo "Packaging / release:"
	@echo "  make lock-check     verify uv.lock matches pyproject"
	@echo "  make build          build sdist+wheel (uv build)"
	@echo "  make release-check  sync-dev + check + test + lock-check + build"
	@echo ""
	@echo "Docs (Mintlify):"
	@echo "  make docs           build docs site (mint build)"
	@echo "  make docs-serve     serve docs locally (mint dev)"
	@echo "  make docs-test      check docs for broken links (mint broken-links)"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          remove build artifacts"
	@echo "  make mcp            run MCP server (python -m archetype.mcp)"
	@echo "  make example EX=... run an example (e.g. examples/grpo_text_end_to_end.py)"


# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

.PHONY: sync
sync:
	@uv sync

.PHONY: sync-dev
sync-dev:
	@uv sync --group dev


# ------------------------------------------------------------------------------
# Quality
# ------------------------------------------------------------------------------

.PHONY: format
format:
	@uv run ruff format src tests

.PHONY: lint
lint:
	@uv run ruff check src tests

.PHONY: lint-fix
lint-fix:
	@uv run ruff check src tests --fix

.PHONY: format-all
format-all:
	@uv run ruff format .

.PHONY: lint-all
lint-all:
	@uv run ruff check .

.PHONY: check
check: format lint


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

.PHONY: test
test:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q

.PHONY: test-cov
test-cov:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q --cov=src --cov-report=term-missing


# ------------------------------------------------------------------------------
# Pre-commit
# ------------------------------------------------------------------------------

.PHONY: precommit-install
precommit-install:
	@uv run pre-commit install

.PHONY: precommit-run
precommit-run:
	@uv run pre-commit run --all-files


# ------------------------------------------------------------------------------
# Packaging / release
# ------------------------------------------------------------------------------

.PHONY: lock-check
lock-check:
	@uv lock --check

.PHONY: build
build:
	@uv build

.PHONY: release-check
release-check: sync-dev check test lock-check build
	@echo "OK: release-check passed"


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

.PHONY: docs-check-node
docs-check-node:
	@command -v node >/dev/null 2>&1 || ( \
		echo "Error: Node.js is required for docs (unless using Bun)."; \
		echo "Install Node ^18.17.0 or ^20.3.0 or >=21.0.0."; \
		exit 1; \
	)
	@node -e ' \
		const v = (process.versions && process.versions.node) ? process.versions.node : ""; \
		if (!v) { \
			console.error("Error: Unable to determine Node.js version."); \
			console.error("Required: ^18.17.0 || ^20.3.0 || >=21.0.0"); \
			process.exit(1); \
		} \
		const [maj, min, pat] = v.split(".").map((x) => parseInt(x, 10)); \
		const ok = (maj === 18 && (min > 17 || (min === 17 && pat >= 0))) \
			|| (maj === 20 && (min > 3 || (min === 3 && pat >= 0))) \
			|| maj >= 21; \
		if (!ok) { \
			console.error(`Error: Node.js ${v} is not supported by Mintlify dependencies (sharp).`); \
			console.error("Please upgrade to Node ^18.17.0 or ^20.3.0 or >=21.0.0."); \
			process.exit(1); \
		} \
	'

.PHONY: docs-check-runtime
docs-check-runtime:
	@if command -v bun >/dev/null 2>&1; then \
		echo "Using Bun for docs: $$(bun --version)"; \
	else \
		$(MAKE) docs-check-node; \
	fi

.PHONY: docs
.PHONY: docs
docs: docs-check-runtime
	@if command -v bun >/dev/null 2>&1; then \
		cd docs && bunx mint build; \
	else \
		cd docs && npx --yes mint build; \
	fi

.PHONY: docs-serve
.PHONY: docs-serve
docs-serve: docs-check-runtime
	@if command -v bun >/dev/null 2>&1; then \
		cd docs && bunx mint dev; \
	else \
		cd docs && npx --yes mint dev; \
	fi

.PHONY: docs-test
.PHONY: docs-test
docs-test: docs-check-runtime
	@if command -v bun >/dev/null 2>&1; then \
		cd docs && bunx mint broken-links; \
	else \
		cd docs && npx --yes mint broken-links; \
	fi

.PHONY: clean
clean:
	@rm -rf dist build .pytest_cache .ruff_cache
	@rm -rf src/*.egg-info src/*/*.egg-info

.PHONY: mcp
mcp:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m archetype.mcp

.PHONY: example
example:
	@test -n "$(EX)" || (echo "Set EX=examples/<file>.py" && exit 1)
	@PYTHONPATH=$(PYTHONPATH) uv run python "$(EX)"
