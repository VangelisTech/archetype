SHELL := /bin/bash

.DEFAULT_GOAL := help

# ==============================================================================
# Archetype dev workflow (uv + ruff + pre-commit)
# ==============================================================================

PYTHONPATH ?= src
VERSION := $(shell grep -m1 'version = ' pyproject.toml | cut -d'"' -f2)

.PHONY: help
help:
	@echo "Archetype Makefile (v$(VERSION))"
	@echo ""
	@echo "Setup:"
	@echo "  make sync           Install runtime deps (uv)"
	@echo "  make sync-dev       Install runtime + dev deps"
	@echo ""
	@echo "Quality:"
	@echo "  make format         Format code (ruff)"
	@echo "  make lint           Lint code (ruff)"
	@echo "  make lint-fix       Lint and auto-fix"
	@echo "  make check          Format + lint"
	@echo ""
	@echo "Tests:"
	@echo "  make test           Run tests (fast)"
	@echo "  make test-cov       Run tests with coverage"
	@echo "  make test-all       Run all tests verbose"
	@echo "  make ci             CI gate (format-check + lint + lock-check + test-cov)"
	@echo ""
	@echo "Build & Release:"
	@echo "  make build          Build sdist + wheel"
	@echo "  make release-check  Full pre-release validation"
	@echo "  make publish-test   Publish to TestPyPI"
	@echo "  make publish        Publish to PyPI"
	@echo "  make version        Show current version"
	@echo ""
	@echo "Docs:"
	@echo "  make docs           Build docs (MkDocs)"
	@echo "  make docs-serve     Serve docs locally"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          Remove build artifacts"
	@echo "  make clean-all      Remove all generated files"

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

.PHONY: format-check
format-check:
	@uv run ruff format --check src tests

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
	@PYTHONPATH=$(PYTHONPATH) uv run pytest \
		--cov=archetype \
		--cov-branch \
		--cov-report=term-missing:skip-covered \
		--cov-report=xml \
		--cov-fail-under=70

.PHONY: test-all
test-all:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -v --tb=short

.PHONY: ci
ci: format-check lint lock-check test-cov
	@echo "CI gate passed"

# ------------------------------------------------------------------------------
# Build & Release
# ------------------------------------------------------------------------------

.PHONY: version
version:
	@echo "$(VERSION)"

.PHONY: lock-check
lock-check:
	@uv lock --check

.PHONY: build
build: clean
	@echo "Building archetype v$(VERSION)..."
	@uv build
	@echo ""
	@echo "Built:"
	@ls -lh dist/

.PHONY: release-check
release-check: sync-dev check test-cov lock-check build
	@echo ""
	@echo "✅ Release check passed for v$(VERSION)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. git tag v$(VERSION)"
	@echo "  2. git push origin v$(VERSION)"
	@echo "  3. make publish (or let CI handle it)"

.PHONY: publish-test
publish-test: build
	@echo "Publishing to TestPyPI..."
	@uv publish --publish-url https://test.pypi.org/legacy/

.PHONY: publish
publish: build
	@echo "Publishing to PyPI..."
	@uv publish

# ------------------------------------------------------------------------------
# Docs (MkDocs + shadcn)
# ------------------------------------------------------------------------------

.PHONY: docs
docs:
	@uv run --extra docs mkdocs build

.PHONY: docs-serve
docs-serve:
	@uv run --extra docs mkdocs serve

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

.PHONY: clean
clean:
	@rm -rf dist build
	@rm -rf src/*.egg-info src/*/*.egg-info
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

.PHONY: clean-all
clean-all: clean
	@rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov
	@rm -rf .venv

.PHONY: precommit-install
precommit-install:
	@uv run pre-commit install

.PHONY: precommit-run
precommit-run:
	@uv run pre-commit run --all-files
